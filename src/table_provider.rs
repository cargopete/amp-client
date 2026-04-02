//! DataFusion [`TableProvider`] for Amp datasets.
//!
//! Enabled with the `datafusion` feature flag.
//!
//! # Example
//!
//! ```rust,no_run
//! use std::sync::Arc;
//! use amp_client::{Pool, AmpTable};
//! use datafusion::prelude::*;
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let pool = Pool::connect("grpc://localhost:1602").await?;
//!
//!     let ctx = SessionContext::new();
//!     let table = AmpTable::new(pool, "eth/blocks").await?;
//!     ctx.register_table("eth_blocks", Arc::new(table))?;
//!
//!     ctx.sql("SELECT block_number FROM eth_blocks LIMIT 10")
//!        .await?
//!        .show()
//!        .await?;
//!     Ok(())
//! }
//! ```

use std::{any::Any, fmt, io::Cursor, sync::Arc};

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::{
    arrow::{
        datatypes::SchemaRef as DfSchemaRef,
        ipc::reader::FileReader,
        record_batch::RecordBatch as DfRecordBatch,
    },
    catalog::Session,
    common::{DataFusionError, Result as DfResult},
    datasource::{MemTable, TableProvider, TableType},
    logical_expr::Expr,
    physical_plan::ExecutionPlan,
};

use crate::{error::Result, Pool};

// ── IPC conversion ────────────────────────────────────────────────────────────

/// Serialize our arrow-58 batches to IPC bytes, then deserialise using
/// DataFusion's bundled arrow (v56).  The IPC binary format is stable across
/// minor versions so the round-trip is lossless.
fn to_df_batches(
    schema: &arrow_schema::Schema,
    batches: Vec<arrow_array::RecordBatch>,
) -> DfResult<(DfSchemaRef, Vec<DfRecordBatch>)> {
    use arrow_ipc::writer::FileWriter;

    // Serialise with arrow v58 writer.
    let mut buf = Vec::new();
    {
        let mut writer = FileWriter::try_new(&mut buf, schema)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        for batch in &batches {
            writer.write(batch)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
        }
        writer.finish()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
    }

    // Deserialise with DataFusion's arrow v56 reader.
    let reader = FileReader::try_new(Cursor::new(buf), None)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let df_schema = reader.schema();
    let df_batches = reader
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    Ok((df_schema, df_batches))
}

// ── AmpTable ──────────────────────────────────────────────────────────────────

/// A DataFusion [`TableProvider`] that fetches data from an Amp dataset.
///
/// Construct with [`AmpTable::new`], then register with a DataFusion
/// [`SessionContext`](datafusion::prelude::SessionContext).
pub struct AmpTable {
    pool:    Pool,
    dataset: String,
    schema:  SchemaRef,
}

impl fmt::Debug for AmpTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AmpTable")
            .field("dataset", &self.dataset)
            .finish()
    }
}

impl AmpTable {
    /// Connect to `dataset` (e.g. `"eth/blocks"`) and cache its schema.
    pub async fn new(pool: Pool, dataset: impl Into<String>) -> Result<Self> {
        let dataset = dataset.into();
        let mut client = pool.get().await?;
        let schema = client.describe(&format!("\"{dataset}\"")).await?;
        Ok(Self { pool, dataset, schema: Arc::new(schema) })
    }

    fn build_sql(&self, limit: Option<usize>) -> String {
        let mut sql = format!("SELECT * FROM \"{}\"", self.dataset);
        if let Some(n) = limit {
            sql.push_str(&format!(" LIMIT {n}"));
        }
        sql
    }
}

#[async_trait]
impl TableProvider for AmpTable {
    fn as_any(&self) -> &dyn Any { self }

    fn schema(&self) -> DfSchemaRef {
        // Convert our arrow-58 SchemaRef to DataFusion's arrow-56 SchemaRef
        // via a minimal IPC round-trip.
        let (df_schema, _) = to_df_batches(&self.schema, vec![]).unwrap_or_else(|_| {
            (Arc::new(datafusion::arrow::datatypes::Schema::empty()), vec![])
        });
        df_schema
    }

    fn table_type(&self) -> TableType { TableType::Base }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let sql = self.build_sql(limit);

        let mut client = self.pool.get().await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let batches = client.query(&sql).await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let (df_schema, df_batches) = to_df_batches(&self.schema, batches)?;
        let mem = MemTable::try_new(df_schema, vec![df_batches])?;
        mem.scan(state, projection, filters, limit).await
    }
}
