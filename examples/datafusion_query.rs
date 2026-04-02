/// Query an Amp dataset using DataFusion SQL.
///
/// Requires the `datafusion` feature:
///   cargo run --example datafusion_query --features datafusion
///
/// Start ampd first:
///   AMP_CONFIG=config.toml ampd solo --flight-server
use std::sync::Arc;

use amp_client::{AmpTable, Pool};
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let pool = Pool::connect("grpc://localhost:1602").await?;

    let ctx = SessionContext::new();

    // Register an Amp dataset as a DataFusion table.
    // Swap "eth/blocks" for a dataset you have deployed.
    let table = AmpTable::new(pool, "eth/blocks").await?;
    ctx.register_table("eth_blocks", Arc::new(table))?;

    // Now use DataFusion SQL — join with local data, run aggregations, etc.
    let df = ctx
        .sql("SELECT block_number, block_hash FROM eth_blocks ORDER BY block_number DESC LIMIT 10")
        .await?;

    df.show().await?;
    Ok(())
}
