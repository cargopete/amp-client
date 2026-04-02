//! Conversion from Arrow RecordBatches (arrow-rs v58) to a Polars DataFrame.
//!
//! Uses an IPC file round-trip: arrow-ipc v58 writes the bytes, Polars reads
//! them back.  The Arrow IPC file format is stable across minor versions so
//! the conversion is lossless.

use std::io::Cursor;

use arrow_ipc::writer::FileWriter;
use polars::prelude::{DataFrame, IpcReader, PolarsError, PolarsResult, SerReader};

pub(crate) fn to_dataframe(
    schema: &arrow_schema::Schema,
    batches: &[arrow_array::RecordBatch],
) -> PolarsResult<DataFrame> {
    let buf = serialize(schema, batches)?;
    IpcReader::new(Cursor::new(buf)).finish()
}

fn serialize(
    schema: &arrow_schema::Schema,
    batches: &[arrow_array::RecordBatch],
) -> PolarsResult<Vec<u8>> {
    let mut buf = Vec::new();
    let mut writer = FileWriter::try_new(&mut buf, schema)
        .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
    for batch in batches {
        writer
            .write(batch)
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
    }
    writer
        .finish()
        .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
    Ok(buf)
}
