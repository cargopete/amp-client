mod auth;
mod client;
mod error;

pub use client::{Client, ClientBuilder};
pub use error::{Error, Result};

// Re-export the Arrow types callers will need to work with results.
pub use arrow_array::RecordBatch;
pub use arrow_schema::Schema;

// Re-export Stream so callers don't need a separate `futures` dependency.
pub use futures::Stream;
