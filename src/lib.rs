mod auth;
mod client;
mod error;
mod pool;
mod retry;
#[cfg(feature = "datafusion")]
mod table_provider;

pub use client::{Client, ClientBuilder};
pub use error::{Error, Result};
pub use pool::{Pool, PoolBuilder, PooledClient};
pub use retry::RetryConfig;
#[cfg(feature = "datafusion")]
pub use table_provider::AmpTable;

// Re-export the Arrow types callers will need to work with results.
pub use arrow_array::RecordBatch;
pub use arrow_schema::Schema;

// Re-export Stream so callers don't need a separate `futures` dependency.
pub use futures::Stream;
