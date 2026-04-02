/// Demonstrates `query_polars()` — returns a Polars DataFrame directly.
///
/// Run with:
///   cargo run --example polars_query --features polars
use amp_client::Client;
use polars::prelude::*;

#[tokio::main]
async fn main() -> amp_client::Result<()> {
    let mut client = Client::connect("grpc://localhost:1602").await?;

    let df = client
        .query_polars("SELECT 1 AS id, 'hello' AS msg, 3.14 AS pi")
        .await?;

    println!("{df}");

    // Chain lazy operations — filter, select, aggregate, etc.
    let result = df
        .lazy()
        .select([col("*")])
        .collect()
        .map_err(|e| amp_client::Error::Config(e.to_string()))?;

    println!("\nLazy result:\n{result}");

    Ok(())
}
