use amp_client::Client;

/// Connect to a local `ampd solo` instance and run a query.
///
/// Start ampd first:
///   cd amp-dev && AMP_CONFIG=config.toml ampd solo --flight-server --jsonl-server
#[tokio::main]
async fn main() -> amp_client::Result<()> {
    let mut client = Client::connect("grpc://localhost:1602").await?;

    // Scalar query — works without any dataset deployed
    let batches = client
        .query("SELECT current_date AS today, 42 AS answer")
        .await?;

    for batch in &batches {
        let schema = batch.schema();
        println!("schema: {schema}");
        println!("{batch:?}");
    }

    println!(
        "{} batch(es), {} row(s) total",
        batches.len(),
        batches.iter().map(|b| b.num_rows()).sum::<usize>()
    );

    Ok(())
}
