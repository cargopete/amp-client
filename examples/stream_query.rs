/// Stream results from a local `ampd solo` instance.
///
/// Start ampd first:
///   AMP_CONFIG=config.toml ampd solo --flight-server
use amp_client::Client;
use futures::{pin_mut, StreamExt};

#[tokio::main]
async fn main() -> amp_client::Result<()> {
    let mut client = Client::connect("grpc://localhost:1602").await?;

    let stream = client.query_stream("SELECT v FROM (VALUES (1),(2),(3),(4),(5)) t(v)");
    pin_mut!(stream);

    let mut total_rows = 0usize;
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        total_rows += batch.num_rows();
        println!("batch: {} row(s) — {:?}", batch.num_rows(), batch);
    }

    println!("{total_rows} row(s) total");
    Ok(())
}
