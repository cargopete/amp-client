/// Demonstrate concurrent queries using the connection pool.
///
/// Start ampd first:
///   AMP_CONFIG=config.toml ampd solo --flight-server
use amp_client::Pool;

#[tokio::main]
async fn main() -> amp_client::Result<()> {
    let pool = Pool::builder("grpc://localhost:1602")
        .max_size(4)
        .build()
        .await?;

    println!("pool ready (max_size={})", pool.max_size());

    // Spawn 4 concurrent queries sharing the pool.
    let handles: Vec<_> = (1..=4)
        .map(|i| {
            let pool = pool.clone();
            tokio::spawn(async move {
                let mut client = pool.get().await?;
                let batches = client.query(&format!("SELECT {i} AS n")).await?;
                let n: i64 = batches[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow_array::Int64Array>()
                    .unwrap()
                    .value(0);
                println!("task {i}: got {n}");
                amp_client::Result::Ok(())
            })
        })
        .collect();

    for h in handles {
        h.await.unwrap()?;
    }

    println!("done — available slots: {}", pool.available());
    Ok(())
}
