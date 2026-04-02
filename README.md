# amp-client

An unofficial Rust client for [Amp](https://docs.amp.xyz) — Edge & Node's blockchain-native database.

Queries run over Arrow Flight gRPC and results come back as Apache Arrow [`RecordBatch`](https://docs.rs/arrow-array/latest/arrow_array/struct.RecordBatch.html) values, making it straightforward to pipe blockchain data into Polars, DataFusion, or any other Arrow-native tool.

> **Status:** early / experimental — follows Amp v0.0.x which is itself pre-release. Expect API changes.

---

## Installation

```toml
[dependencies]
amp-client = "0.1"
tokio = { version = "1", features = ["full"] }
```

## Quick start

```rust
use amp_client::Client;

#[tokio::main]
async fn main() -> amp_client::Result<()> {
    let mut client = Client::connect("grpc://localhost:1602").await?;

    let batches = client
        .query(r#"SELECT * FROM "myproject/eth_mainnet" LIMIT 100"#)
        .await?;

    for batch in &batches {
        println!("{batch:?}");
    }

    Ok(())
}
```

## Auth

Credentials are resolved in the same priority order as the official SDKs:

| Priority | Source |
|----------|--------|
| 1 | `.token("…")` on the builder |
| 2 | `AMP_AUTH_TOKEN` environment variable |
| 3 | `~/.amp/cache/amp_cli_auth` (written by `ampctl login`) |

If none are present the client connects unauthenticated, which is fine for a local `ampd solo` instance.

## TLS

```rust
let mut client = Client::connect("grpc+tls://your-amp-host:1602").await?;
```

## Builder API

```rust
let mut client = Client::builder()
    .url("grpc+tls://your-amp-host:1602")
    .token("your-bearer-token")
    .build()
    .await?;
```

## Retry and backoff

Retry is opt-in. Configure it on the builder:

```rust
use std::time::Duration;
use amp_client::{Client, RetryConfig};

let client = Client::builder()
    .url("grpc://localhost:1602")
    .retry_config(RetryConfig {
        max_attempts:  4,
        initial_delay: Duration::from_millis(200),
        max_delay:     Duration::from_secs(10),
        jitter:        true,
    })
    .build()
    .await?;
```

Retried on: `Unavailable`, `DeadlineExceeded`, `ResourceExhausted`, `Unknown`, `Aborted`, and transport errors. Streaming (`query_stream`) is not retried.

## Polars integration

Enable the `polars` feature to get results as a Polars [`DataFrame`](https://docs.rs/polars/latest/polars/prelude/struct.DataFrame.html):

```toml
amp-client = { version = "0.1", features = ["polars"] }
```

```rust
let mut client = Client::connect("grpc://localhost:1602").await?;

let df = client
    .query_polars(r#"SELECT * FROM "eth/blocks" LIMIT 1000"#)
    .await?;

println!("{df}");

// Chain lazy operations — filter, aggregate, join, export to Parquet, etc.
use polars::prelude::*;
let summary = df.lazy()
    .select([col("block_number").max()])
    .collect()?;
println!("{summary}");
```

Conversion uses the Arrow IPC format as a zero-copy-friendly bridge between `arrow-rs` and Polars.

## DataFusion integration

Enable the `datafusion` feature to register Amp datasets as DataFusion tables:

```toml
amp-client = { version = "0.1", features = ["datafusion"] }
```

```rust
use std::sync::Arc;
use amp_client::{Pool, AmpTable};
use datafusion::prelude::*;

let pool = Pool::connect("grpc://localhost:1602").await?;
let ctx = SessionContext::new();

ctx.register_table("eth_blocks", Arc::new(AmpTable::new(pool, "eth/blocks").await?))?;

// Join Amp data with local data, run aggregations, export to Parquet — the full DataFusion ecosystem.
ctx.sql("SELECT block_number FROM eth_blocks ORDER BY block_number DESC LIMIT 10")
   .await?
   .show()
   .await?;
```

## Connection pool

For multi-threaded applications use `Pool` instead of wrapping `Client` in `Arc<Mutex<>>`:

```rust
use amp_client::Pool;

let pool = Pool::builder("grpc://localhost:1602")
    .max_size(10)
    .build()
    .await?;

// Pool is Clone — share it freely across tasks.
let pool2 = pool.clone();
tokio::spawn(async move {
    let mut client = pool2.get().await?;
    client.query("SELECT 1").await?;
    // connection returned to pool on drop
});
```

## SQL

Amp uses standard SQL. Dataset tables are referenced as `"namespace/dataset"`:

```sql
-- scalar expressions — no dataset required
SELECT current_date AS today

-- query a deployed dataset
SELECT block_number, block_hash, timestamp
FROM "acme/eth_mainnet"
WHERE block_number > 19000000
LIMIT 100
```

For small queries, results come back as `Vec<RecordBatch>`. For large datasets use `query_stream` to process batches as they arrive without buffering everything in memory:

```rust
use amp_client::Client;
use futures::{pin_mut, StreamExt};

let stream = client.query_stream(r#"SELECT * FROM "acme/eth_mainnet""#);
pin_mut!(stream);

while let Some(batch) = stream.next().await {
    let batch = batch?;
    println!("{} rows", batch.num_rows());
}
```

Results come back as `Vec<RecordBatch>`. Each batch holds typed columnar data — call `.schema()` to inspect field types and `.column(i)` to access arrays.

## Running ampd locally

Install `ampd` via [ampup](https://github.com/edgeandnode/ampup):

```sh
curl --proto '=https' --tlsv1.2 -sSf https://ampup.sh/install | sh
```

Create a minimal working directory and start in solo mode:

```sh
mkdir -p amp-dev/{data,providers,manifests}

cat > amp-dev/config.toml << 'EOF'
data_dir      = "data"
providers_dir = "providers"
manifests_dir = "manifests"
EOF

cd amp-dev
AMP_CONFIG=config.toml ampd solo --flight-server --jsonl-server
```

`ampd solo` starts the Arrow Flight server on **port 1602** and the JSON Lines HTTP server on **port 1603**. `amp-client` targets the Flight port.

---

## Testing locally

The repo includes a Rhai-based integration test runner. A single script starts `ampd`, runs the test suite, and shuts everything down:

```sh
./test.sh
```

To run a custom Rhai script instead:

```sh
./test.sh path/to/your/script.rhai
```

Inside a script, `query(sql)` returns an array of row maps:

```rhai
let rows = query("SELECT current_date AS today, 42 AS answer");
print(`today=${rows[0].today}  answer=${rows[0].answer}`);
```

See [`examples/test.rhai`](examples/test.rhai) for the full example. Requires `ampd` installed — see [Running ampd locally](#running-ampd-locally) above.

---

## Roadmap

Things planned or under consideration, roughly in order:

**~~v0.2 — streaming~~** ✓ _done_
- `query_stream()` returning `impl Stream<Item = Result<RecordBatch>>` for large result sets without materialising everything in memory

**~~v0.3 — schema introspection~~** ✓ _done_
- `list_datasets()` — enumerate deployed datasets (returns empty on a bare `ampd solo` with no manifests)
- `describe(table_ref)` — return the Arrow schema for a table reference without fetching any rows

**~~v0.4 — connection pool~~** ✓ _done_
- `Pool` / `PoolBuilder` / `PooledClient` for multi-threaded applications; `Pool` is `Clone + Send + Sync`, connections are lazy and returned on drop

**~~v0.5 — DataFusion integration~~** ✓ _done_
- `AmpTable` — a DataFusion `TableProvider` behind the `datafusion` feature flag; registers Amp datasets as queryable tables, enabling joins with local data sources

**~~Retry and backoff~~** ✓ _done_
- `RetryConfig` on `ClientBuilder` — exponential backoff with optional jitter, off by default

**~~Polars integration~~** ✓ _done_
- `query_polars()` — returns a Polars `DataFrame` directly; IPC bridge between arrow-rs v58 and Polars; `df.lazy()` for chained operations

**Unscheduled / considering**
- JSON Lines HTTP transport as a fallback for environments where gRPC is not available
- `ampctl login` OAuth flow so callers can obtain tokens programmatically
- Async iterator / `for await` ergonomics once `AsyncIterator` stabilises in Rust

Contributions and issue reports are welcome. This is MIT-licensed and entirely independent of Edge & Node.

---

## License

MIT
