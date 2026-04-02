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

## Roadmap

Things planned or under consideration, roughly in order:

**v0.2 — streaming**
- `query_stream()` returning `impl Stream<Item = Result<RecordBatch>>` for large result sets without materialising everything in memory

**v0.3 — schema introspection**
- `list_datasets()` — enumerate deployed datasets
- `describe(dataset)` — return the Arrow schema for a dataset without running a full query

**v0.4 — connection pool**
- A `Pool` type for multi-threaded applications that need shared, concurrent access without wrapping in `Arc<Mutex<>>`

**v0.5 — DataFusion integration** _(optional feature flag)_
- A DataFusion `TableProvider` that registers Amp datasets as queryable tables, enabling joins between Amp data and local data sources

**Unscheduled / considering**
- JSON Lines HTTP transport as a fallback for environments where gRPC is not available
- `ampctl login` OAuth flow so callers can obtain tokens programmatically
- Polars `LazyFrame` helper (optional feature flag)
- Retry and backoff on transient transport errors
- Async iterator / `for await` ergonomics once `AsyncIterator` stabilises in Rust

Contributions and issue reports are welcome. This is MIT-licensed and entirely independent of Edge & Node.

---

## License

MIT
