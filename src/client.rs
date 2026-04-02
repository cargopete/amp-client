use arrow_array::{Array, RecordBatch, StringArray};
use arrow_flight::{sql::client::FlightSqlServiceClient, IpcMessage};
use arrow_schema::Schema;
use async_stream::try_stream;
use futures::{Stream, StreamExt, TryStreamExt};
use tonic::transport::{Channel, Endpoint};

use crate::{
    auth,
    error::{Error, Result},
    retry::{with_retry, RetryConfig},
};

/// A connected Amp client.
///
/// Obtain one via [`Client::connect`] or [`Client::builder`].
#[derive(Debug, Clone)]
pub struct Client {
    inner: FlightSqlServiceClient<Channel>,
    retry: RetryConfig,
}

impl Client {
    /// Connect to `ampd` at `url`, resolving auth automatically.
    ///
    /// `url` accepts `grpc://host:port` and `grpc+tls://host:port`.
    pub async fn connect(url: impl Into<String>) -> Result<Self> {
        ClientBuilder::default().url(url).build().await
    }

    /// Returns a builder for fine-grained configuration.
    pub fn builder() -> ClientBuilder {
        ClientBuilder::default()
    }

    /// Execute a SQL query and return a lazy stream of record batches.
    ///
    /// Batches are yielded as they arrive; nothing is buffered in memory.
    ///
    /// **Note:** streaming operations are not retried. Use [`Client::query`]
    /// if you need automatic retry on transient failures.
    pub fn query_stream(
        &mut self,
        sql: &str,
    ) -> impl Stream<Item = Result<RecordBatch>> + Send + 'static {
        let mut client = self.inner.clone();
        let sql = sql.to_owned();

        try_stream! {
            let info = client.execute(sql, None).await?;
            for endpoint in info.endpoint {
                let Some(ticket) = endpoint.ticket else { continue };
                let stream = client.do_get(ticket).await?;
                let mut stream = std::pin::pin!(stream);
                while let Some(batch) = stream.next().await {
                    let batch = batch?;
                    if batch.num_rows() > 0 {
                        yield batch;
                    }
                }
            }
        }
    }

    /// List all datasets available on the connected Amp node.
    ///
    /// Returns names in Amp's `"namespace/table"` convention.
    /// Retry behaviour inherited from [`Client::query`].
    pub async fn list_datasets(&mut self) -> Result<Vec<String>> {
        let batches = self.query("SHOW TABLES").await?;
        let mut datasets = Vec::new();

        for batch in &batches {
            let schema_col = batch.column_by_name("table_schema")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>());
            let table_col = batch.column_by_name("table_name")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>());

            let Some(tables) = table_col else { continue };
            for row in 0..batch.num_rows() {
                let table = tables.value(row);
                let name = if !table.contains('/') {
                    if let Some(schemas) = schema_col {
                        let ns = schemas.value(row);
                        if !schemas.is_null(row) && !ns.is_empty() && ns != "public" {
                            format!("{ns}/{table}")
                        } else {
                            table.to_owned()
                        }
                    } else {
                        table.to_owned()
                    }
                } else {
                    table.to_owned()
                };
                datasets.push(name);
            }
        }

        Ok(datasets)
    }

    /// Return the Arrow schema for a table reference without fetching any rows.
    ///
    /// `table_ref` is used verbatim in `FROM` — quote Amp datasets yourself:
    /// ```rust,no_run
    /// # async fn example(mut client: amp_client::Client) -> amp_client::Result<()> {
    /// client.describe("\"eth/blocks\"").await?;
    /// client.describe("information_schema.tables").await?;
    /// # Ok(()) }
    /// ```
    pub async fn describe(&mut self, table_ref: &str) -> Result<Schema> {
        let sql   = format!("SELECT * FROM {table_ref} LIMIT 0");
        let inner = self.inner.clone();
        let retry = self.retry.clone();

        with_retry(&retry, || {
            let mut c = inner.clone();
            let s     = sql.clone();
            async move {
                let info = c.execute(s.clone(), None).await?;

                if !info.schema.is_empty() {
                    return Ok(Schema::try_from(IpcMessage(info.schema))?);
                }

                for endpoint in info.endpoint {
                    let Some(ticket) = endpoint.ticket else { continue };
                    let mut stream = c.do_get(ticket).await?;
                    if let Some(batch) = stream.try_next().await? {
                        return Ok((*batch.schema()).clone());
                    }
                }

                Err(Error::Config(format!("could not determine schema for '{s}'")))
            }
        })
        .await
    }

    /// Execute a SQL query and collect all result batches.
    ///
    /// Table references follow Amp's `"namespace/table"` convention,
    /// e.g. `SELECT * FROM "eth/blocks" LIMIT 10`.
    ///
    /// Retried on transient errors according to the client's [`RetryConfig`].
    pub async fn query(&mut self, sql: &str) -> Result<Vec<RecordBatch>> {
        let inner = self.inner.clone();
        let retry = self.retry.clone();
        let sql   = sql.to_owned();

        with_retry(&retry, || {
            let mut c = inner.clone();
            let s     = sql.clone();
            async move {
                let info = c.execute(s, None).await?;
                let mut batches = Vec::new();
                for endpoint in info.endpoint {
                    let Some(ticket) = endpoint.ticket else { continue };
                    let stream = c.do_get(ticket).await?;
                    let mut chunk: Vec<RecordBatch> = stream.try_collect().await?;
                    chunk.retain(|b| b.num_rows() > 0);
                    batches.append(&mut chunk);
                }
                Ok(batches)
            }
        })
        .await
    }
}

// ── Builder ───────────────────────────────────────────────────────────────────

#[derive(Default)]
pub struct ClientBuilder {
    url:   Option<String>,
    token: Option<String>,
    retry: RetryConfig,
}

impl ClientBuilder {
    /// Set the ampd endpoint, e.g. `grpc://localhost:1602`.
    pub fn url(mut self, url: impl Into<String>) -> Self {
        self.url = Some(url.into());
        self
    }

    /// Override the auth token (skips env-var and file lookup).
    pub fn token(mut self, token: impl Into<String>) -> Self {
        self.token = Some(token.into());
        self
    }

    /// Configure retry behaviour.
    ///
    /// By default no retries are attempted (`max_attempts = 1`).
    ///
    /// ```rust,no_run
    /// use std::time::Duration;
    /// use amp_client::{Client, RetryConfig};
    ///
    /// # #[tokio::main] async fn main() -> amp_client::Result<()> {
    /// let client = Client::builder()
    ///     .url("grpc://localhost:1602")
    ///     .retry_config(RetryConfig {
    ///         max_attempts:  4,
    ///         initial_delay: Duration::from_millis(250),
    ///         max_delay:     Duration::from_secs(8),
    ///         jitter:        true,
    ///     })
    ///     .build()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    pub fn retry_config(mut self, cfg: RetryConfig) -> Self {
        self.retry = cfg;
        self
    }

    pub async fn build(self) -> Result<Client> {
        let url = self.url.ok_or_else(|| Error::Config("URL is required".into()))?;

        let endpoint = to_tonic_endpoint(&url)?;
        let channel  = endpoint.connect().await?;

        let mut inner = FlightSqlServiceClient::new(channel);
        if let Some(token) = auth::resolve(self.token.as_deref())? {
            inner.set_header("authorization", format!("Bearer {token}"));
        }

        Ok(Client { inner, retry: self.retry })
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn to_tonic_endpoint(url: &str) -> Result<Endpoint> {
    let translated = url
        .replacen("grpc+tls://", "https://", 1)
        .replacen("grpc://", "http://", 1);

    translated
        .try_into()
        .map_err(|e| Error::Config(format!("invalid endpoint URL '{url}': {e}")))
}
