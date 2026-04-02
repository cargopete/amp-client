use arrow_array::RecordBatch;
use arrow_flight::sql::client::FlightSqlServiceClient;
use futures::TryStreamExt;
use tonic::transport::{Channel, Endpoint};

use crate::{auth, error::{Error, Result}};

/// A connected Amp client.
///
/// Obtain one via [`Client::connect`] or [`Client::builder`].
pub struct Client {
    inner: FlightSqlServiceClient<Channel>,
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

    /// Execute a SQL query and collect all result batches.
    ///
    /// Table references follow Amp's `"namespace/table"` convention,
    /// e.g. `SELECT * FROM "eth/blocks" LIMIT 10`.
    pub async fn query(&mut self, sql: &str) -> Result<Vec<RecordBatch>> {
        let info = self.inner.execute(sql.to_owned(), None).await?;

        let mut batches = Vec::new();
        for endpoint in info.endpoint {
            let Some(ticket) = endpoint.ticket else {
                continue;
            };
            let stream = self.inner.do_get(ticket).await?;
            let mut chunk: Vec<RecordBatch> = stream.try_collect().await?;
            // Arrow Flight appends an empty schema-only sentinel batch; skip it.
            chunk.retain(|b| b.num_rows() > 0);
            batches.append(&mut chunk);
        }

        Ok(batches)
    }
}

// ── Builder ──────────────────────────────────────────────────────────────────

#[derive(Default)]
pub struct ClientBuilder {
    url: Option<String>,
    token: Option<String>,
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

    pub async fn build(self) -> Result<Client> {
        let url = self
            .url
            .ok_or_else(|| Error::Config("URL is required".into()))?;

        let endpoint = to_tonic_endpoint(&url)?;
        let channel = endpoint.connect().await?;

        let mut client = FlightSqlServiceClient::new(channel);

        if let Some(token) = auth::resolve(self.token.as_deref())? {
            client.set_header("authorization", format!("Bearer {token}"));
        }

        Ok(Client { inner: client })
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Translates Amp's `grpc://` / `grpc+tls://` schemes to the `http://` /
/// `https://` schemes that tonic's `Endpoint` expects.
fn to_tonic_endpoint(url: &str) -> Result<Endpoint> {
    let translated = url
        .replacen("grpc+tls://", "https://", 1)
        .replacen("grpc://", "http://", 1);

    translated
        .try_into()
        .map_err(|e| Error::Config(format!("invalid endpoint URL '{url}': {e}")))
}
