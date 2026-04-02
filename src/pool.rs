//! Connection pool for [`Client`].
//!
//! [`Pool`] manages multiple [`Client`] connections and hands out
//! [`PooledClient`] guards that return the connection when dropped.
//! `Pool` is `Clone + Send + Sync` — share it freely across tasks.
//!
//! # Example
//!
//! ```rust,no_run
//! use amp_client::Pool;
//!
//! #[tokio::main]
//! async fn main() -> amp_client::Result<()> {
//!     let pool = Pool::connect("grpc://localhost:1602").await?;
//!
//!     let pool2 = pool.clone();
//!     tokio::spawn(async move {
//!         let mut c = pool2.get().await.unwrap();
//!         let _ = c.query("SELECT 1").await;
//!     });
//!
//!     let mut client = pool.get().await?;
//!     let batches = client.query("SELECT 42 AS answer").await?;
//!     println!("{} batch(es)", batches.len());
//!     Ok(())
//! }
//! ```

use std::ops::{Deref, DerefMut};

use deadpool::managed;

use crate::{
    error::{Error, Result},
    Client, ClientBuilder,
};

// ── Manager ───────────────────────────────────────────────────────────────────

#[derive(Debug)]
struct ClientManager {
    url: String,
    token: Option<String>,
}

impl managed::Manager for ClientManager {
    type Type = Client;
    type Error = Error;

    async fn create(&self) -> std::result::Result<Client, Error> {
        let mut builder = ClientBuilder::default().url(&self.url);
        if let Some(t) = &self.token {
            builder = builder.token(t);
        }
        builder.build().await
    }

    async fn recycle(
        &self,
        _client: &mut Client,
        _metrics: &managed::Metrics,
    ) -> managed::RecycleResult<Error> {
        // tonic Channel reconnects transparently — nothing to do here.
        Ok(())
    }
}

// ── PooledClient ──────────────────────────────────────────────────────────────

/// A [`Client`] checked out of a [`Pool`].
///
/// Derefs to [`Client`] — call any `Client` method directly.
/// The connection is returned to the pool automatically on drop.
pub struct PooledClient(managed::Object<ClientManager>);

impl Deref for PooledClient {
    type Target = Client;
    fn deref(&self) -> &Client { &self.0 }
}

impl DerefMut for PooledClient {
    fn deref_mut(&mut self) -> &mut Client { &mut self.0 }
}

impl From<managed::Object<ClientManager>> for PooledClient {
    fn from(obj: managed::Object<ClientManager>) -> Self { Self(obj) }
}

// ── Pool ──────────────────────────────────────────────────────────────────────

type InnerPool = managed::Pool<ClientManager, PooledClient>;

/// A cloneable, thread-safe connection pool for [`Client`].
///
/// Backed by deadpool. Cloning is cheap — all clones share the same pool.
/// Connections are opened lazily on first use.
#[derive(Clone, Debug)]
pub struct Pool(InnerPool);

impl Pool {
    /// Connect to `ampd` at `url` with default settings.
    pub async fn connect(url: impl Into<String>) -> Result<Self> {
        PoolBuilder::new(url.into()).build().await
    }

    /// Returns a [`PoolBuilder`] for fine-grained configuration.
    pub fn builder(url: impl Into<String>) -> PoolBuilder {
        PoolBuilder::new(url.into())
    }

    /// Check out a [`PooledClient`]. Waits until one is available.
    pub async fn get(&self) -> Result<PooledClient> {
        self.0.get().await.map_err(|e| Error::Pool(e.to_string()))
    }

    /// Number of idle connections currently in the pool.
    pub fn available(&self) -> usize {
        self.0.status().available.max(0) as usize
    }

    /// Maximum number of connections this pool will hold.
    pub fn max_size(&self) -> usize {
        self.0.status().max_size
    }
}

// ── PoolBuilder ───────────────────────────────────────────────────────────────

/// Builder for [`Pool`]. Obtain via [`Pool::builder`].
pub struct PoolBuilder {
    url: String,
    token: Option<String>,
    max_size: Option<usize>,
}

impl PoolBuilder {
    fn new(url: String) -> Self {
        Self { url, token: None, max_size: None }
    }

    /// Override the auth token (skips env-var and file lookup).
    pub fn token(mut self, token: impl Into<String>) -> Self {
        self.token = Some(token.into());
        self
    }

    /// Maximum pooled connections (default: 10).
    pub fn max_size(mut self, n: usize) -> Self {
        self.max_size = Some(n);
        self
    }

    /// Build the [`Pool`]. No connections are opened until [`Pool::get`] is called.
    pub async fn build(self) -> Result<Pool> {
        if self.url.is_empty() {
            return Err(Error::Config("pool URL is required".into()));
        }
        let manager = ClientManager { url: self.url, token: self.token };
        let mut builder = InnerPool::builder(manager);
        if let Some(n) = self.max_size {
            builder = builder.max_size(n);
        }
        builder
            .build()
            .map(Pool)
            .map_err(|e| Error::Config(format!("failed to build pool: {e}")))
    }
}
