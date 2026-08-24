#![recursion_limit = "256"]

//! # helix-db Rust SDK
//!
//! The `helix-db` crate (imported as `helix_db`) is the Rust SDK for
//! [HelixDB](https://github.com/helixdb/helix-db). It pairs a query-builder DSL
//! with a small async HTTP client ([`Client`]) for running those queries
//! against a Helix instance.
//!
//! ## Crate layout
//!
//! - [`dsl`] — the query-builder DSL: traversals, predicates, batches, and the
//!   [`QueryRequest`] payload type. This is the bulk of the public API.
//! - The crate root ([`Client`], [`QueryBuilder`], [`QueryExecutionRequest`],
//!   [`HelixError`]) — the async execution surface that sends DSL queries over
//!   HTTP.
//!
//! ## The DSL
//!
//! The DSL is centered on two entry points — [`read_batch`] for read-only
//! transactions and [`write_batch`] for write-capable ones. You attach one or
//! more named traversals (each usually starting with [`g`]) via `.var_as(...)`,
//! then choose the result payload with `.returning(...)`:
//!
//! ```
//! use helix_db::dsl::prelude::*;
//!
//! let query = read_batch()
//!     .var_as(
//!         "user",
//!         g().n_where(SourcePredicate::eq("username", "alice")),
//!     )
//!     .var_as(
//!         "friends",
//!         g().n(NodeRef::var("user")).out(Some("FOLLOWS")).dedup().limit(100),
//!     )
//!     .returning(["user", "friends"]);
//! # let _ = query;
//! ```
//!
//! Most application code only needs this curated builder API, so bring the
//! prelude into scope:
//!
//! ```
//! use helix_db::dsl::prelude::*;
//! ```
//!
//! ## Running queries
//!
//! Build a [`Client`], then send a [`QueryRequest`] to `/v2/query`:
//!
//! ```no_run
//! #![recursion_limit = "256"]
//! use helix_db::Client;
//! use helix_db::dsl::prelude::*;
//! use serde::Deserialize;
//!
//! #[derive(Deserialize)]
//! struct Friends { friends: Vec<u64> }
//!
//! # async fn run(request: QueryRequest) -> Result<(), helix_db::HelixError> {
//! let client = Client::new(Some("https://cluster.helix-db.com"))?
//!     .with_api_key(Some("hx_your_api_key"))
//!     .with_database_id(Some("db_your_database_id"));
//!
//! let response: Friends = client.query(request).send().await?;
//! # let _ = response.friends;
//! # Ok(())
//! # }
//! ```
//!
//! See [`Client`] for the full request-building surface and error handling.

pub mod dsl;
pub mod graph;
pub mod lifecycle;

pub use helix_ast::error_code::{QueryErrorCode, UnknownQueryErrorCode};
pub use lifecycle::*;

#[cfg(feature = "embedded")]
use std::sync::Arc;
use std::{fmt, marker::PhantomData};

// Re-export the DSL surface (types, builders, `prelude`, etc.) at the crate
// root. This is also what makes the `crate::*` paths used inside `dsl.rs`
// resolve.
pub use dsl::*;

// Convenience re-export so `helix_db::prelude::*` is reachable directly, in
// addition to the canonical `helix_db::dsl::prelude::*`.
pub use dsl::prelude;

#[cfg(feature = "embedded")]
pub use db::config::{
    CacheConfig, CacheMode, DbConfig, DiskCacheConfig, SlateHybridCacheConfig,
    SlateObjectStoreCacheSettings, VectorMemoryBudget, VectorMemorySettings,
};
#[cfg(feature = "embedded")]
pub use db::{HelixDB, HelixDbMode, HelixDbSource};

use reqwest::{Client as ReqwestClient, StatusCode};
use serde::Deserialize;
use thiserror::Error;

const DATABASE_ID_HEADER: &str = "x-helix-database-id";

/// Async HTTP client for running queries against a Helix instance.
///
/// A thin async wrapper over [`reqwest`] that knows how to reach a Helix
/// gateway's query routes. Construct it with [`Client::new`] or
/// [`Client::managed`], optionally attach a bearer API key via
/// [`Client::with_api_key`], then build and send requests through
/// [`Client::query`].
///
/// The client is cheap to [`Clone`] — the underlying `reqwest::Client` shares
/// its connection pool — so a single instance can be reused across tasks.
///
/// Reachable as `helix_db::Client`.
///
/// # Examples
///
/// ```no_run
/// use helix_db::Client;
///
/// # fn run() -> Result<(), helix_db::HelixError> {
/// // Defaults to http://localhost:6969 when the URL is `None`.
/// let local = Client::new(None)?;
///
/// // Or point at a remote cluster and attach an API key.
/// let remote = Client::managed(
///     Some("https://cluster.helix-db.com"),
///     "db_your_database_id",
/// )?
/// .with_api_key(Some("hx_your_api_key"));
/// # let _ = (local, remote);
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct Client {
    backend: ClientBackend,
}

#[derive(Clone)]
enum ClientBackend {
    Server(ServerClient),
    #[cfg(feature = "embedded")]
    Embedded(Arc<db::HelixDB>),
}

#[derive(Debug, Clone)]
struct ServerClient {
    client: ReqwestClient,
    url: reqwest::Url,
    api_key: Option<String>,
    database_id: Option<String>,
    require_database_id: bool,
}

impl fmt::Debug for Client {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.backend {
            ClientBackend::Server(server) => formatter
                .debug_struct("Client")
                .field("mode", &"server")
                .field("url", &server.url)
                .field("api_key", &server.api_key.as_ref().map(|_| "<redacted>"))
                .field("database_id", &server.database_id)
                .finish(),
            #[cfg(feature = "embedded")]
            ClientBackend::Embedded(_) => formatter
                .debug_struct("Client")
                .field("mode", &"embedded")
                .finish(),
        }
    }
}

/// Backwards-compatible alias for [`Client`].
pub type HelixDBClient = Client;

/// Errors returned while building or executing a query request.
#[derive(Debug, Error)]
pub enum HelixError {
    /// Transport-level failure talking to the server (connection refused,
    /// timeout, TLS error, …), surfaced from [`reqwest`].
    #[error("Error communicating with server: {0}")]
    ReqwestError(#[from] reqwest::Error),
    /// The server responded with a non-`200` status. `details` carries the
    /// response body, or the status' canonical reason phrase when no body is
    /// available.
    #[error("Got Error from server: {details}")]
    RemoteError {
        /// Static server code, including unknown future values.
        code: Option<String>,
        /// Server-provided error text, or a fallback description of the status.
        details: String,
    },
    /// Failed to (de)serialize a request body or response payload.
    #[error("Error serializing data: {0}")]
    SerializationError(#[from] sonic_rs::Error),
    /// The base URL passed to [`Client::new`] could not be parsed, or the
    /// resolved query route was not a valid URL.
    #[error("Invalid URL: {0}")]
    InvalidURL(String),
    /// The request uses options that are unavailable for the selected client mode.
    #[error("Invalid request: {details}")]
    InvalidRequest {
        /// Description of the unsupported request shape.
        details: String,
    },
    /// Embedded DB execution failed.
    #[cfg(feature = "embedded")]
    #[error("Embedded DB error: {details}")]
    EmbeddedError {
        /// Static embedded error code.
        code: String,
        /// Error text from the embedded DB layer.
        details: String,
    },
}

impl HelixError {
    /// Return the stable query error code when the failure has one.
    #[must_use]
    pub fn error_code(&self) -> Option<&str> {
        match self {
            Self::RemoteError { code, .. } => code.as_deref(),
            Self::InvalidRequest { .. } => Some(QueryErrorCode::InvalidRequest.as_str()),
            #[cfg(feature = "embedded")]
            Self::EmbeddedError { code, .. } => Some(code),
            Self::ReqwestError(_) | Self::SerializationError(_) | Self::InvalidURL(_) => None,
        }
    }
}

impl Client {
    /// Create a client pointed at a Helix instance.
    ///
    /// `url` is the instance base URL; when `None`, it defaults to
    /// `http://localhost:6969`. The `/v2/query` base route is resolved up front
    /// and reused by every request.
    ///
    /// # Errors
    ///
    /// Returns [`HelixError::InvalidURL`] if `url` (or the resolved query route)
    /// cannot be parsed.
    pub fn new(url: Option<&str>) -> Result<Self, HelixError> {
        Self::server(url)
    }

    /// Create a server-mode client pointed at a Helix instance.
    pub fn server(url: Option<&str>) -> Result<Self, HelixError> {
        // Resolve the query endpoint up front. `send()` reuses it for every request.
        let url = reqwest::Url::parse(url.unwrap_or("http://localhost:6969"))
            .map_err(|e| HelixError::InvalidURL(e.to_string()))?
            .join("/v2/query")
            .map_err(|e| HelixError::InvalidURL(e.to_string()))?;
        Ok(Self {
            backend: ClientBackend::Server(ServerClient {
                client: ReqwestClient::new(),
                url,
                api_key: None,
                database_id: None,
                require_database_id: false,
            }),
        })
    }

    /// Create a managed Helix Cloud client with a required database ID.
    ///
    /// The database ID is sent as `x-helix-database-id` with every server request.
    ///
    /// # Errors
    ///
    /// Returns [`HelixError::InvalidRequest`] when `database_id` is empty or
    /// contains only whitespace, or [`HelixError::InvalidURL`] for an invalid
    /// endpoint URL.
    pub fn managed(url: Option<&str>, database_id: &str) -> Result<Self, HelixError> {
        let mut client = Self::server(url)?;
        validate_database_id(Some(database_id), true)?;
        let ClientBackend::Server(server) = &mut client.backend;
        server.database_id = Some(database_id.to_string());
        server.require_database_id = true;
        Ok(client)
    }

    /// Create an embedded-mode writer client backed by the DB crate.
    #[cfg(feature = "embedded")]
    pub async fn open(source: HelixDbSource) -> Result<Self, HelixError> {
        db::HelixDB::open(source)
            .await
            .map(|db| Self {
                backend: ClientBackend::Embedded(Arc::new(db)),
            })
            .map_err(embedded_error)
    }

    /// Create an embedded-mode writer client with explicit DB config.
    ///
    /// [`CacheMode::VectorMemoryOnly`] disables SlateDB and object-store
    /// caches; canonical data still uses the selected [`HelixDbSource`].
    #[cfg(feature = "embedded")]
    pub async fn open_with_config(
        source: HelixDbSource,
        config: DbConfig,
    ) -> Result<Self, HelixError> {
        db::HelixDB::open_with_config(source, config)
            .await
            .map(|db| Self {
                backend: ClientBackend::Embedded(Arc::new(db)),
            })
            .map_err(embedded_error)
    }

    /// Create an embedded-mode read-only client backed by the DB crate.
    #[cfg(feature = "embedded")]
    pub async fn open_reader(source: HelixDbSource) -> Result<Self, HelixError> {
        db::HelixDB::open_reader(source)
            .await
            .map(|db| Self {
                backend: ClientBackend::Embedded(Arc::new(db)),
            })
            .map_err(embedded_error)
    }

    /// Create an embedded-mode read-only client with explicit DB config.
    #[cfg(feature = "embedded")]
    pub async fn open_reader_with_config(
        source: HelixDbSource,
        config: DbConfig,
    ) -> Result<Self, HelixError> {
        db::HelixDB::open_reader_with_config(source, config)
            .await
            .map(|db| Self {
                backend: ClientBackend::Embedded(Arc::new(db)),
            })
            .map_err(embedded_error)
    }

    /// Attach (or clear) the bearer API key sent with every request.
    ///
    /// Passing `Some(key)` sets an `Authorization: Bearer <key>` header on each
    /// request; passing `None` clears any previously set key.
    pub fn with_api_key(mut self, api_key: Option<&str>) -> Self {
        match &mut self.backend {
            ClientBackend::Server(server) => {
                server.api_key = api_key.map(|key| key.to_string());
            }
            #[cfg(feature = "embedded")]
            ClientBackend::Embedded(_) => {}
        }
        self
    }

    /// Attach (or clear) the managed database ID sent as `x-helix-database-id`.
    ///
    /// An empty value is rejected when the first server request is sent. Use
    /// [`Client::managed`] when the endpoint must always have a database ID.
    pub fn with_database_id(mut self, database_id: Option<&str>) -> Self {
        let ClientBackend::Server(server) = &mut self.backend;
        server.database_id = database_id.map(str::to_string);
        self
    }

    /// Execute an SDK-built query request.
    ///
    /// In server mode this posts to `/v2/query`. In embedded mode this executes
    /// directly against the in-process [`HelixDB`].
    pub fn query<R: for<'de> Deserialize<'de>>(
        &self,
        request: QueryRequest,
    ) -> QueryExecutionRequest<'_, 'static, R> {
        QueryBuilder::new(self).query(request)
    }

    /// Execute a query while retaining the response as raw bytes.
    ///
    /// Graph loading uses this path so Rust validates and constructs the graph
    /// without an intermediate language-level object graph.
    pub fn query_raw(&self, request: QueryRequest) -> QueryExecutionRequest<'_, 'static, Vec<u8>> {
        QueryBuilder::new(self).query(request)
    }

    /// Start building an advanced server request.
    ///
    /// `R` is the type the JSON response body is deserialized into by
    /// [`QueryExecutionRequest::send`]. Returns a [`QueryBuilder`] on which you can toggle
    /// request headers, then attach a request with [`QueryBuilder::query`].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// #![recursion_limit = "256"]
    /// use helix_db::Client;
    /// use helix_db::dsl::prelude::*;
    /// use serde::Deserialize;
    ///
    /// #[derive(Deserialize)]
    /// struct Users { count: u64 }
    ///
    /// # async fn run(client: &Client, request: QueryRequest) -> Result<(), helix_db::HelixError> {
    /// let response: Users = client.query(request).send().await?;
    /// # let _ = response;
    /// # Ok(())
    /// # }
    /// ```
    pub fn request_builder<R: for<'de> Deserialize<'de>>(&self) -> QueryBuilder<'_, '_, R> {
        QueryBuilder::new(self)
    }

    /// Flush and close an embedded database handle.
    ///
    /// Server clients do not own database state, so closing them is a no-op.
    pub async fn close(&self) -> Result<(), HelixError> {
        match &self.backend {
            ClientBackend::Server(_) => Ok(()),
            #[cfg(feature = "embedded")]
            ClientBackend::Embedded(database) => database.close().await.map_err(embedded_error),
        }
    }
}

#[cfg(feature = "embedded")]
fn embedded_error(error: db::error::HelixDbError) -> HelixError {
    HelixError::EmbeddedError {
        code: error.error_code().to_string(),
        details: error.to_string(),
    }
}

#[derive(Deserialize)]
struct QueryErrorEnvelope {
    error: String,
    msg: Option<String>,
    code: Option<String>,
}

fn remote_error(response_body: String, fallback: String) -> HelixError {
    let parsed = sonic_rs::from_str::<QueryErrorEnvelope>(&response_body);
    let (code, details) = match parsed {
        Ok(envelope) => match envelope.msg {
            Some(msg) => (Some(envelope.error), msg),
            None => (envelope.code, envelope.error),
        },
        Err(_) => (
            None,
            if response_body.is_empty() {
                fallback
            } else {
                response_body
            },
        ),
    };
    HelixError::RemoteError { code, details }
}

fn validate_database_id(database_id: Option<&str>, required: bool) -> Result<(), HelixError> {
    if required && database_id.is_none() {
        return Err(HelixError::InvalidRequest {
            details: "managed clients require a database ID".to_string(),
        });
    }
    if database_id.is_some_and(|value| value.trim().is_empty()) {
        return Err(HelixError::InvalidRequest {
            details: "database ID must not be empty".to_string(),
        });
    }
    Ok(())
}

/// Fluent builder for a single request, produced by [`Client::query`].
///
/// Optional server header toggles ([`writer_only`](Self::writer_only),
/// [`warm_only`](Self::warm_only),
/// [`should_await_durability`](Self::should_await_durability)) can be chained,
/// then [`query`](Self::query) transitions to a [`QueryExecutionRequest`] ready
/// to [`send`](QueryExecutionRequest::send).
///
/// `R` is the response deserialization target carried through to `send()`.
pub struct QueryBuilder<'hlx, 'a, R> {
    client: &'hlx HelixDBClient,
    headers: [Option<(&'a str, &'a str)>; 4],
    _phantom: PhantomData<R>,
}

impl<'hlx, 'a, R> QueryBuilder<'hlx, 'a, R> {
    /// Create a builder seeded with the `Content-Type: application/json` header.
    ///
    /// Prefer [`Client::query`], which calls this for you.
    #[must_use]
    pub fn new(client: &'hlx HelixDBClient) -> Self {
        let mut headers = [None; 4];
        headers[0] = Some(("Content-Type", "application/json"));
        Self {
            client,
            headers,
            _phantom: PhantomData,
        }
    }

    /// Require the request to be served by a writer node.
    ///
    /// Sets the `x-helix-require-writer` header.
    #[must_use]
    pub fn writer_only(mut self) -> Self {
        self.headers[1] = Some(("x-helix-require-writer", "true"));
        self
    }

    /// Only execute if the query is already warm (reads only).
    ///
    /// Sets the `x-helix-warm` header.
    #[must_use]
    pub fn warm_only(mut self) -> Self {
        self.headers[2] = Some(("x-helix-warm", "true"));
        self
    }

    /// Choose whether a write request blocks until the write is durable.
    ///
    /// Sets the `x-helix-await-durable` header to `"true"` or `"false"`.
    #[must_use]
    pub fn should_await_durability(mut self, should: bool) -> Self {
        self.headers[3] = Some((
            "x-helix-await-durable",
            if should { "true" } else { "false" },
        ));
        self
    }

    /// Target the query route at `/v2/query`.
    ///
    /// The [`QueryRequest`] (DSL query plus parameters) is serialized as
    /// the request body. Build one directly or with a `#[query]` helper, then
    /// call [`QueryExecutionRequest::send`].
    #[must_use]
    pub fn query(self, query: QueryRequest) -> QueryExecutionRequest<'hlx, 'a, R> {
        QueryExecutionRequest {
            client: self.client,
            headers: self.headers,
            query,
            _phantom: PhantomData,
        }
    }
}

/// A fully addressed request, ready to [`send`](Self::send).
///
/// Produced once a query has been attached via [`QueryBuilder::query`].
pub struct QueryExecutionRequest<'hlx, 'a, R> {
    client: &'hlx HelixDBClient,
    headers: [Option<(&'a str, &'a str)>; 4],
    query: QueryRequest,
    _phantom: PhantomData<R>,
}

impl<'hlx, 'a, R> QueryExecutionRequest<'hlx, 'a, R> {
    /// Send the request and return the successful response body unchanged.
    pub async fn send_bytes(self) -> Result<Vec<u8>, HelixError> {
        match &self.client.backend {
            ClientBackend::Server(server) => {
                validate_database_id(server.database_id.as_deref(), server.require_database_id)?;
                let mut request = server.client.post(server.url.clone());
                for (key, value) in self.headers.into_iter().flatten() {
                    request = request.header(key, value);
                }
                if let Some(api_key) = &server.api_key {
                    request = request.bearer_auth(api_key);
                }
                if let Some(database_id) = &server.database_id {
                    request = request.header(DATABASE_ID_HEADER, database_id);
                }
                let response = request.body(sonic_rs::to_vec(&self.query)?).send().await?;
                match response.status() {
                    StatusCode::OK => response
                        .bytes()
                        .await
                        .map(|bytes| bytes.to_vec())
                        .map_err(Into::into),
                    status => {
                        let fallback = status.canonical_reason().map_or_else(
                            || format!("unknown error with code: {status}"),
                            str::to_string,
                        );
                        let response_body = response.text().await.unwrap_or_default();
                        Err(remote_error(response_body, fallback))
                    }
                }
            }
            #[cfg(feature = "embedded")]
            ClientBackend::Embedded(db) => {
                if self.headers.iter().skip(1).any(Option::is_some) {
                    return Err(HelixError::InvalidRequest {
                        details: "request options require server mode".to_string(),
                    });
                }
                let request = sonic_rs::to_vec(&self.query)?;
                db.query_json(&request).await.map_err(embedded_error)
            }
        }
    }
}

impl<'hlx, 'a, R: for<'de> Deserialize<'de>> QueryExecutionRequest<'hlx, 'a, R> {
    /// Send the request and deserialize the response body into `R`.
    ///
    /// Sends the request to `/v2/query`, applies the toggled headers and bearer
    /// API key, and awaits the response.
    ///
    /// # Errors
    ///
    /// - [`HelixError::ReqwestError`] for transport failures.
    /// - [`HelixError::RemoteError`] for any non-`200` response (carrying the
    ///   server's body or status reason).
    /// - [`HelixError::SerializationError`] if the request payload cannot be
    ///   serialized or the response body cannot be deserialized into `R`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// #![recursion_limit = "256"]
    /// use helix_db::Client;
    /// use helix_db::dsl::prelude::*;
    /// use serde::Deserialize;
    ///
    /// #[derive(Deserialize)]
    /// struct AddUserResponse { user_id: u64 }
    ///
    /// # async fn run(client: &Client, request: QueryRequest) -> Result<(), helix_db::HelixError> {
    /// let response: AddUserResponse = client.query(request).send().await?;
    /// # let _ = response.user_id;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn send(self) -> Result<R, HelixError> {
        let response = self.send_bytes().await?;
        sonic_rs::from_slice::<R>(&response).map_err(Into::into)
    }
}

extern crate self as helix_db;

#[cfg(test)]
mod tests {
    use helix_db::dsl::prelude::*;
    use std::collections::BTreeMap;

    #[query]
    fn query1(name: String) {
        // helix_db query that returns a read query or write query
        read_batch()
            .var_as("user", g().n_where(SourcePredicate::eq("username", name)))
            .var_as(
                "friends",
                g().n(NodeRef::var("user"))
                    .out(Some("FOLLOWS"))
                    .dedup()
                    .limit(100),
            )
            .returning(["user", "friends"])
    }

    #[test]
    fn query1_builds_query_request() {
        // Calling the registered fn with concrete args yields a validated QueryRequest.
        let query = query1(String::from("alice")).unwrap();

        assert!(matches!(query.request_type(), QueryRequestType::Read));
        assert_eq!(query.query_name(), Some("query1"));
        let params = query.parameters().expect("parameters present");
        assert!(matches!(
            params.get("name"),
            Some(QueryValue::String(s)) if s == "alice"
        ));
    }

    #[test]
    fn query_request_serializes_query_name() {
        let unnamed = QueryRequest::read(
            read_batch()
                .var_as("count", g().n_with_label("User").count())
                .returning(["count"]),
        )
        .to_json_string()
        .expect("serialize unnamed query request");
        assert!(
            unnamed.contains(r#""query_name":null"#),
            "unnamed request should serialize query_name=null: {unnamed}"
        );

        let named = QueryRequest::read(read_batch())
            .with_query_name("find_users")
            .to_json_string()
            .expect("serialize named query request");
        assert!(
            named.contains(r#""query_name":"find_users""#),
            "named request should serialize query_name: {named}"
        );
    }

    // ---- Group 1: every #[query] param type coerces correctly -----------

    #[query]
    fn q_bool(flag: bool) {
        read_batch()
            .var_as("v", g().n_where(SourcePredicate::eq("field", flag)))
            .returning(["v"])
    }
    #[query]
    fn q_i64(num: i64) {
        read_batch()
            .var_as("v", g().n_where(SourcePredicate::eq("field", num)))
            .returning(["v"])
    }
    #[query]
    fn q_f64(x: f64) {
        read_batch()
            .var_as("v", g().n_where(SourcePredicate::eq("field", x)))
            .returning(["v"])
    }
    #[query]
    fn q_f32(x: f32) {
        read_batch()
            .var_as("v", g().n_where(SourcePredicate::eq("field", x)))
            .returning(["v"])
    }
    #[query]
    fn q_datetime(ts: DateTime) {
        read_batch()
            .var_as("v", g().n_where(SourcePredicate::eq("field", ts)))
            .returning(["v"])
    }
    #[query]
    fn q_value(val: ParamValue) {
        read_batch()
            .var_as("v", g().n_where(SourcePredicate::eq("field", val)))
            .returning(["v"])
    }
    #[query]
    fn q_object(obj: ParamObject) {
        read_batch()
            .var_as("v", g().n_where(SourcePredicate::eq("field", obj)))
            .returning(["v"])
    }
    #[query]
    fn q_array(items: Vec<String>) {
        read_batch()
            .var_as("v", g().n_where(SourcePredicate::eq("field", items)))
            .returning(["v"])
    }
    #[query]
    fn q_map(map: BTreeMap<String, String>) {
        read_batch()
            .var_as("v", g().n_where(SourcePredicate::eq("field", map)))
            .returning(["v"])
    }
    #[query]
    #[allow(unused_variables)] // bytes coercion errors without reading the value (see test below)
    fn q_bytes(blob: Vec<u8>) {
        read_batch()
            .var_as("v", g().n_where(SourcePredicate::eq("field", blob)))
            .returning(["v"])
    }

    #[test]
    fn param_types_coerce_correctly() {
        // bool
        let r = q_bool(true).unwrap();
        assert!(matches!(r.request_type(), QueryRequestType::Read));
        assert!(matches!(
            r.parameters().unwrap().get("flag"),
            Some(QueryValue::Bool(true))
        ));
        assert!(matches!(
            r.parameter_types().unwrap().get("flag"),
            Some(QueryParamType::Bool)
        ));

        // i64
        let r = q_i64(7).unwrap();
        assert!(matches!(
            r.parameters().unwrap().get("num"),
            Some(QueryValue::I64(7))
        ));
        assert!(matches!(
            r.parameter_types().unwrap().get("num"),
            Some(QueryParamType::I64)
        ));

        // f64
        let r = q_f64(1.5).unwrap();
        assert!(matches!(
            r.parameters().unwrap().get("x"),
            Some(QueryValue::F64(v)) if *v == 1.5
        ));
        assert!(matches!(
            r.parameter_types().unwrap().get("x"),
            Some(QueryParamType::F64)
        ));

        // f32
        let r = q_f32(1.5f32).unwrap();
        assert!(matches!(
            r.parameters().unwrap().get("x"),
            Some(QueryValue::F32(v)) if *v == 1.5f32
        ));
        assert!(matches!(
            r.parameter_types().unwrap().get("x"),
            Some(QueryParamType::F32)
        ));

        // DateTime -> rfc3339 string
        let r = q_datetime(DateTime::from_millis(0)).unwrap();
        let expected = DateTime::from_millis(0).to_rfc3339().unwrap();
        assert!(matches!(
            r.parameters().unwrap().get("ts"),
            Some(QueryValue::String(s)) if *s == expected
        ));
        assert!(matches!(
            r.parameter_types().unwrap().get("ts"),
            Some(QueryParamType::DateTime)
        ));

        // ParamValue (PropertyValue)
        let r = q_value(PropertyValue::I64(5)).unwrap();
        assert!(matches!(
            r.parameters().unwrap().get("val"),
            Some(QueryValue::I64(5))
        ));
        assert!(matches!(
            r.parameter_types().unwrap().get("val"),
            Some(QueryParamType::Value)
        ));

        // ParamObject (BTreeMap<String, PropertyValue>)
        let mut obj = BTreeMap::new();
        obj.insert("k".to_string(), PropertyValue::String("x".to_string()));
        let r = q_object(obj).unwrap();
        assert!(matches!(
            r.parameters().unwrap().get("obj"),
            Some(QueryValue::Object(_))
        ));
        assert!(matches!(
            r.parameter_types().unwrap().get("obj"),
            Some(QueryParamType::Object)
        ));

        // Vec<String> -> Array(String)
        let r = q_array(vec!["a".to_string(), "b".to_string()]).unwrap();
        match r.parameters().unwrap().get("items") {
            Some(QueryValue::Array(items)) => {
                assert_eq!(items.len(), 2);
                assert!(matches!(&items[0], QueryValue::String(s) if s == "a"));
                assert!(matches!(&items[1], QueryValue::String(s) if s == "b"));
            }
            other => panic!("expected array, got {other:?}"),
        }
        assert!(matches!(
            r.parameter_types().unwrap().get("items"),
            Some(QueryParamType::Array(inner)) if matches!(**inner, QueryParamType::String)
        ));

        // BTreeMap<String, String> -> Object
        let mut map = BTreeMap::new();
        map.insert("k".to_string(), "v".to_string());
        let r = q_map(map).unwrap();
        assert!(matches!(
            r.parameters().unwrap().get("map"),
            Some(QueryValue::Object(_))
        ));
        assert!(matches!(
            r.parameter_types().unwrap().get("map"),
            Some(QueryParamType::Object)
        ));
    }

    #[test]
    fn bytes_param_returns_error_without_panicking() {
        // Bytes cannot be represented by the query JSON route
        // and the generated callable reports that contract violation.
        assert!(matches!(
            q_bytes(vec![1, 2, 3]),
            Err(QueryError::UnsupportedBytesParameter(name)) if name == "blob"
        ));
    }

    // ---- Group 2: Predicate JSON ------------------------------------------

    #[test]
    fn predicate_literal_json_uses_ast_shape() {
        assert_eq!(
            sonic_rs::to_string(&Predicate::eq("username", "alice")).unwrap(),
            r#"{"eq":{"left":{"property":"username"},"right":{"constant":{"string":"alice"}}}}"#
        );
        assert_eq!(
            sonic_rs::to_string(&Predicate::gt("score", 10i64)).unwrap(),
            r#"{"gt":{"left":{"property":"score"},"right":{"constant":{"i64":10}}}}"#
        );
        assert_eq!(
            sonic_rs::to_string(&Predicate::between("age", 18i64, 65i64)).unwrap(),
            r#"{"between":{"value":{"property":"age"},"min":{"constant":{"i64":18}},"max":{"constant":{"i64":65}}}}"#
        );
    }

    #[test]
    fn predicate_param_json_uses_param_exprs() {
        assert_eq!(
            sonic_rs::to_string(&Predicate::eq("username", Expr::param("name"))).unwrap(),
            r#"{"eq":{"left":{"property":"username"},"right":{"param":"name"}}}"#
        );
        assert_eq!(
            sonic_rs::to_string(&Predicate::lte("score", Expr::param("max"))).unwrap(),
            r#"{"lte":{"left":{"property":"score"},"right":{"param":"max"}}}"#
        );
        assert_eq!(
            sonic_rs::to_string(&Predicate::between("age", Expr::param("lo"), 65i64)).unwrap(),
            r#"{"between":{"value":{"property":"age"},"min":{"param":"lo"},"max":{"constant":{"i64":65}}}}"#
        );
    }

    #[test]
    fn predicate_json_round_trips() {
        for predicate in [
            Predicate::eq("username", "alice"),
            Predicate::eq("username", Expr::param("name")),
            Predicate::between("age", Expr::param("lo"), 65i64),
        ] {
            let json = sonic_rs::to_string(&predicate).unwrap();
            let back: Predicate = sonic_rs::from_str(&json).unwrap();
            assert_eq!(predicate, back);
        }
    }

    // ---- Group 3: SourcePredicate JSON -------------------------------------

    #[test]
    fn source_predicate_literal_json_uses_ast_shape() {
        assert_eq!(
            sonic_rs::to_string(&SourcePredicate::eq("username", "alice")).unwrap(),
            r#"{"eq":{"left":{"property":"username"},"right":{"constant":{"string":"alice"}}}}"#
        );
        assert_eq!(
            sonic_rs::to_string(&SourcePredicate::gt("score", 10i64)).unwrap(),
            r#"{"gt":{"left":{"property":"score"},"right":{"constant":{"i64":10}}}}"#
        );
        assert_eq!(
            sonic_rs::to_string(&SourcePredicate::between("age", 18i64, 65i64)).unwrap(),
            r#"{"between":{"value":{"property":"age"},"min":{"constant":{"i64":18}},"max":{"constant":{"i64":65}}}}"#
        );
    }

    #[test]
    fn source_predicate_param_json_uses_param_exprs() {
        assert_eq!(
            sonic_rs::to_string(&SourcePredicate::eq("username", Expr::param("name"))).unwrap(),
            r#"{"eq":{"left":{"property":"username"},"right":{"param":"name"}}}"#
        );
        assert_eq!(
            sonic_rs::to_string(&SourcePredicate::lte("score", Expr::param("max"))).unwrap(),
            r#"{"lte":{"left":{"property":"score"},"right":{"param":"max"}}}"#
        );
        assert_eq!(
            sonic_rs::to_string(&SourcePredicate::between("age", Expr::param("lo"), 65i64))
                .unwrap(),
            r#"{"between":{"value":{"property":"age"},"min":{"param":"lo"},"max":{"constant":{"i64":65}}}}"#
        );
    }

    #[test]
    fn source_predicate_json_round_trips() {
        for sp in [
            SourcePredicate::eq("username", "alice"),
            SourcePredicate::eq("username", Expr::param("name")),
            SourcePredicate::between("age", Expr::param("lo"), 65i64),
        ] {
            let json = sonic_rs::to_string(&sp).unwrap();
            let back: SourcePredicate = sonic_rs::from_str(&json).unwrap();
            assert_eq!(sp, back);
        }
    }

    // ---- Group 4: full query AST, literal vs param (self-contained) --------

    #[test]
    fn query_ast_literal_vs_param_json() {
        let literal = read_batch()
            .var_as(
                "user",
                g().n_where(SourcePredicate::eq("username", "alice")),
            )
            .returning(["user"]);
        let literal_json = sonic_rs::to_string(&literal).unwrap();
        assert!(
            literal_json.contains(r#""root":{"nodes_where":{"predicate":{"eq":{"left":{"property":"username"},"right":{"constant":{"string":"alice"}}}}}}"#),
            "literal nodes_where AST changed shape: {literal_json}"
        );
        assert!(!literal_json.contains("steps"));

        let param = read_batch()
            .var_as(
                "user",
                g().n_where(SourcePredicate::eq("username", Expr::param("name"))),
            )
            .returning(["user"]);
        let param_json = sonic_rs::to_string(&param).unwrap();
        assert!(
            param_json.contains(r#""root":{"nodes_where":{"predicate":{"eq":{"left":{"property":"username"},"right":{"param":"name"}}}}}}"#),
            "param nodes_where AST missing param expression: {param_json}"
        );
    }

    #[test]
    fn row_binding_query_uses_public_sdk_prelude_ast_shape() {
        let query = read_batch()
            .var_as(
                "workloads",
                g().n_with_label("Service")
                    .bind("service")
                    .optional(sub().in_(Some("CREATES")).bind("deployment"))
                    .union(vec![
                        sub().in_(Some("MANAGES")).bind("owner"),
                        sub().out(Some("ROUTES_TO")).bind("workload"),
                    ])
                    .project_distinct_bindings(vec![
                        BindingProjection::binding("service", "$id", "service_id"),
                        BindingProjection::current("$id", "current_id"),
                        BindingProjection::coalesce(
                            vec![
                                BindingValueRef::binding("deployment", "$id"),
                                BindingValueRef::binding("owner", "$id"),
                                BindingValueRef::binding("workload", "$id"),
                            ],
                            "workload_id",
                        ),
                    ]),
            )
            .returning(["workloads"]);

        let json = sonic_rs::to_string(&query).unwrap();
        assert!(json.contains(r#""project_bindings""#));
        assert!(json.contains(r#""bind":{"input""#));
        assert!(json.contains(r#""name":"service""#));
        assert!(json.contains(r#""target":{"binding":"service"}"#));
        assert!(json.contains(r#""target":"current""#));
        assert!(json.contains(r#""coalesce""#));
        assert!(json.contains(r#""distinct":true"#));
        assert!(!json.contains("steps"));
    }

    #[test]
    fn nested_query_property_json() {
        let metadata = PropertyValue::object(vec![
            ("externalID", PropertyValue::from("some_id")),
            ("score", PropertyValue::from(20i64)),
            (
                "tags",
                PropertyValue::array(vec![
                    PropertyValue::from("alpha"),
                    PropertyValue::from(7i64),
                ]),
            ),
        ]);

        let write = write_batch()
            .var_as(
                "updated",
                g().add_n(
                    "User",
                    vec![
                        ("name", PropertyInput::from("john")),
                        ("metadata", PropertyInput::from(metadata)),
                    ],
                )
                .set_property("metadata", PropertyInput::param("metadata"))
                .value_map(Some(vec!["metadata.externalID"])),
            )
            .returning(["updated"]);
        let write_json = sonic_rs::to_string(&write).unwrap();
        assert!(
            write_json
                .contains(r#""metadata",{"value":{"object":{"externalID":{"string":"some_id"}"#),
            "AddN nested object value changed shape: {write_json}"
        );
        assert!(
            write_json.contains(r#""tags":{"array":[{"string":"alpha"},{"i64":7}]}"#),
            "AddN nested array value changed shape: {write_json}"
        );
        assert!(
            write_json.contains(r#""set_property":{"input":{"add_n""#)
                && write_json
                    .contains(r#""name":"metadata","value":{"expr":{"param":"metadata"}}"#),
            "SetProperty param changed shape: {write_json}"
        );
        assert!(
            write_json.contains(r#""value_map":{"input":{"set_property""#)
                && write_json.contains(r#""properties":["metadata.externalID"]"#),
            "filtered ValueMap dotted path changed shape: {write_json}"
        );

        let read = read_batch()
            .var_as(
                "users",
                g().n_where(SourcePredicate::and(vec![
                    SourcePredicate::eq("name", "john"),
                    SourcePredicate::eq("metadata.externalID", "some_id"),
                ]))
                .order_by("metadata.score", Order::Desc)
                .project(vec![
                    Projection::property("metadata.externalID", "external_id"),
                    Projection::expr("score_copy", Expr::prop("metadata.score")),
                ]),
            )
            .var_as(
                "external_ids",
                g().n_with_label("User").values(vec!["metadata.externalID"]),
            )
            .returning(["users", "external_ids"]);
        let read_json = sonic_rs::to_string(&read).unwrap();
        assert!(
            read_json.contains(r#""eq":{"left":{"property":"metadata.externalID"},"right":{"constant":{"string":"some_id"}}}"#),
            "dotted SourcePredicate changed shape: {read_json}"
        );
        assert!(
            read_json.contains(r#""order_by":{"input":{"nodes_where""#)
                && read_json.contains(r#""property":"metadata.score","order":"desc""#),
            "dotted OrderBy changed shape: {read_json}"
        );
        assert!(
            read_json.contains(r#""source":"metadata.externalID","alias":"external_id""#),
            "dotted property projection changed shape: {read_json}"
        );
        assert!(
            read_json.contains(r#""expr":{"property":"metadata.score"}"#),
            "dotted expression projection changed shape: {read_json}"
        );
        assert!(
            read_json.contains(r#""values":{"input":{"nodes_where""#)
                && read_json.contains(r#""properties":["metadata.externalID"]"#),
            "dotted Values changed shape: {read_json}"
        );
    }
}

#[cfg(test)]
mod client_tests {
    //! Tests for the `Client` / `QueryBuilder` request-building surface. These
    //! exercise everything up to (but not including) the network round-trip, so
    //! they need no running Helix instance. As a child module of the crate root
    //! they can read the builder's private fields directly.
    use super::*;
    use serde::Deserialize;

    #[derive(Debug, Deserialize)]
    struct Resp;

    #[cfg(feature = "embedded")]
    #[derive(Debug, Deserialize)]
    struct CountResp {
        users: u64,
    }

    fn sample_request() -> QueryRequest {
        QueryRequest::read(
            read_batch()
                .var_as(
                    "user",
                    g().n_where(SourcePredicate::eq("username", "alice")),
                )
                .returning(["user"]),
        )
    }

    #[cfg(feature = "embedded")]
    fn count_request() -> QueryRequest {
        QueryRequest::read(
            read_batch()
                .var_as("users", g().n_with_label("Missing").count())
                .returning(["users"]),
        )
    }

    #[cfg(feature = "embedded")]
    fn write_request() -> QueryRequest {
        QueryRequest::write(
            write_batch()
                .var_as(
                    "created",
                    g().add_n("User", vec![("name", PropertyInput::from("Ada"))]),
                )
                .returning(["created"]),
        )
    }

    fn server_backend(client: &Client) -> &ServerClient {
        match &client.backend {
            ClientBackend::Server(server) => server,
            #[cfg(feature = "embedded")]
            ClientBackend::Embedded(_) => panic!("test expected server-mode client"),
        }
    }

    // ---- Client construction ------------------------------------------------

    #[test]
    fn new_defaults_to_localhost() {
        let client = Client::new(None).unwrap();
        let server = server_backend(&client);
        assert_eq!(server.url.as_str(), "http://localhost:6969/v2/query");
        assert!(server.api_key.is_none());
        assert!(server.database_id.is_none());
    }

    #[test]
    fn new_parses_custom_url() {
        let client = Client::new(Some("https://cluster.helix-db.com")).unwrap();
        assert_eq!(
            server_backend(&client).url.as_str(),
            "https://cluster.helix-db.com/v2/query"
        );
    }

    #[test]
    fn new_rejects_invalid_url() {
        let err = Client::new(Some("not a url")).unwrap_err();
        assert!(matches!(err, HelixError::InvalidURL(_)));
    }

    #[test]
    fn with_api_key_sets_and_clears() {
        let client = Client::new(None).unwrap().with_api_key(Some("hx_secret"));
        assert_eq!(
            server_backend(&client).api_key.as_deref(),
            Some("hx_secret")
        );

        let cleared = client.with_api_key(None);
        assert!(server_backend(&cleared).api_key.is_none());
    }

    #[test]
    fn managed_requires_a_non_empty_database_id() {
        for database_id in ["", "   "] {
            let error = Client::managed(Some("https://cluster.helix-db.com"), database_id)
                .unwrap_err();
            assert!(matches!(error, HelixError::InvalidRequest { .. }));
        }

        let client = Client::managed(
            Some("https://cluster.helix-db.com"),
            "db_123",
        )
        .unwrap();
        let server = server_backend(&client);
        assert_eq!(server.database_id.as_deref(), Some("db_123"));
        assert!(server.require_database_id);
    }

    #[test]
    fn with_database_id_sets_and_clears() {
        let client = Client::new(None).unwrap().with_database_id(Some("db_123"));
        assert_eq!(server_backend(&client).database_id.as_deref(), Some("db_123"));

        let cleared = client.with_database_id(None);
        assert!(server_backend(&cleared).database_id.is_none());
    }

    #[test]
    fn remote_errors_parse_new_legacy_future_and_fallback_contracts() {
        let cases = [
            (
                r#"{"error":"index_not_found","msg":"missing index"}"#,
                Some("index_not_found"),
                "missing index",
            ),
            (
                r#"{"error":"legacy message","code":"index_not_found"}"#,
                Some("index_not_found"),
                "legacy message",
            ),
            (
                r#"{"error":"future_code","msg":"future message"}"#,
                Some("future_code"),
                "future message",
            ),
            (
                r#"{"error":"message without a code"}"#,
                None,
                "message without a code",
            ),
            ("not JSON", None, "not JSON"),
            ("", None, "Bad Request"),
        ];

        for (body, expected_code, expected_details) in cases {
            let error = remote_error(body.to_string(), "Bad Request".to_string());
            assert_eq!(error.error_code(), expected_code);
            let HelixError::RemoteError { details, .. } = error else {
                panic!("remote parser always returns a remote error");
            };
            assert_eq!(details, expected_details);
        }
    }

    // ---- Header assembly ----------------------------------------------------

    #[test]
    fn query_builder_starts_with_only_content_type() {
        let client = Client::new(None).unwrap();
        let builder = client.request_builder::<Resp>();
        assert_eq!(
            builder.headers[0],
            Some(("Content-Type", "application/json"))
        );
        assert!(builder.headers.iter().skip(1).all(Option::is_none));
    }

    #[test]
    fn header_toggles_populate_slots() {
        let client = Client::new(None).unwrap();
        let builder = client
            .request_builder::<Resp>()
            .writer_only()
            .warm_only()
            .should_await_durability(true);
        assert_eq!(builder.headers[1], Some(("x-helix-require-writer", "true")));
        assert_eq!(builder.headers[2], Some(("x-helix-warm", "true")));
        assert_eq!(builder.headers[3], Some(("x-helix-await-durable", "true")));
    }

    #[test]
    fn should_await_durability_false_sends_false() {
        let client = Client::new(None).unwrap();
        let builder = client
            .request_builder::<Resp>()
            .should_await_durability(false);
        assert_eq!(builder.headers[3], Some(("x-helix-await-durable", "false")));
    }

    // ---- Query attachment ---------------------------------------------------

    #[test]
    fn query_builder_attaches_query_request() {
        let client = Client::new(None).unwrap();
        let query = sample_request();
        let request = client.request_builder::<Resp>().query(query.clone());
        assert_eq!(request.query, query);
    }

    // ---- Request routing (exercises the real `send()` path) -----------------

    #[derive(Debug, serde::Deserialize)]
    struct EmptyResp {}

    /// Spawn a one-shot HTTP server on a random port. Returns its base URL and a
    /// handle that resolves to the first request received.
    async fn spawn_capture_server() -> (String, tokio::task::JoinHandle<String>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let handle = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = Vec::new();
            let mut tmp = [0u8; 4096];
            // Accumulate bytes until the full HTTP header block has arrived.
            loop {
                let n = socket.read(&mut tmp).await.unwrap();
                buf.extend_from_slice(&tmp[..n]);
                if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                    break;
                }
            }
            let resp = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}";
            socket.write_all(resp.as_bytes()).await.unwrap();
            String::from_utf8_lossy(&buf).into_owned()
        });
        (base, handle)
    }

    #[tokio::test]
    async fn query_posts_to_v2_query() {
        let (base, handle) = spawn_capture_server().await;
        let client = Client::new(Some(&base)).unwrap();
        let _: EmptyResp = client.query(sample_request()).send().await.unwrap();
        assert!(handle.await.unwrap().contains("POST /v2/query HTTP/1.1"));
    }

    #[tokio::test]
    async fn managed_query_sends_database_id_header() {
        let (base, handle) = spawn_capture_server().await;
        let client = Client::managed(Some(&base), "db_123")
            .unwrap()
            .with_api_key(Some("hx_secret"));
        let _: EmptyResp = client.query(sample_request()).send().await.unwrap();
        let captured = handle.await.unwrap();
        assert!(captured.contains("x-helix-database-id: db_123"));
        assert!(captured.contains("authorization: Bearer hx_secret"));
    }

    #[tokio::test]
    async fn empty_database_id_is_rejected_before_sending() {
        let client = Client::new(Some("http://127.0.0.1:1")).unwrap().with_database_id(Some(" "));
        let error = client.query::<EmptyResp>(sample_request()).send().await.unwrap_err();
        assert!(matches!(error, HelixError::InvalidRequest { .. }));
    }

    // ---- Embedded execution -------------------------------------------------

    #[cfg(feature = "embedded")]
    #[tokio::test]
    async fn embedded_client_query_executes_against_in_memory_db() {
        let client = Client::open(HelixDbSource::InMemory {
            database: "rust-sdk-embedded-query".to_string(),
        })
        .await
        .expect("embedded client should open");

        let response: CountResp = client
            .query(count_request())
            .send()
            .await
            .expect("embedded query should execute");

        assert_eq!(response.users, 0);
    }

    #[cfg(feature = "embedded")]
    #[tokio::test]
    async fn embedded_reader_rejects_write_request() {
        let root = tempfile::tempdir().expect("tempdir should be created");
        let source = HelixDbSource::Disk {
            root: root.path().to_path_buf(),
            database: "rust-sdk-reader".to_string(),
        };
        let writer = HelixDB::open(source.clone())
            .await
            .expect("writer DB should initialize disk database");
        writer.close().await.expect("writer should close cleanly");
        let client = Client::open_reader(source)
            .await
            .expect("embedded reader client should open");

        let err = client
            .query::<serde_json::Value>(write_request())
            .send()
            .await
            .expect_err("embedded reader should reject writes");

        assert!(matches!(err, HelixError::EmbeddedError { .. }));
        assert_eq!(err.error_code(), Some("writer_mode_required"));
        assert!(err.to_string().contains("writer mode"));
    }

    #[cfg(feature = "embedded")]
    #[tokio::test]
    async fn embedded_client_rejects_server_options() {
        let client = Client::open(HelixDbSource::InMemory {
            database: "rust-sdk-server-options".to_string(),
        })
        .await
        .expect("embedded client should open");

        let err = client
            .request_builder::<Resp>()
            .writer_only()
            .query(sample_request())
            .send()
            .await
            .expect_err("server options should require server mode");

        assert!(matches!(err, HelixError::InvalidRequest { .. }));
        assert!(err
            .to_string()
            .contains("request options require server mode"));
    }
}
