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
//!   [`DynamicQueryRequest`] payload type. This is the bulk of the public API.
//! - [`query_generator`] — query-bundle generation, used to emit deployable
//!   stored queries from `#[register]`-annotated builders.
//! - The crate root ([`Client`], [`QueryBuilder`], [`QueryRequest`],
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
//! Build a [`Client`], then use its fluent request builder. Pick a query kind —
//! [`dynamic`](QueryBuilder::dynamic) to POST an inline [`DynamicQueryRequest`]
//! to `/v1/query`, or [`stored`](QueryBuilder::stored) to call a deployed query
//! at `/v1/query/<name>` — and `await` the response:
//!
//! ```no_run
//! use helix_db::Client;
//! use helix_db::dsl::prelude::*;
//! use serde::Deserialize;
//!
//! #[derive(Deserialize)]
//! struct Friends { friends: Vec<u64> }
//!
//! # async fn run(request: DynamicQueryRequest) -> Result<(), helix_db::HelixError> {
//! let client = Client::new(Some("https://cluster.helix-db.com"))?
//!     .with_api_key(Some("hx_your_api_key"));
//!
//! let response: Friends = client.query().dynamic(request).send().await?;
//! # let _ = response.friends;
//! # Ok(())
//! # }
//! ```
//!
//! See [`Client`] for the full request-building surface (header toggles,
//! request bodies, error handling).

pub mod dsl;
pub mod query_generator;

use std::marker::PhantomData;

// Re-export the DSL surface (types, builders, `prelude`, etc.) at the crate
// root. This is also what makes the `crate::*` paths used inside `dsl.rs` and
// `query_generator.rs` resolve.
pub use dsl::*;

// Convenience re-export so `helix_db::prelude::*` is reachable directly, in
// addition to the canonical `helix_db::dsl::prelude::*`.
pub use dsl::prelude;

use reqwest::{Client as ReqwestClient, StatusCode};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Async HTTP client for running queries against a Helix instance.
///
/// A thin async wrapper over [`reqwest`] that knows how to reach a Helix
/// gateway's query routes. Construct it with [`Client::new`], optionally attach
/// a bearer API key via [`Client::with_api_key`], then build and send requests
/// through [`Client::query`].
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
/// let remote = Client::new(Some("https://cluster.helix-db.com"))?
///     .with_api_key(Some("hx_your_api_key"));
/// # let _ = (local, remote);
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct Client {
    client: ReqwestClient,
    url: reqwest::Url,
    api_key: Option<String>,
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
}

impl Client {
    /// Create a client pointed at a Helix instance.
    ///
    /// `url` is the instance base URL; when `None`, it defaults to
    /// `http://localhost:6969`. The `/v1/query` base route is resolved up front
    /// and reused by every request — dynamic queries POST to it directly and
    /// stored queries append `/<name>`.
    ///
    /// # Errors
    ///
    /// Returns [`HelixError::InvalidURL`] if `url` (or the resolved query route)
    /// cannot be parsed.
    pub fn new(url: Option<&str>) -> Result<Self, HelixError> {
        // Resolve the base query endpoint up front. `send()` reuses this for
        // dynamic queries and appends `/<name>` for stored queries.
        let url = reqwest::Url::parse(url.unwrap_or("http://localhost:6969"))
            .map_err(|e| HelixError::InvalidURL(e.to_string()))?
            .join("/v1/query")
            .map_err(|e| HelixError::InvalidURL(e.to_string()))?;
        Ok(Self {
            client: ReqwestClient::new(),
            url,
            api_key: None,
        })
    }

    /// Attach (or clear) the bearer API key sent with every request.
    ///
    /// Passing `Some(key)` sets an `Authorization: Bearer <key>` header on each
    /// request; passing `None` clears any previously set key.
    pub fn with_api_key(mut self, api_key: Option<&str>) -> Self {
        self.api_key = api_key.map(|key| key.to_string());
        self
    }

    /// Start building a request.
    ///
    /// `R` is the type the JSON response body is deserialized into by
    /// [`QueryRequest::send`]. Returns a [`QueryBuilder`] on which you can toggle
    /// request headers, then pick a query kind with [`QueryBuilder::dynamic`] or
    /// [`QueryBuilder::stored`].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use helix_db::Client;
    /// use helix_db::dsl::prelude::*;
    /// use serde::Deserialize;
    ///
    /// #[derive(Deserialize)]
    /// struct Users { count: u64 }
    ///
    /// # async fn run(client: &Client, request: DynamicQueryRequest) -> Result<(), helix_db::HelixError> {
    /// let response: Users = client.query().dynamic(request).send().await?;
    /// # let _ = response;
    /// # Ok(())
    /// # }
    /// ```
    pub fn query<R: for<'de> Deserialize<'de>>(&self) -> QueryBuilder<'_, '_, R> {
        QueryBuilder::new(self)
    }
}

/// Fluent builder for a single request, produced by [`Client::query`].
///
/// Optional header toggles ([`writer_only`](Self::writer_only),
/// [`warm_only`](Self::warm_only),
/// [`should_await_durability`](Self::should_await_durability)) and an optional
/// [`body`](Self::body) can be chained, then exactly one query kind is selected
/// with [`stored`](Self::stored) or [`dynamic`](Self::dynamic) — both of which
/// transition to a [`QueryRequest`] ready to [`send`](QueryRequest::send).
///
/// `R` is the response deserialization target carried through to `send()`.
pub struct QueryBuilder<'hlx, 'a, R> {
    client: &'hlx HelixDBClient,
    query_type: QueryType,
    headers: [Option<(&'a str, &'a str)>; 4],
    body: Option<Vec<u8>>,
    _phantom: PhantomData<R>,
}

/// Which Helix query route a [`QueryBuilder`] targets.
///
/// Set internally by [`QueryBuilder::stored`] / [`QueryBuilder::dynamic`]; the
/// [`Empty`](QueryType::Empty) default is never observed by `send()` because
/// reaching it requires picking a query kind first.
#[derive(Default)]
pub(crate) enum QueryType {
    /// A deployed stored query, posted to `/v1/query/<name>`.
    Stored(String),
    /// An inline dynamic query, posted to `/v1/query`.
    Dynamic(DynamicQueryRequest),
    /// No query kind chosen yet (builder default).
    #[default]
    Empty,
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
            query_type: QueryType::default(),
            headers,
            body: None,
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

    /// Attach a serialized JSON request body.
    ///
    /// Used to pass parameters to a [`stored`](Self::stored) query route.
    /// [`dynamic`](Self::dynamic) requests ignore this body — they serialize the
    /// [`DynamicQueryRequest`] itself as the payload.
    ///
    /// # Errors
    ///
    /// Returns [`HelixError::SerializationError`] if `data` cannot be serialized
    /// to JSON.
    pub fn body<T: Serialize + Sync>(mut self, data: &T) -> Result<Self, HelixError> {
        self.body = Some(sonic_rs::to_vec(data)?);
        Ok(self)
    }

    /// Target a deployed stored query at `/v1/query/<query_name>`.
    ///
    /// Pair with [`body`](Self::body) to supply the query's parameters, then
    /// call [`QueryRequest::send`].
    #[must_use]
    pub fn stored(mut self, query_name: String) -> QueryRequest<'hlx, 'a, R> {
        self.query_type = QueryType::Stored(query_name);
        QueryRequest { request: self }
    }

    /// Target an inline dynamic query at `/v1/query`.
    ///
    /// The [`DynamicQueryRequest`] (DSL query plus parameters) is serialized as
    /// the request body. Build one directly or with a `#[register]` helper, then
    /// call [`QueryRequest::send`].
    #[must_use]
    pub fn dynamic(mut self, query: DynamicQueryRequest) -> QueryRequest<'hlx, 'a, R> {
        self.query_type = QueryType::Dynamic(query);
        QueryRequest { request: self }
    }
}

/// A fully addressed request, ready to [`send`](Self::send).
///
/// Produced once a query kind has been chosen via [`QueryBuilder::stored`] or
/// [`QueryBuilder::dynamic`]; the only remaining step is `send()`.
pub struct QueryRequest<'hlx, 'a, R> {
    request: QueryBuilder<'hlx, 'a, R>,
}

impl<'hlx, 'a, R: for<'de> Deserialize<'de>> QueryRequest<'hlx, 'a, R> {
    /// Send the request and deserialize the response body into `R`.
    ///
    /// Resolves the route (`/v1/query` for dynamic, `/v1/query/<name>` for
    /// stored), applies the toggled headers and bearer API key, attaches the
    /// body, and awaits the response.
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
    /// use helix_db::Client;
    /// use helix_db::dsl::prelude::*;
    /// use serde::Deserialize;
    ///
    /// #[derive(Deserialize)]
    /// struct AddUserResponse { user_id: u64 }
    ///
    /// # async fn run(client: &Client, request: DynamicQueryRequest) -> Result<(), helix_db::HelixError> {
    /// let response: AddUserResponse = client.query().dynamic(request).send().await?;
    /// # let _ = response.user_id;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn send(self) -> Result<R, HelixError> {
        let query_request = self.request;
        let (url, body) = match query_request.query_type {
            QueryType::Dynamic(query) => ("/v1/query".to_string(), Some(sonic_rs::to_vec(&query)?)),
            QueryType::Stored(name) => (format!("/v1/query/{name}"), query_request.body),
            QueryType::Empty => {
                unreachable!("send() is only reachable after stored() or dynamic() sets query_type")
            }
        };
        let url = query_request
            .client
            .url
            .join(&url)
            .map_err(|e| HelixError::InvalidURL(e.to_string()))?;

        let mut request = query_request.client.client.post(url);

        for (k, v) in query_request.headers.into_iter().flatten() {
            request = request.header(k, v);
        }
        if let Some(ref api_key) = query_request.client.api_key {
            request = request.bearer_auth(api_key);
        }
        if let Some(body) = body {
            request = request.body(body);
        }

        let response = request.send().await?;

        match response.status() {
            StatusCode::OK => {
                let bytes = response.bytes().await?;
                sonic_rs::from_slice::<R>(&bytes).map_err(Into::into)
            }
            code => match response.text().await {
                Ok(t) => Err(HelixError::RemoteError { details: t }),
                Err(_) => match code.canonical_reason() {
                    Some(r) => Err(HelixError::RemoteError {
                        details: r.to_string(),
                    }),
                    None => Err(HelixError::RemoteError {
                        details: format!("unkown error with code: {code}"),
                    }),
                },
            },
        }
    }
}

extern crate self as helix_db;

#[cfg(test)]
mod tests {
    use helix_db::dsl::prelude::*;
    use std::collections::BTreeMap;

    #[register]
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
    fn query1_builds_dynamic_request() {
        // Calling the registered fn with concrete args yields a DynamicQueryRequest directly.
        let query = query1(String::from("alice"));

        assert!(matches!(query.request_type, DynamicQueryRequestType::Read));
        assert_eq!(query.query_name.as_deref(), Some("query1"));
        let params = query.parameters.expect("parameters present");
        assert!(matches!(
            params.get("name"),
            Some(DynamicQueryValue::String(s)) if s == "alice"
        ));
    }

    #[test]
    fn dynamic_request_serializes_query_name() {
        let unnamed = DynamicQueryRequest::read(
            read_batch()
                .var_as("count", g().n_with_label("User").count())
                .returning(["count"]),
        )
        .to_json_string()
        .expect("serialize unnamed dynamic request");
        assert!(
            unnamed.contains(r#""query_name":null"#),
            "unnamed request should serialize query_name=null: {unnamed}"
        );

        let named = DynamicQueryRequest::read(read_batch())
            .with_query_name("find_users")
            .to_json_string()
            .expect("serialize named dynamic request");
        assert!(
            named.contains(r#""query_name":"find_users""#),
            "named request should serialize query_name: {named}"
        );
    }

    // ---- Group 1: every #[register] param type coerces correctly -----------

    #[register]
    fn q_bool(flag: bool) {
        read_batch()
            .var_as("v", g().n_where(SourcePredicate::eq("field", flag)))
            .returning(["v"])
    }
    #[register]
    fn q_i64(num: i64) {
        read_batch()
            .var_as("v", g().n_where(SourcePredicate::eq("field", num)))
            .returning(["v"])
    }
    #[register]
    fn q_f64(x: f64) {
        read_batch()
            .var_as("v", g().n_where(SourcePredicate::eq("field", x)))
            .returning(["v"])
    }
    #[register]
    fn q_f32(x: f32) {
        read_batch()
            .var_as("v", g().n_where(SourcePredicate::eq("field", x)))
            .returning(["v"])
    }
    #[register]
    fn q_datetime(ts: DateTime) {
        read_batch()
            .var_as("v", g().n_where(SourcePredicate::eq("field", ts)))
            .returning(["v"])
    }
    #[register]
    fn q_value(val: ParamValue) {
        read_batch()
            .var_as("v", g().n_where(SourcePredicate::eq("field", val)))
            .returning(["v"])
    }
    #[register]
    fn q_object(obj: ParamObject) {
        read_batch()
            .var_as("v", g().n_where(SourcePredicate::eq("field", obj)))
            .returning(["v"])
    }
    #[register]
    fn q_array(items: Vec<String>) {
        read_batch()
            .var_as("v", g().n_where(SourcePredicate::eq("field", items)))
            .returning(["v"])
    }
    #[register]
    fn q_map(map: BTreeMap<String, String>) {
        read_batch()
            .var_as("v", g().n_where(SourcePredicate::eq("field", map)))
            .returning(["v"])
    }
    #[register]
    #[allow(unused_variables)] // bytes coercion errors without reading the value (see test below)
    fn q_bytes(blob: Vec<u8>) {
        read_batch()
            .var_as("v", g().n_where(SourcePredicate::eq("field", blob)))
            .returning(["v"])
    }

    #[test]
    fn param_types_coerce_correctly() {
        // bool
        let r = q_bool(true);
        assert!(matches!(r.request_type, DynamicQueryRequestType::Read));
        assert!(matches!(
            r.parameters.as_ref().unwrap().get("flag"),
            Some(DynamicQueryValue::Bool(true))
        ));
        assert!(matches!(
            r.parameter_types.as_ref().unwrap().get("flag"),
            Some(QueryParamType::Bool)
        ));

        // i64
        let r = q_i64(7);
        assert!(matches!(
            r.parameters.as_ref().unwrap().get("num"),
            Some(DynamicQueryValue::I64(7))
        ));
        assert!(matches!(
            r.parameter_types.as_ref().unwrap().get("num"),
            Some(QueryParamType::I64)
        ));

        // f64
        let r = q_f64(1.5);
        assert!(matches!(
            r.parameters.as_ref().unwrap().get("x"),
            Some(DynamicQueryValue::F64(v)) if *v == 1.5
        ));
        assert!(matches!(
            r.parameter_types.as_ref().unwrap().get("x"),
            Some(QueryParamType::F64)
        ));

        // f32
        let r = q_f32(1.5f32);
        assert!(matches!(
            r.parameters.as_ref().unwrap().get("x"),
            Some(DynamicQueryValue::F32(v)) if *v == 1.5f32
        ));
        assert!(matches!(
            r.parameter_types.as_ref().unwrap().get("x"),
            Some(QueryParamType::F32)
        ));

        // DateTime -> rfc3339 string
        let r = q_datetime(DateTime::from_millis(0));
        let expected = DateTime::from_millis(0).to_rfc3339().unwrap();
        assert!(matches!(
            r.parameters.as_ref().unwrap().get("ts"),
            Some(DynamicQueryValue::String(s)) if *s == expected
        ));
        assert!(matches!(
            r.parameter_types.as_ref().unwrap().get("ts"),
            Some(QueryParamType::DateTime)
        ));

        // ParamValue (PropertyValue)
        let r = q_value(PropertyValue::I64(5));
        assert!(matches!(
            r.parameters.as_ref().unwrap().get("val"),
            Some(DynamicQueryValue::I64(5))
        ));
        assert!(matches!(
            r.parameter_types.as_ref().unwrap().get("val"),
            Some(QueryParamType::Value)
        ));

        // ParamObject (BTreeMap<String, PropertyValue>)
        let mut obj = BTreeMap::new();
        obj.insert("k".to_string(), PropertyValue::String("x".to_string()));
        let r = q_object(obj);
        assert!(matches!(
            r.parameters.as_ref().unwrap().get("obj"),
            Some(DynamicQueryValue::Object(_))
        ));
        assert!(matches!(
            r.parameter_types.as_ref().unwrap().get("obj"),
            Some(QueryParamType::Object)
        ));

        // Vec<String> -> Array(String)
        let r = q_array(vec!["a".to_string(), "b".to_string()]);
        match r.parameters.as_ref().unwrap().get("items") {
            Some(DynamicQueryValue::Array(items)) => {
                assert_eq!(items.len(), 2);
                assert!(matches!(&items[0], DynamicQueryValue::String(s) if s == "a"));
                assert!(matches!(&items[1], DynamicQueryValue::String(s) if s == "b"));
            }
            other => panic!("expected array, got {other:?}"),
        }
        assert!(matches!(
            r.parameter_types.as_ref().unwrap().get("items"),
            Some(QueryParamType::Array(inner)) if matches!(**inner, QueryParamType::String)
        ));

        // BTreeMap<String, String> -> Object
        let mut map = BTreeMap::new();
        map.insert("k".to_string(), "v".to_string());
        let r = q_map(map);
        assert!(matches!(
            r.parameters.as_ref().unwrap().get("map"),
            Some(DynamicQueryValue::Object(_))
        ));
        assert!(matches!(
            r.parameter_types.as_ref().unwrap().get("map"),
            Some(QueryParamType::Object)
        ));
    }

    #[test]
    #[should_panic(expected = "failed to coerce parameter")]
    fn bytes_param_panics_on_dynamic_call() {
        // Bytes params register fine for the stored query, but dynamic coercion is unsupported
        // and the generated callable panics when invoked.
        let _ = q_bytes(vec![1, 2, 3]);
    }

    // ---- Group 2: Predicate JSON — old (literal) vs new (param) ------------

    #[test]
    fn predicate_literal_json_is_unchanged() {
        assert_eq!(
            sonic_rs::to_string(&Predicate::eq("username", "alice")).unwrap(),
            r#"{"Eq":["username",{"String":"alice"}]}"#
        );
        assert_eq!(
            sonic_rs::to_string(&Predicate::gt("score", 10i64)).unwrap(),
            r#"{"Gt":["score",{"I64":10}]}"#
        );
        assert_eq!(
            sonic_rs::to_string(&Predicate::between("age", 18i64, 65i64)).unwrap(),
            r#"{"Between":["age",{"I64":18},{"I64":65}]}"#
        );
    }

    #[test]
    fn predicate_param_json_uses_expr_variants() {
        assert_eq!(
            sonic_rs::to_string(&Predicate::eq("username", Expr::param("name"))).unwrap(),
            r#"{"EqExpr":["username",{"Param":"name"}]}"#
        );
        assert_eq!(
            sonic_rs::to_string(&Predicate::lte("score", Expr::param("max"))).unwrap(),
            r#"{"LteExpr":["score",{"Param":"max"}]}"#
        );
        assert_eq!(
            sonic_rs::to_string(&Predicate::between("age", Expr::param("lo"), 65i64)).unwrap(),
            r#"{"BetweenExpr":["age",{"Param":"lo"},{"Constant":{"I64":65}}]}"#
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

    // ---- Group 3: SourcePredicate JSON — old (literal) vs new (param) -------

    #[test]
    fn source_predicate_literal_json_is_unchanged() {
        assert_eq!(
            sonic_rs::to_string(&SourcePredicate::eq("username", "alice")).unwrap(),
            r#"{"Eq":["username",{"String":"alice"}]}"#
        );
        assert_eq!(
            sonic_rs::to_string(&SourcePredicate::gt("score", 10i64)).unwrap(),
            r#"{"Gt":["score",{"I64":10}]}"#
        );
        assert_eq!(
            sonic_rs::to_string(&SourcePredicate::between("age", 18i64, 65i64)).unwrap(),
            r#"{"Between":["age",{"I64":18},{"I64":65}]}"#
        );
    }

    #[test]
    fn source_predicate_param_json_uses_expr_variants() {
        assert_eq!(
            sonic_rs::to_string(&SourcePredicate::eq("username", Expr::param("name"))).unwrap(),
            r#"{"EqExpr":["username",{"Param":"name"}]}"#
        );
        assert_eq!(
            sonic_rs::to_string(&SourcePredicate::lte("score", Expr::param("max"))).unwrap(),
            r#"{"LteExpr":["score",{"Param":"max"}]}"#
        );
        assert_eq!(
            sonic_rs::to_string(&SourcePredicate::between("age", Expr::param("lo"), 65i64))
                .unwrap(),
            r#"{"BetweenExpr":["age",{"Param":"lo"},{"Constant":{"I64":65}}]}"#
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
            literal_json.contains(r#"{"NWhere":{"Eq":["username",{"String":"alice"}]}}"#),
            "literal NWhere step changed shape: {literal_json}"
        );
        assert!(!literal_json.contains("EqExpr"));

        let param = read_batch()
            .var_as(
                "user",
                g().n_where(SourcePredicate::eq("username", Expr::param("name"))),
            )
            .returning(["user"]);
        let param_json = sonic_rs::to_string(&param).unwrap();
        assert!(
            param_json.contains(r#"{"NWhere":{"EqExpr":["username",{"Param":"name"}]}}"#),
            "param NWhere step missing EqExpr/Param: {param_json}"
        );
    }

    #[test]
    fn nested_dynamic_property_query_json() {
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
                .contains(r#""metadata",{"Value":{"Object":{"externalID":{"String":"some_id"}"#),
            "AddN nested object value changed shape: {write_json}"
        );
        assert!(
            write_json.contains(r#""tags":{"Array":[{"String":"alpha"},{"I64":7}]}"#),
            "AddN nested array value changed shape: {write_json}"
        );
        assert!(
            write_json.contains(r#"{"SetProperty":["metadata",{"Expr":{"Param":"metadata"}}]}"#),
            "SetProperty param changed shape: {write_json}"
        );
        assert!(
            write_json.contains(r#"{"ValueMap":["metadata.externalID"]}"#),
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
            read_json.contains(r#"{"Eq":["metadata.externalID",{"String":"some_id"}]}"#),
            "dotted SourcePredicate changed shape: {read_json}"
        );
        assert!(
            read_json.contains(r#"{"OrderBy":["metadata.score","Desc"]}"#),
            "dotted OrderBy changed shape: {read_json}"
        );
        assert!(
            read_json.contains(r#""source":"metadata.externalID","alias":"external_id""#),
            "dotted property projection changed shape: {read_json}"
        );
        assert!(
            read_json.contains(r#""expr":{"Property":"metadata.score"}"#),
            "dotted expression projection changed shape: {read_json}"
        );
        assert!(
            read_json.contains(r#"{"Values":["metadata.externalID"]}"#),
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

    #[derive(Deserialize)]
    struct Resp;

    fn sample_request() -> DynamicQueryRequest {
        DynamicQueryRequest::read(
            read_batch()
                .var_as(
                    "user",
                    g().n_where(SourcePredicate::eq("username", "alice")),
                )
                .returning(["user"]),
        )
    }

    // ---- Client construction ------------------------------------------------

    #[test]
    fn new_defaults_to_localhost() {
        let client = Client::new(None).unwrap();
        assert_eq!(client.url.as_str(), "http://localhost:6969/v1/query");
        assert!(client.api_key.is_none());
    }

    #[test]
    fn new_parses_custom_url() {
        let client = Client::new(Some("https://cluster.helix-db.com")).unwrap();
        assert_eq!(client.url.as_str(), "https://cluster.helix-db.com/v1/query");
    }

    #[test]
    fn new_rejects_invalid_url() {
        let err = Client::new(Some("not a url")).unwrap_err();
        assert!(matches!(err, HelixError::InvalidURL(_)));
    }

    #[test]
    fn with_api_key_sets_and_clears() {
        let client = Client::new(None).unwrap().with_api_key(Some("hx_secret"));
        assert_eq!(client.api_key.as_deref(), Some("hx_secret"));

        let cleared = client.with_api_key(None);
        assert!(cleared.api_key.is_none());
    }

    // ---- Header assembly ----------------------------------------------------

    #[test]
    fn query_builder_starts_with_only_content_type() {
        let client = Client::new(None).unwrap();
        let builder = client.query::<Resp>();
        assert_eq!(
            builder.headers[0],
            Some(("Content-Type", "application/json"))
        );
        assert!(builder.headers[1..].iter().all(Option::is_none));
    }

    #[test]
    fn header_toggles_populate_slots() {
        let client = Client::new(None).unwrap();
        let builder = client
            .query::<Resp>()
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
        let builder = client.query::<Resp>().should_await_durability(false);
        assert_eq!(builder.headers[3], Some(("x-helix-await-durable", "false")));
    }

    // ---- Query type + body --------------------------------------------------

    #[test]
    fn dynamic_query_sets_query_type() {
        let client = Client::new(None).unwrap();
        let request = client.query::<Resp>().dynamic(sample_request());
        assert!(matches!(request.request.query_type, QueryType::Dynamic(_)));
    }

    #[test]
    fn stored_query_sets_query_type() {
        let client = Client::new(None).unwrap();
        let request = client.query::<Resp>().stored("add_user".to_string());
        assert!(
            matches!(&request.request.query_type, QueryType::Stored(name) if name == "add_user")
        );
    }

    #[derive(serde::Serialize)]
    struct Payload {
        name: String,
    }

    #[test]
    fn body_serializes_payload() {
        let client = Client::new(None).unwrap();
        let payload = Payload {
            name: "alice".to_string(),
        };
        let builder = client.query::<Resp>().body(&payload).unwrap();
        assert_eq!(builder.body, Some(sonic_rs::to_vec(&payload).unwrap()));
    }

    // ---- Request routing (exercises the real `send()` path) -----------------

    #[derive(serde::Deserialize)]
    struct EmptyResp {}

    /// Spawn a one-shot HTTP server on a random port. Returns its base URL and a
    /// handle that resolves to the request-target (path) of the first request.
    async fn spawn_capture_server() -> (String, tokio::task::JoinHandle<String>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let handle = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 4096];
            let n = socket.read(&mut buf).await.unwrap();
            let request_line = String::from_utf8_lossy(&buf[..n])
                .lines()
                .next()
                .unwrap()
                .to_string();
            // `METHOD <target> HTTP/1.1` -> the target.
            let target = request_line.split_whitespace().nth(1).unwrap().to_string();
            let resp = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}";
            socket.write_all(resp.as_bytes()).await.unwrap();
            target
        });
        (base, handle)
    }

    #[tokio::test]
    async fn dynamic_query_posts_to_v1_query() {
        let (base, handle) = spawn_capture_server().await;
        let client = Client::new(Some(&base)).unwrap();
        let _: EmptyResp = client
            .query()
            .dynamic(sample_request())
            .send()
            .await
            .unwrap();
        assert_eq!(handle.await.unwrap(), "/v1/query");
    }

    #[tokio::test]
    async fn stored_query_posts_to_named_route() {
        let (base, handle) = spawn_capture_server().await;
        let client = Client::new(Some(&base)).unwrap();
        let _: EmptyResp = client
            .query()
            .stored("add_user".to_string())
            .send()
            .await
            .unwrap();
        assert_eq!(handle.await.unwrap(), "/v1/query/add_user");
    }
}
