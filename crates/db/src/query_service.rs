//! Request-level query service for transport wrappers.
//!
//! This module owns the query contract between transports and the
//! planner/interpreter stack. HTTP and gRPC should call this boundary instead
//! of deserializing, planning, or serializing execution results themselves.

use std::collections::BTreeMap;
use std::sync::Arc;

use helix_ast::batch::{BatchQuery, ReadBatch, WriteBatch};
use helix_ast::query::{QueryRequest, QueryRequestType, QueryValue};
use helix_planner::context::ParamBindings;
use helix_planner::ir::NonEmptyString;
use serde::ser::Serializer;
use serde::Serialize;
use serde_json::Value as JsonValue;

use crate::encoding::keys::tenant::DataScope;
use crate::encoding::property::property_value::PropertyValue as DbPropertyValue;
use crate::error::HelixDbError;
use crate::execution::interpreter::{ElementRef, ExecutionResult, ExecutionScalar, ExecutionValue};
use crate::HelixDB;

/// Shared request executor used by server transports.
#[derive(Clone)]
pub struct HelixQueryService {
    db: Arc<HelixDB>,
}

impl HelixQueryService {
    /// Create a query service.
    pub fn new(db: Arc<HelixDB>) -> Self {
        Self { db }
    }

    /// Execute an inline query.
    pub async fn execute_query(
        &self,
        request: QueryRequest,
    ) -> std::result::Result<QueryResponse, QueryServiceError> {
        self.execute_query_with_mode(request, QueryMode::Execute)
            .await
    }

    /// Execute an inline query with an explicit server transport mode.
    pub async fn execute_query_with_mode(
        &self,
        request: QueryRequest,
        mode: QueryMode,
    ) -> std::result::Result<QueryResponse, QueryServiceError> {
        execute_query_on(self.db.as_ref(), request, mode).await
    }

    /// Execute an inline query in a request storage namespace.
    pub async fn execute_query_scoped(
        &self,
        request: QueryRequest,
        tenant_scope: DataScope,
    ) -> std::result::Result<QueryResponse, QueryServiceError> {
        self.execute_query_with_mode_scoped(request, QueryMode::Execute, tenant_scope)
            .await
    }

    /// Execute an inline query with explicit server transport mode and storage namespace.
    pub async fn execute_query_with_mode_scoped(
        &self,
        request: QueryRequest,
        mode: QueryMode,
        tenant_scope: DataScope,
    ) -> std::result::Result<QueryResponse, QueryServiceError> {
        execute_query_on_scoped(self.db.as_ref(), request, mode, tenant_scope).await
    }
}

pub(crate) async fn execute_query_on(
    db: &HelixDB,
    request: QueryRequest,
    mode: QueryMode,
) -> std::result::Result<QueryResponse, QueryServiceError> {
    execute_query_on_scoped(db, request, mode, DataScope::LegacyUnscoped).await
}

/// Execute a query request in a request storage namespace.
pub async fn execute_query_on_scoped(
    db: &HelixDB,
    request: QueryRequest,
    mode: QueryMode,
    tenant_scope: DataScope,
) -> std::result::Result<QueryResponse, QueryServiceError> {
    let query = ValidatedQuery::from_request(request)?;
    query.validate_mode(mode)?;
    execute_validated(db, query, tenant_scope).await
}

async fn execute_validated(
    db: &HelixDB,
    query: ValidatedQuery,
    tenant_scope: DataScope,
) -> std::result::Result<QueryResponse, QueryServiceError> {
    let (batch, params) = match query {
        ValidatedQuery::Read { batch, parameters } => {
            (BatchQuery::Read(batch), query_param_bindings(parameters)?)
        }
        ValidatedQuery::Write { batch, parameters } => {
            if db.is_reader_mode() {
                return Err(HelixDbError::WriterModeRequired {
                    actual: db.mode().as_str(),
                }
                .into());
            }
            (BatchQuery::Write(batch), query_param_bindings(parameters)?)
        }
    };
    let ctx = db
        .planner_context_scoped(params.clone(), tenant_scope)
        .await?;
    let plan = helix_planner::planning::plan(&batch, &ctx)?;
    let result = db.execute_scoped(&plan, params, tenant_scope).await?;
    QueryResponse::from_execution_result(result)
}

enum ValidatedQuery {
    Read {
        batch: ReadBatch,
        parameters: BTreeMap<String, QueryValue>,
    },
    Write {
        batch: WriteBatch,
        parameters: BTreeMap<String, QueryValue>,
    },
}

impl ValidatedQuery {
    fn from_request(request: QueryRequest) -> std::result::Result<Self, QueryServiceError> {
        let parameters = request.parameters.unwrap_or_default();
        match request.request_type {
            QueryRequestType::Read => Ok(Self::Read {
                batch: read_batch_from_query(request.query),
                parameters,
            }),
            QueryRequestType::Write => Ok(Self::Write {
                batch: write_batch_from_query(request.query),
                parameters,
            }),
        }
    }

    fn validate_mode(&self, mode: QueryMode) -> std::result::Result<(), QueryServiceError> {
        match (mode, self) {
            (QueryMode::Execute, _) | (QueryMode::Warm, Self::Read { .. }) => Ok(()),
            (QueryMode::Warm, Self::Write { .. }) => Err(QueryServiceError::InvalidRequest(
                "warm queries must be read requests".to_string(),
            )),
        }
    }
}

/// Execution behavior for query requests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryMode {
    /// Execute the query and return its results.
    Execute,
    /// Execute a read query through the normal path so caches are populated.
    Warm,
}

fn read_batch_from_query(query: BatchQuery) -> ReadBatch {
    match query {
        BatchQuery::Read(batch) => batch,
        BatchQuery::Write(batch) => ReadBatch {
            entries: batch.entries,
            returns: batch.returns,
        },
    }
}

fn write_batch_from_query(query: BatchQuery) -> WriteBatch {
    match query {
        BatchQuery::Read(batch) => WriteBatch {
            entries: batch.entries,
            returns: batch.returns,
        },
        BatchQuery::Write(batch) => batch,
    }
}

fn query_param_bindings(
    parameters: BTreeMap<String, QueryValue>,
) -> std::result::Result<ParamBindings, QueryServiceError> {
    let mut query_values = BTreeMap::new();
    for (name, value) in parameters {
        let Some(name) = NonEmptyString::new(name) else {
            return Err(QueryServiceError::InvalidRequest(
                "parameter name must not be empty".to_string(),
            ));
        };
        query_values.insert(name, value);
    }
    Ok(ParamBindings {
        values: BTreeMap::new(),
        query_values,
    })
}

/// JSON response for query returns.
#[derive(Debug, Clone, PartialEq)]
pub struct QueryResponse {
    returns: BTreeMap<String, JsonValue>,
}

impl QueryResponse {
    /// Convert an interpreter result into the public JSON response shape.
    pub fn from_execution_result(
        result: ExecutionResult,
    ) -> std::result::Result<Self, QueryServiceError> {
        let returns = result
            .returns
            .into_iter()
            .map(|(name, value)| Ok((name.into_string(), execution_value_to_json(value)?)))
            .collect::<std::result::Result<BTreeMap<_, _>, QueryServiceError>>()?;
        Ok(Self { returns })
    }

    /// Serialize the response as JSON bytes.
    pub fn to_json_bytes(&self) -> std::result::Result<Vec<u8>, QueryServiceError> {
        sonic_rs::to_vec(self).map_err(QueryServiceError::Serialize)
    }

    /// Borrow the returned values.
    pub fn returns(&self) -> &BTreeMap<String, JsonValue> {
        &self.returns
    }
}

impl Serialize for QueryResponse {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.returns.serialize(serializer)
    }
}

fn execution_value_to_json(
    value: ExecutionValue,
) -> std::result::Result<JsonValue, QueryServiceError> {
    match value {
        ExecutionValue::Stream(rows) => rows
            .into_iter()
            .map(execution_row_to_json)
            .collect::<std::result::Result<Vec<_>, QueryServiceError>>()
            .map(JsonValue::Array),
        ExecutionValue::FoldedStream(rows) => rows
            .into_rows()
            .into_iter()
            .map(execution_row_to_json)
            .collect::<std::result::Result<Vec<_>, QueryServiceError>>()
            .map(JsonValue::Array),
        ExecutionValue::Count(count) => Ok(JsonValue::from(count)),
        ExecutionValue::Bool(value) => Ok(JsonValue::Bool(value)),
        ExecutionValue::Scalars(values) => values
            .into_iter()
            .map(execution_scalar_to_json)
            .collect::<std::result::Result<Vec<_>, QueryServiceError>>()
            .map(JsonValue::Array),
        ExecutionValue::IndexDdlReceipt(receipt) => {
            serde_json::to_value(receipt).map_err(QueryServiceError::JsonSerialize)
        }
        ExecutionValue::IndexOperationStatus(status) => {
            serde_json::to_value(status).map_err(QueryServiceError::JsonSerialize)
        }
    }
}

fn execution_row_to_json(
    row: crate::execution::interpreter::ExecutionRow,
) -> std::result::Result<JsonValue, QueryServiceError> {
    let bindings = row
        .bindings
        .into_iter()
        .map(|(name, value)| (name.into_string(), element_ref_to_json(value)))
        .collect::<serde_json::Map<_, _>>();
    Ok(JsonValue::Object(serde_json::Map::from_iter([
        (
            "current".to_string(),
            row.current.map_or(JsonValue::Null, element_ref_to_json),
        ),
        ("bindings".to_string(), JsonValue::Object(bindings)),
    ])))
}

fn execution_scalar_to_json(
    value: ExecutionScalar,
) -> std::result::Result<JsonValue, QueryServiceError> {
    match value {
        ExecutionScalar::NodeId(id) | ExecutionScalar::EdgeId(id) => Ok(JsonValue::from(id)),
        ExecutionScalar::String(value) => Ok(JsonValue::String(value)),
        ExecutionScalar::Value(value) => property_value_to_json(value),
        ExecutionScalar::Object(values) => values
            .into_iter()
            .map(|(name, value)| Ok((name, property_value_to_json(value)?)))
            .collect::<std::result::Result<serde_json::Map<_, _>, QueryServiceError>>()
            .map(JsonValue::Object),
    }
}

fn property_value_to_json(
    value: DbPropertyValue,
) -> std::result::Result<JsonValue, QueryServiceError> {
    serde_json::to_value(value).map_err(QueryServiceError::JsonSerialize)
}

fn element_ref_to_json(value: ElementRef) -> JsonValue {
    let (kind, id) = match value {
        ElementRef::Node(id) => ("node", id),
        ElementRef::Edge(id) => ("edge", id),
    };
    JsonValue::Object(serde_json::Map::from_iter([(
        kind.to_string(),
        JsonValue::from(id),
    )]))
}

/// Query service failures mapped by transports into protocol-specific errors.
#[derive(Debug, thiserror::Error)]
pub enum QueryServiceError {
    /// Request body or route parameters are invalid.
    #[error("invalid request: {0}")]
    InvalidRequest(String),

    /// Planning failed.
    #[error("planner error: {0}")]
    Planner(#[from] helix_planner::error::PlannerError),

    /// Execution failed.
    #[error("db error: {0}")]
    Db(#[from] HelixDbError),

    /// Response serialization failed.
    #[error("json serialization error: {0}")]
    JsonSerialize(serde_json::Error),

    /// Response serialization failed.
    #[error("json serialization error: {0}")]
    Serialize(sonic_rs::Error),
}

impl QueryServiceError {
    /// Returns true when the request can be retried after a transaction conflict.
    pub fn is_transaction_conflict(&self) -> bool {
        matches!(self, Self::Db(error) if error.is_transaction_conflict())
    }

    /// Stable index lifecycle error code, when this failure belongs to that
    /// public compatibility surface.
    pub fn index_error_code(&self) -> Option<&'static str> {
        match self {
            Self::Db(error) => error.index_error_code(),
            Self::InvalidRequest(_)
            | Self::Planner(_)
            | Self::JsonSerialize(_)
            | Self::Serialize(_) => None,
        }
    }
}

impl From<QueryServiceError> for HelixDbError {
    fn from(value: QueryServiceError) -> Self {
        match value {
            QueryServiceError::Db(error) => error,
            other @ (QueryServiceError::InvalidRequest(_)
            | QueryServiceError::Planner(_)
            | QueryServiceError::JsonSerialize(_)
            | QueryServiceError::Serialize(_)) => HelixDbError::Query(other.to_string()),
        }
    }
}

impl QueryResponse {
    #[cfg(test)]
    fn from_returns(returns: BTreeMap<String, JsonValue>) -> Self {
        Self { returns }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use helix_ast::batch::{read_batch, write_batch};
    use helix_ast::graph::NodeRef;
    use helix_ast::query::{QueryRequest, QueryValue};
    use helix_ast::traversal::{g, ShortestPathDirection};
    use helix_ast::value::PropertyInput;
    use slatedb::object_store::memory::InMemory;

    use crate::config::IndexConfig;
    use crate::encoding::property::property_value::PropertyValue;
    use crate::execution::interpreter::{ExecutionRow, ExecutionValue, FoldedStream};
    use crate::HelixDbSource;

    fn name(value: &str) -> NonEmptyString {
        NonEmptyString::new(value).expect("test name is non-empty")
    }

    #[test]
    fn query_param_bindings_reject_empty_names() {
        let err = query_param_bindings(BTreeMap::from([(String::new(), QueryValue::Bool(true))]))
            .expect_err("empty parameter name should be rejected");

        assert!(matches!(err, QueryServiceError::InvalidRequest(_)));
    }

    #[test]
    fn batch_normalization_follows_request_type() {
        let read_from_write = read_batch_from_query(BatchQuery::Write(WriteBatch {
            entries: Vec::new(),
            returns: Vec::new(),
        }));
        assert!(read_from_write.entries.is_empty());
        assert!(read_from_write.returns.is_empty());

        let write_from_read = write_batch_from_query(BatchQuery::Read(ReadBatch {
            entries: Vec::new(),
            returns: Vec::new(),
        }));
        assert!(write_from_read.entries.is_empty());
        assert!(write_from_read.returns.is_empty());
    }

    #[test]
    fn query_response_serializes_as_top_level_returns_object() {
        let response = QueryResponse::from_returns(BTreeMap::from([
            ("count".to_string(), JsonValue::from(2)),
            ("exists".to_string(), JsonValue::Bool(true)),
        ]));

        let json = response.to_json_bytes().expect("serialize response");
        let value: JsonValue = serde_json::from_slice(&json).expect("valid json");

        assert_eq!(value["count"], JsonValue::from(2));
        assert_eq!(value["exists"], JsonValue::Bool(true));
        assert!(value.get("returns").is_none());
    }

    #[tokio::test]
    async fn db_query_executes_shortest_path_after_query_writes() {
        let db = HelixDB::open(HelixDbSource::InMemory {
            database: "shortest-path-query".to_string(),
        })
        .await
        .expect("writer should open");

        let create = write_batch()
            .var_as("a", g().add_n("Node", Vec::<(&str, PropertyInput)>::new()))
            .var_as("b", g().add_n("Node", Vec::<(&str, PropertyInput)>::new()))
            .var_as("c", g().add_n("Node", Vec::<(&str, PropertyInput)>::new()))
            .var_as("a_id", g().n(NodeRef::var("a")).id())
            .var_as("b_id", g().n(NodeRef::var("b")).id())
            .var_as("c_id", g().n(NodeRef::var("c")).id())
            .var_as(
                "ab",
                g().n(NodeRef::var("a"))
                    .add_e(
                        "LINK",
                        NodeRef::var("b"),
                        Vec::<(&str, PropertyInput)>::new(),
                    )
                    .count(),
            )
            .var_as(
                "bc",
                g().n(NodeRef::var("b"))
                    .add_e(
                        "LINK",
                        NodeRef::var("c"),
                        Vec::<(&str, PropertyInput)>::new(),
                    )
                    .count(),
            )
            .returning(["a_id", "b_id", "c_id", "ab", "bc"]);

        let create_response = db
            .query(QueryRequest::write(create))
            .await
            .expect("fixture write should execute through query service");

        assert_eq!(create_response.get("a_id"), Some(&serde_json::json!([0])));
        assert_eq!(create_response.get("b_id"), Some(&serde_json::json!([1])));
        assert_eq!(create_response.get("c_id"), Some(&serde_json::json!([2])));
        assert_eq!(create_response.get("ab"), Some(&serde_json::json!(1)));
        assert_eq!(create_response.get("bc"), Some(&serde_json::json!(1)));

        let read = read_batch()
            .var_as("source", g().n(NodeRef::id(0)))
            .var_as("target", g().n(NodeRef::id(2)))
            .var_as("path", g().shortest_path(NodeRef::id(0), NodeRef::id(2), 3))
            .var_as(
                "var_path",
                g().shortest_path(NodeRef::var("source"), NodeRef::var("target"), 3),
            )
            .var_as(
                "param_path",
                g().shortest_path_with(
                    NodeRef::var("source"),
                    NodeRef::param("target_id"),
                    Some("LINK"),
                    ShortestPathDirection::Out,
                    2,
                ),
            )
            .var_as(
                "cutoff",
                g().shortest_path_with(
                    NodeRef::id(0),
                    NodeRef::id(2),
                    None::<&str>,
                    ShortestPathDirection::Out,
                    1,
                ),
            )
            .var_as(
                "reverse_in",
                g().shortest_path_with(
                    NodeRef::id(2),
                    NodeRef::id(0),
                    None::<&str>,
                    ShortestPathDirection::In,
                    3,
                ),
            )
            .var_as(
                "labeled",
                g().shortest_path_with(
                    NodeRef::id(0),
                    NodeRef::id(2),
                    Some("LINK"),
                    ShortestPathDirection::Both,
                    2,
                ),
            )
            .var_as(
                "missing_label",
                g().shortest_path_with(
                    NodeRef::id(0),
                    NodeRef::id(2),
                    Some("MISSING"),
                    ShortestPathDirection::Both,
                    2,
                ),
            )
            .var_as(
                "identity",
                g().shortest_path(NodeRef::id(0), NodeRef::id(0), 2),
            )
            .returning([
                "path",
                "var_path",
                "param_path",
                "cutoff",
                "reverse_in",
                "labeled",
                "missing_label",
                "identity",
            ]);

        let read = QueryRequest::read(read).with_parameter_value("target_id", QueryValue::I64(2));
        let response = db
            .query(read)
            .await
            .expect("shortest-path read should execute through query service");

        assert_eq!(response.get("path"), Some(&serde_json::json!([0, 1, 2])));
        assert_eq!(
            response.get("var_path"),
            Some(&serde_json::json!([0, 1, 2]))
        );
        assert_eq!(
            response.get("param_path"),
            Some(&serde_json::json!([0, 1, 2]))
        );
        assert_eq!(response.get("cutoff"), Some(&serde_json::json!([])));
        assert_eq!(
            response.get("reverse_in"),
            Some(&serde_json::json!([2, 1, 0]))
        );
        assert_eq!(response.get("labeled"), Some(&serde_json::json!([0, 1, 2])));
        assert_eq!(response.get("missing_label"), Some(&serde_json::json!([])));
        assert_eq!(response.get("identity"), Some(&serde_json::json!([0])));
    }

    #[tokio::test]
    async fn query_service_wrappers_delegate_execute_warm_and_scoped_reads() {
        let db = Arc::new(
            HelixDB::open(HelixDbSource::InMemory {
                database: "query-service-wrappers".to_string(),
            })
            .await
            .expect("writer should open"),
        );
        let service = HelixQueryService::new(db);
        let request = QueryRequest::read(
            read_batch()
                .var_as("count", g().n(NodeRef::id(999)).count())
                .returning(["count"]),
        );

        let execute = service
            .execute_query(request.clone())
            .await
            .expect("execute wrapper should delegate");
        let warm = service
            .execute_query_with_mode(request.clone(), QueryMode::Warm)
            .await
            .expect("warm wrapper should delegate");
        let scoped = service
            .execute_query_scoped(request.clone(), DataScope::LegacyUnscoped)
            .await
            .expect("scoped wrapper should delegate");
        let scoped_warm = service
            .execute_query_with_mode_scoped(request, QueryMode::Warm, DataScope::LegacyUnscoped)
            .await
            .expect("scoped warm wrapper should delegate");

        for response in [execute, warm, scoped, scoped_warm] {
            assert_eq!(response.returns().get("count"), Some(&JsonValue::from(0)));
        }
    }

    #[tokio::test]
    async fn query_service_rejects_writes_on_reader_handles() {
        let object_store: Arc<dyn slatedb::object_store::ObjectStore> = Arc::new(InMemory::new());
        let _writer = HelixDB::open_with_object_store_and_index_config_for_tests(
            "query-service-reader-mode",
            Arc::clone(&object_store),
            IndexConfig::new(),
        )
        .await
        .expect("writer should initialize storage");
        let reader = HelixDB::open_reader_with_object_store_and_index_config_for_tests(
            "query-service-reader-mode",
            object_store,
            IndexConfig::new(),
        )
        .await
        .expect("reader should open");
        let service = HelixQueryService::new(Arc::new(reader));

        let error = service
            .execute_query(QueryRequest::write(WriteBatch {
                entries: Vec::new(),
                returns: Vec::new(),
            }))
            .await
            .expect_err("reader must reject write requests");

        assert!(matches!(
            error,
            QueryServiceError::Db(HelixDbError::WriterModeRequired { actual: "reader" })
        ));
    }

    #[test]
    fn warm_mode_accepts_read_queries() {
        let query = ValidatedQuery::Read {
            batch: ReadBatch {
                entries: Vec::new(),
                returns: Vec::new(),
            },
            parameters: BTreeMap::new(),
        };

        query
            .validate_mode(QueryMode::Warm)
            .expect("warm mode accepts read queries");
    }

    #[test]
    fn warm_mode_rejects_write_queries() {
        let query = ValidatedQuery::Write {
            batch: WriteBatch {
                entries: Vec::new(),
                returns: Vec::new(),
            },
            parameters: BTreeMap::new(),
        };
        let err = query
            .validate_mode(QueryMode::Warm)
            .expect_err("warm mode rejects write queries");

        assert!(matches!(err, QueryServiceError::InvalidRequest(_)));
    }

    #[test]
    fn execution_result_serializes_scalars_and_stream_rows() {
        let result = ExecutionResult {
            last: None,
            variables: BTreeMap::new(),
            returns: BTreeMap::from([
                (
                    name("users"),
                    ExecutionValue::Scalars(vec![ExecutionScalar::Object(BTreeMap::from([(
                        "name".to_string(),
                        PropertyValue::String("alice".to_string()),
                    )]))]),
                ),
                (
                    name("rows"),
                    ExecutionValue::Stream(vec![{
                        let mut row = ExecutionRow::empty();
                        row.current = Some(ElementRef::Node(7));
                        row.bindings = BTreeMap::from([(name("friend"), ElementRef::Edge(9))]);
                        row
                    }]),
                ),
            ]),
        };

        let response =
            QueryResponse::from_execution_result(result).expect("execution result serializes");

        assert_eq!(
            response.returns()["users"][0]["name"],
            JsonValue::from("alice")
        );
        assert_eq!(
            response.returns()["rows"][0]["current"]["node"],
            JsonValue::from(7)
        );
        assert_eq!(
            response.returns()["rows"][0]["bindings"]["friend"]["edge"],
            JsonValue::from(9)
        );
    }

    #[test]
    fn execution_result_serializes_folded_bool_string_and_property_values() {
        let mut folded_row = ExecutionRow::empty();
        folded_row.current = Some(ElementRef::Edge(11));
        let result = ExecutionResult {
            last: None,
            variables: BTreeMap::new(),
            returns: BTreeMap::from([
                (
                    name("folded"),
                    ExecutionValue::FoldedStream(FoldedStream::new(vec![folded_row])),
                ),
                (name("exists"), ExecutionValue::Bool(true)),
                (
                    name("scalars"),
                    ExecutionValue::Scalars(vec![
                        ExecutionScalar::String("ready".to_string()),
                        ExecutionScalar::Value(PropertyValue::I64(7)),
                    ]),
                ),
            ]),
        };

        let response = QueryResponse::from_execution_result(result).expect("values serialize");

        assert_eq!(response.returns()["folded"][0]["current"]["edge"], 11);
        assert_eq!(response.returns()["exists"], JsonValue::Bool(true));
        assert_eq!(
            response.returns()["scalars"],
            serde_json::json!(["ready", 7])
        );
    }

    #[test]
    fn query_service_errors_classify_conflicts_and_preserve_db_errors() {
        let conflict = QueryServiceError::Db(HelixDbError::TransactionConflict(
            "retry request".to_string(),
        ));
        assert!(conflict.is_transaction_conflict());
        assert!(matches!(
            HelixDbError::from(conflict),
            HelixDbError::TransactionConflict(_)
        ));

        let invalid = QueryServiceError::InvalidRequest("bad request".to_string());
        assert!(!invalid.is_transaction_conflict());
        assert!(matches!(
            HelixDbError::from(invalid),
            HelixDbError::Query(message) if message.contains("invalid request")
        ));

        let planner = QueryServiceError::Planner(
            helix_planner::error::PlannerError::UnsupportedEdgeAllTarget,
        );
        assert!(matches!(
            HelixDbError::from(planner),
            HelixDbError::Query(_)
        ));

        let json =
            execution_scalar_to_json(ExecutionScalar::Value(PropertyValue::DateTime(i64::MAX)))
                .expect_err("invalid datetime should fail JSON conversion");
        assert!(matches!(json, QueryServiceError::JsonSerialize(_)));
        assert!(matches!(HelixDbError::from(json), HelixDbError::Query(_)));

        let sonic = sonic_rs::from_str::<u8>("not-json").expect_err("invalid JSON should fail");
        assert!(matches!(
            HelixDbError::from(QueryServiceError::Serialize(sonic)),
            HelixDbError::Query(_)
        ));
    }
}
