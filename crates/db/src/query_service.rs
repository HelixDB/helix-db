//! Request-level query service for transport wrappers.
//!
//! This module owns the query contract between transports and the
//! planner/interpreter stack. HTTP and gRPC should call this boundary instead
//! of deserializing, planning, or serializing execution results themselves.

use std::collections::BTreeMap;
use std::sync::Arc;

use helix_ast::batch::{BatchQuery, ReadBatch, WriteBatch};
use helix_ast::query::{QueryRequest, QueryRequestType, QueryValue};
use helix_metrics::{query, query::transport::OssQueryMetrics};
use helix_planner::{context::ParamBindings, diagnostics::PlannerDiagnostics, ir::NonEmptyString};
use serde::ser::Serializer;
use serde::Serialize;
use serde_json::Value as JsonValue;

use crate::encoding::keys::scope::DataScope;
use crate::encoding::property::property_value::PropertyValue as DbPropertyValue;
use crate::error::HelixDbError;
use crate::execution::interpreter::{
    ElementRef, ExecutionResult, ExecutionScalar, ExecutionValue, ReturnedValue,
};
use crate::execution_control::ExecutionControl;
use crate::HelixDB;

/// Shared request executor used by server transports.
#[derive(Clone)]
pub struct HelixQueryService {
    db: Arc<HelixDB>,
    query_metrics: Option<OssQueryMetrics>,
}

impl HelixQueryService {
    /// Create a query service.
    pub fn new(db: Arc<HelixDB>) -> Self {
        Self {
            db,
            query_metrics: None,
        }
    }

    /// Create a query service that emits one OSS server event per execution.
    pub fn with_query_metrics(db: Arc<HelixDB>, query_metrics: OssQueryMetrics) -> Self {
        Self {
            db,
            query_metrics: Some(query_metrics),
        }
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
        self.execute_query_with_mode_and_metrics_tenant(request, mode, None)
            .await
    }

    /// Execute an inline query and attach a transport tenant to anonymous telemetry.
    ///
    /// This identity does not select the storage namespace. Call
    /// [`Self::execute_query_with_mode_scoped`] when execution must be tenant-scoped.
    pub async fn execute_query_with_mode_and_metrics_tenant(
        &self,
        request: QueryRequest,
        mode: QueryMode,
        tenant_id: Option<query::TenantId>,
    ) -> std::result::Result<QueryResponse, QueryServiceError> {
        execute_query_on_observed(
            self.db.as_ref(),
            request,
            mode,
            tenant_id,
            self.query_metrics.as_ref(),
        )
        .await
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
        self.execute_query_with_mode_scoped_controlled(
            request,
            mode,
            tenant_scope,
            ExecutionControl::unlimited(),
        )
        .await
    }

    /// Execute an inline query with request-scoped monotonic cancellation.
    pub async fn execute_query_with_mode_scoped_controlled(
        &self,
        request: QueryRequest,
        mode: QueryMode,
        tenant_scope: DataScope,
        execution_control: ExecutionControl,
    ) -> std::result::Result<QueryResponse, QueryServiceError> {
        execute_query_on_scoped_observed(
            self.db.as_ref(),
            request,
            mode,
            tenant_scope,
            None,
            self.query_metrics.as_ref(),
            execution_control,
        )
        .await
    }
}

pub(crate) async fn execute_query_on_observed(
    db: &HelixDB,
    request: QueryRequest,
    mode: QueryMode,
    tenant_id: Option<query::TenantId>,
    query_metrics: Option<&OssQueryMetrics>,
) -> std::result::Result<QueryResponse, QueryServiceError> {
    execute_query_on_scoped_observed(
        db,
        request,
        mode,
        DataScope::LegacyUnscoped,
        tenant_id,
        query_metrics,
        ExecutionControl::unlimited(),
    )
    .await
}

/// Execute a query request in a request storage namespace.
pub async fn execute_query_on_scoped(
    db: &HelixDB,
    request: QueryRequest,
    mode: QueryMode,
    tenant_scope: DataScope,
) -> std::result::Result<QueryResponse, QueryServiceError> {
    execute_query_on_scoped_observed(
        db,
        request,
        mode,
        tenant_scope,
        None,
        None,
        ExecutionControl::unlimited(),
    )
    .await
}

pub(crate) async fn execute_query_on_scoped_observed(
    db: &HelixDB,
    request: QueryRequest,
    mode: QueryMode,
    tenant_scope: DataScope,
    tenant_id: Option<query::TenantId>,
    query_metrics: Option<&OssQueryMetrics>,
    execution_control: ExecutionControl,
) -> std::result::Result<QueryResponse, QueryServiceError> {
    let observation = query_metrics.and_then(|_| QueryObservation::capture(&request, tenant_id));
    let started_at = std::time::Instant::now();
    let result = match execution_control.check() {
        Ok(()) => match ValidatedQuery::from_request(request) {
            Ok(query) => match query.validate_mode(mode) {
                Ok(()) => execute_validated(db, query, tenant_scope, execution_control).await,
                Err(error) => Err(error),
            },
            Err(error) => Err(error),
        },
        Err(error) => Err(error.into()),
    };
    if let (Some(query_metrics), Some(observation)) = (query_metrics, observation) {
        let _ = query_metrics.record(observation.event(&result, started_at.elapsed()));
    }
    result
}

struct QueryObservation {
    name: Option<query::QueryName>,
    raw_query: query::CanonicalQuery,
    query_type: query::QueryType,
    tenant_id: Option<query::TenantId>,
}

impl QueryObservation {
    fn capture(request: &QueryRequest, tenant_id: Option<query::TenantId>) -> Option<Self> {
        Some(Self {
            name: request
                .query_name()
                .and_then(|name| query::QueryName::new(name).ok()),
            raw_query: query::CanonicalQuery::from_telemetry_serializable(request.query()).ok()?,
            query_type: match request.request_type() {
                QueryRequestType::Read => query::QueryType::Read,
                QueryRequestType::Write => query::QueryType::Write,
            },
            tenant_id,
        })
    }

    fn event(
        self,
        result: &std::result::Result<QueryResponse, QueryServiceError>,
        latency: std::time::Duration,
    ) -> query::QueryEvent {
        let outcome = match result {
            Ok(_) => query::QueryOutcome::Succeeded {
                warnings: Vec::new(),
            },
            Err(error) => {
                let error_type = query_error_type(error);
                let message = match error_type {
                    query::QueryErrorType::InvalidRequest => "query request was invalid",
                    query::QueryErrorType::Planning => "query planning failed",
                    query::QueryErrorType::Execution => "query execution failed",
                    query::QueryErrorType::Conflict => "query conflicted with another transaction",
                    query::QueryErrorType::Internal => "query response serialization failed",
                };
                query::QueryOutcome::Failed {
                    errors: vec![query::QueryError {
                        error_type,
                        message: message.to_owned(),
                    }],
                }
            }
        };
        let planner_diagnostics = result
            .as_ref()
            .ok()
            .and_then(|response| serde_json::to_value(response.diagnostics()).ok());
        query::QueryEvent::now(
            self.name,
            self.raw_query,
            self.query_type,
            u64::try_from(latency.as_micros()).unwrap_or(u64::MAX),
            self.tenant_id,
            outcome,
            planner_diagnostics,
        )
    }
}

fn query_error_type(error: &QueryServiceError) -> query::QueryErrorType {
    match error {
        QueryServiceError::InvalidRequest(_) => query::QueryErrorType::InvalidRequest,
        QueryServiceError::Planner(_) => query::QueryErrorType::Planning,
        QueryServiceError::Db(error) if error.is_transaction_conflict() => {
            query::QueryErrorType::Conflict
        }
        QueryServiceError::Db(error) if error.is_invalid_vector_input() => {
            query::QueryErrorType::InvalidRequest
        }
        QueryServiceError::Db(_) => query::QueryErrorType::Execution,
        QueryServiceError::JsonSerialize(_) | QueryServiceError::Serialize(_) => {
            query::QueryErrorType::Internal
        }
    }
}

async fn execute_validated(
    db: &HelixDB,
    query: ValidatedQuery,
    tenant_scope: DataScope,
    execution_control: ExecutionControl,
) -> std::result::Result<QueryResponse, QueryServiceError> {
    execution_control.check()?;
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
    let prepared = execution_control
        .run(db.planner_context_scoped_prepared(params.clone(), tenant_scope))
        .await?;
    execution_control.check()?;
    let planning = helix_planner::planning::plan_with_diagnostics(&batch, prepared.context())?;
    execution_control.check()?;
    let result = db
        .execute_prepared_scoped_controlled(
            planning.plan(),
            params,
            tenant_scope,
            execution_control,
            prepared.into_catalog_proof(),
        )
        .await?;
    let (_, diagnostics) = planning.into_parts();
    QueryResponse::from_execution_result_with_diagnostics(result, diagnostics)
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
        let (query, parameters) = request.into_query();
        match query {
            BatchQuery::Read(batch) => Ok(Self::Read { batch, parameters }),
            BatchQuery::Write(batch) => Ok(Self::Write { batch, parameters }),
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
///
/// Ordinary graph-element streams expose public identity objects such as
/// `{"$id": 7}` rather than interpreter row state. Ranked search streams add
/// their public `$distance` or `$score` field. Rows with explicitly visible
/// bindings, paths, or sacks retain their annotated row envelope.
#[derive(Debug, Clone, PartialEq)]
pub struct QueryResponse {
    returns: BTreeMap<String, JsonValue>,
    diagnostics: PlannerDiagnostics,
}

impl QueryResponse {
    /// Convert an interpreter result into the public JSON response shape.
    pub fn from_execution_result(
        result: ExecutionResult,
    ) -> std::result::Result<Self, QueryServiceError> {
        Self::from_execution_result_with_diagnostics(result, PlannerDiagnostics::default())
    }

    /// Convert an interpreter result and its selected-plan diagnostics into a
    /// transport response. Diagnostics remain separate from the public query
    /// result JSON so transports can forward them through metadata channels.
    pub fn from_execution_result_with_diagnostics(
        result: ExecutionResult,
        diagnostics: PlannerDiagnostics,
    ) -> std::result::Result<Self, QueryServiceError> {
        let returns = result
            .returns
            .into_iter()
            .map(|(name, value)| Ok((name.into_string(), returned_value_to_json(value)?)))
            .collect::<std::result::Result<BTreeMap<_, _>, QueryServiceError>>()?;
        Ok(Self {
            returns,
            diagnostics,
        })
    }

    /// Serialize the response as JSON bytes.
    pub fn to_json_bytes(&self) -> std::result::Result<Vec<u8>, QueryServiceError> {
        sonic_rs::to_vec(self).map_err(QueryServiceError::Serialize)
    }

    /// Borrow the returned values.
    pub fn returns(&self) -> &BTreeMap<String, JsonValue> {
        &self.returns
    }

    /// Borrow the telemetry-safe diagnostics for the exact executed plan.
    pub const fn diagnostics(&self) -> &PlannerDiagnostics {
        &self.diagnostics
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

fn returned_value_to_json(
    value: ReturnedValue,
) -> std::result::Result<JsonValue, QueryServiceError> {
    match value {
        ReturnedValue::Present(value) => execution_value_to_json(value),
        ReturnedValue::EmptyList => Ok(JsonValue::Array(Vec::new())),
        ReturnedValue::EmptyObject => Ok(JsonValue::Null),
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
    match row.current.as_ref() {
        Some(current)
            if row.bindings.is_empty()
                && row.binding_virtual_properties.is_empty()
                && !row.path_visible
                && !row.sack.is_visible() =>
        {
            let id = match current {
                ElementRef::Node(id) | ElementRef::Edge(id) => *id,
            };
            let mut object = serde_json::Map::from_iter([("$id".to_string(), JsonValue::from(id))]);
            for property in ["$distance", "$score"] {
                let property = NonEmptyString::new(property)
                    .expect("public virtual property name is non-empty");
                if let Some(value) = row.virtual_properties.get(&property) {
                    object.insert(property.into_string(), property_value_to_json(value)?);
                }
            }
            return Ok(JsonValue::Object(object));
        }
        Some(_) | None => {}
    }

    let path = row.path_visible.then(|| {
        JsonValue::Array(
            row.path
                .elements()
                .iter()
                .cloned()
                .map(element_ref_to_json)
                .collect(),
        )
    });
    let sack = if row.sack.is_visible() {
        Some(
            row.sack
                .value()
                .cloned()
                .map(property_value_to_json)
                .transpose()?
                .unwrap_or(JsonValue::Null),
        )
    } else {
        None
    };
    let bindings = row
        .bindings
        .into_iter()
        .map(|(name, value)| (name.into_string(), element_ref_to_json(value)))
        .collect::<serde_json::Map<_, _>>();
    let mut object = serde_json::Map::from_iter([
        (
            "current".to_string(),
            row.current.map_or(JsonValue::Null, element_ref_to_json),
        ),
        ("bindings".to_string(), JsonValue::Object(bindings)),
    ]);
    if let Some(path) = path {
        object.insert("path".to_string(), path);
    }
    if let Some(sack) = sack {
        object.insert("sack".to_string(), sack);
    }
    Ok(JsonValue::Object(object))
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

    /// Returns true when cooperative execution observed its monotonic deadline.
    pub fn is_deadline_exceeded(&self) -> bool {
        matches!(self, Self::Db(HelixDbError::QueryDeadlineExceeded))
    }

    /// Returns true when fencing made a started commit's outcome unknowable.
    pub fn is_commit_outcome_unknown(&self) -> bool {
        matches!(
            self,
            Self::Db(HelixDbError::WriterFencedCommitOutcomeUnknown)
        )
    }

    /// Stable machine-readable code for this public query failure.
    pub fn error_code(&self) -> helix_ast::error_code::QueryErrorCode {
        match self {
            Self::InvalidRequest(_) => helix_ast::error_code::QueryErrorCode::InvalidRequest,
            Self::Planner(error) => error.error_code(),
            Self::Db(error) => error.error_code(),
            Self::JsonSerialize(_) | Self::Serialize(_) => {
                helix_ast::error_code::QueryErrorCode::ResponseSerializationError
            }
        }
    }

    /// Stable index lifecycle error code, when this failure belongs to that
    /// public compatibility surface.
    pub fn index_error_code(&self) -> Option<&'static str> {
        match self {
            Self::Db(error) => error.index_error_code(),
            Self::Planner(error) => error.index_error_code(),
            Self::InvalidRequest(_) | Self::JsonSerialize(_) | Self::Serialize(_) => None,
        }
    }
}

impl From<QueryServiceError> for HelixDbError {
    fn from(value: QueryServiceError) -> Self {
        match value {
            QueryServiceError::Db(error) => error,
            QueryServiceError::Planner(error)
                if error.error_code() == helix_ast::error_code::QueryErrorCode::IndexNotFound =>
            {
                HelixDbError::IndexNotFound(error.to_string())
            }
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
        Self {
            returns,
            diagnostics: PlannerDiagnostics::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use helix_ast::batch::{read_batch, write_batch, BatchCondition};
    use helix_ast::expr::Predicate;
    use helix_ast::graph::{EdgeRef, NodeRef};
    use helix_ast::index::IndexSpec;
    use helix_ast::query::{QueryRequest, QueryValue};
    use helix_ast::traversal::{g, ShortestPathDirection};
    use helix_ast::value::PropertyInput;
    use slatedb::object_store::memory::InMemory;

    use crate::encoding::property::property_value::PropertyValue;
    use crate::execution::interpreter::{
        ExecutionRow, ExecutionValue, FoldedStream, RowVirtualProperties,
    };
    use crate::HelixDbSource;

    fn name(value: &str) -> NonEmptyString {
        NonEmptyString::new(value).expect("test name is non-empty")
    }

    fn row(current: ElementRef) -> ExecutionRow {
        let mut row = ExecutionRow::empty();
        row.current = Some(current);
        row
    }

    fn row_with_virtual_properties(
        current: ElementRef,
        virtual_properties: RowVirtualProperties,
    ) -> ExecutionRow {
        let mut row = row(current);
        row.virtual_properties = virtual_properties;
        row
    }

    fn recommends_node_equality_index(
        diagnostics: &PlannerDiagnostics,
        label: &str,
        property: &str,
    ) -> bool {
        diagnostics.insights.iter().any(|insight| {
            matches!(
                insight,
                helix_planner::diagnostics::PlannerInsight::MissingIndex(missing)
                    if missing.element == helix_planner::catalog::ElementKind::Node
                        && missing.label.as_ref() == label
                        && missing.property.as_ref() == property
                        && missing.index_kind
                            == helix_planner::diagnostics::SecondaryIndexKind::Equality
            )
        })
    }

    #[test]
    fn query_param_bindings_reject_empty_names() {
        let err = query_param_bindings(BTreeMap::from([(String::new(), QueryValue::Bool(true))]))
            .expect_err("empty parameter name should be rejected");

        assert!(matches!(err, QueryServiceError::InvalidRequest(_)));
    }

    #[test]
    fn validated_query_preserves_closed_request_kind() {
        assert!(matches!(
            ValidatedQuery::from_request(QueryRequest::read(read_batch())).unwrap(),
            ValidatedQuery::Read { .. }
        ));
        assert!(matches!(
            ValidatedQuery::from_request(QueryRequest::write(write_batch())).unwrap(),
            ValidatedQuery::Write { .. }
        ));
    }

    #[tokio::test]
    async fn query_service_write_reuses_its_planner_catalog_refresh() {
        let db = Arc::new(
            HelixDB::open(HelixDbSource::InMemory {
                database: "query-service-prepared-catalog".to_string(),
            })
            .await
            .expect("writer should open"),
        );
        db.wait_for_startup_cache_warm().await;
        let service = HelixQueryService::new(Arc::clone(&db));
        let before = db.runtime_catalog_generation_for_tests(DataScope::LegacyUnscoped);

        service
            .execute_query(QueryRequest::write(write_batch().var_as(
                "created",
                g().add_n("User", Vec::<(&str, PropertyInput)>::new()),
            )))
            .await
            .expect("prepared write should execute");

        let after = db.runtime_catalog_generation_for_tests(DataScope::LegacyUnscoped);
        assert_eq!(after, before + 1, "planning is the request's only refresh");
    }

    #[tokio::test]
    async fn prepared_equality_lookup_uses_one_complete_point_read() {
        const EMAIL: &str = "shared@example.com";

        let db = Arc::new(
            HelixDB::open(HelixDbSource::InMemory {
                database: "query-service-one-read-equality".to_string(),
            })
            .await
            .expect("writer should open"),
        );
        db.wait_for_startup_cache_warm().await;
        db.install_index_for_tests(
            crate::config::SecondaryIndexDefinition::node_equality("User", "email")
                .expect("secondary index definition validates")
                .try_into()
                .expect("secondary index definition enters V2"),
        )
        .await
        .expect("secondary index becomes active");
        let service = HelixQueryService::new(Arc::clone(&db));
        service
            .execute_query(QueryRequest::write(write_batch().var_as(
                "created",
                g().add_n("User", vec![("email", PropertyInput::from(EMAIL))]),
            )))
            .await
            .expect("indexed node insert succeeds");
        let batch = read_batch()
            .var_as(
                "users",
                g().n_with_label_where("User", Predicate::eq("email", EMAIL)),
            )
            .returning(["users"]);

        crate::index_lifecycle::secondary::reset_equality_read_metrics();
        service
            .execute_query(QueryRequest::read(batch.clone()))
            .await
            .expect("prepared equality lookup succeeds");
        assert_eq!(
            crate::index_lifecycle::secondary::equality_read_metrics(),
            crate::index_lifecycle::secondary::SecondaryEqualityReadMetrics {
                point_reads: 1,
                multi_get_calls: 0,
                scans: 0,
                graph_reads: 0,
            }
        );

        let parallel_batch = read_batch()
            .var_as(
                "first",
                g().n_with_label_where("User", Predicate::eq("email", EMAIL)),
            )
            .var_as(
                "second",
                g().n_with_label_where("User", Predicate::eq("email", EMAIL)),
            )
            .returning(["first", "second"]);
        crate::index_lifecycle::secondary::reset_equality_read_metrics();
        service
            .execute_query(QueryRequest::read(parallel_batch))
            .await
            .expect("parallel prepared equality lookups succeed");
        assert_eq!(
            crate::index_lifecycle::secondary::equality_read_metrics().point_reads,
            2,
            "parallel contexts must retain the prepared catalog"
        );

        let prepared = db
            .planner_context_scoped_prepared(ParamBindings::default(), DataScope::LegacyUnscoped)
            .await
            .expect("read request captures one exact catalog view");
        let prepared_plan = helix_planner::planning::plan_read_batch(&batch, prepared.context())
            .expect("prepared equality lookup plans");
        db.refresh_runtime_catalog(DataScope::LegacyUnscoped)
            .await
            .expect("a concurrent catalog publication succeeds");
        crate::index_lifecycle::secondary::reset_equality_read_metrics();
        db.execute_prepared_scoped_controlled(
            &prepared_plan,
            ParamBindings::default(),
            DataScope::LegacyUnscoped,
            ExecutionControl::unlimited(),
            prepared.into_catalog_proof(),
        )
        .await
        .expect("the exact prepared read view survives a newer catalog publication");
        assert_eq!(
            crate::index_lifecycle::secondary::equality_read_metrics().point_reads,
            1
        );

        let plan = helix_planner::planning::plan_read_batch(
            &batch,
            &db.planner_context(ParamBindings::default()),
        )
        .expect("public equality lookup plans");
        crate::index_lifecycle::secondary::reset_equality_read_metrics();
        db.execute(&plan, ParamBindings::default())
            .await
            .expect("unprepared equality lookup retains its safe fallback");
        assert_eq!(
            crate::index_lifecycle::secondary::equality_read_metrics(),
            crate::index_lifecycle::secondary::SecondaryEqualityReadMetrics {
                point_reads: 2,
                multi_get_calls: 0,
                scans: 0,
                graph_reads: 0,
            }
        );
    }

    #[tokio::test]
    async fn prepared_reader_equality_lookup_reuses_its_exact_catalog_view() {
        const EMAIL: &str = "reader@example.com";

        let object_store: Arc<dyn slatedb::object_store::ObjectStore> = Arc::new(InMemory::new());
        let writer = Arc::new(
            HelixDB::open_with_object_store_for_tests(
                "query-service-reader-one-read-equality",
                Arc::clone(&object_store),
            )
            .await
            .expect("writer should initialize storage"),
        );
        writer
            .install_index_for_tests(
                crate::config::SecondaryIndexDefinition::node_equality("User", "email")
                    .expect("secondary index definition validates")
                    .try_into()
                    .expect("secondary index definition enters V2"),
            )
            .await
            .expect("secondary index becomes active");
        HelixQueryService::new(Arc::clone(&writer))
            .execute_query(QueryRequest::write(write_batch().var_as(
                "created",
                g().add_n("User", vec![("email", PropertyInput::from(EMAIL))]),
            )))
            .await
            .expect("indexed node insert succeeds");
        writer
            .flush_writer()
            .await
            .expect("writer state becomes reader-visible");

        let reader = Arc::new(
            HelixDB::open_reader_with_object_store_for_tests(
                "query-service-reader-one-read-equality",
                object_store,
            )
            .await
            .expect("reader should open"),
        );
        let service = HelixQueryService::new(Arc::clone(&reader));
        crate::index_lifecycle::secondary::reset_equality_read_metrics();
        service
            .execute_query(QueryRequest::read(
                read_batch()
                    .var_as(
                        "users",
                        g().n_with_label_where("User", Predicate::eq("email", EMAIL)),
                    )
                    .returning(["users"]),
            ))
            .await
            .expect("reader equality lookup succeeds");
        assert_eq!(
            crate::index_lifecycle::secondary::equality_read_metrics(),
            crate::index_lifecycle::secondary::SecondaryEqualityReadMetrics {
                point_reads: 1,
                multi_get_calls: 0,
                scans: 0,
                graph_reads: 0,
            }
        );

        reader.close().await.expect("reader closes");
        writer.close().await.expect("writer closes");
    }

    #[tokio::test]
    async fn overlapping_runtime_refresh_preserves_guarded_graph_write_authority() {
        let db = HelixDB::open(HelixDbSource::InMemory {
            database: "query-service-guarded-prepared-catalog".to_string(),
        })
        .await
        .expect("writer should open");
        db.wait_for_startup_cache_warm().await;
        let batch = write_batch().var_as(
            "created",
            g().add_n("User", Vec::<(&str, PropertyInput)>::new()),
        );
        let prepared = db
            .planner_context_scoped_prepared(ParamBindings::default(), DataScope::LegacyUnscoped)
            .await
            .expect("planner captures a gated catalog and read snapshot");
        let plan = helix_planner::planning::plan_write_batch(&batch, prepared.context())
            .expect("write plans");
        db.refresh_runtime_catalog(DataScope::LegacyUnscoped)
            .await
            .expect("an overlapping in-memory catalog refresh succeeds");
        let observed_generation =
            db.runtime_catalog_generation_for_tests(DataScope::LegacyUnscoped);

        crate::index_lifecycle::secondary::reset_equality_read_metrics();
        db.execute_prepared_scoped_controlled(
            &plan,
            ParamBindings::default(),
            DataScope::LegacyUnscoped,
            ExecutionControl::unlimited(),
            prepared.into_catalog_proof(),
        )
        .await
        .expect("graph write opens under its prepared authority");

        assert_eq!(
            db.runtime_catalog_generation_for_tests(DataScope::LegacyUnscoped),
            observed_generation,
            "a valid gated proof must not trigger a fallback catalog refresh"
        );
    }

    #[tokio::test]
    async fn foreign_prepared_catalog_proof_cannot_skip_refresh() {
        let source = HelixDB::open(HelixDbSource::InMemory {
            database: "query-service-foreign-proof-source".to_string(),
        })
        .await
        .expect("source writer should open");
        let target = HelixDB::open(HelixDbSource::InMemory {
            database: "query-service-foreign-proof-target".to_string(),
        })
        .await
        .expect("target writer should open");
        source.wait_for_startup_cache_warm().await;
        target.wait_for_startup_cache_warm().await;
        let batch = write_batch().var_as(
            "created",
            g().add_n("User", Vec::<(&str, PropertyInput)>::new()),
        );
        let prepared = source
            .planner_context_scoped_prepared(ParamBindings::default(), DataScope::LegacyUnscoped)
            .await
            .expect("source planner catalog refreshes");
        let plan = helix_planner::planning::plan_write_batch(&batch, prepared.context())
            .expect("write plans");
        let before = target.runtime_catalog_generation_for_tests(DataScope::LegacyUnscoped);

        target
            .execute_prepared_scoped_controlled(
                &plan,
                ParamBindings::default(),
                DataScope::LegacyUnscoped,
                ExecutionControl::unlimited(),
                prepared.into_catalog_proof(),
            )
            .await
            .expect("foreign proof safely falls back");

        assert_eq!(
            target.runtime_catalog_generation_for_tests(DataScope::LegacyUnscoped),
            before + 1
        );
    }

    #[tokio::test]
    async fn foreign_prepared_read_proof_cannot_authorize_an_index() {
        let source = HelixDB::open(HelixDbSource::InMemory {
            database: "query-service-foreign-read-proof-source".to_string(),
        })
        .await
        .expect("source writer should open");
        source
            .install_index_for_tests(
                crate::config::SecondaryIndexDefinition::node_equality("User", "email")
                    .expect("secondary index definition validates")
                    .try_into()
                    .expect("secondary index definition enters V2"),
            )
            .await
            .expect("source index becomes active");
        let batch = read_batch()
            .var_as(
                "users",
                g().n_with_label_where("User", Predicate::eq("email", "source@example.com")),
            )
            .returning(["users"]);
        let prepared = source
            .planner_context_scoped_prepared(ParamBindings::default(), DataScope::LegacyUnscoped)
            .await
            .expect("source read captures its exact catalog view");
        let plan = helix_planner::planning::plan_read_batch(&batch, prepared.context())
            .expect("source equality lookup plans");

        let target = HelixDB::open(HelixDbSource::InMemory {
            database: "query-service-foreign-read-proof-target".to_string(),
        })
        .await
        .expect("target writer should open");
        crate::index_lifecycle::secondary::reset_equality_read_metrics();
        let error = target
            .execute_prepared_scoped_controlled(
                &plan,
                ParamBindings::default(),
                DataScope::LegacyUnscoped,
                ExecutionControl::unlimited(),
                prepared.into_catalog_proof(),
            )
            .await
            .expect_err("foreign catalog authority must be discarded");
        assert!(matches!(
            error,
            HelixDbError::IndexLifecycleUnavailable {
                family: crate::error::IndexFamily::Secondary,
                reason: crate::error::IndexLifecycleUnavailableReason::CanonicalStateUnavailable,
            }
        ));
        assert_eq!(
            crate::index_lifecycle::secondary::equality_read_metrics().point_reads,
            1,
            "foreign proof must fall back to the target catalog point read"
        );
    }

    #[tokio::test]
    async fn expired_prepared_catalog_proof_cannot_skip_refresh() {
        let source = HelixDB::open(HelixDbSource::InMemory {
            database: "query-service-expired-proof-source".to_string(),
        })
        .await
        .expect("source writer should open");
        let target = HelixDB::open(HelixDbSource::InMemory {
            database: "query-service-expired-proof-target".to_string(),
        })
        .await
        .expect("target writer should open");
        source.wait_for_startup_cache_warm().await;
        target.wait_for_startup_cache_warm().await;
        let batch = write_batch().var_as(
            "created",
            g().add_n("User", Vec::<(&str, PropertyInput)>::new()),
        );
        let prepared = source
            .planner_context_scoped_prepared(ParamBindings::default(), DataScope::LegacyUnscoped)
            .await
            .expect("source planner catalog refreshes");
        let plan = helix_planner::planning::plan_write_batch(&batch, prepared.context())
            .expect("write plans");
        let proof = prepared.into_catalog_proof();
        source.close().await.expect("source writer closes");
        drop(source);
        let before = target.runtime_catalog_generation_for_tests(DataScope::LegacyUnscoped);

        target
            .execute_prepared_scoped_controlled(
                &plan,
                ParamBindings::default(),
                DataScope::LegacyUnscoped,
                ExecutionControl::unlimited(),
                proof,
            )
            .await
            .expect("expired proof safely falls back");

        assert_eq!(
            target.runtime_catalog_generation_for_tests(DataScope::LegacyUnscoped),
            before + 1
        );
    }

    #[tokio::test]
    async fn cross_scope_prepared_catalog_proof_cannot_skip_refresh() {
        let db = HelixDB::open(HelixDbSource::InMemory {
            database: "query-service-cross-scope-proof".to_string(),
        })
        .await
        .expect("writer should open");
        db.wait_for_startup_cache_warm().await;
        let batch = write_batch().var_as(
            "created",
            g().add_n("User", Vec::<(&str, PropertyInput)>::new()),
        );
        let prepared = db
            .planner_context_scoped_prepared(ParamBindings::default(), DataScope::LegacyUnscoped)
            .await
            .expect("unscoped planner catalog refreshes");
        let plan = helix_planner::planning::plan_write_batch(&batch, prepared.context())
            .expect("write plans");
        let tenant_scope = DataScope::Tenant(
            crate::encoding::keys::scope::TenantId::from_ulid_str("0000000000000000000000000A")
                .expect("tenant ULID is valid"),
        );

        db.execute_prepared_scoped_controlled(
            &plan,
            ParamBindings::default(),
            tenant_scope,
            ExecutionControl::unlimited(),
            prepared.into_catalog_proof(),
        )
        .await
        .expect("cross-scope proof safely falls back");

        assert_eq!(db.runtime_catalog_generation_for_tests(tenant_scope), 1);
    }

    #[tokio::test]
    async fn public_plan_execution_remains_unverified() {
        let db = HelixDB::open(HelixDbSource::InMemory {
            database: "public-plan-catalog-refresh".to_string(),
        })
        .await
        .expect("writer should open");
        db.wait_for_startup_cache_warm().await;
        let before = db.runtime_catalog_generation_for_tests(DataScope::LegacyUnscoped);
        let context = db
            .planner_context_scoped(ParamBindings::default(), DataScope::LegacyUnscoped)
            .await
            .expect("public planner catalog refreshes");
        let plan = helix_planner::planning::plan_write_batch(
            &write_batch().var_as(
                "created",
                g().add_n("User", Vec::<(&str, PropertyInput)>::new()),
            ),
            &context,
        )
        .expect("write plans");

        db.execute_scoped(&plan, ParamBindings::default(), DataScope::LegacyUnscoped)
            .await
            .expect("public plan executes conservatively");

        assert_eq!(
            db.runtime_catalog_generation_for_tests(DataScope::LegacyUnscoped),
            before + 2,
            "uncoupled public planning and execution each refresh"
        );
    }

    #[test]
    fn query_service_rejects_parameter_value_that_disagrees_with_declared_type() {
        let error = QueryRequest::read(read_batch())
            .with_typed_parameter(
                "limit",
                helix_ast::query::QueryParamType::Bool,
                QueryValue::I64(1),
            )
            .expect_err("an i64 parameter must not satisfy a bool declaration");

        assert!(matches!(
            error,
            helix_ast::query::QueryError::ParameterTypeMismatch { .. }
        ));
    }

    #[tokio::test]
    async fn query_service_rejects_write_payload_disguised_as_read() {
        let db = Arc::new(
            HelixDB::open(HelixDbSource::InMemory {
                database: "query-service-mismatched-read-envelope".to_string(),
            })
            .await
            .expect("writer should open"),
        );
        let service = HelixQueryService::new(db);
        let write = QueryRequest::write(
            write_batch()
                .var_as(
                    "created",
                    g().add_n("Forbidden", Vec::<(&str, PropertyInput)>::new()),
                )
                .returning(["created"]),
        )
        .to_json_string()
        .expect("write request should serialize");
        let disguised = write.replacen(r#""request_type":"write""#, r#""request_type":"read""#, 1);
        sonic_rs::from_str::<QueryRequest>(&disguised)
            .expect_err("a read envelope must reject a write payload");

        let response = service
            .execute_query(QueryRequest::read(
                read_batch()
                    .var_as("count", g().n_with_label("Forbidden").count())
                    .returning(["count"]),
            ))
            .await
            .expect("postcondition read should execute");
        assert_eq!(response.returns().get("count"), Some(&JsonValue::from(0)));
    }

    #[tokio::test]
    async fn query_service_rejects_mutation_ast_inside_read_batch() {
        let db = Arc::new(
            HelixDB::open(HelixDbSource::InMemory {
                database: "query-service-read-batch-mutation".to_string(),
            })
            .await
            .expect("writer should open"),
        );
        let service = HelixQueryService::new(db);
        let write = write_batch()
            .var_as(
                "created",
                g().add_n("Forbidden", Vec::<(&str, PropertyInput)>::new()),
            )
            .returning(["created"]);
        let write = QueryRequest::write(write)
            .to_json_string()
            .expect("write request should serialize");
        let disguised = write
            .replacen(r#""request_type":"write""#, r#""request_type":"read""#, 1)
            .replacen(r#""write":{"#, r#""read":{"#, 1);
        sonic_rs::from_str::<QueryRequest>(&disguised)
            .expect_err("a read batch must reject a mutation traversal");

        let response = service
            .execute_query(QueryRequest::read(
                read_batch()
                    .var_as("count", g().n_with_label("Forbidden").count())
                    .returning(["count"]),
            ))
            .await
            .expect("postcondition read should execute");
        assert_eq!(response.returns().get("count"), Some(&JsonValue::from(0)));
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
        assert!(value.get("diagnostics").is_none());
    }

    #[tokio::test]
    async fn query_response_carries_executed_plan_diagnostics_outside_result_json() {
        const SECRET_LITERAL: &str = "secret-query-value";

        let db = Arc::new(
            HelixDB::open(HelixDbSource::InMemory {
                database: "query-service-planner-diagnostics".to_string(),
            })
            .await
            .expect("writer should open"),
        );
        let service = HelixQueryService::new(db);
        let request = QueryRequest::read(
            read_batch()
                .var_as(
                    "users",
                    g().n_with_label_where("User", Predicate::eq("username", SECRET_LITERAL)),
                )
                .var_as("count", g().n(NodeRef::var("users")).count())
                .returning(["count"]),
        );
        let observation = QueryObservation::capture(
            &request,
            Some(query::TenantId::new("tenant-1").expect("tenant ID")),
        )
        .expect("query observation");

        let response = service
            .execute_query(request)
            .await
            .expect("diagnostic query should execute");

        assert_eq!(response.returns().get("count"), Some(&JsonValue::from(0)));
        let missing_index = response
            .diagnostics()
            .insights
            .iter()
            .find_map(|insight| match insight {
                helix_planner::diagnostics::PlannerInsight::MissingIndex(insight) => Some(insight),
                helix_planner::diagnostics::PlannerInsight::UnboundedScan(_)
                | helix_planner::diagnostics::PlannerInsight::DeepTraversal(_) => None,
            })
            .expect("selected residual filter should recommend an index");
        assert_eq!(missing_index.label.to_string(), "User");
        assert_eq!(missing_index.property.to_string(), "username");
        let unbounded_scan = response
            .diagnostics()
            .insights
            .iter()
            .find_map(|insight| match insight {
                helix_planner::diagnostics::PlannerInsight::UnboundedScan(insight) => Some(insight),
                helix_planner::diagnostics::PlannerInsight::MissingIndex(_)
                | helix_planner::diagnostics::PlannerInsight::DeepTraversal(_) => None,
            })
            .expect("selected residual filter should retain its unbounded scan facts");
        assert_eq!(
            unbounded_scan.element,
            helix_planner::catalog::ElementKind::Node
        );
        assert_eq!(unbounded_scan.label.as_ref().unwrap().as_ref(), "User");
        assert_eq!(
            unbounded_scan
                .predicate_properties
                .iter()
                .map(AsRef::as_ref)
                .collect::<Vec<_>>(),
            ["username"]
        );

        let diagnostics_json = serde_json::to_string(response.diagnostics())
            .expect("diagnostics should serialize for transports");
        assert!(!diagnostics_json.contains(SECRET_LITERAL));

        let public_json = response.to_json_bytes().expect("response should serialize");
        let public_json = String::from_utf8(public_json).expect("response should be utf-8");
        assert_eq!(public_json, r#"{"count":0}"#);
        assert!(!public_json.contains("diagnostics"));

        let telemetry_event = observation
            .event(&Ok(response), std::time::Duration::from_micros(42))
            .into_telemetry()
            .expect("telemetry event");
        assert!(telemetry_event
            .properties
            .get("planner_diagnostics")
            .is_some());
        let telemetry_scan = telemetry_event.properties["planner_diagnostics"]["insights"]
            .as_array()
            .expect("planner insights are a JSON array")
            .iter()
            .find(|insight| insight["type"] == "unbounded_scan")
            .expect("telemetry carries the selected unbounded scan");
        assert_eq!(telemetry_scan["details"]["element"], "node");
        assert_eq!(telemetry_scan["details"]["label"], "User");
        assert_eq!(
            telemetry_scan["details"]["predicate_properties"],
            serde_json::json!(["username"])
        );
        assert!(!telemetry_event
            .properties
            .to_string()
            .contains(SECRET_LITERAL));
        assert_eq!(telemetry_event.properties["tenant_id"], "tenant-1");
        for sensitive in ["returned_rows", "parameters", "embeddings", "email"] {
            assert!(telemetry_event.properties.get(sensitive).is_none());
        }
    }

    #[tokio::test]
    async fn write_batch_drop_does_not_reanalyze_post_commit_catalog() {
        const SECRET_LITERAL: &str = "private@example.com";

        let db = Arc::new(
            HelixDB::open(HelixDbSource::InMemory {
                database: "query-diagnostics-write-drop-snapshot".to_string(),
            })
            .await
            .expect("writer should open"),
        );
        db.install_index_for_tests(
            crate::config::SecondaryIndexDefinition::node_equality("User", "email")
                .expect("secondary index definition is valid")
                .try_into()
                .expect("secondary index definition enters V2"),
        )
        .await
        .expect("secondary index becomes active");
        let service = HelixQueryService::new(Arc::clone(&db));
        let filter = || g().n_with_label_where("User", Predicate::eq("email", SECRET_LITERAL));
        let request = QueryRequest::write(
            write_batch()
                .var_as("users", filter())
                .var_as(
                    "dropped",
                    g().drop_index(IndexSpec::node_equality("User", "email")),
                )
                .returning(["users"]),
        );

        let response = service
            .execute_query(request)
            .await
            .expect("indexed read and following drop should execute");

        assert_eq!(
            response
                .diagnostics()
                .statistics
                .node_accesses
                .equality_index_lookups,
            1
        );
        assert!(!recommends_node_equality_index(
            response.diagnostics(),
            "User",
            "email"
        ));

        let refreshed = db
            .planner_context_scoped(ParamBindings::default(), DataScope::LegacyUnscoped)
            .await
            .expect("post-drop catalog refreshes");
        let post_drop = helix_planner::planning::plan_read_batch_with_diagnostics(
            &read_batch().var_as("users", filter()).returning(["users"]),
            &refreshed,
        )
        .expect("post-drop query replans");
        assert!(recommends_node_equality_index(
            post_drop.diagnostics(),
            "User",
            "email"
        ));

        let public_json = response
            .to_json_bytes()
            .expect("public response should serialize");
        assert_eq!(public_json, br#"{"users":[]}"#);
        assert!(!public_json
            .windows(SECRET_LITERAL.len())
            .any(|window| window == SECRET_LITERAL.as_bytes()));
    }

    #[tokio::test]
    async fn captured_planning_output_survives_catalog_activation() {
        const SECRET_LITERAL: &str = "private@example.com";

        let db = HelixDB::open(HelixDbSource::InMemory {
            database: "query-diagnostics-concurrent-create-snapshot".to_string(),
        })
        .await
        .expect("writer should open");
        let batch = BatchQuery::Read(
            read_batch()
                .var_as(
                    "users",
                    g().n_with_label_where("User", Predicate::eq("email", SECRET_LITERAL)),
                )
                .returning(["users"]),
        );
        let context = db
            .planner_context_scoped(ParamBindings::default(), DataScope::LegacyUnscoped)
            .await
            .expect("initial catalog snapshot loads");
        let planning = helix_planner::planning::plan_with_diagnostics(&batch, &context)
            .expect("query plans without an index");
        let expected_diagnostics = planning.diagnostics().clone();
        assert!(recommends_node_equality_index(
            &expected_diagnostics,
            "User",
            "email"
        ));

        db.install_index_for_tests(
            crate::config::SecondaryIndexDefinition::node_equality("User", "email")
                .expect("secondary index definition is valid")
                .try_into()
                .expect("secondary index definition enters V2"),
        )
        .await
        .expect("concurrent secondary index becomes active");
        let refreshed = db
            .planner_context_scoped(ParamBindings::default(), DataScope::LegacyUnscoped)
            .await
            .expect("active catalog refreshes");
        let current = helix_planner::planning::plan_with_diagnostics(&batch, &refreshed)
            .expect("query replans with the active index");
        assert!(!recommends_node_equality_index(
            current.diagnostics(),
            "User",
            "email"
        ));

        let result = db
            .execute_scoped_controlled(
                planning.plan(),
                ParamBindings::default(),
                DataScope::LegacyUnscoped,
                ExecutionControl::unlimited(),
            )
            .await
            .expect("captured scan plan executes after index activation");
        let (_, diagnostics) = planning.into_parts();
        let response = QueryResponse::from_execution_result_with_diagnostics(result, diagnostics)
            .expect("captured diagnostics enter the response");

        assert_eq!(response.diagnostics(), &expected_diagnostics);
        assert!(recommends_node_equality_index(
            response.diagnostics(),
            "User",
            "email"
        ));
        let public_json = response
            .to_json_bytes()
            .expect("public response should serialize");
        assert_eq!(public_json, br#"{"users":[]}"#);
        assert!(!public_json
            .windows(SECRET_LITERAL.len())
            .any(|window| window == SECRET_LITERAL.as_bytes()));
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
    async fn query_response_normalizes_graph_element_streams() {
        let db = HelixDB::open(HelixDbSource::InMemory {
            database: "query-service-normalized-graph-elements".to_string(),
        })
        .await
        .expect("writer should open");

        let write_users = write_batch()
            .var_as(
                "alice",
                g().add_n("User", vec![("name", PropertyInput::from("Alice"))]),
            )
            .var_as(
                "bob",
                g().add_n("User", vec![("name", PropertyInput::from("Bob"))]),
            )
            .var_as(
                "follow",
                g().n(NodeRef::var("alice")).add_e(
                    "FOLLOWS",
                    NodeRef::var("bob"),
                    vec![("since", PropertyInput::from("2026-07-24"))],
                ),
            )
            .var_as(
                "friends",
                g().n(NodeRef::var("alice"))
                    .out(Some("FOLLOWS"))
                    .value_map(Some(vec!["$id", "name"])),
            )
            .returning(["alice", "bob", "friends"]);

        assert_eq!(
            db.query(QueryRequest::write(write_users))
                .await
                .expect("user graph write should execute"),
            serde_json::json!({
                "alice": [{ "$id": 0 }],
                "bob": [{ "$id": 1 }],
                "friends": [{ "$id": 1, "name": "Bob" }],
            })
        );

        let mutations = write_batch()
            .var_as(
                "added_edge",
                g().n(NodeRef::id(1)).add_e(
                    "FOLLOWS",
                    NodeRef::id(0),
                    Vec::<(&str, PropertyInput)>::new(),
                ),
            )
            .var_as(
                "updated_node",
                g().n(NodeRef::id(0)).set_property("active", true),
            )
            .var_as(
                "updated_edge",
                g().e(EdgeRef::id(0)).remove_property("since"),
            )
            .returning(["added_edge", "updated_node", "updated_edge"]);

        assert_eq!(
            db.query(QueryRequest::write(mutations))
                .await
                .expect("graph mutations should execute"),
            serde_json::json!({
                "added_edge": [{ "$id": 1 }],
                "updated_node": [{ "$id": 0 }],
                "updated_edge": [{ "$id": 0 }],
            })
        );

        let reads = read_batch()
            .var_as("node", g().n(NodeRef::id(0)))
            .var_as("edge", g().e(EdgeRef::id(0)))
            .var_as("folded", g().n(NodeRef::id(1)).fold())
            .returning(["node", "edge", "folded"]);

        assert_eq!(
            db.query(QueryRequest::read(reads))
                .await
                .expect("raw graph reads should execute"),
            serde_json::json!({
                "node": [{ "$id": 0 }],
                "edge": [{ "$id": 0 }],
                "folded": [{ "$id": 1 }],
            })
        );
    }

    #[tokio::test]
    async fn query_response_uses_planner_shape_only_for_empty_returns() {
        let db = Arc::new(
            HelixDB::open(HelixDbSource::InMemory {
                database: "query-service-empty-return-shapes".to_string(),
            })
            .await
            .expect("writer should open"),
        );
        let service = HelixQueryService::new(db);

        let created = service
            .execute_query(QueryRequest::write(
                write_batch()
                    .var_as(
                        "created",
                        g().add_n("User", Vec::<(&str, PropertyInput)>::new()),
                    )
                    .returning(["created"]),
            ))
            .await
            .expect("fixture write should execute");
        assert_eq!(
            created.to_json_bytes().unwrap(),
            br#"{"created":[{"$id":0}]}"#
        );

        let response = service
            .execute_query(
                QueryRequest::read(
                    read_batch()
                        .var_as("present_object", g().n(NodeRef::id(0)))
                        .var_as("present_list", g().n_with_label("User"))
                        .var_as("missing_object", g().n(NodeRef::id(999)))
                        .var_as("missing_list", g().n_with_label("Missing"))
                        .var_as(
                            "empty_membership",
                            g().n_with_label("User")
                                .where_(Predicate::is_in_param("orbit_id", "orbit_ids")),
                        )
                        .var_as("bounded_object", g().n_with_label("Missing").limit(1usize))
                        .var_as("empty_fold", g().n(NodeRef::id(999)).fold())
                        .var_as("seed", g().n_with_label("Missing"))
                        .var_as_if(
                            "skipped_object",
                            BatchCondition::PrevNotEmpty,
                            g().n(NodeRef::id(0)),
                        )
                        .var_as("count", g().n_with_label("Missing").count())
                        .returning([
                            "present_object",
                            "missing_object",
                            "missing_list",
                            "empty_membership",
                            "bounded_object",
                            "empty_fold",
                            "skipped_object",
                            "count",
                            "present_list",
                        ]),
                )
                .with_parameter_value("orbit_ids", QueryValue::Array(Vec::new())),
            )
            .await
            .expect("shape read should execute");

        assert_eq!(
            response.to_json_bytes().unwrap(),
            br#"{"bounded_object":null,"count":0,"empty_fold":[],"empty_membership":[],"missing_list":[],"missing_object":null,"present_list":[{"$id":0}],"present_object":[{"$id":0}],"skipped_object":null}"#
        );

        let mutation = service
            .execute_query(QueryRequest::write(
                write_batch()
                    .var_as("missing_mutation", g().n(NodeRef::id(999)).drop())
                    .returning(["missing_mutation"]),
            ))
            .await
            .expect("empty mutation should remain successful");
        assert_eq!(
            mutation.to_json_bytes().unwrap(),
            br#"{"missing_mutation":[]}"#
        );

        let empty_returns = service
            .execute_query(QueryRequest::read(
                read_batch().var_as("ignored", g().n(NodeRef::id(999))),
            ))
            .await
            .expect("empty return declaration should execute");
        assert_eq!(empty_returns.to_json_bytes().unwrap(), br#"{}"#);
    }

    #[tokio::test]
    async fn query_response_uses_the_shape_of_the_binding_that_executed() {
        let db = Arc::new(
            HelixDB::open(HelixDbSource::InMemory {
                database: "query-service-runtime-return-shape".to_string(),
            })
            .await
            .expect("writer should open"),
        );
        let service = HelixQueryService::new(db);
        let created = service
            .execute_query(QueryRequest::write(
                write_batch()
                    .var_as(
                        "created",
                        g().add_n("User", Vec::<(&str, PropertyInput)>::new()),
                    )
                    .returning(["created"]),
            ))
            .await
            .expect("fixture write should execute");
        assert_eq!(
            created.to_json_bytes().unwrap(),
            br#"{"created":[{"$id":0}]}"#
        );

        let empty_scalar_batch = read_batch()
            .var_as("result", g().n_with_label("Missing").count())
            .for_each_param(
                "items",
                read_batch().var_as("result", g().n(NodeRef::id(999))),
            )
            .returning(["result"]);

        let skipped = service
            .execute_query(
                QueryRequest::read(empty_scalar_batch.clone())
                    .with_parameter_value("items", QueryValue::Array(Vec::new())),
            )
            .await
            .expect("empty for_each should preserve the earlier binding");
        assert_eq!(skipped.to_json_bytes().unwrap(), br#"{"result":0}"#);

        let executed = service
            .execute_query(QueryRequest::read(empty_scalar_batch).with_parameter_value(
                "items",
                QueryValue::Array(vec![QueryValue::Object(BTreeMap::new())]),
            ))
            .await
            .expect("non-empty for_each should replace the earlier binding");
        assert_eq!(executed.to_json_bytes().unwrap(), br#"{"result":null}"#);

        let present_scalar = service
            .execute_query(
                QueryRequest::read(
                    read_batch()
                        .var_as("result", g().n_with_label("User").count())
                        .for_each_param(
                            "items",
                            read_batch().var_as("result", g().n(NodeRef::id(999))),
                        )
                        .returning(["result"]),
                )
                .with_parameter_value("items", QueryValue::Array(Vec::new())),
            )
            .await
            .expect("empty for_each should not rewrite a non-empty scalar");
        assert_eq!(present_scalar.to_json_bytes().unwrap(), br#"{"result":1}"#);
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
        let _writer = HelixDB::open_with_object_store_for_tests(
            "query-service-reader-mode",
            Arc::clone(&object_store),
        )
        .await
        .expect("writer should initialize storage");
        let reader = HelixDB::open_reader_with_object_store_for_tests(
            "query-service-reader-mode",
            object_store,
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
            batch: ReadBatch::new(),
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
                    ReturnedValue::Present(ExecutionValue::Scalars(vec![ExecutionScalar::Object(
                        BTreeMap::from([(
                            "name".to_string(),
                            PropertyValue::String("alice".to_string()),
                        )]),
                    )])),
                ),
                (
                    name("rows"),
                    ReturnedValue::Present(ExecutionValue::Stream(vec![{
                        let mut row = ExecutionRow::empty();
                        row.current = Some(ElementRef::Node(7));
                        row.bindings = BTreeMap::from([(name("friend"), ElementRef::Edge(9))]);
                        row
                    }])),
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
    fn execution_result_serializes_plain_and_ranked_rows_as_public_elements() {
        let distance = name("$distance");
        let score = name("$score");
        let result = ExecutionResult {
            last: None,
            variables: BTreeMap::new(),
            returns: BTreeMap::from([
                (
                    name("node"),
                    ReturnedValue::Present(ExecutionValue::Stream(vec![row(ElementRef::Node(7))])),
                ),
                (
                    name("ranked_edge"),
                    ReturnedValue::Present(ExecutionValue::Stream(vec![
                        row_with_virtual_properties(
                            ElementRef::Edge(9),
                            RowVirtualProperties::from_one(distance, PropertyValue::F64(0.25)),
                        ),
                    ])),
                ),
                (
                    name("scored_node"),
                    ReturnedValue::Present(ExecutionValue::Stream(vec![
                        row_with_virtual_properties(
                            ElementRef::Node(11),
                            RowVirtualProperties::from_one(score, PropertyValue::F64(1.5)),
                        ),
                    ])),
                ),
            ]),
        };

        let response =
            QueryResponse::from_execution_result(result).expect("execution result serializes");

        assert_eq!(
            response.returns()["node"],
            serde_json::json!([{ "$id": 7 }])
        );
        assert_eq!(
            response.returns()["ranked_edge"],
            serde_json::json!([{ "$id": 9, "$distance": 0.25 }])
        );
        assert_eq!(
            response.returns()["scored_node"],
            serde_json::json!([{ "$id": 11, "$score": 1.5 }])
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
                    ReturnedValue::Present(ExecutionValue::FoldedStream(FoldedStream::new(vec![
                        folded_row,
                    ]))),
                ),
                (
                    name("exists"),
                    ReturnedValue::Present(ExecutionValue::Bool(true)),
                ),
                (
                    name("scalars"),
                    ReturnedValue::Present(ExecutionValue::Scalars(vec![
                        ExecutionScalar::String("ready".to_string()),
                        ExecutionScalar::Value(PropertyValue::I64(7)),
                    ])),
                ),
            ]),
        };

        let response = QueryResponse::from_execution_result(result).expect("values serialize");

        assert_eq!(
            response.returns()["folded"],
            serde_json::json!([{ "$id": 11 }])
        );
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

    #[test]
    fn query_observation_excludes_runtime_parameters_and_maps_failures() {
        let request = QueryRequest::read(read_batch().var_as(
            "users",
            g().n_with_label_where("User", Predicate::eq("email", "secret@example.com")),
        ))
        .with_parameter_value("secret", QueryValue::String("do-not-send".to_owned()));
        let observation = QueryObservation::capture(&request, None).expect("canonical query");
        assert!(!observation.raw_query.as_str().contains("secret"));
        assert!(!observation.raw_query.as_str().contains("do-not-send"));
        assert!(!observation
            .raw_query
            .as_str()
            .contains("secret@example.com"));
        assert!(observation
            .raw_query
            .as_str()
            .contains("\"property\":\"email\""));
        assert_eq!(observation.query_type, query::QueryType::Read);

        assert_eq!(
            query_error_type(&QueryServiceError::InvalidRequest("invalid".to_owned())),
            query::QueryErrorType::InvalidRequest
        );
        assert_eq!(
            query_error_type(&QueryServiceError::Db(HelixDbError::TransactionConflict(
                "conflict".to_owned()
            ))),
            query::QueryErrorType::Conflict
        );
        let invalid_vector_inputs = [
            HelixDbError::InvalidDimension {
                expected: 3,
                got: 2,
            },
            HelixDbError::InvalidVectorComponent { index: 1 },
            HelixDbError::VectorComponentMagnitudeExceeded {
                metric: crate::search::vector::VectorDistanceMetric::Manhattan,
                dimension: 3,
                component_index: 1,
                observed_magnitude: 4.0,
                inclusive_maximum: 3.0,
            },
            HelixDbError::ZeroNormCosineVector,
        ];
        for error in invalid_vector_inputs {
            assert_eq!(
                query_error_type(&QueryServiceError::Db(error)),
                query::QueryErrorType::InvalidRequest
            );
        }
        assert_eq!(
            query_error_type(&QueryServiceError::Db(HelixDbError::InvalidVectorItem(
                crate::search::vector::VectorItemDecodeError::HeaderMismatch
            ))),
            query::QueryErrorType::Execution
        );
        assert_eq!(
            query_error_type(&QueryServiceError::Db(HelixDbError::Query(
                "execution".to_owned()
            ))),
            query::QueryErrorType::Execution
        );

        let failed = observation.event(
            &Err(QueryServiceError::Db(
                HelixDbError::UniqueConstraintViolation {
                    label: "User".to_owned(),
                    property: "email".to_owned(),
                    value: "\"secret@example.com\"".to_owned(),
                    existing_node_id: 1,
                    attempted_node_id: 2,
                },
            )),
            std::time::Duration::from_micros(1),
        );
        let encoded = serde_json::to_string(
            &failed
                .into_telemetry()
                .expect("failed telemetry event")
                .properties,
        )
        .expect("telemetry JSON");
        assert!(!encoded.contains("secret@example.com"));
        assert!(encoded.contains("query execution failed"));
    }
}
