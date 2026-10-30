//! Stable machine-readable query error codes.

use serde::{Deserialize, Serialize};

/// A stable machine-readable failure returned by a Helix query boundary.
///
/// Display messages may gain context over time. These codes are the compatibility
/// contract that callers should use for branching and retry decisions.
///
/// # Examples
///
/// ```
/// use helix_ast::error_code::QueryErrorCode;
///
/// let code = "index_not_found".parse::<QueryErrorCode>()?;
/// assert_eq!(code, QueryErrorCode::IndexNotFound);
/// assert_eq!(code.as_str(), "index_not_found");
/// # Ok::<(), helix_ast::error_code::UnknownQueryErrorCode>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryErrorCode {
    /// The request is incompatible with the selected query mode.
    InvalidRequest,
    /// The query body is not valid query JSON.
    InvalidQueryJson,
    /// The transport could not read the request body.
    InvalidRequestBody,
    /// A transport option or header is invalid for the request.
    InvalidRequestOption,
    /// An index lifecycle operation ID is invalid.
    InvalidIndexOperationId,
    /// Planning failed because an internal planner contract was violated.
    InternalPlannerError,
    /// An all-edges reference was used as a finite mutation target.
    UnsupportedEdgeAllTarget,
    /// An index expression was not a literal or parameter.
    NonLiteralIndexExpression,
    /// A parameter required to select an equality algorithm was not bound.
    MissingPlanningEqualityParameter,
    /// A bound equality parameter cannot be represented by an index literal.
    UnsupportedPlanningEqualityParameter,
    /// A search tenant was supplied for an unscoped index.
    InvalidSearchTenant,
    /// A search tenant value has the wrong shape.
    InvalidSearchTenantValue,
    /// A search result count is zero.
    InvalidSearchResultCount,
    /// A search result-count expression has the wrong shape.
    InvalidSearchResultCountExpression,
    /// A search query input has the wrong shape.
    InvalidSearchInput,
    /// A batch condition minimum size is zero.
    InvalidBatchConditionMinSize,
    /// The first batch entry requires a previous result.
    InvalidInitialBatchCondition,
    /// A mutation assigns the same property more than once.
    DuplicatePropertyAssignment,
    /// A projection selects the same property more than once.
    DuplicatePropertySelection,
    /// A projection emits the same alias more than once.
    DuplicateProjectionAlias,
    /// A batch returns the same variable more than once.
    DuplicateReturnVariable,
    /// A concrete point lookup contains the same element ID more than once.
    DuplicateElementId,
    /// A sort contains the same property more than once.
    DuplicateOrderKey,
    /// A sub-traversal context is missing its parent input.
    UnboundContext,
    /// An operation is not valid inside a branch or repeat sub-traversal.
    InvalidSubTraversalOperation,
    /// An operation is not valid after a row-local bind.
    InvalidAfterBindOperation,
    /// A write batch contains a read-only traversal.
    ReadOnlyTraversalInWriteBatch,
    /// A branch has too few traversals.
    InvalidBranchArity,
    /// A batch has too few entries.
    InvalidBatchArity,
    /// A repeat emit predicate is incompatible with its emit mode.
    InvalidRepeatEmit,
    /// A repeat count or depth is zero.
    InvalidRepeatCount,
    /// A shortest-path count or depth is zero.
    InvalidShortestPathCount,
    /// An order operation has no sort keys.
    InvalidOrderKeys,
    /// A projection has too few fields.
    InvalidProjectionArity,
    /// A stream range is statically inverted.
    InvalidStreamRange,
    /// A stream-bound expression has the wrong shape.
    InvalidStreamBoundExpression,
    /// A required query name is empty.
    InvalidEmptyName,
    /// A predicate set has too few children.
    InvalidPredicateArity,
    /// Storage failed outside a retryable transaction conflict.
    StorageError,
    /// A retryable transaction conflict prevented the query from committing.
    TransactionConflict,
    /// A standalone reader changed views while the request was executing.
    RequestReadViewChanged,
    /// The query exceeded its execution deadline.
    QueryDeadlineExceeded,
    /// Retaining a query buffer would exceed the request's memory budget.
    QueryMemoryLimitExceeded,
    /// Reader retirement cancelled an admitted read before completion.
    QueryCancelledByReaderRetirement,
    /// A supplied node ID is invalid.
    InvalidNodeId,
    /// A requested node does not exist.
    NodeNotFound,
    /// A requested edge does not exist.
    EdgeNotFound,
    /// The database handle is closed.
    DatabaseClosed,
    /// Database configuration is invalid.
    InvalidConfiguration,
    /// The required index lifecycle authority is unavailable.
    IndexLifecycleUnavailable,
    /// Explicit secondary stepping requires disabled worker mode.
    SecondaryLifecycleSteppingRequiresDisabledMode,
    /// Explicit migration stepping requires disabled worker mode.
    MigrationSteppingRequiresDisabledMode,
    /// An active text mutation exceeded an admission limit.
    ActiveTextMutationLimitExceeded,
    /// A vector/text index has too much committed but unpublished work.
    ///
    /// The whole request was rejected (a write before commit, or a strong
    /// search whose answer lies behind too many results superseded by
    /// committed but unpublished work) and is safe to retry after the index
    /// worker publishes outstanding operations.
    IndexBackpressure,
    /// A write would saturate the queued work of a blocked hidden index
    /// build. Its generation publishes nothing until the build activates, so
    /// retrying the same write cannot succeed until the blocked build
    /// operation is retried (after repairing what its blocker names) or
    /// aborted.
    IndexBuildBlocked,
    /// One transaction staged more queued index work than a single transaction
    /// may carry (an operand above the durable write-ahead-log entry limit, or
    /// a per-index backlog limit exceeded on its own); retrying the same write
    /// cannot succeed.
    IndexOperationBatchTooLarge,
    /// Existing graph data cannot satisfy an index source contract.
    InvalidIndexSourceData,
    /// A value cannot satisfy the current index model.
    InvalidIndexModel,
    /// A value cannot satisfy a secondary-index contract.
    InvalidSecondaryIndexValue,
    /// Existing storage requires an explicit migration.
    MigrationRequired,
    /// Existing storage must be opened and migrated by a writer.
    WriterMigrationRequired,
    /// The stored index format is newer than this binary supports.
    UnsupportedIndexStorageVersion,
    /// A bounded identifier namespace is exhausted.
    IdentifierExhausted,
    /// The logical index ID namespace is exhausted.
    IndexIdExhausted,
    /// The vector physical index ID namespace is exhausted.
    VectorPhysicalIdExhausted,
    /// The index generation namespace is exhausted.
    IndexGenerationExhausted,
    /// The index revision namespace is exhausted.
    IndexRevisionExhausted,
    /// The index-operation revision namespace is exhausted.
    IndexOperationRevisionExhausted,
    /// A retained handle refers to a stale index generation.
    StaleIndexGeneration,
    /// Writer fencing made the final commit outcome unknowable.
    WriterFencedCommitOutcomeUnknown,
    /// Vector index configuration is invalid.
    InvalidVectorConfiguration,
    /// A query payload or encoded query is invalid.
    InvalidQuery,
    /// The query reads a parameter the request did not bind.
    UnboundParameter,
    /// The operation requires a writer database handle.
    WriterModeRequired,
    /// The operation requires a standalone reader database handle.
    ReaderModeRequired,
    /// An index with the requested identity already exists.
    IndexAlreadyExists,
    /// An existing index has a conflicting definition.
    IndexDefinitionConflict,
    /// The index is already changing lifecycle state.
    IndexBusy,
    /// The requested index operation does not exist.
    IndexOperationNotFound,
    /// The requested index operation cannot be aborted.
    IndexOperationNotAbortable,
    /// The requested logical or physical index does not exist.
    IndexNotFound,
    /// A unique index already owns the requested value.
    UniqueConstraintViolation,
    /// A unique index does not support the supplied value type.
    UnsupportedUniqueIndexValueType,
    /// A vector has the wrong dimension.
    InvalidVectorDimension,
    /// A vector contains a non-finite component.
    InvalidVectorComponent,
    /// A vector component exceeds its score-safe magnitude.
    VectorComponentMagnitudeExceeded,
    /// A cosine vector has zero norm.
    ZeroNormCosineVector,
    /// A response could not be serialized.
    ResponseSerializationError,
    /// A non-actionable internal invariant or persisted-data contract failed.
    InternalError,
}

/// How every transport reports a [`QueryErrorCode`].
///
/// Each code has exactly one class ([`QueryErrorCode::failure_class`]), and the
/// class alone decides the HTTP status, the gRPC code, the `retryable` hint and
/// whether a failed write is proven to have committed nothing. Transports match
/// on the class rather than on codes, so a new code cannot reach a client with
/// a status chosen by a fallthrough.
///
/// ```
/// use helix_ast::error_code::{QueryErrorCode, QueryFailureClass};
/// let class = QueryErrorCode::UniqueConstraintViolation.failure_class();
/// assert_eq!(class, QueryFailureClass::StateConflict);
/// assert_eq!(class.http_status(), 409);
/// assert_eq!(class.retryable(), Some(false));
/// assert!(class.rejects_before_commit());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum QueryFailureClass {
    /// The request or the data it carries is invalid; resending it unchanged
    /// fails again.
    InvalidRequest,
    /// Query planning rejected the request before execution started.
    Planning,
    /// The request names a node, edge, index or index operation that does not
    /// exist.
    NotFound,
    /// Concurrent work aborted the request: a transaction conflict, a changed
    /// reader view or a stale index generation. An unchanged retry may succeed.
    Conflict,
    /// The request conflicts with the database's current state, such as a
    /// unique value another node already owns or an index that is busy.
    /// Resending it unchanged fails again until that state changes.
    StateConflict,
    /// Asynchronous index work is saturated. An unchanged retry may succeed
    /// once outstanding work is published.
    Backpressure,
    /// The request exceeded its own resource budget.
    ResourceLimit,
    /// The request ran past its deadline. A write may have committed.
    Timeout,
    /// This database handle cannot serve the request: it is the wrong mode,
    /// closed, retiring or not yet migrated.
    Unavailable,
    /// A started commit's outcome cannot be proven either way.
    CommitOutcomeUnknown,
    /// The database failed internally. A write may have committed.
    Internal,
}

impl QueryFailureClass {
    /// HTTP status every Helix HTTP endpoint returns for this class.
    #[must_use]
    pub const fn http_status(self) -> u16 {
        match self {
            Self::InvalidRequest | Self::Planning => 400,
            Self::NotFound => 404,
            Self::Timeout => 408,
            Self::Conflict | Self::StateConflict => 409,
            Self::Backpressure | Self::ResourceLimit => 429,
            Self::Internal => 500,
            Self::Unavailable | Self::CommitOutcomeUnknown => 503,
        }
    }

    /// Canonical gRPC status code number every Helix gRPC service returns for
    /// this class.
    ///
    /// ```
    /// use helix_ast::error_code::QueryFailureClass;
    /// // INVALID_ARGUMENT
    /// assert_eq!(QueryFailureClass::InvalidRequest.grpc_code(), 3);
    /// ```
    #[must_use]
    pub const fn grpc_code(self) -> i32 {
        const INVALID_ARGUMENT: i32 = 3;
        const DEADLINE_EXCEEDED: i32 = 4;
        const NOT_FOUND: i32 = 5;
        const RESOURCE_EXHAUSTED: i32 = 8;
        const FAILED_PRECONDITION: i32 = 9;
        const ABORTED: i32 = 10;
        const INTERNAL: i32 = 13;
        const UNAVAILABLE: i32 = 14;
        match self {
            Self::InvalidRequest | Self::Planning => INVALID_ARGUMENT,
            Self::NotFound => NOT_FOUND,
            Self::Conflict => ABORTED,
            // Retrying the same handle cannot help either, so both match
            // the Cypher API's writer-required status.
            Self::StateConflict | Self::Unavailable => FAILED_PRECONDITION,
            Self::Backpressure | Self::ResourceLimit => RESOURCE_EXHAUSTED,
            Self::Timeout => DEADLINE_EXCEEDED,
            Self::CommitOutcomeUnknown => UNAVAILABLE,
            Self::Internal => INTERNAL,
        }
    }

    /// The public `retryable` hint, when the class settles it.
    ///
    /// Both conflict classes answer 409, so each states its hint.
    #[must_use]
    pub const fn retryable(self) -> Option<bool> {
        match self {
            Self::Conflict | Self::Backpressure => Some(true),
            Self::StateConflict | Self::CommitOutcomeUnknown => Some(false),
            Self::InvalidRequest
            | Self::Planning
            | Self::NotFound
            | Self::ResourceLimit
            | Self::Timeout
            | Self::Unavailable
            | Self::Internal => None,
        }
    }

    /// Whether a failed write of this class is proven to have committed nothing.
    ///
    /// The database raises codes of these classes only before a request's
    /// commit starts, or, for [`Self::Conflict`], when the commit itself
    /// aborts. Timeouts, unknown commit outcomes and internal failures can
    /// follow a durable commit, so a caller must reconcile rather than resend.
    ///
    /// The guarantee covers the failing operation. An index-management request
    /// with several create, drop, retry or abort operations commits each one
    /// on its own, so a lifecycle failure after the first leaves the earlier
    /// operations committed (HEL-991).
    ///
    /// ```
    /// use helix_ast::error_code::QueryFailureClass;
    /// assert!(QueryFailureClass::StateConflict.rejects_before_commit());
    /// assert!(!QueryFailureClass::Internal.rejects_before_commit());
    /// ```
    #[must_use]
    pub const fn rejects_before_commit(self) -> bool {
        match self {
            Self::InvalidRequest
            | Self::Planning
            | Self::NotFound
            | Self::Conflict
            | Self::StateConflict
            | Self::Backpressure
            | Self::ResourceLimit
            | Self::Unavailable => true,
            Self::Timeout | Self::CommitOutcomeUnknown | Self::Internal => false,
        }
    }
}

impl QueryErrorCode {
    /// The one class every transport reports this code with.
    #[must_use]
    pub const fn failure_class(self) -> QueryFailureClass {
        match self {
            Self::InvalidRequest
            | Self::InvalidQueryJson
            | Self::InvalidRequestBody
            | Self::InvalidRequestOption
            | Self::InvalidIndexOperationId
            | Self::InvalidNodeId
            | Self::UnboundParameter
            | Self::SecondaryLifecycleSteppingRequiresDisabledMode
            | Self::MigrationSteppingRequiresDisabledMode
            | Self::ActiveTextMutationLimitExceeded
            | Self::IndexOperationBatchTooLarge
            | Self::InvalidIndexSourceData
            | Self::InvalidSecondaryIndexValue
            | Self::InvalidVectorConfiguration
            | Self::UnsupportedUniqueIndexValueType
            | Self::InvalidVectorDimension
            | Self::InvalidVectorComponent
            | Self::VectorComponentMagnitudeExceeded
            | Self::ZeroNormCosineVector => QueryFailureClass::InvalidRequest,
            Self::InternalPlannerError
            | Self::UnsupportedEdgeAllTarget
            | Self::NonLiteralIndexExpression
            | Self::MissingPlanningEqualityParameter
            | Self::UnsupportedPlanningEqualityParameter
            | Self::InvalidSearchTenant
            | Self::InvalidSearchTenantValue
            | Self::InvalidSearchResultCount
            | Self::InvalidSearchResultCountExpression
            | Self::InvalidSearchInput
            | Self::InvalidBatchConditionMinSize
            | Self::InvalidInitialBatchCondition
            | Self::DuplicatePropertyAssignment
            | Self::DuplicatePropertySelection
            | Self::DuplicateProjectionAlias
            | Self::DuplicateReturnVariable
            | Self::DuplicateElementId
            | Self::DuplicateOrderKey
            | Self::UnboundContext
            | Self::InvalidSubTraversalOperation
            | Self::InvalidAfterBindOperation
            | Self::ReadOnlyTraversalInWriteBatch
            | Self::InvalidBranchArity
            | Self::InvalidBatchArity
            | Self::InvalidRepeatEmit
            | Self::InvalidRepeatCount
            | Self::InvalidShortestPathCount
            | Self::InvalidOrderKeys
            | Self::InvalidProjectionArity
            | Self::InvalidStreamRange
            | Self::InvalidStreamBoundExpression
            | Self::InvalidEmptyName
            | Self::InvalidPredicateArity => QueryFailureClass::Planning,
            Self::NodeNotFound
            | Self::EdgeNotFound
            | Self::IndexNotFound
            | Self::IndexOperationNotFound => QueryFailureClass::NotFound,
            Self::TransactionConflict
            | Self::RequestReadViewChanged
            | Self::StaleIndexGeneration => QueryFailureClass::Conflict,
            Self::UniqueConstraintViolation
            | Self::IndexAlreadyExists
            | Self::IndexDefinitionConflict
            | Self::IndexBusy
            | Self::IndexOperationNotAbortable
            | Self::IndexBuildBlocked => QueryFailureClass::StateConflict,
            Self::IndexBackpressure => QueryFailureClass::Backpressure,
            Self::QueryMemoryLimitExceeded => QueryFailureClass::ResourceLimit,
            Self::QueryDeadlineExceeded => QueryFailureClass::Timeout,
            Self::WriterModeRequired
            | Self::ReaderModeRequired
            | Self::DatabaseClosed
            | Self::QueryCancelledByReaderRetirement
            | Self::IndexLifecycleUnavailable
            | Self::MigrationRequired
            | Self::WriterMigrationRequired => QueryFailureClass::Unavailable,
            Self::WriterFencedCommitOutcomeUnknown => QueryFailureClass::CommitOutcomeUnknown,
            Self::StorageError
            | Self::InvalidConfiguration
            | Self::InvalidIndexModel
            | Self::UnsupportedIndexStorageVersion
            | Self::IdentifierExhausted
            | Self::IndexIdExhausted
            | Self::VectorPhysicalIdExhausted
            | Self::IndexGenerationExhausted
            | Self::IndexRevisionExhausted
            | Self::IndexOperationRevisionExhausted
            | Self::InvalidQuery
            | Self::ResponseSerializationError
            | Self::InternalError => QueryFailureClass::Internal,
        }
    }

    /// Every code in the stable public catalog.
    pub const ALL: &'static [Self] = &[
        Self::InvalidRequest,
        Self::InvalidQueryJson,
        Self::InvalidRequestBody,
        Self::InvalidRequestOption,
        Self::InvalidIndexOperationId,
        Self::InternalPlannerError,
        Self::UnsupportedEdgeAllTarget,
        Self::NonLiteralIndexExpression,
        Self::MissingPlanningEqualityParameter,
        Self::UnsupportedPlanningEqualityParameter,
        Self::InvalidSearchTenant,
        Self::InvalidSearchTenantValue,
        Self::InvalidSearchResultCount,
        Self::InvalidSearchResultCountExpression,
        Self::InvalidSearchInput,
        Self::InvalidBatchConditionMinSize,
        Self::InvalidInitialBatchCondition,
        Self::DuplicatePropertyAssignment,
        Self::DuplicatePropertySelection,
        Self::DuplicateProjectionAlias,
        Self::DuplicateReturnVariable,
        Self::DuplicateElementId,
        Self::DuplicateOrderKey,
        Self::UnboundContext,
        Self::InvalidSubTraversalOperation,
        Self::InvalidAfterBindOperation,
        Self::ReadOnlyTraversalInWriteBatch,
        Self::InvalidBranchArity,
        Self::InvalidBatchArity,
        Self::InvalidRepeatEmit,
        Self::InvalidRepeatCount,
        Self::InvalidShortestPathCount,
        Self::InvalidOrderKeys,
        Self::InvalidProjectionArity,
        Self::InvalidStreamRange,
        Self::InvalidStreamBoundExpression,
        Self::InvalidEmptyName,
        Self::InvalidPredicateArity,
        Self::StorageError,
        Self::TransactionConflict,
        Self::RequestReadViewChanged,
        Self::QueryDeadlineExceeded,
        Self::QueryMemoryLimitExceeded,
        Self::QueryCancelledByReaderRetirement,
        Self::InvalidNodeId,
        Self::NodeNotFound,
        Self::EdgeNotFound,
        Self::DatabaseClosed,
        Self::InvalidConfiguration,
        Self::IndexLifecycleUnavailable,
        Self::SecondaryLifecycleSteppingRequiresDisabledMode,
        Self::MigrationSteppingRequiresDisabledMode,
        Self::ActiveTextMutationLimitExceeded,
        Self::IndexBackpressure,
        Self::IndexBuildBlocked,
        Self::IndexOperationBatchTooLarge,
        Self::InvalidIndexSourceData,
        Self::InvalidIndexModel,
        Self::InvalidSecondaryIndexValue,
        Self::MigrationRequired,
        Self::WriterMigrationRequired,
        Self::UnsupportedIndexStorageVersion,
        Self::IdentifierExhausted,
        Self::IndexIdExhausted,
        Self::VectorPhysicalIdExhausted,
        Self::IndexGenerationExhausted,
        Self::IndexRevisionExhausted,
        Self::IndexOperationRevisionExhausted,
        Self::StaleIndexGeneration,
        Self::WriterFencedCommitOutcomeUnknown,
        Self::InvalidVectorConfiguration,
        Self::InvalidQuery,
        Self::UnboundParameter,
        Self::WriterModeRequired,
        Self::ReaderModeRequired,
        Self::IndexAlreadyExists,
        Self::IndexDefinitionConflict,
        Self::IndexBusy,
        Self::IndexOperationNotFound,
        Self::IndexOperationNotAbortable,
        Self::IndexNotFound,
        Self::UniqueConstraintViolation,
        Self::UnsupportedUniqueIndexValueType,
        Self::InvalidVectorDimension,
        Self::InvalidVectorComponent,
        Self::VectorComponentMagnitudeExceeded,
        Self::ZeroNormCosineVector,
        Self::ResponseSerializationError,
        Self::InternalError,
    ];

    /// Return the frozen wire representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidRequest => "invalid_request",
            Self::InvalidQueryJson => "invalid_query_json",
            Self::InvalidRequestBody => "invalid_request_body",
            Self::InvalidRequestOption => "invalid_request_option",
            Self::InvalidIndexOperationId => "invalid_index_operation_id",
            Self::InternalPlannerError => "internal_planner_error",
            Self::UnsupportedEdgeAllTarget => "unsupported_edge_all_target",
            Self::NonLiteralIndexExpression => "non_literal_index_expression",
            Self::MissingPlanningEqualityParameter => "missing_planning_equality_parameter",
            Self::UnsupportedPlanningEqualityParameter => "unsupported_planning_equality_parameter",
            Self::InvalidSearchTenant => "invalid_search_tenant",
            Self::InvalidSearchTenantValue => "invalid_search_tenant_value",
            Self::InvalidSearchResultCount => "invalid_search_result_count",
            Self::InvalidSearchResultCountExpression => "invalid_search_result_count_expression",
            Self::InvalidSearchInput => "invalid_search_input",
            Self::InvalidBatchConditionMinSize => "invalid_batch_condition_min_size",
            Self::InvalidInitialBatchCondition => "invalid_initial_batch_condition",
            Self::DuplicatePropertyAssignment => "duplicate_property_assignment",
            Self::DuplicatePropertySelection => "duplicate_property_selection",
            Self::DuplicateProjectionAlias => "duplicate_projection_alias",
            Self::DuplicateReturnVariable => "duplicate_return_variable",
            Self::DuplicateElementId => "duplicate_element_id",
            Self::DuplicateOrderKey => "duplicate_order_key",
            Self::UnboundContext => "unbound_context",
            Self::InvalidSubTraversalOperation => "invalid_sub_traversal_operation",
            Self::InvalidAfterBindOperation => "invalid_after_bind_operation",
            Self::ReadOnlyTraversalInWriteBatch => "read_only_traversal_in_write_batch",
            Self::InvalidBranchArity => "invalid_branch_arity",
            Self::InvalidBatchArity => "invalid_batch_arity",
            Self::InvalidRepeatEmit => "invalid_repeat_emit",
            Self::InvalidRepeatCount => "invalid_repeat_count",
            Self::InvalidShortestPathCount => "invalid_shortest_path_count",
            Self::InvalidOrderKeys => "invalid_order_keys",
            Self::InvalidProjectionArity => "invalid_projection_arity",
            Self::InvalidStreamRange => "invalid_stream_range",
            Self::InvalidStreamBoundExpression => "invalid_stream_bound_expression",
            Self::InvalidEmptyName => "invalid_empty_name",
            Self::InvalidPredicateArity => "invalid_predicate_arity",
            Self::StorageError => "storage_error",
            Self::TransactionConflict => "transaction_conflict",
            Self::RequestReadViewChanged => "request_read_view_changed",
            Self::QueryDeadlineExceeded => "query_deadline_exceeded",
            Self::QueryMemoryLimitExceeded => "query_memory_limit_exceeded",
            Self::QueryCancelledByReaderRetirement => "query_cancelled_by_reader_retirement",
            Self::InvalidNodeId => "invalid_node_id",
            Self::NodeNotFound => "node_not_found",
            Self::EdgeNotFound => "edge_not_found",
            Self::DatabaseClosed => "database_closed",
            Self::InvalidConfiguration => "invalid_configuration",
            Self::IndexLifecycleUnavailable => "index_lifecycle_unavailable",
            Self::SecondaryLifecycleSteppingRequiresDisabledMode => {
                "secondary_lifecycle_stepping_requires_disabled_mode"
            }
            Self::MigrationSteppingRequiresDisabledMode => {
                "migration_stepping_requires_disabled_mode"
            }
            Self::ActiveTextMutationLimitExceeded => "active_text_mutation_limit_exceeded",
            Self::IndexBackpressure => "index_backpressure",
            Self::IndexBuildBlocked => "index_build_blocked",
            Self::IndexOperationBatchTooLarge => "index_operation_batch_too_large",
            Self::InvalidIndexSourceData => "invalid_index_source_data",
            Self::InvalidIndexModel => "invalid_index_model",
            Self::InvalidSecondaryIndexValue => "invalid_secondary_index_value",
            Self::MigrationRequired => "migration_required",
            Self::WriterMigrationRequired => "writer_migration_required",
            Self::UnsupportedIndexStorageVersion => "unsupported_index_storage_version",
            Self::IdentifierExhausted => "identifier_exhausted",
            Self::IndexIdExhausted => "index_id_exhausted",
            Self::VectorPhysicalIdExhausted => "vector_physical_id_exhausted",
            Self::IndexGenerationExhausted => "index_generation_exhausted",
            Self::IndexRevisionExhausted => "index_revision_exhausted",
            Self::IndexOperationRevisionExhausted => "index_operation_revision_exhausted",
            Self::StaleIndexGeneration => "stale_index_generation",
            Self::WriterFencedCommitOutcomeUnknown => "writer_fenced_commit_outcome_unknown",
            Self::InvalidVectorConfiguration => "invalid_vector_configuration",
            Self::InvalidQuery => "invalid_query",
            Self::UnboundParameter => "unbound_parameter",
            Self::WriterModeRequired => "writer_mode_required",
            Self::ReaderModeRequired => "reader_mode_required",
            Self::IndexAlreadyExists => "index_already_exists",
            Self::IndexDefinitionConflict => "index_definition_conflict",
            Self::IndexBusy => "index_busy",
            Self::IndexOperationNotFound => "index_operation_not_found",
            Self::IndexOperationNotAbortable => "index_operation_not_abortable",
            Self::IndexNotFound => "index_not_found",
            Self::UniqueConstraintViolation => "unique_constraint_violation",
            Self::UnsupportedUniqueIndexValueType => "unsupported_unique_index_value_type",
            Self::InvalidVectorDimension => "invalid_vector_dimension",
            Self::InvalidVectorComponent => "invalid_vector_component",
            Self::VectorComponentMagnitudeExceeded => "vector_component_magnitude_exceeded",
            Self::ZeroNormCosineVector => "zero_norm_cosine_vector",
            Self::ResponseSerializationError => "response_serialization_error",
            Self::InternalError => "internal_error",
        }
    }
}

impl core::fmt::Display for QueryErrorCode {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl core::str::FromStr for QueryErrorCode {
    type Err = UnknownQueryErrorCode;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .iter()
            .copied()
            .find(|code| code.as_str() == value)
            .ok_or_else(|| UnknownQueryErrorCode(value.to_string()))
    }
}

/// A string that is not part of the known static query-error catalog.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnknownQueryErrorCode(String);

impl UnknownQueryErrorCode {
    /// Return the unrecognized wire value.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl core::fmt::Display for UnknownQueryErrorCode {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(formatter, "unknown query error code `{}`", self.0)
    }
}

impl std::error::Error for UnknownQueryErrorCode {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_round_trips_through_every_public_representation() {
        let mut strings = std::collections::BTreeSet::new();
        for code in QueryErrorCode::ALL {
            assert!(strings.insert(code.as_str()), "duplicate code {code}");
            assert_eq!(code.to_string(), code.as_str());
            assert_eq!(code.as_str().parse(), Ok(*code));
            assert_eq!(simd_json::to_string(code).unwrap(), format!("\"{code}\""));
            assert_eq!(
                simd_json::from_reader::<_, QueryErrorCode>(format!("\"{code}\"").as_bytes())
                    .unwrap(),
                *code
            );
        }
    }

    #[test]
    fn every_class_has_one_pinned_transport_contract() {
        use QueryFailureClass as Class;
        // (class, HTTP, gRPC, retryable, rejects before commit)
        let contracts = [
            (Class::InvalidRequest, 400, 3, None, true),
            (Class::Planning, 400, 3, None, true),
            (Class::NotFound, 404, 5, None, true),
            (Class::Conflict, 409, 10, Some(true), true),
            (Class::StateConflict, 409, 9, Some(false), true),
            (Class::Backpressure, 429, 8, Some(true), true),
            (Class::ResourceLimit, 429, 8, None, true),
            (Class::Timeout, 408, 4, None, false),
            (Class::Unavailable, 503, 9, None, true),
            (Class::CommitOutcomeUnknown, 503, 14, Some(false), false),
            (Class::Internal, 500, 13, None, false),
        ];
        for (class, http, grpc, retryable, rejects) in contracts {
            // A new class fails to compile here until it is pinned above.
            match class {
                Class::InvalidRequest
                | Class::Planning
                | Class::NotFound
                | Class::Conflict
                | Class::StateConflict
                | Class::Backpressure
                | Class::ResourceLimit
                | Class::Timeout
                | Class::Unavailable
                | Class::CommitOutcomeUnknown
                | Class::Internal => {}
            }
            assert_eq!(class.http_status(), http, "{class:?}");
            assert_eq!(class.grpc_code(), grpc, "{class:?}");
            assert_eq!(class.retryable(), retryable, "{class:?}");
            assert_eq!(class.rejects_before_commit(), rejects, "{class:?}");
        }
        let classes: std::collections::BTreeSet<_> = QueryErrorCode::ALL
            .iter()
            .map(|code| format!("{:?}", code.failure_class()))
            .collect();
        assert_eq!(classes.len(), contracts.len(), "every class has a code");
    }

    #[test]
    fn caller_visible_failures_keep_their_classes() {
        use QueryFailureClass as Class;
        for (code, class) in [
            (
                QueryErrorCode::UniqueConstraintViolation,
                Class::StateConflict,
            ),
            (QueryErrorCode::IndexAlreadyExists, Class::StateConflict),
            (QueryErrorCode::IndexBuildBlocked, Class::StateConflict),
            (QueryErrorCode::TransactionConflict, Class::Conflict),
            (QueryErrorCode::StaleIndexGeneration, Class::Conflict),
            (QueryErrorCode::NodeNotFound, Class::NotFound),
            (QueryErrorCode::IndexNotFound, Class::NotFound),
            (QueryErrorCode::UnboundParameter, Class::InvalidRequest),
            (
                QueryErrorCode::InvalidVectorDimension,
                Class::InvalidRequest,
            ),
            (QueryErrorCode::InternalPlannerError, Class::Planning),
            (QueryErrorCode::IndexBackpressure, Class::Backpressure),
            (
                QueryErrorCode::QueryMemoryLimitExceeded,
                Class::ResourceLimit,
            ),
            (QueryErrorCode::QueryDeadlineExceeded, Class::Timeout),
            (QueryErrorCode::WriterModeRequired, Class::Unavailable),
            (
                QueryErrorCode::WriterFencedCommitOutcomeUnknown,
                Class::CommitOutcomeUnknown,
            ),
            (QueryErrorCode::StorageError, Class::Internal),
            (QueryErrorCode::InvalidQuery, Class::Internal),
            (QueryErrorCode::ResponseSerializationError, Class::Internal),
        ] {
            assert_eq!(code.failure_class(), class, "{code}");
        }
    }

    /// The public error reference lists every code once, with the HTTP status
    /// of its class, so the documentation cannot drift from the transports.
    #[test]
    fn the_error_reference_documents_each_code_with_its_class_status() {
        let reference =
            include_str!("../../../docs/database/helix-db/query-guides/error-handling.mdx");
        let documented: std::collections::HashMap<_, _> = reference
            .lines()
            .filter_map(|line| {
                // | `code` | meaning | HTTP | retry | correctable |
                let cells: Vec<_> = line.split('|').map(str::trim).collect();
                let [_, code, _, status, _, _, _] = cells.as_slice() else {
                    return None;
                };
                let code = code.strip_prefix('`')?.strip_suffix('`')?;
                Some((code.parse::<QueryErrorCode>().ok()?, *status))
            })
            .collect();
        for code in QueryErrorCode::ALL {
            assert_eq!(
                documented.get(code).copied(),
                Some(code.failure_class().http_status().to_string().as_str()),
                "{code}"
            );
        }
    }

    #[test]
    fn unknown_code_preserves_the_original_wire_value() {
        let error = "future_error".parse::<QueryErrorCode>().unwrap_err();
        assert_eq!(error.as_str(), "future_error");
        assert_eq!(error.to_string(), "unknown query error code `future_error`");
    }
}
