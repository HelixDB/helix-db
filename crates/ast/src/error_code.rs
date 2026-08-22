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
    /// An active text mutation exceeded an admission limit.
    ActiveTextMutationLimitExceeded,
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

impl QueryErrorCode {
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
        Self::QueryCancelledByReaderRetirement,
        Self::InvalidNodeId,
        Self::NodeNotFound,
        Self::EdgeNotFound,
        Self::DatabaseClosed,
        Self::InvalidConfiguration,
        Self::IndexLifecycleUnavailable,
        Self::SecondaryLifecycleSteppingRequiresDisabledMode,
        Self::ActiveTextMutationLimitExceeded,
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
            Self::ActiveTextMutationLimitExceeded => "active_text_mutation_limit_exceeded",
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
            assert_eq!(sonic_rs::to_string(code).unwrap(), format!("\"{code}\""));
            assert_eq!(
                sonic_rs::from_str::<QueryErrorCode>(&format!("\"{code}\"")).unwrap(),
                *code
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
