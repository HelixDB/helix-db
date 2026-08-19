//! Public planner error contracts.

mod operation;
mod search;

use helix_ast::error_code;
use helix_ast::traversal::EmitBehavior;
use thiserror::Error;

pub use self::operation::{
    AfterBindOp, BatchOp, BranchOp, InitialBatchCondition, ProjectionOp, ReadOnlyWriteOp,
    RepeatCountField, ShortestPathCountField, SubTraversalOp,
};
pub use self::search::SearchTenantValueExpected;
use crate::catalog::{ElementKind, SearchIndexKind};
use crate::exec;
use crate::ir::{
    ExprPlanError, NameField, NonEmptyString, PredicateSetOp, SearchLimitExpected,
    SearchQueryInputExpected, StreamBoundExpected,
};
use crate::memo;
use crate::optimizer;

/// Planner errors.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum PlannerError {
    /// An index lifecycle control used a malformed or nil operation UUID.
    #[error(transparent)]
    InvalidIndexOperationId(#[from] crate::ir::IndexOperationIdError),
    /// Physical-to-executable lowering produced an invalid executable DAG.
    #[error("invalid executable plan: {error}")]
    InvalidExecutablePlan {
        /// Executable plan validation failure.
        error: exec::ExecPlanError,
    },
    /// The native root does not yet expose a selected Cascades contract.
    #[error("unsupported cascades plan: {reason}")]
    UnsupportedCascadesPlan {
        /// Unsupported contract reason.
        reason: String,
    },
    /// Cascades optimizer failed before producing a selected result.
    #[error("optimizer failed: {memo_error}")]
    OptimizerFailure {
        /// Memo/optimizer failure.
        memo_error: memo::MemoError,
    },
    /// Cascades optimizer produced a result, but physical selection failed.
    #[error("optimizer selection failed: {selection_error}")]
    OptimizerSelectionFailure {
        /// Typed physical-selection failure.
        selection_error: optimizer::SelectionError,
    },
    /// The AST referenced every edge where a mutation target requires a finite edge set.
    #[error("EdgeRef::All cannot be used as an edge mutation target")]
    UnsupportedEdgeAllTarget,
    /// Index planning saw a dynamic expression where only literals or params are indexable.
    #[error("non-literal index expression: {expression}")]
    NonLiteralIndexExpression {
        /// Debug rendering of the non-literal expression.
        expression: String,
    },
    /// An equality parameter was not bound when its algorithm had to be selected.
    #[error("missing planning equality parameter `{param}`")]
    MissingPlanningEqualityParameter {
        /// Missing parameter name.
        param: NonEmptyString,
    },
    /// A bound equality value cannot be represented by a secondary-index literal.
    #[error("unsupported planning equality parameter `{param}`")]
    UnsupportedPlanningEqualityParameter {
        /// Unsupported parameter name.
        param: NonEmptyString,
    },
    /// Required search index does not exist.
    #[error("missing {element:?} {kind} index for `{label}.{property}`")]
    MissingSearchIndex {
        /// Node or edge.
        element: ElementKind,
        /// Search kind.
        kind: SearchIndexKind,
        /// Label.
        label: NonEmptyString,
        /// Property.
        property: NonEmptyString,
    },
    /// Search query supplied a tenant value for an index that has no tenant property.
    #[error("{kind} search index `{index_id}` is not tenant-scoped")]
    InvalidSearchTenant {
        /// Search kind.
        kind: SearchIndexKind,
        /// Index ID.
        index_id: NonEmptyString,
    },
    /// Search query supplied an invalid tenant value for a tenant-scoped index.
    #[error("{kind} search tenant value must be {expected}")]
    InvalidSearchTenantValue {
        /// Search kind.
        kind: SearchIndexKind,
        /// Expected tenant value shape.
        expected: SearchTenantValueExpected,
    },
    /// Search query requested an invalid result count.
    #[error("{kind} search result count must be positive, got {actual}")]
    InvalidSearchResultCount {
        /// Search kind.
        kind: SearchIndexKind,
        /// Actual literal count.
        actual: usize,
    },
    /// Search query requested a statically invalid result-count expression.
    #[error("{kind} search result count must be {expected}")]
    InvalidSearchResultCountExpression {
        /// Search kind.
        kind: SearchIndexKind,
        /// Expected result-count expression shape.
        expected: SearchLimitExpected,
    },
    /// Search query supplied a literal payload that does not match the search kind.
    #[error("{kind} search query input must be {expected}")]
    InvalidSearchInput {
        /// Search kind.
        kind: SearchIndexKind,
        /// Expected literal payload kind.
        expected: SearchQueryInputExpected,
    },
    /// Batch condition requested an invalid minimum size.
    #[error("batch condition minimum size must be positive, got {actual}")]
    InvalidBatchConditionMinSize {
        /// Actual minimum size.
        actual: usize,
    },
    /// Batch condition depends on a previous result where no previous result exists.
    #[error("batch condition `{condition}` requires a previous batch entry")]
    InvalidInitialBatchCondition {
        /// Invalid condition name.
        condition: InitialBatchCondition,
    },
    /// Mutation property assignments included the same property more than once.
    #[error("duplicate property assignment `{property}`")]
    DuplicatePropertyAssignment {
        /// Duplicate property name.
        property: NonEmptyString,
    },
    /// Explicit property selections included the same property more than once.
    #[error("duplicate property selection `{property}`")]
    DuplicatePropertySelection {
        /// Duplicate selected property name.
        property: NonEmptyString,
    },
    /// Projection output aliases included the same alias more than once.
    #[error("duplicate projection alias `{alias}`")]
    DuplicateProjectionAlias {
        /// Duplicate projection output alias.
        alias: NonEmptyString,
    },
    /// Batch return variables included the same name more than once.
    #[error("duplicate return variable `{name}`")]
    DuplicateReturnVariable {
        /// Duplicate return variable.
        name: NonEmptyString,
    },
    /// Concrete point ID lists included the same ID more than once.
    #[error("duplicate {element} id `{id}`")]
    DuplicateElementId {
        /// Node or edge.
        element: ElementKind,
        /// Duplicate concrete element ID.
        id: u64,
    },
    /// Explicit sort keys included the same property more than once.
    #[error("duplicate order key `{property}`")]
    DuplicateOrderKey {
        /// Duplicate sort property.
        property: NonEmptyString,
    },
    /// Branch context reached a source-only planner path.
    #[error("sub-traversal context cannot be planned without parent input")]
    UnboundContext,
    /// Sub-traversal operation would violate parent-row correlation.
    #[error("{op} is not supported inside branch/repeat sub-traversals")]
    InvalidSubTraversalOperation {
        /// Rejected operation family.
        op: SubTraversalOp,
    },
    /// Operation is not supported after row-local bindings have been captured.
    #[error("{op} is not supported after bind")]
    InvalidAfterBindOperation {
        /// Rejected operation.
        op: AfterBindOp,
    },
    /// Write batches cannot contain read-only row-binding operations.
    #[error("{op} is read-only and cannot be used in write batches")]
    ReadOnlyTraversalInWriteBatch {
        /// Rejected operation.
        op: ReadOnlyWriteOp,
    },
    /// Branch operation does not contain enough traversals for a valid physical branch.
    #[error("{op} branch requires at least {min} traversal(s), got {actual}")]
    InvalidBranchArity {
        /// Branch operation name.
        op: BranchOp,
        /// Minimum valid branch count.
        min: usize,
        /// Actual branch count.
        actual: usize,
    },
    /// Batch operation did not contain enough entries for a valid physical plan.
    #[error("{op} batch requires at least {min} entry(s), got {actual}")]
    InvalidBatchArity {
        /// Batch operation name.
        op: BatchOp,
        /// Minimum valid entry count.
        min: usize,
        /// Actual entry count.
        actual: usize,
    },
    /// Repeat emit predicates are only valid with `EmitBehavior::After`.
    #[error("repeat emit predicate is invalid for {emit:?}")]
    InvalidRepeatEmit {
        /// Emit behavior.
        emit: EmitBehavior,
    },
    /// Repeat configuration used a zero count/depth.
    #[error("repeat {field} must be positive, got {actual}")]
    InvalidRepeatCount {
        /// Invalid repeat field.
        field: RepeatCountField,
        /// Actual literal count.
        actual: usize,
    },
    /// Shortest-path configuration used a zero count/depth.
    #[error("shortest_path {field} must be positive, got {actual}")]
    InvalidShortestPathCount {
        /// Invalid shortest-path field.
        field: ShortestPathCountField,
        /// Actual literal count.
        actual: usize,
    },
    /// Order operation did not contain any keys.
    #[error("order operation requires at least one key")]
    InvalidOrderKeys,
    /// Projection operation did not contain enough fields.
    #[error("{op} projection requires at least {min} item(s), got {actual}")]
    InvalidProjectionArity {
        /// Projection operation name.
        op: ProjectionOp,
        /// Minimum valid item count.
        min: usize,
        /// Actual item count.
        actual: usize,
    },
    /// Stream range literal bounds were statically inverted.
    #[error("stream range start must not exceed end, got {start}..{end}")]
    InvalidStreamRange {
        /// Literal start bound.
        start: usize,
        /// Literal end bound.
        end: usize,
    },
    /// Stream bound was a statically invalid expression.
    #[error("stream bound must be {expected}")]
    InvalidStreamBoundExpression {
        /// Expected stream-bound expression shape.
        expected: StreamBoundExpected,
    },
    /// Physical plan identifier was empty.
    #[error("{field} name must not be empty")]
    InvalidEmptyName {
        /// Invalid field name.
        field: NameField,
    },
    /// Boolean predicate operation did not contain enough children.
    #[error("{op} predicate requires at least {min} child predicate(s), got {actual}")]
    InvalidPredicateArity {
        /// Predicate operator.
        op: PredicateSetOp,
        /// Minimum valid child count.
        min: usize,
        /// Actual child count.
        actual: usize,
    },
}

impl PlannerError {
    /// Stable machine-readable code for this public planning failure.
    pub const fn error_code(&self) -> error_code::QueryErrorCode {
        match self {
            Self::InvalidIndexOperationId(_) => error_code::QueryErrorCode::InvalidIndexOperationId,
            Self::InvalidExecutablePlan { .. }
            | Self::UnsupportedCascadesPlan { .. }
            | Self::OptimizerFailure { .. }
            | Self::OptimizerSelectionFailure { .. } => {
                error_code::QueryErrorCode::InternalPlannerError
            }
            Self::UnsupportedEdgeAllTarget => error_code::QueryErrorCode::UnsupportedEdgeAllTarget,
            Self::NonLiteralIndexExpression { .. } => {
                error_code::QueryErrorCode::NonLiteralIndexExpression
            }
            Self::MissingPlanningEqualityParameter { .. } => {
                error_code::QueryErrorCode::MissingPlanningEqualityParameter
            }
            Self::UnsupportedPlanningEqualityParameter { .. } => {
                error_code::QueryErrorCode::UnsupportedPlanningEqualityParameter
            }
            Self::MissingSearchIndex { .. } => error_code::QueryErrorCode::IndexNotFound,
            Self::InvalidSearchTenant { .. } => error_code::QueryErrorCode::InvalidSearchTenant,
            Self::InvalidSearchTenantValue { .. } => {
                error_code::QueryErrorCode::InvalidSearchTenantValue
            }
            Self::InvalidSearchResultCount { .. } => {
                error_code::QueryErrorCode::InvalidSearchResultCount
            }
            Self::InvalidSearchResultCountExpression { .. } => {
                error_code::QueryErrorCode::InvalidSearchResultCountExpression
            }
            Self::InvalidSearchInput { .. } => error_code::QueryErrorCode::InvalidSearchInput,
            Self::InvalidBatchConditionMinSize { .. } => {
                error_code::QueryErrorCode::InvalidBatchConditionMinSize
            }
            Self::InvalidInitialBatchCondition { .. } => {
                error_code::QueryErrorCode::InvalidInitialBatchCondition
            }
            Self::DuplicatePropertyAssignment { .. } => {
                error_code::QueryErrorCode::DuplicatePropertyAssignment
            }
            Self::DuplicatePropertySelection { .. } => {
                error_code::QueryErrorCode::DuplicatePropertySelection
            }
            Self::DuplicateProjectionAlias { .. } => {
                error_code::QueryErrorCode::DuplicateProjectionAlias
            }
            Self::DuplicateReturnVariable { .. } => {
                error_code::QueryErrorCode::DuplicateReturnVariable
            }
            Self::DuplicateElementId { .. } => error_code::QueryErrorCode::DuplicateElementId,
            Self::DuplicateOrderKey { .. } => error_code::QueryErrorCode::DuplicateOrderKey,
            Self::UnboundContext => error_code::QueryErrorCode::UnboundContext,
            Self::InvalidSubTraversalOperation { .. } => {
                error_code::QueryErrorCode::InvalidSubTraversalOperation
            }
            Self::InvalidAfterBindOperation { .. } => {
                error_code::QueryErrorCode::InvalidAfterBindOperation
            }
            Self::ReadOnlyTraversalInWriteBatch { .. } => {
                error_code::QueryErrorCode::ReadOnlyTraversalInWriteBatch
            }
            Self::InvalidBranchArity { .. } => error_code::QueryErrorCode::InvalidBranchArity,
            Self::InvalidBatchArity { .. } => error_code::QueryErrorCode::InvalidBatchArity,
            Self::InvalidRepeatEmit { .. } => error_code::QueryErrorCode::InvalidRepeatEmit,
            Self::InvalidRepeatCount { .. } => error_code::QueryErrorCode::InvalidRepeatCount,
            Self::InvalidShortestPathCount { .. } => {
                error_code::QueryErrorCode::InvalidShortestPathCount
            }
            Self::InvalidOrderKeys => error_code::QueryErrorCode::InvalidOrderKeys,
            Self::InvalidProjectionArity { .. } => {
                error_code::QueryErrorCode::InvalidProjectionArity
            }
            Self::InvalidStreamRange { .. } => error_code::QueryErrorCode::InvalidStreamRange,
            Self::InvalidStreamBoundExpression { .. } => {
                error_code::QueryErrorCode::InvalidStreamBoundExpression
            }
            Self::InvalidEmptyName { .. } => error_code::QueryErrorCode::InvalidEmptyName,
            Self::InvalidPredicateArity { .. } => error_code::QueryErrorCode::InvalidPredicateArity,
        }
    }

    /// Stable public index error code for planner failures that belong to the
    /// index lifecycle contract.
    pub const fn index_error_code(&self) -> Option<&'static str> {
        match self {
            Self::MissingSearchIndex { .. } => Some("index_not_found"),
            _ => None,
        }
    }
}

impl From<ExprPlanError> for PlannerError {
    fn from(err: ExprPlanError) -> Self {
        match err {
            ExprPlanError::EmptyName { field } => Self::InvalidEmptyName { field },
            ExprPlanError::EmptyPredicateSet { op } => Self::InvalidPredicateArity {
                op,
                min: 1,
                actual: 0,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use helix_ast::error_code::QueryErrorCode as Code;

    #[test]
    fn missing_search_index_has_the_public_not_found_code() {
        let error = PlannerError::MissingSearchIndex {
            element: ElementKind::Node,
            kind: SearchIndexKind::Text,
            label: NonEmptyString::new("User").unwrap(),
            property: NonEmptyString::new("bio").unwrap(),
        };

        assert_eq!(error.index_error_code(), Some("index_not_found"));
    }

    #[test]
    fn every_public_planner_error_arm_has_a_static_code() {
        let name = || NonEmptyString::new("value").unwrap();
        let internal = [
            PlannerError::InvalidExecutablePlan {
                error: exec::ExecPlanError::EmptyMultiGet,
            },
            PlannerError::UnsupportedCascadesPlan {
                reason: "unsupported".to_string(),
            },
            PlannerError::OptimizerFailure {
                memo_error: memo::MemoError::GroupIdSpaceExhausted,
            },
            PlannerError::OptimizerSelectionFailure {
                selection_error: optimizer::SelectionError::NoPhysicalAlternatives {
                    group: memo::MemoGroupId::first(),
                },
            },
        ];
        assert!(internal
            .iter()
            .all(|error| error.error_code() == Code::InternalPlannerError));

        let errors = vec![
            (
                PlannerError::InvalidIndexOperationId(
                    crate::ir::IndexOperationId::try_new("").unwrap_err(),
                ),
                Code::InvalidIndexOperationId,
            ),
            (
                PlannerError::UnsupportedEdgeAllTarget,
                Code::UnsupportedEdgeAllTarget,
            ),
            (
                PlannerError::NonLiteralIndexExpression {
                    expression: "param".to_string(),
                },
                Code::NonLiteralIndexExpression,
            ),
            (
                PlannerError::MissingPlanningEqualityParameter { param: name() },
                Code::MissingPlanningEqualityParameter,
            ),
            (
                PlannerError::UnsupportedPlanningEqualityParameter { param: name() },
                Code::UnsupportedPlanningEqualityParameter,
            ),
            (
                PlannerError::MissingSearchIndex {
                    element: ElementKind::Node,
                    kind: SearchIndexKind::Text,
                    label: name(),
                    property: name(),
                },
                Code::IndexNotFound,
            ),
            (
                PlannerError::InvalidSearchTenant {
                    kind: SearchIndexKind::Text,
                    index_id: name(),
                },
                Code::InvalidSearchTenant,
            ),
            (
                PlannerError::InvalidSearchTenantValue {
                    kind: SearchIndexKind::Text,
                    expected: SearchTenantValueExpected::NonNullPropertyInput,
                },
                Code::InvalidSearchTenantValue,
            ),
            (
                PlannerError::InvalidSearchResultCount {
                    kind: SearchIndexKind::Text,
                    actual: 0,
                },
                Code::InvalidSearchResultCount,
            ),
            (
                PlannerError::InvalidSearchResultCountExpression {
                    kind: SearchIndexKind::Text,
                    expected: SearchLimitExpected::PositiveInteger,
                },
                Code::InvalidSearchResultCountExpression,
            ),
            (
                PlannerError::InvalidSearchInput {
                    kind: SearchIndexKind::Text,
                    expected: SearchQueryInputExpected::NonEmptyString,
                },
                Code::InvalidSearchInput,
            ),
            (
                PlannerError::InvalidBatchConditionMinSize { actual: 0 },
                Code::InvalidBatchConditionMinSize,
            ),
            (
                PlannerError::InvalidInitialBatchCondition {
                    condition: InitialBatchCondition::PrevNotEmpty,
                },
                Code::InvalidInitialBatchCondition,
            ),
            (
                PlannerError::DuplicatePropertyAssignment { property: name() },
                Code::DuplicatePropertyAssignment,
            ),
            (
                PlannerError::DuplicatePropertySelection { property: name() },
                Code::DuplicatePropertySelection,
            ),
            (
                PlannerError::DuplicateProjectionAlias { alias: name() },
                Code::DuplicateProjectionAlias,
            ),
            (
                PlannerError::DuplicateReturnVariable { name: name() },
                Code::DuplicateReturnVariable,
            ),
            (
                PlannerError::DuplicateElementId {
                    element: ElementKind::Node,
                    id: 1,
                },
                Code::DuplicateElementId,
            ),
            (
                PlannerError::DuplicateOrderKey { property: name() },
                Code::DuplicateOrderKey,
            ),
            (PlannerError::UnboundContext, Code::UnboundContext),
            (
                PlannerError::InvalidSubTraversalOperation {
                    op: SubTraversalOp::Source,
                },
                Code::InvalidSubTraversalOperation,
            ),
            (
                PlannerError::InvalidAfterBindOperation {
                    op: AfterBindOp::OrderBy,
                },
                Code::InvalidAfterBindOperation,
            ),
            (
                PlannerError::ReadOnlyTraversalInWriteBatch {
                    op: ReadOnlyWriteOp::Bind,
                },
                Code::ReadOnlyTraversalInWriteBatch,
            ),
            (
                PlannerError::InvalidBranchArity {
                    op: BranchOp::Union,
                    min: 1,
                    actual: 0,
                },
                Code::InvalidBranchArity,
            ),
            (
                PlannerError::InvalidBatchArity {
                    op: BatchOp::Batch,
                    min: 1,
                    actual: 0,
                },
                Code::InvalidBatchArity,
            ),
            (
                PlannerError::InvalidRepeatEmit {
                    emit: EmitBehavior::Before,
                },
                Code::InvalidRepeatEmit,
            ),
            (
                PlannerError::InvalidRepeatCount {
                    field: RepeatCountField::Times,
                    actual: 0,
                },
                Code::InvalidRepeatCount,
            ),
            (
                PlannerError::InvalidShortestPathCount {
                    field: ShortestPathCountField::MaxDepth,
                    actual: 0,
                },
                Code::InvalidShortestPathCount,
            ),
            (PlannerError::InvalidOrderKeys, Code::InvalidOrderKeys),
            (
                PlannerError::InvalidProjectionArity {
                    op: ProjectionOp::Project,
                    min: 1,
                    actual: 0,
                },
                Code::InvalidProjectionArity,
            ),
            (
                PlannerError::InvalidStreamRange { start: 2, end: 1 },
                Code::InvalidStreamRange,
            ),
            (
                PlannerError::InvalidStreamBoundExpression {
                    expected: StreamBoundExpected::NonNegativeInteger,
                },
                Code::InvalidStreamBoundExpression,
            ),
            (
                PlannerError::InvalidEmptyName {
                    field: NameField::Name,
                },
                Code::InvalidEmptyName,
            ),
            (
                PlannerError::InvalidPredicateArity {
                    op: PredicateSetOp::And,
                    min: 1,
                    actual: 0,
                },
                Code::InvalidPredicateArity,
            ),
        ];

        for (error, expected) in errors {
            assert_eq!(error.error_code(), expected, "{error}");
        }
    }
}
