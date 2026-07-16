//! Public planner error contracts.

mod operation;
mod search;

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
