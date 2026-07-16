pub(crate) use helix_ast::batch::{
    read_batch, write_batch, BatchCondition, BatchEntry, BatchQuery, NamedQuery, ReadBatch,
};
pub(crate) use helix_ast::expr::{CompareOp, Expr, Predicate, StreamBound};
pub(crate) use helix_ast::graph::{EdgeRef, NodeRef};
pub(crate) use helix_ast::index::{IndexSpec, RangeIndexDirection};
pub(crate) use helix_ast::projection::{
    BindingProjection, BindingTarget, BindingValueRef, Projection,
};
pub(crate) use helix_ast::query::QueryValue;
pub(crate) use helix_ast::traversal::{
    g, sub, AggregateFunction, AstNode, EmitBehavior, Order, ReadOnly, RepeatConfig, Traversal,
    TraversalState,
};
pub(crate) use helix_ast::value::{PropertyInput, PropertyValue};
pub(crate) use std::num::NonZeroUsize;

pub(crate) use crate::catalog::{
    EdgeEqualityIndexMeta, EdgeRangeIndexMeta, EdgeSearchIndexKey, ElementKind,
    IndexCatalogSnapshot, IndexUniqueness, NodeEqualityIndexMeta, NodeRangeIndexMeta,
    NodeSearchIndexKey, ScopedPropertyDirectionKey, ScopedPropertyKey, SearchIndexKey,
    SearchIndexKind, SearchIndexScope, TextIndexMeta, VectorIndexMeta,
};
pub(crate) use crate::context::{
    IndexUnionBranchLimit, ParamBindings, PlannerContext, StatsSnapshot,
};
pub(crate) use crate::error::InitialBatchCondition;
pub(crate) use crate::error::{
    BatchOp, BranchOp, PlannerError, ProjectionOp, ReadOnlyWriteOp, RepeatCountField,
    SearchTenantValueExpected,
};
pub(crate) use crate::exec::{
    ElementKeyspace, ExecAccessPlan, ExecBranchPlan, ExecCondition, ExecEdgeAccessPlan,
    ExecMergeMode, ExecMutationPlan, ExecNodeAccessPlan, ExecOp, ExecVariableOp, ExecutablePlan,
    KvReadPlan,
};
pub(crate) use crate::ir::{
    AggregatePlan, AtLeast, BatchConditionPlan, BatchOutputPlan, BatchVariableConditionPlan,
    BindingProjectionItems, BindingProjectionPlan, BindingTargetPlan, BindingValueRefPlan,
    EdgeAccessPlan, EdgeAccessSourcePlan, EdgeTargetPlan, ElementIds, ElementIdsError,
    ExpandDirection, ExpandLabelPlan, ExpandOutput, ExpandPlan, ExprPlan, ExprPlanError,
    FilterPlan, IndexBetweenRange, IndexBound, IndexCreateMode, IndexDdlCreateSpec,
    IndexDdlDropSpec, IndexDdlPlan, IndexRange, IndexValue, NameField, NodeAccessPlan,
    NodeAccessSourcePlan, NodeTargetPlan, NonEmptyString, OrderKey, OrderKeys, OrderKeysError,
    OrderPlan, PhysicalOp, PlanKind, PredicatePlan, PredicateSetOp, ProjectionDedupMode,
    ProjectionItem, ProjectionItems, ProjectionItemsError, ProjectionPlan, PropertyAssignments,
    PropertyAssignmentsError, PropertyInputExprPlan, PropertyInputExprPlanError, PropertyInputPlan,
    PropertyNames, PropertyNamesError, PropertySelection, RangeIndexF32, RangeIndexF64,
    RangeIndexLiteral, RangeIndexValue, RepeatEmitPlan, RepeatPlan, RepeatStopPlan, ReservedOp,
    ReturnPlan, ReturnVariables, RunConditionPlan, SearchIndexPlan, SearchLimitExpected,
    SearchLimitExprPlan, SearchLimitExprPlanError, SearchLimitPlan, SearchLimitPlanError,
    SearchQueryExprPlan, SearchQueryExprPlanError, SearchQueryInputExpected,
    SearchQueryInputPlanError, SearchTenantPlan, SearchTenantValuePlan, SearchTenantValuePlanError,
    SearchVector, SearchVectorComponent, SearchVectorError, SecondaryIndexLiteral,
    SecondaryIndexLiteralError, StreamBoundExpected, StreamBoundExprPlan, StreamBoundExprPlanError,
    StreamBoundPlan, StreamBoundPlanError, StreamDynamicRange, StreamLiteralRange, StreamRangePlan,
    StreamRangePlanError, StreamVariableOp, TextQueryInputPlan, VectorQueryInputPlan,
};
pub(crate) use crate::rules::{KnownRuleId, RuleId};
pub(crate) use crate::trace::{TraceDecision, TraceEvent, TracePass, TraceReason};

pub(crate) fn executable_traversal<S: TraversalState>(
    traversal: Traversal<S, ReadOnly>,
    ctx: PlannerContext,
) -> ExecutablePlan {
    let batch = read_batch()
        .var_as("result", traversal)
        .returning(["result"]);
    crate::planning::plan_read_batch(&batch, &ctx).unwrap()
}

pub(crate) fn executable_ast(root: AstNode, ctx: PlannerContext) -> ExecutablePlan {
    let batch = ReadBatch {
        entries: vec![BatchEntry::Query(Box::new(NamedQuery {
            name: Some("result".to_string()),
            root,
            condition: None,
        }))],
        returns: vec!["result".to_string()],
    };
    crate::planning::plan_read_batch(&batch, &ctx).unwrap()
}

pub(crate) fn first_exec_access(plan: &ExecutablePlan) -> &ExecAccessPlan {
    plan.steps()
        .iter()
        .find_map(|step| match &step.op {
            ExecOp::Access { plan } => Some(plan.as_ref()),
            _ => None,
        })
        .unwrap_or_else(|| panic!("expected executable access step: {:?}", plan.steps()))
}

pub(crate) fn first_kv_read(plan: &ExecutablePlan) -> &KvReadPlan {
    plan.steps()
        .iter()
        .find_map(|step| match &step.op {
            ExecOp::KvRead(read) => Some(read),
            _ => None,
        })
        .unwrap_or_else(|| panic!("expected executable KV read step: {:?}", plan.steps()))
}

pub(crate) fn first_exec_op(plan: &ExecutablePlan, predicate: impl Fn(&ExecOp) -> bool) -> &ExecOp {
    plan.steps()
        .iter()
        .map(|step| &step.op)
        .find(|op| predicate(op))
        .unwrap_or_else(|| panic!("expected executable op in plan: {:?}", plan.steps()))
}

pub(crate) fn builtin_label_indexes() -> IndexCatalogSnapshot {
    IndexCatalogSnapshot::default()
        .with_node_eq(ScopedPropertyKey::try_new("User", "$label").unwrap())
        .with_node_eq(ScopedPropertyKey::try_new("Account", "$label").unwrap())
        .with_node_eq(ScopedPropertyKey::try_new("Doc", "$label").unwrap())
        .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "$label").unwrap())
        .with_edge_eq(ScopedPropertyKey::try_new("LIKES", "$label").unwrap())
        .with_edge_eq(ScopedPropertyKey::try_new("MENTIONS", "$label").unwrap())
}

pub(crate) fn ctx(indexes: IndexCatalogSnapshot) -> PlannerContext {
    PlannerContext {
        indexes,
        ..PlannerContext::default()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExecOpFamily {
    Branch,
    Distinct,
    Expand,
    Filter,
    Limit,
    Skip,
    Range,
    Order,
    Repeat,
    Variable,
}

impl ExecOpFamily {
    pub(crate) const fn matches(self, op: &ExecOp) -> bool {
        matches!(
            (self, op),
            (Self::Branch, ExecOp::Branch { .. })
                | (Self::Distinct, ExecOp::Distinct)
                | (Self::Expand, ExecOp::Expand { .. })
                | (Self::Filter, ExecOp::Filter { .. })
                | (Self::Limit, ExecOp::Limit { .. })
                | (Self::Skip, ExecOp::Skip { .. })
                | (Self::Range, ExecOp::Range { .. })
                | (Self::Order, ExecOp::Order { .. })
                | (Self::Repeat, ExecOp::Repeat { .. })
                | (Self::Variable, ExecOp::Variable { .. })
        )
    }
}

pub(crate) fn has_exec_op_family(plan: &ExecutablePlan, family: ExecOpFamily) -> bool {
    plan.steps().iter().any(|step| family.matches(&step.op))
}

pub(crate) fn assert_no_exec_op_family(plan: &ExecutablePlan, family: ExecOpFamily) {
    assert!(
        !has_exec_op_family(plan, family),
        "unexpected {family:?} op in plan: {:?}",
        plan.steps()
    );
}

pub(crate) fn assert_no_exec_window(plan: &ExecutablePlan) {
    [ExecOpFamily::Limit, ExecOpFamily::Skip, ExecOpFamily::Range]
        .into_iter()
        .for_each(|family| assert_no_exec_op_family(plan, family));
}

pub(crate) fn assert_exec_range(plan: &ExecutablePlan, start: usize, end: usize) {
    let actual = plan.steps().iter().find_map(|step| match &step.op {
        ExecOp::Range {
            range: StreamRangePlan::Literal(range),
        } => Some((range.start(), range.end())),
        _ => None,
    });
    assert_eq!(
        actual,
        Some((start, end)),
        "unexpected range shape in plan: {:?}",
        plan.steps()
    );
}

pub(crate) fn first_kv_read_limit(plan: &ExecutablePlan) -> Option<usize> {
    match first_kv_read(plan) {
        KvReadPlan::RangeScan { limit, .. } | KvReadPlan::PrefixScan { limit, .. } => {
            limit.map(|limit| limit.get())
        }
        KvReadPlan::Get { .. } | KvReadPlan::MultiGet(_) => None,
    }
}

pub(crate) fn first_limited_access_limit(plan: &ExecutablePlan) -> Option<usize> {
    match first_exec_access(plan) {
        ExecAccessPlan::Limited(limited) => Some(limited.limit().get()),
        ExecAccessPlan::Node(_) | ExecAccessPlan::Edge(_) => None,
    }
}

pub(crate) fn unwrapped_first_exec_access(plan: &ExecutablePlan) -> &ExecAccessPlan {
    match first_exec_access(plan) {
        ExecAccessPlan::Limited(limited) => limited.source(),
        access @ (ExecAccessPlan::Node(_) | ExecAccessPlan::Edge(_)) => access,
    }
}

pub(crate) fn literal_exec_search_k(plan: &ExecutablePlan) -> usize {
    let SearchLimitPlan::Literal(k) = exec_search_k(plan) else {
        panic!(
            "expected literal executable search k: {:?}",
            first_exec_access(plan)
        );
    };
    k.get()
}

pub(crate) fn exec_search_k(plan: &ExecutablePlan) -> &SearchLimitPlan {
    match unwrapped_first_exec_access(plan) {
        ExecAccessPlan::Node(
            ExecNodeAccessPlan::VectorSearch { k, .. } | ExecNodeAccessPlan::TextSearch { k, .. },
        )
        | ExecAccessPlan::Edge(
            ExecEdgeAccessPlan::VectorSearch { k, .. } | ExecEdgeAccessPlan::TextSearch { k, .. },
        ) => k,
        access => panic!("expected executable search access: {access:?}"),
    }
}

pub(crate) fn assert_selected_root_family(plan: &ExecutablePlan, expected: &str) {
    assert!(
        plan.trace().events.iter().any(|event| matches!(
            &event.reason,
            TraceReason::SelectedRootFamily(family) if family.as_ref() == expected
        )),
        "missing selected root family {expected:?} in trace: {:?}",
        plan.trace().events
    );
}

pub(crate) fn assert_selected_rule(plan: &ExecutablePlan, expected: KnownRuleId) {
    let expected = RuleId::known(expected);
    assert!(
        plan.trace().events.iter().any(|event| matches!(
            &event.reason,
            TraceReason::SelectedOptimizerRule(rule) if rule.as_ref() == expected.as_ref()
        )),
        "missing selected optimizer rule {:?} in trace: {:?}",
        expected.as_ref(),
        plan.trace().events
    );
}

pub(crate) fn access_steps_matching(
    plan: &ExecutablePlan,
    predicate: impl Fn(&ExecAccessPlan) -> bool,
) -> usize {
    plan.steps()
        .iter()
        .filter_map(|step| match &step.op {
            ExecOp::Access { plan } => Some(plan.as_ref()),
            _ => None,
        })
        .filter(|access| {
            let access = match access {
                ExecAccessPlan::Limited(limited) => limited.source(),
                ExecAccessPlan::Node(_) | ExecAccessPlan::Edge(_) => access,
            };
            predicate(access)
        })
        .count()
}
