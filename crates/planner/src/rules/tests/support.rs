//! Shared rule-test fixtures and extraction helpers.

use super::super::*;
use crate::{
    catalog, cost, ir, logical,
    optimizer::{self, OptimizerRule},
    physical, properties,
};

pub(in crate::rules::tests) fn source(element: properties::ElementKind) -> logical::LogicalExpr {
    logical::LogicalExpr::Pure(logical::PureLogicalOp::Source { element })
}

pub(in crate::rules::tests) fn name(value: &str) -> ir::NonEmptyString {
    ir::NonEmptyString::new(value).unwrap()
}

pub(in crate::rules::tests) fn node_all_expr() -> logical::LogicalExpr {
    node_access_expr(ir::NodeAccessPlan::AllScan)
}

pub(in crate::rules::tests) fn edge_all_expr() -> logical::LogicalExpr {
    edge_access_expr(ir::EdgeAccessPlan::AllScan)
}

pub(in crate::rules::tests) fn optional_branch(
    input: logical::LogicalExpr,
    body: logical::LogicalExpr,
) -> logical::RootBranch {
    logical::RootBranch::new(input, ir::BranchPlan::Optional(Box::new(body)))
}

pub(in crate::rules::tests) fn optional_branch_expr(
    input: logical::LogicalExpr,
    body: logical::LogicalExpr,
) -> logical::LogicalExpr {
    logical::LogicalExpr::RootBranch(optional_branch(input, body))
}

pub(in crate::rules::tests) fn repeat_root_expr(
    input: logical::LogicalExpr,
    body: logical::LogicalExpr,
    max_depth: usize,
) -> logical::LogicalExpr {
    logical::LogicalExpr::RootRepeat(logical::RootRepeat::new(
        input,
        ir::RepeatPlan {
            body: Box::new(body),
            stop: ir::RepeatStopPlan::MaxDepthOnly,
            emit: ir::RepeatEmitPlan::None,
            max_depth: std::num::NonZeroUsize::new(max_depth).unwrap(),
        },
    ))
}

pub(in crate::rules::tests) fn order_keys() -> ir::OrderKeys {
    order_keys_for("age", helix_ast::traversal::Order::Asc)
}

pub(in crate::rules::tests) fn physical_alternative(
    result: optimizer::RuleResult,
) -> physical::PhysicalAlternative {
    let optimizer::RuleResult::Applied(optimizer::RuleEffect::Physical(alternatives)) = result
    else {
        panic!("expected physical alternative");
    };
    alternatives.into_iter().next().unwrap()
}

pub(in crate::rules::tests) fn logical_pipeline(
    result: optimizer::RuleResult,
) -> logical::PurePipeline {
    let optimizer::RuleResult::Applied(optimizer::RuleEffect::Logical(expressions)) = result else {
        panic!("expected logical pipeline rewrite");
    };
    let Some(logical::LogicalExpr::PurePipeline(pipeline)) = expressions.into_iter().next() else {
        panic!("expected pure pipeline expression");
    };
    pipeline
}

pub(in crate::rules::tests) fn logical_filter(result: optimizer::RuleResult) -> ir::PredicatePlan {
    let optimizer::RuleResult::Applied(optimizer::RuleEffect::Logical(expressions)) = result else {
        panic!("expected logical filter rewrite");
    };
    let Some(logical::LogicalExpr::Pure(logical::PureLogicalOp::Filter { predicate })) =
        expressions.into_iter().next()
    else {
        panic!("expected filter expression");
    };
    predicate
}

pub(in crate::rules::tests) fn logical_pure_op(
    result: optimizer::RuleResult,
) -> logical::PureLogicalOp {
    let optimizer::RuleResult::Applied(optimizer::RuleEffect::Logical(expressions)) = result else {
        panic!("expected logical rewrite");
    };
    let Some(logical::LogicalExpr::Pure(op)) = expressions.into_iter().next() else {
        panic!("expected pure expression");
    };
    op
}

pub(in crate::rules::tests) fn logical_access_path(
    result: optimizer::RuleResult,
) -> logical::AccessPath {
    let optimizer::RuleResult::Applied(optimizer::RuleEffect::Logical(expressions)) = result else {
        panic!("expected logical access-path rewrite");
    };
    let Some(logical::LogicalExpr::AccessPath(access)) = expressions.into_iter().next() else {
        panic!("expected access-path expression");
    };
    access
}

pub(in crate::rules::tests) fn logical_access_window(
    result: optimizer::RuleResult,
) -> logical::AccessWindow {
    let optimizer::RuleResult::Applied(optimizer::RuleEffect::Logical(expressions)) = result else {
        panic!("expected logical access-window rewrite");
    };
    let Some(logical::LogicalExpr::AccessWindow(window)) = expressions.into_iter().next() else {
        panic!("expected access-window expression");
    };
    window
}

pub(in crate::rules::tests) fn logical_access_pipeline(
    result: optimizer::RuleResult,
) -> logical::AccessPipeline {
    let optimizer::RuleResult::Applied(optimizer::RuleEffect::Logical(expressions)) = result else {
        panic!("expected logical access-pipeline rewrite");
    };
    let Some(logical::LogicalExpr::AccessPipeline(pipeline)) = expressions.into_iter().next()
    else {
        panic!("expected access-pipeline expression");
    };
    pipeline
}

pub(in crate::rules::tests) fn pipeline_expr(
    ops: Vec<logical::PureLogicalOp>,
) -> logical::LogicalExpr {
    logical::LogicalExpr::PurePipeline(logical::PurePipeline::new(
        ir::AtLeast::<_, 1>::try_from_vec(ops).unwrap(),
    ))
}

pub(in crate::rules::tests) fn filter_chain_expr(
    predicates: Vec<ir::PredicatePlan>,
) -> logical::LogicalExpr {
    logical::LogicalExpr::FilterChain(logical::FilterChain::new(
        ir::AtLeast::<_, 2>::try_from_vec(predicates).unwrap(),
    ))
}

pub(in crate::rules::tests) fn filter_pushdown_expr(
    op: logical::FilterPushdownOp,
    predicate: ir::PredicatePlan,
) -> logical::LogicalExpr {
    logical::LogicalExpr::FilterPushdown(logical::FilterPushdown::new(op, predicate))
}

pub(in crate::rules::tests) fn node_access_expr(plan: ir::NodeAccessPlan) -> logical::LogicalExpr {
    logical::LogicalExpr::AccessPath(logical::AccessPath::Node(logical::NodeAccessPath::new(
        ir::NodeAccessSourcePlan::new(plan).unwrap(),
    )))
}

pub(in crate::rules::tests) fn node_access_path(plan: ir::NodeAccessPlan) -> logical::AccessPath {
    logical::AccessPath::Node(logical::NodeAccessPath::new(
        ir::NodeAccessSourcePlan::new(plan).unwrap(),
    ))
}

pub(in crate::rules::tests) fn edge_access_path(plan: ir::EdgeAccessPlan) -> logical::AccessPath {
    logical::AccessPath::Edge(logical::EdgeAccessPath::new(
        ir::EdgeAccessSourcePlan::new(plan).unwrap(),
    ))
}

pub(in crate::rules::tests) fn edge_access_expr(plan: ir::EdgeAccessPlan) -> logical::LogicalExpr {
    logical::LogicalExpr::AccessPath(logical::AccessPath::Edge(logical::EdgeAccessPath::new(
        ir::EdgeAccessSourcePlan::new(plan).unwrap(),
    )))
}

pub(in crate::rules::tests) fn node_access_filter_expr(
    plan: ir::NodeAccessPlan,
    predicate: ir::PredicatePlan,
) -> logical::LogicalExpr {
    logical::LogicalExpr::AccessFilter(logical::AccessFilter::new(
        logical::AccessPath::Node(logical::NodeAccessPath::new(
            ir::NodeAccessSourcePlan::new(plan).unwrap(),
        )),
        predicate,
    ))
}

pub(in crate::rules::tests) fn edge_access_filter_expr(
    plan: ir::EdgeAccessPlan,
    predicate: ir::PredicatePlan,
) -> logical::LogicalExpr {
    logical::LogicalExpr::AccessFilter(logical::AccessFilter::new(
        logical::AccessPath::Edge(logical::EdgeAccessPath::new(
            ir::EdgeAccessSourcePlan::new(plan).unwrap(),
        )),
        predicate,
    ))
}

pub(in crate::rules::tests) fn node_access_window_expr(
    plan: ir::NodeAccessPlan,
    window: logical::AccessWindowRange,
) -> logical::LogicalExpr {
    logical::LogicalExpr::AccessWindow(logical::AccessWindow::new(
        logical::AccessPath::Node(logical::NodeAccessPath::new(
            ir::NodeAccessSourcePlan::new(plan).unwrap(),
        )),
        window,
    ))
}

pub(in crate::rules::tests) fn edge_access_window_expr(
    plan: ir::EdgeAccessPlan,
    window: logical::AccessWindowRange,
) -> logical::LogicalExpr {
    logical::LogicalExpr::AccessWindow(logical::AccessWindow::new(
        logical::AccessPath::Edge(logical::EdgeAccessPath::new(
            ir::EdgeAccessSourcePlan::new(plan).unwrap(),
        )),
        window,
    ))
}

pub(in crate::rules::tests) fn node_access_order_expr(
    plan: ir::NodeAccessPlan,
    ordering: ir::OrderKeys,
) -> logical::LogicalExpr {
    logical::LogicalExpr::AccessOrder(logical::AccessOrder::new(
        logical::AccessPath::Node(logical::NodeAccessPath::new(
            ir::NodeAccessSourcePlan::new(plan).unwrap(),
        )),
        ordering,
    ))
}

pub(in crate::rules::tests) fn edge_access_order_expr(
    plan: ir::EdgeAccessPlan,
    ordering: ir::OrderKeys,
) -> logical::LogicalExpr {
    logical::LogicalExpr::AccessOrder(logical::AccessOrder::new(
        logical::AccessPath::Edge(logical::EdgeAccessPath::new(
            ir::EdgeAccessSourcePlan::new(plan).unwrap(),
        )),
        ordering,
    ))
}

pub(in crate::rules::tests) fn node_access_distinct_expr(
    plan: ir::NodeAccessPlan,
) -> logical::LogicalExpr {
    logical::LogicalExpr::AccessDistinct(logical::AccessDistinct::new(logical::AccessPath::Node(
        logical::NodeAccessPath::new(ir::NodeAccessSourcePlan::new(plan).unwrap()),
    )))
}

pub(in crate::rules::tests) fn edge_access_distinct_expr(
    plan: ir::EdgeAccessPlan,
) -> logical::LogicalExpr {
    logical::LogicalExpr::AccessDistinct(logical::AccessDistinct::new(logical::AccessPath::Edge(
        logical::EdgeAccessPath::new(ir::EdgeAccessSourcePlan::new(plan).unwrap()),
    )))
}

pub(in crate::rules::tests) fn desc_order_keys() -> ir::OrderKeys {
    order_keys_for("age", helix_ast::traversal::Order::Desc)
}

pub(in crate::rules::tests) fn desc_weight_order_keys() -> ir::OrderKeys {
    order_keys_for("weight", helix_ast::traversal::Order::Desc)
}

pub(in crate::rules::tests) fn order_keys_for(
    property: &str,
    order: helix_ast::traversal::Order,
) -> ir::OrderKeys {
    ir::OrderKeys::from(ir::OrderKey {
        property: name(property),
        order,
    })
}

pub(in crate::rules::tests) fn multi_order_keys() -> ir::OrderKeys {
    ir::OrderKeys::new(ir::AtLeast::<_, 1>::from_one_and_rest(
        ir::OrderKey {
            property: name("age"),
            order: helix_ast::traversal::Order::Asc,
        },
        vec![ir::OrderKey {
            property: name("name"),
            order: helix_ast::traversal::Order::Asc,
        }],
    ))
    .unwrap()
}

pub(in crate::rules::tests) fn range_literal(value: i64) -> ir::RangeIndexValue {
    ir::RangeIndexValue::literal(helix_ast::value::PropertyValue::from(value)).unwrap()
}

pub(in crate::rules::tests) fn equality_literal(value: i64) -> ir::IndexValue {
    ir::IndexValue::Literal(
        ir::SecondaryIndexLiteral::new(helix_ast::value::PropertyValue::from(value)).unwrap(),
    )
}

pub(in crate::rules::tests) fn lower_range(value: i64) -> ir::IndexRange {
    ir::IndexRange::Lower {
        lower: ir::IndexBound::Inclusive(range_literal(value)),
    }
}

pub(in crate::rules::tests) fn upper_range(value: i64) -> ir::IndexRange {
    ir::IndexRange::Upper {
        upper: ir::IndexBound::Exclusive(range_literal(value)),
    }
}

pub(in crate::rules::tests) fn node_range_source(
    label: &str,
    property: &str,
    range: ir::IndexRange,
) -> ir::NodeAccessSourcePlan {
    node_range_source_with_direction(
        label,
        property,
        helix_ast::index::RangeIndexDirection::Asc,
        range,
    )
}

pub(in crate::rules::tests) fn node_range_source_with_direction(
    label: &str,
    property: &str,
    direction: helix_ast::index::RangeIndexDirection,
    range: ir::IndexRange,
) -> ir::NodeAccessSourcePlan {
    ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::RangeIndex {
        index: catalog::NodeRangeIndexMeta::try_new(format!("node_range_{label}_{property}"))
            .unwrap(),
        key: range_key(label, property, direction),
        range,
    })
    .unwrap()
}

pub(in crate::rules::tests) fn node_eq_source(
    label: &str,
    property: &str,
    value: ir::IndexValue,
) -> ir::NodeAccessSourcePlan {
    ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::EqualityIndex {
        index: catalog::NodeEqualityIndexMeta::try_new(format!("node_eq_{label}_{property}"))
            .unwrap(),
        key: catalog::ScopedPropertyKey::try_new(label, property).unwrap(),
        value,
    })
    .unwrap()
}

pub(in crate::rules::tests) fn edge_eq_source(
    label: &str,
    property: &str,
    value: ir::IndexValue,
) -> ir::EdgeAccessSourcePlan {
    ir::EdgeAccessSourcePlan::new(ir::EdgeAccessPlan::EqualityIndex {
        index: catalog::EdgeEqualityIndexMeta::try_new(format!("edge_eq_{label}_{property}"))
            .unwrap(),
        key: catalog::ScopedPropertyKey::try_new(label, property).unwrap(),
        value,
    })
    .unwrap()
}

pub(in crate::rules::tests) fn edge_range_source(
    label: &str,
    property: &str,
    range: ir::IndexRange,
) -> ir::EdgeAccessSourcePlan {
    edge_range_source_with_direction(
        label,
        property,
        helix_ast::index::RangeIndexDirection::Asc,
        range,
    )
}

pub(in crate::rules::tests) fn edge_range_source_with_direction(
    label: &str,
    property: &str,
    direction: helix_ast::index::RangeIndexDirection,
    range: ir::IndexRange,
) -> ir::EdgeAccessSourcePlan {
    ir::EdgeAccessSourcePlan::new(ir::EdgeAccessPlan::RangeIndex {
        index: catalog::EdgeRangeIndexMeta::try_new(format!("edge_range_{label}_{property}"))
            .unwrap(),
        key: range_key(label, property, direction),
        range,
    })
    .unwrap()
}

pub(in crate::rules::tests) fn range_key(
    label: &str,
    property: &str,
    direction: helix_ast::index::RangeIndexDirection,
) -> catalog::ScopedPropertyDirectionKey {
    catalog::ScopedPropertyDirectionKey::try_new(label, property, direction).unwrap()
}

pub(in crate::rules::tests) fn element_ids(ids: Vec<u64>) -> ir::ElementIds {
    ir::ElementIds::new(ir::AtLeast::<_, 1>::try_from_vec(ids).unwrap()).unwrap()
}

pub(in crate::rules::tests) fn search_limit(value: usize) -> ir::SearchLimitPlan {
    ir::SearchLimitPlan::Literal(std::num::NonZeroUsize::new(value).unwrap())
}

pub(in crate::rules::tests) fn node_vector_search(k: ir::SearchLimitPlan) -> ir::NodeAccessPlan {
    ir::NodeAccessPlan::VectorSearch {
        key: catalog::NodeSearchIndexKey::try_new("User", "embedding").unwrap(),
        index: ir::SearchIndexPlan {
            index_id: name("user_embedding"),
            tenant: ir::SearchTenantPlan::Unscoped,
        },
        query_vector: ir::VectorQueryInputPlan::new(helix_ast::value::PropertyInput::from(
            helix_ast::value::PropertyValue::F32Array(vec![0.5]),
        ))
        .unwrap(),
        k,
    }
}

pub(in crate::rules::tests) fn edge_vector_search(k: ir::SearchLimitPlan) -> ir::EdgeAccessPlan {
    ir::EdgeAccessPlan::VectorSearch {
        key: catalog::EdgeSearchIndexKey::try_new("LIKES", "embedding").unwrap(),
        index: ir::SearchIndexPlan {
            index_id: name("likes_embedding"),
            tenant: ir::SearchTenantPlan::Unscoped,
        },
        query_vector: ir::VectorQueryInputPlan::new(helix_ast::value::PropertyInput::from(
            helix_ast::value::PropertyValue::F32Array(vec![0.5]),
        ))
        .unwrap(),
        k,
    }
}

pub(in crate::rules::tests) fn edge_text_search(k: ir::SearchLimitPlan) -> ir::EdgeAccessPlan {
    ir::EdgeAccessPlan::TextSearch {
        key: catalog::EdgeSearchIndexKey::try_new("LIKES", "comment").unwrap(),
        index: ir::SearchIndexPlan {
            index_id: name("likes_comment"),
            tenant: ir::SearchTenantPlan::Unscoped,
        },
        query_text: ir::TextQueryInputPlan::new(helix_ast::value::PropertyInput::from(
            helix_ast::value::PropertyValue::from("great"),
        ))
        .unwrap(),
        k,
    }
}

pub(in crate::rules::tests) fn limit(count: usize) -> logical::PureLogicalOp {
    logical::PureLogicalOp::Limit {
        count: ir::StreamBoundPlan::Literal(count),
    }
}

pub(in crate::rules::tests) fn skip(count: usize) -> logical::PureLogicalOp {
    logical::PureLogicalOp::Skip {
        count: ir::StreamBoundPlan::Literal(count),
    }
}

pub(in crate::rules::tests) fn range(start: usize, end: usize) -> logical::PureLogicalOp {
    logical::PureLogicalOp::Range {
        range: ir::StreamRangePlan::Literal(ir::StreamLiteralRange::new(start, end).unwrap()),
    }
}

pub(in crate::rules::tests) fn stream_alternative(
    op: logical::PureLogicalOp,
    storage: &cost::StorageCostProfile,
) -> physical::PhysicalAlternative {
    let expr = logical::LogicalExpr::Pure(op);
    physical_alternative(
        StreamImplementationRule::default().apply(optimizer::RuleInput {
            expr: &expr,
            storage,
            indexes: empty_indexes(),
            planner_limits: default_planner_limits(),
            stats: default_stats(),
        }),
    )
}

pub(in crate::rules::tests) fn empty_indexes() -> &'static catalog::IndexCatalogSnapshot {
    static EMPTY: std::sync::OnceLock<catalog::IndexCatalogSnapshot> = std::sync::OnceLock::new();
    EMPTY.get_or_init(catalog::IndexCatalogSnapshot::default)
}

pub(in crate::rules::tests) fn default_planner_limits() -> &'static crate::context::PlannerLimits {
    static LIMITS: std::sync::OnceLock<crate::context::PlannerLimits> = std::sync::OnceLock::new();
    LIMITS.get_or_init(crate::context::PlannerLimits::default)
}

pub(in crate::rules::tests) fn default_stats() -> &'static crate::context::StatsSnapshot {
    static STATS: std::sync::OnceLock<crate::context::StatsSnapshot> = std::sync::OnceLock::new();
    STATS.get_or_init(crate::context::StatsSnapshot::default)
}
