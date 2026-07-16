use super::RuleSchedule;
use crate::{catalog, ir, logical, optimizer, properties, rules};

struct TestRule {
    metadata: rules::RuleMetadata,
}

impl TestRule {
    fn new(id: &'static str, applicability: rules::RuleApplicability) -> Self {
        Self {
            metadata: rules::RuleMetadata::new(
                rules::RuleId::new(id).unwrap(),
                rules::RuleKind::Exploration,
            )
            .with_applicability(applicability),
        }
    }
}

impl optimizer::OptimizerRule for TestRule {
    fn metadata(&self) -> &rules::RuleMetadata {
        &self.metadata
    }

    fn apply(&self, _input: optimizer::RuleInput<'_>) -> optimizer::RuleResult {
        optimizer::RuleResult::NotApplicable
    }
}

fn rule_ids<'a>(rules: impl Iterator<Item = &'a dyn optimizer::OptimizerRule>) -> Vec<&'a str> {
    rules.map(|rule| rule.metadata().id.as_ref()).collect()
}

fn schedule(rules: Vec<&dyn optimizer::OptimizerRule>) -> RuleSchedule<'_> {
    RuleSchedule::new(
        optimizer::OptimizerRuleRegistry::try_from_rules(rules)
            .expect("test rule registries must be non-empty with unique IDs"),
    )
}

fn pure_expr(kind: logical::PureLogicalOpKind) -> logical::LogicalExpr {
    let op = match kind {
        logical::PureLogicalOpKind::NoOp => logical::PureLogicalOp::NoOp,
        logical::PureLogicalOpKind::Empty => logical::PureLogicalOp::Empty,
        logical::PureLogicalOpKind::Source => logical::PureLogicalOp::Source {
            element: properties::ElementKind::Node,
        },
        logical::PureLogicalOpKind::Filter => logical::PureLogicalOp::Filter {
            predicate: ir::PredicatePlan::new(helix_ast::expr::Predicate::eq("age", 42)).unwrap(),
        },
        logical::PureLogicalOpKind::Limit => logical::PureLogicalOp::Limit {
            count: ir::StreamBoundPlan::Literal(1),
        },
        logical::PureLogicalOpKind::Order => logical::PureLogicalOp::Order {
            ordering: properties::RequiredOrdering::Any,
        },
        logical::PureLogicalOpKind::Skip => logical::PureLogicalOp::Skip {
            count: ir::StreamBoundPlan::Literal(1),
        },
        logical::PureLogicalOpKind::Range => logical::PureLogicalOp::Range {
            range: ir::StreamRangePlan::Literal(ir::StreamLiteralRange::new(0, 1).unwrap()),
        },
        logical::PureLogicalOpKind::Distinct => logical::PureLogicalOp::Distinct,
        logical::PureLogicalOpKind::Expand => logical::PureLogicalOp::Expand {
            element: properties::ElementKind::Edge,
        },
        logical::PureLogicalOpKind::Project => logical::PureLogicalOp::Project,
        logical::PureLogicalOpKind::Aggregate => logical::PureLogicalOp::Aggregate,
        logical::PureLogicalOpKind::Variable => logical::PureLogicalOp::Variable,
        logical::PureLogicalOpKind::Reserved => logical::PureLogicalOp::Reserved,
    };
    logical::LogicalExpr::Pure(op)
}

fn pure_pipeline_expr(ops: Vec<logical::PureLogicalOp>) -> logical::LogicalExpr {
    logical::LogicalExpr::PurePipeline(logical::PurePipeline::new(
        ir::AtLeast::<_, 1>::try_from_vec(ops).unwrap(),
    ))
}

fn stream_op(kind: logical::StreamPipelineOpKind) -> logical::StreamPipelineOp {
    match kind {
        logical::StreamPipelineOpKind::Filter => logical::StreamPipelineOp::Filter {
            predicate: ir::PredicatePlan::new(helix_ast::expr::Predicate::eq("age", 42)).unwrap(),
        },
        logical::StreamPipelineOpKind::Window => logical::StreamPipelineOp::Window {
            window: logical::AccessWindowRange::new(0, Some(1)).unwrap(),
        },
        logical::StreamPipelineOpKind::Limit => logical::StreamPipelineOp::Limit {
            count: ir::StreamBoundPlan::Literal(1),
        },
        logical::StreamPipelineOpKind::Skip => logical::StreamPipelineOp::Skip {
            count: ir::StreamBoundPlan::Literal(1),
        },
        logical::StreamPipelineOpKind::Range => logical::StreamPipelineOp::Range {
            range: ir::StreamRangePlan::Literal(ir::StreamLiteralRange::new(0, 1).unwrap()),
        },
        logical::StreamPipelineOpKind::Order => logical::StreamPipelineOp::Order {
            ordering: ir::OrderKeys::from(ir::OrderKey {
                property: ir::NonEmptyString::new("age").unwrap(),
                order: helix_ast::traversal::Order::Asc,
            }),
        },
        logical::StreamPipelineOpKind::Expand => logical::StreamPipelineOp::Expand {
            plan: ir::ExpandPlan {
                direction: ir::ExpandDirection::Out,
                output: ir::ExpandOutput::Edges,
                label: ir::ExpandLabelPlan::Any,
            },
        },
        logical::StreamPipelineOpKind::Variable => logical::StreamPipelineOp::Variable {
            op: logical::PureStreamVariableOp::Within(ir::NonEmptyString::new("allowed").unwrap()),
        },
        logical::StreamPipelineOpKind::VariableWrite => logical::StreamPipelineOp::VariableWrite {
            op: logical::StreamVariableWriteOp::Store(ir::NonEmptyString::new("seen").unwrap()),
        },
        logical::StreamPipelineOpKind::Distinct => logical::StreamPipelineOp::Distinct,
    }
}

fn access_pipeline_expr(kind: logical::StreamPipelineOpKind) -> logical::LogicalExpr {
    let access = logical::AccessPath::Node(logical::NodeAccessPath::new(
        ir::NodeAccessSourcePlan::from_unfiltered(ir::NodeAccessPlan::AllScan),
    ));
    logical::LogicalExpr::AccessPipeline(
        logical::AccessPipeline::new(access, ir::AtLeast::<_, 1>::from_one(stream_op(kind)))
            .unwrap(),
    )
}

fn empty_access_pipeline_expr(kind: logical::StreamPipelineOpKind) -> logical::LogicalExpr {
    let access = logical::AccessPath::Node(logical::NodeAccessPath::new(
        ir::NodeAccessSourcePlan::from_unfiltered(ir::NodeAccessPlan::Empty),
    ));
    logical::LogicalExpr::AccessPipeline(
        logical::AccessPipeline::new(access, ir::AtLeast::<_, 1>::from_one(stream_op(kind)))
            .unwrap(),
    )
}

fn adjacent_filter_pipeline_expr() -> logical::LogicalExpr {
    let access = logical::AccessPath::Node(logical::NodeAccessPath::new(
        ir::NodeAccessSourcePlan::from_unfiltered(ir::NodeAccessPlan::AllScan),
    ));
    logical::LogicalExpr::AccessPipeline(
        logical::AccessPipeline::new(
            access,
            ir::AtLeast::<_, 1>::from_one_and_rest(
                stream_op(logical::StreamPipelineOpKind::Filter),
                vec![stream_op(logical::StreamPipelineOpKind::Filter)],
            ),
        )
        .unwrap(),
    )
}

fn access_source_expr(source: ir::NodeAccessPlan) -> logical::LogicalExpr {
    logical::LogicalExpr::AccessPath(logical::AccessPath::Node(logical::NodeAccessPath::new(
        ir::NodeAccessSourcePlan::from_unfiltered(source),
    )))
}

fn root_branch_expr(input: logical::LogicalExpr) -> logical::LogicalExpr {
    logical::LogicalExpr::RootBranch(logical::RootBranch::new(
        input,
        ir::BranchPlan::Optional(Box::new(access_source_expr(ir::NodeAccessPlan::AllScan))),
    ))
}

fn root_repeat_expr(input: logical::LogicalExpr) -> logical::LogicalExpr {
    logical::LogicalExpr::RootRepeat(logical::RootRepeat::new(
        input,
        ir::RepeatPlan {
            body: Box::new(access_source_expr(ir::NodeAccessPlan::AllScan)),
            stop: ir::RepeatStopPlan::MaxDepthOnly,
            emit: ir::RepeatEmitPlan::None,
            max_depth: std::num::NonZeroUsize::new(2).unwrap(),
        },
    ))
}

fn node_source(source: ir::NodeAccessPlan) -> ir::NodeAccessSourcePlan {
    ir::NodeAccessSourcePlan::from_unfiltered(source)
}

fn node_label_source(label: &str) -> ir::NodeAccessSourcePlan {
    node_source(ir::NodeAccessPlan::LabelScan {
        label: ir::NonEmptyString::new(label).unwrap(),
    })
}

fn access_filter_expr(
    source: ir::NodeAccessPlan,
    predicate: helix_ast::expr::Predicate,
) -> logical::LogicalExpr {
    logical::LogicalExpr::AccessFilter(logical::AccessFilter::new(
        logical::AccessPath::Node(logical::NodeAccessPath::new(
            ir::NodeAccessSourcePlan::from_unfiltered(source),
        )),
        ir::PredicatePlan::new(predicate).unwrap(),
    ))
}

fn access_window_expr(
    source: ir::NodeAccessPlan,
    window: logical::AccessWindowRange,
) -> logical::LogicalExpr {
    logical::LogicalExpr::AccessWindow(logical::AccessWindow::new(
        logical::AccessPath::Node(logical::NodeAccessPath::new(
            ir::NodeAccessSourcePlan::from_unfiltered(source),
        )),
        window,
    ))
}

fn order_key(property: &str, order: helix_ast::traversal::Order) -> ir::OrderKey {
    ir::OrderKey {
        property: ir::NonEmptyString::new(property).unwrap(),
        order,
    }
}

fn order_keys(property: &str, order: helix_ast::traversal::Order) -> ir::OrderKeys {
    ir::OrderKeys::from(order_key(property, order))
}

fn access_order_expr(source: ir::NodeAccessPlan, ordering: ir::OrderKeys) -> logical::LogicalExpr {
    logical::LogicalExpr::AccessOrder(logical::AccessOrder::new(
        logical::AccessPath::Node(logical::NodeAccessPath::new(
            ir::NodeAccessSourcePlan::from_unfiltered(source),
        )),
        ordering,
    ))
}

fn access_distinct_expr(source: ir::NodeAccessPlan) -> logical::LogicalExpr {
    logical::LogicalExpr::AccessDistinct(logical::AccessDistinct::new(logical::AccessPath::Node(
        logical::NodeAccessPath::new(ir::NodeAccessSourcePlan::from_unfiltered(source)),
    )))
}

fn node_range_source(
    property: &str,
    direction: helix_ast::index::RangeIndexDirection,
) -> ir::NodeAccessPlan {
    ir::NodeAccessPlan::RangeIndex {
        index: catalog::NodeRangeIndexMeta::try_new("node_range").unwrap(),
        key: catalog::ScopedPropertyDirectionKey::try_new("User", property, direction).unwrap(),
        range: ir::IndexRange::All,
    }
}

fn node_range_access_source(property: &str, range: ir::IndexRange) -> ir::NodeAccessSourcePlan {
    node_source(ir::NodeAccessPlan::RangeIndex {
        index: catalog::NodeRangeIndexMeta::try_new(format!("node_range_User_{property}")).unwrap(),
        key: catalog::ScopedPropertyDirectionKey::try_new(
            "User",
            property,
            helix_ast::index::RangeIndexDirection::Asc,
        )
        .unwrap(),
        range,
    })
}

fn node_equality_source(property: &str, value: i64) -> ir::NodeAccessSourcePlan {
    node_source(ir::NodeAccessPlan::EqualityIndex {
        index: catalog::NodeEqualityIndexMeta::try_new(format!("node_eq_User_{property}_{value}"))
            .unwrap(),
        key: catalog::ScopedPropertyKey::try_new("User", property).unwrap(),
        value: ir::IndexValue::Literal(
            ir::SecondaryIndexLiteral::new(helix_ast::value::PropertyValue::from(value)).unwrap(),
        ),
    })
}

#[test]
fn rule_schedule_preserves_registry_order_by_logical_family() {
    let access = TestRule::new(
        "access",
        rules::RuleApplicability::only(logical::LogicalExprKind::AccessFilter),
    );
    let any = TestRule::new("any", rules::RuleApplicability::Any);
    let pure = TestRule::new(
        "pure",
        rules::RuleApplicability::only(logical::LogicalExprKind::Pure),
    );
    let both = TestRule::new(
        "both",
        rules::RuleApplicability::any_of(ir::AtLeast::<_, 1>::from_one_and_rest(
            logical::LogicalExprKind::AccessFilter,
            vec![
                logical::LogicalExprKind::Pure,
                logical::LogicalExprKind::Pure,
            ],
        )),
    );
    let schedule = schedule(vec![&access, &any, &pure, &both]);

    assert_eq!(
        rule_ids(schedule.rules_for_kind(logical::LogicalExprKind::Pure)),
        ["any", "pure", "both"]
    );
    assert_eq!(
        rule_ids(schedule.rules_for_kind(logical::LogicalExprKind::AccessFilter)),
        ["access", "any", "both"]
    );
    assert_eq!(
        rule_ids(schedule.rules_for_kind(logical::LogicalExprKind::RootPipeline)),
        ["any"]
    );
}

#[test]
fn rule_schedule_covers_every_logical_family() {
    let any = TestRule::new("any", rules::RuleApplicability::Any);
    let schedule = schedule(vec![&any]);

    for kind in logical::LogicalExprKind::ALL {
        assert_eq!(rule_ids(schedule.rules_for_kind(kind)), ["any"]);
    }
}

#[test]
fn rule_schedule_routes_pure_ops_without_scanning_all_pure_rules() {
    let any = TestRule::new("any", rules::RuleApplicability::Any);
    let filter = TestRule::new(
        "filter",
        rules::RuleApplicability::pure_only(logical::PureLogicalOpKind::Filter),
    );
    let pure_broad = TestRule::new(
        "pure_broad",
        rules::RuleApplicability::only(logical::LogicalExprKind::Pure),
    );
    let source_or_order = TestRule::new(
        "source_or_order",
        rules::RuleApplicability::pure_any_of(ir::AtLeast::<_, 1>::from_one_and_rest(
            logical::PureLogicalOpKind::Source,
            vec![logical::PureLogicalOpKind::Order],
        )),
    );
    let schedule = schedule(vec![&any, &filter, &pure_broad, &source_or_order]);

    assert_eq!(
        rule_ids(schedule.rules_for_expr(&pure_expr(logical::PureLogicalOpKind::Filter))),
        ["any", "filter", "pure_broad"]
    );
    assert_eq!(
        rule_ids(schedule.rules_for_expr(&pure_expr(logical::PureLogicalOpKind::Source))),
        ["any", "pure_broad", "source_or_order"]
    );
    assert_eq!(
        rule_ids(schedule.rules_for_expr(&pure_expr(logical::PureLogicalOpKind::Order))),
        ["any", "pure_broad", "source_or_order"]
    );
    assert_eq!(
        rule_ids(schedule.rules_for_expr(&pure_expr(logical::PureLogicalOpKind::Distinct))),
        ["any", "pure_broad"]
    );
}

#[test]
fn rule_schedule_routes_pure_pipeline_feature_candidates() {
    let any = TestRule::new("any", rules::RuleApplicability::Any);
    let local_simplification = TestRule::new(
        "local_simplification",
        rules::RuleApplicability::pure_pipeline_local_simplification(),
    );
    let pipeline_broad = TestRule::new(
        "pipeline_broad",
        rules::RuleApplicability::only(logical::LogicalExprKind::PurePipeline),
    );
    let window_composition = TestRule::new(
        "window_composition",
        rules::RuleApplicability::pure_pipeline_static_window_composition(),
    );
    let schedule = schedule(vec![
        &any,
        &local_simplification,
        &pipeline_broad,
        &window_composition,
    ]);
    let source = logical::PureLogicalOp::Source {
        element: properties::ElementKind::Node,
    };
    let limit = |count| logical::PureLogicalOp::Limit {
        count: ir::StreamBoundPlan::Literal(count),
    };
    let skip = |count| logical::PureLogicalOp::Skip {
        count: ir::StreamBoundPlan::Literal(count),
    };

    assert_eq!(
        rule_ids(schedule.rules_for_expr(&pure_pipeline_expr(vec![source.clone(), limit(5),]))),
        ["any", "pipeline_broad"]
    );
    assert_eq!(
        rule_ids(schedule.rules_for_expr(&pure_pipeline_expr(vec![
            logical::PureLogicalOp::NoOp,
            source.clone(),
        ]))),
        ["any", "local_simplification", "pipeline_broad"]
    );
    assert_eq!(
        rule_ids(schedule.rules_for_expr(&pure_pipeline_expr(vec![skip(2), limit(5)]))),
        ["any", "pipeline_broad", "window_composition"]
    );
    assert_eq!(
        rule_ids(schedule.rules_for_expr(&pure_pipeline_expr(vec![
            logical::PureLogicalOp::NoOp,
            skip(2),
            limit(5),
        ]))),
        [
            "any",
            "local_simplification",
            "pipeline_broad",
            "window_composition"
        ]
    );
}

#[test]
fn rule_schedule_routes_access_paths_by_source_kind() {
    let any = TestRule::new("any", rules::RuleApplicability::Any);
    let access_broad = TestRule::new(
        "access_broad",
        rules::RuleApplicability::only(logical::LogicalExprKind::AccessPath),
    );
    let intersection = TestRule::new(
        "intersection",
        rules::RuleApplicability::access_source_only(logical::AccessSourceKind::Intersection),
    );
    let union = TestRule::new(
        "union",
        rules::RuleApplicability::access_source_only(logical::AccessSourceKind::Union),
    );
    let any_set = TestRule::new(
        "any_set",
        rules::RuleApplicability::access_source_any_of(ir::AtLeast::<_, 1>::from_one_and_rest(
            logical::AccessSourceKind::Intersection,
            vec![logical::AccessSourceKind::Union],
        )),
    );
    let schedule = schedule(vec![&any, &intersection, &access_broad, &union, &any_set]);
    let empty = ir::NodeAccessSourcePlan::from_unfiltered(ir::NodeAccessPlan::Empty);
    let scan = access_source_expr(ir::NodeAccessPlan::AllScan);
    let intersect = access_source_expr(ir::NodeAccessPlan::Intersect(
        ir::AtLeast::<_, 2>::from_pair(empty.clone(), empty.clone()),
    ));
    let union = access_source_expr(ir::NodeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
        empty.clone(),
        empty,
    )));

    assert_eq!(
        rule_ids(schedule.rules_for_expr(&scan)),
        ["any", "access_broad"]
    );
    assert_eq!(
        rule_ids(schedule.rules_for_expr(&intersect)),
        ["any", "intersection", "access_broad", "any_set"]
    );
    assert_eq!(
        rule_ids(schedule.rules_for_expr(&union)),
        ["any", "access_broad", "union", "any_set"]
    );
}

#[test]
fn rule_schedule_routes_access_set_feature_candidates() {
    let any = TestRule::new("any", rules::RuleApplicability::Any);
    let canonicalization = TestRule::new(
        "canonicalization",
        rules::RuleApplicability::access_set_canonicalization_candidate(),
    );
    let subsumption = TestRule::new(
        "subsumption",
        rules::RuleApplicability::access_set_subsumption_candidate(),
    );
    let access_broad = TestRule::new(
        "access_broad",
        rules::RuleApplicability::only(logical::LogicalExprKind::AccessPath),
    );
    let set_source = TestRule::new(
        "set_source",
        rules::RuleApplicability::access_source_any_of(ir::AtLeast::<_, 1>::from_one_and_rest(
            logical::AccessSourceKind::Intersection,
            vec![logical::AccessSourceKind::Union],
        )),
    );
    let schedule = schedule(vec![
        &any,
        &canonicalization,
        &subsumption,
        &access_broad,
        &set_source,
    ]);
    let ordinary_union = access_source_expr(ir::NodeAccessPlan::Union(
        ir::AtLeast::<_, 2>::from_pair(node_label_source("User"), node_label_source("Account")),
    ));
    let nested_union =
        access_source_expr(ir::NodeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
            node_label_source("User"),
            node_source(ir::NodeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
                node_label_source("Account"),
                node_label_source("Org"),
            ))),
        )));
    let subsumed_union =
        access_source_expr(ir::NodeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
            node_source(ir::NodeAccessPlan::AllScan),
            node_label_source("User"),
        )));

    assert_eq!(
        rule_ids(schedule.rules_for_expr(&ordinary_union)),
        ["any", "access_broad", "set_source"]
    );
    assert_eq!(
        rule_ids(schedule.rules_for_expr(&nested_union)),
        ["any", "canonicalization", "access_broad", "set_source"]
    );
    assert_eq!(
        rule_ids(schedule.rules_for_expr(&subsumed_union)),
        ["any", "subsumption", "access_broad", "set_source"]
    );
}

#[test]
fn rule_schedule_routes_access_source_algebra_feature_candidates() {
    let any = TestRule::new("any", rules::RuleApplicability::Any);
    let range_intersection = TestRule::new(
        "range_intersection",
        rules::RuleApplicability::access_range_intersection_candidate(),
    );
    let equality_range_intersection = TestRule::new(
        "equality_range_intersection",
        rules::RuleApplicability::access_equality_range_intersection_candidate(),
    );
    let equality_range_union = TestRule::new(
        "equality_range_union",
        rules::RuleApplicability::access_equality_range_union_candidate(),
    );
    let contradiction = TestRule::new(
        "contradiction",
        rules::RuleApplicability::access_contradiction_candidate(),
    );
    let intersection_broad = TestRule::new(
        "intersection_broad",
        rules::RuleApplicability::access_source_only(logical::AccessSourceKind::Intersection),
    );
    let union_broad = TestRule::new(
        "union_broad",
        rules::RuleApplicability::access_source_only(logical::AccessSourceKind::Union),
    );
    let schedule = schedule(vec![
        &any,
        &range_intersection,
        &equality_range_intersection,
        &equality_range_union,
        &contradiction,
        &intersection_broad,
        &union_broad,
    ]);

    let ordinary_intersection =
        access_source_expr(ir::NodeAccessPlan::Intersect(
            ir::AtLeast::<_, 2>::from_pair(node_label_source("User"), node_label_source("Account")),
        ));
    let range_intersection_expr = access_source_expr(ir::NodeAccessPlan::Intersect(ir::AtLeast::<
        _,
        2,
    >::from_pair(
        node_range_access_source("age", ir::IndexRange::All),
        node_range_access_source("age", ir::IndexRange::All),
    )));
    let equality_union = node_source(ir::NodeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
        node_equality_source("age", 18),
        node_equality_source("age", 21),
    )));
    let equality_range_intersection_expr = access_source_expr(ir::NodeAccessPlan::Intersect(
        ir::AtLeast::<_, 2>::from_pair(
            node_range_access_source("age", ir::IndexRange::All),
            equality_union,
        ),
    ));
    let equality_range_union_expr =
        access_source_expr(ir::NodeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
            node_range_access_source("age", ir::IndexRange::All),
            node_equality_source("age", 42),
        )));
    let contradiction_expr = access_source_expr(ir::NodeAccessPlan::Intersect(
        ir::AtLeast::<_, 2>::from_pair(
            node_equality_source("age", 18),
            node_equality_source("age", 21),
        ),
    ));

    assert_eq!(
        rule_ids(schedule.rules_for_expr(&ordinary_intersection)),
        ["any", "intersection_broad"]
    );
    assert_eq!(
        rule_ids(schedule.rules_for_expr(&range_intersection_expr)),
        ["any", "range_intersection", "intersection_broad"]
    );
    assert_eq!(
        rule_ids(schedule.rules_for_expr(&equality_range_intersection_expr)),
        ["any", "equality_range_intersection", "intersection_broad"]
    );
    assert_eq!(
        rule_ids(schedule.rules_for_expr(&equality_range_union_expr)),
        ["any", "equality_range_union", "union_broad"]
    );
    assert_eq!(
        rule_ids(schedule.rules_for_expr(&contradiction_expr)),
        ["any", "contradiction", "intersection_broad"]
    );
}

#[test]
fn rule_schedule_routes_access_window_rewrite_candidates() {
    let any = TestRule::new("any", rules::RuleApplicability::Any);
    let rewrite = TestRule::new(
        "rewrite",
        rules::RuleApplicability::access_window_rewrite_candidate(),
    );
    let window_broad = TestRule::new(
        "window_broad",
        rules::RuleApplicability::only(logical::LogicalExprKind::AccessWindow),
    );
    let schedule = schedule(vec![&any, &rewrite, &window_broad]);
    let point_ids =
        ir::ElementIds::new(ir::AtLeast::<_, 1>::from_one_and_rest(7, vec![9])).unwrap();

    assert_eq!(
        rule_ids(schedule.rules_for_expr(&access_window_expr(
            ir::NodeAccessPlan::LabelScan {
                label: ir::NonEmptyString::new("User").unwrap(),
            },
            logical::AccessWindowRange::new(1, Some(3)).unwrap(),
        ))),
        ["any", "window_broad"]
    );
    assert_eq!(
        rule_ids(schedule.rules_for_expr(&access_window_expr(
            ir::NodeAccessPlan::PointIds { ids: point_ids },
            logical::AccessWindowRange::new(1, Some(2)).unwrap(),
        ))),
        ["any", "rewrite", "window_broad"]
    );
    assert_eq!(
        rule_ids(schedule.rules_for_expr(&access_window_expr(
            ir::NodeAccessPlan::AllScan,
            logical::AccessWindowRange::new(3, Some(3)).unwrap(),
        ))),
        ["any", "rewrite", "window_broad"]
    );
}

#[test]
fn rule_schedule_routes_access_filter_feature_candidates() {
    let any = TestRule::new("any", rules::RuleApplicability::Any);
    let simplification = TestRule::new(
        "simplification",
        rules::RuleApplicability::access_filter_simplification_candidate(),
    );
    let index = TestRule::new(
        "index",
        rules::RuleApplicability::access_filter_index_candidate(),
    );
    let filter_broad = TestRule::new(
        "filter_broad",
        rules::RuleApplicability::only(logical::LogicalExprKind::AccessFilter),
    );
    let schedule = schedule(vec![&any, &simplification, &index, &filter_broad]);
    let label_scan = ir::NodeAccessPlan::LabelScan {
        label: ir::NonEmptyString::new("User").unwrap(),
    };

    assert_eq!(
        rule_ids(schedule.rules_for_expr(&access_filter_expr(
            label_scan.clone(),
            helix_ast::expr::Predicate::eq("$label", "Admin"),
        ))),
        ["any", "simplification", "filter_broad"]
    );
    assert_eq!(
        rule_ids(schedule.rules_for_expr(&access_filter_expr(
            label_scan.clone(),
            helix_ast::expr::Predicate::eq("age", 42),
        ))),
        ["any", "index", "filter_broad"]
    );
    assert_eq!(
        rule_ids(schedule.rules_for_expr(&access_filter_expr(
            ir::NodeAccessPlan::AllScan,
            helix_ast::expr::Predicate::eq("age", 42),
        ))),
        ["any", "filter_broad"]
    );
    assert_eq!(
        rule_ids(schedule.rules_for_expr(&access_filter_expr(
            label_scan,
            helix_ast::expr::Predicate::contains("bio", "rust"),
        ))),
        ["any", "filter_broad"]
    );
}

#[test]
fn rule_schedule_routes_access_order_feature_candidates() {
    let any = TestRule::new("any", rules::RuleApplicability::Any);
    let range_direction = TestRule::new(
        "range_direction",
        rules::RuleApplicability::access_order_range_direction_candidate(),
    );
    let elision = TestRule::new(
        "elision",
        rules::RuleApplicability::access_order_elision_candidate(),
    );
    let order_broad = TestRule::new(
        "order_broad",
        rules::RuleApplicability::only(logical::LogicalExprKind::AccessOrder),
    );
    let schedule = schedule(vec![&any, &range_direction, &elision, &order_broad]);
    let point_ids = ir::ElementIds::new(ir::AtLeast::<_, 1>::from_one(7)).unwrap();

    assert_eq!(
        rule_ids(schedule.rules_for_expr(&access_order_expr(
            ir::NodeAccessPlan::LabelScan {
                label: ir::NonEmptyString::new("User").unwrap(),
            },
            order_keys("age", helix_ast::traversal::Order::Asc),
        ))),
        ["any", "order_broad"]
    );
    assert_eq!(
        rule_ids(schedule.rules_for_expr(&access_order_expr(
            ir::NodeAccessPlan::PointIds { ids: point_ids },
            order_keys("age", helix_ast::traversal::Order::Asc),
        ))),
        ["any", "elision", "order_broad"]
    );
    assert_eq!(
        rule_ids(schedule.rules_for_expr(&access_order_expr(
            node_range_source("age", helix_ast::index::RangeIndexDirection::Asc),
            order_keys("age", helix_ast::traversal::Order::Asc),
        ))),
        ["any", "elision", "order_broad"]
    );
    assert_eq!(
        rule_ids(schedule.rules_for_expr(&access_order_expr(
            node_range_source("age", helix_ast::index::RangeIndexDirection::Asc),
            order_keys("age", helix_ast::traversal::Order::Desc),
        ))),
        ["any", "range_direction", "order_broad"]
    );
}

#[test]
fn rule_schedule_routes_access_distinct_noop_candidates() {
    let any = TestRule::new("any", rules::RuleApplicability::Any);
    let noop = TestRule::new(
        "noop",
        rules::RuleApplicability::access_distinct_noop_candidate(),
    );
    let distinct_broad = TestRule::new(
        "distinct_broad",
        rules::RuleApplicability::only(logical::LogicalExprKind::AccessDistinct),
    );
    let schedule = schedule(vec![&any, &noop, &distinct_broad]);
    let point_ids =
        ir::ElementIds::new(ir::AtLeast::<_, 1>::from_one_and_rest(7, vec![9])).unwrap();

    assert_eq!(
        rule_ids(schedule.rules_for_expr(&access_distinct_expr(ir::NodeAccessPlan::AllScan))),
        ["any", "distinct_broad"]
    );
    assert_eq!(
        rule_ids(
            schedule.rules_for_expr(&access_distinct_expr(ir::NodeAccessPlan::PointIds {
                ids: point_ids
            }))
        ),
        ["any", "noop", "distinct_broad"]
    );
}

#[test]
fn rule_schedule_routes_root_control_flow_by_empty_input() {
    let any = TestRule::new("any", rules::RuleApplicability::Any);
    let empty = TestRule::new(
        "empty",
        rules::RuleApplicability::root_control_flow_empty_input_candidate(),
    );
    let branch_impl = TestRule::new(
        "branch_impl",
        rules::RuleApplicability::root_branch_implementation_candidate(),
    );
    let repeat_impl = TestRule::new(
        "repeat_impl",
        rules::RuleApplicability::root_repeat_implementation_candidate(),
    );
    let branch_broad = TestRule::new(
        "branch_broad",
        rules::RuleApplicability::only(logical::LogicalExprKind::RootBranch),
    );
    let repeat_broad = TestRule::new(
        "repeat_broad",
        rules::RuleApplicability::only(logical::LogicalExprKind::RootRepeat),
    );
    let schedule = schedule(vec![
        &any,
        &empty,
        &branch_impl,
        &repeat_impl,
        &branch_broad,
        &repeat_broad,
    ]);
    let empty_input = access_source_expr(ir::NodeAccessPlan::Empty);
    let non_empty_input = access_source_expr(ir::NodeAccessPlan::AllScan);

    assert_eq!(
        rule_ids(schedule.rules_for_expr(&root_branch_expr(empty_input.clone()))),
        ["any", "empty", "branch_broad"]
    );
    assert_eq!(
        rule_ids(schedule.rules_for_expr(&root_branch_expr(non_empty_input.clone()))),
        ["any", "branch_impl", "branch_broad"]
    );
    assert_eq!(
        rule_ids(schedule.rules_for_expr(&root_repeat_expr(empty_input))),
        ["any", "empty", "repeat_broad"]
    );
    assert_eq!(
        rule_ids(schedule.rules_for_expr(&root_repeat_expr(non_empty_input))),
        ["any", "repeat_impl", "repeat_broad"]
    );
}

#[test]
fn rule_schedule_covers_every_pure_op_family() {
    let any = TestRule::new("any", rules::RuleApplicability::Any);
    let stream = TestRule::new(
        "stream",
        rules::RuleApplicability::pure_any_of(ir::AtLeast::<_, 1>::from_one_and_rest(
            logical::PureLogicalOpKind::Limit,
            vec![
                logical::PureLogicalOpKind::Skip,
                logical::PureLogicalOpKind::Range,
                logical::PureLogicalOpKind::Distinct,
                logical::PureLogicalOpKind::Expand,
                logical::PureLogicalOpKind::Project,
                logical::PureLogicalOpKind::Aggregate,
                logical::PureLogicalOpKind::Variable,
                logical::PureLogicalOpKind::Reserved,
            ],
        )),
    );
    let schedule = schedule(vec![&any, &stream]);

    for kind in logical::PureLogicalOpKind::ALL {
        let expected = match kind {
            logical::PureLogicalOpKind::Limit
            | logical::PureLogicalOpKind::Skip
            | logical::PureLogicalOpKind::Range
            | logical::PureLogicalOpKind::Distinct
            | logical::PureLogicalOpKind::Expand
            | logical::PureLogicalOpKind::Project
            | logical::PureLogicalOpKind::Aggregate
            | logical::PureLogicalOpKind::Variable
            | logical::PureLogicalOpKind::Reserved => vec!["any", "stream"],
            logical::PureLogicalOpKind::NoOp
            | logical::PureLogicalOpKind::Empty
            | logical::PureLogicalOpKind::Source
            | logical::PureLogicalOpKind::Filter
            | logical::PureLogicalOpKind::Order => vec!["any"],
        };
        assert_eq!(
            rule_ids(schedule.rules_for_expr(&pure_expr(kind))),
            expected
        );
    }
}

#[test]
fn rule_schedule_routes_access_pipelines_by_head_op() {
    let any = TestRule::new("any", rules::RuleApplicability::Any);
    let pipeline_broad = TestRule::new(
        "pipeline_broad",
        rules::RuleApplicability::only(logical::LogicalExprKind::AccessPipeline),
    );
    let leading_filter = TestRule::new(
        "leading_filter",
        rules::RuleApplicability::access_pipeline_head_only(logical::StreamPipelineOpKind::Filter),
    );
    let leading_order_or_distinct = TestRule::new(
        "leading_order_or_distinct",
        rules::RuleApplicability::access_pipeline_head_any_of(
            ir::AtLeast::<_, 1>::from_one_and_rest(
                logical::StreamPipelineOpKind::Order,
                vec![logical::StreamPipelineOpKind::Distinct],
            ),
        ),
    );
    let schedule = schedule(vec![
        &any,
        &leading_filter,
        &pipeline_broad,
        &leading_order_or_distinct,
    ]);

    assert_eq!(
        rule_ids(
            schedule.rules_for_expr(&access_pipeline_expr(logical::StreamPipelineOpKind::Filter))
        ),
        ["any", "leading_filter", "pipeline_broad"]
    );
    assert_eq!(
        rule_ids(
            schedule.rules_for_expr(&access_pipeline_expr(logical::StreamPipelineOpKind::Order))
        ),
        ["any", "pipeline_broad", "leading_order_or_distinct"]
    );
    assert_eq!(
        rule_ids(schedule.rules_for_expr(&access_pipeline_expr(
            logical::StreamPipelineOpKind::Distinct
        ))),
        ["any", "pipeline_broad", "leading_order_or_distinct"]
    );
    assert_eq!(
        rule_ids(
            schedule.rules_for_expr(&access_pipeline_expr(logical::StreamPipelineOpKind::Window))
        ),
        ["any", "pipeline_broad"]
    );
}

#[test]
fn rule_schedule_routes_access_pipeline_local_simplification_candidates() {
    let any = TestRule::new("any", rules::RuleApplicability::Any);
    let local_simplification = TestRule::new(
        "local_simplification",
        rules::RuleApplicability::access_pipeline_local_simplification(),
    );
    let pipeline_broad = TestRule::new(
        "pipeline_broad",
        rules::RuleApplicability::only(logical::LogicalExprKind::AccessPipeline),
    );
    let schedule = schedule(vec![&any, &local_simplification, &pipeline_broad]);

    assert_eq!(
        rule_ids(
            schedule.rules_for_expr(&access_pipeline_expr(logical::StreamPipelineOpKind::Limit))
        ),
        ["any", "pipeline_broad"]
    );
    assert_eq!(
        rule_ids(schedule.rules_for_expr(&empty_access_pipeline_expr(
            logical::StreamPipelineOpKind::Limit
        ))),
        ["any", "local_simplification", "pipeline_broad"]
    );
    assert_eq!(
        rule_ids(schedule.rules_for_expr(&adjacent_filter_pipeline_expr())),
        ["any", "local_simplification", "pipeline_broad"]
    );
    assert_eq!(
        rule_ids(schedule.rules_for_expr(&access_pipeline_expr(
            logical::StreamPipelineOpKind::Distinct
        ))),
        ["any", "local_simplification", "pipeline_broad"]
    );
}

#[test]
fn rule_schedule_covers_every_stream_pipeline_op_family() {
    let any = TestRule::new("any", rules::RuleApplicability::Any);
    let head_specific = TestRule::new(
        "head_specific",
        rules::RuleApplicability::access_pipeline_head_any_of(
            ir::AtLeast::<_, 1>::from_one_and_rest(
                logical::StreamPipelineOpKind::Filter,
                logical::StreamPipelineOpKind::ALL[1..].to_vec(),
            ),
        ),
    );
    let schedule = schedule(vec![&any, &head_specific]);

    for kind in logical::StreamPipelineOpKind::ALL {
        assert_eq!(
            rule_ids(schedule.rules_for_expr(&access_pipeline_expr(kind))),
            ["any", "head_specific"]
        );
    }
}
