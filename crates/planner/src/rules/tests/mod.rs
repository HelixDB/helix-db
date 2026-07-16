use super::*;
use crate::{catalog, context, cost, exec, optimizer::OptimizerRule, properties};

mod access;
mod core;
mod implementation;
mod registry;
mod root_stream;
mod support;

pub(in crate::rules::tests) use support::*;

#[test]
fn rule_id_rejects_empty_names() {
    assert!(RuleId::new("").is_none());
    assert_eq!(RuleId::new("access_path").unwrap().as_ref(), "access_path");
}

#[test]
fn known_rule_ids_are_unique_and_round_trip_through_rule_id() {
    let mut names = std::collections::BTreeSet::new();

    for id in KnownRuleId::ALL {
        assert!(!id.as_ref().is_empty());
        assert!(names.insert(id.as_ref()));
        assert_eq!(KnownRuleId::from_name(id.as_ref()), Some(*id));
        assert_eq!(RuleId::new(id.as_ref()), Some(RuleId::known(*id)));

        let encoded = serde_json::to_string(id).unwrap();
        assert_eq!(serde_json::from_str::<KnownRuleId>(&encoded).unwrap(), *id);
        assert_eq!(
            serde_json::from_str::<RuleId>(&encoded).unwrap(),
            RuleId::known(*id)
        );
    }

    assert_eq!(names.len(), KnownRuleId::ALL.len());
    assert!(KnownRuleId::from_name("test_rule").is_none());
}

#[test]
fn rule_ids_preserve_string_serialization_contract() {
    let known = RuleId::known(KnownRuleId::SeedAccessPath);
    let custom = RuleId::new("test_rule").unwrap();

    assert_eq!(known.to_non_empty_string().as_ref(), "seed_access_path");
    assert_eq!(custom.to_non_empty_string().as_ref(), "test_rule");
    assert_eq!(
        serde_json::to_string(&known).unwrap(),
        "\"seed_access_path\""
    );
    assert_eq!(serde_json::to_string(&custom).unwrap(), "\"test_rule\"");
    assert_eq!(
        serde_json::from_str::<RuleId>("\"seed_access_path\"").unwrap(),
        known
    );
    assert_eq!(
        serde_json::from_str::<RuleId>("\"test_rule\"").unwrap(),
        custom
    );
    assert_eq!(
        serde_json::from_str::<KnownRuleId>("\"seed_access_path\"").unwrap(),
        KnownRuleId::SeedAccessPath
    );
    assert!(serde_json::from_str::<RuleId>("\"\"").is_err());
    assert!(serde_json::from_str::<KnownRuleId>("\"test_rule\"").is_err());
}

#[test]
fn rule_metadata_carries_property_contracts_and_rejections_are_non_empty() {
    let required = properties::RequiredProperties {
        element: Some(properties::ElementKind::Node),
        ordering: properties::RequiredOrdering::Any,
    };
    let delivered = properties::DeliveredProperties {
        element: Some(properties::ElementKind::Node),
        ..properties::DeliveredProperties::default()
    };

    let metadata = RuleMetadata::new(RuleId::new("node_scan").unwrap(), RuleKind::Implementation)
        .with_required(required.clone())
        .with_delivered(delivered.clone());

    assert_eq!(metadata.required, required);
    assert_eq!(metadata.delivered, Some(delivered));
    assert!(RuleRejection::new("").is_none());
    assert_eq!(
        RuleRejection::new("missing_index").unwrap().reason.as_ref(),
        "missing_index"
    );
}

#[test]
fn rule_metadata_encodes_scheduler_applicability() {
    let known = RuleMetadata::new(
        RuleId::known(KnownRuleId::AccessFilterIndex),
        RuleKind::Exploration,
    );
    let predicate = ir::PredicatePlan::new(helix_ast::expr::Predicate::eq("age", 42)).unwrap();
    assert!(known.applicability.matches(&node_access_filter_expr(
        ir::NodeAccessPlan::LabelScan {
            label: ir::NonEmptyString::new("User").unwrap(),
        },
        predicate.clone()
    )));
    assert!(!known.applicability.matches(&node_access_filter_expr(
        ir::NodeAccessPlan::AllScan,
        predicate,
    )));
    assert!(!known
        .applicability
        .matches(&source(properties::ElementKind::Node)));

    let access_filter_simplification = RuleMetadata::new(
        RuleId::known(KnownRuleId::AccessFilterSimplification),
        RuleKind::Exploration,
    );
    assert!(access_filter_simplification
        .applicability
        .matches(&node_access_filter_expr(
            ir::NodeAccessPlan::LabelScan {
                label: ir::NonEmptyString::new("User").unwrap(),
            },
            ir::PredicatePlan::new(helix_ast::expr::Predicate::eq("$label", "Admin")).unwrap(),
        )));
    assert!(!access_filter_simplification
        .applicability
        .matches(&node_access_filter_expr(
            ir::NodeAccessPlan::LabelScan {
                label: ir::NonEmptyString::new("User").unwrap(),
            },
            ir::PredicatePlan::new(helix_ast::expr::Predicate::contains("bio", "rust")).unwrap(),
        )));

    let access_set_simplification = RuleMetadata::new(
        RuleId::known(KnownRuleId::AccessSetSimplification),
        RuleKind::Exploration,
    );
    let access_subsumption = RuleMetadata::new(
        RuleId::known(KnownRuleId::AccessSubsumption),
        RuleKind::Exploration,
    );
    let user_source = ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::LabelScan {
        label: ir::NonEmptyString::new("User").unwrap(),
    })
    .unwrap();
    let nested_set = node_access_expr(ir::NodeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
        user_source.clone(),
        ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
            ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::LabelScan {
                label: ir::NonEmptyString::new("Account").unwrap(),
            })
            .unwrap(),
            ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::LabelScan {
                label: ir::NonEmptyString::new("Org").unwrap(),
            })
            .unwrap(),
        )))
        .unwrap(),
    )));
    let subsumed_set = node_access_expr(ir::NodeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
        ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::AllScan).unwrap(),
        user_source.clone(),
    )));
    let ordinary_set = node_access_expr(ir::NodeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
        user_source,
        ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::LabelScan {
            label: ir::NonEmptyString::new("Account").unwrap(),
        })
        .unwrap(),
    )));
    assert!(access_set_simplification.applicability.matches(&nested_set));
    assert!(!access_set_simplification
        .applicability
        .matches(&subsumed_set));
    assert!(access_subsumption.applicability.matches(&subsumed_set));
    assert!(!access_subsumption.applicability.matches(&ordinary_set));

    let access_range_intersection = RuleMetadata::new(
        RuleId::known(KnownRuleId::AccessRangeIntersection),
        RuleKind::Exploration,
    );
    let access_equality_range_intersection = RuleMetadata::new(
        RuleId::known(KnownRuleId::AccessEqualityRangeIntersection),
        RuleKind::Exploration,
    );
    let access_equality_range_union = RuleMetadata::new(
        RuleId::known(KnownRuleId::AccessEqualityRangeUnion),
        RuleKind::Exploration,
    );
    let access_contradiction = RuleMetadata::new(
        RuleId::known(KnownRuleId::AccessContradiction),
        RuleKind::Exploration,
    );
    let range_intersection = node_access_expr(ir::NodeAccessPlan::Intersect(
        ir::AtLeast::<_, 2>::from_pair(
            node_range_source("User", "age", lower_range(18)),
            node_range_source("User", "age", upper_range(65)),
        ),
    ));
    let equality_union_source =
        ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
            node_eq_source("User", "age", equality_literal(18)),
            node_eq_source("User", "age", equality_literal(21)),
        )))
        .unwrap();
    let equality_range_intersection = node_access_expr(ir::NodeAccessPlan::Intersect(
        ir::AtLeast::<_, 2>::from_pair(
            node_range_source("User", "age", ir::IndexRange::All),
            equality_union_source,
        ),
    ));
    let equality_range_union =
        node_access_expr(ir::NodeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
            node_range_source("User", "age", ir::IndexRange::All),
            node_eq_source("User", "age", equality_literal(42)),
        )));
    let contradiction = node_access_expr(ir::NodeAccessPlan::Intersect(
        ir::AtLeast::<_, 2>::from_pair(
            node_eq_source("User", "age", equality_literal(18)),
            node_eq_source("User", "age", equality_literal(21)),
        ),
    ));
    assert!(access_range_intersection
        .applicability
        .matches(&range_intersection));
    assert!(!access_range_intersection
        .applicability
        .matches(&ordinary_set));
    assert!(access_equality_range_intersection
        .applicability
        .matches(&equality_range_intersection));
    assert!(!access_equality_range_intersection
        .applicability
        .matches(&ordinary_set));
    assert!(access_equality_range_union
        .applicability
        .matches(&equality_range_union));
    assert!(!access_equality_range_union
        .applicability
        .matches(&ordinary_set));
    assert!(access_contradiction.applicability.matches(&contradiction));
    assert!(!access_contradiction.applicability.matches(&ordinary_set));

    let custom = RuleMetadata::new(RuleId::new("custom_rule").unwrap(), RuleKind::Exploration);
    assert!(custom
        .applicability
        .matches(&source(properties::ElementKind::Node)));

    let source_rule = RuleMetadata::new(
        RuleId::known(KnownRuleId::SeedSourceAccess),
        RuleKind::Implementation,
    );
    assert!(source_rule
        .applicability
        .matches(&source(properties::ElementKind::Node)));
    let filter = logical::LogicalExpr::Pure(logical::PureLogicalOp::Filter {
        predicate: ir::PredicatePlan::new(helix_ast::expr::Predicate::eq("age", 42)).unwrap(),
    });
    assert!(!source_rule.applicability.matches(&filter));

    let pure_simplification_rule = RuleMetadata::new(
        RuleId::known(KnownRuleId::PurePipelineSimplification),
        RuleKind::Exploration,
    );
    let stream_window_rule = RuleMetadata::new(
        RuleId::known(KnownRuleId::StreamWindowComposition),
        RuleKind::Exploration,
    );
    let pure_pipeline = |ops: Vec<logical::PureLogicalOp>| {
        logical::LogicalExpr::PurePipeline(logical::PurePipeline::new(
            ir::AtLeast::<_, 1>::try_from_vec(ops).unwrap(),
        ))
    };
    let pure_source = logical::PureLogicalOp::Source {
        element: properties::ElementKind::Node,
    };
    let pure_limit = |count| logical::PureLogicalOp::Limit {
        count: ir::StreamBoundPlan::Literal(count),
    };
    let pure_skip = |count| logical::PureLogicalOp::Skip {
        count: ir::StreamBoundPlan::Literal(count),
    };
    let reducible_pure_pipeline =
        pure_pipeline(vec![logical::PureLogicalOp::NoOp, pure_source.clone()]);
    let window_pure_pipeline = pure_pipeline(vec![pure_skip(2), pure_limit(5)]);
    let ordinary_pure_pipeline = pure_pipeline(vec![pure_source.clone(), pure_limit(5)]);
    assert!(pure_simplification_rule
        .applicability
        .matches(&reducible_pure_pipeline));
    assert!(!pure_simplification_rule
        .applicability
        .matches(&ordinary_pure_pipeline));
    assert!(stream_window_rule
        .applicability
        .matches(&window_pure_pipeline));
    assert!(!stream_window_rule
        .applicability
        .matches(&ordinary_pure_pipeline));

    let pipeline_filter_rule = RuleMetadata::new(
        RuleId::known(KnownRuleId::AccessPipelineFilter),
        RuleKind::Exploration,
    );
    let access = node_access_path(ir::NodeAccessPlan::AllScan);
    let pipeline_filter = logical::LogicalExpr::AccessPipeline(
        logical::AccessPipeline::new(
            access.clone(),
            ir::AtLeast::<_, 1>::from_one(logical::StreamPipelineOp::Filter {
                predicate: ir::PredicatePlan::new(helix_ast::expr::Predicate::eq("age", 42))
                    .unwrap(),
            }),
        )
        .unwrap(),
    );
    let pipeline_order = logical::LogicalExpr::AccessPipeline(
        logical::AccessPipeline::new(
            access,
            ir::AtLeast::<_, 1>::from_one(logical::StreamPipelineOp::Order {
                ordering: order_keys(),
            }),
        )
        .unwrap(),
    );
    let pipeline_filter_order = logical::LogicalExpr::AccessPipeline(
        logical::AccessPipeline::new(
            node_access_path(ir::NodeAccessPlan::AllScan),
            ir::AtLeast::<_, 1>::from_one_and_rest(
                logical::StreamPipelineOp::Filter {
                    predicate: ir::PredicatePlan::new(helix_ast::expr::Predicate::eq(
                        "active", true,
                    ))
                    .unwrap(),
                },
                vec![logical::StreamPipelineOp::Order {
                    ordering: order_keys(),
                }],
            ),
        )
        .unwrap(),
    );
    assert!(pipeline_filter_rule.applicability.matches(&pipeline_filter));
    assert!(!pipeline_filter_rule.applicability.matches(&pipeline_order));
    let pipeline_order_rule = RuleMetadata::new(
        RuleId::known(KnownRuleId::AccessPipelineOrder),
        RuleKind::Exploration,
    );
    assert!(pipeline_order_rule.applicability.matches(&pipeline_order));
    assert!(pipeline_order_rule
        .applicability
        .matches(&pipeline_filter_order));

    let pipeline_simplification_rule = RuleMetadata::new(
        RuleId::known(KnownRuleId::AccessPipelineSimplification),
        RuleKind::Exploration,
    );
    let empty_pipeline = logical::LogicalExpr::AccessPipeline(
        logical::AccessPipeline::new(
            node_access_path(ir::NodeAccessPlan::Empty),
            ir::AtLeast::<_, 1>::from_one(logical::StreamPipelineOp::Limit {
                count: ir::StreamBoundPlan::Literal(1),
            }),
        )
        .unwrap(),
    );
    let adjacent_filter_pipeline = logical::LogicalExpr::AccessPipeline(
        logical::AccessPipeline::new(
            node_access_path(ir::NodeAccessPlan::AllScan),
            ir::AtLeast::<_, 1>::from_one_and_rest(
                logical::StreamPipelineOp::Filter {
                    predicate: ir::PredicatePlan::new(helix_ast::expr::Predicate::eq(
                        "active", true,
                    ))
                    .unwrap(),
                },
                vec![logical::StreamPipelineOp::Filter {
                    predicate: ir::PredicatePlan::new(helix_ast::expr::Predicate::eq(
                        "verified", true,
                    ))
                    .unwrap(),
                }],
            ),
        )
        .unwrap(),
    );
    assert!(pipeline_simplification_rule
        .applicability
        .matches(&empty_pipeline));
    assert!(pipeline_simplification_rule
        .applicability
        .matches(&adjacent_filter_pipeline));
    assert!(!pipeline_simplification_rule
        .applicability
        .matches(&pipeline_order));

    let root_empty_rule = RuleMetadata::new(
        RuleId::known(KnownRuleId::RootControlFlowEmpty),
        RuleKind::Exploration,
    );
    let root_branch_rule = RuleMetadata::new(
        RuleId::known(KnownRuleId::SeedRootBranch),
        RuleKind::Implementation,
    );
    let root_repeat_rule = RuleMetadata::new(
        RuleId::known(KnownRuleId::SeedRootRepeat),
        RuleKind::Implementation,
    );
    let empty_branch = optional_branch_expr(
        node_access_expr(ir::NodeAccessPlan::Empty),
        node_access_expr(ir::NodeAccessPlan::AllScan),
    );
    let non_empty_branch = optional_branch_expr(
        node_access_expr(ir::NodeAccessPlan::AllScan),
        node_access_expr(ir::NodeAccessPlan::AllScan),
    );
    let empty_repeat = repeat_root_expr(
        node_access_expr(ir::NodeAccessPlan::Empty),
        node_access_expr(ir::NodeAccessPlan::AllScan),
        2,
    );
    let non_empty_repeat = repeat_root_expr(
        node_access_expr(ir::NodeAccessPlan::AllScan),
        node_access_expr(ir::NodeAccessPlan::AllScan),
        2,
    );
    assert!(root_empty_rule.applicability.matches(&empty_branch));
    assert!(root_empty_rule.applicability.matches(&empty_repeat));
    assert!(!root_empty_rule.applicability.matches(&non_empty_branch));
    assert!(root_branch_rule.applicability.matches(&non_empty_branch));
    assert!(!root_branch_rule.applicability.matches(&empty_branch));
    assert!(root_repeat_rule.applicability.matches(&non_empty_repeat));
    assert!(!root_repeat_rule.applicability.matches(&empty_repeat));

    let access_window_rule = RuleMetadata::new(
        RuleId::known(KnownRuleId::AccessWindow),
        RuleKind::Exploration,
    );
    let seed_access_window_rule = RuleMetadata::new(
        RuleId::known(KnownRuleId::SeedAccessWindow),
        RuleKind::Implementation,
    );
    let ordinary_window = node_access_window_expr(
        ir::NodeAccessPlan::LabelScan {
            label: ir::NonEmptyString::new("User").unwrap(),
        },
        logical::AccessWindowRange::new(1, Some(3)).unwrap(),
    );
    let rewrite_window = node_access_window_expr(
        ir::NodeAccessPlan::PointIds {
            ids: ir::ElementIds::new(ir::AtLeast::<_, 1>::from_one_and_rest(7, vec![9])).unwrap(),
        },
        logical::AccessWindowRange::new(1, Some(2)).unwrap(),
    );
    assert!(access_window_rule.applicability.matches(&rewrite_window));
    assert!(!access_window_rule.applicability.matches(&ordinary_window));
    assert!(seed_access_window_rule
        .applicability
        .matches(&ordinary_window));

    let access_order_rule = RuleMetadata::new(
        RuleId::known(KnownRuleId::AccessOrder),
        RuleKind::Exploration,
    );
    let access_order_range_direction_rule = RuleMetadata::new(
        RuleId::known(KnownRuleId::AccessOrderRangeDirection),
        RuleKind::Exploration,
    );
    let seed_access_order_rule = RuleMetadata::new(
        RuleId::known(KnownRuleId::SeedAccessOrder),
        RuleKind::Implementation,
    );
    let satisfied_order = node_access_order_expr(
        ir::NodeAccessPlan::from(node_range_source("User", "age", lower_range(18))),
        order_keys(),
    );
    let opposite_direction_order = node_access_order_expr(
        ir::NodeAccessPlan::from(node_range_source("User", "age", lower_range(18))),
        desc_order_keys(),
    );
    let ordinary_order = node_access_order_expr(
        ir::NodeAccessPlan::LabelScan {
            label: ir::NonEmptyString::new("User").unwrap(),
        },
        order_keys(),
    );
    assert!(access_order_rule.applicability.matches(&satisfied_order));
    assert!(!access_order_rule
        .applicability
        .matches(&opposite_direction_order));
    assert!(access_order_range_direction_rule
        .applicability
        .matches(&opposite_direction_order));
    assert!(!access_order_range_direction_rule
        .applicability
        .matches(&ordinary_order));
    assert!(seed_access_order_rule
        .applicability
        .matches(&ordinary_order));

    let access_distinct_rule = RuleMetadata::new(
        RuleId::known(KnownRuleId::AccessDistinct),
        RuleKind::Exploration,
    );
    let seed_access_distinct_rule = RuleMetadata::new(
        RuleId::known(KnownRuleId::SeedAccessDistinct),
        RuleKind::Implementation,
    );
    let noop_distinct = node_access_distinct_expr(ir::NodeAccessPlan::PointIds {
        ids: ir::ElementIds::new(ir::AtLeast::<_, 1>::from_one_and_rest(7, vec![9])).unwrap(),
    });
    let ordinary_distinct = node_access_distinct_expr(ir::NodeAccessPlan::LabelScan {
        label: ir::NonEmptyString::new("User").unwrap(),
    });
    assert!(access_distinct_rule.applicability.matches(&noop_distinct));
    assert!(!access_distinct_rule
        .applicability
        .matches(&ordinary_distinct));
    assert!(seed_access_distinct_rule
        .applicability
        .matches(&ordinary_distinct));

    let set_rule = RuleMetadata::new(
        RuleId::known(KnownRuleId::AccessSetSimplification),
        RuleKind::Exploration,
    );
    let empty_source = ir::NodeAccessSourcePlan::from_unfiltered(ir::NodeAccessPlan::Empty);
    let access_scan =
        logical::LogicalExpr::AccessPath(node_access_path(ir::NodeAccessPlan::AllScan));
    let access_intersection =
        logical::LogicalExpr::AccessPath(node_access_path(ir::NodeAccessPlan::Intersect(
            ir::AtLeast::<_, 2>::from_pair(empty_source.clone(), empty_source),
        )));
    assert!(set_rule.applicability.matches(&access_intersection));
    assert!(!set_rule.applicability.matches(&access_scan));

    let narrowed = custom.clone().with_applicability(RuleApplicability::only(
        logical::LogicalExprKind::RootPipeline,
    ));
    assert!(!narrowed
        .applicability
        .matches(&source(properties::ElementKind::Node)));
    assert!(serde_json::to_value(&narrowed)
        .unwrap()
        .get("applicability")
        .is_some());
}

#[test]
fn rule_applicability_kind_sets_are_canonical() {
    let applicability = RuleApplicability::any_of(ir::AtLeast::<_, 1>::from_one_and_rest(
        logical::LogicalExprKind::AccessFilter,
        vec![
            logical::LogicalExprKind::Pure,
            logical::LogicalExprKind::AccessFilter,
        ],
    ));

    let RuleApplicability::LogicalKinds(kinds) = applicability else {
        panic!("non-empty kind construction should produce a logical-kind set");
    };
    assert_eq!(
        kinds.as_slice(),
        &[
            logical::LogicalExprKind::Pure,
            logical::LogicalExprKind::AccessFilter
        ]
    );

    let encoded = serde_json::to_string(&kinds).unwrap();
    let decoded = serde_json::from_str::<RuleLogicalKinds>(&encoded).unwrap();
    assert_eq!(decoded, kinds);
    assert!(serde_json::from_str::<RuleLogicalKinds>("[]").is_err());

    let pure_applicability =
        RuleApplicability::pure_any_of(ir::AtLeast::<_, 1>::from_one_and_rest(
            logical::PureLogicalOpKind::Filter,
            vec![
                logical::PureLogicalOpKind::Source,
                logical::PureLogicalOpKind::Filter,
            ],
        ));
    let RuleApplicability::PureOpKinds(pure_kinds) = pure_applicability else {
        panic!("non-empty pure kind construction should produce a pure-op-kind set");
    };
    assert_eq!(
        pure_kinds.as_slice(),
        &[
            logical::PureLogicalOpKind::Source,
            logical::PureLogicalOpKind::Filter
        ]
    );

    let encoded = serde_json::to_string(&pure_kinds).unwrap();
    let decoded = serde_json::from_str::<RulePureOpKinds>(&encoded).unwrap();
    assert_eq!(decoded, pure_kinds);
    assert!(serde_json::from_str::<RulePureOpKinds>("[]").is_err());

    let stream_applicability =
        RuleApplicability::access_pipeline_head_any_of(ir::AtLeast::<_, 1>::from_one_and_rest(
            logical::StreamPipelineOpKind::Order,
            vec![
                logical::StreamPipelineOpKind::Filter,
                logical::StreamPipelineOpKind::Order,
            ],
        ));
    let RuleApplicability::AccessPipelineHeadOpKinds(stream_kinds) = stream_applicability else {
        panic!("non-empty stream kind construction should produce a stream-op-kind set");
    };
    assert_eq!(
        stream_kinds.as_slice(),
        &[
            logical::StreamPipelineOpKind::Filter,
            logical::StreamPipelineOpKind::Order
        ]
    );

    let encoded = serde_json::to_string(&stream_kinds).unwrap();
    let decoded = serde_json::from_str::<RuleStreamPipelineOpKinds>(&encoded).unwrap();
    assert_eq!(decoded, stream_kinds);
    assert!(serde_json::from_str::<RuleStreamPipelineOpKinds>("[]").is_err());

    let source_applicability =
        RuleApplicability::access_source_any_of(ir::AtLeast::<_, 1>::from_one_and_rest(
            logical::AccessSourceKind::Union,
            vec![
                logical::AccessSourceKind::Intersection,
                logical::AccessSourceKind::Union,
            ],
        ));
    let RuleApplicability::AccessSourceKinds(source_kinds) = source_applicability else {
        panic!("non-empty access-source kind construction should produce a source-kind set");
    };
    assert_eq!(
        source_kinds.as_slice(),
        &[
            logical::AccessSourceKind::Intersection,
            logical::AccessSourceKind::Union
        ]
    );

    let encoded = serde_json::to_string(&source_kinds).unwrap();
    let decoded = serde_json::from_str::<RuleAccessSourceKinds>(&encoded).unwrap();
    assert_eq!(decoded, source_kinds);
    assert!(serde_json::from_str::<RuleAccessSourceKinds>("[]").is_err());
}
