use helix_planner::{context, logical, properties, relational as r};
use std::sync::Arc;

fn aggregate_query() -> r::Query {
    r::Query::new(
        ["x", "total"]
            .into_iter()
            .map(|name| r::Binding {
                name: name.into(),
                kind: r::BindingType::Scalar,
                nullable: false,
                value_type: r::ValueType::Integer,
            })
            .collect(),
        vec![
            r::Operator::Unwind {
                expression: r::Expression::Function(
                    r::Function::Range,
                    vec![
                        r::Expression::Literal(r::Value::Integer(1)),
                        r::Expression::Literal(r::Value::Integer(10_000)),
                    ],
                ),
                slot: r::Slot(0),
            },
            r::Operator::Project {
                items: r::ProjectionProgram::new(vec![r::Projection {
                    slot: r::Slot(1),
                    expression: r::Expression::Aggregate {
                        function: r::Aggregate::Sum,
                        argument: Some(Box::new(r::Expression::Slot(r::Slot(0)))),
                        distinct: false,
                    },
                }])
                .unwrap(),
                distinct: false,
                ordering: vec![],
                predicate: None,
                skip: None,
                limit: None,
            },
        ],
        vec![("total".into(), r::Slot(1))],
    )
    .unwrap()
}

#[test]
fn row_choices_share_cascades_and_have_a_bounded_fallback() {
    let query = aggregate_query();
    let mut context = context::PlannerContext::default();
    let selected = r::plan(query.clone(), &context).unwrap();
    assert_eq!(selected.pipeline().execution(), r::RowExecution::Batched);
    assert_eq!(
        selected.batch_consumer(0),
        Some(r::BatchConsumer::Aggregate)
    );
    assert_eq!(selected.metrics.memo_groups, 1);
    assert_eq!(selected.metrics.alternatives_considered, 2);
    assert!(selected.metrics.rule_fires > 0);
    let reference = selected
        .clone()
        .with_execution(r::RowExecution::Materialized);
    assert_eq!(reference.query(), selected.query());
    assert_eq!(reference.batch_consumer(0), None);
    assert!(
        selected.pipeline().cost(&context.storage).peak_memory
            < reference.pipeline().cost(&context.storage).peak_memory
    );
    context.optimizer_limits.rule_fires = properties::PositiveUsize::at_least_one(1);
    let limited = r::plan(query, &context).unwrap();
    assert!(limited.metrics.rule_fires <= 1);
    assert_eq!(limited.query(), selected.query());
    assert_eq!(limited.batch_consumer(0), Some(r::BatchConsumer::Aggregate));
}

#[test]
fn deserialized_rows_recompute_effects_schemas_and_execution_proofs() {
    let query = aggregate_query();
    let pipeline = r::RowPipeline::new(Arc::new(query.clone()), r::RowExecution::Batched);
    let encoded = serde_json::to_value(&pipeline).unwrap();
    assert_eq!(
        serde_json::from_value::<r::RowPipeline>(encoded.clone()).unwrap(),
        pipeline
    );
    let mut forged = encoded;
    forged["query"]["contracts"] = serde_json::json!([]);
    forged["query"]["effect"] = serde_json::json!("Write");
    forged["batch_consumers"] = serde_json::json!({"199":"TopK"});
    let decoded = serde_json::from_value::<r::RowPipeline>(forged.clone()).unwrap();
    assert_eq!(decoded, pipeline);
    forged["query"]["operators"][0]["Unwind"]["slot"] = serde_json::json!(99);
    assert!(serde_json::from_value::<r::RowPipeline>(forged).is_err());
    let logical = logical::LogicalExpr::Rows(Arc::new(query));
    assert_eq!(logical.effect(), properties::EffectKind::Pure);
    assert_eq!(logical.memo_children().len(), 0);
    assert_eq!(
        serde_json::from_value::<logical::LogicalExpr>(serde_json::to_value(&logical).unwrap())
            .unwrap(),
        logical
    );
    assert!(
        serde_json::from_value::<r::Path>(serde_json::json!({"nodes":[],"relationships":[]}))
            .is_err()
    );
}

#[test]
fn optional_and_write_boundaries_cannot_be_forged_into_a_streaming_source() {
    let query = r::Query::new(
        vec![
            r::Binding {
                name: "n".into(),
                kind: r::BindingType::Node,
                nullable: true,
                value_type: r::ValueType::Node,
            },
            r::Binding {
                name: "count".into(),
                kind: r::BindingType::Scalar,
                nullable: false,
                value_type: r::ValueType::Integer,
            },
        ],
        vec![
            r::Operator::Match {
                pattern: r::Pattern {
                    nodes: vec![r::NodePattern {
                        slot: r::Slot(0),
                        label: Some("N".into()),
                        properties: vec![],
                    }],
                    relationships: vec![],
                    paths: vec![],
                },
                optional: true,
                predicate: None,
            },
            r::Operator::Update(vec![r::PropertyMutation::Set {
                entity: r::Slot(0),
                key: "x".into(),
                value: r::Expression::Literal(r::Value::Integer(1)),
            }]),
            r::Operator::Project {
                items: r::ProjectionProgram::new(vec![r::Projection {
                    slot: r::Slot(1),
                    expression: r::Expression::Aggregate {
                        function: r::Aggregate::Count,
                        argument: None,
                        distinct: false,
                    },
                }])
                .unwrap(),
                distinct: false,
                ordering: vec![],
                predicate: None,
                skip: None,
                limit: None,
            },
        ],
        vec![("count".into(), r::Slot(1))],
    )
    .unwrap();
    let plan = r::plan(query.clone(), &context::PlannerContext::default()).unwrap();
    assert!(plan.batch_consumer(0).is_none());
    assert!(plan.batch_consumer(1).is_none());
    assert_eq!(
        logical::LogicalExpr::Rows(Arc::new(query)).effect(),
        properties::EffectKind::Barrier
    );
    assert_eq!(
        plan.query().contracts()[0].boundaries(),
        &[r::Boundary::OptionalMatch]
    );
    assert_eq!(
        plan.query().contracts()[1].boundaries(),
        &[r::Boundary::Mutation]
    );
}

proptest::proptest! {
    #[test]
    fn mutated_resolved_bindings_cannot_bypass_validation(slot in 0_u32..128, depth in 0_usize..64) {
        let mut value=serde_json::to_value(aggregate_query()).unwrap();
        let mut expression=serde_json::json!({"Slot":slot});
        for _ in 0..depth { expression=serde_json::json!({"Unary":["Positive",expression]}); }
        value["operators"][1]["Project"]["items"][0]["expression"]["Aggregate"]["argument"]=expression;
        let result=serde_json::from_value::<r::Query>(value);
        if slot != 0 || depth >= r::MAX_EXPRESSION_DEPTH-1 {
            proptest::prop_assert!(result.is_err());
        } else {
            let query=result.unwrap();
            let plan=r::plan(query.clone(),&context::PlannerContext::default()).unwrap();
            proptest::prop_assert_eq!(plan.query(),&query);
            proptest::prop_assert!(query.contracts()[1].references().contains(&r::Slot(0)));
        }
    }

    #[test]
    fn randomized_graph_plans_keep_every_edge_and_bound_memo_work(
        endpoints in proptest::collection::vec((0_usize..8,0_usize..8),0..16),
        incoming in proptest::collection::btree_set(0_u32..8,0..8),
        budget in 1_usize..16,
    ) {
        use helix_planner::{cost,optimizer,physical,rules};
        let pattern=r::GraphPatternOrder::new(
            (0..8).map(|slot|r::PatternSource { slot:r::Slot(slot),rows:1_u64<<slot,access_cost:cost::CostVector::ZERO }).collect(),
            endpoints.iter().enumerate().map(|(i,(from,to))|r::PatternExpansion {
                from:r::Slot(*from as u32),to:r::Slot(*to as u32),relationship:r::Slot(8+i as u32),forward_rows:2,reverse_rows:3,
            }).collect(),incoming.into_iter().map(r::Slot).collect(),
        ).unwrap();
        let mut ctx=context::PlannerContext::default();
        ctx.optimizer_limits.rule_fires=properties::PositiveUsize::at_least_one(budget);
        let seed=rules::SeedRuleSet::default();
        let result=seed.optimizer().optimize(logical::LogicalExpr::GraphPattern(pattern.clone()),&optimizer::OptimizerConfig::from_context(&ctx)).unwrap();
        proptest::prop_assert!(result.metrics().rule_fires<=budget);
        let order=match result.best_alternative(result.roots()[0]) {
            Ok(alternative)=>match &alternative.expr {physical::PhysicalExpr::GraphPattern(order)=>order, _=>unreachable!()},
            Err(_)=>{proptest::prop_assert!(result.metrics().guardrail_hit);&pattern}
        };
        let schedule=order.schedule(&ctx.storage);
        let edges=schedule.steps.iter().filter_map(|step|match step {r::MatchStep::Expand{relationship,..}=>Some(*relationship), _=>None}).collect::<std::collections::BTreeSet<_>>();
        proptest::prop_assert_eq!(edges.len(),endpoints.len());
        proptest::prop_assert!(schedule.steps.len()<=8+endpoints.len());
    }
}

#[test]
fn planner_float_identity_is_lossless_for_every_ieee_class() {
    let mut encodings = std::collections::BTreeSet::new();
    for bits in [
        0,
        1,
        0x8000_0000_0000_0000,
        1.5_f64.to_bits(),
        f64::MAX.to_bits(),
        f64::INFINITY.to_bits(),
        f64::NEG_INFINITY.to_bits(),
        f64::NAN.to_bits(),
        f64::NAN.to_bits() + 1,
    ] {
        let value = r::Value::Float(f64::from_bits(bits));
        let encoded = serde_json::to_string(&value).unwrap();
        assert!(encodings.insert(encoded.clone()));
        let r::Value::Float(decoded) = serde_json::from_str(&encoded).unwrap() else {
            panic!("float roundtrip");
        };
        assert_eq!(decoded.to_bits(), bits);
        let query = r::Query::new(
            vec![r::Binding {
                name: "x".into(),
                kind: r::BindingType::Scalar,
                nullable: false,
                value_type: r::ValueType::Float,
            }],
            vec![r::Operator::Project {
                items: r::ProjectionProgram::new(vec![r::Projection {
                    slot: r::Slot(0),
                    expression: r::Expression::Literal(value),
                }])
                .unwrap(),
                distinct: false,
                ordering: vec![],
                predicate: None,
                skip: None,
                limit: None,
            }],
            vec![("x".into(), r::Slot(0))],
        )
        .unwrap();
        assert!(r::plan(query, &context::PlannerContext::default()).is_ok());
    }
    for value in [
        serde_json::json!({"Float":null}),
        serde_json::json!({"Float":"0"}),
        serde_json::json!({"Float":"not-a-valid-hex!!"}),
    ] {
        assert!(serde_json::from_value::<r::Value>(value).is_err());
    }
}
