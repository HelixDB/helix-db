use helix_planner::{catalog, context, ir, properties, relational as r};
use std::collections::{BTreeMap, BTreeSet};

#[test]
fn all_sources_share_one_budget_and_have_deterministic_fallbacks() {
    let query = helix_cypher::compile("MATCH (a:A),(b:B),(c:C) RETURN a,b,c").unwrap();
    let mut context = context::PlannerContext::default();
    context.optimizer_limits.memo_groups = properties::PositiveUsize::at_least_one(1);
    context.optimizer_limits.rule_fires = properties::PositiveUsize::at_least_one(1);
    let first = r::plan(query.clone(), &context).unwrap();
    let second = r::plan(query, &context).unwrap();
    assert!(first.metrics.guardrail_hit);
    assert!(first
        .explain()
        .notices
        .contains(&r::PlanNotice::OptimizerBudgetExhausted));
    assert!(first.metrics.memo_groups <= 1);
    assert!(first.metrics.rule_fires <= 1);
    assert_eq!(first.matches()[&0].steps, second.matches()[&0].steps);
    assert_eq!(first.matches()[&0].cartesian_products, 2);
    assert_eq!(first.matches()[&0].sources.len(), 3);
}

#[test]
fn indexed_cardinality_selects_the_start_and_preserves_optional_boundaries() {
    let query = helix_cypher::compile(
        "MATCH (a:N)-[:R]->(b:N {key:7}) OPTIONAL MATCH (b)-[:R]->(c:N) RETURN a,b,c",
    )
    .unwrap();
    let key = catalog::ScopedPropertyKey::try_new("N", "key").unwrap();
    let context = context::PlannerContext {
        indexes: catalog::IndexCatalogSnapshot::default().with_node_eq(key.clone()),
        stats: context::StatsSnapshot::default()
            .with_node_label_cardinality(ir::NonEmptyString::new("N").unwrap(), 1_000_000)
            .with_node_eq_cardinality(key, 1),
        ..context::PlannerContext::default()
    };
    let plan = r::plan(query, &context).unwrap();
    let b = r::Slot(
        plan.query()
            .bindings()
            .iter()
            .position(|binding| binding.name == "b")
            .unwrap() as u32,
    );
    assert_eq!(plan.matches()[&0].steps[0], r::MatchStep::Scan(b));
    assert_eq!(plan.matches()[&1].incoming, BTreeSet::from([b]));
    assert_eq!(
        plan.matches()[&0]
            .sources
            .iter()
            .find(|s| s.slot == b)
            .unwrap()
            .estimated_rows,
        1
    );
}

#[test]
fn property_hydration_excludes_identity_and_unused_bindings() {
    let query = helix_cypher::compile("MATCH (n:N),(m:M) RETURN id(n), n.name, labels(m)").unwrap();
    let r::Operator::Project { items, .. } = &query.operators()[1] else {
        panic!("projection");
    };
    let mut demand = BTreeMap::new();
    items[0].expression.graph_requirements(&mut demand);
    assert!(demand.is_empty());
    for item in &items[1..] {
        item.expression.graph_requirements(&mut demand);
    }
    assert_eq!(demand.len(), 2);
    let n = items[1].expression.slots().into_iter().next().unwrap();
    assert!(demand[&n].contains("name"));
    assert!(!demand[&n].contains("unrelated"));
}

#[test]
fn numeric_predicate_equality_is_separate_from_grouping() {
    assert_eq!(r::Value::Null.equals(&r::Value::Null), None);
    assert!(r::Value::Null.total_cmp(&r::Value::Null).is_eq());
    assert_eq!(
        r::Value::Float(f64::NAN).equals(&r::Value::Float(f64::NAN)),
        Some(false)
    );
    assert_eq!(
        r::Value::Integer(i64::MAX).equals(&r::Value::Float(i64::MAX as f64)),
        Some(false)
    );
    assert_eq!(
        r::Value::Integer(9_007_199_254_740_993).equals(&r::Value::Float(9_007_199_254_740_992.0)),
        Some(false)
    );
    assert_eq!(
        r::Value::List(vec![r::Value::Null, r::Value::Integer(1)])
            .equals(&r::Value::List(vec![r::Value::Null, r::Value::Integer(2)])),
        Some(false)
    );
}

#[test]
fn validated_plans_reject_malformed_paths_and_dangling_slots() {
    let binding = r::Binding {
        name: "n".into(),
        kind: r::BindingType::Node,
        nullable: false,
        value_type: r::ValueType::Node,
    };
    let invalid = r::Operator::Unwind {
        expression: r::Expression::Slot(r::Slot(999)),
        slot: r::Slot(0),
    };
    assert_eq!(
        r::Query::new(vec![binding.clone()], vec![invalid], vec![])
            .unwrap_err()
            .detail,
        "UnboundSlot"
    );
    let pattern = r::Pattern {
        nodes: vec![r::NodePattern {
            slot: r::Slot(0),
            label: Some(String::new()),
            properties: vec![],
        }],
        relationships: vec![],
        paths: vec![],
    };
    assert_eq!(
        r::Query::new(
            vec![binding],
            vec![r::Operator::Match {
                pattern,
                optional: false,
                predicate: None
            }],
            vec![]
        )
        .unwrap_err()
        .detail,
        "EmptyLabel"
    );
    assert!(r::Path::new(vec![1], vec![1]).is_err());
    assert!(r::Path::new(vec![], vec![]).is_err());
    assert_eq!(r::Path::new(vec![1, 1], vec![2]).unwrap().nodes(), &[1, 1]);
    let invalid = r::Expression::Function(r::Function::Id, vec![]);
    assert_eq!(
        invalid.validate_shape().unwrap_err().detail,
        "FunctionArity"
    );
}

#[test]
fn named_path_nodes_must_belong_to_their_own_validated_pattern() {
    let query = helix_cypher::compile("OPTIONAL MATCH (a:N) MATCH p=(b:M) RETURN p").unwrap();
    let mut operators = query.operators().to_vec();
    let r::Operator::Match { pattern, .. } = &mut operators[1] else {
        panic!("second match");
    };
    // A nullable node from an earlier optional match cannot silently replace
    // the node named by this path's pattern, even when its binding type agrees.
    pattern.paths[0].nodes[0] = r::Slot(0);
    let serialized = serde_json::json!({
        "bindings": query.bindings(), "operators": operators, "returns": query.returns(),
    });
    assert!(serde_json::from_value::<r::Query>(serialized).is_err());
    assert_eq!(
        r::Query::new(
            query.bindings().to_vec(),
            operators,
            query.returns().to_vec()
        )
        .unwrap_err()
        .detail,
        "InvalidPath"
    );

    let query = helix_cypher::compile("MATCH p=(n) RETURN p").unwrap();
    let mut operators = query.operators().to_vec();
    let mut bindings = query.bindings().to_vec();
    let r::Operator::Match { pattern, .. } = &mut operators[0] else {
        panic!("match");
    };
    let path = pattern.paths[0].slot;
    pattern.nodes[0].slot = path;
    pattern.paths[0].nodes[0] = path;
    bindings[path.0 as usize].value_type = r::ValueType::Any;
    assert_eq!(
        r::Query::new(bindings, operators, query.returns().to_vec())
            .unwrap_err()
            .detail,
        "InvalidPath"
    );
}

#[test]
fn source_and_expression_limits_fail_cleanly() {
    assert_eq!(
        helix_cypher::parse(&" ".repeat(16 * 1024 * 1024 + 1))
            .unwrap_err()
            .detail,
        "QueryTooLarge"
    );
    let query = format!("RETURN {}1{}", "(".repeat(150), ")".repeat(150));
    assert_eq!(
        helix_cypher::compile(&query).unwrap_err().category,
        "ResourceLimit"
    );
    let query = format!("RETURN 1{}", "+1".repeat(150));
    assert_eq!(
        helix_cypher::compile(&query).unwrap_err().category,
        "ResourceLimit"
    );
    let mut value = r::Value::Null;
    for _ in 0..130 {
        value = r::Value::List(vec![value]);
    }
    assert_eq!(value.validate_shape().unwrap_err().detail, "ValueDepth");
    let query = format!("RETURN {}", vec!["1"; 100_001].join(","));
    assert_eq!(
        helix_cypher::compile(&query).unwrap_err().detail,
        "TooManyTokens"
    );
}

#[test]
fn operator_contracts_preserve_optional_correlation_scope_and_effects() {
    let query = helix_cypher::compile(
        "WITH 1 AS seed OPTIONAL MATCH (n:N) WHERE n.x = seed WITH n AS chosen RETURN chosen",
    )
    .unwrap();
    let contracts = query.contracts();
    let seed = *contracts[0].output().columns().keys().next().unwrap();
    assert!(
        matches!(contracts[1].correlation(),r::Correlation::Bound(slots) if slots.as_ref().contains(&seed))
    );
    assert_eq!(contracts[1].boundaries(), &[r::Boundary::OptionalMatch]);
    let node = contracts[1]
        .output()
        .columns()
        .iter()
        .find(|(_, column)| column.value_type == r::ValueType::Node)
        .unwrap();
    assert!(node.1.nullable);
    assert!(!contracts[2].output().columns().contains_key(&seed));
    assert!(contracts[3]
        .input()
        .columns()
        .values()
        .all(|column| column.nullable));
    let write = helix_cypher::compile("MATCH (n:N) SET n.x=1 RETURN count(n)").unwrap();
    assert_eq!(write.contracts()[1].effect(), r::Effect::Write);
    assert_eq!(write.contracts()[1].boundaries(), &[r::Boundary::Mutation]);
    assert_eq!(
        write.contracts()[2].multiplicity(),
        r::Multiplicity::GroupsRows
    );
    assert!(!write.contracts()[2].is_total_projection());
    let order =
        helix_cypher::compile("UNWIND [1,2] AS x RETURN DISTINCT x ORDER BY x LIMIT 1").unwrap();
    assert_eq!(
        order.contracts()[1].boundaries(),
        &[
            r::Boundary::Distinct,
            r::Boundary::Ordering,
            r::Boundary::Window
        ]
    );
}

#[test]
fn escaped_parameter_names_share_identifier_escaping_and_source_validation() {
    let query =
        helix_cypher::compile("RETURN $`space name` AS x, $`a``b` AS y, $`λ` AS z").unwrap();
    assert_eq!(
        query.parameters(),
        BTreeSet::from(["space name".into(), "a`b".into(), "λ".into()])
    );
    for malformed in ["RETURN $``", "RETURN $`unterminated"] {
        let error = helix_cypher::compile(malformed).unwrap_err();
        assert_eq!(error.category, "SyntaxError");
        assert!(error.span.is_some());
    }
}

#[test]
fn mixed_batch_spans_stop_at_blocking_and_effect_boundaries() {
    for (text, end) in [
        ("UNWIND [1] AS x UNWIND [x] AS y RETURN count(*)", Some(2)),
        ("UNWIND [1] AS x WITH x UNWIND [x] AS y RETURN y", Some(3)),
        (
            "UNWIND [1] AS x OPTIONAL MATCH (n:N {key:x}) RETURN count(*)",
            Some(2),
        ),
        (
            "MATCH (n:N) OPTIONAL MATCH (n) UNWIND [n] AS y RETURN count(*)",
            Some(3),
        ),
        (
            "UNWIND [1] AS x WITH DISTINCT x UNWIND [x] AS y RETURN y",
            None,
        ),
        (
            "UNWIND [1] AS x WITH x ORDER BY x UNWIND [x] AS y RETURN y",
            None,
        ),
        (
            "UNWIND [1] AS x WITH x CREATE (:N {key:x}) UNWIND [x] AS y RETURN y",
            Some(1),
        ),
        (
            "MATCH (n:N) OPTIONAL MATCH (n)-[:R]->(m:N) UNWIND [m] AS y RETURN y",
            Some(3),
        ),
    ] {
        let query = helix_cypher::compile(text).unwrap();
        let pipeline = r::RowPipeline::new(std::sync::Arc::new(query), r::RowExecution::Batched);
        let actual = pipeline.batch_consumer(0).map(|consumer| match consumer {
            r::BatchConsumer::Pipeline { end } => end,
            _ => 1,
        });
        assert_eq!(actual, end, "{text}");
        let round_trip: r::RowPipeline =
            serde_json::from_str(&serde_json::to_string(&pipeline).unwrap()).unwrap();
        assert_eq!(round_trip.batch_consumer(0), pipeline.batch_consumer(0));
    }
}

#[test]
fn correlated_graph_batches_require_a_supported_physical_schedule() {
    for (text, batched) in [
        ("UNWIND [1] AS x MATCH (a:N),(b:N) RETURN count(*)", true),
        (
            "MATCH (a:N) OPTIONAL MATCH (a)-[:R]->(b),(c:N) RETURN count(*)",
            true,
        ),
        (
            "MATCH (a:N) OPTIONAL MATCH (a)-[:R]->(b) RETURN count(*)",
            true,
        ),
        ("UNWIND [1] AS x MATCH (n:N) RETURN n", true),
        ("MATCH (a:N) MATCH (a),(a) RETURN count(*)", true),
    ] {
        let query = helix_cypher::compile(text).unwrap();
        let plan = r::plan(query, &helix_planner::context::PlannerContext::default()).unwrap();
        assert_eq!(plan.batch_consumer(0).is_some(), batched, "{text}");
    }
}
