use crate::planning::tests::support::*;

enum SourceAccess<'a> {
    Node(&'a NodeAccessPlan),
    Edge(&'a EdgeAccessPlan),
}

#[derive(Clone, Copy)]
enum ExpectedSource {
    NodeFirstEq(&'static str),
    NodeFirstRange(&'static str),
    NodeLabel(&'static str),
    NodeUnionFirstEq(&'static str),
    NodeVector(&'static str),
    EdgeFirstEq(&'static str),
    EdgeFirstRange(&'static str),
    EdgeLabel(&'static str),
    EdgeUnionFirstEq(&'static str),
    EdgeVector(&'static str),
}

impl ExpectedSource {
    fn assert_matches(self, name: &str, access: SourceAccess<'_>) {
        match (self, access) {
            (Self::NodeFirstEq(property), SourceAccess::Node(access)) => {
                assert!(
                    matches!(
                        first_node_candidate(access),
                        NodeAccessPlan::EqualityIndex { key, .. } if key.property == property
                    ),
                    "{name}: expected first node equality index on {property}, got {access:?}"
                );
            }
            (Self::NodeFirstRange(property), SourceAccess::Node(access)) => {
                assert!(
                    matches!(
                        first_node_candidate(access),
                        NodeAccessPlan::RangeIndex { key, .. } if key.property == property
                    ),
                    "{name}: expected first node range index on {property}, got {access:?}"
                );
            }
            (Self::NodeLabel(label), SourceAccess::Node(access)) => {
                assert!(
                    matches!(
                        first_node_candidate(access),
                        NodeAccessPlan::LabelScan { label: actual } if actual == label
                    ),
                    "{name}: expected node label scan on {label}, got {access:?}"
                );
            }
            (
                Self::NodeUnionFirstEq(property),
                SourceAccess::Node(NodeAccessPlan::Union(plans)),
            ) => {
                assert!(
                    matches!(
                        plans.first().map(|plan| plan.as_ref()),
                        Some(NodeAccessPlan::EqualityIndex { key, .. }) if key.property == property
                    ),
                    "{name}: expected first node union branch on {property}, got {plans:?}"
                );
            }
            (Self::NodeVector(property), SourceAccess::Node(access)) => {
                assert!(
                    matches!(
                        access,
                        NodeAccessPlan::VectorSearch { key, .. } if key.property == property
                    ),
                    "{name}: expected node vector search on {property}, got {access:?}"
                );
            }
            (Self::EdgeFirstEq(property), SourceAccess::Edge(access)) => {
                assert!(
                    matches!(
                        first_edge_candidate(access),
                        EdgeAccessPlan::EqualityIndex { key, .. } if key.property == property
                    ),
                    "{name}: expected first edge equality index on {property}, got {access:?}"
                );
            }
            (Self::EdgeFirstRange(property), SourceAccess::Edge(access)) => {
                assert!(
                    matches!(
                        first_edge_candidate(access),
                        EdgeAccessPlan::RangeIndex { key, .. } if key.property == property
                    ),
                    "{name}: expected first edge range index on {property}, got {access:?}"
                );
            }
            (Self::EdgeLabel(label), SourceAccess::Edge(access)) => {
                assert!(
                    matches!(
                        first_edge_candidate(access),
                        EdgeAccessPlan::LabelScan { label: actual } if actual == label
                    ),
                    "{name}: expected edge label scan on {label}, got {access:?}"
                );
            }
            (
                Self::EdgeUnionFirstEq(property),
                SourceAccess::Edge(EdgeAccessPlan::Union(plans)),
            ) => {
                assert!(
                    matches!(
                        plans.first().map(|plan| plan.as_ref()),
                        Some(EdgeAccessPlan::EqualityIndex { key, .. }) if key.property == property
                    ),
                    "{name}: expected first edge union branch on {property}, got {plans:?}"
                );
            }
            (Self::EdgeVector(property), SourceAccess::Edge(access)) => {
                assert!(
                    matches!(
                        access,
                        EdgeAccessPlan::VectorSearch { key, .. } if key.property == property
                    ),
                    "{name}: expected edge vector search on {property}, got {access:?}"
                );
            }
            (expected, access) => {
                panic!(
                    "{name}: expected {}, got {:?}",
                    expected.name(),
                    access_name(access)
                );
            }
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::NodeFirstEq(_) => "node equality index",
            Self::NodeFirstRange(_) => "node range index",
            Self::NodeLabel(_) => "node label scan",
            Self::NodeUnionFirstEq(_) => "node union",
            Self::NodeVector(_) => "node vector search",
            Self::EdgeFirstEq(_) => "edge equality index",
            Self::EdgeFirstRange(_) => "edge range index",
            Self::EdgeLabel(_) => "edge label scan",
            Self::EdgeUnionFirstEq(_) => "edge union",
            Self::EdgeVector(_) => "edge vector search",
        }
    }
}

fn first_node_candidate(access: &NodeAccessPlan) -> &NodeAccessPlan {
    match access {
        NodeAccessPlan::ScanThenFilter { source, .. } => first_node_candidate(source.as_ref()),
        NodeAccessPlan::Intersect(plans) | NodeAccessPlan::Union(plans) => plans
            .first()
            .map(|plan| plan.as_ref())
            .expect("planner should not construct empty node set combinations"),
        other => other,
    }
}

fn first_edge_candidate(access: &EdgeAccessPlan) -> &EdgeAccessPlan {
    match access {
        EdgeAccessPlan::ScanThenFilter { source, .. } => first_edge_candidate(source.as_ref()),
        EdgeAccessPlan::Intersect(plans) | EdgeAccessPlan::Union(plans) => plans
            .first()
            .map(|plan| plan.as_ref())
            .expect("planner should not construct empty edge set combinations"),
        other => other,
    }
}

fn access_name(access: SourceAccess<'_>) -> &'static str {
    match access {
        SourceAccess::Node(_) => "node source",
        SourceAccess::Edge(_) => "edge source",
    }
}

fn source_access(op: &PhysicalOp) -> SourceAccess<'_> {
    match op {
        PhysicalOp::NodeAccess(access) => SourceAccess::Node(access),
        PhysicalOp::EdgeAccess(access) => SourceAccess::Edge(access),
        PhysicalOp::Expand { input, .. }
        | PhysicalOp::Filter { input, .. }
        | PhysicalOp::Limit { input, .. }
        | PhysicalOp::Skip { input, .. }
        | PhysicalOp::Range { input, .. }
        | PhysicalOp::Distinct { input }
        | PhysicalOp::Order { input, .. }
        | PhysicalOp::TopN { input, .. }
        | PhysicalOp::Project { input, .. }
        | PhysicalOp::Aggregate { input, .. }
        | PhysicalOp::Branch { input, .. }
        | PhysicalOp::Repeat { input, .. }
        | PhysicalOp::Reserved { input, .. } => source_access(input),
        PhysicalOp::Variable(VariablePlan::Stream { input, .. }) => source_access(input),
        PhysicalOp::Variable(VariablePlan::SourceInject { .. })
        | PhysicalOp::ShortestPath(_)
        | PhysicalOp::Mutation(_)
        | PhysicalOp::IndexDdl(_) => panic!("operation has no source access: {op:?}"),
    }
}

fn expand_depth(op: &PhysicalOp) -> usize {
    match op {
        PhysicalOp::Expand { input, .. } => 1 + expand_depth(input),
        PhysicalOp::Filter { input, .. }
        | PhysicalOp::Limit { input, .. }
        | PhysicalOp::Skip { input, .. }
        | PhysicalOp::Range { input, .. }
        | PhysicalOp::Distinct { input }
        | PhysicalOp::Order { input, .. }
        | PhysicalOp::TopN { input, .. }
        | PhysicalOp::Project { input, .. }
        | PhysicalOp::Aggregate { input, .. }
        | PhysicalOp::Branch { input, .. }
        | PhysicalOp::Repeat { input, .. }
        | PhysicalOp::Reserved { input, .. } => expand_depth(input),
        PhysicalOp::Variable(VariablePlan::Stream { input, .. }) => expand_depth(input),
        PhysicalOp::NodeAccess(_)
        | PhysicalOp::EdgeAccess(_)
        | PhysicalOp::ShortestPath(_)
        | PhysicalOp::Variable(VariablePlan::SourceInject { .. })
        | PhysicalOp::Mutation(_)
        | PhysicalOp::IndexDdl(_) => 0,
    }
}

fn with_node_hops(mut root: AstNode, depth: usize) -> AstNode {
    const LABELS: [&str; 7] = [
        "FOLLOWS",
        "LIKES",
        "MENTIONS",
        "OWNS",
        "REVIEWS",
        "CITES",
        "DEPENDS_ON",
    ];
    for (index, label) in LABELS.iter().cycle().take(depth).enumerate() {
        let input = Box::new(root);
        root = match index % 3 {
            0 => AstNode::Out {
                input,
                label: Some((*label).to_string()),
            },
            1 => AstNode::In {
                input,
                label: Some((*label).to_string()),
            },
            _ => AstNode::Both {
                input,
                label: Some((*label).to_string()),
            },
        };
    }
    root
}

#[test]
fn large_query_corpus_keeps_optimal_indexed_sources_through_seven_hops() {
    let corpus_ctx = PlannerContext {
        indexes: builtin_label_indexes()
            .with_node_eq(ScopedPropertyKey::try_new("User", "tenant_id").unwrap())
            .with_node_eq(ScopedPropertyKey::try_new("User", "username").unwrap())
            .with_node_eq(ScopedPropertyKey::try_new("User", "email").unwrap())
            .with_node_eq(ScopedPropertyKey::try_new("User", "status").unwrap())
            .with_node_range(
                ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc)
                    .unwrap(),
            )
            .with_node_eq(ScopedPropertyKey::try_new("Account", "account_id").unwrap())
            .with_node_eq(ScopedPropertyKey::try_new("Account", "tier").unwrap())
            .with_node_range(
                ScopedPropertyDirectionKey::try_new("Account", "score", RangeIndexDirection::Asc)
                    .unwrap(),
            )
            .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "tenant_id").unwrap())
            .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap())
            .with_edge_range(
                ScopedPropertyDirectionKey::try_new("FOLLOWS", "since", RangeIndexDirection::Asc)
                    .unwrap(),
            )
            .with_vector(
                SearchIndexKey::try_new(ElementKind::Node, "Doc", "embedding").unwrap(),
                SearchIndexScope::Unscoped,
            )
            .with_vector(
                SearchIndexKey::try_new(ElementKind::Edge, "MENTIONS", "embedding").unwrap(),
                SearchIndexScope::Unscoped,
            ),
        stats: StatsSnapshot::default()
            .with_node_label_cardinality(NonEmptyString::new("User").unwrap(), 50_000)
            .with_node_label_cardinality(NonEmptyString::new("Account").unwrap(), 2_000)
            .with_node_label_cardinality(NonEmptyString::new("Doc").unwrap(), 1_200)
            .with_edge_label_cardinality(NonEmptyString::new("FOLLOWS").unwrap(), 1_000_000)
            .with_edge_label_cardinality(NonEmptyString::new("MENTIONS").unwrap(), 300_000)
            .with_edge_label_cardinality(NonEmptyString::new("LIKES").unwrap(), 700_000)
            .with_node_eq_cardinality(ScopedPropertyKey::try_new("User", "tenant_id").unwrap(), 3)
            .with_node_eq_cardinality(ScopedPropertyKey::try_new("User", "username").unwrap(), 4)
            .with_node_eq_cardinality(ScopedPropertyKey::try_new("User", "email").unwrap(), 1)
            .with_node_eq_cardinality(ScopedPropertyKey::try_new("User", "status").unwrap(), 500)
            .with_node_range_cardinality(
                ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc)
                    .unwrap(),
                900,
            )
            .with_node_eq_cardinality(
                ScopedPropertyKey::try_new("Account", "account_id").unwrap(),
                1,
            )
            .with_node_eq_cardinality(ScopedPropertyKey::try_new("Account", "tier").unwrap(), 75)
            .with_node_range_cardinality(
                ScopedPropertyDirectionKey::try_new("Account", "score", RangeIndexDirection::Asc)
                    .unwrap(),
                250,
            )
            .with_edge_eq_cardinality(
                ScopedPropertyKey::try_new("FOLLOWS", "tenant_id").unwrap(),
                30,
            )
            .with_edge_eq_cardinality(
                ScopedPropertyKey::try_new("FOLLOWS", "status").unwrap(),
                900,
            )
            .with_edge_range_cardinality(
                ScopedPropertyDirectionKey::try_new("FOLLOWS", "since", RangeIndexDirection::Asc)
                    .unwrap(),
                5_000,
            ),
        ..PlannerContext::default()
    };

    let node_bases = vec![
        (
            "user_tenant_eq",
            g().n_with_label_where("User", Predicate::eq("tenant_id", "acme"))
                .into_ast(),
            ExpectedSource::NodeFirstEq("tenant_id"),
        ),
        (
            "user_age_range",
            g().n_with_label_where("User", Predicate::gte("age", 21))
                .into_ast(),
            ExpectedSource::NodeFirstRange("age"),
        ),
        (
            "user_status_eq",
            g().n_with_label_where("User", Predicate::eq("status", "active"))
                .into_ast(),
            ExpectedSource::NodeFirstEq("status"),
        ),
        (
            "user_union",
            g().n_where(Predicate::or(vec![
                Predicate::and(vec![
                    Predicate::eq("$label", "User"),
                    Predicate::gte("age", 21),
                ]),
                Predicate::and(vec![
                    Predicate::eq("$label", "User"),
                    Predicate::eq("username", "alice"),
                ]),
                Predicate::and(vec![
                    Predicate::eq("$label", "User"),
                    Predicate::eq("email", "alice@example.com"),
                ]),
            ]))
            .into_ast(),
            ExpectedSource::NodeUnionFirstEq("email"),
        ),
        (
            "account_multi_index",
            g().n_with_label_where(
                "Account",
                Predicate::and(vec![
                    Predicate::gte("score", 700),
                    Predicate::eq("tier", "enterprise"),
                    Predicate::eq("account_id", "acct_123"),
                ]),
            )
            .into_ast(),
            ExpectedSource::NodeFirstEq("account_id"),
        ),
        (
            "account_tier_eq",
            g().n_with_label_where("Account", Predicate::eq("tier", "enterprise"))
                .into_ast(),
            ExpectedSource::NodeFirstEq("tier"),
        ),
        (
            "doc_label",
            g().n_with_label("Doc").into_ast(),
            ExpectedSource::NodeLabel("Doc"),
        ),
        (
            "doc_vector",
            g().vector_search_nodes("Doc", "embedding", vec![0.1f32, 0.2, 0.3], 12, None)
                .into_ast(),
            ExpectedSource::NodeVector("embedding"),
        ),
    ];

    let edge_bases = vec![
        (
            "follows_tenant_eq",
            g().e_with_label_where("FOLLOWS", Predicate::eq("tenant_id", "acme"))
                .out_n()
                .into_ast(),
            ExpectedSource::EdgeFirstEq("tenant_id"),
        ),
        (
            "follows_since_range",
            g().e_with_label_where("FOLLOWS", Predicate::gte("since", 2020))
                .out_n()
                .into_ast(),
            ExpectedSource::EdgeFirstRange("since"),
        ),
        (
            "follows_status_eq",
            g().e_with_label_where("FOLLOWS", Predicate::eq("status", "active"))
                .out_n()
                .into_ast(),
            ExpectedSource::EdgeFirstEq("status"),
        ),
        (
            "follows_union",
            g().e_where(Predicate::or(vec![
                Predicate::and(vec![
                    Predicate::eq("$label", "FOLLOWS"),
                    Predicate::eq("status", "active"),
                ]),
                Predicate::and(vec![
                    Predicate::eq("$label", "FOLLOWS"),
                    Predicate::eq("tenant_id", "acme"),
                ]),
                Predicate::and(vec![
                    Predicate::eq("$label", "FOLLOWS"),
                    Predicate::gte("since", 2020),
                ]),
            ]))
            .out_n()
            .into_ast(),
            ExpectedSource::EdgeUnionFirstEq("tenant_id"),
        ),
        (
            "mentions_label",
            g().e_with_label("MENTIONS").out_n().into_ast(),
            ExpectedSource::EdgeLabel("MENTIONS"),
        ),
        (
            "likes_label",
            g().e_with_label("LIKES").out_n().into_ast(),
            ExpectedSource::EdgeLabel("LIKES"),
        ),
        (
            "mentions_vector",
            g().vector_search_edges("MENTIONS", "embedding", vec![0.4f32, 0.5], 9, None)
                .out_n()
                .into_ast(),
            ExpectedSource::EdgeVector("embedding"),
        ),
    ];

    let mut cases = Vec::new();
    for depth in 0..=7 {
        for (name, root, expected) in &node_bases {
            cases.push((
                format!("{name}_{depth}_node_hops"),
                with_node_hops(root.clone(), depth),
                depth,
                *expected,
            ));
        }
    }
    for total_depth in 1..=7 {
        for (name, root, expected) in &edge_bases {
            cases.push((
                format!("{name}_{total_depth}_total_hops"),
                with_node_hops(root.clone(), total_depth - 1),
                total_depth,
                *expected,
            ));
        }
    }

    assert_eq!(cases.len(), 113);
    for (name, root, expected_depth, expected_source) in cases {
        let plan = plan_ast(root, corpus_ctx.clone());
        let op = run_op(&plan);
        assert_eq!(
            expand_depth(op),
            expected_depth,
            "{name}: unexpected expand depth in {op:?}"
        );
        expected_source.assert_matches(&name, source_access(op));
    }
}
