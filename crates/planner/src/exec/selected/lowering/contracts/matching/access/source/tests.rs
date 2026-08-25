use super::{edge, node, SelectedAccessShapeMatch, SelectedAccessShapeMismatch};
use crate::{catalog, exec, ir, physical};

#[test]
fn node_runtime_access_matches_only_runtime_physical_access() {
    let plan = ir::NodeAccessPlan::FromParam {
        param: ir::NonEmptyString::new("node_ids").expect("test name is non-empty"),
    };

    assert_eq!(
        node::selected_node_access_match(&plan, &physical::PhysicalAccess::RuntimeInput),
        SelectedAccessShapeMatch::Matched
    );
    assert_eq!(
        node::selected_node_access_match(&plan, &physical::PhysicalAccess::Empty),
        SelectedAccessShapeMatch::NotMatched(
            SelectedAccessShapeMismatch::PhysicalAccessFamilyMismatch
        )
    );
    assert!(node::selected_node_access_matches(
        &plan,
        &physical::PhysicalAccess::RuntimeInput,
    ));
    assert!(!node::selected_node_access_matches(
        &plan,
        &physical::PhysicalAccess::Empty,
    ));
}

#[test]
fn scan_then_filter_reports_pipeline_requirement_for_node_and_edge() {
    let predicate = ir::PredicatePlan::new(helix_ast::expr::Predicate::eq("active", true))
        .expect("predicate is valid");

    assert_eq!(
        node::selected_node_access_match(
            &ir::NodeAccessPlan::ScanThenFilter {
                source: ir::NodeAccessSourcePlan::from_unfiltered(ir::NodeAccessPlan::AllScan),
                residual: predicate.clone(),
            },
            &physical::PhysicalAccess::Empty,
        ),
        SelectedAccessShapeMatch::NotMatched(
            SelectedAccessShapeMismatch::ResidualFilterRequiresPipeline
        )
    );
    assert_eq!(
        edge::selected_edge_access_match(
            &ir::EdgeAccessPlan::ScanThenFilter {
                source: ir::EdgeAccessSourcePlan::from_unfiltered(ir::EdgeAccessPlan::AllScan),
                residual: predicate,
            },
            &physical::PhysicalAccess::Empty,
        ),
        SelectedAccessShapeMatch::NotMatched(
            SelectedAccessShapeMismatch::ResidualFilterRequiresPipeline
        )
    );
}

#[test]
fn equality_sources_match_only_their_selected_physical_algorithm() {
    let key = catalog::ScopedPropertyKey::try_new("User", "status").unwrap();
    let literal = |value| {
        ir::IndexValue::Literal(ir::SecondaryIndexLiteral::new(value).expect("test value is valid"))
    };
    let node_plan = |value, uniqueness| ir::NodeAccessPlan::EqualityIndex {
        index: catalog::NodeEqualityIndexMeta::try_new("node_eq:User:status")
            .unwrap()
            .with_uniqueness(uniqueness),
        key: key.clone(),
        value,
    };
    let cases = [
        node_plan(
            literal(helix_ast::value::PropertyValue::from("active")),
            catalog::IndexUniqueness::NonUnique,
        ),
        node_plan(
            literal(helix_ast::value::PropertyValue::from("active")),
            catalog::IndexUniqueness::Unique,
        ),
        node_plan(
            literal(helix_ast::value::PropertyValue::Null),
            catalog::IndexUniqueness::NonUnique,
        ),
        node_plan(
            literal(helix_ast::value::PropertyValue::F64(f64::NAN)),
            catalog::IndexUniqueness::NonUnique,
        ),
        node_plan(
            ir::IndexValue::Param(ir::NonEmptyString::new("late_status").unwrap()),
            catalog::IndexUniqueness::Unique,
        ),
    ];
    let generic_algorithms = [
        physical::PhysicalAccess::Empty,
        physical::PhysicalAccess::SetUnion,
        physical::PhysicalAccess::SetIntersection,
        physical::PhysicalAccess::RangeIndex,
    ];
    for plan in cases {
        let ir::NodeAccessPlan::EqualityIndex { index, key, value } = &plan else {
            unreachable!()
        };
        let exact = physical::PhysicalAccess::NodeExact(Box::new(
            exec::ExecNodeAccessPlan::exact_equality(index.clone(), key.clone(), value.clone()),
        ));
        assert!(node::selected_node_access_matches(&plan, &exact));
        for generic in &generic_algorithms {
            assert!(!node::selected_node_access_matches(&plan, generic));
        }
    }

    let active = node_plan(
        literal(helix_ast::value::PropertyValue::from("active")),
        catalog::IndexUniqueness::NonUnique,
    );
    let paused = node_plan(
        literal(helix_ast::value::PropertyValue::from("paused")),
        catalog::IndexUniqueness::NonUnique,
    );
    let ir::NodeAccessPlan::EqualityIndex { index, key, value } = paused else {
        unreachable!()
    };
    assert!(!node::selected_node_access_matches(
        &active,
        &physical::PhysicalAccess::NodeExact(Box::new(exec::ExecNodeAccessPlan::exact_equality(
            index,
            key.clone(),
            value,
        ),)),
    ));

    let edge = ir::EdgeAccessPlan::EqualityIndex {
        index: catalog::EdgeEqualityIndexMeta::try_new("edge_eq:User:status").unwrap(),
        key,
        value: literal(helix_ast::value::PropertyValue::from("active")),
    };
    let ir::EdgeAccessPlan::EqualityIndex { index, key, value } = &edge else {
        unreachable!()
    };
    assert!(edge::selected_edge_access_matches(
        &edge,
        &physical::PhysicalAccess::EdgeExact(Box::new(exec::ExecEdgeAccessPlan::exact_equality(
            index.clone(),
            key.clone(),
            value.clone()
        ),)),
    ));
    assert!(!edge::selected_edge_access_matches(
        &edge,
        &physical::PhysicalAccess::Empty,
    ));
    assert!(!edge::selected_edge_access_matches(
        &edge,
        &physical::PhysicalAccess::EdgeExact(Box::new(exec::ExecEdgeAccessPlan::exact_equality(
            index.clone(),
            key.clone(),
            literal(helix_ast::value::PropertyValue::from("paused")),
        ),)),
    ));
}

#[test]
fn secondary_sets_require_the_exact_payload_and_mixed_sets_keep_explicit_merges() {
    let source = |property: &str, value: ir::IndexValue| {
        ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::EqualityIndex {
            index: catalog::NodeEqualityIndexMeta::try_new(format!("node_eq:User:{property}"))
                .unwrap(),
            key: catalog::ScopedPropertyKey::try_new("User", property).unwrap(),
            value,
        })
        .unwrap()
    };
    let literal = |value: &str| {
        ir::IndexValue::Literal(
            ir::SecondaryIndexLiteral::new(helix_ast::value::PropertyValue::from(value)).unwrap(),
        )
    };
    let batch = ir::NodeAccessPlan::Union(ir::AtLeast::from_pair(
        source("status", literal("active")),
        source("status", literal("paused")),
    ));
    let batch_exact =
        physical::PhysicalAccess::NodeExact(Box::new(exec::ExecNodeAccessPlan::SecondarySet {
            set: exec::node_secondary_set(&batch).unwrap(),
        }));
    assert!(node::selected_node_access_matches(&batch, &batch_exact,));
    assert!(!node::selected_node_access_matches(
        &batch,
        &physical::PhysicalAccess::SetUnion,
    ));

    let explicit = ir::NodeAccessPlan::Union(ir::AtLeast::from_pair(
        source("status", literal("active")),
        source("role", literal("admin")),
    ));
    let explicit_exact =
        physical::PhysicalAccess::NodeExact(Box::new(exec::ExecNodeAccessPlan::SecondarySet {
            set: exec::node_secondary_set(&explicit).unwrap(),
        }));
    assert!(node::selected_node_access_matches(
        &explicit,
        &explicit_exact,
    ));
    assert!(!node::selected_node_access_matches(&explicit, &batch_exact,));

    let intersection = ir::NodeAccessPlan::Intersect(ir::AtLeast::from_pair(
        source("status", literal("active")),
        source("role", literal("admin")),
    ));
    let intersection_exact =
        physical::PhysicalAccess::NodeExact(Box::new(exec::ExecNodeAccessPlan::SecondarySet {
            set: exec::node_secondary_set(&intersection).unwrap(),
        }));
    assert!(node::selected_node_access_matches(
        &intersection,
        &intersection_exact,
    ));
    assert!(!node::selected_node_access_matches(
        &intersection,
        &physical::PhysicalAccess::SetIntersection,
    ));

    let edge_source = |property: &str, value: ir::IndexValue| {
        ir::EdgeAccessSourcePlan::new(ir::EdgeAccessPlan::EqualityIndex {
            index: catalog::EdgeEqualityIndexMeta::try_new(format!("edge_eq:FOLLOWS:{property}"))
                .unwrap(),
            key: catalog::ScopedPropertyKey::try_new("FOLLOWS", property).unwrap(),
            value,
        })
        .unwrap()
    };
    let edge_batch = ir::EdgeAccessPlan::Union(ir::AtLeast::from_pair(
        edge_source("status", literal("active")),
        edge_source("status", literal("paused")),
    ));
    let edge_batch_exact =
        physical::PhysicalAccess::EdgeExact(Box::new(exec::ExecEdgeAccessPlan::SecondarySet {
            set: exec::edge_secondary_set(&edge_batch).unwrap(),
        }));
    assert!(edge::selected_edge_access_matches(
        &edge_batch,
        &edge_batch_exact,
    ));
    assert!(!edge::selected_edge_access_matches(
        &edge_batch,
        &physical::PhysicalAccess::SetUnion,
    ));
    let edge_explicit = ir::EdgeAccessPlan::Union(ir::AtLeast::from_pair(
        edge_source("status", literal("active")),
        edge_source("kind", literal("friend")),
    ));
    let edge_explicit_exact =
        physical::PhysicalAccess::EdgeExact(Box::new(exec::ExecEdgeAccessPlan::SecondarySet {
            set: exec::edge_secondary_set(&edge_explicit).unwrap(),
        }));
    assert!(edge::selected_edge_access_matches(
        &edge_explicit,
        &edge_explicit_exact,
    ));
    assert!(!edge::selected_edge_access_matches(
        &edge_explicit,
        &edge_batch_exact,
    ));

    let edge_intersection = ir::EdgeAccessPlan::Intersect(ir::AtLeast::from_pair(
        edge_source("status", literal("active")),
        edge_source("kind", literal("friend")),
    ));
    let edge_intersection_exact =
        physical::PhysicalAccess::EdgeExact(Box::new(exec::ExecEdgeAccessPlan::SecondarySet {
            set: exec::edge_secondary_set(&edge_intersection).unwrap(),
        }));
    assert!(edge::selected_edge_access_matches(
        &edge_intersection,
        &edge_intersection_exact,
    ));
    assert!(!edge::selected_edge_access_matches(
        &edge_intersection,
        &physical::PhysicalAccess::SetIntersection,
    ));
    assert!(!edge::selected_edge_access_matches(
        &edge_intersection,
        &edge_batch_exact,
    ));

    let node_mixed = ir::NodeAccessPlan::Union(ir::AtLeast::from_pair(
        ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::PointIds {
            ids: ir::ElementIds::new(ir::AtLeast::from_one(7)).unwrap(),
        })
        .unwrap(),
        source("status", literal("active")),
    ));
    assert!(node::selected_node_access_matches(
        &node_mixed,
        &physical::PhysicalAccess::SetUnion,
    ));
    assert!(!node::selected_node_access_matches(
        &node_mixed,
        &batch_exact
    ));

    let node_mixed_intersection = ir::NodeAccessPlan::Intersect(ir::AtLeast::from_pair(
        ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::PointIds {
            ids: ir::ElementIds::new(ir::AtLeast::from_one(8)).unwrap(),
        })
        .unwrap(),
        source("status", literal("active")),
    ));
    assert!(node::selected_node_access_matches(
        &node_mixed_intersection,
        &physical::PhysicalAccess::SetIntersection,
    ));
    assert!(!node::selected_node_access_matches(
        &node_mixed_intersection,
        &intersection_exact,
    ));

    let edge_mixed = ir::EdgeAccessPlan::Union(ir::AtLeast::from_pair(
        ir::EdgeAccessSourcePlan::new(ir::EdgeAccessPlan::PointIds {
            ids: ir::ElementIds::new(ir::AtLeast::from_one(9)).unwrap(),
        })
        .unwrap(),
        edge_source("status", literal("active")),
    ));
    assert!(edge::selected_edge_access_matches(
        &edge_mixed,
        &physical::PhysicalAccess::SetUnion,
    ));
    assert!(!edge::selected_edge_access_matches(
        &edge_mixed,
        &edge_batch_exact,
    ));

    let edge_mixed_intersection = ir::EdgeAccessPlan::Intersect(ir::AtLeast::from_pair(
        ir::EdgeAccessSourcePlan::new(ir::EdgeAccessPlan::PointIds {
            ids: ir::ElementIds::new(ir::AtLeast::from_one(10)).unwrap(),
        })
        .unwrap(),
        edge_source("status", literal("active")),
    ));
    assert!(edge::selected_edge_access_matches(
        &edge_mixed_intersection,
        &physical::PhysicalAccess::SetIntersection,
    ));
    assert!(!edge::selected_edge_access_matches(
        &edge_mixed_intersection,
        &edge_intersection_exact,
    ));
}
