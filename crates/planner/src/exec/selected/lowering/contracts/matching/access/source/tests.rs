use super::{edge, node, SelectedAccessShapeMatch, SelectedAccessShapeMismatch};
use crate::{ir, physical};

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
