use super::super::rejection;
use super::*;
use crate::{cost, exec, ir, logical, physical, properties};

fn node_source() -> logical::LogicalExpr {
    logical::LogicalExpr::AccessPath(logical::AccessPath::Node(logical::NodeAccessPath::new(
        ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::AllScan).unwrap(),
    )))
}

fn edge_source() -> logical::LogicalExpr {
    logical::LogicalExpr::AccessPath(logical::AccessPath::Edge(logical::EdgeAccessPath::new(
        ir::EdgeAccessSourcePlan::new(ir::EdgeAccessPlan::Empty).unwrap(),
    )))
}

fn predicate() -> ir::PredicatePlan {
    ir::PredicatePlan::new(helix_ast::expr::Predicate::eq("active", true)).unwrap()
}

fn selected_root() -> exec::SelectedExecutableRunRoot {
    exec::SelectedExecutableRunRoot::alternative(
        logical::LogicalExpr::Pure(logical::PureLogicalOp::NoOp),
        physical::PhysicalAlternative::new(
            physical::PhysicalExpr::NoOp,
            properties::DeliveredProperties::default(),
            cost::CostVector::ZERO,
        ),
    )
}

#[test]
fn collect_branch_inputs_preserves_variant_order() {
    let cases = [
        (
            ir::BranchPlan::Union(ir::AtLeast::<_, 2>::from_pair(node_source(), edge_source())),
            2,
        ),
        (
            ir::BranchPlan::Choose {
                condition: predicate(),
                then_plan: Box::new(node_source()),
            },
            1,
        ),
        (
            ir::BranchPlan::ChooseElse {
                condition: predicate(),
                then_plan: Box::new(node_source()),
                else_plan: Box::new(edge_source()),
            },
            2,
        ),
        (
            ir::BranchPlan::Coalesce(ir::AtLeast::<_, 1>::from_one(node_source())),
            1,
        ),
        (ir::BranchPlan::Optional(Box::new(edge_source())), 1),
    ];

    for (plan, expected) in cases {
        let mut inputs = Vec::new();
        collect_branch_plan_inputs(&plan, &mut inputs);
        assert_eq!(inputs.len(), expected);
    }
}

#[test]
fn branch_plan_reconstruction_consumes_expected_selected_roots() {
    let cases = [
        (
            ir::BranchPlan::Union(ir::AtLeast::<_, 2>::from_pair(node_source(), edge_source())),
            2,
        ),
        (
            ir::BranchPlan::Choose {
                condition: predicate(),
                then_plan: Box::new(node_source()),
            },
            1,
        ),
        (
            ir::BranchPlan::ChooseElse {
                condition: predicate(),
                then_plan: Box::new(node_source()),
                else_plan: Box::new(edge_source()),
            },
            2,
        ),
        (
            ir::BranchPlan::Coalesce(ir::AtLeast::<_, 1>::from_one(node_source())),
            1,
        ),
        (ir::BranchPlan::Optional(Box::new(edge_source())), 1),
    ];

    for (plan, selected_count) in cases {
        let selected = (0..selected_count)
            .map(|_| selected_root())
            .collect::<Vec<_>>();
        let selected =
            SelectedBranchRoots::new(&plan, selected).expect("selected count matches plan");
        let reconstructed = selected_branch_plan_from_roots(&plan, selected)
            .expect("test supplies enough selected roots");

        match (&plan, reconstructed) {
            (ir::BranchPlan::Union(_), exec::SelectedBranchPlan::Union(plans)) => {
                assert_eq!(plans.len(), 2);
            }
            (ir::BranchPlan::Choose { .. }, exec::SelectedBranchPlan::Choose { .. })
            | (ir::BranchPlan::ChooseElse { .. }, exec::SelectedBranchPlan::ChooseElse { .. })
            | (ir::BranchPlan::Coalesce(_), exec::SelectedBranchPlan::Coalesce(_))
            | (ir::BranchPlan::Optional(_), exec::SelectedBranchPlan::Optional(_)) => {}
            (plan, reconstructed) => {
                panic!("unexpected reconstruction for {plan:?}: {reconstructed:?}");
            }
        }
    }
}

#[test]
fn branch_plan_reconstruction_rejects_missing_selected_roots() {
    let plan = ir::BranchPlan::ChooseElse {
        condition: predicate(),
        then_plan: Box::new(node_source()),
        else_plan: Box::new(edge_source()),
    };

    assert_eq!(
        SelectedBranchRoots::new(&plan, vec![selected_root()])
            .err()
            .unwrap(),
        rejection::unsupported(rejection::Reason::BranchRootArityMismatch)
    );
}

#[test]
fn branch_root_split_rejects_extra_or_missing_selected_children() {
    let plan = ir::BranchPlan::Choose {
        condition: predicate(),
        then_plan: Box::new(node_source()),
    };

    assert!(split_selected_branch_roots(&plan, vec![selected_root(), selected_root()]).is_ok());
    assert_eq!(
        split_selected_branch_roots(&plan, vec![selected_root()])
            .err()
            .unwrap(),
        rejection::unsupported(rejection::Reason::BranchRootArityMismatch)
    );
    assert_eq!(
        split_selected_branch_roots(
            &plan,
            vec![selected_root(), selected_root(), selected_root()]
        )
        .err()
        .unwrap(),
        rejection::unsupported(rejection::Reason::BranchRootArityMismatch)
    );
}
