use std::num::NonZeroUsize;

use super::*;
use crate::exec::selected::physical::SelectedPhysicalPlan;
use crate::exec::selected::provenance::test_selected_root_provenance;
use crate::exec::selected::run::SelectedExecutableRunRoot;
use crate::exec::selected::SelectedRootConstructionError;
use crate::{cost, ir, logical, physical, properties};

fn selected_input() -> SelectedExecutableRunRoot {
    SelectedExecutableRunRoot::alternative(
        logical::LogicalExpr::Pure(logical::PureLogicalOp::NoOp),
        physical::PhysicalAlternative::new(
            physical::PhysicalExpr::NoOp,
            properties::DeliveredProperties::default(),
            cost::CostVector::ZERO,
        ),
    )
}

fn selected_physical(op: physical::PhysicalControlOp) -> SelectedPhysicalPlan {
    SelectedPhysicalPlan::new(
        physical::PhysicalExpr::Control(op),
        properties::DeliveredProperties::default(),
        cost::CostVector::ZERO,
    )
}

#[test]
fn root_branch_constructor_preserves_contract_parts() {
    let alternative = selected_physical(physical::PhysicalControlOp::Branch);
    let provenance = test_selected_root_provenance();
    let input = selected_input();
    let plan = SelectedBranchPlan::Optional(Box::new(selected_input()));

    let root = SelectedRootBranch::new(
        alternative.clone(),
        provenance.clone(),
        Box::new(input.clone()),
        plan.clone(),
    )
    .unwrap();

    assert_eq!(root.alternative(), &alternative);
    assert_eq!(root.provenance(), &provenance);
    assert_eq!(root.input(), &input);
    assert_eq!(root.plan(), &plan);
    assert_eq!(
        root.into_parts(),
        (alternative, provenance, Box::new(input), plan)
    );
}

#[test]
fn root_repeat_constructor_preserves_contract_parts() {
    let alternative = selected_physical(physical::PhysicalControlOp::Repeat);
    let provenance = test_selected_root_provenance();
    let input = selected_input();
    let plan = SelectedRepeatPlan {
        body: Box::new(selected_input()),
        stop: ir::RepeatStopPlan::MaxDepthOnly,
        emit: ir::RepeatEmitPlan::None,
        max_depth: NonZeroUsize::new(3).unwrap(),
    };

    let root = SelectedRootRepeat::new(
        alternative.clone(),
        provenance.clone(),
        Box::new(input.clone()),
        plan.clone(),
    )
    .unwrap();

    assert_eq!(root.alternative(), &alternative);
    assert_eq!(root.provenance(), &provenance);
    assert_eq!(root.input(), &input);
    assert_eq!(root.plan(), &plan);
    assert_eq!(
        root.into_parts(),
        (alternative, provenance, Box::new(input), plan)
    );
}

#[test]
fn root_control_constructors_reject_wrong_physical_family() {
    assert_eq!(
        SelectedRootBranch::new(
            selected_physical(physical::PhysicalControlOp::Repeat),
            test_selected_root_provenance(),
            Box::new(selected_input()),
            SelectedBranchPlan::Optional(Box::new(selected_input())),
        ),
        Err(SelectedRootConstructionError::IncompatiblePhysicalShape)
    );

    assert_eq!(
        SelectedRootRepeat::new(
            selected_physical(physical::PhysicalControlOp::Branch),
            test_selected_root_provenance(),
            Box::new(selected_input()),
            SelectedRepeatPlan {
                body: Box::new(selected_input()),
                stop: ir::RepeatStopPlan::MaxDepthOnly,
                emit: ir::RepeatEmitPlan::None,
                max_depth: NonZeroUsize::new(3).unwrap(),
            },
        ),
        Err(SelectedRootConstructionError::IncompatiblePhysicalShape)
    );
}
