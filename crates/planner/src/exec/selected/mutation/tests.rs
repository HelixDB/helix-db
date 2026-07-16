use super::*;
use crate::exec::selected::physical::SelectedPhysicalPlan;
use crate::exec::selected::provenance::test_selected_root_provenance;
use crate::exec::selected::SelectedRootConstructionError;
use crate::{cost, ir, physical, properties};

fn selected_physical() -> SelectedPhysicalPlan {
    SelectedPhysicalPlan::new(
        physical::PhysicalExpr::Barrier,
        properties::DeliveredProperties::default(),
        cost::CostVector::ZERO,
    )
}

#[test]
fn root_mutation_constructor_preserves_contract_parts() {
    let alternative = selected_physical();
    let provenance = test_selected_root_provenance();
    let plan = SelectedMutationPlan::AddNode {
        input: SelectedMutationInput::Source,
        label: ir::NonEmptyString::from_static("User"),
        properties: ir::PropertyAssignments::default(),
    };

    let root =
        SelectedRootMutation::new(alternative.clone(), provenance.clone(), plan.clone()).unwrap();

    assert_eq!(root.alternative(), &alternative);
    assert_eq!(root.provenance(), &provenance);
    assert_eq!(root.plan(), &plan);
    assert_eq!(root.into_parts(), (alternative, provenance, plan));
}

#[test]
fn root_mutation_constructor_rejects_non_barrier_physical_shape() {
    let alternative = SelectedPhysicalPlan::new(
        physical::PhysicalExpr::NoOp,
        properties::DeliveredProperties::default(),
        cost::CostVector::ZERO,
    );

    assert_eq!(
        SelectedRootMutation::new(
            alternative,
            test_selected_root_provenance(),
            SelectedMutationPlan::AddNode {
                input: SelectedMutationInput::Source,
                label: ir::NonEmptyString::from_static("User"),
                properties: ir::PropertyAssignments::default(),
            },
        ),
        Err(SelectedRootConstructionError::IncompatiblePhysicalShape)
    );
}
