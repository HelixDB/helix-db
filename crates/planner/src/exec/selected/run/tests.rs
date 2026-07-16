use super::*;
use crate::exec::selected::provenance::test_selected_root_provenance;
use crate::{cost, logical, physical, properties};

fn node_access_expr() -> logical::LogicalExpr {
    logical::LogicalExpr::AccessPath(logical::AccessPath::Node(logical::NodeAccessPath::new(
        crate::ir::NodeAccessSourcePlan::new(crate::ir::NodeAccessPlan::AllScan).unwrap(),
    )))
}

#[test]
fn ordinary_alternative_root_stores_classified_family() {
    let root = SelectedExecutableAlternativeRoot::new(
        node_access_expr(),
        physical::PhysicalAlternative::new(
            physical::PhysicalExpr::Access {
                element: properties::ElementKind::Node,
                access: physical::PhysicalAccess::LabelScan,
            },
            properties::DeliveredProperties::default(),
            cost::CostVector::ZERO,
        ),
        test_selected_root_provenance(),
    )
    .unwrap();

    assert_eq!(
        root.family(),
        super::super::family::SelectedExecutableAlternativeFamily::NODE_ACCESS_PATH
    );
}

#[test]
fn ordinary_alternative_root_rejects_unsupported_pairs() {
    assert_eq!(
        SelectedExecutableAlternativeRoot::new(
            logical::LogicalExpr::Pure(logical::PureLogicalOp::Empty),
            physical::PhysicalAlternative::new(
                physical::PhysicalExpr::Empty,
                properties::DeliveredProperties::default(),
                cost::CostVector::ZERO,
            ),
            test_selected_root_provenance(),
        ),
        Err(super::super::SelectedAlternativeConstructionError::UnsupportedLogicalPhysicalPair)
    );
}

#[test]
fn classified_ordinary_alternative_root_rejects_wrong_family_proofs() {
    assert_eq!(
        SelectedExecutableAlternativeRoot::new_classified(
            node_access_expr(),
            physical::PhysicalAlternative::new(
                physical::PhysicalExpr::Access {
                    element: properties::ElementKind::Node,
                    access: physical::PhysicalAccess::LabelScan,
                },
                properties::DeliveredProperties::default(),
                cost::CostVector::ZERO,
            ),
            test_selected_root_provenance(),
            super::super::family::SelectedExecutableAlternativeFamily::EDGE_ACCESS_PATH,
        ),
        Err(super::super::SelectedAlternativeConstructionError::ClassifiedFamilyMismatch)
    );
}
