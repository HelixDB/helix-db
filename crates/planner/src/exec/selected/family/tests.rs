use super::*;
use crate::exec::{ElementKeyspace, KvKeyBound, KvReadPlan};
use crate::{logical, physical, properties};

fn node_source() -> logical::LogicalExpr {
    logical::LogicalExpr::Pure(logical::PureLogicalOp::Source {
        element: properties::ElementKind::Node,
    })
}

#[test]
fn selected_alternative_family_classifies_executable_pairs() {
    let kv = physical::PhysicalExpr::Access {
        element: properties::ElementKind::Node,
        access: physical::PhysicalAccess::Kv(KvReadPlan::RangeScan {
            keyspace: ElementKeyspace::NodeProperty,
            start: KvKeyBound::Unbounded,
            end: KvKeyBound::Unbounded,
            limit: None,
        }),
    };

    assert_eq!(
        SelectedExecutableAlternativeFamily::classify(&node_source(), &kv),
        SelectedExecutableAlternativeClassification::Classified(
            SelectedExecutableAlternativeFamily::KV_SOURCE
        )
    );
    assert_eq!(
        SelectedExecutableAlternativeFamily::classify(
            &node_source(),
            &physical::PhysicalExpr::Access {
                element: properties::ElementKind::Node,
                access: physical::PhysicalAccess::LabelScan,
            },
        ),
        SelectedExecutableAlternativeClassification::Unsupported
    );
    assert_eq!(
        SelectedExecutableAlternativeFamily::try_classify(
            &node_source(),
            &physical::PhysicalExpr::Access {
                element: properties::ElementKind::Node,
                access: physical::PhysicalAccess::LabelScan,
            },
        ),
        Err(super::super::construction::SelectedAlternativeConstructionError::UnsupportedLogicalPhysicalPair)
    );
}
