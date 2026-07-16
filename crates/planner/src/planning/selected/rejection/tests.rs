use std::collections::BTreeSet;

use super::*;
use crate::{error, exec};

#[test]
fn selected_lowering_rejection_reasons_are_stable_and_unique() {
    let mut reasons = BTreeSet::new();
    for reason in Reason::ALL {
        assert!(!reason.as_str().is_empty());
        assert!(reasons.insert(reason.as_str()));
        assert_eq!(
            unsupported(*reason),
            error::PlannerError::UnsupportedCascadesPlan {
                reason: reason.as_str().to_owned()
            }
        );
    }
    assert_eq!(reasons.len(), Reason::ALL.len());
}

#[test]
fn selected_alternative_construction_errors_map_to_stable_rejections() {
    assert_eq!(
        unsupported_alternative_construction(
            exec::SelectedAlternativeConstructionError::UnsupportedLogicalPhysicalPair,
        ),
        unsupported(Reason::SelectedAlternativeUnsupported)
    );
    assert_eq!(
        unsupported_alternative_construction(
            exec::SelectedAlternativeConstructionError::ClassifiedFamilyMismatch,
        ),
        unsupported(Reason::SelectedAlternativeFamilyMismatch)
    );
}
