use std::collections::BTreeSet;

use super::*;
use crate::error;

#[test]
fn native_unsupported_reasons_are_stable_and_unique() {
    let mut reasons = BTreeSet::new();
    for reason in NativeUnsupportedReason::ALL {
        assert!(!reason.as_str().is_empty());
        assert!(reasons.insert(reason.as_str()));
        assert_eq!(
            unsupported(*reason),
            error::PlannerError::UnsupportedCascadesPlan {
                reason: reason.as_str().to_owned()
            }
        );
    }
    assert_eq!(reasons.len(), NativeUnsupportedReason::ALL.len());
}
