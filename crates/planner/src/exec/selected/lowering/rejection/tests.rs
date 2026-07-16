use std::collections::BTreeSet;

use super::*;
use crate::{exec::ExecPlanError, ir};

#[test]
fn selected_executable_rejection_reasons_are_stable_and_unique() {
    let mut reasons = BTreeSet::new();
    for reason in Reason::ALL {
        assert!(!reason.as_str().is_empty());
        assert!(reasons.insert(reason.as_str()));
        assert_eq!(
            unsupported(*reason),
            ExecPlanError::UnsupportedSelectedExecutableAlternative {
                reason: ir::NonEmptyString::from_static(reason.as_str())
            }
        );
    }
    assert_eq!(reasons.len(), Reason::ALL.len());
}
