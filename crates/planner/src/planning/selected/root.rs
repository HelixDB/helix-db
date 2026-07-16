//! Selectable Cascades run-root contracts.
//!
//! A selectable root always carries the logical expression and the stable digest
//! used to cache or batch it, so cache keys cannot be computed from a different
//! value than the optimizer input.

use crate::{digest, logical};

const SELECTED_LOGICAL_RUN_ROOT_DIGEST_TAG: &str = "selected_logical_run_root:v1";

#[derive(Debug, Clone, PartialEq)]
pub(super) struct SelectableRunRoot {
    expr: logical::LogicalExpr,
    digest: digest::PlanDigest,
}

impl SelectableRunRoot {
    pub(super) fn new(expr: logical::LogicalExpr) -> Self {
        let digest =
            digest::PlanDigest::for_tagged_value(SELECTED_LOGICAL_RUN_ROOT_DIGEST_TAG, &expr);
        Self { expr, digest }
    }

    pub(super) const fn expr(&self) -> &logical::LogicalExpr {
        &self.expr
    }

    pub(super) const fn digest(&self) -> digest::PlanDigest {
        self.digest
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir;

    #[test]
    fn selectable_root_digest_is_tied_to_logical_expression() {
        let first = SelectableRunRoot::new(logical::LogicalExpr::AccessPath(
            logical::AccessPath::Node(logical::NodeAccessPath::new(
                ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::AllScan).unwrap(),
            )),
        ));
        let second = SelectableRunRoot::new(first.expr().clone());

        assert_eq!(first, second);
        assert_eq!(first.digest(), second.digest());
    }
}
