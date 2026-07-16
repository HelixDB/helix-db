//! Access-filter index-union branch limit policy.

use crate::context;

pub(super) fn max_index_union_branches(planner_limits: &context::PlannerLimits) -> Option<usize> {
    match planner_limits.max_index_union_branches {
        context::IndexUnionBranchLimit::Disabled => None,
        context::IndexUnionBranchLimit::Limited(limit) => Some(limit.get()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn max_index_union_branches_distinguishes_disabled_and_limited_modes() {
        let disabled = context::PlannerLimits {
            max_index_union_branches: context::IndexUnionBranchLimit::Disabled,
        };
        assert_eq!(max_index_union_branches(&disabled), None);

        let limited = context::PlannerLimits {
            max_index_union_branches: context::IndexUnionBranchLimit::limited(3).unwrap(),
        };
        assert_eq!(max_index_union_branches(&limited), Some(3));
    }
}
