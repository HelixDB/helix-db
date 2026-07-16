//! Intersection slot replacement contract for equality/range proofs.

use crate::ir;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum IntersectionRestriction<P> {
    Rewritten(P),
    Unchanged,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum RangeRestrictionMatch<S> {
    Found(RangeRestrictedUnion<S>),
    NotFound,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum EqualityUnionRangeRestriction<S> {
    Restricted(S),
    NotApplicable,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct RangeRestrictedUnion<S> {
    union_index: usize,
    range_index: usize,
    replacement: S,
}

impl<S> RangeRestrictedUnion<S> {
    pub(super) fn new(union_index: usize, range_index: usize, replacement: S) -> Self {
        Self {
            union_index,
            range_index,
            replacement,
        }
    }

    fn into_parts(self) -> (usize, usize, S) {
        (self.union_index, self.range_index, self.replacement)
    }
}

pub(super) fn apply_intersection_restriction<S, P, Find, IsEmpty, Build, Empty>(
    plans: &ir::AtLeast<S, 2>,
    find_restriction: Find,
    is_empty: IsEmpty,
    build_intersection: Build,
    empty_plan: Empty,
) -> IntersectionRestriction<P>
where
    S: Clone,
    Find: FnOnce(&[Option<S>]) -> RangeRestrictionMatch<S>,
    IsEmpty: Fn(&S) -> bool,
    Build: FnOnce(Vec<S>) -> P,
    Empty: FnOnce() -> P,
{
    let mut slots = plans.iter().cloned().map(Some).collect::<Vec<_>>();
    let RangeRestrictionMatch::Found(restriction) = find_restriction(&slots) else {
        return IntersectionRestriction::Unchanged;
    };
    let (union_index, range_index, replacement) = restriction.into_parts();
    if is_empty(&replacement) {
        return IntersectionRestriction::Rewritten(empty_plan());
    }
    let replacement_index = union_index.min(range_index);
    let removed_index = union_index.max(range_index);
    slots[replacement_index] = Some(replacement);
    slots[removed_index] = None;
    IntersectionRestriction::Rewritten(build_intersection(slots.into_iter().flatten().collect()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Debug, Eq, PartialEq)]
    enum TestSource {
        Empty,
        Leaf(&'static str),
    }

    #[test]
    fn restriction_replaces_lower_slot_and_removes_other_slot() {
        let plans = ir::AtLeast::<_, 2>::try_from_vec(vec![
            TestSource::Leaf("range"),
            TestSource::Leaf("label"),
            TestSource::Leaf("union"),
        ])
        .unwrap();

        let rewritten = apply_intersection_restriction(
            &plans,
            |_| {
                RangeRestrictionMatch::Found(RangeRestrictedUnion::new(
                    2,
                    0,
                    TestSource::Leaf("restricted"),
                ))
            },
            |source| matches!(source, TestSource::Empty),
            |sources| sources,
            || vec![TestSource::Empty],
        );

        assert_eq!(
            rewritten,
            IntersectionRestriction::Rewritten(vec![
                TestSource::Leaf("restricted"),
                TestSource::Leaf("label"),
            ])
        );
    }

    #[test]
    fn empty_replacement_short_circuits_whole_intersection() {
        let plans =
            ir::AtLeast::<_, 2>::from_pair(TestSource::Leaf("union"), TestSource::Leaf("range"));

        let rewritten = apply_intersection_restriction(
            &plans,
            |_| RangeRestrictionMatch::Found(RangeRestrictedUnion::new(0, 1, TestSource::Empty)),
            |source| matches!(source, TestSource::Empty),
            |sources| sources,
            || vec![TestSource::Empty],
        );

        assert_eq!(
            rewritten,
            IntersectionRestriction::Rewritten(vec![TestSource::Empty])
        );
    }

    #[test]
    fn missing_restriction_leaves_intersection_unchanged() {
        let plans =
            ir::AtLeast::<_, 2>::from_pair(TestSource::Leaf("union"), TestSource::Leaf("range"));

        let rewritten = apply_intersection_restriction(
            &plans,
            |_| RangeRestrictionMatch::NotFound,
            |source| matches!(source, TestSource::Empty),
            |sources| sources,
            || vec![TestSource::Empty],
        );

        assert_eq!(rewritten, IntersectionRestriction::Unchanged);
    }
}
