//! Fixed-capacity feature candidate groups.

use super::index::RuleIndex;
use super::list::CandidateSlice;

pub(super) const FEATURE_CANDIDATE_SLOTS: usize = 6;

/// Ordered feature-specific candidate slices for one expression.
///
/// The scheduler has a bounded number of feature predicates per expression
/// family. This wrapper keeps that fixed-capacity representation private to
/// the scheduler iterator instead of leaking positional arrays through every
/// call site.
#[derive(Clone, Copy)]
pub(in crate::optimizer::driver::schedule) struct FeatureCandidates<'schedule> {
    slices: [CandidateSlice<'schedule>; FEATURE_CANDIDATE_SLOTS],
}

impl<'schedule> FeatureCandidates<'schedule> {
    pub(in crate::optimizer::driver::schedule) const fn empty() -> Self {
        Self {
            slices: [CandidateSlice::empty(); FEATURE_CANDIDATE_SLOTS],
        }
    }

    pub(in crate::optimizer::driver::schedule) const fn one(
        first: CandidateSlice<'schedule>,
    ) -> Self {
        Self::from_slots([
            first,
            CandidateSlice::empty(),
            CandidateSlice::empty(),
            CandidateSlice::empty(),
            CandidateSlice::empty(),
            CandidateSlice::empty(),
        ])
    }

    pub(in crate::optimizer::driver::schedule) const fn two(
        first: CandidateSlice<'schedule>,
        second: CandidateSlice<'schedule>,
    ) -> Self {
        Self::from_slots([
            first,
            second,
            CandidateSlice::empty(),
            CandidateSlice::empty(),
            CandidateSlice::empty(),
            CandidateSlice::empty(),
        ])
    }

    pub(in crate::optimizer::driver::schedule) const fn six(
        first: CandidateSlice<'schedule>,
        second: CandidateSlice<'schedule>,
        third: CandidateSlice<'schedule>,
        fourth: CandidateSlice<'schedule>,
        fifth: CandidateSlice<'schedule>,
        sixth: CandidateSlice<'schedule>,
    ) -> Self {
        Self::from_slots([first, second, third, fourth, fifth, sixth])
    }

    const fn from_slots(slices: [CandidateSlice<'schedule>; FEATURE_CANDIDATE_SLOTS]) -> Self {
        Self { slices }
    }

    pub(super) fn current<'a>(
        &'a self,
        indices: &'a [usize; FEATURE_CANDIDATE_SLOTS],
    ) -> impl Iterator<Item = RuleIndex> + 'a {
        self.slices
            .iter()
            .enumerate()
            .filter_map(|(index, candidates)| candidates.get(indices[index]))
    }

    pub(super) fn advance_if_current(
        &self,
        indices: &mut [usize; FEATURE_CANDIDATE_SLOTS],
        candidate: RuleIndex,
    ) {
        self.slices
            .iter()
            .enumerate()
            .for_each(|(index, candidates)| {
                if candidates.get(indices[index]) == Some(candidate) {
                    indices[index] += 1;
                }
            });
    }
}
