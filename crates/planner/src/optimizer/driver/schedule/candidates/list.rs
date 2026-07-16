//! Mutable and immutable registry-ordered candidate lists.

use super::index::RuleIndex;

/// Mutable registry-ordered candidate list.
#[derive(Debug, Default)]
pub(in crate::optimizer::driver::schedule) struct CandidateList {
    indices: Vec<RuleIndex>,
}

impl CandidateList {
    pub(in crate::optimizer::driver::schedule) fn push(&mut self, rule_index: RuleIndex) {
        if let Some(last) = self.indices.last() {
            assert!(
                *last < rule_index,
                "candidate rule indices must be appended in strict registry order"
            );
        }
        self.indices.push(rule_index);
    }

    pub(in crate::optimizer::driver::schedule) fn as_slice(&self) -> CandidateSlice<'_> {
        CandidateSlice {
            indices: self.indices.as_slice(),
        }
    }
}

/// Registry-ordered candidate index slice.
#[derive(Clone, Copy)]
pub(in crate::optimizer::driver::schedule) struct CandidateSlice<'schedule> {
    indices: &'schedule [RuleIndex],
}

impl<'schedule> CandidateSlice<'schedule> {
    pub(in crate::optimizer::driver::schedule) const fn empty() -> Self {
        Self { indices: &[] }
    }

    pub(super) fn get(self, index: usize) -> Option<RuleIndex> {
        self.indices.get(index).copied()
    }

    #[cfg(test)]
    pub(super) fn from_sorted_test_indices(indices: &'schedule [RuleIndex]) -> Self {
        assert!(
            indices.windows(2).all(|window| window[0] < window[1]),
            "test candidate indices must be in strict registry order"
        );
        Self { indices }
    }
}

impl AsRef<[RuleIndex]> for CandidateSlice<'_> {
    fn as_ref(&self) -> &[RuleIndex] {
        self.indices
    }
}
