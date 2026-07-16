//! Registry-order candidate merge iterator.

use super::super::super::super::rule;
use super::features::{FeatureCandidates, FEATURE_CANDIDATE_SLOTS};
use super::index::RuleIndex;
use super::list::CandidateSlice;

pub(in crate::optimizer::driver) struct RuleCandidates<'schedule, 'rule> {
    rules: &'schedule [&'rule dyn rule::OptimizerRule],
    broad: CandidateSlice<'schedule>,
    narrow: CandidateSlice<'schedule>,
    features: FeatureCandidates<'schedule>,
    broad_index: usize,
    narrow_index: usize,
    feature_indices: [usize; FEATURE_CANDIDATE_SLOTS],
    last_index: Option<RuleIndex>,
}

impl<'schedule, 'rule> RuleCandidates<'schedule, 'rule> {
    pub(in crate::optimizer::driver::schedule) fn new(
        rules: &'schedule [&'rule dyn rule::OptimizerRule],
        broad: CandidateSlice<'schedule>,
        narrow: CandidateSlice<'schedule>,
        features: FeatureCandidates<'schedule>,
    ) -> Self {
        Self {
            rules,
            broad,
            narrow,
            features,
            broad_index: 0,
            narrow_index: 0,
            feature_indices: [0; FEATURE_CANDIDATE_SLOTS],
            last_index: None,
        }
    }
}

impl<'rule> Iterator for RuleCandidates<'_, 'rule> {
    type Item = &'rule dyn rule::OptimizerRule;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let broad = self.broad.get(self.broad_index);
            let narrow = self.narrow.get(self.narrow_index);
            let next = [broad, narrow]
                .into_iter()
                .flatten()
                .chain(self.features.current(&self.feature_indices))
                .min()?;

            if broad == Some(next) {
                self.broad_index += 1;
            }
            if narrow == Some(next) {
                self.narrow_index += 1;
            }
            self.features
                .advance_if_current(&mut self.feature_indices, next);

            if self.last_index == Some(next) {
                continue;
            }
            self.last_index = Some(next);
            return Some(self.rules[next.position()]);
        }
    }
}
