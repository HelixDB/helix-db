use super::*;
use crate::{optimizer, rules};

struct TestRule {
    metadata: rules::RuleMetadata,
}

impl TestRule {
    fn new(id: &'static str) -> Self {
        Self {
            metadata: rules::RuleMetadata::new(
                rules::RuleId::new(id).unwrap(),
                rules::RuleKind::Exploration,
            ),
        }
    }
}

impl optimizer::OptimizerRule for TestRule {
    fn metadata(&self) -> &rules::RuleMetadata {
        &self.metadata
    }

    fn apply(&self, _input: optimizer::RuleInput<'_>) -> optimizer::RuleResult {
        optimizer::RuleResult::NotApplicable
    }
}

fn ids<'a>(rules: impl Iterator<Item = &'a dyn optimizer::OptimizerRule>) -> Vec<&'a str> {
    rules.map(|rule| rule.metadata().id.as_ref()).collect()
}

fn index(position: usize, registry_len: usize) -> RuleIndex {
    RuleIndex::from_registry_position(position, registry_len).unwrap()
}

fn slice(indices: &[RuleIndex]) -> CandidateSlice<'_> {
    CandidateSlice::from_sorted_test_indices(indices)
}

#[test]
fn rule_index_rejects_positions_outside_registry() {
    assert_eq!(
        RuleIndex::from_registry_position(0, 2),
        RuleIndex::from_test_registry_position(0, 2)
    );
    assert_eq!(
        RuleIndex::from_registry_position(1, 2),
        RuleIndex::from_test_registry_position(1, 2)
    );
    assert_eq!(RuleIndex::from_registry_position(2, 2), None);
    assert_eq!(RuleIndex::from_registry_position(0, 0), None);
}

#[test]
#[should_panic(expected = "candidate rule indices must be appended in strict registry order")]
fn candidate_list_rejects_non_increasing_appends() {
    let mut list = CandidateList::default();
    list.push(index(2, 3));
    list.push(index(1, 3));
}

#[test]
fn candidates_merge_broad_and_narrow_indices_in_registry_order() {
    let first = TestRule::new("first");
    let second = TestRule::new("second");
    let third = TestRule::new("third");
    let rules: Vec<&dyn optimizer::OptimizerRule> = vec![&first, &second, &third];
    let idx = |position| index(position, rules.len());
    let broad = [idx(0), idx(2)];
    let narrow = [idx(1)];

    assert_eq!(
        ids(RuleCandidates::new(
            &rules,
            slice(&broad),
            slice(&narrow),
            FeatureCandidates::empty()
        )),
        ["first", "second", "third"]
    );
}

#[test]
fn candidates_skip_duplicate_indices_when_lists_overlap() {
    let first = TestRule::new("first");
    let second = TestRule::new("second");
    let rules: Vec<&dyn optimizer::OptimizerRule> = vec![&first, &second];
    let idx = |position| index(position, rules.len());
    let candidates = [idx(0), idx(1)];

    assert_eq!(
        ids(RuleCandidates::new(
            &rules,
            slice(&candidates),
            slice(&candidates),
            FeatureCandidates::empty()
        )),
        ["first", "second"]
    );
}

#[test]
fn candidates_merge_extra_indices_in_registry_order() {
    let first = TestRule::new("first");
    let second = TestRule::new("second");
    let third = TestRule::new("third");
    let fourth = TestRule::new("fourth");
    let rules: Vec<&dyn optimizer::OptimizerRule> = vec![&first, &second, &third, &fourth];
    let idx = |position| index(position, rules.len());
    let broad = [idx(0), idx(3)];
    let narrow = [idx(2)];
    let extra = [idx(1), idx(2)];

    assert_eq!(
        ids(RuleCandidates::new(
            &rules,
            slice(&broad),
            slice(&narrow),
            FeatureCandidates::one(slice(&extra)),
        )),
        ["first", "second", "third", "fourth"]
    );
}

#[test]
fn candidates_merge_two_extra_slices_in_registry_order() {
    let first = TestRule::new("first");
    let second = TestRule::new("second");
    let third = TestRule::new("third");
    let fourth = TestRule::new("fourth");
    let rules: Vec<&dyn optimizer::OptimizerRule> = vec![&first, &second, &third, &fourth];
    let idx = |position| index(position, rules.len());
    let broad = [idx(2)];
    let first_extra = [idx(1), idx(3)];
    let second_extra = [idx(0), idx(3)];

    assert_eq!(
        ids(RuleCandidates::new(
            &rules,
            slice(&broad),
            CandidateSlice::empty(),
            FeatureCandidates::two(slice(&first_extra), slice(&second_extra)),
        )),
        ["first", "second", "third", "fourth"]
    );
}

#[test]
fn candidates_merge_many_extra_slices_in_registry_order() {
    let first = TestRule::new("first");
    let second = TestRule::new("second");
    let third = TestRule::new("third");
    let fourth = TestRule::new("fourth");
    let fifth = TestRule::new("fifth");
    let sixth = TestRule::new("sixth");
    let rules: Vec<&dyn optimizer::OptimizerRule> =
        vec![&first, &second, &third, &fourth, &fifth, &sixth];
    let idx = |position| index(position, rules.len());
    let broad = [idx(5)];
    let narrow = [idx(2)];
    let first_extra = [idx(4)];
    let second_extra = [idx(1)];
    let third_extra = [idx(3)];
    let fourth_extra = [idx(0)];
    let sixth_extra = [idx(2)];

    assert_eq!(
        ids(RuleCandidates::new(
            &rules,
            slice(&broad),
            slice(&narrow),
            FeatureCandidates::six(
                slice(&first_extra),
                slice(&second_extra),
                slice(&third_extra),
                slice(&fourth_extra),
                CandidateSlice::empty(),
                slice(&sixth_extra),
            ),
        )),
        ["first", "second", "third", "fourth", "fifth", "sixth"]
    );
}
