//! Generic same-key intersection merge contract.

use std::collections::BTreeMap;

use crate::{digest, ir};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum RangeIntersectionMerge<S> {
    Merged(Vec<S>),
    Unchanged(Vec<S>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum RangeMergeKey {
    Key(digest::PlanDigest),
    NotRange,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum RangeSourceMerge<S> {
    Merged(S),
    NotMergeable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BucketMergeStatus {
    Changed,
    Unchanged,
}

pub(super) fn merge_intersection_sources<S, KeyDigest, Merge>(
    plans: &ir::AtLeast<S, 2>,
    key_digest: KeyDigest,
    merge_sources: Merge,
) -> RangeIntersectionMerge<S>
where
    S: Clone,
    KeyDigest: Fn(&S) -> RangeMergeKey,
    Merge: Fn(&S, &S) -> RangeSourceMerge<S>,
{
    let mut slots = plans.iter().cloned().map(Some).collect::<Vec<_>>();
    let mut buckets: BTreeMap<digest::PlanDigest, Vec<usize>> = BTreeMap::new();
    slots.iter().enumerate().for_each(|(index, source)| {
        let Some(source) = source.as_ref() else {
            return;
        };
        if let RangeMergeKey::Key(digest) = key_digest(source) {
            buckets.entry(digest).or_default().push(index);
        }
    });
    let mut changed = false;
    for indexes in buckets.into_values() {
        changed |= matches!(
            merge_bucket(&mut slots, indexes, &merge_sources),
            BucketMergeStatus::Changed
        );
    }
    let sources = slots.into_iter().flatten().collect();
    if changed {
        RangeIntersectionMerge::Merged(sources)
    } else {
        RangeIntersectionMerge::Unchanged(sources)
    }
}

fn merge_bucket<S, Merge>(
    slots: &mut [Option<S>],
    indexes: Vec<usize>,
    merge_sources: &Merge,
) -> BucketMergeStatus
where
    S: Clone,
    Merge: Fn(&S, &S) -> RangeSourceMerge<S>,
{
    let mut groups: Vec<(usize, S)> = Vec::new();
    let mut changed = false;
    for index in indexes {
        let Some(source) = slots[index].take() else {
            continue;
        };
        let Some((group_index, merged)) =
            groups
                .iter()
                .enumerate()
                .find_map(
                    |(group_index, (_, existing))| match merge_sources(existing, &source) {
                        RangeSourceMerge::Merged(merged) => Some((group_index, merged)),
                        RangeSourceMerge::NotMergeable => None,
                    },
                )
        else {
            groups.push((index, source));
            continue;
        };
        groups[group_index].1 = merged;
        changed = true;
    }
    groups
        .into_iter()
        .for_each(|(index, source)| slots[index] = Some(source));
    if changed {
        BucketMergeStatus::Changed
    } else {
        BucketMergeStatus::Unchanged
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct TestSource {
        key: u8,
        low: u8,
        high: u8,
    }

    impl TestSource {
        fn new(key: u8, low: u8, high: u8) -> Self {
            Self { key, low, high }
        }
    }

    fn digest(source: &TestSource) -> RangeMergeKey {
        if source.key == 0 {
            RangeMergeKey::NotRange
        } else {
            RangeMergeKey::Key(digest::PlanDigest::for_tagged_value(
                "test_range_key:v1",
                &source.key,
            ))
        }
    }

    fn merge(left: &TestSource, right: &TestSource) -> RangeSourceMerge<TestSource> {
        if left.key == right.key {
            RangeSourceMerge::Merged(TestSource {
                key: left.key,
                low: left.low.max(right.low),
                high: left.high.min(right.high),
            })
        } else {
            RangeSourceMerge::NotMergeable
        }
    }

    #[test]
    fn merge_intersection_sources_preserves_first_group_slot() {
        let input = ir::AtLeast::<_, 2>::try_from_vec(vec![
            TestSource::new(1, 0, 100),
            TestSource::new(2, 0, 20),
            TestSource::new(1, 10, 90),
        ])
        .unwrap();

        assert_eq!(
            merge_intersection_sources(&input, digest, merge),
            RangeIntersectionMerge::Merged(vec![
                TestSource::new(1, 10, 90),
                TestSource::new(2, 0, 20),
            ])
        );
    }

    #[test]
    fn merge_intersection_sources_reports_unchanged_distinct_keys() {
        let input =
            ir::AtLeast::<_, 2>::from_pair(TestSource::new(1, 0, 100), TestSource::new(2, 0, 20));

        assert_eq!(
            merge_intersection_sources(&input, digest, merge),
            RangeIntersectionMerge::Unchanged(vec![
                TestSource::new(1, 0, 100),
                TestSource::new(2, 0, 20),
            ])
        );
    }

    #[test]
    fn non_range_sources_do_not_enter_merge_buckets() {
        let input =
            ir::AtLeast::<_, 2>::from_pair(TestSource::new(0, 0, 100), TestSource::new(0, 10, 90));

        assert_eq!(
            merge_intersection_sources(&input, digest, merge),
            RangeIntersectionMerge::Unchanged(vec![
                TestSource::new(0, 0, 100),
                TestSource::new(0, 10, 90),
            ])
        );
    }
}
