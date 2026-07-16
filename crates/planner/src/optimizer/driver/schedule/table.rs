//! Dense candidate tables for closed scheduler families.

use std::{fmt, marker::PhantomData};

use crate::logical;

use super::candidates::{CandidateList, CandidateSlice, RuleIndex};

pub(super) trait RuleScheduleKey: Copy + Eq + fmt::Debug + 'static {
    fn all() -> &'static [Self];
    fn index(self) -> usize;
    fn name() -> &'static str;
}

impl RuleScheduleKey for logical::LogicalExprKind {
    fn all() -> &'static [Self] {
        &Self::ALL
    }

    fn index(self) -> usize {
        self as usize
    }

    fn name() -> &'static str {
        "LogicalExprKind"
    }
}

impl RuleScheduleKey for logical::PureLogicalOpKind {
    fn all() -> &'static [Self] {
        &Self::ALL
    }

    fn index(self) -> usize {
        self as usize
    }

    fn name() -> &'static str {
        "PureLogicalOpKind"
    }
}

impl RuleScheduleKey for logical::StreamPipelineOpKind {
    fn all() -> &'static [Self] {
        &Self::ALL
    }

    fn index(self) -> usize {
        self as usize
    }

    fn name() -> &'static str {
        "StreamPipelineOpKind"
    }
}

impl RuleScheduleKey for logical::AccessSourceKind {
    fn all() -> &'static [Self] {
        &Self::ALL
    }

    fn index(self) -> usize {
        self as usize
    }

    fn name() -> &'static str {
        "AccessSourceKind"
    }
}

/// Candidate indices keyed by a closed enum family.
pub(super) struct CandidateTable<K: RuleScheduleKey> {
    buckets: Vec<CandidateList>,
    _kind: PhantomData<fn(K)>,
}

impl<K: RuleScheduleKey> CandidateTable<K> {
    pub(super) fn empty() -> Self {
        Self::assert_dense_key_inventory();
        let mut buckets = Vec::with_capacity(K::all().len());
        buckets.resize_with(K::all().len(), CandidateList::default);
        Self {
            buckets,
            _kind: PhantomData,
        }
    }

    pub(super) fn push(&mut self, kind: K, rule_index: RuleIndex) {
        let index = self.bucket_index(kind);
        self.buckets[index].push(rule_index);
    }

    pub(super) fn get(&self, kind: K) -> CandidateSlice<'_> {
        let index = self.bucket_index(kind);
        self.buckets[index].as_slice()
    }

    fn bucket_index(&self, kind: K) -> usize {
        let index = kind.index();
        debug_assert_eq!(
            K::all().get(index),
            Some(&kind),
            "{} inventory must stay dense and ordered",
            K::name()
        );
        index
    }

    fn assert_dense_key_inventory() {
        for (expected, kind) in K::all().iter().copied().enumerate() {
            assert_eq!(
                kind.index(),
                expected,
                "{} inventory must match enum discriminants",
                K::name()
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_dense<K: RuleScheduleKey>() {
        let table = CandidateTable::<K>::empty();
        for (index, kind) in K::all().iter().copied().enumerate() {
            assert_eq!(table.get(kind).as_ref(), &[] as &[RuleIndex]);
            assert_eq!(kind.index(), index);
        }
    }

    #[test]
    fn candidate_tables_cover_dense_scheduler_families() {
        assert_dense::<logical::LogicalExprKind>();
        assert_dense::<logical::PureLogicalOpKind>();
        assert_dense::<logical::StreamPipelineOpKind>();
        assert_dense::<logical::AccessSourceKind>();
    }

    #[test]
    fn candidate_table_preserves_registry_order_per_family() {
        let mut table = CandidateTable::<logical::LogicalExprKind>::empty();
        let registry_len = 6;

        let second = RuleIndex::from_test_registry_position(2, registry_len).unwrap();
        let third = RuleIndex::from_test_registry_position(3, registry_len).unwrap();
        let fifth = RuleIndex::from_test_registry_position(5, registry_len).unwrap();

        table.push(logical::LogicalExprKind::Pure, second);
        table.push(logical::LogicalExprKind::Pure, fifth);
        table.push(logical::LogicalExprKind::AccessPath, third);

        assert_eq!(
            table.get(logical::LogicalExprKind::Pure).as_ref(),
            &[second, fifth]
        );
        assert_eq!(
            table.get(logical::LogicalExprKind::AccessPath).as_ref(),
            &[third]
        );
        assert_eq!(
            table.get(logical::LogicalExprKind::RootPipeline).as_ref(),
            &[] as &[RuleIndex]
        );
    }
}
