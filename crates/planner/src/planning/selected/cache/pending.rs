use std::collections::BTreeMap;

use super::super::root::SelectableRunRoot;
use super::selected::SelectedRunRoot;
use crate::{digest, ir, logical};

pub(in crate::planning::selected) struct PendingSelectedRunRoot {
    pub(in crate::planning::selected) logical_root: SelectableRunRoot,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::planning::selected) struct PendingSelectedRunRootIndex {
    index: usize,
}

impl PendingSelectedRunRootIndex {
    fn new(index: usize) -> Self {
        Self { index }
    }

    pub(super) fn get(self) -> usize {
        self.index
    }
}

/// Selected root usage planned by a parent/root draft.
pub(in crate::planning::selected) enum SelectedRunRootUse {
    Ready(SelectedRunRoot),
    Pending(PendingSelectedRunRootIndex),
}

/// Ordered pending selected roots with digest-bucketed duplicate detection.
///
/// Pending roots must retain first-seen order because `optimize_many` returns
/// results in the same order. Duplicate detection uses the same digest/equality
/// contract as the selected-root cache, avoiding a linear scan over every
/// pending root as batches grow.
#[derive(Default)]
pub(in crate::planning::selected) struct PendingSelectedRunRoots {
    entries: Vec<PendingSelectedRunRoot>,
    by_digest: BTreeMap<digest::PlanDigest, Vec<usize>>,
}

impl PendingSelectedRunRoots {
    pub(in crate::planning::selected) fn push_or_reuse(
        &mut self,
        logical_root: SelectableRunRoot,
    ) -> SelectedRunRootUse {
        let digest = logical_root.digest();
        if let Some(index) = self.by_digest.get(&digest).and_then(|indexes| {
            indexes.iter().copied().find(|index| {
                self.entries
                    .get(*index)
                    .is_some_and(|entry| entry.logical_root == logical_root)
            })
        }) {
            return SelectedRunRootUse::Pending(PendingSelectedRunRootIndex::new(index));
        }

        let index = self.entries.len();
        self.entries.push(PendingSelectedRunRoot { logical_root });
        self.by_digest.entry(digest).or_default().push(index);
        SelectedRunRootUse::Pending(PendingSelectedRunRootIndex::new(index))
    }

    pub(in crate::planning::selected) fn into_optimizer_batch(
        self,
    ) -> Option<PendingSelectedRunRootBatch> {
        let root_exprs = ir::AtLeast::<_, 1>::try_from_vec(
            self.entries
                .iter()
                .map(|entry| entry.logical_root.expr().clone())
                .collect::<Vec<_>>(),
        )?;
        Some(PendingSelectedRunRootBatch {
            entries: self.entries,
            root_exprs,
        })
    }
}

/// Non-empty pending roots prepared for one shared optimizer request.
pub(in crate::planning::selected) struct PendingSelectedRunRootBatch {
    entries: Vec<PendingSelectedRunRoot>,
    root_exprs: ir::AtLeast<logical::LogicalExpr, 1>,
}

impl PendingSelectedRunRootBatch {
    pub(in crate::planning::selected) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(in crate::planning::selected) fn into_parts(
        self,
    ) -> (
        ir::AtLeast<logical::LogicalExpr, 1>,
        Vec<PendingSelectedRunRoot>,
    ) {
        (self.root_exprs, self.entries)
    }
}
