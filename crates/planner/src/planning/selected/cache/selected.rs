use std::collections::BTreeMap;

use super::super::root::SelectableRunRoot;
use crate::{digest, exec};

/// A selected executable root plus the planner metrics produced to select it.
#[derive(Clone)]
pub(in crate::planning::selected) struct SelectedRunRoot {
    pub(in crate::planning::selected) root: exec::SelectedExecutableRunRoot,
    pub(in crate::planning::selected) metrics: exec::PlannerMetrics,
}

impl SelectedRunRoot {
    pub(super) fn cached_use(&self) -> Self {
        Self {
            root: self.root.clone(),
            metrics: exec::PlannerMetrics {
                selected_cost: self.metrics.selected_cost,
                ..exec::PlannerMetrics::default()
            },
        }
    }
}

struct CachedSelectedRunRoot {
    logical_root: SelectableRunRoot,
    selected: SelectedRunRoot,
}

/// Request-local selected-root cache with collision-safe digest buckets.
///
/// Cached roots are bucketed by stable digest for lookup speed, but hits are
/// accepted only after full `SelectableRunRoot` equality. Cache hits return
/// `SelectedRunRoot::cached_use` so repeated uses keep the selected cost while
/// reporting optimizer work only once.
#[derive(Default)]
pub(in crate::planning::selected) struct SelectedRunRootCache {
    by_digest: BTreeMap<digest::PlanDigest, Vec<CachedSelectedRunRoot>>,
}

impl SelectedRunRootCache {
    pub(in crate::planning::selected) fn get(
        &self,
        logical_root: &SelectableRunRoot,
    ) -> Option<SelectedRunRoot> {
        self.by_digest
            .get(&logical_root.digest())?
            .iter()
            .find(|entry| &entry.logical_root == logical_root)
            .map(|entry| entry.selected.cached_use())
    }

    pub(in crate::planning::selected) fn insert(
        &mut self,
        logical_root: SelectableRunRoot,
        selected: SelectedRunRoot,
    ) {
        self.by_digest
            .entry(logical_root.digest())
            .or_default()
            .push(CachedSelectedRunRoot {
                logical_root,
                selected,
            });
    }
}
