use super::pending::SelectedRunRootUse;
use super::selected::SelectedRunRoot;

/// Optimized-root consumption failures.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::planning::selected) enum OptimizedSelectedRunRootsError {
    /// Optimizer output count did not match the pending-root request count.
    BatchLengthMismatch { roots: usize, pending: usize },
    /// A pending-root use refers to a root outside this optimized batch.
    PendingRootMissing { index: usize, available: usize },
}

struct OptimizedSelectedRunRootSlot {
    root: SelectedRunRoot,
    used: bool,
}

/// Optimized selected roots aligned with one pending-root set.
pub(in crate::planning::selected) struct OptimizedSelectedRunRoots {
    slots: Vec<OptimizedSelectedRunRootSlot>,
}

impl OptimizedSelectedRunRoots {
    pub(in crate::planning::selected) fn empty() -> Self {
        Self { slots: Vec::new() }
    }

    pub(in crate::planning::selected) fn new(
        roots: Vec<SelectedRunRoot>,
        pending_len: usize,
    ) -> Result<Self, OptimizedSelectedRunRootsError> {
        if roots.len() != pending_len {
            return Err(OptimizedSelectedRunRootsError::BatchLengthMismatch {
                roots: roots.len(),
                pending: pending_len,
            });
        }
        let slots = roots
            .into_iter()
            .map(|root| OptimizedSelectedRunRootSlot { root, used: false })
            .collect();
        Ok(Self { slots })
    }

    pub(in crate::planning::selected) fn select(
        &mut self,
        root_use: SelectedRunRootUse,
    ) -> Result<SelectedRunRoot, OptimizedSelectedRunRootsError> {
        match root_use {
            SelectedRunRootUse::Ready(selected) => Ok(selected),
            SelectedRunRootUse::Pending(index) => {
                let index = index.get();
                let available = self.slots.len();
                let slot = self.slots.get_mut(index).ok_or(
                    OptimizedSelectedRunRootsError::PendingRootMissing { index, available },
                )?;
                if slot.used {
                    Ok(slot.root.cached_use())
                } else {
                    slot.used = true;
                    Ok(slot.root.clone())
                }
            }
        }
    }
}
