//! Process-local exclusion between text blob publication and object deletion.
//!
//! [`BlobGcGate`] is independent of any index repository format. A publisher
//! retains a shared permit from object upload through the transaction that
//! makes the blob reachable; final GC recheck and deletion retain an exclusive
//! permit. Writer fencing ensures a crashed process releases its permits before
//! another writer can mutate the same SlateDB database.

use std::sync::Arc;

use tokio::sync::{OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock};

/// Shared/exclusive coordinator owned by one writer runtime.
#[derive(Debug, Clone, Default)]
pub(crate) struct BlobGcGate {
    inner: Arc<RwLock<()>>,
}

impl BlobGcGate {
    /// Creates an unlocked publication/deletion coordinator.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Acquires shared ownership spanning upload through reference commit.
    pub(crate) async fn acquire_publication(&self) -> BlobPublicationPermit {
        BlobPublicationPermit {
            _guard: Arc::clone(&self.inner).read_owned().await,
        }
    }

    /// Acquires exclusive ownership for final recheck and object deletion.
    pub(crate) async fn acquire_deletion(&self) -> BlobDeletionPermit {
        BlobDeletionPermit {
            _guard: Arc::clone(&self.inner).write_owned().await,
        }
    }
}

/// Shared permit retained until uploaded blob references commit or abort.
#[derive(Debug)]
pub(crate) struct BlobPublicationPermit {
    _guard: OwnedRwLockReadGuard<()>,
}

/// Exclusive permit retained through final recheck and object deletion.
#[derive(Debug)]
pub(crate) struct BlobDeletionPermit {
    _guard: OwnedRwLockWriteGuard<()>,
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn deletion_waits_for_every_publication_permit() {
        let gate = BlobGcGate::new();
        let first = gate.acquire_publication().await;
        let second = gate.acquire_publication().await;
        let waiting = gate.clone();
        let mut deletion = tokio::spawn(async move { waiting.acquire_deletion().await });

        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut deletion)
                .await
                .is_err()
        );
        drop(first);
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut deletion)
                .await
                .is_err()
        );
        drop(second);
        deletion.await.unwrap();
    }
}
