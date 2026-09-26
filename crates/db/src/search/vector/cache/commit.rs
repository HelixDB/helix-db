//! Transaction-owned vector cache mutation tracking.
//!
//! [`VectorCacheWriteSet`] groups the one post-commit cache effect for each
//! complete validated generation identity. Ordinary writes track dirty rows;
//! physical partition reclamation replaces that effect with exact retirement.
//! The set itself performs no shared-cache mutation. [`commit_fenced`] commits
//! storage and resolves the dirty-row fences from the storage outcome, while
//! abort drops the set.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;

use super::registry::{VectorCacheCommitOutcome, VectorCacheIdentity, VectorCachePendingCommit};
use super::store::VectorMemoryDirtyRows;
use crate::search::vector::{SimHasherRegistry, ValidatedVectorGenerationHandle};

/// Commits one storage transaction and resolves its vector cache fences.
///
/// SlateDB applies a batch it has accepted even when the caller drops the
/// commit future, so a commit carrying fences runs on a detached task that
/// keeps them pending until the storage outcome is known. A definite pre-apply
/// rejection (a transaction conflict or an empty batch) releases them
/// unchanged; every other outcome, including an error returned after the batch
/// may have been applied, evicts their dirty rows. Commits without fences run
/// inline.
pub(crate) async fn commit_fenced(
    transaction: slatedb::DbTransaction,
    fences: Vec<VectorCachePendingCommit>,
) -> Result<Option<slatedb::WriteHandle>, slatedb::Error> {
    if fences.is_empty() {
        return transaction.commit().await;
    }
    let commit = tokio::spawn(async move {
        let committed = transaction.commit().await;
        let outcome = match &committed {
            Ok(Some(_)) => VectorCacheCommitOutcome::MaybeApplied,
            Ok(None) => VectorCacheCommitOutcome::Rejected,
            Err(error) if error.kind() == slatedb::ErrorKind::Transaction => {
                VectorCacheCommitOutcome::Rejected
            }
            Err(_) => VectorCacheCommitOutcome::MaybeApplied,
        };
        for fence in fences {
            fence.resolve(outcome).await;
        }
        committed
    });
    match commit.await {
        Ok(committed) => committed,
        Err(error) => match error.try_into_panic() {
            Ok(panic) => std::panic::resume_unwind(panic),
            Err(error) => Err(slatedb::Error::internal(format!(
                "vector cache commit task stopped before resolving its fences: {error}"
            ))),
        },
    }
}

/// One exact generation and its transaction-owned post-commit cache effect.
#[derive(Debug, Clone)]
pub(crate) struct VectorCacheWriteEntry {
    handle: ValidatedVectorGenerationHandle,
    effect: VectorCacheCommitEffect,
}

/// Only one post-commit cache effect can own an exact physical generation.
#[derive(Debug, Clone)]
enum VectorCacheCommitEffect {
    /// Evict rows whose durable storage changed.
    EvictDirty(Arc<VectorMemoryDirtyRows>),
    /// Close and forget an empty physical partition after its rows disappear.
    Retire,
}

impl VectorCacheWriteEntry {
    /// Returns the descriptor proof used to locate the registry entry.
    pub(crate) const fn handle(&self) -> &ValidatedVectorGenerationHandle {
        &self.handle
    }

    /// Returns the transaction-local rows to fence and evict at commit.
    pub(crate) const fn dirty_rows(&self) -> Option<&Arc<VectorMemoryDirtyRows>> {
        match &self.effect {
            VectorCacheCommitEffect::EvictDirty(dirty_rows) => Some(dirty_rows),
            VectorCacheCommitEffect::Retire => None,
        }
    }

    /// Returns the exact handle only for a post-commit physical retirement.
    pub(crate) const fn retirement(&self) -> Option<&ValidatedVectorGenerationHandle> {
        match self.effect {
            VectorCacheCommitEffect::EvictDirty(_) => None,
            VectorCacheCommitEffect::Retire => Some(&self.handle),
        }
    }
}

/// Complete vector cache write ownership for one database transaction.
#[derive(Debug)]
pub(crate) struct VectorCacheWriteSet {
    entries: Mutex<HashMap<VectorCacheIdentity, VectorCacheWriteEntry>>,
    simhasher_registry: Arc<SimHasherRegistry>,
}

impl VectorCacheWriteSet {
    /// Creates transaction tracking bound to its database's projection owner.
    pub(crate) fn new(simhasher_registry: Arc<SimHasherRegistry>) -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            simhasher_registry,
        }
    }

    /// Clones the projection owner for exact vector-index construction.
    pub(crate) fn simhasher_registry(&self) -> Arc<SimHasherRegistry> {
        Arc::clone(&self.simhasher_registry)
    }

    /// Returns the single dirty tracker for an exact validated generation.
    ///
    /// Repeated mutations in one transaction share the tracker. Full identity
    /// equality is checked by the map key, so logical-name reuse across
    /// generations cannot merge write sets.
    pub(crate) fn dirty_rows_for(
        &self,
        handle: &ValidatedVectorGenerationHandle,
    ) -> Arc<VectorMemoryDirtyRows> {
        let identity = VectorCacheIdentity::from_validated(handle);
        let mut entries = self.entries.lock();
        let entry = entries
            .entry(identity)
            .or_insert_with(|| VectorCacheWriteEntry {
                handle: handle.clone(),
                effect: VectorCacheCommitEffect::EvictDirty(Arc::new(
                    VectorMemoryDirtyRows::default(),
                )),
            });
        let VectorCacheCommitEffect::EvictDirty(dirty_rows) = &entry.effect else {
            panic!("a retired vector cache generation cannot receive later physical writes");
        };
        Arc::clone(dirty_rows)
    }

    /// Replaces dirty-row eviction with exact post-commit physical retirement.
    ///
    /// The shared registry remains untouched until storage commits. Dropping
    /// the transaction therefore discards this effect without closing a cache
    /// entry that still has durable physical ownership.
    pub(crate) fn retire_after_commit(&self, handle: &ValidatedVectorGenerationHandle) {
        self.entries.lock().insert(
            VectorCacheIdentity::from_validated(handle),
            VectorCacheWriteEntry {
                handle: handle.clone(),
                effect: VectorCacheCommitEffect::Retire,
            },
        );
    }

    /// Takes a stable snapshot for pre-commit pending-guard acquisition.
    pub(crate) fn entries(&self) -> Vec<VectorCacheWriteEntry> {
        self.entries.lock().values().cloned().collect()
    }
}

impl Default for VectorCacheWriteSet {
    fn default() -> Self {
        Self::new(Arc::new(SimHasherRegistry::default()))
    }
}

#[cfg(feature = "production-coverage")]
pub(crate) mod production_contracts {
    use std::num::NonZeroU64;
    use std::panic::AssertUnwindSafe;

    use super::*;
    use crate::encoding::keys::scope::DataScope;
    use crate::search::vector::distance::Cosine;
    use crate::search::vector::{VectorDimension, VectorGenerationIdentity};

    /// Proves retirement closes the transaction-local physical-write state.
    pub(crate) fn run() {
        let handle = ValidatedVectorGenerationHandle::create_current::<Cosine>(
            VectorGenerationIdentity::try_new(
                DataScope::LegacyUnscoped,
                8,
                "production-write-cache-retirement".to_string(),
                80,
                NonZeroU64::MIN,
                1,
                crate::index_lifecycle::IndexElementKind::Node,
                VectorDimension::try_new(3).unwrap(),
            )
            .unwrap(),
        )
        .unwrap();
        let writes = VectorCacheWriteSet::default();
        writes.retire_after_commit(&handle);
        let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
            drop(writes.dirty_rows_for(&handle));
        }));
        assert!(result.is_err(), "retired generations reject later writes");
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use bytes::Bytes;
    use futures::stream::BoxStream;
    use slatedb::object_store::memory::InMemory;
    use slatedb::object_store::{
        path::Path, CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
        ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
        Result as ObjectStoreResult,
    };
    use slatedb::{DbTransaction, IsolationLevel};
    use tokio::sync::watch;

    use super::super::registry::{
        VectorCacheReadGuardError, VectorCacheRegistry, VectorCacheStaleness, VectorCacheVisibility,
    };
    use super::super::store::VectorMemoryStore;
    use super::*;
    use crate::encoding::keys::scope::DataScope;
    use crate::search::vector::distance::Cosine;
    use crate::search::vector::{VectorDimension, VectorGenerationIdentity};

    /// Builds a distinct descriptor identity for write-set isolation tests.
    fn handle(generation: u64) -> ValidatedVectorGenerationHandle {
        ValidatedVectorGenerationHandle::create_current::<Cosine>(
            VectorGenerationIdentity::try_new(
                DataScope::LegacyUnscoped,
                8,
                format!("write-cache-generation-{generation}"),
                80,
                NonZeroU64::new(generation).unwrap(),
                1,
                crate::index_lifecycle::IndexElementKind::Node,
                VectorDimension::try_new(3).unwrap(),
            )
            .unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn write_set_shares_exact_identity_and_isolates_successors() {
        let writes = VectorCacheWriteSet::default();
        let first = handle(1);
        let successor = handle(2);
        let first_rows = writes.dirty_rows_for(&first);
        let same_rows = writes.dirty_rows_for(&first);
        let successor_rows = writes.dirty_rows_for(&successor);

        assert!(Arc::ptr_eq(&first_rows, &same_rows));
        assert!(!Arc::ptr_eq(&first_rows, &successor_rows));
        first_rows.mark_node_dirty(7);
        assert!(same_rows.is_node_dirty(7));
        assert!(!successor_rows.is_node_dirty(7));
        assert_eq!(writes.entries().len(), 2);
    }

    /// How the gated object store treats WAL uploads.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum WalUploads {
        Open,
        Held,
        Failing,
    }

    /// In-memory object store that can hold or fail WAL uploads, which lets a
    /// test observe a batch that reached the memtable but is not yet durable.
    #[derive(Debug)]
    struct GatedWalStore {
        inner: InMemory,
        uploads: watch::Sender<WalUploads>,
    }

    impl std::fmt::Display for GatedWalStore {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("gated-wal-memory")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for GatedWalStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            options: PutOptions,
        ) -> ObjectStoreResult<PutResult> {
            if location.as_ref().contains("/wal/") {
                let uploads = *self
                    .uploads
                    .subscribe()
                    .wait_for(|uploads| *uploads != WalUploads::Held)
                    .await
                    .expect("the gate outlives its store");
                if uploads == WalUploads::Failing {
                    return Err(slatedb::object_store::Error::NotImplemented {
                        operation: "put".to_string(),
                        implementer: self.to_string(),
                    });
                }
            }
            self.inner.put_opts(location, payload, options).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            options: PutMultipartOptions,
        ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, options).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> ObjectStoreResult<GetResult> {
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, ObjectStoreResult<Path>>,
        ) -> BoxStream<'static, ObjectStoreResult<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> ObjectStoreResult<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> ObjectStoreResult<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    /// One writer with a commit-fenced registry whose resident store caches nodes 7 and 8.
    struct FencedWriter {
        gate: Arc<GatedWalStore>,
        db: slatedb::Db,
        registry: VectorCacheRegistry,
        handle: ValidatedVectorGenerationHandle,
        resident: Arc<VectorMemoryStore>,
    }

    impl FencedWriter {
        async fn open(name: &str) -> Self {
            let gate = Arc::new(GatedWalStore {
                inner: InMemory::new(),
                uploads: watch::Sender::new(WalUploads::Open),
            });
            let db = slatedb::Db::builder(name, Arc::clone(&gate) as Arc<dyn ObjectStore>)
                .build()
                .await
                .unwrap();
            let registry = VectorCacheRegistry::new(VectorCacheVisibility::CommitFenced);
            let handle = handle(1);
            let resident = Arc::new(VectorMemoryStore::new(
                DataScope::LegacyUnscoped,
                handle.physical_index_id(),
                db.snapshot().await.unwrap().seq(),
            ));
            resident.insert_upper_vector(7, Bytes::from_static(b"stale"));
            resident.insert_upper_vector(8, Bytes::from_static(b"untouched"));
            let (entry, owns_hydration) = registry.entry_for(&handle);
            assert!(owns_hydration);
            assert!(entry.finish_hydration(Arc::clone(&resident)));
            Self {
                gate,
                db,
                registry,
                handle,
                resident,
            }
        }

        /// Stages one storage write whose vector write set dirties node 7.
        async fn fenced_transaction(&self) -> (DbTransaction, Vec<VectorCachePendingCommit>) {
            let writes = VectorCacheWriteSet::default();
            writes.dirty_rows_for(&self.handle).mark_node_dirty(7);
            let fences = writes
                .entries()
                .iter()
                .filter_map(|write| self.registry.prepare_commit(write))
                .collect::<Vec<_>>();
            let transaction = self.db.begin(IsolationLevel::Snapshot).await.unwrap();
            transaction
                .put(b"fenced-key", Bytes::from_static(b"committed"))
                .unwrap();
            (transaction, fences)
        }

        /// Returns whether the identity's fence still counts an unresolved commit.
        fn commit_pending(&self) -> bool {
            self.registry
                .resident_guard_for(&self.handle)
                .unwrap()
                .pending_dirty()
                .has_pending_commits()
        }
    }

    #[tokio::test]
    async fn fenced_commit_without_fences_commits_inline() {
        let writer = FencedWriter::open("fenced-commit-inline").await;
        let transaction = writer.db.begin(IsolationLevel::Snapshot).await.unwrap();
        transaction
            .put(b"inline-key", Bytes::from_static(b"value"))
            .unwrap();

        assert!(commit_fenced(transaction, Vec::new())
            .await
            .unwrap()
            .is_some());
        assert!(writer.resident.get_upper_vector(7).is_some());
        writer.db.close().await.unwrap();
    }

    #[tokio::test]
    async fn cancelled_fenced_commit_keeps_its_fence_until_the_applied_batch_resolves() {
        let writer = FencedWriter::open("fenced-commit-cancelled").await;
        let (transaction, fences) = writer.fenced_transaction().await;
        writer.gate.uploads.send_replace(WalUploads::Held);
        let mut commit = Box::pin(commit_fenced(transaction, fences));
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                tokio::select! {
                    biased;
                    result = &mut commit => panic!("a held WAL upload cannot finish: {result:?}"),
                    () = tokio::time::sleep(std::time::Duration::from_millis(5)) => {}
                }
                if writer.db.get(b"fenced-key").await.unwrap().is_some() {
                    break;
                }
            }
        })
        .await
        .expect("the batch reaches the memtable before its WAL upload");
        drop(commit);
        let applied_seq = writer.db.snapshot().await.unwrap().seq();

        assert!(matches!(
            writer.registry.read_guard_for(&writer.handle, applied_seq),
            Err(VectorCacheReadGuardError::NotCurrent(
                VectorCacheStaleness::CommitInFlight
            ))
        ));
        assert!(
            writer.resident.get_upper_vector(7).is_some(),
            "nothing is evicted before the storage outcome is known"
        );

        writer.gate.uploads.send_replace(WalUploads::Open);
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            while writer.commit_pending() {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the detached commit resolves its fence after the caller is gone");
        let guard = writer
            .registry
            .read_guard_for(&writer.handle, applied_seq)
            .expect("the evicted store is current for the applied snapshot");
        assert!(guard.store().get_upper_vector(7).is_none());
        assert!(guard.store().get_upper_vector(8).is_some());
        drop(guard);
        writer.db.close().await.unwrap();
    }

    #[tokio::test]
    async fn fenced_commit_evicts_when_storage_fails_after_applying_the_batch() {
        let writer = FencedWriter::open("fenced-commit-post-apply-error").await;
        let (transaction, fences) = writer.fenced_transaction().await;
        writer.gate.uploads.send_replace(WalUploads::Failing);

        let error = commit_fenced(transaction, fences).await.unwrap_err();

        assert_ne!(error.kind(), slatedb::ErrorKind::Transaction);
        assert!(!writer.commit_pending());
        assert!(
            writer.resident.get_upper_vector(7).is_none(),
            "an error after apply may hide a visible write, so its rows are evicted"
        );
        assert!(writer.resident.get_upper_vector(8).is_some());
        assert!(writer
            .registry
            .read_guard_for(&writer.handle, u64::MAX)
            .is_ok());
    }

    #[tokio::test]
    async fn fenced_commit_conflict_releases_its_fence_without_eviction() {
        let writer = FencedWriter::open("fenced-commit-conflict").await;
        let (transaction, fences) = writer.fenced_transaction().await;
        writer
            .db
            .put(b"fenced-key", Bytes::from_static(b"competing"))
            .await
            .unwrap();

        let error = commit_fenced(transaction, fences).await.unwrap_err();

        assert_eq!(error.kind(), slatedb::ErrorKind::Transaction);
        assert!(!writer.commit_pending());
        assert!(writer.resident.get_upper_vector(7).is_some());
        let guard = writer
            .registry
            .read_guard_for(&writer.handle, u64::MAX)
            .expect("a rejected commit leaves the store current");
        assert_eq!(
            guard.pending_dirty().generation(),
            0,
            "a rejected batch changes no row"
        );
        drop(guard);
        writer.db.close().await.unwrap();
    }

    #[test]
    fn retirement_replaces_dirty_eviction_for_one_exact_generation() {
        let writes = VectorCacheWriteSet::default();
        let retired = handle(1);
        writes.dirty_rows_for(&retired).mark_node_dirty(7);
        writes.retire_after_commit(&retired);

        let entries = writes.entries();
        let [entry] = entries.as_slice() else {
            panic!("one exact cache effect remains")
        };
        assert!(entry.dirty_rows().is_none());
        assert_eq!(entry.retirement(), Some(&retired));
    }
}
