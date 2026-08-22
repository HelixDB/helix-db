//! Stable storage views owned by one executable read request.
//!
//! Reader nodes pin one SlateDB [`DbSnapshot`] for the complete request.
//! Writer nodes use one ordinary [`DbTransaction`] opened at
//! [`IsolationLevel::Snapshot`]. The two variants make it impossible to build
//! a request view with the wrong storage capability or isolation level.

use std::sync::Arc;

use slatedb::{DbReadOps, DbSnapshot, DbTransaction, IsolationLevel};

use super::*;
use crate::HelixStorage;

/// Closed read-request authority for one exact storage view.
pub(in crate::execution::interpreter) enum RequestReadScopeState {
    /// No read request is active.
    Disabled,
    /// The request owns one complete storage/catalog authority.
    Active(Box<ActiveRequestReadView>),
}

/// Catalog authority that cannot be detached from its matching storage snapshot.
pub(in crate::execution::interpreter) enum ActiveRequestReadView {
    /// Planning and execution share one view while lifecycle publication is excluded.
    Prepared {
        view: StableRequestReadView,
        catalog: Arc<crate::index_lifecycle::LoadedV2ScopeCatalog>,
        _catalog_permit: Arc<crate::index_lifecycle::IndexScopeCatalogPermit>,
    },
    /// DDL released the process-local gate, while the pinned snapshot remains exact.
    Pinned {
        view: StableRequestReadView,
        catalog: Arc<crate::index_lifecycle::LoadedV2ScopeCatalog>,
    },
    /// Public physical-plan execution resolves canonical records from this view.
    Unprepared(StableRequestReadView),
}

impl ActiveRequestReadView {
    fn view(&self) -> &StableRequestReadView {
        match self {
            Self::Prepared { view, .. } | Self::Pinned { view, .. } | Self::Unprepared(view) => {
                view
            }
        }
    }

    fn catalog(&self) -> Option<&crate::index_lifecycle::LoadedV2ScopeCatalog> {
        match self {
            Self::Prepared { catalog, .. } | Self::Pinned { catalog, .. } => Some(catalog),
            Self::Unprepared(_) => None,
        }
    }

    fn release_catalog_permit(self) -> Self {
        match self {
            Self::Prepared { view, catalog, .. } => Self::Pinned { view, catalog },
            unchanged @ (Self::Pinned { .. } | Self::Unprepared(_)) => unchanged,
        }
    }

    fn clone_reader_snapshot(&self) -> Self {
        match self {
            Self::Prepared {
                view,
                catalog,
                _catalog_permit,
            } => Self::Prepared {
                view: view.clone_reader_snapshot(),
                catalog: Arc::clone(catalog),
                _catalog_permit: Arc::clone(_catalog_permit),
            },
            Self::Pinned { view, catalog } => Self::Pinned {
                view: view.clone_reader_snapshot(),
                catalog: Arc::clone(catalog),
            },
            Self::Unprepared(view) => Self::Unprepared(view.clone_reader_snapshot()),
        }
    }

    fn close(self) {
        match self {
            Self::Prepared { view, .. } | Self::Pinned { view, .. } | Self::Unprepared(view) => {
                view.close()
            }
        }
    }
}

impl RequestReadScopeState {
    fn active(&self) -> Option<&ActiveRequestReadView> {
        match self {
            Self::Disabled => None,
            Self::Active(active) => Some(active),
        }
    }

    fn release_catalog_permit(&mut self) {
        let state = std::mem::replace(self, Self::Disabled);
        *self = match state {
            Self::Disabled => Self::Disabled,
            Self::Active(active) => Self::Active(Box::new((*active).release_catalog_permit())),
        };
    }
}

/// One read-only request view whose storage source cannot change between steps.
pub(crate) enum StableRequestReadView {
    /// A reader-node view pinned to one checkpoint generation and sequence.
    ReaderSnapshot {
        snapshot: Arc<DbSnapshot>,
        compatibility: crate::index_lifecycle::repository::ReaderStorageCompatibility,
    },
    /// A writer-node read transaction pinned at snapshot isolation.
    WriterTransaction(DbTransaction),
}

impl StableRequestReadView {
    /// Opens the storage view that planning and execution must share.
    pub(crate) async fn open(db: &HelixDB) -> Result<Self> {
        Ok(match db.storage() {
            HelixStorage::Reader(reader) => {
                let snapshot = reader.snapshot().await?;
                let compatibility =
                    crate::index_lifecycle::repository::require_reader_bootstrap_or_legacy(
                        snapshot.as_ref(),
                    )
                    .await?;
                db.observe_reader_storage_compatibility(compatibility)?;
                Self::ReaderSnapshot {
                    snapshot,
                    compatibility,
                }
            }
            HelixStorage::Writer(writer) => {
                Self::WriterTransaction(writer.db().begin(IsolationLevel::Snapshot).await?)
            }
        })
    }

    /// Returns the sequence visible to this request-scoped storage view.
    pub(in crate::execution::interpreter) fn comparable_sequence(&self) -> Option<u64> {
        Some(match self {
            Self::ReaderSnapshot { snapshot, .. } => snapshot.seq(),
            Self::WriterTransaction(transaction) => transaction.seqnum(),
        })
    }

    /// Returns whether this view must execute plan stages serially.
    ///
    /// SlateDB transactions are owned, stateful values and are never shared
    /// between concurrently executing plan steps. Reader snapshots are
    /// immutable and may be cloned into isolated parallel contexts.
    pub(in crate::execution::interpreter) const fn requires_serial_stages(&self) -> bool {
        matches!(self, Self::WriterTransaction(_))
    }

    pub(in crate::execution::interpreter) const fn storage_compatibility(
        &self,
    ) -> crate::index_lifecycle::repository::ReaderStorageCompatibility {
        match self {
            Self::ReaderSnapshot { compatibility, .. } => *compatibility,
            Self::WriterTransaction(_) => {
                crate::index_lifecycle::repository::ReaderStorageCompatibility::Current
            }
        }
    }

    /// Clones the immutable reader view into an isolated parallel context.
    fn clone_reader_snapshot(&self) -> Self {
        let Self::ReaderSnapshot {
            snapshot,
            compatibility,
        } = self
        else {
            unreachable!("writer transactions are scheduled serially")
        };
        Self::ReaderSnapshot {
            snapshot: Arc::clone(snapshot),
            compatibility: *compatibility,
        }
    }

    /// Ends the request view and explicitly unregisters writer read transactions.
    pub(crate) fn close(self) {
        match self {
            Self::ReaderSnapshot { snapshot, .. } => drop(snapshot),
            Self::WriterTransaction(transaction) => transaction.rollback(),
        }
    }
}

#[async_trait::async_trait]
impl DbReadOps for StableRequestReadView {
    async fn get_with_options<K: AsRef<[u8]> + Send>(
        &self,
        key: K,
        options: &slatedb::config::ReadOptions,
    ) -> std::result::Result<Option<bytes::Bytes>, slatedb::Error> {
        match self {
            Self::ReaderSnapshot { snapshot, .. } => snapshot.get_with_options(key, options).await,
            Self::WriterTransaction(transaction) => {
                transaction.get_with_options(key, options).await
            }
        }
    }

    async fn get_key_value_with_options<K: AsRef<[u8]> + Send>(
        &self,
        key: K,
        options: &slatedb::config::ReadOptions,
    ) -> std::result::Result<Option<slatedb::KeyValue>, slatedb::Error> {
        match self {
            Self::ReaderSnapshot { snapshot, .. } => {
                snapshot.get_key_value_with_options(key, options).await
            }
            Self::WriterTransaction(transaction) => {
                transaction.get_key_value_with_options(key, options).await
            }
        }
    }

    async fn multi_get_with_options<K>(
        &self,
        keys: &[K],
        options: &slatedb::config::ReadOptions,
    ) -> std::result::Result<Vec<Option<bytes::Bytes>>, slatedb::Error>
    where
        K: AsRef<[u8]> + Send + Sync,
    {
        match self {
            Self::ReaderSnapshot { snapshot, .. } => {
                snapshot.multi_get_with_options(keys, options).await
            }
            Self::WriterTransaction(transaction) => {
                transaction.multi_get_with_options(keys, options).await
            }
        }
    }

    async fn scan_with_options<T>(
        &self,
        range: T,
        options: &slatedb::config::ScanOptions,
    ) -> std::result::Result<slatedb::DbIterator, slatedb::Error>
    where
        T: slatedb::ByteRangeBounds + Send,
    {
        match self {
            Self::ReaderSnapshot { snapshot, .. } => {
                snapshot.scan_with_options(range, options).await
            }
            Self::WriterTransaction(transaction) => {
                transaction.scan_with_options(range, options).await
            }
        }
    }

    async fn scan_prefix_with_options<P, T>(
        &self,
        prefix: P,
        subrange: T,
        options: &slatedb::config::ScanOptions,
    ) -> std::result::Result<slatedb::DbIterator, slatedb::Error>
    where
        P: AsRef<[u8]> + Send,
        T: slatedb::ByteRangeBounds + Send,
    {
        match self {
            Self::ReaderSnapshot { snapshot, .. } => {
                snapshot
                    .scan_prefix_with_options(prefix, subrange, options)
                    .await
            }
            Self::WriterTransaction(transaction) => {
                transaction
                    .scan_prefix_with_options(prefix, subrange, options)
                    .await
            }
        }
    }
}

impl<'db> ExecutionContext<'db> {
    /// Acquires the one stable storage view used by every read-plan step.
    pub(in crate::execution::interpreter) async fn enable_request_read_view(
        &mut self,
    ) -> Result<()> {
        assert!(
            matches!(self.request_read_scope, RequestReadScopeState::Disabled),
            "request read view must be acquired exactly once"
        );
        let pending = std::mem::replace(
            &mut self.pending_catalog_freshness,
            runtime_context::PendingCatalogFreshness::Consumed,
        );
        let active = match pending {
            runtime_context::PendingCatalogFreshness::Prepared(proof) => {
                let (view, catalog, catalog_permit) = proof.into_read_parts();
                ActiveRequestReadView::Prepared {
                    view: *view,
                    catalog,
                    _catalog_permit: Arc::new(catalog_permit),
                }
            }
            runtime_context::PendingCatalogFreshness::Unverified
            | runtime_context::PendingCatalogFreshness::Consumed => {
                ActiveRequestReadView::Unprepared(StableRequestReadView::open(self.db).await?)
            }
        };
        self.request_read_scope = RequestReadScopeState::Active(Box::new(active));
        Ok(())
    }

    /// Verifies that a read plan acquired its request-scoped storage view.
    pub(in crate::execution::interpreter) fn validate_request_read_view(&self) -> Result<()> {
        let RequestReadScopeState::Active(_) = &self.request_read_scope else {
            return Err(HelixDbError::InvariantViolation(
                "read plan completed without a request read view".to_string(),
            ));
        };
        Ok(())
    }

    /// Ends a successful read view after result materialization.
    pub(in crate::execution::interpreter) fn close_request_read_view(&mut self) -> Result<()> {
        let state = std::mem::replace(
            &mut self.request_read_scope,
            RequestReadScopeState::Disabled,
        );
        let RequestReadScopeState::Active(view) = state else {
            return Err(HelixDbError::InvariantViolation(
                "read plan completed without a request read view".to_string(),
            ));
        };
        (*view).close();
        Ok(())
    }

    /// Borrows the request snapshot for storage and search operations.
    pub(in crate::execution::interpreter) fn request_read_view(
        &self,
    ) -> Option<&StableRequestReadView> {
        self.request_read_scope
            .active()
            .map(ActiveRequestReadView::view)
    }

    /// Returns the catalog decoded from the exact active read snapshot.
    pub(in crate::execution::interpreter) fn request_read_index_catalog(
        &self,
    ) -> Option<&crate::index_lifecycle::LoadedV2ScopeCatalog> {
        self.request_read_scope
            .active()
            .and_then(ActiveRequestReadView::catalog)
    }

    /// Releases only the process-local catalog gate before operation-owned DDL.
    pub(in crate::execution::interpreter) fn release_read_catalog_permit_for_ddl(&mut self) {
        self.request_read_scope.release_catalog_permit();
    }

    /// Returns whether this request view must keep plan stages serial.
    pub(in crate::execution::interpreter) fn request_read_view_requires_serial_stages(
        &self,
    ) -> bool {
        self.request_read_view()
            .is_some_and(StableRequestReadView::requires_serial_stages)
    }

    /// Clones a reader snapshot into one isolated parallel step context.
    pub(in crate::execution::interpreter) fn clone_parallel_request_read_scope(
        &self,
    ) -> RequestReadScopeState {
        match self.request_read_scope.active() {
            Some(active) => RequestReadScopeState::Active(Box::new(active.clone_reader_snapshot())),
            None => RequestReadScopeState::Disabled,
        }
    }
}

#[cfg(test)]
mod tests {
    use slatedb::object_store::memory::InMemory;

    use super::super::test_support;
    use super::*;

    #[tokio::test]
    async fn context_enforces_read_view_acquire_validate_and_close_lifecycle() {
        let db = test_support::open_db("read-view-lifecycle").await;
        let mut context = ExecutionContext::new(&db, context::ParamBindings::default());

        assert!(matches!(
            context.validate_request_read_view(),
            Err(HelixDbError::InvariantViolation(_))
        ));
        assert!(matches!(
            context.close_request_read_view(),
            Err(HelixDbError::InvariantViolation(_))
        ));

        context
            .enable_request_read_view()
            .await
            .expect("request view opens");
        context
            .validate_request_read_view()
            .expect("request view validates");
        context
            .close_request_read_view()
            .expect("request view closes");
        assert!(context.request_read_view().is_none());
    }

    #[tokio::test]
    async fn writer_transaction_keeps_its_initial_snapshot_and_requires_serial_stages() {
        let object_store: Arc<dyn slatedb::object_store::ObjectStore> = Arc::new(InMemory::new());
        let db = slatedb::Db::open("writer-read-view", object_store)
            .await
            .expect("writer opens");
        db.put(b"key", b"before").await.expect("initial value");
        let view = StableRequestReadView::WriterTransaction(
            db.begin(IsolationLevel::Snapshot)
                .await
                .expect("snapshot transaction starts"),
        );

        db.put(b"key", b"after").await.expect("concurrent value");

        assert!(view.requires_serial_stages());
        assert_eq!(
            view.get(b"key").await.expect("transaction read"),
            Some(bytes::Bytes::from_static(b"before"))
        );
        let key_value = view
            .get_key_value(b"key")
            .await
            .expect("transaction key-value read")
            .expect("transaction key-value exists");
        assert_eq!(key_value.key, bytes::Bytes::from_static(b"key"));
        assert_eq!(key_value.value, bytes::Bytes::from_static(b"before"));
        view.close();
        db.close().await.expect("writer closes");
    }

    #[tokio::test]
    async fn reader_snapshot_preserves_order_duplicates_and_missing_values() {
        let object_store: Arc<dyn slatedb::object_store::ObjectStore> = Arc::new(InMemory::new());
        let db = slatedb::Db::open("reader-read-view", Arc::clone(&object_store))
            .await
            .expect("writer opens");
        db.put(b"first", b"one").await.expect("first value");
        db.put(b"second", b"two").await.expect("second value");
        db.flush().await.expect("values flush");
        let reader = slatedb::DbReader::open(
            "reader-read-view",
            object_store,
            None,
            slatedb::config::DbReaderOptions {
                manifest_poll_interval: std::time::Duration::from_millis(10),
                ..slatedb::config::DbReaderOptions::default()
            },
        )
        .await
        .expect("reader opens");
        let view = StableRequestReadView::ReaderSnapshot {
            snapshot: reader.snapshot().await.expect("reader snapshot opens"),
            compatibility: crate::index_lifecycle::repository::ReaderStorageCompatibility::Current,
        };

        db.put(b"second", b"updated")
            .await
            .expect("concurrent update");
        db.put(b"second-new", b"phantom")
            .await
            .expect("concurrent prefix phantom");
        db.put(b"third", b"phantom")
            .await
            .expect("concurrent range phantom");
        db.delete(b"first").await.expect("concurrent delete");
        db.flush().await.expect("concurrent update flushes");
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if reader.get(b"second").await.expect("live reader reads")
                    == Some(bytes::Bytes::from_static(b"updated"))
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("live reader advances");

        assert!(!view.requires_serial_stages());
        assert_eq!(
            view.get(b"second").await.expect("snapshot point read"),
            Some(bytes::Bytes::from_static(b"two"))
        );
        let key_value = view
            .get_key_value(b"second")
            .await
            .expect("snapshot key-value read")
            .expect("snapshot key-value exists");
        assert_eq!(key_value.key, bytes::Bytes::from_static(b"second"));
        assert_eq!(key_value.value, bytes::Bytes::from_static(b"two"));
        assert_eq!(
            view.multi_get(&[b"second".as_slice(), b"missing", b"first", b"second"])
                .await
                .expect("snapshot multi-get"),
            vec![
                Some(bytes::Bytes::from_static(b"two")),
                None,
                Some(bytes::Bytes::from_static(b"one")),
                Some(bytes::Bytes::from_static(b"two")),
            ]
        );
        let mut range = view.scan(..).await.expect("snapshot range opens");
        let mut range_rows = Vec::new();
        while let Some(row) = range.next().await.expect("snapshot range advances") {
            range_rows.push((row.key, row.value));
        }
        assert_eq!(
            range_rows,
            vec![
                (
                    bytes::Bytes::from_static(b"first"),
                    bytes::Bytes::from_static(b"one"),
                ),
                (
                    bytes::Bytes::from_static(b"second"),
                    bytes::Bytes::from_static(b"two"),
                ),
            ]
        );
        let mut prefix = view
            .scan_prefix(b"second", ..)
            .await
            .expect("snapshot prefix opens");
        let mut prefix_rows = Vec::new();
        while let Some(row) = prefix.next().await.expect("snapshot prefix advances") {
            prefix_rows.push((row.key, row.value));
        }
        assert_eq!(
            prefix_rows,
            vec![(
                bytes::Bytes::from_static(b"second"),
                bytes::Bytes::from_static(b"two"),
            )]
        );
        let clone = view.clone_reader_snapshot();
        assert_eq!(clone.comparable_sequence(), view.comparable_sequence());

        drop((view, clone));
        reader.close().await.expect("reader closes");
        db.close().await.expect("writer closes");
    }
}
