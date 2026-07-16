//! Stable storage views owned by one executable read request.
//!
//! Writer-backed reads pin a SlateDB [`DbSnapshot`]. Standalone [`DbReader`]
//! does not expose a snapshot constructor, so its view records the complete
//! monotonic [`DbStatus`] and validates it after execution. A reader advance
//! makes the request retry instead of publishing results assembled across two
//! reader states.

use std::sync::Arc;

use slatedb::{DbReadOps, DbReader, DbSnapshot, DbStatus};

use super::*;
use crate::HelixStorage;

/// One read-only request view whose storage source cannot be chosen piecemeal.
#[derive(Clone)]
pub(in crate::execution::interpreter) enum StableRequestReadView {
    /// Writer storage supplies a true sequence-pinned snapshot.
    Snapshot(Arc<DbSnapshot>),
    /// Reader storage is accepted only while its complete status stays fixed.
    GuardedReader {
        reader: Arc<DbReader>,
        observed: Box<DbStatus>,
    },
}

impl StableRequestReadView {
    /// Returns the storage sequence only when SlateDB exposes a comparable one.
    ///
    /// Standalone readers intentionally return `None`: their durable sequence
    /// does not prove the exact WAL-inclusive view used by every read, so it
    /// must not authorize vector cache observations.
    pub(in crate::execution::interpreter) fn comparable_sequence(&self) -> Option<u64> {
        match self {
            Self::Snapshot(snapshot) => Some(snapshot.seq()),
            Self::GuardedReader { .. } => None,
        }
    }

    /// Confirms that a guarded reader did not advance during the request.
    ///
    /// Snapshot-backed views are intrinsically stable. Reader-backed views
    /// compare manifest, durable sequence, segments, and close state together;
    /// a mismatch is retryable and no execution result may be returned.
    pub(in crate::execution::interpreter) fn validate(&self) -> Result<()> {
        match self {
            Self::Snapshot(_) => Ok(()),
            Self::GuardedReader { reader, observed } if reader.status() == **observed => Ok(()),
            Self::GuardedReader { .. } => Err(HelixDbError::RequestReadViewChanged),
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
            Self::Snapshot(snapshot) => snapshot.get_with_options(key, options).await,
            Self::GuardedReader { reader, .. } => reader.get_with_options(key, options).await,
        }
    }

    async fn get_key_value_with_options<K: AsRef<[u8]> + Send>(
        &self,
        key: K,
        options: &slatedb::config::ReadOptions,
    ) -> std::result::Result<Option<slatedb::KeyValue>, slatedb::Error> {
        match self {
            Self::Snapshot(snapshot) => snapshot.get_key_value_with_options(key, options).await,
            Self::GuardedReader { reader, .. } => {
                reader.get_key_value_with_options(key, options).await
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
            Self::Snapshot(snapshot) => snapshot.multi_get_with_options(keys, options).await,
            Self::GuardedReader { reader, .. } => {
                reader.multi_get_with_options(keys, options).await
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
            Self::Snapshot(snapshot) => snapshot.scan_with_options(range, options).await,
            Self::GuardedReader { reader, .. } => reader.scan_with_options(range, options).await,
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
            Self::Snapshot(snapshot) => {
                snapshot
                    .scan_prefix_with_options(prefix, subrange, options)
                    .await
            }
            Self::GuardedReader { reader, .. } => {
                reader
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
            self.request_read_view.is_none(),
            "request read view must be acquired exactly once"
        );
        self.request_read_view = Some(Box::new(match self.db.storage() {
            HelixStorage::Writer(writer) => {
                StableRequestReadView::Snapshot(writer.db().snapshot().await?)
            }
            HelixStorage::Reader(reader) => StableRequestReadView::GuardedReader {
                reader: Arc::clone(reader),
                observed: Box::new(reader.status()),
            },
        }));
        Ok(())
    }

    /// Validates the request view before an execution result is exposed.
    pub(in crate::execution::interpreter) fn validate_request_read_view(&self) -> Result<()> {
        let Some(view) = self.request_read_view.as_deref() else {
            return Err(HelixDbError::InvariantViolation(
                "read plan completed without a request read view".to_string(),
            ));
        };
        view.validate()
    }

    /// Borrows the request snapshot for storage and search operations.
    pub(in crate::execution::interpreter) fn request_read_view(
        &self,
    ) -> Option<&StableRequestReadView> {
        self.request_read_view.as_deref()
    }

    /// Acquires and post-acquisition revalidates one Active generation lease.
    pub(in crate::execution::interpreter) async fn acquire_index_read_lease(
        &self,
        reader: &(impl DbReadOps + Sync),
        handle: &crate::index_v2::ActiveIndexHandle,
    ) -> Result<crate::index_v2::reader_lease::LeaseGenerationKey> {
        self.index_read_leases
            .acquire(
                reader,
                self.db.reader_lease_coordinator(),
                self.db.reader_lease_holder(),
                handle,
            )
            .await
    }

    /// Runs one physical index batch only while its request lease is valid.
    pub(in crate::execution::interpreter) async fn run_index_read_batch<T>(
        &self,
        generation: crate::index_v2::reader_lease::LeaseGenerationKey,
        batch: impl std::future::Future<Output = Result<T>>,
    ) -> Result<T> {
        self.index_read_leases.run_batch(generation, batch).await
    }

    /// Validates every retained generation immediately before result publication.
    pub(in crate::execution::interpreter) async fn validate_and_release_index_read_leases(
        &self,
    ) -> Result<()> {
        self.index_read_leases.validate_and_release().await
    }

    /// Releases retained generation leases once a request is already failing.
    pub(in crate::execution::interpreter) async fn release_index_read_leases(&self) {
        self.index_read_leases.release_all().await;
    }
}

#[cfg(test)]
mod tests {
    use slatedb::object_store::memory::InMemory;

    use super::*;

    /// Proves reader results are accepted only against their captured status.
    #[tokio::test]
    async fn guarded_reader_validation_rejects_a_changed_status() {
        let object_store: Arc<dyn slatedb::object_store::ObjectStore> = Arc::new(InMemory::new());
        let db = slatedb::Db::open("guarded-reader-view", Arc::clone(&object_store))
            .await
            .expect("writer opens");
        let reader = Arc::new(
            DbReader::open(
                "guarded-reader-view",
                object_store,
                None,
                slatedb::config::DbReaderOptions::default(),
            )
            .await
            .expect("reader opens"),
        );
        let observed = reader.status();
        let unchanged = StableRequestReadView::GuardedReader {
            reader: Arc::clone(&reader),
            observed: Box::new(observed.clone()),
        };
        assert!(unchanged.validate().is_ok());

        let mut changed = observed;
        changed.durable_seq = changed.durable_seq.saturating_add(1);
        let stale = StableRequestReadView::GuardedReader {
            reader: Arc::clone(&reader),
            observed: Box::new(changed),
        };
        assert!(matches!(
            stale.validate(),
            Err(HelixDbError::RequestReadViewChanged)
        ));

        drop((unchanged, stale));
        reader.close().await.expect("reader closes");
        db.close().await.expect("writer closes");
    }
}
