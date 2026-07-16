//! Transaction-local vector write measurement with SlateDB replacement semantics.
//!
//! [`MeasuredVectorTransaction`] is the write boundary used by vector mutation
//! code. It forwards each put/delete to the caller-owned SlateDB transaction
//! while retaining only the latest operation for each encoded key, matching
//! SlateDB `WriteBatch` last-write-wins behavior. Lifecycle builders can run a
//! deterministic HNSW insertion in an uncommitted planning transaction, obtain
//! [`VectorWriteMeasurement`], and admit or reject the complete graph invariant
//! before replaying it in the transaction that also publishes V2 outbox progress.
//!
//! Measurement counts existing encoded key/value bytes. It neither wraps nor
//! changes vector keys, vector row values, metadata, or the SlateDB wire format.

use std::collections::BTreeMap;
use std::ops::Deref;
#[cfg(any(test, feature = "production-coverage"))]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::Mutex;

#[cfg(any(test, feature = "production-coverage"))]
const NO_INJECTED_FAILURE: usize = usize::MAX;

/// Exact final vector writes staged in one SlateDB transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct VectorWriteMeasurement {
    operations: u64,
    encoded_bytes: u64,
}

impl VectorWriteMeasurement {
    /// Returns the identity measurement for a transaction with no vector writes.
    pub(crate) const fn zero() -> Self {
        Self {
            operations: 0,
            encoded_bytes: 0,
        }
    }

    /// Returns the unique final put/delete count after same-key replacement.
    pub(crate) const fn operations(self) -> u64 {
        self.operations
    }

    /// Returns final encoded key bytes plus put-value bytes.
    ///
    /// Deletes contribute their encoded key bytes and no value bytes, matching
    /// the measurement convention used by the lifecycle backfill budget.
    pub(crate) const fn encoded_bytes(self) -> u64 {
        self.encoded_bytes
    }
}

/// Borrowed SlateDB transaction that records its final vector write set.
///
/// Read methods continue through [`Deref`] to the underlying transaction, so
/// HNSW traversal observes earlier staged writes exactly as normal. Vector code
/// must use this type's `put`, `put_bytes`, and `delete` methods for every write
/// that belongs to the measured invariant.
pub(crate) struct MeasuredVectorTransaction<'txn> {
    inner: &'txn slatedb::DbTransaction,
    recorder: VectorWriteRecorder,
    #[cfg(any(test, feature = "production-coverage"))]
    writes_until_failure: AtomicUsize,
    #[cfg(any(test, feature = "production-coverage"))]
    reads_until_failure: AtomicUsize,
}

impl<'txn> MeasuredVectorTransaction<'txn> {
    /// Starts empty measurement around an existing caller-owned transaction.
    pub(crate) fn new(inner: &'txn slatedb::DbTransaction) -> Self {
        Self {
            inner,
            recorder: VectorWriteRecorder::new(),
            #[cfg(any(test, feature = "production-coverage"))]
            writes_until_failure: AtomicUsize::new(NO_INJECTED_FAILURE),
            #[cfg(any(test, feature = "production-coverage"))]
            reads_until_failure: AtomicUsize::new(NO_INJECTED_FAILURE),
        }
    }

    /// Injects one pre-write storage failure for transaction-boundary tests.
    #[cfg(any(test, feature = "production-coverage"))]
    pub(crate) fn fail_next_write(&self) {
        self.fail_write_after(0);
    }

    /// Injects one storage failure after `successful_writes` measured writes.
    #[cfg(any(test, feature = "production-coverage"))]
    pub(crate) fn fail_write_after(&self, successful_writes: usize) {
        assert_ne!(successful_writes, NO_INJECTED_FAILURE);
        self.writes_until_failure
            .store(successful_writes, Ordering::Release);
    }

    /// Injects one storage failure after `successful_reads` delegated reads.
    ///
    /// This deterministic test seam faults the complete measured transaction
    /// read boundary, allowing mutation contracts to visit every async error
    /// continuation without replacing SlateDB or duplicating vector codecs.
    #[cfg(feature = "production-coverage")]
    pub(crate) fn fail_read_after(&self, successful_reads: usize) {
        assert_ne!(successful_reads, NO_INJECTED_FAILURE);
        self.reads_until_failure
            .store(successful_reads, Ordering::Release);
    }

    /// Consumes one configured I/O position and reports whether it must fail.
    #[cfg(any(test, feature = "production-coverage"))]
    fn take_injected_failure(counter: &AtomicUsize) -> bool {
        counter
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                (remaining != NO_INJECTED_FAILURE).then_some(if remaining == 0 {
                    NO_INJECTED_FAILURE
                } else {
                    remaining - 1
                })
            })
            .is_ok_and(|remaining| remaining == 0)
    }

    /// Returns the deterministic backend error used by measured read seams.
    #[cfg(any(test, feature = "production-coverage"))]
    fn injected_read_error() -> slatedb::Error {
        slatedb::Error::unavailable("injected measured vector read failure".to_string())
    }

    /// Captures an opaque boundary before one indivisible vector mutation.
    ///
    /// Pass the returned value to [`Self::measurement_since`] after staging the
    /// mutation. Checkpoints belong only to this measured transaction and are
    /// monotonic in write-call order.
    pub(crate) fn checkpoint(&self) -> VectorWriteCheckpoint {
        VectorWriteCheckpoint {
            recorder_identity: Arc::clone(&self.recorder.identity),
            revision: self.recorder.writes.lock().revision,
        }
    }

    /// Stages one put and records its final encoded key/value size.
    pub(crate) fn put<K, V>(&self, key: K, value: V) -> Result<(), slatedb::Error>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.put_bytes(
            Bytes::copy_from_slice(key.as_ref()),
            Bytes::copy_from_slice(value.as_ref()),
        )
    }

    /// Stages an owned-byte put without changing its current encoded bytes.
    pub(crate) fn put_bytes(&self, key: Bytes, value: Bytes) -> Result<(), slatedb::Error> {
        #[cfg(any(test, feature = "production-coverage"))]
        if Self::take_injected_failure(&self.writes_until_failure) {
            return Err(slatedb::Error::unavailable(
                "injected measured vector write failure".to_string(),
            ));
        }
        self.inner.put_bytes(key.clone(), value.clone())?;
        self.recorder
            .writes
            .lock()
            .record(key, FinalVectorWriteKind::Put { value });
        Ok(())
    }

    /// Stages one delete and replaces any earlier measured operation for its key.
    pub(crate) fn delete<K>(&self, key: K) -> Result<(), slatedb::Error>
    where
        K: AsRef<[u8]>,
    {
        #[cfg(any(test, feature = "production-coverage"))]
        if Self::take_injected_failure(&self.writes_until_failure) {
            return Err(slatedb::Error::unavailable(
                "injected measured vector write failure".to_string(),
            ));
        }
        let key = Bytes::copy_from_slice(key.as_ref());
        self.inner.delete(key.clone())?;
        self.recorder
            .writes
            .lock()
            .record(key, FinalVectorWriteKind::Delete);
        Ok(())
    }

    /// Calculates the exact final unique operation and encoded-byte totals.
    ///
    /// Call this only after the complete vector invariant has been staged. The
    /// returned result ignores superseded same-key operations exactly as the
    /// underlying SlateDB batch does.
    pub(crate) fn measurement(
        &self,
    ) -> Result<VectorWriteMeasurement, VectorWriteMeasurementError> {
        self.recorder.writes.lock().measurement_after(None)
    }

    /// Measures final unique writes touched after an entity checkpoint.
    ///
    /// A key written multiple times after `checkpoint` contributes only its
    /// final operation. A key staged by an earlier entity and rewritten by this
    /// entity is included, which makes this the indivisible current-entity
    /// capacity measurement rather than a numeric difference between two
    /// cumulative totals.
    pub(crate) fn measurement_since(
        &self,
        checkpoint: VectorWriteCheckpoint,
    ) -> Result<VectorWriteMeasurement, VectorWriteMeasurementError> {
        let writes = self.recorder.writes.lock();
        if !Arc::ptr_eq(&checkpoint.recorder_identity, &self.recorder.identity)
            || checkpoint.revision > writes.revision
        {
            return Err(VectorWriteMeasurementError::ForeignCheckpoint);
        }
        writes.measurement_after(Some(&checkpoint))
    }
}

/// Shareable measurement state for successive planning calls on one transaction.
///
/// A streaming vector planner owns one recorder for its one-shot planning
/// transaction and calls [`Self::bind`] for each bounded source chunk. This
/// retains cumulative last-write-wins identity without storing a transaction
/// reference inside the planner or extending its lifetime.
#[derive(Clone)]
pub(crate) struct VectorWriteRecorder {
    identity: Arc<()>,
    writes: Arc<Mutex<VectorWriteState>>,
}

impl VectorWriteRecorder {
    /// Creates an empty transaction-local write recorder.
    pub(crate) fn new() -> Self {
        Self {
            identity: Arc::new(()),
            writes: Arc::new(Mutex::new(VectorWriteState::default())),
        }
    }

    /// Borrows a SlateDB transaction while sharing this recorder's write state.
    pub(crate) fn bind<'txn>(
        &self,
        inner: &'txn slatedb::DbTransaction,
    ) -> MeasuredVectorTransaction<'txn> {
        MeasuredVectorTransaction {
            inner,
            recorder: self.clone(),
            #[cfg(any(test, feature = "production-coverage"))]
            writes_until_failure: AtomicUsize::new(NO_INJECTED_FAILURE),
            #[cfg(any(test, feature = "production-coverage"))]
            reads_until_failure: AtomicUsize::new(NO_INJECTED_FAILURE),
        }
    }
}

impl Deref for MeasuredVectorTransaction<'_> {
    type Target = slatedb::DbTransaction;

    fn deref(&self) -> &Self::Target {
        self.inner
    }
}

#[async_trait::async_trait]
impl slatedb::DbReadOps for MeasuredVectorTransaction<'_> {
    async fn get_with_options<K: AsRef<[u8]> + Send>(
        &self,
        key: K,
        options: &slatedb::config::ReadOptions,
    ) -> Result<Option<Bytes>, slatedb::Error> {
        #[cfg(any(test, feature = "production-coverage"))]
        if Self::take_injected_failure(&self.reads_until_failure) {
            return Err(Self::injected_read_error());
        }
        self.inner.get_with_options(key, options).await
    }

    async fn get_key_value_with_options<K: AsRef<[u8]> + Send>(
        &self,
        key: K,
        options: &slatedb::config::ReadOptions,
    ) -> Result<Option<slatedb::KeyValue>, slatedb::Error> {
        #[cfg(any(test, feature = "production-coverage"))]
        if Self::take_injected_failure(&self.reads_until_failure) {
            return Err(Self::injected_read_error());
        }
        self.inner.get_key_value_with_options(key, options).await
    }

    async fn multi_get_with_options<K>(
        &self,
        keys: &[K],
        options: &slatedb::config::ReadOptions,
    ) -> Result<Vec<Option<Bytes>>, slatedb::Error>
    where
        K: AsRef<[u8]> + Send + Sync,
    {
        #[cfg(any(test, feature = "production-coverage"))]
        if Self::take_injected_failure(&self.reads_until_failure) {
            return Err(Self::injected_read_error());
        }
        self.inner.multi_get_with_options(keys, options).await
    }

    async fn scan_with_options<T>(
        &self,
        range: T,
        options: &slatedb::config::ScanOptions,
    ) -> Result<slatedb::DbIterator, slatedb::Error>
    where
        T: slatedb::ByteRangeBounds + Send,
    {
        #[cfg(any(test, feature = "production-coverage"))]
        if Self::take_injected_failure(&self.reads_until_failure) {
            return Err(Self::injected_read_error());
        }
        self.inner.scan_with_options(range, options).await
    }

    async fn scan_prefix_with_options<P, T>(
        &self,
        prefix: P,
        subrange: T,
        options: &slatedb::config::ScanOptions,
    ) -> Result<slatedb::DbIterator, slatedb::Error>
    where
        P: AsRef<[u8]> + Send,
        T: slatedb::ByteRangeBounds + Send,
    {
        #[cfg(any(test, feature = "production-coverage"))]
        if Self::take_injected_failure(&self.reads_until_failure) {
            return Err(Self::injected_read_error());
        }
        self.inner
            .scan_prefix_with_options(prefix, subrange, options)
            .await
    }
}

/// Opaque write-order boundary owned by one shared vector write recorder.
///
/// Recorder identity prevents a numerically plausible checkpoint from another
/// planning transaction from silently producing an incomplete measurement.
#[derive(Debug, Clone)]
pub(crate) struct VectorWriteCheckpoint {
    recorder_identity: Arc<()>,
    revision: u64,
}

/// Final same-key operation and its most recent write-call revision.
struct FinalVectorWrite {
    kind: FinalVectorWriteKind,
    revision: u64,
}

/// Current final operation retained for one encoded key.
enum FinalVectorWriteKind {
    Put { value: Bytes },
    Delete,
}

/// Last-write-wins map plus monotonic write-call order.
#[derive(Default)]
struct VectorWriteState {
    writes: BTreeMap<Bytes, FinalVectorWrite>,
    revision: u64,
}

impl VectorWriteState {
    /// Replaces one final key operation and advances the local write order.
    fn record(&mut self, key: Bytes, kind: FinalVectorWriteKind) {
        self.revision = self
            .revision
            .checked_add(1)
            .expect("one transaction cannot stage more than u64 vector write calls");
        self.writes.insert(
            key,
            FinalVectorWrite {
                kind,
                revision: self.revision,
            },
        );
    }

    /// Measures all final writes or only keys touched after one checkpoint.
    fn measurement_after(
        &self,
        checkpoint: Option<&VectorWriteCheckpoint>,
    ) -> Result<VectorWriteMeasurement, VectorWriteMeasurementError> {
        let minimum_revision = checkpoint.map_or(0, |checkpoint| checkpoint.revision);
        self.writes
            .iter()
            .filter(|(_, write)| write.revision > minimum_revision)
            .try_fold(
                VectorWriteMeasurement {
                    operations: 0,
                    encoded_bytes: 0,
                },
                |measurement, (key, write)| {
                    let key_bytes = u64::try_from(key.len())
                        .map_err(|_| VectorWriteMeasurementError::ArithmeticOverflow)?;
                    let value_bytes = match &write.kind {
                        FinalVectorWriteKind::Put { value } => u64::try_from(value.len())
                            .map_err(|_| VectorWriteMeasurementError::ArithmeticOverflow)?,
                        FinalVectorWriteKind::Delete => 0,
                    };
                    Ok(VectorWriteMeasurement {
                        operations: measurement
                            .operations
                            .checked_add(1)
                            .ok_or(VectorWriteMeasurementError::ArithmeticOverflow)?,
                        encoded_bytes: measurement
                            .encoded_bytes
                            .checked_add(key_bytes)
                            .and_then(|total| total.checked_add(value_bytes))
                            .ok_or(VectorWriteMeasurementError::ArithmeticOverflow)?,
                    })
                },
            )
    }
}

/// Checked conversion or accumulation failure during final measurement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum VectorWriteMeasurementError {
    /// Operation count or encoded key/value byte accumulation exceeded `u64`.
    #[error("vector transaction write measurement overflowed u64")]
    ArithmeticOverflow,
    /// Checkpoint revision is newer than this measured transaction.
    #[error("vector write checkpoint does not belong to the measured transaction state")]
    ForeignCheckpoint,
}

#[cfg(feature = "production-coverage")]
#[path = "../../../tests/production_support/vector/write_transaction.rs"]
pub(crate) mod production_contracts;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use slatedb::object_store::memory::InMemory;
    use slatedb::IsolationLevel;

    use super::*;

    #[tokio::test]
    async fn measurement_matches_final_same_key_replacement_semantics() {
        let db = slatedb::Db::open("vector-write-measurement", Arc::new(InMemory::new()))
            .await
            .unwrap();
        let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let measured = MeasuredVectorTransaction::new(&txn);

        measured.put(b"old", b"stable").unwrap();
        let checkpoint = measured.checkpoint();
        measured.put(b"first", b"superseded").unwrap();
        measured.put(b"first", b"final").unwrap();
        measured
            .put_bytes(Bytes::from_static(b"second"), Bytes::from_static(b"value"))
            .unwrap();
        measured.delete(b"second").unwrap();

        assert_eq!(
            measured.measurement().unwrap(),
            VectorWriteMeasurement {
                operations: 3,
                encoded_bytes: (b"old".len()
                    + b"stable".len()
                    + b"first".len()
                    + b"final".len()
                    + b"second".len()) as u64,
            }
        );
        assert_eq!(
            measured.measurement_since(checkpoint).unwrap(),
            VectorWriteMeasurement {
                operations: 2,
                encoded_bytes: (b"first".len() + b"final".len() + b"second".len()) as u64,
            }
        );
        let foreign = MeasuredVectorTransaction::new(&txn).checkpoint();
        assert_eq!(
            measured.measurement_since(foreign),
            Err(VectorWriteMeasurementError::ForeignCheckpoint)
        );
        assert_eq!(measured.get(b"first").await.unwrap().unwrap(), b"final"[..]);
        assert_eq!(measured.get(b"second").await.unwrap(), None);
    }

    #[tokio::test]
    async fn recorder_preserves_cumulative_identity_across_short_borrows() {
        let db = slatedb::Db::open("vector-write-recorder", Arc::new(InMemory::new()))
            .await
            .unwrap();
        let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let recorder = VectorWriteRecorder::new();

        let first = recorder.bind(&txn);
        first.put(b"shared", b"first").unwrap();
        let checkpoint = first.checkpoint();
        drop(first);

        let second = recorder.bind(&txn);
        second.put(b"shared", b"replacement").unwrap();
        second.put(b"new", b"value").unwrap();

        assert_eq!(
            second.measurement().unwrap(),
            VectorWriteMeasurement {
                operations: 2,
                encoded_bytes: (b"shared".len()
                    + b"replacement".len()
                    + b"new".len()
                    + b"value".len()) as u64,
            }
        );
        assert_eq!(
            second.measurement_since(checkpoint).unwrap(),
            second.measurement().unwrap()
        );
    }
}
