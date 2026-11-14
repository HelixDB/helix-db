//! Borrowed mutation authority and request-owned read/merge admission.
//!
//! Helpers receive [`Mutation`], which exposes reads and writes but cannot
//! commit, roll back, or extract the underlying transaction. Native callers
//! retain their existing SlateDB transaction; request execution owns [`Owned`].
//! Its ledgers remain inside the database's finite commit-completion task
//! until the backend finishes. No persisted representation is changed here.

use bytes::Bytes;
use slatedb::{DbReadOps, DbTransaction};

use crate::error::{HelixDbError, Result};
use crate::query_resources;

mod call;
pub(crate) mod merges;
mod read_tracking;
mod reads;
#[cfg(test)]
mod tests;

/// When a request's commit returns to its caller.
///
/// SlateDB's WAL uploads in sequence order, so a crash loses only a suffix of
/// applied commits: a durable commit makes every lower sequence durable too.
/// That is what lets internal commits, which are always [`Self::Durable`],
/// build on an [`Self::Applied`] request commit: index publication reads a
/// queued operation and acknowledges it at a higher sequence, so the
/// acknowledgement can never outlive the enqueue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CommitDurability {
    /// Return once the commit is durable in object storage.
    Durable,
    /// Return once the commit is applied and visible on the writer. A crash
    /// before its WAL upload loses it, atomically and with every later commit.
    Applied,
}

impl CommitDurability {
    /// SlateDB's write options for a commit that returns as this asks.
    pub(crate) fn write_options(self) -> slatedb::config::WriteOptions {
        slatedb::config::WriteOptions {
            await_durable: self == Self::Durable,
            ..slatedb::config::WriteOptions::default()
        }
    }
}

/// Longest a request waits for a commit, its own or one its response rests on,
/// to become durable.
///
/// Only object storage accepting the WAL ends that wait. A request that runs
/// out of it fails with a timeout, which callers must reconcile rather than
/// resend: its commit, if it made one, is applied and may still become
/// durable.
pub(crate) const DURABLE_WAIT_LIMIT: std::time::Duration = std::time::Duration::from_secs(30);

/// Waits until every commit at or below `sequence` is durable, without forcing
/// a WAL upload. A closing database ends the wait with its close reason.
pub(crate) async fn await_durable_through(
    db: &slatedb::Db,
    sequence: u64,
) -> std::result::Result<(), slatedb::Error> {
    let mut subscription = db.subscribe();
    let status = subscription
        .wait_for(|status| status.durable_seq >= sequence || status.close_reason.is_some())
        .await
        .map_err(|error| slatedb::Error::internal(format!("database status ended: {error}")))?;
    match status.close_reason {
        Some(reason) if status.durable_seq < sequence => Err(slatedb::Error::closed(
            format!("database closed before sequence {sequence} became durable"),
            reason,
        )),
        Some(_) | None => Ok(()),
    }
}

/// An owned request transaction. Only its owner has lifecycle authority.
pub(crate) struct Owned {
    raw: DbTransaction,
    tracking: Option<Box<read_tracking::Tracker>>,
    merges: Option<Box<merges::Tracker>>,
    /// Read calls on this transaction, for round-trip contracts. The backend
    /// resolves a key with staged merges in a multi-get by its own point get.
    #[cfg(test)]
    reads: std::sync::atomic::AtomicUsize,
}

impl Owned {
    /// Open a fresh serializable transaction with admission already installed.
    /// Accepting an existing transaction could hide reads made before wrapping.
    pub(crate) async fn begin(
        db: &slatedb::Db,
        budget: Option<&query_resources::Budget>,
    ) -> Result<Self> {
        let tracking = budget
            .map(read_tracking::Tracker::new)
            .transpose()
            .map_err(|AdmissionFailure| HelixDbError::QueryMemoryLimitExceeded)?
            .map(Box::new);
        let merges = budget.map(merges::Tracker::new).transpose()?.map(Box::new);
        let raw = db
            .begin(slatedb::IsolationLevel::SerializableSnapshot)
            .await?;
        Ok(Self {
            raw,
            tracking,
            merges,
            #[cfg(test)]
            reads: std::sync::atomic::AtomicUsize::new(0),
        })
    }

    /// The sequence of the snapshot this transaction reads.
    pub(crate) fn snapshot_sequence(&self) -> u64 {
        self.raw.seqnum()
    }

    /// Read calls made on this transaction.
    #[cfg(test)]
    pub(crate) fn storage_reads(&self) -> usize {
        self.reads.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Commit the request's writes. Called only inside the finite commit
    /// owner, which survives cancellation, so the ledgers stay admitted until
    /// the backend outcome is known.
    pub(crate) async fn commit(
        self,
        durability: CommitDurability,
    ) -> std::result::Result<Option<slatedb::WriteHandle>, slatedb::Error> {
        let Self {
            raw,
            tracking,
            merges,
            #[cfg(test)]
                reads: _,
        } = self;
        let result = raw.commit_with_options(&durability.write_options()).await;
        drop(merges);
        drop(tracking);
        result
    }
}

/// A closed borrowed view; neither field nor lifecycle authority escapes it.
#[derive(Clone, Copy)]
pub(crate) struct View<'a> {
    raw: &'a DbTransaction,
    tracking: Option<&'a read_tracking::Tracker>,
    merges: Option<&'a merges::Tracker>,
}

mod sealed {
    pub trait Sealed {}
}
impl sealed::Sealed for DbTransaction {}
impl sealed::Sealed for Owned {}
impl sealed::Sealed for View<'_> {}
impl sealed::Sealed for crate::search::vector::MeasuredVectorTransaction<'_> {}

/// Opaque read authority consumed only by the admitted storage adapter. A native
/// read wrapper can return this context without exposing writes or lifecycle.
pub(crate) struct ReadContext<'a>(View<'a>);

/// Logical read category for native measurement hooks, independent of options.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReadKind {
    Point,
    MultiGet { keys: usize },
    Scan,
}

/// Closed source contract for one admitted read frame. Hooks run once, after
/// frame admission and before dependency registration or backend access.
pub(crate) trait ReadSource: Send + Sync + sealed::Sealed {
    fn read_context(&self) -> ReadContext<'_>;

    fn before_read(&self, _: ReadKind) -> std::result::Result<(), slatedb::Error> {
        Ok(())
    }
}

impl ReadSource for Owned {
    fn read_context(&self) -> ReadContext<'_> {
        ReadContext(self.mutation_view())
    }

    #[cfg(test)]
    fn before_read(&self, _: ReadKind) -> std::result::Result<(), slatedb::Error> {
        self.reads
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }
}

impl ReadSource for View<'_> {
    fn read_context(&self) -> ReadContext<'_> {
        ReadContext(*self)
    }
}

/// The native mutation contract preserves backend isolation and write behavior.
/// Membership merges never read the stored row; puts keep those rows decodable.
pub(crate) trait Mutation: DbReadOps + Send + Sync + sealed::Sealed {
    fn mutation_view(&self) -> View<'_>;

    /// Stages a full value. A value for a merge-maintained membership row must
    /// decode exactly as the merge operator decodes that row's stored value,
    /// so later deltas can always merge into it; see [`validate_put`]. With
    /// [`put_unbatched`], this is the production path to the backend's puts:
    /// clippy.toml disallows the raw ones. The only raw put left is the
    /// tenant-envelope startup copy, which moves legacy rows verbatim.
    fn put<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        &self,
        key: K,
        value: V,
    ) -> std::result::Result<(), slatedb::Error> {
        let view = self.mutation_view();
        validate_put(view.tracking, key.as_ref(), value.as_ref())?;
        #[allow(clippy::disallowed_methods)]
        view.raw.put(key, value)
    }

    /// Owned-bytes form of [`Self::put`], with the same validation.
    fn put_bytes(&self, key: Bytes, value: Bytes) -> std::result::Result<(), slatedb::Error> {
        let view = self.mutation_view();
        validate_put(view.tracking, &key, &value)?;
        #[allow(clippy::disallowed_methods)]
        view.raw.put_bytes(key, value)
    }

    fn delete<K: AsRef<[u8]>>(&self, key: K) -> std::result::Result<(), slatedb::Error> {
        self.mutation_view().raw.delete(key)
    }

    /// Reads distinct `keys` from this transaction's view, its own writes
    /// included, without adding a conflict dependency on them. A caller must
    /// show a concurrent change to these rows cannot invalidate its decision,
    /// or register the dependency later through [`Self::mark_read`]. This is
    /// a graph existence read, so vector measurement hooks do not see it.
    fn read_without_dependency<'a>(
        &'a self,
        keys: &'a [Bytes],
    ) -> call::Read<'a, Vec<Option<Bytes>>> {
        let view = self.mutation_view();
        call::admit(view.tracking.map(|tracker| &tracker.budget), async move {
            let _memory = view
                .tracking
                .map(|tracker| tracker.reserve(keys.len().saturating_mul(size_of::<Bytes>())))
                .transpose()
                .map_err(storage_error)?;
            view.raw.multi_get_untracked(keys).await
        })
    }

    /// Adds `keys` to the conflict dependencies, exactly as reading them at
    /// the transaction's snapshot would have.
    fn mark_read<K: AsRef<[u8]>, I: IntoIterator<Item = K>>(
        &self,
        keys: I,
    ) -> std::result::Result<(), slatedb::Error> {
        let view = self.mutation_view();
        match view.tracking {
            Some(tracking) => tracking.mark_read(view.raw, keys),
            None => view.raw.mark_read(keys),
        }
    }

    /// Prepare canonical merge entries under this transaction's admission.
    /// The borrowed batch cannot be submitted to a different transaction.
    fn merge_batch(&self, entries: usize) -> Result<merges::Batch<'_>> {
        merges::Batch::new(self.mutation_view(), entries)
    }

    /// Stage one index-operation queue operand as a blind merge. Its size is
    /// bounded by the database's operand limit when it is built, so it is not
    /// admitted again here.
    fn merge_disjoint_tokens<K: AsRef<[u8]>>(
        &self,
        key: K,
        tokens: Vec<u128>,
        bytes: Bytes,
    ) -> std::result::Result<(), slatedb::Error> {
        self.mutation_view()
            .raw
            .merge_disjoint_tokens(key, tokens, bytes)
    }
}

impl Mutation for DbTransaction {
    fn mutation_view(&self) -> View<'_> {
        View {
            raw: self,
            tracking: None,
            merges: None,
        }
    }
}
impl Mutation for Owned {
    fn mutation_view(&self) -> View<'_> {
        View {
            raw: &self.raw,
            tracking: self.tracking.as_deref(),
            merges: self.merges.as_deref(),
        }
    }
}
impl Mutation for View<'_> {
    fn mutation_view(&self) -> View<'_> {
        *self
    }
}

/// Writes one value outside any transaction, with [`Mutation::put`]'s check.
pub(crate) async fn put_unbatched(
    db: &slatedb::Db,
    key: &[u8],
    value: &[u8],
) -> std::result::Result<slatedb::WriteHandle, slatedb::Error> {
    validate_put(None, key, value)?;
    #[allow(clippy::disallowed_methods)]
    db.put(key, value).await
}

/// Rejects a full value a membership row could not take deltas on.
///
/// Membership deltas are staged without reading the stored row, so this keeps
/// those rows mergeable: a memtable holding both an undecodable base and a
/// delta for one key could not build its L0 table. Any other key returns after
/// classification, without touching the value. A budgeted transaction admits
/// the decode's allocation bound before decoding.
fn validate_put(
    tracking: Option<&read_tracking::Tracker>,
    key: &[u8],
    value: &[u8],
) -> std::result::Result<(), slatedb::Error> {
    let Some(row) = crate::merge_operator::MembershipRow::of(key) else {
        return Ok(());
    };
    let invalid = |error: Box<dyn std::error::Error + Send + Sync>| {
        slatedb::Error::invalid("full value for a merge-maintained row does not decode".to_owned())
            .with_source(error)
    };
    let _decode = tracking
        .map(|tracker| {
            let bound = row
                .base_allocation_bound(value)
                .map_err(|error| invalid(Box::new(error)))?;
            tracker.reserve(bound).map_err(storage_error)
        })
        .transpose()?;
    row.validate_base(value)
        .map_err(|error| invalid(Box::new(error)))
}

/// Private source marker survives SlateDB's required error type. Never infer
/// admission failure from an error message, kind, or an unrelated backend limit.
#[derive(Debug, thiserror::Error)]
#[error("transaction read state exceeds the query memory budget")]
struct AdmissionFailure;

fn storage_error(error: AdmissionFailure) -> slatedb::Error {
    slatedb::Error::unavailable("transaction read admission failed".to_owned())
        .with_source(Box::new(error))
}

pub(crate) fn is_admission_failure(error: &slatedb::Error) -> bool {
    let mut source = std::error::Error::source(error);
    while let Some(error) = source {
        if error.is::<AdmissionFailure>() {
            return true;
        }
        source = error.source();
    }
    false
}
