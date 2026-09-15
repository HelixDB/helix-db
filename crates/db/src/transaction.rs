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

/// An owned request transaction. Only its owner has lifecycle authority.
pub(crate) struct Owned {
    raw: DbTransaction,
    tracking: Option<Box<read_tracking::Tracker>>,
    merges: Option<Box<merges::Tracker>>,
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
        })
    }

    /// Called only inside the finite commit owner, which survives cancellation.
    pub(crate) async fn commit(
        self,
    ) -> std::result::Result<Option<slatedb::WriteHandle>, slatedb::Error> {
        let Self {
            raw,
            tracking,
            merges,
        } = self;
        let result = raw.commit().await;
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
}

impl ReadSource for View<'_> {
    fn read_context(&self) -> ReadContext<'_> {
        ReadContext(*self)
    }
}

/// The native mutation contract preserves backend isolation and write behavior.
/// Checked disjoint merges deliberately retain their untracked validation reads.
pub(crate) trait Mutation: DbReadOps + Send + Sync + sealed::Sealed {
    fn mutation_view(&self) -> View<'_>;

    fn put<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        &self,
        key: K,
        value: V,
    ) -> std::result::Result<(), slatedb::Error> {
        self.mutation_view().raw.put(key, value)
    }

    fn put_bytes(&self, key: Bytes, value: Bytes) -> std::result::Result<(), slatedb::Error> {
        self.mutation_view().raw.put_bytes(key, value)
    }

    fn delete<K: AsRef<[u8]>>(&self, key: K) -> std::result::Result<(), slatedb::Error> {
        self.mutation_view().raw.delete(key)
    }

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
