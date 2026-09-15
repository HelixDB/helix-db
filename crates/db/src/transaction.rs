//! Borrowed mutation authority and request-owned serializable read admission.
//!
//! Helpers receive [`Mutation`], which exposes reads and writes but cannot
//! commit, roll back, or extract the underlying transaction. Native callers
//! retain their existing SlateDB transaction; request execution owns [`Owned`].
//! Its ledger must remain inside the database's finite commit-completion task
//! until the backend finishes. No persisted representation is changed here.

use bytes::Bytes;
use slatedb::{DbReadOps, DbTransaction};

use crate::error::{HelixDbError, Result};
use crate::query_resources;

mod call;
mod read_tracking;
mod reads;
#[cfg(test)]
mod tests;

/// An owned request transaction. Only its owner has lifecycle authority.
pub(crate) struct Owned {
    raw: DbTransaction,
    tracking: Option<Box<read_tracking::Tracker>>,
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
        let raw = db
            .begin(slatedb::IsolationLevel::SerializableSnapshot)
            .await?;
        Ok(Self { raw, tracking })
    }

    /// Called only inside the finite commit owner, which survives cancellation.
    pub(crate) async fn commit(
        self,
    ) -> std::result::Result<Option<slatedb::WriteHandle>, slatedb::Error> {
        let Self { raw, tracking } = self;
        let result = raw.commit().await;
        drop(tracking);
        result
    }
}

/// A closed borrowed view; neither field nor lifecycle authority escapes it.
#[derive(Clone, Copy)]
pub(crate) struct View<'a> {
    raw: &'a DbTransaction,
    tracking: Option<&'a read_tracking::Tracker>,
}

mod sealed {
    pub trait Sealed {}
}
impl sealed::Sealed for DbTransaction {}
impl sealed::Sealed for Owned {}
impl sealed::Sealed for View<'_> {}

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

    fn merge_disjoint_checked_batch<M>(
        &self,
        merges: M,
    ) -> impl std::future::Future<Output = std::result::Result<(), slatedb::Error>> + Send
    where
        M: IntoIterator<Item = slatedb::DisjointMergeBatchEntry> + Send,
        M::IntoIter: Send,
    {
        async move {
            self.mutation_view()
                .raw
                .merge_disjoint_checked_batch(merges)
                .await
        }
    }
}

impl Mutation for DbTransaction {
    fn mutation_view(&self) -> View<'_> {
        View {
            raw: self,
            tracking: None,
        }
    }
}
impl Mutation for Owned {
    fn mutation_view(&self) -> View<'_> {
        View {
            raw: &self.raw,
            tracking: self.tracking.as_deref(),
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
