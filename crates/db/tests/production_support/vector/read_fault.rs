//! Delegating SlateDB read faults for production-boundary coverage.
//!
//! [`FaultingRead`] wraps a real [`slatedb::DbReadOps`] implementation and
//! fails exactly one selected operation class. Vector storage, cache, and
//! search contracts use it to prove that backend failures cross their typed
//! boundaries unchanged without adding fault logic to production builds.

use bytes::Bytes;
use slatedb::config::{ReadOptions, ScanOptions};
use slatedb::{ByteRangeBounds, DbIterator, DbReadOps, KeyValue};

/// Read operation class rejected by one [`FaultingRead`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReadFault {
    /// Reject single-value reads.
    Point,
    /// Reject ordered batch reads.
    MultiGet,
    /// Reject range and prefix scans.
    Scan,
}

/// Real read backend with one deterministic operation-class failure.
pub(crate) struct FaultingRead<'a, R> {
    inner: &'a R,
    fault: ReadFault,
}

impl<'a, R> FaultingRead<'a, R> {
    /// Wraps `inner` and rejects every operation in `fault`.
    pub(crate) const fn new(inner: &'a R, fault: ReadFault) -> Self {
        Self { inner, fault }
    }

    /// Builds the deterministic backend error returned by the selected fault arm.
    fn error(&self) -> slatedb::Error {
        slatedb::Error::unavailable(format!(
            "injected vector production read fault: {:?}",
            self.fault
        ))
    }
}

#[async_trait::async_trait]
impl<R> DbReadOps for FaultingRead<'_, R>
where
    R: DbReadOps + Send + Sync,
{
    async fn get_with_options<K: AsRef<[u8]> + Send>(
        &self,
        key: K,
        options: &ReadOptions,
    ) -> Result<Option<Bytes>, slatedb::Error> {
        if self.fault == ReadFault::Point {
            return Err(self.error());
        }
        self.inner.get_with_options(key, options).await
    }

    async fn multi_get_with_options<K>(
        &self,
        keys: &[K],
        options: &ReadOptions,
    ) -> Result<Vec<Option<Bytes>>, slatedb::Error>
    where
        K: AsRef<[u8]> + Send + Sync,
    {
        if self.fault == ReadFault::MultiGet {
            return Err(self.error());
        }
        self.inner.multi_get_with_options(keys, options).await
    }

    async fn get_key_value_with_options<K: AsRef<[u8]> + Send>(
        &self,
        key: K,
        options: &ReadOptions,
    ) -> Result<Option<KeyValue>, slatedb::Error> {
        self.inner.get_key_value_with_options(key, options).await
    }

    async fn scan_with_options<T>(
        &self,
        range: T,
        options: &ScanOptions,
    ) -> Result<DbIterator, slatedb::Error>
    where
        T: ByteRangeBounds + Send,
    {
        if self.fault == ReadFault::Scan {
            return Err(self.error());
        }
        self.inner.scan_with_options(range, options).await
    }
}
