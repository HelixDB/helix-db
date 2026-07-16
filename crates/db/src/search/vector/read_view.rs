//! Explicit request-scoped read boundary for vector traversal.
//!
//! [`VectorReadView`] prevents HNSW code from selecting a raw database handle.
//! A write request supplies its transaction; a read request supplies the
//! interpreter-owned stable snapshot contract. Both delegate the narrow
//! SlateDB [`DbReadOps`] interface used by vector storage.

use slatedb::{DbReadOps, DbTransaction};

/// The only storage views accepted by request-driven vector search.
pub(crate) enum VectorReadView<'a, R> {
    /// Read-your-writes view owned by one write request.
    Transaction(&'a DbTransaction),
    /// Stable view owned by one read request.
    Snapshot(&'a R),
}

impl<'a, R> VectorReadView<'a, R> {
    /// Binds vector traversal to the request's read/write transaction.
    pub(crate) const fn transaction(transaction: &'a DbTransaction) -> Self {
        Self::Transaction(transaction)
    }

    /// Binds vector traversal to the request's read-only snapshot contract.
    pub(crate) const fn snapshot(snapshot: &'a R) -> Self {
        Self::Snapshot(snapshot)
    }
}

#[async_trait::async_trait]
impl<R> DbReadOps for VectorReadView<'_, R>
where
    R: DbReadOps + Send + Sync,
{
    async fn get_with_options<K: AsRef<[u8]> + Send>(
        &self,
        key: K,
        options: &slatedb::config::ReadOptions,
    ) -> std::result::Result<Option<bytes::Bytes>, slatedb::Error> {
        match self {
            Self::Transaction(transaction) => transaction.get_with_options(key, options).await,
            Self::Snapshot(snapshot) => snapshot.get_with_options(key, options).await,
        }
    }

    async fn get_key_value_with_options<K: AsRef<[u8]> + Send>(
        &self,
        key: K,
        options: &slatedb::config::ReadOptions,
    ) -> std::result::Result<Option<slatedb::KeyValue>, slatedb::Error> {
        match self {
            Self::Transaction(transaction) => {
                transaction.get_key_value_with_options(key, options).await
            }
            Self::Snapshot(snapshot) => snapshot.get_key_value_with_options(key, options).await,
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
            Self::Transaction(transaction) => {
                transaction.multi_get_with_options(keys, options).await
            }
            Self::Snapshot(snapshot) => snapshot.multi_get_with_options(keys, options).await,
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
            Self::Transaction(transaction) => transaction.scan_with_options(range, options).await,
            Self::Snapshot(snapshot) => snapshot.scan_with_options(range, options).await,
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
            Self::Transaction(transaction) => {
                transaction
                    .scan_prefix_with_options(prefix, subrange, options)
                    .await
            }
            Self::Snapshot(snapshot) => {
                snapshot
                    .scan_prefix_with_options(prefix, subrange, options)
                    .await
            }
        }
    }
}
