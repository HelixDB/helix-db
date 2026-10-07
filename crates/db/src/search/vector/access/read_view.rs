//! Explicit request-scoped read boundary for vector traversal.
//!
//! [`VectorReadView`] prevents HNSW code from selecting a raw database handle.
//! A write request supplies its transaction; a read request supplies the
//! interpreter-owned stable snapshot contract. Both delegate the narrow
//! SlateDB [`DbReadOps`] interface used by vector storage.

use slatedb::DbReadOps;

/// The only storage views accepted by request-driven vector search.
pub(crate) enum VectorReadView<'a, R> {
    /// Read-your-writes view owned by one write request.
    Transaction(crate::transaction::View<'a>),
    /// Stable view owned by one read request.
    Snapshot(&'a R),
}

impl<'a, R> VectorReadView<'a, R> {
    /// Binds vector traversal to the request's read/write transaction.
    pub(crate) fn transaction(transaction: &'a impl crate::transaction::Mutation) -> Self {
        Self::Transaction(transaction.mutation_view())
    }

    /// Binds vector traversal to the request's read-only snapshot contract.
    pub(crate) const fn snapshot(snapshot: &'a R) -> Self {
        Self::Snapshot(snapshot)
    }
}

// Forward the underlying future synchronously. An async wrapper would allocate
// before the provider can admit or reject the call. Override defaults too, since
// the trait's convenience methods otherwise introduce another boxed frame.
type Read<'a, T> = futures::future::BoxFuture<'a, Result<T, slatedb::Error>>;

impl<R> DbReadOps for VectorReadView<'_, R>
where
    R: DbReadOps + Send + Sync,
{
    fn get<'life0, 'async_trait, K>(
        &'life0 self,
        key: K,
    ) -> Read<'async_trait, Option<bytes::Bytes>>
    where
        'life0: 'async_trait,
        K: AsRef<[u8]> + Send + 'async_trait,
        Self: 'async_trait,
    {
        match self {
            Self::Transaction(transaction) => transaction.get(key),
            Self::Snapshot(snapshot) => snapshot.get(key),
        }
    }

    fn get_with_options<'life0, 'life1, 'async_trait, K>(
        &'life0 self,
        key: K,
        options: &'life1 slatedb::config::ReadOptions,
    ) -> Read<'async_trait, Option<bytes::Bytes>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        K: AsRef<[u8]> + Send + 'async_trait,
        Self: 'async_trait,
    {
        match self {
            Self::Transaction(transaction) => transaction.get_with_options(key, options),
            Self::Snapshot(snapshot) => snapshot.get_with_options(key, options),
        }
    }

    fn get_key_value<'life0, 'async_trait, K>(
        &'life0 self,
        key: K,
    ) -> Read<'async_trait, Option<slatedb::KeyValue>>
    where
        'life0: 'async_trait,
        K: AsRef<[u8]> + Send + 'async_trait,
        Self: 'async_trait,
    {
        match self {
            Self::Transaction(transaction) => transaction.get_key_value(key),
            Self::Snapshot(snapshot) => snapshot.get_key_value(key),
        }
    }

    fn get_key_value_with_options<'life0, 'life1, 'async_trait, K>(
        &'life0 self,
        key: K,
        options: &'life1 slatedb::config::ReadOptions,
    ) -> Read<'async_trait, Option<slatedb::KeyValue>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        K: AsRef<[u8]> + Send + 'async_trait,
        Self: 'async_trait,
    {
        match self {
            Self::Transaction(transaction) => transaction.get_key_value_with_options(key, options),
            Self::Snapshot(snapshot) => snapshot.get_key_value_with_options(key, options),
        }
    }

    fn multi_get<'life0, 'life1, 'async_trait, K>(
        &'life0 self,
        keys: &'life1 [K],
    ) -> Read<'async_trait, Vec<Option<bytes::Bytes>>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        K: AsRef<[u8]> + Send + Sync + 'async_trait,
        Self: 'async_trait,
    {
        match self {
            Self::Transaction(transaction) => transaction.multi_get(keys),
            Self::Snapshot(snapshot) => snapshot.multi_get(keys),
        }
    }

    fn multi_get_with_options<'life0, 'life1, 'life2, 'async_trait, K>(
        &'life0 self,
        keys: &'life1 [K],
        options: &'life2 slatedb::config::ReadOptions,
    ) -> Read<'async_trait, Vec<Option<bytes::Bytes>>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        K: AsRef<[u8]> + Send + Sync + 'async_trait,
        Self: 'async_trait,
    {
        match self {
            Self::Transaction(transaction) => transaction.multi_get_with_options(keys, options),
            Self::Snapshot(snapshot) => snapshot.multi_get_with_options(keys, options),
        }
    }

    fn scan<'life0, 'async_trait, T>(
        &'life0 self,
        range: T,
    ) -> Read<'async_trait, slatedb::DbIterator>
    where
        'life0: 'async_trait,
        T: slatedb::ByteRangeBounds + Send + 'async_trait,
        Self: 'async_trait,
    {
        match self {
            Self::Transaction(transaction) => transaction.scan(range),
            Self::Snapshot(snapshot) => snapshot.scan(range),
        }
    }

    fn scan_with_options<'life0, 'life1, 'async_trait, T>(
        &'life0 self,
        range: T,
        options: &'life1 slatedb::config::ScanOptions,
    ) -> Read<'async_trait, slatedb::DbIterator>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        T: slatedb::ByteRangeBounds + Send + 'async_trait,
        Self: 'async_trait,
    {
        match self {
            Self::Transaction(transaction) => transaction.scan_with_options(range, options),
            Self::Snapshot(snapshot) => snapshot.scan_with_options(range, options),
        }
    }

    fn scan_prefix<'life0, 'async_trait, P, T>(
        &'life0 self,
        prefix: P,
        subrange: T,
    ) -> Read<'async_trait, slatedb::DbIterator>
    where
        'life0: 'async_trait,
        P: AsRef<[u8]> + Send + 'async_trait,
        T: slatedb::ByteRangeBounds + Send + 'async_trait,
        Self: 'async_trait,
    {
        match self {
            Self::Transaction(transaction) => transaction.scan_prefix(prefix, subrange),
            Self::Snapshot(snapshot) => snapshot.scan_prefix(prefix, subrange),
        }
    }

    fn scan_prefix_with_options<'life0, 'life1, 'async_trait, P, T>(
        &'life0 self,
        prefix: P,
        subrange: T,
        options: &'life1 slatedb::config::ScanOptions,
    ) -> Read<'async_trait, slatedb::DbIterator>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        P: AsRef<[u8]> + Send + 'async_trait,
        T: slatedb::ByteRangeBounds + Send + 'async_trait,
        Self: 'async_trait,
    {
        match self {
            Self::Transaction(transaction) => {
                transaction.scan_prefix_with_options(prefix, subrange, options)
            }
            Self::Snapshot(snapshot) => {
                snapshot.scan_prefix_with_options(prefix, subrange, options)
            }
        }
    }
}

#[cfg(test)]
mod tests;
