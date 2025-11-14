use crate::{
    helix_engine::{
        storage_core::{HelixGraphStorage, storage_methods::StorageMethods},
        traversal_core::{traversal_iter::RoTraversalIterator, traversal_value::TraversalValue},
        types::GraphError,
    },
    utils::label_hash::hash_label,
};

#[cfg(feature = "lmdb")]
use heed3::{RoTxn, types::Bytes};

#[cfg(feature = "lmdb")]
pub struct InEdgesIterator<'db, 'arena, 'txn>
where
    'db: 'arena,
    'arena: 'txn,
{
    pub storage: &'db HelixGraphStorage<'db>,
    pub arena: &'arena bumpalo::Bump,
    pub txn: &'txn RoTxn<'db>,
    pub iter: heed3::RoIter<
        'txn,
        Bytes,
        heed3::types::LazyDecode<Bytes>,
        heed3::iteration_method::MoveOnCurrentKeyDuplicates,
    >,
}

#[cfg(feature = "lmdb")]
impl<'db, 'arena, 'txn> Iterator for InEdgesIterator<'db, 'arena, 'txn> {
    type Item = Result<TraversalValue<'arena>, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(Ok((_, data))) = self.iter.next() {
            match data.decode() {
                Ok(data) => {
                    let (edge_id, _) = match HelixGraphStorage::unpack_adj_edge_data(data) {
                        Ok(data) => data,
                        Err(e) => {
                            println!("Error unpacking edge data: {e:?}");
                            return Some(Err(e));
                        }
                    };
                    if let Ok(edge) = self.storage.get_edge(self.txn, &edge_id, self.arena) {
                        return Some(Ok(TraversalValue::Edge(edge)));
                    }
                }
                Err(e) => {
                    println!("Error decoding edge data: {e:?}");
                    return Some(Err(GraphError::DecodeError(e.to_string())));
                }
            }
        }
        None
    }
}

#[cfg(feature = "rocks")]
use crate::helix_engine::traversal_core::RTxn;

#[cfg(feature = "rocks")]
pub struct InEdgesIterator<'db, 'arena, 'txn>
where
    'db: 'arena,
    'arena: 'txn,
{
    pub storage: &'db HelixGraphStorage<'db>,
    pub arena: &'arena bumpalo::Bump,
    pub txn: &'txn RTxn<'db>,
    pub iter: rocksdb::DBIteratorWithThreadMode<
        'txn,
        rocksdb::Transaction<'db, rocksdb::TransactionDB>,
    >,
    pub prefix: Vec<u8>,
}

#[cfg(feature = "rocks")]
impl<'db, 'arena, 'txn> Iterator for InEdgesIterator<'db, 'arena, 'txn> {
    type Item = Result<TraversalValue<'arena>, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(result) = self.iter.next() {
            let (key, value) = match result {
                Ok(kv) => kv,
                Err(e) => return Some(Err(GraphError::from(e))),
            };

            // Manual prefix check for RocksDB
            if !key.starts_with(&self.prefix) {
                return None;
            }

            let edge_id = match HelixGraphStorage::unpack_adj_edge_data(value.as_ref()) {
                Ok(id) => id,
                Err(e) => {
                    println!("Error unpacking edge data: {e:?}");
                    return Some(Err(e));
                }
            };

            match self.storage.get_edge(self.txn, edge_id, self.arena) {
                Ok(edge) => return Some(Ok(TraversalValue::Edge(edge))),
                Err(e) => return Some(Err(e)),
            }
        }
        None
    }
}

pub trait InEdgesAdapter<'db, 'arena, 'txn, 's, I>:
    Iterator<Item = Result<TraversalValue<'arena>, GraphError>>
{
    /// Returns an iterator containing the edges that have an incoming edge with the given label.
    ///
    /// Note that the `edge_label` cannot be empty and must be a valid, existing edge label.
    ///
    /// To provide safety, you cannot get all incoming edges as it would be ambiguous as to what
    /// type that resulting edge would be.
    fn in_e(
        self,
        edge_label: &'s str,
    ) -> RoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    >;
}

#[cfg(feature = "lmdb")]
impl<'db, 'arena, 'txn, 's, I: Iterator<Item = Result<TraversalValue<'arena>, GraphError>>>
    InEdgesAdapter<'db, 'arena, 'txn, 's, I> for RoTraversalIterator<'db, 'arena, 'txn, I>
{
    #[inline]
    fn in_e(
        self,
        edge_label: &'s str,
    ) -> RoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    > {
        let iter = self
            .inner
            .filter_map(move |item| {
                let edge_label_hash = hash_label(edge_label, None);

                let prefix = HelixGraphStorage::in_edge_key(
                    &match item {
                        Ok(item) => item.id(),
                        Err(_) => return None,
                    },
                    &edge_label_hash,
                );
                match self
                    .storage
                    .in_edges_db
                    .lazily_decode_data()
                    .get_duplicates(self.txn, &prefix)
                {
                    Ok(Some(iter)) => Some(InEdgesIterator {
                        iter,
                        storage: self.storage,
                        arena: self.arena,
                        txn: self.txn,
                    }),
                    Ok(None) => None,
                    Err(e) => {
                        println!("Error getting in edges: {e:?}");
                        // return Err(e);
                        None
                    }
                }
            })
            .flatten();

        RoTraversalIterator {
            storage: self.storage,
            arena: self.arena,
            txn: self.txn,
            inner: iter,
        }
    }
}

#[cfg(feature = "rocks")]
impl<'db, 'arena, 'txn, 's, I: Iterator<Item = Result<TraversalValue<'arena>, GraphError>>>
    InEdgesAdapter<'db, 'arena, 'txn, 's, I> for RoTraversalIterator<'db, 'arena, 'txn, I>
{
    #[inline]
    fn in_e(
        self,
        edge_label: &'s str,
    ) -> RoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    > {
        let iter = self
            .inner
            .filter_map(move |item| {
                let edge_label_hash = hash_label(edge_label, None);

                let node_id = match item {
                    Ok(item) => item.id(),
                    Err(_) => return None,
                };

                // Create prefix: to_node(16) | label(4)
                let mut prefix = Vec::with_capacity(20);
                prefix.extend_from_slice(&node_id.to_be_bytes());
                prefix.extend_from_slice(&edge_label_hash);

                let iter = self
                    .txn
                    .prefix_iterator_cf(&self.storage.in_edges_db, &prefix);

                Some(InEdgesIterator {
                    iter,
                    storage: self.storage,
                    arena: self.arena,
                    txn: self.txn,
                    prefix,
                })
            })
            .flatten();

        RoTraversalIterator {
            storage: self.storage,
            arena: self.arena,
            txn: self.txn,
            inner: iter,
        }
    }
}
