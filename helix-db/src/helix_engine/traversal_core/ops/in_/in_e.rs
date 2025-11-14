use crate::{
    helix_engine::{
        storage_core::HelixGraphStorage,
        traversal_core::{traversal_iter::RoTraversalIterator, traversal_value::TraversalValue},
        types::GraphError,
    },
    utils::label_hash::hash_label,
};

#[cfg(feature = "lmdb")]
use crate::helix_engine::storage_core::storage_methods::StorageMethods;
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
                    Ok(Some(iter)) => {
                        let iter = iter.map(|item| match item {
                            Ok((_, data)) => match data.decode() {
                                Ok(data) => {
                                    let (edge_id, _) =
                                        match HelixGraphStorage::unpack_adj_edge_data(data) {
                                            Ok(data) => data,
                                            Err(e) => return Err(e),
                                        };
                                    match self.storage.get_edge(self.txn, &edge_id, self.arena) {
                                        Ok(edge) => Ok(TraversalValue::Edge(edge)),
                                        Err(e) => Err(e),
                                    }
                                }
                                Err(e) => Err(GraphError::DecodeError(e.to_string())),
                            },
                            Err(e) => Err(e.into()),
                        });
                        Some(iter)
                    }
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
                match item {
                    Ok(item) => {
                        let prefix =
                            HelixGraphStorage::in_edge_key_prefix(item.id(), &edge_label_hash);
                        let prefix_vec = prefix.to_vec();

                        let edge_iter = self
                            .txn
                            .prefix_iterator_cf(&self.storage.in_edges_db, &prefix_vec)
                            .filter_map(move |result| {
                                match result {
                                    Ok((key, value)) => {
                                        // Manual prefix check for RocksDB
                                        if !key.starts_with(&prefix_vec) {
                                            return None;
                                        }

                                        // Extract edge_id from value (16 bytes)
                                        let edge_id = match value.as_ref().try_into() {
                                            Ok(bytes) => u128::from_be_bytes(bytes),
                                            Err(_) => {
                                                println!("Error: value is not 16 bytes");
                                                return Some(Err(GraphError::SliceLengthError));
                                            }
                                        };

                                        // Get the full edge object
                                        match self.storage.get_edge(self.txn, edge_id, self.arena) {
                                            Ok(edge) => Some(Ok(TraversalValue::Edge(edge))),
                                            Err(e) => {
                                                println!("Error getting edge {edge_id}: {e:?}");
                                                None
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        println!("{} Error iterating in edges: {:?}", line!(), e);
                                        None
                                    }
                                }
                            })
                            .collect::<Vec<_>>();

                        Some(edge_iter.into_iter())
                    }
                    Err(e) => {
                        println!("{} Error getting in edges: {:?}", line!(), e);
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
