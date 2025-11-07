use crate::{
    helix_engine::{
        storage_core::{HelixGraphStorage, storage_methods::StorageMethods},
        traversal_core::{traversal_iter::RoTraversalIterator, traversal_value::TraversalValue},
        types::GraphError,
    },
    utils::label_hash::hash_label,
};

pub trait OutEdgesAdapter<'db, 'arena, 'txn, 's>:
    Iterator<Item = Result<TraversalValue<'arena>, GraphError>>
{
    /// Returns an iterator containing the edges that have an outgoing edge with the given label.
    ///
    /// Note that the `edge_label` cannot be empty and must be a valid, existing edge label.
    ///
    /// To provide safety, you cannot get all outgoing edges as it would be ambiguous as to what
    /// type that resulting edge would be.
    fn out_e(
        self,
        edge_label: &'s str,
    ) -> RoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    >;
}

impl<'db, 'arena, 'txn, 's, I: Iterator<Item = Result<TraversalValue<'arena>, GraphError>>>
    OutEdgesAdapter<'db, 'arena, 'txn, 's> for RoTraversalIterator<'db, 'arena, 'txn, I>
{
    #[inline]
    fn out_e(
        self,
        edge_label: &'s str,
    ) -> RoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    > {
        // iterate through the iterator and create a new iterator on the out edges
        let iter = self
            .inner
            .filter_map(move |item| {
                let edge_label_hash = hash_label(edge_label, None);

                let prefix = HelixGraphStorage::out_edge_key(
                    &match item {
                        Ok(item) => item.id(),
                        Err(_) => return None,
                    },
                    &edge_label_hash,
                );
                match self
                    .storage
                    .out_edges_db
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
