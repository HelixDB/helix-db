use crate::{
    helix_engine::{
        storage_core::{HelixGraphStorage, storage_methods::StorageMethods},
        traversal_core::{traversal_iter::RoTraversalIterator, traversal_value::TraversalValue},
        types::GraphError,
    },
    utils::label_hash::hash_label,
};

pub trait InAdapter<'db, 'arena, 'txn, 's>:
    Iterator<Item = Result<TraversalValue<'arena>, GraphError>>
{
    /// Returns an iterator containing the nodes that have an incoming edge with the given label.
    ///
    /// Note that the `edge_label` cannot be empty and must be a valid, existing edge label.
    ///
    /// To provide safety, you cannot get all outgoing nodes as it would be ambiguous as to what
    /// type that resulting node would be.
    fn in_vec(
        self,
        edge_label: &'s str,
        get_vector_data: bool,
    ) -> RoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    >;

    fn in_node(
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
    InAdapter<'db, 'arena, 'txn, 's> for RoTraversalIterator<'db, 'arena, 'txn, I>
{
    #[inline]
    fn in_vec(
        self,
        edge_label: &'s str,
        get_vector_data: bool,
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

                match self.storage.in_edges_db.get_duplicates(self.txn, &prefix) {
                    Ok(Some(iter)) => Some(iter.filter_map(move |item| {
                        if let Ok((_, value)) = item {
                            let (_, item_id) = match HelixGraphStorage::unpack_adj_edge_data(value)
                            {
                                Ok(data) => data,
                                Err(e) => {
                                    println!("Error unpacking edge data: {e:?}");
                                    return Some(Err(e));
                                }
                            };
                            if get_vector_data {
                                if let Ok(vec) = self
                                    .storage
                                    .vectors
                                    .get_full_vector(self.txn, item_id, self.arena)
                                {
                                    return Some(Ok(TraversalValue::Vector(vec)));
                                }
                            } else if let Ok(Some(vec)) = self
                                .storage
                                .vectors
                                .get_vector_properties(self.txn, item_id, self.arena)
                            {
                                return Some(Ok(TraversalValue::Vector(vec)));
                            }
                            None
                        } else {
                            None
                        }
                    })),
                    Ok(None) => None,
                    Err(e) => {
                        println!("{} Error getting out edges: {:?}", line!(), e);
                        // return Err(e);
                        None
                    }
                }
            })
            .flatten();

        RoTraversalIterator {
            inner: iter,
            storage: self.storage,
            arena: self.arena,
            txn: self.txn,
        }
    }

    #[inline]
    fn in_node(
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
                match self.storage.in_edges_db.get_duplicates(self.txn, &prefix) {
                    Ok(Some(iter)) => Some(iter.filter_map(move |item| {
                        if let Ok((_, data)) = item {
                            let (_, item_id) = match HelixGraphStorage::unpack_adj_edge_data(data) {
                                Ok(data) => data,
                                Err(e) => {
                                    println!("Error unpacking edge data: {e:?}");
                                    return Some(Err(e));
                                }
                            };
                            if let Ok(node) = self.storage.get_node(self.txn, &item_id, self.arena)
                            {
                                return Some(Ok(TraversalValue::Node(node)));
                            }
                        }
                        None
                    })),
                    Ok(None) => None,
                    Err(e) => {
                        println!("{} Error getting out nodes: {:?}", line!(), e);
                        // return Err(e);
                        None
                    }
                }
            })
            .flatten();

        RoTraversalIterator {
            inner: iter,
            storage: self.storage,
            arena: self.arena,
            txn: self.txn,
        }
    }
}
