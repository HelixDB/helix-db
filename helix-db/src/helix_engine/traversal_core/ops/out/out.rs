#[cfg(feature = "slate")]
use crate::helix_engine::traversal_core::traversal_iter::AsyncRoTraversalIterator;
use crate::{
    helix_engine::{
        storage_core::HelixGraphStorage,
        storage_core::storage_methods::StorageMethods,
        traversal_core::{traversal_iter::RoTraversalIterator, traversal_value::TraversalValue},
        types::GraphError,
    },
    utils::label_hash::hash_label,
};

pub trait OutAdapter<'db, 'arena, 'txn, 's>:
    Iterator<Item = Result<TraversalValue<'arena>, GraphError>>
{
    /// Returns an iterator containing the nodes that have an outgoing edge with the given label.
    ///
    /// Note that the `edge_label` cannot be empty and must be a valid, existing edge label.
    ///
    /// To provide safety, you cannot get all outgoing nodes as it would be ambiguous as to what
    /// type that resulting node would be.
    fn out_vec(
        self,
        edge_label: &'s str,
        get_vector_data: bool,
    ) -> RoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    >;

    fn out_node(
        self,
        edge_label: &'s str,
    ) -> RoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    >;
}

// LMDB Implementation
#[cfg(feature = "lmdb")]
impl<'db, 'arena, 'txn, 's, I: Iterator<Item = Result<TraversalValue<'arena>, GraphError>>>
    OutAdapter<'db, 'arena, 'txn, 's> for RoTraversalIterator<'db, 'arena, 'txn, I>
{
    #[inline]
    fn out_vec(
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
                let prefix = HelixGraphStorage::out_edge_key(
                    match item {
                        Ok(item) => item.id(),
                        Err(_) => return None,
                    },
                    &edge_label_hash,
                );

                match self.storage.out_edges_db.get_duplicates(self.txn, &prefix) {
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
                                return Some(Ok(TraversalValue::VectorNodeWithoutVectorData(vec)));
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
    fn out_node(
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
                let prefix = HelixGraphStorage::out_edge_key(
                    match item {
                        Ok(item) => item.id(),
                        Err(_) => return None,
                    },
                    &edge_label_hash,
                );
                match self.storage.out_edges_db.get_duplicates(self.txn, &prefix) {
                    Ok(Some(iter)) => Some(iter.filter_map(move |item| {
                        if let Ok((_, data)) = item {
                            let (_, item_id) = match HelixGraphStorage::unpack_adj_edge_data(data) {
                                Ok(data) => data,
                                Err(e) => {
                                    println!("Error unpacking edge data: {e:?}");
                                    return Some(Err(e));
                                }
                            };
                            if let Ok(node) = self.storage.get_node(self.txn, item_id, self.arena) {
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

// RocksDB Implementation
#[cfg(feature = "rocks")]
impl<'db, 'arena, 'txn, 's, I: Iterator<Item = Result<TraversalValue<'arena>, GraphError>>>
    OutAdapter<'db, 'arena, 'txn, 's> for RoTraversalIterator<'db, 'arena, 'txn, I>
{
    #[inline]
    fn out_vec(
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
                let from_node_id = match item {
                    Ok(item) => item.id(),
                    Err(_) => return None,
                };
                let edge_label_hash = hash_label(edge_label, None);
                let prefix = HelixGraphStorage::out_edge_key_prefix(from_node_id, &edge_label_hash);

                let iter = self
                    .txn
                    .prefix_iterator_cf(&self.storage.cf_out_edges(), prefix);

                Some(iter.filter_map(move |result| {
                    match result {
                        Ok((key, _)) => {
                            // Manual prefix check for RocksDB
                            if !key.starts_with(&prefix) {
                                return None;
                            }

                            // Unpack key to get to_node
                            let (_, _, item_id, _) =
                                match HelixGraphStorage::unpack_adj_edge_key(key.as_ref()) {
                                    Ok(data) => data,
                                    Err(e) => {
                                        println!("Error unpacking edge key: {e:?}");
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
                                return Some(Ok(TraversalValue::VectorNodeWithoutVectorData(vec)));
                            }
                            None
                        }
                        Err(e) => {
                            println!("{} Error iterating out edges: {:?}", line!(), e);
                            Some(Err(GraphError::from(e)))
                        }
                    }
                }))
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
    fn out_node(
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
                let from_node_id = match item {
                    Ok(item) => item.id(),
                    Err(_) => return None,
                };
                let edge_label_hash = hash_label(edge_label, None);
                let prefix = HelixGraphStorage::out_edge_key_prefix(from_node_id, &edge_label_hash);

                let iter = self
                    .txn
                    .prefix_iterator_cf(&self.storage.cf_out_edges(), prefix);

                Some(iter.filter_map(move |result| {
                    match result {
                        Ok((key, _value)) => {
                            // Manual prefix check for RocksDB
                            if !key.starts_with(&prefix) {
                                return None;
                            }

                            // Unpack key to get to_node
                            let (_, _, item_id, _) =
                                match HelixGraphStorage::unpack_adj_edge_key(key.as_ref()) {
                                    Ok(data) => data,
                                    Err(e) => {
                                        println!("Error unpacking edge key: {e:?}");
                                        return Some(Err(e));
                                    }
                                };

                            if let Ok(node) = self.storage.get_node(self.txn, item_id, self.arena) {
                                return Some(Ok(TraversalValue::Node(node)));
                            }
                            None
                        }
                        Err(e) => {
                            println!("{} Error iterating out nodes: {:?}", line!(), e);
                            Some(Err(GraphError::from(e)))
                        }
                    }
                }))
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

#[cfg(feature = "slate")]
use futures::Stream;

#[cfg(feature = "slate")]
pub trait AsyncOutAdapter<'db, 'arena, 'txn, 's>: Sized {
    fn out_vec(
        self,
        edge_label: &'s str,
        get_vector_data: bool,
    ) -> AsyncRoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Stream<Item = Result<TraversalValue<'arena>, GraphError>>,
    >;

    fn out_node(
        self,
        edge_label: &'s str,
    ) -> AsyncRoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Stream<Item = Result<TraversalValue<'arena>, GraphError>>,
    >;
}

#[cfg(feature = "slate")]
impl<'db, 'arena, 'txn, 's, S> AsyncOutAdapter<'db, 'arena, 'txn, 's>
    for AsyncRoTraversalIterator<'db, 'arena, 'txn, S>
where
    S: Stream<Item = Result<TraversalValue<'arena>, GraphError>>,
{
    fn out_vec(
        self,
        edge_label: &'s str,
        get_vector_data: bool,
    ) -> AsyncRoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Stream<Item = Result<TraversalValue<'arena>, GraphError>>,
    > {
        use crate::helix_engine::{
            slate_utils::SlateUtils, storage_core::DIRECTION_LABEL_PREFIX_LEN,
        };
        use futures::StreamExt;

        let stream = async_stream::try_stream! {
            let edge_label_hash = hash_label(edge_label, None);
            let mut inner = Box::pin(self.inner);

            while let Some(item) = inner.next().await {
                let item = item?;
                let node_id = item.id();

                let prefix = HelixGraphStorage::out_edge_key_prefix(node_id, &edge_label_hash);
                let mut iter = self.txn.prefix_iter::<DIRECTION_LABEL_PREFIX_LEN>(&prefix).await?;

                while let Some(kv) = iter.next().await? {
                    let key = kv.key;
                    let (_, _, to_node_id, _) = HelixGraphStorage::unpack_adj_edge_key(&key)?;

                    if get_vector_data {
                        // TODO: implement get_full_vector for slate
                        todo!("get_full_vector not yet implemented for slate");
                    } else {
                        // TODO: implement get_vector_properties for slate
                        todo!("get_vector_properties not yet implemented for slate");
                    }
                }
            }
        };

        AsyncRoTraversalIterator {
            storage: self.storage,
            arena: self.arena,
            txn: self.txn,
            inner: stream,
        }
    }

    fn out_node(
        self,
        edge_label: &'s str,
    ) -> AsyncRoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Stream<Item = Result<TraversalValue<'arena>, GraphError>>,
    > {
        use crate::helix_engine::{
            slate_utils::SlateUtils, storage_core::DIRECTION_LABEL_PREFIX_LEN,
        };
        use futures::StreamExt;

        let stream = async_stream::try_stream! {
            let edge_label_hash = hash_label(edge_label, None);
            let mut inner = Box::pin(self.inner);

            while let Some(item) = inner.next().await {
                let item = item?;
                let node_id = item.id();

                let prefix = HelixGraphStorage::out_edge_key_prefix(node_id, &edge_label_hash);
                let mut iter = self.txn.prefix_iter::<DIRECTION_LABEL_PREFIX_LEN>(&prefix).await?;

                while let Some(kv) = iter.next().await? {
                    let key = kv.key;
                    let (_, _, to_node_id, _) = HelixGraphStorage::unpack_adj_edge_key(&key)?;

                    let node = self.storage.get_node(self.txn, to_node_id, self.arena).await?;
                    yield TraversalValue::Node(node);
                }
            }
        };

        AsyncRoTraversalIterator {
            storage: self.storage,
            arena: self.arena,
            txn: self.txn,
            inner: stream,
        }
    }
}
