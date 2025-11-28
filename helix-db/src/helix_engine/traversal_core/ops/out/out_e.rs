#[cfg(feature = "slate")]
use crate::helix_engine::traversal_core::traversal_iter::AsyncRoTraversalIterator;
use crate::{
    helix_engine::{
        storage_core::HelixGraphStorage,
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

#[cfg(feature = "lmdb")]
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
                    match item {
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
                                    match self.storage.get_edge(self.txn, edge_id, self.arena) {
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
                match item {
                    Ok(item) => {
                        use crate::helix_engine::rocks_utils::RocksUtils;

                        let prefix =
                            HelixGraphStorage::out_edge_key_prefix(item.id(), &edge_label_hash);

                        let mut iter = self
                            .txn
                            .raw_prefix_iter(&self.storage.cf_out_edges(), &prefix);

                        let edge_iter = std::iter::from_fn(move || {
                            while let Some(key) = iter.key() {
                                let (_, _, _, edge_id) =
                                    HelixGraphStorage::unpack_adj_edge_key(key).unwrap();

                                // Get the full edge object
                                match self.storage.get_edge(self.txn, edge_id, self.arena) {
                                    Ok(edge) => {
                                        iter.next();
                                        return Some(Ok(TraversalValue::Edge(edge)));
                                    }
                                    Err(e) => {
                                        iter.next();
                                        println!("Error getting edge {edge_id}: {e:?}");
                                        continue;
                                    }
                                }
                            }
                            None
                        });

                        Some(edge_iter)
                    }
                    Err(e) => {
                        println!("{} Error getting out edges: {:?}", line!(), e);
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

#[cfg(feature = "slate")]
use futures::Stream;

#[cfg(feature = "slate")]
pub trait AsyncOutEdgesAdapter<'db, 'arena, 'txn, 's>: Sized {
    fn out_e(
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
impl<'db, 'arena, 'txn, 's, S> AsyncOutEdgesAdapter<'db, 'arena, 'txn, 's>
    for AsyncRoTraversalIterator<'db, 'arena, 'txn, S>
where
    S: Stream<Item = Result<TraversalValue<'arena>, GraphError>>,
{
    fn out_e(
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
                    let (_, _, _, edge_id) = HelixGraphStorage::unpack_adj_edge_key(&key)?;

                    let edge = self.storage.get_edge(self.txn, edge_id, self.arena).await?;
                    yield TraversalValue::Edge(edge);
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
