use crate::{
    helix_engine::{
        graph_core::{
            ops::tr_val::{Traversable, TraversalVal},
            traversal_iter::RoTraversalIterator,
        },
        storage_core::{storage_core::HelixGraphStorage, storage_methods::StorageMethods},
        types::GraphError,
    },
    protocol::label_hash::hash_label,
};
use heed3::{types::Bytes, RoTxn};
use std::sync::Arc;

pub struct InEdgesIterator<'a, T> {
    iter: heed3::RoIter<
        'a,
        Bytes,
        heed3::types::LazyDecode<Bytes>,
        heed3::iteration_method::MoveOnCurrentKeyDuplicates,
    >,
    storage: Arc<HelixGraphStorage>,
    txn: &'a T,
}

impl<'a> Iterator for InEdgesIterator<'a, RoTxn<'a>> {
    type Item = Result<TraversalVal, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(Ok((_, data))) = self.iter.next() {
            match data.decode() {
                Ok(data) => {
                    let (_, edge_id) = match HelixGraphStorage::unpack_adj_edge_data(&data) {
                        Ok(data) => data,
                        Err(e) => {
                            println!("Error unpacking edge data: {:?}", e);
                            return Some(Err(e));
                        }
                    };
                    if let Ok(edge) = self.storage.get_edge(self.txn, &edge_id) {
                        return Some(Ok(TraversalVal::Edge(edge)));
                    }
                }
                Err(e) => {
                    println!("Error decoding edge data: {:?}", e);
                    return Some(Err(GraphError::DecodeError(e.to_string())));
                }
            }
        }
        None
    }
}

pub trait InEdgesAdapter<'a, T>: Iterator<Item = Result<TraversalVal, GraphError>> {
    /// Returns an iterator containing the edges that have an incoming edge with the given label.
    ///
    /// Note that the `edge_label` cannot be empty and must be a valid, existing edge label.
    ///
    /// To provide safety, you cannot get all incoming edges as it would be ambiguous as to what
    /// type that resulting edge would be.  
    fn in_e(
        self,
        edge_label: &'a str,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>>;
}

impl<'a, I: Iterator<Item = Result<TraversalVal, GraphError>>> InEdgesAdapter<'a, RoTxn<'a>>
    for RoTraversalIterator<'a, I>
{
    #[inline]
    fn in_e(
        self,
        edge_label: &'a str,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>> {
        let db = Arc::clone(&self.storage);
        let storage = Arc::clone(&self.storage);
        let txn = self.txn;
        let iter = self
            .inner
            .filter_map(move |item| {
                let edge_label_hash = hash_label(edge_label, None);
                let prefix = HelixGraphStorage::in_edge_key(&item.unwrap().id(), &edge_label_hash);
                match db
                    .in_edges_db
                    .lazily_decode_data()
                    .get_duplicates(txn, &prefix)
                {
                    Ok(Some(iter)) => Some(InEdgesIterator {
                        iter,
                        storage: Arc::clone(&db),
                        txn,
                    }),
                    Ok(None) => None,
                    Err(e) => {
                        println!("Error getting in edges: {:?}", e);
                        // return Err(e);
                        None
                    }
                }
            })
            .flatten();

        RoTraversalIterator {
            inner: iter,
            storage,
            txn,
        }
    }
}
