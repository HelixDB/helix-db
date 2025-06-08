use crate::{
    helix_engine::{
        graph_core::{
            ops::{
                source::add_e::EdgeType,
                tr_val::{Traversable, TraversalVal},
            },
            traversal_iter::RoTraversalIterator,
        },
        storage_core::{storage_core::HelixGraphStorage, storage_methods::StorageMethods},
        types::GraphError,
    },
    protocol::label_hash::hash_label,
};
use heed3::{types::Bytes, RoTxn};
use std::sync::Arc;

pub struct InNodesIterator<'a, T> {
    pub iter: heed3::RoIter<
        'a,
        Bytes,
        heed3::types::LazyDecode<Bytes>,
        heed3::iteration_method::MoveOnCurrentKeyDuplicates,
    >,
    pub storage: Arc<HelixGraphStorage>,
    pub txn: &'a T,
    pub edge_type: &'a EdgeType,
}

impl<'a> Iterator for InNodesIterator<'a, RoTxn<'a>> {
    type Item = Result<TraversalVal, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(Ok((_, data))) = self.iter.next() {
            match data.decode() {
                Ok(data) => {
                    let (node_id, _) = match HelixGraphStorage::unpack_adj_edge_data(&data) {
                        Ok(data) => data,
                        Err(e) => {
                            println!("Error unpacking edge data: {:?}", e);
                            return Some(Err(e));
                        }
                    };
                    match self.edge_type {
                        EdgeType::Node => {
                            if let Ok(node) = self.storage.get_node(self.txn, &node_id) {
                                return Some(Ok(TraversalVal::Node(node)));
                            }
                        }
                        EdgeType::Vec => {
                            if let Ok(vector) = self.storage.get_vector(self.txn, &node_id) {
                                return Some(Ok(TraversalVal::Vector(vector)));
                            }
                        }
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

pub trait InAdapter<'a, T>: Iterator<Item = Result<TraversalVal, GraphError>> {
    /// Returns an iterator containing the nodes that have an incoming edge with the given label.
    ///
    /// Note that the `edge_label` cannot be empty and must be a valid, existing edge label.
    ///
    /// To provide safety, you cannot get all incoming nodes as it would be ambiguous as to what
    /// type that resulting node would be.
    fn in_(
        self,
        edge_label: &'a str,
        edge_type: &'a EdgeType,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>>;
}

impl<'a, I: Iterator<Item = Result<TraversalVal, GraphError>> + 'a> InAdapter<'a, RoTxn<'a>>
    for RoTraversalIterator<'a, I>
{
    #[inline]
    fn in_(
        self,
        edge_label: &'a str,
        edge_type: &'a EdgeType,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>> {
        let db = Arc::clone(&self.storage);
        let storage = Arc::clone(&self.storage);
        let txn = self.txn;
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
                match db
                    .in_edges_db
                    .lazily_decode_data()
                    .get_duplicates(txn, &prefix)
                {
                    Ok(Some(iter)) => Some(InNodesIterator {
                        iter,
                        storage: Arc::clone(&db),
                        txn,
                        edge_type,
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
