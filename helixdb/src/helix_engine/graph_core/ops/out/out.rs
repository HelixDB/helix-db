use std::sync::Arc;

use heed3::{types::Bytes, RoTxn, RwTxn};

use crate::{
    helix_engine::{
        graph_core::traversal_iter::RoTraversalIterator,
        storage_core::{storage_core::HelixGraphStorage, storage_methods::StorageMethods},
        types::GraphError,
    },
    protocol::label_hash::hash_label,
};

use super::super::tr_val::{Traversable, TraversalVal};

pub struct OutNodesIterator<'a, T> {
    iter: heed3::RoIter<
        'a,
        Bytes,
        heed3::types::LazyDecode<Bytes>,
        heed3::iteration_method::MoveOnCurrentKeyDuplicates,
    >,
    storage: Arc<HelixGraphStorage>,
    txn: &'a T,
}

// implementing iterator for OutIterator
impl<'a> Iterator for OutNodesIterator<'a, RoTxn<'a>> {
    type Item = Result<TraversalVal, GraphError>;

    /// Returns the next outgoing node by decoding the edge id and then getting the edge and node
    fn next(&mut self) -> Option<Self::Item> {
        while let Some(Ok((_, data))) = self.iter.next() {
            let (node_id, _) =
                HelixGraphStorage::unpack_adj_edge_data(&data.decode().unwrap()).unwrap();
            if let Ok(node) = self.storage.get_node(self.txn, &node_id) {
                return Some(Ok(TraversalVal::Node(node)));
            }
        }
        None
    }
}
impl<'a> Iterator for OutNodesIterator<'a, RwTxn<'a>> {
    type Item = Result<TraversalVal, GraphError>;

    /// Returns the next outgoing node by decoding the edge id and then getting the edge and node
    fn next(&mut self) -> Option<Self::Item> {
        while let Some(Ok((_, data))) = self.iter.next() {
            let (node_id, _) =
                HelixGraphStorage::unpack_adj_edge_data(&data.decode().unwrap()).unwrap();
            if let Ok(node) = self.storage.get_node(self.txn, &node_id) {
                return Some(Ok(TraversalVal::Node(node)));
            }
        }
        None
    }
}

pub trait OutAdapter<'a, T>: Iterator<Item = Result<TraversalVal, GraphError>> + Sized {
    fn out(
        self,
        edge_label: &'a str,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>>;
    // where
    //     OutNodesIterator<'a, T>: std::iter::Iterator,
    //     T: 'a;
}

impl<'a, I: Iterator<Item = Result<TraversalVal, GraphError>> + 'a> OutAdapter<'a, RoTxn<'a>>
    for RoTraversalIterator<'a, I>
{
    fn out(
        self,
        edge_label: &'a str,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>> {
        // iterate through the iterator and create a new iterator on the out edges
        let db = Arc::clone(&self.storage);
        let storage = Arc::clone(&self.storage);
        let txn = self.txn;

        let iter = self
            .inner
            .filter_map(move |item| {
                let edge_label_hash = hash_label(edge_label, None);
                let prefix = HelixGraphStorage::out_edge_key(&item.unwrap().id(), &edge_label_hash);
                match db
                    .out_edges_db
                    .lazily_decode_data()
                    .get_duplicates(txn, &prefix)
                {
                    Ok(Some(iter)) => Some(OutNodesIterator {
                        iter,
                        storage: Arc::clone(&db),
                        txn,
                    }),
                    Ok(None) => None,
                    Err(e) => {
                        println!("Error getting out edges: {:?}", e);
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

// impl<'a, I: Iterator<Item = Result<TraversalVal, GraphError>> + 'a> OutAdapter<'a, RwTxn<'a>>
//     for RwTraversalIterator<'a, I>
// {
//     fn out(
//         self,
//         edge_label: &'a str,
//     ) -> OutNodes<
//         'a,
//         Self,
//         impl FnMut(Result<TraversalVal, GraphError>) -> OutNodesIterator<'a, RwTxn<'a>>,
//         RwTxn<'a>,
//     > {
//         {
//             // iterate through the iterator and create a new iterator on the out edges
//             let db = Arc::clone(&self.storage);
//             let storage = Arc::clone(&self.storage);
//             let txn = self.txn;
//             let iter = self
//                 .map(move |item| {
//                     let prefix = HelixGraphStorage::out_edge_key(item.unwrap().id(), "");
//                     let iter = db
//                         .out_edges_db
//                         .lazily_decode_data()
//                         .prefix_iter(txn, &prefix)
//                         .unwrap();

//                     OutNodesIterator {
//                         iter,
//                         storage: Arc::clone(&storage),
//                         txn,
//                         edge_label,
//                     }
//                 })
//                 .flatten();
//             OutNodes { iter }
//         }
//     }
// }
