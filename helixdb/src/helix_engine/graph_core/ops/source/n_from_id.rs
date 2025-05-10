use std::{iter::Once, sync::Arc};

use heed3::{
    RoTxn, RwTxn,
};

use crate::{
    helix_engine::{
        graph_core::traversal_iter::RoTraversalIterator,
        storage_core::{storage_core::HelixGraphStorage, storage_methods::StorageMethods},
        types::GraphError,
    },
    protocol::items::Node,
};

use super::super::tr_val::TraversalVal;

pub struct NFromId<'a, T> {
    iter: Once<Result<TraversalVal, GraphError>>, // Use Once instead of Empty so we get exactly one item
    storage: Arc<HelixGraphStorage>,
    txn: &'a T,
    id: &'a u128,
}

impl<'a> Iterator for NFromId<'a, RoTxn<'a>> {
    type Item = Result<TraversalVal, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|_| {
            let node: Node = match self.storage.get_node(self.txn, self.id) {
                Ok(node) => node,
                Err(e) => return Err(e),
            };
            Ok(TraversalVal::Node(node))
        })
    }
}

impl<'a> Iterator for NFromId<'a, RwTxn<'a>> {
    type Item = Result<TraversalVal, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|_| {
            let node: Node = match self.storage.get_node(self.txn, self.id) {
                Ok(node) => node,
                Err(e) => return Err(e),
            };
            Ok(TraversalVal::Node(node))
        })
    }
}

pub trait NFromIdAdapter<'a>: Iterator<Item = Result<TraversalVal, GraphError>> + Sized {
    type OutputIter: Iterator<Item = Result<TraversalVal, GraphError>>;

    fn n_from_id(self, id: &'a u128) -> Self::OutputIter;
}

impl<'a, I: Iterator<Item = Result<TraversalVal, GraphError>>> NFromIdAdapter<'a>
    for RoTraversalIterator<'a, I>
{
    type OutputIter = RoTraversalIterator<'a, NFromId<'a, RoTxn<'a>>>;

    fn n_from_id(self, id: &'a u128) -> Self::OutputIter {
        let n_from_id = NFromId {
            iter: std::iter::once(Ok(TraversalVal::Empty)),
            storage: Arc::clone(&self.storage),
            txn: self.txn,
            id,
        };

        RoTraversalIterator {
            inner: n_from_id,
            storage: self.storage,
            txn: self.txn,
        }
    }
}

// impl<'a, I: Iterator<Item = Result<TraversalVal, GraphError>>> NFromIdAdapter<'a>
//     for RwTraversalIterator<'a, I>
// {
//     type OutputIter = RwTraversalIterator<'a, NFromId<'a, RwTxn<'a>>>;

//     fn v_from_id(self, id: &'a str) -> Self::OutputIter {
//         let v_from_id = VFromId {
//             iter: std::iter::once(Ok(TraversalVal::Empty)),
//             storage: Arc::clone(&self.storage),
//             txn: self.txn,
//             id,
//         };

//         RwTraversalIterator {
//             inner: v_from_id,
//             storage: self.storage,
//             txn: self.txn,
//         }
//     }
// }
