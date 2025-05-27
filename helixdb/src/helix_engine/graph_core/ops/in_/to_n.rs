use crate::helix_engine::{
    graph_core::{ops::tr_val::TraversalVal, traversal_iter::RoTraversalIterator},
    storage_core::{storage_core::HelixGraphStorage, storage_methods::StorageMethods},
    types::GraphError,
};
use heed3::RoTxn;
use std::sync::Arc;

pub struct ToNIterator<'a, I, T> {
    iter: I,
    storage: Arc<HelixGraphStorage>,
    txn: &'a T,
}

// implementing iterator for OutIterator
impl<'a, I> Iterator for ToNIterator<'a, I, RoTxn<'a>>
where
    I: Iterator<Item = Result<TraversalVal, GraphError>>,
{
    type Item = Result<TraversalVal, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next() {
            Some(item) => match item {
                Ok(TraversalVal::Edge(item)) => Some(Ok(TraversalVal::Node(
                    match self.storage.get_node(self.txn, &item.to_node) {
                        Ok(node) => node,
                        Err(e) => {
                            println!("Error getting node: {:?}", e);
                            return Some(Err(e));
                        }
                    },
                ))),
                _ => return None,
            },
            None => None,
        }
    }
}
pub trait ToNAdapter<'a, T>: Iterator<Item = Result<TraversalVal, GraphError>> {
    fn to_n(
        self,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>>;
}

impl<'a, I: Iterator<Item = Result<TraversalVal, GraphError>>> ToNAdapter<'a, RoTxn<'a>>
    for RoTraversalIterator<'a, I>
{
    #[inline(always)]
    fn to_n(
        self,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>> {
        let iter = ToNIterator {
            iter: self.inner,
            storage: Arc::clone(&self.storage),
            txn: self.txn,
        };
        RoTraversalIterator {
            inner: iter,
            storage: self.storage,
            txn: self.txn,
        }
    }
}
