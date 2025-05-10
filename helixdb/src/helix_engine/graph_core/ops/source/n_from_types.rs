use std::sync::Arc;

use heed3::{
    byteorder::BE,
    types::{Bytes, U128},
    RoTxn,
};

use crate::{
    helix_engine::{
        graph_core::traversal_iter::RoTraversalIterator,
        storage_core::{storage_core::HelixGraphStorage},
        types::GraphError,
    },
    protocol::items::SerializedNode,
};

use super::super::tr_val::TraversalVal;

pub struct NFromTypes<'a> {
    iter: heed3::RoIter<'a, U128<BE>, heed3::types::LazyDecode<Bytes>>,
    storage: Arc<HelixGraphStorage>,
    txn: &'a RoTxn<'a>,
    label: &'a str,
}
// implementing iterator for OutIterator
impl<'a> Iterator for NFromTypes<'a> {
    type Item = Result<TraversalVal, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(value) = self.iter.next() {
            let (key_, value) = value.unwrap();
            match value.decode() {
                Ok(value) => match SerializedNode::decode_node(&value, key_) {
                    Ok(node) => match &node.label {
                        label if label == self.label => return Some(Ok(TraversalVal::Node(node))),
                        _ => continue,
                    },
                    Err(e) => return Some(Err(GraphError::ConversionError(e.to_string()))),
                },
                Err(e) => return Some(Err(GraphError::ConversionError(e.to_string()))),
            }
        }
        None
    }
}
pub trait NFromTypesAdapter<'a>: Iterator<Item = Result<TraversalVal, GraphError>> + Sized {
    fn n_from_types(
        self,
        types: &'a [&'a str],
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>>;
}
impl<'a, I: Iterator<Item = Result<TraversalVal, GraphError>>> NFromTypesAdapter<'a>
    for RoTraversalIterator<'a, I>
{
    fn n_from_types(
        self,
        types: &'a [&'a str],
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>> {
        let db = self.storage.clone();
        let txn: &RoTxn<'_> = self.txn;
        let iter = types.iter().flat_map(move |label| {
            let iter = db.nodes_db.lazily_decode_data().iter(txn).unwrap();
            NFromTypes {
                iter,
                storage: db.clone(),
                txn,
                label,
            }
        });
        RoTraversalIterator {
            inner: iter,
            storage: self.storage,
            txn: self.txn,
        }
    }
}
