use heed3::{
    byteorder::BE,
    types::{Bytes, U128},
};

use crate::{
    helix_engine::{
        graph_core::traversal_iter::RoTraversalIterator,
        types::GraphError,
    },
    protocol::items::SerializedNode,
};

use super::super::tr_val::TraversalVal;

pub struct N<'a> {
    iter: heed3::RoIter<'a, U128<BE>, heed3::types::LazyDecode<Bytes>>,
}

// implementing iterator for OutIterator
impl<'a> Iterator for N<'a> {
    type Item = Result<TraversalVal, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|value| {
            let (key, value) = value.unwrap();
            let value = value.decode().unwrap();
            if !value.is_empty() {
                match SerializedNode::decode_node(&value, key) {
                    Ok(node) => Ok(TraversalVal::Node(node)),
                    Err(e) => Err(GraphError::ConversionError(format!(
                        "Error deserializing node: {}",
                        e
                    ))),
                }
            } else {
                Err(GraphError::ConversionError(format!(
                    "Error deserializing node"
                )))
            }
        })
    }
}

pub trait NAdapter<'a>: Iterator<Item = Result<TraversalVal, GraphError>> + Sized {
    fn n(self) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>>;
}

impl<'a, I: Iterator<Item = Result<TraversalVal, GraphError>> + 'a> NAdapter<'a>
    for RoTraversalIterator<'a, I>
{
    fn n(self) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>> {
        let iter = self
            .storage
            .nodes_db
            .lazily_decode_data()
            .iter(self.txn)
            .unwrap();

        // Create the base V iterator
        let n_iter = N { iter };

        // Wrap it with the RoTraversalIterator adapter
        RoTraversalIterator {
            inner: n_iter,
            storage: self.storage,
            txn: self.txn,
        }
    }
}
