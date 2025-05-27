use crate::{
    helix_engine::{
        graph_core::{ops::tr_val::TraversalVal, traversal_iter::RoTraversalIterator},
        types::GraphError,
    },
    protocol::items::Node,
};
use heed3::{
    byteorder::BE,
    types::{Bytes, U128},
};

pub struct NFromType<'a> {
    iter: heed3::RoIter<'a, U128<BE>, heed3::types::LazyDecode<Bytes>>,
    label: &'a str,
}

impl<'a> Iterator for NFromType<'a> {
    type Item = Result<TraversalVal, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(value) = self.iter.next() {
            let (key_, value) = value.unwrap();
            match value.decode() {
                Ok(value) => match Node::decode_node(&value, key_) {
                    Ok(node) => match &node.label {
                        label if label == self.label => return Some(Ok(TraversalVal::Node(node))),
                        _ => continue,
                    },
                    Err(e) => {
                        println!("{} Error decoding node: {:?}", line!(), e);
                        return Some(Err(GraphError::ConversionError(e.to_string())));
                    }
                },
                Err(e) => return Some(Err(GraphError::ConversionError(e.to_string()))),
            }
        }
        None
    }
}
pub trait NFromTypeAdapter<'a>: Iterator<Item = Result<TraversalVal, GraphError>> {
    /// Returns an iterator containing the nodes with the given label.
    ///
    /// Note that the `label` cannot be empty and must be a valid, existing node label.
    fn n_from_type(
        self,
        label: &'a str,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>>;
}
impl<'a, I: Iterator<Item = Result<TraversalVal, GraphError>>> NFromTypeAdapter<'a>
    for RoTraversalIterator<'a, I>
{   
    #[inline]
    fn n_from_type(
        self,
        label: &'a str,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>> {
        let iter = self
            .storage
            .nodes_db
            .lazily_decode_data()
            .iter(self.txn)
            .unwrap();
        RoTraversalIterator {
            inner: NFromType { iter, label },
            storage: self.storage,
            txn: self.txn,
        }
    }
}
