use crate::{
    helix_engine::{
        graph_core::{ops::tr_val::TraversalVal, traversal_iter::RoTraversalIterator},
        types::GraphError,
    },
    protocol::items::Edge,
};
use heed3::{
    byteorder::BE,
    types::{Bytes, U128},
};

pub struct EFromType<'a> {
    iter: heed3::RoIter<'a, U128<BE>, heed3::types::LazyDecode<Bytes>>,
    label: &'a str,
}

impl<'a> Iterator for EFromType<'a> {
    type Item = Result<TraversalVal, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(value) = self.iter.next() {
            let (key, value) = value.unwrap();
            match value.decode() {
                Ok(value) => match Edge::decode_edge(&value, key) {
                    Ok(edge) => match &edge.label {
                        label if label == self.label => return Some(Ok(TraversalVal::Edge(edge))),
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
pub trait EFromTypeAdapter<'a>: Iterator<Item = Result<TraversalVal, GraphError>> {
    fn e_from_type(
        self,
        label: &'a str,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>>;
}
impl<'a, I: Iterator<Item = Result<TraversalVal, GraphError>>> EFromTypeAdapter<'a>
    for RoTraversalIterator<'a, I>
{
    #[inline]
    fn e_from_type(
        self,
        label: &'a str,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>> {
        let iter = self
            .storage
            .edges_db
            .lazily_decode_data()
            .iter(self.txn)
            .unwrap();
        RoTraversalIterator {
            inner: EFromType { iter, label },
            storage: self.storage,
            txn: self.txn,
        }
    }
}
