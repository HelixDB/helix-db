use super::super::tr_val::TraversalVal;
use crate::{
    helix_engine::{
        graph_core::traversal_iter::{RoTraversalIterator, RwTraversalIterator},
        types::GraphError,
    },
    protocol::items::{Edge, SerializedEdge},
};
use std::ops::Deref;
use heed3::{
    byteorder::BE,
    types::{Bytes, U128},
};

pub struct E<'a> {
    iter: heed3::RoIter<'a, U128<BE>, heed3::types::LazyDecode<Bytes>>,
}

// implementing iterator for OutIterator
impl<'a> Iterator for E<'a> {
    type Item = Result<TraversalVal, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|value| {
            let (key, value) = value.unwrap();
            let value = value.decode().unwrap();
            if !value.is_empty() {
                match SerializedEdge::decode_edge(&value, key) {
                    Ok(edge) => Ok(TraversalVal::Edge(edge)),
                    Err(e) => Err(GraphError::ConversionError(format!(
                        "Error deserializing edge: {}",
                        e
                    ))),
                }
            } else {
                Err(GraphError::ConversionError(format!(
                    "Error deserializing edge"
                )))
            }
        })
    }
}

pub trait EAdapter<'a>: Iterator<Item = Result<TraversalVal, GraphError>> + Sized {
    fn e(self) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>>;
}

impl<'a, I: Iterator<Item = Result<TraversalVal, GraphError>> + 'a> EAdapter<'a>
    for RoTraversalIterator<'a, I>
{
    fn e(self) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>> {
        let iter = self
            .storage
            .edges_db
            .lazily_decode_data()
            .iter(self.txn)
            .unwrap();
        let e_iter = E { iter };
        RoTraversalIterator {
            inner: e_iter,
            storage: self.storage,
            txn: self.txn,
        }
    }
}

pub trait RwEAdapter<'a, 'b>: Iterator<Item = Result<TraversalVal, GraphError>> + Sized {
    fn e(
        self,
    ) -> RwTraversalIterator<'a, 'b, impl Iterator<Item = Result<TraversalVal, GraphError>>>;
}

impl<'a, 'b, I: Iterator<Item = Result<TraversalVal, GraphError>> + 'a> RwEAdapter<'a, 'b>
    for RwTraversalIterator<'a, 'b, I>
{
    fn e(
        self,
    ) -> RwTraversalIterator<'a, 'b, impl Iterator<Item = Result<TraversalVal, GraphError>>> {
        let txn = self.txn.deref().deref();
        let iter = self
            .storage
            .edges_db
            .lazily_decode_data()
            .iter(txn)
            .unwrap()
            .filter_map(|result| {
                let (key, value) = result.unwrap();
                let value: Edge = match SerializedEdge::decode_edge(&value.decode().unwrap(), key) {
                    Ok(edge) => edge,
                    Err(e) => {
                        eprintln!("Error decoding edge: {:?}", e);
                        return None;
                    }
                };
                Some(Ok(TraversalVal::Edge(value)))
            })
            .collect::<Vec<_>>()
            .into_iter();

        RwTraversalIterator {
            inner: iter,
            storage: self.storage,
            txn: self.txn,
        }
    }
}
