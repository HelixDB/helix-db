use std::sync::Arc;

use heed3::{
    byteorder::BE,
    types::{Bytes, U128},
    RoTxn,
};

use crate::{
    helix_engine::{
        storage_core::storage_core::HelixGraphStorage,
        types::GraphError,
    },
    protocol::items::SerializedEdge,
};

use super::super::tr_val::TraversalVal;

pub struct EFromTypes<'a> {
    iter: heed3::RoIter<'a, U128<BE>, heed3::types::LazyDecode<Bytes>>,
    storage: &'a Arc<HelixGraphStorage>,
    txn: &'a RoTxn<'a>,
    label: &'a str,
}
// implementing iterator for OutIterator
impl<'a> Iterator for EFromTypes<'a> {
    type Item = Result<TraversalVal, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(value) = self.iter.next() {
            let (key, value) = value.unwrap();
            match value.decode() {
                Ok(value) => match SerializedEdge::decode_edge(&value, key) {
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

impl<'a> EFromTypes<'a> {
    pub fn new(storage: &'a Arc<HelixGraphStorage>, txn: &'a RoTxn, label: &'a str) -> Self {
        let iter = storage.edges_db.lazily_decode_data().iter(txn).unwrap();
        EFromTypes {
            iter,
            storage: &storage,
            txn: &txn,
            label,
        }
    }
}
