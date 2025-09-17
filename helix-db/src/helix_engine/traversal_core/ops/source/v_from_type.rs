use crate::helix_engine::{
    storage_core::HelixGraphStorage,
    traversal_core::{traversal_iter::RoTraversalIterator, traversal_value::TraversalValue},
    types::{GraphError, VectorError},
    vector_core::vector::HVector,
};
use heed3::{RoTxn, types::Bytes};
use helix_macros::debug_trace;
use std::sync::Arc;

pub struct VFromType<'a, T> {
    iter: heed3::RoIter<'a, Bytes, heed3::types::LazyDecode<Bytes>>,
    storage: Arc<HelixGraphStorage>,
    txn: &'a T,
    label: &'a str,
}

impl<'a> Iterator for VFromType<'a, RoTxn<'a>> {
    type Item = Result<TraversalValue, GraphError>;

    #[debug_trace("V_FROM_TYPE")]
    fn next(&mut self) -> Option<Self::Item> {
        for value in self.iter.by_ref() {
            let (key, vector_data) = value.unwrap();
            match vector_data.decode() {
                Ok(value) => {
                    let properties = match self
                        .storage
                        .vectors
                        .vector_data_db
                        .get(self.txn, key)
                        .ok()?
                    {
                        Some(bytes) => Some(
                            bincode::deserialize(bytes)
                                .map_err(VectorError::from)
                                .ok()?,
                        ),
                        None => None,
                    };
                    let mut bytes = [0u8; 16];
                    bytes.copy_from_slice(&key[1..=16]);
                    let id = u128::from_be_bytes(bytes);
                    match HVector::decode_vector(value, properties, id) {
                        Ok(vector) => match &vector.get_label() {
                            Some(label) if label.as_str() == self.label => {
                                return Some(Ok(TraversalValue::Vector(vector)));
                            }
                            _ => continue,
                        },
                        Err(e) => {
                            println!("{} Error decoding vector: {:?}", line!(), e);
                            return Some(Err(GraphError::ConversionError(e.to_string())));
                        }
                    }
                }
                Err(e) => return Some(Err(GraphError::ConversionError(e.to_string()))),
            }
        }
        None
    }
}

pub trait VFromTypeAdapter<'a>: Iterator<Item = Result<TraversalValue, GraphError>> {
    type OutputIter: Iterator<Item = Result<TraversalValue, GraphError>>;

    /// Returns an iterator containing the vector with the given label.
    ///
    /// Note that the `label` cannot be empty and must be a valid, existing vector label.
    fn v_from_type(self, label: &'a str) -> Self::OutputIter;
}

impl<'a, I: Iterator<Item = Result<TraversalValue, GraphError>>> VFromTypeAdapter<'a>
    for RoTraversalIterator<'a, I>
{
    type OutputIter = RoTraversalIterator<'a, VFromType<'a, RoTxn<'a>>>;

    #[inline]
    fn v_from_type(self, label: &'a str) -> Self::OutputIter {
        let iter = self
            .storage
            .vectors
            .vectors_db
            .lazily_decode_data()
            .iter(self.txn)
            .unwrap();
        let v_from_type = VFromType {
            iter,
            storage: Arc::clone(&self.storage),
            txn: self.txn,
            label,
        };

        RoTraversalIterator {
            inner: v_from_type,
            storage: self.storage,
            txn: self.txn,
        }
    }
}
