use crate::{
    helix_engine::{
        traversal_core::{traversal_iter::RoTraversalIterator, traversal_value::TraversalValue, LMDB_STRING_HEADER_LENGTH},
        types::{GraphError, VectorError},
        vector_core::vector_without_data::VectorWithoutData,
    },
};

pub trait VFromTypeAdapter<'db, 'arena, 'txn>:
    Iterator<Item = Result<TraversalValue<'arena>, GraphError>>
{
    /// Returns an iterator containing the vector with the given label.
    ///
    /// Note that the `label` cannot be empty and must be a valid, existing vector label.
    fn v_from_type(
        self,
        label: &'arena str,
        get_vector_data: bool,
    ) -> RoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    >;
}

impl<'db, 'arena, 'txn, I: Iterator<Item = Result<TraversalValue<'arena>, GraphError>>>
    VFromTypeAdapter<'db, 'arena, 'txn> for RoTraversalIterator<'db, 'arena, 'txn, I>
{
    #[inline]
    fn v_from_type(
        self,
        label: &'arena str,
        get_vector_data: bool,
    ) -> RoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    > {
        let label_bytes = label.as_bytes();
        let iter = self
            .storage
            .vectors
            .vector_properties_db
            .iter(self.txn)
            .unwrap()
            .filter_map(move |item| {
                if let Ok((id, value)) = item {

                    assert!(
                        value.len() >= LMDB_STRING_HEADER_LENGTH,
                        "value length does not contain header which means the `label` field was missing from the node on insertion"
                    );
                    let length_of_label_in_lmdb =
                        u64::from_le_bytes(value[..LMDB_STRING_HEADER_LENGTH].try_into().unwrap()) as usize;
                    assert!(
                        value.len() >= length_of_label_in_lmdb + LMDB_STRING_HEADER_LENGTH,
                        "value length is not at least the header length plus the label length meaning there has been a corruption on node insertion"
                    );
                    let label_in_lmdb = &value[LMDB_STRING_HEADER_LENGTH
                        ..LMDB_STRING_HEADER_LENGTH + length_of_label_in_lmdb];
        
                    if label_in_lmdb == label_bytes {
                        
        
                        if get_vector_data {
                            let vector = match self.storage.vectors.get_full_vector(self.txn, id,  self.arena) {
                                Ok(bytes) => bytes,
                                Err(VectorError::VectorDeleted) => return None,
                                Err(e) => return Some(Err(GraphError::from(e))),
                            };

                            return Some(Ok(TraversalValue::Vector(vector)));
                        } else {
                            return Some(Ok(TraversalValue::VectorNodeWithoutVectorData(

                                // TODO change to use bump map here
                                VectorWithoutData::from_bincode_bytes(self.arena, value, id)
                                    .map_err(|e| VectorError::ConversionError(e.to_string()))
                                    .ok()?
                            )));
                        }
                    } else {
                        return None;
                    }
                   
                }
                None
            });

        RoTraversalIterator {
            storage: self.storage,
            arena: self.arena,
            txn: self.txn,
            inner: iter,
        }
    }
}
