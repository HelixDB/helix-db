use crate::helix_engine::{
    traversal_core::{
        LMDB_STRING_HEADER_LENGTH, traversal_iter::RoTraversalIterator,
        traversal_value::TraversalValue,
    },
    types::{GraphError, VectorError},
    vector_core::vector_without_data::VectorWithoutData,
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


                    // get label via bytes directly
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


                    // get deleted via bytes directly
                    
                    // skip single byte for version
                    let version_index = length_of_label_in_lmdb + LMDB_STRING_HEADER_LENGTH;

                    // get bool for deleted 
                    let deleted_index = version_index + 1;
                    let deleted = value[deleted_index] == 1;

                    if deleted {
                        return None;
                    }
        
                    if label_in_lmdb == label_bytes {
                        let vector_without_data = VectorWithoutData::from_bincode_bytes(self.arena, value, id)
                                    .map_err(|e| VectorError::ConversionError(e.to_string()))
                                    .ok()?;
        
                        if get_vector_data {
                            let mut vector = match self.storage.vectors.get_raw_vector_data(self.txn, id, label, self.arena) {
                                Ok(bytes) => bytes,
                                Err(VectorError::VectorDeleted) => return None,
                                Err(e) => return Some(Err(GraphError::from(e))),
                            };
                            vector.expand_from_vector_without_data(vector_without_data);
                            return Some(Ok(TraversalValue::Vector(vector)));
                        } else {
                            return Some(Ok(TraversalValue::VectorNodeWithoutVectorData(
                                vector_without_data
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
