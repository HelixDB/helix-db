use crate::{
    helix_engine::{
        traversal_core::{
            LMDB_STRING_HEADER_LENGTH, traversal_iter::RoTraversalIterator,
            traversal_value::TraversalValue,
        },
        types::GraphError,
    },
    utils::items::Edge,
};
#[cfg(feature = "lmdb")]
use heed3::{
    byteorder::BE,
    types::{Bytes, U128},
};

#[cfg(feature = "lmdb")]
pub struct EFromType<'arena, 'txn, 's>
where
    'arena: 'txn,
{
    pub arena: &'arena bumpalo::Bump,
    pub iter: heed3::RoIter<'txn, U128<BE>, heed3::types::LazyDecode<Bytes>>,
    pub label: &'s [u8],
}

#[cfg(feature = "lmdb")]
impl<'arena, 'txn, 's> Iterator for EFromType<'arena, 'txn, 's> {
    type Item = Result<TraversalValue<'arena>, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        for value in self.iter.by_ref() {
            let (id, value) = value.unwrap();

            match value.decode() {
                Ok(value) => {
                    assert!(
                        value.len() >= LMDB_STRING_HEADER_LENGTH,
                        "value length does not contain header which means the `label` field was missing from the node on insertion"
                    );
                    let length_of_label_in_lmdb =
                        u64::from_le_bytes(value[..LMDB_STRING_HEADER_LENGTH].try_into().unwrap())
                            as usize;

                    println!("{:?}", value);

                    assert!(
                        value.len() >= length_of_label_in_lmdb + LMDB_STRING_HEADER_LENGTH,
                        "value length is not at least the header length plus the label length meaning there has been a corruption on node insertion"
                    );
                    let label_in_lmdb = &value[LMDB_STRING_HEADER_LENGTH
                        ..LMDB_STRING_HEADER_LENGTH + length_of_label_in_lmdb];

                    if label_in_lmdb == self.label {
                        match Edge::<'arena>::from_bincode_bytes(id, value, self.arena) {
                            Ok(edge) => {
                                return Some(Ok(TraversalValue::Edge(edge)));
                            }
                            Err(e) => {
                                println!("{} Error decoding edge: {:?}", line!(), e);
                                return Some(Err(GraphError::ConversionError(e.to_string())));
                            }
                        }
                    } else {
                        continue;
                    }
                }
                Err(e) => return Some(Err(GraphError::ConversionError(e.to_string()))),
            }
        }
        None
    }
}
pub trait EFromTypeAdapter<'db, 'arena, 'txn, 's>:
    Iterator<Item = Result<TraversalValue<'arena>, GraphError>>
{
    fn e_from_type(
        self,
        label: &'s str,
    ) -> RoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    >;
}
#[cfg(feature = "lmdb")]
impl<'db, 'arena, 'txn, 's, I: Iterator<Item = Result<TraversalValue<'arena>, GraphError>>>
    EFromTypeAdapter<'db, 'arena, 'txn, 's> for RoTraversalIterator<'db, 'arena, 'txn, I>
{
    #[inline]
    fn e_from_type(
        self,
        label: &'s str,
    ) -> RoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    > {
        let iter = self
            .storage
            .edges_db
            .lazily_decode_data()
            .iter(self.txn)
            .unwrap();
        RoTraversalIterator {
            storage: self.storage,
            arena: self.arena,
            txn: self.txn,
            inner: EFromType {
                arena: self.arena,
                iter,
                label: label.as_bytes(),
            },
        }
    }
}

#[cfg(feature = "rocks")]
impl<'db, 'arena, 'txn, 's, I: Iterator<Item = Result<TraversalValue<'arena>, GraphError>>>
    EFromTypeAdapter<'db, 'arena, 'txn, 's> for RoTraversalIterator<'db, 'arena, 'txn, I>
{
    #[inline]
    fn e_from_type(
        self,
        label: &'s str,
    ) -> RoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    > {
        let label_as_bytes = label.as_bytes();
        let storage = self.storage;
        let arena = self.arena;
        let txn = self.txn;

        // Collect results using raw iterator
        let mut results = Vec::new();
        let mut iter = txn.raw_iterator_cf(&storage.edges_db);
        iter.seek_to_first();

        while iter.valid() {
            if let (Some(key), Some(value)) = (iter.key(), iter.value()) {
                // Extract edge ID from key
                let id = match key.try_into() {
                    Ok(bytes) => u128::from_be_bytes(bytes),
                    Err(_) => {
                        println!("{} Error converting key to edge ID", line!());
                        iter.next();
                        continue;
                    }
                };

                assert!(
                    value.len() >= LMDB_STRING_HEADER_LENGTH,
                    "value length does not contain header which means the `label` field was missing from the edge on insertion"
                );
                let length_of_label_in_db =
                    u64::from_le_bytes(value[..LMDB_STRING_HEADER_LENGTH].try_into().unwrap())
                        as usize;

                if length_of_label_in_db != label.len() {
                    iter.next();
                    continue;
                }

                assert!(
                    value.len() >= length_of_label_in_db + LMDB_STRING_HEADER_LENGTH,
                    "value length is not at least the header length plus the label length meaning there has been a corruption on edge insertion"
                );
                let label_in_db = &value
                    [LMDB_STRING_HEADER_LENGTH..LMDB_STRING_HEADER_LENGTH + length_of_label_in_db];

                if label_in_db == label_as_bytes {
                    match Edge::<'arena>::from_bincode_bytes(id, value, arena) {
                        Ok(edge) => {
                            results.push(Ok(TraversalValue::Edge(edge)));
                        }
                        Err(e) => {
                            println!("{} Error decoding edge: {:?}", line!(), e);
                            results.push(Err(GraphError::ConversionError(e.to_string())));
                        }
                    }
                }
            }
            iter.next();
        }

        RoTraversalIterator {
            storage,
            arena,
            txn,
            inner: results.into_iter(),
        }
    }
}
