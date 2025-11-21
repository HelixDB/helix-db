use crate::{
    helix_engine::{
        traversal_core::{
            LMDB_STRING_HEADER_LENGTH, traversal_iter::RoTraversalIterator,
            traversal_value::TraversalValue,
        },
        types::GraphError,
    },
    utils::items::Node,
};

pub trait NFromTypeAdapter<'db, 'arena, 'txn, 's>:
    Iterator<Item = Result<TraversalValue<'arena>, GraphError>>
{
    /// Returns an iterator containing the nodes with the given label.
    ///
    /// Note that the `label` cannot be empty and must be a valid, existing node label.'
    ///
    /// The label is stored before the node properties in LMDB.
    /// Bincode assures that the fields of a struct are stored in the same order as they are defined in the struct (first to last).
    ///
    /// Bincode stores an 8 byte u64 length field before strings.
    /// Therefore to check the label of a node without deserializing the node, we read the 8 byte header and create a u64 from those bytes.
    /// We then assert the length is valid to avoid out of bounds panics.
    ///
    /// We can the get the label bytes using the header length and the length of the label.
    ///
    /// We then compare the label bytes to the given label; deserializing the node into the arena if it matches.
    fn n_from_type(
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
    NFromTypeAdapter<'db, 'arena, 'txn, 's> for RoTraversalIterator<'db, 'arena, 'txn, I>
{
    #[inline]
    fn n_from_type(
        self,
        label: &'s str,
    ) -> RoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    > {
        let label_as_bytes = label.as_bytes();
        let iter = self.storage.nodes_db.iter(self.txn).unwrap().filter_map(move |item| {
            if let Ok((id, value)) = item {
                assert!(
                    value.len() >= LMDB_STRING_HEADER_LENGTH,
                    "value length does not contain header which means the `label` field was missing from the node on insertion"
                );
                let length_of_label_in_lmdb =
                    u64::from_le_bytes(value[..LMDB_STRING_HEADER_LENGTH].try_into().unwrap()) as usize;

                if length_of_label_in_lmdb != label.len() {
                    return None;
                }

                assert!(
                    value.len() >= length_of_label_in_lmdb + LMDB_STRING_HEADER_LENGTH,
                    "value length is not at least the header length plus the label length meaning there has been a corruption on node insertion"
                );
                let label_in_lmdb = &value[LMDB_STRING_HEADER_LENGTH
                    ..LMDB_STRING_HEADER_LENGTH + length_of_label_in_lmdb];

                if label_in_lmdb == label_as_bytes {
                    match Node::<'arena>::from_bincode_bytes(id, value, self.arena) {
                        Ok(node) => {
                            return Some(Ok(TraversalValue::Node(node)));
                        }
                        Err(e) => {
                            println!("{} Error decoding node: {:?}", line!(), e);
                            return Some(Err(GraphError::ConversionError(e.to_string())));
                        }
                    }
                } else {
                    return None;
                }
            }
            None

        }); // should be handled because label may be variable in the future

        RoTraversalIterator {
            storage: self.storage,
            arena: self.arena,
            txn: self.txn,
            inner: iter,
        }
    }
}

#[cfg(feature = "rocks")]
impl<'db, 'arena, 'txn, 's, I: Iterator<Item = Result<TraversalValue<'arena>, GraphError>>>
    NFromTypeAdapter<'db, 'arena, 'txn, 's> for RoTraversalIterator<'db, 'arena, 'txn, I>
{
    #[inline]
    fn n_from_type(
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

        let mut iter = txn.raw_iterator_cf(&storage.cf_nodes());
        iter.seek_to_first();

        let label_len = label.len();

        let inner = std::iter::from_fn(move || {
            while iter.valid() {
                if let Some((key, value)) = iter.item() {
                    let id = match key.try_into() {
                        Ok(bytes) => u128::from_be_bytes(bytes),
                        Err(_) => {
                            println!("{} Error converting key to node ID", line!());
                            iter.next();
                            continue;
                        }
                    };

                    if value.len() < LMDB_STRING_HEADER_LENGTH {
                        panic!(
                            "value length does not contain header which means the `label` field was missing from the node on insertion"
                        );
                    }

                    let length_of_label_in_db =
                        u64::from_le_bytes(value[..LMDB_STRING_HEADER_LENGTH].try_into().unwrap())
                            as usize;

                    if length_of_label_in_db != label_len {
                        iter.next();
                        continue;
                    }

                    let end = LMDB_STRING_HEADER_LENGTH + length_of_label_in_db;
                    if value.len() < end {
                        panic!(
                            "value length is not at least the header length plus the label length meaning there has been a corruption on node insertion"
                        );
                    }

                    let label_in_db = &value[LMDB_STRING_HEADER_LENGTH..end];

                    if label_in_db == label_as_bytes {
                        match Node::<'arena>::from_bincode_bytes(id, value, arena) {
                            Ok(node) => {
                                iter.next();
                                return Some(Ok(TraversalValue::Node(node)));
                            }
                            Err(e) => {
                                iter.next();
                                println!("{} Error decoding node: {:?}", line!(), e);
                                return Some(Err(GraphError::ConversionError(e.to_string())));
                            }
                        }
                    } else {
                        iter.next();
                        continue;
                    }
                } else {
                    // no key/value, advance
                    iter.next();
                }
            }
            None
        });

        RoTraversalIterator {
            storage,
            arena,
            txn,
            inner,
        }
    }
}
