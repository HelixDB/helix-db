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
use heed3::{
    byteorder::BE,
    types::{Bytes, U128},
};

pub struct NFromType<'arena, 'txn, 's>
where
    'arena: 'txn,
{
    pub arena: &'arena bumpalo::Bump,
    pub iter: heed3::RoIter<'txn, U128<BE>, Bytes>,
    pub label: &'s [u8],
}

/// Iterator for the `NFromType` operation.
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
impl<'arena, 'txn, 's> Iterator for NFromType<'arena, 'txn, 's> {
    type Item = Result<TraversalValue<'arena>, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        for value in self.iter.by_ref() {
            let (key, value) = value.unwrap();

            assert!(
                value.len() >= LMDB_STRING_HEADER_LENGTH,
                "value length does not contain header which means the `label` field was missing from the node on insertion"
            );
            let length_of_label_in_lmdb =
                u64::from_le_bytes(value[..LMDB_STRING_HEADER_LENGTH].try_into().unwrap()) as usize;

            if length_of_label_in_lmdb != self.label.len() {
                continue;
            }

            assert!(
                value.len() >= length_of_label_in_lmdb + LMDB_STRING_HEADER_LENGTH,
                "value length is not at least the header length plus the label length meaning there has been a corruption on node insertion"
            );
            let label_in_lmdb = &value[LMDB_STRING_HEADER_LENGTH
                ..LMDB_STRING_HEADER_LENGTH + length_of_label_in_lmdb as usize];

            if label_in_lmdb == self.label {
                match Node::<'arena>::decode_node(value, key, self.arena) {
                    Ok(node) => {
                        return Some(Ok(TraversalValue::Node(node)));
                    }
                    Err(e) => {
                        println!("{} Error decoding node: {:?}", line!(), e);
                        return Some(Err(GraphError::ConversionError(e.to_string())));
                    }
                }
            } else {
                continue;
            }
        }
        None
    }
}
pub trait NFromTypeAdapter<'db, 'arena, 'txn, 's>:
    Iterator<Item = Result<TraversalValue<'arena>, GraphError>>
{
    /// Returns an iterator containing the nodes with the given label.
    ///
    /// Note that the `label` cannot be empty and must be a valid, existing node label.
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
        let iter = self.storage.nodes_db.iter(self.txn).unwrap(); // should be handled because label may be variable in the future

        RoTraversalIterator {
            storage: self.storage,
            arena: self.arena,
            txn: self.txn,
            inner: NFromType {
                iter,
                label: label.as_bytes(),
                arena: self.arena,
            },
        }
    }
}
