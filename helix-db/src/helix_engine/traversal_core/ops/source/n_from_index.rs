use crate::{
    helix_engine::{
        traversal_core::{
            LMDB_STRING_HEADER_LENGTH, traversal_iter::RoTraversalIterator,
            traversal_value::TraversalValue,
        },
        types::GraphError,
    },
    protocol::value::Value,
    utils::items::Node,
};
use serde::Serialize;

pub trait NFromIndexAdapter<'db, 'arena, 'txn, 's, K: Into<Value> + Serialize>:
    Iterator<Item = Result<TraversalValue<'arena>, GraphError>>
{
    /// Returns a new iterator that will return the node from the secondary index.
    ///
    /// # Arguments
    ///
    /// * `index` - The name of the secondary index.
    /// * `key` - The key to search for in the secondary index.
    ///
    /// Note that both the `index` and `key` must be provided.
    /// The index must be a valid and existing secondary index and the key should match the type of the index.
    fn n_from_index(
        self,
        label: &'s str,
        index: &'s str,
        key: &'s K,
    ) -> RoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    >
    where
        K: Into<Value> + Serialize + Clone;
}

impl<
    'db,
    'arena,
    'txn,
    's,
    K: Into<Value> + Serialize,
    I: Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
> NFromIndexAdapter<'db, 'arena, 'txn, 's, K> for RoTraversalIterator<'db, 'arena, 'txn, I>
{
    #[inline]
    fn n_from_index(
        self,
        label: &'s str,
        index: &'s str,
        key: &K,
    ) -> RoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    >
    where
        K: Into<Value> + Serialize + Clone,
    {
        let db = self
            .storage
            .secondary_indices
            .get(index)
            .ok_or(GraphError::New(format!(
                "Secondary Index {index} not found"
            )))
            .unwrap();
        let label_as_bytes = label.as_bytes();
        let res = db
            .prefix_iter(self.txn, &bincode::serialize(&Value::from(key)).unwrap())
            .unwrap()
            .filter_map(move |item| {
                if let Ok((_, node_id)) = item &&
                 let Some(value) = self.storage.nodes_db.get(self.txn, &node_id).ok()? {
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
                        match Node::<'arena>::from_bincode_bytes(node_id, value, self.arena) {
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


            });

        RoTraversalIterator {
            storage: self.storage,
            arena: self.arena,
            txn: self.txn,
            inner: res,
        }
    }
}
