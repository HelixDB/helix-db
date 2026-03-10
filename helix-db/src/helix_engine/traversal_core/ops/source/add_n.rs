use crate::{
    helix_engine::{
        bm25::bm25::HBM25Config,
        storage_core::HelixGraphStorage,
        traversal_core::{traversal_iter::RwTraversalIterator, traversal_value::TraversalValue},
        types::GraphError,
    },
    utils::{id::v6_uuid, items::Node, properties::ImmutablePropertiesMap},
};
use heed3::{PutFlags, RwTxn};

fn insert_bm25_node_doc(
    bm25: &HBM25Config,
    txn: &mut heed3::RwTxn<'_>,
    node_id: u128,
    properties: &ImmutablePropertiesMap<'_>,
    label: &str,
) -> Result<(), GraphError> {
    bm25.insert_doc_for_node(txn, node_id, properties, label)
}

pub struct AddNIterator<'db, 'arena, 'txn>
where
    'db: 'arena,
    'arena: 'txn,
{
    pub storage: &'db HelixGraphStorage,
    pub arena: &'arena bumpalo::Bump,
    pub txn: &'txn RwTxn<'db>,
    inner: std::iter::Once<Result<TraversalValue<'arena>, GraphError>>,
}

impl<'db, 'arena, 'txn> Iterator for AddNIterator<'db, 'arena, 'txn> {
    type Item = Result<TraversalValue<'arena>, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

pub trait AddNAdapter<'db, 'arena, 'txn, 's>:
    Iterator<Item = Result<TraversalValue<'arena>, GraphError>>
{
    fn add_n(
        self,
        label: &'arena str,
        properties: Option<ImmutablePropertiesMap<'arena>>,
        secondary_indices: Option<&'s [&str]>,
    ) -> RwTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    >;
}

impl<'db, 'arena, 'txn, 's, I: Iterator<Item = Result<TraversalValue<'arena>, GraphError>>>
    AddNAdapter<'db, 'arena, 'txn, 's> for RwTraversalIterator<'db, 'arena, 'txn, I>
{
    fn add_n(
        self,
        label: &'arena str,
        properties: Option<ImmutablePropertiesMap<'arena>>,
        secondary_indices: Option<&'s [&str]>,
    ) -> RwTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    > {
        let node = Node {
            id: v6_uuid(),
            label,
            version: 1,
            properties,
        };
        let secondary_indices = secondary_indices.unwrap_or(&[]).to_vec();
        let mut result: Result<TraversalValue, GraphError> = Ok(TraversalValue::Empty);

        for index in secondary_indices {
            match self.storage.secondary_indices.get(index) {
                Some((db, secondary_index)) => {
                    let key = match node.get_property(index) {
                        Some(value) => value,
                        None => continue,
                    };
                    // look into if there is a way to serialize to a slice
                    match bincode::serialize(&key) {
                        Ok(serialized) => {
                            // possibly append dup

                            if let Err(_e) = {
                                match secondary_index {
                                    crate::helix_engine::types::SecondaryIndex::Unique(_) => db
                                        .put_with_flags(
                                            self.txn,
                                            PutFlags::NO_OVERWRITE,
                                            &serialized,
                                            &node.id,
                                        ),
                                    crate::helix_engine::types::SecondaryIndex::Index(_) => db
                                        .put_with_flags(
                                            self.txn,
                                            PutFlags::APPEND_DUP,
                                            &serialized,
                                            &node.id,
                                        ),
                                    crate::helix_engine::types::SecondaryIndex::None => {
                                        unreachable!()
                                    }
                                }
                            } {
                                result = Err(GraphError::DuplicateKey(index.to_string()));
                                break;
                            }
                        }
                        Err(e) => {
                            result = Err(GraphError::from(e));
                            break;
                        }
                    }
                }
                None => {
                    result = Err(GraphError::New(format!(
                        "Secondary Index {index} not found"
                    )));
                    break;
                }
            }
        }

        if result.is_ok() {
            match bincode::serialize(&node) {
                Ok(bytes) => {
                    if let Err(e) = self.storage.nodes_db.put_with_flags(
                        self.txn,
                        PutFlags::APPEND,
                        &node.id,
                        &bytes,
                    ) {
                        result = Err(GraphError::from(e));
                    }
                }
                Err(e) => result = Err(GraphError::from(e)),
            }

            if result.is_ok()
                && let Some(bm25) = &self.storage.bm25
                && let Some(props) = node.properties.as_ref()
                && let Err(e) = insert_bm25_node_doc(bm25, self.txn, node.id, props, node.label)
            {
                result = Err(e);
            }
        }

        if result.is_ok() {
            result = Ok(TraversalValue::Node(node));
        }
        // Preserve original error - don't overwrite with generic message

        RwTraversalIterator {
            storage: self.storage,
            arena: self.arena,
            txn: self.txn,
            inner: std::iter::once(result),
        }
    }
}
