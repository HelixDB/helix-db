use crate::{
    helix_engine::{
        storage_core::HelixGraphStorage,
        traversal_core::{traversal_iter::RwTraversalIterator, traversal_value::TraversalValue},
        types::GraphError,
    },
    utils::{id::v6_uuid, items::Node, properties::ImmutablePropertiesMap},
};

#[cfg(feature = "lmdb")]
use crate::helix_engine::bm25::lmdb_bm25::{BM25, BM25Flatten};

#[cfg(feature = "rocks")]
use crate::helix_engine::bm25::rocks_bm25::{BM25, BM25Flatten};

#[cfg(feature = "lmdb")]
use heed3::PutFlags;

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

// LMDB Implementation
#[cfg(feature = "lmdb")]
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

        for index in secondary_indices {
            match self.storage.secondary_indices.get(index) {
                Some(db) => {
                    let key = match node.get_property(index) {
                        Some(value) => value,
                        None => continue,
                    };
                    // look into if there is a way to serialize to a slice
                    match bincode::serialize(&key) {
                        Ok(serialized) => {
                            // possibly append dup

                            if let Err(e) = db.put(self.txn, &serialized, &node.id) {
                                println!(
                                    "{} Error adding node to secondary index: {:?}",
                                    line!(),
                                    e
                                );
                                result = Err(GraphError::from(e));
                            }
                        }
                        Err(e) => result = Err(GraphError::from(e)),
                    }
                }
                None => {
                    result = Err(GraphError::New(format!(
                        "Secondary Index {index} not found"
                    )));
                }
            }
        }

        if let Some(bm25) = &self.storage.bm25
            && let Some(props) = node.properties.as_ref()
        {
            let mut data = props.flatten_bm25();
            data.push_str(node.label);
            if let Err(e) = bm25.insert_doc(self.txn, node.id, &data) {
                result = Err(e);
            }
        }

        if result.is_ok() {
            result = Ok(TraversalValue::Node(node));
        } else {
            result = Err(GraphError::New(
                "Failed to add node to secondary indices".to_string(),
            ));
        }

        RwTraversalIterator {
            storage: self.storage,
            arena: self.arena,
            txn: self.txn,
            inner: std::iter::once(result),
        }
    }
}

// RocksDB Implementation
#[cfg(feature = "rocks")]
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

        match bincode::serialize(&node) {
            Ok(bytes) => {
                if let Err(e) = self.txn.put_cf(
                    &self.storage.cf_nodes(),
                    &HelixGraphStorage::node_key(node.id),
                    &bytes,
                ) {
                    result = Err(GraphError::from(e));
                }
            }
            Err(e) => result = Err(GraphError::from(e)),
        }

        for index in secondary_indices {
            match self.storage.secondary_indices.get(index) {
                Some(cf_name) => {
                    let cf = self.storage.get_secondary_index_cf_handle(cf_name).unwrap();
                    let key = match node.get_property(index) {
                        Some(value) => value,
                        None => continue,
                    };
                    // Serialize the property value
                    match bincode::serialize(&key) {
                        Ok(serialized) => {
                            // Create composite key: serialized_value | node_id
                            let mut buf = bumpalo::collections::Vec::new_in(self.arena);
                            let composite_key = HelixGraphStorage::secondary_index_key(
                                &mut buf,
                                &serialized,
                                node.id,
                            );

                            if let Err(e) = self.txn.put_cf(&cf, composite_key, &[]) {
                                println!(
                                    "{} Error adding node to secondary index: {:?}",
                                    line!(),
                                    e
                                );
                                result = Err(GraphError::from(e));
                            }
                        }
                        Err(e) => result = Err(GraphError::from(e)),
                    }
                }
                None => {
                    result = Err(GraphError::New(format!(
                        "Secondary Index {index} not found"
                    )));
                }
            }
        }

        if let Some(bm25) = &self.storage.bm25
            && let Some(props) = node.properties.as_ref()
        {
            let mut data = props.flatten_bm25();
            data.push_str(node.label);
            if let Err(e) = bm25.insert_doc(self.txn, node.id, &data) {
                result = Err(e);
            }
        }

        if result.is_ok() {
            result = Ok(TraversalValue::Node(node));
        } else {
            result = Err(GraphError::New(
                "Failed to add node to secondary indices".to_string(),
            ));
        }

        RwTraversalIterator {
            storage: self.storage,
            arena: self.arena,
            txn: self.txn,
            inner: std::iter::once(result),
        }
    }
}
