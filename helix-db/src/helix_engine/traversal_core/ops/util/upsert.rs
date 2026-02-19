use heed3::PutFlags;
use itertools::Itertools;

use crate::{
    helix_engine::{
        bm25::bm25::{BM25, BM25Flatten},
        storage_core::{HelixGraphStorage, storage_methods::StorageMethods},
        traversal_core::{traversal_iter::RwTraversalIterator, traversal_value::TraversalValue},
        types::GraphError,
        vector_core::{hnsw::HNSW, vector::HVector},
    },
    protocol::value::Value,
    utils::{
        id::v6_uuid,
        items::{Edge, Node},
        label_hash::hash_label,
        properties::ImmutablePropertiesMap,
    },
};

fn merge_create_props(
    props: &[(&'static str, Value)],
    create_defaults: &[(&'static str, Value)],
) -> Vec<(&'static str, Value)> {
    let mut merged = props
        .iter()
        .map(|(key, value)| (*key, value.clone()))
        .collect::<Vec<_>>();

    for (key, value) in create_defaults {
        if !merged.iter().any(|(existing_key, _)| existing_key == key) {
            merged.push((*key, value.clone()));
        }
    }

    merged
}

pub trait UpsertAdapter<'db, 'arena, 'txn>: Iterator {
    fn upsert_n(
        self,
        label: &'static str,
        props: &[(&'static str, Value)],
    ) -> RwTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    >;

    fn upsert_n_with_defaults(
        self,
        label: &'static str,
        props: &[(&'static str, Value)],
        create_defaults: &[(&'static str, Value)],
    ) -> RwTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    >;

    fn upsert_e(
        self,
        label: &'arena str,
        from_node: u128,
        to_node: u128,
        props: &[(&'static str, Value)],
    ) -> RwTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    >;

    fn upsert_e_with_defaults(
        self,
        label: &'arena str,
        from_node: u128,
        to_node: u128,
        props: &[(&'static str, Value)],
        create_defaults: &[(&'static str, Value)],
    ) -> RwTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    >;

    fn upsert_v(
        self,
        query: &'arena [f64],
        label: &'arena str,
        props: &[(&'static str, Value)],
    ) -> RwTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    >;

    fn upsert_v_with_defaults(
        self,
        query: &'arena [f64],
        label: &'arena str,
        props: &[(&'static str, Value)],
        create_defaults: &[(&'static str, Value)],
    ) -> RwTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    >;
}

impl<'db, 'arena, 'txn, I: Iterator<Item = Result<TraversalValue<'arena>, GraphError>>>
    UpsertAdapter<'db, 'arena, 'txn> for RwTraversalIterator<'db, 'arena, 'txn, I>
{
    fn upsert_n(
        self,
        label: &'static str,
        props: &[(&'static str, Value)],
    ) -> RwTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    > {
        self.upsert_n_with_defaults(label, props, &[])
    }

    fn upsert_n_with_defaults(
        mut self,
        label: &'static str,
        props: &[(&'static str, Value)],
        create_defaults: &[(&'static str, Value)],
    ) -> RwTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    > {
        let result = (|| -> Result<TraversalValue<'arena>, GraphError> {
            match self.inner.next() {
                Some(Ok(TraversalValue::Node(mut node))) => {
                    match node.properties {
                        None => {
                            // Insert secondary indices
                            for (k, v) in props.iter() {
                                let Some((db, secondary_index)) =
                                    self.storage.secondary_indices.get(*k)
                                else {
                                    continue;
                                };

                                let v_serialized = bincode::serialize(v)?;
                                match secondary_index {
                                    crate::helix_engine::types::SecondaryIndex::Unique(_) => db
                                        .put_with_flags(
                                            self.txn,
                                            PutFlags::NO_OVERWRITE,
                                            &v_serialized,
                                            &node.id,
                                        )
                                        .map_err(|_| GraphError::DuplicateKey(k.to_string()))?,
                                    crate::helix_engine::types::SecondaryIndex::Index(_) => {
                                        db.put(self.txn, &v_serialized, &node.id)?
                                    }
                                    crate::helix_engine::types::SecondaryIndex::None => {
                                        unreachable!()
                                    }
                                }
                            }

                            // Create properties map and insert node
                            let map = ImmutablePropertiesMap::new(
                                props.len(),
                                props.iter().map(|(k, v)| (*k, v.clone())),
                                self.arena,
                            );

                            node.properties = Some(map);
                        }
                        Some(old) => {
                            for (k, v) in props.iter() {
                                let Some((db, secondary_index)) =
                                    self.storage.secondary_indices.get(*k)
                                else {
                                    continue;
                                };

                                // delete secondary indexes for the props changed
                                let Some(old_value) = old.get(k) else {
                                    continue;
                                };

                                let old_serialized = bincode::serialize(old_value)?;
                                db.delete_one_duplicate(self.txn, &old_serialized, &node.id)?;

                                // create new secondary indexes for the props changed
                                let v_serialized = bincode::serialize(v)?;
                                match secondary_index {
                                    crate::helix_engine::types::SecondaryIndex::Unique(_) => db
                                        .put_with_flags(
                                            self.txn,
                                            PutFlags::NO_OVERWRITE,
                                            &v_serialized,
                                            &node.id,
                                        )
                                        .map_err(|_| GraphError::DuplicateKey(k.to_string()))?,
                                    crate::helix_engine::types::SecondaryIndex::Index(_) => {
                                        db.put(self.txn, &v_serialized, &node.id)?
                                    }
                                    crate::helix_engine::types::SecondaryIndex::None => {
                                        unreachable!()
                                    }
                                }
                            }

                            let diff: Vec<_> = props
                                .iter()
                                .filter(|(k, _)| !old.iter().map(|(old_k, _)| old_k).contains(k))
                                .cloned()
                                .collect();

                            // Add secondary indices for NEW properties (not in old)
                            for (k, v) in &diff {
                                let Some((db, secondary_index)) =
                                    self.storage.secondary_indices.get(*k)
                                else {
                                    continue;
                                };

                                let v_serialized = bincode::serialize(v)?;
                                match secondary_index {
                                    crate::helix_engine::types::SecondaryIndex::Unique(_) => db
                                        .put_with_flags(
                                            self.txn,
                                            PutFlags::NO_OVERWRITE,
                                            &v_serialized,
                                            &node.id,
                                        )
                                        .map_err(|_| GraphError::DuplicateKey(k.to_string()))?,
                                    crate::helix_engine::types::SecondaryIndex::Index(_) => {
                                        db.put(self.txn, &v_serialized, &node.id)?
                                    }
                                    crate::helix_engine::types::SecondaryIndex::None => {
                                        unreachable!()
                                    }
                                }
                            }

                            // find out how many new properties we'll need space for
                            let len_diff = diff.len();

                            let merged = old
                                .iter()
                                .map(|(old_k, old_v)| {
                                    props
                                        .iter()
                                        .find_map(|(k, v)| old_k.eq(*k).then_some(v))
                                        .map_or_else(
                                            || (old_k, old_v.clone()),
                                            |v| (old_k, v.clone()),
                                        )
                                })
                                .chain(diff);

                            // make new props, updated by current props
                            let new_map = ImmutablePropertiesMap::new(
                                old.len() + len_diff,
                                merged,
                                self.arena,
                            );

                            node.properties = Some(new_map);
                        }
                    }

                    // Update BM25 index for existing node
                    if let Some(bm25) = &self.storage.bm25
                        && let Some(props) = node.properties.as_ref()
                    {
                        let mut data = props.flatten_bm25();
                        data.push_str(node.label);
                        bm25.update_doc(self.txn, node.id, &data)?;
                    }

                    let serialized_node = bincode::serialize(&node)?;
                    self.storage
                        .nodes_db
                        .put(self.txn, &node.id, &serialized_node)?;
                    Ok(TraversalValue::Node(node))
                }
                None => {
                    let create_props = merge_create_props(props, create_defaults);

                    let properties = {
                        if create_props.is_empty() {
                            None
                        } else {
                            Some(ImmutablePropertiesMap::new(
                                create_props.len(),
                                create_props.iter().map(|(k, v)| (*k, v.clone())),
                                self.arena,
                            ))
                        }
                    };

                    let node = Node {
                        id: v6_uuid(),
                        label,
                        version: 1,
                        properties,
                    };

                    let bytes = bincode::serialize(&node)?;
                    self.storage.nodes_db.put_with_flags(
                        self.txn,
                        PutFlags::APPEND,
                        &node.id,
                        &bytes,
                    )?;

                    for (k, v) in create_props.iter() {
                        let Some((db, secondary_index)) = self.storage.secondary_indices.get(*k)
                        else {
                            continue;
                        };

                        let v_serialized = bincode::serialize(v)?;
                        match secondary_index {
                            crate::helix_engine::types::SecondaryIndex::Unique(_) => db
                                .put_with_flags(
                                    self.txn,
                                    PutFlags::NO_OVERWRITE,
                                    &v_serialized,
                                    &node.id,
                                )
                                .map_err(|_| GraphError::DuplicateKey(k.to_string()))?,
                            crate::helix_engine::types::SecondaryIndex::Index(_) => db
                                .put_with_flags(
                                    self.txn,
                                    PutFlags::APPEND_DUP,
                                    &v_serialized,
                                    &node.id,
                                )?,
                            crate::helix_engine::types::SecondaryIndex::None => unreachable!(),
                        }
                    }

                    if let Some(bm25) = &self.storage.bm25
                        && let Some(props) = node.properties.as_ref()
                    {
                        let mut data = props.flatten_bm25();
                        data.push_str(node.label);
                        bm25.insert_doc(self.txn, node.id, &data)?;
                    }

                    Ok(TraversalValue::Node(node))
                }
                Some(Err(e)) => Err(e),
                Some(Ok(_)) => Ok(TraversalValue::Empty),
            }
        })();

        RwTraversalIterator {
            storage: self.storage,
            arena: self.arena,
            txn: self.txn,
            inner: std::iter::once(result),
        }
    }

    fn upsert_e(
        self,
        label: &'arena str,
        from_node: u128,
        to_node: u128,
        props: &[(&'static str, Value)],
    ) -> RwTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    > {
        self.upsert_e_with_defaults(label, from_node, to_node, props, &[])
    }

    fn upsert_e_with_defaults(
        self,
        label: &'arena str,
        from_node: u128,
        to_node: u128,
        props: &[(&'static str, Value)],
        create_defaults: &[(&'static str, Value)],
    ) -> RwTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    > {
        let label_hash = hash_label(label, None);
        let out_key = HelixGraphStorage::out_edge_key(&from_node, &label_hash);
        let existing_edge: Result<Option<Edge>, GraphError> = (|| {
            let Some(iter) = self
                .storage
                .out_edges_db
                .lazily_decode_data()
                .get_duplicates(self.txn, &out_key)?
            else {
                return Ok(None);
            };
            for item in iter {
                let (_, data) = item?;
                let data = data
                    .decode()
                    .map_err(|e| GraphError::DecodeError(e.to_string()))?;
                let (edge_id, node_id) = HelixGraphStorage::unpack_adj_edge_data(data)?;
                if node_id == to_node {
                    return Ok(Some(self.storage.get_edge(self.txn, &edge_id, self.arena)?));
                }
            }
            Ok(None)
        })();
        let result = (|| -> Result<TraversalValue<'arena>, GraphError> {
            match existing_edge {
                Ok(Some(mut edge)) => {
                    // Update existing edge - merge properties
                    match edge.properties {
                        None => {
                            let map = ImmutablePropertiesMap::new(
                                props.len(),
                                props.iter().map(|(k, v)| (*k, v.clone())),
                                self.arena,
                            );
                            edge.properties = Some(map);
                        }
                        Some(old) => {
                            let diff: Vec<_> = props
                                .iter()
                                .filter(|(k, _)| !old.iter().map(|(old_k, _)| old_k).contains(k))
                                .cloned()
                                .collect();

                            let len_diff = diff.len();

                            let merged = old
                                .iter()
                                .map(|(old_k, old_v)| {
                                    props
                                        .iter()
                                        .find_map(|(k, v)| old_k.eq(*k).then_some(v))
                                        .map_or_else(
                                            || (old_k, old_v.clone()),
                                            |v| (old_k, v.clone()),
                                        )
                                })
                                .chain(diff);

                            let new_map = ImmutablePropertiesMap::new(
                                old.len() + len_diff,
                                merged,
                                self.arena,
                            );
                            edge.properties = Some(new_map);
                        }
                    }

                    let serialized_edge = edge.to_bincode_bytes()?;
                    self.storage.edges_db.put(
                        self.txn,
                        HelixGraphStorage::edge_key(&edge.id),
                        &serialized_edge,
                    )?;
                    Ok(TraversalValue::Edge(edge))
                }
                Ok(None) => {
                    // Create new edge
                    let version = self.storage.version_info.get_latest(label);
                    let create_props = merge_create_props(props, create_defaults);
                    let properties = if create_props.is_empty() {
                        None
                    } else {
                        Some(ImmutablePropertiesMap::new(
                            create_props.len(),
                            create_props.iter().map(|(k, v)| (*k, v.clone())),
                            self.arena,
                        ))
                    };

                    let edge = Edge {
                        id: v6_uuid(),
                        label,
                        version,
                        properties,
                        from_node,
                        to_node,
                    };

                    let bytes = edge.to_bincode_bytes()?;
                    self.storage.edges_db.put_with_flags(
                        self.txn,
                        PutFlags::APPEND,
                        HelixGraphStorage::edge_key(&edge.id),
                        &bytes,
                    )?;
                    self.storage.out_edges_db.put_with_flags(
                        self.txn,
                        PutFlags::APPEND_DUP,
                        &HelixGraphStorage::out_edge_key(&from_node, &label_hash),
                        &HelixGraphStorage::pack_edge_data(&edge.id, &to_node),
                    )?;
                    self.storage.in_edges_db.put_with_flags(
                        self.txn,
                        PutFlags::APPEND_DUP,
                        &HelixGraphStorage::in_edge_key(&to_node, &label_hash),
                        &HelixGraphStorage::pack_edge_data(&edge.id, &from_node),
                    )?;
                    Ok(TraversalValue::Edge(edge))
                }
                Err(e) => Err(e),
            }
        })();

        RwTraversalIterator {
            storage: self.storage,
            arena: self.arena,
            txn: self.txn,
            inner: std::iter::once(result),
        }
    }

    fn upsert_v(
        self,
        query: &'arena [f64],
        label: &'arena str,
        props: &[(&'static str, Value)],
    ) -> RwTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    > {
        self.upsert_v_with_defaults(query, label, props, &[])
    }

    fn upsert_v_with_defaults(
        mut self,
        query: &'arena [f64],
        label: &'arena str,
        props: &[(&'static str, Value)],
        create_defaults: &[(&'static str, Value)],
    ) -> RwTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    > {
        let result = (|| -> Result<TraversalValue<'arena>, GraphError> {
            match self.inner.next() {
                Some(Ok(TraversalValue::Vector(mut vector))) => {
                    match vector.properties {
                        None => {
                            // Insert secondary indices
                            for (k, v) in props.iter() {
                                let Some((db, secondary_index)) =
                                    self.storage.secondary_indices.get(*k)
                                else {
                                    continue;
                                };

                                let v_serialized = bincode::serialize(v)?;
                                match secondary_index {
                                    crate::helix_engine::types::SecondaryIndex::Unique(_) => db
                                        .put_with_flags(
                                            self.txn,
                                            PutFlags::NO_OVERWRITE,
                                            &v_serialized,
                                            &vector.id,
                                        )
                                        .map_err(|_| GraphError::DuplicateKey(k.to_string()))?,
                                    crate::helix_engine::types::SecondaryIndex::Index(_) => {
                                        db.put(self.txn, &v_serialized, &vector.id)?
                                    }
                                    crate::helix_engine::types::SecondaryIndex::None => {
                                        unreachable!()
                                    }
                                }
                            }

                            // Create properties map and insert node
                            let map = ImmutablePropertiesMap::new(
                                props.len(),
                                props.iter().map(|(k, v)| (*k, v.clone())),
                                self.arena,
                            );

                            vector.properties = Some(map);
                        }
                        Some(old) => {
                            for (k, v) in props.iter() {
                                let Some((db, secondary_index)) =
                                    self.storage.secondary_indices.get(*k)
                                else {
                                    continue;
                                };

                                // delete secondary indexes for the props changed
                                let Some(old_value) = old.get(k) else {
                                    continue;
                                };

                                let old_serialized = bincode::serialize(old_value)?;
                                db.delete_one_duplicate(self.txn, &old_serialized, &vector.id)?;

                                // create new secondary indexes for the props changed
                                let v_serialized = bincode::serialize(v)?;
                                match secondary_index {
                                    crate::helix_engine::types::SecondaryIndex::Unique(_) => db
                                        .put_with_flags(
                                            self.txn,
                                            PutFlags::NO_OVERWRITE,
                                            &v_serialized,
                                            &vector.id,
                                        )
                                        .map_err(|_| GraphError::DuplicateKey(k.to_string()))?,
                                    crate::helix_engine::types::SecondaryIndex::Index(_) => {
                                        db.put(self.txn, &v_serialized, &vector.id)?
                                    }
                                    crate::helix_engine::types::SecondaryIndex::None => {
                                        unreachable!()
                                    }
                                }
                            }

                            let diff: Vec<_> = props
                                .iter()
                                .filter(|(k, _)| !old.iter().map(|(old_k, _)| old_k).contains(k))
                                .cloned()
                                .collect();

                            // Add secondary indices for NEW properties (not in old)
                            for (k, v) in &diff {
                                let Some((db, secondary_index)) =
                                    self.storage.secondary_indices.get(*k)
                                else {
                                    continue;
                                };

                                let v_serialized = bincode::serialize(v)?;
                                match secondary_index {
                                    crate::helix_engine::types::SecondaryIndex::Unique(_) => db
                                        .put_with_flags(
                                            self.txn,
                                            PutFlags::NO_OVERWRITE,
                                            &v_serialized,
                                            &vector.id,
                                        )
                                        .map_err(|_| GraphError::DuplicateKey(k.to_string()))?,
                                    crate::helix_engine::types::SecondaryIndex::Index(_) => {
                                        db.put(self.txn, &v_serialized, &vector.id)?
                                    }
                                    crate::helix_engine::types::SecondaryIndex::None => {
                                        unreachable!()
                                    }
                                }
                            }

                            // find out how many new properties we'll need space for
                            let len_diff = diff.len();

                            let merged = old
                                .iter()
                                .map(|(old_k, old_v)| {
                                    props
                                        .iter()
                                        .find_map(|(k, v)| old_k.eq(*k).then_some(v))
                                        .map_or_else(
                                            || (old_k, old_v.clone()),
                                            |v| (old_k, v.clone()),
                                        )
                                })
                                .chain(diff);

                            // make new props, updated by current props
                            let new_map = ImmutablePropertiesMap::new(
                                old.len() + len_diff,
                                merged,
                                self.arena,
                            );

                            vector.properties = Some(new_map);
                        }
                    }

                    // Update BM25 index for existing vector
                    if let Some(bm25) = &self.storage.bm25
                        && let Some(props) = vector.properties.as_ref()
                    {
                        let mut data = props.flatten_bm25();
                        data.push_str(vector.label);
                        bm25.update_doc(self.txn, vector.id, &data)?;
                    }

                    self.storage.vectors.put_vector(self.txn, &vector)?;
                    Ok(TraversalValue::Vector(vector))
                }
                None => {
                    let create_props = merge_create_props(props, create_defaults);

                    let properties = {
                        if create_props.is_empty() {
                            None
                        } else {
                            Some(ImmutablePropertiesMap::new(
                                create_props.len(),
                                create_props.iter().map(|(k, v)| (*k, v.clone())),
                                self.arena,
                            ))
                        }
                    };

                    let vector = self
                        .storage
                        .vectors
                        .insert::<fn(&HVector, &heed3::RoTxn) -> bool>(
                            self.txn, label, query, properties, self.arena,
                        )?;

                    for (k, v) in create_props.iter() {
                        let Some((db, secondary_index)) = self.storage.secondary_indices.get(*k)
                        else {
                            continue;
                        };

                        let v_serialized = bincode::serialize(v)?;
                        match secondary_index {
                            crate::helix_engine::types::SecondaryIndex::Unique(_) => db
                                .put_with_flags(
                                    self.txn,
                                    PutFlags::NO_OVERWRITE,
                                    &v_serialized,
                                    &vector.id,
                                )
                                .map_err(|_| GraphError::DuplicateKey(k.to_string()))?,
                            crate::helix_engine::types::SecondaryIndex::Index(_) => db
                                .put_with_flags(
                                    self.txn,
                                    PutFlags::APPEND_DUP,
                                    &v_serialized,
                                    &vector.id,
                                )?,
                            crate::helix_engine::types::SecondaryIndex::None => unreachable!(),
                        }
                    }

                    if let Some(bm25) = &self.storage.bm25
                        && let Some(props) = vector.properties.as_ref()
                    {
                        let mut data = props.flatten_bm25();
                        data.push_str(vector.label);
                        bm25.insert_doc(self.txn, vector.id, &data)?;
                    }

                    Ok(TraversalValue::Vector(vector))
                }
                Some(Err(e)) => Err(e),
                Some(Ok(TraversalValue::VectorNodeWithoutVectorData(vector_without_data))) => {
                    // Convert VectorWithoutData to HVector using From impl
                    let mut vector: HVector = vector_without_data.into();
                    // Set the vector data from query parameter
                    vector.data = query;

                    match vector.properties {
                        None => {
                            // Insert secondary indices
                            for (k, v) in props.iter() {
                                let Some((db, secondary_index)) =
                                    self.storage.secondary_indices.get(*k)
                                else {
                                    continue;
                                };

                                let v_serialized = bincode::serialize(v)?;
                                match secondary_index {
                                    crate::helix_engine::types::SecondaryIndex::Unique(_) => db
                                        .put_with_flags(
                                            self.txn,
                                            PutFlags::NO_OVERWRITE,
                                            &v_serialized,
                                            &vector.id,
                                        )
                                        .map_err(|_| GraphError::DuplicateKey(k.to_string()))?,
                                    crate::helix_engine::types::SecondaryIndex::Index(_) => {
                                        db.put(self.txn, &v_serialized, &vector.id)?
                                    }
                                    crate::helix_engine::types::SecondaryIndex::None => {
                                        unreachable!()
                                    }
                                }
                            }

                            // Create properties map and insert node
                            let map = ImmutablePropertiesMap::new(
                                props.len(),
                                props.iter().map(|(k, v)| (*k, v.clone())),
                                self.arena,
                            );

                            vector.properties = Some(map);
                        }
                        Some(old) => {
                            for (k, v) in props.iter() {
                                let Some((db, secondary_index)) =
                                    self.storage.secondary_indices.get(*k)
                                else {
                                    continue;
                                };

                                // delete secondary indexes for the props changed
                                let Some(old_value) = old.get(k) else {
                                    continue;
                                };

                                let old_serialized = bincode::serialize(old_value)?;
                                db.delete_one_duplicate(self.txn, &old_serialized, &vector.id)?;

                                // create new secondary indexes for the props changed
                                let v_serialized = bincode::serialize(v)?;
                                match secondary_index {
                                    crate::helix_engine::types::SecondaryIndex::Unique(_) => db
                                        .put_with_flags(
                                            self.txn,
                                            PutFlags::NO_OVERWRITE,
                                            &v_serialized,
                                            &vector.id,
                                        )
                                        .map_err(|_| GraphError::DuplicateKey(k.to_string()))?,
                                    crate::helix_engine::types::SecondaryIndex::Index(_) => {
                                        db.put(self.txn, &v_serialized, &vector.id)?
                                    }
                                    crate::helix_engine::types::SecondaryIndex::None => {
                                        unreachable!()
                                    }
                                }
                            }

                            let diff: Vec<_> = props
                                .iter()
                                .filter(|(k, _)| !old.iter().map(|(old_k, _)| old_k).contains(k))
                                .cloned()
                                .collect();

                            // Add secondary indices for NEW properties (not in old)
                            for (k, v) in &diff {
                                let Some((db, secondary_index)) =
                                    self.storage.secondary_indices.get(*k)
                                else {
                                    continue;
                                };

                                let v_serialized = bincode::serialize(v)?;
                                match secondary_index {
                                    crate::helix_engine::types::SecondaryIndex::Unique(_) => db
                                        .put_with_flags(
                                            self.txn,
                                            PutFlags::NO_OVERWRITE,
                                            &v_serialized,
                                            &vector.id,
                                        )
                                        .map_err(|_| GraphError::DuplicateKey(k.to_string()))?,
                                    crate::helix_engine::types::SecondaryIndex::Index(_) => {
                                        db.put(self.txn, &v_serialized, &vector.id)?
                                    }
                                    crate::helix_engine::types::SecondaryIndex::None => {
                                        unreachable!()
                                    }
                                }
                            }

                            // find out how many new properties we'll need space for
                            let len_diff = diff.len();

                            let merged = old
                                .iter()
                                .map(|(old_k, old_v)| {
                                    props
                                        .iter()
                                        .find_map(|(k, v)| old_k.eq(*k).then_some(v))
                                        .map_or_else(
                                            || (old_k, old_v.clone()),
                                            |v| (old_k, v.clone()),
                                        )
                                })
                                .chain(diff);

                            // make new props, updated by current props
                            let new_map = ImmutablePropertiesMap::new(
                                old.len() + len_diff,
                                merged,
                                self.arena,
                            );

                            vector.properties = Some(new_map);
                        }
                    }

                    // Update BM25 index for existing vector
                    if let Some(bm25) = &self.storage.bm25
                        && let Some(props) = vector.properties.as_ref()
                    {
                        let mut data = props.flatten_bm25();
                        data.push_str(vector.label);
                        bm25.update_doc(self.txn, vector.id, &data)?;
                    }

                    self.storage.vectors.put_vector(self.txn, &vector)?;
                    Ok(TraversalValue::Vector(vector))
                }
                Some(Ok(_)) => Ok(TraversalValue::Empty),
            }
        })();

        RwTraversalIterator {
            storage: self.storage,
            arena: self.arena,
            txn: self.txn,
            inner: std::iter::once(result),
        }
    }
}
