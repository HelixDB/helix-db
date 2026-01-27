use heed3::PutFlags;
use itertools::Itertools;

use crate::{
    helix_engine::{
        bm25::bm25::{BM25, BM25Flatten},
        storage_core::HelixGraphStorage,
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
}

impl<'db, 'arena, 'txn, I: Iterator<Item = Result<TraversalValue<'arena>, GraphError>>>
    UpsertAdapter<'db, 'arena, 'txn> for RwTraversalIterator<'db, 'arena, 'txn, I>
{
    fn upsert_n(
        mut self,
        label: &'static str,
        props: &[(&'static str, Value)],
    ) -> RwTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    > {
        let mut result: Result<TraversalValue, GraphError> = Ok(TraversalValue::Empty);
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

                            match bincode::serialize(v) {
                                Ok(v_serialized) => {
                                    if let Err(e) = match secondary_index {
                                        crate::helix_engine::types::SecondaryIndex::Unique(_) => db
                                            .put_with_flags(
                                                self.txn,
                                                PutFlags::NO_OVERWRITE,
                                                &v_serialized,
                                                &node.id,
                                            ),
                                        crate::helix_engine::types::SecondaryIndex::Index(_) => {
                                            db.put(self.txn, &v_serialized, &node.id)
                                        }
                                        crate::helix_engine::types::SecondaryIndex::None => {
                                            unreachable!()
                                        }
                                    } {
                                        result = Err(GraphError::from(e));
                                    }
                                }
                                Err(e) => result = Err(GraphError::from(e)),
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

                            match bincode::serialize(old_value) {
                                Ok(old_serialized) => {
                                    if let Err(e) =
                                        db.delete_one_duplicate(self.txn, &old_serialized, &node.id)
                                    {
                                        result = Err(GraphError::from(e));
                                        break;
                                    }
                                }
                                Err(e) => {
                                    result = Err(GraphError::from(e));
                                    break;
                                }
                            }

                            // create new secondary indexes for the props changed
                            match bincode::serialize(v) {
                                Ok(v_serialized) => {
                                    if let Err(e) = match secondary_index {
                                        crate::helix_engine::types::SecondaryIndex::Unique(_) => db
                                            .put_with_flags(
                                                self.txn,
                                                PutFlags::NO_OVERWRITE,
                                                &v_serialized,
                                                &node.id,
                                            ),
                                        crate::helix_engine::types::SecondaryIndex::Index(_) => {
                                            db.put(self.txn, &v_serialized, &node.id)
                                        }
                                        crate::helix_engine::types::SecondaryIndex::None => {
                                            unreachable!()
                                        }
                                    } {
                                        result = Err(GraphError::from(e));
                                    }
                                }
                                Err(e) => result = Err(GraphError::from(e)),
                            }
                        }

                        let diff = props
                            .iter()
                            .filter(|(k, _)| !old.iter().map(|(old_k, _)| old_k).contains(k));

                        // Add secondary indices for NEW properties (not in old)
                        for (k, v) in diff.clone() {
                            let Some((db, secondary_index)) =
                                self.storage.secondary_indices.get(*k)
                            else {
                                continue;
                            };

                            match bincode::serialize(v) {
                                Ok(v_serialized) => {
                                    if let Err(e) = match secondary_index {
                                        crate::helix_engine::types::SecondaryIndex::Unique(_) => db
                                            .put_with_flags(
                                                self.txn,
                                                PutFlags::NO_OVERWRITE,
                                                &v_serialized,
                                                &node.id,
                                            ),
                                        crate::helix_engine::types::SecondaryIndex::Index(_) => {
                                            db.put(self.txn, &v_serialized, &node.id)
                                        }
                                        crate::helix_engine::types::SecondaryIndex::None => {
                                            unreachable!()
                                        }
                                    } {
                                        result = Err(GraphError::from(e));
                                    }
                                }
                                Err(e) => result = Err(GraphError::from(e)),
                            }
                        }

                        // find out how many new properties we'll need space for
                        let len_diff = diff.clone().count();

                        let merged = old
                            .iter()
                            .map(|(old_k, old_v)| {
                                props
                                    .iter()
                                    .find_map(|(k, v)| old_k.eq(*k).then_some(v))
                                    .map_or_else(|| (old_k, old_v.clone()), |v| (old_k, v.clone()))
                            })
                            .chain(diff.cloned());

                        // make new props, updated by current props
                        let new_map =
                            ImmutablePropertiesMap::new(old.len() + len_diff, merged, self.arena);

                        node.properties = Some(new_map);
                    }
                }

                // Update BM25 index for existing node
                if let Some(bm25) = &self.storage.bm25
                    && let Some(props) = node.properties.as_ref()
                {
                    let mut data = props.flatten_bm25();
                    data.push_str(node.label);
                    if let Err(e) = bm25.update_doc(self.txn, node.id, &data) {
                        result = Err(e);
                    }
                }

                match bincode::serialize(&node) {
                    Ok(serialized_node) => {
                        match self
                            .storage
                            .nodes_db
                            .put(self.txn, &node.id, &serialized_node)
                        {
                            Ok(_) => {
                                if result.is_ok() {
                                    result = Ok(TraversalValue::Node(node));
                                }
                            }
                            Err(e) => result = Err(GraphError::from(e)),
                        }
                    }
                    Err(e) => result = Err(GraphError::from(e)),
                }
            }
            None => {
                let properties = {
                    if props.is_empty() {
                        None
                    } else {
                        Some(ImmutablePropertiesMap::new(
                            props.len(),
                            props.iter().map(|(k, v)| (*k, v.clone())),
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

                for (k, v) in props.iter() {
                    let Some((db, secondary_index)) = self.storage.secondary_indices.get(*k) else {
                        continue;
                    };

                    match bincode::serialize(v) {
                        Ok(v_serialized) => {
                            if let Err(e) = match secondary_index {
                                crate::helix_engine::types::SecondaryIndex::Unique(_) => db
                                    .put_with_flags(
                                        self.txn,
                                        PutFlags::NO_OVERWRITE,
                                        &v_serialized,
                                        &node.id,
                                    ),
                                crate::helix_engine::types::SecondaryIndex::Index(_) => db
                                    .put_with_flags(
                                        self.txn,
                                        PutFlags::APPEND_DUP,
                                        &v_serialized,
                                        &node.id,
                                    ),
                                crate::helix_engine::types::SecondaryIndex::None => unreachable!(),
                            } {
                                result = Err(GraphError::from(e));
                            }
                        }
                        Err(e) => result = Err(GraphError::from(e)),
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
                }
                // Don't overwrite existing errors with a generic message
            }
            Some(Err(e)) => {
                result = Err(e);
            }
            Some(Ok(_)) => {
                // Non-node value in iterator - ignore
            }
        }

        RwTraversalIterator {
            storage: self.storage,
            arena: self.arena,
            txn: self.txn,
            inner: std::iter::once(result),
        }
    }

    fn upsert_e(
        mut self,
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
        let mut result: Result<TraversalValue, GraphError> = Ok(TraversalValue::Empty);

        match self.inner.next() {
            Some(Ok(TraversalValue::Edge(mut edge))) => {
                // Update existing edge - merge properties
                match edge.properties {
                    None => {
                        // Create properties map
                        let map = ImmutablePropertiesMap::new(
                            props.len(),
                            props.iter().map(|(k, v)| (*k, v.clone())),
                            self.arena,
                        );
                        edge.properties = Some(map);
                    }
                    Some(old) => {
                        let diff = props
                            .iter()
                            .filter(|(k, _)| !old.iter().map(|(old_k, _)| old_k).contains(k));

                        let len_diff = diff.clone().count();

                        let merged = old
                            .iter()
                            .map(|(old_k, old_v)| {
                                props
                                    .iter()
                                    .find_map(|(k, v)| old_k.eq(*k).then_some(v))
                                    .map_or_else(|| (old_k, old_v.clone()), |v| (old_k, v.clone()))
                            })
                            .chain(diff.cloned());

                        let new_map =
                            ImmutablePropertiesMap::new(old.len() + len_diff, merged, self.arena);
                        edge.properties = Some(new_map);
                    }
                }

                // Update edges_db only (no secondary indices or BM25 for edges)
                match edge.to_bincode_bytes() {
                    Ok(serialized_edge) => {
                        match self.storage.edges_db.put(
                            self.txn,
                            HelixGraphStorage::edge_key(&edge.id),
                            &serialized_edge,
                        ) {
                            Ok(_) => {
                                if result.is_ok() {
                                    result = Ok(TraversalValue::Edge(edge));
                                }
                            }
                            Err(e) => result = Err(GraphError::from(e)),
                        }
                    }
                    Err(e) => result = Err(GraphError::from(e)),
                }
            }
            Some(Err(e)) => {
                result = Err(e);
            }
            None => {
                // Create new edge
                let version = self.storage.version_info.get_latest(label);
                let properties = if props.is_empty() {
                    None
                } else {
                    Some(ImmutablePropertiesMap::new(
                        props.len(),
                        props.iter().map(|(k, v)| (*k, v.clone())),
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

                // Insert into edges_db
                match edge.to_bincode_bytes() {
                    Ok(bytes) => {
                        if let Err(e) = self.storage.edges_db.put_with_flags(
                            self.txn,
                            PutFlags::APPEND,
                            HelixGraphStorage::edge_key(&edge.id),
                            &bytes,
                        ) {
                            result = Err(GraphError::from(e));
                        }
                    }
                    Err(e) => result = Err(GraphError::from(e)),
                }

                // Insert into out_edges_db
                let label_hash = hash_label(edge.label, None);
                if let Err(e) = self.storage.out_edges_db.put_with_flags(
                    self.txn,
                    PutFlags::APPEND_DUP,
                    &HelixGraphStorage::out_edge_key(&from_node, &label_hash),
                    &HelixGraphStorage::pack_edge_data(&edge.id, &to_node),
                ) {
                    result = Err(GraphError::from(e));
                }

                // Insert into in_edges_db
                if let Err(e) = self.storage.in_edges_db.put_with_flags(
                    self.txn,
                    PutFlags::APPEND_DUP,
                    &HelixGraphStorage::in_edge_key(&to_node, &label_hash),
                    &HelixGraphStorage::pack_edge_data(&edge.id, &from_node),
                ) {
                    result = Err(GraphError::from(e));
                }

                if result.is_ok() {
                    result = Ok(TraversalValue::Edge(edge));
                }
            }
            Some(Ok(_)) => {
                // Non-edge value in iterator - ignore
            }
        }

        RwTraversalIterator {
            storage: self.storage,
            arena: self.arena,
            txn: self.txn,
            inner: std::iter::once(result),
        }
    }

    fn upsert_v(
        mut self,
        query: &'arena [f64],
        label: &'arena str,
        props: &[(&'static str, Value)],
    ) -> RwTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    > {
        let mut result: Result<TraversalValue, GraphError> = Ok(TraversalValue::Empty);
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

                            match bincode::serialize(v) {
                                Ok(v_serialized) => {
                                    if let Err(e) = match secondary_index {
                                        crate::helix_engine::types::SecondaryIndex::Unique(_) => db
                                            .put_with_flags(
                                                self.txn,
                                                PutFlags::NO_OVERWRITE,
                                                &v_serialized,
                                                &vector.id,
                                            ),
                                        crate::helix_engine::types::SecondaryIndex::Index(_) => {
                                            db.put(self.txn, &v_serialized, &vector.id)
                                        }
                                        crate::helix_engine::types::SecondaryIndex::None => {
                                            unreachable!()
                                        }
                                    } {
                                        result = Err(GraphError::from(e));
                                    }
                                }
                                Err(e) => result = Err(GraphError::from(e)),
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

                            match bincode::serialize(old_value) {
                                Ok(old_serialized) => {
                                    if let Err(e) = db.delete_one_duplicate(
                                        self.txn,
                                        &old_serialized,
                                        &vector.id,
                                    ) {
                                        result = Err(GraphError::from(e));
                                        break;
                                    }
                                }
                                Err(e) => {
                                    result = Err(GraphError::from(e));
                                    break;
                                }
                            }

                            // create new secondary indexes for the props changed
                            match bincode::serialize(v) {
                                Ok(v_serialized) => {
                                    if let Err(e) = match secondary_index {
                                        crate::helix_engine::types::SecondaryIndex::Unique(_) => db
                                            .put_with_flags(
                                                self.txn,
                                                PutFlags::NO_OVERWRITE,
                                                &v_serialized,
                                                &vector.id,
                                            ),
                                        crate::helix_engine::types::SecondaryIndex::Index(_) => {
                                            db.put(self.txn, &v_serialized, &vector.id)
                                        }
                                        crate::helix_engine::types::SecondaryIndex::None => {
                                            unreachable!()
                                        }
                                    } {
                                        result = Err(GraphError::from(e));
                                    }
                                }
                                Err(e) => result = Err(GraphError::from(e)),
                            }
                        }

                        let diff = props
                            .iter()
                            .filter(|(k, _)| !old.iter().map(|(old_k, _)| old_k).contains(k));

                        // Add secondary indices for NEW properties (not in old)
                        for (k, v) in diff.clone() {
                            let Some((db, secondary_index)) =
                                self.storage.secondary_indices.get(*k)
                            else {
                                continue;
                            };

                            match bincode::serialize(v) {
                                Ok(v_serialized) => {
                                    if let Err(e) = match secondary_index {
                                        crate::helix_engine::types::SecondaryIndex::Unique(_) => db
                                            .put_with_flags(
                                                self.txn,
                                                PutFlags::NO_OVERWRITE,
                                                &v_serialized,
                                                &vector.id,
                                            ),
                                        crate::helix_engine::types::SecondaryIndex::Index(_) => {
                                            db.put(self.txn, &v_serialized, &vector.id)
                                        }
                                        crate::helix_engine::types::SecondaryIndex::None => {
                                            unreachable!()
                                        }
                                    } {
                                        result = Err(GraphError::from(e));
                                    }
                                }
                                Err(e) => result = Err(GraphError::from(e)),
                            }
                        }

                        // find out how many new properties we'll need space for
                        let len_diff = diff.clone().count();

                        let merged = old
                            .iter()
                            .map(|(old_k, old_v)| {
                                props
                                    .iter()
                                    .find_map(|(k, v)| old_k.eq(*k).then_some(v))
                                    .map_or_else(|| (old_k, old_v.clone()), |v| (old_k, v.clone()))
                            })
                            .chain(diff.cloned());

                        // make new props, updated by current props
                        let new_map =
                            ImmutablePropertiesMap::new(old.len() + len_diff, merged, self.arena);

                        vector.properties = Some(new_map);
                    }
                }

                // Update BM25 index for existing node
                if let Some(bm25) = &self.storage.bm25
                    && let Some(props) = vector.properties.as_ref()
                {
                    let mut data = props.flatten_bm25();
                    data.push_str(vector.label);
                    if let Err(e) = bm25.update_doc(self.txn, vector.id, &data) {
                        result = Err(e);
                    }
                }

                match self.storage.vectors.put_vector(self.txn, &vector) {
                    Ok(_) => {
                        if result.is_ok() {
                            result = Ok(TraversalValue::Vector(vector));
                        }
                    }
                    Err(e) => result = Err(GraphError::from(e)),
                }
            }
            None => {
                let properties = {
                    if props.is_empty() {
                        None
                    } else {
                        Some(ImmutablePropertiesMap::new(
                            props.len(),
                            props.iter().map(|(k, v)| (*k, v.clone())),
                            self.arena,
                        ))
                    }
                };

                match self
                    .storage
                    .vectors
                    .insert::<fn(&HVector, &heed3::RoTxn) -> bool>(
                        self.txn, label, query, properties, self.arena,
                    ) {
                    Ok(vector) => {
                        result = Ok(TraversalValue::Vector(vector));
                    }
                    Err(e) => result = Err(GraphError::from(e)),
                }

                if result.is_ok()
                    && let Ok(TraversalValue::Vector(ref vector)) = result
                {
                    for (k, v) in props.iter() {
                        let Some((db, secondary_index)) = self.storage.secondary_indices.get(*k)
                        else {
                            continue;
                        };

                        match bincode::serialize(v) {
                            Ok(v_serialized) => {
                                if let Err(e) = match secondary_index {
                                    crate::helix_engine::types::SecondaryIndex::Unique(_) => db
                                        .put_with_flags(
                                            self.txn,
                                            PutFlags::NO_OVERWRITE,
                                            &v_serialized,
                                            &vector.id,
                                        ),
                                    crate::helix_engine::types::SecondaryIndex::Index(_) => db
                                        .put_with_flags(
                                            self.txn,
                                            PutFlags::APPEND_DUP,
                                            &v_serialized,
                                            &vector.id,
                                        ),
                                    crate::helix_engine::types::SecondaryIndex::None => {
                                        unreachable!()
                                    }
                                } {
                                    result = Err(GraphError::from(e));
                                    break;
                                }
                            }
                            Err(e) => {
                                result = Err(GraphError::from(e));
                                break;
                            }
                        }
                    }
                }

                if result.is_ok()
                    && let Ok(TraversalValue::Vector(ref vector)) = result
                    && let Some(bm25) = &self.storage.bm25
                    && let Some(props) = vector.properties.as_ref()
                {
                    let mut data = props.flatten_bm25();
                    data.push_str(vector.label);
                    if let Err(e) = bm25.insert_doc(self.txn, vector.id, &data) {
                        result = Err(e);
                    }
                }
            }
            Some(Err(e)) => {
                result = Err(e);
            }
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

                            match bincode::serialize(v) {
                                Ok(v_serialized) => {
                                    if let Err(e) = match secondary_index {
                                        crate::helix_engine::types::SecondaryIndex::Unique(_) => db
                                            .put_with_flags(
                                                self.txn,
                                                PutFlags::NO_OVERWRITE,
                                                &v_serialized,
                                                &vector.id,
                                            ),
                                        crate::helix_engine::types::SecondaryIndex::Index(_) => {
                                            db.put(self.txn, &v_serialized, &vector.id)
                                        }
                                        crate::helix_engine::types::SecondaryIndex::None => {
                                            unreachable!()
                                        }
                                    } {
                                        result = Err(GraphError::from(e));
                                    }
                                }
                                Err(e) => result = Err(GraphError::from(e)),
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

                            match bincode::serialize(old_value) {
                                Ok(old_serialized) => {
                                    if let Err(e) = db.delete_one_duplicate(
                                        self.txn,
                                        &old_serialized,
                                        &vector.id,
                                    ) {
                                        result = Err(GraphError::from(e));
                                        break;
                                    }
                                }
                                Err(e) => {
                                    result = Err(GraphError::from(e));
                                    break;
                                }
                            }

                            // create new secondary indexes for the props changed
                            match bincode::serialize(v) {
                                Ok(v_serialized) => {
                                    if let Err(e) = match secondary_index {
                                        crate::helix_engine::types::SecondaryIndex::Unique(_) => db
                                            .put_with_flags(
                                                self.txn,
                                                PutFlags::NO_OVERWRITE,
                                                &v_serialized,
                                                &vector.id,
                                            ),
                                        crate::helix_engine::types::SecondaryIndex::Index(_) => {
                                            db.put(self.txn, &v_serialized, &vector.id)
                                        }
                                        crate::helix_engine::types::SecondaryIndex::None => {
                                            unreachable!()
                                        }
                                    } {
                                        result = Err(GraphError::from(e));
                                    }
                                }
                                Err(e) => result = Err(GraphError::from(e)),
                            }
                        }

                        let diff = props
                            .iter()
                            .filter(|(k, _)| !old.iter().map(|(old_k, _)| old_k).contains(k));

                        // Add secondary indices for NEW properties (not in old)
                        for (k, v) in diff.clone() {
                            let Some((db, secondary_index)) =
                                self.storage.secondary_indices.get(*k)
                            else {
                                continue;
                            };

                            match bincode::serialize(v) {
                                Ok(v_serialized) => {
                                    if let Err(e) = match secondary_index {
                                        crate::helix_engine::types::SecondaryIndex::Unique(_) => db
                                            .put_with_flags(
                                                self.txn,
                                                PutFlags::NO_OVERWRITE,
                                                &v_serialized,
                                                &vector.id,
                                            ),
                                        crate::helix_engine::types::SecondaryIndex::Index(_) => {
                                            db.put(self.txn, &v_serialized, &vector.id)
                                        }
                                        crate::helix_engine::types::SecondaryIndex::None => {
                                            unreachable!()
                                        }
                                    } {
                                        result = Err(GraphError::from(e));
                                    }
                                }
                                Err(e) => result = Err(GraphError::from(e)),
                            }
                        }

                        // find out how many new properties we'll need space for
                        let len_diff = diff.clone().count();

                        let merged = old
                            .iter()
                            .map(|(old_k, old_v)| {
                                props
                                    .iter()
                                    .find_map(|(k, v)| old_k.eq(*k).then_some(v))
                                    .map_or_else(|| (old_k, old_v.clone()), |v| (old_k, v.clone()))
                            })
                            .chain(diff.cloned());

                        // make new props, updated by current props
                        let new_map =
                            ImmutablePropertiesMap::new(old.len() + len_diff, merged, self.arena);

                        vector.properties = Some(new_map);
                    }
                }

                // Update BM25 index for existing node
                if let Some(bm25) = &self.storage.bm25
                    && let Some(props) = vector.properties.as_ref()
                {
                    let mut data = props.flatten_bm25();
                    data.push_str(vector.label);
                    if let Err(e) = bm25.update_doc(self.txn, vector.id, &data) {
                        result = Err(e);
                    }
                }

                match self.storage.vectors.put_vector(self.txn, &vector) {
                    Ok(_) => {
                        if result.is_ok() {
                            result = Ok(TraversalValue::Vector(vector));
                        }
                    }
                    Err(e) => result = Err(GraphError::from(e)),
                }
            }
            Some(Ok(_)) => {
                // Non-Vector value in iterator - ignore
            }
        }

        RwTraversalIterator {
            storage: self.storage,
            arena: self.arena,
            txn: self.txn,
            inner: std::iter::once(result),
        }
    }
}
