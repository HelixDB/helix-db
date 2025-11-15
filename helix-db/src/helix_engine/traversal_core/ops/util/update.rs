#[cfg(feature = "lmdb")]
use heed3::PutFlags;
use itertools::Itertools;

use crate::{
    helix_engine::{
        storage_core::HelixGraphStorage,
        traversal_core::{traversal_iter::RwTraversalIterator, traversal_value::TraversalValue},
        types::GraphError,
    },
    protocol::value::Value,
    utils::properties::ImmutablePropertiesMap,
};

pub struct Update<I> {
    iter: I,
}

impl<'arena, I> Iterator for Update<I>
where
    I: Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
{
    type Item = Result<TraversalValue<'arena>, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

pub trait UpdateAdapter<'db, 'arena, 'txn>: Iterator {
    fn update(
        self,
        props: &[(&'static str, Value)],
    ) -> RwTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    >;
}

#[cfg(feature = "lmdb")]
impl<'db, 'arena, 'txn, I: Iterator<Item = Result<TraversalValue<'arena>, GraphError>>>
    UpdateAdapter<'db, 'arena, 'txn> for RwTraversalIterator<'db, 'arena, 'txn, I>
{
    fn update(
        self,
        props: &[(&'static str, Value)],
    ) -> RwTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    > {
        // TODO: use a non-contiguous arena vec to avoid copying stuff
        // around when we run out of capacity
        let mut results = bumpalo::collections::Vec::new_in(self.arena);

        for item in self.inner {
            match item {
                Ok(value) => match value {
                    TraversalValue::Node(mut node) => {
                        match node.properties {
                            None => {
                                // Insert secondary indices
                                for (k, v) in props.iter() {
                                    let Some(db) = self.storage.secondary_indices.get(*k) else {
                                        continue;
                                    };

                                    match bincode::serialize(v) {
                                        Ok(v_serialized) => {
                                            if let Err(e) = db.put_with_flags(
                                                self.txn,
                                                PutFlags::APPEND_DUP,
                                                &v_serialized,
                                                &node.id,
                                            ) {
                                                results.push(Err(GraphError::from(e)));
                                            }
                                        }
                                        Err(e) => results.push(Err(GraphError::from(e))),
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
                                    let Some(db) = self.storage.secondary_indices.get(*k) else {
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
                                                &node.id,
                                            ) {
                                                results.push(Err(GraphError::from(e)));
                                                continue;
                                            }
                                        }
                                        Err(e) => {
                                            results.push(Err(GraphError::from(e)));
                                            continue;
                                        }
                                    }

                                    // create new secondary indexes for the props changed
                                    match bincode::serialize(v) {
                                        Ok(v_serialized) => {
                                            if let Err(e) = db.put_with_flags(
                                                self.txn,
                                                PutFlags::APPEND_DUP,
                                                &v_serialized,
                                                &node.id,
                                            ) {
                                                results.push(Err(GraphError::from(e)));
                                            }
                                        }
                                        Err(e) => results.push(Err(GraphError::from(e))),
                                    }
                                }

                                let diff = props.iter().filter(|(k, _)| {
                                    !old.iter().map(|(old_k, _)| old_k).contains(k)
                                });

                                // find out how many new properties we'll need space for
                                let len_diff = diff.clone().count();

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
                                    .chain(diff.cloned());

                                // make new props, updated by current props
                                let new_map = ImmutablePropertiesMap::new(
                                    old.len() + len_diff,
                                    merged,
                                    self.arena,
                                );

                                node.properties = Some(new_map);
                            }
                        }

                        match bincode::serialize(&node) {
                            Ok(serialized_node) => {
                                match self.storage.nodes_db.put(
                                    self.txn,
                                    &node.id,
                                    &serialized_node,
                                ) {
                                    Ok(_) => results.push(Ok(TraversalValue::Node(node))),
                                    Err(e) => results.push(Err(GraphError::from(e))),
                                }
                            }
                            Err(e) => results.push(Err(GraphError::from(e))),
                        }
                    }
                    TraversalValue::Edge(mut edge) => {
                        match edge.properties {
                            None => {
                                // Create properties map and insert edge
                                let map = ImmutablePropertiesMap::new(
                                    props.len(),
                                    props.iter().map(|(k, v)| (*k, v.clone())),
                                    self.arena,
                                );

                                edge.properties = Some(map);
                            }
                            Some(old) => {
                                let diff = props.iter().filter(|(k, _)| {
                                    !old.iter().map(|(old_k, _)| old_k).contains(k)
                                });

                                // find out how many new properties we'll need space for
                                let len_diff = diff.clone().count();

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
                                    .chain(diff.cloned());

                                // make new props, updated by current props
                                let new_map = ImmutablePropertiesMap::new(
                                    old.len() + len_diff,
                                    merged,
                                    self.arena,
                                );

                                edge.properties = Some(new_map);
                            }
                        }

                        match bincode::serialize(&edge) {
                            Ok(serialized_edge) => {
                                match self.storage.edges_db.put(
                                    self.txn,
                                    &edge.id,
                                    &serialized_edge,
                                ) {
                                    Ok(_) => results.push(Ok(TraversalValue::Edge(edge))),
                                    Err(e) => results.push(Err(GraphError::from(e))),
                                }
                            }
                            Err(e) => results.push(Err(GraphError::from(e))),
                        }
                    }
                    // TODO: Implement update properties for Vectors:
                    // TraversalValue::Vector(hvector) => todo!(),
                    // TraversalValue::VectorNodeWithoutVectorData(vector_without_data) => todo!(),
                    _ => results.push(Err(GraphError::New("Unsupported value type".to_string()))),
                },
                Err(e) => results.push(Err(e)),
            }
        }

        RwTraversalIterator {
            inner: Update {
                iter: results.into_iter(),
            },
            storage: self.storage,
            arena: self.arena,
            txn: self.txn,
        }
    }
}

#[cfg(feature = "rocks")]
impl<'db, 'arena, 'txn, I: Iterator<Item = Result<TraversalValue<'arena>, GraphError>>>
    UpdateAdapter<'db, 'arena, 'txn> for RwTraversalIterator<'db, 'arena, 'txn, I>
{
    fn update(
        self,
        props: &[(&'static str, Value)],
    ) -> RwTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    > {
        // TODO: use a non-contiguous arena vec to avoid copying stuff
        // around when we run out of capacity
        let mut results = bumpalo::collections::Vec::new_in(self.arena);

        for item in self.inner {
            match item {
                Ok(value) => match value {
                    TraversalValue::Node(mut node) => {
                        match node.properties {
                            None => {
                                // Insert secondary indices
                                for (k, v) in props.iter() {
                                    let Some(cf_name) = self.storage.secondary_indices.get(*k) else {
                                        continue;
                                    };
                                    let cf = self.storage.graph_env.cf_handle(cf_name).unwrap();

                                    match bincode::serialize(v) {
                                        Ok(v_serialized) => {
                                            let mut buf =
                                                bumpalo::collections::Vec::new_in(self.arena);
                                            let composite_key =
                                                HelixGraphStorage::secondary_index_key(
                                                    &mut buf,
                                                    &v_serialized,
                                                    node.id,
                                                );
                                            if let Err(e) = self.txn.put_cf(&cf, composite_key, &[])
                                            {
                                                results.push(Err(GraphError::from(e)));
                                            }
                                        }
                                        Err(e) => results.push(Err(GraphError::from(e))),
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
                                    let Some(cf_name) = self.storage.secondary_indices.get(*k) else {
                                        continue;
                                    };
                                    let cf = self.storage.graph_env.cf_handle(cf_name).unwrap();

                                    // delete secondary indexes for the props changed
                                    let Some(old_value) = old.get(k) else {
                                        continue;
                                    };

                                    match bincode::serialize(old_value) {
                                        Ok(old_serialized) => {
                                            let mut buf =
                                                bumpalo::collections::Vec::new_in(self.arena);
                                            let composite_key =
                                                HelixGraphStorage::secondary_index_key(
                                                    &mut buf,
                                                    &old_serialized,
                                                    node.id,
                                                );
                                            if let Err(e) = self.txn.delete_cf(&cf, composite_key) {
                                                results.push(Err(GraphError::from(e)));
                                                continue;
                                            }
                                        }
                                        Err(e) => {
                                            results.push(Err(GraphError::from(e)));
                                            continue;
                                        }
                                    }

                                    // create new secondary indexes for the props changed
                                    match bincode::serialize(v) {
                                        Ok(v_serialized) => {
                                            let mut buf =
                                                bumpalo::collections::Vec::new_in(self.arena);
                                            let composite_key =
                                                HelixGraphStorage::secondary_index_key(
                                                    &mut buf,
                                                    &v_serialized,
                                                    node.id,
                                                );
                                            if let Err(e) = self.txn.put_cf(&cf, composite_key, &[])
                                            {
                                                results.push(Err(GraphError::from(e)));
                                            }
                                        }
                                        Err(e) => results.push(Err(GraphError::from(e))),
                                    }
                                }

                                let diff = props.iter().filter(|(k, _)| {
                                    !old.iter().map(|(old_k, _)| old_k).contains(k)
                                });

                                // find out how many new properties we'll need space for
                                let len_diff = diff.clone().count();

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
                                    .chain(diff.cloned());

                                // make new props, updated by current props
                                let new_map = ImmutablePropertiesMap::new(
                                    old.len() + len_diff,
                                    merged,
                                    self.arena,
                                );

                                node.properties = Some(new_map);
                            }
                        }

                        match bincode::serialize(&node) {
                            Ok(serialized_node) => {
                                match self.txn.put_cf(
                                    &self.storage.cf_nodes(),
                                    &HelixGraphStorage::node_key(node.id),
                                    &serialized_node,
                                ) {
                                    Ok(_) => results.push(Ok(TraversalValue::Node(node))),
                                    Err(e) => results.push(Err(GraphError::from(e))),
                                }
                            }
                            Err(e) => results.push(Err(GraphError::from(e))),
                        }
                    }
                    TraversalValue::Edge(mut edge) => {
                        match edge.properties {
                            None => {
                                // Create properties map and insert edge
                                let map = ImmutablePropertiesMap::new(
                                    props.len(),
                                    props.iter().map(|(k, v)| (*k, v.clone())),
                                    self.arena,
                                );

                                edge.properties = Some(map);
                            }
                            Some(old) => {
                                let diff = props.iter().filter(|(k, _)| {
                                    !old.iter().map(|(old_k, _)| old_k).contains(k)
                                });

                                // find out how many new properties we'll need space for
                                let len_diff = diff.clone().count();

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
                                    .chain(diff.cloned());

                                // make new props, updated by current props
                                let new_map = ImmutablePropertiesMap::new(
                                    old.len() + len_diff,
                                    merged,
                                    self.arena,
                                );

                                edge.properties = Some(new_map);
                            }
                        }

                        match bincode::serialize(&edge) {
                            Ok(serialized_edge) => {
                                match self.txn.put_cf(
                                    &self.storage.cf_edges(),
                                    &HelixGraphStorage::edge_key(edge.id),
                                    &serialized_edge,
                                ) {
                                    Ok(_) => results.push(Ok(TraversalValue::Edge(edge))),
                                    Err(e) => results.push(Err(GraphError::from(e))),
                                }
                            }
                            Err(e) => results.push(Err(GraphError::from(e))),
                        }
                    }
                    // TODO: Implement update properties for Vectors:
                    // TraversalValue::Vector(hvector) => todo!(),
                    // TraversalValue::VectorNodeWithoutVectorData(vector_without_data) => todo!(),
                    _ => results.push(Err(GraphError::New("Unsupported value type".to_string()))),
                },
                Err(e) => results.push(Err(e)),
            }
        }

        RwTraversalIterator {
            inner: Update {
                iter: results.into_iter(),
            },
            storage: self.storage,
            arena: self.arena,
            txn: self.txn,
        }
    }
}
