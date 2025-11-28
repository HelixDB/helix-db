use crate::{
    helix_engine::{
        storage_core::HelixGraphStorage,
        traversal_core::{traversal_iter::RwTraversalIterator, traversal_value::TraversalValue},
        types::GraphError,
    },
    utils::{id::v6_uuid, items::Edge, label_hash::hash_label, properties::ImmutablePropertiesMap},
};

#[cfg(feature = "lmdb")]
use heed3::PutFlags;

pub trait AddEAdapter<'db, 'arena, 'txn, 's>:
    Iterator<Item = Result<TraversalValue<'arena>, GraphError>>
{
    fn add_edge(
        self,
        label: &'arena str,
        properties: Option<ImmutablePropertiesMap<'arena>>,
        from_node: u128,
        to_node: u128,
        should_check: bool,
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
    AddEAdapter<'db, 'arena, 'txn, 's> for RwTraversalIterator<'db, 'arena, 'txn, I>
{
    #[inline(always)]
    #[allow(unused_variables)]
    fn add_edge(
        self,
        label: &'arena str,
        properties: Option<ImmutablePropertiesMap<'arena>>,
        from_node: u128,
        to_node: u128,
        should_check: bool,
    ) -> RwTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    > {
        let version = self.storage.version_info.get_latest(label);
        let edge = Edge {
            id: v6_uuid(),
            label,
            version,
            properties,
            from_node,
            to_node,
        };

        let mut result: Result<TraversalValue, GraphError> = Ok(TraversalValue::Empty);

        match edge.to_bincode_bytes() {
            Ok(bytes) => {
                if let Err(e) = self.storage.edges_db.put_with_flags(
                    self.txn,
                    PutFlags::APPEND,
                    &HelixGraphStorage::edge_key(edge.id),
                    &bytes,
                ) {
                    result = Err(GraphError::from(e));
                }
            }
            Err(e) => result = Err(GraphError::from(e)),
        }

        let label_hash = hash_label(edge.label, None);

        match self.storage.out_edges_db.put_with_flags(
            self.txn,
            PutFlags::APPEND_DUP,
            &HelixGraphStorage::out_edge_key(from_node, &label_hash),
            &HelixGraphStorage::pack_edge_data(edge.id, to_node),
        ) {
            Ok(_) => {}
            Err(e) => {
                println!(
                    "add_e => error adding out edge between {from_node:?} and {to_node:?}: {e:?}"
                );
                result = Err(GraphError::from(e));
            }
        }

        match self.storage.in_edges_db.put_with_flags(
            self.txn,
            PutFlags::APPEND_DUP,
            &HelixGraphStorage::in_edge_key(to_node, &label_hash),
            &HelixGraphStorage::pack_edge_data(edge.id, from_node),
        ) {
            Ok(_) => {}
            Err(e) => {
                println!(
                    "add_e => error adding in edge between {from_node:?} and {to_node:?}: {e:?}"
                );
                result = Err(GraphError::from(e));
            }
        }

        let result = match result {
            Ok(_) => Ok(TraversalValue::Edge(edge)),
            Err(e) => Err(e),
        };

        RwTraversalIterator {
            arena: self.arena,
            storage: self.storage,
            txn: self.txn,
            inner: std::iter::once(result), // TODO: change to support adding multiple edges
        }
    }
}

// RocksDB Implementation
#[cfg(feature = "rocks")]
impl<'db, 'arena, 'txn, 's, I: Iterator<Item = Result<TraversalValue<'arena>, GraphError>>>
    AddEAdapter<'db, 'arena, 'txn, 's> for RwTraversalIterator<'db, 'arena, 'txn, I>
{
    #[inline(always)]
    #[allow(unused_variables)]
    fn add_edge(
        self,
        label: &'arena str,
        properties: Option<ImmutablePropertiesMap<'arena>>,
        from_node: u128,
        to_node: u128,
        should_check: bool,
    ) -> RwTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    > {
        let version = self.storage.version_info.get_latest(label);
        let edge = Edge {
            id: v6_uuid(),
            label,
            version,
            properties,
            from_node,
            to_node,
        };

        let mut result: Result<TraversalValue, GraphError> = Ok(TraversalValue::Empty);

        match edge.to_bincode_bytes() {
            Ok(bytes) => {
                if let Err(e) = self.txn.put_cf(
                    &self.storage.cf_edges(),
                    HelixGraphStorage::edge_key(edge.id),
                    &bytes,
                ) {
                    result = Err(GraphError::from(e));
                }
            }
            Err(e) => result = Err(GraphError::from(e)),
        }

        let label_hash = hash_label(edge.label, None);

        // For RocksDB, the key includes from_node, label, to_node, and edge_id (52 bytes)
        // The value is empty
        let out_edge_key =
            HelixGraphStorage::out_edge_key(from_node, &label_hash, to_node, edge.id);
        match self
            .txn
            .put_cf(&self.storage.cf_out_edges(), out_edge_key, [])
        {
            Ok(_) => {}
            Err(e) => {
                println!(
                    "add_e => error adding out edge between {from_node:?} and {to_node:?}: {e:?}"
                );
                result = Err(GraphError::from(e));
            }
        }

        let in_edge_key = HelixGraphStorage::in_edge_key(to_node, &label_hash, from_node, edge.id);
        match self
            .txn
            .put_cf(&self.storage.cf_in_edges(), in_edge_key, [])
        {
            Ok(_) => {}
            Err(e) => {
                println!(
                    "add_e => error adding in edge between {from_node:?} and {to_node:?}: {e:?}"
                );
                result = Err(GraphError::from(e));
            }
        }

        let result = match result {
            Ok(_) => Ok(TraversalValue::Edge(edge)),
            Err(e) => Err(e),
        };

        RwTraversalIterator {
            arena: self.arena,
            storage: self.storage,
            txn: self.txn,
            inner: std::iter::once(result), // TODO: change to support adding multiple edges
        }
    }
}

#[cfg(feature = "slate")]
use crate::helix_engine::traversal_core::traversal_iter::AsyncRoTraversalIterator;
#[cfg(feature = "slate")]
use futures::Stream;

#[cfg(feature = "slate")]
pub trait AsyncAddEAdapter<'db, 'arena, 'txn>: Sized {
    fn add_e(
        self,
        label: &'arena str,
        properties: Option<ImmutablePropertiesMap<'arena>>,
        from_node: u128,
        to_node: u128,
        should_check: bool,
    ) -> AsyncRoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Stream<Item = Result<TraversalValue<'arena>, GraphError>>,
    >;
}

#[cfg(feature = "slate")]
impl<'db, 'arena, 'txn, I: Stream<Item = Result<TraversalValue<'arena>, GraphError>>>
    AsyncAddEAdapter<'db, 'arena, 'txn> for AsyncRoTraversalIterator<'db, 'arena, 'txn, I>
{
    fn add_e(
        self,
        label: &'arena str,
        properties: Option<ImmutablePropertiesMap<'arena>>,
        from_node: u128,
        to_node: u128,
        _should_check: bool,
    ) -> AsyncRoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Stream<Item = Result<TraversalValue<'arena>, GraphError>>,
    > {
        let iter = futures::stream::once(async move {
            let version = self.storage.version_info.get_latest(label);
            let edge = Edge {
                id: v6_uuid(),
                label,
                version,
                properties,
                from_node,
                to_node,
            };

            let mut result: Result<TraversalValue, GraphError> = Ok(TraversalValue::Empty);

            match edge.to_bincode_bytes() {
                Ok(bytes) => {
                    if let Err(e) = self.txn.put(HelixGraphStorage::edge_key(edge.id), &bytes) {
                        result = Err(GraphError::from(e));
                    }
                }
                Err(e) => result = Err(GraphError::from(e)),
            }

            let label_hash = hash_label(edge.label, None);

            // For RocksDB, the key includes from_node, label, to_node, and edge_id (52 bytes)
            // The value is empty
            let out_edge_key =
                HelixGraphStorage::out_edge_key(from_node, &label_hash, to_node, edge.id);
            match self.txn.put(out_edge_key, []) {
                Ok(_) => {}
                Err(e) => {
                    println!(
                        "add_e => error adding out edge between {from_node:?} and {to_node:?}: {e:?}"
                    );
                    result = Err(GraphError::from(e));
                }
            }

            let in_edge_key =
                HelixGraphStorage::in_edge_key(to_node, &label_hash, from_node, edge.id);
            match self.txn.put(in_edge_key, []) {
                Ok(_) => {}
                Err(e) => {
                    println!(
                        "add_e => error adding in edge between {from_node:?} and {to_node:?}: {e:?}"
                    );
                    result = Err(GraphError::from(e));
                }
            }

            let result = match result {
                Ok(_) => Ok(TraversalValue::Edge(edge)),
                Err(e) => Err(e),
            };
            result
        });

        AsyncRoTraversalIterator {
            storage: self.storage,
            arena: self.arena,
            txn: self.txn,
            inner: iter,
        }
    }
}
