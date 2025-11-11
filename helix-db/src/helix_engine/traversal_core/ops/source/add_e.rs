use crate::{
    helix_engine::{
        storage_core::HelixGraphStorage,
        traversal_core::{traversal_iter::RwTraversalIterator, traversal_value::TraversalValue},
        types::GraphError,
    },
    utils::{id::v6_uuid, items::Edge, label_hash::hash_label, properties::ImmutablePropertiesMap},
};
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
                    HelixGraphStorage::edge_key(&edge.id),
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
            &HelixGraphStorage::out_edge_key(&from_node, &label_hash),
            &HelixGraphStorage::pack_edge_data(&edge.id, &to_node),
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
            &HelixGraphStorage::in_edge_key(&to_node, &label_hash),
            &HelixGraphStorage::pack_edge_data(&edge.id, &from_node),
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
