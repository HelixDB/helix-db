use crate::{
    helix_engine::{
        storage_core::HelixGraphStorage,
        traversal_core::{traversal_iter::RwTraversalIterator, traversal_value::TraversalValue},
        types::GraphError,
    },
    utils::{id::v6_uuid, items::Edge, label_hash::hash_label, properties::ImmutablePropertiesMap},
};
use heed3::{PutFlags, RwTxn};

pub struct AddE<'db, 'arena, 'txn>
where
    'db: 'arena,
    'arena: 'txn,
{
    pub storage: &'db HelixGraphStorage,
    pub arena: &'arena bumpalo::Bump,
    pub txn: &'txn RwTxn<'db>,
    inner: std::iter::Once<Result<TraversalValue<'arena>, GraphError>>,
}

impl<'db, 'arena, 'txn> Iterator for AddE<'db, 'arena, 'txn> {
    type Item = Result<TraversalValue<'arena>, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

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
        is_unique: bool,
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
        is_unique: bool,
    ) -> RwTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    > {
        let result = (|| -> Result<TraversalValue<'arena>, GraphError> {
            let label_hash = hash_label(label, None);
            let out_key = HelixGraphStorage::out_edge_key(&from_node, &label_hash);

            if is_unique
                && let Some(iter) = self
                    .storage
                    .out_edges_db
                    .lazily_decode_data()
                    .get_duplicates(self.txn, &out_key)?
            {
                for item in iter {
                    let (_, data) = item?;
                    let data = data.decode().map_err(|e| GraphError::DecodeError(e.to_string()))?;
                    let (_, existing_to_node) = HelixGraphStorage::unpack_adj_edge_data(data)?;
                    if existing_to_node == to_node {
                        return Err(GraphError::DuplicateKey(format!(
                            "{label}:{from_node}->{to_node}"
                        )));
                    }
                }
            }

            let version = self.storage.version_info.get_latest(label);
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
                &out_key,
                &HelixGraphStorage::pack_edge_data(&edge.id, &to_node),
            )?;
            self.storage.in_edges_db.put_with_flags(
                self.txn,
                PutFlags::APPEND_DUP,
                &HelixGraphStorage::in_edge_key(&to_node, &label_hash),
                &HelixGraphStorage::pack_edge_data(&edge.id, &from_node),
            )?;

            Ok(TraversalValue::Edge(edge))
        })();

        RwTraversalIterator {
            arena: self.arena,
            storage: self.storage,
            txn: self.txn,
            inner: std::iter::once(result), // TODO: change to support adding multiple edges
        }
    }
}
