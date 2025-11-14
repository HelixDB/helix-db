use crate::{
    helix_engine::{
        traversal_core::{traversal_iter::RwTraversalIterator, traversal_value::TraversalValue},
        types::GraphError,
        vector_core::{HNSW, vector::HVector},
    },
    utils::properties::ImmutablePropertiesMap,
};

pub trait InsertVAdapter<'db, 'arena, 'txn>:
    Iterator<Item = Result<TraversalValue<'arena>, GraphError>>
{
    fn insert_v<F>(
        self,
        query: &'arena [f64],
        label: &'arena str,
        properties: Option<ImmutablePropertiesMap<'arena>>,
    ) -> RwTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    >
    where
        F: Fn(&HVector<'arena>, &Txn<'db>) -> bool;
}

#[cfg(feature = "lmdb")]
type Txn<'db> = heed3::RoTxn<'db>;
#[cfg(feature = "rocks")]
type Txn<'db> = rocksdb::Transaction<'db, rocksdb::TransactionDB>;

impl<'db, 'arena, 'txn, I: Iterator<Item = Result<TraversalValue<'arena>, GraphError>>>
    InsertVAdapter<'db, 'arena, 'txn> for RwTraversalIterator<'db, 'arena, 'txn, I>
{
    fn insert_v<F>(
        self,
        query: &'arena [f64],
        label: &'arena str,
        properties: Option<ImmutablePropertiesMap<'arena>>,
    ) -> RwTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    >
    where
        F: Fn(&HVector<'arena>, &Txn<'db>) -> bool,
    {
        let vector: Result<HVector<'arena>, crate::helix_engine::types::VectorError> = self
            .storage
            .vectors
            .insert::<F>(self.txn, label, query, properties, self.arena);

        let result = match vector {
            Ok(vector) => Ok(TraversalValue::Vector(vector)),
            Err(e) => Err(GraphError::from(e)),
        };

        RwTraversalIterator {
            inner: std::iter::once(result),
            storage: self.storage,
            arena: self.arena,
            txn: self.txn,
        }
    }
}
