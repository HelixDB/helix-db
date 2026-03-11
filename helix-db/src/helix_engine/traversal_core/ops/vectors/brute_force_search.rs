use crate::helix_engine::{
    traversal_core::{traversal_iter::RoTraversalIterator, traversal_value::TraversalValue},
    types::GraphError,
    vector_core::vector_distance::cosine_similarity,
};
use itertools::Itertools;

pub trait BruteForceSearchVAdapter<'db, 'arena, 'txn>:
    Iterator<Item = Result<TraversalValue<'arena>, GraphError>>
{
    fn brute_force_search_v<K>(
        self,
        query: &'arena [f64],
        k: K,
    ) -> RoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    >
    where
        K: TryInto<usize>,
        K::Error: std::fmt::Debug;
}

impl<'db, 'arena, 'txn, I: Iterator<Item = Result<TraversalValue<'arena>, GraphError>>>
    BruteForceSearchVAdapter<'db, 'arena, 'txn> for RoTraversalIterator<'db, 'arena, 'txn, I>
{
    fn brute_force_search_v<K>(
        self,
        query: &'arena [f64],
        k: K,
    ) -> RoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    >
    where
        K: TryInto<usize>,
        K::Error: std::fmt::Debug,
    {
        let k = k.try_into().unwrap();
        let mut vectors = Vec::new();
        let mut errors = Vec::new();

        for item in self.inner {
            let mut vector = match item {
                Ok(TraversalValue::Vector(vector)) => vector,
                Ok(TraversalValue::VectorNodeWithoutVectorData(vector_without_data)) => {
                    match self.storage.vectors.get_full_vector(
                        self.txn,
                        *vector_without_data.id(),
                        self.arena,
                    ) {
                        Ok(vector) => vector,
                        Err(e) => {
                            errors.push(Err(GraphError::from(e)));
                            continue;
                        }
                    }
                }
                Ok(_) => continue,
                Err(e) => {
                    errors.push(Err(e));
                    continue;
                }
            };

            match cosine_similarity(vector.data, query) {
                Ok(distance) => {
                    vector.set_distance(distance);
                    vectors.push(vector);
                }
                Err(e) => errors.push(Err(GraphError::from(e))),
            }
        }

        vectors.sort_by(|v1, v2| v1.partial_cmp(v2).unwrap());

        let mut ranked_results = Vec::new();
        for mut item in vectors.into_iter().take(k) {
            match self
                .storage
                .vectors
                .get_vector_properties(self.txn, *item.id(), self.arena)
            {
                Ok(Some(vector_without_data)) => {
                    item.expand_from_vector_without_data(vector_without_data);
                    ranked_results.push(Ok(TraversalValue::Vector(item)));
                }
                Ok(None) => {
                    ranked_results.push(Err(GraphError::VectorError(format!(
                        "vector metadata not found for id {}",
                        item.id()
                    ))));
                }
                Err(e) => ranked_results.push(Err(GraphError::from(e))),
            }
        }

        ranked_results.extend(errors);

        RoTraversalIterator {
            storage: self.storage,
            arena: self.arena,
            txn: self.txn,
            inner: ranked_results.into_iter(),
        }
    }
}
