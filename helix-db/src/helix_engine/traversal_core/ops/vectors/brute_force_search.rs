use crate::helix_engine::{
    traversal_core::{traversal_iter::RoTraversalIterator, traversal_value::TraversalValue},
    types::GraphError,
    vector_core::{
        distance::{Cosine, Distance},
        node::Item,
    },
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
        let arena = bumpalo::Bump::new();
        let iter = self
            .inner
            .filter_map(|v| match v {
                Ok(TraversalValue::Vector(mut v)) => {
                    let d = Cosine::distance(
                        v.data.as_ref().unwrap(),
                        &Item::<Cosine>::from(query, &arena),
                    );
                    v.set_distance(d as f64);
                    Some(v)
                }
                _ => None,
            })
            .sorted_by(|v1, v2| v1.partial_cmp(v2).unwrap())
            .take(k.try_into().unwrap())
            .filter_map(move |mut item| {
                match self
                    .storage
                    .vectors
                    .get_vector_properties(self.txn, item.id, self.arena)
                {
                    Ok(Some(vector_without_data)) => {
                        // todo!
                        // item.expand_from_vector_without_data(vector_without_data);
                        Some(item)
                    }

                    Ok(None) => None, // TODO: maybe should be an error?
                    Err(e) => {
                        println!("error getting vector data: {e:?}");
                        None
                    }
                }
            })
            .map(|v| Ok(TraversalValue::Vector(v)));

        RoTraversalIterator {
            storage: self.storage,
            arena: self.arena,
            txn: self.txn,
            inner: iter.into_iter(),
        }
    }
}
