use crate::helix_engine::{
    traversal_core::{traversal_iter::RoTraversalIterator, traversal_value::TraversalValue},
    types::GraphError,
    vector_core::{binary_heap::BinaryHeap, vector::HVector, vector_distance::cosine_similarity},
};
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
        let k: usize = k.try_into().unwrap();
        let arena = self.arena;
        let storage = self.storage;
        let txn = self.txn;
        let mut heap: BinaryHeap<'arena, HVector<'arena>> = BinaryHeap::with_capacity(arena, k);

        for item in self.inner {
            if let Ok(TraversalValue::Vector(mut v)) = item {
                let sim = cosine_similarity(v.data, query).unwrap();
                v.set_distance(sim);

                if heap.len() < k {
                    heap.push(v);
                } else if let Some(min) = heap.peek() {
                    if sim > min.get_distance() {
                        heap.pop();
                        heap.push(v);
                    }
                }
            }
        }
        let mut results = bumpalo::collections::Vec::with_capacity_in(heap.len(), arena);
        while let Some(v) = heap.pop() {
            results.push(v);
        }
        results.reverse();

        let iter = results
            .into_iter()
            .filter_map(move |mut item| {
                match storage
                    .vectors
                    .get_vector_properties(txn, *item.id(), arena)
                {
                    Ok(Some(vector_without_data)) => {
                        item.expand_from_vector_without_data(vector_without_data);
                        Some(item)
                    }
                    Ok(None) => None,
                    Err(e) => {
                        println!("error getting vector data: {e:?}");
                        None
                    }
                }
            })
            .map(|v| Ok(TraversalValue::Vector(v)));

        RoTraversalIterator {
            storage,
            arena,
            txn,
            inner: iter.into_iter(),
        }
    }
}
