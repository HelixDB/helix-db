use std::collections::HashSet;

use crate::helix_engine::{
    storage_core::HelixGraphStorage,
    traversal_core::{traversal_iter::RoTraversalIterator, traversal_value::TraversalValue},
    types::GraphError,
};
use heed3::RoTxn;

pub trait IntersectAdapter<'db, 'arena, 'txn>: Iterator {
    /// Computes a set intersection across per-item sub-traversal results.
    ///
    /// `intersect` runs `f` once for each upstream traversal item, then keeps only
    /// element IDs that appear in every sub-traversal output.
    ///
    /// Semantics:
    /// - Intersection is ID-based (works uniformly for nodes, edges, and vectors).
    /// - Empty upstream input returns an empty iterator.
    /// - Any upstream/sub-traversal error short-circuits and returns that error.
    /// - Returned values are sourced from the smallest sub-result for efficiency.
    fn intersect<F>(
        self,
        f: F,
    ) -> RoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    >
    where
        F: Fn(
            TraversalValue<'arena>,
            &'db HelixGraphStorage,
            &'txn RoTxn<'db>,
            &'arena bumpalo::Bump,
        ) -> Result<Vec<TraversalValue<'arena>>, GraphError>;
}

impl<'db, 'arena, 'txn, I: Iterator<Item = Result<TraversalValue<'arena>, GraphError>>>
    IntersectAdapter<'db, 'arena, 'txn> for RoTraversalIterator<'db, 'arena, 'txn, I>
{
    fn intersect<F>(
        self,
        f: F,
    ) -> RoTraversalIterator<
        'db,
        'arena,
        'txn,
        impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
    >
    where
        F: Fn(
            TraversalValue<'arena>,
            &'db HelixGraphStorage,
            &'txn RoTxn<'db>,
            &'arena bumpalo::Bump,
        ) -> Result<Vec<TraversalValue<'arena>>, GraphError>,
    {
        let storage = self.storage;
        let txn = self.txn;
        let arena = self.arena;

        // Materialize upstream items so we can evaluate all per-item sub-traversals
        // before computing a global intersection. Any upstream error short-circuits.
        let mut upstream: Vec<TraversalValue<'arena>> = Vec::new();
        for item in self.inner {
            match item {
                Ok(val) => upstream.push(val),
                Err(e) => {
                    return RoTraversalIterator {
                        storage,
                        arena,
                        txn,
                        inner: vec![Err(e)].into_iter(),
                    };
                }
            }
        }

        // INTERSECT over an empty upstream has no candidates.
        if upstream.is_empty() {
            return RoTraversalIterator {
                storage,
                arena,
                txn,
                inner: Vec::new().into_iter(),
            };
        }

        // Execute the sub-traversal for every upstream value and collect each result set.
        // Any sub-traversal error short-circuits.
        let mut all_results: Vec<Vec<TraversalValue<'arena>>> = Vec::new();
        for item in upstream {
            match f(item, storage, txn, arena) {
                Ok(results) => all_results.push(results),
                Err(e) => {
                    return RoTraversalIterator {
                        storage,
                        arena,
                        txn,
                        inner: vec![Err(e)].into_iter(),
                    };
                }
            }
        }

        // Start from the smallest set so retained candidates shrink as quickly as possible.
        all_results.sort_by_key(|r| r.len());

        // Seed with IDs from the smallest set.
        let first = all_results.remove(0);
        let mut intersection: HashSet<u128> = first.iter().map(|v| v.id()).collect();

        // Retain only IDs shared with each remaining set.
        for results in &all_results {
            let id_set: HashSet<u128> = results.iter().map(|v| v.id()).collect();
            intersection.retain(|id| id_set.contains(id));
            // Early exit once no shared IDs remain.
            if intersection.is_empty() {
                return RoTraversalIterator {
                    storage,
                    arena,
                    txn,
                    inner: Vec::new().into_iter(),
                };
            }
        }

        // Rehydrate result values from `first` so output items stay as TraversalValue.
        // This also preserves ordering from the chosen seed result set.
        let result: Vec<Result<TraversalValue<'arena>, GraphError>> = first
            .into_iter()
            .filter(|v| intersection.contains(&v.id()))
            .map(Ok)
            .collect();

        RoTraversalIterator {
            storage,
            arena,
            txn,
            inner: result.into_iter(),
        }
    }
}
