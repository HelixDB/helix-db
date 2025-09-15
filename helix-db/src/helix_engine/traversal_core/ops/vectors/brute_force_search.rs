use std::sync::Arc;

use crate::{
    debug_println,
    helix_engine::{
        traversal_core::{traversal_iter::RoTraversalIterator, traversal_value::TraversalValue},
        types::GraphError,
        vector_core::{
            vector::HVector,
            vector_distance::DistanceCalc,
        },
    },
    protocol::value::Value,
    utils::filterable::Filterable,
};
use helix_macros::debug_trace;
use itertools::Itertools;

pub struct BruteForceSearchV<I: Iterator<Item = Result<TraversalValue, GraphError>>> {
    iter: I,
}

// implementing iterator for OutIterator
impl<I: Iterator<Item = Result<TraversalValue, GraphError>>> Iterator for BruteForceSearchV<I> {
    type Item = Result<TraversalValue, GraphError>;

    #[debug_trace("BRUTE_FORCE_SEARCH_V")]
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

pub trait BruteForceSearchVAdapter<'a>:
    Iterator<Item = Result<TraversalValue, GraphError>>
{
    fn brute_force_search_v<K>(
        self,
        query: &[f64],
        k: K,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalValue, GraphError>>>
    where
        K: TryInto<usize>,
        K::Error: std::fmt::Debug;
}

impl<'a, I: Iterator<Item = Result<TraversalValue, GraphError>> + 'a> BruteForceSearchVAdapter<'a>
    for RoTraversalIterator<'a, I>
{
    fn brute_force_search_v<K>(
        self,
        query: &[f64],
        k: K,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalValue, GraphError>>>
    where
        K: TryInto<usize>,
        K::Error: std::fmt::Debug,
    {
        let storage = Arc::clone(&self.storage);
        let txn = self.txn;

        let iter = self
            .inner
            .filter_map(|v| match v {
                Ok(TraversalValue::Vector(mut v)) => {
                    let d = HVector::distance(v.get_data(), query, &self.storage.vectors.method).unwrap();
                    v.set_distance(d);
                    Some(v)
                }
                _ => None,
            })
            .sorted_by(|v1, v2| v1.partial_cmp(v2).unwrap())
            .take(k.try_into().unwrap())
            .filter_map(move |mut item| {
                item.properties = match storage
                    .vectors
                    .vector_data_db
                    .get(txn, &item.get_id().to_be_bytes())
                {
                    Ok(Some(bytes)) => Some(
                        bincode::deserialize(bytes)
                            .map_err(GraphError::from)
                            .unwrap(),
                    ),
                    Ok(None) => None, // TODO: maybe should be an error?
                    Err(e) => {
                        println!("error getting vector data: {e:?}");
                        return None;
                    }
                };
                debug_println!("item: {item:?}");

                if let Ok(is_deleted) = item.check_property("is_deleted") {
                    debug_println!("is_deleted: {is_deleted:?}");
                    if let Value::Boolean(is_deleted) = is_deleted.as_ref() {
                        if *is_deleted { None } else { Some(item) }
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .map(|v| Ok(TraversalValue::Vector(v)));

        RoTraversalIterator {
            inner: iter.into_iter(),
            storage: self.storage,
            txn: self.txn,
        }
    }
}
