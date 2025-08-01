use std::sync::Arc;

use super::super::tr_val::TraversalVal;
use crate::{
    helix_engine::{
        graph_core::traversal_iter::RoTraversalIterator, types::GraphError,
        vector_core::vector_distance::cosine_similarity,
    },
    protocol::value::Value,
    utils::filterable::Filterable,
};
use helix_macros::debug_trace;
use itertools::Itertools;

pub struct BruteForceSearchV<I: Iterator<Item = Result<TraversalVal, GraphError>>> {
    iter: I,
}

// implementing iterator for OutIterator
impl<I: Iterator<Item = Result<TraversalVal, GraphError>>> Iterator for BruteForceSearchV<I> {
    type Item = Result<TraversalVal, GraphError>;

    #[debug_trace("BRUTE_FORCE_SEARCH_V")]
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

pub trait BruteForceSearchVAdapter<'a>: Iterator<Item = Result<TraversalVal, GraphError>> {
    fn brute_force_search_v<K>(
        self,
        query: &[f64],
        k: K,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>>
    where
        K: TryInto<usize>,
        K::Error: std::fmt::Debug;
}

impl<'a, I: Iterator<Item = Result<TraversalVal, GraphError>> + 'a> BruteForceSearchVAdapter<'a>
    for RoTraversalIterator<'a, I>
{
    fn brute_force_search_v<K>(
        self,
        query: &[f64],
        k: K,
    ) -> RoTraversalIterator<'a, impl Iterator<Item = Result<TraversalVal, GraphError>>>
    where
        K: TryInto<usize>,
        K::Error: std::fmt::Debug,
    {
        let iter = self.inner.map(|v| match v {
            Ok(TraversalVal::Vector(mut v)) => {
                let d = cosine_similarity(v.get_data(), query).unwrap();
                v.set_distance(d);
                v
            }
            other => {
                println!("expected vector traversal values, got: {other:?}");
                panic!("expected vector traversal values")
            }
        });

        let storage = Arc::clone(&self.storage);
        let txn = self.txn;

        let iter = iter
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
                println!("item: {item:?}");

                if let Ok(is_deleted) = item.check_property("is_deleted") {
                    println!("is_deleted: {is_deleted:?}");
                    if let Value::Boolean(is_deleted) = is_deleted.as_ref() {
                        if *is_deleted {
                            None
                        } else {
                            Some(item)
                        }
                    } else {
                        None
                    }
                } else {
                    None
                }

                // get properties
            })
            .map(|v| Ok(TraversalVal::Vector(v)));

        RoTraversalIterator {
            inner: iter.into_iter(),
            storage: self.storage,
            txn: self.txn,
        }
    }
}
