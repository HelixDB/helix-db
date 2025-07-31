use crate::{protocol::value::Value, utils::filterable::Filterable};
use heed3::RoTxn;
use std::{cmp::Ordering, collections::BinaryHeap};

#[derive(PartialEq)]
pub(super) struct Candidate {
    pub id: u128,
    pub distance: f64,
}

impl Eq for Candidate {}

impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .distance
            .partial_cmp(&self.distance)
            .unwrap_or(Ordering::Equal)
    }
}

pub(super) trait HeapOps<T> {
    /// Extend the heap with another heap
    /// Used because using `.extend()` does not keep the order
    fn extend_inord(&mut self, other: BinaryHeap<T>)
    where
        T: Ord;

    /// Take the top k elements from the heap
    /// Used because using `.iter()` does not keep the order
    fn take_inord(&mut self, k: usize) -> BinaryHeap<T>
    where
        T: Ord;

    /// Get the maximum element from the heap
    fn get_max(&self) -> Option<&T>
    where
        T: Ord;

    fn to_vec_with_filter<F, const SHOULD_CHECK_DELETED: bool>(
        &mut self,
        k: usize,
        filter: Option<&[F]>,
        //label: &str,
        txn: &RoTxn,
    ) -> Vec<T>
    where
        T: Ord + Filterable,
        F: Fn(&T, &RoTxn) -> bool;
}

impl<T> HeapOps<T> for BinaryHeap<T> {
    #[inline(always)]
    fn extend_inord(&mut self, mut other: BinaryHeap<T>)
    where
        T: Ord,
    {
        self.reserve(other.len());
        for item in other.drain() {
            self.push(item);
        }
    }

    #[inline(always)]
    fn take_inord(&mut self, k: usize) -> BinaryHeap<T>
    where
        T: Ord,
    {
        let mut result = BinaryHeap::with_capacity(k);
        for _ in 0..k {
            if let Some(item) = self.pop() {
                result.push(item);
            } else {
                break;
            }
        }
        result
    }

    #[inline(always)]
    fn get_max(&self) -> Option<&T>
    where
        T: Ord,
    {
        self.iter().max()
    }

    #[inline(always)]
    fn to_vec_with_filter<F, const SHOULD_CHECK_DELETED: bool>(
        &mut self,
        k: usize,
        filter: Option<&[F]>,
        //label: &str,
        txn: &RoTxn,
    ) -> Vec<T>
    where
        T: Ord + Filterable,
        F: Fn(&T, &RoTxn) -> bool,
    {
        let mut result = Vec::with_capacity(k);
        for _ in 0..k {
            // while pop check filters and pop until one passes
            while let Some(item) = self.pop() {
                if SHOULD_CHECK_DELETED {
                    if let Ok(is_deleted) = item.check_property("is_deleted") {
                        if let Value::Boolean(is_deleted) = is_deleted.as_ref() {
                            if *is_deleted {
                                continue;
                            }
                        }
                    }
                }

                //if item.label() == label &&
                    if (filter.is_none() || filter.unwrap().iter().all(|f| f(&item, txn)))
                {
                    result.push(item);
                    break;
                }
            }
        }

        result
    }
}

