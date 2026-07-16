//! Parallel cost helper algorithms.

use std::cmp::Reverse;
use std::collections::BinaryHeap;

use crate::properties::PositiveUsize;

use super::{ByteEstimate, CostVector};

pub(super) fn bounded_peak_memory(
    children: &[CostVector],
    max_concurrency: PositiveUsize,
) -> ByteEstimate {
    let mut largest = BinaryHeap::<Reverse<ByteEstimate>>::new();
    let max_concurrency = max_concurrency.get();
    for peak in children.iter().map(|cost| cost.peak_memory) {
        if largest.len() < max_concurrency {
            largest.push(Reverse(peak));
        } else if largest
            .peek()
            .is_some_and(|Reverse(smallest)| peak > *smallest)
        {
            largest.pop();
            largest.push(Reverse(peak));
        }
    }
    largest
        .into_iter()
        .map(|Reverse(peak)| peak)
        .fold(ByteEstimate::ZERO, ByteEstimate::saturating_add)
}
