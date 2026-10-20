//! Cost ordering helpers for physical alternatives.

use crate::{cost, physical};

use std::cmp::Ordering;

pub(crate) type CostOrderingKey = (u64, u64, u64, u64, u64, u64, u64, u64, usize);
pub(crate) type AlternativeOrderingKey = (CostOrderingKey, u8);

/// Orders costed alternatives by cost, then execution strategy, then their
/// stable digests, which are only computed when everything else ties.
pub(crate) fn compare_alternatives(
    (left, left_cost): (&physical::PhysicalAlternative, cost::CostVector),
    (right, right_cost): (&physical::PhysicalAlternative, cost::CostVector),
) -> Ordering {
    alternative_key_for_cost(left, left_cost)
        .cmp(&alternative_key_for_cost(right, right_cost))
        .then_with(|| left.digest().get().cmp(&right.digest().get()))
}

/// The digest-free prefix of the order [`compare_alternatives`] defines.
pub(crate) fn alternative_key_for_cost(
    alternative: &physical::PhysicalAlternative,
    cost: cost::CostVector,
) -> AlternativeOrderingKey {
    // Equal estimates must not discard proven bounded execution just because a
    // parameterized limit cannot be estimated. Cost still takes precedence;
    // the stable digest breaks ties within the same execution strategy.
    let materializes = u8::from(matches!(
        &alternative.expr,
        physical::PhysicalExpr::Rows(pipeline)
            if pipeline.execution() == crate::relational::RowExecution::Materialized
    ));
    (cost_key(cost), materializes)
}

pub(crate) fn cost_key(cost: cost::CostVector) -> CostOrderingKey {
    (
        cost.latency.as_micros(),
        cost.object_reads,
        cost.multi_get_calls,
        cost.range_seeks,
        cost.range_nexts,
        cost.cpu_units,
        cost.bytes.as_bytes(),
        cost.peak_memory.as_bytes(),
        cost.parallel_width,
    )
}
