//! Cost ordering helpers for physical alternatives.

use crate::{cost, physical};

type CostOrderingKey = (u64, u64, u64, u64, u64, u64, u64, u64, usize);
pub(super) type AlternativeOrderingKey = (CostOrderingKey, u64);

pub(super) fn alternative_key_for_cost(
    alternative: &physical::PhysicalAlternative,
    cost: cost::CostVector,
) -> AlternativeOrderingKey {
    (cost_key(cost), alternative.digest.get())
}

fn cost_key(cost: cost::CostVector) -> CostOrderingKey {
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
