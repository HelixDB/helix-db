//! Cost ordering helpers for physical alternatives.

use crate::{cost, physical};

type CostOrderingKey = (u64, u64, u64, u64, u64, u64, u64, u64, usize);
pub(crate) type AlternativeOrderingKey = (CostOrderingKey, u8, u64);

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
    (cost_key(cost), materializes, alternative.digest.get())
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
