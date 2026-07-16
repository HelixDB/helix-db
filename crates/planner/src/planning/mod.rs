use std::time::Instant;

use crate::{context, error, exec};
use helix_ast::batch::{BatchQuery, ReadBatch, WriteBatch};

pub mod control_flow;
mod envelope;
mod executable;
mod index_ddl;
pub mod mutation;
pub mod search;
mod selected;

/// Plan a read or write batch into the executable DAG contract.
pub fn plan(
    query: &BatchQuery,
    ctx: &context::PlannerContext,
) -> Result<exec::ExecutablePlan, error::PlannerError> {
    executable::executable_from_query(query, ctx, Instant::now())
}

/// Plan a read batch into the executable DAG contract.
pub fn plan_read_batch(
    batch: &ReadBatch,
    ctx: &context::PlannerContext,
) -> Result<exec::ExecutablePlan, error::PlannerError> {
    executable::executable_from_read_batch(batch, ctx, Instant::now())
}

/// Plan a write batch into the executable DAG contract.
pub fn plan_write_batch(
    batch: &WriteBatch,
    ctx: &context::PlannerContext,
) -> Result<exec::ExecutablePlan, error::PlannerError> {
    executable::executable_from_write_batch(batch, ctx, Instant::now())
}

#[cfg(test)]
mod tests;
