use std::time::Instant;

use helix_ast::batch::{BatchQuery, ReadBatch, WriteBatch};

use crate::{context, error, exec};

use super::{envelope, selected};

pub(crate) fn executable_from_query(
    query: &BatchQuery,
    ctx: &context::PlannerContext,
    started: Instant,
) -> Result<exec::ExecutablePlan, error::PlannerError> {
    let envelope = envelope::PlanEnvelope::from_query(query)?;
    let entries = selected::cascades_batch_entries_from_ast(query, ctx)?;
    executable_from_selected_entries(envelope, entries, ctx, started)
}

pub(crate) fn executable_from_read_batch(
    batch: &ReadBatch,
    ctx: &context::PlannerContext,
    started: Instant,
) -> Result<exec::ExecutablePlan, error::PlannerError> {
    let envelope = envelope::PlanEnvelope::read(batch)?;
    let entries = selected::cascades_batch_entries_from_ast_entries(
        batch.entries.as_slice(),
        ctx,
        error::BatchOp::Batch,
    )?;
    executable_from_selected_entries(envelope, entries, ctx, started)
}

pub(crate) fn executable_from_write_batch(
    batch: &WriteBatch,
    ctx: &context::PlannerContext,
    started: Instant,
) -> Result<exec::ExecutablePlan, error::PlannerError> {
    let envelope = envelope::PlanEnvelope::write(batch)?;
    let entries = selected::cascades_batch_entries_from_ast_entries(
        batch.entries.as_slice(),
        ctx,
        error::BatchOp::Batch,
    )?;
    executable_from_selected_entries(envelope, entries, ctx, started)
}

fn executable_from_selected_entries(
    mut envelope: envelope::PlanEnvelope,
    selected: (exec::SelectedExecutableBatchEntries, exec::PlannerMetrics),
    ctx: &context::PlannerContext,
    started: Instant,
) -> Result<exec::ExecutablePlan, error::PlannerError> {
    let (entries, mut metrics) = selected;
    metrics.optimization_micros = started.elapsed().as_micros() as u64;
    selected::append_selected_trace(&mut envelope.trace, &entries);

    match exec::ExecutablePlan::from_selected_executable_batch(
        exec::SelectedExecutableBatchPlanRequest {
            kind: envelope.kind,
            returns: envelope.returns,
            trace: envelope.trace,
            metrics,
            entries,
            profile: &ctx.storage,
        },
    ) {
        Ok(executable) => Ok(executable),
        Err(exec::ExecPlanError::UnsupportedSelectedExecutableAlternative { reason }) => {
            Err(error::PlannerError::UnsupportedCascadesPlan {
                reason: reason.to_string(),
            })
        }
        Err(error) => Err(error::PlannerError::InvalidExecutablePlan { error }),
    }
}
