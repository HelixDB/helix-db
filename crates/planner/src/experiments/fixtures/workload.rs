//! Typed planner workload carried by scalability fixtures.

use helix_ast::batch;

use crate::{context, error, exec, planning};

/// Planner input for one scalability fixture.
///
/// Keeping read and write batches in a closed enum makes the benchmark and
/// regression harness dispatch through the correct planner entrypoint.
///
/// ```
/// use helix_ast::batch;
/// use helix_planner::experiments::PlanningScalabilityWorkload;
///
/// let workload = PlanningScalabilityWorkload::Read(batch::ReadBatch::new());
/// assert!(workload.read_batch().is_some());
/// assert!(workload.write_batch().is_none());
/// ```
#[derive(Debug, Clone, PartialEq)]
pub enum PlanningScalabilityWorkload {
    /// Read-only query batch.
    Read(batch::ReadBatch),
    /// Write-capable query batch.
    Write(batch::WriteBatch),
}

impl PlanningScalabilityWorkload {
    /// Return the contained read batch, if this is a read workload.
    pub const fn read_batch(&self) -> Option<&batch::ReadBatch> {
        match self {
            Self::Read(batch) => Some(batch),
            Self::Write(_) => None,
        }
    }

    /// Return the contained write batch, if this is a write workload.
    pub const fn write_batch(&self) -> Option<&batch::WriteBatch> {
        match self {
            Self::Read(_) => None,
            Self::Write(batch) => Some(batch),
        }
    }

    /// Plan the workload with the supplied context.
    pub fn plan_with_context(
        &self,
        ctx: &context::PlannerContext,
    ) -> Result<exec::ExecutablePlan, error::PlannerError> {
        match self {
            Self::Read(batch) => planning::plan_read_batch(batch, ctx),
            Self::Write(batch) => planning::plan_write_batch(batch, ctx),
        }
    }
}
