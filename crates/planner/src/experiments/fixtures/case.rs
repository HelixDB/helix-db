//! Executable planner case for a scalability fixture.

use helix_ast::batch;

use crate::{context, error, exec};

use super::super::metrics;
use super::{PlanScalabilityFixture, PlanningScalabilityWorkload};

/// Planner input and thresholds for a scalability fixture.
#[derive(Debug, Clone)]
pub struct PlanningScalabilityCase {
    fixture: PlanScalabilityFixture,
    ctx: context::PlannerContext,
    workload: PlanningScalabilityWorkload,
    thresholds: metrics::PlannerMetricThresholds,
}

impl PlanningScalabilityCase {
    pub(super) const fn new(
        fixture: PlanScalabilityFixture,
        ctx: context::PlannerContext,
        workload: PlanningScalabilityWorkload,
        thresholds: metrics::PlannerMetricThresholds,
    ) -> Self {
        Self {
            fixture,
            ctx,
            workload,
            thresholds,
        }
    }

    /// Fixture identity.
    pub const fn fixture(&self) -> PlanScalabilityFixture {
        self.fixture
    }

    /// Planner context for this fixture.
    pub const fn context(&self) -> &context::PlannerContext {
        &self.ctx
    }

    /// Typed planner workload for this fixture.
    pub const fn workload(&self) -> &PlanningScalabilityWorkload {
        &self.workload
    }

    /// Read batch for read workloads.
    pub const fn read_batch(&self) -> Option<&batch::ReadBatch> {
        self.workload.read_batch()
    }

    /// Write batch for write workloads.
    pub const fn write_batch(&self) -> Option<&batch::WriteBatch> {
        self.workload.write_batch()
    }

    /// Deterministic metric thresholds for this fixture.
    pub const fn thresholds(&self) -> metrics::PlannerMetricThresholds {
        self.thresholds
    }

    /// Plan this fixture.
    pub fn plan(&self) -> Result<exec::ExecutablePlan, error::PlannerError> {
        self.workload.plan_with_context(&self.ctx)
    }

    /// Plan this fixture against an alternate planner context.
    pub fn plan_with_context(
        &self,
        ctx: &context::PlannerContext,
    ) -> Result<exec::ExecutablePlan, error::PlannerError> {
        self.workload.plan_with_context(ctx)
    }

    /// Plan this fixture and validate deterministic regression thresholds.
    pub fn plan_checked(&self) -> Result<exec::ExecutablePlan, metrics::PlanningRegressionError> {
        let plan = self.plan()?;
        self.thresholds.check(plan.metrics())?;
        Ok(plan)
    }
}
