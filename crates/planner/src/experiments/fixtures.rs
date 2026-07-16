//! Planner scalability fixture facade.
//!
//! Fixture identity, planner input construction, catalog/stat context setup,
//! deterministic thresholds, and executable planning live in sibling modules so
//! scalability experiment logic stays independently testable.

mod batch;
mod case;
mod context;
mod shape;
#[cfg(test)]
mod tests;
mod thresholds;
mod workload;

use serde::{Deserialize, Serialize};

use crate::properties;
use crate::{error, exec};

pub use self::case::PlanningScalabilityCase;
pub use self::shape::PlanningScalabilityShape;
pub use self::workload::PlanningScalabilityWorkload;

/// One concrete planning scalability fixture.
///
/// ```
/// use helix_planner::experiments::{PlanScalabilityFixture, PlanningScalabilityShape};
///
/// let fixture = PlanScalabilityFixture::new(
///     PlanningScalabilityShape::WideBooleanPredicates,
///     8,
/// )
/// .unwrap();
///
/// assert_eq!(fixture.scale().get(), 8);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PlanScalabilityFixture {
    shape: PlanningScalabilityShape,
    scale: properties::PositiveUsize,
}

impl PlanScalabilityFixture {
    /// Build a fixture, rejecting zero scale.
    pub fn new(shape: PlanningScalabilityShape, scale: usize) -> Option<Self> {
        properties::PositiveUsize::new(scale).map(|scale| Self { shape, scale })
    }

    /// Fixture family.
    pub const fn shape(self) -> PlanningScalabilityShape {
        self.shape
    }

    /// Positive fixture scale.
    pub const fn scale(self) -> properties::PositiveUsize {
        self.scale
    }

    /// Build the planner input and deterministic thresholds for this fixture.
    pub fn case(self) -> PlanningScalabilityCase {
        PlanningScalabilityCase::new(
            self,
            context::context_for(self.shape, self.scale),
            batch::workload_for(self.shape, self.scale),
            thresholds::thresholds_for(self.shape, self.scale),
        )
    }

    /// Plan the fixture query.
    pub fn plan(self) -> Result<exec::ExecutablePlan, error::PlannerError> {
        self.case().plan()
    }
}
