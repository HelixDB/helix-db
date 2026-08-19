//! Selected exact cardinality root contract.

use super::{
    SelectedExecutableRunRoot, SelectedPhysicalPlan, SelectedRootConstructionError,
    SelectedRootProvenance,
};
use crate::{exec, physical};

/// Selected input encoded for the physical count program.
#[derive(Debug, Clone, PartialEq)]
pub enum SelectedCountInput {
    /// The selected count program performs its reads directly.
    Direct,
    /// Exactly one dependency supplies execution rows.
    Rows(Box<SelectedExecutableRunRoot>),
    /// Exactly one dependency supplies scalar items.
    Scalars(Box<SelectedExecutableRunRoot>),
}

impl SelectedCountInput {
    const fn dependency(&self) -> exec::ExecCountDependency {
        match self {
            Self::Direct => exec::ExecCountDependency::Direct,
            Self::Rows(_) => exec::ExecCountDependency::Rows,
            Self::Scalars(_) => exec::ExecCountDependency::Scalars,
        }
    }
}

/// Selected cardinality root ready for one-to-one executable lowering.
#[derive(Debug, Clone, PartialEq)]
pub struct SelectedRootCount {
    alternative: SelectedPhysicalPlan,
    provenance: SelectedRootProvenance,
    input: SelectedCountInput,
    plan: exec::ExecCountPlan,
}

impl SelectedRootCount {
    /// Build a selected count root and validate its exact input contract.
    pub fn new(
        alternative: SelectedPhysicalPlan,
        provenance: SelectedRootProvenance,
        input: SelectedCountInput,
    ) -> Result<Self, SelectedRootConstructionError> {
        let physical::PhysicalExpr::Cardinality(physical) = alternative.expr() else {
            return Err(SelectedRootConstructionError::IncompatiblePhysicalShape);
        };
        let plan = physical.executable().clone();
        let dependency = plan
            .validated_dependency()
            .map_err(|_| SelectedRootConstructionError::CountInputMismatch)?;
        if dependency != input.dependency() {
            return Err(SelectedRootConstructionError::CountInputMismatch);
        }
        Ok(Self {
            alternative,
            provenance,
            input,
            plan,
        })
    }

    /// Decompose the selected count root for executable lowering.
    pub fn into_parts(
        self,
    ) -> (
        SelectedPhysicalPlan,
        SelectedRootProvenance,
        SelectedCountInput,
        exec::ExecCountPlan,
    ) {
        (self.alternative, self.provenance, self.input, self.plan)
    }

    /// Selected physical contract.
    pub const fn alternative(&self) -> &SelectedPhysicalPlan {
        &self.alternative
    }

    /// Optimizer provenance.
    pub const fn provenance(&self) -> &SelectedRootProvenance {
        &self.provenance
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{cost, logical, properties};

    fn provenance() -> SelectedRootProvenance {
        super::super::provenance::test_selected_root_provenance()
    }

    fn physical(plan: exec::ExecCountPlan) -> SelectedPhysicalPlan {
        SelectedPhysicalPlan::new(
            physical::PhysicalExpr::Cardinality(Box::new(physical::PhysicalCountPlan::new(plan))),
            properties::DeliveredProperties::default(),
            cost::CostVector::ZERO,
        )
    }

    fn child() -> SelectedExecutableRunRoot {
        SelectedExecutableRunRoot::alternative(
            logical::LogicalExpr::Pure(logical::PureLogicalOp::NoOp),
            physical::PhysicalAlternative::new(
                physical::PhysicalExpr::NoOp,
                properties::DeliveredProperties::default(),
                cost::CostVector::ZERO,
            ),
        )
    }

    #[test]
    fn selected_count_constructor_enforces_direct_row_and_scalar_inputs() {
        assert!(SelectedRootCount::new(
            physical(exec::ExecCountPlan::Constant(0)),
            provenance(),
            SelectedCountInput::Direct,
        )
        .is_ok());
        assert_eq!(
            SelectedRootCount::new(
                physical(exec::ExecCountPlan::InputRows {
                    window: exec::ExecCountWindowPlan::identity(),
                }),
                provenance(),
                SelectedCountInput::Direct,
            ),
            Err(SelectedRootConstructionError::CountInputMismatch)
        );
        assert!(SelectedRootCount::new(
            physical(exec::ExecCountPlan::InputRows {
                window: exec::ExecCountWindowPlan::identity(),
            }),
            provenance(),
            SelectedCountInput::Rows(Box::new(child())),
        )
        .is_ok());
        assert!(SelectedRootCount::new(
            physical(exec::ExecCountPlan::InputScalars {
                window: exec::ExecCountWindowPlan::identity(),
            }),
            provenance(),
            SelectedCountInput::Scalars(Box::new(child())),
        )
        .is_ok());
    }

    #[test]
    fn selected_count_constructor_rejects_wrong_and_malformed_physical_shapes() {
        let wrong = SelectedPhysicalPlan::new(
            physical::PhysicalExpr::NoOp,
            properties::DeliveredProperties::default(),
            cost::CostVector::ZERO,
        );
        assert_eq!(
            SelectedRootCount::new(wrong, provenance(), SelectedCountInput::Direct),
            Err(SelectedRootConstructionError::IncompatiblePhysicalShape)
        );

        let malformed = exec::ExecCountPlan::Stream(exec::ExecCountStreamPlan {
            cursor: exec::ExecCountCursorPlan::Intersect {
                driver: Box::new(exec::ExecCountCursorPlan::InputRows),
                rest: crate::ir::AtLeast::from_one(exec::ExecCountCursorPlan::InputRows),
            },
            window: exec::ExecCountWindowPlan::identity(),
        });
        assert_eq!(
            SelectedRootCount::new(
                physical(malformed),
                provenance(),
                SelectedCountInput::Rows(Box::new(child())),
            ),
            Err(SelectedRootConstructionError::CountInputMismatch)
        );
    }
}
