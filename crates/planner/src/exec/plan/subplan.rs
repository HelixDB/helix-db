use serde::{Deserialize, Serialize};

use crate::{cost, ir, logical, physical};

use crate::exec::validation::execution_order;
use crate::exec::{
    selected, ExecCondition, ExecExecutionOrder, ExecPlanError, ExecStep, ExecStepId,
};

/// Validated executable subplan used by nested operators.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "ExecutableSubplanUnchecked")]
pub struct ExecutableSubplan {
    /// DAG steps.
    steps: ir::AtLeast<ExecStep, 1>,
    /// Root step.
    root: ExecStepId,
    /// Deterministic interpreter-ready execution stages, derived during
    /// validation and skipped in serde because it is redundant with `steps`.
    #[serde(skip)]
    execution_order: ExecExecutionOrder,
}

impl ExecutableSubplan {
    /// Build and validate an executable subplan.
    pub fn new(steps: ir::AtLeast<ExecStep, 1>, root: ExecStepId) -> Result<Self, ExecPlanError> {
        let execution_order = execution_order(&steps, root)?;
        Ok(Self {
            steps,
            root,
            execution_order,
        })
    }

    /// Lower a Cascades-selected standalone physical alternative into a native
    /// executable subplan.
    ///
    /// This boundary is intentionally strict: it only accepts alternatives whose
    /// source logical expression carries enough operator detail to build a real
    /// executable DAG. Detail-free physical operators are rejected instead of
    /// being guessed.
    ///
    /// ```
    /// use helix_planner::{cost, exec, ir, logical, physical, properties};
    ///
    /// let ids = ir::ElementIds::new(ir::AtLeast::<_, 1>::from_one(7)).unwrap();
    /// let source = logical::LogicalExpr::AccessPath(logical::AccessPath::Node(
    ///     logical::NodeAccessPath::new(
    ///         ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::PointIds { ids }).unwrap(),
    ///     ),
    /// ));
    /// let alternative = physical::PhysicalAlternative::new(
    ///     physical::PhysicalExpr::Access {
    ///         element: properties::ElementKind::Node,
    ///         access: physical::PhysicalAccess::PointReads {
    ///             locality: properties::KeyLocality::Unknown,
    ///         },
    ///     },
    ///     properties::DeliveredProperties {
    ///         element: Some(properties::ElementKind::Node),
    ///         cardinality: properties::CardinalityBounds::exact(1),
    ///         ..properties::DeliveredProperties::default()
    ///     },
    ///     cost::StorageCostProfile::default()
    ///         .point_gets(properties::PositiveUsize::new(1).unwrap()),
    /// );
    ///
    /// let subplan = exec::ExecutableSubplan::from_selected_executable_alternative(
    ///     &source,
    ///     &alternative,
    ///     &cost::StorageCostProfile::default(),
    /// )
    /// .unwrap();
    ///
    /// assert_eq!(subplan.steps().len(), 1);
    /// ```
    pub fn from_selected_executable_alternative(
        source_expr: &logical::LogicalExpr,
        alternative: &physical::PhysicalAlternative,
        profile: &cost::StorageCostProfile,
    ) -> Result<Self, ExecPlanError> {
        Self::from_selected_executable_alternative_with_io(
            source_expr,
            alternative,
            profile,
            ir::BatchOutputPlan::Discard,
            ExecCondition::Always,
        )
    }

    /// Lower a Cascades-selected standalone physical alternative with explicit
    /// batch output and run condition.
    pub fn from_selected_executable_alternative_with_io(
        source_expr: &logical::LogicalExpr,
        alternative: &physical::PhysicalAlternative,
        profile: &cost::StorageCostProfile,
        output: ir::BatchOutputPlan,
        condition: ExecCondition,
    ) -> Result<Self, ExecPlanError> {
        selected::lowering::lower_selected_executable_alternative(
            source_expr,
            alternative,
            profile,
            Vec::new(),
            output,
            condition,
        )
    }

    /// DAG steps in stable ID order chosen by the planner.
    pub fn steps(&self) -> &[ExecStep] {
        self.steps.as_ref()
    }

    /// Root step.
    pub const fn root(&self) -> ExecStepId {
        self.root
    }

    /// Deterministic interpreter-ready execution stages.
    pub fn execution_order(&self) -> ExecExecutionOrder {
        self.execution_order.clone()
    }

    pub(in crate::exec) fn into_parts(self) -> (ir::AtLeast<ExecStep, 1>, ExecStepId) {
        (self.steps, self.root)
    }
}

#[derive(Debug, Deserialize)]
struct ExecutableSubplanUnchecked {
    steps: ir::AtLeast<ExecStep, 1>,
    root: ExecStepId,
}

impl TryFrom<ExecutableSubplanUnchecked> for ExecutableSubplan {
    type Error = ExecPlanError;

    fn try_from(value: ExecutableSubplanUnchecked) -> Result<Self, Self::Error> {
        Self::new(value.steps, value.root)
    }
}
