use serde::{Deserialize, Serialize};

use super::{PhysicalAccess, PhysicalControlOp, PhysicalStreamOp};
use crate::{ir, properties};

/// Physical operator family inside a non-empty pipeline.
///
/// Payload and effect semantics live in the logical source contract and
/// delivered properties; this enum only records the selected physical shape.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PhysicalPipelineOp {
    /// Identity operation.
    NoOp,
    /// Empty stream.
    Empty,
    /// Access path.
    Access {
        /// Element kind.
        element: properties::ElementKind,
        /// Access operator.
        access: PhysicalAccess,
    },
    /// Residual filter.
    ResidualFilter,
    /// Generic stream operator.
    Stream(PhysicalStreamOp),
    /// Control-flow root used as the input of a selected root pipeline.
    Control(PhysicalControlOp),
    /// Mutation root used as the input of a selected root pipeline.
    Mutation,
    /// Explicit sort.
    Sort,
}

/// Non-empty physical pipeline.
///
/// Barriers are intentionally represented by delivered properties and the
/// selected logical source contract rather than by opaque payload-free pipeline
/// operators.
///
/// ```
/// use helix_planner::{ir, physical};
///
/// let pipeline = physical::PhysicalPipeline::new(ir::AtLeast::<_, 1>::from_one(
///     physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Limit),
/// ));
///
/// assert_eq!(pipeline.ops().len(), 1);
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PhysicalPipeline {
    ops: ir::AtLeast<PhysicalPipelineOp, 1>,
}

/// Borrowed split of a non-empty physical pipeline into prefix plus suffix.
///
/// Selected root-pipeline and terminal lowering use this to localize parent
/// prefixes while keeping the final selected operation explicit.
///
/// ```
/// use helix_planner::{ir, physical};
///
/// let pipeline = physical::PhysicalPipeline::new(ir::AtLeast::<_, 1>::from_one_and_rest(
///     physical::PhysicalPipelineOp::ResidualFilter,
///     vec![physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Project)],
/// ));
/// let split = pipeline.terminal_split();
///
/// assert_eq!(split.prefix(), &[physical::PhysicalPipelineOp::ResidualFilter]);
/// assert_eq!(
///     split.terminal(),
///     &physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Project)
/// );
/// ```
pub struct PhysicalPipelineTerminalSplit<'a> {
    prefix: &'a [PhysicalPipelineOp],
    terminal: &'a PhysicalPipelineOp,
}

impl<'a> PhysicalPipelineTerminalSplit<'a> {
    /// Prefix operators before the terminal suffix.
    pub fn prefix(&self) -> &'a [PhysicalPipelineOp] {
        self.prefix
    }

    /// Final physical pipeline operator.
    pub fn terminal(&self) -> &'a PhysicalPipelineOp {
        self.terminal
    }
}

impl PhysicalPipeline {
    /// Build a non-empty physical pipeline.
    pub fn new(ops: ir::AtLeast<PhysicalPipelineOp, 1>) -> Self {
        Self { ops }
    }

    /// Pipeline operators in execution order.
    pub fn ops(&self) -> &[PhysicalPipelineOp] {
        self.ops.as_ref()
    }

    /// Split the non-empty pipeline into its final operator and prefix.
    pub fn terminal_split(&self) -> PhysicalPipelineTerminalSplit<'_> {
        let (terminal, prefix) = self.ops.split_last();
        PhysicalPipelineTerminalSplit { prefix, terminal }
    }
}
