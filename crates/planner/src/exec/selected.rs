//! Selected executable IR contract.
//!
//! These ADTs are the boundary between Cascades physical selection and native
//! executable DAG construction. They encode selected roots, recursive child
//! subplans, batch entry conditions, and root-stream inputs without carrying
//! legacy compatibility-tree fallbacks. Planner selection may build these values;
//! executable lowering may consume them; neither side should smuggle unsupported
//! physical trees through this contract.

mod batch;
mod construction;
mod control_flow;
mod family;
mod index_ddl;
mod matching;
mod mutation;
mod physical;
mod provenance;
mod request;
mod root_stream;
mod run;
mod shortest_path;

pub(in crate::exec) mod lowering;
pub(crate) use self::family::{
    SelectedExecutableAlternativeClassification, SelectedExecutableAlternativeFamily,
};

pub(crate) fn selected_executable_alternative_family(
    source_expr: &crate::logical::LogicalExpr,
    physical_expr: &crate::physical::PhysicalExpr,
) -> Result<SelectedExecutableAlternativeFamily, SelectedAlternativeConstructionError> {
    SelectedExecutableAlternativeFamily::try_classify(source_expr, physical_expr)
}

pub use self::batch::{
    SelectedExecutableBatchEntries, SelectedExecutableRunEntry,
    SelectedFollowupExecutableBatchEntry, SelectedForEachBatch,
    SelectedInitialExecutableBatchEntry,
};
pub use self::construction::{SelectedAlternativeConstructionError, SelectedRootConstructionError};
pub use self::control_flow::{
    SelectedBranchPlan, SelectedRepeatPlan, SelectedRootBranch, SelectedRootRepeat,
};
pub use self::index_ddl::SelectedRootIndexDdl;
pub use self::mutation::{SelectedMutationInput, SelectedMutationPlan, SelectedRootMutation};
pub use self::physical::SelectedPhysicalPlan;
pub use self::provenance::{SelectedOptimizerProvenance, SelectedRootProvenance};
pub use self::request::{SelectedExecutableBatchPlanRequest, SelectedExecutablePlanRequest};
pub use self::root_stream::{
    SelectedRootPipeline, SelectedRootStreamInput, SelectedRootTerminal, SelectedRootTerminalPlan,
};
pub use self::run::{SelectedExecutableAlternativeRoot, SelectedExecutableRunRoot};
pub use self::shortest_path::SelectedRootShortestPath;
