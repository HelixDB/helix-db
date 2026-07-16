//! Selected control-flow payload reconstruction helpers.
//!
//! Branch and repeat payload arity is already encoded in `ir::BranchPlan` and
//! `ir::RepeatPlan`. This facade keeps selected branch reconstruction separate
//! from selected lowering orchestration so child roots can be batched and then
//! rebuilt without smuggling compatibility physical trees through the selected
//! contract.

mod branch;
#[cfg(test)]
mod tests;

#[cfg(test)]
pub(super) use branch::SelectedBranchRoots;
pub(super) use branch::{
    collect_branch_plan_inputs, selected_branch_plan_from_roots, split_selected_branch_roots,
};
