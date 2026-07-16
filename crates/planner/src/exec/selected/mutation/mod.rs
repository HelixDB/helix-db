//! Selected mutation contracts.
//!
//! Mutation payloads preserve the executable mutation shape while requiring
//! selected input roots where a mutation consumes prior stream output.

mod input;
mod plan;
mod root;

#[cfg(test)]
mod tests;

pub use self::input::SelectedMutationInput;
pub use self::plan::SelectedMutationPlan;
pub use self::root::SelectedRootMutation;
