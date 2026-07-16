//! Optimizer rule contract facade.
//!
//! These ADTs are the stable boundary between rule implementations, the
//! Cascades optimizer, and trace/test assertions. Concrete rule families live
//! in sibling modules and depend on these contracts instead of each other.

mod applicability;
mod id;
mod known;
mod metadata;
mod outcome;

pub(crate) use self::applicability::{
    access_filter_has_index_candidate, access_filter_has_simplification_candidate,
    access_path_has_contradiction_candidate, access_path_has_equality_range_intersection_candidate,
    access_path_has_equality_range_union_candidate, access_path_has_range_intersection_candidate,
    access_path_has_set_canonicalization_candidate, access_path_has_set_subsumption_candidate,
    root_branch_has_empty_input, root_repeat_has_empty_input,
};
pub use self::{
    applicability::{
        RuleAccessSourceKinds, RuleApplicability, RuleLogicalKinds, RulePureOpKinds,
        RuleStreamPipelineOpKinds,
    },
    id::RuleId,
    known::KnownRuleId,
    metadata::RuleMetadata,
    outcome::{RuleKind, RuleOutcome, RuleRejection},
};
