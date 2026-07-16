//! Access-family optimizer rules.
//!
//! This facade wires the access rule family. Concrete rule wrappers live next
//! to the proof helpers and physical contracts they depend on, so each module
//! owns one narrow optimizer contract boundary.

mod filter;
mod order;
mod path;
mod pipeline;
mod sets;
mod sources;
mod window;

pub(in crate::rules) use self::filter::{
    index_access_filter, simplify_access_filter, AccessFilterRewrite,
};
pub(in crate::rules) use self::sets::{
    access_path_has_contradiction_candidate as access_path_has_contradiction_proof_candidate,
    access_path_has_equality_range_intersection_candidate as access_path_has_equality_range_intersection_proof_candidate,
    access_path_has_equality_range_union_candidate as access_path_has_equality_range_union_proof_candidate,
    access_path_has_range_intersection_candidate as access_path_has_range_intersection_proof_candidate,
};
pub use self::{
    filter::{
        AccessFilterImplementationRule, AccessFilterIndexRule, AccessFilterSimplificationRule,
    },
    order::{
        AccessDistinctImplementationRule, AccessDistinctRule, AccessOrderImplementationRule,
        AccessOrderRangeDirectionRule, AccessOrderRule,
    },
    path::AccessPathImplementationRule,
    pipeline::{
        AccessPipelineFilterRule, AccessPipelineImplementationRule, AccessPipelineOrderRule,
        AccessPipelineSimplificationRule,
    },
    sets::{
        AccessContradictionRule, AccessEqualityRangeIntersectionRule, AccessEqualityRangeUnionRule,
        AccessRangeIntersectionRule, AccessSetSimplificationRule, AccessSubsumptionRule,
    },
    window::{AccessWindowImplementationRule, AccessWindowRule},
};
