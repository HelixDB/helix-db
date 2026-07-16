//! Literal equality/range access-set proofs.
//!
//! Intersections and unions use different proof strategies. The facade keeps
//! the access-set rule surface stable while each proof family owns its local
//! search and replacement logic.

mod intersection;
mod range_union;
mod support;

pub(in crate::rules) use self::{
    intersection::access_path_has_equality_range_intersection_candidate,
    range_union::access_path_has_equality_range_union_candidate,
};

pub(in crate::rules::access) use self::{
    intersection::simplify_access_equality_range_intersection,
    range_union::simplify_access_equality_range_union,
};
