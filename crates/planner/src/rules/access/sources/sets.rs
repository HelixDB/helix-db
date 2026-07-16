use crate::ir;

pub(in crate::rules::access) fn node_union_from_sources(
    plans: Vec<ir::NodeAccessSourcePlan>,
) -> ir::NodeAccessPlan {
    match access_sources_from_vec(plans) {
        AccessSources::Empty => ir::NodeAccessPlan::Empty,
        AccessSources::One(plan) => ir::NodeAccessPlan::from(plan),
        AccessSources::Many(first, second, rest) => {
            ir::NodeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair_and_rest(first, second, rest))
        }
    }
}

pub(in crate::rules::access) fn node_intersection_from_sources(
    plans: Vec<ir::NodeAccessSourcePlan>,
) -> ir::NodeAccessPlan {
    match access_sources_from_vec(plans) {
        AccessSources::Empty => ir::NodeAccessPlan::Empty,
        AccessSources::One(plan) => ir::NodeAccessPlan::from(plan),
        AccessSources::Many(first, second, rest) => ir::NodeAccessPlan::Intersect(
            ir::AtLeast::<_, 2>::from_pair_and_rest(first, second, rest),
        ),
    }
}

pub(in crate::rules::access) fn edge_union_from_sources(
    plans: Vec<ir::EdgeAccessSourcePlan>,
) -> ir::EdgeAccessPlan {
    match access_sources_from_vec(plans) {
        AccessSources::Empty => ir::EdgeAccessPlan::Empty,
        AccessSources::One(plan) => ir::EdgeAccessPlan::from(plan),
        AccessSources::Many(first, second, rest) => {
            ir::EdgeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair_and_rest(first, second, rest))
        }
    }
}

pub(in crate::rules::access) fn edge_intersection_from_sources(
    plans: Vec<ir::EdgeAccessSourcePlan>,
) -> ir::EdgeAccessPlan {
    match access_sources_from_vec(plans) {
        AccessSources::Empty => ir::EdgeAccessPlan::Empty,
        AccessSources::One(plan) => ir::EdgeAccessPlan::from(plan),
        AccessSources::Many(first, second, rest) => ir::EdgeAccessPlan::Intersect(
            ir::AtLeast::<_, 2>::from_pair_and_rest(first, second, rest),
        ),
    }
}

enum AccessSources<T> {
    Empty,
    One(T),
    Many(T, T, Vec<T>),
}

fn access_sources_from_vec<T>(plans: Vec<T>) -> AccessSources<T> {
    let mut plans = plans.into_iter();
    let Some(first) = plans.next() else {
        return AccessSources::Empty;
    };
    let Some(second) = plans.next() else {
        return AccessSources::One(first);
    };
    AccessSources::Many(first, second, plans.collect())
}
