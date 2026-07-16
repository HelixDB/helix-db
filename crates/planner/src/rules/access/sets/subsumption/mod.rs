//! Access source subsumption proofs for unions and intersections.

mod edge;
mod node;

use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
enum AccessSourceRemoval<T> {
    Removed(Vec<T>),
    Unchanged,
}

pub(in crate::rules::access) fn simplify_access_subsumption(
    access: &logical::AccessPath,
) -> AccessSetRewrite {
    match access {
        logical::AccessPath::Node(path) => {
            AccessSetRewrite::from_node_plan(node::simplify(path.source().as_ref()))
        }
        logical::AccessPath::Edge(path) => {
            AccessSetRewrite::from_edge_plan(edge::simplify(path.source().as_ref()))
        }
    }
}

fn remove_subsumed_union_sources<T, F>(
    plans: &ir::AtLeast<T, 2>,
    subsumes: F,
) -> AccessSourceRemoval<T>
where
    T: Clone,
    F: Fn(&T, &T) -> bool,
{
    remove_access_sources_when(plans, |index, source| {
        plans.iter().enumerate().any(|(other_index, other)| {
            other_index != index
                && subsumes(other, source)
                && (!subsumes(source, other) || other_index < index)
        })
    })
}

fn remove_redundant_intersection_sources<T, F>(
    plans: &ir::AtLeast<T, 2>,
    subsumes: F,
) -> AccessSourceRemoval<T>
where
    T: Clone,
    F: Fn(&T, &T) -> bool,
{
    remove_access_sources_when(plans, |index, source| {
        plans.iter().enumerate().any(|(other_index, other)| {
            other_index != index
                && subsumes(source, other)
                && (!subsumes(other, source) || other_index < index)
        })
    })
}

fn remove_access_sources_when<T, F>(
    plans: &ir::AtLeast<T, 2>,
    should_remove: F,
) -> AccessSourceRemoval<T>
where
    T: Clone,
    F: Fn(usize, &T) -> bool,
{
    let mut changed = false;
    let mut retained = Vec::with_capacity(plans.len());
    for (index, source) in plans.iter().enumerate() {
        if should_remove(index, source) {
            changed = true;
        } else {
            retained.push(source.clone());
        }
    }
    if changed {
        AccessSourceRemoval::Removed(retained)
    } else {
        AccessSourceRemoval::Unchanged
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn union_removal_keeps_first_of_equivalent_sources() {
        let plans = ir::AtLeast::<_, 2>::from_pair(1_u8, 1);

        assert_eq!(
            remove_subsumed_union_sources(&plans, |left, right| left == right),
            AccessSourceRemoval::Removed(vec![1])
        );
    }

    #[test]
    fn unchanged_sets_are_not_rebuilt() {
        let plans = ir::AtLeast::<_, 2>::from_pair(1_u8, 2);

        assert_eq!(
            remove_access_sources_when(&plans, |_, _| false),
            AccessSourceRemoval::Unchanged
        );
    }
}
