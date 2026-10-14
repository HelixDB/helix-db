//! Ordered range alternatives retain the logical ORDER BY contract.
use crate::{ir, logical};

#[derive(Debug, Clone, PartialEq)]
pub(in crate::rules) enum AccessOrderSatisfaction {
    NotSatisfied,
    Satisfied(logical::AccessPath),
}

pub(in crate::rules) fn access_order_satisfaction(
    order: &logical::AccessOrder,
) -> AccessOrderSatisfaction {
    if order
        .access()
        .hard_cardinality_upper_bound()
        .is_some_and(|upper| upper <= 1)
    {
        return AccessOrderSatisfaction::Satisfied(order.access().clone());
    }
    let [required] = order.ordering().as_ref() else {
        return AccessOrderSatisfaction::NotSatisfied;
    };
    let access = match order.access() {
        logical::AccessPath::Node(path) => ordered_node_source(path.source().as_ref(), required)
            .map(|source| {
                logical::AccessPath::Node(logical::NodeAccessPath::new(
                    ir::NodeAccessSourcePlan::from_unfiltered(source),
                ))
            }),
        logical::AccessPath::Edge(path) => ordered_edge_source(path.source().as_ref(), required)
            .map(|source| {
                logical::AccessPath::Edge(logical::EdgeAccessPath::new(
                    ir::EdgeAccessSourcePlan::from_unfiltered(source),
                ))
            }),
    };
    match access {
        Some(access) => AccessOrderSatisfaction::Satisfied(access),
        None => AccessOrderSatisfaction::NotSatisfied,
    }
}

fn iteration_for(
    key: &crate::catalog::ScopedPropertyDirectionKey,
    required: &ir::OrderKey,
) -> ir::RangeScanIteration {
    let physical_order = match key.direction {
        helix_ast::index::RangeIndexDirection::Asc => helix_ast::traversal::Order::Asc,
        helix_ast::index::RangeIndexDirection::Desc => helix_ast::traversal::Order::Desc,
    };
    if physical_order == required.order {
        ir::RangeScanIteration::Forward
    } else {
        ir::RangeScanIteration::Reverse
    }
}

fn ordered_node_source(
    source: &ir::NodeAccessPlan,
    required: &ir::OrderKey,
) -> Option<ir::NodeAccessPlan> {
    match source {
        ir::NodeAccessPlan::RangeIndex {
            index, key, range, ..
        } if key.property == required.property => Some(ir::NodeAccessPlan::RangeIndex {
            index: index.clone(),
            key: key.clone(),
            range: range.clone(),
            iteration: iteration_for(key, required),
        }),
        ir::NodeAccessPlan::Intersect(children) if source.is_secondary_set_eligible() => {
            fn flatten(source: &ir::NodeAccessSourcePlan, out: &mut Vec<ir::NodeAccessSourcePlan>) {
                match source.as_ref() {
                    ir::NodeAccessPlan::Intersect(children) => {
                        for child in children {
                            flatten(child, out);
                        }
                    }
                    _ => out.push(source.clone()),
                }
            }
            let mut flattened = Vec::new();
            for child in children {
                flatten(child, &mut flattened);
            }
            let selected = flattened.iter().position(|child| matches!(child.as_ref(), ir::NodeAccessPlan::RangeIndex { key, .. } if key.property == required.property))?;
            let driver = ordered_node_source(flattened.remove(selected).as_ref(), required)?;
            flattened.insert(0, ir::NodeAccessSourcePlan::from_unfiltered(driver));
            Some(ir::NodeAccessPlan::Intersect(
                ir::AtLeast::try_from_vec(flattened)
                    .expect("flattening preserves intersection arity"),
            ))
        }
        _ => None,
    }
}

fn ordered_edge_source(
    source: &ir::EdgeAccessPlan,
    required: &ir::OrderKey,
) -> Option<ir::EdgeAccessPlan> {
    match source {
        ir::EdgeAccessPlan::RangeIndex {
            index, key, range, ..
        } if key.property == required.property => Some(ir::EdgeAccessPlan::RangeIndex {
            index: index.clone(),
            key: key.clone(),
            range: range.clone(),
            iteration: iteration_for(key, required),
        }),
        ir::EdgeAccessPlan::Intersect(children) if source.is_secondary_set_eligible() => {
            fn flatten(source: &ir::EdgeAccessSourcePlan, out: &mut Vec<ir::EdgeAccessSourcePlan>) {
                match source.as_ref() {
                    ir::EdgeAccessPlan::Intersect(children) => {
                        for child in children {
                            flatten(child, out);
                        }
                    }
                    _ => out.push(source.clone()),
                }
            }
            let mut flattened = Vec::new();
            for child in children {
                flatten(child, &mut flattened);
            }
            let selected = flattened.iter().position(|child| matches!(child.as_ref(), ir::EdgeAccessPlan::RangeIndex { key, .. } if key.property == required.property))?;
            let driver = ordered_edge_source(flattened.remove(selected).as_ref(), required)?;
            flattened.insert(0, ir::EdgeAccessSourcePlan::from_unfiltered(driver));
            Some(ir::EdgeAccessPlan::Intersect(
                ir::AtLeast::try_from_vec(flattened)
                    .expect("flattening preserves intersection arity"),
            ))
        }
        _ => None,
    }
}
