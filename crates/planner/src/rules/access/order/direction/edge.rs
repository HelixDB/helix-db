//! Edge range-index direction rewrite application.

use super::{contracts, source};
use crate::{catalog, ir};

pub(super) fn rewrite_access_order_range_direction(
    source: &ir::EdgeAccessSourcePlan,
    ordering: &ir::OrderKeys,
    indexes: &catalog::IndexCatalogSnapshot,
) -> contracts::RangeDirectionRewriteApplication<ir::EdgeAccessSourcePlan> {
    if let ir::EdgeAccessPlan::Intersect(children) = source.as_ref()
        && source.is_secondary_set_eligible()
    {
        let [required] = ordering.as_ref() else {
            return contracts::RangeDirectionRewriteApplication::NotApplicable(
                contracts::RangeDirectionRewriteRejection::MultiKeyOrdering,
            );
        };
        // Intersection is associative. Flatten only intersections so the
        // selected leaf is the exact direct driver used by set lowering.
        let mut pending = children.iter().rev().collect::<Vec<_>>();
        let mut leaves = Vec::new();
        while let Some(child) = pending.pop() {
            match child.as_ref() {
                ir::EdgeAccessPlan::Intersect(nested) => pending.extend(nested.iter().rev()),
                _ => leaves.push(child.clone()),
            }
        }
        let selected = leaves.iter().enumerate().find_map(|(position, child)| {
            if let ir::EdgeAccessPlan::RangeIndex { key, .. } = child.as_ref()
                && key.property == required.property
                && key.direction == contracts::range_direction_for_order(required.order)
            {
                return Some((position, child.clone()));
            }
            match rewrite_access_order_range_direction(child, ordering, indexes) {
                contracts::RangeDirectionRewriteApplication::Rewritten(replacement) => {
                    Some((position, replacement))
                }
                contracts::RangeDirectionRewriteApplication::NotApplicable(_) => None,
            }
        });
        let Some((position, driver)) = selected else {
            return contracts::RangeDirectionRewriteApplication::NotApplicable(
                contracts::RangeDirectionRewriteRejection::MissingIndex,
            );
        };
        leaves.remove(position);
        leaves.insert(0, driver);
        return contracts::RangeDirectionRewriteApplication::Rewritten(
            ir::EdgeAccessSourcePlan::from_unfiltered(ir::EdgeAccessPlan::Intersect(
                ir::AtLeast::try_from_vec(leaves)
                    .expect("flattening preserves at least two intersection leaves"),
            )),
        );
    }
    let (key, range, direction) =
        match source::matchable_range_direction_source(source.as_ref(), ordering) {
            contracts::RangeDirectionRewriteMatch::Matched {
                key,
                range,
                direction,
            } => (key, range, direction),
            contracts::RangeDirectionRewriteMatch::NotApplicable(reason) => {
                return contracts::RangeDirectionRewriteApplication::NotApplicable(reason);
            }
        };
    let replacement_key = catalog::ScopedPropertyDirectionKey::new(
        key.label.clone(),
        key.property.clone(),
        direction,
    );
    match indexes.edge_range.get(&replacement_key).cloned() {
        Some(index) => contracts::RangeDirectionRewriteApplication::Rewritten(
            ir::EdgeAccessSourcePlan::from_unfiltered(ir::EdgeAccessPlan::RangeIndex {
                index,
                key: replacement_key,
                range: range.clone(),
            }),
        ),
        None => contracts::RangeDirectionRewriteApplication::NotApplicable(
            contracts::RangeDirectionRewriteRejection::MissingIndex,
        ),
    }
}
