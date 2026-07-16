//! Edge range-index direction rewrite application.

use super::{contracts, source};
use crate::{catalog, ir};

pub(super) fn rewrite_access_order_range_direction(
    source: &ir::EdgeAccessSourcePlan,
    ordering: &ir::OrderKeys,
    indexes: &catalog::IndexCatalogSnapshot,
) -> contracts::RangeDirectionRewriteApplication<ir::EdgeAccessSourcePlan> {
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
