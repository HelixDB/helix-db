use super::*;
use crate::encoding::v2::values::property::{property_value::PropertyValue, Property};
use crate::{allocation_testing, index_lifecycle::graph_mutation as graph, query_resources};

#[test]
fn map_routes_borrow_single_targets_and_admit_deduplicated_unions_before_allocation() {
    let mut routes = MutationRouteCatalog::default();
    let kind = super::super::IndexElementKind::Node;
    routes.register(kind, "N", ["a", "b"], MutationRouteTarget::Secondary(0));
    routes.register(kind, "N", ["b"], MutationRouteTarget::Vector(1));
    routes.register(kind, "N", ["c"], MutationRouteTarget::TextActive(2));
    for names in [
        vec!["unindexed"],
        vec!["a", "unindexed"],
        vec!["a", "b", "c", "unindexed"],
    ] {
        let mut edit = graph::map::Edit::new(graph::map::Mode::Extend);
        for name in &names {
            edit.insert((*name).into(), Some(PropertyValue::I64(1)))
                .unwrap();
        }
        let graph::PropertyEditOutcome::Changed(transition) = edit
            .apply(
                DataScope::LegacyUnscoped,
                graph::GraphEntity::node(1),
                graph::CanonicalPropertyRow::new(vec![Property::string("$label", "N")]),
                None,
            )
            .unwrap()
        else {
            panic!("addition");
        };
        let budget = query_resources::Budget::new(0);
        let (result, allocations) = allocation_testing::observe(|| {
            routes.targets_for_with_budget(&transition, Some(&budget))
        });
        assert_eq!(allocations.allocations, 0);
        if names.len() == 4 {
            assert!(matches!(
                result,
                Err(HelixDbError::QueryMemoryLimitExceeded)
            ));
            let budget = query_resources::Budget::new(4 * size_of::<MutationRouteTarget>());
            let (result, allocations) = allocation_testing::observe(|| {
                routes.targets_for_with_budget(&transition, Some(&budget))
            });
            let selected = result.unwrap();
            assert_eq!(budget.available(), 0);
            assert_eq!(allocations.bytes, 4 * size_of::<MutationRouteTarget>());
            let expected = [
                MutationRouteTarget::Secondary(0),
                MutationRouteTarget::Vector(1),
                MutationRouteTarget::TextActive(2),
            ];
            assert_eq!(selected.iter().collect::<Vec<_>>(), expected);
            assert_eq!(
                routes.targets_for(&transition).iter().collect::<Vec<_>>(),
                expected
            );
            drop(selected);
            assert_eq!(budget.available(), 4 * size_of::<MutationRouteTarget>());
        } else {
            let selected = result.unwrap();
            assert_eq!(selected.iter().count(), usize::from(names.len() == 2));
        }
    }
}
