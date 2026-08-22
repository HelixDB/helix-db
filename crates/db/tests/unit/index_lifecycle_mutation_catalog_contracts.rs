use super::*;
use crate::encoding::v2::values::property::Property;
use crate::index_lifecycle::graph_mutation::{
    CanonicalPropertyRow, GraphEntity, GraphMutationTransition, PropertyEdit, PropertyEditOutcome,
};
use crate::index_lifecycle::IndexElementKind;

fn row(label: Option<&str>, properties: &[(&str, &str)]) -> CanonicalPropertyRow {
    let mut values = Vec::new();
    if let Some(label) = label {
        values.push(Property::string("$label", label));
    }
    values.extend(
        properties
            .iter()
            .map(|(name, value)| Property::string(*name, *value)),
    );
    CanonicalPropertyRow::new(values)
}

#[test]
fn routed_target_iterator_covers_every_representable_storage_shape() {
    let first = [MutationRouteTarget::Secondary(0)];
    let second = [MutationRouteTarget::TextActive(1)];
    let cases = [
        (
            RoutedMutationTargets::None,
            Vec::<MutationRouteTarget>::new(),
        ),
        (RoutedMutationTargets::One(&first), first.to_vec()),
        (
            RoutedMutationTargets::Two(&first, &second),
            vec![first[0], second[0]],
        ),
        (
            RoutedMutationTargets::Owned(vec![
                MutationRouteTarget::Vector(2),
                MutationRouteTarget::TextBuilding(3),
            ]),
            vec![
                MutationRouteTarget::Vector(2),
                MutationRouteTarget::TextBuilding(3),
            ],
        ),
    ];
    for (routed, expected) in cases {
        assert_eq!(routed.iter().collect::<Vec<_>>(), expected);
    }
}

#[test]
fn route_registration_and_state_selection_cover_absent_single_and_two_label_shapes() {
    let mut routes = MutationRouteCatalog::default();
    let secondary = MutationRouteTarget::Secondary(0);
    let vector = MutationRouteTarget::Vector(1);
    routes.register(
        IndexElementKind::Node,
        "Before",
        ["value", "value"],
        secondary,
    );
    routes.register(IndexElementKind::Node, "After", ["value"], vector);

    assert!(matches!(
        routes.targets_for_states(IndexElementKind::Edge, &[], &[]),
        RoutedMutationTargets::None
    ));
    assert!(matches!(
        routes.targets_for_states(
            IndexElementKind::Node,
            row(None, &[]).properties(),
            row(Some("Before"), &[]).properties(),
        ),
        RoutedMutationTargets::One(targets) if targets == [secondary]
    ));
    assert!(matches!(
        routes.targets_for_states(
            IndexElementKind::Node,
            row(Some("Before"), &[]).properties(),
            row(Some("After"), &[]).properties(),
        ),
        RoutedMutationTargets::Two(first, second)
            if first == [secondary] && second == [vector]
    ));
    assert!(matches!(
        routes.targets_for_states(
            IndexElementKind::Node,
            row(Some("Before"), &[]).properties(),
            row(Some("Before"), &[("value", "same")]).properties(),
        ),
        RoutedMutationTargets::One(targets) if targets == [secondary]
    ));
}

#[test]
fn transition_routing_deduplicates_multi_property_targets_and_handles_label_moves() {
    let mut routes = MutationRouteCatalog::default();
    routes.register(
        IndexElementKind::Node,
        "Document",
        ["first", "second"],
        MutationRouteTarget::Secondary(0),
    );
    routes.register(
        IndexElementKind::Node,
        "Document",
        ["second"],
        MutationRouteTarget::Vector(1),
    );
    routes.register(
        IndexElementKind::Node,
        "Moved",
        ["first"],
        MutationRouteTarget::TextActive(2),
    );
    let scope = DataScope::LegacyUnscoped;
    let entity = GraphEntity::node(7);

    let before = row(
        Some("Document"),
        &[("first", "before"), ("second", "before")],
    );
    let PropertyEditOutcome::Changed(first) = GraphMutationTransition::edit(
        scope,
        entity,
        before,
        PropertyEdit::set(Property::string("first", "after")),
    ) else {
        panic!("first property edit changes the row")
    };
    let PropertyEditOutcome::Changed(second) = GraphMutationTransition::edit(
        scope,
        entity,
        first.after().expect("replacement has an after row").clone(),
        PropertyEdit::set(Property::string("second", "after")),
    ) else {
        panic!("second property edit changes the row")
    };
    assert!(matches!(
        routes.targets_for(&second),
        RoutedMutationTargets::One(targets)
            if targets == [MutationRouteTarget::Secondary(0), MutationRouteTarget::Vector(1)]
    ));

    let before_move = row(Some("Document"), &[("first", "after")]);
    let PropertyEditOutcome::Changed(moved) = GraphMutationTransition::edit(
        scope,
        entity,
        before_move,
        PropertyEdit::set(Property::string("$label", "Moved")),
    ) else {
        panic!("label edit changes the row")
    };
    assert!(matches!(
        routes.targets_for(&moved),
        RoutedMutationTargets::Two(first, second)
            if first == [MutationRouteTarget::Secondary(0), MutationRouteTarget::Vector(1)]
                && second == [MutationRouteTarget::TextActive(2)]
    ));

    let unlabelled = GraphMutationTransition::create(scope, entity, row(None, &[]));
    assert!(matches!(
        routes.targets_for(&unlabelled),
        RoutedMutationTargets::None
    ));
}
