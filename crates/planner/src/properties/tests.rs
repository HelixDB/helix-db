use helix_ast::traversal::Order;
use std::num::NonZeroUsize;

use super::*;
use crate::ir;

fn order_keys(property: &str) -> ir::OrderKeys {
    ir::OrderKeys::from(ir::OrderKey {
        property: ir::NonEmptyString::new(property).unwrap(),
        order: Order::Asc,
    })
}

#[test]
fn positive_usize_rejects_zero() {
    assert!(PositiveUsize::new(0).is_none());
    assert_eq!(PositiveUsize::new(1).unwrap().get(), 1);
    assert_eq!(PositiveUsize::at_least_one(0).get(), 1);
    assert_eq!(PositiveUsize::at_least_one(3).get(), 3);
    assert_eq!(PositiveUsize::from(NonZeroUsize::MIN).get(), 1);
}

#[test]
fn positive_usize_serde_preserves_non_zero_contract() {
    let serialized = serde_json::to_string(&PositiveUsize::new(8).unwrap()).unwrap();

    assert_eq!(serialized, "8");
    assert_eq!(
        serde_json::from_str::<PositiveUsize>(&serialized)
            .unwrap()
            .get(),
        8
    );
    assert!(serde_json::from_str::<PositiveUsize>("0").is_err());
}

#[test]
fn cardinality_bounds_reject_inverted_upper_bound() {
    assert!(CardinalityBounds::new(2, Some(1)).is_none());
    assert_eq!(CardinalityBounds::new(1, Some(2)).unwrap().upper(), Some(2));
    assert_eq!(CardinalityBounds::zero_to(Some(2)).lower(), 0);
    assert_eq!(CardinalityBounds::zero_to(Some(2)).upper(), Some(2));
    assert_eq!(
        CardinalityBounds::zero_to(None),
        CardinalityBounds::unknown()
    );
}

#[test]
fn cardinality_bounds_transform_stream_windows_without_invalid_states() {
    let bounded = CardinalityBounds::new(3, Some(10)).unwrap();
    let unbounded = CardinalityBounds::new(3, None).unwrap();

    assert_eq!(
        bounded.after_limit(4),
        CardinalityBounds::new(3, Some(4)).unwrap()
    );
    assert_eq!(bounded.after_limit(2), CardinalityBounds::exact(2));
    assert_eq!(
        unbounded.after_limit(4),
        CardinalityBounds::new(3, Some(4)).unwrap()
    );

    assert_eq!(
        bounded.after_skip(2),
        CardinalityBounds::new(1, Some(8)).unwrap()
    );
    assert_eq!(
        unbounded.after_skip(2),
        CardinalityBounds::new(1, None).unwrap()
    );
    assert_eq!(bounded.after_skip(12), CardinalityBounds::exact(0));

    assert_eq!(
        bounded.after_range(2..5),
        CardinalityBounds::new(1, Some(3)).unwrap()
    );
    assert_eq!(
        unbounded.after_range(2..5),
        CardinalityBounds::new(1, Some(3)).unwrap()
    );
    assert_eq!(bounded.after_range(10..12), CardinalityBounds::exact(0));
}

#[test]
fn delivered_order_satisfies_any_or_matching_prefix() {
    let delivered = DeliveredOrdering::ByKeys(order_keys("age"));
    assert!(delivered.satisfies(&RequiredOrdering::Any));
    assert!(delivered.satisfies(&RequiredOrdering::ByKeys(order_keys("age"))));
    assert!(!DeliveredOrdering::Unordered.satisfies(&RequiredOrdering::ByKeys(order_keys("age"))));
}

#[test]
fn delivered_properties_check_element_and_order() {
    let delivered = DeliveredProperties {
        element: Some(ElementKind::Node),
        ordering: DeliveredOrdering::ByKeys(order_keys("age")),
        ..DeliveredProperties::default()
    };

    assert!(delivered.satisfies(&RequiredProperties {
        element: Some(ElementKind::Node),
        ordering: RequiredOrdering::ByKeys(order_keys("age")),
    }));
    assert!(!delivered.satisfies(&RequiredProperties {
        element: Some(ElementKind::Edge),
        ordering: RequiredOrdering::Any,
    }));
    assert_eq!(
        DeliveredProperties::unknown(),
        DeliveredProperties::default()
    );
}

#[test]
fn property_order_key_preserves_name_and_direction() {
    let key = PropertyOrderKey {
        property: ir::NonEmptyString::new("age").unwrap(),
        order: Order::Desc,
    };
    let serialized = serde_json::to_string(&key).unwrap();
    let deserialized: PropertyOrderKey = serde_json::from_str(&serialized).unwrap();

    assert_eq!(deserialized, key);
}
