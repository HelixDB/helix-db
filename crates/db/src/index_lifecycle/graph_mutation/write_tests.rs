use super::*;
use crate::{
    allocation_testing, encoding::v2::values::property::property_value::PropertyValue as V,
};

#[test]
fn admitted_rewrites_preserve_native_bytes_and_share_every_owned_version() {
    let scope = DataScope::LegacyUnscoped;
    let entity = GraphEntity::node(7);
    let original = vec![
        Property::bytes("native", vec![7; 8192]),
        Property::new("float", V::F64(-0.0)),
        Property::i64("key", 1),
    ];
    let mut spare = String::with_capacity(4096);
    spare.push('x');
    for edit in [
        PropertyEdit::set(Property::string("key", spare)),
        PropertyEdit::set(Property::i64("new", 3)),
        PropertyEdit::remove("key"),
    ] {
        let budget = query_resources::Budget::new(128 * 1024);
        let before =
            CanonicalPropertyRow::new_with_budget(original.clone(), Some(&budget)).unwrap();
        let expected = GraphMutationTransition::edit(
            scope,
            entity,
            CanonicalPropertyRow::new(original.clone()),
            edit.clone(),
        );
        let PropertyEditOutcome::Changed(changed) =
            GraphMutationTransition::edit_with_budget(scope, entity, before, edit, Some(&budget))
                .unwrap()
        else {
            panic!("edit changes a property");
        };
        let PropertyEditOutcome::Changed(expected) = expected else {
            panic!("native edit changes a property");
        };
        assert_eq!(
            changed.after().unwrap().encoded(),
            expected.after().unwrap().encoded()
        );
        assert!(changed
            .after()
            .unwrap()
            .properties()
            .iter()
            .any(|p| p.same_v1_representation(&original[0])));
        // Bytes lazily creates its admitted sharing header on the first clone.
        // Subsequent index consumers must not allocate either row's payload.
        drop(changed.before().unwrap().encoded().clone());
        drop(changed.after().unwrap().encoded().clone());
        let remaining = budget.available();
        let (clone, allocations) = allocation_testing::observe(|| changed.clone());
        assert_eq!(
            allocations.allocations, 0,
            "index consumers share names, snapshots and write payloads"
        );
        assert_eq!(budget.available(), remaining);
        drop(changed);
        assert_eq!(budget.available(), remaining);
        drop(clone);
        assert_eq!(budget.available(), 128 * 1024);
    }
}

#[test]
fn denied_rewrites_and_noops_do_not_allocate_or_mutate_their_input() {
    let scope = DataScope::LegacyUnscoped;
    let entity = GraphEntity::edge(9);
    let original = vec![
        Property::bytes("native", vec![7; 8192]),
        Property::i64("key", 1),
    ];
    for edit in [
        PropertyEdit::set(Property::i64("key", 2)),
        PropertyEdit::remove("key"),
    ] {
        let before = CanonicalPropertyRow::new(original.clone());
        let budget = query_resources::Budget::new(0);
        let (result, allocations) = allocation_testing::observe(|| {
            GraphMutationTransition::edit_with_budget(scope, entity, before, edit, Some(&budget))
        });
        assert!(matches!(
            result,
            Err(crate::HelixDbError::QueryMemoryLimitExceeded)
        ));
        assert_eq!(allocations.allocations, 0);
        assert_eq!(budget.available(), 0);
    }
    for edit in [
        PropertyEdit::set(Property::i64("key", 1)),
        PropertyEdit::remove("absent"),
    ] {
        let before = CanonicalPropertyRow::new(original.clone());
        let pointer = before.encoded().as_ptr();
        let budget = query_resources::Budget::new(0);
        let (result, allocations) = allocation_testing::observe(|| {
            GraphMutationTransition::edit_with_budget(scope, entity, before, edit, Some(&budget))
        });
        let PropertyEditOutcome::Unchanged(row) = result.unwrap() else {
            panic!("noop");
        };
        assert_eq!(row.encoded().as_ptr(), pointer);
        assert_eq!(allocations.allocations, 0);
    }
}
