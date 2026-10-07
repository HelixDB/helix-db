use super::*;
use crate::{allocation_testing, HelixDbError};
use PropertyValue as V;

#[test]
fn map_edits_preserve_metadata_and_native_representations_and_route_exact_changes() {
    let original = vec![
        Property::string("$label", "N"),
        Property::i64("$private", 9),
        Property::bytes("bytes", vec![7; 64]),
        Property::i64("remove", 1),
        Property::new("zero", V::F64(-0.0)),
        Property::new("nan", V::F64(f64::from_bits(0x7ff8_0000_0000_0001))),
    ];
    for mode in [Mode::Extend, Mode::ReplaceUserProperties] {
        for admitted in [false, true] {
            let budget = query_resources::Budget::new(128 * 1024);
            let mut edit = Edit::new(mode);
            for (name, value) in [
                ("remove", None),
                ("absent", None),
                ("new", Some(V::I64(3))),
                ("zero", Some(V::F64(0.0))),
                ("stored_null", Some(V::Null)),
                ("nan", Some(original[5].value.clone())),
            ] {
                edit.insert(name.to_owned(), value).unwrap();
            }
            // Repeated input keys keep the final value without duplicate fields.
            edit.insert("new".into(), Some(V::I64(4))).unwrap();
            let PropertyEditOutcome::Changed(transition) = edit
                .apply(
                    DataScope::LegacyUnscoped,
                    GraphEntity::edge(7),
                    CanonicalPropertyRow::new(original.clone()),
                    admitted.then_some(&budget),
                )
                .unwrap()
            else {
                panic!("the map changes the row");
            };
            let GraphMutationTransition::Replace {
                before,
                after,
                changed,
                ..
            } = &transition
            else {
                panic!("replacement");
            };
            assert_eq!(
                before.encoded(),
                CanonicalPropertyRow::new(original.clone()).encoded()
            );
            let mut expected = original[..2].to_vec();
            if mode == Mode::Extend {
                expected.push(original[2].clone());
            }
            expected.extend([
                Property::new("zero", V::F64(0.0)),
                original[5].clone(),
                Property::i64("new", 4),
                Property::new("stored_null", V::Null),
            ]);
            assert_eq!(after.encoded(), &property::encode_properties(&expected));
            let expected_names = if mode == Mode::Extend {
                vec!["new", "remove", "stored_null", "zero"]
            } else {
                vec!["bytes", "new", "remove", "stored_null", "zero"]
            };
            assert_eq!(changed.iter().collect::<Vec<_>>(), expected_names);
            for name in expected_names {
                assert!(changed.contains(name));
            }
            for name in ["$label", "nan", "absent"] {
                assert!(!changed.contains(name));
            }
            assert_eq!(
                format!("{changed:?}"),
                format!(
                    "ChangedProperties({:?})",
                    changed.iter().collect::<Vec<_>>()
                )
            );
            drop(before.encoded().clone());
            drop(after.encoded().clone());
            let retained = budget.available();
            let (clone, allocations) = allocation_testing::observe(|| transition.clone());
            assert_eq!(allocations.allocations, 0);
            assert_eq!(budget.available(), retained);
            drop(transition);
            drop(clone);
            assert_eq!(budget.available(), 128 * 1024);
        }
    }
}

#[test]
fn map_noops_preserve_original_owners_without_allocating() {
    for mode in [Mode::Extend, Mode::ReplaceUserProperties] {
        for supplied in [false, true] {
            let mut edit = Edit::new(mode);
            let mut original = vec![Property::string("$label", "N")];
            if supplied {
                original.push(Property::new("zero", V::F64(-0.0)));
                edit.insert("zero".into(), Some(V::F64(-0.0))).unwrap();
                edit.insert("absent".into(), None).unwrap();
            }
            let row = CanonicalPropertyRow::new(original);
            let pointer = row.encoded().as_ptr();
            let budget = query_resources::Budget::new(0);
            let (outcome, allocations) = allocation_testing::observe(|| {
                edit.apply(
                    DataScope::LegacyUnscoped,
                    GraphEntity::node(1),
                    row,
                    Some(&budget),
                )
            });
            let PropertyEditOutcome::Unchanged(row) = outcome.unwrap() else {
                panic!("noop");
            };
            assert_eq!(row.encoded().as_ptr(), pointer);
            assert_eq!(allocations.allocations, 0);
        }
    }
}

#[test]
fn map_replacement_does_not_clone_discarded_payloads_and_denial_allocates_nothing() {
    let row = CanonicalPropertyRow::new(vec![
        Property::string("$label", "N"),
        Property::bytes("large", vec![3; 1024 * 1024]),
    ]);
    for mode in [Mode::Extend, Mode::ReplaceUserProperties] {
        let mut edit = Edit::new(mode);
        edit.insert("large".into(), None).unwrap();
        let budget = query_resources::Budget::new(4096);
        let before = row.clone();
        let (outcome, allocations) = allocation_testing::observe(|| {
            edit.apply(
                DataScope::LegacyUnscoped,
                GraphEntity::node(1),
                before,
                Some(&budget),
            )
        });
        let PropertyEditOutcome::Changed(transition) = outcome.unwrap() else {
            panic!("removal");
        };
        assert!(
            allocations.bytes < 4096,
            "discarded payload was cloned: {allocations:?}"
        );
        assert_eq!(
            transition.after().unwrap().properties(),
            &[Property::string("$label", "N")]
        );
        drop(transition);
        assert_eq!(budget.available(), 4096);
        let mut edit = Edit::new(mode);
        edit.insert("new".into(), Some(V::I64(2))).unwrap();
        let before = row.clone();
        let budget = query_resources::Budget::new(0);
        let (result, allocations) = allocation_testing::observe(|| {
            edit.apply(
                DataScope::LegacyUnscoped,
                GraphEntity::node(1),
                before,
                Some(&budget),
            )
        });
        assert!(matches!(
            result,
            Err(HelixDbError::QueryMemoryLimitExceeded)
        ));
        assert_eq!(allocations.allocations, 0);
    }
}

#[test]
fn map_keys_and_depth_are_validated_before_recursive_equality_or_output() {
    let mut edit = Edit::new(Mode::Extend);
    for name in ["", "$label", "$internal"] {
        let error = edit.insert(name.into(), Some(V::I64(1))).unwrap_err();
        assert!(matches!(
            (&error, name),
            (NameError::Empty, "") | (NameError::Reserved, "$label" | "$internal")
        ));
        assert!(!error.to_string().is_empty());
    }
    assert!(edit.entries.is_empty());
    let deep = (0..48).fold(V::Null, |value, _| V::Array(vec![value]));
    for incoming in [false, true] {
        let before = CanonicalPropertyRow::new(vec![Property::new(
            "value",
            if incoming { V::I64(1) } else { deep.clone() },
        )]);
        let mut edit = Edit::new(Mode::Extend);
        edit.insert(
            "value".into(),
            Some(if incoming { deep.clone() } else { V::I64(1) }),
        )
        .unwrap();
        let budget = query_resources::Budget::new(128 * 1024);
        assert!(edit
            .apply(
                DataScope::LegacyUnscoped,
                GraphEntity::node(1),
                before,
                Some(&budget)
            )
            .is_err());
        assert_eq!(budget.available(), 128 * 1024);
    }
}

#[test]
fn changed_property_names_are_sorted_deduplicated_and_admitted() {
    let budget = query_resources::Budget::new(4096);
    assert!(
        ChangedProperties::from_names(std::iter::empty(), Some(&budget))
            .unwrap()
            .is_none()
    );
    let changes = ChangedProperties::from_names(["z", "a", "a", "m"].into_iter(), Some(&budget))
        .unwrap()
        .unwrap();
    assert_eq!(changes.iter().collect::<Vec<_>>(), ["a", "m", "z"]);
    assert_eq!(
        changes,
        ChangedProperties::from_names(["m", "z", "a"].into_iter(), None)
            .unwrap()
            .unwrap()
    );
    assert_ne!(changes, ChangedProperties::admitted("a", None).unwrap());
    drop(changes);
    assert_eq!(budget.available(), 4096);
}

#[test]
fn map_rewrite_allocation_work_grows_linearly_with_updated_property_count() {
    for count in [32_usize, 256, 1024] {
        let before = CanonicalPropertyRow::new(
            (0..count)
                .map(|i| Property::bytes(format!("key_{i}"), vec![1; 128]))
                .collect(),
        );
        let mut edit = Edit::new(Mode::ReplaceUserProperties);
        for i in 0..count {
            edit.insert(format!("key_{i}"), Some(V::Bytes(vec![2; 128])))
                .unwrap();
        }
        let budget = query_resources::Budget::new(16 * 1024 * 1024);
        let (result, allocations) = allocation_testing::observe(|| {
            edit.apply(
                DataScope::LegacyUnscoped,
                GraphEntity::node(1),
                before,
                Some(&budget),
            )
        });
        let PropertyEditOutcome::Changed(transition) = result.unwrap() else {
            panic!("all values change");
        };
        // Includes output, changed names, serializer scratch and archive. A
        // whole-row rewrite per key would exceed this linear bound at scale.
        assert!(
            allocations.bytes <= count * 4096,
            "{count} properties: {allocations:?}"
        );
        assert!(budget.peak() <= count * 4096);
        assert_eq!(transition.after().unwrap().properties().len(), count);
        assert!(transition
            .after()
            .unwrap()
            .properties()
            .iter()
            .all(|p| p.value == V::Bytes(vec![2; 128])));
        drop(transition);
        assert_eq!(budget.available(), 16 * 1024 * 1024);
    }
}
