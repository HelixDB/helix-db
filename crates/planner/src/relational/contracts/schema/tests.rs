use super::*;
use crate::analysis::tests::allocations;

fn bindings(count: usize) -> Vec<r::Binding> {
    (0..count)
        .map(|slot| r::Binding {
            name: format!("v{slot}"),
            kind: r::BindingType::Scalar,
            nullable: false,
            value_type: r::ValueType::Integer,
        })
        .collect()
}

#[test]
fn empty_sparse_and_word_boundary_scopes_iterate_without_expanding_maps() {
    for slots in [
        vec![],
        vec![0],
        vec![63],
        vec![64],
        vec![0, 63, 64, 65, 127, 128, 4095],
        (0..16).chain([128]).collect(),
        (0..4096).collect(),
    ] {
        let source = RowSchema::empty(&bindings(4096));
        let expected: BTreeSet<_> = slots.into_iter().map(r::Slot).collect();
        let schema = source.derive(&expected, false);
        assert_eq!(schema.len(), expected.len());
        assert_eq!(schema.is_empty(), expected.is_empty());
        assert_eq!(schema.slots(), expected);
        let mut iter = schema.iter();
        for (index, slot) in expected.iter().enumerate() {
            assert_eq!(
                iter.size_hint(),
                (expected.len() - index, Some(expected.len() - index))
            );
            assert_eq!(iter.len(), expected.len() - index);
            assert_eq!(
                iter.next(),
                Some((
                    *slot,
                    r::ColumnType {
                        value_type: r::ValueType::Integer,
                        nullable: false
                    }
                ))
            );
        }
        for _ in 0..3 {
            assert!(iter.next().is_none());
            assert_eq!(iter.len(), 0);
        }
        for slot in [
            0,
            1,
            62,
            63,
            64,
            65,
            66,
            127,
            128,
            4094,
            4095,
            4096,
            u32::MAX,
        ] {
            assert_eq!(
                schema.get(r::Slot(slot)).is_some(),
                expected.contains(&r::Slot(slot))
            );
        }
        let (_, count) = allocations::observe(|| {
            for _ in 0..16 {
                std::hint::black_box(schema.get(r::Slot(64)));
                assert_eq!(schema.iter().count(), expected.len());
                drop(schema.clone());
            }
        });
        assert_eq!((count.allocations, count.bytes), (0, 0));
        assert!(schema.0.columns.get().is_none());
    }
}

#[test]
fn optional_null_extension_preserves_incoming_and_catalog_facts() {
    let mut catalog = bindings(3);
    catalog[1].nullable = true;
    let empty = RowSchema::empty(&catalog);
    let first = empty.derive(&[r::Slot(0)].into(), false);
    let optional = first.derive(&[r::Slot(0), r::Slot(1), r::Slot(2)].into(), true);
    assert_eq!(
        optional
            .iter()
            .map(|(s, c)| (s.0, c.nullable))
            .collect::<Vec<_>>(),
        [(0, false), (1, true), (2, true)]
    );
    let projected = optional.derive(&[r::Slot(2)].into(), false);
    let extended = projected.derive(&[r::Slot(0), r::Slot(2)].into(), false);
    assert_eq!(
        extended
            .iter()
            .map(|(s, c)| (s.0, c.nullable))
            .collect::<Vec<_>>(),
        [(0, false), (2, true)]
    );
    let (_, count) = allocations::observe(|| {
        let same = extended.derive(&extended.slots(), true);
        assert!(Arc::ptr_eq(&extended.0, &same.0));
    });
    // Only constructing the caller's owned slot set allocates above.
    let slots = extended.slots();
    let (_, shared) = allocations::observe(|| extended.derive(&slots, true));
    assert!(count.allocations > 0);
    assert_eq!((shared.allocations, shared.bytes), (0, 0));
    assert!(!first.get(r::Slot(0)).unwrap().nullable);
    assert!(first.get(r::Slot(2)).is_none());
    assert!(extended.derive(&BTreeSet::new(), false).is_empty());
}

#[test]
fn equality_ignores_cache_state_and_unused_catalog_entries() {
    let a = bindings(4);
    let mut b = bindings(8);
    b[7].value_type = r::ValueType::String;
    b[7].nullable = true;
    let a = RowSchema::empty(&a).derive(&[r::Slot(0), r::Slot(2)].into(), false);
    let b = RowSchema::empty(&b).derive(&[r::Slot(0), r::Slot(2)].into(), false);
    assert_eq!(a, b);
    a.columns();
    assert_eq!(a, b);
    assert!(b.0.columns.get().is_none());
    assert_eq!(a, a.clone());
    assert_ne!(a, a.derive(&[r::Slot(0)].into(), false));
    let mut changed = bindings(4);
    changed[2].value_type = r::ValueType::Boolean;
    let changed = RowSchema::empty(&changed).derive(&[r::Slot(0), r::Slot(2)].into(), false);
    assert_ne!(a, changed);
    let nullable = RowSchema::empty(&bindings(4)).derive(&[r::Slot(0), r::Slot(2)].into(), true);
    assert_ne!(a, nullable);
    assert_eq!(
        RowSchema::empty(&bindings(0)),
        RowSchema::empty(&bindings(8))
    );
}

#[test]
fn serialization_and_debug_stream_the_existing_map_without_caching() {
    let schema =
        RowSchema::empty(&bindings(16)).derive(&[r::Slot(0), r::Slot(2), r::Slot(10)].into(), true);
    let expected: BTreeMap<_, _> = [0, 2, 10]
        .into_iter()
        .map(|slot| {
            (
                r::Slot(slot),
                r::ColumnType {
                    value_type: r::ValueType::Integer,
                    nullable: true,
                },
            )
        })
        .collect();
    assert_eq!(
        serde_json::to_vec(&schema).unwrap(),
        serde_json::to_vec(&expected).unwrap()
    );
    assert_eq!(format!("{schema:?}"), format!("RowSchema({expected:?})"));
    #[derive(Debug)]
    struct RowSchemaDebug<'a>(&'a BTreeMap<r::Slot, r::ColumnType>);
    let wrapper = RowSchemaDebug(&expected);
    assert_eq!(wrapper.0, &expected);
    let rendered = format!("{wrapper:#?}");
    assert_eq!(
        format!("{schema:#?}"),
        rendered.replacen("RowSchemaDebug", "RowSchema", 1)
    );
    assert!(schema.0.columns.get().is_none());
    assert_eq!(schema.columns(), &expected);
    assert_eq!(
        serde_json::to_vec(&schema).unwrap(),
        serde_json::to_vec(&expected).unwrap()
    );
    let (_, count) = allocations::observe(|| schema.columns());
    assert_eq!((count.allocations, count.bytes), (0, 0));
}

#[test]
fn compatibility_cache_and_catalog_survive_clones_without_cycles() {
    let schema = RowSchema::empty(&bindings(130))
        .derive(&[r::Slot(0), r::Slot(64), r::Slot(129)].into(), true);
    let weak = Arc::downgrade(&schema.0);
    let catalog = Arc::downgrade(&schema.0.catalog);
    let clone = schema.clone();
    std::thread::scope(|scope| {
        let handles = (0..16)
            .map(|_| scope.spawn(|| schema.columns() as *const _ as usize))
            .collect::<Vec<_>>();
        let addresses = handles
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect::<Vec<_>>();
        assert!(addresses.iter().all(|p| *p == addresses[0]));
    });
    drop(schema);
    assert!(weak.upgrade().is_some());
    assert_eq!(clone.columns().len(), 3);
    assert!(clone.iter().all(|(_, c)| c.nullable));
    drop(clone);
    assert!(weak.upgrade().is_none());
    assert!(catalog.upgrade().is_none());
}

#[test]
#[should_panic(expected = "query validation checks every scope output")]
fn unchecked_out_of_catalog_slots_fail_the_internal_invariant() {
    RowSchema::empty(&bindings(1)).derive(&[r::Slot(1)].into(), false);
}

#[test]
fn singleton_scope_allocation_is_independent_of_binding_id() {
    let input = RowSchema::empty(&bindings(4096));
    for optional in [false, true] {
        let mut baseline = None;
        for id in [0, 63, 64, 1023, 4095] {
            let slots = [r::Slot(id)].into();
            let (schema, count) = allocations::observe(|| input.derive(&slots, optional));
            let measured = (count.allocations, count.bytes);
            assert_eq!(*baseline.get_or_insert(measured), measured);
            assert_eq!(count.allocations, 1);
            assert_eq!(schema.get(r::Slot(id)).unwrap().nullable, optional);
            assert!(schema.0.columns.get().is_none());
        }
    }
}

#[test]
fn sparse_and_dense_nullable_overrides_preserve_facts_across_scope_changes() {
    let empty = RowSchema::empty(&bindings(4096));
    for slots in [vec![0, 4095], (0..4096).collect()] {
        let selected: BTreeSet<_> = slots.into_iter().map(r::Slot).collect();
        let optional = empty.derive(&selected, true);
        assert!(optional.iter().all(|(_, column)| column.nullable));
        let projected = optional.derive(&[r::Slot(4095)].into(), false);
        assert!(projected.get(r::Slot(4095)).unwrap().nullable);
        let reintroduced = projected.derive(&selected, false);
        assert!(reintroduced.get(r::Slot(4095)).unwrap().nullable);
        assert!(!reintroduced.get(r::Slot(0)).unwrap().nullable);
        assert!(optional.iter().all(|(_, column)| column.nullable));
    }
}

#[test]
fn compact_slot_sets_reject_unsorted_or_duplicate_internal_input() {
    for slots in [[r::Slot(1), r::Slot(0)], [r::Slot(1), r::Slot(1)]] {
        assert!(std::panic::catch_unwind(|| SlotSet::new(slots.into_iter())).is_err());
    }
}
