//! Contracts for literal secondary reads and verified ranges over the exact
//! storage primitives, without executor-side dispatch.

use super::*;

/// Exercises literal secondary storage primitives, reverse serving, and stale
/// recovery against encoded rows.
#[tokio::test]
async fn exact_storage_obeys_encoded_primitives() {
    use slatedb::object_store::memory::InMemory;

    fn active_handle(
        definition: crate::index_lifecycle::ValidatedDynamicIndexDefinition,
        physical: crate::index_lifecycle::PhysicalGeneration,
    ) -> ActiveIndexHandle {
        let building = crate::index_lifecycle::IndexRecordV2::building(
            IndexId::initial(),
            definition,
            crate::index_lifecycle::IndexRevision::initial(),
            physical,
            crate::index_lifecycle::IndexOperationId::new_v4(),
        )
        .expect("exact serving fixture starts building");
        let active = building
            .transition(crate::index_lifecycle::IndexStateTransition::Activate)
            .expect("exact serving fixture activates");
        ActiveIndexHandle::try_from_record(DataScope::LegacyUnscoped, &active)
            .expect("exact serving fixture projects an Active handle")
    }

    fn secondary_handle(definition: crate::config::SecondaryIndexDefinition) -> ActiveIndexHandle {
        active_handle(
            crate::index_lifecycle::ValidatedDynamicIndexDefinition::try_from(definition)
                .expect("exact secondary fixture validates"),
            crate::index_lifecycle::PhysicalGeneration::Secondary {
                generation: IndexGenerationId::initial(),
            },
        )
    }

    async fn put_entry(db: &slatedb::Db, handle: &ActiveIndexHandle, value: &str, entity_id: u64) {
        let definition = handle
            .secondary_definition()
            .expect("exact entry fixture uses a secondary handle");
        let direction = match definition.direction() {
            RangeIndexDirection::Asc => StorageRangeIndexDirection::Asc,
            RangeIndexDirection::Desc => StorageRangeIndexDirection::Desc,
        };
        let canonical = if definition_uses_equality_bitmap(definition)
            || matches!(
                definition,
                ValidatedSecondaryIndexDefinition::NodeEquality { unique: true, .. }
            ) {
            let EqualityValueProjection::Indexed(value) =
                project_equality_value(&PropertyValue::String(value.to_owned()))
            else {
                panic!("string equality fixtures are always indexable")
            };
            CanonicalSecondaryValue::equality(value)
        } else {
            let RangeValueProjection::Indexed(value) =
                project_range_value(&PropertyValue::String(value.to_owned()), direction)
            else {
                panic!("string range fixtures are always indexable")
            };
            CanonicalSecondaryValue::range(value)
        };
        let entity_id = IndexEntityId::new(entity_id);
        let lane = definition_lane(definition);
        let key = secondary_entry_key(
            handle.scope(),
            handle.index_id(),
            handle.generation(),
            definition,
            canonical,
            entity_id,
        )
        .expect("exact entry key validates");
        let value_bytes = if definition_uses_equality_bitmap(definition) {
            SecondaryEqualityBitmapValue::new(roaring::RoaringTreemap::from_iter([entity_id.get()]))
                .encode()
        } else {
            encode_secondary_entry(&SecondaryEntryValue {
                index_id: handle.index_id(),
                generation: handle.generation(),
                lane,
                entity_id,
            })
        };
        db.put(key, value_bytes)
            .await
            .expect("exact entry persists");
        db.put(
            authoritative_property_key(
                handle.scope(),
                IndexEntity {
                    kind: definition.element_kind(),
                    id: entity_id,
                },
            ),
            crate::encoding::v2::values::property::encode_properties(&[
                Property::string("$label", definition.label().as_str()),
                Property::string(definition.property().as_str(), value),
            ]),
        )
        .await
        .expect("exact authoritative row persists");
    }

    let db = slatedb::Db::builder(
        "secondary-exact-production-contracts",
        Arc::new(InMemory::new()),
    )
    .with_merge_operator(Arc::new(crate::merge_operator::HelixMergeOperator::new()))
    .build()
    .await
    .expect("exact serving database opens");
    let equality = secondary_handle(
        crate::config::SecondaryIndexDefinition::node_equality("User", "value").unwrap(),
    );
    let unique = secondary_handle(
        crate::config::SecondaryIndexDefinition::node_unique_equality("User", "value").unwrap(),
    );
    let range = secondary_handle(
        crate::config::SecondaryIndexDefinition::node_range_desc("User", "value").unwrap(),
    );
    let text_definition = crate::index_lifecycle::ValidatedDynamicIndexDefinition::try_from(
        crate::config::TextIndexDefinition::new_node("User", "value").unwrap(),
    )
    .unwrap();
    let text = active_handle(
        text_definition,
        crate::index_lifecycle::PhysicalGeneration::Text {
            generation: IndexGenerationId::initial(),
        },
    );

    assert!(lookup_active_equality_point_literal(
        &db,
        &text,
        &PropertyValue::String("value".to_string()),
    )
    .await
    .is_err());
    assert!(lookup_active_equality_point_literal(
        &db,
        &range,
        &PropertyValue::String("value".to_string()),
    )
    .await
    .is_err());
    assert!(
        lookup_active_equality_point_literal(&db, &equality, &PropertyValue::Null)
            .await
            .is_err()
    );
    assert!(lookup_active_equality_point_literal(
        &db,
        &equality,
        &PropertyValue::String("missing".to_string()),
    )
    .await
    .unwrap()
    .is_empty());
    let oversized = PropertyValue::String("x".repeat(
        crate::encoding::v2::values::property::equality_index_value::MAX_EQUALITY_CANONICAL_LEN + 1,
    ));
    assert!(matches!(
        lookup_active_equality_point_literal(&db, &equality, &oversized).await,
        Err(HelixDbError::SecondaryIndexValue(
            SecondaryIndexValueError::EncodedKeyTooLarge { .. }
        ))
    ));

    put_entry(&db, &equality, "same", 3).await;
    put_entry(&db, &unique, "owner", 7).await;
    assert_eq!(
        lookup_active_equality_point_literal(
            &db,
            &unique,
            &PropertyValue::String("owner".to_string()),
        )
        .await
        .unwrap()
        .into_iter()
        .collect::<Vec<_>>(),
        vec![7]
    );
    assert!(lookup_active_equality_literal_batch(
        &db,
        &equality,
        &[PropertyValue::String("same".to_string())],
    )
    .await
    .is_err());
    assert!(matches!(
        lookup_active_equality_literal_batch(
            &db,
            &equality,
            &[PropertyValue::String("same".to_string()), oversized],
        )
        .await,
        Err(HelixDbError::SecondaryIndexValue(
            SecondaryIndexValueError::EncodedKeyTooLarge { .. }
        ))
    ));
    assert!(lookup_active_equality_literal_batch(
        &db,
        &text,
        &[
            PropertyValue::String("same".to_string()),
            PropertyValue::String("other".to_string()),
        ],
    )
    .await
    .is_err());
    assert!(lookup_active_equality_literal_batch(
        &db,
        &unique,
        &[
            PropertyValue::String("same".to_string()),
            PropertyValue::String("other".to_string()),
        ],
    )
    .await
    .is_err());
    assert!(lookup_active_equality_literal_batch(
        &db,
        &equality,
        &[
            PropertyValue::String("same".to_string()),
            PropertyValue::Null,
        ],
    )
    .await
    .is_err());
    assert_eq!(
        lookup_active_equality_literal_batch(
            &db,
            &equality,
            &[
                PropertyValue::String("same".to_string()),
                PropertyValue::String("missing".to_string()),
            ],
        )
        .await
        .unwrap()
        .into_iter()
        .collect::<Vec<_>>(),
        vec![3]
    );

    assert!(
        scan_active_range_generation_with_membership(&db, &text, None, None, &[])
            .await
            .is_err()
    );
    assert!(
        scan_active_range_generation_with_membership(&db, &equality, None, None, &[])
            .await
            .is_err()
    );
    assert!(scan_active_range_generation_with_membership(
        &db,
        &range,
        Some(&SecondaryRangeQuery::Between {
            lower: PropertyValue::String("z".to_string()),
            lower_inclusive: true,
            upper: PropertyValue::String("a".to_string()),
            upper_inclusive: true,
        }),
        None,
        &[],
    )
    .await
    .unwrap()
    .is_empty());

    put_entry(&db, &range, "a", 1).await;
    put_entry(&db, &range, "b", 2).await;
    let rejects_all = roaring::RoaringTreemap::new();
    assert!(
        scan_active_range_generation_with_membership(&db, &range, None, None, &[&rejects_all],)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        scan_active_range_generation_with_membership(&db, &range, None, Some(1), &[])
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        count_active_range_generation_with_membership(&db, &range, None, None, &[])
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        count_active_range_generation_with_membership(&db, &range, None, Some(1), &[])
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        count_active_range_generation_with_membership(&db, &range, None, Some(0), &[])
            .await
            .unwrap(),
        0
    );
    db.close().await.expect("exact serving database closes");

    // Reverse serving and stale recovery over both range directions.
    for direction in [RangeIndexDirection::Asc, RangeIndexDirection::Desc] {
        for definition in [
            crate::config::SecondaryIndexDefinition::node_range_with_direction(
                "Item", "value", direction,
            )
            .unwrap(),
            crate::config::SecondaryIndexDefinition::edge_range_with_direction(
                "LINK", "value", direction,
            )
            .unwrap(),
        ] {
            let db = slatedb::Db::builder("ordered-range-production", Arc::new(InMemory::new()))
                .build()
                .await
                .unwrap();
            let handle = secondary_handle(definition);
            for (id, value) in [(1, "a"), (2, "b"), (3, "b"), (4, "b"), (5, "c")] {
                put_entry(&db, &handle, value, id).await;
            }
            for iteration in [
                helix_planner::ir::RangeScanIteration::Forward,
                helix_planner::ir::RangeScanIteration::Reverse,
            ] {
                let expected = if iteration.effective_direction(match direction {
                    RangeIndexDirection::Asc => helix_ast::index::RangeIndexDirection::Asc,
                    RangeIndexDirection::Desc => helix_ast::index::RangeIndexDirection::Desc,
                }) == helix_ast::index::RangeIndexDirection::Asc
                {
                    vec![1, 2, 3, 4, 5]
                } else {
                    vec![5, 2, 3, 4, 1]
                };
                for limit in [None, Some(0), Some(1), Some(3), Some(20)] {
                    assert_eq!(
                        scan_active_range_generation_ordered(
                            &db,
                            &handle,
                            None,
                            iteration,
                            limit,
                            &[],
                            &UnobservedRangeScan
                        )
                        .await
                        .unwrap(),
                        expected
                            .iter()
                            .copied()
                            .take(limit.unwrap_or(usize::MAX))
                            .collect::<Vec<_>>()
                    );
                }
                let membership = roaring::RoaringTreemap::from_iter([2, 4]);
                assert_eq!(
                    scan_active_range_generation_ordered(
                        &db,
                        &handle,
                        None,
                        iteration,
                        Some(2),
                        &[&membership],
                        &UnobservedRangeScan
                    )
                    .await
                    .unwrap(),
                    vec![2, 4]
                );
            }
            let query = SecondaryRangeQuery::Between {
                lower: PropertyValue::String("b".into()),
                lower_inclusive: true,
                upper: PropertyValue::String("b".into()),
                upper_inclusive: true,
            };
            // Retained IDs 2 and 3 include a stale row; ID 4 was discarded.
            // Recovery must replace the provisional prefix and return [2, 4] once.
            let property_key = |id| {
                authoritative_property_key(
                    handle.scope(),
                    IndexEntity {
                        kind: handle.secondary_definition().unwrap().element_kind(),
                        id: IndexEntityId::new(id),
                    },
                )
            };
            db.delete(property_key(3)).await.unwrap();
            for (limit, expected) in [
                (Some(2), vec![2, 4]),
                (None, vec![2, 4]),
                (Some(4), vec![2, 4]),
            ] {
                assert_eq!(
                    scan_active_range_generation_ordered(
                        &db,
                        &handle,
                        Some(&query),
                        helix_planner::ir::RangeScanIteration::Reverse,
                        limit,
                        &[],
                        &UnobservedRangeScan
                    )
                    .await
                    .unwrap(),
                    expected
                );
            }
            // Exhausted recovery: every discarded candidate is stale too.
            db.delete(property_key(4)).await.unwrap();
            assert_eq!(
                scan_active_range_generation_ordered(
                    &db,
                    &handle,
                    Some(&query),
                    helix_planner::ir::RangeScanIteration::Reverse,
                    Some(2),
                    &[],
                    &UnobservedRangeScan
                )
                .await
                .unwrap(),
                vec![2]
            );
            // Skipped blobs are not audited, but an attempted decode must fail.
            db.put(property_key(2), vec![255]).await.unwrap();
            assert!(scan_active_range_generation_ordered(
                &db,
                &handle,
                Some(&query),
                helix_planner::ir::RangeScanIteration::Reverse,
                Some(2),
                &[],
                &UnobservedRangeScan
            )
            .await
            .is_err());
            assert!(scan_active_range_generation_ordered(
                &db,
                &handle,
                Some(&query),
                helix_planner::ir::RangeScanIteration::Reverse,
                Some(2),
                &[&roaring::RoaringTreemap::new()],
                &UnobservedRangeScan
            )
            .await
            .unwrap()
            .is_empty());
            db.close().await.unwrap();
        }
    }
}
