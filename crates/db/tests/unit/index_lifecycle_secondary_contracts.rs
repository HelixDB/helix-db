use std::collections::BTreeMap;

use slatedb::IsolationLevel;

use super::*;
use crate::config::SecondaryIndexDefinition;
use crate::encoding::v2::values::property::Property;
use crate::index_lifecycle::graph_mutation::{
    CanonicalPropertyRow, GraphEntity, GraphMutationTransition, PropertyEdit, PropertyEditOutcome,
};
use crate::index_lifecycle::mutation_catalog::{MutationRouteTarget, RoutedMutationTargets};

fn validated(definition: SecondaryIndexDefinition) -> ValidatedSecondaryIndexDefinition {
    let ValidatedDynamicIndexDefinition::Secondary(definition) =
        ValidatedDynamicIndexDefinition::try_from(definition).unwrap()
    else {
        unreachable!("secondary runtime definition validates as secondary")
    };
    definition
}

fn target(definition: ValidatedSecondaryIndexDefinition) -> SecondaryMutationTarget {
    SecondaryMutationTarget {
        index_id: IndexId::initial(),
        generation: IndexGenerationId::initial(),
        definition,
        mode: SecondaryMutationMode::MaintainActive,
    }
}

fn equality(value: &str) -> CanonicalSecondaryValue {
    CanonicalSecondaryValue::equality_string(value)
}

fn applied_key(
    scope: DataScope,
    definition: &ValidatedSecondaryIndexDefinition,
    entity_id: IndexEntityId,
) -> Bytes {
    scoped_index_key(
        scope,
        ScopedKey::AppliedState(IndexEntityStateKey {
            index_id: IndexId::initial(),
            generation: IndexGenerationId::initial(),
            entity: IndexEntity {
                kind: definition.element_kind(),
                id: entity_id,
            },
        }),
    )
}

fn encoded_applied(
    definition: &ValidatedSecondaryIndexDefinition,
    entity_id: IndexEntityId,
    value: Option<CanonicalSecondaryValue>,
) -> Bytes {
    encode_applied_state(&AppliedEntityStateValue {
        index_id: IndexId::initial(),
        generation: IndexGenerationId::initial(),
        entity_kind: definition.element_kind(),
        entity_id,
        state: AppliedFamilyState::Secondary(value),
    })
}

fn encoded_owner(
    definition: &ValidatedSecondaryIndexDefinition,
    entity_id: IndexEntityId,
) -> Bytes {
    encode_secondary_entry(&SecondaryEntryValue {
        index_id: IndexId::initial(),
        generation: IndexGenerationId::initial(),
        lane: definition_lane(definition),
        entity_id,
    })
}

#[tokio::test]
async fn repeated_build_deltas_preserve_the_first_secondary_value() {
    let db = tests::test_db("secondary-build-delta-first-value").await;
    let scope = DataScope::LegacyUnscoped;
    let definition = validated(SecondaryIndexDefinition::node_equality("User", "email").unwrap());
    let target = SecondaryMutationTarget {
        index_id: IndexId::initial(),
        generation: IndexGenerationId::initial(),
        definition,
        mode: SecondaryMutationMode::RecordBuildDelta,
    };
    let transaction = db
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .unwrap();

    for (entity_id, first, second) in [
        (
            IndexEntityId::new(7),
            Some(equality("original@example.com")),
            Some(equality("intermediate@example.com")),
        ),
        (
            IndexEntityId::new(8),
            None,
            Some(equality("created@example.com")),
        ),
    ] {
        let entity = IndexEntity {
            kind: IndexElementKind::Node,
            id: entity_id,
        };
        stage_secondary_build_delta(&transaction, scope, &target, entity, first.clone())
            .await
            .unwrap();
        stage_secondary_build_delta(&transaction, scope, &target, entity, second)
            .await
            .unwrap();

        let key = scoped_index_key(
            scope,
            ScopedKey::BuildDelta(IndexEntityStateKey {
                index_id: target.index_id,
                generation: target.generation,
                entity,
            }),
        );
        let (_, delta) =
            decode_delta(scope, &key, &transaction.get(&key).await.unwrap().unwrap()).unwrap();
        assert_eq!(
            delta.state,
            CoalescedBuildDeltaState::SecondaryBefore(first)
        );
    }

    transaction.rollback();
    db.close().await.unwrap();
}

#[tokio::test]
async fn legacy_secondary_marker_without_applied_state_fails_closed() {
    let db = tests::test_db("secondary-legacy-marker-fails-closed").await;
    let transaction = db
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .unwrap();
    let definition = validated(SecondaryIndexDefinition::node_equality("User", "email").unwrap());

    assert!(matches!(
        reconciliation_plan(
            &transaction,
            DataScope::LegacyUnscoped,
            IndexId::initial(),
            IndexGenerationId::initial(),
            &definition,
            IndexEntityId::new(7),
            &CoalescedBuildDeltaState::Marker,
            Some(equality("current@example.com")),
        )
        .await,
        Err(HelixDbError::IndexCatalogCorruption(_))
    ));

    transaction.rollback();
    db.close().await.unwrap();
}

#[tokio::test]
async fn reconciliation_executes_exact_bitmap_add_noop_and_remove_programs() {
    let db = tests::test_db("secondary-reconciliation-bitmap-contracts").await;
    let transaction = db
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .unwrap();
    let scope = DataScope::LegacyUnscoped;
    let definition = validated(SecondaryIndexDefinition::node_equality("User", "email").unwrap());
    let entity_id = IndexEntityId::new(7);
    let value = equality("first@example.com");

    let ReconciliationPlan::Writes(add) = reconciliation_plan(
        &transaction,
        scope,
        IndexId::initial(),
        IndexGenerationId::initial(),
        &definition,
        entity_id,
        &CoalescedBuildDeltaState::SecondaryBefore(None),
        Some(value.clone()),
    )
    .await
    .unwrap() else {
        panic!("unclaimed bitmap value produces writes");
    };
    assert_eq!(add.writes.len(), 2);
    assert!(matches!(
        add.writes[0],
        EntityWrite::Bitmap { present: true, .. }
    ));
    assert!(matches!(add.writes[1], EntityWrite::Put { .. }));
    add.stage(&transaction).await.unwrap();

    let ReconciliationPlan::Writes(noop) = reconciliation_plan(
        &transaction,
        scope,
        IndexId::initial(),
        IndexGenerationId::initial(),
        &definition,
        entity_id,
        &CoalescedBuildDeltaState::Marker,
        Some(value),
    )
    .await
    .unwrap() else {
        panic!("unchanged bitmap value still refreshes applied authority");
    };
    assert_eq!(noop.writes.len(), 1);
    assert!(matches!(noop.writes[0], EntityWrite::Put { .. }));

    let ReconciliationPlan::Writes(remove) = reconciliation_plan(
        &transaction,
        scope,
        IndexId::initial(),
        IndexGenerationId::initial(),
        &definition,
        entity_id,
        &CoalescedBuildDeltaState::Marker,
        None,
    )
    .await
    .unwrap() else {
        panic!("removing the indexed value produces writes");
    };
    assert_eq!(remove.writes.len(), 2);
    assert!(matches!(
        remove.writes[0],
        EntityWrite::Bitmap { present: false, .. }
    ));
    assert!(matches!(remove.writes[1], EntityWrite::Delete(_)));
    remove.stage(&transaction).await.unwrap();
    transaction.rollback();
    db.close().await.unwrap();
}

#[tokio::test]
async fn reconciliation_unique_observations_block_foreign_release_and_claim() {
    let db = tests::test_db("secondary-reconciliation-unique-contracts").await;
    let transaction = db
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .unwrap();
    let scope = DataScope::LegacyUnscoped;
    let definition =
        validated(SecondaryIndexDefinition::node_unique_equality("User", "email").unwrap());
    let entity_id = IndexEntityId::new(7);
    let foreign_id = IndexEntityId::new(9);
    let previous = equality("previous@example.com");
    let next = equality("next@example.com");
    let previous_key = secondary_entry_key(
        scope,
        IndexId::initial(),
        IndexGenerationId::initial(),
        &definition,
        previous.clone(),
        entity_id,
    )
    .unwrap();
    let next_key = secondary_entry_key(
        scope,
        IndexId::initial(),
        IndexGenerationId::initial(),
        &definition,
        next.clone(),
        entity_id,
    )
    .unwrap();
    transaction
        .put(
            applied_key(scope, &definition, entity_id),
            encoded_applied(&definition, entity_id, Some(previous.clone())),
        )
        .unwrap();
    transaction
        .put(previous_key.clone(), encoded_owner(&definition, foreign_id))
        .unwrap();
    let ReconciliationPlan::Blocked(IndexOperationBlocker::UniquenessViolation {
        first_entity_id,
        second_entity_id,
    }) = reconciliation_plan(
        &transaction,
        scope,
        IndexId::initial(),
        IndexGenerationId::initial(),
        &definition,
        entity_id,
        &CoalescedBuildDeltaState::Marker,
        Some(next.clone()),
    )
    .await
    .unwrap()
    else {
        panic!("foreign previous owner blocks reconciliation");
    };
    assert_eq!((first_entity_id, second_entity_id), (foreign_id, entity_id));

    transaction
        .put(previous_key, encoded_owner(&definition, entity_id))
        .unwrap();
    transaction
        .put(next_key, encoded_owner(&definition, foreign_id))
        .unwrap();
    let ReconciliationPlan::Blocked(IndexOperationBlocker::UniquenessViolation {
        first_entity_id,
        second_entity_id,
    }) = reconciliation_plan(
        &transaction,
        scope,
        IndexId::initial(),
        IndexGenerationId::initial(),
        &definition,
        entity_id,
        &CoalescedBuildDeltaState::Marker,
        Some(next),
    )
    .await
    .unwrap()
    else {
        panic!("foreign next owner blocks reconciliation");
    };
    assert_eq!((first_entity_id, second_entity_id), (foreign_id, entity_id));
    transaction.rollback();
    db.close().await.unwrap();
}

#[tokio::test]
async fn active_change_executes_bitmap_and_unique_primitives_literally() {
    let db = tests::test_db("secondary-active-change-contracts").await;
    let transaction = db
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .unwrap();
    let scope = DataScope::LegacyUnscoped;
    let entity_id = IndexEntityId::new(3);
    let bitmap_target = target(validated(
        SecondaryIndexDefinition::node_equality("User", "email").unwrap(),
    ));
    let old = equality("old@example.com");
    let new = equality("new@example.com");
    apply_active_change(
        &transaction,
        scope,
        &bitmap_target,
        entity_id,
        None,
        Some(old.clone()),
    )
    .await
    .unwrap();
    apply_active_change(
        &transaction,
        scope,
        &bitmap_target,
        entity_id,
        Some(old),
        Some(new.clone()),
    )
    .await
    .unwrap();
    apply_active_change(
        &transaction,
        scope,
        &bitmap_target,
        entity_id,
        Some(new),
        None,
    )
    .await
    .unwrap();

    let unique_target = target(validated(
        SecondaryIndexDefinition::node_unique_equality("User", "username").unwrap(),
    ));
    let claimed = equality("claimed");
    apply_active_change(
        &transaction,
        scope,
        &unique_target,
        entity_id,
        None,
        Some(claimed.clone()),
    )
    .await
    .unwrap();
    let unique_key = secondary_entry_key(
        scope,
        unique_target.index_id,
        unique_target.generation,
        &unique_target.definition,
        claimed.clone(),
        entity_id,
    )
    .unwrap();
    transaction
        .put(
            unique_key.clone(),
            encoded_owner(&unique_target.definition, IndexEntityId::new(4)),
        )
        .unwrap();
    assert!(matches!(
        apply_active_change(
            &transaction,
            scope,
            &unique_target,
            entity_id,
            None,
            Some(claimed.clone()),
        )
        .await,
        Err(HelixDbError::UniqueConstraintViolation { .. })
    ));
    assert!(matches!(
        apply_active_change(
            &transaction,
            scope,
            &unique_target,
            entity_id,
            Some(claimed),
            None,
        )
        .await,
        Err(HelixDbError::IndexCatalogCorruption(_))
    ));
    transaction.delete(unique_key).unwrap();
    apply_active_change(
        &transaction,
        scope,
        &unique_target,
        entity_id,
        Some(equality("missing")),
        None,
    )
    .await
    .unwrap();
    transaction.rollback();
    db.close().await.unwrap();
}

#[tokio::test]
async fn unique_overlay_requires_exact_observations_and_updates_them_in_order() {
    let db = tests::test_db("secondary-active-overlay-contracts").await;
    let transaction = db
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .unwrap();
    let scope = DataScope::LegacyUnscoped;
    let entity_id = IndexEntityId::new(5);
    let target = target(validated(
        SecondaryIndexDefinition::node_unique_equality("User", "email").unwrap(),
    ));
    let old = equality("old@example.com");
    let new = equality("new@example.com");
    assert!(matches!(
        apply_active_change_from_overlay(
            &transaction,
            scope,
            &target,
            entity_id,
            Some(old.clone()),
            None,
            &mut BTreeMap::new(),
        ),
        Err(HelixDbError::IndexCatalogCorruption(_))
    ));
    assert!(matches!(
        apply_active_change_from_overlay(
            &transaction,
            scope,
            &target,
            entity_id,
            None,
            Some(new.clone()),
            &mut BTreeMap::new(),
        ),
        Err(HelixDbError::IndexCatalogCorruption(_))
    ));

    let old_key = secondary_entry_key(
        scope,
        target.index_id,
        target.generation,
        &target.definition,
        old.clone(),
        entity_id,
    )
    .unwrap();
    let new_key = secondary_entry_key(
        scope,
        target.index_id,
        target.generation,
        &target.definition,
        new.clone(),
        entity_id,
    )
    .unwrap();
    let mut overlay = BTreeMap::from([
        (
            old_key.clone(),
            Some(encoded_owner(&target.definition, entity_id)),
        ),
        (new_key.clone(), None),
    ]);
    apply_active_change_from_overlay(
        &transaction,
        scope,
        &target,
        entity_id,
        Some(old),
        Some(new),
        &mut overlay,
    )
    .unwrap();
    assert_eq!(overlay[&old_key], None);
    assert!(overlay[&new_key].is_some());

    let mut foreign_release = BTreeMap::from([(
        old_key,
        Some(encoded_owner(&target.definition, IndexEntityId::new(8))),
    )]);
    assert!(matches!(
        apply_active_change_from_overlay(
            &transaction,
            scope,
            &target,
            entity_id,
            Some(equality("old@example.com")),
            None,
            &mut foreign_release,
        ),
        Err(HelixDbError::IndexCatalogCorruption(_))
    ));
    let mut foreign_claim = BTreeMap::from([(
        new_key,
        Some(encoded_owner(&target.definition, IndexEntityId::new(8))),
    )]);
    assert!(matches!(
        apply_active_change_from_overlay(
            &transaction,
            scope,
            &target,
            entity_id,
            None,
            Some(equality("new@example.com")),
            &mut foreign_claim,
        ),
        Err(HelixDbError::UniqueConstraintViolation { .. })
    ));
    transaction.rollback();
    db.close().await.unwrap();
}

#[test]
fn batch_accounting_saturates_admission_and_checks_every_counter_overflow() {
    let limits = SearchIndexBatchLimits::try_new(
        core::num::NonZeroUsize::MIN,
        NonZeroU64::MIN,
        NonZeroU64::MIN,
        NonZeroU64::MIN,
        NonZeroU64::MIN,
    )
    .unwrap();
    let mut accounting = BatchAccounting::new(OperationCounters::default(), limits);
    assert!(accounting.is_empty());
    assert!(accounting.can_read_another());
    assert!(accounting.can_admit_input(1));
    assert!(!accounting.can_admit_input(2));
    let mut plan = EntityWritePlan::default();
    plan.put(Bytes::from_static(b"k"), Bytes::from_static(b"v"));
    assert!(!accounting.can_admit_output(&plan));
    accounting.admit_scan(1, None).unwrap();
    assert!(!accounting.is_empty());
    assert!(!accounting.can_read_another());
    assert_eq!(accounting.finish().unwrap().entities, 1);

    let mut input_overflow = BatchAccounting::new(OperationCounters::default(), limits);
    input_overflow.input_bytes = u64::MAX;
    assert!(input_overflow.admit_scan(1, None).is_err());
    let mut operations_overflow = BatchAccounting::new(OperationCounters::default(), limits);
    operations_overflow.output_operations = u64::MAX;
    assert!(operations_overflow.admit_scan(0, Some(&plan)).is_err());
    let mut bytes_overflow = BatchAccounting::new(OperationCounters::default(), limits);
    bytes_overflow.output_bytes = u64::MAX;
    assert!(bytes_overflow.admit_scan(0, Some(&plan)).is_err());

    for counters in [
        OperationCounters {
            entities: u64::MAX,
            ..OperationCounters::default()
        },
        OperationCounters {
            input_bytes: u64::MAX,
            ..OperationCounters::default()
        },
        OperationCounters {
            output_operations: u64::MAX,
            ..OperationCounters::default()
        },
        OperationCounters {
            output_bytes: u64::MAX,
            ..OperationCounters::default()
        },
    ] {
        let mut accounting = BatchAccounting::new(counters, limits);
        accounting.entities = 1;
        accounting.input_bytes = 1;
        accounting.output_operations = 1;
        accounting.output_bytes = 1;
        assert!(accounting.finish().is_err());
    }
}

#[test]
fn value_error_and_property_type_contracts_cover_every_closed_variant() {
    let non_unique =
        validated(SecondaryIndexDefinition::node_equality("User", "property").unwrap());
    let unique =
        validated(SecondaryIndexDefinition::node_unique_equality("User", "property").unwrap());
    let entity = IndexEntityId::new(9);
    assert!(matches!(
        mutation_value_error(
            &unique,
            entity,
            SecondaryValueError::UnsupportedEquality("Object")
        ),
        HelixDbError::UnsupportedUniqueIndexValueType { .. }
    ));
    assert!(matches!(
        mutation_value_error(
            &non_unique,
            entity,
            SecondaryValueError::UnsupportedEquality("Object")
        ),
        HelixDbError::SecondaryIndexValue(_)
    ));
    assert!(matches!(
        mutation_value_error(
            &non_unique,
            entity,
            SecondaryValueError::UnsupportedRange("Bool")
        ),
        HelixDbError::SecondaryIndexValue(_)
    ));
    assert!(matches!(
        mutation_value_error(&non_unique, entity, SecondaryValueError::NaNRange),
        HelixDbError::SecondaryIndexValue(_)
    ));
    assert!(matches!(
        mutation_value_error(
            &non_unique,
            entity,
            SecondaryValueError::Oversized {
                encoded_len: 2,
                maximum: 1,
            }
        ),
        HelixDbError::SecondaryIndexValue(_)
    ));

    let values = [
        PropertyValue::Null,
        PropertyValue::Bool(false),
        PropertyValue::I64(0),
        PropertyValue::DateTime(0),
        PropertyValue::F64(0.0),
        PropertyValue::F32(0.0),
        PropertyValue::String(String::new()),
        PropertyValue::Bytes(Vec::new()),
        PropertyValue::I64Array(Vec::new()),
        PropertyValue::F64Array(Vec::new()),
        PropertyValue::F32Array(Vec::new()),
        PropertyValue::StringArray(Vec::new()),
        PropertyValue::Array(Vec::new()),
        PropertyValue::Object(BTreeMap::new()),
    ];
    assert_eq!(
        values.map(|value| property_value_type_name(&value)),
        [
            "Null",
            "Bool",
            "I64",
            "DateTime",
            "F64",
            "F32",
            "String",
            "Bytes",
            "I64Array",
            "F64Array",
            "F32Array",
            "StringArray",
            "Array",
            "Object",
        ]
    );
}

#[test]
fn range_bounds_and_predicates_cover_direction_inclusion_and_invalid_domains() {
    for direction in [
        StorageRangeIndexDirection::Asc,
        StorageRangeIndexDirection::Desc,
    ] {
        for inclusive in [false, true] {
            for query in [
                SecondaryRangeQuery::Lower {
                    value: PropertyValue::I64(2),
                    inclusive,
                },
                SecondaryRangeQuery::Upper {
                    value: PropertyValue::I64(8),
                    inclusive,
                },
            ] {
                assert!(secondary_range_scan_bounds(direction, &query)
                    .unwrap()
                    .is_some());
            }
        }
        for lower_inclusive in [false, true] {
            for upper_inclusive in [false, true] {
                let query = SecondaryRangeQuery::Between {
                    lower: PropertyValue::I64(2),
                    lower_inclusive,
                    upper: PropertyValue::I64(8),
                    upper_inclusive,
                };
                assert!(secondary_range_scan_bounds(direction, &query)
                    .unwrap()
                    .is_some());
                assert_eq!(
                    secondary_range_query_matches(&query, &PropertyValue::I64(2)),
                    lower_inclusive
                );
                assert_eq!(
                    secondary_range_query_matches(&query, &PropertyValue::I64(8)),
                    upper_inclusive
                );
                assert!(secondary_range_query_matches(
                    &query,
                    &PropertyValue::I64(5)
                ));
            }
        }
    }

    assert!(matches!(
        secondary_range_scan_bounds(
            StorageRangeIndexDirection::Asc,
            &SecondaryRangeQuery::Between {
                lower: PropertyValue::Bool(false),
                lower_inclusive: true,
                upper: PropertyValue::String("z".to_string()),
                upper_inclusive: true,
            },
        ),
        Err(HelixDbError::SecondaryIndexValue(_))
    ));
    for query in [
        SecondaryRangeQuery::Between {
            lower: PropertyValue::I64(9),
            lower_inclusive: true,
            upper: PropertyValue::I64(2),
            upper_inclusive: true,
        },
        SecondaryRangeQuery::Between {
            lower: PropertyValue::I64(2),
            lower_inclusive: false,
            upper: PropertyValue::I64(2),
            upper_inclusive: true,
        },
    ] {
        assert!(
            secondary_range_scan_bounds(StorageRangeIndexDirection::Asc, &query)
                .unwrap()
                .is_none()
        );
    }
    assert!(matches!(
        project_query_range_value(
            &PropertyValue::F64(f64::NAN),
            StorageRangeIndexDirection::Asc,
        ),
        Err(HelixDbError::SecondaryIndexValue(_))
    ));
    assert!(!secondary_range_query_matches(
        &SecondaryRangeQuery::Lower {
            value: PropertyValue::I64(0),
            inclusive: true,
        },
        &PropertyValue::String("not-comparable".to_string()),
    ));
    assert!(!secondary_range_query_matches(
        &SecondaryRangeQuery::Upper {
            value: PropertyValue::I64(0),
            inclusive: true,
        },
        &PropertyValue::String("not-comparable".to_string()),
    ));
}

#[tokio::test]
async fn authoritative_equality_and_range_verification_reject_every_stale_shape() {
    let db = tests::test_db("secondary-authoritative-contracts").await;
    let scope = DataScope::LegacyUnscoped;
    let entity_id = IndexEntityId::new(12);
    let entity = IndexEntity {
        kind: IndexElementKind::Node,
        id: entity_id,
    };
    let equality_definition =
        validated(SecondaryIndexDefinition::node_equality("User", "score").unwrap());
    assert!(!authoritative_equality_matches(
        &db,
        scope,
        &equality_definition,
        entity_id,
        &PropertyValue::I64(7),
    )
    .await
    .unwrap());
    db.put(
        authoritative_property_key(scope, entity),
        crate::encoding::property::encode_properties(&[
            Property::string("$label", "Other"),
            Property::new("score", PropertyValue::I64(7)),
        ]),
    )
    .await
    .unwrap();
    assert!(!authoritative_equality_matches(
        &db,
        scope,
        &equality_definition,
        entity_id,
        &PropertyValue::I64(7),
    )
    .await
    .unwrap());
    db.put(
        authoritative_property_key(scope, entity),
        crate::encoding::property::encode_properties(&[
            Property::string("$label", "User"),
            Property::new("score", PropertyValue::I64(7)),
        ]),
    )
    .await
    .unwrap();
    assert!(authoritative_equality_matches(
        &db,
        scope,
        &equality_definition,
        entity_id,
        &PropertyValue::I64(7),
    )
    .await
    .unwrap());
    assert!(!authoritative_equality_matches(
        &db,
        scope,
        &equality_definition,
        entity_id,
        &PropertyValue::I64(8),
    )
    .await
    .unwrap());

    let range_definition =
        validated(SecondaryIndexDefinition::node_range("User", "score").unwrap());
    let key_value =
        match project_range_value(&PropertyValue::I64(7), StorageRangeIndexDirection::Asc) {
            RangeValueProjection::Indexed(value) => value,
            RangeValueProjection::Unsupported(_)
            | RangeValueProjection::NaN
            | RangeValueProjection::Oversized { .. } => {
                unreachable!("integer is a range value")
            }
        };
    assert!(authoritative_range_matches(
        &db,
        scope,
        &range_definition,
        entity_id,
        StorageRangeIndexDirection::Asc,
        &key_value,
        None,
    )
    .await
    .unwrap());
    let different =
        match project_range_value(&PropertyValue::I64(8), StorageRangeIndexDirection::Asc) {
            RangeValueProjection::Indexed(value) => value,
            RangeValueProjection::Unsupported(_)
            | RangeValueProjection::NaN
            | RangeValueProjection::Oversized { .. } => {
                unreachable!("integer is a range value")
            }
        };
    assert!(!authoritative_range_matches(
        &db,
        scope,
        &range_definition,
        entity_id,
        StorageRangeIndexDirection::Asc,
        &different,
        None,
    )
    .await
    .unwrap());
    assert!(!authoritative_range_matches(
        &db,
        scope,
        &range_definition,
        entity_id,
        StorageRangeIndexDirection::Asc,
        &key_value,
        Some(&SecondaryRangeQuery::Lower {
            value: PropertyValue::I64(8),
            inclusive: true,
        }),
    )
    .await
    .unwrap());

    db.put(
        authoritative_property_key(scope, entity),
        crate::encoding::property::encode_properties(&[Property::string("$label", "User")]),
    )
    .await
    .unwrap();
    assert!(!authoritative_range_matches(
        &db,
        scope,
        &range_definition,
        entity_id,
        StorageRangeIndexDirection::Asc,
        &key_value,
        None,
    )
    .await
    .unwrap());
    db.put(
        authoritative_property_key(scope, entity),
        crate::encoding::property::encode_properties(&[
            Property::string("$label", "User"),
            Property::new("score", PropertyValue::Bool(true)),
        ]),
    )
    .await
    .unwrap();
    assert!(!authoritative_range_matches(
        &db,
        scope,
        &range_definition,
        entity_id,
        StorageRangeIndexDirection::Asc,
        &key_value,
        None,
    )
    .await
    .unwrap());
    db.close().await.unwrap();
}

#[test]
fn typed_secondary_decoders_reject_wrong_kind_family_and_ownership() {
    let scope = DataScope::LegacyUnscoped;
    let definition = validated(SecondaryIndexDefinition::node_equality("User", "email").unwrap());
    let entity = IndexEntity {
        kind: IndexElementKind::Node,
        id: IndexEntityId::new(4),
    };
    assert!(secondary_entry_key(
        scope,
        IndexId::initial(),
        IndexGenerationId::initial(),
        &definition,
        CanonicalSecondaryValue::range_string(StorageRangeIndexDirection::Asc, "wrong"),
        entity.id,
    )
    .is_err());
    assert!(decode_secondary_entry_value(
        IndexId::initial(),
        IndexGenerationId::initial(),
        SecondaryEntryLane::NodeEquality,
        &encode_secondary_entry(&SecondaryEntryValue {
            index_id: IndexId::new(2).unwrap(),
            generation: IndexGenerationId::initial(),
            lane: SecondaryEntryLane::NodeEquality,
            entity_id: entity.id,
        }),
    )
    .is_err());

    let state_key = IndexEntityStateKey {
        index_id: IndexId::initial(),
        generation: IndexGenerationId::initial(),
        entity,
    };
    let delta_key = scoped_index_key(scope, ScopedKey::BuildDelta(state_key));
    let delta = CoalescedBuildDeltaValue {
        index_id: IndexId::initial(),
        generation: IndexGenerationId::initial(),
        entity_kind: entity.kind,
        entity_id: entity.id,
        state: CoalescedBuildDeltaState::SecondaryBefore(None),
    };
    assert_eq!(
        decode_delta(scope, &delta_key, &encode_build_delta(&delta))
            .unwrap()
            .0,
        entity
    );
    let applied_key = scoped_index_key(scope, ScopedKey::AppliedState(state_key));
    assert!(decode_delta(scope, &applied_key, &encode_build_delta(&delta)).is_err());
    let mismatched_delta = CoalescedBuildDeltaValue {
        entity_id: IndexEntityId::new(5),
        ..delta
    };
    assert!(decode_delta(scope, &delta_key, &encode_build_delta(&mismatched_delta)).is_err());

    let applied = AppliedEntityStateValue {
        index_id: IndexId::initial(),
        generation: IndexGenerationId::initial(),
        entity_kind: entity.kind,
        entity_id: entity.id,
        state: AppliedFamilyState::Secondary(None),
    };
    assert_eq!(
        decode_applied(scope, &applied_key, &encode_applied_state(&applied))
            .unwrap()
            .0,
        entity
    );
    assert!(decode_applied(scope, &delta_key, &encode_applied_state(&applied)).is_err());
    let mismatched_applied = AppliedEntityStateValue {
        entity_id: IndexEntityId::new(5),
        ..applied
    };
    assert!(decode_applied(
        scope,
        &applied_key,
        &encode_applied_state(&mismatched_applied)
    )
    .is_err());
}

#[tokio::test]
async fn generation_source_and_cursor_helpers_cover_empty_present_and_wrong_lanes() {
    let db = tests::test_db("secondary-helper-contracts").await;
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let scope = DataScope::LegacyUnscoped;
    assert!(!generation_has_rows(
        &transaction,
        scope,
        RecordKind::BuildDelta,
        IndexId::initial(),
        IndexGenerationId::initial(),
    )
    .await
    .unwrap());
    let entity = IndexEntity {
        kind: IndexElementKind::Edge,
        id: IndexEntityId::new(6),
    };
    transaction
        .put(
            scoped_index_key(
                scope,
                ScopedKey::BuildDelta(IndexEntityStateKey {
                    index_id: IndexId::initial(),
                    generation: IndexGenerationId::initial(),
                    entity,
                }),
            ),
            Bytes::from_static(b"present"),
        )
        .unwrap();
    assert!(generation_has_rows(
        &transaction,
        scope,
        RecordKind::BuildDelta,
        IndexId::initial(),
        IndexGenerationId::initial(),
    )
    .await
    .unwrap());

    let node_key = authoritative_property_key(
        scope,
        IndexEntity {
            kind: IndexElementKind::Node,
            id: IndexEntityId::new(1),
        },
    );
    let edge_key = authoritative_property_key(scope, entity);
    assert_eq!(
        source_entity(scope, IndexElementKind::Node, &node_key).unwrap(),
        Some(IndexEntityId::new(1))
    );
    assert_eq!(
        source_entity(scope, IndexElementKind::Edge, &edge_key).unwrap(),
        Some(entity.id)
    );
    assert_eq!(
        source_entity(scope, IndexElementKind::Edge, &node_key).unwrap(),
        None
    );
    assert!(source_entity(scope, IndexElementKind::Node, &edge_key).is_err());

    let prefix = Bytes::from_static(b"prefix/");
    assert_eq!(cursor_suffix(&prefix, None).unwrap(), None);
    assert_eq!(
        cursor_suffix(
            &prefix,
            Some(&IndexCursor::try_new(Bytes::from_static(b"prefix/suffix")).unwrap()),
        )
        .unwrap(),
        Some(Bytes::from_static(b"suffix"))
    );
    assert!(cursor_suffix(
        &prefix,
        Some(&IndexCursor::try_new(Bytes::from_static(b"other/suffix")).unwrap()),
    )
    .is_err());
    transaction.rollback();
    db.close().await.unwrap();
}

#[tokio::test]
async fn mutation_runtime_executes_build_delta_and_active_bitmap_programs_then_seals() {
    let db = tests::test_db("secondary-mutation-runtime-contracts").await;
    let scope = DataScope::LegacyUnscoped;
    let definition = validated(SecondaryIndexDefinition::node_equality("User", "email").unwrap());
    let entity_id = IndexEntityId::new(17);
    let entity = IndexEntity {
        kind: IndexElementKind::Node,
        id: entity_id,
    };
    let route = [MutationRouteTarget::Secondary(0)];
    let routes = RoutedMutationTargets::One(&route);

    let building = SecondaryMutationSet {
        targets: vec![SecondaryMutationTarget {
            index_id: IndexId::initial(),
            generation: IndexGenerationId::initial(),
            definition: definition.clone(),
            mode: SecondaryMutationMode::RecordBuildDelta,
        }],
    };
    let created = GraphMutationTransition::create(
        scope,
        GraphEntity::node(entity_id.get()),
        CanonicalPropertyRow::new(vec![
            Property::string("$label", "User"),
            Property::string("email", "created@example.com"),
        ]),
    );
    let transaction = db
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .unwrap();
    let mut runtime = SecondaryMutationRuntime::default();
    runtime
        .collect(scope, &building, &routes, &created)
        .unwrap();
    runtime.flush(&transaction, &building).await.unwrap();
    let delta_key = scoped_index_key(
        scope,
        ScopedKey::BuildDelta(IndexEntityStateKey {
            index_id: IndexId::initial(),
            generation: IndexGenerationId::initial(),
            entity,
        }),
    );
    assert!(transaction.get(&delta_key).await.unwrap().is_some());
    runtime.prepare(&transaction, &building).await.unwrap();
    assert!(runtime
        .collect(scope, &building, &routes, &created)
        .is_err());
    assert!(runtime.flush(&transaction, &building).await.is_err());
    runtime.consume_prepared().unwrap();
    assert!(SecondaryMutationRuntime::default()
        .consume_prepared()
        .is_err());
    transaction.rollback();

    let active = SecondaryMutationSet {
        targets: vec![SecondaryMutationTarget {
            index_id: IndexId::initial(),
            generation: IndexGenerationId::initial(),
            definition: definition.clone(),
            mode: SecondaryMutationMode::MaintainActive,
        }],
    };
    let before = CanonicalPropertyRow::new(vec![
        Property::string("$label", "User"),
        Property::string("email", "before@example.com"),
    ]);
    let PropertyEditOutcome::Changed(replaced) = GraphMutationTransition::edit(
        scope,
        GraphEntity::node(entity_id.get()),
        before,
        PropertyEdit::set(Property::string("email", "after@example.com")),
    ) else {
        panic!("email edit changes the authoritative row")
    };
    let transaction = db
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .unwrap();
    let mut runtime = SecondaryMutationRuntime::default();
    runtime.collect(scope, &active, &routes, &replaced).unwrap();
    runtime.prepare(&transaction, &active).await.unwrap();
    let before_key = secondary_entry_key(
        scope,
        IndexId::initial(),
        IndexGenerationId::initial(),
        &definition,
        equality("before@example.com"),
        entity_id,
    )
    .unwrap();
    let after_key = secondary_entry_key(
        scope,
        IndexId::initial(),
        IndexGenerationId::initial(),
        &definition,
        equality("after@example.com"),
        entity_id,
    )
    .unwrap();
    assert!(transaction.get(&before_key).await.unwrap().is_none());
    let after = transaction
        .get(&after_key)
        .await
        .unwrap()
        .expect("active bitmap addition is staged");
    assert!(SecondaryEqualityBitmapValue::decode(&after)
        .unwrap()
        .ids()
        .contains(entity_id.get()));
    transaction.rollback();
    db.close().await.unwrap();
}

#[tokio::test]
async fn mutation_runtime_filters_other_families_skips_noops_and_executes_unique_owner_reads() {
    let db = tests::test_db("secondary-mutation-runtime-unique-contracts").await;
    let scope = DataScope::LegacyUnscoped;
    let definition =
        validated(SecondaryIndexDefinition::node_unique_equality("User", "email").unwrap());
    let mutations = SecondaryMutationSet {
        targets: vec![SecondaryMutationTarget {
            index_id: IndexId::initial(),
            generation: IndexGenerationId::initial(),
            definition: definition.clone(),
            mode: SecondaryMutationMode::MaintainActive,
        }],
    };
    let entity_id = IndexEntityId::new(23);
    let created = GraphMutationTransition::create(
        scope,
        GraphEntity::node(entity_id.get()),
        CanonicalPropertyRow::new(vec![
            Property::string("$label", "User"),
            Property::string("email", "unique@example.com"),
        ]),
    );
    let route = [
        MutationRouteTarget::Vector(0),
        MutationRouteTarget::TextBuilding(0),
        MutationRouteTarget::TextActive(0),
        MutationRouteTarget::Secondary(0),
    ];
    let routes = RoutedMutationTargets::Owned(route.to_vec());
    let transaction = db
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .unwrap();
    let mut runtime = SecondaryMutationRuntime::default();
    runtime
        .collect(scope, &mutations, &routes, &created)
        .unwrap();
    runtime.prepare(&transaction, &mutations).await.unwrap();
    let owner_key = secondary_entry_key(
        scope,
        IndexId::initial(),
        IndexGenerationId::initial(),
        &definition,
        equality("unique@example.com"),
        entity_id,
    )
    .unwrap();
    assert_eq!(
        decode_secondary_entry_value(
            IndexId::initial(),
            IndexGenerationId::initial(),
            definition_lane(&definition),
            &transaction
                .get(&owner_key)
                .await
                .unwrap()
                .expect("unique owner is staged"),
        )
        .unwrap(),
        entity_id
    );
    transaction.rollback();

    let irrelevant = GraphMutationTransition::create(
        scope,
        GraphEntity::node(24),
        CanonicalPropertyRow::new(vec![Property::string("$label", "Other")]),
    );
    let transaction = db
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .unwrap();
    let mut runtime = SecondaryMutationRuntime::default();
    runtime
        .collect(
            scope,
            &mutations,
            &RoutedMutationTargets::One(&[MutationRouteTarget::Secondary(0)]),
            &irrelevant,
        )
        .unwrap();
    runtime.prepare(&transaction, &mutations).await.unwrap();
    runtime.consume_prepared().unwrap();
    transaction.rollback();
    db.close().await.unwrap();
}
