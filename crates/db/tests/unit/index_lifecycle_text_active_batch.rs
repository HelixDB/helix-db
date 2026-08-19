use std::collections::BTreeMap;

use super::*;
use crate::config::SearchIndexBackfillLimits;
use crate::index_lifecycle::{
    IndexGenerationId, IndexId, IndexOperationId, IndexRevision, IndexStateTransition,
    PhysicalGeneration, ValidatedDynamicIndexDefinition,
};

fn text_handle() -> (
    index_lifecycle::ActiveIndexHandle,
    index_lifecycle::ValidatedTextIndexDefinition,
) {
    let runtime = crate::config::TextIndexDefinition::new_node("Document", "body").unwrap();
    let validated = index_lifecycle::ValidatedTextIndexDefinition::try_from_runtime(&runtime)
        .expect("text definition validates");
    let record = index_lifecycle::IndexRecordV2::building(
        IndexId::initial(),
        ValidatedDynamicIndexDefinition::Text(validated.clone()),
        IndexRevision::initial(),
        PhysicalGeneration::Text {
            generation: IndexGenerationId::initial(),
        },
        IndexOperationId::from_bytes([9; 16]).unwrap(),
    )
    .unwrap()
    .transition(IndexStateTransition::Activate)
    .unwrap();
    let handle =
        index_lifecycle::ActiveIndexHandle::try_from_record(DataScope::LegacyUnscoped, &record)
            .unwrap();
    (handle, validated)
}

fn analyzed(
    definition: &index_lifecycle::ValidatedTextIndexDefinition,
    partition: work::TextPartition,
    text: &str,
) -> AnalyzedActiveTextDocument {
    let mut budget = crate::search::text::TextAnalysisMemoryBudget::new(
        SearchIndexBackfillLimits::default()
            .active_text_mutation()
            .max_input_bytes(),
    );
    analyze_document(
        definition,
        ActiveTextDocument {
            partition,
            text: text.to_string(),
        },
        &mut budget,
    )
    .unwrap()
}

fn destination_key(partition: work::TextPartition) -> DestinationKey {
    DestinationKey {
        scope: DataScope::LegacyUnscoped,
        index_id: IndexId::initial(),
        generation: IndexGenerationId::initial(),
        partition,
    }
}

fn entity(id: u64) -> index_keys::IndexEntity {
    index_keys::IndexEntity {
        kind: index_lifecycle::IndexElementKind::Node,
        id: index_lifecycle::IndexEntityId::new(id),
    }
}

#[test]
fn coalesced_graph_and_document_projection_cover_absent_indexed_and_invalid_states() {
    let (_, definition) = text_handle();
    let original = CanonicalPropertyRow::new(vec![
        Property::string("$label", "Document"),
        Property::string("body", "before"),
    ]);
    let final_state = CanonicalPropertyRow::new(vec![
        Property::string("$label", "Document"),
        Property::string("body", "after"),
    ]);
    let graph = CoalescedActiveTextMutation {
        scope: DataScope::LegacyUnscoped,
        entity: GraphEntity::node(7),
        original: Some(original.clone()),
        final_state: Some(final_state.clone()),
    };
    assert!(!graph.graph_key().is_empty());
    assert_eq!(graph.original_properties(), original.properties());
    assert_eq!(graph.final_properties(), final_state.properties());
    assert!(graph.retained_input_bytes() > 0);

    let absent = CoalescedActiveTextMutation {
        original: None,
        final_state: None,
        ..graph.clone()
    };
    assert!(absent.original_properties().is_empty());
    assert!(absent.final_properties().is_empty());

    assert!(active_document(&definition, &[]).unwrap().is_none());
    let projected = active_document(&definition, final_state.properties())
        .unwrap()
        .unwrap();
    assert_eq!(projected.partition, work::TextPartition::Unpartitioned);
    assert_eq!(projected.text, "after");
    let invalid = [
        Property::string("$label", "Document"),
        Property::new(
            "body",
            crate::encoding::v1::property::property_value::PropertyValue::I64(1),
        ),
    ];
    assert!(matches!(
        active_document(&definition, &invalid),
        Err(HelixDbError::InvalidIndexSourceData { .. })
    ));

    assert_eq!(
        contribution(&definition, None).unwrap(),
        work::TextStatisticsContribution::Absent
    );
    let document = analyzed(
        &definition,
        work::TextPartition::Unpartitioned,
        "searchable text",
    );
    assert!(matches!(
        contribution(&definition, Some(&document)).unwrap(),
        work::TextStatisticsContribution::Present { .. }
    ));
}

#[test]
fn destination_grouping_encodes_every_transition_and_rejects_duplicate_work() {
    let (handle, definition) = text_handle();
    let first = work::TextPartition::Unpartitioned;
    let second = work::TextPartition::try_tenant_value(Bytes::from_static(b"tenant-b")).unwrap();
    let entity = entity(7);

    let mut none = BTreeMap::new();
    group_effect(&mut none, &handle, &definition, entity, None, None).unwrap();
    assert!(none.is_empty());

    let mut insert = BTreeMap::new();
    group_effect(
        &mut insert,
        &handle,
        &definition,
        entity,
        None,
        Some(analyzed(&definition, first.clone(), "insert")),
    )
    .unwrap();
    assert_eq!(insert.len(), 1);
    assert!(insert.values().next().unwrap().retirements.is_empty());
    assert!(insert.values().next().unwrap().build_reservation_bytes() > 1);
    assert!(group_effect(
        &mut insert,
        &handle,
        &definition,
        entity,
        None,
        Some(analyzed(&definition, first.clone(), "duplicate")),
    )
    .is_err());

    let mut retirement = BTreeMap::new();
    group_effect(
        &mut retirement,
        &handle,
        &definition,
        entity,
        Some(analyzed(&definition, first.clone(), "retire")),
        None,
    )
    .unwrap();
    assert_eq!(
        retirement
            .values()
            .next()
            .unwrap()
            .build_reservation_bytes(),
        1
    );
    assert!(group_effect(
        &mut retirement,
        &handle,
        &definition,
        entity,
        Some(analyzed(&definition, first.clone(), "duplicate")),
        None,
    )
    .is_err());
    assert!(insert_live(
        &mut retirement,
        &handle,
        &definition,
        entity,
        analyzed(&definition, first.clone(), "conflict"),
        false,
    )
    .is_err());

    let mut update = BTreeMap::new();
    group_effect(
        &mut update,
        &handle,
        &definition,
        entity,
        Some(analyzed(&definition, first.clone(), "before")),
        Some(analyzed(&definition, first.clone(), "after")),
    )
    .unwrap();
    assert!(update.values().next().unwrap().live[&entity].requires_existing_live_state);

    let mut moved = BTreeMap::new();
    group_effect(
        &mut moved,
        &handle,
        &definition,
        entity,
        Some(analyzed(&definition, first, "before")),
        Some(analyzed(&definition, second, "after")),
    )
    .unwrap();
    assert_eq!(moved.len(), 2);
    assert_eq!(
        moved.values().filter(|work| !work.live.is_empty()).count(),
        1
    );
    assert_eq!(
        moved
            .values()
            .filter(|work| !work.retirements.is_empty())
            .count(),
        1
    );
}

#[test]
fn entity_state_validation_accepts_only_exact_owned_live_versions() {
    let key = destination_key(work::TextPartition::Unpartitioned);
    let entity = entity(7);
    assert!(validate_existing_state(None, &key, entity, 1, false).is_ok());
    assert!(validate_existing_state(None, &key, entity, 1, true).is_err());

    let valid = work::TextEntityStateValue {
        index_id: key.index_id,
        generation: key.generation,
        partition: key.partition.clone(),
        entity_kind: entity.kind,
        entity_id: entity.id,
        logical_version: index_lifecycle::TextLogicalVersion::initial(),
        live: true,
    };
    let encode = |state: &work::TextEntityStateValue| index_values::encode_text_entity_state(state);
    assert!(validate_existing_state(Some(&encode(&valid)), &key, entity, 1, true).is_ok());
    assert!(validate_existing_state(Some(b"malformed"), &key, entity, 1, true).is_err());

    let invalid = [
        work::TextEntityStateValue {
            index_id: IndexId::new(2).unwrap(),
            ..valid.clone()
        },
        work::TextEntityStateValue {
            generation: IndexGenerationId::new(2).unwrap(),
            ..valid.clone()
        },
        work::TextEntityStateValue {
            partition: work::TextPartition::try_tenant_value(Bytes::from_static(b"other")).unwrap(),
            ..valid.clone()
        },
        work::TextEntityStateValue {
            entity_kind: index_lifecycle::IndexElementKind::Edge,
            ..valid.clone()
        },
        work::TextEntityStateValue {
            entity_id: index_lifecycle::IndexEntityId::new(8),
            ..valid.clone()
        },
        work::TextEntityStateValue {
            logical_version: index_lifecycle::TextLogicalVersion::new(2).unwrap(),
            ..valid.clone()
        },
        work::TextEntityStateValue {
            live: false,
            ..valid
        },
    ];
    for state in invalid {
        assert!(validate_existing_state(Some(&encode(&state)), &key, entity, 1, true).is_err());
    }
}

#[test]
fn prepared_epoch_upload_ownership_is_moved_once_in_destination_order() {
    let limits = SearchIndexBackfillLimits::default().active_text_mutation();
    let measurements = ActiveTextMutationMeasurements::try_admit(limits, 1, 1, 1, 1, 1)
        .expect("fixture fits active limits");
    let split = work::SplitRef::try_new(
        work::BlobRef::new([3; 32], 128),
        80,
        16,
        4,
        128,
        work::SplitPruning::Unavailable,
    )
    .unwrap();
    let destination = |partition, payload| PreparedDestination {
        key: destination_key(partition),
        observations: Vec::new(),
        writes: Vec::new(),
        payload,
        split: Some(split),
        measurements,
    };
    let mut epoch = PreparedActiveTextEpoch {
        build_deltas: Vec::new(),
        statistics: super::super::statistics::PreparedTextStatisticsBatch::default(),
        destinations: vec![
            destination(
                work::TextPartition::Unpartitioned,
                Some(Bytes::from_static(b"payload")),
            ),
            destination(
                work::TextPartition::try_tenant_value(Bytes::from_static(b"tenant-b")).unwrap(),
                None,
            ),
        ],
        measurements,
    };
    assert_eq!(epoch.upload_count(), 1);
    assert!(epoch.has_destination_work());
    assert_eq!(
        epoch.take_uploads(),
        vec![(Bytes::from_static(b"payload"), split)]
    );
    assert_eq!(epoch.upload_count(), 0);
    assert!(epoch.take_uploads().is_empty());

    let empty = PreparedActiveTextEpoch {
        build_deltas: Vec::new(),
        statistics: super::super::statistics::PreparedTextStatisticsBatch::default(),
        destinations: Vec::new(),
        measurements,
    };
    assert!(!empty.has_destination_work());
    assert!(matches!(
        corruption("fixture"),
        HelixDbError::IndexCatalogCorruption(_)
    ));
}
