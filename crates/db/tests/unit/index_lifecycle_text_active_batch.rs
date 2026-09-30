use std::collections::BTreeMap;

use super::*;
use crate::config::SearchIndexBackfillLimits;
use crate::encoding::v2::values::property::Property;
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
fn queued_document_projection_covers_absent_indexed_and_invalid_states() {
    let (_, definition) = text_handle();
    let final_state = [
        Property::string("$label", "Document"),
        Property::string("body", "after"),
    ];
    assert!(
        crate::index_lifecycle::text::mutation::queued_document(&definition, &[])
            .unwrap()
            .is_none()
    );
    let projected =
        crate::index_lifecycle::text::mutation::queued_document(&definition, &final_state)
            .unwrap()
            .unwrap();
    assert_eq!(projected.partition, work::TextPartition::Unpartitioned);
    assert_eq!(&*projected.text, "after");
    let invalid = [
        Property::string("$label", "Document"),
        Property::new(
            "body",
            crate::encoding::v2::values::property::property_value::PropertyValue::I64(1),
        ),
    ];
    assert!(matches!(
        crate::index_lifecycle::text::mutation::queued_document(&definition, &invalid),
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
        Some(analyzed(&definition, first.clone(), "retire").partition),
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
        Some(analyzed(&definition, first.clone(), "duplicate").partition),
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
        Some(analyzed(&definition, first.clone(), "before").partition),
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
        Some(analyzed(&definition, first, "before").partition),
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
    assert_eq!(
        epoch.take_uploads(),
        vec![(Bytes::from_static(b"payload"), split)]
    );
    assert_eq!(epoch.upload_count(), 0);
    assert!(epoch.take_uploads().is_empty());

    let empty = PreparedActiveTextEpoch {
        statistics: super::super::statistics::PreparedTextStatisticsBatch::default(),
        destinations: Vec::new(),
        measurements,
    };
    assert_eq!(empty.upload_count(), 0);
    assert!(matches!(
        corruption("fixture"),
        HelixDbError::IndexCatalogCorruption(_)
    ));
}

/// Writes one `Doc` node with `body` in `tenant`.
async fn add_doc(db: &crate::HelixDB, body: &str, tenant: &str) -> index_keys::IndexEntity {
    let created = db
        .query(helix_ast::query::QueryRequest::write(
            helix_ast::batch::write_batch()
                .var_as(
                    "created",
                    helix_ast::traversal::g().add_n(
                        "Doc",
                        vec![
                            (
                                "body",
                                helix_ast::value::PropertyInput::from(body.to_string()),
                            ),
                            (
                                "tenant",
                                helix_ast::value::PropertyInput::from(tenant.to_string()),
                            ),
                        ],
                    ),
                )
                .returning(["created"]),
        ))
        .await
        .unwrap();
    entity(created["created"][0]["$id"].as_u64().unwrap())
}

fn distinct_words(tag: &str, count: usize) -> String {
    (0..count)
        .map(|index| format!("{tag}{index}"))
        .collect::<Vec<_>>()
        .join(" ")
}

/// Publishing one entity's replacement spends at most both documents'
/// footprints plus the manifest-page values admission reserves, for inserts,
/// overlapping replacements, tenant moves, and deletes alike.
#[tokio::test]
async fn single_entity_publication_fits_both_document_footprints() {
    let scope = DataScope::LegacyUnscoped;
    let db = crate::HelixDB::open_with_object_store_and_config(
        "active-text-footprint-bound",
        Arc::new(slatedb::object_store::memory::InMemory::new()),
        crate::config::DbConfig::new().with_index_operation_queue_tuning(
            crate::config::IndexOperationQueueTuning::default().with_publication_paused_for_tests(),
        ),
    )
    .await
    .unwrap();
    db.install_index_for_tests(
        ValidatedDynamicIndexDefinition::try_from(
            crate::config::TextIndexDefinition::new_node("Doc", "body")
                .unwrap()
                .with_tenant_property("tenant")
                .unwrap(),
        )
        .unwrap(),
    )
    .await
    .unwrap();
    let shared = distinct_words("shared", 30);
    let moving = distinct_words("moving", 20);
    let long = "long".repeat(300);
    let resident = add_doc(&db, &shared, "a").await;
    let moved = add_doc(&db, &moving, "a").await;
    let deleted = add_doc(&db, &long, "b").await;

    let prefix = ManagedIndexKey::data_prefix(
        scope,
        index_keys::ScopedKey::logical_prefix(index_keys::RecordKind::IndexRecord),
    );
    let storage = db.inner_db();
    let mut rows = storage.scan_prefix(&prefix, ..).await.unwrap();
    let record =
        index_values::decode_index_record(&rows.next().await.unwrap().unwrap().value).unwrap();
    let handle = index_lifecycle::ActiveIndexHandle::try_from_record(scope, &record).unwrap();
    let target = crate::index_lifecycle::queue::QueueTarget::new(
        scope,
        record.index_id(),
        record.state().generation(),
    );
    let publisher = db.index_queue_publisher().unwrap();
    use crate::index_lifecycle::queue::publication::PublicationOutcome;
    for _ in 0..100 {
        match publisher.publish_once(target).await.unwrap() {
            PublicationOutcome::Empty => break,
            PublicationOutcome::Published { .. } => {}
            outcome @ (PublicationOutcome::Discarded { .. }
            | PublicationOutcome::Deferred
            | PublicationOutcome::Retry
            | PublicationOutcome::Trimmed
            | PublicationOutcome::Blocked) => panic!("fixture publication stalled: {outcome:?}"),
        }
    }

    let limits = SearchIndexBackfillLimits::default().active_text_mutation();
    let page = limits.max_manifest_page_bytes().get();
    let definition = handle.text_definition().unwrap();
    let document = |text: &str, tenant: &str| {
        crate::index_lifecycle::text::mutation::queued_document(
            definition,
            &[
                Property::string("$label", "Doc"),
                Property::string("body", text),
                Property::string("tenant", tenant),
            ],
        )
        .unwrap()
        .unwrap()
    };
    let overlapping = format!(
        "{} {}",
        distinct_words("shared", 15),
        distinct_words("other", 15)
    );
    let fresh = distinct_words("fresh", 25);
    let moved_text = distinct_words("moved", 20);
    let cases = [
        (entity(10_000), None, Some((fresh.as_str(), "a"))),
        (
            resident,
            Some((shared.as_str(), "a")),
            Some((overlapping.as_str(), "a")),
        ),
        (
            moved,
            Some((moving.as_str(), "a")),
            Some((moved_text.as_str(), "b")),
        ),
        (deleted, Some((long.as_str(), "b")), None),
    ];
    for (entity, before, after) in cases {
        let footprints = [before, after]
            .into_iter()
            .flatten()
            .map(|(text, tenant)| {
                let (_, totals) = crate::search::text::analyze_text_within_budget(
                    definition.analyzer(),
                    text,
                    &mut crate::search::text::TextAnalysisMemoryBudget::new(
                        limits.max_input_bytes(),
                    ),
                )
                .unwrap();
                TextDocumentFootprint::measure(
                    scope,
                    &record,
                    entity,
                    &document(text, tenant).partition,
                    totals,
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        let transaction = storage
            .begin(slatedb::IsolationLevel::SerializableSnapshot)
            .await
            .unwrap();
        let prepared = prepare_queued_text_epoch(
            &transaction,
            &handle,
            vec![QueuedTextEffect {
                entity,
                replacement: after
                    .map(|(text, tenant)| (document(text, tenant).partition, text.to_string())),
            }],
            limits,
            crate::index_lifecycle::queue::storage::AcknowledgementOutput {
                operations: 0,
                bytes: 0,
            },
        )
        .await
        .unwrap();
        let measured = prepared.measurements;
        let sum =
            |field: fn(&TextDocumentFootprint) -> u64| footprints.iter().map(field).sum::<u64>();
        assert!(
            measured.output_operations() <= sum(|footprint| footprint.output_operations),
            "{entity:?}: {measured:?} within {footprints:?}"
        );
        assert!(
            measured.output_bytes() <= sum(|footprint| footprint.output_bytes) + page,
            "{entity:?}: {measured:?} within {footprints:?}"
        );
        assert!(
            measured.input_bytes() <= sum(|footprint| footprint.input_bytes) + 2 * page,
            "{entity:?}: {measured:?} within {footprints:?}"
        );
        // The only split holds the final document, within its admitted bound.
        let split_bound = after.map_or(0, |_| {
            crate::search::text::single_document_split_bytes(
                footprints[footprints.len() - 1].analysis_bytes,
            )
        });
        assert!(
            measured.split_bytes() <= split_bound && measured.retained_split_bytes() <= split_bound,
            "{entity:?}: {measured:?} within {footprints:?}"
        );
        if before.is_none() {
            // An insert touches exactly the rows its footprint counts.
            assert_eq!(
                measured.output_operations(),
                footprints[0].output_operations
            );
        }
        drop(transaction);
    }
    db.close().await.unwrap();
}

#[test]
fn document_admission_reserves_page_values_and_halves_every_allowance() {
    let limits = SearchIndexBackfillLimits::default().active_text_mutation();
    let page = limits.max_manifest_page_bytes().get();
    // A lone split's fixed layout comes out of the smaller split ceiling.
    let split_ceiling = limits
        .max_split_bytes()
        .get()
        .min(limits.max_input_bytes().get());
    let fits = TextDocumentFootprint {
        analysis_bytes: split_ceiling - crate::search::text::single_document_split_bytes(0),
        output_operations: limits.max_output_operations().get() / 2,
        output_bytes: (limits.max_output_bytes().get() - page) / 2,
        input_bytes: (limits.max_input_bytes().get() - 2 * page) / 2,
        single_split_page_bytes: page,
    };
    fits.admit(limits)
        .expect("every exact allowance is admitted");
    let over = |footprint: TextDocumentFootprint| footprint.admit(limits).unwrap_err();
    for (footprint, expected, allowance) in [
        (
            TextDocumentFootprint {
                analysis_bytes: limits.max_input_bytes().get() + 1,
                ..fits
            },
            crate::error::ActiveTextMutationResource::AnalysisBytes,
            limits.max_input_bytes().get(),
        ),
        (
            TextDocumentFootprint {
                analysis_bytes: fits.analysis_bytes + 1,
                ..fits
            },
            crate::error::ActiveTextMutationResource::SplitBytes,
            limits.max_split_bytes().get(),
        ),
        (
            TextDocumentFootprint {
                single_split_page_bytes: page + 1,
                ..fits
            },
            crate::error::ActiveTextMutationResource::ManifestPageBytes,
            page,
        ),
        (
            TextDocumentFootprint {
                output_operations: fits.output_operations + 1,
                ..fits
            },
            crate::error::ActiveTextMutationResource::OutputOperations,
            fits.output_operations,
        ),
        (
            TextDocumentFootprint {
                output_bytes: fits.output_bytes + 1,
                ..fits
            },
            crate::error::ActiveTextMutationResource::OutputBytes,
            fits.output_bytes,
        ),
        (
            TextDocumentFootprint {
                input_bytes: fits.input_bytes + 1,
                ..fits
            },
            crate::error::ActiveTextMutationResource::InputBytes,
            fits.input_bytes,
        ),
    ] {
        assert!(matches!(
            over(footprint),
            HelixDbError::ActiveTextMutationLimitExceeded {
                resource,
                observed,
                limit,
            } if resource == expected && observed == allowance + 1 && limit == allowance
        ));
    }
    // Beneath an unbounded split ceiling, the lone split still has to fit the
    // retained-split budget.
    let unbounded_splits = crate::config::ActiveTextMutationLimits::unchecked_for_tests(
        SearchIndexBackfillLimits::default().batch(),
        limits.max_input_bytes(),
        std::num::NonZeroU64::MAX,
        limits.max_manifest_page_bytes(),
    );
    fits.admit(unbounded_splits)
        .expect("the retained-split budget admits the largest lone split");
    assert!(matches!(
        TextDocumentFootprint {
            analysis_bytes: fits.analysis_bytes + 1,
            ..fits
        }
        .admit(unbounded_splits),
        Err(HelixDbError::ActiveTextMutationLimitExceeded {
            resource: crate::error::ActiveTextMutationResource::RetainedSplitBytes,
            observed,
            limit,
        }) if limit == limits.max_input_bytes().get() && observed == limit + 1
    ));
    let all_over = TextDocumentFootprint {
        analysis_bytes: u64::MAX,
        output_operations: u64::MAX,
        output_bytes: u64::MAX,
        input_bytes: u64::MAX,
        single_split_page_bytes: u64::MAX,
    };
    assert!(matches!(
        over(all_over),
        HelixDbError::ActiveTextMutationLimitExceeded {
            resource: crate::error::ActiveTextMutationResource::AnalysisBytes,
            ..
        }
    ));
}

/// However large the output budget, a document's statistics stay within
/// twice one length-delimited field, so its marker remains encodable.
#[test]
fn document_output_is_capped_where_the_marker_stays_encodable() {
    let defaults = SearchIndexBackfillLimits::default();
    let batch = defaults.batch();
    let unbounded = crate::config::SearchIndexBackfillLimits::try_new(
        crate::config::SearchIndexBatchLimits::try_new(
            batch.max_entities(),
            batch.max_input_bytes(),
            batch.max_output_operations(),
            std::num::NonZeroU64::MAX,
            batch.max_single_vector_output_bytes(),
        )
        .unwrap(),
        defaults.edge_property_read_batch(),
        defaults.text_artifacts(),
        defaults.text_compaction(),
    )
    .unwrap()
    .active_text_mutation();
    let at_cap = TextDocumentFootprint {
        analysis_bytes: 1,
        output_operations: 1,
        output_bytes: MAX_DOCUMENT_OUTPUT_BYTES,
        input_bytes: 1,
        single_split_page_bytes: 1,
    };
    at_cap.admit(unbounded).expect("the cap itself is admitted");
    assert!(matches!(
        TextDocumentFootprint {
            output_bytes: MAX_DOCUMENT_OUTPUT_BYTES + 1,
            ..at_cap
        }
        .admit(unbounded),
        Err(HelixDbError::ActiveTextMutationLimitExceeded {
            resource: crate::error::ActiveTextMutationResource::OutputBytes,
            observed,
            limit: MAX_DOCUMENT_OUTPUT_BYTES,
        }) if observed == MAX_DOCUMENT_OUTPUT_BYTES + 1
    ));
}

/// The smallest indexable one-term document, measured with the stored
/// encoders, is exactly the footprint every valid policy must admit.
#[test]
fn smallest_document_footprint_matches_the_stored_encoders() {
    let definition = index_lifecycle::ValidatedTextIndexDefinition::try_from_runtime(
        &crate::config::TextIndexDefinition::new_node("a", "a").unwrap(),
    )
    .unwrap();
    let record = index_lifecycle::IndexRecordV2::building(
        IndexId::initial(),
        ValidatedDynamicIndexDefinition::Text(definition.clone()),
        IndexRevision::initial(),
        PhysicalGeneration::Text {
            generation: IndexGenerationId::initial(),
        },
        IndexOperationId::from_bytes([9; 16]).unwrap(),
    )
    .unwrap();
    let (_, totals) = crate::search::text::analyze_text_within_budget(
        definition.analyzer(),
        "a",
        &mut crate::search::text::TextAnalysisMemoryBudget::new(std::num::NonZeroU64::MAX),
    )
    .unwrap();
    assert_eq!(
        TextDocumentFootprint::measure(
            DataScope::LegacyUnscoped,
            &record,
            entity(1),
            &work::TextPartition::Unpartitioned,
            totals,
        )
        .unwrap(),
        TextDocumentFootprint::SMALLEST
    );
}

/// Sizing statistics rows from term totals matches encoding every row, and
/// the rows hold the marker's term list at least twice.
#[test]
fn statistics_row_bytes_match_encoding_every_row() {
    let scope = DataScope::LegacyUnscoped;
    for (partition, terms) in [
        (work::TextPartition::Unpartitioned, vec!["a"]),
        (
            work::TextPartition::try_tenant_value(Bytes::from_static(b"tenant")).unwrap(),
            vec!["alpha", "b", "gamma-delta", "zeta"],
        ),
    ] {
        let terms = terms
            .into_iter()
            .map(|term| Bytes::copy_from_slice(term.as_bytes()))
            .collect::<Vec<_>>();
        let unique_terms = u64::try_from(terms.len()).unwrap();
        let unique_term_bytes = terms.iter().map(|term| term.len() as u64).sum::<u64>();
        let contribution =
            work::TextStatisticsContribution::try_present(partition.clone(), [7; 32], 9, terms)
                .unwrap();
        let (rows, encoded) = super::super::statistics::contribution_rows(
            scope,
            IndexId::initial(),
            IndexGenerationId::initial(),
            entity(3),
            &contribution,
        )
        .unwrap();
        let sized = super::super::statistics::contribution_row_bytes(
            scope,
            IndexId::initial(),
            IndexGenerationId::initial(),
            entity(3),
            &partition,
            unique_terms,
            unique_term_bytes,
        )
        .unwrap();
        assert_eq!(rows, unique_terms + 1);
        assert_eq!(sized, encoded);
        assert!(sized >= 2 * (4 * unique_terms + unique_term_bytes));
    }
}

/// The ASCII fast path only ever accepts, so producer admission decides
/// exactly as full analysis does on both sides of every boundary.
#[test]
fn queued_admission_matches_full_analysis_at_every_boundary() {
    let scope = DataScope::LegacyUnscoped;
    let (_, definition) = text_handle();
    let record = index_lifecycle::IndexRecordV2::building(
        IndexId::initial(),
        ValidatedDynamicIndexDefinition::Text(definition.clone()),
        IndexRevision::initial(),
        PhysicalGeneration::Text {
            generation: IndexGenerationId::initial(),
        },
        IndexOperationId::from_bytes([9; 16]).unwrap(),
    )
    .unwrap();
    let defaults = SearchIndexBackfillLimits::default();
    let batch = defaults.batch();
    let tight = crate::config::SearchIndexBackfillLimits::try_new(
        crate::config::SearchIndexBatchLimits::try_new(
            batch.max_entities(),
            batch.max_input_bytes(),
            std::num::NonZeroU64::new(64).unwrap(),
            std::num::NonZeroU64::new(64 * 1024).unwrap(),
            std::num::NonZeroU64::new(64 * 1024).unwrap(),
        )
        .unwrap(),
        defaults.edge_property_read_batch(),
        crate::config::TextBuildArtifactLimits::new(
            std::num::NonZeroUsize::new(64).unwrap(),
            std::num::NonZeroU64::new(16 * 1024).unwrap(),
        ),
        crate::config::TextBackfillCompactionLimits::new(
            defaults.text_compaction().max_fan_in(),
            std::num::NonZeroU64::new(96 * 1024).unwrap(),
            defaults.text_compaction().max_temporary_disk_bytes(),
            defaults.text_compaction().max_output_blob_bytes(),
            std::num::NonZeroU64::new(16 * 1024).unwrap(),
        ),
    )
    .unwrap()
    .active_text_mutation();
    let mut decisions = BTreeMap::new();
    for text in (0..40)
        .map(|count| distinct_words("w", count))
        .chain((0..8).map(|count| "long".repeat(count * 1_000)))
        .chain([
            "a ".repeat(200),
            "Ünïcödé wörds ".repeat(20),
            "running runs ran".to_string(),
        ])
    {
        let queued = admit_queued_document(
            scope,
            &record,
            entity(1),
            &work::TextPartition::Unpartitioned,
            &text,
            tight,
        );
        let analyzed = admit_document(
            scope,
            &record,
            &definition,
            entity(1),
            &work::TextPartition::Unpartitioned,
            &text,
            tight,
        );
        let accepted = analyzed.is_ok();
        match (queued, analyzed) {
            (Ok(()), Ok(_)) => {}
            (Err(queued), Err(analyzed)) => {
                assert_eq!(queued.to_string(), analyzed.to_string(), "{text:?}");
            }
            (queued, analyzed) => panic!("{text:?}: {queued:?} versus {:?}", analyzed.map(drop)),
        }
        decisions
            .entry(accepted)
            .or_insert_with(Vec::new)
            .push(text.len());
    }
    assert_eq!(decisions.len(), 2, "the sweep crosses the boundary");
}

#[test]
fn admission_accepts_small_text_and_rejects_other_families() {
    let scope = DataScope::LegacyUnscoped;
    let (_, definition) = text_handle();
    let operation = IndexOperationId::from_bytes([9; 16]).unwrap();
    let record = index_lifecycle::IndexRecordV2::building(
        IndexId::initial(),
        ValidatedDynamicIndexDefinition::Text(definition),
        IndexRevision::initial(),
        PhysicalGeneration::Text {
            generation: IndexGenerationId::initial(),
        },
        operation,
    )
    .unwrap();
    let limits = SearchIndexBackfillLimits::default().active_text_mutation();
    admit_queued_document(
        scope,
        &record,
        entity(1),
        &work::TextPartition::Unpartitioned,
        "small searchable text",
        limits,
    )
    .expect("a small document fits");

    let ValidatedDynamicIndexDefinition::Vector(vector_definition) =
        ValidatedDynamicIndexDefinition::try_from(
            crate::config::VectorIndexDefinition::new_node(
                "Document",
                "embedding",
                2,
                crate::search::vector::VectorDistanceMetric::Euclidean,
            )
            .unwrap(),
        )
        .unwrap()
    else {
        panic!("a vector definition converts to the vector family");
    };
    let vector = index_lifecycle::IndexRecordV2::building(
        IndexId::initial(),
        ValidatedDynamicIndexDefinition::Vector(vector_definition.clone()),
        IndexRevision::initial(),
        PhysicalGeneration::Vector {
            generation: IndexGenerationId::initial(),
            layout: index_lifecycle::VectorPhysicalLayout::Unpartitioned {
                physical_index_id: index_lifecycle::VectorPhysicalIndexId::initial(),
            },
            descriptor: index_lifecycle::VectorGenerationDescriptor::for_definition(
                &vector_definition,
            ),
        },
        operation,
    )
    .unwrap();
    assert!(matches!(
        admit_queued_document(
            scope,
            &vector,
            entity(1),
            &work::TextPartition::Unpartitioned,
            "text",
            limits,
        ),
        Err(HelixDbError::IndexCatalogCorruption(_))
    ));
}
