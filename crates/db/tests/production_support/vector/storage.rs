//! Production contracts for the typed vector-row storage boundary.
//!
//! This feature-gated child module exercises tenant-scoped key construction,
//! current row codecs, opaque canonical/candidate/reverse tokens, measured
//! writes, and exhaustive lane cleanup. It uses only canonical `encoding::v2`
//! keys and values in isolated databases, so deployed bytes remain unchanged.

use std::num::NonZeroU64;
use std::sync::Arc;

use bytes::Bytes;
use slatedb::object_store::memory::InMemory;
use slatedb::{Db, IsolationLevel};

use super::*;
use crate::config::VectorIndexDefinition;
use crate::encoding::keys::scope::TenantId;
use crate::encoding::v2::legacy::vector::transaction_guard::LegacyVectorTxnGuardKey;
use crate::encoding::v2::values::indexes::vector::simhash::encode_simhash;
use crate::index_lifecycle::{ValidatedDynamicIndexDefinition, ValidatedVectorIndexDefinition};
use crate::search::vector::read_fault_production_support::{FaultingRead, ReadFault};
use crate::search::vector::{distance, encode_item, Item, VectorDistanceMetric, VectorIndexConfig};

fn legacy_definition() -> ValidatedVectorIndexDefinition {
    let definition: ValidatedDynamicIndexDefinition =
        VectorIndexDefinition::new_node("Document", "embedding", 3, VectorDistanceMetric::Cosine)
            .unwrap()
            .try_into()
            .unwrap();
    let ValidatedDynamicIndexDefinition::Vector(definition) = definition else {
        unreachable!("vector definition validates as vector")
    };
    definition
}

/// Exercises every frozen physical value codec used by adoption validation.
fn run_legacy_validation_codec_contracts() {
    let physical_name = "production-legacy-validation-codecs";
    let index_id = index_id_from_name(physical_name);
    let definition = legacy_definition();
    let config = VectorIndexConfig::from_v2_definition(&definition, physical_name);
    let dimension = VectorDimension::try_new(3).unwrap();
    let metadata = VectorIndexMetadata::new(config.clone());
    let item = encode_item(&Item::<distance::Cosine>::new(vec![1.0, 0.0, 0.0]));
    let rows = [
        (
            VectorKey::IndexMetadata(VectorIndexMetadataKey::new(index_id)),
            Bytes::copy_from_slice(
                &crate::encoding::v2::legacy::vector::metadata::encode_legacy_metadata_for_contract(
                    &metadata,
                ),
            ),
        ),
        (
            VectorKey::TxnGuard(LegacyVectorTxnGuardKey::new(index_id)),
            crate::encoding::v2::legacy::vector::transaction_guard::encode_active_txn_guard(),
        ),
        (
            VectorKey::Layer0Neighbors(VectorLayer0NeighborsKey::new(index_id, 1)),
            encode_layer0_neighbors(&[2, 3]),
        ),
        (
            VectorKey::UpperNeighbors(VectorUpperNeighborsKey::new(index_id, 2, 1)),
            encode_upper_neighbors(&[2, 3]).unwrap(),
        ),
        (
            VectorKey::SimHash(VectorSimHashKey::new(index_id, 1)),
            Bytes::copy_from_slice(&encode_simhash(17)),
        ),
        (
            VectorKey::SimHashDirectory(VectorSimHashDirectoryKey::new(index_id, 17, 1)),
            encode_simhash_directory_marker_v1(),
        ),
        (
            VectorKey::UpperVector(VectorUpperVectorKey::new(index_id, 1)),
            item.clone(),
        ),
        (VectorKey::Vector(VectorItemKey::new(index_id, 17, 1)), item),
        (
            VectorKey::EntryCandidateSorted(VectorEntryCandidateKey::new(index_id, 2, 1)),
            encode_empty_marker(),
        ),
        (
            VectorKey::EntryCandidateNode(VectorEntryCandidateNodeKey::new(index_id, 1)),
            encode_entry_candidate_layer(2),
        ),
        (
            VectorKey::ReverseEdge(VectorReverseEdgeKey::new(index_id, 2, 1, 3)),
            encode_empty_marker(),
        ),
    ];

    for (key, value) in rows {
        assert!(
            validate_legacy_row::<distance::Cosine>(&key, &value, &config, dimension).is_ok(),
            "valid {key:?} value must decode"
        );
        assert!(
            validate_legacy_row::<distance::Cosine>(&key, b"malformed", &config, dimension,)
                .is_err(),
            "malformed {key:?} value must fail closed"
        );
    }

    let zero_key = VectorKey::Vector(VectorItemKey::new(index_id, 0, 2));
    let zero_cosine = encode_item(&Item::<distance::Cosine>::new(vec![0.0, 0.0, 0.0]));
    assert_eq!(
        validate_legacy_row::<distance::Cosine>(&zero_key, &zero_cosine, &config, dimension,),
        Err("legacy cosine vector payload has zero norm".to_string())
    );
    let zero_euclidean = encode_item(&Item::<distance::Euclidean>::new(vec![0.0, 0.0, 0.0]));
    assert!(
        validate_legacy_row::<distance::Euclidean>(&zero_key, &zero_euclidean, &config, dimension,)
            .is_ok(),
        "zero norm remains valid for non-cosine metrics"
    );

    let mut mismatched = VectorIndexMetadata::new(config.clone());
    mismatched.config.dimension = 4;
    assert!(validate_legacy_row::<distance::Cosine>(
        &VectorKey::IndexMetadata(VectorIndexMetadataKey::new(index_id)),
        &crate::encoding::v2::legacy::vector::metadata::encode_legacy_metadata_for_contract(
            &mismatched,
        ),
        &config,
        dimension,
    )
    .is_err());
    let mut invalid_state = VectorIndexMetadata::new(config.clone());
    invalid_state.max_layer = 1;
    assert!(validate_legacy_row::<distance::Cosine>(
        &VectorKey::IndexMetadata(VectorIndexMetadataKey::new(index_id)),
        &crate::encoding::v2::legacy::vector::metadata::encode_legacy_metadata_for_contract(
            &invalid_state,
        ),
        &config,
        dimension,
    )
    .is_err());

    for prefix in [
        VectorStorageLane::Core.prefix_key(index_id),
        VectorStorageLane::Hot.prefix_key(index_id),
        VectorStorageLane::Layer0.prefix_key(index_id),
        VectorKey::EntryCandidatePrefix(VectorEntryCandidatePrefixKey::new(index_id)),
        VectorKey::ReverseEdgePrefix(VectorReverseEdgePrefixKey::new(index_id, 1)),
    ] {
        assert!(
            validate_legacy_row::<distance::Cosine>(&prefix, &[], &config, dimension).is_err(),
            "persisted prefix {prefix:?} must fail closed"
        );
    }
}

/// Exercises exact migration-read absence, corruption, and payload outcomes.
async fn run_legacy_migration_read_contracts() {
    let db = Db::open(
        "production-legacy-migration-reads",
        Arc::new(InMemory::new()),
    )
    .await
    .unwrap();
    let definition = legacy_definition();
    let physical_name = "production-legacy-migration-reads";
    let keyspace =
        VectorRowKeyspace::from_legacy_name(physical_name.into(), DataScope::LegacyUnscoped);
    let metadata = VectorIndexMetadata::new(VectorIndexConfig::from_v2_definition(
        &definition,
        physical_name,
    ));
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    transaction
        .put(
            keyspace.key(VectorKey::IndexMetadata(VectorIndexMetadataKey::new(
                keyspace.index_id(),
            ))),
            Bytes::copy_from_slice(
                &crate::encoding::v2::legacy::vector::metadata::encode_legacy_metadata_for_contract(
                    &metadata,
                ),
            ),
        )
        .unwrap();
    transaction
        .put(
            keyspace.key(VectorKey::Layer0Neighbors(VectorLayer0NeighborsKey::new(
                keyspace.index_id(),
                2,
            ))),
            encode_layer0_neighbors(&[]),
        )
        .unwrap();
    for entity_id in [3, 4] {
        transaction
            .put(
                keyspace.key(VectorKey::SimHash(VectorSimHashKey::new(
                    keyspace.index_id(),
                    entity_id,
                ))),
                encode_simhash(17),
            )
            .unwrap();
    }
    transaction
        .put(
            keyspace.key(VectorKey::Vector(VectorItemKey::new(
                keyspace.index_id(),
                crate::search::vector::simhash::order_code_from_simhash_bits(17),
                4,
            ))),
            encode_item(&Item::<distance::Cosine>::new(vec![1.0, 0.0, 0.0])),
        )
        .unwrap();
    transaction.commit().await.unwrap();

    let rows = VectorRows::new(&db, &keyspace);
    assert!(matches!(
        rows.legacy_vector_for_migration::<distance::Cosine>(1, &definition)
            .await
            .unwrap(),
        LegacyVectorMigrationRead::Absent { .. }
    ));
    assert!(matches!(
        rows.legacy_vector_for_migration::<distance::Cosine>(2, &definition)
            .await,
        Err(HelixDbError::InvariantViolation(message)) if message.contains("missing simhash")
    ));
    assert!(matches!(
        rows.legacy_vector_for_migration::<distance::Cosine>(3, &definition)
            .await
            .unwrap(),
        LegacyVectorMigrationRead::Absent { .. }
    ));
    let LegacyVectorMigrationRead::Present {
        vector,
        input_bytes,
    } = rows
        .legacy_vector_for_migration::<distance::Cosine>(4, &definition)
        .await
        .unwrap()
    else {
        panic!("complete legacy migration rows return their decoded payload")
    };
    assert_eq!(vector, vec![1.0, 0.0, 0.0]);
    assert!(input_bytes > 0);
    db.close().await.unwrap();
}

async fn run_simhash_directory_contracts() {
    let db = Db::open(
        "production-vector-storage-simhash-directory",
        Arc::new(InMemory::new()),
    )
    .await
    .unwrap();
    let keyspace = VectorRowKeyspace::new(
        "production-vector-storage-simhash-directory".to_string(),
        DataScope::LegacyUnscoped,
    );
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    for (order_code, node_id) in [(17, 1), (23, 2)] {
        transaction
            .put(
                keyspace.key(VectorKey::SimHashDirectory(VectorSimHashDirectoryKey::new(
                    keyspace.index_id(),
                    order_code,
                    node_id,
                ))),
                encode_simhash_directory_marker_v1(),
            )
            .unwrap();
    }
    transaction.commit().await.unwrap();

    let rows = VectorRows::new(&db, &keyspace);
    let empty = rows
        .simhash_directory_window_measured(0, u64::MAX, 0, usize::MAX)
        .await
        .unwrap();
    assert!(empty.into_entries().is_empty());
    let byte_limited = rows
        .simhash_directory_window_measured(0, u64::MAX, 2, 1)
        .await
        .unwrap();
    assert!(byte_limited.into_entries().is_empty());
    let row_limited = rows
        .simhash_directory_window_measured(0, u64::MAX, 1, usize::MAX)
        .await
        .unwrap();
    assert_eq!(row_limited.into_entries().len(), 1);
    db.close().await.unwrap();
}

/// Exercises directory publication inputs, validation modes, backfill, and cleanup.
async fn run_simhash_directory_migration_contracts() {
    let db = Db::open(
        "production-vector-storage-directory-migration",
        Arc::new(InMemory::new()),
    )
    .await
    .unwrap();
    let definition = legacy_definition();
    let physical_name = "production-vector-storage-directory-migration";
    let keyspace =
        VectorRowKeyspace::from_legacy_name(physical_name.into(), DataScope::LegacyUnscoped);
    let simhash_bits = 17;
    let first_order_code =
        crate::search::vector::simhash::order_code_from_simhash_bits(simhash_bits);
    let first_node_id = 1;
    let second_order_code = first_order_code.saturating_add(1);
    let second_node_id = 2;
    let first_vector_key = keyspace.key(VectorKey::Vector(VectorItemKey::new(
        keyspace.index_id(),
        first_order_code,
        first_node_id,
    )));
    let second_vector_key = keyspace.key(VectorKey::Vector(VectorItemKey::new(
        keyspace.index_id(),
        second_order_code,
        second_node_id,
    )));
    let first_marker_key = keyspace.key(VectorKey::SimHashDirectory(
        VectorSimHashDirectoryKey::new(keyspace.index_id(), first_order_code, first_node_id),
    ));
    let second_marker_key = keyspace.key(VectorKey::SimHashDirectory(
        VectorSimHashDirectoryKey::new(keyspace.index_id(), second_order_code, second_node_id),
    ));
    let metadata_key = keyspace.key(VectorKey::IndexMetadata(VectorIndexMetadataKey::new(
        keyspace.index_id(),
    )));
    let mut metadata = VectorIndexMetadata::new(VectorIndexConfig::from_v2_definition(
        &definition,
        physical_name,
    ));
    metadata.entry_point = Some(first_node_id);
    let current_metadata_value = Bytes::copy_from_slice(&encode_metadata(&metadata));

    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    transaction
        .put(
            &metadata_key,
            Bytes::copy_from_slice(
                &crate::encoding::v2::legacy::vector::metadata::encode_legacy_metadata_for_contract(
                    &metadata,
                ),
            ),
        )
        .unwrap();
    transaction
        .put(
            keyspace.key(VectorKey::SimHash(VectorSimHashKey::new(
                keyspace.index_id(),
                first_node_id,
            ))),
            encode_simhash(simhash_bits),
        )
        .unwrap();
    for key in [&first_vector_key, &second_vector_key] {
        transaction
            .put(
                key,
                encode_item(&Item::<distance::Cosine>::new(vec![1.0, 0.0, 0.0])),
            )
            .unwrap();
    }
    transaction
        .put(&first_marker_key, encode_simhash_directory_marker_v1())
        .unwrap();
    transaction.commit().await.unwrap();

    let rows = VectorRows::new(&db, &keyspace);
    let preflight = rows
        .validate_simhash_directory::<distance::Cosine>(
            None,
            &definition,
            SimHashDirectoryValidationMode::PreflightCanonicalCorrespondence,
            1,
            u64::MAX,
        )
        .await
        .unwrap();
    let SimHashDirectoryValidationOutcome::Valid {
        last_key: Some(first_cursor),
        markers: 1,
        input_bytes: first_preflight_bytes,
        exhausted: false,
        ..
    } = preflight
    else {
        panic!("the first directory preflight page must expose one exact cursor")
    };
    assert!(matches!(
        rows.validate_simhash_directory::<distance::Cosine>(
            Some(&first_cursor),
            &definition,
            SimHashDirectoryValidationMode::PreflightCanonicalCorrespondence,
            usize::MAX,
            u64::MAX,
        )
        .await
        .unwrap(),
        SimHashDirectoryValidationOutcome::Valid {
            markers: 0,
            exhausted: true,
            ..
        }
    ));
    assert!(matches!(
        rows.validate_simhash_directory::<distance::Cosine>(
            None,
            &definition,
            SimHashDirectoryValidationMode::PreflightCanonicalCorrespondence,
            usize::MAX,
            1,
        )
        .await
        .unwrap(),
        SimHashDirectoryValidationOutcome::Oversized { limit: 1, .. }
    ));
    assert!(matches!(
        rows.validate_simhash_directory::<distance::Cosine>(
            Some(&second_vector_key),
            &definition,
            SimHashDirectoryValidationMode::PreflightCanonicalCorrespondence,
            usize::MAX,
            u64::MAX,
        )
        .await
        .unwrap(),
        SimHashDirectoryValidationOutcome::Invalid { .. }
    ));
    assert!(matches!(
        rows.validate_legacy_physical::<distance::Cosine>(
            VectorStorageLane::Layer0,
            None,
            &definition,
            LegacyVectorValidationMode::BackfillSimHashDirectory {
                max_output_operations: NonZeroU64::MIN,
                max_output_bytes: NonZeroU64::new(u64::MAX).unwrap(),
            },
            usize::MAX,
            u64::MAX,
        )
        .await
        .unwrap(),
        LegacyVectorValidationOutcome::Valid {
            rows: 1,
            exhausted: false,
            ..
        }
    ));
    assert!(matches!(
        rows.validate_legacy_physical::<distance::Cosine>(
            VectorStorageLane::Layer0,
            None,
            &definition,
            LegacyVectorValidationMode::BackfillSimHashDirectory {
                max_output_operations: NonZeroU64::MIN,
                max_output_bytes: NonZeroU64::MIN,
            },
            usize::MAX,
            u64::MAX,
        )
        .await
        .unwrap(),
        LegacyVectorValidationOutcome::Oversized { limit: 1, .. }
    ));
    assert!(matches!(
        rows.validate_simhash_directory::<distance::Cosine>(
            None,
            &definition,
            SimHashDirectoryValidationMode::FinalLegacyWithEntryPoint,
            usize::MAX,
            u64::MAX,
        )
        .await
        .unwrap(),
        SimHashDirectoryValidationOutcome::Valid {
            markers: 1,
            exhausted: true,
            ..
        }
    ));

    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    transaction
        .put(&metadata_key, &current_metadata_value)
        .unwrap();
    transaction.commit().await.unwrap();
    assert!(matches!(
        rows.validate_simhash_directory::<distance::Cosine>(
            None,
            &definition,
            SimHashDirectoryValidationMode::FinalCurrentWithEntryPoint,
            usize::MAX,
            u64::MAX,
        )
        .await
        .unwrap(),
        SimHashDirectoryValidationOutcome::Valid {
            markers: 1,
            exhausted: true,
            ..
        }
    ));

    let backfill = rows
        .backfill_missing_simhash_directory::<distance::Cosine>(
            None,
            &definition,
            usize::MAX,
            u64::MAX,
            NonZeroU64::new(u64::MAX).unwrap(),
            NonZeroU64::new(u64::MAX).unwrap(),
        )
        .await
        .unwrap();
    let CanonicalVectorDirectoryBackfillOutcome::Valid {
        canonical_vectors: 2,
        existing_markers: 1,
        directory_entries,
        predicted_directory_writes,
        exhausted: true,
        ..
    } = backfill
    else {
        panic!("backfill must find the one missing directory marker")
    };
    assert_eq!(directory_entries.len(), 1);
    assert_eq!(
        directory_entries[0].physical_key.as_ref(),
        second_vector_key.as_ref()
    );
    assert_eq!(predicted_directory_writes.operations(), 1);
    assert!(matches!(
        rows.backfill_missing_simhash_directory::<distance::Cosine>(
            None,
            &definition,
            usize::MAX,
            u64::MAX,
            NonZeroU64::new(u64::MAX).unwrap(),
            NonZeroU64::MIN,
        )
        .await
        .unwrap(),
        CanonicalVectorDirectoryBackfillOutcome::Valid {
            canonical_vectors: 1,
            exhausted: false,
            ..
        }
    ));

    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let measured = super::super::VectorWriteRecorder::new();
    let measured = measured.bind(&transaction);
    VectorWriteRows::new(&measured, &keyspace)
        .put_simhash_directory_entry(&directory_entries[0])
        .unwrap();
    assert_eq!(measured.measurement().unwrap(), predicted_directory_writes);
    transaction.commit().await.unwrap();
    assert!(db.get(&second_marker_key).await.unwrap().is_some());

    let marker_row_bytes =
        u64::try_from(first_marker_key.len() + encode_simhash_directory_marker_v1().len()).unwrap();
    let second_marker_row_bytes =
        u64::try_from(second_marker_key.len() + encode_simhash_directory_marker_v1().len())
            .unwrap();
    let directory_rows_bytes = marker_row_bytes + second_marker_row_bytes;
    let metadata_row_bytes =
        u64::try_from(metadata_key.len() + current_metadata_value.len()).unwrap();
    let entry_simhash_key = keyspace.key(VectorKey::SimHash(VectorSimHashKey::new(
        keyspace.index_id(),
        first_node_id,
    )));
    let entry_simhash_bytes =
        u64::try_from(entry_simhash_key.len() + encode_simhash(simhash_bits).len()).unwrap();
    assert!(matches!(
        rows.validate_simhash_directory::<distance::Cosine>(
            None,
            &definition,
            SimHashDirectoryValidationMode::FinalCurrentWithEntryPoint,
            usize::MAX,
            marker_row_bytes,
        )
        .await
        .unwrap(),
        SimHashDirectoryValidationOutcome::Valid {
            markers: 1,
            exhausted: false,
            ..
        }
    ));
    assert!(matches!(
        rows.validate_simhash_directory::<distance::Cosine>(
            None,
            &definition,
            SimHashDirectoryValidationMode::PreflightCanonicalCorrespondence,
            usize::MAX,
            first_preflight_bytes + second_marker_row_bytes,
        )
        .await
        .unwrap(),
        SimHashDirectoryValidationOutcome::Valid {
            markers: 1,
            exhausted: false,
            ..
        }
    ));

    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    transaction.delete(&metadata_key).unwrap();
    transaction.commit().await.unwrap();
    assert!(matches!(
        rows.validate_simhash_directory::<distance::Cosine>(
            None,
            &definition,
            SimHashDirectoryValidationMode::FinalCurrentWithEntryPoint,
            usize::MAX,
            u64::MAX,
        )
        .await
        .unwrap(),
        SimHashDirectoryValidationOutcome::Invalid { .. }
    ));

    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    transaction
        .put(&metadata_key, Bytes::from_static(b"corrupt"))
        .unwrap();
    transaction.commit().await.unwrap();
    assert!(matches!(
        rows.validate_simhash_directory::<distance::Cosine>(
            None,
            &definition,
            SimHashDirectoryValidationMode::FinalCurrentWithEntryPoint,
            usize::MAX,
            u64::MAX,
        )
        .await
        .unwrap(),
        SimHashDirectoryValidationOutcome::Invalid { .. }
    ));

    let mut invalid_state = metadata.clone();
    invalid_state.entry_point = None;
    invalid_state.max_layer = 1;
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    transaction
        .put(
            &metadata_key,
            Bytes::copy_from_slice(&encode_metadata(&invalid_state)),
        )
        .unwrap();
    transaction.commit().await.unwrap();
    assert!(matches!(
        rows.validate_simhash_directory::<distance::Cosine>(
            None,
            &definition,
            SimHashDirectoryValidationMode::FinalCurrentWithEntryPoint,
            usize::MAX,
            u64::MAX,
        )
        .await
        .unwrap(),
        SimHashDirectoryValidationOutcome::Invalid { .. }
    ));

    let mut wrong_name = metadata.clone();
    wrong_name.config.index_name = "another-physical-name".into();
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    transaction
        .put(
            &metadata_key,
            Bytes::copy_from_slice(&encode_metadata(&wrong_name)),
        )
        .unwrap();
    transaction.commit().await.unwrap();
    assert!(matches!(
        rows.validate_simhash_directory::<distance::Cosine>(
            None,
            &definition,
            SimHashDirectoryValidationMode::FinalCurrentWithEntryPoint,
            usize::MAX,
            u64::MAX,
        )
        .await
        .unwrap(),
        SimHashDirectoryValidationOutcome::Invalid { .. }
    ));

    let mut wrong_contract = metadata.clone();
    wrong_contract.config.dimension = 4;
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    transaction
        .put(
            &metadata_key,
            Bytes::copy_from_slice(&encode_metadata(&wrong_contract)),
        )
        .unwrap();
    transaction.commit().await.unwrap();
    assert!(matches!(
        rows.validate_simhash_directory::<distance::Cosine>(
            None,
            &definition,
            SimHashDirectoryValidationMode::FinalCurrentWithEntryPoint,
            usize::MAX,
            u64::MAX,
        )
        .await
        .unwrap(),
        SimHashDirectoryValidationOutcome::Invalid { .. }
    ));

    let mut missing_entry_rows = metadata.clone();
    missing_entry_rows.entry_point = Some(99);
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    transaction
        .put(
            &metadata_key,
            Bytes::copy_from_slice(&encode_metadata(&missing_entry_rows)),
        )
        .unwrap();
    transaction.commit().await.unwrap();
    assert!(matches!(
        rows.validate_simhash_directory::<distance::Cosine>(
            None,
            &definition,
            SimHashDirectoryValidationMode::FinalCurrentWithEntryPoint,
            usize::MAX,
            u64::MAX,
        )
        .await
        .unwrap(),
        SimHashDirectoryValidationOutcome::Invalid { .. }
    ));

    let missing_simhash_key = keyspace.key(VectorKey::SimHash(VectorSimHashKey::new(
        keyspace.index_id(),
        99,
    )));
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    transaction
        .put(&missing_simhash_key, Bytes::from_static(b"corrupt"))
        .unwrap();
    transaction.commit().await.unwrap();
    assert!(matches!(
        rows.validate_simhash_directory::<distance::Cosine>(
            None,
            &definition,
            SimHashDirectoryValidationMode::FinalCurrentWithEntryPoint,
            usize::MAX,
            u64::MAX,
        )
        .await
        .unwrap(),
        SimHashDirectoryValidationOutcome::Invalid { .. }
    ));

    let missing_entry_bits = 42;
    let missing_entry_marker =
        keyspace.key(VectorKey::SimHashDirectory(VectorSimHashDirectoryKey::new(
            keyspace.index_id(),
            crate::search::vector::simhash::order_code_from_simhash_bits(missing_entry_bits),
            99,
        )));
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    transaction
        .put(&missing_simhash_key, encode_simhash(missing_entry_bits))
        .unwrap();
    transaction.commit().await.unwrap();
    assert!(matches!(
        rows.validate_simhash_directory::<distance::Cosine>(
            None,
            &definition,
            SimHashDirectoryValidationMode::FinalCurrentWithEntryPoint,
            usize::MAX,
            u64::MAX,
        )
        .await
        .unwrap(),
        SimHashDirectoryValidationOutcome::Invalid { .. }
    ));

    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    transaction
        .put(&missing_entry_marker, Bytes::from_static(b"corrupt"))
        .unwrap();
    transaction.commit().await.unwrap();
    assert!(matches!(
        rows.validate_simhash_directory::<distance::Cosine>(
            None,
            &definition,
            SimHashDirectoryValidationMode::FinalCurrentWithEntryPoint,
            usize::MAX,
            u64::MAX,
        )
        .await
        .unwrap(),
        SimHashDirectoryValidationOutcome::Invalid { .. }
    ));

    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    transaction
        .put(
            &metadata_key,
            Bytes::copy_from_slice(&encode_metadata(&metadata)),
        )
        .unwrap();
    transaction.delete(&missing_simhash_key).unwrap();
    transaction.delete(&missing_entry_marker).unwrap();
    transaction
        .put(&second_marker_key, Bytes::from_static(b"corrupt"))
        .unwrap();
    transaction.commit().await.unwrap();
    assert!(matches!(
        rows.backfill_missing_simhash_directory::<distance::Cosine>(
            None,
            &definition,
            usize::MAX,
            u64::MAX,
            NonZeroU64::new(u64::MAX).unwrap(),
            NonZeroU64::new(u64::MAX).unwrap(),
        )
        .await
        .unwrap(),
        CanonicalVectorDirectoryBackfillOutcome::Invalid { .. }
    ));
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    transaction
        .put(&second_marker_key, encode_simhash_directory_marker_v1())
        .unwrap();
    transaction.commit().await.unwrap();

    for limit in [
        directory_rows_bytes,
        directory_rows_bytes + metadata_row_bytes,
        directory_rows_bytes + metadata_row_bytes + entry_simhash_bytes,
    ] {
        assert!(matches!(
            rows.validate_simhash_directory::<distance::Cosine>(
                None,
                &definition,
                SimHashDirectoryValidationMode::FinalCurrentWithEntryPoint,
                usize::MAX,
                limit,
            )
            .await
            .unwrap(),
            SimHashDirectoryValidationOutcome::Valid {
                markers: 2,
                exhausted: false,
                ..
            }
        ));
    }
    assert!(matches!(
        rows.validate_simhash_directory::<distance::Cosine>(
            None,
            &definition,
            SimHashDirectoryValidationMode::PreflightCanonicalCorrespondence,
            usize::MAX,
            marker_row_bytes,
        )
        .await
        .unwrap(),
        SimHashDirectoryValidationOutcome::Oversized { .. }
    ));
    assert!(matches!(
        rows.validate_simhash_directory::<distance::Cosine>(
            Some(&second_marker_key),
            &definition,
            SimHashDirectoryValidationMode::FinalCurrentWithEntryPoint,
            usize::MAX,
            metadata_row_bytes,
        )
        .await
        .unwrap(),
        SimHashDirectoryValidationOutcome::Oversized { .. }
    ));
    assert!(matches!(
        rows.validate_simhash_directory::<distance::Cosine>(
            Some(&second_marker_key),
            &definition,
            SimHashDirectoryValidationMode::FinalCurrentWithEntryPoint,
            usize::MAX,
            metadata_row_bytes + entry_simhash_bytes,
        )
        .await
        .unwrap(),
        SimHashDirectoryValidationOutcome::Oversized { .. }
    ));

    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    transaction
        .put(&first_marker_key, Bytes::from_static(b"corrupt"))
        .unwrap();
    transaction.commit().await.unwrap();
    assert!(matches!(
        rows.validate_simhash_directory::<distance::Cosine>(
            Some(&second_marker_key),
            &definition,
            SimHashDirectoryValidationMode::FinalCurrentWithEntryPoint,
            usize::MAX,
            u64::MAX,
        )
        .await
        .unwrap(),
        SimHashDirectoryValidationOutcome::Invalid { .. }
    ));
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    transaction
        .put(&first_marker_key, encode_simhash_directory_marker_v1())
        .unwrap();
    transaction.commit().await.unwrap();

    let empty_directory = VectorRowKeyspace::from_legacy_name(
        "production-empty-directory-final-proof".into(),
        DataScope::LegacyUnscoped,
    );
    let empty_metadata = VectorIndexMetadata::new(VectorIndexConfig::from_v2_definition(
        &definition,
        empty_directory.physical_name(),
    ));
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    transaction
        .put(
            empty_directory.key(VectorKey::IndexMetadata(VectorIndexMetadataKey::new(
                empty_directory.index_id(),
            ))),
            Bytes::copy_from_slice(&encode_metadata(&empty_metadata)),
        )
        .unwrap();
    transaction.commit().await.unwrap();
    assert!(matches!(
        VectorRows::new(&db, &empty_directory)
            .validate_simhash_directory::<distance::Cosine>(
                None,
                &definition,
                SimHashDirectoryValidationMode::FinalCurrentWithEntryPoint,
                usize::MAX,
                1,
            )
            .await
            .unwrap(),
        SimHashDirectoryValidationOutcome::Oversized { .. }
    ));

    assert!(matches!(
        rows.backfill_missing_simhash_directory::<distance::Cosine>(
            Some(&first_vector_key),
            &definition,
            usize::MAX,
            u64::MAX,
            NonZeroU64::new(u64::MAX).unwrap(),
            NonZeroU64::new(u64::MAX).unwrap(),
        )
        .await
        .unwrap(),
        CanonicalVectorDirectoryBackfillOutcome::Valid {
            canonical_vectors: 1,
            exhausted: true,
            ..
        }
    ));

    assert!(matches!(
        rows.backfill_missing_simhash_directory::<distance::Cosine>(
            None,
            &definition,
            1,
            u64::MAX,
            NonZeroU64::new(u64::MAX).unwrap(),
            NonZeroU64::new(u64::MAX).unwrap(),
        )
        .await
        .unwrap(),
        CanonicalVectorDirectoryBackfillOutcome::Valid {
            canonical_vectors: 1,
            exhausted: false,
            ..
        }
    ));
    assert!(matches!(
        rows.backfill_missing_simhash_directory::<distance::Cosine>(
            None,
            &definition,
            usize::MAX,
            1,
            NonZeroU64::new(u64::MAX).unwrap(),
            NonZeroU64::new(u64::MAX).unwrap(),
        )
        .await
        .unwrap(),
        CanonicalVectorDirectoryBackfillOutcome::Oversized { limit: 1, .. }
    ));
    assert!(matches!(
        rows.backfill_missing_simhash_directory::<distance::Cosine>(
            Some(&first_marker_key),
            &definition,
            usize::MAX,
            u64::MAX,
            NonZeroU64::new(u64::MAX).unwrap(),
            NonZeroU64::new(u64::MAX).unwrap(),
        )
        .await
        .unwrap(),
        CanonicalVectorDirectoryBackfillOutcome::Invalid { .. }
    ));

    let mut cleanup = rows.simhash_directory_cleanup_scan().await.unwrap();
    let first_cleanup = cleanup.next().await.unwrap().unwrap();
    assert!(first_cleanup.input_bytes > 0);
    assert!(cleanup.next().await.unwrap().is_some());
    assert!(cleanup.next().await.unwrap().is_none());

    let missing_canonical = VectorRowKeyspace::from_legacy_name(
        "production-directory-missing-canonical".into(),
        DataScope::LegacyUnscoped,
    );
    let missing_marker_key = missing_canonical.key(VectorKey::SimHashDirectory(
        VectorSimHashDirectoryKey::new(missing_canonical.index_id(), 1, 1),
    ));
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    transaction
        .put(&missing_marker_key, encode_simhash_directory_marker_v1())
        .unwrap();
    transaction.commit().await.unwrap();
    assert!(matches!(
        VectorRows::new(&db, &missing_canonical)
            .validate_simhash_directory::<distance::Cosine>(
                None,
                &definition,
                SimHashDirectoryValidationMode::PreflightCanonicalCorrespondence,
                usize::MAX,
                u64::MAX,
            )
            .await
            .unwrap(),
        SimHashDirectoryValidationOutcome::Invalid { .. }
    ));
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    transaction
        .put(
            missing_canonical.key(VectorKey::Vector(VectorItemKey::new(
                missing_canonical.index_id(),
                1,
                1,
            ))),
            Bytes::from_static(b"corrupt"),
        )
        .unwrap();
    transaction.commit().await.unwrap();
    assert!(matches!(
        VectorRows::new(&db, &missing_canonical)
            .validate_simhash_directory::<distance::Cosine>(
                None,
                &definition,
                SimHashDirectoryValidationMode::PreflightCanonicalCorrespondence,
                usize::MAX,
                u64::MAX,
            )
            .await
            .unwrap(),
        SimHashDirectoryValidationOutcome::Invalid { .. }
    ));

    let corrupt = VectorRowKeyspace::from_legacy_name(
        "production-directory-corrupt-marker".into(),
        DataScope::LegacyUnscoped,
    );
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    transaction
        .put(
            corrupt.key(VectorKey::SimHashDirectory(VectorSimHashDirectoryKey::new(
                corrupt.index_id(),
                1,
                1,
            ))),
            Bytes::from_static(b"corrupt"),
        )
        .unwrap();
    transaction.commit().await.unwrap();
    assert!(matches!(
        VectorRows::new(&db, &corrupt)
            .validate_simhash_directory::<distance::Cosine>(
                None,
                &definition,
                SimHashDirectoryValidationMode::PreflightCanonicalCorrespondence,
                usize::MAX,
                u64::MAX,
            )
            .await
            .unwrap(),
        SimHashDirectoryValidationOutcome::Invalid { .. }
    ));

    let malformed_directory = VectorRowKeyspace::from_legacy_name(
        "production-directory-malformed-key".into(),
        DataScope::LegacyUnscoped,
    );
    let mut malformed_directory_key = malformed_directory
        .key(VectorKey::SimHashDirectoryPrefix(
            VectorSimHashDirectoryPrefixKey::new(malformed_directory.index_id()),
        ))
        .to_vec();
    malformed_directory_key.push(0xff);
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    transaction
        .put(
            Bytes::from(malformed_directory_key),
            encode_simhash_directory_marker_v1(),
        )
        .unwrap();
    transaction.commit().await.unwrap();
    assert!(matches!(
        VectorRows::new(&db, &malformed_directory)
            .validate_simhash_directory::<distance::Cosine>(
                None,
                &definition,
                SimHashDirectoryValidationMode::PreflightCanonicalCorrespondence,
                usize::MAX,
                u64::MAX,
            )
            .await
            .unwrap(),
        SimHashDirectoryValidationOutcome::Invalid { .. }
    ));

    let invalid_payload = VectorRowKeyspace::from_legacy_name(
        "production-directory-invalid-payload".into(),
        DataScope::LegacyUnscoped,
    );
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    transaction
        .put(
            invalid_payload.key(VectorKey::Vector(VectorItemKey::new(
                invalid_payload.index_id(),
                1,
                1,
            ))),
            Bytes::from_static(b"corrupt"),
        )
        .unwrap();
    transaction.commit().await.unwrap();
    assert!(matches!(
        VectorRows::new(&db, &invalid_payload)
            .backfill_missing_simhash_directory::<distance::Cosine>(
                None,
                &definition,
                usize::MAX,
                u64::MAX,
                NonZeroU64::new(u64::MAX).unwrap(),
                NonZeroU64::new(u64::MAX).unwrap(),
            )
            .await
            .unwrap(),
        CanonicalVectorDirectoryBackfillOutcome::Invalid { .. }
    ));
    let malformed_canonical = VectorRowKeyspace::from_legacy_name(
        "production-canonical-malformed-key".into(),
        DataScope::LegacyUnscoped,
    );
    let mut malformed_canonical_key = malformed_canonical
        .key(VectorKey::VectorPrefix(VectorItemPrefixKey::new(
            malformed_canonical.index_id(),
        )))
        .to_vec();
    malformed_canonical_key.push(0xff);
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    transaction
        .put(
            Bytes::from(malformed_canonical_key),
            Bytes::from_static(b"corrupt"),
        )
        .unwrap();
    transaction.commit().await.unwrap();
    assert!(matches!(
        VectorRows::new(&db, &malformed_canonical)
            .backfill_missing_simhash_directory::<distance::Cosine>(
                None,
                &definition,
                usize::MAX,
                u64::MAX,
                NonZeroU64::new(u64::MAX).unwrap(),
                NonZeroU64::new(u64::MAX).unwrap(),
            )
            .await
            .unwrap(),
        CanonicalVectorDirectoryBackfillOutcome::Invalid { .. }
    ));

    db.close().await.unwrap();
}

/// Exercises row and byte checkpoints, cursors, and terminal entry-point proofs.
async fn run_legacy_validation_scan_contracts() {
    let db = Db::open(
        "production-legacy-validation-scans",
        Arc::new(InMemory::new()),
    )
    .await
    .unwrap();
    let definition = legacy_definition();
    let physical_name = "production-legacy-validation-pages";
    let keyspace =
        VectorRowKeyspace::from_legacy_name(physical_name.into(), DataScope::LegacyUnscoped);
    let metadata = VectorIndexMetadata::new(VectorIndexConfig::from_v2_definition(
        &definition,
        physical_name,
    ));
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    transaction
        .put(
            keyspace.key(VectorKey::IndexMetadata(VectorIndexMetadataKey::new(
                keyspace.index_id(),
            ))),
            Bytes::copy_from_slice(
                &crate::encoding::v2::legacy::vector::metadata::encode_legacy_metadata_for_contract(
                    &metadata,
                ),
            ),
        )
        .unwrap();
    transaction
        .put(
            keyspace.key(VectorKey::TxnGuard(LegacyVectorTxnGuardKey::new(
                keyspace.index_id(),
            ))),
            crate::encoding::v2::legacy::vector::transaction_guard::encode_active_txn_guard(),
        )
        .unwrap();
    transaction.commit().await.unwrap();

    let rows = VectorRows::new(&db, &keyspace);
    let first = rows
        .validate_legacy_physical::<distance::Cosine>(
            VectorStorageLane::Core,
            None,
            &definition,
            LegacyVectorValidationMode::ReadOnly,
            1,
            u64::MAX,
        )
        .await
        .unwrap();
    let LegacyVectorValidationOutcome::Valid {
        last_key: Some(first_cursor),
        rows: 1,
        input_bytes: first_input_bytes,
        exhausted: false,
        ..
    } = first
    else {
        panic!("first row-limited page must retain its exact cursor")
    };
    assert!(matches!(
        rows.validate_legacy_physical::<distance::Cosine>(
            VectorStorageLane::Core,
            Some(&first_cursor),
            &definition,
            LegacyVectorValidationMode::ReadOnly,
            2,
            u64::MAX,
        )
        .await
        .unwrap(),
        LegacyVectorValidationOutcome::Valid {
            rows: 1,
            exhausted: true,
            ..
        }
    ));
    assert!(matches!(
        rows.validate_legacy_physical::<distance::Cosine>(
            VectorStorageLane::Core,
            None,
            &definition,
            LegacyVectorValidationMode::ReadOnly,
            usize::MAX,
            1,
        )
        .await
        .unwrap(),
        LegacyVectorValidationOutcome::Oversized { limit: 1, .. }
    ));
    assert!(matches!(
        rows.validate_legacy_physical::<distance::Cosine>(
            VectorStorageLane::Core,
            None,
            &definition,
            LegacyVectorValidationMode::ReadOnly,
            usize::MAX,
            first_input_bytes,
        )
        .await
        .unwrap(),
        LegacyVectorValidationOutcome::Valid {
            rows: 1,
            exhausted: false,
            ..
        }
    ));
    let foreign_cursor = keyspace.key(VectorStorageLane::Hot.prefix_key(keyspace.index_id()));
    assert!(matches!(
        rows.validate_legacy_physical::<distance::Cosine>(
            VectorStorageLane::Core,
            Some(&foreign_cursor),
            &definition,
            LegacyVectorValidationMode::ReadOnly,
            usize::MAX,
            u64::MAX,
        )
        .await
        .unwrap(),
        LegacyVectorValidationOutcome::Invalid { .. }
    ));

    let missing = VectorRowKeyspace::from_legacy_name(
        "production-legacy-validation-missing".into(),
        DataScope::LegacyUnscoped,
    );
    assert!(matches!(
        VectorRows::new(&db, &missing)
            .validate_legacy_physical::<distance::Cosine>(
                VectorStorageLane::Hot,
                None,
                &definition,
                LegacyVectorValidationMode::ReadOnly,
                usize::MAX,
                u64::MAX,
            )
            .await
            .unwrap(),
        LegacyVectorValidationOutcome::Invalid { .. }
    ));

    let corrupt = VectorRowKeyspace::from_legacy_name(
        "production-legacy-validation-corrupt".into(),
        DataScope::LegacyUnscoped,
    );
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    transaction
        .put(
            corrupt.key(VectorKey::IndexMetadata(VectorIndexMetadataKey::new(
                corrupt.index_id(),
            ))),
            Bytes::from_static(b"corrupt"),
        )
        .unwrap();
    transaction.commit().await.unwrap();
    assert!(matches!(
        VectorRows::new(&db, &corrupt)
            .validate_legacy_physical::<distance::Cosine>(
                VectorStorageLane::Hot,
                None,
                &definition,
                LegacyVectorValidationMode::ReadOnly,
                usize::MAX,
                u64::MAX,
            )
            .await
            .unwrap(),
        LegacyVectorValidationOutcome::Invalid { .. }
    ));

    let mismatch_name = "production-legacy-validation-mismatch";
    let mismatch =
        VectorRowKeyspace::from_legacy_name(mismatch_name.into(), DataScope::LegacyUnscoped);
    let mut mismatch_metadata = VectorIndexMetadata::new(VectorIndexConfig::from_v2_definition(
        &definition,
        mismatch_name,
    ));
    mismatch_metadata.config.dimension = 4;
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    transaction
        .put(
            mismatch.key(VectorKey::IndexMetadata(VectorIndexMetadataKey::new(
                mismatch.index_id(),
            ))),
            Bytes::copy_from_slice(
                &crate::encoding::v2::legacy::vector::metadata::encode_legacy_metadata_for_contract(
                    &mismatch_metadata,
                ),
            ),
        )
        .unwrap();
    transaction.commit().await.unwrap();
    assert!(matches!(
        VectorRows::new(&db, &mismatch)
            .validate_legacy_physical::<distance::Cosine>(
                VectorStorageLane::Hot,
                None,
                &definition,
                LegacyVectorValidationMode::ReadOnly,
                usize::MAX,
                u64::MAX,
            )
            .await
            .unwrap(),
        LegacyVectorValidationOutcome::Invalid { .. }
    ));

    db.close().await.unwrap();
}

/// Exercises all terminal entry-point proofs without rewriting physical rows.
async fn run_legacy_validation_entry_point_contracts() {
    #[derive(Clone, Copy)]
    enum Fixture {
        MissingSimHash,
        MalformedSimHash,
        MissingPayload,
        MalformedPayload,
        Valid,
    }

    let db = Db::open(
        "production-legacy-validation-entry-points",
        Arc::new(InMemory::new()),
    )
    .await
    .unwrap();
    let definition = legacy_definition();
    for (position, fixture) in [
        Fixture::MissingSimHash,
        Fixture::MalformedSimHash,
        Fixture::MissingPayload,
        Fixture::MalformedPayload,
        Fixture::Valid,
    ]
    .into_iter()
    .enumerate()
    {
        let physical_name = format!("production-legacy-entry-point-{position}");
        let keyspace =
            VectorRowKeyspace::from_legacy_name(physical_name.clone(), DataScope::LegacyUnscoped);
        let mut metadata = VectorIndexMetadata::new(VectorIndexConfig::from_v2_definition(
            &definition,
            &physical_name,
        ));
        metadata.entry_point = Some(1);
        metadata.count = 1;
        let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
        transaction
            .put(
                keyspace.key(VectorKey::IndexMetadata(VectorIndexMetadataKey::new(
                    keyspace.index_id(),
                ))),
                Bytes::copy_from_slice(
                    &crate::encoding::v2::legacy::vector::metadata::encode_legacy_metadata_for_contract(
                        &metadata,
                    ),
                ),
            )
            .unwrap();
        if !matches!(fixture, Fixture::MissingSimHash) {
            transaction
                .put(
                    keyspace.key(VectorKey::SimHash(VectorSimHashKey::new(
                        keyspace.index_id(),
                        1,
                    ))),
                    if matches!(fixture, Fixture::MalformedSimHash) {
                        Bytes::from_static(b"malformed")
                    } else {
                        Bytes::copy_from_slice(&encode_simhash(17))
                    },
                )
                .unwrap();
        }
        let item_key = keyspace.key(VectorKey::Vector(VectorItemKey::new(
            keyspace.index_id(),
            crate::search::vector::simhash::order_code_from_simhash_bits(17),
            1,
        )));
        if matches!(fixture, Fixture::MalformedPayload | Fixture::Valid) {
            transaction
                .put(
                    item_key.clone(),
                    if matches!(fixture, Fixture::MalformedPayload) {
                        Bytes::from_static(b"malformed")
                    } else {
                        encode_item(&Item::<distance::Cosine>::new(vec![1.0, 0.0, 0.0]))
                    },
                )
                .unwrap();
        }
        transaction.commit().await.unwrap();

        let cursor = matches!(fixture, Fixture::MalformedPayload | Fixture::Valid)
            .then_some(item_key.as_ref());
        let outcome = VectorRows::new(&db, &keyspace)
            .validate_legacy_physical::<distance::Cosine>(
                VectorStorageLane::Layer0,
                cursor,
                &definition,
                LegacyVectorValidationMode::ReadOnly,
                usize::MAX,
                u64::MAX,
            )
            .await
            .unwrap();
        if matches!(fixture, Fixture::Valid) {
            assert!(matches!(
                outcome,
                LegacyVectorValidationOutcome::Valid {
                    exhausted: true,
                    ..
                }
            ));
        } else {
            assert!(matches!(
                outcome,
                LegacyVectorValidationOutcome::Invalid { .. }
            ));
        }
    }

    let physical_name = "production-legacy-validation-malformed-key";
    let keyspace =
        VectorRowKeyspace::from_legacy_name(physical_name.into(), DataScope::LegacyUnscoped);
    let metadata = VectorIndexMetadata::new(VectorIndexConfig::from_v2_definition(
        &definition,
        physical_name,
    ));
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    transaction
        .put(
            keyspace.key(VectorKey::IndexMetadata(VectorIndexMetadataKey::new(
                keyspace.index_id(),
            ))),
            Bytes::copy_from_slice(
                &crate::encoding::v2::legacy::vector::metadata::encode_legacy_metadata_for_contract(
                    &metadata,
                ),
            ),
        )
        .unwrap();
    let mut malformed_key = keyspace
        .key(VectorStorageLane::Layer0.prefix_key(keyspace.index_id()))
        .to_vec();
    malformed_key.push(0xff);
    transaction
        .put(malformed_key, Bytes::from_static(b"malformed"))
        .unwrap();
    transaction.commit().await.unwrap();
    assert!(matches!(
        VectorRows::new(&db, &keyspace)
            .validate_legacy_physical::<distance::Cosine>(
                VectorStorageLane::Layer0,
                None,
                &definition,
                LegacyVectorValidationMode::ReadOnly,
                usize::MAX,
                u64::MAX,
            )
            .await
            .unwrap(),
        LegacyVectorValidationOutcome::Invalid { .. }
    ));

    db.close().await.unwrap();
}

/// Verifies legacy bytes, tenant isolation, and opaque canonical ordering.
fn run_keyspace_contracts() {
    let physical_name = "production-typed-row-keyspace";
    let index_id = index_id_from_name(physical_name);
    let logical = VectorKey::IndexMetadata(VectorIndexMetadataKey::new(index_id));
    let legacy = VectorRowKeyspace::new(physical_name.to_string(), DataScope::LegacyUnscoped);
    assert_eq!(legacy.physical_name(), physical_name);
    assert_eq!(legacy.index_id(), index_id);
    assert_eq!(legacy.scope(), DataScope::LegacyUnscoped);
    assert_eq!(legacy.key(logical), logical.to_bytes());
    let persisted =
        VectorRowKeyspace::from_legacy_name(physical_name.to_string(), DataScope::LegacyUnscoped);
    assert_eq!(persisted.physical_name(), legacy.physical_name());
    assert_eq!(persisted.index_id(), legacy.index_id());
    assert_eq!(persisted.scope(), legacy.scope());

    let first = VectorRowKeyspace::new(
        physical_name.to_string(),
        DataScope::Tenant(TenantId::from_u128(1)),
    );
    let second = VectorRowKeyspace::new(
        physical_name.to_string(),
        DataScope::Tenant(TenantId::from_u128(2)),
    );
    let first_key = first.key(logical);
    assert_eq!(
        first.strip_physical_key(&first_key).unwrap(),
        logical.to_bytes()
    );
    assert!(first.strip_physical_key(&second.key(logical)).is_err());

    let first_token = first.canonical_vector_row_key(7, 11);
    let second_token = first.canonical_vector_row_key(3, 12);
    assert_eq!(
        first_token.physical_order(&second_token),
        first_token.physical_key.cmp(&second_token.physical_key)
    );
}

/// Verifies every current typed read/write family and cross-keyspace rejection.
async fn run_row_contracts() {
    let db = Db::open("production-typed-vector-rows", Arc::new(InMemory::new()))
        .await
        .unwrap();
    let legacy_keyspace = VectorRowKeyspace::from_legacy_name(
        "production-legacy-vector-rows".to_string(),
        DataScope::LegacyUnscoped,
    );
    assert!(VectorRows::new(&db, &legacy_keyspace)
        .legacy_metadata()
        .await
        .unwrap()
        .is_none());
    let legacy_metadata = VectorIndexMetadata::new(VectorIndexConfig::new(
        legacy_keyspace.physical_name(),
        "embedding",
        3,
    ));
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    transaction
        .put(
            legacy_keyspace.key(VectorKey::IndexMetadata(VectorIndexMetadataKey::new(
                legacy_keyspace.index_id(),
            ))),
            Bytes::copy_from_slice(
                &crate::encoding::v2::legacy::vector::metadata::encode_legacy_metadata_for_contract(
                    &legacy_metadata,
                ),
            ),
        )
        .unwrap();
    transaction.commit().await.unwrap();
    let decoded = VectorRows::new(&db, &legacy_keyspace)
        .legacy_metadata()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(decoded.config.index_name, legacy_keyspace.physical_name());
    assert_eq!(decoded.config.dimension, 3);

    let mismatch_keyspace = VectorRowKeyspace::from_legacy_name(
        "production-legacy-vector-mismatch".to_string(),
        DataScope::LegacyUnscoped,
    );
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    transaction
        .put(
            mismatch_keyspace.key(VectorKey::IndexMetadata(VectorIndexMetadataKey::new(
                mismatch_keyspace.index_id(),
            ))),
            Bytes::copy_from_slice(
                &crate::encoding::v2::legacy::vector::metadata::encode_legacy_metadata_for_contract(
                    &legacy_metadata,
                ),
            ),
        )
        .unwrap();
    transaction.commit().await.unwrap();
    assert!(matches!(
        VectorRows::new(&db, &mismatch_keyspace)
            .legacy_metadata()
            .await,
        Err(HelixDbError::Config(message)) if message.contains("collision")
    ));

    let corrupt_keyspace = VectorRowKeyspace::from_legacy_name(
        "production-legacy-vector-corrupt".to_string(),
        DataScope::LegacyUnscoped,
    );
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    transaction
        .put(
            corrupt_keyspace.key(VectorKey::IndexMetadata(VectorIndexMetadataKey::new(
                corrupt_keyspace.index_id(),
            ))),
            Bytes::from_static(b"corrupt"),
        )
        .unwrap();
    transaction.commit().await.unwrap();
    assert!(VectorRows::new(&db, &corrupt_keyspace)
        .legacy_metadata()
        .await
        .is_err());

    let keyspace = VectorRowKeyspace::new(
        "production-typed-vector-rows".to_string(),
        DataScope::Tenant(TenantId::from_u128(7)),
    );
    let foreign = VectorRowKeyspace::new(
        "production-typed-vector-rows:foreign".to_string(),
        keyspace.scope(),
    );
    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let measured = MeasuredVectorTransaction::new(&txn);
    let writes = VectorWriteRows::new(&measured, &keyspace);
    assert!(!writes.metadata_exists().await.unwrap());
    assert_eq!(
        VectorRows::new(&measured, &keyspace)
            .metadata_input_bytes()
            .await
            .unwrap(),
        u64::try_from(
            keyspace
                .key(VectorKey::IndexMetadata(VectorIndexMetadataKey::new(
                    keyspace.index_id()
                )))
                .len()
        )
        .unwrap()
    );

    let metadata = VectorIndexMetadata::new(VectorIndexConfig::new(
        keyspace.physical_name(),
        "embedding",
        3,
    ));
    writes.put_metadata(&metadata).unwrap();
    writes.put_layer0_neighbors(1, &[2, 3]).unwrap();
    writes.put_upper_neighbors(2, 1, &[4, 5]).unwrap();
    writes
        .put_upper_vector(1, Bytes::from_static(b"upper-vector"))
        .unwrap();
    let canonical = keyspace.canonical_vector_row_key(1, 17);
    writes
        .put_canonical_vector(&canonical, Bytes::from_static(b"canonical-vector"))
        .unwrap();
    writes.put_entry_candidate(1, 3).unwrap();
    writes.put_entry_candidate(2, 1).unwrap();
    writes.put_reverse_locator(9, 2, 1).unwrap();
    writes.put_reverse_locator(9, 2, 2).unwrap();
    writes.put_reverse_locator(9, 1, 3).unwrap();
    measured
        .put(
            keyspace.key(VectorKey::SimHash(VectorSimHashKey::new(
                keyspace.index_id(),
                1,
            ))),
            encode_simhash(0x1234),
        )
        .unwrap();

    let mut cleanup_scan = VectorRows::new(&measured, &keyspace)
        .cleanup_scan()
        .await
        .unwrap();
    let mut cleanup_row_count = 0;
    while let Some(row) = cleanup_scan.next().await.unwrap() {
        assert!(row.input_bytes() >= row.output_bytes());
        cleanup_row_count += 1;
    }
    assert_eq!(cleanup_row_count, 13);

    let mut malformed_candidate = keyspace
        .key(VectorKey::EntryCandidatePrefix(
            VectorEntryCandidatePrefixKey::new(keyspace.index_id()),
        ))
        .to_vec();
    malformed_candidate.push(0xFF);
    measured
        .put(malformed_candidate, Bytes::from_static(b"malformed"))
        .unwrap();
    let mut malformed_reverse = keyspace
        .key(VectorKey::ReverseEdgePrefix(
            VectorReverseEdgePrefixKey::new(keyspace.index_id(), 9),
        ))
        .to_vec();
    malformed_reverse.push(0xFF);
    measured
        .put(malformed_reverse, Bytes::from_static(b"malformed"))
        .unwrap();
    measured
        .put(
            keyspace.key(VectorKey::SimHash(VectorSimHashKey::new(
                keyspace.index_id(),
                8,
            ))),
            Bytes::from_static(b"corrupt"),
        )
        .unwrap();
    measured
        .put(
            keyspace.key(VectorKey::EntryCandidateNode(
                VectorEntryCandidateNodeKey::new(keyspace.index_id(), 8),
            )),
            Bytes::from_static(b"corrupt"),
        )
        .unwrap();

    assert!(writes.metadata_exists().await.unwrap());
    let rows = VectorRows::new(&measured, &keyspace);
    let decoded_metadata = rows.metadata().await.unwrap().unwrap();
    assert_eq!(
        decoded_metadata.config.index_name,
        metadata.config.index_name
    );
    assert_eq!(decoded_metadata.config.property_name, "embedding");
    assert_eq!(decoded_metadata.config.dimension, 3);
    assert!(rows.metadata_input_bytes().await.unwrap() > 0);
    assert_eq!(rows.layer0_neighbors(1).await.unwrap(), vec![2, 3]);
    assert_eq!(rows.layer0_neighbors(99).await.unwrap(), Vec::<u64>::new());
    assert_eq!(rows.layer0_neighbor_row(1).await.unwrap(), Some(vec![2, 3]));
    assert!(rows.layer0_row_exists(1).await.unwrap());
    assert!(!rows.layer0_row_exists(99).await.unwrap());
    assert_eq!(
        rows.layer0_rows_exist(&[]).await.unwrap(),
        Vec::<bool>::new()
    );
    assert_eq!(
        rows.layer0_rows_exist(&[1, 99]).await.unwrap(),
        vec![true, false]
    );
    assert_eq!(
        rows.layer0_neighbor_rows(&[1, 99]).await.unwrap(),
        vec![Some(vec![2, 3]), None]
    );
    assert_eq!(rows.layer0_neighbor_rows(&[]).await.unwrap(), Vec::new());
    assert_eq!(rows.upper_neighbors(2, 1).await.unwrap(), Some(vec![4, 5]));
    assert_eq!(rows.upper_neighbors(2, 99).await.unwrap(), None);
    assert_eq!(
        rows.upper_vector_row(1).await.unwrap(),
        Some(Bytes::from_static(b"upper-vector"))
    );
    assert_eq!(
        rows.upper_vector_rows(&[1, 99]).await.unwrap(),
        vec![Some(Bytes::from_static(b"upper-vector")), None]
    );
    assert_eq!(rows.upper_vector_rows(&[]).await.unwrap(), Vec::new());
    assert_eq!(
        rows.simhash_rows(&[1, 7, 8]).await.unwrap(),
        vec![
            SimHashRow::Present(SimHash::from_bits(0x1234)),
            SimHashRow::Missing,
            SimHashRow::Corrupt,
        ]
    );
    assert_eq!(rows.simhash_rows(&[]).await.unwrap(), Vec::new());
    assert_eq!(
        rows.canonical_vector_row(&canonical).await.unwrap(),
        Some(Bytes::from_static(b"canonical-vector"))
    );
    assert_eq!(
        rows.canonical_vector_rows(std::slice::from_ref(&canonical))
            .await
            .unwrap(),
        vec![Some(Bytes::from_static(b"canonical-vector"))]
    );
    assert_eq!(rows.canonical_vector_rows(&[]).await.unwrap(), Vec::new());
    let foreign_token = foreign.canonical_vector_row_key(1, 17);
    assert!(rows.canonical_vector_row(&foreign_token).await.is_err());
    assert!(rows
        .canonical_vector_rows(std::slice::from_ref(&foreign_token))
        .await
        .is_err());
    assert_eq!(
        rows.entry_candidate_layer(1).await.unwrap(),
        EntryCandidateLayerRow::Present(3)
    );
    assert_eq!(
        rows.entry_candidate_layer(7).await.unwrap(),
        EntryCandidateLayerRow::Missing
    );
    assert_eq!(
        rows.entry_candidate_layer(8).await.unwrap(),
        EntryCandidateLayerRow::Corrupt
    );

    let candidate = {
        let mut candidates = rows.entry_candidates().await.unwrap();
        let candidate = candidates.next().await.unwrap().unwrap();
        assert_eq!(candidate.node_id(), 1);
        assert_eq!(candidate.layer(), 3);
        candidate
    };
    let reverse = rows.reverse_sources_for_target(9).await.unwrap();
    assert_eq!(reverse.sources_at(2), &[1, 2]);
    assert_eq!(reverse.sources_at(1), &[3]);
    assert!(reverse.sources_at(0).is_empty());
    assert_eq!(reverse.sources_by_layer().len(), 2);

    assert_eq!(
        writes.layer0_neighbor_rows(&[1, 99]).await.unwrap(),
        vec![Some(vec![2, 3]), None]
    );
    assert_eq!(
        writes.entry_candidate_layer(2).await.unwrap(),
        EntryCandidateLayerRow::Present(1)
    );
    let mut writable_candidates = writes.entry_candidates().await.unwrap();
    assert!(writable_candidates.next().await.unwrap().is_some());
    drop(writable_candidates);
    assert_eq!(
        writes
            .reverse_sources_for_target(9)
            .await
            .unwrap()
            .sources_at(2),
        &[1, 2]
    );

    assert!(writes
        .put_canonical_vector(&foreign_token, Bytes::new())
        .is_err());
    assert!(writes.delete_canonical_vector(&foreign_token).is_err());
    assert!(writes.put_simhash_directory_entry(&foreign_token).is_err());
    assert!(writes
        .delete_simhash_directory_entry(&foreign_token)
        .is_err());
    writes.delete_scanned_entry_candidate(&candidate).unwrap();
    let foreign_candidate = EntryCandidateRow {
        keyspace: &foreign,
        physical_key: foreign.key(VectorKey::EntryCandidateSorted(
            VectorEntryCandidateKey::new(foreign.index_id(), 1, 1),
        )),
        node_id: 1,
        layer: 1,
    };
    assert!(writes
        .delete_scanned_entry_candidate(&foreign_candidate)
        .is_err());
    let foreign_reverse = ReverseSourcesForTarget {
        keyspace: foreign.clone(),
        sources_by_layer: BTreeMap::new(),
        locator_keys: Vec::new(),
    };
    assert!(writes.delete_reverse_sources(&foreign_reverse).is_err());
    let foreign_cleanup = VectorCleanupRow {
        keyspace: foreign.clone(),
        physical_key: foreign.key(VectorKey::IndexMetadata(VectorIndexMetadataKey::new(
            foreign.index_id(),
        ))),
        input_bytes: 1,
    };
    assert!(writes.delete_cleanup_row(&foreign_cleanup).is_err());
    writes.delete_reverse_sources(&reverse).unwrap();
    writes.delete_entry_candidate_sorted(2, 1).unwrap();
    writes.delete_entry_candidate_node(1).unwrap();
    writes.delete_entry_candidate_node(2).unwrap();
    writes.delete_reverse_locator(9, 2, 1).unwrap();
    writes.delete_upper_neighbors(2, 1).unwrap();
    writes.delete_upper_vector(1).unwrap();
    writes.delete_simhash(1).unwrap();
    writes.delete_layer0_neighbors(1).unwrap();
    writes.delete_canonical_vector(&canonical).unwrap();

    writes
        .put_metadata(&VectorIndexMetadata::new(VectorIndexConfig::new(
            keyspace.physical_name(),
            "embedding",
            3,
        )))
        .unwrap();
    writes.put_layer0_neighbors(10, &[11]).unwrap();
    writes
        .put_upper_vector(10, Bytes::from_static(b"cleanup"))
        .unwrap();
    writes.delete_all().await.unwrap();
    assert!(measured.measurement().unwrap().operations() > 0);
    txn.commit().await.unwrap();

    assert!(VectorRows::new(&db, &keyspace)
        .metadata()
        .await
        .unwrap()
        .is_none());
    for lane in VectorStorageLane::ALL {
        let mut scan = db
            .scan_prefix(keyspace.key(lane.prefix_key(keyspace.index_id())), ..)
            .await
            .unwrap();
        assert!(scan.next().await.unwrap().is_none());
    }
}

/// Verifies malformed and identity-mismatched rows fail at the storage boundary.
async fn run_corruption_contracts() {
    let db = Db::open(
        "production-corrupt-typed-vector-rows",
        Arc::new(InMemory::new()),
    )
    .await
    .unwrap();
    let keyspace = VectorRowKeyspace::new(
        "production-corrupt-typed-vector-rows".to_string(),
        DataScope::LegacyUnscoped,
    );
    let metadata_key = keyspace.key(VectorKey::IndexMetadata(VectorIndexMetadataKey::new(
        keyspace.index_id(),
    )));

    let txn = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let measured = MeasuredVectorTransaction::new(&txn);
    let rows = VectorRows::new(&measured, &keyspace);
    measured
        .put(metadata_key.clone(), Bytes::from_static(b"corrupt"))
        .unwrap();
    assert!(matches!(
        rows.metadata().await,
        Err(HelixDbError::Encoding(_))
    ));

    let mut invalid = VectorIndexMetadata::new(VectorIndexConfig::new(
        keyspace.physical_name(),
        "embedding",
        3,
    ));
    invalid.entry_point = None;
    invalid.max_layer = 1;
    measured
        .put(metadata_key.clone(), encode_metadata(&invalid))
        .unwrap();
    assert!(rows.metadata().await.is_err());

    let collision = VectorIndexMetadata::new(VectorIndexConfig::new(
        "production-colliding-vector-name",
        "embedding",
        3,
    ));
    measured
        .put(metadata_key, encode_metadata(&collision))
        .unwrap();
    assert!(matches!(
        rows.metadata().await,
        Err(HelixDbError::Config(_))
    ));

    measured
        .put(
            keyspace.key(VectorKey::Layer0Neighbors(VectorLayer0NeighborsKey::new(
                keyspace.index_id(),
                1,
            ))),
            Bytes::from_static(b"corrupt-layer-zero"),
        )
        .unwrap();
    assert!(rows.layer0_neighbor_row(1).await.is_err());
    assert!(rows.layer0_neighbor_rows(&[1]).await.is_err());

    measured
        .put(
            keyspace.key(VectorKey::UpperNeighbors(VectorUpperNeighborsKey::new(
                keyspace.index_id(),
                2,
                1,
            ))),
            Bytes::from_static(b"corrupt-upper-neighbors"),
        )
        .unwrap();
    assert!(rows.upper_neighbors(2, 1).await.is_err());
    txn.rollback();
}

/// Verifies every typed storage read propagates its backend operation failure.
async fn run_read_fault_contracts() {
    let db = Db::open(
        "production-vector-storage-read-faults",
        Arc::new(InMemory::new()),
    )
    .await
    .unwrap();
    let keyspace = VectorRowKeyspace::new(
        "production-vector-storage-read-faults".to_string(),
        DataScope::LegacyUnscoped,
    );
    let canonical = keyspace.canonical_vector_row_key(1, 7);

    let point = FaultingRead::new(&db, ReadFault::Point);
    let rows = VectorRows::new(&point, &keyspace);
    assert!(rows.metadata().await.is_err());
    assert!(rows.metadata_input_bytes().await.is_err());
    assert!(rows.layer0_neighbors(1).await.is_err());
    assert!(rows.layer0_neighbor_row(1).await.is_err());
    assert!(rows.layer0_row_exists(1).await.is_err());
    assert!(rows.upper_neighbors(1, 1).await.is_err());
    assert!(rows.upper_vector_row(1).await.is_err());
    assert!(rows.canonical_vector_row(&canonical).await.is_err());
    assert!(rows.entry_candidate_layer(1).await.is_err());

    let multi_get = FaultingRead::new(&db, ReadFault::MultiGet);
    let rows = VectorRows::new(&multi_get, &keyspace);
    assert!(rows.layer0_rows_exist(&[1]).await.is_err());
    assert!(rows.layer0_neighbor_rows(&[1]).await.is_err());
    assert!(rows.upper_vector_rows(&[1]).await.is_err());
    assert!(rows.simhash_rows(&[1]).await.is_err());
    assert!(rows
        .canonical_vector_rows(std::slice::from_ref(&canonical))
        .await
        .is_err());

    let scan = FaultingRead::new(&db, ReadFault::Scan);
    let rows = VectorRows::new(&scan, &keyspace);
    assert!(rows.entry_candidates().await.is_err());
    assert!(rows.reverse_sources_for_target(1).await.is_err());
}

/// Exercises scoped keys, typed row codecs, opaque tokens, and lane cleanup.
pub(crate) async fn run() {
    run_legacy_validation_codec_contracts();
    run_legacy_migration_read_contracts().await;
    run_simhash_directory_contracts().await;
    run_simhash_directory_migration_contracts().await;
    run_legacy_validation_scan_contracts().await;
    run_legacy_validation_entry_point_contracts().await;
    run_keyspace_contracts();
    run_row_contracts().await;
    run_corruption_contracts().await;
    run_read_fault_contracts().await;
}
