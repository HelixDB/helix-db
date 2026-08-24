//! Production-linked graph and index-definition migration acceptance contracts.

#![recursion_limit = "256"]

#[allow(dead_code)]
mod text_correctness_support;

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::num::NonZeroU32;
use std::sync::Mutex;

use db::migration_parity::MigrationParityTextEntityContribution;
use sha2::{Digest, Sha256};

use text_correctness_support::{analyze_text, search_live_corpus, OracleDocument};

const CONTRACT_STACK_BYTES: usize = 16 * 1024 * 1024;
static MIGRATION_CONTRACT_LOCK: Mutex<()> = Mutex::new(());

fn run_contract<Factory, Contract, Output>(factory: Factory) -> Output
where
    Factory: FnOnce() -> Contract + Send + 'static,
    Contract: Future<Output = Output> + Send + 'static,
    Output: Send + 'static,
{
    // Migration failpoints and lifecycle checkpoints are process-global test
    // controls, so independent libtest workers must not arm them concurrently.
    let _contract_guard = MIGRATION_CONTRACT_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    std::thread::Builder::new()
        .name("migration-contract".to_string())
        .stack_size(CONTRACT_STACK_BYTES)
        .spawn(move || {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .thread_stack_size(CONTRACT_STACK_BYTES)
                .enable_all()
                .build()
                .expect("migration contract runtime builds")
                .block_on(factory())
        })
        .expect("migration contract thread starts")
        .join()
        .expect("migration contract thread succeeds")
}

fn contribution_fingerprint(
    analyzer: db::config::TextAnalyzerKind,
    partition_bytes: &[u8],
    text: &str,
) -> ([u8; 32], u64, Vec<Vec<u8>>) {
    let analyzed = analyze_text(analyzer, text);
    let token_count = u64::try_from(analyzed.len()).expect("fixture token count fits u64");
    let terms = analyzed
        .into_iter()
        .map(String::into_bytes)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let mut fingerprint = Sha256::new();
    fingerprint.update(b"helix-text-statistics-contribution-v1");
    fingerprint.update(analyzer.as_str().as_bytes());
    fingerprint.update(
        u64::try_from(partition_bytes.len())
            .expect("fixture partition length fits u64")
            .to_be_bytes(),
    );
    fingerprint.update(partition_bytes);
    fingerprint.update(token_count.to_be_bytes());
    fingerprint.update(
        u64::try_from(terms.len())
            .expect("fixture term count fits u64")
            .to_be_bytes(),
    );
    for term in &terms {
        fingerprint.update(
            u64::try_from(term.len())
                .expect("fixture term length fits u64")
                .to_be_bytes(),
        );
        fingerprint.update(term);
    }
    (fingerprint.finalize().into(), token_count, terms)
}

async fn assert_populated_text_fixture(
    database: &db::HelixDB,
    fixture: &db::production_coverage::PopulatedLegacyTextFixture,
) -> (BTreeSet<[u8; 32]>, Option<u16>) {
    let state = database
        .migration_parity_v2_state()
        .await
        .expect("migrated V2 evidence reads");
    assert_eq!(state.legacy_definition_rows, 0);
    assert_eq!(state.pending_operation_pointers, 0);
    assert_eq!(
        state
            .canonical_records
            .iter()
            .filter(|record| record.definition.get("family").map(String::as_str) == Some("text"))
            .count(),
        fixture.cases.len()
    );
    assert!(state
        .canonical_records
        .iter()
        .filter(|record| record.definition.get("family").map(String::as_str) == Some("text"))
        .all(|record| record.state == "active"));

    let mut reachable = BTreeSet::new();
    for case in &fixture.cases {
        let analyzer = case.definition.analyzer();
        let documents = case
            .documents
            .iter()
            .map(|document| OracleDocument {
                entity_id: document.entity_id,
                text: &document.text,
            })
            .collect::<Vec<_>>();
        let mut queries = vec!["alpha alpha", "running runners", ""];
        if let Some(document) = case.documents.get(4) {
            queries.push(&document.text);
        }
        if let Some(document) = case.documents.get(5) {
            queries.push(&document.text);
        }
        for query in queries {
            let expected = search_live_corpus(analyzer, &documents, query, documents.len());
            let observed = database
                .migration_parity_text_search_definition(
                    &case.definition,
                    case.target_tenant.as_deref(),
                    query,
                    documents.len(),
                )
                .await
                .expect("migrated text query succeeds");
            assert_eq!(observed.analyzer, analyzer.as_str());
            assert_eq!(observed.partition_bytes, case.target_partition_bytes);
            assert_eq!(
                observed
                    .hits
                    .iter()
                    .map(|hit| (hit.entity_id, hit.score_bits))
                    .collect::<Vec<_>>(),
                expected
                    .iter()
                    .map(|hit| (hit.entity_id, hit.score_bits))
                    .collect::<Vec<_>>(),
                "migrated {:?} {:?} query {query:?} differs from the monolithic oracle",
                case.definition.element_type(),
                analyzer
            );
            reachable.extend(observed.splits.iter().map(|split| split.sha256));
        }
        if !case.other_partition_documents.is_empty() {
            let other_documents = case
                .other_partition_documents
                .iter()
                .map(|document| OracleDocument {
                    entity_id: document.entity_id,
                    text: &document.text,
                })
                .collect::<Vec<_>>();
            let expected =
                search_live_corpus(analyzer, &other_documents, "alpha", other_documents.len());
            let observed = database
                .migration_parity_text_search_definition(
                    &case.definition,
                    Some("tenant-b"),
                    "alpha",
                    other_documents.len(),
                )
                .await
                .expect("secondary migrated tenant query succeeds");
            assert_eq!(
                observed
                    .hits
                    .iter()
                    .map(|hit| (hit.entity_id, hit.score_bits))
                    .collect::<Vec<_>>(),
                expected
                    .iter()
                    .map(|hit| (hit.entity_id, hit.score_bits))
                    .collect::<Vec<_>>()
            );
            reachable.extend(observed.splits.iter().map(|split| split.sha256));
        }

        let observed = database
            .migration_parity_text_search_definition(
                &case.definition,
                case.target_tenant.as_deref(),
                "alpha alpha",
                documents.len(),
            )
            .await
            .expect("statistics owner query succeeds");
        let corpus = state
            .text_corpus_statistics
            .iter()
            .find(|statistics| {
                statistics.index_id == observed.index_id
                    && statistics.generation == observed.generation
                    && statistics.partition_bytes == case.target_partition_bytes
            })
            .expect("target corpus statistics exist");
        let analyzed = case
            .documents
            .iter()
            .map(|document| analyze_text(analyzer, &document.text))
            .collect::<Vec<_>>();
        assert_eq!(
            corpus.document_count,
            u64::try_from(case.documents.len()).expect("fixture document count fits u64")
        );
        assert_eq!(
            corpus.total_token_count,
            analyzed
                .iter()
                .map(|terms| u64::try_from(terms.len()).expect("fixture token count fits u64"))
                .sum::<u64>()
        );
        let expected_frequencies =
            analyzed
                .iter()
                .fold(BTreeMap::<Vec<u8>, u64>::new(), |mut frequencies, terms| {
                    for term in terms.iter().map(String::as_bytes).collect::<BTreeSet<_>>() {
                        *frequencies.entry(term.to_vec()).or_default() += 1;
                    }
                    frequencies
                });
        let observed_frequencies = state
            .text_term_statistics
            .iter()
            .filter(|statistics| {
                statistics.index_id == observed.index_id
                    && statistics.generation == observed.generation
                    && statistics.partition_bytes == case.target_partition_bytes
            })
            .map(|statistics| (statistics.term.clone(), statistics.document_frequency))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(observed_frequencies, expected_frequencies);

        for document in &case.documents {
            let marker = state
                .text_entity_statistics
                .iter()
                .find(|statistics| {
                    statistics.index_id == observed.index_id
                        && statistics.generation == observed.generation
                        && statistics.entity_id == document.entity_id
                })
                .expect("live document accounting marker exists");
            let (fingerprint, token_count, terms) =
                contribution_fingerprint(analyzer, &case.target_partition_bytes, &document.text);
            assert_eq!(
                marker.contribution,
                MigrationParityTextEntityContribution::Present {
                    partition_bytes: case.target_partition_bytes.clone(),
                    fingerprint,
                    token_count,
                    terms,
                }
            );
        }
        assert!(
            state.text_entity_statistics.iter().all(|statistics| {
                statistics.index_id != observed.index_id
                    || statistics.generation != observed.generation
                    || statistics.entity_id != case.absent_entity_id
            }),
            "a source row without indexed text has neither entity state nor accounting marker"
        );
    }

    (reachable, state.storage_version)
}

/// Covers no-marker bootstrap, non-mutating legacy reader compatibility,
/// reader gating, and every supported persisted legacy
/// definition shape, one-row checkpoints, cold reopen, and idempotent reopen.
#[test]
fn legacy_definitions_converge_through_real_v2_lifecycle() {
    run_contract(db::production_coverage::migration_definition_contracts);
}

/// Disabled scheduling must not strand writer-open on accepted secondary work.
#[test]
fn disabled_secondary_worker_never_hangs_writer_open() {
    run_contract(db::production_coverage::migration_disabled_secondary_worker_open_contract);
}

/// Manual migration stepping must have exclusive controller ownership.
#[test]
fn migration_worker_mode_controls_manual_stepping() {
    run_contract(db::production_coverage::migration_worker_mode_stepping_contract);
}

/// Adopts a populated HNSW namespace without reconstruction, then exercises
/// cold reopen, ordinary DROP cleanup, and recreation allocation.
#[test]
fn populated_legacy_vector_is_adopted_in_place() {
    run_contract(db::production_coverage::migration_vector_adoption_contract);
}

/// Every adoption-checkpoint interruption must return from writer-open so its
/// durable state can be inspected and cold-recovered.
#[test]
fn legacy_vector_adoption_failpoints_preserve_physical_ownership() {
    let boundaries = run_contract(
        db::production_coverage::migration_vector_adoption_failpoint_recovery_contracts,
    );
    assert_eq!(
        boundaries,
        vec![
            "legacy_vector_validation_checkpoint_before",
            "legacy_vector_validation_checkpoint_after",
            "legacy_vector_metadata_publication_before",
            "legacy_vector_metadata_publication_after",
            "legacy_vector_reservation_transition_before",
            "legacy_vector_reservation_transition_after",
            "legacy_definition_retirement_before",
            "legacy_definition_retirement_after",
        ]
    );
}

/// Malformed core, hot, and layer-zero rows must preserve their exact sources.
#[test]
fn malformed_legacy_vector_lanes_fail_closed() {
    run_contract(db::production_coverage::migration_vector_corruption_contracts);
}

/// Consumed physical IDs and tenant partitioning must use normal rebuilds.
#[test]
fn ineligible_legacy_vectors_retain_the_rebuild_path() {
    run_contract(db::production_coverage::migration_vector_ineligible_contracts);
}

/// Physical-name and reservation ownership conflicts must preserve the source.
#[test]
fn legacy_vector_ownership_conflicts_fail_closed() {
    run_contract(db::production_coverage::migration_vector_ownership_conflict_contracts);
}

/// Rejects every incomplete or unsupported V2 bootstrap tuple without repair.
#[test]
fn malformed_partial_older_and_future_v2_metadata_fail_closed() {
    run_contract(db::production_coverage::migration_bootstrap_rejection_contracts);
}

/// Keeps source definitions intact across recoverable failure and resumes later.
#[test]
fn failed_definition_migration_preserves_source_and_recovers() {
    run_contract(db::production_coverage::migration_failure_preservation_contract);
}

/// Exercises every vector migration batch boundary through a clean reopen.
#[test]
fn vector_migration_failpoints_recover_materialization_and_retirement() {
    run_contract(db::production_coverage::migration_vector_failpoint_recovery_contracts);
}

/// Repairs a rejected zero-cosine payload and retries the exact durable cursor.
#[test]
fn vector_zero_cosine_recovery_resumes_the_failed_entity() {
    run_contract(db::production_coverage::migration_vector_zero_cosine_recovery_contract);
}

/// Retires exact legacy duplicates and preserves both sides of a conflict.
#[test]
fn already_active_and_conflicting_definitions_follow_closed_rules() {
    run_contract(db::production_coverage::migration_existing_active_and_conflict_contracts);
}

/// Rebuilds every supported populated legacy text shape from authoritative graph rows.
#[test]
fn populated_legacy_text_rebuild_matches_live_corpus_oracle() {
    run_contract(|| async {
        let fixture = db::production_coverage::seed_populated_legacy_text_fixture()
            .await
            .expect("populated legacy text fixture seeds");
        assert_eq!(fixture.cases.len(), 12);
        let before = db::production_coverage::inspect_legacy_text_physical_rows(&fixture)
            .await
            .expect("legacy text physical evidence reads");
        assert!(before.manifest_present);
        assert!(before.live_state_present);
        assert!(before.txn_guard_present);
        assert!(before.version_counter_present);
        assert!(before.blob_hashes.contains(&fixture.legacy_blob_hash));

        let migrated = db::HelixDB::open_with_object_store_for_migration_parity(
            fixture.database.clone(),
            std::sync::Arc::clone(&fixture.store),
            fixture.config.clone(),
        )
        .await
        .expect("populated legacy text fixture migrates");
        let (_reachable, storage_version) =
            assert_populated_text_fixture(&migrated, &fixture).await;
        migrated.close().await.expect("migrated fixture closes");
        // Legacy rebuilds publish the current direct-publication format.
        assert_eq!(
            storage_version,
            Some(db::production_coverage::CURRENT_INDEX_STORAGE_VERSION)
        );
    });
}

/// Legacy rows retire after activation, cold reopen is idempotent, and every
/// uploaded blob remains retained.
#[test]
fn legacy_text_rebuild_retires_only_after_active_and_cold_reopens() {
    run_contract(|| async {
        let fixture = db::production_coverage::seed_populated_legacy_text_fixture()
            .await
            .expect("populated legacy text fixture seeds");
        let migrated = db::HelixDB::open_with_object_store_for_migration_parity(
            fixture.database.clone(),
            std::sync::Arc::clone(&fixture.store),
            fixture.config.clone(),
        )
        .await
        .expect("legacy text fixture migrates");
        let (reachable, storage_version) = assert_populated_text_fixture(&migrated, &fixture).await;
        let first_state = migrated
            .migration_parity_v2_state()
            .await
            .expect("first migrated state reads");
        migrated
            .close()
            .await
            .expect("first migrated handle closes");

        let retired = db::production_coverage::inspect_legacy_text_physical_rows(&fixture)
            .await
            .expect("retired physical evidence reads");
        assert!(!retired.manifest_present);
        assert!(!retired.live_state_present);
        assert!(!retired.txn_guard_present);
        assert!(!retired.version_counter_present);
        assert!(
            retired.blob_hashes.contains(&fixture.legacy_blob_hash),
            "legacy-only blob remains after metadata retirement"
        );
        assert!(reachable
            .iter()
            .all(|hash| retired.blob_hashes.contains(hash)));

        let reopened = db::HelixDB::open_with_object_store_for_migration_parity(
            fixture.database.clone(),
            std::sync::Arc::clone(&fixture.store),
            fixture.config.clone(),
        )
        .await
        .expect("migrated text fixture cold-reopens");
        let reopened_state = reopened
            .migration_parity_v2_state()
            .await
            .expect("cold-reopened state reads");
        assert_eq!(reopened_state, first_state);
        reopened.close().await.expect("cold-reopened handle closes");

        let retained = db::production_coverage::inspect_legacy_text_physical_rows(&fixture)
            .await
            .expect("retained blob evidence reads");
        assert!(
            retained.blob_hashes.contains(&fixture.legacy_blob_hash),
            "migration never deletes an unreachable text blob"
        );
        assert!(reachable
            .iter()
            .all(|hash| retained.blob_hashes.contains(hash)));
        assert_eq!(
            storage_version,
            Some(db::production_coverage::CURRENT_INDEX_STORAGE_VERSION)
        );
    });
}

/// Every durable rebuild boundary preserves the legacy source and resumes to
/// the same authoritative Active generation on a clean reopen.
#[test]
fn legacy_text_rebuild_recovers_at_every_durable_boundary() {
    run_contract(|| async {
        use db::migrations::LegacyTextMigrationCheckpoint::{
            AfterActivationBeforeRetirement, BeforeEnqueue, CatchUp, SourceScan,
            ValidateEntityStates, ValidatePages, ValidateRoots,
        };

        let mut observed_versions = Vec::new();
        for checkpoint in [
            BeforeEnqueue,
            SourceScan,
            CatchUp,
            ValidatePages,
            ValidateRoots,
            ValidateEntityStates,
            AfterActivationBeforeRetirement,
        ] {
            let fixture = db::production_coverage::seed_recovery_legacy_text_fixture()
                .await
                .expect("legacy recovery fixture seeds");
            db::migrations::inject_legacy_text_migration_checkpoint_once(checkpoint)
                .expect("legacy migration checkpoint arms");
            let interrupted = db::HelixDB::open_with_object_store_for_migration_parity(
                fixture.database.clone(),
                std::sync::Arc::clone(&fixture.store),
                fixture.config.clone(),
            )
            .await;
            assert!(
                interrupted.is_err(),
                "{checkpoint:?} must interrupt the writer open"
            );
            assert!(
                db::migrations::legacy_text_migration_checkpoint_was_triggered(),
                "{checkpoint:?} must be observed at its exact durable boundary"
            );
            let retained = db::production_coverage::inspect_legacy_text_physical_rows(&fixture)
                .await
                .expect("interrupted legacy physical evidence reads");
            assert!(retained.manifest_present);
            assert!(retained.live_state_present);
            assert!(retained.txn_guard_present);
            assert!(retained.version_counter_present);
            assert!(retained.blob_hashes.contains(&fixture.legacy_blob_hash));

            db::migrations::clear_legacy_text_migration_checkpoint();
            let recovered = db::HelixDB::open_with_object_store_for_migration_parity(
                fixture.database.clone(),
                std::sync::Arc::clone(&fixture.store),
                fixture.config.clone(),
            )
            .await
            .unwrap_or_else(|error| {
                panic!("{checkpoint:?} must recover on a clean reopen: {error}")
            });
            let (_, storage_version) = assert_populated_text_fixture(&recovered, &fixture).await;
            observed_versions.push(storage_version);
            recovered
                .close()
                .await
                .expect("recovered migration handle closes");
            let retired = db::production_coverage::inspect_legacy_text_physical_rows(&fixture)
                .await
                .expect("recovered legacy physical evidence reads");
            assert!(!retired.manifest_present);
            assert!(!retired.live_state_present);
            assert!(!retired.txn_guard_present);
            assert!(!retired.version_counter_present);
        }
        assert_eq!(
            observed_versions,
            vec![Some(db::production_coverage::CURRENT_INDEX_STORAGE_VERSION); 7]
        );
    });
}

/// Invalid indexed source rows fail closed without retiring any legacy source;
/// all supported non-null tenant values remain compatible.
#[test]
fn invalid_legacy_text_sources_fail_closed_and_retry_after_correction() {
    run_contract(|| async {
        use db::production_coverage::LegacyTextSourceFixtureKind::{
            ArrayTenant, BooleanTenant, EmptyStringTenant, IntegerTenant, MissingTenant,
            NullTenant, ObjectTenant, OversizedTenant, UnsupportedText,
        };

        let mut invalid_fixtures = Vec::new();
        for kind in [MissingTenant, NullTenant, OversizedTenant, UnsupportedText] {
            let fixture = db::production_coverage::seed_legacy_text_source_fixture(kind)
                .await
                .expect("invalid legacy text fixture seeds");
            let before = db::production_coverage::inspect_legacy_text_source(&fixture)
                .await
                .expect("invalid source evidence reads before migration");
            assert_eq!(before.graph_value.as_ref(), Some(&fixture.graph_value));
            assert_eq!(before.catalog_value.as_ref(), Some(&fixture.catalog_value));
            assert!(before.physical.manifest_present);
            assert!(before.physical.live_state_present);
            assert!(before.physical.txn_guard_present);
            assert!(before.physical.version_counter_present);
            assert!(before
                .physical
                .blob_hashes
                .contains(&fixture.migration.legacy_blob_hash));

            let error = match db::HelixDB::open_with_object_store_for_migration_parity(
                fixture.migration.database.clone(),
                std::sync::Arc::clone(&fixture.migration.store),
                fixture.migration.config.clone(),
            )
            .await
            {
                Err(error) => error,
                Ok(database) => {
                    database
                        .close()
                        .await
                        .expect("unexpected valid-source handle closes");
                    panic!("invalid legacy source must fail writer migration")
                }
            };
            assert!(
                matches!(error, db::error::HelixDbError::MigrationRequired { .. }),
                "{kind:?} returned the wrong failure category: {error}"
            );
            assert!(
                error.to_string().contains("InvalidSourceData"),
                "{kind:?} must retain the lifecycle InvalidSourceData blocker: {error}"
            );
            let failed = db::production_coverage::inspect_legacy_text_source(&fixture)
                .await
                .expect("failed source evidence reads");
            assert_eq!(failed, before);
            invalid_fixtures.push(fixture);
        }

        let mut observed_versions = Vec::new();
        for kind in [
            IntegerTenant,
            BooleanTenant,
            EmptyStringTenant,
            ArrayTenant,
            ObjectTenant,
        ] {
            let fixture = db::production_coverage::seed_legacy_text_source_fixture(kind)
                .await
                .expect("compatible tenant fixture seeds");
            let migrated = db::HelixDB::open_with_object_store_for_migration_parity(
                fixture.migration.database.clone(),
                std::sync::Arc::clone(&fixture.migration.store),
                fixture.migration.config.clone(),
            )
            .await
            .unwrap_or_else(|error| panic!("{kind:?} tenant must remain supported: {error}"));
            let state = migrated
                .migration_parity_v2_state()
                .await
                .expect("compatible tenant V2 evidence reads");
            let expected_partition = fixture
                .compatibility_partition_bytes
                .as_ref()
                .expect("compatible tenant has canonical partition evidence");
            assert!(state.text_entity_statistics.iter().any(|statistics| {
                statistics.entity_id == fixture.entity_id
                    && matches!(
                        &statistics.contribution,
                        MigrationParityTextEntityContribution::Present {
                            partition_bytes,
                            ..
                        } if partition_bytes == expected_partition
                    )
            }));
            assert!(state
                .text_corpus_statistics
                .iter()
                .any(|statistics| { &statistics.partition_bytes == expected_partition }));
            observed_versions.push(state.storage_version);
            migrated.close().await.expect("compatible fixture closes");
        }

        for fixture in invalid_fixtures {
            let kind = fixture.kind;
            db::production_coverage::repair_legacy_text_source(&fixture)
                .await
                .expect("invalid legacy source repairs");
            let recovered = db::HelixDB::open_with_object_store_for_migration_parity(
                fixture.migration.database.clone(),
                std::sync::Arc::clone(&fixture.migration.store),
                fixture.migration.config.clone(),
            )
            .await
            .unwrap_or_else(|reopen| {
                panic!("{kind:?} must migrate after source correction: {reopen}")
            });
            let (_, storage_version) =
                assert_populated_text_fixture(&recovered, &fixture.migration).await;
            observed_versions.push(storage_version);
            recovered.close().await.expect("corrected migration closes");
        }
        assert_eq!(
            observed_versions,
            vec![Some(db::production_coverage::CURRENT_INDEX_STORAGE_VERSION); 9]
        );
    });
}

/// Populated V1 graph rows and every current dynamic-index family converge
/// together, preserve graph state, and retain exact IDs across cold reopen.
#[test]
fn populated_v1_graph_and_all_index_families_converge_together() {
    let observation =
        run_contract(db::production_coverage::populated_v1_current_index_migration_contract);
    assert_eq!(observation.node_ids.len(), 3);
    assert_eq!(observation.edge_ids.len(), 3);
    assert_eq!(observation.active_indexes.len(), 11);
    assert!(observation.readiness_published);
    assert!(observation.legacy_catalog_empty);
    assert!(observation.cold_reopen_identical);
}

fn oracle_property(
    value: &db::production_coverage::V1OracleValue,
) -> helix_db_testkit::action::PropertyValue {
    use db::production_coverage::V1OracleValue as Source;
    use helix_db_testkit::action::{OracleF32, OracleF64, PropertyValue};

    match value {
        Source::Null => PropertyValue::Null,
        Source::Bool(value) => PropertyValue::Bool(*value),
        Source::I64(value) => PropertyValue::I64(*value),
        Source::DateTime(value) => PropertyValue::DateTime(*value),
        Source::F64Bits(bits) => PropertyValue::F64(OracleF64::new(f64::from_bits(*bits))),
        Source::F32Bits(bits) => PropertyValue::F32(OracleF32::new(f32::from_bits(*bits))),
        Source::String(value) => PropertyValue::String(value.clone()),
        Source::Bytes(value) => PropertyValue::Bytes(value.clone()),
        Source::I64Array(values) => PropertyValue::I64Array(values.clone()),
        Source::F64ArrayBits(values) => PropertyValue::F64Array(
            values
                .iter()
                .copied()
                .map(f64::from_bits)
                .map(OracleF64::new)
                .collect(),
        ),
        Source::F32ArrayBits(values) => PropertyValue::F32Array(
            values
                .iter()
                .copied()
                .map(f32::from_bits)
                .map(OracleF32::new)
                .collect(),
        ),
        Source::StringArray(values) => PropertyValue::StringArray(values.clone()),
    }
}

fn oracle_range_value(
    value: &db::production_coverage::V1OracleValue,
) -> helix_db_testkit::action::SecondaryRangeValue {
    use db::production_coverage::V1OracleValue as Source;
    use helix_db_testkit::action::{OracleF32, OracleF64, SecondaryRangeValue};

    match value {
        Source::I64(value) => SecondaryRangeValue::I64(*value),
        Source::DateTime(value) => SecondaryRangeValue::DateTime(*value),
        Source::F64Bits(bits) => SecondaryRangeValue::F64(OracleF64::new(f64::from_bits(*bits))),
        Source::F32Bits(bits) => SecondaryRangeValue::F32(OracleF32::new(f32::from_bits(*bits))),
        Source::String(value) => SecondaryRangeValue::String(value.clone()),
        Source::Null
        | Source::Bool(_)
        | Source::Bytes(_)
        | Source::I64Array(_)
        | Source::F64ArrayBits(_)
        | Source::F32ArrayBits(_)
        | Source::StringArray(_) => panic!("range fixture emitted an unsupported oracle value"),
    }
}

fn oracle_range_bound(
    bound: &db::production_coverage::V1RangeBound,
) -> helix_db_testkit::action::SecondaryRangeBound {
    use db::production_coverage::V1RangeBound as Source;
    use helix_db_testkit::action::SecondaryRangeBound;

    match bound {
        Source::Inclusive(value) => SecondaryRangeBound::Inclusive(oracle_range_value(value)),
        Source::Exclusive(value) => SecondaryRangeBound::Exclusive(oracle_range_value(value)),
    }
}

/// Every migrated typed equality result must match the independent testkit
/// oracle for nodes and edges before and after cold reopen.
#[test]
fn migrated_v1_equality_matches_independent_typed_oracle() {
    use helix_db_testkit::ids::EntityId;
    use helix_db_testkit::model::secondary_optional_equality_ids;

    let observation =
        run_contract(db::production_coverage::v1_equality_semantics_migration_contract);
    assert!(observation.cold_reopen_identical);
    for query in &observation.queries {
        let rows = observation
            .rows
            .iter()
            .filter(|row| row.element_kind == query.element_kind)
            .map(|row| {
                (
                    EntityId::new(row.entity_id),
                    row.value.as_ref().map(oracle_property),
                )
            })
            .collect::<Vec<_>>();
        let expected = secondary_optional_equality_ids(&rows, &oracle_property(&query.value))
            .into_iter()
            .map(EntityId::get)
            .collect::<Vec<_>>();
        assert_eq!(
            query.actual_ids, expected,
            "migrated {:?} equality diverged for {:?}",
            query.element_kind, query.value
        );
    }
}

/// Ascending and descending migrated ranges must match independent typed
/// bounds, domain order, ID ties, and limits for nodes and edges.
#[test]
fn migrated_v1_ranges_match_independent_typed_oracle() {
    use helix_db_testkit::action::{SecondaryRange, SecondaryRangeDirection};
    use helix_db_testkit::ids::EntityId;
    use helix_db_testkit::model::secondary_range_ids;

    let observation = run_contract(db::production_coverage::v1_range_semantics_migration_contract);
    assert!(observation.cold_reopen_identical);
    for case in &observation.cases {
        let rows = observation
            .rows
            .iter()
            .filter(|row| row.element_kind == case.element_kind)
            .filter_map(|row| {
                row.value
                    .as_ref()
                    .map(|value| (EntityId::new(row.entity_id), oracle_range_value(value)))
            })
            .collect::<Vec<_>>();
        let range = SecondaryRange::try_new(
            case.lower.as_ref().map(oracle_range_bound),
            case.upper.as_ref().map(oracle_range_bound),
            match case.direction {
                db::production_coverage::V1RangeDirection::Ascending => {
                    SecondaryRangeDirection::Ascending
                }
                db::production_coverage::V1RangeDirection::Descending => {
                    SecondaryRangeDirection::Descending
                }
            },
            case.limit.and_then(NonZeroU32::new),
        )
        .expect("migration fixture emits valid independent range bounds");
        let expected = secondary_range_ids(&rows, &range)
            .into_iter()
            .map(EntityId::get)
            .collect::<Vec<_>>();
        assert_eq!(
            case.actual_ids, expected,
            "migrated {:?} {:?} {:?} range diverged for {:?}..{:?}",
            case.element_kind, case.direction, case.access, case.lower, case.upper
        );
    }
}

/// The exact deployed 32-bit property-hash collision must not merge current
/// full-string identities or make misleading legacy rows visible.
#[test]
fn migrated_v1_property_hash_collision_rebuilds_from_graph_truth() {
    let observation =
        run_contract(db::production_coverage::v1_property_hash_collision_migration_contract);
    assert!(observation.exact_legacy_hash_collision);
    assert!(observation.cold_reopen_identical);
    assert!(observation.graph_rows_exact);
    assert!(observation.legacy_catalog_empty);
    assert!(observation.readiness_published);
    assert_eq!(observation.indexes.len(), 8);

    for index in &observation.indexes {
        let expected_id = match (index.element_kind, index.property) {
            ("node", "property_16755") => 50_000,
            ("node", "property_36911") => 50_001,
            ("edge", "property_16755") => 60_000,
            ("edge", "property_36911") => 60_001,
            unexpected => panic!("unexpected collision observation: {unexpected:?}"),
        };
        assert_eq!(
            index.ids,
            vec![expected_id],
            "migrated {} {} index for {} served legacy collision debris",
            index.element_kind,
            index.family,
            index.property
        );
    }
    assert!(
        observation.legacy_node_equality_ids.is_empty()
            && observation.legacy_node_range_ids.is_empty()
            && observation.legacy_global_edge_equality_ids.is_empty()
            && observation.legacy_global_edge_range_ids.is_empty(),
        "legacy physical rows survived retirement: node equality={:?}, node range={:?}, \
         global edge equality={:?}, global edge range={:?}",
        observation.legacy_node_equality_ids,
        observation.legacy_node_range_ids,
        observation.legacy_global_edge_equality_ids,
        observation.legacy_global_edge_range_ids
    );
}

/// Distinct typed values must migrate into a unique index, while exact
/// numeric and signed-zero duplicates block deterministically and preserve
/// their repairable source state.
#[test]
fn migrated_v1_unique_semantics_fail_closed_and_resume_after_repair() {
    use helix_db_testkit::ids::EntityId;
    use helix_db_testkit::model::secondary_optional_equality_ids;

    let observation =
        run_contract(db::production_coverage::v1_unique_failure_preservation_contract);
    assert_eq!(observation.success_queries.len(), 8);
    for query in &observation.success_queries {
        let rows = observation
            .success_rows
            .iter()
            .map(|row| {
                (
                    EntityId::new(row.entity_id),
                    row.value.as_ref().map(oracle_property),
                )
            })
            .collect::<Vec<_>>();
        let expected = secondary_optional_equality_ids(&rows, &oracle_property(&query.value))
            .into_iter()
            .map(EntityId::get)
            .collect::<Vec<_>>();
        assert_eq!(
            query.actual_ids, expected,
            "migrated unique result diverged for {:?}",
            query.value
        );
    }
    assert!(observation.exact_numeric_duplicate_blocked);
    assert!(observation.exact_numeric_blocker_stable);
    assert!(observation.exact_numeric_failure_preserved);
    assert!(observation.repaired_same_generation_active);
    assert!(observation.signed_zero_duplicate_blocked);
}

/// Missing range properties must be omitted, while every unsupported source
/// shape must block deterministically, preserve its source, and resume the
/// same generation after repair.
#[test]
fn migrated_v1_unsupported_ranges_fail_closed_and_resume_after_repair() {
    let observation = run_contract(db::production_coverage::v1_range_failure_preservation_contract);
    assert!(observation.missing_property_active_without_row);
    assert_eq!(observation.cases.len(), 11);
    for case in &observation.cases {
        assert!(
            case.blocker_stable,
            "{} range blocker changed across reopen",
            case.value_type
        );
        assert!(
            case.failure_preserved,
            "{} range failure changed graph/catalog/readiness state",
            case.value_type
        );
        assert!(
            case.repaired_same_generation_active,
            "{} range repair did not resume the same generation",
            case.value_type
        );
    }
}

/// The production prefix-successor helper must retain every `0xFF` suffix and
/// exclude the first key outside the requested prefix.
#[test]
fn v1_cleanup_prefix_successor_includes_every_prefixed_key() {
    let observation = run_contract(db::production_coverage::v1_prefix_successor_contract);
    assert_eq!(
        observation.included,
        vec![
            vec![0x03, 0x01, 0xAA],
            vec![0x03, 0x01, 0xAA, 0xFE],
            vec![0x03, 0x01, 0xAA, 0xFF],
            vec![0x03, 0x01, 0xAA, 0xFF, 0x00],
            vec![0x03, 0x01, 0xAA, 0xFF, 0x7A, 0xFE],
        ]
    );
    assert!(observation.first_outside_excluded);
    assert!(observation.all_ff_is_unbounded);
}

/// Catalog and all directional/global edge rows retire in one atomic commit
/// after the current generation is Active.
#[test]
fn legacy_edge_secondary_retirement_is_atomic_across_failpoints() {
    let observations =
        run_contract(db::production_coverage::v1_secondary_retirement_failpoint_contract);
    assert_eq!(observations.len(), 4);
    for observation in observations {
        let expected_legacy_ids = match observation.boundary {
            "before" => vec![0xFF00_0000_0000_0001],
            "after" => Vec::new(),
            boundary => panic!("unexpected retirement boundary {boundary}"),
        };
        assert_eq!(
            observation.legacy_catalog_present,
            observation.boundary == "before",
            "{} catalog retirement was not atomic at {}",
            observation.family,
            observation.boundary
        );
        assert_eq!(
            observation.directional_out_ids, expected_legacy_ids,
            "{} outgoing rows diverged at {}",
            observation.family, observation.boundary
        );
        assert_eq!(
            observation.directional_in_ids, expected_legacy_ids,
            "{} incoming rows diverged at {}",
            observation.family, observation.boundary
        );
        assert_eq!(
            observation.global_ids, expected_legacy_ids,
            "{} global rows diverged at {}",
            observation.family, observation.boundary
        );
        assert!(observation.current_active);
        assert!(observation.graph_preserved);
        assert!(!observation.readiness_published);
        assert!(observation.index_id > 0);
        assert!(observation.generation > 0);
        assert!(observation.ownership_stable_after_reopen);
        assert_eq!(
            observation.current_ids_after_reopen,
            vec![0xFF00_0000_0000_0001]
        );
        assert!(observation.converged_after_reopen);
    }
}

/// A malformed legacy catalog key/value identity must survive repeated opens
/// exactly and resume only after the source value is repaired.
#[test]
fn malformed_legacy_catalog_identity_fails_closed_and_recovers() {
    let observation =
        run_contract(db::production_coverage::v1_malformed_catalog_failure_preservation_contract);
    assert!(observation.typed_catalog_error);
    assert!(observation.blocker_stable);
    assert!(observation.source_preserved);
    assert!(observation.repaired_to_active);
}

/// Every graph/bootstrap commit boundary must preserve graph truth and recover
/// through the same writer bootstrap and explicit production cleanup surface.
#[test]
fn v1_graph_and_bootstrap_failpoints_recover_without_partial_readiness() {
    let observations = run_contract(db::production_coverage::v1_graph_crash_recovery_contract);
    assert_eq!(observations.len(), 22);
    for observation in observations {
        assert!(
            observation.triggered,
            "{} did not trigger",
            observation.failpoint
        );
        assert!(
            observation.operation_terminated,
            "{} exceeded its five-second timeout",
            observation.failpoint
        );
        assert!(
            observation.node_source_preserved,
            "{} changed authoritative node bytes",
            observation.failpoint
        );
        assert!(
            observation.legacy_edge_pair_preserved,
            "{} retired the legacy cleanup source outside a successful commit",
            observation.failpoint
        );
        assert!(
            !observation.index_ready_after_failure || observation.graph_ready_after_failure,
            "{} published index readiness before graph readiness",
            observation.failpoint
        );
        assert!(
            !observation.schema_ready_after_failure
                || (observation.graph_ready_after_failure && observation.index_ready_after_failure),
            "{} published schema readiness before its prerequisites",
            observation.failpoint
        );
        assert!(
            observation.existing_ownership_stable,
            "{} changed an allocated current ID or generation",
            observation.failpoint
        );
        assert!(
            observation.converged_after_reopen,
            "{} did not converge after cold reopen",
            observation.failpoint
        );
    }
}
