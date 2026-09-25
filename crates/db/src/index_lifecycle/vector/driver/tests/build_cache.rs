//! Persisted-graph equivalence and throughput contracts for vector source backfill.
//!
//! The golden digest pins every physical vector row produced by a deterministic
//! Euclidean build. Its components are small integers, so every squared
//! distance is exact in `f32` and the contract does not depend on SIMD
//! summation order. Equivalence contracts compare complete row sets built under
//! different step boundaries and cache policies inside one process.

use std::time::{Duration, Instant};

use super::*;

/// SHA-256 over every physical row of [`euclidean_golden_fixture`].
const EUCLIDEAN_GOLDEN_DIGEST: &str =
    "805ae4c6120ed75d9ddb0027da2191f6968f5abd65a991c88ad067d6c54ad76c";
/// Physical row count of [`euclidean_golden_fixture`].
const EUCLIDEAN_GOLDEN_ROWS: usize = 11_686;

/// Deterministic 64-bit mixer used to derive fixture components.
fn splitmix64(state: u64) -> u64 {
    let mut mixed = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    mixed ^ (mixed >> 31)
}

/// Returns one fixture component seed for an entity and dimension.
fn component_seed(entity_id: u64, component: usize) -> u64 {
    splitmix64(
        entity_id
            .wrapping_mul(1_000_003)
            .wrapping_add(component as u64),
    )
}

/// Returns small integer components whose squared distances are exact in `f32`.
fn integral_vector(entity_id: u64, dimension: usize) -> Vec<f32> {
    (0..dimension)
        .map(|component| (component_seed(entity_id, component) % 16) as f32)
        .collect()
}

/// Returns non-zero components in `[-1, 1)` for cosine fixtures.
fn unit_range_vector(entity_id: u64, dimension: usize) -> Vec<f32> {
    (0..dimension)
        .map(|component| {
            let bits = component_seed(entity_id, component) >> 40;
            bits as f32 / (1_u64 << 23) as f32 - 1.0
        })
        .collect()
}

fn vector_definition(
    dimension: usize,
    metric: VectorDistanceMetric,
) -> ValidatedDynamicIndexDefinition {
    let runtime = VectorIndexDefinition::new_node("Document", "embedding", dimension, metric)
        .expect("fixture vector definition validates");
    ValidatedDynamicIndexDefinition::Vector(
        ValidatedVectorIndexDefinition::try_from_runtime(&runtime)
            .expect("fixture V2 vector definition validates"),
    )
}

/// Writes `count` source nodes in bounded transactions.
async fn seed_sources(db: &Db, scope: DataScope, count: u64, vector: impl Fn(u64) -> Vec<f32>) {
    for chunk_start in (0..count).step_by(1_024) {
        let transaction = db
            .begin(IsolationLevel::Snapshot)
            .await
            .expect("fixture source transaction opens");
        for entity_id in chunk_start..count.min(chunk_start + 1_024) {
            transaction
                .put(
                    source_key(scope, entity_id),
                    encode_properties(&[
                        Property::new("$label", PropertyValue::String("Document".to_string())),
                        Property::new("embedding", PropertyValue::F32Array(vector(entity_id))),
                    ]),
                )
                .expect("fixture source stages");
        }
        transaction
            .commit()
            .await
            .expect("fixture source chunk commits");
    }
}

/// Wall-clock evidence for one complete build.
struct BuildReport {
    physical_index_id: VectorPhysicalIndexId,
    steps: u64,
    elapsed: Duration,
    /// Elapsed time when source scanning finished.
    scan_elapsed: Duration,
    /// Elapsed time when each quarter of the source entities was admitted.
    quarter_elapsed: [Duration; 4],
}

/// Returns cumulative admitted source entities for a constructing build.
async fn admitted_entities(db: &Db, scope: DataScope, operation_id: IndexOperationId) -> u64 {
    match read_operation(db, scope, operation_id).await.progress() {
        IndexOperationProgress::VectorBuild(VectorBuildProgress::Constructing(
            VectorBuildStage::Scan(progress),
        )) => progress.counters.entities,
        _ => u64::MAX,
    }
}

/// Drives one build to activation with `driver`, returning timing evidence.
async fn build_to_active(
    db: &Db,
    scope: DataScope,
    definition: &ValidatedDynamicIndexDefinition,
    count: u64,
    driver: &VectorIndexDriver,
    limits: SearchIndexBatchLimits,
) -> BuildReport {
    let (build_id, _, _) = create_build(db, scope, definition, count - 1).await;
    let mut claim_sequence = 1;
    let mut steps = 0_u64;
    let mut quarter_elapsed = [Duration::ZERO; 4];
    let mut next_quarter = 0_usize;
    let mut scan_elapsed = None;
    let started = Instant::now();
    loop {
        steps += 1;
        let step = drive_one(db, driver, build_id, &mut claim_sequence, limits).await;
        let admitted = admitted_entities(db, scope, build_id).await;
        while next_quarter < quarter_elapsed.len()
            && admitted >= count * (next_quarter as u64 + 1) / 4
        {
            quarter_elapsed[next_quarter] = started.elapsed();
            next_quarter += 1;
        }
        if admitted == u64::MAX && scan_elapsed.is_none() {
            scan_elapsed = Some(started.elapsed());
        }
        match step {
            CommittedOperationStep::Progressed => {}
            CommittedOperationStep::Completed => break,
            other => panic!("vector fixture build stopped at {other:?}"),
        }
    }
    let elapsed = started.elapsed();
    let active = read_index(db, scope, definition).await;
    let IndexStateV2::Active {
        physical:
            PhysicalGeneration::Vector {
                layout: VectorPhysicalLayout::Unpartitioned { physical_index_id },
                ..
            },
        ..
    } = active.state()
    else {
        panic!("completed fixture build is active and unpartitioned");
    };
    BuildReport {
        physical_index_id: *physical_index_id,
        steps,
        elapsed,
        scan_elapsed: scan_elapsed.expect("completed build left its source scan"),
        quarter_elapsed,
    }
}

/// Returns the physical row count and SHA-256 over every sorted key/value pair.
async fn physical_digest(
    db: &Db,
    scope: DataScope,
    physical_index_id: VectorPhysicalIndexId,
) -> (usize, String) {
    let rows = physical_vector_rows(db, scope, physical_index_id).await;
    let mut digest = Sha256::new();
    for (key, value) in &rows {
        digest.update((key.len() as u64).to_be_bytes());
        digest.update(key);
        digest.update((value.len() as u64).to_be_bytes());
        digest.update(value);
    }
    let bytes: [u8; 32] = digest.finalize().into();
    (
        rows.len(),
        bytes.iter().map(|byte| format!("{byte:02x}")).collect(),
    )
}

/// Builds the deterministic golden fixture and returns its complete digest.
async fn euclidean_golden_fixture(
    name: &str,
    driver: &VectorIndexDriver,
    limits: SearchIndexBatchLimits,
) -> (usize, String, u64) {
    const ENTITIES: u64 = 320;
    const DIMENSION: usize = 8;
    let db = test_db(name).await;
    let scope = DataScope::LegacyUnscoped;
    let definition = vector_definition(DIMENSION, VectorDistanceMetric::Euclidean);
    seed_sources(&db, scope, ENTITIES, |entity_id| {
        integral_vector(entity_id, DIMENSION)
    })
    .await;
    let report = build_to_active(&db, scope, &definition, ENTITIES, driver, limits).await;
    let (rows, digest) = physical_digest(&db, scope, report.physical_index_id).await;
    db.close().await.expect("golden fixture database closes");
    (rows, digest, report.steps)
}

fn limits_with_output_operations(operations: u64) -> SearchIndexBatchLimits {
    let defaults = SearchIndexBackfillLimits::default().batch();
    SearchIndexBatchLimits::try_new(
        defaults.max_entities(),
        defaults.max_input_bytes(),
        NonZeroU64::new(operations).expect("fixture operation limit is positive"),
        defaults.max_output_bytes(),
        defaults.max_single_vector_output_bytes(),
    )
    .expect("fixture limits validate")
}

#[tokio::test]
async fn source_backfill_matches_the_pinned_physical_graph_golden() {
    let (rows, digest, steps) = euclidean_golden_fixture(
        "vector-build-golden-bounded-steps",
        &driver(),
        limits_with_output_operations(2_048),
    )
    .await;
    assert!(steps > 8, "fixture must span many bounded steps");
    assert_eq!(
        (rows, digest.as_str()),
        (EUCLIDEAN_GOLDEN_ROWS, EUCLIDEAN_GOLDEN_DIGEST)
    );
}

#[tokio::test]
async fn source_backfill_graph_is_independent_of_step_boundaries() {
    let defaults = SearchIndexBackfillLimits::default().batch();
    let thirteen_entities = SearchIndexBatchLimits::try_new(
        NonZeroUsize::new(13).expect("fixture entity limit is positive"),
        defaults.max_input_bytes(),
        defaults.max_output_operations(),
        defaults.max_output_bytes(),
        defaults.max_single_vector_output_bytes(),
    )
    .expect("thirteen-entity limits validate");
    for (name, limits) in [
        ("vector-build-golden-default-steps", defaults),
        (
            "vector-build-golden-thirteen-entity-steps",
            thirteen_entities,
        ),
    ] {
        let (rows, digest, _) = euclidean_golden_fixture(name, &driver(), limits).await;
        assert_eq!(
            (rows, digest.as_str()),
            (EUCLIDEAN_GOLDEN_ROWS, EUCLIDEAN_GOLDEN_DIGEST),
            "{name}"
        );
    }
}

/// Reports source-backfill throughput for 20k 128-dimensional cosine vectors.
///
/// Run with `cargo test -p db --lib --release -- --ignored --nocapture
/// vector_source_backfill_throughput_report`.
#[tokio::test]
#[ignore = "manual throughput report; run in release mode"]
async fn vector_source_backfill_throughput_report() {
    const ENTITIES: u64 = 20_000;
    const DIMENSION: usize = 128;
    let db = test_db("vector-build-throughput-report").await;
    let scope = DataScope::LegacyUnscoped;
    let definition = vector_definition(DIMENSION, VectorDistanceMetric::Cosine);
    seed_sources(&db, scope, ENTITIES, |entity_id| {
        unit_range_vector(entity_id, DIMENSION)
    })
    .await;
    let report = build_to_active(
        &db,
        scope,
        &definition,
        ENTITIES,
        &driver(),
        SearchIndexBackfillLimits::default().batch(),
    )
    .await;
    let quarter = ENTITIES as f64 / 4.0;
    let mut previous = Duration::ZERO;
    for (index, elapsed) in report.quarter_elapsed.iter().enumerate() {
        println!(
            "quarter {} vectors/s={:.1}",
            index + 1,
            quarter / (*elapsed - previous).as_secs_f64()
        );
        previous = *elapsed;
    }
    println!(
        "vectors={ENTITIES} dimension={DIMENSION} steps={} scan={:.2}s scan vectors/s={:.1} total={:.2}s",
        report.steps,
        report.scan_elapsed.as_secs_f64(),
        ENTITIES as f64 / report.scan_elapsed.as_secs_f64(),
        report.elapsed.as_secs_f64(),
    );
    db.close().await.expect("throughput database closes");
}
