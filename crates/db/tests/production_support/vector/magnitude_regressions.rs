//! Regression contracts for finite vector-component magnitude safety.
//!
//! These tests use public or crate-private production boundaries and persisted
//! codecs so validation cannot pass through a parallel test-only path.

use std::sync::Arc;

use bytes::Bytes;
use slatedb::object_store::memory::InMemory;
use slatedb::{Db, DbReadOps, IsolationLevel};

use super::distance::{Cosine, Distance, Euclidean, Manhattan};
use super::magnitude_oracle;
use super::memory_store::VectorMemoryDirtyRows;
use super::mutation::VectorInsertContract;
use super::storage::{
    LegacyVectorValidationMode, LegacyVectorValidationOutcome, VectorRowKeyspace, VectorRows,
};
use super::unaligned_vector::UnalignedVector;
use super::*;
use crate::config::VectorIndexDefinition;
use crate::encoding::keys::scope::DataScope;
use crate::encoding::v2::keys::indexes::vector::{
    VectorIndexMetadataKey, VectorItemKey, VectorKey, VectorSimHashKey, VectorStorageLane,
};
use crate::encoding::v2::values::indexes::vector::simhash::encode_simhash;
use crate::index_lifecycle::{ValidatedDynamicIndexDefinition, ValidatedVectorIndexDefinition};

const DIMENSION: usize = 2;

fn definition(metric: VectorDistanceMetric) -> ValidatedVectorIndexDefinition {
    let definition: ValidatedDynamicIndexDefinition =
        VectorIndexDefinition::new_node("Document", "embedding", DIMENSION, metric)
            .expect("magnitude fixture definition validates")
            .try_into()
            .expect("magnitude fixture V2 definition validates");
    let ValidatedDynamicIndexDefinition::Vector(definition) = definition else {
        unreachable!("vector definition remains vector")
    };
    definition
}

async fn create_index<D: Distance>(name: &str) -> (Db, VectorIndex<D>) {
    let db = Db::open(name, Arc::new(InMemory::new()))
        .await
        .expect("magnitude fixture database opens");
    let index = VectorIndex::<D>::new(format!("{name}-index"));
    let transaction = db
        .begin(IsolationLevel::Snapshot)
        .await
        .expect("magnitude fixture create transaction opens");
    index
        .create(
            &transaction,
            VectorIndexConfig::new(index.name(), "embedding", DIMENSION),
        )
        .await
        .expect("magnitude fixture index creates");
    transaction
        .commit()
        .await
        .expect("magnitude fixture index commits");
    (db, index)
}

async fn insert_committed<D: Distance>(
    db: &Db,
    index: &VectorIndex<D>,
    entity_id: u64,
    vector: &[f32],
) {
    let transaction = db
        .begin(IsolationLevel::Snapshot)
        .await
        .expect("magnitude fixture insert transaction opens");
    index
        .insert(&transaction, entity_id, vector)
        .await
        .expect("magnitude fixture vector inserts");
    transaction
        .commit()
        .await
        .expect("magnitude fixture vector commits");
}

async fn namespace_rows(
    read: &(impl DbReadOps + Send + Sync),
    keyspace: &VectorRowKeyspace,
) -> Vec<(Bytes, Bytes)> {
    let mut rows = Vec::new();
    for lane in VectorStorageLane::ALL {
        let prefix = keyspace.key(lane.prefix_key(keyspace.index_id()));
        let mut scan = read
            .scan_prefix(prefix, ..)
            .await
            .expect("magnitude fixture namespace scans");
        while let Some(row) = scan
            .next()
            .await
            .expect("magnitude fixture namespace row reads")
        {
            rows.push((row.key, row.value));
        }
    }
    rows.sort_unstable_by(|left, right| left.0.cmp(&right.0));
    rows
}

fn record_rejection(
    failures: &mut Vec<String>,
    case: &str,
    result: Result<impl Sized, HelixDbError>,
    metric: VectorDistanceMetric,
    observed_magnitude: f32,
) {
    let inclusive_maximum =
        magnitude_oracle::inclusive_limit(metric, DIMENSION).expect("bounded metric has a limit");
    match result {
        Err(HelixDbError::VectorComponentMagnitudeExceeded {
            metric: actual_metric,
            dimension,
            component_index,
            observed_magnitude: actual_magnitude,
            inclusive_maximum: actual_maximum,
        }) if actual_metric == metric
            && dimension == DIMENSION
            && component_index == 0
            && actual_magnitude == observed_magnitude
            && actual_maximum == inclusive_maximum => {}
        Err(error) => failures.push(format!(
            "{case}: returned {error:?}, expected VectorComponentMagnitudeExceeded {{ metric: {metric:?}, dimension: {DIMENSION}, component_index: 0, observed_magnitude: {observed_magnitude}, inclusive_maximum: {inclusive_maximum} }}"
        )),
        Ok(_) => failures.push(format!("{case}: accepted an out-of-domain finite vector")),
    }
}

/// Characterizes the independent limit oracle and every current float kernel.
pub(crate) fn run_oracle_and_kernel_contracts() {
    for dimension in [1_usize, 15, 16, 17, 31, 32, 33, 1536, u32::MAX as usize] {
        for metric in [
            VectorDistanceMetric::Euclidean,
            VectorDistanceMetric::Manhattan,
        ] {
            let exact = magnitude_oracle::exact_limit(metric, dimension).unwrap();
            let limit = magnitude_oracle::inclusive_limit(metric, dimension).unwrap();
            let next = magnitude_oracle::next_up(limit);
            let production = super::domain::VectorComponentLimit::try_new(
                metric,
                VectorDimension::try_new(dimension).unwrap(),
            )
            .unwrap()
            .unwrap();
            assert_eq!(production.metric(), metric);
            assert_eq!(production.dimension().get(), dimension);
            assert_eq!(production.inclusive_maximum(), limit);
            assert!(f64::from(limit) <= exact);
            assert!(f64::from(next) > exact);
            if dimension <= 1536 {
                let accepted = vec![limit; dimension];
                let mut outside = accepted.clone();
                outside[dimension - 1] = next;
                assert!(magnitude_oracle::accepts(metric, dimension, &accepted));
                assert!(!magnitude_oracle::accepts(metric, dimension, &outside));
            }
        }
    }
    assert_eq!(
        magnitude_oracle::inclusive_limit(VectorDistanceMetric::Cosine, DIMENSION),
        None
    );
    #[cfg(target_pointer_width = "64")]
    assert_eq!(
        super::domain::VectorComponentLimit::try_new(
            VectorDistanceMetric::Euclidean,
            VectorDimension::try_new(usize::MAX).unwrap(),
        ),
        Err(
            super::domain::VectorMagnitudeDomainError::DimensionArithmeticOverflow {
                dimension: usize::MAX,
            }
        )
    );

    for dimension in [1_usize, 15, 16, 17, 31, 32, 33, 1536] {
        let euclidean_limit =
            magnitude_oracle::inclusive_limit(VectorDistanceMetric::Euclidean, dimension).unwrap();
        let euclidean_left = vec![euclidean_limit; dimension];
        let euclidean_right = vec![-euclidean_limit; dimension];
        let euclidean_left_view = UnalignedVector::from_slice(&euclidean_left);
        let euclidean_right_view = UnalignedVector::from_slice(&euclidean_right);
        let scalar = spaces::simple::euclidean_distance_non_optimized(
            &euclidean_left_view,
            &euclidean_right_view,
        );
        let dispatched =
            spaces::simple::euclidean_distance(&euclidean_left_view, &euclidean_right_view);
        let reverse =
            spaces::simple::euclidean_distance(&euclidean_right_view, &euclidean_left_view);
        let oracle = magnitude_oracle::squared_euclidean(&euclidean_left, &euclidean_right);
        let relative_tolerance = (dimension as f64 * f64::from(f32::EPSILON)).max(1.0e-5);
        assert!(scalar.is_finite() && scalar >= 0.0);
        assert!(dispatched.is_finite() && dispatched >= 0.0);
        assert_eq!(dispatched, reverse);
        assert!(
            (f64::from(scalar) - oracle).abs() <= oracle * relative_tolerance,
            "dimension {dimension} scalar squared-Euclidean score differs from f64 oracle"
        );
        assert!(
            (f64::from(dispatched) - oracle).abs() <= oracle * relative_tolerance,
            "dimension {dimension} dispatched squared-Euclidean score differs from f64 oracle"
        );

        let manhattan_limit =
            magnitude_oracle::inclusive_limit(VectorDistanceMetric::Manhattan, dimension).unwrap();
        let manhattan_left = vec![manhattan_limit; dimension];
        let manhattan_right = vec![-manhattan_limit; dimension];
        let manhattan_left_view = UnalignedVector::from_slice(&manhattan_left);
        let manhattan_right_view = UnalignedVector::from_slice(&manhattan_right);
        let score = spaces::simple::manhattan_distance(&manhattan_left_view, &manhattan_right_view);
        let reverse =
            spaces::simple::manhattan_distance(&manhattan_right_view, &manhattan_left_view);
        let oracle = magnitude_oracle::manhattan(&manhattan_left, &manhattan_right);
        assert!(score.is_finite() && score >= 0.0);
        assert_eq!(score, reverse);
        assert!(
            (f64::from(score) - oracle).abs() <= oracle * relative_tolerance,
            "dimension {dimension} Manhattan score differs from f64 oracle"
        );
    }

    let euclidean_huge = UnalignedVector::from_slice(&[1.0e20_f32]);
    let euclidean_opposite = UnalignedVector::from_slice(&[-1.0e20_f32]);
    assert!(!spaces::simple::euclidean_distance_non_optimized(
        &euclidean_huge,
        &euclidean_opposite
    )
    .is_finite());
    assert!(!spaces::simple::euclidean_distance(&euclidean_huge, &euclidean_opposite).is_finite());
    let manhattan_huge = UnalignedVector::from_slice(&[f32::MAX]);
    let manhattan_opposite = UnalignedVector::from_slice(&[-f32::MAX]);
    assert!(!spaces::simple::manhattan_distance(&manhattan_huge, &manhattan_opposite).is_finite());

    let special = [
        0.0,
        -0.0,
        f32::from_bits(1),
        -f32::from_bits(1),
        f32::MAX.sqrt(),
        -f32::MAX.sqrt(),
        1.0e20,
        -1.0e20,
        f32::MAX,
        -f32::MAX,
    ];
    for metric in [
        VectorDistanceMetric::Euclidean,
        VectorDistanceMetric::Manhattan,
    ] {
        let limit = magnitude_oracle::inclusive_limit(metric, DIMENSION).unwrap();
        for value in special {
            assert_eq!(
                magnitude_oracle::accepts(metric, DIMENSION, &[value, 0.0]),
                value.abs() <= limit
            );
        }
    }

    let generated_domains = [
        (VectorDistanceMetric::Euclidean, 1_usize),
        (VectorDistanceMetric::Euclidean, 15),
        (VectorDistanceMetric::Euclidean, 16),
        (VectorDistanceMetric::Euclidean, 17),
        (VectorDistanceMetric::Euclidean, 31),
        (VectorDistanceMetric::Euclidean, 32),
        (VectorDistanceMetric::Euclidean, 33),
        (VectorDistanceMetric::Euclidean, 1536),
        (VectorDistanceMetric::Manhattan, 1),
        (VectorDistanceMetric::Manhattan, 15),
        (VectorDistanceMetric::Manhattan, 16),
        (VectorDistanceMetric::Manhattan, 17),
        (VectorDistanceMetric::Manhattan, 31),
        (VectorDistanceMetric::Manhattan, 32),
        (VectorDistanceMetric::Manhattan, 33),
        (VectorDistanceMetric::Manhattan, 1536),
    ];
    let mut random_state = 0x5eed_5afe_cafe_babe_u64;
    for case in 0..128 {
        let (metric, dimension) = generated_domains[case % generated_domains.len()];
        let limit = magnitude_oracle::inclusive_limit(metric, dimension).unwrap();
        let mut generate_vector = || {
            (0..dimension)
                .map(|_| {
                    random_state = random_state
                        .wrapping_mul(6_364_136_223_846_793_005)
                        .wrapping_add(1_442_695_040_888_963_407);
                    let unit = (random_state >> 32) as u32 as f64 / f64::from(u32::MAX);
                    (((unit * 2.0) - 1.0) * f64::from(limit)) as f32
                })
                .collect::<Vec<_>>()
        };
        let left = generate_vector();
        let right = generate_vector();
        assert!(magnitude_oracle::accepts(metric, dimension, &left));
        assert!(magnitude_oracle::accepts(metric, dimension, &right));
        let left_view = UnalignedVector::from_slice(&left);
        let right_view = UnalignedVector::from_slice(&right);
        let (score, reverse, oracle) = match metric {
            VectorDistanceMetric::Euclidean => (
                spaces::simple::euclidean_distance(&left_view, &right_view),
                spaces::simple::euclidean_distance(&right_view, &left_view),
                magnitude_oracle::squared_euclidean(&left, &right),
            ),
            VectorDistanceMetric::Manhattan => (
                spaces::simple::manhattan_distance(&left_view, &right_view),
                spaces::simple::manhattan_distance(&right_view, &left_view),
                magnitude_oracle::manhattan(&left, &right),
            ),
            VectorDistanceMetric::Cosine => unreachable!("generated domains exclude cosine"),
        };
        let relative_tolerance = (dimension as f64 * f64::from(f32::EPSILON)).max(1.0e-5);
        assert!(score.is_finite(), "{metric:?} dimension {dimension}");
        assert!(score >= 0.0, "{metric:?} dimension {dimension}");
        assert_eq!(score, reverse, "{metric:?} dimension {dimension}");
        assert!(
            (f64::from(score) - oracle).abs() <= oracle.abs().max(1.0) * relative_tolerance,
            "{metric:?} dimension {dimension}: score={score}, oracle={oracle}"
        );
    }
}

/// Proves accepted bytes/scores remain frozen and cosine keeps its full finite domain.
pub(crate) async fn run_golden_and_cosine_contracts() {
    let item = Item::<Euclidean>::new(vec![1.0, -2.0]);
    let mut expected = Vec::new();
    expected.extend_from_slice(&0.0_f32.to_ne_bytes());
    expected.extend_from_slice(&1.0_f32.to_ne_bytes());
    expected.extend_from_slice(&(-2.0_f32).to_ne_bytes());
    assert_eq!(encode_item(&item).as_ref(), expected);
    assert_eq!(
        Euclidean::distance(&Item::new(vec![1.0, 2.0]), &Item::new(vec![4.0, 6.0])),
        25.0
    );
    assert_eq!(
        Manhattan::distance(
            &Item::new(vec![1.0, -2.0, 3.0]),
            &Item::new(vec![-1.0, 2.0, 1.0])
        ),
        8.0
    );

    let (db, index) = create_index::<Cosine>("production-vector-magnitude-cosine-domain").await;
    insert_committed(&db, &index, 1, &[f32::MAX, f32::MAX]).await;
    let results = index
        .search(
            &db,
            &[f32::MAX, f32::MAX],
            &SearchParams::new(1).expect("cosine result count validates"),
        )
        .await
        .expect("cosine keeps accepting extreme finite non-zero vectors");
    assert_eq!(results.len(), 1);
    assert!(results[0].score().get().is_finite());
    db.close().await.expect("cosine fixture closes");
}

/// Requires current-row decoding to reject finite values outside the oracle domain.
pub(crate) fn run_current_row_decode_contracts() {
    let dimension = VectorDimension::try_new(DIMENSION).unwrap();
    let euclidean_limit =
        magnitude_oracle::inclusive_limit(VectorDistanceMetric::Euclidean, DIMENSION).unwrap();
    let euclidean_next = magnitude_oracle::next_up(euclidean_limit);
    let exact = encode_item(&Item::<Euclidean>::new(vec![
        euclidean_limit,
        -euclidean_limit,
    ]));
    let outside = encode_item(&Item::<Euclidean>::new(vec![
        euclidean_next,
        -euclidean_limit,
    ]));
    assert!(decode_item_borrowed::<Euclidean>(&exact, dimension).is_ok());
    assert_eq!(
        decode_item_borrowed::<Euclidean>(&outside, dimension).unwrap_err(),
        VectorItemDecodeError::ComponentMagnitudeExceeded {
            metric: VectorDistanceMetric::Euclidean,
            dimension: DIMENSION,
            component_index: 0,
            observed_magnitude: euclidean_next,
            inclusive_maximum: euclidean_limit,
        }
    );

    let manhattan_limit =
        magnitude_oracle::inclusive_limit(VectorDistanceMetric::Manhattan, DIMENSION).unwrap();
    let manhattan_next = magnitude_oracle::next_up(manhattan_limit);
    let exact = encode_item(&Item::<Manhattan>::new(vec![
        manhattan_limit,
        -manhattan_limit,
    ]));
    let outside = encode_item(&Item::<Manhattan>::new(vec![
        manhattan_next,
        -manhattan_limit,
    ]));
    assert!(decode_item_borrowed::<Manhattan>(&exact, dimension).is_ok());
    assert_eq!(
        decode_item_borrowed::<Manhattan>(&outside, dimension).unwrap_err(),
        VectorItemDecodeError::ComponentMagnitudeExceeded {
            metric: VectorDistanceMetric::Manhattan,
            dimension: DIMENSION,
            component_index: 0,
            observed_magnitude: manhattan_next,
            inclusive_maximum: manhattan_limit,
        }
    );
}

async fn exercise_mutation_rejection<D: Distance>(
    name: &str,
    metric: VectorDistanceMetric,
) -> Vec<String> {
    let (db, base_index) = create_index::<D>(name).await;
    let limit = magnitude_oracle::inclusive_limit(metric, DIMENSION).unwrap();
    let outside = magnitude_oracle::next_up(limit);
    insert_committed(&db, &base_index, 99, &[limit, -limit]).await;
    let before = namespace_rows(&db, base_index.row_keyspace()).await;
    let dirty = Arc::new(VectorMemoryDirtyRows::default());
    let index = VectorIndex::<D>::new(base_index.name()).with_write_dirty_rows(Arc::clone(&dirty));
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let result = index.insert(&transaction, 1, &[outside, 0.0]).await;
    let staged = namespace_rows(&transaction, index.row_keyspace()).await;
    transaction.rollback();
    let durable_after = namespace_rows(&db, index.row_keyspace()).await;

    let mut failures = Vec::new();
    record_rejection(
        &mut failures,
        &format!("{name} fresh insert"),
        result,
        metric,
        outside,
    );
    if staged != before {
        failures.push(format!(
            "{name} fresh insert staged vector rows before magnitude rejection"
        ));
    }
    if dirty.is_node_dirty(1) {
        failures.push(format!(
            "{name} fresh insert dirtied transaction-local cache state"
        ));
    }
    if durable_after != before {
        failures.push(format!(
            "{name} fresh insert changed durable rows after rollback"
        ));
    }

    insert_committed(&db, &base_index, 2, &[0.25, -0.25]).await;
    let before = namespace_rows(&db, base_index.row_keyspace()).await;
    let dirty = Arc::new(VectorMemoryDirtyRows::default());
    let index = VectorIndex::<D>::new(base_index.name()).with_write_dirty_rows(Arc::clone(&dirty));
    let transaction = db.begin(IsolationLevel::Snapshot).await.unwrap();
    let measured = MeasuredVectorTransaction::new(&transaction);
    let result = index
        .insert_with_measured_transaction(
            &measured,
            2,
            &[outside, 0.0],
            VectorInsertContract::Upsert,
            Some(0),
        )
        .await;
    let staged = namespace_rows(&transaction, index.row_keyspace()).await;
    let measurement = measured.measurement().unwrap();
    transaction.rollback();
    let durable_after = namespace_rows(&db, index.row_keyspace()).await;
    record_rejection(
        &mut failures,
        &format!("{name} upsert"),
        result,
        metric,
        outside,
    );
    if staged != before || measurement.operations() != 0 || measurement.encoded_bytes() != 0 {
        failures.push(format!(
            "{name} upsert staged writes before magnitude rejection"
        ));
    }
    if dirty.is_node_dirty(2) {
        failures.push(format!("{name} upsert dirtied cache state"));
    }
    if durable_after != before {
        failures.push(format!("{name} upsert changed durable rows after rollback"));
    }

    db.close().await.expect("mutation fixture closes");
    failures
}

/// Requires insert and upsert to reject before any row or cache mutation.
pub(crate) async fn run_mutation_contracts() {
    let mut failures = exercise_mutation_rejection::<Euclidean>(
        "production-vector-magnitude-euclidean-mutation",
        VectorDistanceMetric::Euclidean,
    )
    .await;
    failures.extend(
        exercise_mutation_rejection::<Manhattan>(
            "production-vector-magnitude-manhattan-mutation",
            VectorDistanceMetric::Manhattan,
        )
        .await,
    );
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

async fn exercise_search_rejection<D: Distance>(
    name: &str,
    metric: VectorDistanceMetric,
    restricted: bool,
) -> Vec<String> {
    let (db, index) = create_index::<D>(name).await;
    insert_committed(&db, &index, 1, &[0.0, 0.0]).await;
    insert_committed(&db, &index, 2, &[1.0, -1.0]).await;
    let params = SearchParams::new(1).unwrap();
    let limit = magnitude_oracle::inclusive_limit(metric, DIMENSION).unwrap();
    let accepted = [limit, -limit];
    let outside = [magnitude_oracle::next_up(limit), -limit];
    let accepted_result = if restricted {
        index
            .search_restricted(
                &db,
                &accepted,
                &params,
                &RestrictedVectorCandidates::from_ids([1, 2]).unwrap(),
            )
            .await
    } else {
        index.search(&db, &accepted, &params).await
    };
    assert!(
        accepted_result.is_ok(),
        "{name} exact inclusive limit failed"
    );

    let outside_result = if restricted {
        index
            .search_restricted(
                &db,
                &outside,
                &params,
                &RestrictedVectorCandidates::from_ids([1, 2]).unwrap(),
            )
            .await
    } else {
        index.search(&db, &outside, &params).await
    };
    let catastrophic_result = if restricted {
        index
            .search_restricted(
                &db,
                &[f32::MAX, -f32::MAX],
                &params,
                &RestrictedVectorCandidates::from_ids([1, 2]).unwrap(),
            )
            .await
    } else {
        index.search(&db, &[f32::MAX, -f32::MAX], &params).await
    };
    db.close().await.expect("search fixture closes");

    let mut failures = Vec::new();
    record_rejection(
        &mut failures,
        &format!("{name} next-representable query"),
        outside_result,
        metric,
        outside[0],
    );
    record_rejection(
        &mut failures,
        &format!("{name} catastrophic finite query"),
        catastrophic_result,
        metric,
        f32::MAX,
    );
    failures
}

/// Requires unrestricted search to reject before an invalid score reaches ordering.
pub(crate) async fn run_search_contracts() {
    let mut failures = exercise_search_rejection::<Euclidean>(
        "production-vector-magnitude-euclidean-search",
        VectorDistanceMetric::Euclidean,
        false,
    )
    .await;
    failures.extend(
        exercise_search_rejection::<Manhattan>(
            "production-vector-magnitude-manhattan-search",
            VectorDistanceMetric::Manhattan,
            false,
        )
        .await,
    );
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// Requires restricted search to enforce the same numeric domain.
pub(crate) async fn run_restricted_search_contracts() {
    let mut failures = exercise_search_rejection::<Euclidean>(
        "production-vector-magnitude-euclidean-restricted",
        VectorDistanceMetric::Euclidean,
        true,
    )
    .await;
    failures.extend(
        exercise_search_rejection::<Manhattan>(
            "production-vector-magnitude-manhattan-restricted",
            VectorDistanceMetric::Manhattan,
            true,
        )
        .await,
    );
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

async fn exercise_legacy_rejection<D: Distance>(
    name: &str,
    metric: VectorDistanceMetric,
) -> Option<String> {
    let db = Db::open(name, Arc::new(InMemory::new()))
        .await
        .expect("legacy magnitude database opens");
    let definition = definition(metric);
    let physical_name = format!("{name}-physical");
    let keyspace =
        VectorRowKeyspace::from_legacy_name(physical_name.clone(), DataScope::LegacyUnscoped);
    let mut metadata = VectorIndexMetadata::new(VectorIndexConfig::from_v2_definition(
        &definition,
        &physical_name,
    ));
    metadata.entry_point = Some(1);
    metadata.count = 1;
    let limit = magnitude_oracle::inclusive_limit(metric, DIMENSION).unwrap();
    let outside = magnitude_oracle::next_up(limit);
    let item = encode_item(&Item::<D>::new(vec![outside, 0.0]));
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
            keyspace.key(VectorKey::SimHash(VectorSimHashKey::new(
                keyspace.index_id(),
                1,
            ))),
            Bytes::copy_from_slice(&encode_simhash(0)),
        )
        .unwrap();
    transaction
        .put(
            keyspace.key(VectorKey::Vector(VectorItemKey::new(
                keyspace.index_id(),
                0,
                1,
            ))),
            item,
        )
        .unwrap();
    transaction.commit().await.unwrap();
    let outcome = VectorRows::new(&db, &keyspace)
        .validate_legacy_physical::<D>(
            VectorStorageLane::Layer0,
            None,
            &definition,
            LegacyVectorValidationMode::ReadOnly,
            usize::MAX,
            u64::MAX,
        )
        .await
        .unwrap();
    db.close().await.expect("legacy magnitude database closes");
    if matches!(outcome, LegacyVectorValidationOutcome::Invalid { .. }) {
        None
    } else {
        Some(format!(
            "{name}: legacy physical validation accepted an out-of-domain item"
        ))
    }
}

/// Requires frozen legacy physical validation to fail before adoption.
pub(crate) async fn run_legacy_validation_contracts() {
    let mut failures = Vec::new();
    if let Some(failure) = exercise_legacy_rejection::<Euclidean>(
        "production-vector-magnitude-euclidean-legacy",
        VectorDistanceMetric::Euclidean,
    )
    .await
    {
        failures.push(failure);
    }
    if let Some(failure) = exercise_legacy_rejection::<Manhattan>(
        "production-vector-magnitude-manhattan-legacy",
        VectorDistanceMetric::Manhattan,
    )
    .await
    {
        failures.push(failure);
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
