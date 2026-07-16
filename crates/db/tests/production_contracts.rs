//! Public production-linked compatibility contracts.
//!
//! This target imports the compiled `db` library without `cfg(test)`. It covers
//! only supported public contracts. Physical vector rows, cache stores, raw
//! metadata DTOs, and the direct HNSW facade are exercised by the feature-gated
//! internal production-contract target instead of being kept public for tests.

use std::{cmp::Ordering, num::NonZeroUsize};

use db::config::VectorIndexDefinition;
use db::encoding::v1::values::vectors::{decode_layer0_neighbors, encode_layer0_neighbors};
use db::search::vector::distance::{Cosine, Distance, Euclidean, Manhattan};
use db::search::vector::unaligned_vector::{SimHashError, UnalignedVector};
use db::search::vector::{
    CollisionThreshold, Connections, ConstructionBeamWidth, DistanceScore, FailureProbability,
    Item, Layer0Connections, LayerMultiplier, ResultCount, SameDimensionPair, SearchBeamWidth,
    SearchParams, SimHashMode, SimHasher, UnitInterval, VectorDimension, VectorDistanceMetric,
    VectorParameterError, VectorRef,
};
use db::{HelixDB, HelixDbMode, HelixDbSource};

#[test]
fn current_layer0_neighbor_bytes_are_stable_through_the_public_codec() {
    let encoded = encode_layer0_neighbors(&[9, 2, 9]);
    assert_eq!(
        encoded.as_ref(),
        &[
            0x12, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x09,
        ]
    );
    assert_eq!(decode_layer0_neighbors(&encoded).unwrap(), vec![2, 9]);
}

#[test]
fn public_vector_definition_owns_validated_configuration() {
    let definition =
        VectorIndexDefinition::new_node("Document", "embedding", 384, VectorDistanceMetric::Cosine)
            .unwrap();
    assert_eq!(definition.dimension(), 384);
    assert_eq!(definition.m0(), definition.m() * 2);
    assert_eq!(VectorDistanceMetric::Cosine.as_str(), "cosine");
    assert_eq!(VectorDistanceMetric::Euclidean.as_str(), "euclidean");
    assert_eq!(VectorDistanceMetric::Manhattan.as_str(), "manhattan");

    assert!(VectorIndexDefinition::new_node(
        "Document",
        "embedding",
        0,
        VectorDistanceMetric::Cosine,
    )
    .is_err());
    assert!(definition.clone().with_m(0).is_err());
    assert!(definition.clone().with_m0(0).is_err());
    assert!(definition.clone().with_ef_construction(0).is_err());
    assert!(definition.clone().with_ml(f32::NAN).is_err());
    assert!(definition.clone().with_sampling_ratio(2.0).is_err());
    assert!(definition.with_adaptive_failure_prob(1.0).is_err());
}

#[tokio::test]
async fn public_in_memory_writer_opens_and_closes() {
    let db = HelixDB::open(HelixDbSource::InMemory {
        database: "production-coverage-baseline".to_owned(),
    })
    .await
    .expect("in-memory writer opens");

    assert_eq!(db.mode(), HelixDbMode::Writer);
    assert!(db.is_writer_mode());
    assert!(!db.is_reader_mode());
    db.close().await.expect("in-memory writer closes");
}

#[test]
fn public_vector_parameter_types_reject_invalid_states() {
    let connections = Connections::try_new(4).unwrap();
    assert_eq!(connections.get(), 4);
    assert_eq!(connections.checked_double().unwrap().get(), 8);
    assert!(matches!(
        Connections::try_new(0),
        Err(VectorParameterError::Zero { .. })
    ));
    assert!(matches!(
        Connections::try_new(usize::MAX).unwrap().checked_double(),
        Err(VectorParameterError::ArithmeticOverflow { .. })
    ));

    assert_eq!(Layer0Connections::try_new(8, connections).unwrap().get(), 8);
    assert!(Layer0Connections::try_new(3, connections).is_err());
    assert_eq!(
        ConstructionBeamWidth::try_new(12, connections)
            .unwrap()
            .get(),
        12
    );
    assert!(ConstructionBeamWidth::try_new(3, connections).is_err());

    let result_count = ResultCount::try_new(3).unwrap();
    assert_eq!(result_count.get(), 3);
    assert!(ResultCount::try_new(0).is_err());
    assert_eq!(SearchBeamWidth::try_new(8, result_count).unwrap().get(), 8);
    assert!(SearchBeamWidth::try_new(2, result_count).is_err());

    assert_eq!(LayerMultiplier::try_new(0.5).unwrap().get(), 0.5);
    for invalid in [0.0, -1.0, f32::NAN, f32::INFINITY] {
        assert!(LayerMultiplier::try_new(invalid).is_err());
    }
    for valid in [0.0, 0.5, 1.0, -0.0] {
        assert!(UnitInterval::try_new(valid).is_ok());
    }
    for invalid in [-0.1, 1.1, f32::NAN] {
        assert!(UnitInterval::try_new(invalid).is_err());
    }
    assert_eq!(FailureProbability::try_new(0.5).unwrap().get(), 0.5);
    for invalid in [0.0, 1.0, f32::NAN] {
        assert!(FailureProbability::try_new(invalid).is_err());
    }

    let bits = NonZeroUsize::new(64).unwrap();
    assert_eq!(CollisionThreshold::try_new(64, bits).unwrap().get(), 64);
    assert!(CollisionThreshold::try_new(65, bits).is_err());

    let negative_zero = DistanceScore::try_new(-0.0).unwrap();
    let positive = DistanceScore::try_new(0.25).unwrap();
    assert_eq!(negative_zero.get(), 0.0);
    assert_eq!(negative_zero.cmp(&positive), Ordering::Less);
    assert_eq!(negative_zero.partial_cmp(&positive), Some(Ordering::Less));
    assert!(DistanceScore::try_new(-0.1).is_err());
    assert!(DistanceScore::try_new(f32::NAN).is_err());
}

#[test]
fn public_vector_dimension_types_bind_exact_lengths() {
    let dimension = VectorDimension::try_new(3).unwrap();
    assert_eq!(dimension.get(), 3);
    assert!(VectorDimension::try_new(0).is_err());
    assert!(VectorDimension::try_new_with_max(4, NonZeroUsize::new(3).unwrap()).is_err());

    let left = UnalignedVector::<f32>::from_slice(&[1.0, 2.0, 3.0]);
    let right = UnalignedVector::<f32>::from_slice(&[3.0, 2.0, 1.0]);
    let left_ref = VectorRef::try_new(&left, dimension).unwrap();
    assert_eq!(left_ref.dimension(), dimension);
    assert_eq!(left_ref.values().len(), 3);
    assert!(VectorRef::try_new(&left, VectorDimension::try_new(2).unwrap()).is_err());

    let pair = SameDimensionPair::try_new(&left, &right).unwrap();
    assert_eq!(pair.dimension(), dimension);
    assert_eq!(pair.left().values().len(), pair.right().values().len());
    let short = UnalignedVector::<f32>::from_slice(&[1.0]);
    assert!(SameDimensionPair::try_new(&left, &short).is_err());
}

#[test]
fn public_vector_search_parameters_reject_invalid_overrides() {
    let default = SearchParams::new(7).unwrap();
    assert_eq!(default.k(), 7);
    assert!(default.ef() >= default.k());
    assert!(SearchParams::new(0).is_err());
    assert!(SearchParams::new(7).unwrap().with_ef(2).is_err());

    assert!(SearchParams::new(3)
        .unwrap()
        .with_pre_simhash_sampling_ratio(f32::NAN)
        .is_err());
    assert!(SearchParams::new(3)
        .unwrap()
        .with_simhash_sampling_ratio(f32::INFINITY)
        .is_err());
    assert!(SearchParams::new(3)
        .unwrap()
        .with_simhash_failure_prob(2.0)
        .is_err());
    assert!(SearchParams::new(3)
        .unwrap()
        .with_simhash_bypass_tuning(0, 1, 0.5, 1)
        .is_err());

    for mode in [SimHashMode::Off, SimHashMode::Always, SimHashMode::Adaptive] {
        let params = SearchParams::new(3)
            .unwrap()
            .with_simhash_mode(mode)
            .with_pre_simhash_sampling_ratio(0.75)
            .unwrap()
            .clear_pre_simhash_sampling_ratio_override()
            .with_pre_simhash_sampling_ratio(0.5)
            .unwrap()
            .with_simhash_sampling_ratio(0.25)
            .unwrap()
            .clear_simhash_sampling_ratio_override()
            .with_simhash_sampling_ratio(0.4)
            .unwrap()
            .with_simhash_failure_prob(0.2)
            .unwrap()
            .clear_simhash_failure_prob_override()
            .with_simhash_failure_prob(0.3)
            .unwrap()
            .with_simhash_bypass_tuning(2, 3, 0.5, 4)
            .unwrap();
        assert_eq!(params.k(), 3);
        assert!(params.ef() >= 3);
    }

    let throughput = SearchParams::throughput_profile_floor_92(10).unwrap();
    assert_eq!(throughput.k(), 10);
    assert!(throughput.ef() >= 48);
}

#[test]
fn public_float_distance_kernels_cover_long_vectors_and_invalid_zero_cosine() {
    let left_values = (0..33).map(|value| value as f32).collect::<Vec<_>>();
    let right_values = (0..33)
        .map(|value| (value as f32) + 1.0)
        .collect::<Vec<_>>();

    let cosine_left = Item::<Cosine>::new(left_values.clone());
    let cosine_right = Item::<Cosine>::new(right_values.clone());
    let cosine_zero = Item::<Cosine>::new(vec![0.0; 33]);
    assert_eq!(Cosine::name(), "cosine");
    assert!(Cosine::distance(&cosine_left, &cosine_right).is_finite());
    assert!(Cosine::distance(&cosine_left, &cosine_zero).is_nan());
    assert!(format!("{:?}", cosine_left.header).contains("norm"));

    let euclidean_left = Item::<Euclidean>::new(left_values.clone());
    let euclidean_right = Item::<Euclidean>::new(right_values.clone());
    assert_eq!(Euclidean::name(), "euclidean");
    assert_eq!(Euclidean::distance(&euclidean_left, &euclidean_right), 33.0);
    assert!(Euclidean::norm(&euclidean_left).is_finite());
    assert!(format!("{:?}", euclidean_left.header).contains("bias"));

    let manhattan_left = Item::<Manhattan>::new(left_values);
    let manhattan_right = Item::<Manhattan>::new(right_values);
    assert_eq!(Manhattan::name(), "manhattan");
    assert_eq!(Manhattan::distance(&manhattan_left, &manhattan_right), 33.0);
    assert!(Manhattan::norm(&manhattan_left).is_finite());
    assert!(format!("{:?}", manhattan_left.header).contains("bias"));

    let hasher = SimHasher::new(3);
    assert_eq!(
        hasher.hash_from_slice(&[1.0, 2.0]),
        Err(SimHashError::DimensionMismatch {
            expected: 3,
            actual: 2,
        })
    );
}
