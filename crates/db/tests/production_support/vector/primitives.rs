//! Production contracts for active f32 and policy-neutral vector primitives.
//!
//! This feature-gated child module executes the real byte-view, SimHash,
//! candidate-ordering, typed-result, and query-local randomness boundaries.
//! It deliberately excludes reserved f16, binary, and binary-quantized codecs
//! and performs no database I/O, so coverage cannot introduce or migrate a
//! persisted representation.

use std::borrow::{Borrow, Cow};
use std::cell::Cell;
use std::num::NonZeroUsize;

use super::*;
use crate::encoding::v2::values::indexes::vector::{ActiveScoreSemantic, VectorEntityKind};

/// Process-local invalid codec used only to exercise generic decoder defenses.
///
/// No durable semantics map to this marker, and it is never encoded or stored.
#[derive(Debug, Clone)]
struct InvalidWordCodec<const WORD_SIZE: usize>;

impl<const WORD_SIZE: usize> unaligned_vector::UnalignedVectorCodec
    for InvalidWordCodec<WORD_SIZE>
{
    fn from_bytes(
        bytes: &[u8],
    ) -> Result<Cow<'_, unaligned_vector::UnalignedVector<Self>>, unaligned_vector::SizeMismatch>
    {
        Ok(Cow::Borrowed(
            unaligned_vector::UnalignedVector::from_bytes_unchecked(bytes),
        ))
    }

    fn from_slice(slice: &[f32]) -> Cow<'_, unaligned_vector::UnalignedVector<Self>> {
        Cow::Borrowed(unaligned_vector::UnalignedVector::from_bytes_unchecked(
            bytemuck::cast_slice(slice),
        ))
    }

    fn from_vec(vec: Vec<f32>) -> Cow<'static, unaligned_vector::UnalignedVector<Self>> {
        Cow::Owned(bytemuck::cast_slice(&vec).to_vec())
    }

    fn to_vec(vec: &unaligned_vector::UnalignedVector<Self>) -> Vec<f32> {
        Self::iter(vec).collect()
    }

    fn iter(
        vec: &unaligned_vector::UnalignedVector<Self>,
    ) -> impl ExactSizeIterator<Item = f32> + '_ {
        const F32_BYTES: usize = core::mem::size_of::<f32>();

        vec.as_bytes()
            .as_chunks::<F32_BYTES>()
            .0
            .iter()
            .map(|bytes| f32::from_ne_bytes(*bytes))
    }

    fn len(vec: &unaligned_vector::UnalignedVector<Self>) -> usize {
        vec.as_bytes().len() / core::mem::size_of::<f32>()
    }

    fn is_zero(vec: &unaligned_vector::UnalignedVector<Self>) -> bool {
        Self::iter(vec).all(|value| value == 0.0)
    }

    fn word_size() -> usize {
        WORD_SIZE
    }

    fn compute_simhash(
        vec: &unaligned_vector::UnalignedVector<Self>,
        hasher: &unaligned_vector::SimHasher,
    ) -> Result<unaligned_vector::SimHash, unaligned_vector::SimHashError> {
        hasher.hash_from_iter(Self::iter(vec))
    }
}

/// Non-bindable distance wrapper for an intentionally invalid codec contract.
#[derive(Debug, Clone)]
enum InvalidWordDistance<const WORD_SIZE: usize> {}

impl<const WORD_SIZE: usize> crate::search::vector::distance::sealed::Sealed
    for InvalidWordDistance<WORD_SIZE>
{
}

impl<const WORD_SIZE: usize> Distance for InvalidWordDistance<WORD_SIZE> {
    type Header = ();
    type VectorCodec = InvalidWordCodec<WORD_SIZE>;

    fn name() -> &'static str {
        "production-invalid-word-codec"
    }

    fn new_header(_vector: &unaligned_vector::UnalignedVector<Self::VectorCodec>) -> Self::Header {}

    fn distance(_p: &Item<Self>, _q: &Item<Self>) -> f32 {
        0.0
    }

    fn norm_no_header(_vector: &unaligned_vector::UnalignedVector<Self::VectorCodec>) -> f32 {
        0.0
    }
}

/// Exercises every typed vector-item decoder result without future codecs.
fn run_item_decoder_contracts() {
    let dimension = VectorDimension::try_new(3).unwrap();
    let item = Item::<distance::Cosine>::new(vec![1.0, 2.0, 3.0]);
    let encoded = encode_item(&item);
    assert_eq!(
        decode_item_borrowed::<distance::Cosine>(&encoded, dimension)
            .unwrap()
            .vector
            .to_vec(),
        vec![1.0, 2.0, 3.0]
    );
    assert!(decode_item::<distance::Cosine>(&encoded, dimension).is_ok());

    assert!(matches!(
        decode_item_borrowed::<distance::Cosine>(&[], dimension),
        Err(VectorItemDecodeError::HeaderTooShort { .. })
    ));
    const COSINE_HEADER_LEN: usize = core::mem::size_of::<<distance::Cosine as Distance>::Header>();
    let invalid_payload = vec![0_u8; COSINE_HEADER_LEN + 1];
    assert!(matches!(
        decode_item_borrowed::<distance::Cosine>(&invalid_payload, dimension),
        Err(VectorItemDecodeError::InvalidPayload(_))
    ));
    assert!(matches!(
        decode_item_borrowed::<distance::Cosine>(&encoded, VectorDimension::try_new(2).unwrap(),),
        Err(VectorItemDecodeError::DimensionMismatch {
            expected: 2,
            actual: 3,
        })
    ));

    let mut non_finite = encoded.to_vec();
    const COMPONENT_LEN: usize = core::mem::size_of::<f32>();
    non_finite[COSINE_HEADER_LEN..COSINE_HEADER_LEN + COMPONENT_LEN]
        .copy_from_slice(&f32::NAN.to_ne_bytes());
    assert!(matches!(
        decode_item_borrowed::<distance::Cosine>(&non_finite, dimension),
        Err(VectorItemDecodeError::NonFiniteComponent { index: 0 })
    ));

    let mut wrong_header = encoded.to_vec();
    let Some(first_header_byte) = wrong_header.first_mut() else {
        panic!("encoded cosine item contains a header")
    };
    *first_header_byte ^= 1;
    assert!(matches!(
        decode_item_borrowed::<distance::Cosine>(&wrong_header, dimension),
        Err(VectorItemDecodeError::HeaderMismatch)
    ));

    assert!(matches!(
        decode_item_borrowed::<InvalidWordDistance<0>>(&[], dimension),
        Err(VectorItemDecodeError::ZeroCodecWordSize)
    ));
    assert!(matches!(
        decode_item_borrowed::<InvalidWordDistance<{ usize::MAX }>>(
            &[],
            VectorDimension::try_new(usize::MAX).unwrap(),
        ),
        Err(VectorItemDecodeError::DimensionArithmeticOverflow)
    ));

    let unbound_values = [1.0_f32, 2.0, 3.0];
    let unbound_bytes = bytemuck::cast_slice(&unbound_values);
    assert!(decode_item_borrowed::<InvalidWordDistance<1>>(unbound_bytes, dimension).is_ok());
    let unbound_non_finite = [1.0_f32, f32::NAN, 3.0];
    assert!(matches!(
        decode_item_borrowed::<InvalidWordDistance<1>>(
            bytemuck::cast_slice(&unbound_non_finite),
            dimension,
        ),
        Err(VectorItemDecodeError::NonFiniteComponent { index: 1 })
    ));
    assert_eq!(
        VectorItemDecodeError::from(VectorValidationError::DimensionMismatch {
            expected: 3,
            actual: 2,
        }),
        VectorItemDecodeError::DimensionMismatch {
            expected: 3,
            actual: 2,
        }
    );
    assert_eq!(
        VectorItemDecodeError::from(VectorValidationError::MagnitudeDomain(
            super::domain::VectorMagnitudeDomainError::DimensionArithmeticOverflow {
                dimension: usize::MAX,
            },
        )),
        VectorItemDecodeError::DimensionArithmeticOverflow
    );
}

/// Exercises both allocation-free f32 SimHash paths and their closed errors.
fn run_simhash_byte_view_contracts() {
    let values = [
        0.0,
        -0.0,
        f32::from_bits(1),
        -f32::from_bits(1),
        0.5,
        -0.25,
        1.0,
        -1.0,
    ];
    let hasher = unaligned_vector::SimHasher::new_with_seed(values.len(), 42);
    let expected = hasher.hash_from_slice(&values).unwrap();

    let aligned = unaligned_vector::UnalignedVector::<f32>::from_slice(&values);
    assert!(bytemuck::try_cast_slice::<u8, f32>(aligned.as_bytes()).is_ok());
    assert_eq!(
        <f32 as unaligned_vector::UnalignedVectorCodec>::compute_simhash(&aligned, &hasher)
            .unwrap(),
        expected
    );

    let value_bytes = core::mem::size_of_val(&values);
    let mut storage = Vec::with_capacity(core::mem::align_of::<f32>() + value_bytes);
    let base = storage.as_ptr() as usize;
    let prefix_len = (1..=core::mem::align_of::<f32>())
        .find(|offset| !(base + offset).is_multiple_of(core::mem::align_of::<f32>()))
        .unwrap();
    storage.resize(prefix_len, 0);
    storage.extend(values.iter().flat_map(|value| value.to_ne_bytes()));
    let payload = &storage[storage.len() - value_bytes..storage.len()];
    assert!(bytemuck::try_cast_slice::<u8, f32>(payload).is_err());
    let unaligned = unaligned_vector::UnalignedVector::<f32>::from_bytes(payload).unwrap();
    assert_eq!(
        <f32 as unaligned_vector::UnalignedVectorCodec>::compute_simhash(&unaligned, &hasher)
            .unwrap(),
        expected
    );

    assert_eq!(
        hasher.hash_from_repeated_iter(|| values[..values.len() - 1].iter().copied()),
        Err(unaligned_vector::SimHashError::DimensionMismatch {
            expected: values.len(),
            actual: values.len() - 1,
        })
    );
    let calls = Cell::new(0);
    assert_eq!(
        hasher.hash_from_repeated_iter(|| {
            let call = calls.get();
            calls.set(call + 1);
            if call == 0 {
                values.iter().copied()
            } else {
                values[..values.len() - 1].iter().copied()
            }
        }),
        Err(unaligned_vector::SimHashError::DimensionMismatch {
            expected: values.len(),
            actual: values.len() - 1,
        })
    );
}

/// Exercises every active primitive boundary without creating an alternate codec.
pub(crate) fn run() {
    run_item_decoder_contracts();
    run_simhash_byte_view_contracts();
    let item = Item::<distance::Cosine>::new(vec![3.0, 4.0]);
    let cloned = item.clone();
    assert_eq!(cloned.vector.to_vec(), [3.0, 4.0]);
    assert!(format!("{cloned:?}").contains("Item"));
    let owned = cloned.into_owned();
    assert_eq!(owned.vector.to_vec(), [3.0, 4.0]);
    assert_eq!(distance::Cosine::norm(&owned), 5.0);

    assert!(matches!(
        VectorIndexConfig::new(" ", "embedding", 3).validate(),
        Err(VectorConfigError::EmptyIndexName)
    ));
    assert!(matches!(
        VectorIndexConfig::new("documents", " ", 3).validate(),
        Err(VectorConfigError::EmptyPropertyName)
    ));
    let mut excessive_threshold = VectorIndexConfig::new("documents", "embedding", 3);
    excessive_threshold.simhash_threshold = SIMHASH_BITS + 1;
    assert!(excessive_threshold.validate().is_err());
    let mut zero_connections = VectorIndexConfig::new("documents", "embedding", 3);
    zero_connections.m = 0;
    assert!(zero_connections.validate().is_err());
    let mut insufficient_layer0 = VectorIndexConfig::new("documents", "embedding", 3);
    insufficient_layer0.m0 = 1;
    assert!(insufficient_layer0.validate().is_err());
    let mut insufficient_construction = VectorIndexConfig::new("documents", "embedding", 3);
    insufficient_construction.ef_construction = 1;
    assert!(insufficient_construction.validate().is_err());
    let mut invalid_multiplier = VectorIndexConfig::new("documents", "embedding", 3);
    invalid_multiplier.ml = f32::NAN;
    assert!(invalid_multiplier.validate().is_err());
    let mut invalid_sampling = VectorIndexConfig::new("documents", "embedding", 3);
    invalid_sampling.sampling_ratio = f32::NAN;
    assert!(invalid_sampling.validate().is_err());
    let mut invalid_failure = VectorIndexConfig::new("documents", "embedding", 3);
    invalid_failure.adaptive_failure_prob = 1.0;
    assert!(invalid_failure.validate().is_err());
    let invalid_metadata = VectorIndexMetadata {
        config: invalid_failure,
        entry_point: None,
        max_layer: 0,
        count: 0,
    };
    assert!(invalid_metadata.validated_state().is_err());
    let encoded_layer = encode_entry_candidate_layer(7);
    assert_eq!(decode_entry_candidate_layer(&encoded_layer).unwrap(), 7);
    assert!(decode_entry_candidate_layer(&[0]).is_err());
    let encoded_neighbors = encode_neighbors(&[1, 2, 3]);
    assert_eq!(decode_neighbors(&encoded_neighbors).unwrap(), [1, 2, 3]);
    assert!(decode_neighbors(&[0]).is_err());
    assert_eq!(
        VectorDimension::try_new_with_max(3, NonZeroUsize::new(4).unwrap())
            .unwrap()
            .get(),
        3
    );
    assert!(matches!(
        VectorDimension::try_new_with_max(5, NonZeroUsize::new(4).unwrap()),
        Err(VectorDimensionError::ExceedsMaximum {
            maximum: 4,
            actual: 5
        })
    ));
    assert!(matches!(
        VectorDimension::try_new_with_max(0, NonZeroUsize::new(4).unwrap()),
        Err(VectorDimensionError::ZeroDimension)
    ));
    assert_eq!(select_layer_from_uniform(1.0, f32::NAN), 0);
    let extreme_connections = VectorIndexConfig::new("extreme", "embedding", 1)
        .with_m0(0)
        .with_m(usize::MAX);
    assert_eq!(extreme_connections.m0, 0);
    assert!(extreme_connections.validate().is_err());
    let metadata_key = crate::encoding::v2::keys::indexes::vector::VectorKey::IndexMetadata(
        crate::encoding::v2::keys::indexes::vector::VectorIndexMetadataKey::new(7),
    )
    .to_bytes();
    assert!(is_vector_index_metadata_key(&metadata_key));
    assert!(!is_vector_index_metadata_key(
        &make_vector_index_metadata_scan_prefix()
    ));

    let invalid_bytes = [0_u8; 3];
    let error = unaligned_vector::UnalignedVector::<f32>::from_bytes(&invalid_bytes).unwrap_err();
    assert!(error.to_string().contains("3 too many bytes"));

    let empty = unaligned_vector::UnalignedVector::<f32>::from_slice(&[]);
    assert!(matches!(empty, Cow::Borrowed(_)));
    assert!(empty.is_empty());
    assert!(empty.is_zero());

    let borrowed = unaligned_vector::UnalignedVector::<f32>::from_slice(&[1.0, 2.0, 3.0]);
    assert!(matches!(borrowed, Cow::Borrowed(_)));
    assert_eq!(borrowed.len(), 3);
    assert_eq!(borrowed.iter().collect::<Vec<_>>(), [1.0, 2.0, 3.0]);
    assert_eq!(borrowed.to_vec(), [1.0, 2.0, 3.0]);
    assert!(!borrowed.is_zero());
    assert_eq!(
        <f32 as unaligned_vector::UnalignedVectorCodec>::word_size(),
        1
    );
    assert_eq!(format!("{borrowed:?}"), "[1.0000, 2.0000, 3.0000]");

    let owned = unaligned_vector::UnalignedVector::<f32>::from_vec(vec![4.0, 5.0]);
    assert!(matches!(owned, Cow::Owned(_)));
    assert_eq!(owned.to_vec(), [4.0, 5.0]);

    let bytes = borrowed.as_bytes().to_vec();
    let rebound: &unaligned_vector::UnalignedVector<f32> = bytes.borrow();
    assert_eq!(rebound.as_ptr(), bytes.as_ptr());
    assert_eq!(rebound.to_owned(), bytes);

    let mut zero_tail = vec![1.0; 10];
    zero_tail.extend([0.0, 0.0]);
    let zero_tail = unaligned_vector::UnalignedVector::<f32>::from_vec(zero_tail);
    assert!(format!("{:?}", &*zero_tail).contains("0.0, ..."));
    let other_tail = unaligned_vector::UnalignedVector::<f32>::from_vec(vec![1.0; 12]);
    assert!(format!("{:?}", &*other_tail).contains("other ..."));

    let hasher = unaligned_vector::SimHasher::new_with_seed(3, 42);
    assert_eq!(hasher.dimension(), 3);
    assert_eq!(hasher.hyperplanes().len(), 64 * 3);
    let from_slice = hasher.hash_from_slice(&[1.0, 2.0, 3.0]).unwrap();
    let from_iter = hasher.hash_from_iter([1.0, 2.0, 3.0]).unwrap();
    assert_eq!(from_slice, from_iter);
    assert_eq!(from_slice.collision_count(&from_iter), 64);
    assert_eq!(from_slice.hamming_distance(&from_iter), 0);
    assert!(from_slice.passes_threshold(&from_iter, 64));
    assert_eq!(
        unaligned_vector::SimHash::from_bytes(&from_slice.to_bytes()).unwrap(),
        from_slice
    );
    assert!(matches!(
        unaligned_vector::SimHash::from_bytes(&[0; 7]),
        Err(unaligned_vector::SimHashError::InvalidLength {
            expected: 8,
            actual: 7
        })
    ));
    assert!(matches!(
        hasher.hash_from_iter([1.0, 2.0]),
        Err(unaligned_vector::SimHashError::DimensionMismatch {
            expected: 3,
            actual: 2
        })
    ));
    assert!(matches!(
        hasher.hash_from_slice(&[1.0, 2.0]),
        Err(unaligned_vector::SimHashError::DimensionMismatch {
            expected: 3,
            actual: 2
        })
    ));
    assert_eq!(
        <f32 as unaligned_vector::UnalignedVectorCodec>::compute_simhash(&borrowed, &hasher)
            .unwrap(),
        from_slice
    );
    assert!(matches!(
        <f32 as unaligned_vector::UnalignedVectorCodec>::compute_simhash(&owned, &hasher),
        Err(unaligned_vector::SimHashError::DimensionMismatch {
            expected: 3,
            actual: 2
        })
    ));

    let first = model::Candidate::try_new(1, 0.25).unwrap();
    let same = model::Candidate::try_new(1, 0.25).unwrap();
    let tied = model::Candidate::try_new(2, 0.25).unwrap();
    assert_eq!(first, same);
    assert!(first < tied);
    assert_eq!(first.score(), 0.25);
    assert_eq!(first.distance().get(), 0.25);
    for invalid in [f32::NAN, f32::INFINITY, -1.0] {
        assert!(model::Candidate::try_new(1, invalid).is_err());
    }

    let node = result::TypedVectorSearchResult::from_physical(
        VectorEntityKind::Node,
        ActiveScoreSemantic::CosineHalfF32V1,
        result::SearchResult::new(7, DistanceScore::try_new(0.25).unwrap()),
    );
    assert_eq!(node.entity_id(), result::VectorEntityId::Node(7));
    for version in [
        result::DistanceOutputVersion::CurrentScore,
        result::DistanceOutputVersion::MetricDistance,
    ] {
        let distance = node.materialize_distance(version);
        assert_eq!(distance.value(), 0.25);
        assert_eq!(distance.unit(), result::DistanceOutputUnit::HalfCosineScore);
    }

    let squared = result::TypedVectorSearchResult::from_physical(
        VectorEntityKind::Edge,
        ActiveScoreSemantic::SquaredEuclideanF32V1,
        result::SearchResult::new(8, DistanceScore::try_new(25.0).unwrap()),
    );
    assert_eq!(squared.entity_id(), result::VectorEntityId::Edge(8));
    let current = squared.materialize_distance(result::DistanceOutputVersion::default());
    assert_eq!(current.value(), 25.0);
    assert_eq!(
        current.unit(),
        result::DistanceOutputUnit::SquaredEuclideanScore
    );
    let metric = squared.materialize_distance(result::DistanceOutputVersion::MetricDistance);
    assert_eq!(metric.value(), 5.0);
    assert_eq!(metric.unit(), result::DistanceOutputUnit::EuclideanDistance);

    let manhattan = result::TypedVectorSearchResult::from_physical(
        VectorEntityKind::Node,
        ActiveScoreSemantic::ManhattanF32V1,
        result::SearchResult::new(9, DistanceScore::try_new(3.0).unwrap()),
    );
    for version in [
        result::DistanceOutputVersion::CurrentScore,
        result::DistanceOutputVersion::MetricDistance,
    ] {
        let distance = manhattan.materialize_distance(version);
        assert_eq!(distance.value(), 3.0);
        assert_eq!(
            distance.unit(),
            result::DistanceOutputUnit::ManhattanDistance
        );
    }
    assert!(DistanceScore::try_new(f32::NAN).is_err());

    let selector = randomness::LayerSelector::random();
    assert!(selector.select(f32::NAN) <= 63);
    let query = unaligned_vector::SimHash::from_bits(0x0123_4567_89AB_CDEF);
    let mut actual = randomness::SearchRandomness::QueryDerived.start(&query, 42, 128);
    let seed = query.bits() ^ 42_u64.rotate_left(17) ^ 128_u64.rotate_left(7);
    let mut expected = randomness::SearchSession::seeded(seed);
    assert!(actual.should_sample(1.0));
    assert!(!actual.should_sample(0.0));
    assert_eq!(actual.choose_index(0), None);
    for _ in 0..32 {
        assert_eq!(actual.should_sample(0.37), expected.should_sample(0.37));
        let actual_index = actual.choose_index(11).unwrap();
        let expected_index = expected.choose_index(11).unwrap();
        assert_eq!(actual_index, expected_index);
        assert!(actual_index < 11);
    }
}
