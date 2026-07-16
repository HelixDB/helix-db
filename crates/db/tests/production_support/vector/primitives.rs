//! Production contracts for active f32 and policy-neutral vector primitives.
//!
//! This feature-gated child module executes the real byte-view, SimHash,
//! candidate-ordering, typed-result, and query-local randomness boundaries.
//! It deliberately excludes reserved f16, binary, and binary-quantized codecs
//! and performs no database I/O, so coverage cannot introduce or migrate a
//! persisted representation.

use std::borrow::{Borrow, Cow};
use std::num::NonZeroUsize;

use super::*;
use crate::encoding::v1::values::vector_generation::{ActiveScoreSemantic, VectorEntityKind};

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
        vec.as_bytes()
            .chunks_exact(core::mem::size_of::<f32>())
            .map(|bytes| {
                f32::from_ne_bytes(bytes.try_into().expect("f32 chunk has exactly four bytes"))
            })
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
}

/// Exercises every active primitive boundary without creating an alternate codec.
pub(crate) fn run() {
    run_item_decoder_contracts();
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
    assert_eq!(select_layer_from_uniform(1.0, f32::NAN), 0);
    let extreme_connections = VectorIndexConfig::new("extreme", "embedding", 1)
        .with_m0(0)
        .with_m(usize::MAX);
    assert_eq!(extreme_connections.m0, 0);
    assert!(extreme_connections.validate().is_err());
    let metadata_key = crate::encoding::v1::keys::vectors::VectorKey::IndexMetadata(
        crate::encoding::v1::keys::vectors::VectorIndexMetadataKey::new(7),
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
