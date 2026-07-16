use std::borrow::Borrow;

use proptest::collection::vec;
use proptest::prelude::*;

use super::*;

// ── A. from_bytes error paths (SizeMismatch) ────────────────────────────

#[test]
fn f32_from_bytes_size_mismatch() {
    // Invalid: not multiples of 4
    for n in [1, 2, 3, 5, 6, 7, 9] {
        let bytes = vec![0u8; n];
        assert!(
            UnalignedVector::<f32>::from_bytes(&bytes).is_err(),
            "expected SizeMismatch for {n} bytes"
        );
    }
    // Valid: multiples of 4
    for n in [0, 4, 8, 12, 16] {
        let bytes = vec![0u8; n];
        assert!(
            UnalignedVector::<f32>::from_bytes(&bytes).is_ok(),
            "expected Ok for {n} bytes"
        );
    }
}

#[test]
fn binary_from_bytes_size_mismatch() {
    // Invalid: not multiples of 8
    for n in [1, 2, 3, 4, 5, 6, 7, 9, 15] {
        let bytes = vec![0u8; n];
        assert!(
            UnalignedVector::<Binary>::from_bytes(&bytes).is_err(),
            "expected SizeMismatch for {n} bytes"
        );
    }
    // Valid: multiples of 8
    for n in [0, 8, 16, 24] {
        let bytes = vec![0u8; n];
        assert!(
            UnalignedVector::<Binary>::from_bytes(&bytes).is_ok(),
            "expected Ok for {n} bytes"
        );
    }
}

#[test]
fn binary_quantized_from_bytes_size_mismatch() {
    // Same word size as Binary (u64 = 8 bytes)
    for n in [1, 2, 3, 4, 5, 6, 7, 9, 15] {
        let bytes = vec![0u8; n];
        assert!(
            UnalignedVector::<BinaryQuantized>::from_bytes(&bytes).is_err(),
            "expected SizeMismatch for {n} bytes"
        );
    }
    for n in [0, 8, 16, 24] {
        let bytes = vec![0u8; n];
        assert!(
            UnalignedVector::<BinaryQuantized>::from_bytes(&bytes).is_ok(),
            "expected Ok for {n} bytes"
        );
    }
}

// ── B. f32 codec edge cases ─────────────────────────────────────────────

#[test]
fn f32_roundtrip_special_values() {
    let specials = [
        f32::NAN,
        f32::INFINITY,
        f32::NEG_INFINITY,
        0.0_f32,
        -0.0_f32,
        f32::MIN_POSITIVE, // smallest positive subnormal
        -f32::MIN_POSITIVE,
    ];
    let encoded = UnalignedVector::<f32>::from_slice(&specials);
    let decoded = encoded.to_vec();

    assert_eq!(specials.len(), decoded.len());
    for (orig, dec) in specials.iter().zip(&decoded) {
        assert_eq!(orig.to_bits(), dec.to_bits(), "bitwise mismatch for {orig}");
    }
}

#[test]
fn f32_roundtrip_empty() {
    let empty: &[f32] = &[];
    let encoded = UnalignedVector::<f32>::from_slice(empty);
    assert_eq!(encoded.len(), 0);
    assert!(encoded.is_empty());
    assert_eq!(encoded.to_vec(), Vec::<f32>::new());
}

// ── C. Binary / BinaryQuantized special value semantics ─────────────────

#[test]
fn binary_encoding_edge_values() {
    // Binary uses: bits < 0x8000_0000 && bits > 0 → 1, else 0
    // NaN (positive): bits = 0x7FC00000 → encodes as 1
    // +0.0: bits = 0x00000000 → encodes as 0
    // -0.0: bits = 0x80000000 → encodes as 0
    // +Inf: bits = 0x7F800000 → encodes as 1
    // -Inf: bits = 0xFF800000 → encodes as 0
    let input = [f32::NAN, 0.0, -0.0, f32::INFINITY, f32::NEG_INFINITY];
    let encoded = UnalignedVector::<Binary>::from_slice(&input);
    let decoded: Vec<f32> = encoded.iter().take(input.len()).collect();

    assert_eq!(decoded[0], 1.0, "NaN should encode as 1 in Binary");
    assert_eq!(decoded[1], 0.0, "+0.0 should encode as 0 in Binary");
    assert_eq!(decoded[2], 0.0, "-0.0 should encode as 0 in Binary");
    assert_eq!(decoded[3], 1.0, "+Inf should encode as 1 in Binary");
    assert_eq!(decoded[4], 0.0, "-Inf should encode as 0 in Binary");
}

#[test]
fn binary_quantized_encoding_edge_values() {
    // BinaryQuantized uses: is_sign_positive() → 1 (decoded as 1.0), else 0 (decoded as -1.0)
    // +0.0: is_sign_positive = true → encodes as 1, decodes as 1.0
    // -0.0: is_sign_positive = false → encodes as 0, decodes as -1.0
    // NaN (positive): is_sign_positive = true → encodes as 1, decodes as 1.0
    // +Inf: is_sign_positive = true → encodes as 1, decodes as 1.0
    // -Inf: is_sign_positive = false → encodes as 0, decodes as -1.0
    let input = [0.0_f32, -0.0, f32::NAN, f32::INFINITY, f32::NEG_INFINITY];
    let encoded = UnalignedVector::<BinaryQuantized>::from_slice(&input);
    let decoded: Vec<f32> = encoded.iter().take(input.len()).collect();

    assert_eq!(decoded[0], 1.0, "+0.0 should encode as 1 in BQ");
    assert_eq!(decoded[1], -1.0, "-0.0 should encode as 0 in BQ");
    assert_eq!(decoded[2], 1.0, "NaN should encode as 1 in BQ");
    assert_eq!(decoded[3], 1.0, "+Inf should encode as 1 in BQ");
    assert_eq!(decoded[4], -1.0, "-Inf should encode as 0 in BQ");
}

// ── D. is_zero tests ────────────────────────────────────────────────────

#[test]
fn f32_is_zero() {
    // All zeros
    let v = UnalignedVector::<f32>::from_slice(&[0.0, 0.0, 0.0]);
    assert!(v.is_zero());

    // -0.0 treated as zero (IEEE 754: -0.0 == 0.0)
    let v = UnalignedVector::<f32>::from_slice(&[-0.0, 0.0, -0.0]);
    assert!(v.is_zero());

    // Non-zero
    let v = UnalignedVector::<f32>::from_slice(&[0.0, 1.0, 0.0]);
    assert!(!v.is_zero());

    // Empty
    let v = UnalignedVector::<f32>::from_slice(&[]);
    assert!(v.is_zero());
}

#[test]
fn binary_is_zero() {
    // All-zero input (+0.0): Binary encodes +0.0 as 0 → all zero bytes → is_zero
    let v = UnalignedVector::<Binary>::from_slice(&[0.0; 64]);
    assert!(v.is_zero(), "all +0.0 should be zero in Binary");

    // All-negative input: Binary encodes negatives as 0 → all zero bytes → is_zero
    let v = UnalignedVector::<Binary>::from_slice(&[-1.0; 64]);
    assert!(v.is_zero(), "all negatives should be zero in Binary");

    // One positive → not zero
    let mut input = [-1.0_f32; 64];
    input[0] = 1.0;
    let v = UnalignedVector::<Binary>::from_slice(&input);
    assert!(!v.is_zero(), "one positive should make it non-zero");
}

#[test]
fn binary_quantized_is_zero() {
    // All-negative = zero bytes (BQ encodes negatives as 0)
    let v = UnalignedVector::<BinaryQuantized>::from_slice(&[-1.0; 64]);
    assert!(v.is_zero(), "all negatives should be zero in BQ");

    // All-positive = non-zero
    let v = UnalignedVector::<BinaryQuantized>::from_slice(&[1.0; 64]);
    assert!(!v.is_zero(), "all positives should be non-zero in BQ");

    // All +0.0 = non-zero (BQ encodes +0.0 as 1 via is_sign_positive)
    let v = UnalignedVector::<BinaryQuantized>::from_slice(&[0.0; 64]);
    assert!(!v.is_zero(), "+0.0 encodes as 1 in BQ, so not zero");
}

// ── E. is_empty tests ──────────────────────────────────────────────────

#[test]
fn is_empty_all_codecs() {
    let f32_v = UnalignedVector::<f32>::from_bytes(&[]).unwrap();
    assert!(f32_v.is_empty());

    let bin_v = UnalignedVector::<Binary>::from_bytes(&[]).unwrap();
    assert!(bin_v.is_empty());

    let bq_v = UnalignedVector::<BinaryQuantized>::from_bytes(&[]).unwrap();
    assert!(bq_v.is_empty());
}

// ── F. Word boundary tests (Binary + BinaryQuantized) ───────────────────

#[test]
fn binary_word_boundaries() {
    // 63 elements: sub-word, padded to 1 word (8 bytes)
    let input_63: Vec<f32> = (0..63)
        .map(|i| if i % 2 == 0 { 1.0 } else { -1.0 })
        .collect();
    let v = UnalignedVector::<Binary>::from_slice(&input_63);
    assert_eq!(v.as_bytes().len(), 8, "63 elements → 1 word = 8 bytes");
    assert_eq!(v.len(), 64, "len reports full word capacity");
    // Padding bit (index 63) should be 0
    let decoded = v.to_vec();
    assert_eq!(decoded[63], 0.0, "padding bit should decode as 0");

    // 64 elements: exact word boundary
    let input_64: Vec<f32> = (0..64)
        .map(|i| if i % 2 == 0 { 1.0 } else { -1.0 })
        .collect();
    let v = UnalignedVector::<Binary>::from_slice(&input_64);
    assert_eq!(v.as_bytes().len(), 8);
    assert_eq!(v.len(), 64);

    // 65 elements: two words
    let input_65: Vec<f32> = (0..65)
        .map(|i| if i % 2 == 0 { 1.0 } else { -1.0 })
        .collect();
    let v = UnalignedVector::<Binary>::from_slice(&input_65);
    assert_eq!(v.as_bytes().len(), 16, "65 elements → 2 words = 16 bytes");
    assert_eq!(v.len(), 128);
    // Elements 65..128 should be padding zeros
    let decoded = v.to_vec();
    for (i, value) in decoded.iter().enumerate().take(128).skip(65) {
        assert_eq!(*value, 0.0, "padding at index {i} should be 0");
    }

    // 128 elements: exact two words
    let input_128: Vec<f32> = (0..128)
        .map(|i| if i % 2 == 0 { 1.0 } else { -1.0 })
        .collect();
    let v = UnalignedVector::<Binary>::from_slice(&input_128);
    assert_eq!(v.as_bytes().len(), 16);
    assert_eq!(v.len(), 128);
}

#[test]
fn binary_quantized_word_boundaries() {
    // 63 elements: padded to 1 word
    let input_63: Vec<f32> = (0..63)
        .map(|i| if i % 2 == 0 { 1.0 } else { -1.0 })
        .collect();
    let v = UnalignedVector::<BinaryQuantized>::from_slice(&input_63);
    assert_eq!(v.as_bytes().len(), 8);
    assert_eq!(v.len(), 64);
    // Padding bit decodes as -1.0 (bit=0 → -1.0 in BQ)
    let decoded = v.to_vec();
    assert_eq!(decoded[63], -1.0, "padding bit should decode as -1.0 in BQ");

    // 65 elements: two words
    let input_65: Vec<f32> = (0..65)
        .map(|i| if i % 2 == 0 { 1.0 } else { -1.0 })
        .collect();
    let v = UnalignedVector::<BinaryQuantized>::from_slice(&input_65);
    assert_eq!(v.as_bytes().len(), 16);
    assert_eq!(v.len(), 128);
    let decoded = v.to_vec();
    for (i, value) in decoded.iter().enumerate().take(128).skip(65) {
        assert_eq!(*value, -1.0, "padding at index {i} should be -1.0 in BQ");
    }
}

// ── G. from_vec equivalence ─────────────────────────────────────────────

#[test]
fn from_vec_equals_from_slice() {
    let data = vec![1.0_f32, -2.0, 3.0, 0.0, -0.5];

    // f32
    let from_slice = f32::from_slice(&data);
    let from_vec = f32::from_vec(data.clone());
    assert_eq!(from_slice.as_bytes(), from_vec.as_bytes());

    // Binary
    let from_slice = Binary::from_slice(&data);
    let from_vec = Binary::from_vec(data.clone());
    assert_eq!(from_slice.as_bytes(), from_vec.as_bytes());

    // BinaryQuantized
    let from_slice = BinaryQuantized::from_slice(&data);
    let from_vec = BinaryQuantized::from_vec(data.clone());
    assert_eq!(from_slice.as_bytes(), from_vec.as_bytes());
}

// ── H. ToOwned / Borrow roundtrip ──────────────────────────────────────

#[test]
fn to_owned_borrow_roundtrip_f32() {
    let original = vec![1.0_f32, -2.5, core::f32::consts::PI, 0.0];
    let encoded = UnalignedVector::<f32>::from_slice(&original);
    let owned: Vec<u8> = (*encoded).to_owned();
    let borrowed: &UnalignedVector<f32> = owned.borrow();
    assert_eq!(encoded.to_vec(), borrowed.to_vec());
}

#[test]
fn to_owned_borrow_roundtrip_binary() {
    let original: Vec<f32> = (0..100)
        .map(|i| if i % 3 == 0 { -1.0 } else { 1.0 })
        .collect();
    let encoded = UnalignedVector::<Binary>::from_slice(&original);
    let owned: Vec<u8> = (*encoded).to_owned();
    let borrowed: &UnalignedVector<Binary> = owned.borrow();
    assert_eq!(encoded.to_vec(), borrowed.to_vec());
}

#[test]
fn to_owned_borrow_roundtrip_binary_quantized() {
    let original: Vec<f32> = (0..100)
        .map(|i| if i % 3 == 0 { -1.0 } else { 1.0 })
        .collect();
    let encoded = UnalignedVector::<BinaryQuantized>::from_slice(&original);
    let owned: Vec<u8> = (*encoded).to_owned();
    let borrowed: &UnalignedVector<BinaryQuantized> = owned.borrow();
    assert_eq!(encoded.to_vec(), borrowed.to_vec());
}

// ── I. word_size() values ───────────────────────────────────────────────

#[test]
fn word_size_values() {
    assert_eq!(f32::word_size(), 1);
    assert_eq!(Binary::word_size(), 64);
    assert_eq!(BinaryQuantized::word_size(), 64);
}

// ── J. Debug formatting ────────────────────────────────────────────────

#[test]
fn debug_short_vector() {
    let v = UnalignedVector::<f32>::from_slice(&[1.0, 2.0, 3.0]);
    let dbg = format!("{v:?}");
    assert!(dbg.contains("1.0000"));
    assert!(dbg.contains("2.0000"));
    assert!(dbg.contains("3.0000"));
    assert!(!dbg.contains("..."));
}

#[test]
fn debug_long_vector_trailing_zeros() {
    // 12 elements: first 10 non-zero, then zeros
    let mut data = vec![1.0_f32; 10];
    data.extend_from_slice(&[0.0; 54]); // pad to fill a word for Binary
    let v = UnalignedVector::<f32>::from_slice(&data);
    let dbg = format!("{v:?}");
    assert!(
        dbg.contains("0.0, ..."),
        "expected trailing zero marker, got: {dbg}"
    );
}

#[test]
fn debug_long_vector_nonzero_tail() {
    let mut data = vec![1.0_f32; 10];
    data.extend_from_slice(&[5.0; 5]);
    let v = UnalignedVector::<f32>::from_slice(&data);
    let dbg = format!("{v:?}");
    assert!(
        dbg.contains("other ..."),
        "expected 'other ...' marker, got: {dbg}"
    );
}

// ── K. Property-based tests ────────────────────────────────────────────

proptest! {
    #[test]
    fn f32_roundtrip_arbitrary(
        original in vec(proptest::num::f32::ANY, 0..128)
    ) {
        let encoded = UnalignedVector::<f32>::from_slice(&original);
        let decoded = encoded.to_vec();
        assert_eq!(original.len(), decoded.len());
        for (orig, dec) in original.iter().zip(&decoded) {
            assert_eq!(orig.to_bits(), dec.to_bits(), "bitwise mismatch");
        }
    }

    #[test]
    fn f32_iter_equals_to_vec(
        original in vec(proptest::num::f32::ANY, 0..128)
    ) {
        let encoded = UnalignedVector::<f32>::from_slice(&original);
        let from_iter: Vec<f32> = encoded.iter().collect();
        let from_to_vec = encoded.to_vec();
        assert_eq!(from_iter.len(), from_to_vec.len());
        for (a, b) in from_iter.iter().zip(&from_to_vec) {
            assert_eq!(a.to_bits(), b.to_bits());
        }
    }

    #[test]
    fn binary_from_vec_equals_from_slice(
        data in vec(-50f32..=50.0, 0..256)
    ) {
        let from_slice = Binary::from_slice(&data);
        let from_vec = Binary::from_vec(data.clone());
        assert_eq!(from_slice.as_bytes(), from_vec.as_bytes());
    }

    #[test]
    fn bq_from_vec_equals_from_slice(
        data in vec(-50f32..=50.0, 0..256)
    ) {
        let from_slice = BinaryQuantized::from_slice(&data);
        let from_vec = BinaryQuantized::from_vec(data.clone());
        assert_eq!(from_slice.as_bytes(), from_vec.as_bytes());
    }

    #[test]
    fn binary_to_owned_borrow_roundtrip(
        data in vec(-50f32..=50.0, 0..256)
    ) {
        let encoded = UnalignedVector::<Binary>::from_slice(&data);
        let owned: Vec<u8> = (*encoded).to_owned();
        let borrowed: &UnalignedVector<Binary> = owned.borrow();
        assert_eq!(encoded.to_vec(), borrowed.to_vec());
    }

    #[test]
    fn f32_from_bytes_from_slice_equivalence(
        original in vec(proptest::num::f32::ANY, 0..128)
    ) {
        let from_slice = UnalignedVector::<f32>::from_slice(&original);
        let bytes = from_slice.as_bytes();
        let from_bytes = UnalignedVector::<f32>::from_bytes(bytes).unwrap();
        assert_eq!(from_slice.to_vec().len(), from_bytes.to_vec().len());
        for (a, b) in from_slice.to_vec().iter().zip(&from_bytes.to_vec()) {
            assert_eq!(a.to_bits(), b.to_bits());
        }
    }

    #[test]
    fn binary_len_matches_iter_count(
        data in vec(-50f32..=50.0, 0..256)
    ) {
        let encoded = UnalignedVector::<Binary>::from_slice(&data);
        assert_eq!(encoded.len(), encoded.iter().count());
    }

    #[test]
    fn bq_len_matches_iter_count(
        data in vec(-50f32..=50.0, 0..256)
    ) {
        let encoded = UnalignedVector::<BinaryQuantized>::from_slice(&data);
        assert_eq!(encoded.len(), encoded.iter().count());
    }
}
