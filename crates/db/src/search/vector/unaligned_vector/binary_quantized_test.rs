#![allow(clippy::format_collect)]

use insta::{assert_debug_snapshot, assert_snapshot};
use proptest::collection::vec;
use proptest::prelude::*;

use super::binary_quantized::{from_slice_non_optimized, to_vec_non_optimized};
use crate::search::vector::unaligned_vector::{
    BinaryQuantized, UnalignedVector, UnalignedVectorCodec,
};

#[test]
fn test_from_slice() {
    let original = [0.1, 0.2, -0.3, 0.4, -0.5, 0.6, -0.7, 0.8, -0.9];
    let vector = BinaryQuantized::from_slice(&original);

    let internal = vector
        .as_bytes()
        .iter()
        .map(|b| format!("{b:08b}\n"))
        .collect::<String>();
    assert_snapshot!(internal, @r###"
        10101011
        00000000
        00000000
        00000000
        00000000
        00000000
        00000000
        00000000
        "###);
}

#[test]
fn test_to_vec_iter() {
    let original = [0.1, 0.2, -0.3, 0.4, -0.5, 0.6, -0.7, 0.8, -0.9];
    let vector = BinaryQuantized::from_slice(&original);
    let iter_vec: Vec<_> = BinaryQuantized::iter(&vector)
        .take(original.len())
        .collect();
    assert_debug_snapshot!(iter_vec, @r###"
        [
            1.0,
            1.0,
            -1.0,
            1.0,
            -1.0,
            1.0,
            -1.0,
            1.0,
            -1.0,
        ]
        "###);
    let mut vec_vec: Vec<_> = BinaryQuantized::to_vec(&vector);
    vec_vec.truncate(original.len());
    assert_debug_snapshot!(vec_vec, @r###"
        [
            1.0,
            1.0,
            -1.0,
            1.0,
            -1.0,
            1.0,
            -1.0,
            1.0,
            -1.0,
        ]
        "###);

    assert_eq!(vec_vec, iter_vec);
}

#[test]
fn unaligned_f32_vec() {
    let original: Vec<f32> = vec![1.0, 2.0, 3.0, 4.0, 5.0];
    let bytes: Vec<u8> = original.iter().flat_map(|f| f.to_ne_bytes()).collect();

    let unaligned_owned_from_f32 = UnalignedVector::<f32>::from_vec(original.clone());
    assert_eq!(bytes, unaligned_owned_from_f32.as_bytes());

    let unchecked_unaligned_owned_from_bytes = UnalignedVector::<f32>::from_bytes_unchecked(&bytes);
    assert_eq!(bytes, unchecked_unaligned_owned_from_bytes.as_bytes());

    let unaligned_owned_from_bytes = UnalignedVector::<f32>::from_bytes(&bytes).unwrap();
    assert_eq!(bytes, unaligned_owned_from_bytes.as_bytes());
}

#[test]
fn unaligned_binary_quantized_iter_size() {
    let original: Vec<f32> = vec![-1.0, 2.0, -3.0, 4.0, 5.0];
    let unaligned = UnalignedVector::<BinaryQuantized>::from_slice(&original);
    assert_snapshot!(unaligned.len(), @"64");
    let mut iter = unaligned.iter();
    assert_snapshot!(iter.len(), @"64");
    iter.next().unwrap();
    assert_snapshot!(iter.len(), @"63");
    iter.by_ref().take(10).for_each(drop);
    assert_snapshot!(iter.len(), @"53");
    iter.by_ref().take(52).for_each(drop);
    assert_snapshot!(iter.len(), @"1");
    iter.next().unwrap();
    assert_snapshot!(iter.len(), @"0");
    iter.next();
    assert_snapshot!(iter.len(), @"0");
}

#[test]
fn unaligned_binary_quantized_smol() {
    let original: Vec<f32> = vec![-1.0, 2.0, -3.0, 4.0, 5.0];

    let unaligned = UnalignedVector::<BinaryQuantized>::from_slice(&original);
    let s = unaligned
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:08b}\n"))
        .collect::<String>();
    assert_snapshot!(s, @r###"
    00011010
    00000000
    00000000
    00000000
    00000000
    00000000
    00000000
    00000000
    "###);

    let deser: Vec<_> = unaligned.iter().collect();
    assert_debug_snapshot!(deser[0..original.len()], @r###"
    [
        -1.0,
        1.0,
        -1.0,
        1.0,
        1.0,
    ]
    "###);
}

#[test]
fn unaligned_binary_quantized_large() {
    let original: Vec<f32> = (0..100)
        .map(|n| if n % 3 == 0 || n % 5 == 0 { -1.0 } else { 1.0 })
        .collect();

    // Two numbers should be used
    let unaligned = UnalignedVector::<BinaryQuantized>::from_slice(&original);
    let s = unaligned
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:08b}\n"))
        .collect::<String>();
    assert_snapshot!(s, @r###"
    10010110
    01101001
    11001011
    10110100
    01100101
    11011010
    00110010
    01101101
    10011001
    10110110
    01001100
    01011011
    00000110
    00000000
    00000000
    00000000
    "###);

    let deser: Vec<_> = unaligned.to_vec();
    assert_snapshot!(format!("{:?}", &deser[0..original.len()]),
    @"[-1.0, 1.0, 1.0, -1.0, 1.0, -1.0, -1.0, 1.0, 1.0, -1.0, -1.0, 1.0, -1.0, 1.0, 1.0, -1.0, 1.0, 1.0, -1.0, 1.0, -1.0, -1.0, 1.0, 1.0, -1.0, -1.0, 1.0, -1.0, 1.0, 1.0, -1.0, 1.0, 1.0, -1.0, 1.0, -1.0, -1.0, 1.0, 1.0, -1.0, -1.0, 1.0, -1.0, 1.0, 1.0, -1.0, 1.0, 1.0, -1.0, 1.0, -1.0, -1.0, 1.0, 1.0, -1.0, -1.0, 1.0, -1.0, 1.0, 1.0, -1.0, 1.0, 1.0, -1.0, 1.0, -1.0, -1.0, 1.0, 1.0, -1.0, -1.0, 1.0, -1.0, 1.0, 1.0, -1.0, 1.0, 1.0, -1.0, 1.0, -1.0, -1.0, 1.0, 1.0, -1.0, -1.0, 1.0, -1.0, 1.0, 1.0, -1.0, 1.0, 1.0, -1.0, 1.0, -1.0, -1.0, 1.0, 1.0, -1.0]");
    //[   0,   1,   2,    3,   4,    5,    6,   7,   8,    9,   10,  11,   12,  13,  14,   15, ...
    for (orig, deser) in original.iter().zip(&deser) {
        if orig.is_sign_positive() {
            assert_eq!(deser, &1.0, "Expected 1 but found {deser}");
        } else {
            assert_eq!(deser, &-1.0, "Expected -1 but found {deser}");
        }
    }
}

proptest! {
    #[test]
    fn from_slice_simd_vs_non_optimized(
        original in vec(-50f32..=50.2, 0..516)
    ){
        let vector = BinaryQuantized::from_slice(&original);
        let iter_vec: Vec<_> = to_vec_non_optimized(&vector);
        let vec_vec: Vec<_> = BinaryQuantized::to_vec(&vector);

        assert_eq!(vec_vec, iter_vec);
    }

    #[test]
    fn to_vec_simd_vs_non_optimized(
        original in vec(-50f32..=50.2, 0..516)
    ){
        let vector1 = BinaryQuantized::from_slice(&original);
        let vector2 = from_slice_non_optimized(&original);

        assert_eq!(vector1.as_bytes(), &vector2);
    }
}
