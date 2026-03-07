use crate::helix_engine::vector_core::vector_distance::{
    cosine_similarity, MAX_DISTANCE, MIN_DISTANCE, ORTHOGONAL,
};

use crate::helix_engine::vector_core::vector::HVector;
use bumpalo::Bump;

fn alloc_vector<'a>(arena: &'a Bump, data: &[f64]) -> HVector<'a> {
    let slice = arena.alloc_slice_copy(data);
    HVector::from_slice("vector", 0, slice)
}

#[test]
fn test_hvector_from_slice() {
    let arena = Bump::new();
    let vector = alloc_vector(&arena, &[1.0, 2.0, 3.0]);
    assert_eq!(vector.data, &[1.0, 2.0, 3.0]);
}

#[test]
fn test_hvector_distance_orthogonal() {
    let arena = Bump::new();
    let v1 = alloc_vector(&arena, &[1.0, 0.0]);
    let v2 = alloc_vector(&arena, &[0.0, 1.0]);
    let distance = v1.distance_to(&v2).unwrap();
    assert_eq!(distance, ORTHOGONAL);
}

#[test]
fn test_hvector_distance_min() {
    let arena = Bump::new();
    let v1 = alloc_vector(&arena, &[1.0, 2.0, 3.0]);
    let v2 = alloc_vector(&arena, &[1.0, 2.0, 3.0]);
    let distance = v2.distance_to(&v1).unwrap();
    assert_eq!(distance, MIN_DISTANCE);
}

#[test]
fn test_hvector_distance_max() {
    let arena = Bump::new();
    let v1 = alloc_vector(&arena, &[-1.0, -2.0, -3.0]);
    let v2 = alloc_vector(&arena, &[1.0, 2.0, 3.0]);
    let distance = v1.distance_to(&v2).unwrap();
    assert_eq!(distance, MAX_DISTANCE);
}

#[test]
fn test_hvector_len() {
    let arena = Bump::new();
    let vector = alloc_vector(&arena, &[1.0, 2.0, 3.0, 4.0]);
    assert_eq!(vector.len(), 4);
}

#[test]
fn test_hvector_is_empty() {
    let arena = Bump::new();
    let empty_vector = alloc_vector(&arena, &[]);
    let arena2 = Bump::new();
    let non_empty_vector = alloc_vector(&arena2, &[1.0, 2.0]);

    assert!(empty_vector.is_empty());
    assert!(!non_empty_vector.is_empty());
}

#[test]
#[should_panic]
fn test_hvector_distance_different_dimensions() {
    let arena = Bump::new();
    let v1 = alloc_vector(&arena, &[1.0, 2.0, 3.0]);
    let arena2 = Bump::new();
    let v2 = alloc_vector(&arena2, &[1.0, 2.0, 3.0, 4.0]);
    let _ = v1.distance_to(&v2).unwrap();
}

#[test]
fn test_hvector_large_values() {
    let arena = Bump::new();
    let v1 = alloc_vector(&arena, &[1e6, 2e6]);
    let arena2 = Bump::new();
    let v2 = alloc_vector(&arena2, &[1e6, 2e6]);
    let distance = v1.distance_to(&v2).unwrap();
    assert!(distance.abs() < 1e-10);
}

#[test]
fn test_hvector_negative_values() {
    let arena = Bump::new();
    let v1 = alloc_vector(&arena, &[-1.0, -2.0]);
    let arena2 = Bump::new();
    let v2 = alloc_vector(&arena2, &[1.0, 2.0]);
    let distance = v1.distance_to(&v2).unwrap();
    assert_eq!(distance.round(), MAX_DISTANCE);
}

#[test]
fn test_hvector_cosine_similarity() {
    let arena = Bump::new();
    let v1 = alloc_vector(&arena, &[1.0, 2.0, 3.0]);
    let arena2 = Bump::new();
    let v2 = alloc_vector(&arena2, &[4.0, 5.0, 6.0]);
    let similarity = v1.distance_to(&v2).unwrap();
    assert!((similarity - (1.0 - 0.9746318461970762)).abs() < 1e-9);
}

#[test]
fn test_cosine_similarity_zero_vector_returns_error() {
    let result = cosine_similarity(&[0.0, 0.0, 0.0], &[1.0, 2.0, 3.0]);
    assert!(result.is_err());
}

#[test]
fn test_cosine_similarity_both_zero_vectors_returns_error() {
    let result = cosine_similarity(&[0.0, 0.0], &[0.0, 0.0]);
    assert!(result.is_err());
}

#[test]
fn test_cosine_similarity_empty_vectors_returns_error() {
    let result = cosine_similarity(&[], &[]);
    assert!(result.is_err());
}

#[test]
fn test_cosine_similarity_one_empty_vector_returns_error() {
    let result = cosine_similarity(&[], &[1.0, 2.0]);
    assert!(result.is_err());
}

#[test]
fn test_cosine_similarity_dimension_mismatch_returns_error() {
    let result = cosine_similarity(&[1.0, 2.0], &[1.0, 2.0, 3.0]);
    assert!(result.is_err());
}

#[test]
fn test_cosine_similarity_identical_vectors() {
    let result = cosine_similarity(&[1.0, 2.0, 3.0], &[1.0, 2.0, 3.0]);
    assert!((result.unwrap() - 1.0).abs() < 1e-10);
}

#[test]
fn test_cosine_similarity_opposite_vectors() {
    let result = cosine_similarity(&[1.0, 2.0, 3.0], &[-1.0, -2.0, -3.0]);
    assert!((result.unwrap() - (-1.0)).abs() < 1e-10);
}

#[test]
fn test_cosine_similarity_orthogonal_vectors() {
    let result = cosine_similarity(&[1.0, 0.0], &[0.0, 1.0]);
    assert!(result.unwrap().abs() < 1e-10);
}

#[test]
fn test_cosine_similarity_single_element() {
    let result = cosine_similarity(&[5.0], &[3.0]);
    assert!((result.unwrap() - 1.0).abs() < 1e-10);
}

#[test]
fn test_cosine_similarity_large_dimensions() {
    let a: Vec<f64> = (0..1024).map(|i| (i as f64).sin()).collect();
    let b: Vec<f64> = (0..1024).map(|i| (i as f64).cos()).collect();
    let result = cosine_similarity(&a, &b);
    assert!(result.is_ok());
    let sim = result.unwrap();
    assert!(sim >= -1.0 && sim <= 1.0);
}

#[test]
fn test_hvector_distance_zero_vector_returns_error() {
    let arena = Bump::new();
    let v1 = alloc_vector(&arena, &[0.0, 0.0]);
    let v2 = alloc_vector(&arena, &[3.0, 4.0]);
    assert!(v1.distance_to(&v2).is_err());
}

#[test]
fn test_cosine_similarity_near_zero_magnitude_returns_error() {
    let tiny = f64::EPSILON * 0.1;
    let result = cosine_similarity(&[tiny, 0.0], &[1.0, 2.0]);
    assert!(result.is_err());
}
