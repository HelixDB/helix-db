use crate::helix_engine::vector_core::{
    HVector,
    distance::{MAX_DISTANCE, MIN_DISTANCE, ORTHOGONAL},
};
use bumpalo::Bump;

fn alloc_vector<'a>(arena: &'a Bump, data: &[f32]) -> HVector<'a> {
    let mut bump_vec = bumpalo::collections::Vec::new_in(arena);
    bump_vec.extend_from_slice(data);
    HVector::from_vec("test_vector", bump_vec)
}

#[test]
fn test_hvector_from_slice() {
    let arena = Bump::new();
    let vector = alloc_vector(&arena, &[1.0, 2.0, 3.0]);
    assert_eq!(vector.data_borrowed(), &[1.0, 2.0, 3.0]);
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
    let v1 = alloc_vector(&arena, &[0.0, 0.0]);
    let v2 = alloc_vector(&arena, &[3.0, 4.0]);
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
