use crate::helix_engine::vector_core::vector_distance::{MAX_DISTANCE, MIN_DISTANCE, ORTHOGONAL};

use crate::helix_engine::vector_core::VectorData;
use crate::helix_engine::vector_core::vector::HVector;

#[test]
fn test_hvector_new() {
    let data: Vec<f64> = vec![1.0, 2.0, 3.0];
    let vector = HVector::new(VectorData::F64(data));
    assert_eq!(vector.get_data(), &VectorData::F64(vec![1.0, 2.0, 3.0]));
}

#[test]
fn test_hvector_from_slice() {
    let data: Vec<f64> = vec![1.0, 2.0, 3.0];
    let vector = HVector::from_slice(0, VectorData::F64(data));
    assert_eq!(vector.get_data(), &VectorData::F64(vec![1.0, 2.0, 3.0]));
}

#[test]
fn test_hvector_distance_orthogonal() {
    let v1 = HVector::new(VectorData::F64(vec![1.0, 0.0]));
    let v2 = HVector::new(VectorData::F64(vec![0.0, 1.0]));
    let distance = v1.distance_to(&v2).unwrap();
    assert!(distance == ORTHOGONAL);
}

#[test]
fn test_hvector_distance_min() {
    let v1 = HVector::new(VectorData::F64(vec![1.0, 2.0, 3.0]));
    let v2 = HVector::new(VectorData::F64(vec![1.0, 2.0, 3.0]));
    let distance = v2.distance_to(&v1).unwrap();
    assert!(distance.abs() == MIN_DISTANCE);
}

#[test]
fn test_hvector_distance_max() {
    let v1 = HVector::new(VectorData::F64(vec![0.0, 0.0]));
    let v2 = HVector::new(VectorData::F64(vec![3.0, 4.0]));
    let distance = v1.distance_to(&v2).unwrap();
    assert!(distance == MAX_DISTANCE);
}

#[test]
fn test_bytes_roundtrip() {
    let original = HVector::new(VectorData::F64(vec![1.0, 2.0, 3.0]));
    let bytes = original.to_bytes();
    let reconstructed = HVector::from_bytes(original.get_id(), 0, &bytes).unwrap();
    assert_eq!(original.get_data(), reconstructed.get_data());
}

#[test]
fn test_hvector_len() {
    let data = vec![1.0, 2.0, 3.0, 4.0];
    let vector = HVector::new(VectorData::F64(data));
    assert_eq!(vector.len(), 4);
}

#[test]
fn test_hvector_is_empty() {
    let empty_vector = HVector::new(VectorData::F64(vec![]));
    let non_empty_vector = HVector::new(VectorData::F64(vec![1.0, 2.0]));

    assert!(empty_vector.is_empty());
    assert!(!non_empty_vector.is_empty());
}

#[test]
#[should_panic]
fn test_hvector_distance_different_dimensions() {
    let v1 = HVector::new(VectorData::F64(vec![1.0, 2.0, 3.0]));
    let v2 = HVector::new(VectorData::F64(vec![1.0, 2.0, 3.0, 4.0]));
    let distance = v1.distance_to(&v2).unwrap();
    println!("distance: {distance}");
    assert!(distance.is_finite());
}

#[test]
fn test_hvector_large_values() {
    let v1 = HVector::new(VectorData::F64(vec![1e6, 2e6]));
    let v2 = HVector::new(VectorData::F64(vec![1e6, 2e6]));
    let distance = v1.distance_to(&v2).unwrap();
    assert!(distance.abs() < 1e-10);
}

#[test]
fn test_hvector_negative_values() {
    let v1 = HVector::new(VectorData::F64(vec![-1.0, -2.0]));
    let v2 = HVector::new(VectorData::F64(vec![1.0, 2.0]));
    let distance = v1.distance_to(&v2).unwrap();
    // used round to avoid floating point precision issues
    assert!(distance.round() == MAX_DISTANCE);
}

#[test]
fn test_hvector_cosine_similarity() {
    let v1 = HVector::new(VectorData::F64(vec![1.0, 2.0, 3.0]));
    let v2 = HVector::new(VectorData::F64(vec![4.0, 5.0, 6.0]));
    let similarity = v1.distance_to(&v2).unwrap();
    assert!(similarity == 1.0 - 0.9746318461970762);
}
