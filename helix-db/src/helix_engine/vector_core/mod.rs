pub mod vector;
pub mod vector_without_data;

#[cfg(feature = "rocks")]
pub mod rocks;
#[cfg(feature = "rocks")]
pub use rocks::{
    hnsw::HNSW,
    vector_core::{HNSWConfig, VectorCore},
    vector_distance::{self, DistanceCalc},
};

#[cfg(feature = "lmdb")]
pub mod lmdb;
#[cfg(feature = "lmdb")]
pub use lmdb::{
    hnsw::HNSW,
    vector_core::{ENTRY_POINT_KEY, HNSWConfig, VectorCore},
    vector_distance::{self, DistanceCalc},
};
