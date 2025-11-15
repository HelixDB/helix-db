// #[cfg(feature = "lmdb")]
pub mod lmdb_bm25;
#[cfg(feature = "rocks")]
pub mod rocks_bm25;

#[cfg(feature = "lmdb")]
pub use lmdb_bm25::HBM25Config;
#[cfg(feature = "rocks")]
pub use rocks_bm25::HBM25Config;

#[cfg(feature = "lmdb")]
pub use lmdb_bm25::BM25;
#[cfg(feature = "rocks")]
pub use rocks_bm25::BM25;

#[cfg(test)]
pub mod bm25_tests;
