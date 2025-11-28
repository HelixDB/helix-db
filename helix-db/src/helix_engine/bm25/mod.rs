#[cfg(feature = "lmdb")]
pub mod lmdb_bm25;
#[cfg(feature = "rocks")]
pub mod rocks_bm25;
#[cfg(feature = "slate")]
pub mod slate_bm25;

#[cfg(feature = "lmdb")]
pub use lmdb_bm25::{BM25, BM25Flatten, BM25Metadata, HBM25Config, HybridSearch, METADATA_KEY};
#[cfg(feature = "rocks")]
pub use rocks_bm25::{BM25, BM25Flatten, BM25Metadata, HBM25Config, HybridSearch, METADATA_KEY};
#[cfg(feature = "slate")]
pub use slate_bm25::{BM25, BM25Flatten, BM25Metadata, HBM25Config, HybridSearch, METADATA_KEY};

#[cfg(test)]
pub mod bm25_tests;
