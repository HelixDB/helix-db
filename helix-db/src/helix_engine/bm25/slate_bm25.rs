use crate::{
    debug_println,
    helix_engine::{
        slate_utils::SlateUtils,
        storage_core::{HelixGraphStorage, TableIndex},
        traversal_core::WTxn,
        types::GraphError,
    },
    utils::properties::ImmutablePropertiesMap,
};

use bytes::BytesMut;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};

pub const METADATA_KEY: &[u8] = b"metadata";

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct BM25Metadata {
    pub total_docs: u64,
    pub avgdl: f64,
    pub k1: f32, // controls term frequency saturation
    pub b: f32,  // controls document length normalization
}

/// For inverted index
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PostingListEntry {
    pub doc_id: u128,
    pub term_frequency: u32,
}

pub struct HBM25 {
    pub graph_env: Arc<slatedb::Db>,
    k1: f64,
    b: f64,
}

/// Type alias for compatibility with other backends
pub type HBM25Config = HBM25;

/// BM25 marker trait for Slate
/// Note: Slate uses async methods, so this is a marker trait for compatibility
pub trait BM25 {}

impl<'db> HBM25 {
    pub fn new(graph_env: Arc<slatedb::Db>) -> Result<HBM25, GraphError> {
        Ok(HBM25 {
            graph_env,
            k1: 1.2,
            b: 0.75,
        })
    }

    pub fn new_temp(
        graph_env: Arc<slatedb::Db>,
        _wtxn: &mut WTxn<'_>,
        _uuid: &str,
    ) -> Result<HBM25, GraphError> {
        Ok(HBM25 {
            graph_env,
            k1: 1.2,
            b: 0.75,
        })
    }
    #[inline(always)]
    fn doc_lengths_key(doc_lengths: &[u8]) -> BytesMut {
        let table_index = TableIndex::DocLengths.as_bytes();
        // consider using arena here
        let mut bytes = BytesMut::with_capacity(table_index.len() + doc_lengths.len());
        bytes.extend_from_slice(table_index);
        bytes.extend_from_slice(doc_lengths);
        bytes
    }
    #[inline(always)]
    fn inverted_index_key(index_key: &[u8]) -> BytesMut {
        let table_index = TableIndex::InvertedIndex.as_bytes();
        // consider using arena here
        let mut bytes = BytesMut::with_capacity(table_index.len() + index_key.len());
        bytes.extend_from_slice(table_index);
        bytes.extend_from_slice(index_key);
        bytes
    }
    #[inline(always)]
    fn term_frequency_key(term: &[u8]) -> BytesMut {
        let table_index = TableIndex::TermFrequencies.as_bytes();
        // consider using arena here
        let mut bytes = BytesMut::with_capacity(table_index.len() + term.len());
        bytes.extend_from_slice(table_index);
        bytes.extend_from_slice(term);
        bytes
    }
    #[inline(always)]
    fn metadata_key() -> [u8; 10] {
        let table_index = TableIndex::Metadata.to_bytes();
        let mut bytes = [0u8; 10];
        bytes[..table_index.len()].copy_from_slice(&table_index);
        bytes[table_index.len()..].copy_from_slice(METADATA_KEY);
        bytes
    }

    /// Converts text to lowercase, removes non-alphanumeric chars, splits into words
    fn tokenize<const SHOULD_FILTER: bool>(&self, text: &str) -> Vec<String> {
        text.to_lowercase()
            .split(|c: char| !c.is_alphanumeric())
            .filter(|s| !s.is_empty())
            .filter_map(|s| (!SHOULD_FILTER || s.len() > 2).then_some(s.to_string()))
            .collect()
    }

    /// Inserts needed information into doc_lengths_db, inverted_index_db, term_frequencies_db, and
    /// metadata_db
    pub async fn insert_doc(
        &self,
        txn: &mut WTxn<'db>,
        doc_id: u128,
        doc: &str,
    ) -> Result<(), GraphError> {
        let tokens = self.tokenize::<true>(doc);
        let doc_length = tokens.len() as u32;

        let mut term_counts: HashMap<String, u32> = HashMap::new();
        for token in tokens {
            *term_counts.entry(token).or_insert(0) += 1;
        }

        txn.put(
            HBM25::doc_lengths_key(&doc_id.to_be_bytes()),
            doc_length.to_be_bytes(),
        )?;

        for (term, tf) in term_counts {
            let term_bytes = term.as_bytes();

            let posting_entry = PostingListEntry {
                doc_id,
                term_frequency: tf,
            };

            let posting_bytes = bincode::serialize(&posting_entry)?;

            // Create composite key: term + doc_id
            let mut key = term_bytes.to_vec();
            key.extend_from_slice(&doc_id.to_be_bytes());
            txn.put(&key, &posting_bytes)?;

            let current_df = txn.get(term_bytes).await?.map_or(0, |data| {
                u32::from_be_bytes(data.as_ref().try_into().unwrap())
            });
            txn.put(term_bytes, (current_df + 1).to_be_bytes())?;
        }

        let mut metadata = if let Some(data) = txn.get(&HBM25::metadata_key()).await? {
            bincode::deserialize::<BM25Metadata>(&data)?
        } else {
            BM25Metadata {
                total_docs: 0,
                avgdl: 0.0,
                k1: 1.2,
                b: 0.75,
            }
        };

        let old_total_docs = metadata.total_docs;
        metadata.total_docs += 1;
        metadata.avgdl = (metadata.avgdl * old_total_docs as f64 + doc_length as f64)
            / metadata.total_docs as f64;

        let metadata_bytes = bincode::serialize(&metadata)?;
        txn.put(&HBM25::metadata_key(), &metadata_bytes)?;

        Ok(())
    }

    pub async fn delete_doc(&self, txn: &mut WTxn<'db>, doc_id: u128) -> Result<(), GraphError> {
        // Find all composite keys for this doc_id
        let keys_to_delete = {
            let mut keys = Vec::new();
            let mut iter = txn.table_iter(TableIndex::InvertedIndex).await?;

            while let Some((key_bytes, posting_bytes)) =
                iter.next().await?.map(|entry| (entry.key, entry.value))
            {
                let posting: PostingListEntry = bincode::deserialize(&posting_bytes)?;
                if posting.doc_id == doc_id {
                    keys.push(key_bytes.to_vec());
                }
            }
            keys
        };

        // Group keys by term to update term frequencies
        let mut terms_updated = std::collections::HashSet::new();

        for key in keys_to_delete {
            // Extract term from composite key (term is everything except last 16 bytes for u128)
            if key.len() > 16 {
                let term_bytes = &key[..key.len() - 16];
                terms_updated.insert(term_bytes.to_vec());
            }

            // Delete the specific term-doc entry
            txn.delete(HBM25::inverted_index_key(&key))?;
        }

        // Update term frequencies
        for term_bytes in terms_updated {
            let current_df = txn
                .get(HBM25::inverted_index_key(&term_bytes))
                .await?
                .map_or(0, |data| {
                    u32::from_be_bytes(data.as_ref().try_into().unwrap())
                });
            if current_df > 0 {
                txn.put(
                    HBM25::term_frequency_key(&term_bytes),
                    (current_df - 1).to_be_bytes(),
                )?;
            }
        }

        let doc_length = txn
            .get(HBM25::doc_lengths_key(&doc_id.to_be_bytes()))
            .await?
            .map_or(0, |data| {
                u32::from_be_bytes(data.as_ref().try_into().unwrap())
            });

        txn.delete(HBM25::doc_lengths_key(&doc_id.to_be_bytes()))?;

        let metadata_data = txn.get(HBM25::metadata_key()).await?;

        if let Some(data) = metadata_data {
            let mut metadata: BM25Metadata = bincode::deserialize(&data.to_vec())?;
            if metadata.total_docs > 0 {
                // update average document length
                metadata.avgdl = if metadata.total_docs > 1 {
                    (metadata.avgdl * metadata.total_docs as f64 - doc_length as f64)
                        / (metadata.total_docs - 1) as f64
                } else {
                    0.0
                };
                metadata.total_docs -= 1;

                let metadata_bytes = bincode::serialize(&metadata)?;
                txn.put(&HBM25::metadata_key(), &metadata_bytes)?;
            }
        }

        Ok(())
    }

    /// Simply delete doc_id and then re-insert new doc with same doc-id
    pub async fn update_doc(
        &self,
        txn: &mut WTxn<'db>,
        doc_id: u128,
        doc: &str,
    ) -> Result<(), GraphError> {
        self.delete_doc(txn, doc_id).await?;
        self.insert_doc(txn, doc_id, doc).await
    }

    fn calculate_bm25_score(
        &self,
        tf: u32,         // term frequency
        doc_len: u32,    // document length
        df: u32,         // document frequency
        total_docs: u64, // total documents
        avgdl: f64,      // average document length
    ) -> f32 {
        // ensure we don't have division by zero
        let df = df.max(1) as f64;
        let total_docs = total_docs.max(1) as f64;

        // calculate IDF: ln((N - df + 0.5) / (df + 0.5) + 1)
        // this can be negative when df is high relative to N, which is mathematically correct
        let idf = (((total_docs - df + 0.5) / (df + 0.5)) + 1.0).ln();

        // ensure avgdl is not zero
        let avgdl = if avgdl > 0.0 { avgdl } else { doc_len as f64 };

        // calculate BM25 score
        let tf = tf as f64;
        let doc_len = doc_len as f64;
        let tf_component = (tf * (self.k1 + 1.0))
            / (tf + self.k1 * (1.0 - self.b + self.b * (doc_len.abs() / avgdl)));

        (idf * tf_component) as f32
    }

    pub async fn search(
        &self,
        txn: &slatedb::DBTransaction,
        query: &str,
        limit: usize,
    ) -> Result<Vec<(u128, f32)>, GraphError> {
        let query_terms = self.tokenize::<true>(query);
        // (node uuid, score)
        let mut doc_scores: HashMap<u128, f32> = HashMap::with_capacity(limit);

        let metadata = txn
            .get(&HBM25::metadata_key())
            .await?
            .ok_or(GraphError::New("BM25 metadata not found".to_string()))?;
        let metadata: BM25Metadata = bincode::deserialize(&metadata)?;

        // for each query term, calculate scores
        for term in query_terms {
            let term_bytes = term.as_bytes();

            let doc_frequency = txn
                .get(HBM25::term_frequency_key(term_bytes))
                .await?
                .map_or(0, |data| {
                    u32::from_be_bytes(data.as_ref().try_into().unwrap())
                });
            if doc_frequency == 0 {
                continue;
            }

            // Get all documents containing this term using table_prefix_iter
            let mut iter = txn
                .table_prefix_iter::<{ 2 + 64 }>(TableIndex::InvertedIndex, term_bytes)
                .await?;

            while let Some(entry) = iter.next().await? {
                let key = entry.key;
                let posting_bytes = entry.value;

                // The key includes table prefix (2 bytes) + term + doc_id (16 bytes)
                // Check if key still has our term as prefix (after table prefix)
                let key_without_prefix = &key[2..];
                if !key_without_prefix.starts_with(term_bytes) {
                    break;
                }
                let posting: PostingListEntry = bincode::deserialize(&posting_bytes)?;

                // Get document length
                let doc_length = txn
                    .get(HBM25::doc_lengths_key(&posting.doc_id.to_be_bytes()))
                    .await?
                    .map_or(0, |data| {
                        u32::from_be_bytes(data.as_ref().try_into().unwrap())
                    });

                // Calculate BM25 score for this term in this document
                let score = self.calculate_bm25_score(
                    posting.term_frequency,
                    doc_length,
                    doc_frequency,
                    metadata.total_docs,
                    metadata.avgdl,
                );

                *doc_scores.entry(posting.doc_id).or_insert(0.0) += score;
            }
        }

        // Sort by score and return top results
        let mut results: Vec<(u128, f32)> = doc_scores.into_iter().collect();
        results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        results.truncate(limit);

        debug_println!("found {} results in bm25 search", results.len());

        Ok(results)
    }
}

pub trait HybridSearch {
    /// Search both hnsw index and bm25 docs
    fn hybrid_search(
        self,
        query: &str,
        query_vector: &[f64],
        alpha: f32,
        limit: usize,
    ) -> impl std::future::Future<Output = Result<Vec<(u128, f32)>, GraphError>> + Send;
}

impl HybridSearch for HelixGraphStorage {
    async fn hybrid_search(
        self,
        query: &str,
        query_vector: &[f64],
        alpha: f32,
        limit: usize,
    ) -> Result<Vec<(u128, f32)>, GraphError> {
        // Run BM25 search
        let bm25_results = match self.bm25.as_ref() {
            Some(bm25) => {
                let txn = self.read_txn().await?;
                bm25.search(&txn, query, limit * 2).await?
            }
            None => return Err(GraphError::from("BM25 not enabled!")),
        };

        // TODO: Vector search not yet implemented for Slate
        // For now, just return BM25 results scaled by alpha
        let vector_results: Option<Vec<(u128, f64)>> = todo!("Vector search not yet implemented");

        let mut combined_scores: HashMap<u128, f32> = HashMap::new();

        for (doc_id, score) in bm25_results {
            combined_scores.insert(doc_id, alpha * score);
        }

        // correct_score = alpha * bm25_score + (1.0 - alpha) * vector_score
        if let Some(vector_results) = vector_results {
            for (doc_id, score) in vector_results {
                let similarity = (1.0 / (1.0 + score)) as f32;
                combined_scores
                    .entry(doc_id)
                    .and_modify(|existing_score| *existing_score += (1.0 - alpha) * similarity)
                    .or_insert((1.0 - alpha) * similarity);
            }
        }

        let mut results = combined_scores.into_iter().collect::<Vec<(u128, f32)>>();
        results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        results.truncate(limit);

        Ok(results)
    }
}

pub trait BM25Flatten {
    /// util func to flatten array of strings to a single string
    fn flatten_bm25(&self) -> String;
}

impl BM25Flatten for ImmutablePropertiesMap<'_> {
    fn flatten_bm25(&self) -> String {
        self.iter()
            .fold(String::with_capacity(self.len() * 4), |mut s, (k, v)| {
                s.push_str(k);
                s.push(' ');
                s.push_str(&v.inner_stringify());
                s.push(' ');
                s
            })
    }
}
