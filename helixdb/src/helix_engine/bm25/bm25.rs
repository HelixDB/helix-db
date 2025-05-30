use heed3::{types::*, Database, Env, RoTxn, RwTxn};
use serde::{Deserialize, Serialize};
use std::{borrow::Cow, collections::HashMap, sync::Arc};

use crate::helix_engine::{
    storage_core::{storage_core::HelixGraphStorage, storage_methods::StorageMethods},
    types::GraphError,
    vector_core::{hnsw::HNSW, vector::HVector},
};

const DB_BM25_INVERTED_INDEX: &str = "bm25_inverted_index"; // term -> list of (doc_id, tf)
const DB_BM25_DOC_LENGTHS: &str = "bm25_doc_lengths"; // doc_id -> document length
const DB_BM25_TERM_FREQUENCIES: &str = "bm25_term_frequencies"; // term -> document frequency
const DB_BM25_METADATA: &str = "bm25_metadata"; // stores total docs, avgdl, etc.

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct BM25Metadata {
    pub total_docs: u64,
    pub avgdl: f64,
    pub k1: f32,
    pub b: f32,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PostingListEntry {
    pub doc_id: u128,
    pub term_frequency: u32,
}

pub trait BM25 {
    fn tokenize<const SHOULD_FILTER: bool>(&self, text: &str) -> Vec<Cow<'_, str>>;
    fn insert_doc(&self, txn: &mut RwTxn, doc_id: u128, doc: &str) -> Result<(), GraphError>;
    fn update_doc(&self, txn: &mut RwTxn, doc_id: u128, doc: &str) -> Result<(), GraphError>;
    fn delete_doc(&self, txn: &mut RwTxn, doc_id: u128) -> Result<(), GraphError>;
    fn search(
        &self,
        txn: &RoTxn,
        query: &str,
        limit: usize,
    ) -> Result<Vec<(u128, f32)>, GraphError>;
    fn calculate_bm25_score(
        &self,
        term: &str,
        doc_id: u128,
        tf: u32,
        doc_length: u32,
        df: u32,
        total_docs: u64,
        avgdl: f64,
    ) -> f32;
}

pub struct HBM25Config {
    pub graph_env: Env,
    pub inverted_index_db: Database<Bytes, Bytes>,
    pub doc_lengths_db: Database<U128<heed3::byteorder::BE>, U32<heed3::byteorder::BE>>,
    pub term_frequencies_db: Database<Bytes, U32<heed3::byteorder::BE>>,
    pub metadata_db: Database<Bytes, Bytes>,
}

impl HBM25Config {
    pub fn new(graph_env: &Env, wtxn: &mut RwTxn) -> Result<HBM25Config, GraphError> {
        let inverted_index_db: Database<Bytes, Bytes> = graph_env
            .database_options()
            .types::<Bytes, Bytes>()
            .flags(heed3::DatabaseFlags::DUP_SORT)
            .name(DB_BM25_INVERTED_INDEX)
            .create(wtxn)?;

        let doc_lengths_db: Database<U128<heed3::byteorder::BE>, U32<heed3::byteorder::BE>> =
            graph_env
                .database_options()
                .types::<U128<heed3::byteorder::BE>, U32<heed3::byteorder::BE>>()
                .name(DB_BM25_DOC_LENGTHS)
                .create(wtxn)?;

        let term_frequencies_db: Database<Bytes, U32<heed3::byteorder::BE>> = graph_env
            .database_options()
            .types::<Bytes, U32<heed3::byteorder::BE>>()
            .name(DB_BM25_TERM_FREQUENCIES)
            .create(wtxn)?;

        let metadata_db: Database<Bytes, Bytes> = graph_env
            .database_options()
            .types::<Bytes, Bytes>()
            .name(DB_BM25_METADATA)
            .create(wtxn)?;

        Ok(HBM25Config {
            graph_env: graph_env.clone(),
            inverted_index_db,
            doc_lengths_db,
            term_frequencies_db,
            metadata_db,
        })
    }
}

impl BM25 for HBM25Config {
    fn tokenize<const SHOULD_FILTER: bool>(&self, text: &str) -> Vec<Cow<'_, str>> {
        text.to_lowercase()
            .replace(|c: char| !c.is_alphanumeric(), " ")
            .split_whitespace()
            .filter(|s| !SHOULD_FILTER || s.len() > 2)
            .map(|s| Cow::Owned(s.to_string()))
            .collect()
    }

    fn insert_doc(&self, txn: &mut RwTxn, doc_id: u128, doc: &str) -> Result<(), GraphError> {
        let tokens = self.tokenize::<true>(doc);
        let doc_length = tokens.len() as u32;

        let mut term_counts: HashMap<Cow<'_, str>, u32> = HashMap::new();
        for token in tokens {
            *term_counts.entry(token).or_insert(0) += 1;
        }

        self.doc_lengths_db.put(txn, &doc_id, &doc_length)?;

        for (term, tf) in term_counts {
            let term_bytes = term.as_bytes();

            let posting_entry = PostingListEntry {
                doc_id,
                term_frequency: tf,
            };

            let posting_bytes = bincode::serialize(&posting_entry)?;

            self.inverted_index_db
                .put(txn, term_bytes, &posting_bytes)?;

            let current_df = self.term_frequencies_db.get(txn, term_bytes)?.unwrap_or(0);
            self.term_frequencies_db
                .put(txn, term_bytes, &(current_df + 1))?;
        }
        let metadata_key = b"metadata";
        let mut metadata = if let Some(data) = self.metadata_db.get(txn, metadata_key)? {
            bincode::deserialize::<BM25Metadata>(data)?
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
        self.metadata_db.put(txn, metadata_key, &metadata_bytes)?;

        // txn.commit()?;
        Ok(())
    }

    fn update_doc(&self, txn: &mut RwTxn, doc_id: u128, doc: &str) -> Result<(), GraphError> {
        // For simplicity, delete and re-insert
        self.delete_doc(txn, doc_id)?;
        self.insert_doc(txn, doc_id, doc)
    }

    fn delete_doc(&self, txn: &mut RwTxn, doc_id: u128) -> Result<(), GraphError> {
        let terms_to_update = {
            let mut terms = Vec::new();
            let mut iter = self.inverted_index_db.iter(txn)?;

            while let Some((term_bytes, posting_bytes)) = iter.next().transpose()? {
                let posting: PostingListEntry = bincode::deserialize(posting_bytes)?;
                if posting.doc_id == doc_id {
                    terms.push(term_bytes.to_vec());
                }
            }
            terms
        };

        // Remove postings and update term frequencies
        for term_bytes in terms_to_update {
            // Collect entries to keep
            let entries_to_keep = {
                let mut entries = Vec::new();
                if let Some(duplicates) =
                    self.inverted_index_db.get_duplicates(txn, &term_bytes)?
                {
                    for result in duplicates {
                        let (_, posting_bytes) = result?;
                        let posting: PostingListEntry = bincode::deserialize(posting_bytes)?;
                        if posting.doc_id != doc_id {
                            entries.push(posting_bytes.to_vec());
                        }
                    }
                }
                entries
            };

            // Delete all entries for this term
            self.inverted_index_db.delete(txn, &term_bytes)?;

            // Re-add the entries we want to keep
            for entry_bytes in entries_to_keep {
                self.inverted_index_db.put(txn, &term_bytes, &entry_bytes)?;
            }

            // Update document frequency
            let current_df = self.term_frequencies_db
                .get(txn, &term_bytes)?
                .unwrap_or(0);
            if current_df > 0 {
                self.term_frequencies_db
                    .put(txn, &term_bytes, &(current_df - 1))?;
            }
        }

        // Get document length before deleting it
        let doc_length = self.doc_lengths_db.get(txn, &doc_id)?.unwrap_or(0);
        
        self.doc_lengths_db.delete(txn, &doc_id)?;

        // Update metadata
        let metadata_key = b"metadata";
        let metadata_data = self.metadata_db
            .get(txn, metadata_key)?
            .map(|data| data.to_vec());

        if let Some(data) = metadata_data {
            let mut metadata: BM25Metadata = bincode::deserialize(&data)?;
            if metadata.total_docs > 0 {
                // Update average document length
                metadata.avgdl = if metadata.total_docs > 1 {
                    (metadata.avgdl * metadata.total_docs as f64 - doc_length as f64)
                        / (metadata.total_docs - 1) as f64
                } else {
                    0.0
                };
                metadata.total_docs -= 1;

                let metadata_bytes = bincode::serialize(&metadata)?;
                self.metadata_db.put(txn, metadata_key, &metadata_bytes)?;
            }
        }

        Ok(())
    }

    fn search(
        &self,
        txn: &RoTxn,
        query: &str,
        limit: usize,
    ) -> Result<Vec<(u128, f32)>, GraphError> {
        let query_terms = self.tokenize::<true>(query);
        let mut doc_scores: HashMap<u128, f32> = HashMap::with_capacity(limit);

        let metadata_key = b"metadata";
        let metadata = self
            .metadata_db
            .get(txn, metadata_key)?
            .ok_or(GraphError::New("BM25 metadata not found".to_string()))?;
        let metadata: BM25Metadata = bincode::deserialize(metadata)?;

        // For each query term, calculate scores
        for term in query_terms {
            let term_bytes = term.as_bytes();

            // Get document frequency for this term
            let df = self.term_frequencies_db.get(txn, term_bytes)?.unwrap_or(0);
            // if df == 0 {
            //     continue; // Term not in index
            // }

            // Get all documents containing this term
            if let Some(duplicates) = self.inverted_index_db.get_duplicates(txn, term_bytes)? {
                for result in duplicates {
                    let (_, posting_bytes) = result?;
                    let posting: PostingListEntry = bincode::deserialize(posting_bytes)?;

                    // Get document length
                    let doc_length = self.doc_lengths_db.get(txn, &posting.doc_id)?.unwrap_or(0);

                    // Calculate BM25 score for this term in this document
                    let score = self.calculate_bm25_score(
                        &term,
                        posting.doc_id,
                        posting.term_frequency,
                        doc_length,
                        df,
                        metadata.total_docs,
                        metadata.avgdl,
                    );

                    // Add to document's total score
                    *doc_scores.entry(posting.doc_id).or_insert(0.0) += score;
                }
            }
        }

        // Sort by score and return top results
        let mut results: Vec<(u128, f32)> = doc_scores.into_iter().collect();
        results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        results.truncate(limit);

        Ok(results)
    }

    fn calculate_bm25_score(
        &self,
        term: &str,
        doc_id: u128,
        tf: u32,
        doc_length: u32,
        df: u32,
        total_docs: u64,
        avgdl: f64,
    ) -> f32 {
        let k1 = 1.2;
        let b = 0.75;

        // Ensure we don't have division by zero
        let df = df.max(1);
        let total_docs = total_docs.max(1);

        // Calculate IDF: log((N - df + 0.5) / (df + 0.5))
        // This can be negative when df is high relative to N, which is mathematically correct
        let idf = ((total_docs as f64 - df as f64 + 0.5) / (df as f64 + 0.5)).ln();

        // Ensure avgdl is not zero
        let avgdl = if avgdl > 0.0 {
            avgdl
        } else {
            doc_length as f64
        };

        // Calculate BM25 score
        let tf_component = (tf as f64 * (k1 as f64 + 1.0))
            / (tf as f64 + k1 as f64 * (1.0 - b as f64 + b as f64 * (doc_length as f64 / avgdl)));

        let score = (idf * tf_component) as f32;

        // The score can be negative when IDF is negative (term appears in most documents)
        // This is mathematically correct - such terms have low discriminative power
        // But documents with higher tf should still score higher than those with lower tf
        score
    }
}

pub trait HybridSearch {
    fn hybrid_search(
        &self,
        txn: &RoTxn,
        query: &str,
        query_vector: &[f64],
        vector_query: Option<&[f32]>,
        alpha: f32,
        limit: usize,
    ) -> Result<Vec<(u128, f32)>, GraphError>;
}

impl HybridSearch for HelixGraphStorage {
    fn hybrid_search(
        &self,
        txn: &RoTxn,
        query: &str,
        query_vector: &[f64],
        vector_query: Option<&[f32]>,
        alpha: f32,
        limit: usize,
    ) -> Result<Vec<(u128, f32)>, GraphError> {
        // Get BM25 scores
        let bm25_results = self.bm25.search(txn, query, limit * 2)?; // Get more results for better fusion
        let mut combined_scores: HashMap<u128, f32> = HashMap::new();

        // Add BM25 scores (weighted by alpha)
        for (doc_id, score) in bm25_results {
            combined_scores.insert(doc_id, alpha * score);
        }

        // Add vector similarity scores if provided (weighted by 1-alpha)
        if let Some(_query_vector) = vector_query {
            // This would integrate with your existing vector search
            // For now, we'll just use BM25 scores
            // You would call your vector similarity search here and combine scores
            let vector_results = self.vectors.search::<fn(&HVector, &RoTxn) -> bool>(
                txn,
                query_vector,
                limit * 2,
                None,
                false,
            )?;
            for doc in vector_results {
                let doc_id = doc.id;
                let score = doc.distance.unwrap_or(0.0);
                combined_scores.insert(doc_id, (1.0 - alpha) * score as f32);
            }
        }

        // Sort by combined score and return top results
        let mut results: Vec<(u128, f32)> = combined_scores.into_iter().collect();
        results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        results.truncate(limit);

        Ok(results)
    }
}


