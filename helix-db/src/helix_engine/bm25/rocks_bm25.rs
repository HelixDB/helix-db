use crate::{
    debug_println,
    helix_engine::{
        storage_core::HelixGraphStorage,
        traversal_core::txn::{RTxn, WTxn},
        types::GraphError,
        vector_core::{hnsw::HNSW, vector::HVector},
    },
    utils::properties::ImmutablePropertiesMap,
};

use bumpalo::Bump;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};
use tokio::task;

const DB_BM25_INVERTED_INDEX: &str = "bm25_inverted_index"; // term -> list of (doc_id, tf)
const DB_BM25_DOC_LENGTHS: &str = "bm25_doc_lengths"; // doc_id -> document length
const DB_BM25_TERM_FREQUENCIES: &str = "bm25_term_frequencies"; // term -> document frequency
const DB_BM25_METADATA: &str = "bm25_metadata"; // stores total docs, avgdl, etc.
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

pub trait BM25 {
    fn tokenize<const SHOULD_FILTER: bool>(&self, text: &str) -> Vec<String>;

    fn insert_doc(&self, txn: &mut WTxn, doc_id: u128, doc: &str) -> Result<(), GraphError>;

    fn delete_doc(&self, txn: &mut WTxn, doc_id: u128) -> Result<(), GraphError>;

    fn update_doc(&self, txn: &mut WTxn, doc_id: u128, doc: &str) -> Result<(), GraphError>;

    /// Calculate the BM25 score for a single term of a query (no sum)
    fn calculate_bm25_score(
        &self,
        tf: u32,         // term frequency
        doc_len: u32,    // document length
        df: u32,         // document frequency
        total_docs: u64, // total documents
        avgdl: f64,      // average document length
    ) -> f32;

    fn search(&self, txn: &RTxn, query: &str, limit: usize)
    -> Result<Vec<(u128, f32)>, GraphError>;
}

pub struct HBM25Config<'db> {
    pub graph_env: &'db rocksdb::TransactionDB<rocksdb::MultiThreaded>,
    pub inverted_index_db: Arc<rocksdb::BoundColumnFamily<'db>>,
    pub doc_lengths_db: Arc<rocksdb::BoundColumnFamily<'db>>,
    pub term_frequencies_db: Arc<rocksdb::BoundColumnFamily<'db>>,
    pub metadata_db: Arc<rocksdb::BoundColumnFamily<'db>>,
    k1: f64,
    b: f64,
}

impl<'db> HBM25Config<'db> {
    pub fn new(
        graph_env: &'db rocksdb::TransactionDB<rocksdb::MultiThreaded>,
        _wtxn: &mut WTxn<'db>,
    ) -> Result<HBM25Config<'db>, GraphError> {
        Ok(HBM25Config {
            graph_env,
            inverted_index_db: graph_env.cf_handle("inverted_index").unwrap(),
            doc_lengths_db: graph_env.cf_handle("doc_lengths").unwrap(),
            term_frequencies_db: graph_env.cf_handle("term_frequencies").unwrap(),
            metadata_db: graph_env.cf_handle("metadata").unwrap(),
            k1: 1.2,
            b: 0.75,
        })
    }

    pub fn new_temp(
        graph_env: &'db rocksdb::TransactionDB<rocksdb::MultiThreaded>,
        _wtxn: &mut WTxn<'db>,
        _uuid: &str,
    ) -> Result<HBM25Config<'db>, GraphError> {
        Ok(HBM25Config {
            graph_env: graph_env.clone(),
            inverted_index_db: graph_env.cf_handle("inverted_index").unwrap(),
            doc_lengths_db: graph_env.cf_handle("doc_lengths").unwrap(),
            term_frequencies_db: graph_env.cf_handle("term_frequencies").unwrap(),
            metadata_db: graph_env.cf_handle("metadata").unwrap(),
            k1: 1.2,
            b: 0.75,
        })
    }
}

impl<'db> BM25 for HBM25Config<'db> {
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
    fn insert_doc(&self, txn: &mut WTxn, doc_id: u128, doc: &str) -> Result<(), GraphError> {
        let tokens = self.tokenize::<true>(doc);
        let doc_length = tokens.len() as u32;

        let mut term_counts: HashMap<String, u32> = HashMap::new();
        for token in tokens {
            *term_counts.entry(token).or_insert(0) += 1;
        }

        txn.txn.put_cf(
            &self.doc_lengths_db,
            &doc_id.to_be_bytes(),
            &doc_length.to_be_bytes(),
        )?;

        for (term, tf) in term_counts {
            let term_bytes = term.as_bytes();

            let posting_entry = PostingListEntry {
                doc_id,
                term_frequency: tf,
            };

            let posting_bytes = bincode::serialize(&posting_entry)?;

            txn.txn
                .put_cf(&self.inverted_index_db, term_bytes, &posting_bytes)?;

            let current_df = txn
                .txn
                .get_cf(&self.term_frequencies_db, term_bytes)?
                .map_or(0, |data| u32::from_be_bytes(data.try_into().unwrap()));
            txn.txn.put_cf(
                &self.term_frequencies_db,
                term_bytes,
                &(current_df + 1).to_be_bytes(),
            )?;
        }

        let mut metadata = if let Some(data) = txn.txn.get_cf(&self.metadata_db, METADATA_KEY)? {
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
        txn.txn
            .put_cf(&self.metadata_db, METADATA_KEY, &metadata_bytes)?;

        Ok(())
    }

    fn delete_doc(&self, txn: &mut WTxn, doc_id: u128) -> Result<(), GraphError> {
        let terms_to_update = {
            let mut terms = Vec::new();
            let mut iter = txn
                .txn
                .iterator_cf(&self.inverted_index_db, rocksdb::IteratorMode::Start);

            while let Some((term_bytes, posting_bytes)) = iter.next().transpose()? {
                let posting: PostingListEntry = bincode::deserialize(&posting_bytes)?;
                if posting.doc_id == doc_id {
                    terms.push(term_bytes.to_vec());
                }
            }
            terms
        };

        // remove postings and update term frequencies
        for term_bytes in terms_to_update {
            // collect entries to keep
            let entries_to_keep = {
                let mut entries = Vec::new();
                for result in txn
                    .txn
                    .prefix_iterator_cf(&self.inverted_index_db, &term_bytes)
                {
                    let (_, posting_bytes) = result?;
                    let posting: PostingListEntry = bincode::deserialize(&posting_bytes)?;
                    if posting.doc_id != doc_id {
                        entries.push(posting_bytes.to_vec());
                    }
                }
                entries
            };

            // delete all entries for this term
            txn.txn.delete_cf(&self.inverted_index_db, &term_bytes)?;

            // re-add the entries we want to keep
            for entry_bytes in entries_to_keep {
                txn.txn
                    .put_cf(&self.inverted_index_db, &term_bytes, &entry_bytes)?;
            }

            let current_df = txn
                .txn
                .get_cf(&self.term_frequencies_db, &term_bytes)?
                .map_or(0, |data| u32::from_be_bytes(data.try_into().unwrap()));
            if current_df > 0 {
                txn.txn.put_cf(
                    &self.term_frequencies_db,
                    &term_bytes,
                    &(current_df - 1).to_be_bytes(),
                )?;
            }
        }

        let doc_length = txn
            .txn
            .get_cf(&self.doc_lengths_db, &doc_id.to_be_bytes())?
            .map_or(0, |data| u32::from_be_bytes(data.try_into().unwrap()));

        txn.txn
            .delete_cf(&self.doc_lengths_db, &doc_id.to_be_bytes())?;

        let metadata_data = txn.txn.get_cf(&self.metadata_db, METADATA_KEY)?;

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
                txn.txn
                    .put_cf(&self.metadata_db, METADATA_KEY, &metadata_bytes)?;
            }
        }

        Ok(())
    }

    /// Simply delete doc_id and then re-insert new doc with same doc-id
    fn update_doc(&self, txn: &mut WTxn, doc_id: u128, doc: &str) -> Result<(), GraphError> {
        self.delete_doc(txn, doc_id)?;
        self.insert_doc(txn, doc_id, doc)
    }

    fn calculate_bm25_score(
        &self,
        tf: u32,
        doc_len: u32,
        df: u32,
        total_docs: u64,
        avgdl: f64,
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

    fn search(
        &self,
        txn: &RTxn,
        query: &str,
        limit: usize,
    ) -> Result<Vec<(u128, f32)>, GraphError> {
        let query_terms = self.tokenize::<true>(query);
        // (node uuid, score)
        let mut doc_scores: HashMap<u128, f32> = HashMap::with_capacity(limit);

        let metadata = txn
            .txn
            .get_cf(&self.metadata_db, METADATA_KEY)?
            .ok_or(GraphError::New("BM25 metadata not found".to_string()))?;
        let metadata: BM25Metadata = bincode::deserialize(&metadata)?;

        // for each query term, calculate scores
        for term in query_terms {
            let term_bytes = term.as_bytes();

            let doc_frequency = txn
                .txn
                .get_cf(&self.term_frequencies_db, term_bytes)?
                .map_or(0, |data| u32::from_be_bytes(data.try_into().unwrap()));
            if doc_frequency == 0 {
                continue;
            }

            // Get all documents containing this term

            for result in txn
                .txn
                .prefix_iterator_cf(&self.inverted_index_db, term_bytes)
            {
                let (key, posting_bytes) = result?;
                if key.as_ref() != term_bytes {
                    break;
                }
                let posting: PostingListEntry = bincode::deserialize(&posting_bytes)?;

                // Get document length
                let doc_length = txn
                    .txn
                    .get_cf(&self.doc_lengths_db, &posting.doc_id.to_be_bytes())?
                    .map_or(0, |data| u32::from_be_bytes(data.try_into().unwrap()));

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

impl<'db> HybridSearch for HelixGraphStorage<'db> {
    async fn hybrid_search(
        self,
        query: &str,
        query_vector: &[f64],
        alpha: f32,
        limit: usize,
    ) -> Result<Vec<(u128, f32)>, GraphError> {
        let query_owned = query.to_string();
        let query_vector_owned = query_vector.to_vec();

        let graph_env_bm25 = self.graph_env;
        let graph_env_vector = self.graph_env;

        let bm25_handle = task::spawn_blocking(move || -> Result<Vec<(u128, f32)>, GraphError> {
            let txn = RTxn::new(&graph_env_bm25);
            match self.bm25.as_ref() {
                Some(s) => s.search(&txn, &query_owned, limit * 2),
                None => Err(GraphError::from("BM25 not enabled!")),
            }
        });

        let vector_handle =
            task::spawn_blocking(move || -> Result<Option<Vec<(u128, f64)>>, GraphError> {
                let txn = RTxn::new(&graph_env_vector);
                let arena = Bump::new(); // MOVE 
                let query_slice = arena.alloc_slice_copy(query_vector_owned.as_slice());
                let results = self.vectors.search::<fn(&HVector, &RoTxn) -> bool>(
                    &txn,
                    query_slice,
                    limit * 2,
                    "vector",
                    None,
                    false,
                    &arena,
                )?;
                let scores = results
                    .into_iter()
                    .map(|vec| (vec.id, vec.distance.unwrap_or(0.0)))
                    .collect::<Vec<(u128, f64)>>();
                Ok(Some(scores))
            });

        let (bm25_results, vector_results) = match tokio::try_join!(bm25_handle, vector_handle) {
            Ok((a, b)) => (a, b),
            Err(e) => return Err(GraphError::from(e.to_string())),
        };

        let mut combined_scores: HashMap<u128, f32> = HashMap::new();

        for (doc_id, score) in bm25_results? {
            combined_scores.insert(doc_id, alpha * score);
        }

        // correct_score = alpha * bm25_score + (1.0 - alpha) * vector_score
        if let Some(vector_results) = vector_results? {
            for (doc_id, score) in vector_results {
                let similarity = (1.0 / (1.0 + score)) as f32;
                combined_scores
                    .entry(doc_id)
                    .and_modify(|existing_score| *existing_score += (1.0 - alpha) * similarity)
                    .or_insert((1.0 - alpha) * similarity); // correction made here from score as f32 to similarity
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
