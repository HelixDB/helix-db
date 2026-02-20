use crate::helix_engine::{
    bm25::bm25::BM25,
    traversal_core::{
        LMDB_STRING_HEADER_LENGTH, traversal_iter::RoTraversalIterator,
        traversal_value::TraversalValue,
    },
    types::GraphError,
    vector_core::hnsw::HNSW,
};

/// Trait that adds hybrid search capability (vector + BM25) to traversal iterators.
///
/// This trait enables combining semantic vector search with keyword-based BM25 search,
/// returning combined results that can be fused using `RerankRRF` or `RerankMMR`.
pub trait SearchHybridAdapter<'db, 'arena, 'txn>:
    Iterator<Item = Result<TraversalValue<'arena>, GraphError>>
{
    /// Perform a hybrid search combining vector similarity and BM25 keyword search.
    ///
    /// # Arguments
    /// * `label` - The type label for the vectors/documents to search
    /// * `query_vec` - The query vector for similarity search
    /// * `query_text` - The query text for BM25 keyword search
    /// * `k` - Number of results to return from each search
    ///
    /// # Returns
    /// A traversal iterator containing combined results from both searches.
    /// Results can be piped to `::RerankRRF` or `::RerankMMR` for fusion.
    ///
    /// # Example
    /// ```ignore
    /// let results = storage
    ///     .search_hybrid("Document", query_vec, "search query", 10)?
    ///     .rerank(RRFReranker::new(), None)
    ///     .collect_to::<Vec<_>>();
    /// ```
    fn search_hybrid<K>(
        self,
        label: &'arena str,
        query_vec: &'arena [f64],
        query_text: &str,
        k: K,
    ) -> Result<
        RoTraversalIterator<
            'db,
            'arena,
            'txn,
            impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
        >,
        GraphError,
    >
    where
        K: TryInto<usize> + Copy,
        K::Error: std::fmt::Debug;
}

impl<'db, 'arena, 'txn, I: Iterator<Item = Result<TraversalValue<'arena>, GraphError>>>
    SearchHybridAdapter<'db, 'arena, 'txn> for RoTraversalIterator<'db, 'arena, 'txn, I>
{
    fn search_hybrid<K>(
        self,
        label: &'arena str,
        query_vec: &'arena [f64],
        query_text: &str,
        k: K,
    ) -> Result<
        RoTraversalIterator<
            'db,
            'arena,
            'txn,
            impl Iterator<Item = Result<TraversalValue<'arena>, GraphError>>,
        >,
        GraphError,
    >
    where
        K: TryInto<usize> + Copy,
        K::Error: std::fmt::Debug,
    {
        let k_usize = k.try_into().unwrap();

        // 1. Execute vector search
        let vector_results =
            self.storage.vectors.search(
                self.txn,
                query_vec,
                k_usize,
                label,
                None::<
                    &[fn(
                        &crate::helix_engine::vector_core::vector::HVector,
                        &heed3::RoTxn,
                    ) -> bool],
                >,
                false,
                self.arena,
            );

        // 2. Execute BM25 search
        let bm25_results = match self.storage.bm25.as_ref() {
            Some(bm25) => bm25.search(self.txn, query_text, k_usize, self.arena)?,
            None => return Err(GraphError::from("BM25 not enabled for hybrid search")),
        };

        // 3. Collect vector results as TraversalValues
        let vector_iter: Vec<Result<TraversalValue<'arena>, GraphError>> = match vector_results {
            Ok(vectors) => vectors
                .into_iter()
                .map(|vector| Ok(TraversalValue::Vector(vector)))
                .collect(),
            Err(e) => {
                vec![Err(GraphError::VectorError(format!(
                    "Vector search error: {:?}",
                    e
                )))]
            }
        };

        // 4. Collect BM25 results as TraversalValues
        // BM25 returns (doc_id, score), we need to look up the actual nodes
        let label_as_bytes = label.as_bytes();
        let bm25_iter: Vec<Result<TraversalValue<'arena>, GraphError>> = bm25_results
            .into_iter()
            .filter_map(|(id, score)| {
                if let Ok(Some(value)) = self.storage.nodes_db.get(self.txn, &id) {
                    if value.len() < LMDB_STRING_HEADER_LENGTH {
                        return None;
                    }

                    let length_of_label_in_lmdb =
                        u64::from_le_bytes(value[..LMDB_STRING_HEADER_LENGTH].try_into().unwrap())
                            as usize;

                    if length_of_label_in_lmdb != label.len() {
                        return None;
                    }

                    if value.len() < length_of_label_in_lmdb + LMDB_STRING_HEADER_LENGTH {
                        return None;
                    }

                    let label_in_lmdb = &value[LMDB_STRING_HEADER_LENGTH
                        ..LMDB_STRING_HEADER_LENGTH + length_of_label_in_lmdb];

                    if label_in_lmdb == label_as_bytes {
                        match crate::utils::items::Node::from_bincode_bytes(id, value, self.arena) {
                            Ok(node) => {
                                return Some(Ok(TraversalValue::NodeWithScore {
                                    node,
                                    score: score as f64,
                                }));
                            }
                            Err(e) => {
                                return Some(Err(GraphError::ConversionError(e.to_string())));
                            }
                        }
                    }
                }
                None
            })
            .collect();

        // 5. Combine both result sets
        // Vector results come first, then BM25 results
        // The RerankRRF/RerankMMR will handle fusion based on their positions
        let combined_iter = vector_iter.into_iter().chain(bm25_iter.into_iter());

        Ok(RoTraversalIterator {
            storage: self.storage,
            arena: self.arena,
            txn: self.txn,
            inner: combined_iter,
        })
    }
}
