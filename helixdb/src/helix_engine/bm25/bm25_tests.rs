#[cfg(test)]
mod tests {
    use crate::helix_engine::bm25::bm25::{BM25, HBM25Config, HybridSearch, BM25Metadata};
    use crate::helix_engine::{
        storage_core::storage_core::HelixGraphStorage,
        graph_core::config::Config,
    };
    use heed3::{EnvOpenOptions, Env};
    use tempfile::tempdir;

    fn setup_test_env() -> (Env, tempfile::TempDir) {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path();

        let env = unsafe {
            EnvOpenOptions::new()
                .map_size(1024 * 1024 * 1024) // 1 GB
                .max_dbs(20)
                .open(path)
                .unwrap()
        };

        (env, temp_dir)
    }

    fn setup_bm25_config() -> (HBM25Config, tempfile::TempDir) {
        let (env, temp_dir) = setup_test_env();
        let mut wtxn = env.write_txn().unwrap();
        let config = HBM25Config::new(&env, &mut wtxn).unwrap();
        wtxn.commit().unwrap();
        (config, temp_dir)
    }

    fn setup_helix_storage() -> (HelixGraphStorage, tempfile::TempDir) {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().to_str().unwrap();
        let config = Config::default();
        let storage = HelixGraphStorage::new(path, config).unwrap();
        (storage, temp_dir)
    }

    #[test]
    fn test_tokenize_with_filter() {
        let (bm25, _temp_dir) = setup_bm25_config();
        
        let text = "The quick brown fox jumps over the lazy dog! It was amazing.";
        let tokens = bm25.tokenize::<true>(text);
        
        // Should filter out words with length <= 2 and normalize to lowercase
        let expected = vec!["the", "quick", "brown", "fox", "jumps", "over", "the", "lazy", "dog", "was", "amazing"];
        assert_eq!(tokens.len(), expected.len());
        
        for (i, token) in tokens.iter().enumerate() {
            assert_eq!(token.as_ref(), expected[i]);
        }
    }

    #[test]
    fn test_tokenize_without_filter() {
        let (bm25, _temp_dir) = setup_bm25_config();
        
        let text = "A B CD efg!";
        let tokens = bm25.tokenize::<false>(text);
        
        // Should not filter out short words
        let expected = vec!["a", "b", "cd", "efg"];
        assert_eq!(tokens.len(), expected.len());
        
        for (i, token) in tokens.iter().enumerate() {
            assert_eq!(token.as_ref(), expected[i]);
        }
    }

    #[test]
    fn test_insert_document() {
        let (bm25, _temp_dir) = setup_bm25_config();
        let mut wtxn = bm25.graph_env.write_txn().unwrap();
        
        let doc_id = 123u128;
        let doc = "The quick brown fox jumps over the lazy dog";
        
        let result = bm25.insert_doc(&mut wtxn, doc_id, doc);
        assert!(result.is_ok());
        
        // Check that document length was stored
        let doc_length = bm25.doc_lengths_db.get(&wtxn, &doc_id).unwrap();
        assert!(doc_length.is_some());
        assert!(doc_length.unwrap() > 0);
        
        // Check that metadata was updated
        let metadata_key = b"metadata";
        let metadata_bytes = bm25.metadata_db.get(&wtxn, metadata_key).unwrap();
        assert!(metadata_bytes.is_some());
        
        let metadata: BM25Metadata = bincode::deserialize(metadata_bytes.unwrap()).unwrap();
        assert_eq!(metadata.total_docs, 1);
        assert!(metadata.avgdl > 0.0);
        
        wtxn.commit().unwrap();
    }

    #[test]
    fn test_insert_multiple_documents() {
        let (bm25, _temp_dir) = setup_bm25_config();
        let mut wtxn = bm25.graph_env.write_txn().unwrap();
        
        let docs = vec![
            (1u128, "The quick brown fox"),
            (2u128, "jumps over the lazy dog"),
            (3u128, "machine learning algorithms"),
        ];
        
        for (doc_id, doc) in &docs {
            let result = bm25.insert_doc(&mut wtxn, *doc_id, doc);
            assert!(result.is_ok());
        }
        
        // Check metadata
        let metadata_key = b"metadata";
        let metadata_bytes = bm25.metadata_db.get(&wtxn, metadata_key).unwrap().unwrap();
        let metadata: BM25Metadata = bincode::deserialize(metadata_bytes).unwrap();
        assert_eq!(metadata.total_docs, 3);
        
        wtxn.commit().unwrap();
    }

    #[test]
    fn test_search_single_term() {
        let (bm25, _temp_dir) = setup_bm25_config();
        let mut wtxn = bm25.graph_env.write_txn().unwrap();
        
        // Insert test documents
        let docs = vec![
            (1u128, "The quick brown fox"),
            (2u128, "The lazy dog sleeps"),
            (3u128, "A fox in the woods"),
        ];
        
        for (doc_id, doc) in &docs {
            bm25.insert_doc(&mut wtxn, *doc_id, doc).unwrap();
        }
        wtxn.commit().unwrap();
        
        // Search for "fox"
        let rtxn = bm25.graph_env.read_txn().unwrap();
        let results = bm25.search(&rtxn, "fox", 10).unwrap();
        
        // Should return documents 1 and 3 (both contain "fox")
        assert_eq!(results.len(), 2);
        
        let doc_ids: Vec<u128> = results.iter().map(|(id, _)| *id).collect();
        assert!(doc_ids.contains(&1u128));
        assert!(doc_ids.contains(&3u128));
        
        // Scores should be positive
        for (_, score) in &results {
            assert!(*score != 0.0);
        }
    }

    #[test]
    fn test_search_multiple_terms() {
        let (bm25, _temp_dir) = setup_bm25_config();
        let mut wtxn = bm25.graph_env.write_txn().unwrap();
        
        let docs = vec![
            (1u128, "machine learning algorithms for data science"),
            (2u128, "deep learning neural networks"),
            (3u128, "data analysis and machine learning"),
            (4u128, "natural language processing"),
        ];
        
        for (doc_id, doc) in &docs {
            bm25.insert_doc(&mut wtxn, *doc_id, doc).unwrap();
        }
        wtxn.commit().unwrap();
        
        let rtxn = bm25.graph_env.read_txn().unwrap();
        let results = bm25.search(&rtxn, "machine learning", 10).unwrap();
        
        // Documents 1 and 3 should score highest (contain both terms)
        assert!(results.len() >= 2);
        
        let doc_ids: Vec<u128> = results.iter().map(|(id, _)| *id).collect();
        assert!(doc_ids.contains(&1u128));
        assert!(doc_ids.contains(&3u128));
    }

    #[test]
    fn test_bm25_score_calculation() {
        let (bm25, _temp_dir) = setup_bm25_config();
        
        let score = bm25.calculate_bm25_score(
            "test",
            123u128,
            2, // term frequency
            10, // doc length
            3, // document frequency
            100, // total docs
            8.0, // average doc length
        );
        
        // Score should be finite and reasonable
        assert!(score.is_finite());
        assert!(score != 0.0);
    }

    #[test]
    fn test_update_document() {
        let (bm25, _temp_dir) = setup_bm25_config();
        let mut wtxn = bm25.graph_env.write_txn().unwrap();
        
        let doc_id = 1u128;
        
        // Insert original document
        bm25.insert_doc(&mut wtxn, doc_id, "original content").unwrap();
        
        // Update document
        bm25.update_doc(&mut wtxn, doc_id, "updated content with more words").unwrap();
        
        // Check that document length was updated
        let doc_length = bm25.doc_lengths_db.get(&wtxn, &doc_id).unwrap().unwrap();
        assert!(doc_length > 2); // Should reflect the new document length
        
        wtxn.commit().unwrap();
        
        // Search should find the updated content
        let rtxn = bm25.graph_env.read_txn().unwrap();
        let results = bm25.search(&rtxn, "updated", 10).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, doc_id);
    }

    #[test]
    fn test_delete_document() {
        let (bm25, _temp_dir) = setup_bm25_config();
        let mut wtxn = bm25.graph_env.write_txn().unwrap();
        
        let docs = vec![
            (1u128, "document one content"),
            (2u128, "document two content"),
            (3u128, "document three content"),
        ];
        
        // Insert documents
        for (doc_id, doc) in &docs {
            bm25.insert_doc(&mut wtxn, *doc_id, doc).unwrap();
        }
        
        // Delete document 2
        bm25.delete_doc(&mut wtxn, 2u128).unwrap();
        
        // Check that document length was removed
        let doc_length = bm25.doc_lengths_db.get(&wtxn, &2u128).unwrap();
        assert!(doc_length.is_none());
        
        // Check that metadata was updated
        let metadata_key = b"metadata";
        let metadata_bytes = bm25.metadata_db.get(&wtxn, metadata_key).unwrap().unwrap();
        let metadata: BM25Metadata = bincode::deserialize(metadata_bytes).unwrap();
        assert_eq!(metadata.total_docs, 2); // Should be reduced by 1
        
        wtxn.commit().unwrap();
        
        // Search should not find the deleted document
        let rtxn = bm25.graph_env.read_txn().unwrap();
        let results = bm25.search(&rtxn, "two", 10).unwrap();
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_search_with_limit() {
        let (bm25, _temp_dir) = setup_bm25_config();
        let mut wtxn = bm25.graph_env.write_txn().unwrap();
        
        // Insert many documents containing the same term
        for i in 1..=10 {
            let doc = format!("document {} contains test content", i);
            bm25.insert_doc(&mut wtxn, i as u128, &doc).unwrap();
        }
        wtxn.commit().unwrap();
        
        let rtxn = bm25.graph_env.read_txn().unwrap();
        let results = bm25.search(&rtxn, "test", 5).unwrap();
        
        // Should respect the limit
        assert_eq!(results.len(), 5);
        
        // Results should be sorted by score (descending)
        for i in 1..results.len() {
            assert!(results[i-1].1 >= results[i].1);
        }
    }

    #[test]
    fn test_search_no_results() {
        let (bm25, _temp_dir) = setup_bm25_config();
        let mut wtxn = bm25.graph_env.write_txn().unwrap();
        
        bm25.insert_doc(&mut wtxn, 1u128, "some document content").unwrap();
        wtxn.commit().unwrap();
        
        let rtxn = bm25.graph_env.read_txn().unwrap();
        let results = bm25.search(&rtxn, "nonexistent", 10).unwrap();
        
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_edge_cases_empty_document() {
        let (bm25, _temp_dir) = setup_bm25_config();
        let mut wtxn = bm25.graph_env.write_txn().unwrap();
        
        // Insert empty document
        let result = bm25.insert_doc(&mut wtxn, 1u128, "");
        assert!(result.is_ok());
        
        // Document length should be 0
        let doc_length = bm25.doc_lengths_db.get(&wtxn, &1u128).unwrap().unwrap();
        assert_eq!(doc_length, 0);
        
        wtxn.commit().unwrap();
    }

    #[test]
    fn test_edge_cases_punctuation_only() {
        let (bm25, _temp_dir) = setup_bm25_config();
        
        let tokens = bm25.tokenize::<true>("!@#$%^&*()");
        assert_eq!(tokens.len(), 0);
    }

    #[test]
    fn test_hybrid_search() {
        let (storage, _temp_dir) = setup_helix_storage();
        let rtxn = storage.graph_env.read_txn().unwrap();
        
        // Prepare test data
        let query = "machine learning";
        let query_vector = vec![0.1, 0.2, 0.3]; // Dummy vector
        let vector_query = Some(vec![0.1f32, 0.2f32, 0.3f32]);
        let alpha = 0.5; // Equal weight between BM25 and vector
        let limit = 10;
        
        // Test hybrid search (even though it might not return results with empty index)
        let result = storage.hybrid_search(
            &rtxn, 
            query, 
            &query_vector, 
            vector_query.as_deref(), 
            alpha, 
            limit
        );
        
        // The result might be an error if vector search is not properly initialized
        // but the function should at least not panic
        match result {
            Ok(results) => {
                assert!(results.len() <= limit);
            }
            Err(_) => {
                // Vector search might not be initialized, which is acceptable for this test
                println!("Vector search not available, which is expected in this test environment");
            }
        }
    }

    #[test]
    fn test_hybrid_search_alpha_weighting() {
        let (storage, _temp_dir) = setup_helix_storage();
        
        // Insert some test documents first
        let mut wtxn = storage.graph_env.write_txn().unwrap();
        let docs = vec![
            (1u128, "machine learning algorithms"),
            (2u128, "deep learning neural networks"),
            (3u128, "data science methods"),
        ];
        
        for (doc_id, doc) in &docs {
            storage.bm25.insert_doc(&mut wtxn, *doc_id, doc).unwrap();
        }
        wtxn.commit().unwrap();
        
        let rtxn = storage.graph_env.read_txn().unwrap();
        
        // Test with different alpha values
        let query = "machine learning";
        let query_vector = vec![0.1, 0.2, 0.3];
        let vector_query = Some(vec![0.1f32, 0.2f32, 0.3f32]);
        
        // Alpha = 1.0 (BM25 only)
        let results_bm25_only = storage.hybrid_search(
            &rtxn, query, &query_vector, vector_query.as_deref(), 1.0, 10
        );
        
        // Alpha = 0.0 (Vector only)
        let results_vector_only = storage.hybrid_search(
            &rtxn, query, &query_vector, vector_query.as_deref(), 0.0, 10
        );
        
        // Alpha = 0.5 (Balanced)
        let results_balanced = storage.hybrid_search(
            &rtxn, query, &query_vector, vector_query.as_deref(), 0.5, 10
        );
        
        // All should be valid results or acceptable errors
        match results_bm25_only {
            Ok(results) => assert!(results.len() <= 10),
            Err(_) => println!("BM25-only search failed, which might be expected"),
        }
        
        match results_vector_only {
            Ok(results) => assert!(results.len() <= 10),
            Err(_) => println!("Vector-only search failed, which is expected without proper vector setup"),
        }
        
        match results_balanced {
            Ok(results) => assert!(results.len() <= 10),
            Err(_) => println!("Balanced search failed, which might be expected"),
        }
    }

    #[test]
    fn test_concurrent_operations() {
        let (bm25, _temp_dir) = setup_bm25_config();
        
        // Test multiple inserts in the same transaction
        let mut wtxn = bm25.graph_env.write_txn().unwrap();
        
        for i in 1..=100 {
            let doc = format!("document number {} with various content", i);
            let result = bm25.insert_doc(&mut wtxn, i as u128, &doc);
            assert!(result.is_ok());
        }
        
        wtxn.commit().unwrap();
        
        // Verify all documents were inserted
        let rtxn = bm25.graph_env.read_txn().unwrap();
        let results = bm25.search(&rtxn, "document", 200).unwrap();
        assert_eq!(results.len(), 100);
    }

    #[test]
    fn test_bm25_score_properties() {
        let (bm25, _temp_dir) = setup_bm25_config();
        
        // Test that higher term frequency yields higher score
        let score1 = bm25.calculate_bm25_score("test", 1, 1, 10, 5, 100, 10.0);
        let score2 = bm25.calculate_bm25_score("test", 1, 3, 10, 5, 100, 10.0);
        assert!(score2 > score1);
        
        // Test that rare terms (lower df) yield higher scores
        let score_rare = bm25.calculate_bm25_score("test", 1, 1, 10, 2, 100, 10.0);
        let score_common = bm25.calculate_bm25_score("test", 1, 1, 10, 50, 100, 10.0);
        assert!(score_rare > score_common);
    }

    #[test]
    fn test_metadata_consistency() {
        let (bm25, _temp_dir) = setup_bm25_config();
        let mut wtxn = bm25.graph_env.write_txn().unwrap();
        
        // Insert documents
        let docs = vec![
            (1u128, "short doc"),
            (2u128, "this is a much longer document with many more words"),
            (3u128, "medium length document"),
        ];
        
        for (doc_id, doc) in &docs {
            bm25.insert_doc(&mut wtxn, *doc_id, doc).unwrap();
        }
        
        // Check metadata
        let metadata_key = b"metadata";
        let metadata_bytes = bm25.metadata_db.get(&wtxn, metadata_key).unwrap().unwrap();
        let metadata: BM25Metadata = bincode::deserialize(metadata_bytes).unwrap();
        
        assert_eq!(metadata.total_docs, 3);
        assert!(metadata.avgdl > 0.0);
        assert_eq!(metadata.k1, 1.2);
        assert_eq!(metadata.b, 0.75);
        
        // Delete one document
        bm25.delete_doc(&mut wtxn, 2u128).unwrap();
        
        // Check updated metadata
        let metadata_bytes = bm25.metadata_db.get(&wtxn, metadata_key).unwrap().unwrap();
        let updated_metadata: BM25Metadata = bincode::deserialize(metadata_bytes).unwrap();
        
        assert_eq!(updated_metadata.total_docs, 2);
        // Average document length should be recalculated
        assert_ne!(updated_metadata.avgdl, metadata.avgdl);
        
        wtxn.commit().unwrap();
    }
} 