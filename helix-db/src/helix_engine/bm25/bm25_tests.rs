#[cfg(test)]
mod tests {
    use crate::{
        helix_engine::{
            bm25::bm25::{
                BM25, BM25Flatten, BM25Metadata, HBM25Config, HybridSearch, METADATA_KEY,
            },
            storage_core::{HelixGraphStorage, version_info::VersionInfo},
            traversal_core::config::Config,
            vector_core::{hnsw::HNSW, vector::HVector},
        },
        protocol::value::Value,
        utils::properties::ImmutablePropertiesMap,
    };

    use bumpalo::Bump;
    use heed3::{Env, EnvOpenOptions, RoTxn};
    use rand::Rng;
    use std::collections::HashMap;
    use tempfile::tempdir;

    fn setup_test_env() -> (Env, tempfile::TempDir) {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path();

        let env = unsafe {
            EnvOpenOptions::new()
                .map_size(4 * 1024 * 1024 * 1024) // 4GB
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
        let storage = HelixGraphStorage::new(path, config, VersionInfo::default()).unwrap();
        (storage, temp_dir)
    }

    fn generate_random_vectors(n: usize, d: usize) -> Vec<Vec<f64>> {
        let mut rng = rand::rng();
        let mut vectors = Vec::with_capacity(n);

        for _ in 0..n {
            let mut vector = Vec::with_capacity(d);
            for _ in 0..d {
                vector.push(rng.random::<f64>());
            }
            vectors.push(vector);
        }

        vectors
    }

    #[test]
    fn test_tokenize_with_filter() {
        let (bm25, _temp_dir) = setup_bm25_config();

        let text = "The quick brown fox jumps over the lazy dog! It was amazing.";
        let tokens = bm25.tokenize::<true>(text);

        // should filter out words with length <= 2 and normalize to lowercase
        let expected = [
            "the", "quick", "brown", "fox", "jumps", "over", "the", "lazy", "dog", "was", "amazing",
        ];
        assert_eq!(tokens.len(), expected.len());

        for (i, token) in tokens.iter().enumerate() {
            assert_eq!(token, expected[i]);
        }
    }

    #[test]
    fn test_tokenize_without_filter() {
        let (bm25, _temp_dir) = setup_bm25_config();

        let text = "A B CD efg!";
        let tokens = bm25.tokenize::<false>(text);

        // should not filter out short words
        let expected = ["a", "b", "cd", "efg"];
        assert_eq!(tokens.len(), expected.len());

        for (i, token) in tokens.iter().enumerate() {
            assert_eq!(token, expected[i]);
        }
    }

    #[test]
    fn test_tokenize_edge_cases_punctuation_only() {
        let (bm25, _temp_dir) = setup_bm25_config();

        let tokens = bm25.tokenize::<true>("!@#$%^&*()");
        assert_eq!(tokens.len(), 0);
    }

    #[test]
    fn test_insert_document() {
        let (bm25, _temp_dir) = setup_bm25_config();
        let mut wtxn = bm25.graph_env.write_txn().unwrap();

        let doc_id = 123u128;
        let doc = "The quick brown fox jumps over the lazy dog";

        let result = bm25.insert_doc(&mut wtxn, doc_id, doc);
        assert!(result.is_ok());

        // check that document length was stored
        let doc_length = bm25.doc_lengths_db.get(&wtxn, &doc_id).unwrap();
        assert!(doc_length.is_some());
        assert!(doc_length.unwrap() > 0);

        // check that metadata was updated
        let metadata_bytes = bm25.metadata_db.get(&wtxn, METADATA_KEY).unwrap();
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

        // check metadata
        let metadata_bytes = bm25.metadata_db.get(&wtxn, METADATA_KEY).unwrap().unwrap();
        let metadata: BM25Metadata = bincode::deserialize(metadata_bytes).unwrap();
        assert_eq!(metadata.total_docs, 3);

        wtxn.commit().unwrap();
    }

    #[test]
    fn test_search_single_term() {
        let (bm25, _temp_dir) = setup_bm25_config();
        let mut wtxn = bm25.graph_env.write_txn().unwrap();
        let arena = Bump::new();

        // model properties list stored in nodes
        let props1: HashMap<String, Value> = HashMap::from([
            (
                "label1".to_string(),
                Value::String("Swift shadow leaps".to_string()),
            ),
            (
                "label2".to_string(),
                Value::String("Idle fox wolf rests".to_string()),
            ),
        ]);

        let props2: HashMap<String, Value> = HashMap::from([
            (
                "label1".to_string(),
                Value::String("Rapid hare bounds".to_string()),
            ),
            (
                "label2".to_string(),
                Value::String("Quiet bear naps".to_string()),
            ),
        ]);

        let props3: HashMap<String, Value> = HashMap::from([
            (
                "label1".to_string(),
                Value::String("Fleet deer fox sprints".to_string()),
            ),
            (
                "label2".to_string(),
                Value::String("Calm owl dozes".to_string()),
            ),
        ]);

        let nodes = [props1, props2, props3];

        for (i, props) in nodes.iter().enumerate() {
            let props_map = ImmutablePropertiesMap::new(
                props.len(),
                props
                    .iter()
                    .map(|(k, v)| (arena.alloc_str(k) as &str, v.clone())),
                &arena,
            );
            let data = props_map.flatten_bm25();
            bm25.insert_doc(&mut wtxn, i as u128, &data).unwrap();
        }
        wtxn.commit().unwrap();

        // search for "fox"
        let rtxn = bm25.graph_env.read_txn().unwrap();
        let arena = Bump::new();
        let results = bm25.search(&rtxn, "fox", 10, &arena).unwrap();

        println!("results: {results:?}");

        // should return documents 1 and 3 (both contain "fox")
        assert_eq!(results.len(), 2);

        let doc_ids: Vec<u128> = results.iter().map(|(id, _)| *id).collect();
        assert!(doc_ids.contains(&0u128));
        assert!(doc_ids.contains(&2u128));

        // scores should be positive
        for (_, score) in &results {
            assert!(*score != 0.0);
        }
    }

    #[test]
    fn test_search_multiple_terms() {
        let (bm25, _temp_dir) = setup_bm25_config();
        let mut wtxn = bm25.graph_env.write_txn().unwrap();
        let arena = Bump::new();

        let props1: HashMap<String, Value> = HashMap::from([
            (
                "label1".to_string(),
                Value::String("machine learning algorithms for data science".to_string()),
            ),
            (
                "label2".to_string(),
                Value::String("deep learning".to_string()),
            ),
        ]);

        let props2: HashMap<String, Value> = HashMap::from([
            (
                "label1".to_string(),
                Value::String("deep learning neural networks".to_string()),
            ),
            ("label2".to_string(), Value::I64(6969)),
        ]);

        let props3: HashMap<String, Value> = HashMap::from([
            (
                "label1".to_string(),
                Value::String("data analysis and machine learning".to_string()),
            ),
            (
                "label2".to_string(),
                Value::String("natural language processing".to_string()),
            ),
        ]);

        let nodes = [props1, props2, props3];

        for (i, props) in nodes.iter().enumerate() {
            let props_map = ImmutablePropertiesMap::new(
                props.len(),
                props
                    .iter()
                    .map(|(k, v)| (arena.alloc_str(k) as &str, v.clone())),
                &arena,
            );
            let data = props_map.flatten_bm25();
            bm25.insert_doc(&mut wtxn, i as u128, &data).unwrap();
        }
        wtxn.commit().unwrap();

        let rtxn = bm25.graph_env.read_txn().unwrap();
        let arena = Bump::new();
        let results = bm25.search(&rtxn, "machine learning", 10, &arena).unwrap();

        println!("results: {results:?}");

        // documents 1 and 3 should score highest (contain both terms)
        assert!(results.len() >= 2);

        let doc_ids: Vec<u128> = results.iter().map(|(id, _)| *id).collect();
        assert!(doc_ids.contains(&0u128));
        assert!(doc_ids.contains(&1u128));
        assert!(doc_ids.contains(&2u128));
    }

    #[test]
    fn test_search_many_terms() {
        let (bm25, _temp_dir) = setup_bm25_config();
        let mut wtxn = bm25.graph_env.write_txn().unwrap();
        let arena = Bump::new();

        let props1: HashMap<String, Value> = HashMap::from([
            (
                "label1".to_string(),
                Value::String("machine learning algorithms for data science".to_string()),
            ),
            (
                "label2".to_string(),
                Value::String("deep learning".to_string()),
            ),
            (
                "label3".to_string(),
                Value::String("neural networks optimization".to_string()),
            ),
            (
                "label4".to_string(),
                Value::String("data analysis techniques".to_string()),
            ),
        ]);

        let props2: HashMap<String, Value> = HashMap::from([
            (
                "label1".to_string(),
                Value::String("deep learning neural networks".to_string()),
            ),
            (
                "label2".to_string(),
                Value::String("machine learning".to_string()),
            ),
            (
                "label3".to_string(),
                Value::String("computer vision models".to_string()),
            ),
            (
                "label4".to_string(),
                Value::String("reinforcement learning".to_string()),
            ),
        ]);

        let props3: HashMap<String, Value> = HashMap::from([
            (
                "label1".to_string(),
                Value::String("data analysis and machine learning".to_string()),
            ),
            (
                "label2".to_string(),
                Value::String("natural language processing".to_string()),
            ),
            (
                "label3".to_string(),
                Value::String("sentiment analysis".to_string()),
            ),
            (
                "label4".to_string(),
                Value::String("text mining".to_string()),
            ),
        ]);

        let props4: HashMap<String, Value> = HashMap::from([
            (
                "label1".to_string(),
                Value::String("machine learning for predictive analytics".to_string()),
            ),
            (
                "label2".to_string(),
                Value::String("deep learning frameworks".to_string()),
            ),
            (
                "label3".to_string(),
                Value::String("image recognition".to_string()),
            ),
            (
                "label4".to_string(),
                Value::String("data preprocessing".to_string()),
            ),
        ]);

        let props5: HashMap<String, Value> = HashMap::from([
            (
                "label1".to_string(),
                Value::String("neural networks for data science".to_string()),
            ),
            (
                "label2".to_string(),
                Value::String("machine learning pipelines".to_string()),
            ),
            (
                "label3".to_string(),
                Value::String("feature engineering".to_string()),
            ),
            (
                "label4".to_string(),
                Value::String("model evaluation".to_string()),
            ),
        ]);

        let props6: HashMap<String, Value> = HashMap::from([
            (
                "label1".to_string(),
                Value::String("deep learning for image processing".to_string()),
            ),
            (
                "label2".to_string(),
                Value::String("machine learning models".to_string()),
            ),
            (
                "label3".to_string(),
                Value::String("clustering algorithms".to_string()),
            ),
            (
                "label4".to_string(),
                Value::String("dimensionality reduction".to_string()),
            ),
        ]);

        let props7: HashMap<String, Value> = HashMap::from([
            (
                "label1".to_string(),
                Value::String("natural language processing techniques".to_string()),
            ),
            (
                "label2".to_string(),
                Value::String("machine learning applications".to_string()),
            ),
            (
                "label3".to_string(),
                Value::String("text classification".to_string()),
            ),
            (
                "label4".to_string(),
                Value::String("data visualization".to_string()),
            ),
        ]);

        let props8: HashMap<String, Value> = HashMap::from([
            (
                "label1".to_string(),
                Value::String("machine learning for time series".to_string()),
            ),
            (
                "label2".to_string(),
                Value::String("deep learning architectures".to_string()),
            ),
            (
                "label3".to_string(),
                Value::String("anomaly detection".to_string()),
            ),
            (
                "label4".to_string(),
                Value::String("predictive modeling".to_string()),
            ),
        ]);

        let props9: HashMap<String, Value> = HashMap::from([
            (
                "label1".to_string(),
                Value::String("data science with machine learning".to_string()),
            ),
            (
                "label2".to_string(),
                Value::String("neural networks training".to_string()),
            ),
            (
                "label3".to_string(),
                Value::String("regression analysis".to_string()),
            ),
            (
                "label4".to_string(),
                Value::String("model optimization".to_string()),
            ),
        ]);

        let props10: HashMap<String, Value> = HashMap::from([
            (
                "label1".to_string(),
                Value::String("deep learning for speech recognition".to_string()),
            ),
            (
                "label2".to_string(),
                Value::String("machine learning workflows".to_string()),
            ),
            (
                "label3".to_string(),
                Value::String("audio processing".to_string()),
            ),
            (
                "label4".to_string(),
                Value::String("data augmentation".to_string()),
            ),
        ]);

        let props11: HashMap<String, Value> = HashMap::from([
            (
                "label1".to_string(),
                Value::String("machine learning for fraud detection".to_string()),
            ),
            (
                "label2".to_string(),
                Value::String("deep learning systems".to_string()),
            ),
            (
                "label3".to_string(),
                Value::String("pattern recognition".to_string()),
            ),
            (
                "label4".to_string(),
                Value::String("data cleaning".to_string()),
            ),
        ]);

        let props12: HashMap<String, Value> = HashMap::from([
            (
                "label1".to_string(),
                Value::String("natural language processing models".to_string()),
            ),
            (
                "label2".to_string(),
                Value::String("machine learning algorithms".to_string()),
            ),
            (
                "label3".to_string(),
                Value::String("topic modeling".to_string()),
            ),
            (
                "label4".to_string(),
                Value::String("text analytics".to_string()),
            ),
        ]);

        let props13: HashMap<String, Value> = HashMap::from([
            (
                "label1".to_string(),
                Value::String("machine learning for recommendation systems".to_string()),
            ),
            (
                "label2".to_string(),
                Value::String("deep learning techniques".to_string()),
            ),
            (
                "label3".to_string(),
                Value::String("collaborative filtering".to_string()),
            ),
            (
                "label4".to_string(),
                Value::String("user profiling".to_string()),
            ),
        ]);

        let props14: HashMap<String, Value> = HashMap::from([
            (
                "label1".to_string(),
                Value::String("data science and neural networks".to_string()),
            ),
            (
                "label2".to_string(),
                Value::String("machine learning strategies".to_string()),
            ),
            (
                "label3".to_string(),
                Value::String("classification models".to_string()),
            ),
            (
                "label4".to_string(),
                Value::String("data exploration".to_string()),
            ),
        ]);

        let props15: HashMap<String, Value> = HashMap::from([
            (
                "label1".to_string(),
                Value::String("deep learning for object detection".to_string()),
            ),
            (
                "label2".to_string(),
                Value::String("machine learning tools".to_string()),
            ),
            (
                "label3".to_string(),
                Value::String("image segmentation".to_string()),
            ),
            (
                "label4".to_string(),
                Value::String("feature extraction".to_string()),
            ),
        ]);

        let props16: HashMap<String, Value> = HashMap::from([
            (
                "label1".to_string(),
                Value::String("machine learning for customer segmentation".to_string()),
            ),
            (
                "label2".to_string(),
                Value::String("deep learning applications".to_string()),
            ),
            (
                "label3".to_string(),
                Value::String("market analysis".to_string()),
            ),
            (
                "label4".to_string(),
                Value::String("data clustering".to_string()),
            ),
        ]);

        let props17: HashMap<String, Value> = HashMap::from([
            (
                "label1".to_string(),
                Value::String("natural language processing for chatbots".to_string()),
            ),
            (
                "label2".to_string(),
                Value::String("machine learning frameworks".to_string()),
            ),
            (
                "label3".to_string(),
                Value::String("dialogue systems".to_string()),
            ),
            (
                "label4".to_string(),
                Value::String("text generation".to_string()),
            ),
        ]);

        let props18: HashMap<String, Value> = HashMap::from([
            (
                "label1".to_string(),
                Value::String("machine learning for predictive maintenance".to_string()),
            ),
            (
                "label2".to_string(),
                Value::String("deep learning models".to_string()),
            ),
            (
                "label3".to_string(),
                Value::String("equipment monitoring".to_string()),
            ),
            (
                "label4".to_string(),
                Value::String("failure prediction".to_string()),
            ),
        ]);

        let props19: HashMap<String, Value> = HashMap::from([
            (
                "label1".to_string(),
                Value::String("data science with deep learning".to_string()),
            ),
            (
                "label2".to_string(),
                Value::String("machine learning techniques".to_string()),
            ),
            (
                "label3".to_string(),
                Value::String("statistical modeling".to_string()),
            ),
            (
                "label4".to_string(),
                Value::String("data interpretation".to_string()),
            ),
        ]);

        let props20: HashMap<String, Value> = HashMap::from([
            (
                "label1".to_string(),
                Value::String("deep learning for facial recognition".to_string()),
            ),
            (
                "label2".to_string(),
                Value::String("machine learning processes".to_string()),
            ),
            (
                "label3".to_string(),
                Value::String("biometric analysis".to_string()),
            ),
            (
                "label4".to_string(),
                Value::String("identity verification".to_string()),
            ),
        ]);

        let props21: HashMap<String, Value> = HashMap::from([
            (
                "label1".to_string(),
                Value::String("machine learning for supply chain".to_string()),
            ),
            (
                "label2".to_string(),
                Value::String("deep learning optimization".to_string()),
            ),
            (
                "label3".to_string(),
                Value::String("inventory management".to_string()),
            ),
            (
                "label4".to_string(),
                Value::String("demand forecasting".to_string()),
            ),
        ]);

        let props22: HashMap<String, Value> = HashMap::from([
            (
                "label1".to_string(),
                Value::String("natural language processing for sentiment".to_string()),
            ),
            (
                "label2".to_string(),
                Value::String("machine learning solutions".to_string()),
            ),
            (
                "label3".to_string(),
                Value::String("opinion mining".to_string()),
            ),
            (
                "label4".to_string(),
                Value::String("text processing".to_string()),
            ),
        ]);

        let props23: HashMap<String, Value> = HashMap::from([
            (
                "label1".to_string(),
                Value::String("machine learning for risk assessment".to_string()),
            ),
            (
                "label2".to_string(),
                Value::String("deep learning algorithms".to_string()),
            ),
            (
                "label3".to_string(),
                Value::String("probability analysis".to_string()),
            ),
            (
                "label4".to_string(),
                Value::String("data modeling".to_string()),
            ),
        ]);

        let props24: HashMap<String, Value> = HashMap::from([
            (
                "label1".to_string(),
                Value::String("data science for business intelligence".to_string()),
            ),
            (
                "label2".to_string(),
                Value::String("machine learning insights".to_string()),
            ),
            (
                "label3".to_string(),
                Value::String("decision support".to_string()),
            ),
            (
                "label4".to_string(),
                Value::String("data reporting".to_string()),
            ),
        ]);

        let props25: HashMap<String, Value> = HashMap::from([
            (
                "label1".to_string(),
                Value::String("deep learning for autonomous vehicles".to_string()),
            ),
            (
                "label2".to_string(),
                Value::String("machine learning systems".to_string()),
            ),
            (
                "label3".to_string(),
                Value::String("path planning".to_string()),
            ),
            (
                "label4".to_string(),
                Value::String("sensor fusion".to_string()),
            ),
        ]);

        let props26: HashMap<String, Value> = HashMap::from([
            (
                "label1".to_string(),
                Value::String("machine learning for healthcare".to_string()),
            ),
            (
                "label2".to_string(),
                Value::String("deep learning diagnostics".to_string()),
            ),
            (
                "label3".to_string(),
                Value::String("medical imaging".to_string()),
            ),
            (
                "label4".to_string(),
                Value::String("patient data analysis".to_string()),
            ),
        ]);

        let props27: HashMap<String, Value> = HashMap::from([
            (
                "label1".to_string(),
                Value::String("natural language processing for translation".to_string()),
            ),
            (
                "label2".to_string(),
                Value::String("machine learning models".to_string()),
            ),
            (
                "label3".to_string(),
                Value::String("language models".to_string()),
            ),
            (
                "label4".to_string(),
                Value::String("text translation".to_string()),
            ),
        ]);

        let props28: HashMap<String, Value> = HashMap::from([
            (
                "label1".to_string(),
                Value::String("machine learning for energy optimization".to_string()),
            ),
            (
                "label2".to_string(),
                Value::String("deep learning strategies".to_string()),
            ),
            (
                "label3".to_string(),
                Value::String("energy forecasting".to_string()),
            ),
            (
                "label4".to_string(),
                Value::String("resource allocation".to_string()),
            ),
        ]);

        let props29: HashMap<String, Value> = HashMap::from([
            (
                "label1".to_string(),
                Value::String("data science for marketing".to_string()),
            ),
            (
                "label2".to_string(),
                Value::String("machine learning analytics".to_string()),
            ),
            (
                "label3".to_string(),
                Value::String("customer insights".to_string()),
            ),
            (
                "label4".to_string(),
                Value::String("campaign analysis".to_string()),
            ),
        ]);

        let props30: HashMap<String, Value> = HashMap::from([
            (
                "label1".to_string(),
                Value::String("deep learning for video analysis".to_string()),
            ),
            (
                "label2".to_string(),
                Value::String("machine learning pipelines".to_string()),
            ),
            (
                "label3".to_string(),
                Value::String("motion detection".to_string()),
            ),
            (
                "label4".to_string(),
                Value::String("frame analysis".to_string()),
            ),
        ]);

        let props31: HashMap<String, Value> = HashMap::from([
            (
                "label1".to_string(),
                Value::String("machine learning for cybersecurity".to_string()),
            ),
            (
                "label2".to_string(),
                Value::String("deep learning detection".to_string()),
            ),
            (
                "label3".to_string(),
                Value::String("threat analysis".to_string()),
            ),
            (
                "label4".to_string(),
                Value::String("network security".to_string()),
            ),
        ]);

        let props32: HashMap<String, Value> = HashMap::from([
            (
                "label1".to_string(),
                Value::String("natural language processing for summarization".to_string()),
            ),
            (
                "label2".to_string(),
                Value::String("machine learning techniques".to_string()),
            ),
            (
                "label3".to_string(),
                Value::String("text summarization".to_string()),
            ),
            (
                "label4".to_string(),
                Value::String("content analysis".to_string()),
            ),
        ]);

        let props33: HashMap<String, Value> = HashMap::from([
            (
                "label1".to_string(),
                Value::String("machine learning for logistics".to_string()),
            ),
            (
                "label2".to_string(),
                Value::String("deep learning optimization".to_string()),
            ),
            (
                "label3".to_string(),
                Value::String("route planning".to_string()),
            ),
            (
                "label4".to_string(),
                Value::String("supply chain analytics".to_string()),
            ),
        ]);

        let props34: HashMap<String, Value> = HashMap::from([
            (
                "label1".to_string(),
                Value::String("data science for finance".to_string()),
            ),
            (
                "label2".to_string(),
                Value::String("machine learning predictions".to_string()),
            ),
            (
                "label3".to_string(),
                Value::String("market forecasting".to_string()),
            ),
            (
                "label4".to_string(),
                Value::String("risk modeling".to_string()),
            ),
        ]);

        let props35: HashMap<String, Value> = HashMap::from([
            (
                "label1".to_string(),
                Value::String("deep learning for robotics".to_string()),
            ),
            (
                "label2".to_string(),
                Value::String("machine learning algorithms".to_string()),
            ),
            (
                "label3".to_string(),
                Value::String("motion planning".to_string()),
            ),
            (
                "label4".to_string(),
                Value::String("sensor processing".to_string()),
            ),
        ]);

        let props36: HashMap<String, Value> = HashMap::from([
            (
                "label1".to_string(),
                Value::String("machine learning for agriculture".to_string()),
            ),
            (
                "label2".to_string(),
                Value::String("deep learning applications".to_string()),
            ),
            (
                "label3".to_string(),
                Value::String("crop monitoring".to_string()),
            ),
            (
                "label4".to_string(),
                Value::String("yield prediction".to_string()),
            ),
        ]);

        let props37: HashMap<String, Value> = HashMap::from([
            (
                "label1".to_string(),
                Value::String("natural language processing for search".to_string()),
            ),
            (
                "label2".to_string(),
                Value::String("machine learning systems".to_string()),
            ),
            (
                "label3".to_string(),
                Value::String("query processing".to_string()),
            ),
            (
                "label4".to_string(),
                Value::String("search optimization".to_string()),
            ),
        ]);

        let props38: HashMap<String, Value> = HashMap::from([
            (
                "label1".to_string(),
                Value::String("machine learning for retail".to_string()),
            ),
            (
                "label2".to_string(),
                Value::String("deep learning models".to_string()),
            ),
            (
                "label3".to_string(),
                Value::String("sales forecasting".to_string()),
            ),
            (
                "label4".to_string(),
                Value::String("inventory optimization".to_string()),
            ),
        ]);

        let props39: HashMap<String, Value> = HashMap::from([
            (
                "label1".to_string(),
                Value::String("data science for education".to_string()),
            ),
            (
                "label2".to_string(),
                Value::String("machine learning tools".to_string()),
            ),
            (
                "label3".to_string(),
                Value::String("student performance".to_string()),
            ),
            (
                "label4".to_string(),
                Value::String("learning analytics".to_string()),
            ),
        ]);

        let props40: HashMap<String, Value> = HashMap::from([
            (
                "label1".to_string(),
                Value::String("deep learning for gaming".to_string()),
            ),
            (
                "label2".to_string(),
                Value::String("machine learning strategies".to_string()),
            ),
            ("label3".to_string(), Value::String("game AI".to_string())),
            (
                "label4".to_string(),
                Value::String("player behavior".to_string()),
            ),
        ]);

        let props41: HashMap<String, Value> = HashMap::from([
            (
                "label1".to_string(),
                Value::String("machine learning for transportation".to_string()),
            ),
            (
                "label2".to_string(),
                Value::String("deep learning frameworks".to_string()),
            ),
            (
                "label3".to_string(),
                Value::String("traffic prediction".to_string()),
            ),
            (
                "label4".to_string(),
                Value::String("route optimization".to_string()),
            ),
        ]);

        let props42: HashMap<String, Value> = HashMap::from([
            (
                "label1".to_string(),
                Value::String("natural language processing for legal".to_string()),
            ),
            (
                "label2".to_string(),
                Value::String("machine learning applications".to_string()),
            ),
            (
                "label3".to_string(),
                Value::String("document analysis".to_string()),
            ),
            (
                "label4".to_string(),
                Value::String("contract review".to_string()),
            ),
        ]);

        let props43: HashMap<String, Value> = HashMap::from([
            (
                "label1".to_string(),
                Value::String("machine learning for manufacturing".to_string()),
            ),
            (
                "label2".to_string(),
                Value::String("deep learning systems".to_string()),
            ),
            (
                "label3".to_string(),
                Value::String("quality control".to_string()),
            ),
            (
                "label4".to_string(),
                Value::String("process optimization".to_string()),
            ),
        ]);

        let props44: HashMap<String, Value> = HashMap::from([
            (
                "label1".to_string(),
                Value::String("data science for e-commerce".to_string()),
            ),
            (
                "label2".to_string(),
                Value::String("machine learning insights".to_string()),
            ),
            (
                "label3".to_string(),
                Value::String("product recommendation".to_string()),
            ),
            (
                "label4".to_string(),
                Value::String("customer segmentation".to_string()),
            ),
        ]);

        let props45: HashMap<String, Value> = HashMap::from([
            (
                "label1".to_string(),
                Value::String("deep learning for environmental monitoring".to_string()),
            ),
            (
                "label2".to_string(),
                Value::String("machine learning models".to_string()),
            ),
            (
                "label3".to_string(),
                Value::String("climate analysis".to_string()),
            ),
            (
                "label4".to_string(),
                Value::String("sensor data".to_string()),
            ),
        ]);

        let props46: HashMap<String, Value> = HashMap::from([
            (
                "label1".to_string(),
                Value::String("machine learning for sports analytics".to_string()),
            ),
            (
                "label2".to_string(),
                Value::String("deep learning techniques".to_string()),
            ),
            (
                "label3".to_string(),
                Value::String("player performance".to_string()),
            ),
            (
                "label4".to_string(),
                Value::String("game strategy".to_string()),
            ),
        ]);

        let props47: HashMap<String, Value> = HashMap::from([
            (
                "label1".to_string(),
                Value::String("natural language processing for news".to_string()),
            ),
            (
                "label2".to_string(),
                Value::String("machine learning algorithms".to_string()),
            ),
            (
                "label3".to_string(),
                Value::String("article classification".to_string()),
            ),
            (
                "label4".to_string(),
                Value::String("content summarization".to_string()),
            ),
        ]);

        let props48: HashMap<String, Value> = HashMap::from([
            (
                "label1".to_string(),
                Value::String("machine learning for urban planning".to_string()),
            ),
            (
                "label2".to_string(),
                Value::String("deep learning applications".to_string()),
            ),
            (
                "label3".to_string(),
                Value::String("traffic modeling".to_string()),
            ),
            (
                "label4".to_string(),
                Value::String("resource planning".to_string()),
            ),
        ]);

        let props49: HashMap<String, Value> = HashMap::from([
            (
                "label1".to_string(),
                Value::String("data science for telecommunications".to_string()),
            ),
            (
                "label2".to_string(),
                Value::String("machine learning systems".to_string()),
            ),
            (
                "label3".to_string(),
                Value::String("network optimization".to_string()),
            ),
            (
                "label4".to_string(),
                Value::String("customer analytics".to_string()),
            ),
        ]);

        let props50: HashMap<String, Value> = HashMap::from([
            (
                "label1".to_string(),
                Value::String("deep learning for astronomy".to_string()),
            ),
            (
                "label2".to_string(),
                Value::String("machine learning frameworks".to_string()),
            ),
            (
                "label3".to_string(),
                Value::String("star classification".to_string()),
            ),
            (
                "label4".to_string(),
                Value::String("cosmic data analysis".to_string()),
            ),
        ]);

        let nodes = [
            props1, props2, props3, props4, props5, props6, props7, props8, props9, props10,
            props11, props12, props13, props14, props15, props16, props17, props18, props19,
            props20, props21, props22, props23, props24, props25, props26, props27, props28,
            props29, props30, props31, props32, props33, props34, props35, props36, props37,
            props38, props39, props40, props41, props42, props43, props44, props45, props46,
            props47, props48, props49, props50,
        ];

        for (i, props) in nodes.iter().enumerate() {
            let props_map = ImmutablePropertiesMap::new(
                props.len(),
                props
                    .iter()
                    .map(|(k, v)| (arena.alloc_str(k) as &str, v.clone())),
                &arena,
            );
            let data = props_map.flatten_bm25();
            bm25.insert_doc(&mut wtxn, i as u128, &data).unwrap();
            println!("{data:?}");
        }
        wtxn.commit().unwrap();

        let rtxn = bm25.graph_env.read_txn().unwrap();
        let arena = Bump::new();
        let results = bm25.search(&rtxn, "science", 10, &arena).unwrap();

        println!("results: {results:?}");

        assert!(results.len() >= 2);

        let doc_ids: Vec<u128> = results.iter().map(|(id, _)| *id).collect();
        assert!(doc_ids.contains(&38u128));
        assert!(doc_ids.contains(&43u128));
        assert!(doc_ids.contains(&28u128));
        assert!(doc_ids.contains(&33u128));
        assert!(doc_ids.contains(&48u128));
        assert!(doc_ids.contains(&18u128));
        assert!(doc_ids.contains(&8u128));
        assert!(doc_ids.contains(&13u128));
        assert!(doc_ids.contains(&23u128));
    }

    #[test]
    fn test_bm25_score_calculation() {
        let (bm25, _temp_dir) = setup_bm25_config();

        let score = bm25.calculate_bm25_score(
            2,   // term frequency
            10,  // doc length
            3,   // document frequency
            100, // total docs
            8.0, // average doc length
        );

        println!("score {score}");

        // Score should be finite and reasonable
        assert!(score.is_finite());
        assert!(score != 0.0);
    }

    #[test]
    fn test_update_document() {
        let (bm25, _temp_dir) = setup_bm25_config();
        let mut wtxn = bm25.graph_env.write_txn().unwrap();

        let doc_id = 1u128;

        // insert original document
        bm25.insert_doc(&mut wtxn, doc_id, "original content")
            .unwrap();

        // update document
        bm25.update_doc(&mut wtxn, doc_id, "updated content with more words")
            .unwrap();

        // check that document length was updated
        let doc_length = bm25.doc_lengths_db.get(&wtxn, &doc_id).unwrap().unwrap();
        assert!(doc_length > 2); // Should reflect the new document length

        wtxn.commit().unwrap();

        // search should find the updated content
        let rtxn = bm25.graph_env.read_txn().unwrap();
        let arena = Bump::new();
        let results = bm25.search(&rtxn, "updated", 10, &arena).unwrap();
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

        // insert documents
        for (doc_id, doc) in &docs {
            bm25.insert_doc(&mut wtxn, *doc_id, doc).unwrap();
        }

        // delete document 2
        bm25.delete_doc(&mut wtxn, 2u128).unwrap();

        // check that document length was removed
        let doc_length = bm25.doc_lengths_db.get(&wtxn, &2u128).unwrap();
        assert!(doc_length.is_none());

        // check that metadata was updated
        let metadata_bytes = bm25.metadata_db.get(&wtxn, METADATA_KEY).unwrap().unwrap();
        let metadata: BM25Metadata = bincode::deserialize(metadata_bytes).unwrap();
        assert_eq!(metadata.total_docs, 2); // Should be reduced by 1

        wtxn.commit().unwrap();

        // search should not find the deleted document
        let rtxn = bm25.graph_env.read_txn().unwrap();
        let arena = Bump::new();
        let results = bm25.search(&rtxn, "two", 10, &arena).unwrap();
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_search_with_limit() {
        let (bm25, _temp_dir) = setup_bm25_config();
        let mut wtxn = bm25.graph_env.write_txn().unwrap();

        // insert many documents containing the same term
        for i in 1..=10 {
            let doc = format!("document {i} contains test content");
            bm25.insert_doc(&mut wtxn, i as u128, &doc).unwrap();
        }
        wtxn.commit().unwrap();

        let rtxn = bm25.graph_env.read_txn().unwrap();
        let arena = Bump::new();
        let results = bm25.search(&rtxn, "test", 5, &arena).unwrap();

        // should respect the limit
        assert_eq!(results.len(), 5);

        // results should be sorted by score (descending)
        for i in 1..results.len() {
            assert!(results[i - 1].1 >= results[i].1);
        }
    }

    #[test]
    fn test_search_no_results() {
        let (bm25, _temp_dir) = setup_bm25_config();
        let mut wtxn = bm25.graph_env.write_txn().unwrap();

        bm25.insert_doc(&mut wtxn, 1u128, "some document content")
            .unwrap();
        wtxn.commit().unwrap();

        let rtxn = bm25.graph_env.read_txn().unwrap();
        let arena = Bump::new();
        let results = bm25.search(&rtxn, "nonexistent", 10, &arena).unwrap();

        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_edge_cases_empty_document() {
        let (bm25, _temp_dir) = setup_bm25_config();
        let mut wtxn = bm25.graph_env.write_txn().unwrap();

        // Insert empty document
        let result = bm25.insert_doc(&mut wtxn, 1u128, "");
        assert!(result.is_ok());

        // document length should be 0
        let doc_length = bm25.doc_lengths_db.get(&wtxn, &1u128).unwrap().unwrap();
        assert_eq!(doc_length, 0);

        wtxn.commit().unwrap();
    }

    #[tokio::test]
    async fn test_hybrid_search() {
        let (storage, _temp_dir) = setup_helix_storage();

        let mut wtxn = storage.graph_env.write_txn().unwrap();
        let docs = vec![
            (1u128, "machine learning algorithms"),
            (2u128, "deep learning neural networks"),
            (3u128, "data science methods"),
        ];

        let bm25 = storage.bm25.as_ref().unwrap();
        for (doc_id, doc) in &docs {
            bm25.insert_doc(&mut wtxn, *doc_id, doc).unwrap();
        }
        wtxn.commit().unwrap();

        let mut wtxn = storage.graph_env.write_txn().unwrap();
        let vectors = generate_random_vectors(800, 650);
        let mut arena = Bump::new();
        for vec in &vectors {
            let slice = arena.alloc_slice_copy(vec.as_slice());
            let _ = storage
                .vectors
                .insert::<fn(&HVector, &RoTxn) -> bool>(&mut wtxn, "vector", slice, None, &arena);
            arena.reset();
        }
        wtxn.commit().unwrap();

        let query = "machine learning";
        let query_vector = generate_random_vectors(1, 650);
        let alpha = 0.5; // equal weight between BM25 and vector
        let limit = 10;

        let result = storage
            .hybrid_search(query, &query_vector[0], alpha, limit)
            .await;

        match result {
            Ok(results) => assert!(results.len() <= limit),
            Err(_) => println!("Vector search not available"),
        }
    }

    #[tokio::test]
    async fn test_hybrid_search_alpha_vectors() {
        let (storage, _temp_dir) = setup_helix_storage();

        // Insert some test documents first
        let mut wtxn = storage.graph_env.write_txn().unwrap();
        let docs = vec![
            (1u128, "machine learning algorithms"),
            (2u128, "deep learning neural networks"),
            (3u128, "data science methods"),
        ];

        let bm25 = storage.bm25.as_ref().unwrap();
        for (doc_id, doc) in &docs {
            bm25.insert_doc(&mut wtxn, *doc_id, doc).unwrap();
        }
        wtxn.commit().unwrap();

        let mut wtxn = storage.graph_env.write_txn().unwrap();
        let vectors = generate_random_vectors(800, 650);
        let mut arena = Bump::new();
        for vec in &vectors {
            let slice = arena.alloc_slice_copy(vec.as_slice());
            let _ = storage
                .vectors
                .insert::<fn(&HVector, &RoTxn) -> bool>(&mut wtxn, "vector", slice, None, &arena);
            arena.reset();
        }
        wtxn.commit().unwrap();

        let query = "machine learning";
        let query_vector = generate_random_vectors(1, 650);

        // alpha = 0.0 (Vector only)
        let results_vector_only = storage
            .hybrid_search(query, &query_vector[0], 0.0, 10)
            .await;

        match results_vector_only {
            Ok(results) => assert!(results.len() <= 10),
            Err(_) => {
                println!("Vector-only search failed")
            }
        }
    }

    #[tokio::test]
    async fn test_hybrid_search_alpha_bm25() {
        let (storage, _temp_dir) = setup_helix_storage();

        // Insert some test documents first
        let mut wtxn = storage.graph_env.write_txn().unwrap();
        let docs = vec![
            (1u128, "machine learning algorithms"),
            (2u128, "deep learning neural networks"),
            (3u128, "data science methods"),
        ];

        let bm25 = storage.bm25.as_ref().unwrap();
        for (doc_id, doc) in &docs {
            bm25.insert_doc(&mut wtxn, *doc_id, doc).unwrap();
        }
        wtxn.commit().unwrap();

        let mut wtxn = storage.graph_env.write_txn().unwrap();
        let vectors = generate_random_vectors(800, 650);
        let mut arena = Bump::new();
        for vec in &vectors {
            let slice = arena.alloc_slice_copy(vec.as_slice());
            let _ = storage
                .vectors
                .insert::<fn(&HVector, &RoTxn) -> bool>(&mut wtxn, "vector", slice, None, &arena);
            arena.reset();
        }
        wtxn.commit().unwrap();

        let query = "machine learning";
        let query_vector = generate_random_vectors(1, 650);

        // alpha = 1.0 (BM25 only)
        let results_bm25_only = storage
            .hybrid_search(query, &query_vector[0], 1.0, 10)
            .await;

        // all should be valid results or acceptable errors
        match results_bm25_only {
            Ok(results) => assert!(results.len() <= 10),
            Err(_) => println!("BM25-only search failed"),
        }
    }

    #[test]
    fn test_bm25_score_properties() {
        let (bm25, _temp_dir) = setup_bm25_config();

        // test that higher term frequency yields higher score
        let score1 = bm25.calculate_bm25_score(1, 10, 5, 100, 10.0);
        let score2 = bm25.calculate_bm25_score(3, 10, 5, 100, 10.0);
        assert!(score2 > score1);

        // test that rare terms (lower df) yield higher scores
        let score_rare = bm25.calculate_bm25_score(1, 10, 2, 100, 10.0);
        let score_common = bm25.calculate_bm25_score(1, 10, 50, 100, 10.0);
        assert!(score_rare > score_common);
    }

    #[test]
    fn test_metadata_consistency() {
        let (bm25, _temp_dir) = setup_bm25_config();
        let mut wtxn = bm25.graph_env.write_txn().unwrap();

        let docs = vec![
            (1u128, "short doc"),
            (2u128, "this is a much longer document with many more words"),
            (3u128, "medium length document"),
        ];

        for (doc_id, doc) in &docs {
            bm25.insert_doc(&mut wtxn, *doc_id, doc).unwrap();
        }

        let metadata_bytes = bm25.metadata_db.get(&wtxn, METADATA_KEY).unwrap().unwrap();
        let metadata: BM25Metadata = bincode::deserialize(metadata_bytes).unwrap();

        assert_eq!(metadata.total_docs, 3);
        assert!(metadata.avgdl > 0.0);
        assert_eq!(metadata.k1, 1.2);
        assert_eq!(metadata.b, 0.75);

        bm25.delete_doc(&mut wtxn, 2u128).unwrap();

        // check updated metadata
        let metadata_bytes = bm25.metadata_db.get(&wtxn, METADATA_KEY).unwrap().unwrap();
        let updated_metadata: BM25Metadata = bincode::deserialize(metadata_bytes).unwrap();

        assert_eq!(updated_metadata.total_docs, 2);
        // average document length should be recalculated
        assert_ne!(updated_metadata.avgdl, metadata.avgdl);

        wtxn.commit().unwrap();
    }

    // ============================================================================
    // Additional Edge Case Tests
    // ============================================================================

    #[test]
    fn test_tokenize_empty_string() {
        let (bm25, _temp_dir) = setup_bm25_config();
        let tokens = bm25.tokenize::<true>("");
        assert_eq!(tokens.len(), 0);
    }

    #[test]
    fn test_tokenize_whitespace_only() {
        let (bm25, _temp_dir) = setup_bm25_config();
        let tokens = bm25.tokenize::<true>("   \t\n   ");
        assert_eq!(tokens.len(), 0);
    }

    #[test]
    fn test_tokenize_with_numbers() {
        let (bm25, _temp_dir) = setup_bm25_config();
        let tokens = bm25.tokenize::<true>("test123 456abc");
        assert!(tokens.contains(&"test123".to_string()));
        assert!(tokens.contains(&"456abc".to_string()));
    }

    #[test]
    fn test_tokenize_unicode() {
        let (bm25, _temp_dir) = setup_bm25_config();
        let tokens = bm25.tokenize::<false>("日本語 русский français");
        // Should handle unicode alphanumeric characters
        assert!(!tokens.is_empty());
    }

    #[test]
    fn test_tokenize_mixed_case() {
        let (bm25, _temp_dir) = setup_bm25_config();
        let tokens = bm25.tokenize::<false>("HELLO hello HeLLo");
        // All should be lowercase
        for token in &tokens {
            assert_eq!(token, "hello");
        }
        assert_eq!(tokens.len(), 3);
    }

    #[test]
    fn test_calculate_bm25_score_edge_case_zero_avgdl() {
        let (bm25, _temp_dir) = setup_bm25_config();
        // When avgdl is 0, should use doc_len as fallback
        let score = bm25.calculate_bm25_score(1, 10, 5, 100, 0.0);
        assert!(score.is_finite());
    }

    #[test]
    fn test_calculate_bm25_score_edge_case_zero_total_docs() {
        let (bm25, _temp_dir) = setup_bm25_config();
        // Edge case: 0 total docs (uses max(1))
        let score = bm25.calculate_bm25_score(1, 10, 5, 0, 10.0);
        assert!(score.is_finite());
    }

    #[test]
    fn test_calculate_bm25_score_edge_case_zero_df() {
        let (bm25, _temp_dir) = setup_bm25_config();
        // Edge case: 0 document frequency (uses max(1))
        let score = bm25.calculate_bm25_score(1, 10, 0, 100, 10.0);
        assert!(score.is_finite());
    }

    #[test]
    fn test_calculate_bm25_score_high_df_low_idf() {
        let (bm25, _temp_dir) = setup_bm25_config();
        // When df is very high relative to total_docs, IDF can be negative
        let score = bm25.calculate_bm25_score(1, 10, 95, 100, 10.0);
        // Score should still be finite (may be negative with high df)
        assert!(score.is_finite());
    }

    #[test]
    fn test_calculate_bm25_score_very_short_document() {
        let (bm25, _temp_dir) = setup_bm25_config();
        // Very short document with doc_len = 1
        let score = bm25.calculate_bm25_score(1, 1, 5, 100, 10.0);
        assert!(score.is_finite());
        assert!(score > 0.0);
    }

    #[test]
    fn test_calculate_bm25_score_very_long_document() {
        let (bm25, _temp_dir) = setup_bm25_config();
        // Very long document
        let score = bm25.calculate_bm25_score(5, 10000, 5, 100, 100.0);
        assert!(score.is_finite());
    }

    #[test]
    fn test_search_with_limit_zero() {
        let (bm25, _temp_dir) = setup_bm25_config();
        let mut wtxn = bm25.graph_env.write_txn().unwrap();

        bm25.insert_doc(&mut wtxn, 1u128, "test document content")
            .unwrap();
        wtxn.commit().unwrap();

        let rtxn = bm25.graph_env.read_txn().unwrap();
        let arena = Bump::new();
        let results = bm25.search(&rtxn, "test", 0, &arena).unwrap();

        // With limit 0, should return empty results
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_delete_last_document_avgdl_becomes_zero() {
        let (bm25, _temp_dir) = setup_bm25_config();
        let mut wtxn = bm25.graph_env.write_txn().unwrap();

        // Insert and then delete the only document
        bm25.insert_doc(&mut wtxn, 1u128, "only document").unwrap();
        bm25.delete_doc(&mut wtxn, 1u128).unwrap();

        // Check metadata after deleting last document
        let metadata_bytes = bm25.metadata_db.get(&wtxn, METADATA_KEY).unwrap().unwrap();
        let metadata: BM25Metadata = bincode::deserialize(metadata_bytes).unwrap();

        assert_eq!(metadata.total_docs, 0);
        assert_eq!(metadata.avgdl, 0.0);

        wtxn.commit().unwrap();
    }

    #[test]
    fn test_insert_document_with_repeated_terms() {
        let (bm25, _temp_dir) = setup_bm25_config();
        let mut wtxn = bm25.graph_env.write_txn().unwrap();

        // Document with repeated terms
        bm25.insert_doc(&mut wtxn, 1u128, "test test test unique word word")
            .unwrap();

        // Document length should count all tokens (including duplicates)
        // "test", "test", "test", "unique", "word", "word" = 6 tokens
        let doc_length = bm25.doc_lengths_db.get(&wtxn, &1u128).unwrap().unwrap();
        assert_eq!(doc_length, 6);

        wtxn.commit().unwrap();

        // Search should still work
        let rtxn = bm25.graph_env.read_txn().unwrap();
        let arena = Bump::new();
        let results = bm25.search(&rtxn, "test", 10, &arena).unwrap();
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_search_results_sorted_by_score() {
        let (bm25, _temp_dir) = setup_bm25_config();
        let mut wtxn = bm25.graph_env.write_txn().unwrap();

        // Insert documents with varying relevance to "machine learning"
        let docs = vec![
            (1u128, "machine learning machine learning machine learning"), // High relevance
            (2u128, "machine learning"),                                   // Medium relevance
            (3u128, "learning about machines"),                            // Lower relevance
            (4u128, "machine"),                                            // Lowest
        ];

        for (doc_id, doc) in &docs {
            bm25.insert_doc(&mut wtxn, *doc_id, doc).unwrap();
        }
        wtxn.commit().unwrap();

        let rtxn = bm25.graph_env.read_txn().unwrap();
        let arena = Bump::new();
        let results = bm25.search(&rtxn, "machine learning", 10, &arena).unwrap();

        // Results should be sorted by score (descending)
        for i in 1..results.len() {
            assert!(
                results[i - 1].1 >= results[i].1,
                "Results not sorted: {} < {}",
                results[i - 1].1,
                results[i].1
            );
        }
    }

    #[test]
    fn test_insert_first_document_initializes_metadata() {
        let (bm25, _temp_dir) = setup_bm25_config();
        let mut wtxn = bm25.graph_env.write_txn().unwrap();

        // Before inserting, metadata should not exist
        let metadata_before = bm25.metadata_db.get(&wtxn, METADATA_KEY).unwrap();
        assert!(metadata_before.is_none());

        // Insert first document
        bm25.insert_doc(&mut wtxn, 1u128, "first document").unwrap();

        // After inserting, metadata should exist
        let metadata_bytes = bm25.metadata_db.get(&wtxn, METADATA_KEY).unwrap().unwrap();
        let metadata: BM25Metadata = bincode::deserialize(metadata_bytes).unwrap();

        assert_eq!(metadata.total_docs, 1);
        assert!(metadata.avgdl > 0.0);

        wtxn.commit().unwrap();
    }

    #[test]
    fn test_bm25_temp_config() {
        let (env, _temp_dir) = setup_test_env();
        let mut wtxn = env.write_txn().unwrap();

        // Create temp BM25 config with unique ID
        let config = HBM25Config::new_temp(&env, &mut wtxn, "test_unique_id").unwrap();

        // Should be able to insert and search
        config
            .insert_doc(&mut wtxn, 1u128, "test document")
            .unwrap();
        wtxn.commit().unwrap();

        let rtxn = env.read_txn().unwrap();
        let arena = Bump::new();
        let results = config.search(&rtxn, "test", 10, &arena).unwrap();
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_bm25_flatten_properties() {
        let arena = Bump::new();

        let props: HashMap<String, Value> = HashMap::from([
            (
                "title".to_string(),
                Value::String("Test Document".to_string()),
            ),
            (
                "content".to_string(),
                Value::String("This is content".to_string()),
            ),
            ("count".to_string(), Value::I32(42)),
        ]);

        let props_map = ImmutablePropertiesMap::new(
            props.len(),
            props
                .iter()
                .map(|(k, v)| (arena.alloc_str(k) as &str, v.clone())),
            &arena,
        );

        let flattened = props_map.flatten_bm25();

        // Should contain all keys and values
        assert!(flattened.contains("title"));
        assert!(flattened.contains("Test Document"));
        assert!(flattened.contains("content"));
        assert!(flattened.contains("This is content"));
        assert!(flattened.contains("count"));
        assert!(flattened.contains("42"));
    }
}
