/// cargo test --test bm25_benches --features dev -- --no-capture
#[cfg(feature = "bench")]
mod tests {
    use helix_db::{
        debug_println,
        helix_engine::bm25::bm25::{HBM25Config, BM25},
        utils::{id::v6_uuid, tqdm::tqdm},
    };

    use heed3::{Env, EnvOpenOptions};
    use rand::seq::SliceRandom;
    use reqwest::blocking::get;
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

    /// 5 most frequent words: (the: 27660, and: 26784, i: 22538, to: 19819, of: 18191)
    /// 5 least frequent words: (glowed: 1, lovered: 1, hovered: 1, unexperient: 1, preached: 1)
    fn fetch_shakespeare() -> Result<String, reqwest::Error> {
        get("https://ocw.mit.edu/ans7870/6/6.006/s08/lecturenotes/files/t8.shakespeare.txt")?.text()
    }

    /// Tests the precision (number of docs returned) of the implemented
    /// bm25 search algorithm
    #[test]
    fn test_bm25_precision_short() {
        let (bm25, _temp_dir) = setup_bm25_config();
        let mut wtxn = bm25.graph_env.write_txn().unwrap();

        let mut rng = rand::rng();
        let mut docs = vec![];
        let relevant_count = 4000 as usize;
        let total_docs = 1_000_000;

        for i in tqdm::new(
            0..relevant_count,
            relevant_count,
            None,
            Some("relevant docs"),
        ) {
            let id = v6_uuid();
            let doc = format!("queryterm document {}", i);
            docs.push((id, doc));
        }

        for i in tqdm::new(
            relevant_count..total_docs,
            total_docs - relevant_count,
            None,
            Some("irrelevant docs"),
        ) {
            let id = v6_uuid();
            let doc = format!("document {} other words", i);
            docs.push((id, doc));
        }

        docs.shuffle(&mut rng);
        for (doc_id, doc) in tqdm::new(docs.iter(), total_docs, None, Some("inserting docs")) {
            bm25.insert_doc(&mut wtxn, *doc_id, doc).unwrap();
        }

        wtxn.commit().unwrap();

        let rtxn = bm25.graph_env.read_txn().unwrap();
        let results = bm25.search(&rtxn, "queryterm", relevant_count + 1).unwrap();

        let precision = results.len() as f64 / results.len() as f64;

        assert!(
            precision >= 0.9,
            "precision {} below threshold 0.9",
            precision
        );
        assert_eq!(
            results.len(),
            relevant_count,
            "not all relevant docs retrieved"
        );
    }

    #[test]
    fn test_bm25_precision_long() {
        let (bm25, _temp_dir) = setup_bm25_config();
        let mut wtxn = bm25.graph_env.write_txn().unwrap();

        let query_terms = vec!["the", "and"];
        let mut query_term_counts: HashMap<String, usize> = HashMap::new();
        for term in &query_terms {
            query_term_counts.insert(term.to_string(), 0);
        }
        let limit = 30_000;

        let txt = fetch_shakespeare().unwrap();
        //let word_count = shakespeare_txt.split_whitespace().count();

        let docs = txt
            .split_whitespace()
            .collect::<Vec<_>>()
            .chunks(250)
            .map(|chunk| chunk.join(" "))
            .collect::<Vec<_>>();

        for doc in tqdm::new(docs.iter(), docs.len(), None, Some("inserting docs")) {
            let id = v6_uuid();
            let doc_lower = doc.to_lowercase();

            let _ = bm25.insert_doc(&mut wtxn, id, &doc_lower).unwrap();

            for term in &query_terms {
                if doc_lower.contains(term) {
                    *query_term_counts.get_mut(*term).unwrap() += 1;
                }
            }
        }

        wtxn.commit().unwrap();

        for query_term in query_terms {
            let rtxn = bm25.graph_env.read_txn().unwrap();
            let term_count = query_term_counts.get(query_term).unwrap().clone();

            let results = bm25.search(&rtxn, query_term, limit).unwrap();

            let precision = results.len() as f64 / term_count as f64;

            debug_println!("term count: {}, results len: {}", term_count, results.len());

            assert!(
                precision >= 0.9 && precision <= 1.0,
                "precision {} below 0.9 or above 1.0",
                precision
            );
        }
    }
}

