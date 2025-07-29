/// cargo test --test hnsw_benches --features dev -- --no-capture
#[cfg(test)]
mod tests {
    use heed3::{Env, EnvOpenOptions, RoTxn};
    use helix_db::{
        helix_engine::vector_core::{
            hnsw::HNSW,
            vector::HVector,
            vector_core::{HNSWConfig, VectorCore},
        },
        utils::tqdm::tqdm,
    };
    use polars::prelude::*;
    use rand::prelude::SliceRandom;
    use std::{
        collections::HashSet,
        fs::{self, File},
        sync::{Arc, Mutex},
        thread,
        time::Instant,
    };

    fn setup_temp_env() -> Env {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().to_str().unwrap();

        unsafe {
            EnvOpenOptions::new()
                .map_size(40 * 1024 * 1024 * 1024) // 20 GB
                .max_dbs(10)
                .open(path)
                .unwrap()
        }
    }

    fn fetch_parquet_vectors() -> Result<(), Box<dyn std::error::Error>> {
        let urls = [
            "https://huggingface.co/datasets/KShivendu/dbpedia-entities-openai-1M/resolve/main/data/train-00002-of-00026-b05ce48965853dad.parquet",
            "https://huggingface.co/datasets/KShivendu/dbpedia-entities-openai-1M/resolve/main/data/train-00000-of-00026-3c7b99d1c7eda36e.parquet",
            "https://huggingface.co/datasets/KShivendu/dbpedia-entities-openai-1M/resolve/main/data/train-00003-of-00026-d116c3c239aa7895.parquet",
        ];

        for url in tqdm::new(urls.iter(), urls.len(), None, Some("fetching vectors")) {
            let res = reqwest::blocking::get(*url).unwrap();
            //let mut file = File::create("output_file")?;
            let content = res.bytes()?;
            println!("content: {:?}", content);
            //file.write_all(&content)?;
        }

        Ok(())
    }

    fn calc_ground_truths(
        vectors: Vec<HVector>,
        query_vectors: Vec<(String, Vec<f64>)>,
        k: usize,
    ) -> Vec<Vec<String>> {
        let vectors = Arc::new(vectors);
        let result = Arc::new(Mutex::new(Vec::new()));
        let chunk_size = (query_vectors.len() + num_cpus::get() - 1) / num_cpus::get();

        let handles: Vec<_> = query_vectors
            .into_iter()
            .collect::<Vec<_>>()
            .chunks(chunk_size)
            .map(|chunk| {
                let vectors = Arc::clone(&vectors);
                let result = Arc::clone(&result);
                let chunk = chunk.to_vec();

                thread::spawn(move || {
                    let mut local_results: Vec<Vec<String>> = chunk
                        .into_iter()
                        .map(|(_, query)| {
                            let hquery = HVector::from_slice(0, query);
                            let mut distances: Vec<(String, f64)> = vectors
                                .iter()
                                .filter_map(|hvector| {
                                    hvector
                                        .distance_to(&hquery)
                                        .map(|dist| (hvector.get_id().to_string(), dist))
                                        .ok()
                                })
                                .collect();

                            distances.sort_by(|a, b| {
                                a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal)
                            });
                            distances.into_iter().take(k).map(|(id, _)| id).collect()
                        })
                        .collect();

                    result.lock().unwrap().append(&mut local_results);
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        Arc::try_unwrap(result).unwrap().into_inner().unwrap()
    }

    fn load_dbpedia_vectors(limit: usize) -> Result<Vec<(String, Vec<f64>)>, PolarsError> {
        // https://huggingface.co/datasets/KShivendu/dbpedia-entities-openai-1M
        if limit > 1_000_000 {
            return Err(PolarsError::OutOfBounds(
                "can't load more than 1,000,000 vecs from this dataset".into(),
            ));
        }

        let data_dir = "../data/dbpedia-openai-1m/";
        let mut all_vectors = Vec::new();
        let mut total_loaded = 0;

        for entry in fs::read_dir(data_dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.is_file() && path.extension().map_or(false, |ext| ext == "parquet") {
                let df = ParquetReader::new(File::open(&path)?)
                    .finish()?
                    .lazy()
                    .limit((limit - total_loaded) as u32)
                    .collect()?;

                let ids = df.column("_id")?.str()?;
                let embeddings = df.column("openai")?.list()?;

                for (_id, embedding) in ids.into_iter().zip(embeddings.into_iter()) {
                    if total_loaded >= limit {
                        break;
                    }

                    let embedding = embedding.unwrap();
                    let f64_series = embedding.cast(&DataType::Float64).unwrap();
                    let chunked = f64_series.f64().unwrap();
                    let vector: Vec<f64> = chunked.into_no_null_iter().collect();

                    all_vectors.push((_id.unwrap().to_string(), vector));

                    total_loaded += 1;
                }

                if total_loaded >= limit {
                    break;
                }
            }
        }
        Ok(all_vectors)
    }

    /// Test the precision of the HNSW search algorithm
    #[test]
    fn bench_hnsw_search_long() {
        type Filter = fn(&HVector, &RoTxn) -> bool;
        //fetch_parquet_vectors().unwrap();
        let n_base = 70_000;
        let vectors = load_dbpedia_vectors(n_base).unwrap();

        let n_query = 8_000; // 10-20%
        let mut rng = rand::rng();
        let mut shuffled_vectors = vectors.clone();
        shuffled_vectors.shuffle(&mut rng);
        let base_vectors = &shuffled_vectors[..n_base - n_query];
        let query_vectors = &shuffled_vectors[n_base - n_query..];

        println!("num of base vecs: {}", base_vectors.len());
        println!("num of query vecs: {}", query_vectors.len());

        let k = 10;

        let env = setup_temp_env();
        let mut txn = env.write_txn().unwrap();

        let mut total_insertion_time = std::time::Duration::from_secs(0);
        let index = VectorCore::new(&env, &mut txn, HNSWConfig::new(None, None, None)).unwrap();

        let mut all_vectors: Vec<HVector> = Vec::new();
        let over_all_time = Instant::now();
        for (i, (_, data)) in vectors.iter().enumerate() {
            let start_time = Instant::now();
            let vec = index.insert::<Filter>(&mut txn, data, None).unwrap();
            let time = start_time.elapsed();
            all_vectors.push(vec);
            //if i % 0 == 0 {
                println!("{} => inserting in {} ms", i, time.as_millis());
                println!("time taken so far: {:?}", over_all_time.elapsed());
            //}
            total_insertion_time += time;
        }
        txn.commit().unwrap();

        let txn = env.read_txn().unwrap();
        println!("{:?}", index.config);

        println!(
            "total insertion time: {:.2?} seconds",
            total_insertion_time.as_secs_f64()
        );
        println!(
            "average insertion time per vec: {:.2?} milliseconds",
            total_insertion_time.as_millis() as f64 / n_base as f64
        );

        println!("calculating ground truths");
        let ground_truths = calc_ground_truths(all_vectors, query_vectors.to_vec(), k);

        println!("searching and comparing...");
        let test_id = format!("k = {} with {} queries", k, n_query);

        let mut total_recall = 0.0;
        let mut total_precision = 0.0;
        let mut total_search_time = std::time::Duration::from_secs(0);
        for ((_, query), gt) in query_vectors.iter().zip(ground_truths.iter()) {
            let start_time = Instant::now();
            let results = index.search::<Filter>(&txn, query, k, None, false).unwrap();
            let search_duration = start_time.elapsed();
            total_search_time += search_duration;

            let result_indices: HashSet<String> = results
                .into_iter()
                .map(|hvector| hvector.get_id().to_string())
                .collect();

            let gt_indices: HashSet<String> = gt.iter().cloned().collect();
            //println!("gt: {:?}\nresults: {:?}\n", gt_indices, result_indices);
            let true_positives = result_indices.intersection(&gt_indices).count();

            let recall: f64 = true_positives as f64 / gt_indices.len() as f64;
            let precision: f64 = true_positives as f64 / result_indices.len() as f64;

            total_recall += recall;
            total_precision += precision;
        }

        println!(
            "total search time: {:.2?} seconds",
            total_search_time.as_secs_f64()
        );
        println!(
            "average search time per query: {:.2?} milliseconds",
            total_search_time.as_millis() as f64 / n_query as f64
        );

        total_recall = total_recall / n_query as f64;
        total_precision = total_precision / n_query as f64;
        println!(
            "{}: avg. recall: {:.4?}, avg. precision: {:.4?}",
            test_id, total_recall, total_precision
        );
        assert!(total_recall >= 0.8, "recall not high enough!");
    }
}
