/// cargo test --test hnsw_benches --release -- --no-capture
#[cfg(feature = "bench")]
mod tests {
    use heed3::{Env, EnvOpenOptions, RoTxn};
    use helix_db::{
        helix_engine::vector_core::{HNSWConfig, HVector, VectorCore},
        utils::tqdm::tqdm,
    };
    use polars::prelude::*;
    use rand::{Rng, prelude::SliceRandom};
    use std::{
        collections::{HashMap, HashSet},
        fs::{self, File},
        sync::{Arc, Mutex},
        thread,
        time::Instant,
    };

    type Filter = fn(&HVector, &RoTxn) -> bool;

    fn setup_temp_env() -> Env {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().to_str().unwrap();

        unsafe {
            EnvOpenOptions::new()
                .map_size(20 * 1024 * 1024 * 1024) // 20 GB
                .max_dbs(10)
                .open(path)
                .unwrap()
        }
    }

    #[allow(dead_code)]
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

    /// Returns query ids and their associated closest k vectors (by vec id)
    fn calc_ground_truths(
        base_vectors: Vec<HVector>,
        query_vectors: &Vec<(usize, Vec<f32>)>,
        k: usize,
    ) -> HashMap<usize, Vec<u128>> {
        let base_vectors = Arc::new(base_vectors);
        let results = Arc::new(Mutex::new(HashMap::new()));
        let chunk_size = (query_vectors.len() + num_cpus::get() - 1) / num_cpus::get();
        let arena = bumpalo::Bump::new();
        let label = arena.alloc_str("test");

        let handles: Vec<_> = query_vectors
            .chunks(chunk_size)
            .map(|chunk| {
                let base_vectors = Arc::clone(&base_vectors);
                let results = Arc::clone(&results);
                let chunk = chunk.to_vec();

                thread::spawn(move || {
                    let local_results: HashMap<usize, Vec<u128>> = chunk
                        .into_iter()
                        .map(|(query_id, query_vec)| {
                            let mut vecs = bumpalo::collections::Vec::new_in(&arena);
                            vecs.extend_from_slice(query_vec.as_slice());
                            let query_hvector = HVector::from_vec(&label, vecs);

                            let mut distances: Vec<(u128, f64)> = base_vectors
                                .iter()
                                .filter_map(|base_vec| {
                                    query_hvector
                                        .distance_to(base_vec)
                                        .map(|dist| (base_vec.id, dist))
                                        .ok()
                                })
                                .collect();

                            distances.sort_by(|a, b| {
                                a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal)
                            });

                            let top_k_ids: Vec<u128> =
                                distances.into_iter().take(k).map(|(id, _)| id).collect();

                            (query_id, top_k_ids)
                        })
                        .collect();

                    results.lock().unwrap().extend(local_results);
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        Arc::try_unwrap(results).unwrap().into_inner().unwrap()
    }

    fn load_dbpedia_vectors(limit: usize) -> Result<Vec<Vec<f64>>, PolarsError> {
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

                let embeddings = df.column("openai")?.list()?;

                for embedding in embeddings.into_iter() {
                    if total_loaded >= limit {
                        break;
                    }

                    let embedding = embedding.unwrap();
                    let f64_series = embedding.cast(&DataType::Float64).unwrap();
                    let chunked = f64_series.f64().unwrap();
                    let vector: Vec<f64> = chunked.into_no_null_iter().collect();

                    all_vectors.push(vector);

                    total_loaded += 1;
                }

                if total_loaded >= limit {
                    break;
                }
            }
        }
        Ok(all_vectors)
    }

    /// Higher values of similarity make the vectors more similar
    #[allow(dead_code)]
    fn gen_sim_vecs(n: usize, dim: usize, similarity: f64) -> Vec<Vec<f64>> {
        let mut rng = rand::rng();
        let mut vectors = Vec::with_capacity(n);
        let similarity = 1.0 - similarity;

        let base: Vec<f64> = (0..dim).map(|_| rng.random_range(-1.0..1.0)).collect();

        for _ in 0..n {
            let mut vec = base.clone();
            for v in vec.iter_mut() {
                *v += rng.random_range(-similarity..similarity);
                *v = v.clamp(-1.0, 1.0);
            }
            vectors.push(vec);
        }

        vectors
    }

    /// Test the precision of the HNSW search algorithm
    #[test]
    fn bench_hnsw_search_long() {
        let n_base = 5_000;
        let n_query = 1000; // 10-20%
        let k = 10;
        let mut vectors = load_dbpedia_vectors(n_base).unwrap();
        let arena = bumpalo::Bump::new();
        let label = arena.alloc_str("test");

        let mut rng = rand::rng();
        vectors.shuffle(&mut rng);

        let base_vectors = &vectors[..n_base - n_query];
        let query_vectors = vectors[n_base - n_query..]
            .to_vec()
            .iter()
            .enumerate()
            .map(|(i, x)| (i + 1, x.clone()))
            .collect::<Vec<(usize, Vec<f32>)>>();

        println!("num of base vecs: {}", base_vectors.len());
        println!("num of query vecs: {}", query_vectors.len());

        let env = setup_temp_env();
        let mut txn = env.write_txn().unwrap();
        let index = VectorCore::new(&env, &mut txn, HNSWConfig::new(None, None, None)).unwrap();
        let mut total_insertion_time = std::time::Duration::from_secs(0);

        let mut base_all_vectors: Vec<HVector> = Vec::new();
        let over_all_time = Instant::now();
        for (i, data) in base_vectors.iter().enumerate() {
            let start_time = Instant::now();
            let vec = index.insert(&mut txn, label, &data, None, &arena).unwrap();
            let time = start_time.elapsed();
            base_all_vectors.push(vec);
            //println!("{} => inserting in {} ms", i, time.as_millis());
            if i % 500 == 0 {
                println!("{} => inserting in {} ms", i, time.as_millis());
                println!("time taken so far: {:?}", over_all_time.elapsed());
            }
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

        let ground_truths = calc_ground_truths(base_all_vectors, &query_vectors, k);
        println!("calculating ground truths");

        println!("searching and comparing...");
        let test_id = format!("k = {} with {} queries", k, n_query);

        let mut total_recall = 0.0;
        let mut total_precision = 0.0;
        let mut total_search_time = std::time::Duration::from_secs(0);
        for (qid, query) in query_vectors.iter() {
            let start_time = Instant::now();
            let results = index
                .search(&txn, query, k, "vector", false, &arena)
                .unwrap();
            let search_duration = start_time.elapsed();
            total_search_time += search_duration;

            let result_indices = results
                .into_iter()
                .map(|hvec| hvec.get_id())
                .collect::<HashSet<u128>>();

            let gt_indices = ground_truths
                .get(&qid)
                .unwrap()
                .clone()
                .into_iter()
                .collect::<HashSet<u128>>();

            println!("gt: {:?}\nresults: {:?}\n", gt_indices, result_indices);
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
            test_id, total_recall, total_precision,
        );
        assert!(total_recall >= 0.8, "recall not high enough!");
    }
}

// TODO: memory benchmark (only the hnsw index ofc)
