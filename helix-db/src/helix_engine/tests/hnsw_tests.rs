// MAKE SURE TO --release
use crate::helix_engine::traversal_core::config::SimilarityMethod;
use crate::helix_engine::vector_core::{
    hnsw::HNSW,
    vector::HVector,
    vector_core::{HNSWConfig, VectorCore},
};
use heed3::{Env, EnvOpenOptions, RoTxn};
use rand::{Rng, seq::SliceRandom};
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    thread,
};

type Filter = fn(&HVector, &RoTxn) -> bool;

fn setup_temp_env() -> Env {
    let temp_dir = tempfile::tempdir().unwrap();
    let path = temp_dir.path().to_str().unwrap();

    unsafe {
        EnvOpenOptions::new()
            .map_size(2 * 1024 * 1024 * 1024) // 2 GB
            .max_dbs(10)
            .open(path)
            .unwrap()
    }
}

/// Returns query ids and their associated closest k vectors (by vec id)
fn calc_ground_truths(
    base_vectors: Vec<HVector>,
    query_vectors: &Vec<(usize, Vec<f64>)>,
    k: usize,
) -> HashMap<usize, Vec<u128>> {
    let base_vectors = Arc::new(base_vectors);
    let results = Arc::new(Mutex::new(HashMap::new()));
    let chunk_size = (query_vectors.len() + num_cpus::get() - 1) / num_cpus::get();

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
                        let query_hvector = HVector::from_slice(0, query_vec);

                        let mut distances: Vec<(u128, f64)> = base_vectors
                            .iter()
                            .filter_map(|base_vec| {
                                query_hvector
                                    .distance_to(base_vec, &SimilarityMethod::default())
                                    .map(|dist| (base_vec.id.clone(), dist))
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

/// Higher values of similarity make the vectors more similar
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

#[test]
fn tests_hnsw_config_build() {
    let env = setup_temp_env();
    let mut txn = env.write_txn().unwrap();

    let config = HNSWConfig::new(Some(32), Some(256), Some(256));

    let index = VectorCore::new(&env, &mut txn, config, Some(SimilarityMethod::default())).unwrap();
    assert_eq!(index.config.m, 32);
    assert_eq!(index.config.ef_construct, 256);
    assert_eq!(index.config.ef, 256);

    let config = HNSWConfig::new(Some(6969), Some(6969), Some(6969));
    assert_eq!(config.m, 48);
    assert_eq!(config.ef_construct, 512);
    assert_eq!(config.ef, 512);
}

#[test]
fn test_hnsw_insert() {
    let env = setup_temp_env();
    let mut txn = env.write_txn().unwrap();
    let index = VectorCore::new(&env, &mut txn, HNSWConfig::new(None, None, None), Some(SimilarityMethod::default())).unwrap();

    let n_base = 500;
    let dims = 750;
    let vectors = gen_sim_vecs(n_base, dims, 0.8);

    for data in vectors {
        let vec = index
            .insert::<Filter>(&mut txn, &data, None)
            .unwrap();
        assert_eq!(vec.data, data);
        assert!(vec.properties.is_none());
    }

    // >= because vecs spread over levels
    assert!(index.num_inserted_vectors(&txn).unwrap() >= n_base as u64);
}

#[test]
fn test_get_vector() {
    let env = setup_temp_env();
    let mut txn = env.write_txn().unwrap();
    let index = VectorCore::new(&env, &mut txn, HNSWConfig::new(None, None, None), Some(SimilarityMethod::default())).unwrap();

    let n_base = 500;
    let dims = 750;
    let vectors = gen_sim_vecs(n_base, dims, 0.8);

    let mut all_vectors: Vec<HVector> = Vec::with_capacity(n_base);
    for data in vectors {
        all_vectors.push(
            index
                .insert::<Filter>(&mut txn, &data, None)
                .unwrap(),
        );
    }

    for inserted_vec in all_vectors {
        let got_vec = match index.get_vector(&txn, inserted_vec.id, 0, true) {
            Ok(vec) => vec,
            Err(_) => panic!("couldn't find the vector"),
        };

        assert_eq!(got_vec.get_level(), 0);
        assert_eq!(got_vec.id, inserted_vec.id);
        assert_eq!(got_vec.get_data(), inserted_vec.get_data());
    }
}

#[test]
fn test_hnsw_search() {
    let n_base = 1_000;
    let dims = 450;
    let n_query = 100;
    let k = 10;
    let mut rng = rand::rng();
    let mut vectors = gen_sim_vecs(n_base, dims, 0.8);

    vectors.shuffle(&mut rng);
    let base_vectors = &vectors[..n_base - n_query];
    let query_vectors = vectors[n_base - n_query..]
        .to_vec()
        .iter()
        .enumerate()
        .map(|(i, x)| (i + 1, x.clone()))
        .collect::<Vec<(usize, Vec<f64>)>>();

    println!("num of base vecs: {}", base_vectors.len());
    println!("num of query vecs: {}", query_vectors.len());

    let env = setup_temp_env();
    let mut txn = env.write_txn().unwrap();
    let index = VectorCore::new(&env, &mut txn, HNSWConfig::new(None, None, None), Some(SimilarityMethod::default())).unwrap();

    let mut base_all_vectors: Vec<HVector> = Vec::new();
    for data in base_vectors.iter() {
        base_all_vectors.push(
            index
                .insert::<Filter>(&mut txn, &data, None)
                .unwrap(),
        );
    }
    txn.commit().unwrap();

    let txn = env.read_txn().unwrap();

    println!("calculating ground truths");
    let ground_truths = calc_ground_truths(base_all_vectors, &query_vectors, k);

    println!("searching and comparing...");

    let mut total_recall = 0.0;
    let mut total_precision = 0.0;
    for (qid, query) in query_vectors {
        let results = index
            .search::<Filter>(
                &txn,
                &query,
                k,
                "vector",
                None,
                false,
            )
            .unwrap();

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

    total_recall = total_recall / n_query as f64;
    total_precision = total_precision / n_query as f64;
    println!(
        "avg. recall: {:.4?}, avg. precision: {:.4?}",
        total_recall, total_precision
    );
    assert!(total_recall >= 0.8, "recall not high enough!");
    assert!(total_precision >= 0.8, "precision not high enough!");
}

#[test]
fn test_hnsw_search_property_ordering() {}

#[test]
fn test_hnsw_search_filter_ordering() {}

#[test]
fn test_hnsw_delete() {}
