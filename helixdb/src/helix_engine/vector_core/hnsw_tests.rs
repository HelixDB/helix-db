use crate::helix_engine::{
    storage_core::storage_core::HelixGraphStorage,
    vector_core::vector::HVector,
    graph_core::{
        config::Config,
        ops::{
            g::G,
            tr_val::TraversalVal,
            vectors::{insert::InsertVAdapter, search::SearchVAdapter},
        },
    },
};
use std::{
    collections::HashSet,
    fs::{self, File},
    sync::Arc,
    time::{Duration, Instant},
};
use heed3::{RwTxn, RoTxn};
use rand::seq::SliceRandom;
use polars::prelude::*;
use kdam::tqdm;
use rayon::prelude::*;

// make sure to test with cargo test --release <test_name> -- --nocapture

type Filter = fn(&HVector, &RoTxn) -> bool;

fn setup_db() -> HelixGraphStorage {
    let config = Config::new(16, 128, 768, 10);
    let db = HelixGraphStorage::new("test-store/", config).unwrap();
    db
}

// download the data from 'https://huggingface.co/datasets/KShivendu/dbpedia-entities-openai-1M'
//      and put it into '../data/dbpedia-openai-1m/'. this will just be a set of .parquet files.
//      this is the same dataset used here: 'https://qdrant.tech/benchmarks/'. we use this dataset
//      because the vectors are of higher dimensionality
fn load_dbpedia_vectors(limit: usize) -> Result<Vec<Vec<f64>>, PolarsError> {
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

    println!("loaded {} vectors", all_vectors.len());

    Ok(all_vectors)
}

// TODO: doesn't work correctly
fn clear_dbs(txn: &mut RwTxn, db: &Arc<HelixGraphStorage>) {
    db.nodes_db.clear(txn).unwrap();
    db.edges_db.clear(txn).unwrap();
    db.out_edges_db.clear(txn).unwrap();
    db.in_edges_db.clear(txn).unwrap();

    db.vectors.vectors_db.clear(txn).unwrap();
    db.vectors.vector_data_db.clear(txn).unwrap();
    db.vectors.out_edges_db.clear(txn).unwrap();
}

fn calc_ground_truths(
    vectors: Vec<HVector>,
    query_vectors: Vec<HVector>,
    k: usize,
) -> Vec<Vec<u128>> {
    println!("calculating ground truths");

    query_vectors
        .par_iter()
        .map(|query| {
            let mut distances: Vec<(u128, f64)> = vectors
                .iter()
                .map(|hvector| (hvector.get_id(), hvector.distance_to(query).unwrap()))
                .collect();

            distances.sort_by(|a, b| {
                a.1.partial_cmp(&b.1).unwrap()
            });

            distances
                .iter()
                .take(k)
                .map(|(id, _)| id.clone())
                .collect::<Vec<u128>>()
        })
        .collect()
}

#[test]
fn bench_hnsw_insert_100k() {
    let n_vecs = 100_000;
    let vectors = load_dbpedia_vectors(n_vecs).unwrap();
    let db = Arc::new(setup_db());
    let mut txn = db.graph_env.write_txn().unwrap();
    clear_dbs(&mut txn, &db);

    let mut insert_times = Vec::with_capacity(n_vecs);
    let start = Instant::now();
    for vec in tqdm!(vectors.iter()) {
        let insert_start = Instant::now();
        G::new_mut(Arc::clone(&db), &mut txn)
            .insert_v::<Filter>(&vec, "vector", None);
        insert_times.push(insert_start.elapsed());
    }
    txn.commit().unwrap();
    let duration = start.elapsed();

    let total_insert_time: Duration = insert_times.iter().sum();
    let avg_insert_time = if !insert_times.is_empty() {
        total_insert_time / insert_times.len() as u32
    } else {
        Duration::from_secs(0)
    };

    println!("Total insertion time for {} vectors: {:?}", n_vecs, duration);
    println!("Average time per insertion (total/num_vectors): {:?}", duration / n_vecs as u32);
    println!("Average insertion time per query (measured individually): {:?}", avg_insert_time);
}

#[test]
fn bench_hnsw_memory() {
    let db: Arc<HelixGraphStorage> = Arc::new(setup_db());
    let size = db.graph_env.real_disk_size().unwrap() as usize;
    assert!(size >= 1832419328, "vectors have been inserted before running this test");
    println!("storage space size: {} bytes or {} MB", size, size / 1024 / 1024);
}

#[test]
fn bench_hnsw_search() {
    let db: Arc<HelixGraphStorage> = Arc::new(setup_db());
    let txn = db.graph_env.read_txn().unwrap();
    let size = db.graph_env.real_disk_size().unwrap() as usize;
    assert!(size >= 1832419328, "vectors have been inserted before running this test");

    let n_vecs = 5_000;
    let k: usize = 12;
    let query_vectors = load_dbpedia_vectors(n_vecs).unwrap();

    println!("searching...");
    let mut search_times = Vec::with_capacity(n_vecs);
    let start = Instant::now();
    for vec in tqdm!(query_vectors.iter()) {
        let search_start = Instant::now();
        let tr  = G::new(Arc::clone(&db), &txn)
            .search_v::<Filter>(&vec, k, None);
        let results: Vec<HVector> = tr
            .filter_map(|result| match result {
                Ok(TraversalVal::Vector(hvector)) => Some(hvector),
                Err(e) => {
                    println!("Error: {}", e);
                    None
                }
                _ => None,
            })
        .collect();

        assert!(results.len() > 0);

        search_times.push(search_start.elapsed());
    }
    let duration = start.elapsed();

    let total_search_time: Duration = search_times.iter().sum();
    let avg_search_time = if !search_times.is_empty() {
        total_search_time / search_times.len() as u32
    } else {
        Duration::from_secs(0)
    };

    println!("Total search time for {} queries, at k = {}: {:?}", n_vecs, k, total_search_time);
    println!("Average time per search (total/num_vectors): {:?}", duration / n_vecs as u32);
    println!("Average search time per query (measured individually): {:?}", avg_search_time);
}

#[test]
fn bench_hnsw_precision() {
    let n_vecs = 10_00;
    let n_query = 1_00; // 10-20%
    let k = 12;
    let vectors = load_dbpedia_vectors(n_vecs).unwrap();
    let db = Arc::new(setup_db());
    let mut txn = db.graph_env.write_txn().unwrap();
    clear_dbs(&mut txn, &db);

    let mut all_vectors: Vec<HVector> = Vec::new();

    let mut total_insertion_time = std::time::Duration::from_secs(0);
    let over_all_time = Instant::now();
    for (i, data) in tqdm!(vectors.iter().enumerate()) {
        let start_time = Instant::now();

        let mut tr = G::new_mut(Arc::clone(&db), &mut txn)
            .insert_v::<Filter>(&data, "vector", None);
        let vec = match tr.next() {
            Some(Ok(TraversalVal::Vector(hvector))) => Some(hvector),
            Some(Err(e)) => {
                println!("Error: {}", e);
                None
            }
            _ => None,
        };
        all_vectors.push(vec.unwrap());

        let time = start_time.elapsed();

        if i % 1_000 == 0 {
            println!("{} => inserting in {:?}", i, time);
            println!("time taken so far: {:?}", over_all_time.elapsed());
        }
        total_insertion_time += time;
    }

    println!("total insertion time: {:.2?} seconds", total_insertion_time.as_secs_f64());
    println!(
        "average insertion time per vec: {:.2?} milliseconds",
        total_insertion_time.as_millis() as f64 / n_vecs as f64
    );

    txn.commit().unwrap();
    let txn = db.graph_env.read_txn().unwrap();
    let size = db.graph_env.real_disk_size().unwrap() as usize;
    assert!(size >= 49152, "vectors have been inserted before running this test");

    let mut rng = rand::rng();
    let mut shuffled_vectors: Vec<HVector> = all_vectors.clone();
    shuffled_vectors.shuffle(&mut rng);
    let query_vectors = &shuffled_vectors[n_vecs - n_query..];

    let ground_truths = calc_ground_truths(all_vectors, query_vectors.to_vec(), k);

    println!("searching and comparing...");
    let test_id = format!("k = {} with {} queries", k, n_query);
    let mut total_recall = 0.0;
    let mut total_precision = 0.0;
    let mut total_search_time = std::time::Duration::from_secs(0);
    for (query, gt) in query_vectors.iter().zip(ground_truths.iter()) {
        let start_time = Instant::now();

        let query_vec = query.get_data().to_vec();
        let tr  = G::new(Arc::clone(&db), &txn)
            .search_v::<Filter>(&query_vec, k, None);
        let results: Vec<HVector> = tr
            .filter_map(|result| match result {
                Ok(TraversalVal::Vector(hvector)) => Some(hvector),
                _ => None,
            })
        .collect();

        let search_duration = start_time.elapsed();
        total_search_time += search_duration;

        let result_indices: HashSet<u128> = results
            .into_iter()
            .map(|hvector| hvector.get_id())
            .collect();

        let gt_indices: HashSet<u128> = gt.iter().cloned().collect();
        //println!("gt: {:?}\nresults: {:?}\n", gt_indices, result_indices);
        let true_positives = result_indices.intersection(&gt_indices).count();

        let recall: f64 = true_positives as f64 / gt_indices.len() as f64;
        let precision: f64 = true_positives as f64 / result_indices.len() as f64;

        total_recall += recall;
        total_precision += precision;
    }

    total_recall = total_recall / n_query as f64;
    total_precision = total_precision / n_query as f64;

    println!("total search time: {:.2?} seconds", total_search_time.as_secs_f64());
    println!(
        "average search time per query: {:.2?} milliseconds",
        total_search_time.as_millis() as f64 / n_query as f64
    );
    println!("{}: avg. recall: {:.4?}, avg. precision: {:.4?}", test_id, total_recall, total_precision);
    assert!(total_recall >= 0.8, "recall not high enough!");
}

