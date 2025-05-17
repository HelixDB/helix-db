use crate::helix_engine::{
    storage_core::storage_core::HelixGraphStorage,
    graph_core::{
        config::Config,
        ops::{
            g::G,
            vectors::insert::InsertVAdapter,
        },
    },
    vector_core::{
        vector::HVector,
    },
};
use heed3::RwTxn;
use std::{
    sync::Arc,
    time::{
        Duration,
        Instant,
    },
    fs::{self, File},
};
use polars::prelude::*;
use kdam::tqdm;

type Filter = fn(&HVector) -> bool;

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

    Ok(all_vectors)
}

fn clear_dbs(txn: &mut RwTxn, db: &Arc<HelixGraphStorage>) {
    let _ = db.nodes_db.clear(txn);
    let _ = db.edges_db.clear(txn);
    let _ = db.out_edges_db.clear(txn);
    let _ = db.in_edges_db.clear(txn);
    let _ = db.in_edges_db.clear(txn);

    let _ = db.vectors.vectors_db.clear(txn);
    let _ = db.vectors.vector_data_db.clear(txn);
    let _ = db.vectors.out_edges_db.clear(txn);
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
fn bench_hnsw_bulk_insert_100k() {
    let n_vecs = 100_000;
    let vectors = load_dbpedia_vectors(n_vecs).unwrap();
    let db = Arc::new(setup_db());
    let mut txn = db.graph_env.write_txn().unwrap();
    clear_dbs(&mut txn, &db);

    let start = Instant::now();
    G::new_mut(Arc::clone(&db), &mut txn)
        .insert_vs::<Filter>(&vectors, None);
    let duration = start.elapsed();

    println!("Total insertion time for {} vectors: {:?}", n_vecs, duration);
}

#[test]
fn bench_hnsw_memory_inserted() {
    let db: Arc<HelixGraphStorage> = Arc::new(setup_db());
    let mut txn = db.graph_env.write_txn().unwrap();
    let size = db.graph_env.real_disk_size().unwrap() as usize;
    println!("Storage space size: {} bytes", size); // div 1024 for kb, div 1024 for mb
}

#[test]
fn bench_hnsw_memory_100k() {
    let n_vecs = 100_000;
    let vectors = load_dbpedia_vectors(n_vecs).unwrap();
    let db: Arc<HelixGraphStorage> = Arc::new(setup_db());
    let mut txn = db.graph_env.write_txn().unwrap();
    clear_dbs(&mut txn, &db);

    for vec in tqdm!(vectors.iter()) {
        let _  = G::new_mut(Arc::clone(&db), &mut txn)
            .insert_v::<Filter>(&vec, "vector", None);
    }

    let size = db.graph_env.real_disk_size().unwrap() as usize;
    println!("Storage space size: {} bytes", size); // div 1024 for kb, div 1024 for mb
}

//#[test]
//fn bench_hnsw_search() {}

//#[test]
//fn bench_hnsw_precision() {}

/*
fn calc_ground_truths(
    vectors: Vec<HVector>,
    query_vectors: Vec<(String, Vec<f64>)>,
    k: usize,
) -> Vec<Vec<String>> {
    query_vectors
        .par_iter()
        .map(|query| {
            let hquery = HVector::from_slice(0, 0, query.clone());

            let mut distances: Vec<(String, f64)> = vectors
                .iter()
                .map(|hvector| (hvector.get_id().to_string(), hvector.distance_to(&hquery)))
                .collect();

            distances.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

            distances.iter().take(k).map(|(id, _)| id.clone()).collect()
        })
        .collect()
}

// cargo test --release test_recall_precision_real_data -- --nocapture
#[test]
fn test_hnsw_precision_dbpedia_vectors() {
    let n_vecs = 50_000;
    let vectors = load_dbpedia_vectors(n_vecs).unwrap();
    let db = Arc::new(setup_db());
    let mut txn = db.graph_env.write_txn().unwrap();
    db.clear(&mut txn).unwrap();
    println!("loaded {} vectors", vectors.len());

    let n_query = 5_000; // 10-20%
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
    for (i, (id, data)) in vectors.iter().enumerate() {
        let start_time = Instant::now();
        let vec = index.insert::<Filter>(&mut txn, data, Some(id.parse::<u128>().unwrap())).unwrap();
        all_vectors.push(vec);
        let time = start_time.elapsed();
        if i % 1000 == 0 {
            println!(
                "{} => inserting in {} ms, vector: {}",
                i,
                time.as_millis(),
                id
            );
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
*/

