use helixdb::helix_engine::graph_core::graph_core::{HelixGraphEngine, HelixGraphEngineOpts};
use heed3::{EnvOpenOptions, Env};
use std::{
    sync::Arc,
    time::{
        Duration,
        Instant,
    },
};
use criterion::{
    criterion_group,
    criterion_main,
    BenchmarkId,
    Criterion,
    black_box
};
use tempfile::TempDir;
use polars::error;

fn setup_temp_env() -> Env {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().to_str().unwrap();
    unsafe {
        EnvOpenOptions::new()
            .map_size(4 * 1024 * 1024 * 1024) // 4 GB
            .max_dbs(10)
            .open(path)
            .unwrap()
    }
}

fn setup_db() -> HelixGraphEngine {
    HelixGraphEngine::new(HelixGraphEngineOpts::default()).unwrap() // TODO: should be to temp dir
                                                                    // maybe?
}

// download the data from 'https://huggingface.co/datasets/KShivendu/dbpedia-entities-openai-1M'
//      and put it into '../data/dbpedia-openai-1m/'. this will just be a set of .parquet files.
//      this is the same dataset used here: 'https://qdrant.tech/benchmarks/'. we use this dataset
//      because the vectors are of higher dimensionality
fn load_dbpedia_vectors(limit: usize) -> Result<Vec<(String, Vec<f64>)>, PolarsError> {
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

fn bench_hnsw_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("hnsw_insert");

    let vectors = load_dbpedia_vectors(100_000);
    let env = setup_temp_env();
    let mut txn = env.write_txn().unwrap();
    let graph_engine = Arc::new(setup_db());

    group.sample_size(1);

    let start = Instant::now();
    group.bench_function(BenchmarkId::new("hnsw_insert_100k", dims), |b| {
        b.iter(|| {
            for vector in vectors.iter() {
                black_box(index.insert(black_box(vector)));
            }
        });
    });

    //let mut total_insertion_time = Duration::from_secs(0);
}

fn bench_hnsw_bulk_insert(c: &mut Criterion) {}

fn bench_hnsw_search(c: &mut Criterion) {}

fn bench_hnsw_memory(c: &mut Criterion) {}

fn bench_hnsw_precision(c: &mut Criterion) {}

/*
fn bench_vector_insertion(c: &mut Criterion) {
    let mut group = c.benchmark_group("vector_insertion");
    group.measurement_time(Duration::from_secs(20));
    group.sample_size(10);

    for &dim in &[128, 1024, 4096, 8192] {
        let vectors_per_iter = 10;

        let id = BenchmarkId::new(format!("insert_{}vecs", vectors_per_iter), dim);
        group.bench_with_input(id, &dim, |b, &dim| {
            eprintln!("Benchmarking insertion of {} vectors with {} dimensions", vectors_per_iter, dim);

            b.iter_with_setup(
                || {
                    let (env, _temp_dir) = setup_temp_env();
                    let mut txn = env.write_txn().unwrap();
                    let hnsw = VectorCore::new(&env, &mut txn, HNSWConfig::new(100)).unwrap();
                    txn.commit().unwrap();

                    let vectors = generate_random_vectors(100, dim, 42);
                    (env, hnsw, vectors)
                },
                |(env, hnsw, vectors)| {
                    let mut txn = env.write_txn().unwrap();
                    for (_id, data) in vectors.iter().take(vectors_per_iter) {
                        hnsw.insert(&mut txn, data, None).unwrap();
                    }
                    txn.commit().unwrap();
                },
            );
        });
    }

    group.finish();
}

fn bench_vector_search(c: &mut Criterion) {
    let mut group = c.benchmark_group("vector_search");
    group.sample_size(20);

    for &dim in &[128, 1024, 4096, 8192] {
        let index_size = 1000;
        let queries_per_iter = 10;

        let id = BenchmarkId::new(format!("search_{}q_{}idx", queries_per_iter, index_size), dim);
        group.bench_with_input(id, &dim, |b, &dim| {
            eprintln!("benchmarking {} queries against index of {} vectors with {} dimensions",
                     queries_per_iter, index_size, dim);

            let (env, _temp_dir) = setup_temp_env();
            let mut txn = env.write_txn().unwrap();
            let hnsw = VectorCore::new(&env, &mut txn, dim, Some(HNSWConfig::optimized(index_size)), None).unwrap();
            let vectors = generate_random_vectors(index_size, dim, 42);
            eprintln!("building index with {} vectors of {} dimensions...", vectors.len(), dim);
            for (_id, data) in &vectors {
                hnsw.insert(&mut txn, data).unwrap();
            }
            txn.commit().unwrap();
            eprintln!("index built successfully");

            let query_vectors = generate_random_vectors(queries_per_iter, dim, 1);
            b.iter(|| {
                let txn = env.read_txn().unwrap();
                for (_, data) in &query_vectors {
                    let results = hnsw.search(&txn, &data, 10).unwrap();
                    black_box(results);
                }
            });
        });
    }

    group.finish();
}
*/

criterion_group!(
    benches,
    bench_hnsw_insert,
    bench_hnsw_bulk_insert,
    bench_hnsw_search,
    bench_hnsw_memory,
    bench_hnsw_precision,
);

criterion_main!(benches);

