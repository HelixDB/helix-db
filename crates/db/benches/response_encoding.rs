//! Large-result response encoding benchmark.
//!
//! Seeds wide nodes once, then measures the embedded entry points that return
//! many rows: `query_json` (the bytes HTTP/gRPC send), `query` (the embedded
//! `JsonValue`) and `execute` (interpreter result without encoding). Request
//! parsing and planning are included because they are part of every call; the
//! fixture keeps them small next to the 10k-row results.

#![recursion_limit = "256"]

use std::sync::OnceLock;

use db::{HelixDB, HelixDbSource};
use helix_ast::prelude::*;
use helix_planner::{context::ParamBindings, exec::ExecutablePlan, planning};

/// Rows seeded under `cargo bench`. `cargo test --all-targets` also runs this
/// binary (without `--bench`, and with `cfg(test)` set in both modes), so the
/// smoke run seeds a handful of rows instead.
fn row_count() -> u64 {
    if std::env::args().any(|arg| arg == "--bench") {
        10_000
    } else {
        4
    }
}
const SEED_BATCH: u64 = 500;

#[global_allocator]
static ALLOC: divan::AllocProfiler = divan::AllocProfiler::system();

fn main() {
    divan::main();
}

struct Fixture {
    runtime: tokio::runtime::Runtime,
    db: HelixDB,
    project_json: Vec<u8>,
    project: QueryRequest,
    value_map_json: Vec<u8>,
    nodes_json: Vec<u8>,
    project_plan: ExecutablePlan,
}

fn fixture() -> &'static Fixture {
    static FIXTURE: OnceLock<Fixture> = OnceLock::new();
    FIXTURE.get_or_init(|| {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("benchmark runtime starts");
        let db = runtime.block_on(seed());
        let project_batch = read_batch()
            .var_as(
                "rows",
                g().n_with_label("Row").project(vec![
                    Projection::property("$id", "id"),
                    Projection::property("external_id", "external_id"),
                    Projection::property("name", "name"),
                    Projection::property("rank", "rank"),
                    Projection::property("score", "score"),
                    Projection::property("active", "active"),
                    Projection::property("created", "created"),
                ]),
            )
            .returning(["rows"]);
        let project = QueryRequest::read(project_batch.clone());
        let value_map = QueryRequest::read(
            read_batch()
                .var_as(
                    "rows",
                    g().n_with_label("Row").value_map(None::<Vec<String>>),
                )
                .returning(["rows"]),
        );
        let nodes = QueryRequest::read(
            read_batch()
                .var_as("rows", g().n_with_label("Row"))
                .returning(["rows"]),
        );
        let project_plan = planning::plan_read_batch(
            &project_batch,
            &db.planner_context(ParamBindings::default()),
        )
        .expect("projection plans");
        let fixture = Fixture {
            project_json: project.to_json_bytes().expect("request serializes"),
            value_map_json: value_map.to_json_bytes().expect("request serializes"),
            nodes_json: nodes.to_json_bytes().expect("request serializes"),
            project,
            project_plan,
            runtime,
            db,
        };
        // Warm caches so every sample measures the steady state.
        for request in [
            &fixture.project_json,
            &fixture.value_map_json,
            &fixture.nodes_json,
        ] {
            fixture
                .runtime
                .block_on(fixture.db.query_json(request))
                .expect("warm-up query executes");
        }
        fixture
    })
}

async fn seed() -> HelixDB {
    let db = HelixDB::open(HelixDbSource::InMemory {
        database: "response-encoding-bench".to_string(),
    })
    .await
    .expect("benchmark database opens");
    let row_count = row_count();
    for start in (0..row_count).step_by(SEED_BATCH as usize) {
        let mut create = write_batch();
        for id in start..(start + SEED_BATCH).min(row_count) {
            let id_i64 = id as i64;
            create = create.var_as(
                &format!("row_{id}"),
                g().add_n(
                    "Row",
                    vec![
                        ("external_id", PropertyInput::from(format!("row-{id:08}"))),
                        ("name", PropertyInput::from(format!("Name \"{id}\" é"))),
                        ("rank", PropertyInput::from(id_i64 * 7)),
                        ("score", PropertyInput::from(id as f64 / 3.0)),
                        ("active", PropertyInput::from(id % 2 == 0)),
                        (
                            "created",
                            PropertyInput::Value(PropertyValue::DateTime(
                                1_700_000_000_000 + id_i64,
                            )),
                        ),
                    ],
                )
                .count(),
            );
        }
        let plan =
            planning::plan_write_batch(&create, &db.planner_context(ParamBindings::default()))
                .expect("seed batch plans");
        db.execute(&plan, ParamBindings::default())
            .await
            .expect("seed batch executes");
    }
    db
}

/// Projection of 7 fields per row, encoded to the transport bytes.
#[divan::bench(threads = 1, sample_count = 60, sample_size = 1)]
fn query_json_project(bencher: divan::Bencher<'_, '_>) {
    let fixture = fixture();
    bencher.bench_local(|| {
        divan::black_box(
            fixture
                .runtime
                .block_on(fixture.db.query_json(&fixture.project_json))
                .expect("projection executes"),
        )
    });
}

/// The same projection through the embedded `JsonValue` API.
#[divan::bench(threads = 1, sample_count = 60, sample_size = 1)]
fn query_value_project(bencher: divan::Bencher<'_, '_>) {
    let fixture = fixture();
    bencher
        .with_inputs(|| fixture.project.clone())
        .bench_local_values(|request| {
            divan::black_box(
                fixture
                    .runtime
                    .block_on(fixture.db.query(request))
                    .expect("projection executes"),
            )
        });
}

/// Every stored property per row, encoded to the transport bytes.
#[divan::bench(threads = 1, sample_count = 60, sample_size = 1)]
fn query_json_value_map(bencher: divan::Bencher<'_, '_>) {
    let fixture = fixture();
    bencher.bench_local(|| {
        divan::black_box(
            fixture
                .runtime
                .block_on(fixture.db.query_json(&fixture.value_map_json))
                .expect("value map executes"),
        )
    });
}

/// Plain node rows (`{"$id": n}`), encoded to the transport bytes.
#[divan::bench(threads = 1, sample_count = 60, sample_size = 1)]
fn query_json_nodes(bencher: divan::Bencher<'_, '_>) {
    let fixture = fixture();
    bencher.bench_local(|| {
        divan::black_box(
            fixture
                .runtime
                .block_on(fixture.db.query_json(&fixture.nodes_json))
                .expect("node stream executes"),
        )
    });
}

/// The planned projection through the interpreter only (no encoding).
#[divan::bench(threads = 1, sample_count = 60, sample_size = 1)]
fn execute_project(bencher: divan::Bencher<'_, '_>) {
    let fixture = fixture();
    bencher.bench_local(|| {
        divan::black_box(
            fixture
                .runtime
                .block_on(
                    fixture
                        .db
                        .execute(&fixture.project_plan, ParamBindings::default()),
                )
                .expect("projection executes"),
        )
    });
}
