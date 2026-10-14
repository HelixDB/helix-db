//! Property-row read benchmark.
//!
//! Wide nodes (scalar fields, a long text body and an embedding) are read by
//! queries that need only one or two fields: a filtered count, a filtered
//! single-field projection and an unfiltered single-field projection. Full
//! row materialization is measured alongside so that reading every field
//! cannot regress unnoticed. Graph creation and planning happen once outside
//! measurement; each sample executes only the planned read.
//!
//! The top-level benchmarks read rows still in the memtable, where each row
//! is its own allocation. The `reopened` benchmarks read the same rows after
//! the database is closed and reopened, so rows come from stored blocks at
//! arbitrary offsets, as on a long-running server.

#![recursion_limit = "256"]

use std::sync::OnceLock;

use db::{HelixDB, HelixDbSource, ProcessLocalDatabaseToken};
use helix_ast::prelude::*;
use helix_planner::{context::ParamBindings, exec::ExecutablePlan, planning};

/// Nodes seeded for `cargo bench`, which passes `--bench` to the harness.
const BENCH_ENTITY_COUNT: u64 = 2_000;
/// Nodes seeded when `cargo test --all-targets` runs the harness as a smoke
/// test. Cargo compiles bench targets with `cfg(test)` in both modes, so the
/// size is chosen from the arguments at run time instead.
const SMOKE_ENTITY_COUNT: u64 = 4;

const EMBEDDING_DIMENSIONS: usize = 384;

#[global_allocator]
static ALLOC: divan::AllocProfiler = divan::AllocProfiler::system();

fn main() {
    divan::main();
}

struct Fixture {
    runtime: tokio::runtime::Runtime,
    db: HelixDB,
    filtered_count: ExecutablePlan,
    filtered_project_one: ExecutablePlan,
    project_one: ExecutablePlan,
    full_rows: ExecutablePlan,
}

/// Where the measured rows are read from.
#[derive(Clone, Copy)]
enum Rows {
    /// The memtable the rows were written to.
    Written,
    /// Stored blocks, after the database is closed and reopened.
    Reopened,
}

fn fixture(rows: Rows) -> &'static Fixture {
    static WRITTEN: OnceLock<Fixture> = OnceLock::new();
    static REOPENED: OnceLock<Fixture> = OnceLock::new();
    let fixture = match rows {
        Rows::Written => &WRITTEN,
        Rows::Reopened => &REOPENED,
    };
    fixture.get_or_init(|| {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("benchmark runtime starts");
        let fixture = runtime.block_on(seed_and_plan(rows));
        let (db, [filtered_count, filtered_project_one, project_one, full_rows]) = fixture;
        Fixture {
            runtime,
            db,
            filtered_count,
            filtered_project_one,
            project_one,
            full_rows,
        }
    })
}

async fn seed_and_plan(rows: Rows) -> (HelixDB, [ExecutablePlan; 4]) {
    let token = ProcessLocalDatabaseToken::new("property-row-reads-bench")
        .expect("benchmark database token is valid");
    let source = || HelixDbSource::InMemoryToken {
        token: token.clone(),
    };
    let db = HelixDB::open(source())
        .await
        .expect("benchmark database opens");

    let entity_count = if std::env::args().any(|argument| argument == "--bench") {
        BENCH_ENTITY_COUNT
    } else {
        SMOKE_ENTITY_COUNT
    };
    let body = "lorem ipsum dolor sit amet ".repeat(80);
    for chunk in (0..entity_count).collect::<Vec<_>>().chunks(250) {
        let mut create = write_batch();
        for &id in chunk {
            let embedding = (0..EMBEDDING_DIMENSIONS)
                .map(|dimension| ((id as usize + dimension) % 97) as f32 / 97.0)
                .collect::<Vec<f32>>();
            create = create.var_as(
                &format!("node_{id}"),
                g().add_n(
                    "PropertyRowBenchNode",
                    vec![
                        ("external_id", PropertyInput::from(format!("node-{id}"))),
                        (
                            "status",
                            PropertyInput::from(if id % 2 == 0 { "active" } else { "inactive" }),
                        ),
                        ("score", PropertyInput::from(id as i64)),
                        ("title", PropertyInput::from(format!("title for node {id}"))),
                        ("attribute_1", PropertyInput::from(id as i64 + 1)),
                        ("attribute_2", PropertyInput::from(id as f64 / 3.0)),
                        ("attribute_3", PropertyInput::from(id % 3 == 0)),
                        (
                            "attribute_4",
                            PropertyInput::from(format!("tag-{}", id % 17)),
                        ),
                        ("body", PropertyInput::from(body.clone())),
                        ("embedding", PropertyInput::from(embedding)),
                    ],
                ),
            );
        }
        let plan =
            planning::plan_write_batch(&create, &db.planner_context(ParamBindings::default()))
                .expect("benchmark graph plans");
        db.execute(&plan, ParamBindings::default())
            .await
            .expect("benchmark graph is created");
    }

    let db = match rows {
        Rows::Written => db,
        Rows::Reopened => {
            db.close().await.expect("benchmark database closes");
            HelixDB::open(source())
                .await
                .expect("benchmark database reopens")
        }
    };

    let threshold = entity_count as i64 / 2;
    let queries = [
        read_batch()
            .var_as(
                "rows",
                g().n(NodeRef::all())
                    .where_(Predicate::eq("status", "active"))
                    .count(),
            )
            .returning(["rows"]),
        read_batch()
            .var_as(
                "rows",
                g().n(NodeRef::all())
                    .where_(Predicate::gt("score", threshold))
                    .project(vec![Projection::property("title", "title")]),
            )
            .returning(["rows"]),
        read_batch()
            .var_as(
                "rows",
                g().n(NodeRef::all())
                    .project(vec![Projection::property("score", "score")]),
            )
            .returning(["rows"]),
        read_batch()
            .var_as("rows", g().n(NodeRef::all()).limit(100))
            .returning(["rows"]),
    ];
    let planner_context = db.planner_context(ParamBindings::default());
    let mut plans = Vec::new();
    for query in &queries {
        let plan = planning::plan_read_batch(query, &planner_context).expect("query plans");
        db.execute(&plan, ParamBindings::default())
            .await
            .expect("query warms");
        plans.push(plan);
    }
    let plans: [ExecutablePlan; 4] = plans.try_into().expect("four plans");
    (db, plans)
}

fn run(bencher: divan::Bencher<'_, '_>, rows: Rows, plan: fn(&Fixture) -> &ExecutablePlan) {
    let fixture = fixture(rows);
    bencher.bench_local(|| {
        divan::black_box(
            fixture
                .runtime
                .block_on(fixture.db.execute(plan(fixture), ParamBindings::default()))
                .expect("query executes"),
        )
    });
}

#[divan::bench(threads = 1)]
fn filtered_count(bencher: divan::Bencher<'_, '_>) {
    run(bencher, Rows::Written, |fixture| &fixture.filtered_count);
}

#[divan::bench(threads = 1)]
fn filtered_project_one(bencher: divan::Bencher<'_, '_>) {
    run(bencher, Rows::Written, |fixture| {
        &fixture.filtered_project_one
    });
}

#[divan::bench(threads = 1)]
fn project_one(bencher: divan::Bencher<'_, '_>) {
    run(bencher, Rows::Written, |fixture| &fixture.project_one);
}

#[divan::bench(threads = 1)]
fn full_rows(bencher: divan::Bencher<'_, '_>) {
    run(bencher, Rows::Written, |fixture| &fixture.full_rows);
}

mod reopened {
    use super::{run, Rows};

    #[divan::bench(threads = 1)]
    fn filtered_count(bencher: divan::Bencher<'_, '_>) {
        run(bencher, Rows::Reopened, |fixture| &fixture.filtered_count);
    }

    #[divan::bench(threads = 1)]
    fn filtered_project_one(bencher: divan::Bencher<'_, '_>) {
        run(bencher, Rows::Reopened, |fixture| {
            &fixture.filtered_project_one
        });
    }

    #[divan::bench(threads = 1)]
    fn project_one(bencher: divan::Bencher<'_, '_>) {
        run(bencher, Rows::Reopened, |fixture| &fixture.project_one);
    }

    #[divan::bench(threads = 1)]
    fn full_rows(bencher: divan::Bencher<'_, '_>) {
        run(bencher, Rows::Reopened, |fixture| &fixture.full_rows);
    }
}
