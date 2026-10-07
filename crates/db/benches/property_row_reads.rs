//! Property-row read benchmark.
//!
//! Wide nodes (scalar fields, a long text body and an embedding) are read by
//! queries that need only one or two fields: a filtered count, a filtered
//! single-field projection and an unfiltered single-field projection. Full
//! row materialization is measured alongside so that reading every field
//! cannot regress unnoticed. Graph creation and planning happen once outside
//! measurement; each sample executes only the planned read.

#![recursion_limit = "256"]

use std::sync::OnceLock;

use db::{HelixDB, HelixDbSource};
use helix_ast::prelude::*;
use helix_planner::{context::ParamBindings, exec::ExecutablePlan, planning};

#[cfg(not(test))]
const ENTITY_COUNT: u64 = 2_000;
// `cargo test --all-targets` executes the Divan harness; keep that smoke run small.
#[cfg(test)]
const ENTITY_COUNT: u64 = 4;

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

fn fixture() -> &'static Fixture {
    static FIXTURE: OnceLock<Fixture> = OnceLock::new();
    FIXTURE.get_or_init(|| {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("benchmark runtime starts");
        let fixture = runtime.block_on(seed_and_plan());
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

async fn seed_and_plan() -> (HelixDB, [ExecutablePlan; 4]) {
    let db = HelixDB::open(HelixDbSource::InMemory {
        database: "property-row-reads-bench".to_string(),
    })
    .await
    .expect("benchmark database opens");

    let body = "lorem ipsum dolor sit amet ".repeat(80);
    for chunk in (0..ENTITY_COUNT).collect::<Vec<_>>().chunks(250) {
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
                        ("attribute_4", PropertyInput::from(format!("tag-{}", id % 17))),
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

    let threshold = ENTITY_COUNT as i64 / 2;
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

fn run(bencher: divan::Bencher<'_, '_>, plan: fn(&Fixture) -> &ExecutablePlan) {
    let fixture = fixture();
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
    run(bencher, |fixture| &fixture.filtered_count);
}

#[divan::bench(threads = 1)]
fn filtered_project_one(bencher: divan::Bencher<'_, '_>) {
    run(bencher, |fixture| &fixture.filtered_project_one);
}

#[divan::bench(threads = 1)]
fn project_one(bencher: divan::Bencher<'_, '_>) {
    run(bencher, |fixture| &fixture.project_one);
}

#[divan::bench(threads = 1)]
fn full_rows(bencher: divan::Bencher<'_, '_>) {
    run(bencher, |fixture| &fixture.full_rows);
}
