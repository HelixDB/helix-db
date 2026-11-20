//! Projection materialization benchmark for HEL-726.
//!
//! The fixture mirrors GraphSelection's wide node and edge projections. Graph
//! creation and physical planning happen once outside measurement; each sample
//! executes only the already-planned interpreter read path.

#![recursion_limit = "256"]

use std::sync::OnceLock;

use db::{HelixDB, HelixDbSource};
use helix_ast::prelude::*;
use helix_planner::{context::ParamBindings, exec::ExecutablePlan, planning};

#[cfg(not(test))]
const ENTITY_COUNT: u64 = 512;
// `cargo test --all-targets` executes the Divan harness. Keep that correctness
// smoke test small while preserving the full benchmark fixture in bench mode.
#[cfg(test)]
const ENTITY_COUNT: u64 = 4;

#[global_allocator]
static ALLOC: divan::AllocProfiler = divan::AllocProfiler::system();

fn main() {
    divan::main();
}

struct ProjectionFixture {
    runtime: tokio::runtime::Runtime,
    db: HelixDB,
    node_plan: ExecutablePlan,
    edge_plan: ExecutablePlan,
    endpoint_plan: ExecutablePlan,
}

impl ProjectionFixture {
    fn new() -> Self {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("benchmark runtime starts");
        let (db, node_plan, edge_plan, endpoint_plan) = runtime.block_on(seed_and_plan());
        Self {
            runtime,
            db,
            node_plan,
            edge_plan,
            endpoint_plan,
        }
    }
}

fn fixture() -> &'static ProjectionFixture {
    static FIXTURE: OnceLock<ProjectionFixture> = OnceLock::new();
    FIXTURE.get_or_init(ProjectionFixture::new)
}

async fn seed_and_plan() -> (HelixDB, ExecutablePlan, ExecutablePlan, ExecutablePlan) {
    let db = HelixDB::open(HelixDbSource::InMemory {
        database: "projection-materialization-bench".to_string(),
    })
    .await
    .expect("benchmark database opens");

    let mut create = write_batch();
    for node_id in 0..ENTITY_COUNT {
        let variable = format!("node_{node_id}");
        create = create.var_as(
            &variable,
            g().add_n(
                "ProjectionBenchNode",
                vec![
                    (
                        "external_id",
                        PropertyInput::from(format!("node-{node_id}")),
                    ),
                    ("attribute_1", PropertyInput::from(node_id as i64)),
                    ("attribute_2", PropertyInput::from(node_id as i64 + 1)),
                    ("attribute_3", PropertyInput::from(node_id as i64 + 2)),
                    ("attribute_4", PropertyInput::from(node_id as i64 + 3)),
                ],
            ),
        );
    }
    for edge_id in 0..ENTITY_COUNT {
        let from = format!("node_{edge_id}");
        let to = format!("node_{}", (edge_id + 1) % ENTITY_COUNT);
        let variable = format!("edge_{edge_id}");
        create = create.var_as(
            &variable,
            g().n(NodeRef::var(from))
                .add_e(
                    "ProjectionBenchEdge",
                    NodeRef::var(to),
                    vec![
                        (
                            "graphify_key",
                            PropertyInput::from(format!("edge-{edge_id}")),
                        ),
                        ("weight", PropertyInput::from(edge_id as f64 / 10.0)),
                        ("attribute_1", PropertyInput::from(edge_id as i64)),
                        ("attribute_2", PropertyInput::from(edge_id as i64 + 1)),
                        ("attribute_3", PropertyInput::from(edge_id as i64 + 2)),
                        ("attribute_4", PropertyInput::from(edge_id as i64 + 3)),
                    ],
                )
                .count(),
        );
    }

    let create_plan =
        planning::plan_write_batch(&create, &db.planner_context(ParamBindings::default()))
            .expect("benchmark graph plans");
    db.execute(&create_plan, ParamBindings::default())
        .await
        .expect("benchmark graph is created");

    let node_query = read_batch()
        .var_as(
            "rows",
            g().n(NodeRef::all()).project(vec![
                Projection::property("$label", "label"),
                Projection::property("external_id", "external_id"),
                Projection::property("attribute_1", "attribute_1"),
                Projection::property("attribute_2", "attribute_2"),
                Projection::property("attribute_3", "attribute_3"),
                Projection::property("attribute_4", "attribute_4"),
                Projection::property("$id", "id"),
            ]),
        )
        .returning(["rows"]);
    let edge_query = read_batch()
        .var_as(
            "rows",
            g().e(EdgeRef::all()).project(vec![
                Projection::property("$label", "label"),
                Projection::property("graphify_key", "graphify_key"),
                Projection::property("weight", "weight"),
                Projection::property("attribute_1", "attribute_1"),
                Projection::property("attribute_2", "attribute_2"),
                Projection::property("attribute_3", "attribute_3"),
                Projection::property("attribute_4", "attribute_4"),
                Projection::from_endpoint("$id", "from"),
                Projection::to_endpoint("$id", "to"),
                Projection::property("$id", "id"),
            ]),
        )
        .returning(["rows"]);
    let endpoint_query = read_batch()
        .var_as(
            "rows",
            g().e(EdgeRef::all())
                .values(vec!["$from.external_id", "$to.external_id"]),
        )
        .returning(["rows"]);
    let planner_context = db.planner_context(ParamBindings::default());
    let node_plan =
        planning::plan_read_batch(&node_query, &planner_context).expect("node projection plans");
    let edge_plan =
        planning::plan_read_batch(&edge_query, &planner_context).expect("edge projection plans");

    let endpoint_plan = planning::plan_read_batch(&endpoint_query, &planner_context)
        .expect("endpoint projection plans");

    db.execute(&endpoint_plan, ParamBindings::default())
        .await
        .expect("endpoint projection warms");
    db.execute(&node_plan, ParamBindings::default())
        .await
        .expect("node projection warms");
    db.execute(&edge_plan, ParamBindings::default())
        .await
        .expect("edge projection warms");

    (db, node_plan, edge_plan, endpoint_plan)
}

#[divan::bench(threads = 1)]
fn nodes(bencher: divan::Bencher<'_, '_>) {
    let fixture = fixture();
    bencher.bench_local(|| {
        divan::black_box(
            fixture
                .runtime
                .block_on(
                    fixture
                        .db
                        .execute(&fixture.node_plan, ParamBindings::default()),
                )
                .expect("node projection executes"),
        )
    });
}

#[divan::bench(threads = 1)]
fn edges(bencher: divan::Bencher<'_, '_>) {
    let fixture = fixture();
    bencher.bench_local(|| {
        divan::black_box(
            fixture
                .runtime
                .block_on(
                    fixture
                        .db
                        .execute(&fixture.edge_plan, ParamBindings::default()),
                )
                .expect("edge projection executes"),
        )
    });
}

#[divan::bench(threads = 1)]
fn endpoint_properties(bencher: divan::Bencher<'_, '_>) {
    let fixture = fixture();
    bencher.bench_local(|| {
        divan::black_box(
            fixture
                .runtime
                .block_on(
                    fixture
                        .db
                        .execute(&fixture.endpoint_plan, ParamBindings::default()),
                )
                .expect("endpoint projection executes"),
        )
    });
}
