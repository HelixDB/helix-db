//! Foreground graph-mutation benchmark.
//!
//! Graph creation and physical planning happen outside measurement. Each
//! sample executes one already-planned transaction over the requested entity
//! count so planner and setup costs do not hide mutation-path regressions.

#![recursion_limit = "256"]

use std::sync::Arc;

use db::query_service::HelixQueryService;
use db::{HelixDB, HelixDbSource};
use helix_ast::prelude::*;
use helix_ast::query::QueryRequest;
use helix_planner::{context::ParamBindings, exec::ExecutablePlan, planning};

#[global_allocator]
static ALLOC: divan::AllocProfiler = divan::AllocProfiler::system();

// Bound setup planning so the benchmark does not measure or exhaust the
// production optimizer's request-wide exploration budget.
const SEED_BATCH_SIZE: usize = 100;

fn main() {
    divan::main();
}

struct MutationFixture {
    runtime: tokio::runtime::Runtime,
    db: Arc<HelixDB>,
    service: HelixQueryService,
    no_op_node_set: ExecutablePlan,
    no_op_node_set_request: QueryRequest,
    changed_node_sets: [ExecutablePlan; 2],
    changed_node_set_requests: [QueryRequest; 2],
    topology_edge_cycle: [ExecutablePlan; 2],
}

impl MutationFixture {
    fn new(entity_count: usize) -> Self {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("mutation benchmark runtime starts");
        let (db, plans) = runtime.block_on(seed_and_plan(entity_count));
        let service = HelixQueryService::new(Arc::clone(&db));
        Self {
            runtime,
            db,
            service,
            no_op_node_set: plans.no_op,
            no_op_node_set_request: plans.no_op_request,
            changed_node_sets: plans.changed,
            changed_node_set_requests: plans.changed_requests,
            topology_edge_cycle: plans.topology_edge_cycle,
        }
    }
}

struct MutationPlans {
    no_op: ExecutablePlan,
    no_op_request: QueryRequest,
    changed: [ExecutablePlan; 2],
    changed_requests: [QueryRequest; 2],
    topology_edge_cycle: [ExecutablePlan; 2],
}

async fn seed_and_plan(entity_count: usize) -> (Arc<HelixDB>, MutationPlans) {
    let db = Arc::new(
        HelixDB::open(HelixDbSource::InMemory {
            database: format!("write-path-mutation-bench-{entity_count}"),
        })
        .await
        .expect("mutation benchmark database opens"),
    );
    db.wait_for_startup_cache_warm().await;

    for batch_start in (0..entity_count).step_by(SEED_BATCH_SIZE) {
        let batch_end = (batch_start + SEED_BATCH_SIZE).min(entity_count);
        let mut create = write_batch();
        for node_id in batch_start..batch_end {
            let variable = format!("node_{node_id}");
            create = create.var_as(
                &variable,
                g().add_n(
                    "MutationBenchNode",
                    vec![
                        ("status", PropertyInput::from("ready")),
                        (
                            "external_id",
                            PropertyInput::from(format!("node-{node_id}")),
                        ),
                        ("attribute_1", PropertyInput::from(node_id as i64)),
                        ("attribute_2", PropertyInput::from(node_id as i64 + 1)),
                        ("attribute_3", PropertyInput::from(node_id as i64 + 2)),
                        ("attribute_4", PropertyInput::from(node_id as i64 + 3)),
                        ("attribute_5", PropertyInput::from(node_id as i64 + 4)),
                        ("attribute_6", PropertyInput::from(node_id as i64 + 5)),
                        ("attribute_7", PropertyInput::from(node_id as i64 + 6)),
                        ("attribute_8", PropertyInput::from(node_id as i64 + 7)),
                    ],
                ),
            );
        }
        let create_plan =
            planning::plan_write_batch(&create, &db.planner_context(ParamBindings::default()))
                .expect("mutation benchmark graph plans");
        db.execute(&create_plan, ParamBindings::default())
            .await
            .expect("mutation benchmark graph batch is created");
    }

    let no_op = write_batch().var_as(
        "updated",
        g().n(NodeRef::all()).set_property("status", "ready"),
    );
    let no_op_node_set =
        planning::plan_write_batch(&no_op, &db.planner_context(ParamBindings::default()))
            .expect("no-op node update plans");
    db.execute(&no_op_node_set, ParamBindings::default())
        .await
        .expect("no-op node update warms");

    let changed_writes = ["phase-a", "phase-b"].map(|status| {
        write_batch().var_as(
            "updated",
            g().n(NodeRef::all()).set_property("status", status),
        )
    });
    let changed = std::array::from_fn(|index| {
        planning::plan_write_batch(
            &changed_writes[index],
            &db.planner_context(ParamBindings::default()),
        )
        .expect("changed node update plans")
    });
    let changed_requests = changed_writes.map(QueryRequest::write);
    let topology_edge_writes = [
        write_batch().var_as(
            "edges",
            g().n(NodeRef::all()).add_e(
                "MutationBenchEdge",
                NodeRef::id(0),
                Vec::<(String, PropertyInput)>::new(),
            ),
        ),
        write_batch().var_as(
            "dropped",
            g().n(NodeRef::all())
                .drop_edge_labeled(NodeRef::id(0), "MutationBenchEdge"),
        ),
    ];
    let topology_edge_cycle = std::array::from_fn(|index| {
        planning::plan_write_batch(
            &topology_edge_writes[index],
            &db.planner_context(ParamBindings::default()),
        )
        .expect("topology edge cycle plans")
    });

    (
        db,
        MutationPlans {
            no_op: no_op_node_set,
            no_op_request: QueryRequest::write(no_op),
            changed,
            changed_requests,
            topology_edge_cycle,
        },
    )
}

#[divan::bench(args = [1, 10, 100, 500], threads = 1)]
fn query_service_no_op_node_set(bencher: divan::Bencher<'_, '_>, entity_count: usize) {
    let fixture = MutationFixture::new(entity_count);
    bencher.bench_local(|| {
        divan::black_box(
            fixture
                .runtime
                .block_on(
                    fixture
                        .service
                        .execute_query(fixture.no_op_node_set_request.clone()),
                )
                .expect("query-service no-op node update executes"),
        )
    });
}

#[divan::bench(args = [1, 10, 100, 500], threads = 1)]
fn no_op_node_set(bencher: divan::Bencher<'_, '_>, entity_count: usize) {
    let fixture = MutationFixture::new(entity_count);
    bencher.bench_local(|| {
        divan::black_box(
            fixture
                .runtime
                .block_on(
                    fixture
                        .db
                        .execute(&fixture.no_op_node_set, ParamBindings::default()),
                )
                .expect("no-op node update executes"),
        )
    });
}

#[divan::bench(args = [1, 10, 100, 500], threads = 1)]
fn query_service_changed_node_set(bencher: divan::Bencher<'_, '_>, entity_count: usize) {
    let fixture = MutationFixture::new(entity_count);
    let mut phase = 0;
    bencher.bench_local(|| {
        let request = fixture.changed_node_set_requests[phase].clone();
        phase ^= 1;
        divan::black_box(
            fixture
                .runtime
                .block_on(fixture.service.execute_query(request))
                .expect("query-service changed node update executes"),
        )
    });
}

#[divan::bench(args = [1, 10, 100, 500], threads = 1)]
fn changed_node_set(bencher: divan::Bencher<'_, '_>, entity_count: usize) {
    let fixture = MutationFixture::new(entity_count);
    let mut phase = 0;
    bencher.bench_local(|| {
        let plan = &fixture.changed_node_sets[phase];
        phase ^= 1;
        divan::black_box(
            fixture
                .runtime
                .block_on(fixture.db.execute(plan, ParamBindings::default()))
                .expect("changed node update executes"),
        )
    });
}

#[divan::bench(args = [1, 10, 100, 500], threads = 1)]
fn topology_edge_cycle(bencher: divan::Bencher<'_, '_>, entity_count: usize) {
    let fixture = MutationFixture::new(entity_count);
    bencher.bench_local(|| {
        for plan in &fixture.topology_edge_cycle {
            divan::black_box(
                fixture
                    .runtime
                    .block_on(fixture.db.execute(plan, ParamBindings::default()))
                    .expect("topology edge cycle executes"),
            );
        }
    });
}
