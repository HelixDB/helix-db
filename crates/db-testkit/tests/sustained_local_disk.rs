//! Bounded real-LocalFileSystem sample of every sustained workload class.

#![recursion_limit = "256"]

use std::collections::BTreeSet;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};

use db::encoding::v2::keys::scope::{DataScope, TenantId};
use db::{DbConfig, HelixDB};
use helix_ast::batch;
use helix_ast::expr::Predicate;
use helix_ast::graph::NodeRef;
use helix_ast::index::{IndexSpec, VectorDistanceMetric};
use helix_ast::query::QueryRequest;
use helix_ast::traversal;
use helix_ast::value::{PropertyInput, PropertyValue};
use helix_db_testkit::sustained::{
    ReplicaLagPolicy, SustainedMetrics, WorkloadClass, WorkloadSpec,
};
use helix_planner::context::ParamBindings;
use object_store::local::LocalFileSystem;

const INDEX_OPERATION_TIMEOUT: Duration = Duration::from_secs(5 * 60);

#[test]
fn bounded_local_disk_matrix_covers_every_workload_and_index_family() {
    run_local_disk_profile(LocalDiskProfile::PullRequest);
}

#[test]
#[ignore = "pre-launch ten-run LocalFileSystem workload matrix"]
fn pre_launch_local_disk_matrix_completes_ten_stable_runs() {
    run_local_disk_profile(LocalDiskProfile::PreLaunch);
}

#[derive(Debug, Clone, Copy)]
enum LocalDiskProfile {
    PullRequest,
    PreLaunch,
}

impl LocalDiskProfile {
    fn workload(self, class: WorkloadClass) -> WorkloadSpec {
        match self {
            Self::PullRequest => WorkloadSpec::pull_request(class),
            Self::PreLaunch => WorkloadSpec::pre_launch(class),
        }
    }

    const fn stable_runs(self) -> usize {
        match self {
            Self::PullRequest => 1,
            Self::PreLaunch => 10,
        }
    }

    const fn backfill_seed_documents(self) -> u16 {
        match self {
            Self::PullRequest => 16,
            Self::PreLaunch => 128,
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct TrafficOutcome {
    reads: u64,
    writes: u64,
    conflicts: u64,
    retryable_failures: u64,
}

impl TrafficOutcome {
    fn record(self, metrics: &mut SustainedMetrics) {
        metrics.reads += self.reads;
        metrics.writes += self.writes;
        metrics.conflicts += self.conflicts;
        metrics.retryable_failures += self.retryable_failures;
    }
}

struct GraphTrafficRun {
    start: Arc<tokio::sync::Barrier>,
    completion: tokio::task::JoinHandle<TrafficOutcome>,
}

impl GraphTrafficRun {
    fn spawn(
        spec: WorkloadSpec,
        readers: &[Arc<HelixDB>],
        writer: Arc<HelixDB>,
        source: u64,
        first_rank: i64,
    ) -> Self {
        let reader_count = usize::from(spec.concurrency().readers());
        let writer_count = spec.concurrency().writers();
        assert!(reader_count <= readers.len());
        let selected_readers = readers
            .iter()
            .take(reader_count)
            .map(Arc::clone)
            .collect::<Vec<_>>();
        let start = Arc::new(tokio::sync::Barrier::new(
            reader_count + usize::from(writer_count) + 1,
        ));
        let task_start = Arc::clone(&start);
        let completion = tokio::spawn(async move {
            let mut read_tasks = Vec::new();
            for reader in selected_readers {
                let request_start = Arc::clone(&task_start);
                read_tasks.push(tokio::spawn(async move {
                    request_start.wait().await;
                    reader.query(graph_read_mix(source)).await
                }));
            }

            let mut write_tasks = Vec::new();
            for offset in 0..writer_count {
                let request_start = Arc::clone(&task_start);
                let task_writer = Arc::clone(&writer);
                write_tasks.push(tokio::spawn(async move {
                    request_start.wait().await;
                    task_writer
                        .query(set_rank(source, first_rank + i64::from(offset)))
                        .await
                }));
            }

            let mut outcome = TrafficOutcome::default();
            for task in read_tasks {
                let graph = task.await.unwrap().unwrap();
                assert!(graph["point"][0]["rank"].is_i64());
                assert_eq!(graph["traversal"], 1);
                assert!(graph["aggregate"].as_u64().unwrap() >= 2);
                outcome.reads += 1;
            }
            for task in write_tasks {
                match task.await.unwrap() {
                    Ok(_) => outcome.writes += 1,
                    Err(error) if error.is_transaction_conflict() => outcome.conflicts += 1,
                    Err(db::error::HelixDbError::StaleIndexGeneration { .. }) => {
                        outcome.retryable_failures += 1;
                    }
                    Err(error) => panic!("{:?} workload write failed: {error}", spec.class()),
                }
            }
            outcome
        });
        Self { start, completion }
    }

    async fn release(&self) {
        self.start.wait().await;
    }

    async fn finish(self) -> TrafficOutcome {
        self.completion.await.unwrap()
    }
}

fn run_local_disk_profile(profile: LocalDiskProfile) {
    const TEST_THREAD_STACK_SIZE: usize = 8 * 1024 * 1024;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .thread_stack_size(TEST_THREAD_STACK_SIZE)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        for _ in 0..profile.stable_runs() {
            tokio::spawn(run_bounded_local_disk_matrix(profile))
                .await
                .unwrap();
        }
    });
}

async fn run_bounded_local_disk_matrix(profile: LocalDiskProfile) {
    let root = tempfile::tempdir().unwrap();
    let object_store: Arc<dyn object_store::ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(root.path()).unwrap());
    let database = "sustained-local-disk";
    let mut writer = Arc::new(
        HelixDB::open_with_object_store_and_config(
            database,
            Arc::clone(&object_store),
            DbConfig::new(),
        )
        .await
        .unwrap(),
    );
    let source = insert_document(&writer, 0).await;
    let target = insert_document(&writer, 1).await;
    insert_edge(&writer, source, target).await;
    create_index(&writer, IndexSpec::node_equality("Document", "rank")).await;
    create_index(
        &writer,
        IndexSpec::node_vector(
            "Document",
            "vector",
            NonZeroUsize::new(2).unwrap(),
            VectorDistanceMetric::Euclidean,
            None::<String>,
        ),
    )
    .await;
    create_index(
        &writer,
        IndexSpec::node_text("Document", "text", None::<String>),
    )
    .await;
    wait_for_search_indexes(&writer).await;
    let initial_writer_sequence = writer.flush_writer().await.unwrap();

    let reader_count = profile
        .workload(WorkloadClass::ReadOnly)
        .concurrency()
        .readers();
    let mut readers = Vec::new();
    for _ in 0..reader_count {
        readers.push(Arc::new(
            HelixDB::open_reader_with_object_store_and_config(
                database,
                Arc::clone(&object_store),
                DbConfig::new(),
            )
            .await
            .unwrap(),
        ));
    }
    let mut metrics = SustainedMetrics::default();
    for reader in &readers {
        metrics.observe_replica(
            initial_writer_sequence,
            reader.visible_sequence().await.unwrap(),
        );
    }
    let mut completed = BTreeSet::new();

    let mut read_tasks = Vec::new();
    for reader in &readers {
        let reader = Arc::clone(reader);
        read_tasks.push(tokio::spawn(async move {
            let graph = reader.query(graph_read_mix(source)).await.unwrap();
            let search = reader.query(search_mix()).await.unwrap();
            (graph, search)
        }));
    }
    for task in read_tasks {
        let (graph, search) = task.await.unwrap();
        assert_eq!(graph["point"][0]["rank"], 0);
        assert_eq!(graph["aggregate"], 2);
        assert!(!search["secondary"].as_array().unwrap().is_empty());
        assert!(!search["text"].as_array().unwrap().is_empty());
        assert!(!search["vector"].as_array().unwrap().is_empty());
        metrics.reads += 2;
    }
    completed.insert(WorkloadClass::ReadOnly);

    let writer_tasks = profile
        .workload(WorkloadClass::WriteOnly)
        .concurrency()
        .writers();
    let barrier = Arc::new(tokio::sync::Barrier::new(usize::from(writer_tasks) + 1));
    let mut tasks = Vec::new();
    for version in 1..=writer_tasks {
        let task_writer = Arc::clone(&writer);
        let task_barrier = Arc::clone(&barrier);
        tasks.push(tokio::spawn(async move {
            task_barrier.wait().await;
            task_writer
                .query(set_rank(source, i64::from(version)))
                .await
        }));
    }
    barrier.wait().await;
    for task in tasks {
        match task.await.unwrap() {
            Ok(_) => metrics.writes += 1,
            Err(error) if error.is_transaction_conflict() => metrics.conflicts += 1,
            Err(error) => panic!("write-only workload failed: {error}"),
        }
    }
    assert!(metrics.writes >= 1);
    completed.insert(WorkloadClass::WriteOnly);

    let mixed = GraphTrafficRun::spawn(
        profile.workload(WorkloadClass::ReadsDuringWrites),
        &readers,
        Arc::clone(&writer),
        source,
        100,
    );
    mixed.release().await;
    let mixed = mixed.finish().await;
    assert!(mixed.writes >= 1);
    mixed.record(&mut metrics);
    completed.insert(WorkloadClass::ReadsDuringWrites);

    let backfill_seed_documents = profile.backfill_seed_documents();
    for offset in 0..backfill_seed_documents {
        insert_document(&writer, 1_000 + i64::from(offset)).await;
        metrics.writes += 1;
    }
    let unscoped_document_count = 2 + u64::from(backfill_seed_documents);
    let range_spec = IndexSpec::node_range("Document", "rank");
    let backfill = GraphTrafficRun::spawn(
        profile.workload(WorkloadClass::BackfillUnderTraffic),
        &readers,
        Arc::clone(&writer),
        source,
        200,
    );
    backfill.release().await;
    create_index(&writer, range_spec.clone()).await;
    backfill.finish().await.record(&mut metrics);
    wait_for_range_index(&writer, true).await;
    writer.query(set_rank(source, 250)).await.unwrap();
    metrics.writes += 1;
    let range = writer.query(range_count(1_000)).await.unwrap();
    assert_eq!(range["count"], u64::from(backfill_seed_documents));
    completed.insert(WorkloadClass::BackfillUnderTraffic);

    let lifecycle = GraphTrafficRun::spawn(
        profile.workload(WorkloadClass::LifecycleChurn),
        &readers,
        Arc::clone(&writer),
        source,
        300,
    );
    lifecycle.release().await;
    drop_index(&writer, range_spec.clone()).await;
    wait_for_range_index(&writer, false).await;
    lifecycle.finish().await.record(&mut metrics);
    create_index(&writer, range_spec).await;
    wait_for_range_index(&writer, true).await;
    writer.query(set_rank(source, 350)).await.unwrap();
    metrics.writes += 1;
    completed.insert(WorkloadClass::LifecycleChurn);

    let text_spec = IndexSpec::node_text("Document", "text", None::<String>);
    let maintenance = GraphTrafficRun::spawn(
        profile.workload(WorkloadClass::BackgroundMaintenance),
        &readers,
        Arc::clone(&writer),
        source,
        400,
    );
    maintenance.release().await;
    let text_drop_operation = enqueue_drop_index(&writer, text_spec.clone()).await;
    maintenance.finish().await.record(&mut metrics);
    for reader in &readers {
        reader.close().await.unwrap();
    }
    readers.clear();
    wait_for_operation_success(&writer, &text_drop_operation, "DROP").await;
    create_index(&writer, text_spec).await;
    wait_for_search_indexes(&writer).await;
    let recreated_text_sequence = writer.flush_writer().await.unwrap();
    for _ in 0..reader_count {
        readers.push(Arc::new(
            HelixDB::open_reader_with_object_store_and_config(
                database,
                Arc::clone(&object_store),
                DbConfig::new(),
            )
            .await
            .unwrap(),
        ));
    }
    wait_for_reader_sequence(&readers, recreated_text_sequence).await;
    writer.query(set_rank(source, 450)).await.unwrap();
    metrics.writes += 1;
    completed.insert(WorkloadClass::BackgroundMaintenance);

    let restart = GraphTrafficRun::spawn(
        profile.workload(WorkloadClass::RestartAndRecovery),
        &readers,
        Arc::clone(&writer),
        source,
        500,
    );
    restart.release().await;
    let restart = restart.finish().await;
    assert!(restart.writes >= 1);
    restart.record(&mut metrics);
    let expected_recovered_rank = writer.query(graph_read_mix(source)).await.unwrap()["point"][0]
        ["rank"]
        .as_i64()
        .unwrap();
    let writer_sequence = writer.flush_writer().await.unwrap();
    for reader in &readers {
        let visible = reader.visible_sequence().await.unwrap();
        metrics.observe_replica(writer_sequence, visible);
    }
    writer.close().await.unwrap();
    writer = Arc::new(
        HelixDB::open_with_object_store_and_config(
            database,
            Arc::clone(&object_store),
            DbConfig::new(),
        )
        .await
        .unwrap(),
    );
    metrics.restarts += 1;
    let recovered = writer.query(graph_read_mix(source)).await.unwrap();
    assert_eq!(recovered["point"][0]["rank"], expected_recovered_rank);
    assert_eq!(recovered["aggregate"], unscoped_document_count);
    assert!(!writer.query(search_mix()).await.unwrap()["text"]
        .as_array()
        .unwrap()
        .is_empty());
    completed.insert(WorkloadClass::RestartAndRecovery);

    let tenant_scopes = [
        DataScope::Tenant(TenantId::from_ulid_str("00000000000000000000000001").unwrap()),
        DataScope::Tenant(TenantId::from_ulid_str("00000000000000000000000002").unwrap()),
        DataScope::Tenant(TenantId::from_ulid_str("00000000000000000000000003").unwrap()),
        DataScope::Tenant(TenantId::from_ulid_str("00000000000000000000000004").unwrap()),
    ];
    for (index, scope) in tenant_scopes.into_iter().enumerate() {
        writer
            .query_scoped(insert_document_request(index as i64), scope)
            .await
            .unwrap();
        metrics.writes += 1;
    }
    let tenant_seed_sequence = writer.flush_writer().await.unwrap();
    wait_for_reader_sequence(&readers, tenant_seed_sequence).await;

    let contention = profile.workload(WorkloadClass::MultiTenantContention);
    assert_eq!(contention.concurrency().tenants().get(), 4);
    let contention_readers = usize::from(contention.concurrency().readers());
    let contention_writers = contention.concurrency().writers();
    assert!(contention_readers <= readers.len());
    let barrier = Arc::new(tokio::sync::Barrier::new(
        contention_readers + usize::from(contention_writers) + 1,
    ));
    let mut tenant_reads = Vec::new();
    for (index, reader) in readers.iter().take(contention_readers).enumerate() {
        let task_reader = Arc::clone(reader);
        let task_barrier = Arc::clone(&barrier);
        let scope = tenant_scopes[index % tenant_scopes.len()];
        tenant_reads.push(tokio::spawn(async move {
            task_barrier.wait().await;
            task_reader.query_scoped(count_documents(), scope).await
        }));
    }
    let mut tenant_writes = Vec::new();
    for index in 0..contention_writers {
        let task_writer = Arc::clone(&writer);
        let task_barrier = Arc::clone(&barrier);
        let scope_index = usize::from(index) % tenant_scopes.len();
        let scope = tenant_scopes[scope_index];
        tenant_writes.push(tokio::spawn(async move {
            task_barrier.wait().await;
            (
                scope_index,
                task_writer
                    .query_scoped(insert_document_request(2_000 + i64::from(index)), scope)
                    .await,
            )
        }));
    }
    barrier.wait().await;
    for task in tenant_reads {
        let count = task.await.unwrap().unwrap()["count"].as_u64().unwrap();
        assert!((1..=2).contains(&count));
        metrics.reads += 1;
    }
    let mut successful_writes = [0_u64; 4];
    for task in tenant_writes {
        let (scope_index, result) = task.await.unwrap();
        match result {
            Ok(_) => {
                successful_writes[scope_index] += 1;
                metrics.writes += 1;
            }
            Err(error) if error.is_transaction_conflict() => metrics.conflicts += 1,
            Err(error) => panic!("multi-tenant workload write failed: {error}"),
        }
    }
    assert!(successful_writes.into_iter().sum::<u64>() >= 1);
    for (index, scope) in tenant_scopes.into_iter().enumerate() {
        let count = writer.query_scoped(count_documents(), scope).await.unwrap();
        assert_eq!(count["count"], 1 + successful_writes[index]);
        metrics.reads += 1;
    }
    completed.insert(WorkloadClass::MultiTenantContention);

    assert_eq!(completed, WorkloadClass::ALL.into_iter().collect());
    metrics
        .validate_lag(ReplicaLagPolicy::launch_default())
        .unwrap();
    assert!(metrics.reads > 0 && metrics.writes > 0 && metrics.restarts > 0);
    for reader in readers {
        reader.close().await.unwrap();
    }
    writer.close().await.unwrap();
}

async fn wait_for_search_indexes(db: &HelixDB) {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if db.query(search_mix()).await.is_ok() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("secondary, vector, and text indexes become Active");
}

async fn wait_for_reader_sequence(readers: &[Arc<HelixDB>], writer: db::DatabaseSequence) {
    tokio::time::timeout(Duration::from_secs(30), async {
        for reader in readers {
            loop {
                if reader.visible_sequence().await.unwrap().lag_to(writer) == 0 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        }
    })
    .await
    .expect("reader replicas converge to the flushed writer sequence");
}

async fn wait_for_range_index(db: &HelixDB, expected: bool) {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let context = db
                .planner_context_scoped(ParamBindings::default(), DataScope::LegacyUnscoped)
                .await
                .unwrap();
            let active = !context.indexes.node_range.is_empty();
            if active == expected {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("range lifecycle reaches the requested public visibility");
}

async fn create_index(db: &HelixDB, spec: IndexSpec) {
    let receipt = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let result = db
                .query(QueryRequest::write(
                    batch::write_batch()
                        .var_as(
                            "operation",
                            traversal::g().create_index_if_not_exists(spec.clone()),
                        )
                        .returning(["operation"]),
                ))
                .await;
            match result {
                Ok(receipt) => break receipt,
                Err(db::error::HelixDbError::IndexBusy { state: "dropping" }) => {
                    tokio::task::yield_now().await;
                }
                Err(error) if error.is_transaction_conflict() => {
                    tokio::task::yield_now().await;
                }
                Err(error) => panic!("create-index workload failed: {error}"),
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("CREATE request did not converge for {spec:?}"));
    match receipt["operation"]["kind"].as_str() {
        Some("accepted" | "existing_operation") => {
            let operation_id = receipt["operation"]["operation_id"]
                .as_str()
                .expect("CREATE receipt has an operation ID");
            wait_for_operation_success(db, operation_id, "CREATE").await;
        }
        Some("already_active") => {}
        kind => panic!("unexpected CREATE receipt kind {kind:?}: {receipt}"),
    }
}

async fn drop_index(db: &HelixDB, spec: IndexSpec) {
    let operation_id = enqueue_drop_index(db, spec).await;
    wait_for_operation_success(db, &operation_id, "DROP").await;
}

async fn enqueue_drop_index(db: &HelixDB, spec: IndexSpec) -> String {
    let receipt = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let result = db
                .query(QueryRequest::write(
                    batch::write_batch()
                        .var_as("operation", traversal::g().drop_index(spec.clone()))
                        .returning(["operation"]),
                ))
                .await;
            match result {
                Ok(receipt) => break receipt,
                Err(error) if error.is_transaction_conflict() => {
                    tokio::task::yield_now().await;
                }
                Err(error) => panic!("drop-index workload failed: {error}"),
            }
        }
    })
    .await
    .expect("drop-index workload resolves transaction conflicts");
    let Some(operation_id) = receipt["operation"]["operation_id"].as_str() else {
        panic!("DROP receipt has an operation ID: {receipt}");
    };
    operation_id.to_string()
}

async fn wait_for_operation_success(db: &HelixDB, operation_id: &str, action: &str) {
    let started = Instant::now();
    loop {
        let status = db
            .query(QueryRequest::read(
                batch::read_batch()
                    .var_as("status", traversal::g().get_index_operation(operation_id))
                    .returning(["status"]),
            ))
            .await
            .unwrap();
        match status["status"]["status"].as_str() {
            Some("succeeded") => break,
            Some("queued" | "running") if started.elapsed() < INDEX_OPERATION_TIMEOUT => {
                tokio::task::yield_now().await;
            }
            Some("queued" | "running") => {
                panic!("{action} operation {operation_id} did not converge: {status}")
            }
            Some("blocked" | "aborted") => {
                panic!("{action} operation did not succeed: {status}")
            }
            state => panic!("unexpected {action} operation state {state:?}: {status}"),
        }
    }
}

fn search_mix() -> QueryRequest {
    QueryRequest::read(
        batch::read_batch()
            .var_as(
                "secondary",
                traversal::g()
                    .n_with_label("Document")
                    .has("rank", 0_i64)
                    .id(),
            )
            .var_as(
                "text",
                traversal::g()
                    .text_search_nodes("Document", "text", "shared", 8, None)
                    .id(),
            )
            .var_as(
                "vector",
                traversal::g()
                    .vector_search_nodes("Document", "vector", vec![1.0_f32, 0.0], 8, None)
                    .id(),
            )
            .returning(["secondary", "text", "vector"]),
    )
}

fn graph_read_mix(source: u64) -> QueryRequest {
    QueryRequest::read(
        batch::read_batch()
            .var_as(
                "point",
                traversal::g().n(NodeRef::id(source)).values(vec!["rank"]),
            )
            .var_as(
                "range",
                traversal::g()
                    .n(NodeRef::all())
                    .range(0_usize, 32_usize)
                    .id(),
            )
            .var_as(
                "traversal",
                traversal::g()
                    .n(NodeRef::id(source))
                    .out(Some("LINK"))
                    .count(),
            )
            .var_as(
                "projection",
                traversal::g()
                    .n(NodeRef::id(source))
                    .value_map(Some(vec!["rank", "text"])),
            )
            .var_as("aggregate", traversal::g().n(NodeRef::all()).count())
            .returning(["point", "range", "traversal", "projection", "aggregate"]),
    )
}

fn range_count(minimum_rank: i64) -> QueryRequest {
    QueryRequest::read(
        batch::read_batch()
            .var_as(
                "count",
                traversal::g()
                    .n_with_label_where("Document", Predicate::gte("rank", minimum_rank))
                    .count(),
            )
            .returning(["count"]),
    )
}

fn set_rank(source: u64, rank: i64) -> QueryRequest {
    QueryRequest::write(
        batch::write_batch()
            .var_as(
                "updated",
                traversal::g()
                    .n(NodeRef::id(source))
                    .set_property("rank", rank),
            )
            .returning(["updated"]),
    )
}

fn insert_document_request(rank: i64) -> QueryRequest {
    QueryRequest::write(
        batch::write_batch()
            .var_as(
                "created",
                traversal::g()
                    .add_n(
                        "Document",
                        vec![
                            ("rank", PropertyInput::from(rank)),
                            ("text", PropertyInput::from("shared text")),
                            (
                                "vector",
                                PropertyInput::from(PropertyValue::F32Array(vec![1.0, 0.0])),
                            ),
                        ],
                    )
                    .id(),
            )
            .returning(["created"]),
    )
}

fn count_documents() -> QueryRequest {
    QueryRequest::read(
        batch::read_batch()
            .var_as("count", traversal::g().n(NodeRef::all()).count())
            .returning(["count"]),
    )
}

async fn insert_document(db: &HelixDB, rank: i64) -> u64 {
    let response = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            match db.query(insert_document_request(rank)).await {
                Ok(response) => break response,
                Err(error) if error.is_transaction_conflict() => tokio::task::yield_now().await,
                Err(error) => panic!("setup document insert failed: {error}"),
            }
        }
    })
    .await
    .expect("setup document insert resolves transaction conflicts");
    response["created"][0].as_u64().unwrap()
}

async fn insert_edge(db: &HelixDB, source: u64, target: u64) {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let request = QueryRequest::write(
                batch::write_batch()
                    .var_as(
                        "created",
                        traversal::g().n(NodeRef::id(source)).add_e(
                            "LINK",
                            NodeRef::id(target),
                            Vec::<(&str, PropertyInput)>::new(),
                        ),
                    )
                    .returning(["created"]),
            );
            match db.query(request).await {
                Ok(_) => break,
                Err(error) if error.is_transaction_conflict() => tokio::task::yield_now().await,
                Err(error) => panic!("setup edge insert failed: {error}"),
            }
        }
    })
    .await
    .expect("setup edge insert resolves transaction conflicts");
}
