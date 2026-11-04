//! Text transaction batching benchmark fixture and its contract.
//!
//! `bench-internals` exposes the benchmark sample to the
//! `text_transaction_batching` bench; unit tests run the batching contract
//! over the same fixtures. Both need `index-lifecycle-testing` scheduling.

// The bench and the unit-test contract each use only part of the fixtures.
#![cfg_attr(not(all(test, feature = "bench-internals")), allow(dead_code))]

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::stream::BoxStream;
use helix_ast::value::PropertyValue as AstPropertyValue;
use helix_planner::{catalog, context, cost, exec, ir, properties, trace};
use slatedb::object_store::local::LocalFileSystem;
use slatedb::object_store::path::Path;
use slatedb::object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as ObjectStoreResult,
};
use slatedb::Db;

#[cfg(test)]
use super::text_fixtures::seed_active_text_generation_with;
use super::text_fixtures::{scoped_key, seed_active_text_generation};
use super::*;
use crate::config::{
    CacheConfig, CacheMode, DbConfig, TextElementType, TextIndexDefinition, VectorMemorySettings,
};
use crate::encoding::v2::keys as index_keys;
use crate::encoding::v2::keys::scope::DataScope;
use crate::encoding::v2::values as index_values;
use crate::index_lifecycle::{IndexGenerationId, IndexId, ValidatedDynamicIndexDefinition};
use crate::index_lifecycle_testing::LifecycleTestScheduling;
use crate::search::text_index_name;

const LABEL: &str = "TextTransactionBenchmarkDocument";
const PROPERTY: &str = "body";
const QUERY: &str = "transactionbatchtoken";
const QUIESCENCE_POLL: Duration = Duration::from_millis(25);
const QUIESCENCE_STABLE: Duration = Duration::from_millis(750);
const QUIESCENCE_TIMEOUT: Duration = Duration::from_secs(120);

/// One deterministic text-transaction benchmark case.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TextTransactionBatchBenchmarkCase {
    /// Inserts committed by one transaction.
    pub batch_size: usize,
    /// Synthetic delay applied to each FTS blob upload.
    pub upload_latency_millis: u64,
}

impl TextTransactionBatchBenchmarkCase {
    /// Validates one non-empty benchmark case.
    pub fn try_new(batch_size: usize, upload_latency_millis: u64) -> Result<Self> {
        if batch_size == 0 {
            return Err(HelixDbError::Config(
                "text transaction benchmark batch size must be positive".to_string(),
            ));
        }
        Ok(Self {
            batch_size,
            upload_latency_millis,
        })
    }
}

/// Measurements from one clean local-store benchmark sample.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TextTransactionBatchBenchmarkSample {
    /// Executed benchmark case.
    pub case: TextTransactionBatchBenchmarkCase,
    /// End-to-end transaction latency.
    pub transaction_latency_ns: u64,
    /// FTS immutable blob uploads performed by the transaction.
    pub upload_count: u64,
    /// FTS immutable payload bytes uploaded by the transaction.
    pub upload_bytes: u64,
    /// Manifest split-count growth caused by the transaction.
    pub manifest_split_growth: u64,
    /// Cold immediate search latency while compaction is held.
    pub immediate_search_latency_ns: u64,
    /// Cold search latency after automatic compaction reaches quiescence.
    pub post_compaction_search_latency_ns: u64,
    /// Split count after compaction reaches quiescence.
    pub post_compaction_split_count: u64,
    /// Stable digest of the immediate and post-compaction result IDs.
    pub search_digest: String,
}

#[derive(Debug)]
struct MeasuredObjectStore {
    inner: Arc<dyn ObjectStore>,
    upload_latency: Duration,
    fts_puts: AtomicU64,
    fts_bytes: AtomicU64,
}

impl MeasuredObjectStore {
    fn new(inner: Arc<dyn ObjectStore>, upload_latency: Duration) -> Self {
        Self {
            inner,
            upload_latency,
            fts_puts: AtomicU64::new(0),
            fts_bytes: AtomicU64::new(0),
        }
    }

    fn reset_fts_uploads(&self) {
        self.fts_puts.store(0, Ordering::Relaxed);
        self.fts_bytes.store(0, Ordering::Relaxed);
    }

    fn fts_uploads(&self) -> (u64, u64) {
        (
            self.fts_puts.load(Ordering::Relaxed),
            self.fts_bytes.load(Ordering::Relaxed),
        )
    }

    fn is_fts_blob(location: &Path) -> bool {
        location.as_ref().contains("/fts/blobs/")
    }
}

impl fmt::Display for MeasuredObjectStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("text-transaction-benchmark-local-store")
    }
}

#[async_trait::async_trait]
impl ObjectStore for MeasuredObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        if Self::is_fts_blob(location) {
            if !self.upload_latency.is_zero() {
                tokio::time::sleep(self.upload_latency).await;
            }
            self.fts_puts.fetch_add(1, Ordering::Relaxed);
            self.fts_bytes.fetch_add(
                u64::try_from(payload.content_length()).unwrap_or(u64::MAX),
                Ordering::Relaxed,
            );
        }
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> ObjectStoreResult<GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, ObjectStoreResult<Path>>,
    ) -> BoxStream<'static, ObjectStoreResult<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> ObjectStoreResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> ObjectStoreResult<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

fn benchmark_config() -> DbConfig {
    DbConfig::new().with_cache(CacheConfig::new(
        VectorMemorySettings::default(),
        CacheMode::VectorMemoryOnly,
    ))
}

fn name(value: &str) -> ir::NonEmptyString {
    ir::NonEmptyString::new(value).expect("benchmark identifier is non-empty")
}

fn step(id: usize, dependencies: Vec<exec::ExecStepId>, op: exec::ExecOp) -> exec::ExecStep {
    exec::ExecStep {
        id: exec::ExecStepId::new(id).expect("benchmark step IDs are positive"),
        dependencies,
        output: ir::BatchOutputPlan::Discard,
        semantic_return_shape: None,
        condition: exec::ExecCondition::Always,
        op,
        schedule: exec::ExecSchedule::Pipeline,
        delivered: properties::DeliveredProperties::default(),
        cost: cost::CostVector::ZERO,
    }
}

fn executable(kind: ir::PlanKind, steps: Vec<exec::ExecStep>, root: usize) -> exec::ExecutablePlan {
    exec::ExecutablePlan::new(
        kind,
        ir::ReturnPlan::None,
        ir::AtLeast::<_, 1>::try_from_vec(steps).expect("benchmark plan is non-empty"),
        exec::ExecStepId::new(root).expect("benchmark root ID is positive"),
        trace::PlanningTrace::default(),
        exec::PlannerMetrics::default(),
    )
    .expect("benchmark dependencies form a valid executable plan")
}

fn insert_plan(batch_size: usize) -> exec::ExecutablePlan {
    let mut steps = Vec::with_capacity(batch_size);
    for offset in 0..batch_size {
        let id = offset + 1;
        let dependencies = (id > 1)
            .then(|| exec::ExecStepId::new(id - 1).expect("previous step ID is positive"))
            .into_iter()
            .collect();
        let properties = ir::PropertyAssignments::try_from_vec(vec![(
            name(PROPERTY),
            ir::PropertyInputPlan::Value(AstPropertyValue::from(format!(
                "{QUERY} document {offset:04}"
            ))),
        )])
        .expect("benchmark property assignment validates");
        steps.push(step(
            id,
            dependencies,
            exec::ExecOp::Mutation {
                plan: exec::ExecMutationPlan::AddNodeSource {
                    label: name(LABEL),
                    properties,
                },
            },
        ));
    }
    executable(ir::PlanKind::Write, steps, batch_size)
}

fn dual_index_insert_plan(batch_size: usize) -> exec::ExecutablePlan {
    let mut steps = Vec::with_capacity(batch_size);
    for offset in 0..batch_size {
        let id = offset + 1;
        let dependencies = (id > 1)
            .then(|| exec::ExecStepId::new(id - 1).expect("previous step ID is positive"))
            .into_iter()
            .collect();
        let properties = ir::PropertyAssignments::try_from_vec(vec![
            (
                name(PROPERTY),
                ir::PropertyInputPlan::Value(AstPropertyValue::from(format!(
                    "{QUERY} body {offset:04}"
                ))),
            ),
            (
                name("title"),
                ir::PropertyInputPlan::Value(AstPropertyValue::from(format!(
                    "{QUERY} title {offset:04}"
                ))),
            ),
        ])
        .expect("dual-index assignments validate");
        steps.push(step(
            id,
            dependencies,
            exec::ExecOp::Mutation {
                plan: exec::ExecMutationPlan::AddNodeSource {
                    label: name(LABEL),
                    properties,
                },
            },
        ));
    }
    executable(ir::PlanKind::Write, steps, batch_size)
}

fn partitioned_insert_plan() -> exec::ExecutablePlan {
    let mut steps = Vec::with_capacity(2);
    for (offset, tenant) in ["acme", "globex"].into_iter().enumerate() {
        let id = offset + 1;
        let dependencies = (id > 1)
            .then(|| exec::ExecStepId::new(id - 1).expect("previous step ID is positive"))
            .into_iter()
            .collect();
        let properties = ir::PropertyAssignments::try_from_vec(vec![
            (
                name(PROPERTY),
                ir::PropertyInputPlan::Value(AstPropertyValue::from(format!(
                    "{QUERY} tenant {tenant}"
                ))),
            ),
            (
                name("tenant"),
                ir::PropertyInputPlan::Value(AstPropertyValue::from(tenant)),
            ),
        ])
        .expect("partitioned assignments validate");
        steps.push(step(
            id,
            dependencies,
            exec::ExecOp::Mutation {
                plan: exec::ExecMutationPlan::AddNodeSource {
                    label: name(LABEL),
                    properties,
                },
            },
        ));
    }
    executable(ir::PlanKind::Write, steps, 2)
}

fn delete_all_plan() -> exec::ExecutablePlan {
    let access = exec::ExecStepId::new(1).expect("access step ID is positive");
    executable(
        ir::PlanKind::Write,
        vec![
            step(
                1,
                Vec::new(),
                exec::ExecOp::Access {
                    plan: Box::new(exec::ExecAccessPlan::Node(
                        exec::ExecNodeAccessPlan::AllScan,
                    )),
                },
            ),
            step(
                2,
                vec![access],
                exec::ExecOp::Mutation {
                    plan: exec::ExecMutationPlan::Drop,
                },
            ),
        ],
        2,
    )
}

fn create_then_delete_plan() -> exec::ExecutablePlan {
    let created = exec::ExecStepId::new(1).expect("create step ID is positive");
    executable(
        ir::PlanKind::Write,
        vec![
            step(
                1,
                Vec::new(),
                exec::ExecOp::Mutation {
                    plan: exec::ExecMutationPlan::AddNodeSource {
                        label: name(LABEL),
                        properties: ir::PropertyAssignments::try_from_vec(vec![(
                            name(PROPERTY),
                            ir::PropertyInputPlan::Value(AstPropertyValue::from(
                                "create then delete",
                            )),
                        )])
                        .expect("create/delete properties validate"),
                    },
                },
            ),
            step(
                2,
                vec![created],
                exec::ExecOp::Mutation {
                    plan: exec::ExecMutationPlan::Drop,
                },
            ),
        ],
        2,
    )
}

fn access_and_mutate_plan(mutations: Vec<exec::ExecMutationPlan>) -> exec::ExecutablePlan {
    let mut steps = Vec::with_capacity(mutations.len() + 1);
    steps.push(step(
        1,
        Vec::new(),
        exec::ExecOp::Access {
            plan: Box::new(exec::ExecAccessPlan::Node(
                exec::ExecNodeAccessPlan::FromParam {
                    param: name("target"),
                },
            )),
        },
    ));
    for (offset, plan) in mutations.into_iter().enumerate() {
        let id = offset + 2;
        steps.push(step(
            id,
            vec![exec::ExecStepId::new(id - 1).expect("previous step ID is positive")],
            exec::ExecOp::Mutation { plan },
        ));
    }
    let root = steps.len();
    executable(ir::PlanKind::Write, steps, root)
}

fn set_body(value: &str) -> exec::ExecMutationPlan {
    set_property(PROPERTY, value)
}

fn set_property(property: &str, value: &str) -> exec::ExecMutationPlan {
    exec::ExecMutationPlan::SetProperty {
        name: name(property),
        value: ir::PropertyInputPlan::Value(AstPropertyValue::from(value)),
    }
}

fn write_read_write_plan() -> exec::ExecutablePlan {
    let first = exec::ExecStepId::new(1).expect("first step ID is positive");
    let read = exec::ExecStepId::new(2).expect("read step ID is positive");
    let properties = |suffix: &str| {
        ir::PropertyAssignments::try_from_vec(vec![(
            name(PROPERTY),
            ir::PropertyInputPlan::Value(AstPropertyValue::from(format!("{QUERY} {suffix}"))),
        )])
        .expect("barrier properties validate")
    };
    executable(
        ir::PlanKind::Write,
        vec![
            step(
                1,
                Vec::new(),
                exec::ExecOp::Mutation {
                    plan: exec::ExecMutationPlan::AddNodeSource {
                        label: name(LABEL),
                        properties: properties("before read"),
                    },
                },
            ),
            step(
                2,
                vec![first],
                exec::ExecOp::Access {
                    plan: Box::new(exec::ExecAccessPlan::Node(
                        exec::ExecNodeAccessPlan::TextSearch {
                            key: catalog::NodeSearchIndexKey::try_new(LABEL, PROPERTY)
                                .expect("barrier search key validates"),
                            index: ir::SearchIndexPlan {
                                index_id: name(&text_index_name(
                                    TextElementType::Node,
                                    LABEL,
                                    PROPERTY,
                                )),
                                tenant: ir::SearchTenantPlan::Unscoped,
                            },
                            query_text: ir::TextQueryInputPlan::Text(name(QUERY)),
                            k: ir::SearchLimitPlan::Literal(std::num::NonZeroUsize::MIN),
                        },
                    )),
                },
            ),
            step(
                3,
                vec![read],
                exec::ExecOp::Mutation {
                    plan: exec::ExecMutationPlan::AddNodeSource {
                        label: name(LABEL),
                        properties: properties("after read"),
                    },
                },
            ),
        ],
        3,
    )
}

fn search_plan_for(query: &str, batch_size: usize) -> exec::ExecutablePlan {
    let access_id = exec::ExecStepId::new(1).expect("benchmark access ID is positive");
    executable(
        ir::PlanKind::Read,
        vec![
            step(
                1,
                Vec::new(),
                exec::ExecOp::Access {
                    plan: Box::new(exec::ExecAccessPlan::Node(
                        exec::ExecNodeAccessPlan::TextSearch {
                            key: catalog::NodeSearchIndexKey::try_new(LABEL, PROPERTY)
                                .expect("benchmark text key validates"),
                            index: ir::SearchIndexPlan {
                                index_id: name(&text_index_name(
                                    TextElementType::Node,
                                    LABEL,
                                    PROPERTY,
                                )),
                                tenant: ir::SearchTenantPlan::Unscoped,
                            },
                            query_text: ir::TextQueryInputPlan::Text(name(query)),
                            k: ir::SearchLimitPlan::Literal(
                                std::num::NonZeroUsize::new(batch_size)
                                    .expect("benchmark result limit is positive"),
                            ),
                        },
                    )),
                },
            ),
            step(
                2,
                vec![access_id],
                exec::ExecOp::Project {
                    projection: ir::ProjectionPlan::Id,
                },
            ),
        ],
        2,
    )
}

async fn manifest_split_count(db: &Db, root: index_keys::TextManifestRootKey) -> Result<u64> {
    let bytes = db
        .get(scoped_key(index_keys::ScopedKey::TextManifestRoot(root)))
        .await?
        .ok_or_else(|| {
            HelixDbError::InvariantViolation(
                "text transaction benchmark manifest root disappeared".to_string(),
            )
        })?;
    let value = index_values::decode_manifest_root(&bytes)?;
    Ok(value.split_count())
}

async fn manifest_root(
    db: &Db,
    root: index_keys::TextManifestRootKey,
) -> Result<crate::index_lifecycle::work::TextManifestRootValue> {
    let bytes = db
        .get(scoped_key(index_keys::ScopedKey::TextManifestRoot(root)))
        .await?
        .ok_or_else(|| {
            HelixDbError::InvariantViolation(
                "text transaction contract manifest root disappeared".to_string(),
            )
        })?;
    let value = index_values::decode_manifest_root(&bytes)?;
    Ok(value)
}

async fn entity_state(
    db: &Db,
    root: index_keys::TextManifestRootKey,
    entity_id: u64,
) -> Result<crate::index_lifecycle::work::TextEntityStateValue> {
    let key = scoped_key(index_keys::ScopedKey::TextEntityState(
        index_keys::TextEntityStateKey {
            root,
            entity: index_keys::IndexEntity {
                kind: crate::index_lifecycle::IndexElementKind::Node,
                id: crate::index_lifecycle::IndexEntityId::new(entity_id),
            },
        },
    ));
    let bytes = db.get(key).await?.ok_or_else(|| {
        HelixDbError::InvariantViolation(
            "text transaction contract entity state disappeared".to_string(),
        )
    })?;
    let state = index_values::decode_text_entity_state(&bytes)?;
    Ok(state)
}

async fn compaction_pointer_revision(
    db: &Db,
    definition: &TextIndexDefinition,
    root: index_keys::TextManifestRootKey,
    page: u32,
) -> Result<u64> {
    let identity = ValidatedDynamicIndexDefinition::try_from(definition.clone())
        .expect("text definition validates for V2")
        .identity();
    let target = index_keys::TextCompactionTarget::try_new(
        DataScope::LegacyUnscoped,
        identity,
        root.index_id,
        root.generation,
        root.partition,
        page,
    )?;
    let key = index_keys::ManagedIndexKey::Global {
        kind: index_keys::GlobalKey::TextCompactionPointer(target),
    }
    .to_bytes();
    let bytes = db.get(key).await?.ok_or_else(|| {
        HelixDbError::InvariantViolation(
            "text transaction contract compaction pointer disappeared".to_string(),
        )
    })?;
    let crate::index_lifecycle::IndexV2MetadataValue::TextCompactionPointer(pointer) =
        index_values::decode_metadata_value(&bytes)?
    else {
        return Err(HelixDbError::InvariantViolation(
            "text transaction contract pointer key contains another value kind".to_string(),
        ));
    };
    Ok(pointer.revision.get())
}

fn tenant_root(tenant: &str) -> index_keys::TextManifestRootKey {
    let tenant_value = Property::string("tenant", tenant).value;
    let partition = crate::index_lifecycle::work::TextPartition::try_tenant_value(
        crate::encoding::v2::values::property::encode_index_partition_value(&tenant_value),
    )
    .expect("tenant partition validates");
    index_keys::TextManifestRootKey {
        index_id: IndexId::initial(),
        generation: IndexGenerationId::initial(),
        partition: partition.fingerprint(),
    }
}

fn projected_ids(result: ExecutionResult) -> Result<Vec<u64>> {
    let Some(ExecutionValue::Scalars(values)) = result.last else {
        return Err(HelixDbError::InvariantViolation(
            "text transaction benchmark projection returned another value shape".to_string(),
        ));
    };
    values
        .into_iter()
        .map(|value| match value {
            ExecutionScalar::NodeId(id) => Ok(id),
            ExecutionScalar::EdgeId(_)
            | ExecutionScalar::String(_)
            | ExecutionScalar::Value(_)
            | ExecutionScalar::Object(_) => Err(HelixDbError::InvariantViolation(
                "text transaction benchmark projection returned a non-node ID".to_string(),
            )),
        })
        .collect()
}

async fn search(db: &crate::HelixDB, batch_size: usize) -> Result<(u64, Vec<u64>)> {
    search_for(db, QUERY, batch_size).await
}

async fn search_for(
    db: &crate::HelixDB,
    query: &str,
    batch_size: usize,
) -> Result<(u64, Vec<u64>)> {
    let started = Instant::now();
    let result = db
        .execute(
            &search_plan_for(query, batch_size),
            context::ParamBindings::default(),
        )
        .await?;
    let latency = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
    let mut ids = projected_ids(result)?;
    ids.sort_unstable();
    Ok((latency, ids))
}

fn digest(ids: &[u64]) -> String {
    let mut bytes = Vec::with_capacity(core::mem::size_of_val(ids));
    for id in ids {
        bytes.extend_from_slice(&id.to_be_bytes());
    }
    format!("{:032x}", xxhash_rust::xxh3::xxh3_128(&bytes))
}

async fn wait_for_manifest_quiescence(
    db: &crate::HelixDB,
    store: &MeasuredObjectStore,
    root: index_keys::TextManifestRootKey,
) -> Result<u64> {
    let started = Instant::now();
    let mut observation = (
        manifest_split_count(writer_db(db), root).await?,
        store.fts_uploads().0,
    );
    let mut stable_since = Instant::now();
    loop {
        tokio::time::sleep(QUIESCENCE_POLL).await;
        let next = (
            manifest_split_count(writer_db(db), root).await?,
            store.fts_uploads().0,
        );
        if next != observation {
            observation = next;
            stable_since = Instant::now();
        } else if stable_since.elapsed() >= QUIESCENCE_STABLE {
            return Ok(observation.0);
        }
        if started.elapsed() >= QUIESCENCE_TIMEOUT {
            return Err(HelixDbError::Config(
                "text transaction benchmark compaction did not quiesce".to_string(),
            ));
        }
    }
}

/// Publishes every queued text operation, as the automatic worker would.
async fn publish(db: &crate::HelixDB) {
    db.publish_index_queues_for_lifecycle_testing()
        .await
        .expect("queued text operations publish");
}

/// Proves queued publication batches by destination, delete-only publication,
/// versions, partitions, and in-request read-your-writes without barriers.
#[cfg(test)]
async fn text_transaction_batching_contract() {
    let temporary = tempfile::tempdir().expect("batching contract directory creates");
    let local: Arc<dyn ObjectStore> = Arc::new(
        LocalFileSystem::new_with_prefix(temporary.path()).expect("batching contract store opens"),
    );
    let measured = Arc::new(MeasuredObjectStore::new(local, Duration::ZERO));
    let object_store: Arc<dyn ObjectStore> = measured.clone();
    let database = "text-transaction-batching-contract";
    let definition =
        TextIndexDefinition::new_node(LABEL, PROPERTY).expect("batching text definition validates");
    let root = seed_active_text_generation(database, Arc::clone(&object_store), &definition).await;
    let db = crate::HelixDB::open_with_object_store_for_index_lifecycle_testing(
        database,
        Arc::clone(&object_store),
        benchmark_config(),
        LifecycleTestScheduling::Explicit,
    )
    .await
    .expect("batching contract database opens");

    measured.reset_fts_uploads();
    db.execute(&insert_plan(100), context::ParamBindings::default())
        .await
        .expect("one hundred inserts commit");
    assert_eq!(
        measured.fts_uploads().0,
        0,
        "graph transactions never upload text splits"
    );
    publish(&db).await;
    assert_eq!(measured.fts_uploads().0, 1);
    let inserted_root = manifest_root(writer_db(&db), root)
        .await
        .expect("inserted root reads");
    assert_eq!(inserted_root.split_count(), 1);
    for entity_id in 0..100 {
        let state = entity_state(writer_db(&db), root, entity_id)
            .await
            .expect("batched live state reads");
        assert!(state.live);
        assert_eq!(state.logical_version.get(), inserted_root.revision().get());
    }
    let page_key = scoped_key(index_keys::ScopedKey::TextManifestPage(
        index_keys::TextManifestPageKey { root, page: 0 },
    ));
    let page_bytes = writer_db(&db)
        .get(page_key)
        .await
        .expect("batched manifest page reads")
        .expect("batched manifest page exists");
    let page = index_values::decode_manifest_page(&page_bytes).expect("batched page decodes");
    assert_eq!(page.entries().len(), 1);
    assert!(page.entries()[0]
        .pruning()
        .may_match_any([QUERY.as_bytes()]));
    let (_, inserted_ids) = search(&db, 100)
        .await
        .expect("batched inserts are searchable");
    assert_eq!(inserted_ids.len(), 100);

    measured.reset_fts_uploads();
    db.execute(
        &access_and_mutate_plan(vec![
            set_body("intermediateonlytoken"),
            set_body(&format!("{QUERY} finalonlytoken")),
        ]),
        context::ParamBindings::default().with_value(name("target"), AstPropertyValue::I64(0)),
    )
    .await
    .expect("two updates for one entity coalesce");
    assert_eq!(
        measured.fts_uploads().0,
        0,
        "graph transactions never upload text splits"
    );
    publish(&db).await;
    assert_eq!(measured.fts_uploads().0, 1);
    let updated_root = manifest_root(writer_db(&db), root)
        .await
        .expect("updated root reads");
    assert_eq!(updated_root.split_count(), 2);
    assert_eq!(
        updated_root.revision().get(),
        inserted_root.revision().get() + 1
    );
    assert!(search_for(&db, "intermediateonlytoken", 100)
        .await
        .expect("intermediate update results read")
        .1
        .is_empty());
    assert_eq!(
        search_for(&db, "finalonlytoken", 100)
            .await
            .expect("final update results read")
            .1,
        vec![0]
    );

    measured.reset_fts_uploads();
    db.execute(
        &create_then_delete_plan(),
        context::ParamBindings::default(),
    )
    .await
    .expect("create/delete net-zero epoch commits");
    assert_eq!(
        measured.fts_uploads().0,
        0,
        "graph transactions never upload text splits"
    );
    publish(&db).await;
    assert_eq!(measured.fts_uploads().0, 0);
    assert_eq!(
        manifest_root(writer_db(&db), root)
            .await
            .expect("net-zero root reads"),
        updated_root
    );

    measured.reset_fts_uploads();
    db.execute(&delete_all_plan(), context::ParamBindings::default())
        .await
        .expect("delete-only epoch commits");
    assert_eq!(
        measured.fts_uploads().0,
        0,
        "graph transactions never upload text splits"
    );
    publish(&db).await;
    assert_eq!(measured.fts_uploads().0, 0);
    let deleted_root = manifest_root(writer_db(&db), root)
        .await
        .expect("delete-only root reads");
    assert_eq!(deleted_root.split_count(), 2);
    assert_eq!(
        deleted_root.revision().get(),
        updated_root.revision().get() + 1
    );
    assert_eq!(
        compaction_pointer_revision(writer_db(&db), &definition, root, 0)
            .await
            .expect("delete-only tail pointer reads"),
        deleted_root.revision().get()
    );
    for entity_id in 0..100 {
        let state = entity_state(writer_db(&db), root, entity_id)
            .await
            .expect("batched dead state reads");
        assert!(!state.live);
        assert_eq!(state.logical_version.get(), deleted_root.revision().get());
    }
    assert!(search(&db, 100)
        .await
        .expect("delete-only results read")
        .1
        .is_empty());

    measured.reset_fts_uploads();
    db.execute(&write_read_write_plan(), context::ParamBindings::default())
        .await
        .expect("write/read/write request commits one queued transaction");
    assert_eq!(
        measured.fts_uploads().0,
        0,
        "graph transactions never upload text splits"
    );
    publish(&db).await;
    // The in-request read saw the first write through the overlay; both
    // writes publish in one epoch.
    assert_eq!(measured.fts_uploads().0, 1);
    let barrier_root = manifest_root(writer_db(&db), root)
        .await
        .expect("barrier root reads");
    assert_eq!(barrier_root.split_count(), 3);
    assert_eq!(
        barrier_root.revision().get(),
        deleted_root.revision().get() + 1
    );
    assert_eq!(
        entity_state(writer_db(&db), root, 101)
            .await
            .expect("pre-read entity state reads")
            .logical_version
            .get(),
        barrier_root.revision().get()
    );
    assert_eq!(
        entity_state(writer_db(&db), root, 102)
            .await
            .expect("post-read entity state reads")
            .logical_version
            .get(),
        barrier_root.revision().get()
    );

    measured.reset_fts_uploads();
    db.execute(
        &access_and_mutate_plan(vec![
            set_body("temporary replacement"),
            exec::ExecMutationPlan::Drop,
        ]),
        context::ParamBindings::default().with_value(name("target"), AstPropertyValue::I64(101)),
    )
    .await
    .expect("update/delete coalesces to a retirement");
    assert_eq!(
        measured.fts_uploads().0,
        0,
        "graph transactions never upload text splits"
    );
    publish(&db).await;
    assert_eq!(measured.fts_uploads().0, 0);
    let update_delete_root = manifest_root(writer_db(&db), root)
        .await
        .expect("update/delete root reads");
    assert_eq!(update_delete_root.split_count(), 3);
    assert_eq!(
        update_delete_root.revision().get(),
        barrier_root.revision().get() + 1
    );
    let retired = entity_state(writer_db(&db), root, 101)
        .await
        .expect("update/delete state reads");
    assert!(!retired.live);
    assert_eq!(
        retired.logical_version.get(),
        update_delete_root.revision().get()
    );
    db.close().await.expect("batching contract database closes");
    drop(object_store);
    drop(measured);
    temporary
        .close()
        .expect("batching contract directory removes");

    let temporary = tempfile::tempdir().expect("multi-index contract directory creates");
    let local: Arc<dyn ObjectStore> = Arc::new(
        LocalFileSystem::new_with_prefix(temporary.path())
            .expect("multi-index contract store opens"),
    );
    let measured = Arc::new(MeasuredObjectStore::new(local, Duration::ZERO));
    let object_store: Arc<dyn ObjectStore> = measured.clone();
    let database = "text-transaction-multi-index-contract";
    let body = TextIndexDefinition::new_node(LABEL, PROPERTY).expect("body definition validates");
    let title = TextIndexDefinition::new_node(LABEL, "title").expect("title definition validates");
    let body_root = seed_active_text_generation_with(
        database,
        Arc::clone(&object_store),
        &body,
        IndexId::new(1).expect("body index ID is non-zero"),
        crate::index_lifecycle::TextManifestRevision::new(3).expect("body revision is non-zero"),
        true,
    )
    .await
    .expect("body root seeds");
    let title_root = seed_active_text_generation_with(
        database,
        Arc::clone(&object_store),
        &title,
        IndexId::new(2).expect("title index ID is non-zero"),
        crate::index_lifecycle::TextManifestRevision::new(9).expect("title revision is non-zero"),
        true,
    )
    .await
    .expect("title root seeds");
    let db = crate::HelixDB::open_with_object_store_for_index_lifecycle_testing(
        database,
        Arc::clone(&object_store),
        benchmark_config(),
        LifecycleTestScheduling::Explicit,
    )
    .await
    .expect("multi-index contract database opens");
    measured.reset_fts_uploads();
    db.execute(
        &dual_index_insert_plan(10),
        context::ParamBindings::default(),
    )
    .await
    .expect("dual-index inserts commit");
    assert_eq!(
        measured.fts_uploads().0,
        0,
        "graph transactions never upload text splits"
    );
    publish(&db).await;
    assert_eq!(measured.fts_uploads().0, 2);
    let body_value = manifest_root(writer_db(&db), body_root)
        .await
        .expect("body root reads");
    let title_value = manifest_root(writer_db(&db), title_root)
        .await
        .expect("title root reads");
    assert_eq!(
        (body_value.split_count(), body_value.revision().get()),
        (1, 4)
    );
    assert_eq!(
        (title_value.split_count(), title_value.revision().get()),
        (1, 10)
    );
    for entity_id in 0..10 {
        assert_eq!(
            entity_state(writer_db(&db), body_root, entity_id)
                .await
                .expect("body state reads")
                .logical_version
                .get(),
            4
        );
        assert_eq!(
            entity_state(writer_db(&db), title_root, entity_id)
                .await
                .expect("title state reads")
                .logical_version
                .get(),
            10
        );
    }
    db.close()
        .await
        .expect("multi-index contract database closes");
    drop(object_store);
    drop(measured);
    temporary
        .close()
        .expect("multi-index contract directory removes");

    let temporary = tempfile::tempdir().expect("partition contract directory creates");
    let local: Arc<dyn ObjectStore> = Arc::new(
        LocalFileSystem::new_with_prefix(temporary.path()).expect("partition contract store opens"),
    );
    let measured = Arc::new(MeasuredObjectStore::new(local, Duration::ZERO));
    let object_store: Arc<dyn ObjectStore> = measured.clone();
    let database = "text-transaction-partition-contract";
    let definition = TextIndexDefinition::new_node(LABEL, PROPERTY)
        .expect("partitioned definition validates")
        .with_tenant_property("tenant")
        .expect("tenant property validates");
    assert!(seed_active_text_generation_with(
        database,
        Arc::clone(&object_store),
        &definition,
        IndexId::initial(),
        crate::index_lifecycle::TextManifestRevision::initial(),
        false,
    )
    .await
    .is_none());
    let db = crate::HelixDB::open_with_object_store_for_index_lifecycle_testing(
        database,
        Arc::clone(&object_store),
        benchmark_config(),
        LifecycleTestScheduling::Explicit,
    )
    .await
    .expect("partition contract database opens");
    measured.reset_fts_uploads();
    db.execute(
        &partitioned_insert_plan(),
        context::ParamBindings::default(),
    )
    .await
    .expect("two-partition inserts commit");
    assert_eq!(
        measured.fts_uploads().0,
        0,
        "graph transactions never upload text splits"
    );
    publish(&db).await;
    assert_eq!(measured.fts_uploads().0, 2);
    for (entity_id, tenant) in [(0, "acme"), (1, "globex")] {
        let root = tenant_root(tenant);
        let root_value = manifest_root(writer_db(&db), root)
            .await
            .expect("partition root reads");
        assert_eq!(root_value.split_count(), 1);
        let state = entity_state(writer_db(&db), root, entity_id)
            .await
            .expect("partition state reads");
        assert!(state.live);
        assert_eq!(state.logical_version.get(), root_value.revision().get());
    }

    let acme_root = tenant_root("acme");
    let acme_before = manifest_root(writer_db(&db), acme_root)
        .await
        .expect("source partition root reads");
    measured.reset_fts_uploads();
    db.execute(
        &access_and_mutate_plan(vec![
            set_property("tenant", "temporary-partition"),
            set_property("tenant", "initech"),
        ]),
        context::ParamBindings::default().with_value(name("target"), AstPropertyValue::I64(0)),
    )
    .await
    .expect("two tenant changes coalesce to one partition move");
    assert_eq!(
        measured.fts_uploads().0,
        0,
        "graph transactions never upload text splits"
    );
    publish(&db).await;
    assert_eq!(measured.fts_uploads().0, 1);
    let acme_after = manifest_root(writer_db(&db), acme_root)
        .await
        .expect("retired source partition root reads");
    assert_eq!(acme_after.split_count(), acme_before.split_count());
    assert_eq!(
        acme_after.revision().get(),
        acme_before.revision().get() + 1
    );
    let source_state = entity_state(writer_db(&db), acme_root, 0)
        .await
        .expect("retired source partition state reads");
    assert!(!source_state.live);
    assert_eq!(
        source_state.logical_version.get(),
        acme_after.revision().get()
    );

    let final_root = tenant_root("initech");
    let final_value = manifest_root(writer_db(&db), final_root)
        .await
        .expect("final partition root reads");
    assert_eq!(final_value.split_count(), 1);
    let final_state = entity_state(writer_db(&db), final_root, 0)
        .await
        .expect("final partition state reads");
    assert!(final_state.live);
    assert_eq!(
        final_state.logical_version.get(),
        final_value.revision().get()
    );

    let intermediate_key = scoped_key(index_keys::ScopedKey::TextManifestRoot(tenant_root(
        "temporary-partition",
    )));
    assert!(writer_db(&db)
        .get(intermediate_key)
        .await
        .expect("intermediate partition root lookup succeeds")
        .is_none());
    db.close()
        .await
        .expect("partition contract database closes");
    drop(object_store);
    drop(measured);
    temporary
        .close()
        .expect("partition contract directory removes");
}

/// Runs one clean local-storage sample through production mutation and search paths.
pub async fn run_text_transaction_batch_benchmark_sample(
    case: TextTransactionBatchBenchmarkCase,
) -> Result<TextTransactionBatchBenchmarkSample> {
    let temporary = tempfile::tempdir().expect("benchmark temporary directory creates");
    let local: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(temporary.path())?);
    let measured = Arc::new(MeasuredObjectStore::new(
        local,
        Duration::from_millis(case.upload_latency_millis),
    ));
    let object_store: Arc<dyn ObjectStore> = measured.clone();
    let database = "text-transaction-benchmark";
    let definition = TextIndexDefinition::new_node(LABEL, PROPERTY)
        .expect("benchmark text definition validates");
    let root = seed_active_text_generation(database, Arc::clone(&object_store), &definition).await;
    let config = benchmark_config();
    let db = crate::HelixDB::open_with_object_store_for_index_lifecycle_testing(
        database,
        Arc::clone(&object_store),
        config.clone(),
        LifecycleTestScheduling::Explicit,
    )
    .await?;
    let splits_before = manifest_split_count(writer_db(&db), root).await?;
    measured.reset_fts_uploads();
    let transaction_started = Instant::now();
    db.execute(
        &insert_plan(case.batch_size),
        context::ParamBindings::default(),
    )
    .await?;
    let transaction_latency_ns =
        u64::try_from(transaction_started.elapsed().as_nanos()).unwrap_or(u64::MAX);
    let (upload_count, upload_bytes) = measured.fts_uploads();
    let splits_after = manifest_split_count(writer_db(&db), root).await?;
    let (immediate_search_latency_ns, immediate_ids) = search(&db, case.batch_size).await?;
    if immediate_ids.len() != case.batch_size {
        return Err(HelixDbError::InvariantViolation(format!(
            "text transaction benchmark expected {} immediate hits, found {}",
            case.batch_size,
            immediate_ids.len()
        )));
    }
    db.close().await?;

    let db = crate::HelixDB::open_with_object_store_and_config(
        database,
        Arc::clone(&object_store),
        config,
    )
    .await?;
    let post_compaction_split_count = wait_for_manifest_quiescence(&db, &measured, root).await?;
    let (post_compaction_search_latency_ns, post_ids) = search(&db, case.batch_size).await?;
    if immediate_ids != post_ids {
        return Err(HelixDbError::InvariantViolation(
            "text transaction benchmark search results changed after compaction".to_string(),
        ));
    }
    let search_digest = digest(&immediate_ids);
    db.close().await?;
    drop(object_store);
    drop(measured);
    temporary
        .close()
        .expect("benchmark temporary directory removes");

    Ok(TextTransactionBatchBenchmarkSample {
        case,
        transaction_latency_ns,
        upload_count,
        upload_bytes,
        manifest_split_growth: splits_after.saturating_sub(splits_before),
        immediate_search_latency_ns,
        post_compaction_search_latency_ns,
        post_compaction_split_count,
        search_digest,
    })
}

/// Borrows the writer storage owned by one interpreter fixture.
fn writer_db(db: &crate::HelixDB) -> &Db {
    let crate::HelixStorage::Writer(writer) = db.storage() else {
        panic!("interpreter fixture must be a writer");
    };
    writer.db()
}

#[cfg(test)]
mod lifecycle_testing_tests {
    /// Proves one split per destination/epoch, delete-only behavior, and shared versions.
    #[tokio::test]
    async fn text_transactions_batch_by_destination() {
        super::text_transaction_batching_contract().await;
    }
}
