//! Text builds converge when entities change between the source reads.
//!
//! `ScanSource` records each indexed entity with its partition and statistics.
//! `ScanPartitions` reads the graph row again later to build the document, so
//! an entity can be deleted, re-texted, moved to another tenant, or stop being
//! indexed in between. Every case pauses the build at an exact boundary with
//! the explicit controller and applies queued writes there. Entities that
//! change back after partition construction cover the publisher's view of the
//! build: queued work applies against what the build produced. The build must
//! activate, and strong and eventual searches must return exactly the hits and
//! BM25 scores of an independent build over the final graph.

use std::collections::BTreeMap;
use std::num::NonZeroUsize;

use helix_ast::{batch, query::QueryRequest, query::SearchConsistency, traversal};

use crate::config::{SearchIndexBackfillLimits, SearchIndexBatchLimits, TextIndexDefinition};
use crate::encoding::property::Property;
use crate::encoding::v2::keys::scope::DataScope;
use crate::index_lifecycle::{
    IndexDdlReceipt, IndexElementKind, IndexOperationId, IndexOperationStage, IndexOperationStatus,
    ValidatedDynamicIndexDefinition,
};
use crate::index_lifecycle_testing::{LifecycleTestController, LifecycleTestScheduling};
use crate::{DbConfig, HelixDB, HelixDbSource};

use super::{
    allocate_node_ids, drive_to_terminal, drive_until, enqueue_search_mutation, put_source,
    queued_operations,
};

const LABEL: &str = "ReconcileDoc";
const PROPERTY: &str = "body";
const TENANT: &str = "tenant";
const SCOPE: DataScope = DataScope::LegacyUnscoped;
/// Unindexed rows after the targets keep `ScanSource` running past them.
const FILLERS: u64 = 4;

/// Build boundary where the first writes land.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LatePoint {
    /// `ScanSource` has recorded every target but still has rows to read.
    SourceMidScan,
    /// `ScanSource` finished and no partition has been read.
    PartitionsStart,
    /// `ScanPartitions` has read exactly one entity.
    PartitionsMid,
}

/// Index shape and the targets it exercises.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Shape {
    /// Tenant partitions, including tenants whose only document goes away.
    Tenant,
    /// One unpartitioned root whose every scanned document goes away.
    Unpartitioned,
}

/// One target's text document and its scripted writes.
struct Target {
    tenant: &'static str,
    body: &'static str,
    /// Applied at the late point. At `PartitionsMid` one target was already
    /// read, so its write publishes after activation like `second` instead of
    /// being reconciled.
    first: Write,
    /// Applied after `ScanPartitions` finished.
    second: Write,
}

/// One scripted graph change of a target.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Write {
    Keep,
    Delete,
    Text(&'static str),
    Tenant(&'static str),
    RemoveText,
    RestoreText,
}

impl Shape {
    fn targets(self) -> Vec<Target> {
        let target = |tenant, body, first, second| Target {
            tenant,
            body,
            first,
            second,
        };
        match self {
            Self::Tenant => vec![
                target(
                    "solo-delete",
                    "common solodelete",
                    Write::Delete,
                    Write::Keep,
                ),
                target(
                    "solo-move",
                    "common solomove",
                    Write::Tenant("shared"),
                    Write::Keep,
                ),
                target(
                    "solo-unindex",
                    "common solounindex",
                    Write::RemoveText,
                    Write::Keep,
                ),
                target(
                    "solo-change",
                    "common solochangebefore",
                    Write::Text("common solochangeafter extra"),
                    Write::Keep,
                ),
                target(
                    "shared",
                    "common abatextfirst",
                    Write::Text("common abatextsecond"),
                    Write::Text("common abatextfirst"),
                ),
                target(
                    "shared",
                    "common abamove",
                    Write::Tenant("aside"),
                    Write::Tenant("shared"),
                ),
                target(
                    "shared",
                    "common abaindex",
                    Write::RemoveText,
                    Write::RestoreText,
                ),
                target("shared", "common stable", Write::Keep, Write::Keep),
            ],
            Self::Unpartitioned => vec![
                target("shared", "common solodelete", Write::Delete, Write::Keep),
                target(
                    "shared",
                    "common solounindex",
                    Write::RemoveText,
                    Write::Keep,
                ),
                target(
                    "shared",
                    "common abaindex",
                    Write::RemoveText,
                    Write::RestoreText,
                ),
            ],
        }
    }

    fn definition(self) -> ValidatedDynamicIndexDefinition {
        let definition = TextIndexDefinition::new_node(LABEL, PROPERTY)
            .expect("reconciliation text definition validates");
        match self {
            Self::Tenant => definition
                .with_tenant_property(TENANT)
                .expect("reconciliation tenant property validates"),
            Self::Unpartitioned => definition,
        }
        .try_into()
        .expect("reconciliation text definition converts")
    }

    const fn tenants(self) -> &'static [Option<&'static str>] {
        match self {
            Self::Tenant => &[
                Some("solo-delete"),
                Some("solo-move"),
                Some("solo-unindex"),
                Some("solo-change"),
                Some("shared"),
                Some("aside"),
            ],
            Self::Unpartitioned => &[None],
        }
    }
}

/// `common` scores every document; the OR query also matches each target's
/// current and former unique terms, so stale documents change its hits.
const QUERIES: [&str; 2] = [
    "common",
    "solodelete solomove solounindex solochangebefore solochangeafter abatextfirst \
     abatextsecond abamove abaindex stable",
];

/// Runs every late point for every shape.
pub(super) async fn run() {
    let mut ordinal = 0_usize;
    for shape in [Shape::Tenant, Shape::Unpartitioned] {
        for point in [
            LatePoint::SourceMidScan,
            LatePoint::PartitionsStart,
            LatePoint::PartitionsMid,
        ] {
            run_case(ordinal, shape, point).await;
            ordinal += 1;
        }
    }
    assert_eq!(ordinal, 6, "every shape runs at every late point");
}

/// One-entity build batches, so every stage takes one step per entity.
fn config() -> DbConfig {
    let defaults = SearchIndexBackfillLimits::default();
    let batch = defaults.batch();
    let limits = SearchIndexBackfillLimits::try_new(
        SearchIndexBatchLimits::try_new(
            NonZeroUsize::MIN,
            batch.max_input_bytes(),
            batch.max_output_operations(),
            batch.max_output_bytes(),
            batch.max_single_vector_output_bytes(),
        )
        .expect("one-entity reconciliation batch is valid"),
        defaults.edge_property_read_batch(),
        defaults.text_artifacts(),
        defaults.text_compaction(),
    )
    .expect("one-entity reconciliation limits are valid");
    DbConfig::new().with_search_index_backfill_limits(limits)
}

async fn open(database: String, config: DbConfig) -> HelixDB {
    HelixDB::open_for_index_lifecycle_testing(
        HelixDbSource::InMemory { database },
        config,
        LifecycleTestScheduling::Explicit,
    )
    .await
    .expect("reconciliation writer opens")
}

fn text_row(tenant: &str, body: Option<&str>) -> Vec<Property> {
    [
        Some(Property::string("$label", LABEL)),
        Some(Property::string(TENANT, tenant)),
        body.map(|body| Property::string(PROPERTY, body)),
    ]
    .into_iter()
    .flatten()
    .collect()
}

async fn run_case(ordinal: usize, shape: Shape, point: LatePoint) {
    let db = open(format!("text-build-reconciliation-{ordinal}"), config()).await;
    let controller = LifecycleTestController::new();
    let targets = shape.targets();
    let target_count = u64::try_from(targets.len()).expect("target count fits u64");
    let ids = allocate_node_ids(&db, target_count + FILLERS).await;
    let mut rows = BTreeMap::new();
    for (id, target) in ids.clone().zip(&targets) {
        rows.insert(id, text_row(target.tenant, Some(target.body)));
    }
    for id in ids.clone().skip(targets.len()) {
        rows.insert(id, vec![Property::string("$label", "ReconcileFiller")]);
    }
    for (id, row) in &rows {
        put_source(&db, SCOPE, *id, row).await;
    }

    let definition = shape.definition();
    let IndexDdlReceipt::Accepted { operation_id, .. } = controller
        .create_index(
            &db,
            SCOPE,
            definition.clone(),
            helix_planner::ir::IndexCreateMode::ErrorIfExists,
        )
        .await
        .expect("reconciliation build is accepted")
    else {
        panic!("a fresh reconciliation build must enqueue");
    };
    pause_at(&db, &controller, operation_id, point, target_count).await;
    for (id, target) in ids.clone().zip(&targets) {
        apply(&db, &mut rows, id, target, target.first).await;
    }
    drive_until(&db, &controller, SCOPE, operation_id, |status| {
        status.common().stage == IndexOperationStage::Compact
    })
    .await;
    for (id, target) in ids.clone().zip(&targets) {
        apply(&db, &mut rows, id, target, target.second).await;
    }

    let terminal = drive_to_terminal(&db, &controller, SCOPE, operation_id).await;
    assert!(
        matches!(terminal, IndexOperationStatus::Succeeded { .. }),
        "{shape:?} build with writes at {point:?} did not activate: {terminal:?}"
    );
    let reference = reference_build(ordinal, shape, ids, &rows).await;
    let context = format!("{shape:?} at {point:?}");
    assert_matches_reference(
        &db,
        &reference,
        shape,
        SearchConsistency::Strong,
        &format!("{context} before publication"),
    )
    .await;
    db.publish_index_queues_for_lifecycle_testing()
        .await
        .expect("queued build writes publish after activation");
    assert_eq!(queued_operations(&db, SCOPE, &definition).await, 0);
    for consistency in [SearchConsistency::Strong, SearchConsistency::Eventual] {
        assert_matches_reference(
            &db,
            &reference,
            shape,
            consistency,
            &format!("{context} after publication"),
        )
        .await;
    }
    reference.close().await.expect("reference writer closes");
    db.close().await.expect("reconciliation writer closes");
}

/// Stops the build at `point` with the operation still claimable.
async fn pause_at(
    db: &HelixDB,
    controller: &LifecycleTestController,
    operation_id: IndexOperationId,
    point: LatePoint,
    target_count: u64,
) {
    let paused = match point {
        LatePoint::SourceMidScan => {
            drive_until(db, controller, SCOPE, operation_id, |status| {
                status.common().progress.entities >= target_count
            })
            .await
        }
        LatePoint::PartitionsStart => {
            drive_until(db, controller, SCOPE, operation_id, |status| {
                status.common().stage == IndexOperationStage::ScanPartitions
            })
            .await
        }
        LatePoint::PartitionsMid => {
            let scanned = drive_until(db, controller, SCOPE, operation_id, |status| {
                status.common().stage == IndexOperationStage::ScanPartitions
            })
            .await
            .common()
            .progress
            .entities;
            drive_until(db, controller, SCOPE, operation_id, |status| {
                status.common().progress.entities > scanned
            })
            .await
        }
    };
    let expected = match point {
        LatePoint::SourceMidScan => IndexOperationStage::Scan,
        LatePoint::PartitionsStart | LatePoint::PartitionsMid => {
            IndexOperationStage::ScanPartitions
        }
    };
    assert_eq!(
        paused.common().stage,
        expected,
        "{point:?} pauses inside its stage"
    );
}

/// Applies one scripted write as a queued graph transaction.
async fn apply(
    db: &HelixDB,
    rows: &mut BTreeMap<u64, Vec<Property>>,
    id: u64,
    target: &Target,
    write: Write,
) {
    let before = rows.get(&id).cloned().unwrap_or_default();
    let tenant = || {
        before
            .iter()
            .find(|property| property.name == TENANT)
            .and_then(|property| property.value.as_str())
            .expect("a live target keeps its tenant")
            .to_string()
    };
    let body = || {
        before
            .iter()
            .find(|property| property.name == PROPERTY)
            .and_then(|property| property.value.as_str())
            .map(str::to_string)
    };
    let after = match write {
        Write::Keep => return,
        Write::Delete => Vec::new(),
        Write::Text(text) => text_row(&tenant(), Some(text)),
        Write::Tenant(moved) => text_row(moved, body().as_deref()),
        Write::RemoveText => text_row(&tenant(), None),
        Write::RestoreText => text_row(&tenant(), Some(target.body)),
    };
    enqueue_search_mutation(db, SCOPE, IndexElementKind::Node, id, &before, &after)
        .await
        .expect("scripted write enqueues with its source row");
    if after.is_empty() {
        rows.remove(&id);
    } else {
        rows.insert(id, after);
    }
}

/// Builds the same index over the final graph rows in a fresh database.
///
/// Default batches keep the reference fast; BM25 scores use corpus-wide
/// statistics, so split layout does not change them. The same node IDs are
/// allocated first, so the build's source watermark covers every row.
async fn reference_build(
    ordinal: usize,
    shape: Shape,
    ids: std::ops::Range<u64>,
    rows: &BTreeMap<u64, Vec<Property>>,
) -> HelixDB {
    let db = open(
        format!("text-build-reconciliation-reference-{ordinal}"),
        DbConfig::new(),
    )
    .await;
    assert_eq!(
        allocate_node_ids(&db, ids.end - ids.start).await,
        ids,
        "a fresh reference allocates the same node IDs"
    );
    for (id, row) in rows {
        put_source(&db, SCOPE, *id, row).await;
    }
    let controller = LifecycleTestController::new();
    let IndexDdlReceipt::Accepted { operation_id, .. } = controller
        .create_index(
            &db,
            SCOPE,
            shape.definition(),
            helix_planner::ir::IndexCreateMode::ErrorIfExists,
        )
        .await
        .expect("reference build is accepted")
    else {
        panic!("a fresh reference build must enqueue");
    };
    assert!(matches!(
        drive_to_terminal(&db, &controller, SCOPE, operation_id).await,
        IndexOperationStatus::Succeeded { .. }
    ));
    db
}

/// Requires identical ranked hits and score bits for every query and tenant.
async fn assert_matches_reference(
    db: &HelixDB,
    reference: &HelixDB,
    shape: Shape,
    consistency: SearchConsistency,
    context: &str,
) {
    for tenant in shape.tenants() {
        for query in QUERIES {
            let expected = text_hits(reference, query, *tenant, SearchConsistency::Strong).await;
            assert_eq!(
                text_hits(db, query, *tenant, consistency).await,
                expected,
                "{context}: {consistency:?} search {query:?} in tenant {tenant:?}"
            );
        }
    }
}

/// Returns `(id, score bits)` hits ordered by descending score, then ID.
async fn text_hits(
    db: &HelixDB,
    query: &str,
    tenant: Option<&str>,
    consistency: SearchConsistency,
) -> Vec<(u64, u64)> {
    let request = QueryRequest::read(
        batch::read_batch()
            .var_as(
                "hits",
                traversal::g().text_search_nodes(
                    LABEL,
                    PROPERTY,
                    query,
                    20,
                    tenant.map(helix_ast::value::PropertyValue::from),
                ),
            )
            .returning(["hits"]),
    )
    .with_search_consistency(consistency)
    .expect("reconciliation search accepts its consistency");
    let result = Box::pin(db.query(request))
        .await
        .expect("reconciliation search succeeds");
    let mut hits = result["hits"].as_array().map_or_else(Vec::new, |hits| {
        hits.iter()
            .map(|hit| {
                (
                    hit["$id"].as_u64().expect("a text hit has an ID"),
                    hit["$score"]
                        .as_f64()
                        .expect("a text hit has a score")
                        .to_bits(),
                )
            })
            .collect::<Vec<_>>()
    });
    hits.sort_by(|left, right| {
        f64::from_bits(right.1)
            .total_cmp(&f64::from_bits(left.1))
            .then_with(|| left.0.cmp(&right.0))
    });
    hits
}
