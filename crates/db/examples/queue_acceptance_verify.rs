//! Verifies a pre-queue release fixture through the normal open path.
//!
//! Usage: `queue_acceptance_verify <fixture-root>`, where the fixture was
//! produced by the pre-queue release's acceptance fixture generator (a
//! `settled` database with Active secondary/vector/text indexes, an
//! `inflight` database with interrupted vector/text builds, and
//! `manifest.json` with the expected graph state). The verifier works on the
//! directory it is given; callers pass a copy so the preserved fixture stays
//! untouched. It prints a JSON report and exits non-zero on any mismatch.
//!
//! Checks: existing graph data and physical indexes remain usable without
//! manual initialization; the absent queue loads as empty; strong and
//! eventual searches match exact oracles before and after new queued writes,
//! publication, and a restart; interrupted pre-queue builds either finish or
//! block explicitly, and a blocked build recovers through abort and recreate.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use db::{HelixDB, HelixDbSource};
use helix_ast::expr::Predicate;
use helix_ast::graph::NodeRef;
use helix_ast::index::{IndexSpec, VectorDistanceMetric};
use helix_ast::query::{QueryRequest, SearchConsistency};
use helix_ast::value::{PropertyInput, PropertyValue};
use helix_ast::{batch, traversal};

/// Effective text-search result cap enforced by the server.
const TEXT_RESULT_CAP: usize = 800;

#[derive(Clone, Debug)]
struct Doc {
    key: String,
    tenant: String,
    embedding: [f32; 2],
    body: String,
}

fn docs(manifest: &serde_json::Value) -> BTreeMap<u64, Doc> {
    manifest
        .as_object()
        .unwrap()
        .iter()
        .map(|(id, doc)| {
            let embedding = doc["embedding"].as_array().unwrap();
            (
                id.parse().unwrap(),
                Doc {
                    key: doc["key"].as_str().unwrap().to_string(),
                    tenant: doc["tenant"].as_str().unwrap().to_string(),
                    embedding: [
                        embedding[0].as_f64().unwrap() as f32,
                        embedding[1].as_f64().unwrap() as f32,
                    ],
                    body: doc["body"].as_str().unwrap().to_string(),
                },
            )
        })
        .collect()
}

async fn open(root: &Path, database: &str) -> HelixDB {
    HelixDB::open(HelixDbSource::Disk {
        root: root.to_path_buf(),
        database: database.to_string(),
    })
    .await
    .unwrap_or_else(|error| panic!("{database} opens through the normal path: {error}"))
}

async fn write(db: &HelixDB, request: impl Fn() -> QueryRequest) -> serde_json::Value {
    for _ in 0..100 {
        match db.query(request()).await {
            Ok(result) => return result,
            Err(error) if error.is_transaction_conflict() || error.is_index_backpressure() => {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Err(error) => panic!("write failed: {error}"),
        }
    }
    panic!("write kept conflicting")
}

fn operation_id(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::Object(object) => object
            .get("operation_id")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string)
            .or_else(|| object.values().find_map(operation_id)),
        serde_json::Value::Array(values) => values.iter().find_map(operation_id),
        serde_json::Value::Null
        | serde_json::Value::Bool(_)
        | serde_json::Value::Number(_)
        | serde_json::Value::String(_) => None,
    }
}

async fn status(db: &HelixDB, operation: &str) -> serde_json::Value {
    db.query(QueryRequest::read(
        batch::read_batch()
            .var_as("status", traversal::g().get_index_operation(operation))
            .returning(["status"]),
    ))
    .await
    .unwrap()["status"]
        .clone()
}

async fn wait_terminal(db: &HelixDB, operation: &str) -> serde_json::Value {
    let deadline = Instant::now() + Duration::from_secs(300);
    loop {
        let current = status(db, operation).await;
        if !matches!(current["status"].as_str(), Some("queued" | "running")) {
            return current;
        }
        assert!(Instant::now() < deadline, "operation {operation} stalled");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn ddl(
    db: &HelixDB,
    name: &str,
    traversal: traversal::Traversal<traversal::Terminal, traversal::WriteEnabled>,
) -> Option<String> {
    let receipt = write(db, || {
        QueryRequest::write(
            batch::write_batch()
                .var_as(name, traversal.clone())
                .returning([name]),
        )
    })
    .await;
    operation_id(&receipt)
}

fn vector_spec() -> IndexSpec {
    IndexSpec::node_vector(
        "Doc",
        "embedding",
        NonZeroUsize::new(2).unwrap(),
        VectorDistanceMetric::Euclidean,
        Some("tenant"),
    )
}

fn text_spec() -> IndexSpec {
    IndexSpec::node_text("Doc", "body", None::<&str>)
}

async fn wait_published(db: &HelixDB) {
    let deadline = Instant::now() + Duration::from_secs(120);
    while db.index_operation_queue_stats().pending_operations != 0 {
        assert!(
            Instant::now() < deadline,
            "queued index work did not publish"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn vector_hits(
    db: &HelixDB,
    tenant: &str,
    query: [f32; 2],
    consistency: SearchConsistency,
) -> Vec<u64> {
    let request = QueryRequest::read(
        batch::read_batch()
            .var_as(
                "hits",
                traversal::g().vector_search_nodes(
                    "Doc",
                    "embedding",
                    query.to_vec(),
                    10,
                    Some(PropertyValue::from(tenant)),
                ),
            )
            .returning(["hits"]),
    )
    .with_search_consistency(consistency)
    .unwrap();
    let result = db.query(request).await.unwrap();
    result["hits"].as_array().map_or_else(Vec::new, |hits| {
        hits.iter()
            .map(|hit| hit["$id"].as_u64().unwrap())
            .collect()
    })
}

async fn text_hits(db: &HelixDB, term: &str, consistency: SearchConsistency) -> BTreeSet<u64> {
    let request = QueryRequest::read(
        batch::read_batch()
            .var_as(
                "hits",
                traversal::g().text_search_nodes("Doc", "body", term, 1_000, None::<PropertyValue>),
            )
            .returning(["hits"]),
    )
    .with_search_consistency(consistency)
    .unwrap();
    let result = db.query(request).await.unwrap();
    result["hits"]
        .as_array()
        .map_or_else(BTreeSet::new, |hits| {
            hits.iter()
                .map(|hit| hit["$id"].as_u64().unwrap())
                .collect()
        })
}

/// Compares vector and text searches with exact oracles over `state`.
async fn check_searches(
    db: &HelixDB,
    state: &BTreeMap<u64, Doc>,
    consistency: SearchConsistency,
    failures: &mut Vec<String>,
    phase: &str,
) {
    for tenant in ["a", "b"] {
        for query in [[3.3_f32, 2.7], [16.2, 8.1], [41.4, 1.2], [0.4, 17.6]] {
            let mut exact = state
                .iter()
                .filter(|(_, doc)| doc.tenant == tenant)
                .map(|(id, doc)| {
                    let distance = (doc.embedding[0] - query[0]).powi(2)
                        + (doc.embedding[1] - query[1]).powi(2);
                    (distance, *id)
                })
                .collect::<Vec<_>>();
            exact.sort_by(|left, right| left.partial_cmp(right).unwrap());
            let expected = exact.iter().take(10).map(|(_, id)| *id).collect::<Vec<_>>();
            let found = vector_hits(db, tenant, query, consistency).await;
            if found != expected {
                failures.push(format!(
                    "{phase}: vector {tenant} {query:?} {consistency:?}: {found:?} != {expected:?}"
                ));
            }
        }
    }
    for term in ["shared", "word3", "rewritten", "arrival"] {
        let expected = state
            .iter()
            .filter(|(_, doc)| doc.body.split(' ').any(|word| word == term))
            .map(|(id, _)| *id)
            .collect::<BTreeSet<_>>();
        let found = text_hits(db, term, consistency).await;
        // Text search returns at most 800 hits per request.
        let matches = if expected.len() > TEXT_RESULT_CAP {
            found.len() == TEXT_RESULT_CAP && found.is_subset(&expected)
        } else {
            found == expected
        };
        if !matches {
            failures.push(format!(
                "{phase}: text {term:?} {consistency:?}: {} found, {} expected",
                found.len(),
                expected.len()
            ));
        }
    }
}

async fn check_graph(db: &HelixDB, state: &BTreeMap<u64, Doc>, failures: &mut Vec<String>) {
    let result = db
        .query(QueryRequest::read(
            batch::read_batch()
                .var_as("count", traversal::g().n_with_label("Doc").count())
                .returning(["count"]),
        ))
        .await
        .unwrap();
    let count = result["count"].as_u64().unwrap_or_default();
    if count != state.len() as u64 {
        failures.push(format!(
            "graph holds {count} docs, expected {}",
            state.len()
        ));
    }
    for doc in state.values().step_by(37) {
        // The fixture reuses keys across its two seeding rounds, so a key can
        // name several documents.
        let expected = state
            .iter()
            .filter(|(_, other)| other.key == doc.key)
            .map(|(id, _)| *id)
            .collect::<Vec<_>>();
        let result = db
            .query(QueryRequest::read(
                batch::read_batch()
                    .var_as(
                        "found",
                        traversal::g()
                            .n_with_label("Doc")
                            .where_(Predicate::eq("key", doc.key.clone()))
                            .value_map(Some(vec!["$id"])),
                    )
                    .returning(["found"]),
            ))
            .await
            .unwrap();
        let mut found = result["found"].as_array().map_or_else(Vec::new, |rows| {
            rows.iter()
                .filter_map(|row| row["$id"].as_u64())
                .collect::<Vec<_>>()
        });
        found.sort_unstable();
        if found != expected {
            failures.push(format!(
                "equality lookup for {} returned {found:?}",
                doc.key
            ));
        }
    }
}

async fn add(db: &HelixDB, state: &mut BTreeMap<u64, Doc>, doc: Doc) {
    let result = write(db, || {
        QueryRequest::write(
            batch::write_batch()
                .var_as(
                    "created",
                    traversal::g().add_n(
                        "Doc",
                        vec![
                            ("key", PropertyInput::from(doc.key.clone())),
                            ("tenant", PropertyInput::from(doc.tenant.clone())),
                            ("embedding", PropertyInput::from(doc.embedding.to_vec())),
                            ("body", PropertyInput::from(doc.body.clone())),
                        ],
                    ),
                )
                .returning(["created"]),
        )
    })
    .await;
    state.insert(result["created"][0]["$id"].as_u64().unwrap(), doc);
}

async fn set(db: &HelixDB, id: u64, property: &str, value: PropertyInput) {
    write(db, || {
        QueryRequest::write(
            batch::write_batch().var_as(
                "updated",
                traversal::g()
                    .n(NodeRef::from(id))
                    .set_property(property, value.clone()),
            ),
        )
    })
    .await;
}

/// New queued writes against indexes built by the previous release.
async fn new_writes(db: &HelixDB, state: &mut BTreeMap<u64, Doc>) {
    let ids = state.keys().copied().collect::<Vec<_>>();
    for (step, id) in ids.iter().step_by(11).take(15).enumerate() {
        let embedding = [16.0 + step as f32 * 0.1, 8.0];
        set(
            db,
            *id,
            "embedding",
            PropertyInput::from(embedding.to_vec()),
        )
        .await;
        state.get_mut(id).unwrap().embedding = embedding;
    }
    for id in ids.iter().skip(4).step_by(23).take(6) {
        let tenant = if state[id].tenant == "a" { "b" } else { "a" }.to_string();
        set(db, *id, "tenant", PropertyInput::from(tenant.clone())).await;
        state.get_mut(id).unwrap().tenant = tenant;
    }
    for id in ids.iter().skip(2).step_by(31).take(6) {
        write(db, || {
            QueryRequest::write(
                batch::write_batch().var_as("dropped", traversal::g().n(NodeRef::from(*id)).drop()),
            )
        })
        .await;
        state.remove(id);
    }
    for index in 0..12_u32 {
        add(
            db,
            state,
            Doc {
                key: format!("new{index}"),
                tenant: if index % 2 == 0 { "a" } else { "b" }.to_string(),
                embedding: [3.0 + index as f32 * 0.05, 2.5],
                body: format!("new arrival {index} shared"),
            },
        )
        .await;
    }
}

#[tokio::main]
async fn main() {
    let root = PathBuf::from(std::env::args().nth(1).expect("fixture root argument"));
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join("manifest.json")).unwrap()).unwrap();
    let mut failures = Vec::new();
    let mut report = serde_json::Map::new();

    // Settled database.
    let mut settled = docs(&manifest["settled"]);
    let db = open(&root, "settled").await;
    let stats = db.index_operation_queue_stats();
    report.insert(
        "settled_queue_at_open".into(),
        serde_json::json!(stats.pending_operations),
    );
    if stats.pending_operations != 0 {
        failures.push("an upgraded database started with queued operations".to_string());
    }
    check_graph(&db, &settled, &mut failures).await;
    for consistency in [SearchConsistency::Strong, SearchConsistency::Eventual] {
        check_searches(&db, &settled, consistency, &mut failures, "existing").await;
    }
    new_writes(&db, &mut settled).await;
    check_searches(
        &db,
        &settled,
        SearchConsistency::Strong,
        &mut failures,
        "new-writes-strong",
    )
    .await;
    wait_published(&db).await;
    let stats = db.index_operation_queue_stats();
    report.insert(
        "settled_published_operations".into(),
        serde_json::json!(stats.published_operations),
    );
    for consistency in [SearchConsistency::Strong, SearchConsistency::Eventual] {
        check_searches(&db, &settled, consistency, &mut failures, "published").await;
    }
    db.close().await.unwrap();
    let db = open(&root, "settled").await;
    check_graph(&db, &settled, &mut failures).await;
    for consistency in [SearchConsistency::Strong, SearchConsistency::Eventual] {
        check_searches(&db, &settled, consistency, &mut failures, "restart").await;
    }
    db.close().await.unwrap();

    // In-flight database.
    let inflight = docs(&manifest["inflight"]);
    let db = open(&root, "inflight").await;
    let mut builds = serde_json::Map::new();
    let mut recreate = Vec::new();
    for (family, spec) in [("vector", vector_spec()), ("text", text_spec())] {
        let operation = manifest["inflight_builds"][family]["operation"]
            .as_str()
            .unwrap();
        let terminal = wait_terminal(&db, operation).await;
        builds.insert(
            family.into(),
            serde_json::json!({
                "status_at_close": manifest["inflight_builds"][family]["status_at_close"],
                "stage_at_close_reopen": terminal["stage"],
                "terminal": terminal["status"],
                "blocker_code": terminal["blocker_code"],
            }),
        );
        if terminal["status"] == "blocked" {
            recreate.push(spec);
        } else if terminal["status"] != "succeeded" {
            failures.push(format!("{family} in-flight build ended {terminal}"));
        }
    }
    for spec in recreate {
        if let Some(abort) = ddl(&db, "dropped", traversal::g().drop_index(spec.clone())).await {
            wait_terminal(&db, &abort).await;
        }
        let rebuild = ddl(
            &db,
            "created",
            traversal::g().create_index_if_not_exists(spec),
        )
        .await
        .expect("recreate enqueues a build");
        let terminal = wait_terminal(&db, &rebuild).await;
        if terminal["status"] != "succeeded" {
            failures.push(format!("recreated build ended {terminal}"));
        }
    }
    wait_published(&db).await;
    for consistency in [SearchConsistency::Strong, SearchConsistency::Eventual] {
        check_searches(&db, &inflight, consistency, &mut failures, "inflight").await;
    }
    report.insert("inflight_builds".into(), serde_json::Value::Object(builds));
    db.close().await.unwrap();

    report.insert("failures".into(), serde_json::json!(failures));
    println!("{}", serde_json::to_string_pretty(&report).unwrap());
    if !failures.is_empty() {
        std::process::exit(1);
    }
}
