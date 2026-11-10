//! A node unique equality index admits at most one live node per value on
//! every write path. Each check reads the authoritative node rows through a
//! label scan, so a broken index cannot hide a duplicate.
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use db::error::HelixDbError;
use db::{HelixDB, HelixDbSource};
use helix_ast::graph::NodeRef;
use helix_ast::index::IndexSpec;
use helix_ast::query::{QueryRequest, QueryValue};
use helix_ast::value::PropertyInput;
use helix_ast::{batch, traversal};

const LABEL: &str = "User";

async fn open(source: HelixDbSource) -> Arc<HelixDB> {
    Arc::new(HelixDB::open(source).await.unwrap())
}

fn memory(name: &str) -> HelixDbSource {
    HelixDbSource::InMemory {
        database: name.to_owned(),
    }
}

/// Starts the unique `User.email` index build and returns its operation ID.
async fn create_unique_index(db: &HelixDB) -> String {
    let receipt = db
        .query(QueryRequest::write(
            batch::write_batch()
                .var_as(
                    "operation",
                    traversal::g().create_index_if_not_exists(IndexSpec::node_unique_equality(
                        LABEL, "email",
                    )),
                )
                .returning(["operation"]),
        ))
        .await
        .unwrap();
    receipt["operation"]["operation_id"]
        .as_str()
        .unwrap()
        .to_owned()
}

/// Waits for an index operation to finish and returns its terminal status.
async fn finished(db: &HelixDB, operation_id: &str) -> String {
    tokio::time::timeout(Duration::from_secs(60), async {
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
                Some("queued" | "running") => tokio::time::sleep(Duration::from_millis(5)).await,
                Some(state) => break state.to_owned(),
                None => panic!("index operation status: {status}"),
            }
        }
    })
    .await
    .expect("index operation finishes")
}

async fn unique_db(name: &str) -> Arc<HelixDB> {
    let db = open(memory(name)).await;
    let operation = create_unique_index(&db).await;
    assert_eq!(finished(&db, &operation).await, "succeeded");
    db
}

async fn add(db: &HelixDB, email: &str) -> Result<u64, HelixDbError> {
    let created = db
        .query(QueryRequest::write(
            batch::write_batch()
                .var_as(
                    "user",
                    traversal::g().add_n(LABEL, vec![("email", PropertyInput::from(email))]),
                )
                .returning(["user"]),
        ))
        .await?;
    Ok(created["user"][0]["$id"].as_u64().unwrap())
}

async fn set_email(db: &HelixDB, id: u64, email: &str) -> Result<(), HelixDbError> {
    db.query(QueryRequest::write(
        batch::write_batch()
            .var_as(
                "updated",
                traversal::g()
                    .n(NodeRef::id(id))
                    .set_property("email", email),
            )
            .returning(Vec::<String>::new()),
    ))
    .await
    .map(drop)
}

/// Live node count per email, read from the node rows of a label scan.
async fn emails(db: &HelixDB) -> BTreeMap<String, usize> {
    let rows = db
        .query(QueryRequest::read(
            batch::read_batch()
                .var_as(
                    "rows",
                    traversal::g()
                        .n_with_label(LABEL)
                        .value_map(Some(vec!["$id", "email"])),
                )
                .returning(["rows"]),
        ))
        .await
        .unwrap();
    rows["rows"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|row| row["email"].as_str())
        .fold(BTreeMap::new(), |mut counts, email| {
            *counts.entry(email.to_owned()).or_default() += 1;
            counts
        })
}

fn assert_unique(counts: &BTreeMap<String, usize>) {
    let duplicated = counts
        .iter()
        .filter(|(_, count)| **count > 1)
        .collect::<Vec<_>>();
    assert!(
        duplicated.is_empty(),
        "duplicate live values: {duplicated:?}"
    );
}

fn assert_violation(result: Result<impl std::fmt::Debug, HelixDbError>) {
    assert!(
        matches!(result, Err(HelixDbError::UniqueConstraintViolation { .. })),
        "{result:?}"
    );
}

#[tokio::test]
async fn separate_requests_reject_a_taken_value() {
    let db = unique_db("unique-separate").await;
    add(&db, "a@x").await.unwrap();
    assert_violation(add(&db, "a@x").await);
    assert_unique(&emails(&db).await);
}

#[tokio::test]
async fn one_batch_rejects_two_claims_of_one_value() {
    let db = unique_db("unique-same-batch").await;
    let add = || traversal::g().add_n(LABEL, vec![("email", PropertyInput::from("a@x"))]);
    assert_violation(
        db.query(QueryRequest::write(
            batch::write_batch()
                .var_as("first", add())
                .var_as("second", add())
                .returning(Vec::<String>::new()),
        ))
        .await,
    );
    assert!(emails(&db).await.is_empty());
}

#[tokio::test]
async fn for_each_rejects_duplicate_items() {
    let db = unique_db("unique-for-each").await;
    let item = |email: &str| {
        QueryValue::Object(BTreeMap::from([(
            "email".to_owned(),
            QueryValue::String(email.to_owned()),
        )]))
    };
    assert_violation(
        db.query(
            QueryRequest::write(
                batch::write_batch()
                    .for_each_param(
                        "items",
                        batch::write_batch().var_as(
                            "user",
                            traversal::g()
                                .add_n(LABEL, vec![("email", PropertyInput::param("email"))]),
                        ),
                    )
                    .returning(Vec::<String>::new()),
            )
            .with_parameter_value(
                "items",
                QueryValue::Array(vec![item("a@x"), item("b@x"), item("a@x")]),
            ),
        )
        .await,
    );
    assert!(emails(&db).await.is_empty());
}

#[tokio::test]
async fn set_property_rejects_a_taken_value_and_allows_its_own() {
    let db = unique_db("unique-set-property").await;
    let first = add(&db, "a@x").await.unwrap();
    let second = add(&db, "b@x").await.unwrap();
    assert_violation(set_email(&db, second, "a@x").await);
    set_email(&db, first, "a@x").await.unwrap();
    set_email(&db, first, "c@x").await.unwrap();
    set_email(&db, second, "a@x").await.unwrap();
    assert_eq!(
        emails(&db).await,
        BTreeMap::from([("a@x".to_owned(), 1), ("c@x".to_owned(), 1)])
    );
}

#[tokio::test]
async fn released_values_can_be_claimed_again() {
    let db = unique_db("unique-release").await;
    let first = add(&db, "a@x").await.unwrap();
    db.query(QueryRequest::write(
        batch::write_batch()
            .var_as("gone", traversal::g().n(NodeRef::id(first)).drop())
            .returning(Vec::<String>::new()),
    ))
    .await
    .unwrap();
    let second = add(&db, "a@x").await.unwrap();
    db.query(QueryRequest::write(
        batch::write_batch()
            .var_as(
                "cleared",
                traversal::g()
                    .n(NodeRef::id(second))
                    .remove_property("email"),
            )
            .returning(Vec::<String>::new()),
    ))
    .await
    .unwrap();
    add(&db, "a@x").await.unwrap();
    assert_eq!(emails(&db).await, BTreeMap::from([("a@x".to_owned(), 1)]));
}

#[tokio::test]
async fn drop_and_reclaim_in_one_batch() {
    let db = unique_db("unique-drop-reclaim").await;
    let first = add(&db, "a@x").await.unwrap();
    db.query(QueryRequest::write(
        batch::write_batch()
            .var_as("gone", traversal::g().n(NodeRef::id(first)).drop())
            .var_as(
                "user",
                traversal::g().add_n(LABEL, vec![("email", PropertyInput::from("a@x"))]),
            )
            .returning(Vec::<String>::new()),
    ))
    .await
    .unwrap();
    assert_eq!(emails(&db).await, BTreeMap::from([("a@x".to_owned(), 1)]));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_inserts_admit_one_owner() {
    let db = unique_db("unique-concurrent-insert").await;
    for round in 0..20 {
        let email = format!("race-{round}@x");
        let mut writers = tokio::task::JoinSet::new();
        for _ in 0..8 {
            let db = Arc::clone(&db);
            let email = email.clone();
            writers.spawn(async move { add(&db, &email).await });
        }
        let results = writers.join_all().await;
        let admitted = results.iter().filter(|result| result.is_ok()).count();
        assert!(admitted <= 1, "round {round}: {results:?}");
    }
    assert_unique(&emails(&db).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_updates_admit_one_owner() {
    let db = unique_db("unique-concurrent-update").await;
    for round in 0..20 {
        let mut ids = Vec::new();
        for writer in 0..8 {
            ids.push(add(&db, &format!("seed-{round}-{writer}@x")).await.unwrap());
        }
        let email = format!("taken-{round}@x");
        let mut writers = tokio::task::JoinSet::new();
        for id in ids {
            let db = Arc::clone(&db);
            let email = email.clone();
            writers.spawn(async move { set_email(&db, id, &email).await });
        }
        let results = writers.join_all().await;
        let admitted = results.iter().filter(|result| result.is_ok()).count();
        assert!(admitted <= 1, "round {round}: {results:?}");
    }
    assert_unique(&emails(&db).await);
}

#[tokio::test]
async fn reopened_database_keeps_enforcing() {
    let root = std::env::temp_dir().join(format!("unique-reopen-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    let source = || HelixDbSource::Disk {
        root: root.clone(),
        database: "unique-reopen".to_owned(),
    };
    {
        let db = open(source()).await;
        let operation = create_unique_index(&db).await;
        assert_eq!(finished(&db, &operation).await, "succeeded");
        add(&db, "a@x").await.unwrap();
        db.close().await.unwrap();
    }
    let db = open(source()).await;
    assert_violation(add(&db, "a@x").await);
    assert_unique(&emails(&db).await);
    db.close().await.unwrap();
    std::fs::remove_dir_all(&root).unwrap();
}

#[tokio::test]
async fn build_rejects_existing_duplicates() {
    let db = open(memory("unique-build-existing")).await;
    add(&db, "a@x").await.unwrap();
    add(&db, "a@x").await.unwrap();
    let operation = create_unique_index(&db).await;
    assert_ne!(finished(&db, &operation).await, "succeeded");
    // A failed build enforces nothing, so later writes stay unconstrained.
    add(&db, "b@x").await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn writes_during_the_build_never_leave_an_active_index_with_duplicates() {
    for round in 0..10 {
        let db = open(memory(&format!("unique-build-race-{round}"))).await;
        for seed in 0..200 {
            add(&db, &format!("seed-{seed}@x")).await.unwrap();
        }
        add(&db, "a@x").await.unwrap();
        let operation = create_unique_index(&db).await;
        let mut writers = tokio::task::JoinSet::new();
        for _ in 0..4 {
            let db = Arc::clone(&db);
            writers.spawn(async move { add(&db, "a@x").await });
        }
        writers.join_all().await;
        let state = finished(&db, &operation).await;
        let counts = emails(&db).await;
        if state == "succeeded" {
            assert_unique(&counts);
            assert_violation(add(&db, "a@x").await);
        } else {
            assert!(
                counts["a@x"] > 1,
                "round {round}: {state} without duplicates"
            );
        }
    }
}

#[tokio::test]
async fn duplicates_are_rejected_while_the_index_builds() {
    let db = open(memory("unique-while-building")).await;
    seed(&db, "seed", 2_000).await;
    let operation = create_unique_index(&db).await;
    add(&db, "a@x").await.unwrap();
    assert_violation(add(&db, "a@x").await);
    assert_eq!(finished(&db, &operation).await, "succeeded");
    assert_violation(add(&db, "a@x").await);
    assert_unique(&emails(&db).await);
}

#[tokio::test]
async fn a_blocked_build_keeps_rejecting_duplicates_of_claimed_values() {
    let db = open(memory("unique-blocked-still-claims")).await;
    add(&db, "b@x").await.unwrap();
    add(&db, "a@x").await.unwrap();
    add(&db, "a@x").await.unwrap();
    let operation = create_unique_index(&db).await;
    assert_eq!(finished(&db, &operation).await, "blocked");
    // The scan claimed b@x before it met the pre-existing duplicate.
    assert_violation(add(&db, "b@x").await);
    add(&db, "c@x").await.unwrap();
    assert_violation(add(&db, "c@x").await);
}

async fn cypher(db: &HelixDB, query: &str) -> Result<db::cypher::Response, db::cypher::Error> {
    db.cypher(db::cypher::Request::new(query)).await
}

#[tokio::test]
async fn cypher_writes_respect_the_unique_index() {
    let db = unique_db("unique-cypher").await;
    cypher(&db, "CREATE (:User {email: 'a@x'})").await.unwrap();
    let create = cypher(&db, "CREATE (:User {email: 'a@x'})").await;
    let unwind = cypher(&db, "UNWIND ['c@x', 'c@x'] AS e CREATE (:User {email: e})").await;
    cypher(&db, "CREATE (:User {email: 'b@x'})").await.unwrap();
    let set = cypher(&db, "MATCH (u:User {email: 'b@x'}) SET u.email = 'a@x'").await;
    assert!(create.is_err(), "{create:?}");
    assert!(unwind.is_err(), "{unwind:?}");
    assert!(set.is_err(), "{set:?}");
    assert_unique(&emails(&db).await);
}

/// Adds `count` users with distinct emails in batches and returns their IDs.
async fn seed(db: &HelixDB, prefix: &str, count: usize) -> Vec<u64> {
    let mut ids = Vec::with_capacity(count);
    for start in (0..count).step_by(100) {
        let end = (start + 100).min(count);
        let names = (start..end)
            .map(|seed| format!("s{seed}"))
            .collect::<Vec<_>>();
        let created = db
            .query(QueryRequest::write(
                names
                    .iter()
                    .fold(batch::write_batch(), |batch, name| {
                        batch.var_as(
                            name,
                            traversal::g().add_n(
                                LABEL,
                                vec![("email", PropertyInput::from(format!("{prefix}-{name}@x")))],
                            ),
                        )
                    })
                    .returning(names.clone()),
            ))
            .await
            .unwrap();
        ids.extend(
            names
                .iter()
                .map(|name| created[name.as_str()][0]["$id"].as_u64().unwrap()),
        );
    }
    ids
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn build_finishes_under_steady_writes() {
    let db = open(memory("unique-steady-writes")).await;
    let ids = seed(&db, "seed", 20_000).await;
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let writer = {
        let db = Arc::clone(&db);
        let stop = Arc::clone(&stop);
        tokio::spawn(async move {
            let mut writes = 0_u64;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                let id = ids[(writes as usize * 7_919) % ids.len()];
                // A write that races the build's own transactions may
                // conflict; the writer only needs to keep touching the index.
                let _ = set_email(&db, id, &format!("moved-{writes}@x")).await;
                writes += 1;
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            writes
        })
    };
    let operation = create_unique_index(&db).await;
    let outcome = tokio::time::timeout(Duration::from_secs(60), finished(&db, &operation)).await;
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    writer.await.unwrap();
    assert_eq!(outcome.ok().as_deref(), Some("succeeded"));
}

#[tokio::test]
async fn moving_a_value_to_a_later_node_mid_build_does_not_block() {
    for round in 0..10 {
        let db = open(memory(&format!("unique-move-up-{round}"))).await;
        let low = add(&db, "x@x").await.unwrap();
        for seed in 0..2_000 {
            add(&db, &format!("seed-{seed}@x")).await.unwrap();
        }
        let high = add(&db, "z@x").await.unwrap();
        let operation = create_unique_index(&db).await;
        tokio::time::sleep(Duration::from_millis(round * 2)).await;
        // A write racing one of the build's transactions may lose with a
        // retryable conflict; the move only has to land.
        for (id, email) in [(low, "y@x"), (high, "x@x")] {
            loop {
                match set_email(&db, id, email).await {
                    Ok(()) => break,
                    Err(error) if error.is_transaction_conflict() => {}
                    Err(error) => panic!("round {round}: {error}"),
                }
            }
        }
        let state = finished(&db, &operation).await;
        assert_unique(&emails(&db).await);
        assert_eq!(state, "succeeded", "round {round}");
    }
}
