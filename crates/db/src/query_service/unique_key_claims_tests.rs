//! Conditional claims keyed by a unique property through `HelixDB::query`.
//!
//! A claim reads its jobs by a unique key, or a list of keys, and evaluates
//! the status and expiry guard on those candidates, so no request reads the
//! status or expiry index across the label. The indexed database must return the same
//! responses and end in the same state as one that scans and filters per
//! row, including claims of one job earlier in the same batch, and
//! concurrent claims of one job must let exactly one through.

use helix_ast::{batch, expr, index, query, traversal, value};
use helix_planner::{context, planning};

use crate::encoding::keys::scope::DataScope;
use crate::error::HelixDbError;
use crate::{HelixDB, HelixDbSource};

const JOBS: i64 = 60;
const CUTOFF: i64 = 30;

async fn open(name: &str) -> HelixDB {
    HelixDB::open(HelixDbSource::InMemory {
        database: name.into(),
    })
    .await
    .unwrap()
}

async fn create_index(db: &HelixDB, spec: index::IndexSpec) {
    let receipt = db
        .query(query::QueryRequest::write(
            batch::write_batch()
                .var_as("index", traversal::g().create_index_if_not_exists(spec))
                .returning(["index"]),
        ))
        .await
        .unwrap();
    let operation = receipt["index"]["operation_id"]
        .as_str()
        .unwrap()
        .to_owned();
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        loop {
            let status = db
                .query(query::QueryRequest::read(
                    batch::read_batch()
                        .var_as("status", traversal::g().get_index_operation(&operation))
                        .returning(["status"]),
                ))
                .await
                .unwrap();
            match status["status"]["status"].as_str().unwrap() {
                "succeeded" => break,
                "queued" | "running" => tokio::task::yield_now().await,
                other => panic!("index failed: {other}"),
            }
        }
    })
    .await
    .unwrap();
}

/// Job `j{n}` is available, claimed at `n`, or done, by `n % 3`; every
/// tenth job has no `job_id`, so a null key has rows to match.
fn seed() -> batch::WriteBatch {
    (0..JOBS).fold(batch::write_batch(), |write, n| {
        let mut properties = vec![(
            "status",
            value::PropertyInput::from(["available", "claimed", "done"][(n % 3) as usize]),
        )];
        if n % 3 == 1 {
            properties.push(("claimed_at", value::PropertyInput::from(n)));
        }
        if n % 10 != 0 {
            properties.push(("job_id", value::PropertyInput::from(format!("j{n}"))));
        }
        write.var_as(&format!("j{n}"), traversal::g().add_n("Job", properties))
    })
}

async fn seeded(name: &str, indexed: bool) -> HelixDB {
    let db = open(name).await;
    if indexed {
        create_index(&db, index::IndexSpec::node_unique_equality("Job", "job_id")).await;
        create_index(&db, index::IndexSpec::node_equality("Job", "status")).await;
        create_index(&db, index::IndexSpec::node_range("Job", "claimed_at")).await;
    }
    db.query(query::QueryRequest::write(seed())).await.unwrap();
    db
}

/// The claim guard of one job: available, or claimed before the cutoff.
fn claim(
    key: expr::Predicate,
    at: i64,
) -> traversal::Traversal<traversal::Terminal, traversal::WriteEnabled> {
    traversal::g()
        .n_with_label_where(
            "Job",
            expr::Predicate::and(vec![
                key,
                expr::Predicate::is_in(
                    "status",
                    value::PropertyValue::StringArray(vec![
                        "available".to_owned(),
                        "claimed".to_owned(),
                    ]),
                ),
                expr::Predicate::or(vec![
                    expr::Predicate::eq("status", "available"),
                    expr::Predicate::lt_param("claimed_at", "cutoff"),
                ]),
            ]),
        )
        .set_property("status", "claimed")
        .set_property("claimed_at", at)
        .set_property("attempt", at)
        .values(vec!["job_id", "status", "claimed_at", "attempt"])
}

/// Five claims in one batch, as one request sends them.
fn claims(keys: [expr::Predicate; 5]) -> query::QueryRequest {
    let (write, names) = keys.into_iter().enumerate().fold(
        (batch::write_batch(), Vec::new()),
        |(write, mut names), (position, key)| {
            let name = format!("c{position}");
            names.push(name.clone());
            (
                write.var_as(&name, claim(key, 100 + position as i64)),
                names,
            )
        },
    );
    query::QueryRequest::write(write.returning(names))
        .with_parameter_value("cutoff", query::QueryValue::I64(CUTOFF))
        .with_parameter_value("a", query::QueryValue::String("j2".to_owned()))
        .with_parameter_value("none", query::QueryValue::Null)
        .with_parameter_value(
            "nulls",
            query::QueryValue::Array(vec![
                query::QueryValue::String("j41".to_owned()),
                query::QueryValue::Null,
            ]),
        )
        .with_parameter_value(
            "list",
            query::QueryValue::Array(
                ["j33", "j34", "j35", "absent"]
                    .map(|id| query::QueryValue::String(id.to_owned()))
                    .to_vec(),
            ),
        )
}

fn state() -> query::QueryRequest {
    query::QueryRequest::read(
        batch::read_batch()
            .var_as(
                "jobs",
                traversal::g().n_with_label("Job").values(vec![
                    "job_id",
                    "status",
                    "claimed_at",
                    "attempt",
                ]),
            )
            .returning(["jobs"]),
    )
}

/// The response with the rows of every entry sorted: a list of keys returns
/// its rows in no promised order.
fn sorted(mut response: serde_json::Value) -> serde_json::Value {
    let Some(entries) = response.as_object_mut() else {
        return response;
    };
    entries
        .values_mut()
        .filter_map(serde_json::Value::as_array_mut)
        .for_each(|rows| rows.sort_by_key(|row| row.to_string()));
    response
}

#[tokio::test]
async fn unique_key_claims_match_the_per_row_filter() {
    let indexed = seeded("unique-key-claims-indexed", true).await;
    let unindexed = seeded("unique-key-claims-unindexed", false).await;
    let key = |id: &str| expr::Predicate::eq("job_id", id);
    // Each batch with the range scans its plan may read: only a null key,
    // which matches every job without one, is no unique key.
    let batches = [
        // Available, expired, unexpired, done, and missing jobs.
        (
            [key("j3"), key("j4"), key("j31"), key("j5"), key("absent")],
            0,
        ),
        // A job claimed earlier in the batch is no longer claimable, and a
        // parameter key behaves like its literal.
        (
            [
                key("j6"),
                key("j6"),
                expr::Predicate::eq_param("job_id", "a"),
                key("j2"),
                key("j7"),
            ],
            0,
        ),
        // A null key matches only jobs without one.
        (
            [
                expr::Predicate::eq("job_id", value::PropertyValue::Null),
                expr::Predicate::eq_param("job_id", "none"),
                key("j9"),
                key("j9"),
                key("j12"),
            ],
            2,
        ),
        // Lists of keys: literal, bound parameter and `OR`, each claiming
        // only its claimable jobs; a job an earlier list claimed is not
        // claimable again.
        (
            [
                expr::Predicate::is_in(
                    "job_id",
                    value::PropertyValue::StringArray(
                        ["j18", "j19", "j23", "j18", "absent"]
                            .map(str::to_owned)
                            .to_vec(),
                    ),
                ),
                expr::Predicate::is_in_param("job_id", "list"),
                expr::Predicate::or(vec![key("j24"), key("j25")]),
                key("j19"),
                expr::Predicate::is_in(
                    "job_id",
                    value::PropertyValue::StringArray(
                        ["j33", "j36", "j37"].map(str::to_owned).to_vec(),
                    ),
                ),
            ],
            0,
        ),
        // A list wider than one index union is a key too, a list parameter
        // holding null is not, and a descending list returns its rows.
        (
            [
                expr::Predicate::is_in(
                    "job_id",
                    value::PropertyValue::StringArray((40..105).map(|n| format!("j{n}")).collect()),
                ),
                expr::Predicate::is_in_param("job_id", "nulls"),
                key("j42"),
                expr::Predicate::is_in(
                    "job_id",
                    value::PropertyValue::StringArray(
                        ["j28", "j27", "j22", "j21"].map(str::to_owned).to_vec(),
                    ),
                ),
                key("absent"),
            ],
            1,
        ),
    ];
    // Frame fields stay runtime parameters, so each frame's key is resolved
    // only when the body runs, a null one included.
    let frames = query::QueryValue::Array(
        [Some("j15"), None, Some("j15"), Some("j16"), Some("absent")]
            .into_iter()
            .map(|id| {
                query::QueryValue::Object(
                    [(
                        "id".to_owned(),
                        id.map_or(query::QueryValue::Null, |id| {
                            query::QueryValue::String(id.to_owned())
                        }),
                    )]
                    .into_iter()
                    .collect(),
                )
            })
            .collect(),
    );
    let foreach = query::QueryRequest::write(
        batch::write_batch()
            .for_each_param(
                "frames",
                batch::write_batch().var_as(
                    "claim",
                    claim(expr::Predicate::eq_param("job_id", "id"), 300),
                ),
            )
            .var_as("jobs", traversal::g().n_with_label("Job").count())
            .returning(["jobs"]),
    )
    .with_parameter_value("frames", frames)
    .with_parameter_value("cutoff", query::QueryValue::I64(CUTOFF));
    let requests = batches
        .into_iter()
        .map(|(keys, ranges)| (claims(keys), ranges))
        .chain(core::iter::once((foreach, 0)));
    for (request, ranges) in requests {
        let (batch::BatchQuery::Write(write), parameters) = request.clone().into_query() else {
            unreachable!("claims are a write batch");
        };
        let prepared = indexed
            .planner_context_scoped_prepared(
                parameters.into_iter().fold(
                    context::ParamBindings::default(),
                    |bindings, (name, value)| {
                        bindings.with_query_value(
                            helix_planner::ir::NonEmptyString::new(name).unwrap(),
                            value,
                        )
                    },
                ),
                DataScope::LegacyUnscoped,
            )
            .await
            .unwrap();
        let planned =
            planning::plan_write_batch_with_diagnostics(&write, prepared.context()).unwrap();
        assert_eq!(
            planned
                .diagnostics()
                .statistics
                .node_accesses
                .range_index_scans,
            ranges,
            "{:#?}",
            planned.plan().steps()
        );

        let expected = unindexed.query(request.clone()).await.unwrap();
        let actual = indexed.query(request).await.unwrap();
        assert_eq!(sorted(actual), sorted(expected));
        assert_eq!(
            sorted(indexed.query(state()).await.unwrap()),
            sorted(unindexed.query(state()).await.unwrap())
        );
    }
    indexed.close().await.unwrap();
    unindexed.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_unique_key_claims_let_exactly_one_through() {
    let db = std::sync::Arc::new(seeded("unique-key-claims-concurrent", true).await);
    for job in ["j3", "j4"] {
        let claimed = futures::future::join_all((0..8).map(|attempt| {
            let db = db.clone();
            let job = job.to_owned();
            async move {
                loop {
                    let request = query::QueryRequest::write(
                        batch::write_batch()
                            // Claimed past the cutoff, so a claim holds.
                            .var_as(
                                "claim",
                                claim(expr::Predicate::eq("job_id", job.as_str()), 1_000 + attempt),
                            )
                            .returning(["claim"]),
                    )
                    .with_parameter_value("cutoff", query::QueryValue::I64(CUTOFF));
                    match db.query(request).await {
                        Ok(response) => {
                            break response["claim"].as_array().unwrap().len();
                        }
                        Err(HelixDbError::TransactionConflict(_)) => tokio::task::yield_now().await,
                        Err(error) => panic!("claim failed: {error}"),
                    }
                }
            }
        }))
        .await;
        assert_eq!(claimed.iter().sum::<usize>(), 1, "{job}: {claimed:?}");
    }
    std::sync::Arc::into_inner(db)
        .unwrap()
        .close()
        .await
        .unwrap();
}

#[tokio::test]
async fn cypher_unique_key_claims_match_the_per_row_filter() {
    let indexed = seeded("unique-key-claims-cypher-indexed", true).await;
    let unindexed = seeded("unique-key-claims-cypher-unindexed", false).await;
    let request = |id: &str| {
        let mut request = crate::cypher::Request::new(
            "MATCH (j:Job) WHERE j.job_id = $id AND j.status IN ['available', 'claimed'] \
             AND (j.status = 'available' OR j.claimed_at < $cutoff) \
             SET j.status = 'claimed', j.claimed_at = 500, j.attempt = 500 \
             RETURN j.job_id AS job_id",
        );
        request
            .parameters
            .insert("id".to_owned(), query::QueryValue::String(id.to_owned()));
        request
            .parameters
            .insert("cutoff".to_owned(), query::QueryValue::I64(CUTOFF));
        request
    };
    let explained =
        serde_json::to_string(&indexed.explain_cypher(request("j3")).await.unwrap()).unwrap();
    // The key is the only index read: neither the status bitmaps nor the
    // expiry range.
    assert!(explained.contains("node_eq:Job:job_id"), "{explained}");
    assert!(!explained.contains("node_eq:Job:status"), "{explained}");
    assert!(
        !explained.contains("node_range:Job:claimed_at"),
        "{explained}"
    );
    for id in ["j3", "j3", "j4", "j31", "j5", "absent"] {
        let expected = unindexed.cypher(request(id)).await.unwrap();
        let actual = indexed.cypher(request(id)).await.unwrap();
        assert_eq!(actual.rows, expected.rows, "{id}");
    }
    assert_eq!(
        sorted(indexed.query(state()).await.unwrap()),
        sorted(unindexed.query(state()).await.unwrap())
    );
    indexed.close().await.unwrap();
    unindexed.close().await.unwrap();
}
