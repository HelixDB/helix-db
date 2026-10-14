//! End-to-end index-served source filters and counts through `HelixDB::query`.
//!
//! Every query runs against a database with the `Item` indexes and one
//! without, which scans and filters per row; both must return the same rows.
//! The indexed plans never scan.

use helix_ast::{batch, expr, index, query, traversal, value};
use helix_planner::{context, exec, planning};

use crate::encoding::keys::scope::DataScope;
use crate::{HelixDB, HelixDbSource};

const ITEMS: i64 = 30;

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

/// `Item` `i{n}` has `kind` `A`, `B` or `C` (none for every seventh), an
/// unindexed `rank` `n`, and a `uid`; `Note` nodes share the kinds.
fn seed() -> batch::WriteBatch {
    (0..ITEMS).fold(batch::write_batch(), |write, n| {
        let mut properties = vec![
            ("uid", value::PropertyInput::from(format!("i{n}"))),
            ("rank", value::PropertyInput::from(n)),
        ];
        if n % 7 != 0 {
            properties.push((
                "kind",
                value::PropertyInput::from(["A", "B", "C"][(n % 3) as usize]),
            ));
        }
        write
            .var_as(&format!("i{n}"), traversal::g().add_n("Item", properties))
            .var_as(
                &format!("n{n}"),
                traversal::g().add_n(
                    "Note",
                    vec![(
                        "kind",
                        value::PropertyInput::from(["A", "B", "C"][(n % 3) as usize]),
                    )],
                ),
            )
    })
}

async fn seeded(name: &str, indexed: bool) -> HelixDB {
    let db = open(name).await;
    if indexed {
        create_index(&db, index::IndexSpec::node_equality("Item", "kind")).await;
        create_index(&db, index::IndexSpec::node_equality("Item", "uid")).await;
    }
    db.query(query::QueryRequest::write(seed())).await.unwrap();
    db
}

fn read(result: traversal::Traversal<traversal::Terminal>) -> batch::ReadBatch {
    batch::read_batch()
        .var_as("result", result)
        .returning(["result"])
}

/// The response with every row array sorted, so row order does not matter.
fn sorted(mut response: serde_json::Value) -> serde_json::Value {
    if let Some(rows) = response["result"].as_array_mut() {
        rows.sort_by_key(|row| row.to_string());
    }
    response
}

#[test]
fn index_served_sources_and_counts_match_the_per_row_filter() {
    // Counts over branch-residual unions nest cursor polls whose frames
    // exceed the default test stack in debug builds, so run on a dedicated
    // stack.
    std::thread::Builder::new()
        .name("index-served-sources-e2e".to_owned())
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("index-served sources test runtime builds")
                .block_on(index_served_sources_and_counts_match_the_per_row_filter_contract());
        })
        .expect("index-served sources test thread starts")
        .join()
        .expect("index-served sources test thread completes");
}

async fn index_served_sources_and_counts_match_the_per_row_filter_contract() {
    let indexed = seeded("index-served-sources-indexed", true).await;
    let unindexed = seeded("index-served-sources-unindexed", false).await;
    let items = |predicate: expr::Predicate| traversal::g().n_with_label_where("Item", predicate);
    let wide_uids = (0..120)
        .map(|n| value::PropertyValue::from(format!("i{n}")))
        .chain(core::iter::once(value::PropertyValue::Null))
        .collect::<Vec<_>>();
    let reads = [
        // A partly indexed OR: each branch reads its index and evaluates its
        // own unindexed conjunct; rows several branches accept count once.
        items(expr::Predicate::or(vec![
            expr::Predicate::and(vec![
                expr::Predicate::eq("kind", "B"),
                expr::Predicate::gte("rank", 10),
            ]),
            expr::Predicate::and(vec![
                expr::Predicate::eq("kind", "B"),
                expr::Predicate::lt("rank", 20),
            ]),
            expr::Predicate::eq("uid", "i7"),
        ]))
        .values(vec!["uid"]),
        items(expr::Predicate::or(vec![
            expr::Predicate::and(vec![
                expr::Predicate::eq("kind", "A"),
                expr::Predicate::lt("rank", 20),
            ]),
            expr::Predicate::eq("kind", "C"),
        ]))
        .count(),
        // Counts over a saved stream.
        items(expr::Predicate::eq("kind", "B")).as_("saved").count(),
        traversal::g()
            .n_with_label("Item")
            .where_(expr::Predicate::eq("kind", "C"))
            .store("saved")
            .count(),
        // Lists wider than one index union, with null.
        items(expr::Predicate::is_in(
            "uid",
            value::PropertyValue::Array(wide_uids),
        ))
        .values(vec!["uid"]),
        items(expr::Predicate::or(
            (0..100)
                .map(|n| expr::Predicate::eq("uid", format!("i{n}")))
                .collect(),
        ))
        .count(),
        // Null equality reads only the label's rows outside the lane.
        items(expr::Predicate::eq("kind", value::PropertyValue::Null)).values(vec!["uid"]),
        items(expr::Predicate::and(vec![
            expr::Predicate::eq("kind", value::PropertyValue::Null),
            expr::Predicate::is_in(
                "uid",
                value::PropertyValue::StringArray(vec!["i0".into(), "i1".into(), "i14".into()]),
            ),
        ]))
        .count(),
    ];
    for read in reads.map(read) {
        let prepared = indexed
            .planner_context_scoped_prepared(
                context::ParamBindings::default(),
                DataScope::LegacyUnscoped,
            )
            .await
            .unwrap();
        let plan = planning::plan_read_batch(&read, prepared.context()).unwrap();
        let scans = plan
            .steps()
            .iter()
            .filter(|step| {
                matches!(
                    &step.op,
                    exec::ExecOp::Access { plan } if matches!(
                        plan.as_ref(),
                        exec::ExecAccessPlan::Node(
                            exec::ExecNodeAccessPlan::LabelScan { .. }
                                | exec::ExecNodeAccessPlan::AllScan
                        )
                    )
                )
            })
            .count();
        assert_eq!(scans, 0, "{:?}", plan.steps());
        let expected = unindexed
            .query(query::QueryRequest::read(read.clone()))
            .await
            .unwrap();
        let actual = indexed
            .query(query::QueryRequest::read(read.clone()))
            .await
            .unwrap();
        assert_eq!(sorted(actual), sorted(expected));
    }
    indexed.close().await.unwrap();
    unindexed.close().await.unwrap();
}
