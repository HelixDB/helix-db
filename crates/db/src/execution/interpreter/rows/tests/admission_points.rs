//! Every memory admission a row program makes is a possible rejection point.
//! These tests find each one and require the rejection to be a clean
//! `MemoryLimit` that releases every reservation, never a partial result.
use super::super::*;
use crate::execution::interpreter::test_support;
use helix_planner::context;
use serde_json::json;

/// One execution of the request row program under `memory_bytes`. It returns
/// the rows, or `None` for a memory rejection, with the budget's high-water
/// mark. A write runs in a request scope that is aborted afterwards, so every
/// execution observes the same graph.
async fn bounded(
    db: &crate::HelixDB,
    plan: &r::RowPlan,
    memory_bytes: usize,
    batch_rows: usize,
) -> (Option<Vec<Vec<serde_json::Value>>>, usize) {
    let mut ctx = ExecutionContext::new(db, context::ParamBindings::default());
    let budget = memory::Budget::new(memory_bytes);
    ctx.row_memory = Some(budget.clone());
    let opened = match plan.query().effect() {
        r::Effect::Read => ctx.enable_request_read_view().await,
        r::Effect::Write => ctx.enable_request_write_scope().await,
    };
    let limits = Limits {
        memory_bytes,
        batch_rows,
        ..Limits::default()
    };
    let result = match opened {
        Ok(()) => ctx.row_program(plan, &BTreeMap::new(), limits).await,
        Err(error) => Err(error.into()),
    };
    let rows = match result {
        Ok(response) => Some(
            <output::Typed as output::Format>::finish(
                response,
                crate::cypher::ResourceUsage::default(),
            )
            .rows,
        ),
        Err(Error::Query(error)) if error.detail == "MemoryLimit" => None,
        Err(error) => panic!("{memory_bytes} bytes: {error}"),
    };
    match plan.query().effect() {
        r::Effect::Read => ctx.close_request_read_view().unwrap(),
        r::Effect::Write => ctx.abort_request_write_scope(),
    }
    drop(ctx);
    assert_eq!(
        budget.available(),
        memory_bytes,
        "{memory_bytes} bytes: every reservation is released"
    );
    (rows, budget.peak())
}

/// Reject at every admission that raises the budget's high-water mark. A
/// budget that rejects an admission rejects it at every smaller budget, and a
/// budget that passes it records a higher peak, so bisection finds the next
/// rejection point. Every budget that completes returns the unconstrained
/// rows. Bisection is bounded by twice the unconstrained peak, which tolerates
/// storage read paths changing between executions.
async fn sweep(db: &crate::HelixDB, text: &str, execution: r::RowExecution, batch_rows: usize) {
    let plan = r::plan(
        helix_cypher::compile(text).unwrap(),
        &db.planner_context(context::ParamBindings::default()),
    )
    .unwrap()
    .with_execution(execution);
    let (expected, peak) = bounded(db, &plan, 64 * 1024 * 1024, batch_rows).await;
    let Some(expected) = expected else {
        panic!("{text}: unconstrained execution succeeds");
    };
    let bound = peak.saturating_mul(2);
    let mut limit = 0;
    loop {
        let (rows, _) = bounded(db, &plan, limit, batch_rows).await;
        let Some(rows) = rows else {
            assert!(limit < bound, "{text}: twice the peak admits the program");
            let (mut rejected, mut admitted) = (limit, bound);
            while admitted - rejected > 1 {
                let middle = rejected + (admitted - rejected) / 2;
                let (rows, reached) = bounded(db, &plan, middle, batch_rows).await;
                if rows.is_some() || reached > limit {
                    admitted = middle;
                } else {
                    rejected = middle;
                }
            }
            limit = admitted;
            continue;
        };
        assert_eq!(rows, expected, "{text} at {limit} bytes");
        return;
    }
}

/// Small graph data for every source, index lookup, expansion and join, plus
/// a hub of parallel relationships and a label with many equal index keys, so
/// the retained rows of wide lookups outgrow earlier transient admissions.
async fn open_graph(name: &str) -> crate::HelixDB {
    let db = test_support::open_db_with_config(
        test_support::in_memory_config(name)
            .with_equality_index("A", "key")
            .with_equality_index("W", "key")
            .with_range_index("N", "k"),
    )
    .await;
    for text in [
        "UNWIND range(1,6) AS k CREATE (:N {k:k}), (:A {key:k%2})-[:R {w:k}]->(:B {key:k%2})",
        "CREATE (h:H), (t:T) WITH h, t UNWIND range(1,12) AS i CREATE (h)-[:R]->(t)",
        "UNWIND range(1,24) AS i CREATE (:W {key:0})",
    ] {
        db.cypher(crate::cypher::Request::new(text)).await.unwrap();
    }
    db
}

/// Literal result columns widen every row of a query, so each retained row
/// charges more than the transient state that produced it.
fn wide(columns: usize) -> String {
    (1..=columns)
        .map(|column| format!("{column} AS c{column}"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Streamed initial MATCH sources feeding each direct consumer: projection,
/// ordered window, aggregation, joins, named paths and OPTIONAL null rows.
#[tokio::test]
async fn initial_graph_sources_reject_every_admission_cleanly() {
    let db = open_graph("admission-points-sources").await;
    for text in [
        "RETURN 1 AS x".to_owned(),
        "MATCH (n:N) RETURN n.k".to_owned(),
        "MATCH (n:N) RETURN n".to_owned(),
        "MATCH (n:N) WHERE n.k > 0 RETURN n ORDER BY n.k LIMIT 2".to_owned(),
        "MATCH (a:A), (b:B) WHERE a.key = b.key RETURN count(*)".to_owned(),
        "MATCH (a:A)-[:R]->(b:B), (n:N) RETURN count(*)".to_owned(),
        "MATCH p=(h:H)-[:R]->(t:T) RETURN length(p)".to_owned(),
        // A null row this wide outgrows the exhausted source's transients.
        format!("OPTIONAL MATCH (n:Missing) RETURN n, {}", wide(256)),
    ] {
        for batch_rows in [1, 512] {
            sweep(&db, &text, r::RowExecution::Batched, batch_rows).await;
        }
    }
    db.close().await.unwrap();
}

/// UNWIND sources through direct consumers and nonblocking pipelines, with
/// projection, filter, UNWIND, indexed and bound MATCH continuations.
#[tokio::test]
async fn row_pipelines_reject_every_admission_cleanly() {
    let db = open_graph("admission-points-pipelines").await;
    for text in [
        "UNWIND [3,1,2] AS x RETURN x ORDER BY x",
        "UNWIND range(1,12) AS x RETURN x",
        "UNWIND range(1,12) AS x RETURN count(*)",
        "UNWIND range(1,12) AS x RETURN DISTINCT x % 3",
        "UNWIND range(1,12) AS x RETURN x ORDER BY x DESC LIMIT 2",
        "UNWIND range(1,12) AS x WITH x AS a WHERE a > 1 WITH a + 1 AS b RETURN b",
        "UNWIND range(1,12) AS x WITH x AS a LIMIT 10 WHERE a > 1 WITH a + 1 AS b RETURN b",
        "UNWIND range(1,12) AS x WITH x AS a WHERE a > 1 RETURN count(a)",
        "UNWIND range(1,12) AS x WITH x AS a WHERE a > 0 RETURN DISTINCT a % 3",
        "UNWIND range(1,12) AS x WITH x AS a WHERE a > 0 RETURN a ORDER BY a LIMIT 2",
        // Past 128 retained rows the output's doubled capacity outgrows the
        // projection stage, so the next continuation poll is a new peak.
        "UNWIND range(1,65) AS x UNWIND [x, x] AS y RETURN y",
        "UNWIND [0,1,0,1] AS k MATCH (a:A {key:k}) RETURN a.key",
        "UNWIND [0,1] AS k MATCH (a:A)-[:R]->(b:B) WHERE a.key = k RETURN b.key",
        "UNWIND [1] AS i WITH i ORDER BY i MATCH (n:N) RETURN i, n.k",
    ] {
        for batch_rows in [1, 512] {
            sweep(&db, text, r::RowExecution::Batched, batch_rows).await;
        }
    }
    db.close().await.unwrap();
}

/// Mutation barriers roll back on every rejection, and materialized matches
/// reject cleanly while copying index, scan-fallback, join and expansion rows.
#[tokio::test]
async fn materialized_matches_and_mutations_reject_every_admission_cleanly() {
    let db = open_graph("admission-points-materialized").await;
    for (text, execution) in [
        (
            "CREATE (:X) WITH 1 AS one MATCH (n:N) RETURN n.k".to_owned(),
            r::RowExecution::Batched,
        ),
        ("MATCH (n:N) DELETE n".to_owned(), r::RowExecution::Batched),
        // The created write set and a padded row outgrow the CREATE and WITH
        // transients, so the DELETE operator itself is admitted at a new peak.
        (
            "CREATE (n:Tmp) WITH n, range(1,40) AS pad DELETE n RETURN size(pad) AS size"
                .to_owned(),
            r::RowExecution::Batched,
        ),
        // Wide candidates copied from an index probe, or from the source scan
        // that replaces a list probe, outgrow the probe's own transients.
        (
            format!(
                "UNWIND [0] AS k MATCH (w:W {{key:k}}) RETURN w.key, {}",
                wide(32)
            ),
            r::RowExecution::Materialized,
        ),
        (
            format!(
                "UNWIND [[0]] AS k MATCH (w:W {{key:k}}) RETURN w.key, {}",
                wide(32)
            ),
            r::RowExecution::Materialized,
        ),
        (
            "UNWIND [[0], 1] AS k MATCH (a:A {key:k}) RETURN a.key".to_owned(),
            r::RowExecution::Materialized,
        ),
        (
            "MATCH (a:A), (b:B) WHERE a.key = b.key RETURN count(*)".to_owned(),
            r::RowExecution::Materialized,
        ),
        (
            "MATCH (a:A)-[:R]->(b:B) RETURN count(*)".to_owned(),
            r::RowExecution::Materialized,
        ),
    ] {
        for batch_rows in [1, 512] {
            sweep(&db, &text, execution, batch_rows).await;
        }
    }
    db.close().await.unwrap();
}

/// The request admits the compiled program and then its execution state
/// before any operator runs. A budget holding only the program is rejected.
#[tokio::test]
async fn request_boundary_rejects_programs_whose_execution_state_does_not_fit() {
    let db = test_support::open_db("request-program-admission").await;
    let plan = r::plan(
        helix_cypher::compile("RETURN 1 AS x").unwrap(),
        &db.planner_context(context::ParamBindings::default()),
    )
    .unwrap();
    let retained = plan.program().retained_layout_bytes();
    for memory_bytes in [retained.saturating_sub(1), retained] {
        let error = Interpreter::new(&db, context::ParamBindings::default())
            .execute_rows(
                &plan,
                &BTreeMap::new(),
                Limits {
                    memory_bytes,
                    ..Limits::default()
                },
            )
            .await
            .unwrap_err();
        assert!(
            matches!(error, Error::Query(ref error) if error.detail == "MemoryLimit"),
            "{memory_bytes} bytes: {error}"
        );
    }
    let response = Interpreter::new(&db, context::ParamBindings::default())
        .execute_rows(&plan, &BTreeMap::new(), Limits::default())
        .await
        .unwrap();
    assert_eq!(response.rows, vec![vec![json!(1)]]);
    db.close().await.unwrap();
}
