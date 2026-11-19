//! One statement reads one clock: the instant planning folds into index
//! bounds is the one every `datetime()` and `timestamp()` of the statement
//! evaluates to, in residual comparisons, projections and writes.
use super::*;
use crate::execution::interpreter::{test_support, Interpreter};
use helix_planner::context;
use serde_json::json;

/// 2001-09-09T01:46:40.000Z, far from any wall clock a test runs at.
const CLOCK: i64 = 1_000_000_000_000;

async fn run(db: &crate::HelixDB, plan: &r::RowPlan) -> serde_json::Value {
    let response = Interpreter::new(db, context::ParamBindings::default())
        .execute_rows(plan, &BTreeMap::new(), Limits::default())
        .await
        .unwrap();
    serde_json::to_value(response.rows).unwrap()
}

/// The first access step of the statement's first MATCH.
fn access(plan: &r::RowPlan) -> String {
    let [step, ..] = plan.matches()[&0].sources[0].access.steps() else {
        panic!("an access step");
    };
    format!("{:?}", step.op)
        .trim_start_matches("Access { plan: Node(")
        .split(|c: char| !c.is_alphanumeric())
        .next()
        .unwrap()
        .to_owned()
}

/// Writes, projections and the residual comparisons of a range-index plan
/// all read the clock the plan was folded from, so the index plan and a scan
/// at the same clock return the same rows, right at the clock's boundary.
#[tokio::test]
async fn one_statement_evaluates_the_clock_its_plan_was_folded_from() {
    let db = test_support::open_db_with_config(
        test_support::in_memory_config("cypher-statement-clock").with_range_index("E", "at"),
    )
    .await;
    let planned = |text: &str, clock: i64| {
        let ctx = context::PlannerContext {
            statement_time: clock,
            ..db.planner_context(context::ParamBindings::default())
        };
        let query = helix_cypher::compile(text).unwrap();
        (
            r::plan(query.clone(), &ctx).unwrap(),
            r::RowPlan::reference(query, clock).unwrap(),
        )
    };
    // One millisecond before, at and after the clock, written by one
    // statement that also stores the clock it reads.
    let (write, _) = planned(
        "UNWIND range(1, 3) AS i \
         CREATE (e:E {i: i, at: datetime({epochMillis: timestamp() + i - 2}), \
         seen: datetime(), ms: timestamp()}) \
         RETURN count(*), min(e.ms), max(e.ms), timestamp()",
        CLOCK,
    );
    assert_eq!(write.statement_time(), CLOCK);
    assert_eq!(run(&db, &write).await, json!([[3, CLOCK, CLOCK, CLOCK]]));
    let (stored, _) = planned(
        "MATCH (e:E) RETURN e.i, e.at = e.seen, e.seen = datetime({epochMillis: e.ms}) \
         ORDER BY e.i",
        CLOCK,
    );
    assert_eq!(
        run(&db, &stored).await,
        json!([[1, false, true], [2, true, true], [3, false, true]])
    );
    for (text, clock, rows) in [
        (
            "MATCH (e:E) WHERE e.at < datetime() RETURN e.i ORDER BY e.i",
            CLOCK,
            json!([[1]]),
        ),
        (
            "MATCH (e:E) WHERE e.at <= datetime() RETURN e.i ORDER BY e.i",
            CLOCK,
            json!([[1], [2]]),
        ),
        (
            "MATCH (e:E) WHERE e.at > datetime({epochMillis: timestamp() - 1}) \
             RETURN e.i ORDER BY e.i",
            CLOCK,
            json!([[2], [3]]),
        ),
        (
            "MATCH (e:E) WHERE e.at = datetime() RETURN e.i, timestamp(), e.at = datetime()",
            CLOCK,
            json!([[2, CLOCK, true]]),
        ),
        // A later clock moves the bound and the residual comparison together.
        (
            "MATCH (e:E) WHERE e.at < datetime() RETURN e.i ORDER BY e.i",
            CLOCK + 2,
            json!([[1], [2], [3]]),
        ),
        (
            "MATCH (e:E) WHERE e.at >= datetime({epochMillis: timestamp() - 1}) \
             RETURN e.i, timestamp() ORDER BY e.i",
            CLOCK + 2,
            json!([[3, CLOCK + 2]]),
        ),
    ] {
        let (plan, reference) = planned(text, clock);
        assert_eq!(access(&plan), "RangeIndex", "{text}");
        assert_eq!(access(&reference), "AllScan", "{text}");
        assert_eq!(run(&db, &plan).await, rows, "{text} at {clock}");
        assert_eq!(run(&db, &reference).await, rows, "{text} at {clock}");
    }
    db.close().await.unwrap();
}
