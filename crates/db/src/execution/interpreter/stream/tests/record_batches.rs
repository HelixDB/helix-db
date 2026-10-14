//! Batched stored-record reads for native projection, ordering and aggregation.
//!
//! Each operator loads a distinct element's record at most once per record
//! batch, with one multi-get, and never loads a record its per-row evaluation
//! would skip. Projection output is compared with single-row executions, so
//! batching cannot change what a row projects.

use super::super::super::{ExecutionContext, RowVirtualProperties};
use super::super::RECORD_BATCH_ROWS;
use super::support::*;

/// Input rows cycling over three nodes; they span [`BATCHES`] record batches.
const ROWS: usize = 700;
const BATCHES: usize = 3;

struct Fixture {
    db: crate::HelixDB,
    /// `score` 3.
    ada: u64,
    /// `score` 1.
    bob: u64,
    /// No `score`.
    carol: u64,
    /// `ada -> bob`.
    edge: u64,
}

async fn fixture(database: &str) -> Fixture {
    let db = test_support::open_db(database).await;
    let ada = test_support::add_node_with_properties(
        &db,
        "User",
        vec![
            ("name", PropertyValue::from("ada")),
            ("score", PropertyValue::I64(3)),
        ],
    )
    .await;
    let bob = test_support::add_node_with_properties(
        &db,
        "User",
        vec![
            ("name", PropertyValue::from("bob")),
            ("score", PropertyValue::I64(1)),
        ],
    )
    .await;
    let carol = test_support::add_node_with_properties(
        &db,
        "User",
        vec![("name", PropertyValue::from("carol"))],
    )
    .await;
    let edge = test_support::add_edge_with_properties(
        &db,
        ada,
        bob,
        "KNOWS",
        vec![("weight", PropertyValue::I64(7))],
    )
    .await;
    Fixture {
        db,
        ada,
        bob,
        carol,
        edge,
    }
}

fn cycled(elements: &[ElementRef], count: usize) -> Vec<ExecutionRow> {
    elements
        .iter()
        .cycle()
        .take(count)
        .map(|element| ExecutionRow::current(element.clone()))
        .collect()
}

struct Run {
    value: crate::Result<ExecutionValue>,
    property_gets: usize,
    endpoint_gets: usize,
    raw_gets: usize,
    multi_get_keys: usize,
}

async fn run(db: &crate::HelixDB, op: &exec::ExecOp, rows: Vec<ExecutionRow>) -> Run {
    let mut ctx = ExecutionContext::new(db, context::ParamBindings::default());
    ctx.enable_request_read_view().await.unwrap();
    let value = ctx.execute_op(op, ExecutionValue::Stream(rows)).await;
    let reads = ctx.projection_read_snapshot();
    let work = ctx.pull_work.snapshot();
    ctx.close_request_read_view().unwrap();
    Run {
        value,
        property_gets: reads.property_gets,
        endpoint_gets: reads.endpoint_gets,
        raw_gets: work.raw_gets,
        multi_get_keys: work.multi_get_keys,
    }
}

fn project(projection: ir::ProjectionPlan) -> exec::ExecOp {
    exec::ExecOp::Project { projection }
}

fn projection_items(items: Vec<ir::ProjectionItem>) -> ir::ProjectionItems {
    ir::ProjectionItems::new(ir::AtLeast::try_from_vec(items).unwrap()).unwrap()
}

#[tokio::test]
async fn native_row_operators_read_each_distinct_record_once_per_batch() {
    let fixture = fixture("native-record-batches").await;
    let nodes = [fixture.ada, fixture.bob, fixture.carol].map(ElementRef::Node);
    assert_eq!(ROWS.div_ceil(RECORD_BATCH_ROWS), BATCHES);

    let projections = [
        ir::ProjectionPlan::Values(property_names(vec!["name", "score"])),
        ir::ProjectionPlan::ValueMap(ir::PropertySelection::All),
        ir::ProjectionPlan::ValueMap(ir::PropertySelection::Selected(property_names(vec![
            "score",
        ]))),
        ir::ProjectionPlan::Project(projection_items(vec![
            ir::ProjectionItem::Property {
                source: name("name"),
                alias: name("display"),
            },
            ir::ProjectionItem::Expr {
                alias: name("score"),
                expr: ir::ExprPlan::new(Expr::prop("score")).unwrap(),
            },
        ])),
        ir::ProjectionPlan::ProjectBindings {
            projections: binding_projection_items(vec![ir::BindingProjectionPlan::Property {
                target: ir::BindingTargetPlan::Current,
                source: name("name"),
                alias: name("display"),
            }]),
            dedup: ir::ProjectionDedupMode::All,
        },
        ir::ProjectionPlan::Label,
    ];
    for projection in projections {
        let op = project(projection);
        let batched = run(&fixture.db, &op, cycled(&nodes, ROWS)).await;
        assert_eq!(
            (
                batched.property_gets,
                batched.multi_get_keys,
                batched.raw_gets
            ),
            (3 * BATCHES, 3 * BATCHES, 0),
            "{op:?}"
        );
        let mut single = Vec::new();
        for element in &nodes {
            let ExecutionValue::Scalars(scalars) =
                run(&fixture.db, &op, cycled(std::slice::from_ref(element), 1))
                    .await
                    .value
                    .unwrap()
            else {
                panic!("projection returns scalars");
            };
            single.push(scalars);
        }
        let expected = single
            .iter()
            .cycle()
            .take(ROWS)
            .flatten()
            .cloned()
            .collect();
        assert_eq!(
            batched.value.unwrap(),
            ExecutionValue::Scalars(expected),
            "{op:?}"
        );
    }

    let order = run(
        &fixture.db,
        &exec::ExecOp::Order {
            plan: ir::OrderPlan::ExplicitSort(order_keys("score", Order::Asc)),
        },
        cycled(&nodes, ROWS),
    )
    .await;
    assert_eq!(
        (order.property_gets, order.multi_get_keys, order.raw_gets),
        (3 * BATCHES, 3 * BATCHES, 0)
    );
    let ExecutionValue::Stream(ordered) = order.value.unwrap() else {
        panic!("order returns rows");
    };
    let mut expected = cycled(&[ElementRef::Node(fixture.carol)], 233);
    expected.extend(cycled(&[ElementRef::Node(fixture.bob)], 233));
    expected.extend(cycled(&[ElementRef::Node(fixture.ada)], 234));
    assert_eq!(ordered, expected);

    let count = run(
        &fixture.db,
        &exec::ExecOp::Aggregate {
            aggregate: ir::AggregatePlan::GroupCount(name("score")),
        },
        cycled(&nodes, ROWS),
    )
    .await;
    assert_eq!(
        (count.property_gets, count.multi_get_keys, count.raw_gets),
        (3 * BATCHES, 3 * BATCHES, 0)
    );
    let group = |score: DbPropertyValue, count: i64| {
        ExecutionScalar::Object(BTreeMap::from([
            ("score".to_string(), score),
            ("count".to_string(), DbPropertyValue::I64(count)),
        ]))
    };
    assert_eq!(
        count.value.unwrap(),
        ExecutionValue::Scalars(vec![
            group(DbPropertyValue::Null, 233),
            group(DbPropertyValue::I64(1), 233),
            group(DbPropertyValue::I64(3), 234),
        ])
    );

    let sum = run(
        &fixture.db,
        &exec::ExecOp::Aggregate {
            aggregate: ir::AggregatePlan::AggregateBy {
                function: AggregateFunction::Sum,
                property: name("score"),
            },
        },
        cycled(&nodes, ROWS),
    )
    .await;
    assert_eq!(
        (sum.property_gets, sum.multi_get_keys, sum.raw_gets),
        (3 * BATCHES, 3 * BATCHES, 0)
    );
    assert_eq!(
        sum.value.unwrap(),
        ExecutionValue::Scalars(vec![ExecutionScalar::Object(BTreeMap::from([(
            "score_Sum".to_string(),
            DbPropertyValue::F64(3.0 * 234.0 + 233.0),
        )]))])
    );
    fixture.db.close().await.unwrap();
}

#[tokio::test]
async fn record_prefetch_never_reads_what_rows_skip() {
    let fixture = fixture("native-record-batch-skips").await;
    let nodes = [fixture.ada, fixture.bob, fixture.carol].map(ElementRef::Node);

    // Row-local values and virtual properties need no stored record.
    let ids = run(
        &fixture.db,
        &project(ir::ProjectionPlan::Values(property_names(vec!["$id"]))),
        cycled(&nodes, ROWS),
    )
    .await;
    assert_eq!((ids.property_gets, ids.multi_get_keys), (0, 0));
    let shadowed = nodes
        .iter()
        .map(|element| {
            ExecutionRow::current_with_virtual_properties(
                element.clone(),
                RowVirtualProperties::from_one(name("score"), DbPropertyValue::I64(9)),
            )
        })
        .collect::<Vec<_>>();
    let virtual_scores = run(
        &fixture.db,
        &project(ir::ProjectionPlan::Values(property_names(vec!["score"]))),
        shadowed,
    )
    .await;
    assert_eq!(
        (virtual_scores.property_gets, virtual_scores.multi_get_keys),
        (0, 0)
    );

    // Coalesce always reads only its first bound reference.
    let coalesce = run(
        &fixture.db,
        &project(ir::ProjectionPlan::ProjectBindings {
            projections: binding_projection_items(vec![ir::BindingProjectionPlan::Coalesce {
                refs: binding_refs(vec![
                    ir::BindingValueRefPlan {
                        target: ir::BindingTargetPlan::Binding(name("missing")),
                        source: name("name"),
                    },
                    ir::BindingValueRefPlan {
                        target: ir::BindingTargetPlan::Current,
                        source: name("name"),
                    },
                ]),
                alias: name("display"),
            }]),
            dedup: ir::ProjectionDedupMode::All,
        }),
        cycled(&nodes, ROWS),
    )
    .await;
    assert_eq!(
        (
            coalesce.property_gets,
            coalesce.multi_get_keys,
            coalesce.raw_gets
        ),
        (3 * BATCHES, 3 * BATCHES, 0)
    );

    // An empty row fails an aggregate before later rows are read.
    let mut failing = cycled(&nodes[..1], 1);
    failing.push(ExecutionRow::empty());
    failing.extend(cycled(&nodes[1..], 2));
    let failed = run(
        &fixture.db,
        &exec::ExecOp::Aggregate {
            aggregate: ir::AggregatePlan::GroupCount(name("score")),
        },
        failing,
    )
    .await;
    assert!(failed.value.is_err());
    assert_eq!((failed.property_gets, failed.multi_get_keys), (1, 1));

    // Edge rows batch their endpoints as well as their records.
    let edges = [ElementRef::Edge(fixture.edge)];
    let endpoints = run(
        &fixture.db,
        &project(ir::ProjectionPlan::Values(property_names(vec![
            "$from", "$to",
        ]))),
        cycled(&edges, ROWS),
    )
    .await;
    assert_eq!(
        (
            endpoints.endpoint_gets,
            endpoints.property_gets,
            endpoints.multi_get_keys,
            endpoints.raw_gets
        ),
        (BATCHES, 0, BATCHES, 0)
    );
    let edge_properties = run(
        &fixture.db,
        &project(ir::ProjectionPlan::EdgeProperties),
        cycled(&edges, ROWS),
    )
    .await;
    assert_eq!(
        (
            edge_properties.endpoint_gets,
            edge_properties.property_gets,
            edge_properties.multi_get_keys,
            edge_properties.raw_gets
        ),
        (BATCHES, BATCHES, 2 * BATCHES, 0)
    );
    let ExecutionValue::Scalars(objects) = edge_properties.value.unwrap() else {
        panic!("edge properties return scalars");
    };
    assert_eq!(objects.len(), ROWS);
    assert_eq!(
        objects[0],
        ExecutionScalar::Object(BTreeMap::from([
            ("weight".to_string(), DbPropertyValue::I64(7)),
            (
                "$label".to_string(),
                DbPropertyValue::String("KNOWS".to_string())
            ),
            (
                "$id".to_string(),
                DbPropertyValue::I64(fixture.edge.try_into().unwrap())
            ),
            (
                "$from".to_string(),
                DbPropertyValue::I64(fixture.ada.try_into().unwrap())
            ),
            (
                "$to".to_string(),
                DbPropertyValue::I64(fixture.bob.try_into().unwrap())
            ),
        ]))
    );
    fixture.db.close().await.unwrap();
}
