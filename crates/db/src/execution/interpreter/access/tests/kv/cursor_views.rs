//! Resumable reads retain request snapshots, tenant isolation and demand.
use super::*;
use crate::encoding::v2::keys;
use crate::execution::interpreter::{access::kv as raw, ElementRef, ExecutionContext};
use bytes::Bytes;

fn create_node() -> exec::ExecStep {
    test_support::step(
        1,
        Vec::new(),
        exec::ExecOp::Mutation {
            plan: exec::ExecMutationPlan::AddNodeSource {
                label: test_support::name("User"),
                properties: test_support::assignments(vec![(
                    "name",
                    PropertyValue::from("cursor"),
                )]),
            },
        },
    )
}

fn node_id(value: ExecutionValue) -> u64 {
    let ExecutionValue::Stream(rows) = value else {
        panic!("expected a node stream")
    };
    assert_eq!(rows.len(), 1);
    let Some(ElementRef::Node(id)) = rows[0].current else {
        panic!("expected a node")
    };
    id
}

#[tokio::test]
async fn resumable_cursors_keep_scope_snapshot_and_transaction_visibility() {
    let config = test_support::in_memory_config("resumable-cursor-views");
    let writer = test_support::open_db_with_config(config.clone()).await;
    let scopes = [
        keys::scope::DataScope::LegacyUnscoped,
        keys::scope::DataScope::Tenant(
            keys::scope::TenantId::from_ulid_str("0000000000000000000000000A").unwrap(),
        ),
        keys::scope::DataScope::Tenant(
            keys::scope::TenantId::from_ulid_str("0000000000000000000000000B").unwrap(),
        ),
    ];
    let plan = test_support::executable(ir::PlanKind::Write, vec![create_node()], 1);
    let mut scoped_ids = Vec::new();
    for scope in scopes {
        let mut ids = Vec::new();
        for _ in 0..3 {
            ids.push(node_id(
                writer
                    .execute_scoped(&plan, context::ParamBindings::default(), scope)
                    .await
                    .unwrap()
                    .last
                    .unwrap(),
            ));
        }
        scoped_ids.push((scope, ids));
    }
    writer.flush_writer().await.unwrap();
    let reader = test_support::open_reader_with_config(config).await;
    let keyspace = exec::ElementKeyspace::NodeProperty;
    let (start, end) = raw::element_range_bounds(
        keyspace,
        &exec::KvKeyBound::Unbounded,
        &exec::KvKeyBound::Unbounded,
    );
    let prefix = Bytes::from(raw::element_prefix(keyspace));
    for db in [&writer, &reader] {
        for (scope, ids) in &scoped_ids {
            // Direct test adapters and production request views must agree.
            for request_view in [false, true] {
                let mut ctx =
                    ExecutionContext::new_scoped(db, context::ParamBindings::default(), *scope);
                let budget = crate::query_resources::Budget::new(1024 * 1024);
                ctx.row_memory = Some(budget.clone());
                if request_view {
                    ctx.enable_request_read_view().await.unwrap();
                }
                let expected = ids
                    .iter()
                    .map(|id| {
                        ctx.storage_key(keys::DataKeyKind::NodeProperty(
                            keys::NodePropertyKey::new(*id),
                        ))
                    })
                    .collect::<Vec<_>>();
                let mut range = ctx
                    .open_raw_range(start.clone(), end.clone())
                    .await
                    .unwrap();
                let mut prefixed = ctx.open_raw_prefix(prefix.clone()).await.unwrap();
                for key in expected {
                    assert_eq!(range.next().await.unwrap().unwrap().key, key);
                    assert_eq!(prefixed.next().await.unwrap().unwrap().key, key);
                }
                assert!(range.next().await.unwrap().is_none());
                assert!(prefixed.next().await.unwrap().is_none());
                assert_eq!(budget.reads().scans, 2);
                if request_view {
                    ctx.close_request_read_view().unwrap();
                }
            }
        }
    }
    let (scope, ids) = &scoped_ids[1];
    let mut snapshot =
        ExecutionContext::new_scoped(&writer, context::ParamBindings::default(), *scope);
    snapshot.enable_request_read_view().await.unwrap();
    let committed = node_id(
        writer
            .execute_scoped(&plan, context::ParamBindings::default(), *scope)
            .await
            .unwrap()
            .last
            .unwrap(),
    );
    let mut transaction =
        ExecutionContext::new_scoped(&writer, context::ParamBindings::default(), *scope);
    transaction.enable_request_write_scope().await.unwrap();
    let staged = node_id(transaction.execute_step(&create_node()).await.unwrap());
    for (ctx, expected) in [
        (&snapshot, ids.clone()),
        (
            &transaction,
            ids.iter().copied().chain([committed, staged]).collect(),
        ),
    ] {
        let mut range = ctx
            .open_raw_range(start.clone(), end.clone())
            .await
            .unwrap();
        let mut prefixed = ctx.open_raw_prefix(prefix.clone()).await.unwrap();
        for id in expected {
            let key = ctx.storage_key(keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(
                id,
            )));
            assert_eq!(range.next().await.unwrap().unwrap().key, key);
            assert_eq!(prefixed.next().await.unwrap().unwrap().key, key);
        }
        assert!(range.next().await.unwrap().is_none());
        assert!(prefixed.next().await.unwrap().is_none());
    }
    transaction.abort_request_write_scope();
    snapshot.close_request_read_view().unwrap();
    let result = writer
        .execute_scoped(
            &kv_read_ids_plan(exec::KvReadPlan::Get {
                key: keyspace.point_key(staged),
            }),
            context::ParamBindings::default(),
            *scope,
        )
        .await
        .unwrap();
    assert_eq!(result.last, Some(ExecutionValue::Scalars(Vec::new())));
    reader.close().await.unwrap();
    writer.close().await.unwrap();
}

#[tokio::test]
async fn prefix_and_range_pull_scans_stop_at_demand_and_count_consumed_records() {
    let db = test_support::open_db("resumable-scan-demand").await;
    let mut ids = Vec::new();
    for _ in 0..4 {
        ids.push(test_support::add_user(&db, "cursor").await);
    }
    let keyspace = exec::ElementKeyspace::NodeProperty;
    let first_byte = keyspace.point_key(ids[0]).bytes()[0];
    assert!(ids
        .iter()
        .all(|id| keyspace.point_key(*id).bytes()[0] == first_byte));
    for read in [
        exec::KvReadPlan::PrefixScan {
            keyspace,
            prefix: ir::AtLeast::from_one(first_byte),
            limit: properties::PositiveUsize::new(2),
        },
        exec::KvReadPlan::RangeScan {
            keyspace,
            start: exec::KvKeyBound::Unbounded,
            end: exec::KvKeyBound::Unbounded,
            limit: properties::PositiveUsize::new(2),
        },
    ] {
        let plan = test_support::executable(
            ir::PlanKind::Read,
            vec![
                test_support::step(1, Vec::new(), exec::ExecOp::KvRead(read)),
                test_support::step(
                    2,
                    vec![exec::ExecStepId::new(1).unwrap()],
                    exec::ExecOp::Limit {
                        count: ir::StreamBoundPlan::Literal(1),
                    },
                ),
                test_support::step(
                    3,
                    vec![exec::ExecStepId::new(2).unwrap()],
                    exec::ExecOp::Project {
                        projection: ir::ProjectionPlan::Id,
                    },
                ),
            ],
            3,
        );
        assert_eq!(plan.execution_program().regions().count(), 1);
        let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
        let budget = crate::query_resources::Budget::new(1024 * 1024);
        ctx.row_memory = Some(budget.clone());
        ctx.enable_request_read_view().await.unwrap();
        ctx.execute_steps(
            plan.steps(),
            plan.execution_order(),
            plan.root(),
            plan.execution_program(),
        )
        .await
        .unwrap();
        assert_eq!(
            ctx.finish(plan.root(), &exec::ExecutableReturns::None)
                .unwrap()
                .last,
            Some(ExecutionValue::Scalars(vec![ExecutionScalar::NodeId(
                ids[0]
            )]))
        );
        assert_eq!(ctx.pull_work.snapshot().source_visits, 1);
        assert_eq!(budget.reads().scans, 1);
        assert_eq!(budget.reads().scan_rows, 1);
        ctx.close_request_read_view().unwrap();
    }
    db.close().await.unwrap();
}
