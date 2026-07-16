use super::support;
use crate::{context, cost, exec, ir};
use helix_ast::batch as ast_batch;

#[test]
fn native_batch_boundary_accepts_foreach_with_native_body() {
    let batch = ast_batch::BatchQuery::Write(ast_batch::WriteBatch {
        entries: vec![ast_batch::BatchEntry::ForEach {
            param: "id".to_owned(),
            body: vec![support::query(support::node_source())],
        }],
        returns: Vec::new(),
    });

    let (entries, _) = support::lower_batch(&batch, &context::PlannerContext::default()).unwrap();
    assert!(matches!(
        entries,
        exec::SelectedExecutableBatchEntries::Single(
            exec::SelectedInitialExecutableBatchEntry::ForEach(batch)
        ) if batch.param().as_ref() == "id"
            && matches!(
                batch.body(),
                exec::SelectedExecutableBatchEntries::Single(
                    exec::SelectedInitialExecutableBatchEntry::Run(_)
                )
            )
    ));
}

#[test]
fn native_batch_boundary_accepts_mixed_query_and_foreach_batches() {
    let batch = ast_batch::BatchQuery::Write(ast_batch::WriteBatch {
        entries: vec![
            support::query(support::node_source()),
            ast_batch::BatchEntry::ForEach {
                param: "edge_id".to_owned(),
                body: vec![support::query(support::edge_source())],
            },
        ],
        returns: Vec::new(),
    });

    let (entries, _) = support::lower_batch(&batch, &context::PlannerContext::default()).unwrap();
    assert!(matches!(
        entries,
        exec::SelectedExecutableBatchEntries::WithFollowups {
            first: exec::SelectedInitialExecutableBatchEntry::Run(_),
            rest
        } if matches!(
            rest.as_ref(),
            [exec::SelectedFollowupExecutableBatchEntry::ForEach(batch)]
                if batch.param().as_ref() == "edge_id"
                    && matches!(
                        batch.body(),
                        exec::SelectedExecutableBatchEntries::Single(
                            exec::SelectedInitialExecutableBatchEntry::Run(_)
                        )
                    )
        )
    ));
}

#[test]
fn native_batch_boundary_optimizes_foreach_body_roots_in_parent_memo() {
    let batch = ast_batch::BatchQuery::Write(ast_batch::WriteBatch {
        entries: vec![
            support::query(support::node_source()),
            ast_batch::BatchEntry::ForEach {
                param: "edge_id".to_owned(),
                body: vec![support::query(support::edge_source())],
            },
        ],
        returns: Vec::new(),
    });

    let (entries, _) = support::lower_batch(&batch, &context::PlannerContext::default()).unwrap();
    let exec::SelectedExecutableBatchEntries::WithFollowups {
        first: exec::SelectedInitialExecutableBatchEntry::Run(first),
        rest,
    } = entries
    else {
        panic!("expected query followed by foreach");
    };
    let [exec::SelectedFollowupExecutableBatchEntry::ForEach(batch)] = rest.as_ref() else {
        panic!("expected foreach follow-up");
    };
    let exec::SelectedExecutableBatchEntries::Single(
        exec::SelectedInitialExecutableBatchEntry::Run(body),
    ) = batch.body()
    else {
        panic!("expected foreach body run");
    };

    assert_ne!(
        support::selected_group(&first.root),
        support::selected_group(&body.root),
        "foreach body roots should share the parent optimize_many memo instead of starting a fresh memo"
    );
}

#[test]
fn native_batch_boundary_charges_foreach_wrapper_in_selected_metrics() {
    let profile = cost::StorageCostProfile {
        foreach_overhead: cost::LatencyEstimate::micros(123),
        ..cost::StorageCostProfile::default()
    };
    let ctx = context::PlannerContext {
        storage: profile.clone(),
        ..context::PlannerContext::default()
    };
    let batch = ast_batch::BatchQuery::Read(ast_batch::ReadBatch {
        entries: vec![ast_batch::BatchEntry::ForEach {
            param: "id".to_owned(),
            body: vec![support::query(support::node_source())],
        }],
        returns: Vec::new(),
    });

    let (_, metrics) = support::lower_batch(&batch, &ctx).unwrap();

    assert_eq!(
        metrics.selected_cost,
        profile
            .foreach_wrapper()
            .serial(profile.range_scan(profile.default_unknown_scan_rows))
    );
}

#[test]
fn native_batch_boundary_accepts_foreach_then_query_followup() {
    let batch = ast_batch::BatchQuery::Write(ast_batch::WriteBatch {
        entries: vec![
            ast_batch::BatchEntry::ForEach {
                param: "node_id".to_owned(),
                body: vec![support::query(support::node_source())],
            },
            support::conditional_query(
                support::edge_source(),
                ast_batch::BatchCondition::PrevNotEmpty,
            ),
        ],
        returns: Vec::new(),
    });

    let (entries, _) = support::lower_batch(&batch, &context::PlannerContext::default()).unwrap();
    assert!(matches!(
        entries,
        exec::SelectedExecutableBatchEntries::WithFollowups {
            first: exec::SelectedInitialExecutableBatchEntry::ForEach(_),
            rest
        } if matches!(
            rest.as_ref(),
            [exec::SelectedFollowupExecutableBatchEntry::Run(entry)]
                if matches!(entry.condition, ir::RunConditionPlan::If(ir::BatchConditionPlan::PrevNotEmpty))
        )
    ));
}
