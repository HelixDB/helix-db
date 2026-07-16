use super::support;
use crate::{context, exec, ir};
use helix_ast::batch as ast_batch;

#[test]
fn native_batch_boundary_accepts_multi_query_batches() {
    let multi = ast_batch::BatchQuery::Read(ast_batch::ReadBatch {
        entries: vec![
            support::query(support::node_source()),
            support::conditional_query(
                support::edge_source(),
                ast_batch::BatchCondition::PrevNotEmpty,
            ),
        ],
        returns: Vec::new(),
    });

    let (entries, metrics) =
        support::lower_batch(&multi, &context::PlannerContext::default()).unwrap();
    assert!(metrics.memo_groups > 0);
    assert!(matches!(
        entries,
        exec::SelectedExecutableBatchEntries::WithFollowups {
            first: exec::SelectedInitialExecutableBatchEntry::Run(_),
            rest
        } if matches!(
            rest.as_ref(),
            [exec::SelectedFollowupExecutableBatchEntry::Run(entry)]
                if matches!(entry.condition, ir::RunConditionPlan::If(ir::BatchConditionPlan::PrevNotEmpty))
        )
    ));
}
