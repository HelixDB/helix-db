//! A correlated probe uses the selected native equality kernel and bounded
//! cursors. Binding and predicate validation remain owned by the MATCH contract.
use super::{push_row, ExecutionContext, Limits, Result, RowBuffer};
use futures::StreamExt;
use helix_planner::{exec, ir, relational as r};

impl ExecutionContext<'_> {
    pub(super) async fn index_lookup_rows(
        &mut self,
        row: &r::Row,
        lookup: &r::PatternLookup,
        literal: ir::SecondaryIndexLiteral,
        output: &mut RowBuffer,
        limits: Limits,
    ) -> Result<()> {
        let access = exec::ExecAccessPlan::Node(exec::ExecNodeAccessPlan::exact_equality(
            lookup.index.clone(),
            lookup.key.clone(),
            ir::IndexValue::Literal(literal),
        ));
        let operation = exec::ExecOp::Access {
            plan: Box::new(access),
        };
        self.flush_required_mutations(super::super::mutation::visibility::required_for(&operation))
            .await?;
        let cursor = self
            .node_cursor(&operation)
            .await?
            .expect("nonnull literal equality has a bitmap, unique, or empty cursor");
        let batches = self.node_id_batches(cursor, 1, r::Slot(0), limits);
        futures::pin_mut!(batches);
        while let Some(batch) = batches.next().await {
            for found in batch? {
                let mut result = row.clone();
                result[lookup.slot.0 as usize] = found[0].clone();
                push_row(output, result, limits)?;
            }
        }
        Ok(())
    }
}
