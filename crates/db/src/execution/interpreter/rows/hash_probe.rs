//! Resumable equality probes. One probe row may match arbitrarily many build
//! IDs; its bucket offset survives output-batch boundaries without retaining the
//! joined relation. Graph hydration remains bounded by the input batch limit.
use super::{
    joins::HashJoinTable, memory, ExecutionContext, GraphBatch, Limits, Result, RowBuffer,
};
use helix_planner::relational as r;
use r::GraphValues;
use std::{collections::BTreeMap, sync::Arc};

pub(super) struct ProbeCursor {
    rows: memory::Rows,
    graph: GraphBatch,
    hydrated_end: usize,
    parent: usize,
    duplicate: usize,
    pending_error: Option<crate::cypher::Error>,
}
impl ProbeCursor {
    pub(super) fn new(rows: memory::Rows) -> Self {
        Self {
            rows,
            graph: GraphBatch::default(),
            hydrated_end: 0,
            parent: 0,
            duplicate: 0,
            pending_error: None,
        }
    }

    pub(super) async fn next_batch(
        &mut self,
        table: &HashJoinTable,
        context: &ExecutionContext<'_>,
        slot: r::Slot,
        probe: r::Slot,
        property: &str,
        limits: Limits,
    ) -> Result<Option<memory::Rows>> {
        self.pending_error.take().map_or(Ok(()), Err)?;
        let mut output = RowBuffer::new(context.row_budget())?;
        let gathered: Result<()> = async {
            while output.len() < limits.batch_rows && self.parent < self.rows.len() {
                context.check_execution_deadline()?;
                if self.parent == self.hydrated_end {
                    let count = (self.rows.len() - self.parent).min(limits.batch_rows);
                    let _demand_memory = context.row_budget().reserve(
                        r::allocation::btree_bytes::<r::Slot, r::PropertyDemand>(1)
                            .saturating_add(r::allocation::btree_bytes::<String, ()>(1))
                            .saturating_add(property.len()),
                    )?;
                    let demand = BTreeMap::from([(
                        probe,
                        r::PropertyDemand::Keys([property.to_owned()].into_iter().collect()),
                    )]);
                    self.graph = GraphBatch::default();
                    self.graph = context
                        .graph_batch_required(&self.rows[self.parent..self.parent + count], &demand)
                        .await?;
                    self.hydrated_end = self.parent + count;
                }
                let row = &self.rows[self.parent];
                let r::Value::Entity(entity) = row[probe.0 as usize] else {
                    self.parent += 1;
                    continue;
                };
                let value = self.graph.property(entity, property)?;
                if value.equals(value) != Some(true) {
                    self.parent += 1;
                    continue;
                }
                let _key_memory = context.row_budget().reserve(value.allocated_bytes())?;
                let Some(ids) = table.bucket(&r::GroupingKey::new(value.clone())?) else {
                    self.parent += 1;
                    continue;
                };
                let count = (ids.len() - self.duplicate).min(limits.batch_rows - output.len());
                for id in &ids[self.duplicate..self.duplicate + count] {
                    output.push_replacing(row, slot, r::Value::Entity(r::Entity::Node(*id)))?;
                }
                self.duplicate += count;
                if self.duplicate == ids.len() {
                    self.parent += 1;
                    self.duplicate = 0;
                }
            }
            Ok(())
        }
        .await;
        if let Err(error) = gathered {
            self.pending_error = Some(error);
        }
        // Emitted rows own their copied bindings. Do not retain exhausted
        // hydration or input while downstream operators hydrate that output.
        // A queued failure likewise needs no remaining probe state.
        if self.pending_error.is_some() || self.parent == self.rows.len() {
            self.graph = GraphBatch::default();
            self.rows.data = Vec::new();
            self.rows.refresh()?;
            self.parent = 0;
            self.hydrated_end = 0;
        } else if self.parent == self.hydrated_end {
            self.graph = GraphBatch::default();
        }
        if output.len() > 0 {
            return Ok(Some(output.finish()));
        }
        self.pending_error.take().map_or(Ok(()), Err)?;
        Ok(None)
    }
}

impl HashJoinTable {
    pub(super) fn probe_batches<'a>(
        self: Arc<Self>,
        context: &'a ExecutionContext<'_>,
        rows: memory::Rows,
        slot: r::Slot,
        probe: r::Slot,
        property: &'a str,
        limits: Limits,
    ) -> impl futures::Stream<Item = Result<memory::Rows>> + Send + 'a {
        futures::stream::try_unfold(ProbeCursor::new(rows), move |mut cursor| {
            let table = Arc::clone(&self);
            let poll = async move {
                let batch = cursor
                    .next_batch(&table, context, slot, probe, property, limits)
                    .await?;
                Ok(batch.map(|batch| (batch, cursor)))
            };
            match context.row_budget().admitted_future(poll) {
                Ok(poll) => futures::future::Either::Left(poll),
                Err(error) => futures::future::Either::Right(async { Err(error.into()) }),
            }
        })
    }
}
