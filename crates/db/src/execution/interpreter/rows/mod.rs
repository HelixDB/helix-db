//! Common relational operators inside the existing request-owned interpreter.
mod aggregation;
mod expansion;
mod graph;
mod joins;
mod lookup;
mod matches;
pub(in crate::execution::interpreter) mod memory;
mod mutations;
mod projection;
mod projection_chain;
mod scan;
mod streaming;
mod top_k;

use super::{ExecutionContext, ExecutionValue, Interpreter};
use crate::cypher::{Error, Limits, Response, Result};
use graph::GraphBatch;
use helix_planner::relational as r;
use memory::Rows;
use std::collections::BTreeMap;

enum ConsumedProjection {
    Aggregate(usize),
    Complete(usize),
}

impl Interpreter<'_> {
    pub(crate) async fn execute_rows(
        mut self,
        plan: &r::RowPlan,
        parameters: &BTreeMap<String, r::Value>,
        limits: Limits,
    ) -> Result<Response> {
        self.ctx.check_execution_deadline()?;
        self.ctx.row_memory = Some(memory::Budget::new(limits.memory_bytes));
        match plan.query().effect() {
            r::Effect::Read => self.ctx.enable_request_read_view().await?,
            r::Effect::Write => {
                self.ensure_writer()?;
                self.ctx.enable_request_write_scope().await?;
            }
        }
        let result = Box::pin(self.ctx.row_program(plan, parameters, limits)).await;
        match result {
            Err(error) => {
                self.ctx.abort_request_write_scope();
                Err(error)
            }
            Ok(response) => {
                if let Err(error) = self.ctx.check_execution_deadline() {
                    self.ctx.abort_request_write_scope();
                    return Err(error.into());
                }
                match plan.query().effect() {
                    r::Effect::Read => self.ctx.validate_request_read_view()?,
                    r::Effect::Write => self.ctx.commit_request_write_scope().await?,
                }
                Ok(response)
            }
        }
    }
}

impl ExecutionContext<'_> {
    fn row_budget(&self) -> &memory::Budget {
        self.row_memory
            .as_ref()
            .expect("row memory is initialized at the request boundary")
    }
    async fn row_program(
        &mut self,
        plan: &r::RowPlan,
        parameters: &BTreeMap<String, r::Value>,
        limits: Limits,
    ) -> Result<Response> {
        let width = plan.query().bindings().len();
        if width
            .saturating_mul(size_of::<r::Value>())
            .saturating_add(size_of::<r::Row>())
            > limits.memory_bytes
        {
            return Err(resource(
                "MemoryLimit",
                "row schema exceeds the query memory budget",
            ));
        }
        let mut rows = Rows::new(vec![vec![r::Value::Null; width]], self.row_budget())?;
        let mut preprojected = None;
        for (index, operator) in plan.query().operators().iter().enumerate() {
            self.check_execution_deadline()?;
            if matches!(preprojected,Some(ConsumedProjection::Complete(completed)) if index<=completed)
                || matches!(preprojected,Some(ConsumedProjection::Aggregate(aggregated)) if index<aggregated)
            {
                continue;
            }
            let empty = GraphBatch::default();
            let evaluation = self.evaluate(&[], parameters, &empty, limits);
            let demand = plan
                .input_window(index)
                .map(|window| window.demand(|expression| evaluation.eval(expression)))
                .transpose()?
                .unwrap_or(usize::MAX);
            rows = match operator {
                r::Operator::Match {
                    pattern,
                    optional,
                    predicate,
                } => {
                    if let Some(consumer) = plan.batch_consumer(index)
                        && let Some(match_plan) = plan.matches().get(&index)
                        && let Some(r::MatchStep::Scan(start)) = match_plan.steps.first()
                        && match_plan.steps[1..]
                            .iter()
                            .all(|step| matches!(step, r::MatchStep::Expand { .. }))
                        && let Some(source) = match_plan
                            .sources
                            .iter()
                            .find(|source| source.slot == *start)
                        && let [step] = source.access.steps()
                        && let Some(cursor) = self.node_cursor(&step.op).await?
                    {
                        // Only the initial independent MATCH is admitted. The
                        // consumer finishes before any subsequent mutation runs.
                        drop(rows);
                        let producer_limits = Limits {
                            batch_rows: limits.batch_rows.min(demand.max(1)),
                            ..limits
                        };
                        let batches = self.graph_match_batches(
                            cursor,
                            width,
                            matches::Match {
                                pattern,
                                optional: *optional,
                                predicate: predicate.as_ref(),
                                demand: usize::MAX,
                            },
                            match_plan,
                            parameters,
                            producer_limits,
                        );
                        let (result, consumed) =
                            Box::pin(self.consume_batches(
                                batches, plan, index, consumer, parameters, limits,
                            ))
                            .await?;
                        rows = result;
                        preprojected = Some(consumed);
                        continue;
                    }
                    self.match_rows(
                        rows,
                        matches::Match {
                            pattern,
                            optional: *optional,
                            predicate: predicate.as_ref(),
                            demand,
                        },
                        plan.matches().get(&index).expect("validated match plan"),
                        parameters,
                        limits,
                    )
                    .await?
                }
                r::Operator::Filter(predicate) => {
                    Box::pin(self.filter_relation(rows, predicate, parameters, limits)).await?
                }
                r::Operator::Unwind { expression, slot } => {
                    if let Some(consumer) = plan.batch_consumer(index) {
                        let producer_limits = Limits {
                            batch_rows: limits.batch_rows.min(demand.max(1)),
                            ..limits
                        };
                        let batches = self.unwind_batches(
                            rows,
                            expression,
                            *slot,
                            parameters,
                            producer_limits,
                        );
                        let (result, consumed) =
                            Box::pin(self.consume_batches(
                                batches, plan, index, consumer, parameters, limits,
                            ))
                            .await?;
                        rows = result;
                        preprojected = Some(consumed);
                        continue;
                    }
                    let mut out = RowBuffer::new(self.row_budget())?;
                    'unwind: for batch in rows.chunks(limits.batch_rows) {
                        let graph = self.expression_graph_batch(batch, [expression]).await?;
                        for row in batch {
                            let values = self
                                .evaluate(row, parameters, &graph, limits)
                                .unwind(expression)?;
                            for value in values {
                                if out.len() >= demand {
                                    break 'unwind;
                                }
                                if out.len().is_multiple_of(limits.batch_rows) {
                                    self.check_execution_deadline()?;
                                }
                                out.push_replacing(row, *slot, value)?;
                            }
                        }
                    }
                    out.finish()
                }
                r::Operator::Project {
                    items,
                    distinct,
                    ordering,
                    predicate,
                    skip,
                    limit,
                } => {
                    let identity;
                    let items = if matches!(preprojected,Some(ConsumedProjection::Aggregate(aggregated)) if aggregated==index)
                    {
                        identity = r::ProjectionProgram::new(
                            items
                                .iter()
                                .map(|item| r::Projection {
                                    slot: item.slot,
                                    expression: r::Expression::Slot(item.slot),
                                })
                                .collect::<Vec<_>>(),
                        )?;
                        &identity
                    } else {
                        items
                    };
                    self.project_rows(
                        rows,
                        width,
                        projection::Projection {
                            items,
                            distinct: *distinct,
                            ordering,
                            predicate: predicate.as_ref(),
                            skip: skip.as_ref(),
                            limit: limit.as_ref(),
                        },
                        parameters,
                        limits,
                    )
                    .await?
                }
                r::Operator::Create(pattern) => {
                    self.create_rows(rows, pattern, parameters, limits).await?
                }
                r::Operator::Update(updates) => {
                    self.update_rows(rows, updates, parameters, limits).await?
                }
                r::Operator::Delete { entities, detach } => {
                    self.delete_rows(rows, entities, *detach, parameters, limits)
                        .await?
                }
            };
            rows.refresh()?;
            check_memory(&rows, limits)?;
        }
        let columns: Vec<String> = plan
            .query()
            .returns()
            .iter()
            .map(|(name, _)| name.clone())
            .collect();
        let mut output = Vec::new();
        let mut wire_size = WireSize {
            bytes: b"{\"columns\":,\"rows\":[]}".len(),
        };
        serde_json::to_writer(&mut wire_size, &columns)?;
        let mut output_bytes = columns
            .iter()
            .fold(size_of::<Vec<String>>(), |bytes, name| {
                bytes
                    .saturating_add(size_of::<String>())
                    .saturating_add(name.capacity())
            });
        let mut output_memory = self.row_budget().reserve(output_bytes)?;
        if !plan.query().returns().is_empty() {
            for batch in rows.chunks(limits.batch_rows) {
                let demand = plan
                    .query()
                    .returns()
                    .iter()
                    .map(|(_, slot)| (*slot, r::PropertyDemand::All))
                    .collect();
                let graph = self.graph_batch_required(batch, &demand).await?;
                for row in batch {
                    let admitted_bytes = plan.query().returns().iter().try_fold(
                        size_of::<Vec<serde_json::Value>>(),
                        |bytes, (_, slot)| {
                            Ok::<_, Error>(
                                bytes.saturating_add(graph.wire_memory(&row[slot.0 as usize])?),
                            )
                        },
                    )?;
                    output_memory.resize(output_bytes.saturating_add(admitted_bytes))?;
                    let values = plan
                        .query()
                        .returns()
                        .iter()
                        .map(|(_, slot)| graph.wire(&row[slot.0 as usize]))
                        .collect::<Result<Vec<_>>>()?;
                    let row_bytes = values
                        .iter()
                        .fold(size_of::<Vec<serde_json::Value>>(), |bytes, value| {
                            bytes.saturating_add(json_bytes(value))
                        });
                    assert!(
                        row_bytes <= admitted_bytes,
                        "wire admission must cover the owned response"
                    );
                    // Retain the conservative reservation with the response;
                    // container spare capacity need not match serialized size.
                    output_bytes = output_bytes.saturating_add(admitted_bytes);
                    serde_json::to_writer(&mut wire_size, &values)?;
                    if !output.is_empty() {
                        wire_size.bytes = wire_size.bytes.saturating_add(1);
                    }
                    if wire_size.bytes > limits.result_bytes {
                        return Err(resource(
                            "ResultLimit",
                            "query result exceeds the response byte budget",
                        ));
                    }
                    output.push(values);
                }
            }
        }
        if wire_size.bytes > limits.result_bytes {
            return Err(resource(
                "ResultLimit",
                "query result exceeds the response byte budget",
            ));
        }
        Ok(Response {
            columns,
            rows: output,
            diagnostics: plan.metrics.clone(),
            resources: crate::cypher::ResourceUsage {
                peak_memory_bytes: self.row_budget().peak(),
                reads: self.row_budget().reads(),
            },
        })
    }
}

impl ExecutionContext<'_> {
    fn evaluate<'a>(
        &self,
        row: &'a [r::Value],
        parameters: &'a BTreeMap<String, r::Value>,
        graph: &'a GraphBatch,
        limits: Limits,
    ) -> r::Evaluation<'a> {
        r::Evaluation {
            row,
            parameters,
            graph,
            group: None,
            max_collection_items: limits.collection_items,
            max_value_bytes: self.row_budget().available(),
        }
    }
}
fn resource(detail: &str, message: &str) -> Error {
    r::QueryError::runtime("ResourceLimit", detail, message).into()
}
fn row_bytes(row: &r::Row) -> usize {
    row.iter().fold(
        size_of::<r::Row>().saturating_add(
            row.capacity()
                .saturating_sub(row.len())
                .saturating_mul(size_of::<r::Value>()),
        ),
        |bytes, value| bytes.saturating_add(value.allocated_bytes()),
    )
}
fn rows_bytes(rows: &[r::Row]) -> usize {
    rows.iter()
        .fold(0_usize, |bytes, row| bytes.saturating_add(row_bytes(row)))
}
fn check_memory(rows: &[r::Row], limits: Limits) -> Result<()> {
    if rows_bytes(rows) > limits.memory_bytes {
        Err(resource("MemoryLimit", "query exceeds its memory budget"))
    } else {
        Ok(())
    }
}

/// Account owned row payloads at insertion, including nested containers. The
/// admission check precedes Vec growth; counters never wrap on hostile sizes.
struct RowBuffer {
    rows: Rows,
    bytes: usize,
}
impl RowBuffer {
    fn new(budget: &memory::Budget) -> Result<Self> {
        Ok(Self {
            rows: Rows::new(Vec::new(), budget)?,
            bytes: 0,
        })
    }
    fn len(&self) -> usize {
        self.rows.len()
    }
    fn finish(self) -> Rows {
        self.rows
    }
    /// Admit the destination and owned payload before a caller copies input.
    /// The builder must not exceed its conservative allocation bound.
    fn push_with(&mut self, row_bound: usize, build: impl FnOnce() -> r::Row) -> Result<()> {
        let bytes = self.bytes.saturating_add(row_bound);
        // Reserve a known geometric capacity before allocation; growing by one
        // would make wide scans quadratic in copied row headers.
        let capacity = if self.rows.len() == self.rows.capacity() {
            self.rows.capacity().saturating_mul(2).max(4)
        } else {
            self.rows.capacity()
        };
        let spare = capacity
            .saturating_sub(self.rows.len() + 1)
            .saturating_mul(size_of::<r::Row>());
        self.rows.reservation.resize(bytes.saturating_add(spare))?;
        if capacity > self.rows.capacity() {
            let additional = capacity - self.rows.len();
            self.rows
                .data
                .try_reserve_exact(additional)
                .map_err(|_| resource("MemoryLimit", "row allocation failed"))?;
        }
        let row = build();
        assert!(
            row_bytes(&row) <= row_bound,
            "row builder exceeded its admitted bound"
        );
        self.rows.data.push(row);
        self.bytes = bytes;
        Ok(())
    }
    /// Copy a row while replacing one binding, without cloning the old value
    /// that is being overwritten. Used by both streamed and materialized UNWIND.
    fn push_replacing(&mut self, input: &r::Row, slot: r::Slot, value: r::Value) -> Result<()> {
        let slot = slot.0 as usize;
        assert!(slot < input.len(), "validated UNWIND destination");
        let bytes =
            input
                .iter()
                .enumerate()
                .fold(size_of::<r::Row>(), |bytes, (index, original)| {
                    bytes.saturating_add(if index == slot {
                        value.allocated_bytes()
                    } else {
                        original.allocated_bytes()
                    })
                });
        self.push_with(bytes, || {
            let mut row = Vec::with_capacity(input.len());
            row.extend(input[..slot].iter().cloned());
            row.push(value);
            row.extend(input[slot + 1..].iter().cloned());
            row
        })
    }
}
fn push_row(rows: &mut RowBuffer, row: r::Row, _limits: Limits) -> Result<()> {
    rows.push_with(row_bytes(&row), || row)
}

fn json_bytes(value: &serde_json::Value) -> usize {
    let children = match value {
        serde_json::Value::String(value) => value.capacity(),
        serde_json::Value::Array(values) => values.iter().fold(
            values
                .capacity()
                .saturating_sub(values.len())
                .saturating_mul(size_of::<serde_json::Value>()),
            |bytes, value| bytes.saturating_add(json_bytes(value)),
        ),
        serde_json::Value::Object(values) => values.iter().fold(0_usize, |bytes, (key, value)| {
            bytes
                .saturating_add(key.capacity())
                .saturating_add(64)
                .saturating_add(json_bytes(value))
        }),
        serde_json::Value::Null | serde_json::Value::Bool(_) | serde_json::Value::Number(_) => 0,
    };
    size_of::<serde_json::Value>().saturating_add(children)
}

/// Count JSON encoding without allocating a second copy of the response.
struct WireSize {
    bytes: usize,
}
impl std::io::Write for WireSize {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.bytes = self.bytes.saturating_add(bytes.len());
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests;
