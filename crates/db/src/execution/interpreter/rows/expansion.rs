//! Compact adjacency cursors shared by both row execution strategies. A cursor
//! retains IDs and one parent row, then hydrates endpoints a batch at a time.
use super::{memory, push_row, row_bytes, ExecutionContext, Limits, Result, RowBuffer};
use futures::TryStreamExt;
use helix_planner::{ir, relational as r};

impl ExecutionContext<'_> {
    pub(super) fn expansion_batches<'a>(
        &'a self,
        row: r::Row,
        pattern: &'a r::Pattern,
        step: &'a r::MatchStep,
        limits: Limits,
    ) -> impl futures::Stream<Item = Result<memory::Rows>> + 'a {
        let r::MatchStep::Expand {
            relationship,
            from,
            to,
            reverse,
        } = step
        else {
            unreachable!("validated expansion step");
        };
        let relationship = &pattern.relationships[*relationship];
        let preparation = futures::stream::once(async move {
            let direction = match (relationship.direction, reverse) {
                (r::Direction::Outgoing, false) | (r::Direction::Incoming, true) => {
                    ir::ExpandDirection::Out
                }
                (r::Direction::Incoming, false) | (r::Direction::Outgoing, true) => {
                    ir::ExpandDirection::In
                }
                (r::Direction::Undirected, _) => ir::ExpandDirection::Both,
            };
            let label = if relationship.types.len() == 1 {
                ir::ExpandLabelPlan::Label(
                    ir::NonEmptyString::new(relationship.types[0].clone()).expect("validated type"),
                )
            } else {
                ir::ExpandLabelPlan::Any
            };
            let ids = match row[from.0 as usize] {
                r::Value::Entity(r::Entity::Node(source)) => {
                    self.expand_edge_ids(source, direction, &label).await?
                }
                r::Value::Null
                | r::Value::Boolean(_)
                | r::Value::Integer(_)
                | r::Value::Float(_)
                | r::Value::String(_)
                | r::Value::List(_)
                | r::Value::Map(_)
                | r::Value::Entity(r::Entity::Relationship(_))
                | r::Value::Path(_) => roaring::RoaringTreemap::new(),
            };
            let memory = self
                .row_budget()
                .reserve(row_bytes(&row).saturating_add(memory::bitmap_bytes(&ids)))?;
            Ok::<_, crate::cypher::Error>((row, ids.into_iter(), memory))
        });
        preparation
            .map_ok(move |state| {
                futures::stream::try_unfold(state, move |(row, mut ids, memory)| async move {
                    loop {
                        self.check_execution_deadline()?;
                        // Includes the ID vector, endpoint keys and endpoint results.
                        let _batch_memory = self
                            .row_budget()
                            .reserve(limits.batch_rows.saturating_mul(128))?;
                        let batch = ids.by_ref().take(limits.batch_rows).collect::<Vec<_>>();
                        if batch.is_empty() {
                            return Ok(None);
                        }
                        let mut output = RowBuffer::new(self.row_budget())?;
                        for (id, endpoints) in batch
                            .iter()
                            .copied()
                            .zip(self.edge_endpoints_batch(&batch).await?)
                        {
                            if pattern.relationships.iter().any(|other| {
                                other.slot != relationship.slot
                                    && row[other.slot.0 as usize]
                                        == r::Value::Entity(r::Entity::Relationship(id))
                            }) {
                                continue;
                            }
                            let current = &row[relationship.slot.0 as usize];
                            if *current != r::Value::Null
                                && *current != r::Value::Entity(r::Entity::Relationship(id))
                            {
                                continue;
                            }
                            let Some((start, end)) = endpoints else {
                                continue;
                            };
                            let r::Value::Entity(r::Entity::Node(source)) = row[from.0 as usize]
                            else {
                                unreachable!("only bound nodes produce edge IDs");
                            };
                            let target = r::Value::Entity(r::Entity::Node(if source == start {
                                end
                            } else {
                                start
                            }));
                            if row[to.0 as usize] != r::Value::Null && row[to.0 as usize] != target
                            {
                                continue;
                            }
                            let mut next = row.clone();
                            next[to.0 as usize] = target;
                            next[relationship.slot.0 as usize] =
                                r::Value::Entity(r::Entity::Relationship(id));
                            push_row(&mut output, next, limits)?;
                        }
                        if output.len() > 0 {
                            return Ok(Some((output.finish(), (row, ids, memory))));
                        }
                    }
                })
            })
            .try_flatten()
    }
}
