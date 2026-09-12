//! Bounded expansion over admitted parent rows. Candidate cursors keep compressed
//! neighborhoods; type and endpoint reads combine IDs from different parents.
use super::super::access::{EdgeCursor, EdgeIdBatch};
use super::{memory, row_bytes, ExecutionContext, Limits, Result, RowBuffer};
use helix_planner::{ir, relational as r};

/// The pending IDs and continuation always belong to the current parent. Moving
/// this state into a failed poll drops every unread buffer and reservation.
struct ActiveParent {
    cursor: EdgeCursor,
    pending: Option<(EdgeIdBatch, usize)>,
}

impl ExecutionContext<'_> {
    pub(super) fn expansion_batches<'a>(
        &'a self,
        rows: memory::Rows,
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
        futures::stream::try_unfold(
            (rows, 0_usize, 0_usize, None::<ActiveParent>, label),
            move |(mut rows, mut parent, mut released, mut active, label)| {
                // Only an active poll owns the async storage-operation state.
                // Suspended expansion levels retain their compact continuation.
                let poll = async move {
                    loop {
                        self.check_execution_deadline()?;
                        // Includes candidate-parent pairs, ID scratch, type results,
                        // endpoint keys and decoded endpoints. Admit before Vec growth.
                        let _batch_memory = self
                            .row_budget()
                            .reserve(limits.batch_rows.saturating_mul(128))?;
                        let mut candidates = Vec::with_capacity(limits.batch_rows);
                        let mut ids = Vec::with_capacity(limits.batch_rows);
                        while candidates.len() < limits.batch_rows && parent < rows.len() {
                            self.check_execution_deadline()?;
                            let row = &rows[parent];
                            let mut current = match active.take() {
                                Some(current) => current,
                                None => {
                                    let r::Value::Entity(r::Entity::Node(source)) =
                                        row[from.0 as usize]
                                    else {
                                        parent += 1;
                                        continue;
                                    };
                                    let target = match row[to.0 as usize] {
                                        r::Value::Entity(r::Entity::Node(target)) => Some(target),
                                        r::Value::Null
                                        | r::Value::Boolean(_)
                                        | r::Value::Integer(_)
                                        | r::Value::Float(_)
                                        | r::Value::String(_)
                                        | r::Value::List(_)
                                        | r::Value::Map(_)
                                        | r::Value::Entity(r::Entity::Relationship(_))
                                        | r::Value::Path(_) => None,
                                    };
                                    ActiveParent {
                                        cursor: self
                                            .row_budget()
                                            .admitted_future(EdgeCursor::new(
                                                self,
                                                source,
                                                direction,
                                                &label,
                                                target,
                                                limits.batch_rows,
                                            ))?
                                            .await?,
                                        pending: None,
                                    }
                                }
                            };
                            match current.pending.take() {
                                Some((batch, mut offset)) => {
                                    let count = (limits.batch_rows - candidates.len())
                                        .min(batch.ids().len() - offset);
                                    candidates.extend(
                                        batch.ids()[offset..offset + count]
                                            .iter()
                                            .map(|id| (parent, *id)),
                                    );
                                    offset += count;
                                    if offset < batch.ids().len() {
                                        current.pending = Some((batch, offset));
                                    }
                                    active = Some(current);
                                }
                                None => {
                                    let Some((batch, cursor)) = self
                                        .row_budget()
                                        .admitted_future(current.cursor.next_batch(self))?
                                        .await?
                                    else {
                                        parent += 1;
                                        continue;
                                    };
                                    active = Some(ActiveParent {
                                        cursor,
                                        pending: Some((batch, 0)),
                                    });
                                }
                            }
                        }
                        if candidates.is_empty() {
                            return Ok(None);
                        }
                        ids.extend(candidates.iter().map(|(_, id)| *id));
                        let accepted_types = self
                            .row_budget()
                            .admitted_future(
                                self.relationship_types_batch(&ids, &relationship.types),
                            )?
                            .await?;
                        assert_eq!(
                            accepted_types.len(),
                            candidates.len(),
                            "one type result per candidate"
                        );
                        let mut accepted_types = accepted_types.into_iter();
                        candidates
                            .retain(|_| accepted_types.next().expect("validated candidate count"));
                        if candidates.is_empty() {
                            rows.release_rows(released..parent);
                            released = parent;
                            continue;
                        }
                        ids.clear();
                        ids.extend(candidates.iter().map(|(_, id)| *id));
                        let endpoints = self
                            .row_budget()
                            .admitted_future(self.edge_endpoints_batch(&ids))?
                            .await?;
                        assert_eq!(
                            endpoints.len(),
                            candidates.len(),
                            "one endpoint result per candidate"
                        );
                        let mut output = RowBuffer::new(self.row_budget())?;
                        for ((parent, id), endpoints) in candidates.into_iter().zip(endpoints) {
                            let row = &rows[parent];
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
                            // Stored endpoints confirm index candidates, including
                            // probes into a node bound by a preceding expansion.
                            let target = match direction {
                                ir::ExpandDirection::Out | ir::ExpandDirection::Both
                                    if source == start =>
                                {
                                    end
                                }
                                ir::ExpandDirection::In | ir::ExpandDirection::Both
                                    if source == end =>
                                {
                                    start
                                }
                                ir::ExpandDirection::Out
                                | ir::ExpandDirection::In
                                | ir::ExpandDirection::Both => continue,
                            };
                            let target = r::Value::Entity(r::Entity::Node(target));
                            if row[to.0 as usize] != r::Value::Null && row[to.0 as usize] != target
                            {
                                continue;
                            }
                            // Replaced slots contain null or graph IDs. Admit the
                            // parent payload before copying unrelated nested values.
                            output.push_with(row_bytes(row), || {
                                let mut next = row.clone();
                                next[to.0 as usize] = target;
                                next[relationship.slot.0 as usize] =
                                    r::Value::Entity(r::Entity::Relationship(id));
                                next
                            })?;
                        }
                        // Candidate-parent positions are no longer borrowed.
                        // Completed parents can release their payload before a
                        // downstream expansion suspends this continuation. Keep
                        // the active parent intact for its remaining neighbors.
                        rows.release_rows(released..parent);
                        released = parent;
                        if output.len() > 0 {
                            return Ok(Some((
                                output.finish(),
                                (rows, parent, released, active, label),
                            )));
                        }
                    }
                };
                match self.row_budget().admitted_future(poll) {
                    Ok(poll) => futures::future::Either::Left(poll),
                    Err(error) => futures::future::Either::Right(async move { Err(error.into()) }),
                }
            },
        )
    }
}
