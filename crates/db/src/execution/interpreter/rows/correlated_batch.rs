//! Bounded candidate validation shared by correlated node and graph stages.
//! Completion events preserve outer-row identity independently of filtering.
use super::{matches, memory, ExecutionContext, Limits, Result, RowBuffer};
use helix_planner::relational as r;
use std::collections::BTreeMap;

enum ParentState {
    Unmatched,
    Matched,
    Complete,
}

pub(super) struct Parents {
    rows: memory::Rows,
    states: Vec<ParentState>,
    _memory: memory::Reservation,
}
impl Parents {
    pub(super) fn new(rows: memory::Rows, budget: &memory::Budget) -> Result<Self> {
        let memory = budget.reserve(rows.len().saturating_mul(size_of::<ParentState>()))?;
        let states = (0..rows.len()).map(|_| ParentState::Unmatched).collect();
        Ok(Self {
            rows,
            states,
            _memory: memory,
        })
    }
    pub(super) fn get(&self, parent: usize) -> Option<&r::Row> {
        let row = self.rows.get(parent)?;
        assert!(
            !matches!(self.states[parent], ParentState::Complete),
            "a completed parent cannot resume"
        );
        Some(row)
    }
    pub(super) fn len(&self) -> usize {
        self.rows.len()
    }
}

enum Event {
    Candidate { parent: usize },
    Complete { parent: usize },
}

/// The bound includes empty-parent completions, so a run of unsuccessful outer
/// rows cannot grow an unbounded sidecar while waiting for a matching candidate.
pub(super) struct Batch {
    limit: usize,
    events: Vec<Event>,
    candidates: RowBuffer,
    _memory: memory::Reservation,
}
impl Batch {
    pub(super) fn new(limit: usize, budget: &memory::Budget) -> Result<Self> {
        assert!(limit > 0, "a correlated batch must make progress");
        let memory =
            budget.reserve(limit.saturating_mul(size_of::<Event>() + size_of::<usize>()))?;
        Ok(Self {
            limit,
            events: Vec::with_capacity(limit),
            candidates: RowBuffer::new(budget)?,
            _memory: memory,
        })
    }
    pub(super) fn remaining(&self) -> usize {
        self.limit - self.events.len()
    }
    pub(super) fn candidate_with(
        &mut self,
        parent: usize,
        bytes: usize,
        make: impl FnOnce() -> r::Row,
    ) -> Result<()> {
        assert!(self.remaining() > 0);
        self.candidates.push_with(bytes, make)?;
        self.events.push(Event::Candidate { parent });
        Ok(())
    }
    pub(super) fn complete(&mut self, parent: usize) {
        assert!(self.remaining() > 0);
        self.events.push(Event::Complete { parent });
    }

    pub(super) async fn finish(
        self,
        parents: &mut Parents,
        operation: matches::Match<'_>,
        context: &ExecutionContext<'_>,
        parameters: &BTreeMap<String, r::Value>,
        limits: Limits,
    ) -> Result<memory::Rows> {
        let mut matched = MatchedRows {
            rows: RowBuffer::new(context.row_budget())?,
            positions: Vec::with_capacity(self.limit),
        };
        context
            .row_budget()
            .admitted_future(context.finish_pattern_rows(
                self.candidates.finish(),
                operation,
                parameters,
                limits,
                &mut matched,
            ))?
            .await?;
        let mut kept = matched
            .positions
            .into_iter()
            .zip(matched.rows.finish())
            .peekable();
        let mut output = RowBuffer::new(context.row_budget())?;
        let mut position = 0_usize;
        for event in self.events {
            match event {
                Event::Candidate { parent } => {
                    assert!(
                        parent < parents.len(),
                        "candidate belongs to this outer batch"
                    );
                    assert!(
                        !matches!(parents.states[parent], ParentState::Complete),
                        "a candidate cannot follow parent completion"
                    );
                    if kept.peek().is_some_and(|(kept, _)| *kept == position) {
                        let (_, row) = kept.next().expect("checked retained candidate");
                        parents.states[parent] = ParentState::Matched;
                        super::push_row(&mut output, row, limits)?;
                    }
                    position += 1;
                }
                Event::Complete { parent } => {
                    assert!(
                        parent < parents.len(),
                        "completion belongs to this outer batch"
                    );
                    let previous =
                        std::mem::replace(&mut parents.states[parent], ParentState::Complete);
                    assert!(
                        !matches!(previous, ParentState::Complete),
                        "a parent can complete only once"
                    );
                    if operation.optional && matches!(previous, ParentState::Unmatched) {
                        let outer = &parents.rows[parent];
                        output.push_with(super::row_bytes(outer), || outer.clone())?;
                    }
                    parents.rows.release_rows(parent..parent + 1);
                }
            }
        }
        assert!(
            kept.next().is_none(),
            "every retained candidate has an origin"
        );
        Ok(output.finish())
    }
}

struct MatchedRows {
    rows: RowBuffer,
    positions: Vec<usize>,
}
impl matches::PatternOutput for MatchedRows {
    fn len(&self) -> usize {
        self.rows.len()
    }
    fn retain(&mut self, position: usize, row: r::Row) -> Result<()> {
        assert!(
            self.positions.len() < self.positions.capacity(),
            "one position per bounded candidate"
        );
        self.rows.push_with(super::row_bytes(&row), || row)?;
        self.positions.push(position);
        Ok(())
    }
}

#[cfg(test)]
#[path = "tests/parent_completion.rs"]
mod tests;
