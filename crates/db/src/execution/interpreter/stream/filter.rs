//! Row-preserving residual filter and node index membership contracts.
//!
//! Both operators keep rows in input order with their paths, bindings, and
//! sacks. Rows that need the predicate are evaluated in bounded batches whose
//! stored records are read with one multi-get per batch instead of one serial
//! read per row. Index membership decides nodes of its label from secondary
//! index bitmaps and evaluates the predicate only for rows the index cannot
//! decide.
//!
//! The bitmaps grow with the label and the matching values, not with the
//! stream, so membership reads them only for streams with more node rows
//! than one record batch. Narrower streams cost at most one multi-get per
//! batch and evaluate every row, exactly like the filter they replace. A
//! resolved set is reused by later executions of the same plan in the
//! request, such as branch bodies that run once per parent row.

use std::sync::Arc;

use helix_ast::expr::{Expr, Predicate};

use super::eval::RowValueResolver;
use super::*;

/// Rows evaluated per stored-record batch. This bounds the decoded records a
/// filter holds at once while amortizing one multi-get over many rows. Index
/// membership also resolves its bitmaps only for streams with more node rows.
pub(super) const RECORD_BATCH_ROWS: usize = 256;

/// Decision for one row of a row-preserving filter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::execution::interpreter) enum RowDecision {
    /// Keep the row without evaluating the predicate.
    Keep,
    /// Drop the row without evaluating the predicate.
    Drop,
    /// Evaluate the predicate against the row.
    Evaluate,
}

/// Membership state resolved once per operator execution.
#[derive(Debug, Clone, PartialEq)]
pub(in crate::execution::interpreter) enum PreparedIndexMembership {
    /// Secondary indexes decide every node of the membership label.
    Indexed {
        /// Label nodes satisfying the membership predicate.
        matches: roaring::RoaringTreemap,
        /// Decision for nodes outside `matches`.
        outside: OutsideMatches,
    },
    /// Indexes cannot serve this execution; every row evaluates the predicate.
    PerRow,
}

/// Memberships resolved in one request state, reused by later executions.
///
/// Branch bodies run their pipeline once per parent row and `ForEach` bodies
/// once per item, so the same plan can resolve many times per request. The
/// resolved set depends only on the plan, the request snapshot, and its
/// parameters, so an entry stays exact until one of them changes: every
/// mutation, index DDL, and `ForEach` parameter frame clears the cache first.
/// Entries hold at most one set per distinct plan in the request. A plan that
/// is not equal to itself, because a predicate constant is NaN, is never
/// stored and resolves on every execution instead.
#[derive(Debug, Default)]
pub(in crate::execution::interpreter) struct PreparedMemberships(
    Vec<(
        exec::ExecNodeIndexMembershipPlan,
        Arc<PreparedIndexMembership>,
    )>,
);

impl PreparedMemberships {
    /// Forget every resolved set before the state they were read from changes.
    pub(in crate::execution::interpreter) fn clear(&mut self) {
        self.0.clear();
    }

    fn get(
        &self,
        plan: &exec::ExecNodeIndexMembershipPlan,
    ) -> Option<Arc<PreparedIndexMembership>> {
        self.0
            .iter()
            .find(|(cached, _)| cached == plan)
            .map(|(_, prepared)| Arc::clone(prepared))
    }

    #[cfg(test)]
    pub(in crate::execution::interpreter) fn len(&self) -> usize {
        self.0.len()
    }
}

/// Decision for nodes outside the membership set.
#[derive(Debug, Clone, PartialEq)]
pub(in crate::execution::interpreter) enum OutsideMatches {
    /// The predicate requires the membership label, so every other node fails.
    Reject,
    /// Label nodes fail; nodes of other labels evaluate the predicate.
    Evaluate {
        /// Every node carrying the membership label.
        label_nodes: roaring::RoaringTreemap,
    },
}

impl PreparedIndexMembership {
    /// Decide one row. Edge and element-free rows always evaluate.
    pub(in crate::execution::interpreter) fn decide(&self, row: &ExecutionRow) -> RowDecision {
        let (Self::Indexed { matches, outside }, Some(ElementRef::Node(id))) =
            (self, row.current.as_ref())
        else {
            return RowDecision::Evaluate;
        };
        if matches.contains(*id) {
            return RowDecision::Keep;
        }
        match outside {
            OutsideMatches::Reject => RowDecision::Drop,
            OutsideMatches::Evaluate { label_nodes } if label_nodes.contains(*id) => {
                RowDecision::Drop
            }
            OutsideMatches::Evaluate { .. } => RowDecision::Evaluate,
        }
    }
}

/// Index membership state of one pull cursor.
///
/// A cursor cannot see how many rows it will pull, so it evaluates its first
/// record batch of node rows one by one and resolves the set on the next node
/// row. A short pull, such as an existence check or a small limit, therefore
/// never reads the label-sized bitmaps.
#[derive(Debug)]
pub(in crate::execution::interpreter) enum MembershipCursor {
    /// Node rows evaluated so far, at most [`RECORD_BATCH_ROWS`].
    PerRow { node_rows: usize },
    /// Set resolved after the cursor pulled more than one batch of node rows.
    Prepared(Arc<PreparedIndexMembership>),
}

impl Default for MembershipCursor {
    fn default() -> Self {
        Self::PerRow { node_rows: 0 }
    }
}

impl MembershipCursor {
    /// Decide one pulled row, resolving the set once the cursor has pulled
    /// more node rows than one record batch.
    pub(in crate::execution::interpreter) async fn decide(
        &mut self,
        ctx: &mut ExecutionContext<'_>,
        plan: &exec::ExecNodeIndexMembershipPlan,
        row: &ExecutionRow,
    ) -> Result<RowDecision> {
        match (&mut *self, row.current.as_ref()) {
            (Self::Prepared(prepared), _) => Ok(prepared.decide(row)),
            (Self::PerRow { node_rows }, Some(ElementRef::Node(_)))
                if *node_rows < RECORD_BATCH_ROWS =>
            {
                *node_rows += 1;
                Ok(RowDecision::Evaluate)
            }
            (Self::PerRow { .. }, Some(ElementRef::Node(_))) => {
                let prepared = ctx.cached_index_membership(plan).await?;
                let decision = prepared.decide(row);
                *self = Self::Prepared(prepared);
                Ok(decision)
            }
            (Self::PerRow { .. }, Some(ElementRef::Edge(_)) | None) => Ok(RowDecision::Evaluate),
        }
    }
}

impl<'db> ExecutionContext<'db> {
    pub(in crate::execution::interpreter) async fn filter(
        &self,
        input: ExecutionValue,
        predicate: &ir::PredicatePlan,
    ) -> Result<ExecutionValue> {
        let rows = self.stream_rows(input, "filter")?;
        self.retain_rows(rows, predicate, |_| RowDecision::Evaluate)
            .await
            .map(ExecutionValue::Stream)
    }

    pub(in crate::execution::interpreter) async fn index_membership(
        &mut self,
        input: ExecutionValue,
        plan: &exec::ExecNodeIndexMembershipPlan,
    ) -> Result<ExecutionValue> {
        let rows = self.stream_rows(input, "index membership")?;
        // At most one record batch of node rows evaluates every row, which
        // never reads more than the label-sized bitmaps would.
        let node_rows = rows
            .iter()
            .filter(|row| matches!(row.current, Some(ElementRef::Node(_))))
            .count();
        let prepared = if node_rows > RECORD_BATCH_ROWS {
            self.cached_index_membership(plan).await?
        } else {
            Arc::new(PreparedIndexMembership::PerRow)
        };
        self.retain_rows(rows, &plan.predicate, |row| prepared.decide(row))
            .await
            .map(ExecutionValue::Stream)
    }

    /// Resolve `plan` at most once per request state.
    ///
    /// See [`PreparedMemberships`] for when a resolved set is reused.
    pub(in crate::execution::interpreter) async fn cached_index_membership(
        &mut self,
        plan: &exec::ExecNodeIndexMembershipPlan,
    ) -> Result<Arc<PreparedIndexMembership>> {
        match self.prepared_memberships.get(plan) {
            Some(prepared) => Ok(prepared),
            None => {
                let prepared = Arc::new(self.prepare_index_membership(plan).await?);
                // A lookup could never find an entry for a plan unequal to
                // itself, so storing it would only hold its set until the
                // request ends.
                #[expect(
                    clippy::eq_op,
                    reason = "a NaN constant makes plan equality irreflexive"
                )]
                let reusable = plan == plan;
                if reusable {
                    self.prepared_memberships
                        .0
                        .push((plan.clone(), Arc::clone(&prepared)));
                }
                Ok(prepared)
            }
        }
    }

    /// Resolve the membership set and label domain for this request.
    ///
    /// Both reads go through the request snapshot or write transaction and
    /// its Active catalog. A set that needs an authoritative scan, or an index
    /// the catalog no longer serves, falls back to exact per-row evaluation.
    async fn prepare_index_membership(
        &self,
        plan: &exec::ExecNodeIndexMembershipPlan,
    ) -> Result<PreparedIndexMembership> {
        if !self.node_secondary_set_is_index_served(&plan.set)? {
            return Ok(PreparedIndexMembership::PerRow);
        }
        let outside = async {
            match plan.outside_label {
                ir::NodeMembershipOutsideLabel::Reject => Ok(OutsideMatches::Reject),
                ir::NodeMembershipOutsideLabel::Evaluate => self
                    .lookup_equality_index_set(
                        "$label",
                        &DbPropertyValue::String(plan.label.to_string()),
                    )
                    .await
                    .map(|label_nodes| OutsideMatches::Evaluate {
                        label_nodes: label_nodes.into_unbudgeted(),
                    }),
            }
        };
        match futures::try_join!(self.node_secondary_set_bitmap(&plan.set), outside) {
            Ok((matches, outside)) => {
                #[cfg(test)]
                self.db
                    .inner
                    .resolved_index_memberships
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Ok(PreparedIndexMembership::Indexed { matches, outside })
            }
            Err(HelixDbError::IndexLifecycleUnavailable { .. }) => {
                Ok(PreparedIndexMembership::PerRow)
            }
            Err(error) => Err(error),
        }
    }

    /// Keep rows in order, evaluating `predicate` only where `decide` asks.
    async fn retain_rows(
        &self,
        rows: Vec<ExecutionRow>,
        predicate: &ir::PredicatePlan,
        decide: impl Fn(&ExecutionRow) -> RowDecision,
    ) -> Result<Vec<ExecutionRow>> {
        let prefetch = always_reads_element_record(predicate.predicate());
        let mut kept = Vec::new();
        let mut rows = rows.into_iter().map(|row| (decide(&row), row));
        loop {
            let batch = rows.by_ref().take(RECORD_BATCH_ROWS).collect::<Vec<_>>();
            if batch.is_empty() {
                return Ok(kept);
            }
            let mut resolver = RowValueResolver::new(self);
            if prefetch {
                resolver
                    .prefetch(
                        batch
                            .iter()
                            .filter(|(decision, _)| *decision == RowDecision::Evaluate)
                            .filter_map(|(_, row)| row.current.as_ref()),
                    )
                    .await?;
            }
            kept.extend(self.select_native_rows(batch, predicate, resolver).await?);
        }
    }
}

/// Whether evaluating `predicate` reads the current element's record for
/// every row.
///
/// Only operands that evaluation never short-circuits count: both sides of a
/// comparison, the first child of a conjunction or disjunction, and the
/// value and lower bound of a range. Row-local values (`$id` and search
/// scores) and edge endpoint paths never read the current record. Batching
/// is therefore never allowed to read a record the per-row path would skip.
fn always_reads_element_record(predicate: &Predicate) -> bool {
    match predicate {
        Predicate::Eq { left, right }
        | Predicate::Neq { left, right }
        | Predicate::Gt { left, right }
        | Predicate::Gte { left, right }
        | Predicate::Lt { left, right }
        | Predicate::Lte { left, right }
        | Predicate::Compare { left, right, .. }
        | Predicate::StartsWith {
            value: left,
            prefix: right,
        }
        | Predicate::EndsWith {
            value: left,
            suffix: right,
        }
        | Predicate::Contains {
            value: left,
            substring: right,
        }
        | Predicate::IsIn {
            value: left,
            values: right,
        }
        | Predicate::Between {
            value: left,
            min: right,
            ..
        } => expr_always_reads_element_record(left) || expr_always_reads_element_record(right),
        Predicate::HasKey { property }
        | Predicate::IsNull { property }
        | Predicate::IsNotNull { property } => property_reads_element_record(property),
        Predicate::And { predicates } | Predicate::Or { predicates } => {
            predicates.first().is_some_and(always_reads_element_record)
        }
        Predicate::Not { predicate } => always_reads_element_record(predicate),
    }
}

fn expr_always_reads_element_record(expr: &Expr) -> bool {
    match expr {
        Expr::Property(property) => property_reads_element_record(property),
        Expr::Add { left, right }
        | Expr::Sub { left, right }
        | Expr::Mul { left, right }
        | Expr::Div { left, right }
        | Expr::Mod { left, right } => {
            expr_always_reads_element_record(left) || expr_always_reads_element_record(right)
        }
        Expr::Neg { expr } => expr_always_reads_element_record(expr),
        Expr::Case { .. }
        | Expr::Id
        | Expr::Timestamp
        | Expr::DateTimeNow
        | Expr::Constant(_)
        | Expr::Param(_) => false,
    }
}

fn property_reads_element_record(property: &str) -> bool {
    !matches!(property, "$id" | "$distance" | "$score" | "$from" | "$to")
        && !property.starts_with("$from.")
        && !property.starts_with("$to.")
}
