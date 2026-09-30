//! Row-preserving residual filter and node index membership contracts.
//!
//! Both operators keep rows in input order with their paths, bindings, and
//! sacks. Rows that need a predicate are evaluated in bounded batches whose
//! stored records are read with one multi-get per batch instead of one serial
//! read per row.
//!
//! Index membership decides nodes from a set read from indexes and evaluates
//! a predicate only for rows the set cannot decide, plus the residual
//! conjuncts of nodes in the set. There is no row-count threshold: the first
//! node row that needs a decision resolves the set, and a resolved set is
//! reused by later executions in the request of any plan with the same set,
//! such as branch and `ForEach` bodies, until something it depends on may
//! have changed (see [`PreparedMemberships`](super::PreparedMemberships)).
//! Streams without node rows never resolve it.

use std::sync::Arc;

use futures::FutureExt;

use super::eval::RowValueResolver;
use super::*;

/// Rows evaluated per stored-record batch. This bounds the decoded records a
/// filter holds at once while amortizing one multi-get over many rows, and
/// sizes the multi-get batches of every row-preserving filter.
///
/// The value comes from `helix_planner::cost::RECORD_BATCH_ROWS`, so pricing
/// and execution batch alike.
pub(super) const RECORD_BATCH_ROWS: usize = helix_planner::cost::RECORD_BATCH_ROWS as usize;

/// Decision for one row of a row-preserving filter.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(in crate::execution::interpreter) enum RowDecision<'p> {
    /// Keep the row without evaluating a predicate.
    Keep,
    /// Drop the row without evaluating a predicate.
    Drop,
    /// Keep the row exactly when it satisfies this predicate.
    Evaluate(&'p Predicate),
}

/// Membership state resolved once per operator execution.
#[derive(Debug, Clone, PartialEq)]
pub(in crate::execution::interpreter) enum PreparedIndexMembership {
    /// Indexes decide every node of the membership set.
    Indexed {
        /// Nodes satisfying the decided conjuncts.
        matches: roaring::RoaringTreemap,
        /// Decision for nodes outside `matches`.
        outside: OutsideMatches,
    },
    /// Indexes cannot serve this execution; every row evaluates the predicate.
    PerRow,
}

/// Decision for nodes outside the membership set.
#[derive(Debug, Clone, PartialEq)]
pub(in crate::execution::interpreter) enum OutsideMatches {
    /// Every other node fails the predicate: it requires the set label, or
    /// the set holds every node of the labels the predicate admits.
    Reject,
    /// Label nodes fail; nodes of other labels evaluate the predicate.
    Evaluate {
        /// Every node carrying the membership label.
        label_nodes: roaring::RoaringTreemap,
    },
}

impl PreparedIndexMembership {
    /// Decide one row of `plan`, the plan this membership was resolved from.
    ///
    /// Nodes in the set evaluate the residual, if any. Edge and element-free
    /// rows, and every row of a per-row membership, evaluate the whole
    /// predicate.
    pub(in crate::execution::interpreter) fn decide<'p>(
        &self,
        plan: &'p exec::ExecNodeIndexMembershipPlan,
        row: &ExecutionRow,
    ) -> RowDecision<'p> {
        let (Self::Indexed { matches, outside }, Some(ElementRef::Node(id))) =
            (self, row.current.as_ref())
        else {
            return RowDecision::Evaluate(plan.predicate.predicate());
        };
        if matches.contains(*id) {
            return plan
                .residual
                .as_ref()
                .map_or(RowDecision::Keep, |residual| {
                    RowDecision::Evaluate(residual.predicate())
                });
        }
        match outside {
            OutsideMatches::Reject => RowDecision::Drop,
            OutsideMatches::Evaluate { label_nodes } if label_nodes.contains(*id) => {
                RowDecision::Drop
            }
            OutsideMatches::Evaluate { .. } => RowDecision::Evaluate(plan.predicate.predicate()),
        }
    }
}

/// Index membership state of one pull cursor.
///
/// The cursor resolves the set, cache first, on the first node row it
/// decides, so short pulls such as an existence check or a small limit
/// resolve it on their first node row. Edge and element-free rows evaluate
/// the predicate and never resolve it.
#[derive(Debug, Default)]
pub(in crate::execution::interpreter) enum MembershipCursor {
    /// No node row has been decided yet.
    #[default]
    Unresolved,
    /// Set resolved on the cursor's first node row.
    Resolved(Arc<PreparedIndexMembership>),
}

impl MembershipCursor {
    /// Decide one pulled row, resolving the set on the first node row.
    pub(in crate::execution::interpreter) async fn decide<'p>(
        &mut self,
        ctx: &mut ExecutionContext<'_>,
        plan: &'p exec::ExecNodeIndexMembershipPlan,
        row: &ExecutionRow,
    ) -> Result<RowDecision<'p>> {
        match (&*self, row.current.as_ref()) {
            (Self::Resolved(prepared), _) => Ok(prepared.decide(plan, row)),
            (Self::Unresolved, Some(ElementRef::Node(_))) => {
                let prepared = ctx.cached_index_membership(plan).await?;
                let decision = prepared.decide(plan, row);
                *self = Self::Resolved(prepared);
                Ok(decision)
            }
            (Self::Unresolved, Some(ElementRef::Edge(_)) | None) => {
                Ok(RowDecision::Evaluate(plan.predicate.predicate()))
            }
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
        self.retain_rows(rows, |_| RowDecision::Evaluate(predicate.predicate()))
            .await
            .map(ExecutionValue::Stream)
    }

    pub(in crate::execution::interpreter) async fn index_membership(
        &mut self,
        input: ExecutionValue,
        plan: &exec::ExecNodeIndexMembershipPlan,
    ) -> Result<ExecutionValue> {
        let rows = self.stream_rows(input, "index membership")?;
        let prepared = if rows
            .iter()
            .any(|row| matches!(row.current, Some(ElementRef::Node(_))))
        {
            self.cached_index_membership(plan).await?
        } else {
            Arc::new(PreparedIndexMembership::PerRow)
        };
        self.retain_rows(rows, |row| prepared.decide(plan, row))
            .await
            .map(ExecutionValue::Stream)
    }

    /// Resolve `plan`, reusing a set resolved earlier in the request.
    ///
    /// See [`PreparedMemberships`](super::PreparedMemberships) for when a
    /// resolved set is reused.
    pub(in crate::execution::interpreter) async fn cached_index_membership(
        &mut self,
        plan: &exec::ExecNodeIndexMembershipPlan,
    ) -> Result<Arc<PreparedIndexMembership>> {
        match self.prepared_memberships.get(&plan.set) {
            Some(prepared) => Ok(prepared),
            None => {
                let prepared = Arc::new(self.prepare_index_membership(plan).await?);
                self.prepared_memberships
                    .insert(&plan.set, Arc::clone(&prepared));
                Ok(prepared)
            }
        }
    }

    /// Resolve the membership set, and the label domain it needs, for this
    /// request.
    ///
    /// Every read goes through the request snapshot or write transaction and
    /// its Active catalog. An index set that needs an authoritative scan, or
    /// an index the catalog no longer serves, falls back to exact per-row
    /// evaluation.
    ///
    /// An index set keeps at most `PARALLEL_INDEX_READS` leaf index reads in
    /// flight, and the label domain of an `Evaluate` policy is read alongside
    /// it, so one resolve keeps at most `PARALLEL_INDEX_READS + 1` reads in
    /// flight. A `$label` set reads at most `PARALLEL_INDEX_READS` label
    /// bitmaps at once and rejects every other node. Reads beyond one per set
    /// also draw on the request's shared budget (see
    /// `access::SharedIndexReads`), so parallel steps cannot multiply them.
    async fn prepare_index_membership(
        &self,
        plan: &exec::ExecNodeIndexMembershipPlan,
    ) -> Result<PreparedIndexMembership> {
        let prepared = match &plan.set {
            exec::ExecNodeMembershipSet::Index {
                set,
                label,
                outside_label,
            } => {
                if !self.node_secondary_set_is_index_served(set)? {
                    return Ok(PreparedIndexMembership::PerRow);
                }
                let outside = async {
                    match outside_label {
                        ir::NodeMembershipOutsideLabel::Reject => Ok(OutsideMatches::Reject),
                        ir::NodeMembershipOutsideLabel::Evaluate => self
                            .lookup_equality_index_set(
                                "$label",
                                &DbPropertyValue::String(label.to_string()),
                            )
                            .await
                            .map(|label_nodes| OutsideMatches::Evaluate { label_nodes }),
                    }
                };
                futures::try_join!(self.node_secondary_set_bitmap(set), outside)
                    .map(|(matches, outside)| PreparedIndexMembership::Indexed { matches, outside })
            }
            exec::ExecNodeMembershipSet::Labels(labels) => access::union(self.read_children(
                labels.iter().collect(),
                access::PARALLEL_INDEX_READS,
                |label, _| {
                    async move {
                        self.lookup_equality_index_set(
                            "$label",
                            &DbPropertyValue::String(label.to_string()),
                        )
                        .await
                    }
                    .boxed()
                },
            ))
            .await
            .map(|matches| PreparedIndexMembership::Indexed {
                matches,
                outside: OutsideMatches::Reject,
            }),
        };
        match prepared {
            Ok(prepared) => {
                #[cfg(test)]
                self.db
                    .inner
                    .resolved_index_memberships
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Ok(prepared)
            }
            Err(HelixDbError::IndexLifecycleUnavailable { .. }) => {
                Ok(PreparedIndexMembership::PerRow)
            }
            Err(error) => Err(error),
        }
    }

    /// Keep rows in order, evaluating a predicate only where `decide` asks.
    ///
    /// Each batch prefetches the records of rows whose predicate always reads
    /// them, so a batch reads each record at most once.
    ///
    /// `decide` answers with the few predicates of one plan (the whole
    /// predicate and a residual), so whether a predicate always reads the
    /// record is computed once per distinct predicate reference, not per row.
    async fn retain_rows<'p>(
        &self,
        rows: Vec<ExecutionRow>,
        decide: impl Fn(&ExecutionRow) -> RowDecision<'p>,
    ) -> Result<Vec<ExecutionRow>> {
        let mut kept = Vec::new();
        let mut always_reads = Vec::<(&'p Predicate, bool)>::new();
        let mut rows = rows.into_iter().map(|row| (decide(&row), row));
        loop {
            let batch = rows.by_ref().take(RECORD_BATCH_ROWS).collect::<Vec<_>>();
            if batch.is_empty() {
                return Ok(kept);
            }
            let mut resolver = RowValueResolver::new(self);
            resolver
                .prefetch(
                    batch
                        .iter()
                        .filter(|(decision, _)| {
                            let RowDecision::Evaluate(predicate) = decision else {
                                return false;
                            };
                            always_reads
                                .iter()
                                .find(|(known, _)| core::ptr::eq(*known, *predicate))
                                .map(|(_, reads)| *reads)
                                .unwrap_or_else(|| {
                                    let reads = always_reads_element_record(predicate);
                                    always_reads.push((predicate, reads));
                                    reads
                                })
                        })
                        .filter_map(|(_, row)| row.current.as_ref()),
                )
                .await?;
            for (decision, row) in batch {
                self.check_execution_deadline()?;
                let keep = match decision {
                    RowDecision::Keep => true,
                    RowDecision::Drop => false,
                    RowDecision::Evaluate(predicate) => {
                        self.eval_predicate_with_resolver(&row, predicate, &mut resolver)
                            .await?
                    }
                };
                if keep {
                    kept.push(row);
                }
            }
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
