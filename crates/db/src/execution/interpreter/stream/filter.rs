//! Row-preserving residual filter and node index membership contracts.
//!
//! Both operators keep rows in input order with their paths, bindings, and
//! sacks. Rows that need the predicate are evaluated in bounded batches whose
//! stored records are read with one multi-get per batch instead of one serial
//! read per row. Index membership decides nodes of its label from secondary
//! index bitmaps and evaluates the predicate only for rows the index cannot
//! decide.

use super::eval::RowValueResolver;
use super::*;

/// Rows evaluated per stored-record batch. This bounds the decoded records a
/// filter holds at once while amortizing one multi-get over many rows.
const RECORD_BATCH_ROWS: usize = 256;

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

impl<'db> ExecutionContext<'db> {
    pub(in crate::execution::interpreter) async fn filter(
        &self,
        input: ExecutionValue,
        predicate: &ir::PredicatePlan,
    ) -> Result<ExecutionValue> {
        let rows = self.stream_rows(input, "filter")?;
        self.retain_rows(rows, predicate.predicate(), |_| RowDecision::Evaluate)
            .await
            .map(ExecutionValue::Stream)
    }

    pub(in crate::execution::interpreter) async fn index_membership(
        &self,
        input: ExecutionValue,
        plan: &exec::ExecNodeIndexMembershipPlan,
    ) -> Result<ExecutionValue> {
        let rows = self.stream_rows(input, "index membership")?;
        // A stream without node rows never consults the index.
        let prepared = if rows
            .iter()
            .any(|row| matches!(row.current, Some(ElementRef::Node(_))))
        {
            self.prepare_index_membership(plan).await?
        } else {
            PreparedIndexMembership::PerRow
        };
        self.retain_rows(rows, plan.predicate.predicate(), |row| prepared.decide(row))
            .await
            .map(ExecutionValue::Stream)
    }

    /// Resolve the membership set and label domain for this request.
    ///
    /// Both reads go through the request snapshot or write transaction and
    /// its Active catalog. A set that needs an authoritative scan, or an index
    /// the catalog no longer serves, falls back to exact per-row evaluation.
    pub(in crate::execution::interpreter) async fn prepare_index_membership(
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
                    .map(|label_nodes| OutsideMatches::Evaluate { label_nodes }),
            }
        };
        match futures::try_join!(self.node_secondary_set_bitmap(&plan.set), outside) {
            Ok((matches, outside)) => Ok(PreparedIndexMembership::Indexed { matches, outside }),
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
        predicate: &Predicate,
        decide: impl Fn(&ExecutionRow) -> RowDecision,
    ) -> Result<Vec<ExecutionRow>> {
        let prefetch = always_reads_element_record(predicate);
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
            for (decision, row) in batch {
                self.check_execution_deadline()?;
                let keep = match decision {
                    RowDecision::Keep => true,
                    RowDecision::Drop => false,
                    RowDecision::Evaluate => {
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
