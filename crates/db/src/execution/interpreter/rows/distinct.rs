//! Incremental DISTINCT retains the smallest SKIP + LIMIT equality classes (or
//! all classes without LIMIT). Input still drains, preserving expression errors.
use super::{memory, projection, row_bytes, ExecutionContext, Limits, Result, RowBuffer};
use helix_planner::relational as r;
use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
};

/// Every key in one operator borrows the same validated projection. Owning the
/// representative directly avoids copying large values into a second key tuple.
struct DistinctRow<'plan> {
    row: r::Row,
    items: &'plan r::ProjectionProgram,
}
impl PartialEq for DistinctRow<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}
impl Eq for DistinctRow<'_> {}
impl PartialOrd for DistinctRow<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for DistinctRow<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        assert!(
            std::ptr::eq(self.items, other.items),
            "one projection orders every DISTINCT key"
        );
        self.items
            .iter()
            .map(|item| self.row[item.slot.0 as usize].total_cmp(&other.row[item.slot.0 as usize]))
            .find(|order| !order.is_eq())
            .unwrap_or(Ordering::Equal)
    }
}

impl ExecutionContext<'_> {
    pub(super) async fn distinct_batches(
        &self,
        batches: impl futures::Stream<Item = Result<memory::Rows>>,
        width: usize,
        projection: projection::Projection<'_>,
        parameters: &BTreeMap<String, r::Value>,
        limits: Limits,
    ) -> Result<memory::Rows> {
        use futures::StreamExt;
        futures::pin_mut!(batches);
        assert!(projection.distinct && projection.ordering.is_empty());
        assert!(projection
            .items
            .iter()
            .all(|item| !item.expression.has_aggregate()));
        let empty = super::GraphBatch::default();
        let evaluation = self.evaluate(&[], parameters, &empty, limits);
        let window = r::Window::evaluate(projection.skip, projection.limit, |expression| {
            evaluation.eval(expression)
        })?;
        let keep = window.retained_rows();
        let mut entries = BTreeSet::new();
        let mut memory = self.row_budget().reserve(0)?;
        let mut payload = 0_usize;
        while let Some(batch) = batches.next().await {
            self.check_execution_deadline()?;
            let projected = self
                .project_rows(
                    batch?,
                    width,
                    projection::Projection {
                        items: projection.items,
                        distinct: false,
                        ordering: &[],
                        predicate: projection.predicate,
                        skip: None,
                        limit: None,
                    },
                    parameters,
                    limits,
                )
                .await?;
            for row in projected {
                self.check_execution_deadline()?;
                let entry = DistinctRow {
                    row,
                    items: projection.items,
                };
                // The cutoff only decreases, so an evicted equality class can
                // never become eligible again. Keep the first representative
                // of every retained class, including numerically equal values.
                if keep == 0
                    || (entries.len() == keep && entries.last().is_some_and(|last| &entry >= last))
                    || entries.contains(&entry)
                {
                    continue;
                }
                if entries.len() == keep {
                    let removed = entries.pop_last().expect("nonzero full distinct set");
                    payload -= row_bytes(&removed.row);
                }
                payload = payload.saturating_add(row_bytes(&entry.row));
                memory.resize(payload.saturating_add(r::allocation::btree_bytes::<
                    DistinctRow<'_>,
                    (),
                >(
                    entries.len().saturating_add(1)
                )))?;
                assert!(entries.insert(entry), "membership checked before insertion");
            }
        }
        // The ordered set uses the existing DISTINCT total ordering. Keeping
        // this order also preserves deterministic SKIP/LIMIT behavior. A tree
        // owns each representative once; a hash index would need separate keys.
        let mut output = RowBuffer::new(self.row_budget())?;
        for (index, entry) in entries.into_iter().enumerate() {
            self.check_execution_deadline()?;
            let bytes = row_bytes(&entry.row);
            if index < window.skip() || output.len() >= window.limit() {
                drop(entry);
                memory.release(bytes);
            } else {
                output.push_admitted(entry.row, memory.split(bytes))?;
            }
        }
        Ok(output.finish())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn projection() -> r::ProjectionProgram {
        r::ProjectionProgram::new(vec![
            r::Projection {
                slot: r::Slot(1),
                expression: r::Expression::Slot(r::Slot(1)),
            },
            r::Projection {
                slot: r::Slot(0),
                expression: r::Expression::Slot(r::Slot(0)),
            },
        ])
        .unwrap()
    }
    #[test]
    fn distinct_keys_use_projection_order_and_numeric_grouping_equality() {
        let items = projection();
        let a = DistinctRow {
            row: vec![r::Value::Integer(1), r::Value::Integer(2)],
            items: &items,
        };
        let equal = DistinctRow {
            row: vec![r::Value::Float(1.0), r::Value::Float(2.0)],
            items: &items,
        };
        let b = DistinctRow {
            row: vec![r::Value::Integer(2), r::Value::Integer(1)],
            items: &items,
        };
        assert!(a == equal);
        assert_eq!(a.partial_cmp(&equal), Some(Ordering::Equal));
        assert_eq!(a.partial_cmp(&b), Some(Ordering::Greater));
    }
    #[test]
    #[should_panic(expected = "one projection orders every DISTINCT key")]
    fn keys_from_different_projection_programs_cannot_share_an_order() {
        let a = projection();
        let b = projection();
        let a = DistinctRow {
            row: vec![r::Value::Null; 2],
            items: &a,
        };
        let b = DistinctRow {
            row: vec![r::Value::Null; 2],
            items: &b,
        };
        let _ = a.cmp(&b);
    }
}
