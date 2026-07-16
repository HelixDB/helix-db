//! Runtime ordering contracts.

use std::cmp::Ordering;

use super::*;

impl<'db> ExecutionContext<'db> {
    pub(in crate::execution::interpreter) async fn order(
        &mut self,
        input: ExecutionValue,
        plan: &ir::OrderPlan,
    ) -> Result<ExecutionValue> {
        let mut rows = self.stream_rows(input, "order")?;
        match plan {
            ir::OrderPlan::ExplicitSort(keys) => {
                let mut keyed = Vec::with_capacity(rows.len());
                for row in rows {
                    let mut values = Vec::new();
                    for key in keys.as_ref() {
                        values.push(self.row_property(&row, &key.property).await?);
                    }
                    keyed.push((values, row));
                }
                keyed.sort_by(|left, right| compare_order_keys(left, right, keys.as_ref()));
                rows = keyed.into_iter().map(|(_, row)| row).collect();
                Ok(ExecutionValue::Stream(rows))
            }
            ir::OrderPlan::RangeIndex { .. } => Ok(ExecutionValue::Stream(rows)),
        }
    }
}

fn compare_order_keys(
    left: &(Vec<Option<DbPropertyValue>>, ExecutionRow),
    right: &(Vec<Option<DbPropertyValue>>, ExecutionRow),
    keys: &[ir::OrderKey],
) -> Ordering {
    for ((left, right), key) in left.0.iter().zip(&right.0).zip(keys) {
        let ordering = match (left, right) {
            (Some(left), Some(right)) => left.compare(right),
            (Some(_), None) => Ordering::Greater,
            (None, Some(_)) => Ordering::Less,
            (None, None) => Ordering::Equal,
        };
        if ordering != Ordering::Equal {
            return match key.order {
                helix_ast::traversal::Order::Asc => ordering,
                helix_ast::traversal::Order::Desc => ordering.reverse(),
            };
        }
    }
    left.1.cmp(&right.1)
}

#[cfg(test)]
mod tests {
    use helix_ast::traversal::Order;
    use helix_planner::context;

    use super::super::super::test_support;
    use super::*;

    fn row(id: u64) -> ExecutionRow {
        ExecutionRow::current(ElementRef::Node(id))
    }

    fn key(order: Order) -> ir::OrderKey {
        ir::OrderKey {
            property: ir::NonEmptyString::new("score").expect("valid property"),
            order,
        }
    }

    #[test]
    fn compare_order_keys_places_missing_values_first_for_ascending() {
        let keys = [key(Order::Asc)];
        assert_eq!(
            compare_order_keys(
                &(vec![None], row(1)),
                &(vec![Some(DbPropertyValue::I64(1))], row(2)),
                &keys,
            ),
            Ordering::Less
        );
        assert_eq!(
            compare_order_keys(&(vec![None], row(2)), &(vec![None], row(1)), &keys,),
            Ordering::Greater
        );
    }

    #[test]
    fn compare_order_keys_reverses_value_direction_for_descending() {
        let keys = [key(Order::Desc)];
        assert_eq!(
            compare_order_keys(
                &(vec![Some(DbPropertyValue::I64(1))], row(1)),
                &(vec![Some(DbPropertyValue::I64(2))], row(2)),
                &keys,
            ),
            Ordering::Greater
        );
    }

    #[tokio::test]
    async fn range_index_order_preserves_access_order() {
        let db = test_support::open_db("stream-range-index-order").await;
        let mut context = ExecutionContext::new(&db, context::ParamBindings::default());
        let input = ExecutionValue::Stream(vec![row(2), row(1)]);
        let plan = ir::OrderPlan::RangeIndex {
            key: key(Order::Asc),
            index_id: ir::NonEmptyString::new("node_range:Metric:score").expect("valid index id"),
        };

        assert_eq!(
            context
                .order(input.clone(), &plan)
                .await
                .expect("range index order is already satisfied"),
            input
        );
    }
}
