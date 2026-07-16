//! Runtime aggregate execution contracts.

use std::collections::BTreeMap;

use helix_ast::traversal::AggregateFunction;

use super::values::scalar_items;
use super::*;

impl<'db> ExecutionContext<'db> {
    pub(in crate::execution::interpreter) async fn aggregate(
        &mut self,
        input: ExecutionValue,
        aggregate: &ir::AggregatePlan,
    ) -> Result<ExecutionValue> {
        let rows = match input {
            ExecutionValue::Stream(rows) => rows,
            ExecutionValue::FoldedStream(_) => {
                return Err(HelixDbError::Query(
                    "aggregate expected stream input, got folded stream; use unfold first"
                        .to_string(),
                ));
            }
            value @ (ExecutionValue::Count(_)
            | ExecutionValue::Bool(_)
            | ExecutionValue::Scalars(_)) => {
                return aggregate_scalar_items(value, aggregate);
            }
            ExecutionValue::IndexDdlReceipt(_) | ExecutionValue::IndexOperationStatus(_) => {
                return Err(HelixDbError::Query(
                    "aggregate cannot consume an index lifecycle value".to_string(),
                ));
            }
        };
        match aggregate {
            ir::AggregatePlan::Group(property) => {
                let mut groups = BTreeMap::<String, (Option<DbPropertyValue>, Vec<i64>)>::new();
                for row in &rows {
                    let element_id = row
                        .current
                        .as_ref()
                        .ok_or_else(|| {
                            HelixDbError::Query(
                                "group expected element stream input, got empty row".to_string(),
                            )
                        })?
                        .id()
                        .try_into()
                        .unwrap_or(i64::MAX);
                    let value = self.row_property(row, property).await?;
                    let entry = groups
                        .entry(
                            value
                                .as_ref()
                                .map_or_else(|| "null".to_string(), ToString::to_string),
                        )
                        .or_insert_with(|| (value.clone(), Vec::new()));
                    if entry.0.is_none() {
                        entry.0 = value;
                    }
                    entry.1.push(element_id);
                }
                Ok(ExecutionValue::Scalars(
                    groups
                        .into_iter()
                        .map(|(_, (value, ids))| {
                            ExecutionScalar::Object(BTreeMap::from([
                                (
                                    property.as_ref().to_string(),
                                    value.unwrap_or(DbPropertyValue::Null),
                                ),
                                ("count".to_string(), DbPropertyValue::I64(ids.len() as i64)),
                                ("ids".to_string(), DbPropertyValue::I64Array(ids)),
                            ]))
                        })
                        .collect(),
                ))
            }
            ir::AggregatePlan::GroupCount(property) => {
                let mut groups = BTreeMap::<String, (Option<DbPropertyValue>, i64)>::new();
                for row in &rows {
                    if row.current.is_none() {
                        return Err(HelixDbError::Query(
                            "groupCount expected element stream input, got empty row".to_string(),
                        ));
                    }
                    let value = self.row_property(row, property).await?;
                    let entry = groups
                        .entry(
                            value
                                .as_ref()
                                .map_or_else(|| "null".to_string(), ToString::to_string),
                        )
                        .or_insert_with(|| (value.clone(), 0));
                    if entry.0.is_none() {
                        entry.0 = value;
                    }
                    entry.1 += 1;
                }
                Ok(ExecutionValue::Scalars(
                    groups
                        .into_iter()
                        .map(|(_, (value, count))| {
                            ExecutionScalar::Object(BTreeMap::from([
                                (
                                    property.as_ref().to_string(),
                                    value.unwrap_or(DbPropertyValue::Null),
                                ),
                                ("count".to_string(), DbPropertyValue::I64(count)),
                            ]))
                        })
                        .collect(),
                ))
            }
            ir::AggregatePlan::AggregateBy { function, property } => {
                self.aggregate_by(rows, function.clone(), property).await
            }
        }
    }

    async fn aggregate_by(
        &self,
        rows: Vec<ExecutionRow>,
        function: AggregateFunction,
        property: &ir::NonEmptyString,
    ) -> Result<ExecutionValue> {
        let mut values = Vec::new();
        for row in &rows {
            if row.current.is_none() {
                return Err(HelixDbError::Query(
                    "aggregateBy expected element stream input, got empty row".to_string(),
                ));
            }
            let Some(value) = self.row_property(row, property).await? else {
                continue;
            };
            if let Some(value) = aggregate_numeric_value(&value) {
                values.push(value);
            }
        }
        let value = match function {
            AggregateFunction::Count => values.len() as f64,
            AggregateFunction::Sum => values.iter().sum(),
            AggregateFunction::Min => values.into_iter().reduce(f64::min).unwrap_or(0.0),
            AggregateFunction::Max => values.into_iter().reduce(f64::max).unwrap_or(0.0),
            AggregateFunction::Mean if values.is_empty() => 0.0,
            AggregateFunction::Mean => values.iter().sum::<f64>() / values.len() as f64,
        };
        Ok(ExecutionValue::Scalars(vec![ExecutionScalar::Object(
            BTreeMap::from([(
                format!("{}_{function:?}", property.as_ref()),
                DbPropertyValue::F64(value),
            )]),
        )]))
    }
}

fn aggregate_numeric_value(value: &DbPropertyValue) -> Option<f64> {
    match value {
        DbPropertyValue::I64(value) => Some(*value as f64),
        DbPropertyValue::F64(value) | DbPropertyValue::F32(value) => Some(*value),
        DbPropertyValue::String(value) => value.parse().ok(),
        DbPropertyValue::Null
        | DbPropertyValue::Bool(_)
        | DbPropertyValue::DateTime(_)
        | DbPropertyValue::Bytes(_)
        | DbPropertyValue::I64Array(_)
        | DbPropertyValue::F64Array(_)
        | DbPropertyValue::F32Array(_)
        | DbPropertyValue::StringArray(_)
        | DbPropertyValue::Array(_)
        | DbPropertyValue::Object(_) => None,
    }
}

fn aggregate_scalar_items(
    value: ExecutionValue,
    aggregate: &ir::AggregatePlan,
) -> Result<ExecutionValue> {
    let values = scalar_items(value);
    match aggregate {
        ir::AggregatePlan::AggregateBy {
            function: AggregateFunction::Count,
            ..
        } => Ok(ExecutionValue::Count(values.len())),
        ir::AggregatePlan::Group(_)
        | ir::AggregatePlan::GroupCount(_)
        | ir::AggregatePlan::AggregateBy { .. } => Err(HelixDbError::Query(format!(
            "aggregate {aggregate:?} expected element stream input, got scalar terminal input"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use helix_ast::traversal::AggregateFunction;
    use helix_planner::context::ParamBindings;

    use super::super::super::test_support;
    use super::*;

    #[test]
    fn scalar_aggregate_contract_accepts_count_only() {
        let value = ExecutionValue::Scalars(vec![
            ExecutionScalar::Value(DbPropertyValue::I64(1)),
            ExecutionScalar::Value(DbPropertyValue::I64(2)),
        ]);

        assert_eq!(
            aggregate_scalar_items(
                value.clone(),
                &ir::AggregatePlan::AggregateBy {
                    function: AggregateFunction::Count,
                    property: ir::NonEmptyString::new("score").expect("valid property"),
                },
            )
            .unwrap(),
            ExecutionValue::Count(2)
        );
        assert!(aggregate_scalar_items(
            value,
            &ir::AggregatePlan::AggregateBy {
                function: AggregateFunction::Sum,
                property: ir::NonEmptyString::new("score").expect("valid property"),
            },
        )
        .unwrap_err()
        .to_string()
        .contains("expected element stream input"));
    }

    #[tokio::test]
    async fn aggregate_dispatch_rejects_folded_and_empty_element_rows() {
        let db = test_support::open_db("aggregate-shape-contracts").await;
        let mut context = ExecutionContext::new(&db, ParamBindings::default());
        let property = ir::NonEmptyString::new("score").unwrap();

        assert!(matches!(
            context
                .aggregate(
                    ExecutionValue::FoldedStream(FoldedStream::new(Vec::new())),
                    &ir::AggregatePlan::Group(property.clone()),
                )
                .await,
            Err(HelixDbError::Query(message)) if message.contains("folded stream")
        ));
        assert_eq!(
            context
                .aggregate(
                    ExecutionValue::Count(7),
                    &ir::AggregatePlan::AggregateBy {
                        function: AggregateFunction::Count,
                        property: property.clone(),
                    },
                )
                .await
                .unwrap(),
            ExecutionValue::Count(1)
        );

        for aggregate in [
            ir::AggregatePlan::Group(property.clone()),
            ir::AggregatePlan::GroupCount(property.clone()),
            ir::AggregatePlan::AggregateBy {
                function: AggregateFunction::Mean,
                property,
            },
        ] {
            assert!(matches!(
                context
                    .aggregate(
                        ExecutionValue::Stream(vec![ExecutionRow::empty()]),
                        &aggregate,
                    )
                    .await,
                Err(HelixDbError::Query(message)) if message.contains("empty row")
            ));
        }
    }

    #[test]
    fn aggregate_numeric_values_cover_numeric_strings_and_rejected_shapes() {
        assert_eq!(aggregate_numeric_value(&DbPropertyValue::I64(7)), Some(7.0));
        assert_eq!(
            aggregate_numeric_value(&DbPropertyValue::F64(1.5)),
            Some(1.5)
        );
        assert_eq!(
            aggregate_numeric_value(&DbPropertyValue::F32(2.5)),
            Some(2.5)
        );
        assert_eq!(
            aggregate_numeric_value(&DbPropertyValue::String("3.5".to_string())),
            Some(3.5)
        );
        assert_eq!(
            aggregate_numeric_value(&DbPropertyValue::String("invalid".to_string())),
            None
        );
        assert_eq!(
            aggregate_numeric_value(&DbPropertyValue::Object(BTreeMap::new())),
            None
        );
    }
}
