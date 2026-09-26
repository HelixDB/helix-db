//! Materialized boundary values and their incremental item representation.
use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Shape {
    Rows,
    Scalars,
    Count,
    Bool,
    Folded,
    Lifecycle,
}

impl Shape {
    pub(super) fn of(value: &ExecutionValue) -> Self {
        match value {
            ExecutionValue::Stream(_) => Self::Rows,
            ExecutionValue::Scalars(_) => Self::Scalars,
            ExecutionValue::Count(_) => Self::Count,
            ExecutionValue::Bool(_) => Self::Bool,
            ExecutionValue::FoldedStream(_) => Self::Folded,
            ExecutionValue::IndexDdlReceipt(_) | ExecutionValue::IndexOperationStatus(_) => {
                Self::Lifecycle
            }
        }
    }

    pub(super) fn empty(self) -> Option<ExecutionValue> {
        Some(match self {
            Self::Rows => ExecutionValue::Stream(Vec::new()),
            Self::Scalars => ExecutionValue::Scalars(Vec::new()),
            Self::Count => ExecutionValue::Count(0),
            Self::Bool => ExecutionValue::Bool(false),
            Self::Folded => ExecutionValue::FoldedStream(FoldedStream::new(Vec::new())),
            Self::Lifecycle => return None,
        })
    }

    /// Row-only operations reject incompatible values even when no item is
    /// produced. Validation belongs to the operator contract, not its loop.
    pub(super) fn require_rows(self, operation: &str) -> Result<()> {
        let kind = match self {
            Self::Rows => return Ok(()),
            Self::Scalars => "scalar items",
            Self::Count => "count",
            Self::Bool => "boolean",
            Self::Folded => "folded stream; use unfold first",
            Self::Lifecycle => "index lifecycle value",
        };
        Err(HelixDbError::Query(format!(
            "{operation} expected stream input, got {kind}"
        )))
    }

    /// Membership reads elements from either ordinary rows or a folded row
    /// collection. Validate only the shape here; build the set when polled.
    pub(super) fn require_membership(self) -> Result<()> {
        match self {
            Self::Rows | Self::Folded => Ok(()),
            Self::Scalars | Self::Count | Self::Bool | Self::Lifecycle => {
                self.require_rows("membership operand")
            }
        }
    }

    /// The output shape is independent of cardinality for every projection.
    pub(super) fn projection(self, projection: &ir::ProjectionPlan) -> Result<Self> {
        match self {
            Self::Lifecycle => return Err(HelixDbError::Query(
                "project cannot consume an index lifecycle value".into(),
            )),
            Self::Folded => self.require_rows("project")?,
            Self::Rows => {},
            Self::Scalars | Self::Count | Self::Bool => match projection {
                ir::ProjectionPlan::Id | ir::ProjectionPlan::Exists => {},
                ir::ProjectionPlan::Values(_)
                | ir::ProjectionPlan::ValueMap(_)
                | ir::ProjectionPlan::Project(_)
                | ir::ProjectionPlan::ProjectBindings { .. }
                | ir::ProjectionPlan::Label
                | ir::ProjectionPlan::EdgeProperties => return Err(HelixDbError::Query(format!(
                    "project {projection:?} expected element stream input, got scalar terminal input"
                ))),
            },
        }
        Ok(match projection {
            ir::ProjectionPlan::Exists => Self::Bool,
            ir::ProjectionPlan::Id
            | ir::ProjectionPlan::Values(_)
            | ir::ProjectionPlan::ValueMap(_)
            | ir::ProjectionPlan::Project(_)
            | ir::ProjectionPlan::ProjectBindings { .. }
            | ir::ProjectionPlan::Label
            | ir::ProjectionPlan::EdgeProperties => Self::Scalars,
        })
    }

    pub(super) fn window(self, operation: &str) -> Result<Self> {
        match self {
            Self::Lifecycle => Err(HelixDbError::Query(format!(
                "{operation} cannot consume an index lifecycle value"
            ))),
            Self::Rows => Ok(Self::Rows),
            Self::Scalars | Self::Count | Self::Bool => Ok(Self::Scalars),
            Self::Folded => Err(HelixDbError::Query(format!(
                "{operation} expected stream input, got folded stream; use unfold first"
            ))),
        }
    }
}

pub(super) enum Items {
    Rows(std::vec::IntoIter<ExecutionRow>),
    Scalars(std::vec::IntoIter<ExecutionScalar>),
    Terminal(Option<ExecutionValue>),
}

impl Items {
    pub(super) fn new(value: ExecutionValue) -> Self {
        match value {
            ExecutionValue::Stream(rows) => Self::Rows(rows.into_iter()),
            ExecutionValue::Scalars(values) => Self::Scalars(values.into_iter()),
            value @ ExecutionValue::FoldedStream(_)
            | value @ ExecutionValue::Count(_)
            | value @ ExecutionValue::Bool(_)
            | value @ ExecutionValue::IndexDdlReceipt(_)
            | value @ ExecutionValue::IndexOperationStatus(_) => Self::Terminal(Some(value)),
        }
    }

    pub(super) fn next(&mut self) -> Option<ExecutionValue> {
        match self {
            Self::Rows(rows) => rows.next().map(|row| ExecutionValue::Stream(vec![row])),
            Self::Scalars(values) => values
                .next()
                .map(|value| ExecutionValue::Scalars(vec![value])),
            Self::Terminal(value) => value.take(),
        }
    }
}

pub(super) fn append(out: &mut ExecutionValue, item: ExecutionValue) -> Result<()> {
    match (out, item) {
        (ExecutionValue::Stream(out), ExecutionValue::Stream(mut rows)) => out.append(&mut rows),
        (ExecutionValue::Scalars(out), ExecutionValue::Scalars(mut items)) => {
            out.append(&mut items)
        }
        (out @ ExecutionValue::Count(_), item @ ExecutionValue::Count(_))
        | (out @ ExecutionValue::Bool(_), item @ ExecutionValue::Bool(_))
        | (out @ ExecutionValue::FoldedStream(_), item @ ExecutionValue::FoldedStream(_)) => {
            *out = item
        }
        _ => {
            return Err(HelixDbError::InvariantViolation(
                "pull output changed value shape".into(),
            ))
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn projection_shape_contract_matches_runtime_for_every_value_kind() {
        let db = test_support::open_db("projection-shape-contract").await;
        let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
        let name = test_support::name("value");
        let projections = [
            ir::ProjectionPlan::Id,
            ir::ProjectionPlan::Exists,
            ir::ProjectionPlan::Label,
            ir::ProjectionPlan::EdgeProperties,
            ir::ProjectionPlan::Values(
                ir::PropertyNames::new(ir::AtLeast::from_one(name.clone())).unwrap(),
            ),
            ir::ProjectionPlan::ValueMap(ir::PropertySelection::All),
            ir::ProjectionPlan::Project(
                ir::ProjectionItems::new(ir::AtLeast::from_one(ir::ProjectionItem::Property {
                    source: name.clone(),
                    alias: name.clone(),
                }))
                .unwrap(),
            ),
            ir::ProjectionPlan::ProjectBindings {
                projections: ir::BindingProjectionItems::new(ir::AtLeast::from_one(
                    ir::BindingProjectionPlan::Property {
                        target: ir::BindingTargetPlan::Current,
                        source: name.clone(),
                        alias: name,
                    },
                ))
                .unwrap(),
                dedup: ir::ProjectionDedupMode::Distinct,
            },
        ];
        for shape in [
            Shape::Rows,
            Shape::Scalars,
            Shape::Count,
            Shape::Bool,
            Shape::Folded,
            Shape::Lifecycle,
        ] {
            assert_eq!(shape.require_rows("test").is_ok(), shape == Shape::Rows);
            for projection in &projections {
                let actual = shape.projection(projection);
                let Some(value) = shape.empty() else {
                    assert!(actual.is_err());
                    continue;
                };
                let expected = ctx
                    .project(value, projection)
                    .await
                    .map(|value| Shape::of(&value));
                assert_eq!(
                    actual.map_err(|error| error.to_string()),
                    expected.map_err(|error| error.to_string())
                );
            }
        }
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn empty_non_row_merge_inputs_are_rejected_before_polling() {
        for mode in [
            exec::ExecMergeMode::Concat,
            exec::ExecMergeMode::Union,
            exec::ExecMergeMode::Intersect,
        ] {
            for value in [
                ExecutionValue::Scalars(Vec::new()),
                ExecutionValue::Count(0),
                ExecutionValue::Bool(false),
                ExecutionValue::FoldedStream(FoldedStream::new(Vec::new())),
            ] {
                let cursor = Cursor::materialized(value).unwrap();
                assert!(Cursor::merge(vec![cursor], mode).is_err());
            }
        }
    }

    #[tokio::test]
    async fn distinct_matches_eager_numeric_identity_under_limits() {
        let db = test_support::open_db("pull-distinct-numeric-identity").await;
        let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
        let numeric = [
            DbPropertyValue::I64(42),
            DbPropertyValue::F64(42.0),
            DbPropertyValue::F32(42.0),
            DbPropertyValue::I64(7),
            DbPropertyValue::String("42".into()),
            DbPropertyValue::Bool(true),
        ];
        for objects in [false, true] {
            let mut values = numeric
                .iter()
                .cloned()
                .map(|value| {
                    if objects {
                        ExecutionScalar::Object(BTreeMap::from([("score".into(), value)]))
                    } else {
                        ExecutionScalar::Value(value)
                    }
                })
                .collect::<Vec<_>>();
            values.extend([
                ExecutionScalar::NodeId(42),
                ExecutionScalar::EdgeId(42),
                ExecutionScalar::String("42".into()),
            ]);
            let input = ExecutionValue::Scalars(values);
            let expected = ctx.distinct(input.clone()).unwrap();
            for take in [0, 1, 2, 100] {
                let bound = ir::StreamBoundPlan::Literal(take);
                let limit = exec::ExecOp::Limit {
                    count: bound.clone(),
                };
                let actual = Cursor::materialized(input.clone())
                    .unwrap()
                    .wrap(&ctx, &exec::ExecOp::Distinct)
                    .unwrap()
                    .wrap(&ctx, &limit)
                    .unwrap()
                    .drain(&mut ctx)
                    .await
                    .unwrap();
                assert_eq!(actual, ctx.limit(expected.clone(), &bound).unwrap());
            }
        }
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn distinct_binding_projection_matches_eager_numeric_identity_under_limits() {
        let db = test_support::open_db("pull-projection-distinct-numeric-identity").await;
        let mut rows = Vec::new();
        for value in [
            helix_ast::value::PropertyValue::I64(42),
            helix_ast::value::PropertyValue::F64(42.0),
            helix_ast::value::PropertyValue::F32(42.0),
            helix_ast::value::PropertyValue::I64(7),
        ] {
            let id =
                test_support::add_node_with_properties(&db, "Metric", vec![("score", value)]).await;
            rows.push(ExecutionRow::current(ElementRef::Node(id)));
        }
        let projection = ir::ProjectionPlan::ProjectBindings {
            projections: ir::BindingProjectionItems::new(ir::AtLeast::from_one(
                ir::BindingProjectionPlan::Property {
                    target: ir::BindingTargetPlan::Current,
                    source: test_support::name("score"),
                    alias: test_support::name("score"),
                },
            ))
            .unwrap(),
            dedup: ir::ProjectionDedupMode::Distinct,
        };
        let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
        for input in [
            ExecutionValue::Stream(Vec::new()),
            ExecutionValue::Stream(rows),
        ] {
            let expected = ctx.project(input.clone(), &projection).await.unwrap();
            let op = exec::ExecOp::Project {
                projection: projection.clone(),
            };
            for take in [0, 1, 2, 100] {
                let bound = ir::StreamBoundPlan::Literal(take);
                let limit = exec::ExecOp::Limit {
                    count: bound.clone(),
                };
                let actual = Cursor::materialized(input.clone())
                    .unwrap()
                    .wrap(&ctx, &op)
                    .unwrap()
                    .wrap(&ctx, &limit)
                    .unwrap()
                    .drain(&mut ctx)
                    .await
                    .unwrap();
                assert_eq!(actual, ctx.limit(expected.clone(), &bound).unwrap());
            }
        }
        db.close().await.unwrap();
    }
}
