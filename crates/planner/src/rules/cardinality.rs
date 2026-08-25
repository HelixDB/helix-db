//! Logical cardinality implementation and physical program construction.

use helix_ast::{expr::Expr, query, value};

use crate::{catalog, context, cost, exec, ir, logical, optimizer, physical, properties};

use super::{KnownRuleId, RuleId, RuleKind, RuleMetadata, RuleRejection};

/// Implement logical cardinality as payload-complete physical alternatives.
pub struct StreamCardinalityImplementationRule {
    metadata: RuleMetadata,
}

impl Default for StreamCardinalityImplementationRule {
    fn default() -> Self {
        Self {
            metadata: RuleMetadata::new(
                RuleId::known(KnownRuleId::SeedStreamCardinality),
                RuleKind::Implementation,
            ),
        }
    }
}

impl optimizer::OptimizerRule for StreamCardinalityImplementationRule {
    fn metadata(&self) -> &RuleMetadata {
        &self.metadata
    }

    fn apply(&self, input: optimizer::RuleInput<'_>) -> optimizer::RuleResult {
        let logical::LogicalExpr::StreamCardinality(cardinality) = input.expr else {
            return optimizer::RuleResult::NotApplicable;
        };
        let plans = match count_plans(cardinality.input(), &input) {
            Ok(plans) => plans,
            Err(rejection) => return optimizer::RuleResult::Rejected(rejection),
        };
        let input_delivered = super::root_stream_delivered_properties(
            cardinality.input(),
            input.storage,
            input.stats,
        );
        let delivered = super::cardinality_output_delivered(input_delivered);
        let alternatives = plans
            .into_iter()
            .map(|plan| {
                let cost = count_cost(&plan, input.stats, input.storage);
                physical::PhysicalAlternative::new(
                    physical::PhysicalExpr::Cardinality(Box::new(
                        physical::PhysicalCountPlan::new(plan),
                    )),
                    delivered.clone(),
                    cost,
                )
            })
            .collect::<Vec<_>>();
        let alternatives = ir::AtLeast::<_, 1>::try_from_vec(alternatives)
            .expect("cardinality implementation always produces an alternative");
        optimizer::RuleResult::Applied(optimizer::RuleEffect::Physical(alternatives))
    }
}

fn count_plans(
    input: &logical::RootStream,
    rule: &optimizer::RuleInput<'_>,
) -> Result<Vec<exec::ExecCountPlan>, RuleRejection> {
    match input {
        logical::RootStream::Access(access) => access_count_plans(access, rule),
        logical::RootStream::Pipeline(pipeline) if has_variable_write(pipeline.ops()) => {
            Ok(vec![exec::ExecCountPlan::InputRows {
                window: exec::ExecCountWindowPlan::identity(),
            }])
        }
        logical::RootStream::Pipeline(pipeline) => Ok(vec![root_pipeline_count(pipeline, rule)?]),
        logical::RootStream::Project(_)
        | logical::RootStream::Cardinality(_)
        | logical::RootStream::Aggregate(_) => Ok(vec![exec::ExecCountPlan::InputScalars {
            window: exec::ExecCountWindowPlan::identity(),
        }]),
        logical::RootStream::VariableSource(source) => {
            Ok(vec![exec::ExecCountPlan::RuntimeInput {
                input: exec::ExecRuntimeInputPlan::Variable(source.variable().clone()),
                window: exec::ExecCountWindowPlan::identity(),
            }])
        }
        logical::RootStream::Mutation(_)
        | logical::RootStream::Branch(_)
        | logical::RootStream::Repeat(_)
        | logical::RootStream::Reserved(_)
        | logical::RootStream::VariableWrite(_) => Ok(vec![exec::ExecCountPlan::InputRows {
            window: exec::ExecCountWindowPlan::identity(),
        }]),
    }
}

fn access_count_plans(
    access: &logical::AccessStream,
    rule: &optimizer::RuleInput<'_>,
) -> Result<Vec<exec::ExecCountPlan>, RuleRejection> {
    match access {
        logical::AccessStream::Path(path) => {
            direct_access_plans(path, exec::ExecCountWindowPlan::identity(), rule)
        }
        logical::AccessStream::Order(order) => {
            direct_access_plans(order.access(), exec::ExecCountWindowPlan::identity(), rule)
        }
        logical::AccessStream::Distinct(distinct) => Ok(vec![exec::ExecCountPlan::Stream(
            exec::ExecCountStreamPlan {
                cursor: exec::ExecCountCursorPlan::Distinct {
                    input: Box::new(access_cursor(distinct.access(), rule)?),
                    plan: exec::ExecCountDistinctPlan::HashRows,
                },
                window: exec::ExecCountWindowPlan::identity(),
            },
        )]),
        logical::AccessStream::Window(window) => {
            direct_access_plans(window.access(), static_window(window.window()), rule)
        }
        logical::AccessStream::Filter(filter) => {
            let (_, late_bound_params) = rule
                .cardinality_bindings()
                .expect("cardinality helpers are only called by the cardinality rule");
            if !late_bound_params.is_empty() {
                match super::access::index_access_filter(filter, rule.indexes, rule.planner_limits)
                {
                    super::access::AccessFilterRewrite::Rewritten(access) => {
                        return direct_access_plans(
                            &access,
                            exec::ExecCountWindowPlan::identity(),
                            rule,
                        );
                    }
                    super::access::AccessFilterRewrite::RewrittenPipeline(pipeline) => {
                        return access_pipeline_count(&pipeline, rule);
                    }
                    super::access::AccessFilterRewrite::NotApplicable => {}
                }
            }
            Ok(vec![exec::ExecCountPlan::Stream(
                exec::ExecCountStreamPlan {
                    cursor: exec::ExecCountCursorPlan::Filter {
                        input: Box::new(access_cursor(filter.access(), rule)?),
                        predicate: filter.predicate().clone(),
                    },
                    window: exec::ExecCountWindowPlan::identity(),
                },
            )])
        }
        logical::AccessStream::Pipeline(pipeline) if has_variable_write(pipeline.ops()) => {
            Ok(vec![exec::ExecCountPlan::InputRows {
                window: exec::ExecCountWindowPlan::identity(),
            }])
        }
        logical::AccessStream::Pipeline(pipeline) => access_pipeline_count(pipeline, rule),
    }
}

fn access_pipeline_count(
    pipeline: &logical::AccessPipeline,
    rule: &optimizer::RuleInput<'_>,
) -> Result<Vec<exec::ExecCountPlan>, RuleRejection> {
    let (prefix, window) = trailing_count_window(pipeline.ops())?;
    if prefix.is_empty() {
        return direct_access_plans(pipeline.access(), window, rule);
    }
    let cursor = fold_cursor(access_cursor(pipeline.access(), rule)?, prefix)?;
    Ok(vec![exec::ExecCountPlan::Stream(
        exec::ExecCountStreamPlan { cursor, window },
    )])
}

fn root_pipeline_count(
    pipeline: &logical::RootPipeline,
    rule: &optimizer::RuleInput<'_>,
) -> Result<exec::ExecCountPlan, RuleRejection> {
    let (prefix, window) = trailing_count_window(pipeline.ops())?;
    let cursor = match pipeline.input() {
        logical::RootStream::Access(access) => access_stream_cursor(access, rule)?,
        logical::RootStream::VariableSource(source) => exec::ExecCountCursorPlan::RuntimeInput(
            exec::ExecRuntimeInputPlan::Variable(source.variable().clone()),
        ),
        logical::RootStream::Mutation(_)
        | logical::RootStream::Branch(_)
        | logical::RootStream::Repeat(_)
        | logical::RootStream::Pipeline(_)
        | logical::RootStream::Reserved(_)
        | logical::RootStream::Project(_)
        | logical::RootStream::Cardinality(_)
        | logical::RootStream::Aggregate(_)
        | logical::RootStream::VariableWrite(_) => exec::ExecCountCursorPlan::InputRows,
    };
    let cursor = fold_cursor(cursor, prefix)?;
    Ok(exec::ExecCountPlan::Stream(exec::ExecCountStreamPlan {
        cursor,
        window,
    }))
}

fn has_variable_write(ops: &[logical::StreamPipelineOp]) -> bool {
    ops.iter()
        .any(|op| matches!(op, logical::StreamPipelineOp::VariableWrite { .. }))
}

fn static_window(window: logical::AccessWindowRange) -> exec::ExecCountWindowPlan {
    let window_plan = exec::ExecCountWindowPlan::identity()
        .then_skip(exec::ExecUsizeExpr::literal(window.start()));
    match window.end() {
        Some(end) => window_plan.then_limit(exec::ExecUsizeExpr::literal(
            end.saturating_sub(window.start()),
        )),
        None => window_plan,
    }
}

fn trailing_count_window(
    ops: &[logical::StreamPipelineOp],
) -> Result<(&[logical::StreamPipelineOp], exec::ExecCountWindowPlan), RuleRejection> {
    let suffix_start = ops
        .iter()
        .rposition(|op| {
            !matches!(
                op,
                logical::StreamPipelineOp::Window { .. }
                    | logical::StreamPipelineOp::Limit { .. }
                    | logical::StreamPipelineOp::Skip { .. }
                    | logical::StreamPipelineOp::Range { .. }
                    | logical::StreamPipelineOp::Order { .. }
            )
        })
        .map_or(0, |position| position.saturating_add(1));
    let window = ops[suffix_start..]
        .iter()
        .try_fold(exec::ExecCountWindowPlan::identity(), |window, op| {
            append_window(window, op)
        })?;
    Ok((&ops[..suffix_start], window))
}

fn append_window(
    window: exec::ExecCountWindowPlan,
    op: &logical::StreamPipelineOp,
) -> Result<exec::ExecCountWindowPlan, RuleRejection> {
    match op {
        logical::StreamPipelineOp::Window {
            window: static_range,
        } => {
            let window = window.then_skip(exec::ExecUsizeExpr::literal(static_range.start()));
            Ok(match static_range.end() {
                Some(end) => window.then_limit(exec::ExecUsizeExpr::literal(
                    end.saturating_sub(static_range.start()),
                )),
                None => window,
            })
        }
        logical::StreamPipelineOp::Limit { count } => Ok(window.then_limit(bound_expr(count)?)),
        logical::StreamPipelineOp::Skip { count } => Ok(window.then_skip(bound_expr(count)?)),
        logical::StreamPipelineOp::Range { range } => {
            let (start, end) = range_exprs(range)?;
            Ok(window.then_range(start, end))
        }
        logical::StreamPipelineOp::Order { .. } => Ok(window),
        logical::StreamPipelineOp::Filter { .. }
        | logical::StreamPipelineOp::Expand { .. }
        | logical::StreamPipelineOp::VectorSearch { .. }
        | logical::StreamPipelineOp::TextSearch { .. }
        | logical::StreamPipelineOp::Variable { .. }
        | logical::StreamPipelineOp::VariableWrite { .. }
        | logical::StreamPipelineOp::Distinct => Err(rejection("non_window_suffix_operator")),
    }
}

fn fold_cursor(
    cursor: exec::ExecCountCursorPlan,
    ops: &[logical::StreamPipelineOp],
) -> Result<exec::ExecCountCursorPlan, RuleRejection> {
    let mut cursor = cursor;
    let mut positioned_window = exec::ExecCountWindowPlan::identity();
    let mut has_positioned_window = false;
    macro_rules! flush_positioned_window {
        () => {
            if has_positioned_window {
                cursor = exec::ExecCountCursorPlan::Window {
                    input: Box::new(cursor),
                    window: positioned_window,
                };
                positioned_window = exec::ExecCountWindowPlan::identity();
                has_positioned_window = false;
            }
        };
    }
    for op in ops {
        match op {
            logical::StreamPipelineOp::Window { .. }
            | logical::StreamPipelineOp::Limit { .. }
            | logical::StreamPipelineOp::Skip { .. }
            | logical::StreamPipelineOp::Range { .. } => {
                positioned_window = append_window(positioned_window, op)?;
                has_positioned_window = true;
            }
            logical::StreamPipelineOp::VariableWrite { .. } => {
                return Err(rejection("count_cursor_crossed_variable_write_barrier"));
            }
            logical::StreamPipelineOp::Filter { predicate } => {
                flush_positioned_window!();
                cursor = exec::ExecCountCursorPlan::Filter {
                    input: Box::new(cursor),
                    predicate: predicate.clone(),
                };
            }
            logical::StreamPipelineOp::Order { ordering } => {
                flush_positioned_window!();
                cursor = exec::ExecCountCursorPlan::Order {
                    input: Box::new(cursor),
                    plan: ir::OrderPlan::ExplicitSort(ordering.clone()),
                };
            }
            logical::StreamPipelineOp::Expand { plan } => {
                flush_positioned_window!();
                cursor = exec::ExecCountCursorPlan::Expand {
                    input: Box::new(cursor),
                    plan: plan.clone(),
                };
            }
            logical::StreamPipelineOp::VectorSearch { plan } => {
                flush_positioned_window!();
                cursor = exec::ExecCountCursorPlan::VectorSearch {
                    input: Box::new(cursor),
                    plan: plan.clone(),
                };
            }
            logical::StreamPipelineOp::TextSearch { plan } => {
                flush_positioned_window!();
                cursor = exec::ExecCountCursorPlan::TextSearch {
                    input: Box::new(cursor),
                    plan: plan.clone(),
                };
            }
            logical::StreamPipelineOp::Variable { op } => {
                flush_positioned_window!();
                cursor = exec::ExecCountCursorPlan::Variable {
                    input: Box::new(cursor),
                    op: op.clone(),
                };
            }
            logical::StreamPipelineOp::Distinct => {
                flush_positioned_window!();
                cursor = exec::ExecCountCursorPlan::Distinct {
                    input: Box::new(cursor),
                    plan: exec::ExecCountDistinctPlan::HashRows,
                };
            }
        }
    }
    if has_positioned_window {
        cursor = exec::ExecCountCursorPlan::Window {
            input: Box::new(cursor),
            window: positioned_window,
        };
    }
    Ok(cursor)
}

fn bound_expr(bound: &ir::StreamBoundPlan) -> Result<exec::ExecUsizeExpr, RuleRejection> {
    match bound {
        ir::StreamBoundPlan::Literal(value) => Ok(exec::ExecUsizeExpr::literal(*value)),
        ir::StreamBoundPlan::Expr(expr) => match expr.expr() {
            Expr::Param(param) => Ok(exec::ExecUsizeExpr::Param(
                ir::NonEmptyString::new(param.clone())
                    .expect("validated stream-bound parameters are non-empty"),
            )),
            Expr::Property(_)
            | Expr::Id
            | Expr::Timestamp
            | Expr::DateTimeNow
            | Expr::Constant(_)
            | Expr::Add { .. }
            | Expr::Sub { .. }
            | Expr::Mul { .. }
            | Expr::Div { .. }
            | Expr::Mod { .. }
            | Expr::Neg { .. }
            | Expr::Case { .. } => Err(rejection("unsupported_count_window_expression")),
        },
    }
}

fn range_exprs(
    range: &ir::StreamRangePlan,
) -> Result<(exec::ExecUsizeExpr, exec::ExecUsizeExpr), RuleRejection> {
    match range {
        ir::StreamRangePlan::Literal(range) => Ok((
            exec::ExecUsizeExpr::literal(range.start()),
            exec::ExecUsizeExpr::literal(range.end()),
        )),
        ir::StreamRangePlan::Dynamic(range) => {
            Ok((bound_expr(range.start())?, bound_expr(range.end())?))
        }
    }
}

fn direct_access_plans(
    access: &logical::AccessPath,
    window: exec::ExecCountWindowPlan,
    rule: &optimizer::RuleInput<'_>,
) -> Result<Vec<exec::ExecCountPlan>, RuleRejection> {
    match access {
        logical::AccessPath::Node(path) => node_count_plans(path.source().as_ref(), window, rule),
        logical::AccessPath::Edge(path) => edge_count_plans(path.source().as_ref(), window, rule),
    }
}

fn node_count_plans(
    source: &ir::NodeAccessPlan,
    window: exec::ExecCountWindowPlan,
    rule: &optimizer::RuleInput<'_>,
) -> Result<Vec<exec::ExecCountPlan>, RuleRejection> {
    let plan = match source {
        ir::NodeAccessPlan::Empty => exec::ExecCountPlan::Constant(0),
        ir::NodeAccessPlan::PointIds { ids } => exec::ExecCountPlan::NodePointReads {
            ids: ids.clone(),
            window,
        },
        ir::NodeAccessPlan::FromParam { param } => exec::ExecCountPlan::NodeRuntimeInput {
            input: exec::ExecRuntimeInputPlan::Param(param.clone()),
            window,
        },
        ir::NodeAccessPlan::FromVar { variable } => exec::ExecCountPlan::NodeRuntimeInput {
            input: exec::ExecRuntimeInputPlan::Variable(variable.clone()),
            window,
        },
        ir::NodeAccessPlan::AllScan => exec::ExecCountPlan::NodeFullScan { window },
        ir::NodeAccessPlan::LabelScan { label } => exec::ExecCountPlan::NodeLabelBitmap {
            label: label.clone(),
            window,
        },
        ir::NodeAccessPlan::EqualityIndex { index, key, value } => {
            return Ok(vec![node_equality_count(index, key, value, window, rule)?]);
        }
        ir::NodeAccessPlan::RangeIndex { index, key, range } => {
            exec::ExecCountPlan::NodeRange(exec::ExecNodeRangeCountPlan {
                driver: exec::ExecNodeVerifiedRangeScanPlan {
                    index: index.clone(),
                    key: key.clone(),
                    range: range.clone(),
                },
                membership: exec::ExecNodeRangeMembershipPlan::All,
                window,
            })
        }
        ir::NodeAccessPlan::VectorSearch {
            key,
            index,
            query_vector,
            k,
        } => exec::ExecCountPlan::NodeVectorSearch(exec::ExecNodeVectorSearchCountPlan {
            key: key.clone(),
            index: index.clone(),
            query_vector: query_vector.clone(),
            k: k.clone(),
            window,
        }),
        ir::NodeAccessPlan::TextSearch {
            key,
            index,
            query_text,
            k,
        } => exec::ExecCountPlan::NodeTextSearch(exec::ExecNodeTextSearchCountPlan {
            key: key.clone(),
            index: index.clone(),
            query_text: query_text.clone(),
            k: k.clone(),
            window,
        }),
        ir::NodeAccessPlan::Intersect(children) => {
            return node_intersection_count_plans(children, window, rule);
        }
        ir::NodeAccessPlan::Union(children) => {
            if let Some(bitmap) = node_bitmap_set(children, true, rule)? {
                return Ok(vec![exec::ExecCountPlan::NodeBitmap(
                    exec::ExecNodeBitmapCountPlan { bitmap, window },
                )]);
            }
            exec::ExecCountPlan::Stream(exec::ExecCountStreamPlan {
                cursor: node_set_cursor(children, true, rule)?,
                window,
            })
        }
        ir::NodeAccessPlan::ScanThenFilter { source, residual } => {
            exec::ExecCountPlan::Stream(exec::ExecCountStreamPlan {
                cursor: exec::ExecCountCursorPlan::Filter {
                    input: Box::new(node_cursor(source.as_ref(), rule)?),
                    predicate: residual.clone(),
                },
                window,
            })
        }
    };
    Ok(vec![plan])
}

fn edge_count_plans(
    source: &ir::EdgeAccessPlan,
    window: exec::ExecCountWindowPlan,
    rule: &optimizer::RuleInput<'_>,
) -> Result<Vec<exec::ExecCountPlan>, RuleRejection> {
    let plan = match source {
        ir::EdgeAccessPlan::Empty => exec::ExecCountPlan::Constant(0),
        ir::EdgeAccessPlan::PointIds { ids } => exec::ExecCountPlan::EdgePointReads {
            ids: ids.clone(),
            window,
        },
        ir::EdgeAccessPlan::FromParam { param } => exec::ExecCountPlan::EdgeRuntimeInput {
            input: exec::ExecRuntimeInputPlan::Param(param.clone()),
            window,
        },
        ir::EdgeAccessPlan::FromVar { variable } => exec::ExecCountPlan::EdgeRuntimeInput {
            input: exec::ExecRuntimeInputPlan::Variable(variable.clone()),
            window,
        },
        ir::EdgeAccessPlan::AllScan => exec::ExecCountPlan::EdgeFullScan { window },
        ir::EdgeAccessPlan::LabelScan { label } => exec::ExecCountPlan::EdgeLabelBitmap {
            label: label.clone(),
            window,
        },
        ir::EdgeAccessPlan::EqualityIndex { index, key, value } => {
            return Ok(vec![edge_equality_count(index, key, value, window, rule)?]);
        }
        ir::EdgeAccessPlan::RangeIndex { index, key, range } => {
            exec::ExecCountPlan::EdgeRange(exec::ExecEdgeRangeCountPlan {
                driver: exec::ExecEdgeVerifiedRangeScanPlan {
                    index: index.clone(),
                    key: key.clone(),
                    range: range.clone(),
                },
                membership: exec::ExecEdgeRangeMembershipPlan::All,
                window,
            })
        }
        ir::EdgeAccessPlan::VectorSearch {
            key,
            index,
            query_vector,
            k,
        } => exec::ExecCountPlan::EdgeVectorSearch(exec::ExecEdgeVectorSearchCountPlan {
            key: key.clone(),
            index: index.clone(),
            query_vector: query_vector.clone(),
            k: k.clone(),
            window,
        }),
        ir::EdgeAccessPlan::TextSearch {
            key,
            index,
            query_text,
            k,
        } => exec::ExecCountPlan::EdgeTextSearch(exec::ExecEdgeTextSearchCountPlan {
            key: key.clone(),
            index: index.clone(),
            query_text: query_text.clone(),
            k: k.clone(),
            window,
        }),
        ir::EdgeAccessPlan::Intersect(children) => {
            return edge_intersection_count_plans(children, window, rule);
        }
        ir::EdgeAccessPlan::Union(children) => {
            if let Some(bitmap) = edge_bitmap_set(children, true, rule)? {
                return Ok(vec![exec::ExecCountPlan::EdgeBitmap(
                    exec::ExecEdgeBitmapCountPlan { bitmap, window },
                )]);
            }
            exec::ExecCountPlan::Stream(exec::ExecCountStreamPlan {
                cursor: edge_set_cursor(children, true, rule)?,
                window,
            })
        }
        ir::EdgeAccessPlan::ScanThenFilter { source, residual } => {
            exec::ExecCountPlan::Stream(exec::ExecCountStreamPlan {
                cursor: exec::ExecCountCursorPlan::Filter {
                    input: Box::new(edge_cursor(source.as_ref(), rule)?),
                    predicate: residual.clone(),
                },
                window,
            })
        }
    };
    Ok(vec![plan])
}

enum EqualityValue {
    Indexed(exec::ExecIndexedEqualityValue),
    AuthoritativeNull,
    NonReflexive,
    Dynamic(ir::NonEmptyString),
    DynamicSet(ir::RuntimeEqualitySet),
}

fn classify_equality(
    value: &ir::IndexValue,
    rule: &optimizer::RuleInput<'_>,
) -> Result<EqualityValue, RuleRejection> {
    let (params, late_bound_params) = rule
        .cardinality_bindings()
        .expect("cardinality helpers are only called by the cardinality rule");
    let literal = match value {
        ir::IndexValue::Literal(literal) => literal.clone(),
        ir::IndexValue::ParamSet(values) => {
            return Ok(EqualityValue::DynamicSet(values.clone()));
        }
        // A foreach frame expands object fields into the parameter namespace.
        // The AST exposes the container name but cannot enumerate every field
        // that an iteration may shadow, so any active runtime parameter scope
        // makes equality parameters inside that scope genuinely late-bound.
        ir::IndexValue::Param(param) if !late_bound_params.is_empty() => {
            return Ok(EqualityValue::Dynamic(param.clone()));
        }
        ir::IndexValue::Param(param) => {
            let value = if let Some(value) = params.values.get(param) {
                value.clone()
            } else if let Some(value) = params.query_values.get(param) {
                query_property_value(value)?
            } else {
                return Err(rejection("missing_planning_equality_parameter"));
            };
            ir::SecondaryIndexLiteral::new(value)
                .map_err(|_| rejection("unsupported_planning_equality_parameter"))?
        }
    };
    match literal.semantics() {
        ir::LiteralEqualityIndexValueSemantics::Indexed => Ok(EqualityValue::Indexed(
            exec::ExecIndexedEqualityValue::try_from(literal)
                .expect("indexed literal semantics satisfy the executable wrapper"),
        )),
        ir::LiteralEqualityIndexValueSemantics::AuthoritativeNull => {
            Ok(EqualityValue::AuthoritativeNull)
        }
        ir::LiteralEqualityIndexValueSemantics::NonReflexive => Ok(EqualityValue::NonReflexive),
    }
}

fn query_property_value(value: &query::QueryValue) -> Result<value::PropertyValue, RuleRejection> {
    match value {
        query::QueryValue::Null => Ok(value::PropertyValue::Null),
        query::QueryValue::Bool(value) => Ok(value::PropertyValue::Bool(*value)),
        query::QueryValue::I64(value) => Ok(value::PropertyValue::I64(*value)),
        query::QueryValue::F64(value) => Ok(value::PropertyValue::F64(*value)),
        query::QueryValue::F32(value) => Ok(value::PropertyValue::F32(*value)),
        query::QueryValue::String(value) => Ok(value::PropertyValue::String(value.clone())),
        query::QueryValue::Array(_) | query::QueryValue::Object(_) => {
            Err(rejection("unsupported_planning_equality_parameter"))
        }
    }
}

fn node_equality_count(
    index: &catalog::NodeEqualityIndexMeta,
    key: &catalog::ScopedPropertyKey,
    value: &ir::IndexValue,
    window: exec::ExecCountWindowPlan,
    rule: &optimizer::RuleInput<'_>,
) -> Result<exec::ExecCountPlan, RuleRejection> {
    Ok(match classify_equality(value, rule)? {
        EqualityValue::Indexed(value) => match index.uniqueness {
            catalog::IndexUniqueness::NonUnique => {
                exec::ExecCountPlan::NodeBitmap(exec::ExecNodeBitmapCountPlan {
                    bitmap: exec::ExecNodeBitmapExpr::PointRead {
                        index: index
                            .clone()
                            .try_into()
                            .expect("non-unique catalog metadata satisfies the bitmap wrapper"),
                        key: key.clone(),
                        value,
                    },
                    window,
                })
            }
            catalog::IndexUniqueness::Unique => {
                let index = exec::ExecNodeUniqueEqualityIndex::try_from(index.clone())
                    .expect("unique catalog metadata satisfies the owner wrapper");
                exec::ExecCountPlan::NodeUnique(exec::ExecNodeUniqueCountPlan {
                    lookup: exec::ExecNodeUniqueOwnerReadPlan {
                        index,
                        key: key.clone(),
                        value: value.clone(),
                    },
                    verification: exec::ExecNodeAuthoritativeVerificationPlan {
                        key: key.clone(),
                        value,
                    },
                    window,
                })
            }
        },
        EqualityValue::AuthoritativeNull => {
            exec::ExecCountPlan::NodeAuthoritativeScan(exec::ExecNodeScanCountPlan {
                predicate: exec::ExecNodeAuthoritativeScanPredicate::NullEquality {
                    key: key.clone(),
                },
                window,
            })
        }
        EqualityValue::NonReflexive => exec::ExecCountPlan::Constant(0),
        EqualityValue::Dynamic(param) => {
            exec::ExecCountPlan::NodeDynamicEquality(exec::ExecNodeDynamicEqualityCountPlan {
                index: index.clone(),
                key: key.clone(),
                param,
                window,
            })
        }
        EqualityValue::DynamicSet(values) => {
            exec::ExecCountPlan::NodeDynamicMembership(exec::ExecNodeDynamicMembershipCountPlan {
                index: index.clone(),
                key: key.clone(),
                values,
                window,
            })
        }
    })
}

fn edge_equality_count(
    index: &catalog::EdgeEqualityIndexMeta,
    key: &catalog::ScopedPropertyKey,
    value: &ir::IndexValue,
    window: exec::ExecCountWindowPlan,
    rule: &optimizer::RuleInput<'_>,
) -> Result<exec::ExecCountPlan, RuleRejection> {
    Ok(match classify_equality(value, rule)? {
        EqualityValue::Indexed(value) => {
            exec::ExecCountPlan::EdgeBitmap(exec::ExecEdgeBitmapCountPlan {
                bitmap: exec::ExecEdgeBitmapExpr::PointRead {
                    index: exec::ExecEdgeNonUniqueEqualityIndex::new(index.clone()),
                    key: key.clone(),
                    value,
                },
                window,
            })
        }
        EqualityValue::AuthoritativeNull => {
            exec::ExecCountPlan::EdgeAuthoritativeScan(exec::ExecEdgeScanCountPlan {
                predicate: exec::ExecEdgeAuthoritativeScanPredicate::NullEquality {
                    key: key.clone(),
                },
                window,
            })
        }
        EqualityValue::NonReflexive => exec::ExecCountPlan::Constant(0),
        EqualityValue::Dynamic(param) => {
            exec::ExecCountPlan::EdgeDynamicEquality(exec::ExecEdgeDynamicEqualityCountPlan {
                index: index.clone(),
                key: key.clone(),
                param,
                window,
            })
        }
        EqualityValue::DynamicSet(values) => {
            exec::ExecCountPlan::EdgeDynamicMembership(exec::ExecEdgeDynamicMembershipCountPlan {
                index: index.clone(),
                key: key.clone(),
                values,
                window,
            })
        }
    })
}

fn node_cursor(
    source: &ir::NodeAccessPlan,
    rule: &optimizer::RuleInput<'_>,
) -> Result<exec::ExecCountCursorPlan, RuleRejection> {
    let identity = exec::ExecCountWindowPlan::identity();
    let plan = node_count_plans(source, identity, rule)?
        .into_iter()
        .next()
        .expect("node source produces a count plan");
    count_plan_cursor(plan)
}

fn edge_cursor(
    source: &ir::EdgeAccessPlan,
    rule: &optimizer::RuleInput<'_>,
) -> Result<exec::ExecCountCursorPlan, RuleRejection> {
    let identity = exec::ExecCountWindowPlan::identity();
    let plan = edge_count_plans(source, identity, rule)?
        .into_iter()
        .next()
        .expect("edge source produces a count plan");
    count_plan_cursor(plan)
}

fn access_cursor(
    access: &logical::AccessPath,
    rule: &optimizer::RuleInput<'_>,
) -> Result<exec::ExecCountCursorPlan, RuleRejection> {
    match access {
        logical::AccessPath::Node(path) => node_cursor(path.source().as_ref(), rule),
        logical::AccessPath::Edge(path) => edge_cursor(path.source().as_ref(), rule),
    }
}

fn access_stream_cursor(
    access: &logical::AccessStream,
    rule: &optimizer::RuleInput<'_>,
) -> Result<exec::ExecCountCursorPlan, RuleRejection> {
    match access {
        logical::AccessStream::Path(path) => access_cursor(path, rule),
        logical::AccessStream::Order(order) => Ok(exec::ExecCountCursorPlan::Order {
            input: Box::new(access_cursor(order.access(), rule)?),
            plan: ir::OrderPlan::ExplicitSort(order.ordering().clone()),
        }),
        logical::AccessStream::Distinct(distinct) => Ok(exec::ExecCountCursorPlan::Distinct {
            input: Box::new(access_cursor(distinct.access(), rule)?),
            plan: exec::ExecCountDistinctPlan::HashRows,
        }),
        logical::AccessStream::Window(window) => Ok(exec::ExecCountCursorPlan::Window {
            input: Box::new(access_cursor(window.access(), rule)?),
            window: static_window(window.window()),
        }),
        logical::AccessStream::Filter(filter) => Ok(exec::ExecCountCursorPlan::Filter {
            input: Box::new(access_cursor(filter.access(), rule)?),
            predicate: filter.predicate().clone(),
        }),
        logical::AccessStream::Pipeline(pipeline) => {
            if has_variable_write(pipeline.ops()) {
                return Err(rejection("count_cursor_crossed_variable_write_barrier"));
            }
            fold_cursor(access_cursor(pipeline.access(), rule)?, pipeline.ops())
        }
    }
}

fn count_plan_cursor(
    plan: exec::ExecCountPlan,
) -> Result<exec::ExecCountCursorPlan, RuleRejection> {
    match plan {
        exec::ExecCountPlan::Constant(0) => Ok(exec::ExecCountCursorPlan::EmptyRows),
        exec::ExecCountPlan::Constant(_) => Err(rejection("nonzero_constant_is_not_a_row_cursor")),
        exec::ExecCountPlan::NodeBitmap(plan) => {
            Ok(exec::ExecCountCursorPlan::NodeBitmap(plan.bitmap))
        }
        exec::ExecCountPlan::EdgeBitmap(plan) => {
            Ok(exec::ExecCountCursorPlan::EdgeBitmap(plan.bitmap))
        }
        exec::ExecCountPlan::NodeUnique(plan) => Ok(exec::ExecCountCursorPlan::NodeUnique {
            lookup: plan.lookup,
            verification: plan.verification,
        }),
        exec::ExecCountPlan::NodeRange(plan) => {
            Ok(exec::ExecCountCursorPlan::NodeRange(plan.driver))
        }
        exec::ExecCountPlan::EdgeRange(plan) => {
            Ok(exec::ExecCountCursorPlan::EdgeRange(plan.driver))
        }
        exec::ExecCountPlan::NodeAuthoritativeScan(plan) => Ok(
            exec::ExecCountCursorPlan::NodeAuthoritativeScan(plan.predicate),
        ),
        exec::ExecCountPlan::EdgeAuthoritativeScan(plan) => Ok(
            exec::ExecCountCursorPlan::EdgeAuthoritativeScan(plan.predicate),
        ),
        exec::ExecCountPlan::NodePointReads { ids, .. } => {
            Ok(exec::ExecCountCursorPlan::NodePointReads(ids))
        }
        exec::ExecCountPlan::EdgePointReads { ids, .. } => {
            Ok(exec::ExecCountCursorPlan::EdgePointReads(ids))
        }
        exec::ExecCountPlan::NodeRuntimeInput { input, .. } => {
            Ok(exec::ExecCountCursorPlan::NodeRuntimeInput(input))
        }
        exec::ExecCountPlan::EdgeRuntimeInput { input, .. } => {
            Ok(exec::ExecCountCursorPlan::EdgeRuntimeInput(input))
        }
        exec::ExecCountPlan::RuntimeInput { input, .. } => {
            Ok(exec::ExecCountCursorPlan::RuntimeInput(input))
        }
        exec::ExecCountPlan::NodeFullScan { .. } => Ok(exec::ExecCountCursorPlan::NodeFullScan),
        exec::ExecCountPlan::EdgeFullScan { .. } => Ok(exec::ExecCountCursorPlan::EdgeFullScan),
        exec::ExecCountPlan::NodeLabelBitmap { label, .. } => {
            Ok(exec::ExecCountCursorPlan::NodeLabelBitmap(label))
        }
        exec::ExecCountPlan::EdgeLabelBitmap { label, .. } => {
            Ok(exec::ExecCountCursorPlan::EdgeLabelBitmap(label))
        }
        exec::ExecCountPlan::NodeVectorSearch(plan) => {
            Ok(exec::ExecCountCursorPlan::NodeVectorSearch {
                key: plan.key,
                index: plan.index,
                query_vector: plan.query_vector,
                k: plan.k,
            })
        }
        exec::ExecCountPlan::EdgeVectorSearch(plan) => {
            Ok(exec::ExecCountCursorPlan::EdgeVectorSearch {
                key: plan.key,
                index: plan.index,
                query_vector: plan.query_vector,
                k: plan.k,
            })
        }
        exec::ExecCountPlan::NodeTextSearch(plan) => {
            Ok(exec::ExecCountCursorPlan::NodeTextSearch {
                key: plan.key,
                index: plan.index,
                query_text: plan.query_text,
                k: plan.k,
            })
        }
        exec::ExecCountPlan::EdgeTextSearch(plan) => {
            Ok(exec::ExecCountCursorPlan::EdgeTextSearch {
                key: plan.key,
                index: plan.index,
                query_text: plan.query_text,
                k: plan.k,
            })
        }
        exec::ExecCountPlan::NodeDynamicEquality(plan) => {
            Ok(exec::ExecCountCursorPlan::NodeDynamicEquality {
                index: plan.index,
                key: plan.key,
                param: plan.param,
            })
        }
        exec::ExecCountPlan::EdgeDynamicEquality(plan) => {
            Ok(exec::ExecCountCursorPlan::EdgeDynamicEquality {
                index: plan.index,
                key: plan.key,
                param: plan.param,
            })
        }
        exec::ExecCountPlan::NodeDynamicMembership(plan) => {
            Ok(exec::ExecCountCursorPlan::NodeDynamicMembership {
                index: plan.index,
                key: plan.key,
                values: plan.values,
            })
        }
        exec::ExecCountPlan::EdgeDynamicMembership(plan) => {
            Ok(exec::ExecCountCursorPlan::EdgeDynamicMembership {
                index: plan.index,
                key: plan.key,
                values: plan.values,
            })
        }
        exec::ExecCountPlan::Stream(plan) => Ok(plan.cursor),
        exec::ExecCountPlan::InputRows { .. } => Ok(exec::ExecCountCursorPlan::InputRows),
        exec::ExecCountPlan::InputScalars { .. } => {
            Err(rejection("scalar_count_input_is_not_a_row_cursor"))
        }
    }
}

fn node_bitmap_set(
    children: &ir::AtLeast<ir::NodeAccessSourcePlan, 2>,
    union: bool,
    rule: &optimizer::RuleInput<'_>,
) -> Result<Option<exec::ExecNodeBitmapExpr>, RuleRejection> {
    let mut bitmaps = Vec::with_capacity(children.len());
    for child in children {
        let Some(bitmap) = node_bitmap_expr(child.as_ref(), rule)? else {
            return Ok(None);
        };
        bitmaps.push(bitmap);
    }
    if union && let Some(batch) = node_bitmap_batch(&bitmaps) {
        return Ok(Some(batch));
    }
    let driver = bitmaps.remove(0);
    let rest = ir::AtLeast::<_, 1>::try_from_vec(bitmaps)
        .expect("set source has at least two bitmap children");
    Ok(Some(if union {
        exec::ExecNodeBitmapExpr::Union {
            driver: Box::new(driver),
            rest,
        }
    } else {
        exec::ExecNodeBitmapExpr::Intersect {
            driver: Box::new(driver),
            rest,
        }
    }))
}

fn edge_bitmap_set(
    children: &ir::AtLeast<ir::EdgeAccessSourcePlan, 2>,
    union: bool,
    rule: &optimizer::RuleInput<'_>,
) -> Result<Option<exec::ExecEdgeBitmapExpr>, RuleRejection> {
    let mut bitmaps = Vec::with_capacity(children.len());
    for child in children {
        let Some(bitmap) = edge_bitmap_expr(child.as_ref(), rule)? else {
            return Ok(None);
        };
        bitmaps.push(bitmap);
    }
    if union && let Some(batch) = edge_bitmap_batch(&bitmaps) {
        return Ok(Some(batch));
    }
    let driver = bitmaps.remove(0);
    let rest = ir::AtLeast::<_, 1>::try_from_vec(bitmaps)
        .expect("set source has at least two bitmap children");
    Ok(Some(if union {
        exec::ExecEdgeBitmapExpr::Union {
            driver: Box::new(driver),
            rest,
        }
    } else {
        exec::ExecEdgeBitmapExpr::Intersect {
            driver: Box::new(driver),
            rest,
        }
    }))
}

fn node_bitmap_expr(
    source: &ir::NodeAccessPlan,
    rule: &optimizer::RuleInput<'_>,
) -> Result<Option<exec::ExecNodeBitmapExpr>, RuleRejection> {
    match source {
        ir::NodeAccessPlan::EqualityIndex { index, key, value }
            if index.uniqueness == catalog::IndexUniqueness::NonUnique =>
        {
            Ok(match classify_equality(value, rule)? {
                EqualityValue::Indexed(value) => Some(exec::ExecNodeBitmapExpr::PointRead {
                    index: index
                        .clone()
                        .try_into()
                        .expect("non-unique catalog metadata satisfies the bitmap wrapper"),
                    key: key.clone(),
                    value,
                }),
                EqualityValue::AuthoritativeNull
                | EqualityValue::NonReflexive
                | EqualityValue::Dynamic(_)
                | EqualityValue::DynamicSet(_) => None,
            })
        }
        ir::NodeAccessPlan::Union(children) => node_bitmap_set(children, true, rule),
        ir::NodeAccessPlan::Intersect(children) => node_bitmap_set(children, false, rule),
        ir::NodeAccessPlan::Empty
        | ir::NodeAccessPlan::PointIds { .. }
        | ir::NodeAccessPlan::FromParam { .. }
        | ir::NodeAccessPlan::FromVar { .. }
        | ir::NodeAccessPlan::AllScan
        | ir::NodeAccessPlan::LabelScan { .. }
        | ir::NodeAccessPlan::EqualityIndex { .. }
        | ir::NodeAccessPlan::RangeIndex { .. }
        | ir::NodeAccessPlan::VectorSearch { .. }
        | ir::NodeAccessPlan::TextSearch { .. }
        | ir::NodeAccessPlan::ScanThenFilter { .. } => Ok(None),
    }
}

fn edge_bitmap_expr(
    source: &ir::EdgeAccessPlan,
    rule: &optimizer::RuleInput<'_>,
) -> Result<Option<exec::ExecEdgeBitmapExpr>, RuleRejection> {
    match source {
        ir::EdgeAccessPlan::EqualityIndex { index, key, value } => {
            Ok(match classify_equality(value, rule)? {
                EqualityValue::Indexed(value) => Some(exec::ExecEdgeBitmapExpr::PointRead {
                    index: exec::ExecEdgeNonUniqueEqualityIndex::new(index.clone()),
                    key: key.clone(),
                    value,
                }),
                EqualityValue::AuthoritativeNull
                | EqualityValue::NonReflexive
                | EqualityValue::Dynamic(_)
                | EqualityValue::DynamicSet(_) => None,
            })
        }
        ir::EdgeAccessPlan::Union(children) => edge_bitmap_set(children, true, rule),
        ir::EdgeAccessPlan::Intersect(children) => edge_bitmap_set(children, false, rule),
        ir::EdgeAccessPlan::Empty
        | ir::EdgeAccessPlan::PointIds { .. }
        | ir::EdgeAccessPlan::FromParam { .. }
        | ir::EdgeAccessPlan::FromVar { .. }
        | ir::EdgeAccessPlan::AllScan
        | ir::EdgeAccessPlan::LabelScan { .. }
        | ir::EdgeAccessPlan::RangeIndex { .. }
        | ir::EdgeAccessPlan::VectorSearch { .. }
        | ir::EdgeAccessPlan::TextSearch { .. }
        | ir::EdgeAccessPlan::ScanThenFilter { .. } => Ok(None),
    }
}

fn node_bitmap_batch(bitmaps: &[exec::ExecNodeBitmapExpr]) -> Option<exec::ExecNodeBitmapExpr> {
    let exec::ExecNodeBitmapExpr::PointRead { index, key, value } = bitmaps.first()? else {
        return None;
    };
    let mut values = vec![value.clone()];
    for bitmap in &bitmaps[1..] {
        let exec::ExecNodeBitmapExpr::PointRead {
            index: next_index,
            key: next_key,
            value,
        } = bitmap
        else {
            return None;
        };
        if next_index != index || next_key != key {
            return None;
        }
        values.push(value.clone());
    }
    Some(exec::ExecNodeBitmapExpr::BatchedUnionRead {
        index: index.clone(),
        key: key.clone(),
        values: ir::AtLeast::<_, 2>::try_from_vec(values)
            .expect("bitmap batch comes from an at-least-two set"),
    })
}

fn edge_bitmap_batch(bitmaps: &[exec::ExecEdgeBitmapExpr]) -> Option<exec::ExecEdgeBitmapExpr> {
    let exec::ExecEdgeBitmapExpr::PointRead { index, key, value } = bitmaps.first()? else {
        return None;
    };
    let mut values = vec![value.clone()];
    for bitmap in &bitmaps[1..] {
        let exec::ExecEdgeBitmapExpr::PointRead {
            index: next_index,
            key: next_key,
            value,
        } = bitmap
        else {
            return None;
        };
        if next_index != index || next_key != key {
            return None;
        }
        values.push(value.clone());
    }
    Some(exec::ExecEdgeBitmapExpr::BatchedUnionRead {
        index: index.clone(),
        key: key.clone(),
        values: ir::AtLeast::<_, 2>::try_from_vec(values)
            .expect("bitmap batch comes from an at-least-two set"),
    })
}

fn node_set_cursor(
    children: &ir::AtLeast<ir::NodeAccessSourcePlan, 2>,
    union: bool,
    rule: &optimizer::RuleInput<'_>,
) -> Result<exec::ExecCountCursorPlan, RuleRejection> {
    let mut cursors = children
        .iter()
        .map(|child| node_cursor(child.as_ref(), rule))
        .collect::<Result<Vec<_>, _>>()?;
    let driver = cursors.remove(0);
    let rest = ir::AtLeast::<_, 1>::try_from_vec(cursors).expect("set has remaining children");
    Ok(if union {
        exec::ExecCountCursorPlan::Union {
            driver: Box::new(driver),
            rest,
        }
    } else {
        exec::ExecCountCursorPlan::Intersect {
            driver: Box::new(driver),
            rest,
        }
    })
}

fn edge_set_cursor(
    children: &ir::AtLeast<ir::EdgeAccessSourcePlan, 2>,
    union: bool,
    rule: &optimizer::RuleInput<'_>,
) -> Result<exec::ExecCountCursorPlan, RuleRejection> {
    let mut cursors = children
        .iter()
        .map(|child| edge_cursor(child.as_ref(), rule))
        .collect::<Result<Vec<_>, _>>()?;
    let driver = cursors.remove(0);
    let rest = ir::AtLeast::<_, 1>::try_from_vec(cursors).expect("set has remaining children");
    Ok(if union {
        exec::ExecCountCursorPlan::Union {
            driver: Box::new(driver),
            rest,
        }
    } else {
        exec::ExecCountCursorPlan::Intersect {
            driver: Box::new(driver),
            rest,
        }
    })
}

fn node_intersection_count_plans(
    children: &ir::AtLeast<ir::NodeAccessSourcePlan, 2>,
    window: exec::ExecCountWindowPlan,
    rule: &optimizer::RuleInput<'_>,
) -> Result<Vec<exec::ExecCountPlan>, RuleRejection> {
    let bitmaps = children
        .iter()
        .map(|child| node_bitmap_expr(child.as_ref(), rule))
        .collect::<Result<Option<Vec<_>>, _>>()?;
    if let Some(bitmaps) = bitmaps {
        let bitmaps = ir::AtLeast::<_, 1>::try_from_vec(bitmaps)
            .expect("an intersection has at least two bitmap children");
        return Ok(planner_selected_orders(
            &bitmaps,
            set_order_alternative_limit(rule.planner_limits),
        )
        .into_iter()
        .map(|order| {
            let (driver, rest) = order.into_first_and_rest();
            exec::ExecCountPlan::NodeBitmap(exec::ExecNodeBitmapCountPlan {
                bitmap: exec::ExecNodeBitmapExpr::Intersect {
                    driver: Box::new(driver),
                    rest: ir::AtLeast::try_from_vec(rest)
                        .expect("a bitmap intersection retains at least one child"),
                },
                window: window.clone(),
            })
        })
        .collect());
    }
    let mut alternatives = Vec::new();
    let alternative_limit = set_order_alternative_limit(rule.planner_limits);
    for (driver_index, child) in children.iter().enumerate() {
        if alternatives.len() >= alternative_limit {
            break;
        }
        let ir::NodeAccessPlan::RangeIndex { index, key, range } = child.as_ref() else {
            continue;
        };
        let filters = children
            .iter()
            .enumerate()
            .filter(|(index, _)| *index != driver_index)
            .map(|(_, child)| node_bitmap_expr(child.as_ref(), rule))
            .collect::<Result<Option<Vec<_>>, _>>()?;
        let Some(filters) = filters else { continue };
        let filters = ir::AtLeast::<_, 1>::try_from_vec(filters)
            .expect("a range-driver intersection retains at least one bitmap filter");
        let remaining = alternative_limit.saturating_sub(alternatives.len());
        alternatives.extend(
            planner_selected_orders(&filters, remaining)
                .into_iter()
                .map(|filters| {
                    exec::ExecCountPlan::NodeRange(exec::ExecNodeRangeCountPlan {
                        driver: exec::ExecNodeVerifiedRangeScanPlan {
                            index: index.clone(),
                            key: key.clone(),
                            range: range.clone(),
                        },
                        membership: exec::ExecNodeRangeMembershipPlan::BitmapFilters(filters),
                        window: window.clone(),
                    })
                }),
        );
    }
    if alternatives.is_empty() {
        alternatives.push(exec::ExecCountPlan::Stream(exec::ExecCountStreamPlan {
            cursor: node_set_cursor(children, false, rule)?,
            window,
        }));
    }
    Ok(alternatives)
}

fn edge_intersection_count_plans(
    children: &ir::AtLeast<ir::EdgeAccessSourcePlan, 2>,
    window: exec::ExecCountWindowPlan,
    rule: &optimizer::RuleInput<'_>,
) -> Result<Vec<exec::ExecCountPlan>, RuleRejection> {
    let bitmaps = children
        .iter()
        .map(|child| edge_bitmap_expr(child.as_ref(), rule))
        .collect::<Result<Option<Vec<_>>, _>>()?;
    if let Some(bitmaps) = bitmaps {
        let bitmaps = ir::AtLeast::<_, 1>::try_from_vec(bitmaps)
            .expect("an intersection has at least two bitmap children");
        return Ok(planner_selected_orders(
            &bitmaps,
            set_order_alternative_limit(rule.planner_limits),
        )
        .into_iter()
        .map(|order| {
            let (driver, rest) = order.into_first_and_rest();
            exec::ExecCountPlan::EdgeBitmap(exec::ExecEdgeBitmapCountPlan {
                bitmap: exec::ExecEdgeBitmapExpr::Intersect {
                    driver: Box::new(driver),
                    rest: ir::AtLeast::try_from_vec(rest)
                        .expect("a bitmap intersection retains at least one child"),
                },
                window: window.clone(),
            })
        })
        .collect());
    }
    let mut alternatives = Vec::new();
    let alternative_limit = set_order_alternative_limit(rule.planner_limits);
    for (driver_index, child) in children.iter().enumerate() {
        if alternatives.len() >= alternative_limit {
            break;
        }
        let ir::EdgeAccessPlan::RangeIndex { index, key, range } = child.as_ref() else {
            continue;
        };
        let filters = children
            .iter()
            .enumerate()
            .filter(|(index, _)| *index != driver_index)
            .map(|(_, child)| edge_bitmap_expr(child.as_ref(), rule))
            .collect::<Result<Option<Vec<_>>, _>>()?;
        let Some(filters) = filters else { continue };
        let filters = ir::AtLeast::<_, 1>::try_from_vec(filters)
            .expect("a range-driver intersection retains at least one bitmap filter");
        let remaining = alternative_limit.saturating_sub(alternatives.len());
        alternatives.extend(
            planner_selected_orders(&filters, remaining)
                .into_iter()
                .map(|filters| {
                    exec::ExecCountPlan::EdgeRange(exec::ExecEdgeRangeCountPlan {
                        driver: exec::ExecEdgeVerifiedRangeScanPlan {
                            index: index.clone(),
                            key: key.clone(),
                            range: range.clone(),
                        },
                        membership: exec::ExecEdgeRangeMembershipPlan::BitmapFilters(filters),
                        window: window.clone(),
                    })
                }),
        );
    }
    if alternatives.is_empty() {
        alternatives.push(exec::ExecCountPlan::Stream(exec::ExecCountStreamPlan {
            cursor: edge_set_cursor(children, false, rule)?,
            window,
        }));
    }
    Ok(alternatives)
}

fn set_order_alternative_limit(limits: &context::PlannerLimits) -> usize {
    match limits.max_index_union_branches {
        context::IndexUnionBranchLimit::Disabled => 1,
        context::IndexUnionBranchLimit::Limited(limit) => limit.get(),
    }
}

fn planner_selected_orders<T: Clone>(
    values: &ir::AtLeast<T, 1>,
    limit: usize,
) -> Vec<ir::AtLeast<T, 1>> {
    (0..values.len().min(limit))
        .map(|driver_index| {
            let mut order = values.as_ref().to_vec();
            let driver = order.remove(driver_index);
            order.insert(0, driver);
            ir::AtLeast::try_from_vec(order).expect("a planner-selected order stays non-empty")
        })
        .collect()
}

fn count_cost(
    plan: &exec::ExecCountPlan,
    stats: &context::StatsSnapshot,
    storage: &cost::StorageCostProfile,
) -> cost::CostVector {
    match plan {
        exec::ExecCountPlan::Constant(_) => cost::CostVector::ZERO,
        exec::ExecCountPlan::NodeBitmap(plan) => node_bitmap_cost(&plan.bitmap, stats, storage),
        exec::ExecCountPlan::EdgeBitmap(plan) => edge_bitmap_cost(&plan.bitmap, stats, storage),
        exec::ExecCountPlan::NodeUnique(plan) => storage.unique_equality_lookup(
            storage.unique_equality_rows(stats.node_eq_cardinality.get(&plan.lookup.key).copied()),
        ),
        exec::ExecCountPlan::NodeRange(plan) => node_range_count_cost(plan, stats, storage),
        exec::ExecCountPlan::EdgeRange(plan) => edge_range_count_cost(plan, stats, storage),
        exec::ExecCountPlan::NodeAuthoritativeScan(plan) => {
            storage.null_equality_scan(node_scan_rows(&plan.predicate, stats, storage))
        }
        exec::ExecCountPlan::EdgeAuthoritativeScan(plan) => {
            storage.null_equality_scan(edge_scan_rows(&plan.predicate, stats, storage))
        }
        exec::ExecCountPlan::NodePointReads { ids, .. }
        | exec::ExecCountPlan::EdgePointReads { ids, .. } => {
            storage.point_gets(properties::PositiveUsize::at_least_one(ids.as_ref().len()))
        }
        exec::ExecCountPlan::NodeRuntimeInput { .. }
        | exec::ExecCountPlan::EdgeRuntimeInput { .. }
        | exec::ExecCountPlan::RuntimeInput { .. }
        | exec::ExecCountPlan::InputRows { .. }
        | exec::ExecCountPlan::InputScalars { .. } => storage.source_inject(),
        exec::ExecCountPlan::NodeFullScan { .. } | exec::ExecCountPlan::EdgeFullScan { .. } => {
            storage.range_scan(storage.default_unknown_scan_rows)
        }
        exec::ExecCountPlan::NodeLabelBitmap { label, .. } => storage.range_scan(
            stats
                .node_label_cardinality
                .get(label)
                .copied()
                .map_or(storage.default_unknown_scan_rows, cost::EstimatedRows::rows),
        ),
        exec::ExecCountPlan::EdgeLabelBitmap { label, .. } => storage.range_scan(
            stats
                .edge_label_cardinality
                .get(label)
                .copied()
                .map_or(storage.default_unknown_scan_rows, cost::EstimatedRows::rows),
        ),
        exec::ExecCountPlan::NodeVectorSearch(_)
        | exec::ExecCountPlan::EdgeVectorSearch(_)
        | exec::ExecCountPlan::NodeTextSearch(_)
        | exec::ExecCountPlan::EdgeTextSearch(_) => {
            storage.range_scan(storage.default_unknown_scan_rows)
        }
        exec::ExecCountPlan::NodeDynamicEquality(_)
        | exec::ExecCountPlan::EdgeDynamicEquality(_)
        | exec::ExecCountPlan::NodeDynamicMembership(_)
        | exec::ExecCountPlan::EdgeDynamicMembership(_) => storage
            .bitmap_equality_lookup(storage.default_equality_index_rows)
            .serial(storage.null_equality_scan(storage.default_unknown_scan_rows)),
        exec::ExecCountPlan::Stream(plan) => cursor_cost(&plan.cursor, stats, storage),
    }
}

fn node_bitmap_cost(
    bitmap: &exec::ExecNodeBitmapExpr,
    stats: &context::StatsSnapshot,
    storage: &cost::StorageCostProfile,
) -> cost::CostVector {
    match bitmap {
        exec::ExecNodeBitmapExpr::PointRead { key, .. } => {
            storage.bitmap_equality_lookup(stats.node_eq_cardinality.get(key).copied().map_or(
                storage.default_equality_index_rows,
                cost::EstimatedRows::rows,
            ))
        }
        exec::ExecNodeBitmapExpr::BatchedUnionRead { key, values, .. } => {
            let rows = stats.node_eq_cardinality.get(key).copied().map_or(
                storage.default_equality_index_rows,
                cost::EstimatedRows::rows,
            );
            storage.bitmap_equality_batch(
                properties::PositiveUsize::at_least_one(values.len()),
                cost::EstimatedRows::rows(rows.as_rows().saturating_mul(values.len() as u64)),
            )
        }
        exec::ExecNodeBitmapExpr::Union { driver, rest } => {
            let mut rows = node_bitmap_rows(driver, stats, storage);
            rest.iter()
                .fold(node_bitmap_cost(driver, stats, storage), |cost, child| {
                    let child_rows = node_bitmap_rows(child, stats, storage);
                    rows = cost::EstimatedRows::rows(
                        rows.as_rows().saturating_add(child_rows.as_rows()),
                    );
                    cost.serial(node_bitmap_cost(child, stats, storage))
                        .serial(storage.secondary_set_operation(rows))
                })
        }
        exec::ExecNodeBitmapExpr::Intersect { driver, rest } => {
            let mut rows = node_bitmap_rows(driver, stats, storage);
            rest.iter()
                .fold(node_bitmap_cost(driver, stats, storage), |cost, child| {
                    let child_rows = node_bitmap_rows(child, stats, storage);
                    let operation_rows = rows;
                    rows = cost::EstimatedRows::rows(rows.as_rows().min(child_rows.as_rows()));
                    cost.serial(node_bitmap_cost(child, stats, storage))
                        .serial(storage.secondary_set_operation(operation_rows))
                })
        }
    }
}

fn edge_bitmap_cost(
    bitmap: &exec::ExecEdgeBitmapExpr,
    stats: &context::StatsSnapshot,
    storage: &cost::StorageCostProfile,
) -> cost::CostVector {
    match bitmap {
        exec::ExecEdgeBitmapExpr::PointRead { key, .. } => {
            storage.bitmap_equality_lookup(stats.edge_eq_cardinality.get(key).copied().map_or(
                storage.default_equality_index_rows,
                cost::EstimatedRows::rows,
            ))
        }
        exec::ExecEdgeBitmapExpr::BatchedUnionRead { key, values, .. } => {
            let rows = stats.edge_eq_cardinality.get(key).copied().map_or(
                storage.default_equality_index_rows,
                cost::EstimatedRows::rows,
            );
            storage.bitmap_equality_batch(
                properties::PositiveUsize::at_least_one(values.len()),
                cost::EstimatedRows::rows(rows.as_rows().saturating_mul(values.len() as u64)),
            )
        }
        exec::ExecEdgeBitmapExpr::Union { driver, rest } => {
            let mut rows = edge_bitmap_rows(driver, stats, storage);
            rest.iter()
                .fold(edge_bitmap_cost(driver, stats, storage), |cost, child| {
                    let child_rows = edge_bitmap_rows(child, stats, storage);
                    rows = cost::EstimatedRows::rows(
                        rows.as_rows().saturating_add(child_rows.as_rows()),
                    );
                    cost.serial(edge_bitmap_cost(child, stats, storage))
                        .serial(storage.secondary_set_operation(rows))
                })
        }
        exec::ExecEdgeBitmapExpr::Intersect { driver, rest } => {
            let mut rows = edge_bitmap_rows(driver, stats, storage);
            rest.iter()
                .fold(edge_bitmap_cost(driver, stats, storage), |cost, child| {
                    let child_rows = edge_bitmap_rows(child, stats, storage);
                    let operation_rows = rows;
                    rows = cost::EstimatedRows::rows(rows.as_rows().min(child_rows.as_rows()));
                    cost.serial(edge_bitmap_cost(child, stats, storage))
                        .serial(storage.secondary_set_operation(operation_rows))
                })
        }
    }
}

fn node_bitmap_rows(
    bitmap: &exec::ExecNodeBitmapExpr,
    stats: &context::StatsSnapshot,
    storage: &cost::StorageCostProfile,
) -> cost::EstimatedRows {
    match bitmap {
        exec::ExecNodeBitmapExpr::PointRead { key, .. } => {
            storage.equality_index_rows(stats.node_eq_cardinality.get(key).copied())
        }
        exec::ExecNodeBitmapExpr::BatchedUnionRead { key, values, .. } => {
            let rows = storage
                .equality_index_rows(stats.node_eq_cardinality.get(key).copied())
                .as_rows();
            cost::EstimatedRows::rows(rows.saturating_mul(values.len() as u64))
        }
        exec::ExecNodeBitmapExpr::Union { driver, rest } => {
            let rows = rest.iter().fold(
                node_bitmap_rows(driver, stats, storage).as_rows(),
                |rows, child| {
                    rows.saturating_add(node_bitmap_rows(child, stats, storage).as_rows())
                },
            );
            cost::EstimatedRows::rows(rows)
        }
        exec::ExecNodeBitmapExpr::Intersect { driver, rest } => {
            let rows = rest.iter().fold(
                node_bitmap_rows(driver, stats, storage).as_rows(),
                |rows, child| rows.min(node_bitmap_rows(child, stats, storage).as_rows()),
            );
            cost::EstimatedRows::rows(rows)
        }
    }
}

fn edge_bitmap_rows(
    bitmap: &exec::ExecEdgeBitmapExpr,
    stats: &context::StatsSnapshot,
    storage: &cost::StorageCostProfile,
) -> cost::EstimatedRows {
    match bitmap {
        exec::ExecEdgeBitmapExpr::PointRead { key, .. } => {
            storage.equality_index_rows(stats.edge_eq_cardinality.get(key).copied())
        }
        exec::ExecEdgeBitmapExpr::BatchedUnionRead { key, values, .. } => {
            let rows = storage
                .equality_index_rows(stats.edge_eq_cardinality.get(key).copied())
                .as_rows();
            cost::EstimatedRows::rows(rows.saturating_mul(values.len() as u64))
        }
        exec::ExecEdgeBitmapExpr::Union { driver, rest } => {
            let rows = rest.iter().fold(
                edge_bitmap_rows(driver, stats, storage).as_rows(),
                |rows, child| {
                    rows.saturating_add(edge_bitmap_rows(child, stats, storage).as_rows())
                },
            );
            cost::EstimatedRows::rows(rows)
        }
        exec::ExecEdgeBitmapExpr::Intersect { driver, rest } => {
            let rows = rest.iter().fold(
                edge_bitmap_rows(driver, stats, storage).as_rows(),
                |rows, child| rows.min(edge_bitmap_rows(child, stats, storage).as_rows()),
            );
            cost::EstimatedRows::rows(rows)
        }
    }
}

fn node_range_count_cost(
    plan: &exec::ExecNodeRangeCountPlan,
    stats: &context::StatsSnapshot,
    storage: &cost::StorageCostProfile,
) -> cost::CostVector {
    let driver_rows = stats
        .node_range_cardinality
        .get(&plan.driver.key)
        .copied()
        .map_or(storage.default_range_index_rows, cost::EstimatedRows::rows);
    let filters = match &plan.membership {
        exec::ExecNodeRangeMembershipPlan::All => Vec::new(),
        exec::ExecNodeRangeMembershipPlan::BitmapFilters(filters) => filters
            .iter()
            .map(|filter| {
                (
                    node_bitmap_cost(filter, stats, storage),
                    node_bitmap_rows(filter, stats, storage),
                )
            })
            .collect(),
    };
    verified_range_count_cost(driver_rows, &filters, &plan.window, storage)
}

fn edge_range_count_cost(
    plan: &exec::ExecEdgeRangeCountPlan,
    stats: &context::StatsSnapshot,
    storage: &cost::StorageCostProfile,
) -> cost::CostVector {
    let driver_rows = stats
        .edge_range_cardinality
        .get(&plan.driver.key)
        .copied()
        .map_or(storage.default_range_index_rows, cost::EstimatedRows::rows);
    let filters = match &plan.membership {
        exec::ExecEdgeRangeMembershipPlan::All => Vec::new(),
        exec::ExecEdgeRangeMembershipPlan::BitmapFilters(filters) => filters
            .iter()
            .map(|filter| {
                (
                    edge_bitmap_cost(filter, stats, storage),
                    edge_bitmap_rows(filter, stats, storage),
                )
            })
            .collect(),
    };
    verified_range_count_cost(driver_rows, &filters, &plan.window, storage)
}

fn verified_range_count_cost(
    driver_rows: cost::EstimatedRows,
    filters: &[(cost::CostVector, cost::EstimatedRows)],
    window: &exec::ExecCountWindowPlan,
    storage: &cost::StorageCostProfile,
) -> cost::CostVector {
    let accepted_rows = filters
        .iter()
        .fold(driver_rows.as_rows(), |rows, (_, filter)| {
            apply_membership_selectivity(rows, filter.as_rows(), driver_rows.as_rows())
        });
    let scanned_rows = static_window_threshold(window).map_or(driver_rows.as_rows(), |threshold| {
        if threshold == 0 {
            return 0;
        }
        if accepted_rows == 0 {
            return driver_rows.as_rows();
        }
        let threshold = u64::try_from(threshold).unwrap_or(u64::MAX);
        threshold
            .saturating_mul(driver_rows.as_rows())
            .saturating_add(accepted_rows.saturating_sub(1))
            .checked_div(accepted_rows)
            .unwrap_or(u64::MAX)
            .min(driver_rows.as_rows())
    });
    let mut total = filters
        .iter()
        .fold(cost::CostVector::ZERO, |total, (filter, _)| {
            total.serial(*filter)
        });
    if scanned_rows > 0 {
        total =
            total.serial(storage.secondary_range_lookup(cost::EstimatedRows::rows(scanned_rows)));
    }
    let mut candidates = scanned_rows;
    for (_, filter_rows) in filters {
        total =
            total.serial(storage.secondary_set_operation(cost::EstimatedRows::rows(candidates)));
        candidates =
            apply_membership_selectivity(candidates, filter_rows.as_rows(), driver_rows.as_rows());
    }
    total
}

fn apply_membership_selectivity(rows: u64, filter_rows: u64, driver_rows: u64) -> u64 {
    if driver_rows == 0 {
        return 0;
    }
    let filter_rows = filter_rows.min(driver_rows);
    let numerator = u128::from(rows).saturating_mul(u128::from(filter_rows));
    let rounded = numerator.saturating_add(u128::from(driver_rows.saturating_sub(1)))
        / u128::from(driver_rows);
    u64::try_from(rounded).unwrap_or(u64::MAX)
}

fn static_window_threshold(window: &exec::ExecCountWindowPlan) -> Option<usize> {
    let exec::ExecCountTake::AtMost(take) = &window.take else {
        return None;
    };
    let skip = window.skip.evaluate(&mut |_| Err(())).ok()?;
    let take = take.evaluate(&mut |_| Err(())).ok()?;
    Some(skip.saturating_add(take))
}

fn cursor_cost(
    cursor: &exec::ExecCountCursorPlan,
    stats: &context::StatsSnapshot,
    storage: &cost::StorageCostProfile,
) -> cost::CostVector {
    match cursor {
        exec::ExecCountCursorPlan::EmptyRows => cost::CostVector::ZERO,
        exec::ExecCountCursorPlan::InputRows
        | exec::ExecCountCursorPlan::NodeRuntimeInput(_)
        | exec::ExecCountCursorPlan::EdgeRuntimeInput(_)
        | exec::ExecCountCursorPlan::RuntimeInput(_) => storage.source_inject(),
        exec::ExecCountCursorPlan::NodeBitmap(bitmap) => node_bitmap_cost(bitmap, stats, storage),
        exec::ExecCountCursorPlan::EdgeBitmap(bitmap) => edge_bitmap_cost(bitmap, stats, storage),
        exec::ExecCountCursorPlan::NodeUnique { lookup, .. } => storage.unique_equality_lookup(
            storage.unique_equality_rows(stats.node_eq_cardinality.get(&lookup.key).copied()),
        ),
        exec::ExecCountCursorPlan::NodeRange(plan) => storage.secondary_range_lookup(
            stats
                .node_range_cardinality
                .get(&plan.key)
                .copied()
                .map_or(storage.default_range_index_rows, cost::EstimatedRows::rows),
        ),
        exec::ExecCountCursorPlan::EdgeRange(plan) => storage.secondary_range_lookup(
            stats
                .edge_range_cardinality
                .get(&plan.key)
                .copied()
                .map_or(storage.default_range_index_rows, cost::EstimatedRows::rows),
        ),
        exec::ExecCountCursorPlan::NodeAuthoritativeScan(predicate) => {
            storage.null_equality_scan(node_scan_rows(predicate, stats, storage))
        }
        exec::ExecCountCursorPlan::EdgeAuthoritativeScan(predicate) => {
            storage.null_equality_scan(edge_scan_rows(predicate, stats, storage))
        }
        exec::ExecCountCursorPlan::NodePointReads(ids)
        | exec::ExecCountCursorPlan::EdgePointReads(ids) => {
            storage.point_gets(properties::PositiveUsize::at_least_one(ids.as_ref().len()))
        }
        exec::ExecCountCursorPlan::NodeFullScan | exec::ExecCountCursorPlan::EdgeFullScan => {
            storage.range_scan(storage.default_unknown_scan_rows)
        }
        exec::ExecCountCursorPlan::NodeLabelBitmap(label) => storage.range_scan(
            stats
                .node_label_cardinality
                .get(label)
                .copied()
                .map_or(storage.default_unknown_scan_rows, cost::EstimatedRows::rows),
        ),
        exec::ExecCountCursorPlan::EdgeLabelBitmap(label) => storage.range_scan(
            stats
                .edge_label_cardinality
                .get(label)
                .copied()
                .map_or(storage.default_unknown_scan_rows, cost::EstimatedRows::rows),
        ),
        exec::ExecCountCursorPlan::NodeVectorSearch { .. }
        | exec::ExecCountCursorPlan::EdgeVectorSearch { .. }
        | exec::ExecCountCursorPlan::NodeTextSearch { .. }
        | exec::ExecCountCursorPlan::EdgeTextSearch { .. } => {
            storage.range_scan(storage.default_unknown_scan_rows)
        }
        exec::ExecCountCursorPlan::NodeDynamicEquality { .. }
        | exec::ExecCountCursorPlan::EdgeDynamicEquality { .. }
        | exec::ExecCountCursorPlan::NodeDynamicMembership { .. }
        | exec::ExecCountCursorPlan::EdgeDynamicMembership { .. } => storage
            .bitmap_equality_lookup(storage.default_equality_index_rows)
            .serial(storage.null_equality_scan(storage.default_unknown_scan_rows)),
        exec::ExecCountCursorPlan::Union { driver, rest }
        | exec::ExecCountCursorPlan::Intersect { driver, rest } => {
            rest.iter()
                .fold(cursor_cost(driver, stats, storage), |cost, child| {
                    cost.serial(cursor_cost(child, stats, storage))
                        .serial(storage.secondary_set_operation(storage.default_unknown_scan_rows))
                })
        }
        exec::ExecCountCursorPlan::Filter { input, .. } => cursor_cost(input, stats, storage)
            .serial(storage.predicate_eval(storage.default_unknown_scan_rows)),
        exec::ExecCountCursorPlan::Window { input, .. } => cursor_cost(input, stats, storage),
        exec::ExecCountCursorPlan::Order { input, .. } => cursor_cost(input, stats, storage)
            .serial(storage.explicit_sort(storage.default_unknown_scan_rows)),
        exec::ExecCountCursorPlan::Expand { input, .. }
        | exec::ExecCountCursorPlan::VectorSearch { input, .. }
        | exec::ExecCountCursorPlan::TextSearch { input, .. }
        | exec::ExecCountCursorPlan::Variable { input, .. }
        | exec::ExecCountCursorPlan::Distinct { input, .. } => cursor_cost(input, stats, storage)
            .serial(storage.stream_operator(storage.default_unknown_scan_rows)),
    }
}

fn node_scan_rows(
    predicate: &exec::ExecNodeAuthoritativeScanPredicate,
    stats: &context::StatsSnapshot,
    storage: &cost::StorageCostProfile,
) -> cost::EstimatedRows {
    match predicate {
        exec::ExecNodeAuthoritativeScanPredicate::NullEquality { key } => stats
            .node_label_cardinality
            .get(&key.label)
            .copied()
            .map_or(storage.default_unknown_scan_rows, cost::EstimatedRows::rows),
        exec::ExecNodeAuthoritativeScanPredicate::Predicate(_) => storage.default_unknown_scan_rows,
    }
}

fn edge_scan_rows(
    predicate: &exec::ExecEdgeAuthoritativeScanPredicate,
    stats: &context::StatsSnapshot,
    storage: &cost::StorageCostProfile,
) -> cost::EstimatedRows {
    match predicate {
        exec::ExecEdgeAuthoritativeScanPredicate::NullEquality { key } => stats
            .edge_label_cardinality
            .get(&key.label)
            .copied()
            .map_or(storage.default_unknown_scan_rows, cost::EstimatedRows::rows),
        exec::ExecEdgeAuthoritativeScanPredicate::Predicate(_) => storage.default_unknown_scan_rows,
    }
}

fn rejection(reason: &'static str) -> RuleRejection {
    RuleRejection::new(reason).expect("cardinality rejection reasons are non-empty")
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::num::NonZeroUsize;

    use helix_ast::{
        expr::Predicate,
        index::RangeIndexDirection,
        query::QueryValue,
        traversal::Order,
        value::{PropertyInput, PropertyValue},
    };

    use super::*;
    use crate::optimizer::OptimizerRule;

    fn name(value: &str) -> ir::NonEmptyString {
        ir::NonEmptyString::new(value).unwrap()
    }

    fn node_path(plan: ir::NodeAccessPlan) -> logical::AccessPath {
        logical::AccessPath::Node(logical::NodeAccessPath::new(
            ir::NodeAccessSourcePlan::new(plan).unwrap(),
        ))
    }

    fn edge_path(plan: ir::EdgeAccessPlan) -> logical::AccessPath {
        logical::AccessPath::Edge(logical::EdgeAccessPath::new(
            ir::EdgeAccessSourcePlan::new(plan).unwrap(),
        ))
    }

    fn literal(value: PropertyValue) -> ir::IndexValue {
        ir::IndexValue::Literal(ir::SecondaryIndexLiteral::new(value).unwrap())
    }

    fn exec_indexed(value: &str) -> exec::ExecIndexedEqualityValue {
        ir::SecondaryIndexLiteral::new(PropertyValue::from(value))
            .unwrap()
            .try_into()
            .unwrap()
    }

    fn exec_node_point(value: &str) -> exec::ExecNodeBitmapExpr {
        exec::ExecNodeBitmapExpr::PointRead {
            index: catalog::NodeEqualityIndexMeta::new(name("node_eq:User:status"))
                .try_into()
                .unwrap(),
            key: catalog::ScopedPropertyKey::try_new("User", "status").unwrap(),
            value: exec_indexed(value),
        }
    }

    fn exec_edge_point(value: &str) -> exec::ExecEdgeBitmapExpr {
        exec::ExecEdgeBitmapExpr::PointRead {
            index: exec::ExecEdgeNonUniqueEqualityIndex::new(catalog::EdgeEqualityIndexMeta::new(
                name("edge_eq:LIKES:status"),
            )),
            key: catalog::ScopedPropertyKey::try_new("LIKES", "status").unwrap(),
            value: exec_indexed(value),
        }
    }

    fn element_ids() -> ir::ElementIds {
        ir::ElementIds::new(ir::AtLeast::from_one_and_rest(1, vec![2])).unwrap()
    }

    fn predicate() -> ir::PredicatePlan {
        ir::PredicatePlan::new(Predicate::has_key("status")).unwrap()
    }

    fn search_index() -> ir::SearchIndexPlan {
        ir::SearchIndexPlan {
            index_id: name("search-index"),
            tenant: ir::SearchTenantPlan::Unscoped,
        }
    }

    fn search_limit() -> ir::SearchLimitPlan {
        ir::SearchLimitPlan::Literal(NonZeroUsize::MIN)
    }

    fn ordering() -> ir::OrderKeys {
        ir::OrderKeys::from(ir::OrderKey {
            property: name("age"),
            order: Order::Asc,
        })
    }

    fn expand() -> ir::ExpandPlan {
        ir::ExpandPlan {
            direction: ir::ExpandDirection::Out,
            output: ir::ExpandOutput::Nodes,
            label: ir::ExpandLabelPlan::Any,
        }
    }

    fn restricted_vector() -> Box<ir::RestrictedVectorSearchPlan> {
        Box::new(ir::RestrictedVectorSearchPlan::Nodes {
            key: catalog::NodeSearchIndexKey::try_new("User", "embedding").unwrap(),
            index: search_index(),
            query_vector: ir::VectorQueryInputPlan::new(PropertyInput::from(vec![1.0_f32]))
                .unwrap(),
            k: search_limit(),
        })
    }

    fn restricted_text() -> Box<ir::RestrictedTextSearchPlan> {
        Box::new(ir::RestrictedTextSearchPlan::Edges {
            key: catalog::EdgeSearchIndexKey::try_new("LIKES", "body").unwrap(),
            index: search_index(),
            query_text: ir::TextQueryInputPlan::new(PropertyInput::from("needle")).unwrap(),
            k: search_limit(),
        })
    }

    fn node_vector_search() -> ir::NodeAccessPlan {
        ir::NodeAccessPlan::VectorSearch {
            key: catalog::NodeSearchIndexKey::try_new("User", "embedding").unwrap(),
            index: search_index(),
            query_vector: ir::VectorQueryInputPlan::new(PropertyInput::from(vec![1.0_f32]))
                .unwrap(),
            k: search_limit(),
        }
    }

    fn edge_vector_search() -> ir::EdgeAccessPlan {
        ir::EdgeAccessPlan::VectorSearch {
            key: catalog::EdgeSearchIndexKey::try_new("LIKES", "embedding").unwrap(),
            index: search_index(),
            query_vector: ir::VectorQueryInputPlan::new(PropertyInput::from(vec![1.0_f32]))
                .unwrap(),
            k: search_limit(),
        }
    }

    fn node_text_search() -> ir::NodeAccessPlan {
        ir::NodeAccessPlan::TextSearch {
            key: catalog::NodeSearchIndexKey::try_new("User", "body").unwrap(),
            index: search_index(),
            query_text: ir::TextQueryInputPlan::new(PropertyInput::from("needle")).unwrap(),
            k: search_limit(),
        }
    }

    fn edge_text_search() -> ir::EdgeAccessPlan {
        ir::EdgeAccessPlan::TextSearch {
            key: catalog::EdgeSearchIndexKey::try_new("LIKES", "body").unwrap(),
            index: search_index(),
            query_text: ir::TextQueryInputPlan::new(PropertyInput::from("needle")).unwrap(),
            k: search_limit(),
        }
    }

    fn node_range_plan() -> ir::NodeAccessPlan {
        ir::NodeAccessPlan::RangeIndex {
            index: catalog::NodeRangeIndexMeta::try_new("node-range").unwrap(),
            key: catalog::ScopedPropertyDirectionKey::try_new(
                "User",
                "age",
                RangeIndexDirection::Asc,
            )
            .unwrap(),
            range: ir::IndexRange::All,
        }
    }

    fn edge_range_plan() -> ir::EdgeAccessPlan {
        ir::EdgeAccessPlan::RangeIndex {
            index: catalog::EdgeRangeIndexMeta::try_new("edge-range").unwrap(),
            key: catalog::ScopedPropertyDirectionKey::try_new(
                "LIKES",
                "age",
                RangeIndexDirection::Desc,
            )
            .unwrap(),
            range: ir::IndexRange::All,
        }
    }

    fn node_equality(
        index_id: &str,
        property: &str,
        value: ir::IndexValue,
        uniqueness: catalog::IndexUniqueness,
    ) -> ir::NodeAccessPlan {
        ir::NodeAccessPlan::EqualityIndex {
            index: catalog::NodeEqualityIndexMeta::new(name(index_id)).with_uniqueness(uniqueness),
            key: catalog::ScopedPropertyKey::try_new("User", property).unwrap(),
            value,
        }
    }

    fn edge_equality(index_id: &str, property: &str, value: ir::IndexValue) -> ir::EdgeAccessPlan {
        ir::EdgeAccessPlan::EqualityIndex {
            index: catalog::EdgeEqualityIndexMeta::new(name(index_id)),
            key: catalog::ScopedPropertyKey::try_new("LIKES", property).unwrap(),
            value,
        }
    }

    fn apply(
        input: logical::RootStream,
        params: context::ParamBindings,
        late_bound_params: BTreeSet<ir::NonEmptyString>,
    ) -> optimizer::RuleResult {
        let expr = logical::LogicalExpr::StreamCardinality(
            logical::StreamCardinality::new(input)
                .with_planning_bindings(params, late_bound_params),
        );
        let storage = cost::StorageCostProfile::default();
        let indexes = catalog::IndexCatalogSnapshot::default();
        let limits = context::PlannerLimits::default();
        let stats = context::StatsSnapshot::default();
        StreamCardinalityImplementationRule::default().apply(optimizer::RuleInput {
            expr: &expr,
            planner_limits: &limits,
            stats: &stats,
            storage: &storage,
            indexes: &indexes,
        })
    }

    fn with_rule_input_bindings<T>(
        params: context::ParamBindings,
        late_bound_params: BTreeSet<ir::NonEmptyString>,
        run: impl FnOnce(&optimizer::RuleInput<'_>) -> T,
    ) -> T {
        let expr = logical::LogicalExpr::StreamCardinality(
            logical::StreamCardinality::new(access(node_path(ir::NodeAccessPlan::AllScan)))
                .with_planning_bindings(params, late_bound_params),
        );
        let storage = cost::StorageCostProfile::default();
        let indexes = catalog::IndexCatalogSnapshot::default();
        let limits = context::PlannerLimits::default();
        let stats = context::StatsSnapshot::default();
        run(&optimizer::RuleInput {
            expr: &expr,
            planner_limits: &limits,
            stats: &stats,
            storage: &storage,
            indexes: &indexes,
        })
    }

    fn with_rule_input<T>(run: impl FnOnce(&optimizer::RuleInput<'_>) -> T) -> T {
        with_rule_input_bindings(context::ParamBindings::default(), BTreeSet::new(), run)
    }

    fn plans(result: optimizer::RuleResult) -> Vec<exec::ExecCountPlan> {
        let optimizer::RuleResult::Applied(optimizer::RuleEffect::Physical(alternatives)) = result
        else {
            panic!("expected physical cardinality alternatives");
        };
        alternatives
            .into_iter()
            .map(|alternative| {
                let physical::PhysicalExpr::Cardinality(plan) = alternative.expr else {
                    panic!("expected physical cardinality plan");
                };
                plan.into_executable()
            })
            .collect()
    }

    fn access(input: logical::AccessPath) -> logical::RootStream {
        logical::RootStream::Access(logical::AccessStream::Path(input))
    }

    #[test]
    fn rule_outcome_matrix_covers_not_applicable_applied_and_rejected() {
        let non_cardinality =
            logical::LogicalExpr::AccessPath(node_path(ir::NodeAccessPlan::AllScan));
        let storage = cost::StorageCostProfile::default();
        let indexes = catalog::IndexCatalogSnapshot::default();
        let limits = context::PlannerLimits::default();
        let stats = context::StatsSnapshot::default();
        assert_eq!(
            StreamCardinalityImplementationRule::default().apply(optimizer::RuleInput {
                expr: &non_cardinality,
                planner_limits: &limits,
                stats: &stats,
                storage: &storage,
                indexes: &indexes,
            }),
            optimizer::RuleResult::NotApplicable
        );

        assert!(matches!(
            apply(
                access(node_path(ir::NodeAccessPlan::AllScan)),
                context::ParamBindings::default(),
                BTreeSet::new(),
            ),
            optimizer::RuleResult::Applied(_)
        ));

        let rejected = apply(
            access(node_path(node_equality(
                "user_email",
                "email",
                ir::IndexValue::Param(name("missing")),
                catalog::IndexUniqueness::NonUnique,
            ))),
            context::ParamBindings::default(),
            BTreeSet::new(),
        );
        assert!(matches!(
            rejected,
            optimizer::RuleResult::Rejected(RuleRejection { reason })
                if reason.as_ref() == "missing_planning_equality_parameter"
        ));
    }

    #[test]
    fn node_equality_values_choose_exact_physical_algorithms() {
        let cases = [
            (
                literal(PropertyValue::from("alice@example.test")),
                catalog::IndexUniqueness::NonUnique,
                physical::PhysicalCardinality::BitmapPoint,
            ),
            (
                literal(PropertyValue::from("alice@example.test")),
                catalog::IndexUniqueness::Unique,
                physical::PhysicalCardinality::UniqueVerified,
            ),
            (
                literal(PropertyValue::Null),
                catalog::IndexUniqueness::NonUnique,
                physical::PhysicalCardinality::AuthoritativeScan,
            ),
            (
                literal(PropertyValue::F64(f64::NAN)),
                catalog::IndexUniqueness::NonUnique,
                physical::PhysicalCardinality::Constant,
            ),
            (
                literal(PropertyValue::F32(f32::NAN)),
                catalog::IndexUniqueness::NonUnique,
                physical::PhysicalCardinality::Constant,
            ),
            (
                literal(PropertyValue::F64Array(vec![1.0, f64::NAN])),
                catalog::IndexUniqueness::NonUnique,
                physical::PhysicalCardinality::Constant,
            ),
            (
                literal(PropertyValue::F32Array(vec![f32::NAN])),
                catalog::IndexUniqueness::NonUnique,
                physical::PhysicalCardinality::Constant,
            ),
        ];

        for (value, uniqueness, expected) in cases {
            let [plan] = plans(apply(
                access(node_path(node_equality(
                    "user_email",
                    "email",
                    value,
                    uniqueness,
                ))),
                context::ParamBindings::default(),
                BTreeSet::new(),
            ))
            .try_into()
            .unwrap();
            assert_eq!(physical::PhysicalCountPlan::new(plan).family(), expected);
        }
    }

    #[test]
    fn edge_equality_values_choose_bitmap_scan_and_constant() {
        for (value, expected) in [
            (
                literal(PropertyValue::I64(7)),
                physical::PhysicalCardinality::BitmapPoint,
            ),
            (
                literal(PropertyValue::Null),
                physical::PhysicalCardinality::AuthoritativeScan,
            ),
            (
                literal(PropertyValue::F64(f64::NAN)),
                physical::PhysicalCardinality::Constant,
            ),
        ] {
            let [plan] = plans(apply(
                access(edge_path(edge_equality("likes_weight", "weight", value))),
                context::ParamBindings::default(),
                BTreeSet::new(),
            ))
            .try_into()
            .unwrap();
            assert_eq!(physical::PhysicalCountPlan::new(plan).family(), expected);
        }
    }

    #[test]
    fn ordinary_and_scoped_parameters_are_classified_at_the_correct_boundary() {
        let parameter = name("email");
        let foreach_container = name("items");
        let source = || {
            access(node_path(node_equality(
                "user_email",
                "email",
                ir::IndexValue::Param(parameter.clone()),
                catalog::IndexUniqueness::NonUnique,
            )))
        };
        let property_params =
            context::ParamBindings::default().with_value(parameter.clone(), "alice@example.test");
        let query_params =
            context::ParamBindings::default().with_query_value(parameter.clone(), QueryValue::Null);

        assert!(matches!(
            plans(apply(source(), property_params, BTreeSet::new())).as_slice(),
            [exec::ExecCountPlan::NodeBitmap(_)]
        ));
        assert!(matches!(
            plans(apply(source(), query_params, BTreeSet::new())).as_slice(),
            [exec::ExecCountPlan::NodeAuthoritativeScan(_)]
        ));
        assert!(matches!(
            plans(apply(
                source(),
                context::ParamBindings::default(),
                BTreeSet::from([foreach_container.clone()]),
            ))
            .as_slice(),
            [exec::ExecCountPlan::NodeDynamicEquality(_)]
        ));

        let edge_source = || {
            access(edge_path(edge_equality(
                "likes_status",
                "status",
                ir::IndexValue::Param(parameter.clone()),
            )))
        };
        assert!(matches!(
            plans(apply(
                edge_source(),
                context::ParamBindings::default(),
                BTreeSet::from([foreach_container]),
            ))
            .as_slice(),
            [exec::ExecCountPlan::EdgeDynamicEquality(_)]
        ));
    }

    #[test]
    fn every_scalar_query_parameter_is_inlined_before_algorithm_selection() {
        let parameter = name("value");
        let source = || {
            access(node_path(node_equality(
                "user_value",
                "value",
                ir::IndexValue::Param(parameter.clone()),
                catalog::IndexUniqueness::NonUnique,
            )))
        };
        for value in [
            QueryValue::Bool(true),
            QueryValue::I64(7),
            QueryValue::F64(7.5),
            QueryValue::F32(3.5),
            QueryValue::String("value".to_string()),
        ] {
            assert!(matches!(
                plans(apply(
                    source(),
                    context::ParamBindings::default().with_query_value(parameter.clone(), value),
                    BTreeSet::new(),
                ))
                .as_slice(),
                [exec::ExecCountPlan::NodeBitmap(_)]
            ));
        }
    }

    #[test]
    fn nested_query_parameters_are_rejected_before_physical_lowering() {
        let parameter = name("value");
        for nested in [
            QueryValue::Array(vec![QueryValue::I64(1)]),
            QueryValue::Object(std::collections::BTreeMap::from([(
                "key".to_string(),
                QueryValue::Bool(true),
            )])),
        ] {
            let result = apply(
                access(edge_path(edge_equality(
                    "likes_weight",
                    "weight",
                    ir::IndexValue::Param(parameter.clone()),
                ))),
                context::ParamBindings::default().with_query_value(parameter.clone(), nested),
                BTreeSet::new(),
            );
            assert!(matches!(
                result,
                optimizer::RuleResult::Rejected(RuleRejection { reason })
                    if reason.as_ref() == "unsupported_planning_equality_parameter"
            ));
        }

        for nested in [
            PropertyValue::array([PropertyValue::I64(1)]),
            PropertyValue::object([("key", PropertyValue::Bool(true))]),
        ] {
            let result = apply(
                access(node_path(node_equality(
                    "user_value",
                    "value",
                    ir::IndexValue::Param(parameter.clone()),
                    catalog::IndexUniqueness::NonUnique,
                ))),
                context::ParamBindings::default().with_value(parameter.clone(), nested),
                BTreeSet::new(),
            );
            assert!(matches!(
                result,
                optimizer::RuleResult::Rejected(RuleRejection { reason })
                    if reason.as_ref() == "unsupported_planning_equality_parameter"
            ));
        }
    }

    #[test]
    fn same_index_union_is_batched_and_different_indexes_preserve_driver_order() {
        let left = ir::NodeAccessSourcePlan::new(node_equality(
            "user_age",
            "age",
            literal(PropertyValue::I64(21)),
            catalog::IndexUniqueness::NonUnique,
        ))
        .unwrap();
        let same = ir::NodeAccessSourcePlan::new(node_equality(
            "user_age",
            "age",
            literal(PropertyValue::I64(42)),
            catalog::IndexUniqueness::NonUnique,
        ))
        .unwrap();
        let different = ir::NodeAccessSourcePlan::new(node_equality(
            "user_score",
            "score",
            literal(PropertyValue::I64(90)),
            catalog::IndexUniqueness::NonUnique,
        ))
        .unwrap();

        let [batched] = plans(apply(
            access(node_path(ir::NodeAccessPlan::Union(
                ir::AtLeast::from_pair(left.clone(), same),
            ))),
            context::ParamBindings::default(),
            BTreeSet::new(),
        ))
        .try_into()
        .unwrap();
        assert!(matches!(
            batched,
            exec::ExecCountPlan::NodeBitmap(exec::ExecNodeBitmapCountPlan {
                bitmap: exec::ExecNodeBitmapExpr::BatchedUnionRead { values, .. },
                ..
            }) if values.len() == 2
        ));

        let [union] = plans(apply(
            access(node_path(ir::NodeAccessPlan::Union(
                ir::AtLeast::from_pair(left, different),
            ))),
            context::ParamBindings::default(),
            BTreeSet::new(),
        ))
        .try_into()
        .unwrap();
        let exec::ExecCountPlan::NodeBitmap(exec::ExecNodeBitmapCountPlan {
            bitmap: exec::ExecNodeBitmapExpr::Union { driver, rest },
            ..
        }) = union
        else {
            panic!("expected explicit bitmap union");
        };
        assert!(matches!(
            driver.as_ref(),
            exec::ExecNodeBitmapExpr::PointRead { key, .. } if key.property.as_ref() == "age"
        ));
        assert!(matches!(
            rest.first(),
            Some(exec::ExecNodeBitmapExpr::PointRead { key, .. })
                if key.property.as_ref() == "score"
        ));
    }

    #[test]
    fn edge_batches_bitmap_sets_and_range_drivers_are_all_explicit() {
        let edge_source = |index_id: &str, property: &str, value: i64| {
            ir::EdgeAccessSourcePlan::new(edge_equality(
                index_id,
                property,
                literal(PropertyValue::I64(value)),
            ))
            .unwrap()
        };
        let [batched] = plans(apply(
            access(edge_path(ir::EdgeAccessPlan::Union(
                ir::AtLeast::from_pair(
                    edge_source("likes_weight", "weight", 1),
                    edge_source("likes_weight", "weight", 2),
                ),
            ))),
            context::ParamBindings::default(),
            BTreeSet::new(),
        ))
        .try_into()
        .unwrap();
        assert!(matches!(
            batched,
            exec::ExecCountPlan::EdgeBitmap(exec::ExecEdgeBitmapCountPlan {
                bitmap: exec::ExecEdgeBitmapExpr::BatchedUnionRead { .. },
                ..
            })
        ));

        let intersections = plans(apply(
            access(edge_path(ir::EdgeAccessPlan::Intersect(
                ir::AtLeast::from_pair(
                    edge_source("likes_weight", "weight", 1),
                    edge_source("likes_score", "score", 2),
                ),
            ))),
            context::ParamBindings::default(),
            BTreeSet::new(),
        ));
        assert_eq!(intersections.len(), 2);
        assert!(intersections.iter().all(|intersection| matches!(
            intersection,
            exec::ExecCountPlan::EdgeBitmap(exec::ExecEdgeBitmapCountPlan {
                bitmap: exec::ExecEdgeBitmapExpr::Intersect { .. },
                ..
            })
        )));

        let range = ir::EdgeAccessSourcePlan::new(edge_range_plan()).unwrap();
        let [range_driven] = plans(apply(
            access(edge_path(ir::EdgeAccessPlan::Intersect(
                ir::AtLeast::from_pair(range.clone(), edge_source("likes_weight", "weight", 1)),
            ))),
            context::ParamBindings::default(),
            BTreeSet::new(),
        ))
        .try_into()
        .unwrap();
        assert!(matches!(range_driven, exec::ExecCountPlan::EdgeRange(_)));

        let [materialized] = plans(apply(
            access(edge_path(ir::EdgeAccessPlan::Intersect(
                ir::AtLeast::from_pair(
                    range,
                    ir::EdgeAccessSourcePlan::new(edge_range_plan()).unwrap(),
                ),
            ))),
            context::ParamBindings::default(),
            BTreeSet::new(),
        ))
        .try_into()
        .unwrap();
        assert!(matches!(materialized, exec::ExecCountPlan::Stream(_)));

        let node_bitmap = |value: i64| {
            ir::NodeAccessSourcePlan::new(node_equality(
                "user_age",
                "age",
                literal(PropertyValue::I64(value)),
                catalog::IndexUniqueness::NonUnique,
            ))
            .unwrap()
        };
        let node_intersections = plans(apply(
            access(node_path(ir::NodeAccessPlan::Intersect(
                ir::AtLeast::from_pair(node_bitmap(1), node_bitmap(2)),
            ))),
            context::ParamBindings::default(),
            BTreeSet::new(),
        ));
        assert_eq!(node_intersections.len(), 2);
        assert!(node_intersections
            .iter()
            .all(|plan| matches!(plan, exec::ExecCountPlan::NodeBitmap(_))));

        let [node_materialized] = plans(apply(
            access(node_path(ir::NodeAccessPlan::Intersect(
                ir::AtLeast::from_pair(
                    ir::NodeAccessSourcePlan::new(node_range_plan()).unwrap(),
                    ir::NodeAccessSourcePlan::new(node_range_plan()).unwrap(),
                ),
            ))),
            context::ParamBindings::default(),
            BTreeSet::new(),
        ))
        .try_into()
        .unwrap();
        assert!(matches!(node_materialized, exec::ExecCountPlan::Stream(_)));
    }

    #[test]
    fn verified_range_intersection_encodes_driver_membership_and_window() {
        let range = ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::RangeIndex {
            index: catalog::NodeRangeIndexMeta::try_new("user_age").unwrap(),
            key: catalog::ScopedPropertyDirectionKey::try_new(
                "User",
                "age",
                RangeIndexDirection::Desc,
            )
            .unwrap(),
            range: ir::IndexRange::All,
        })
        .unwrap();
        let bitmap = ir::NodeAccessSourcePlan::new(node_equality(
            "user_active",
            "active",
            literal(PropertyValue::Bool(true)),
            catalog::IndexUniqueness::NonUnique,
        ))
        .unwrap();
        let pipeline = logical::AccessPipeline::new(
            node_path(ir::NodeAccessPlan::Intersect(ir::AtLeast::from_pair(
                range, bitmap,
            ))),
            ir::AtLeast::from_one_and_rest(
                logical::StreamPipelineOp::Skip {
                    count: ir::StreamBoundPlan::Literal(100),
                },
                vec![logical::StreamPipelineOp::Limit {
                    count: ir::StreamBoundPlan::Literal(10),
                }],
            ),
        )
        .unwrap();
        let [plan] = plans(apply(
            logical::RootStream::Access(logical::AccessStream::Pipeline(pipeline)),
            context::ParamBindings::default(),
            BTreeSet::new(),
        ))
        .try_into()
        .unwrap();
        let exec::ExecCountPlan::NodeRange(plan) = plan else {
            panic!("expected range-driven count");
        };
        assert_eq!(plan.driver.key.direction, RangeIndexDirection::Desc);
        assert!(matches!(
            plan.membership,
            exec::ExecNodeRangeMembershipPlan::BitmapFilters(ref filters)
                if filters.len() == 1
        ));
        assert_eq!(plan.window.skip, exec::ExecUsizeExpr::Literal(100));
        assert_eq!(
            plan.window.take,
            exec::ExecCountTake::AtMost(exec::ExecUsizeExpr::Literal(10))
        );
    }

    #[test]
    fn bitmap_intersection_alternatives_are_costed_in_planner_selected_order() {
        let child = |index: &str, property: &str| {
            ir::NodeAccessSourcePlan::new(node_equality(
                index,
                property,
                literal(PropertyValue::I64(1)),
                catalog::IndexUniqueness::NonUnique,
            ))
            .unwrap()
        };
        let children = ir::AtLeast::from_pair_and_rest(
            child("user_wide", "wide"),
            child("user_medium", "medium"),
            vec![child("user_narrow", "narrow")],
        );
        let alternatives = with_rule_input(|rule| {
            node_intersection_count_plans(&children, exec::ExecCountWindowPlan::identity(), rule)
                .unwrap()
        });
        assert_eq!(alternatives.len(), 3);

        let storage = cost::StorageCostProfile::default();
        for (wide, medium, narrow, expected) in [
            (1, 10, 100, "wide"),
            (100, 1, 10, "medium"),
            (10, 100, 1, "narrow"),
        ] {
            let stats = context::StatsSnapshot::default()
                .with_node_eq_cardinality(
                    catalog::ScopedPropertyKey::try_new("User", "wide").unwrap(),
                    wide,
                )
                .with_node_eq_cardinality(
                    catalog::ScopedPropertyKey::try_new("User", "medium").unwrap(),
                    medium,
                )
                .with_node_eq_cardinality(
                    catalog::ScopedPropertyKey::try_new("User", "narrow").unwrap(),
                    narrow,
                );
            let winner = alternatives
                .iter()
                .min_by_key(|plan| count_cost(plan, &stats, &storage).latency)
                .unwrap();
            let exec::ExecCountPlan::NodeBitmap(exec::ExecNodeBitmapCountPlan {
                bitmap: exec::ExecNodeBitmapExpr::Intersect { driver, .. },
                ..
            }) = winner
            else {
                panic!("expected a bitmap intersection alternative")
            };
            assert!(matches!(
                driver.as_ref(),
                exec::ExecNodeBitmapExpr::PointRead { key, .. }
                    if key.property.as_ref() == expected
            ));
        }
    }

    #[test]
    fn verified_range_costs_membership_order_and_literal_early_stopping() {
        let range_key =
            catalog::ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc)
                .unwrap();
        let range = ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::RangeIndex {
            index: catalog::NodeRangeIndexMeta::try_new("user_age").unwrap(),
            key: range_key.clone(),
            range: ir::IndexRange::All,
        })
        .unwrap();
        let filter = |index: &str, property: &str| {
            ir::NodeAccessSourcePlan::new(node_equality(
                index,
                property,
                literal(PropertyValue::Bool(true)),
                catalog::IndexUniqueness::NonUnique,
            ))
            .unwrap()
        };
        let children = ir::AtLeast::from_pair_and_rest(
            range,
            filter("user_wide", "wide"),
            vec![filter("user_narrow", "narrow")],
        );
        let window = exec::ExecCountWindowPlan::identity()
            .then_skip(exec::ExecUsizeExpr::literal(20))
            .then_limit(exec::ExecUsizeExpr::literal(5));
        let alternatives =
            with_rule_input(|rule| node_intersection_count_plans(&children, window, rule).unwrap());
        assert_eq!(alternatives.len(), 2);

        let stats = context::StatsSnapshot::default()
            .with_node_range_cardinality(range_key, 1_000)
            .with_node_eq_cardinality(
                catalog::ScopedPropertyKey::try_new("User", "wide").unwrap(),
                800,
            )
            .with_node_eq_cardinality(
                catalog::ScopedPropertyKey::try_new("User", "narrow").unwrap(),
                500,
            );
        let storage = cost::StorageCostProfile::default();
        let winner = alternatives
            .iter()
            .min_by_key(|plan| count_cost(plan, &stats, &storage).latency)
            .unwrap();
        let winner_cost = count_cost(winner, &stats, &storage);
        let exec::ExecCountPlan::NodeRange(exec::ExecNodeRangeCountPlan {
            membership: exec::ExecNodeRangeMembershipPlan::BitmapFilters(filters),
            ..
        }) = winner
        else {
            panic!("expected a range membership alternative")
        };
        assert!(matches!(
            filters.first(),
            Some(exec::ExecNodeBitmapExpr::PointRead { key, .. })
                if key.property.as_ref() == "narrow"
        ));
        assert_eq!(winner_cost.range_nexts, 63);
        assert_eq!(winner_cost.authoritative_graph_reads, 63);
        assert_eq!(winner_cost.object_reads, 65);
    }

    #[test]
    fn set_order_generation_obeys_the_planner_guardrail() {
        let values = ir::AtLeast::from_one_and_rest(1, vec![2, 3]);
        assert_eq!(planner_selected_orders(&values, 1).len(), 1);
        assert_eq!(planner_selected_orders(&values, 3).len(), 3);
        assert_eq!(
            set_order_alternative_limit(&context::PlannerLimits {
                max_index_union_branches: context::IndexUnionBranchLimit::Disabled,
            }),
            1
        );
        assert_eq!(
            set_order_alternative_limit(&context::PlannerLimits {
                max_index_union_branches: context::IndexUnionBranchLimit::limited(2).unwrap(),
            }),
            2
        );
    }

    #[test]
    fn set_order_guardrail_stops_range_driver_enumeration() {
        let node_children = ir::AtLeast::from_pair_and_rest(
            ir::NodeAccessSourcePlan::new(node_range_plan()).unwrap(),
            ir::NodeAccessSourcePlan::new(node_equality(
                "user_active",
                "active",
                literal(PropertyValue::Bool(true)),
                catalog::IndexUniqueness::NonUnique,
            ))
            .unwrap(),
            vec![ir::NodeAccessSourcePlan::new(node_equality(
                "user_status",
                "status",
                literal(PropertyValue::from("enabled")),
                catalog::IndexUniqueness::NonUnique,
            ))
            .unwrap()],
        );
        let edge_children = ir::AtLeast::from_pair_and_rest(
            ir::EdgeAccessSourcePlan::new(edge_range_plan()).unwrap(),
            ir::EdgeAccessSourcePlan::new(edge_equality(
                "likes_active",
                "active",
                literal(PropertyValue::Bool(true)),
            ))
            .unwrap(),
            vec![ir::EdgeAccessSourcePlan::new(edge_equality(
                "likes_status",
                "status",
                literal(PropertyValue::from("enabled")),
            ))
            .unwrap()],
        );
        let expr = logical::LogicalExpr::StreamCardinality(logical::StreamCardinality::new(
            access(node_path(ir::NodeAccessPlan::AllScan)),
        ));
        let storage = cost::StorageCostProfile::default();
        let indexes = catalog::IndexCatalogSnapshot::default();
        let limits = context::PlannerLimits {
            max_index_union_branches: context::IndexUnionBranchLimit::Disabled,
        };
        let stats = context::StatsSnapshot::default();
        let rule = optimizer::RuleInput {
            expr: &expr,
            planner_limits: &limits,
            stats: &stats,
            storage: &storage,
            indexes: &indexes,
        };

        assert_eq!(
            node_intersection_count_plans(
                &node_children,
                exec::ExecCountWindowPlan::identity(),
                &rule,
            )
            .unwrap()
            .len(),
            1
        );
        assert_eq!(
            edge_intersection_count_plans(
                &edge_children,
                exec::ExecCountWindowPlan::identity(),
                &rule,
            )
            .unwrap()
            .len(),
            1
        );
    }

    #[test]
    fn bitmap_row_estimates_cover_nested_node_and_edge_sets() {
        let storage = cost::StorageCostProfile::default();
        let stats = context::StatsSnapshot::default()
            .with_node_eq_cardinality(
                catalog::ScopedPropertyKey::try_new("User", "status").unwrap(),
                3,
            )
            .with_edge_eq_cardinality(
                catalog::ScopedPropertyKey::try_new("LIKES", "status").unwrap(),
                5,
            );
        let node_union = exec::ExecNodeBitmapExpr::Union {
            driver: Box::new(exec_node_point("first")),
            rest: ir::AtLeast::from_one(exec_node_point("second")),
        };
        let node_intersection = exec::ExecNodeBitmapExpr::Intersect {
            driver: Box::new(node_union.clone()),
            rest: ir::AtLeast::from_one(exec_node_point("third")),
        };
        assert_eq!(node_bitmap_rows(&node_union, &stats, &storage).as_rows(), 6);
        assert_eq!(
            node_bitmap_rows(&node_intersection, &stats, &storage).as_rows(),
            3
        );

        let edge_union = exec::ExecEdgeBitmapExpr::Union {
            driver: Box::new(exec_edge_point("first")),
            rest: ir::AtLeast::from_one(exec_edge_point("second")),
        };
        let edge_intersection = exec::ExecEdgeBitmapExpr::Intersect {
            driver: Box::new(edge_union.clone()),
            rest: ir::AtLeast::from_one(exec_edge_point("third")),
        };
        assert_eq!(
            edge_bitmap_rows(&edge_union, &stats, &storage).as_rows(),
            10
        );
        assert_eq!(
            edge_bitmap_rows(&edge_intersection, &stats, &storage).as_rows(),
            5
        );
    }

    #[test]
    fn verified_range_cost_boundaries_and_dynamic_windows_are_exhaustive() {
        let storage = cost::StorageCostProfile::default();
        let zero_window =
            exec::ExecCountWindowPlan::identity().then_limit(exec::ExecUsizeExpr::literal(0));
        let zero_cost = verified_range_count_cost(
            cost::EstimatedRows::rows(10),
            &[(cost::CostVector::ZERO, cost::EstimatedRows::rows(5))],
            &zero_window,
            &storage,
        );
        assert_eq!(zero_cost.range_nexts, 0);

        let no_matches = verified_range_count_cost(
            cost::EstimatedRows::rows(10),
            &[(cost::CostVector::ZERO, cost::EstimatedRows::rows(0))],
            &exec::ExecCountWindowPlan::identity().then_limit(exec::ExecUsizeExpr::literal(1)),
            &storage,
        );
        assert_eq!(no_matches.range_nexts, 10);
        assert_eq!(apply_membership_selectivity(10, 5, 0), 0);

        let parameter = exec::ExecUsizeExpr::Param(name("bound"));
        assert_eq!(
            static_window_threshold(&exec::ExecCountWindowPlan {
                skip: parameter.clone(),
                take: exec::ExecCountTake::AtMost(exec::ExecUsizeExpr::literal(1)),
            }),
            None
        );
        assert_eq!(
            static_window_threshold(&exec::ExecCountWindowPlan {
                skip: exec::ExecUsizeExpr::literal(1),
                take: exec::ExecCountTake::AtMost(parameter),
            }),
            None
        );
    }

    #[test]
    fn positioned_windows_distinct_and_variable_write_keep_their_semantic_boundaries() {
        let predicate =
            ir::PredicatePlan::new(helix_ast::expr::Predicate::eq("active", true)).unwrap();
        let positioned = logical::AccessPipeline::new(
            node_path(ir::NodeAccessPlan::AllScan),
            ir::AtLeast::from_one_and_rest(
                logical::StreamPipelineOp::Limit {
                    count: ir::StreamBoundPlan::Literal(5),
                },
                vec![logical::StreamPipelineOp::Filter { predicate }],
            ),
        )
        .unwrap();
        let [plan] = plans(apply(
            logical::RootStream::Access(logical::AccessStream::Pipeline(positioned)),
            context::ParamBindings::default(),
            BTreeSet::new(),
        ))
        .try_into()
        .unwrap();
        assert!(matches!(
            plan,
            exec::ExecCountPlan::Stream(exec::ExecCountStreamPlan {
                cursor: exec::ExecCountCursorPlan::Filter { input, .. },
                ..
            }) if matches!(input.as_ref(), exec::ExecCountCursorPlan::Window { .. })
        ));

        let distinct = logical::AccessDistinct::new(node_path(ir::NodeAccessPlan::AllScan));
        assert!(matches!(
            plans(apply(
                logical::RootStream::Access(logical::AccessStream::Distinct(distinct)),
                context::ParamBindings::default(),
                BTreeSet::new(),
            ))
            .as_slice(),
            [exec::ExecCountPlan::Stream(exec::ExecCountStreamPlan {
                cursor: exec::ExecCountCursorPlan::Distinct { .. },
                ..
            })]
        ));

        let write = logical::AccessPipeline::new(
            node_path(ir::NodeAccessPlan::AllScan),
            ir::AtLeast::from_one(logical::StreamPipelineOp::VariableWrite {
                op: logical::StreamVariableWriteOp::Store(name("saved")),
            }),
        )
        .unwrap();
        assert!(matches!(
            plans(apply(
                logical::RootStream::Access(logical::AccessStream::Pipeline(write)),
                context::ParamBindings::default(),
                BTreeSet::new(),
            ))
            .as_slice(),
            [exec::ExecCountPlan::InputRows { .. }]
        ));
    }

    #[test]
    fn every_node_access_source_has_an_exact_count_family() {
        let point_source = || {
            ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::PointIds { ids: element_ids() })
                .unwrap()
        };
        let cases = vec![
            (
                ir::NodeAccessPlan::Empty,
                physical::PhysicalCardinality::Constant,
            ),
            (
                ir::NodeAccessPlan::PointIds { ids: element_ids() },
                physical::PhysicalCardinality::VerifiedPointReads,
            ),
            (
                ir::NodeAccessPlan::FromParam {
                    param: name("nodes"),
                },
                physical::PhysicalCardinality::RuntimeInput,
            ),
            (
                ir::NodeAccessPlan::FromVar {
                    variable: name("nodes"),
                },
                physical::PhysicalCardinality::RuntimeInput,
            ),
            (
                ir::NodeAccessPlan::AllScan,
                physical::PhysicalCardinality::FullScan,
            ),
            (
                ir::NodeAccessPlan::LabelScan {
                    label: name("User"),
                },
                physical::PhysicalCardinality::LabelBitmap,
            ),
            (
                node_range_plan(),
                physical::PhysicalCardinality::VerifiedRange,
            ),
            (
                node_vector_search(),
                physical::PhysicalCardinality::VectorSearch,
            ),
            (
                node_text_search(),
                physical::PhysicalCardinality::TextSearch,
            ),
            (
                ir::NodeAccessPlan::Union(ir::AtLeast::from_pair(point_source(), point_source())),
                physical::PhysicalCardinality::SetUnion,
            ),
            (
                ir::NodeAccessPlan::Intersect(ir::AtLeast::from_pair(
                    point_source(),
                    point_source(),
                )),
                physical::PhysicalCardinality::SetIntersection,
            ),
        ];

        for (source, expected) in cases {
            let alternatives = plans(apply(
                access(node_path(source)),
                context::ParamBindings::default(),
                BTreeSet::new(),
            ));
            assert_eq!(alternatives.len(), 1);
            assert_eq!(
                physical::PhysicalCountPlan::new(alternatives.into_iter().next().unwrap()).family(),
                expected
            );
        }
    }

    #[test]
    fn every_edge_access_source_has_an_exact_count_family() {
        let point_source = || {
            ir::EdgeAccessSourcePlan::new(ir::EdgeAccessPlan::PointIds { ids: element_ids() })
                .unwrap()
        };
        let cases = vec![
            (
                ir::EdgeAccessPlan::Empty,
                physical::PhysicalCardinality::Constant,
            ),
            (
                ir::EdgeAccessPlan::PointIds { ids: element_ids() },
                physical::PhysicalCardinality::VerifiedPointReads,
            ),
            (
                ir::EdgeAccessPlan::FromParam {
                    param: name("edges"),
                },
                physical::PhysicalCardinality::RuntimeInput,
            ),
            (
                ir::EdgeAccessPlan::FromVar {
                    variable: name("edges"),
                },
                physical::PhysicalCardinality::RuntimeInput,
            ),
            (
                ir::EdgeAccessPlan::AllScan,
                physical::PhysicalCardinality::FullScan,
            ),
            (
                ir::EdgeAccessPlan::LabelScan {
                    label: name("LIKES"),
                },
                physical::PhysicalCardinality::LabelBitmap,
            ),
            (
                edge_range_plan(),
                physical::PhysicalCardinality::VerifiedRange,
            ),
            (
                edge_vector_search(),
                physical::PhysicalCardinality::VectorSearch,
            ),
            (
                edge_text_search(),
                physical::PhysicalCardinality::TextSearch,
            ),
            (
                ir::EdgeAccessPlan::Union(ir::AtLeast::from_pair(point_source(), point_source())),
                physical::PhysicalCardinality::SetUnion,
            ),
            (
                ir::EdgeAccessPlan::Intersect(ir::AtLeast::from_pair(
                    point_source(),
                    point_source(),
                )),
                physical::PhysicalCardinality::SetIntersection,
            ),
        ];

        for (source, expected) in cases {
            let alternatives = plans(apply(
                access(edge_path(source)),
                context::ParamBindings::default(),
                BTreeSet::new(),
            ));
            assert_eq!(alternatives.len(), 1);
            assert_eq!(
                physical::PhysicalCountPlan::new(alternatives.into_iter().next().unwrap()).family(),
                expected
            );
        }
    }

    #[test]
    fn access_unary_shapes_preserve_only_semantically_safe_windows() {
        let path = node_path(ir::NodeAccessPlan::AllScan);
        let ordering = ir::OrderKeys::from(ir::OrderKey {
            property: name("age"),
            order: Order::Asc,
        });
        for access_stream in [
            logical::AccessStream::Order(logical::AccessOrder::new(path.clone(), ordering.clone())),
            logical::AccessStream::Window(logical::AccessWindow::new(
                path.clone(),
                logical::AccessWindowRange::new(2, None).unwrap(),
            )),
            logical::AccessStream::Window(logical::AccessWindow::new(
                path.clone(),
                logical::AccessWindowRange::new(2, Some(5)).unwrap(),
            )),
            logical::AccessStream::Filter(logical::AccessFilter::new(path.clone(), predicate())),
        ] {
            assert_eq!(
                plans(apply(
                    logical::RootStream::Access(access_stream),
                    context::ParamBindings::default(),
                    BTreeSet::new(),
                ))
                .len(),
                1
            );
        }
    }

    #[test]
    fn recursive_access_cursors_cover_every_unary_shape_and_residual_source() {
        with_rule_input(|rule| {
            let node_residual = ir::NodeAccessPlan::ScanThenFilter {
                source: ir::NodeAccessSourcePlan::from_unfiltered(ir::NodeAccessPlan::AllScan),
                residual: predicate(),
            };
            assert!(matches!(
                node_count_plans(&node_residual, exec::ExecCountWindowPlan::identity(), rule,)
                    .unwrap()
                    .as_slice(),
                [exec::ExecCountPlan::Stream(exec::ExecCountStreamPlan {
                    cursor: exec::ExecCountCursorPlan::Filter { .. },
                    ..
                })]
            ));
            let edge_residual = ir::EdgeAccessPlan::ScanThenFilter {
                source: ir::EdgeAccessSourcePlan::from_unfiltered(ir::EdgeAccessPlan::AllScan),
                residual: predicate(),
            };
            assert!(matches!(
                edge_count_plans(&edge_residual, exec::ExecCountWindowPlan::identity(), rule,)
                    .unwrap()
                    .as_slice(),
                [exec::ExecCountPlan::Stream(exec::ExecCountStreamPlan {
                    cursor: exec::ExecCountCursorPlan::Filter { .. },
                    ..
                })]
            ));

            let node = node_path(ir::NodeAccessPlan::AllScan);
            let edge = edge_path(ir::EdgeAccessPlan::AllScan);
            let streams = [
                logical::AccessStream::Path(edge),
                logical::AccessStream::Order(logical::AccessOrder::new(node.clone(), ordering())),
                logical::AccessStream::Distinct(logical::AccessDistinct::new(node.clone())),
                logical::AccessStream::Window(logical::AccessWindow::new(
                    node.clone(),
                    logical::AccessWindowRange::new(1, Some(3)).unwrap(),
                )),
                logical::AccessStream::Filter(logical::AccessFilter::new(
                    node.clone(),
                    predicate(),
                )),
                logical::AccessStream::Pipeline(
                    logical::AccessPipeline::new(
                        node,
                        ir::AtLeast::from_one(logical::StreamPipelineOp::Distinct),
                    )
                    .unwrap(),
                ),
            ];
            for stream in &streams {
                assert!(access_stream_cursor(stream, rule).is_ok());
            }
            let write = logical::AccessStream::Pipeline(
                logical::AccessPipeline::new(
                    node_path(ir::NodeAccessPlan::AllScan),
                    ir::AtLeast::from_one(logical::StreamPipelineOp::VariableWrite {
                        op: logical::StreamVariableWriteOp::Store(name("saved")),
                    }),
                )
                .unwrap(),
            );
            assert!(access_stream_cursor(&write, rule).is_err());
            assert!(matches!(
                access_count_plans(&write, rule).unwrap().as_slice(),
                [exec::ExecCountPlan::InputRows { .. }]
            ));
        });
    }

    #[test]
    fn bitmap_batch_detection_covers_empty_non_point_and_identity_mismatch_inputs() {
        let node_other_key = exec::ExecNodeBitmapExpr::PointRead {
            index: catalog::NodeEqualityIndexMeta::new(name("node_eq:User:status"))
                .try_into()
                .unwrap(),
            key: catalog::ScopedPropertyKey::try_new("User", "role").unwrap(),
            value: exec_indexed("admin"),
        };
        let node_non_point = exec::ExecNodeBitmapExpr::Union {
            driver: Box::new(exec_node_point("active")),
            rest: ir::AtLeast::from_one(exec_node_point("paused")),
        };
        assert!(node_bitmap_batch(&[]).is_none());
        assert!(node_bitmap_batch(core::slice::from_ref(&node_non_point)).is_none());
        assert!(node_bitmap_batch(&[exec_node_point("active"), node_non_point.clone()]).is_none());
        assert!(node_bitmap_batch(&[exec_node_point("active"), node_other_key]).is_none());

        let edge_other_key = exec::ExecEdgeBitmapExpr::PointRead {
            index: exec::ExecEdgeNonUniqueEqualityIndex::new(catalog::EdgeEqualityIndexMeta::new(
                name("edge_eq:LIKES:status"),
            )),
            key: catalog::ScopedPropertyKey::try_new("LIKES", "kind").unwrap(),
            value: exec_indexed("friend"),
        };
        let edge_non_point = exec::ExecEdgeBitmapExpr::Intersect {
            driver: Box::new(exec_edge_point("active")),
            rest: ir::AtLeast::from_one(exec_edge_point("paused")),
        };
        assert!(edge_bitmap_batch(&[]).is_none());
        assert!(edge_bitmap_batch(core::slice::from_ref(&edge_non_point)).is_none());
        assert!(edge_bitmap_batch(&[exec_edge_point("active"), edge_non_point.clone()]).is_none());
        assert!(edge_bitmap_batch(&[exec_edge_point("active"), edge_other_key]).is_none());
    }

    #[test]
    fn bitmap_classification_declines_unique_null_nan_dynamic_and_non_equality_sources() {
        let node_cases = [
            node_equality(
                "node_eq:User:email",
                "email",
                literal(PropertyValue::from("alice@example.test")),
                catalog::IndexUniqueness::Unique,
            ),
            node_equality(
                "node_eq:User:status",
                "status",
                literal(PropertyValue::Null),
                catalog::IndexUniqueness::NonUnique,
            ),
            node_equality(
                "node_eq:User:score",
                "score",
                literal(PropertyValue::F64(f64::NAN)),
                catalog::IndexUniqueness::NonUnique,
            ),
            ir::NodeAccessPlan::AllScan,
        ];
        with_rule_input(|rule| {
            for source in &node_cases {
                assert!(node_bitmap_expr(source, rule).unwrap().is_none());
            }
            for source in [
                edge_equality(
                    "edge_eq:LIKES:status",
                    "status",
                    literal(PropertyValue::Null),
                ),
                edge_equality(
                    "edge_eq:LIKES:score",
                    "score",
                    literal(PropertyValue::F32(f32::NAN)),
                ),
                ir::EdgeAccessPlan::AllScan,
            ] {
                assert!(edge_bitmap_expr(&source, rule).unwrap().is_none());
            }

            let node_children = ir::AtLeast::from_pair(
                ir::NodeAccessSourcePlan::new(node_equality(
                    "node_eq:User:status",
                    "status",
                    literal(PropertyValue::from("active")),
                    catalog::IndexUniqueness::NonUnique,
                ))
                .unwrap(),
                ir::NodeAccessSourcePlan::new(node_equality(
                    "node_eq:User:role",
                    "role",
                    literal(PropertyValue::from("admin")),
                    catalog::IndexUniqueness::NonUnique,
                ))
                .unwrap(),
            );
            assert!(
                node_bitmap_expr(&ir::NodeAccessPlan::Union(node_children.clone()), rule)
                    .unwrap()
                    .is_some()
            );
            assert!(
                node_bitmap_expr(&ir::NodeAccessPlan::Intersect(node_children), rule)
                    .unwrap()
                    .is_some()
            );

            let edge_children = ir::AtLeast::from_pair(
                ir::EdgeAccessSourcePlan::new(edge_equality(
                    "edge_eq:LIKES:status",
                    "status",
                    literal(PropertyValue::from("active")),
                ))
                .unwrap(),
                ir::EdgeAccessSourcePlan::new(edge_equality(
                    "edge_eq:LIKES:kind",
                    "kind",
                    literal(PropertyValue::from("friend")),
                ))
                .unwrap(),
            );
            assert!(
                edge_bitmap_expr(&ir::EdgeAccessPlan::Union(edge_children.clone()), rule)
                    .unwrap()
                    .is_some()
            );
            assert!(
                edge_bitmap_expr(&ir::EdgeAccessPlan::Intersect(edge_children), rule)
                    .unwrap()
                    .is_some()
            );
        });

        let late_scope = name("items");
        let shadowed_field = name("status");
        with_rule_input_bindings(
            context::ParamBindings::default(),
            BTreeSet::from([late_scope]),
            |rule| {
                assert!(node_bitmap_expr(
                    &node_equality(
                        "node_eq:User:status",
                        "status",
                        ir::IndexValue::Param(shadowed_field.clone()),
                        catalog::IndexUniqueness::NonUnique,
                    ),
                    rule,
                )
                .unwrap()
                .is_none());
                assert!(edge_bitmap_expr(
                    &edge_equality(
                        "edge_eq:LIKES:status",
                        "status",
                        ir::IndexValue::Param(shadowed_field),
                    ),
                    rule,
                )
                .unwrap()
                .is_none());
            },
        );
    }

    #[test]
    fn every_nested_planning_error_is_propagated_without_late_algorithm_selection() {
        let invalid_bound =
            || ir::StreamBoundPlan::Expr(ir::StreamBoundExprPlan::new(Expr::Id).unwrap());
        let invalid_limit = || logical::StreamPipelineOp::Limit {
            count: invalid_bound(),
        };
        let invalid_skip = logical::StreamPipelineOp::Skip {
            count: invalid_bound(),
        };
        let invalid_range_start = ir::StreamRangePlan::Dynamic(
            ir::StreamDynamicRange::new(
                invalid_bound(),
                ir::StreamBoundPlan::Expr(
                    ir::StreamBoundExprPlan::new(Expr::param("end")).unwrap(),
                ),
            )
            .unwrap(),
        );
        let invalid_range_end = ir::StreamRangePlan::Dynamic(
            ir::StreamDynamicRange::new(
                ir::StreamBoundPlan::Expr(
                    ir::StreamBoundExprPlan::new(Expr::param("start")).unwrap(),
                ),
                invalid_bound(),
            )
            .unwrap(),
        );
        assert!(append_window(exec::ExecCountWindowPlan::identity(), &invalid_limit()).is_err());
        assert!(append_window(exec::ExecCountWindowPlan::identity(), &invalid_skip).is_err());
        assert!(range_exprs(&invalid_range_start).is_err());
        assert!(range_exprs(&invalid_range_end).is_err());
        assert!(append_window(
            exec::ExecCountWindowPlan::identity(),
            &logical::StreamPipelineOp::Range {
                range: invalid_range_start.clone(),
            },
        )
        .is_err());
        assert!(trailing_count_window(&[invalid_limit()]).is_err());
        assert!(fold_cursor(exec::ExecCountCursorPlan::NodeFullScan, &[invalid_limit()],).is_err());

        let missing_node = || {
            node_equality(
                "node_eq:User:status",
                "status",
                ir::IndexValue::Param(name("missing")),
                catalog::IndexUniqueness::NonUnique,
            )
        };
        let missing_edge = || {
            edge_equality(
                "edge_eq:LIKES:status",
                "status",
                ir::IndexValue::Param(name("missing")),
            )
        };
        let node_source = |plan| ir::NodeAccessSourcePlan::new(plan).unwrap();
        let edge_source = |plan| ir::EdgeAccessSourcePlan::new(plan).unwrap();

        with_rule_input(|rule| {
            let missing_path = node_path(missing_node());
            for stream in [
                logical::AccessStream::Path(missing_path.clone()),
                logical::AccessStream::Order(logical::AccessOrder::new(
                    missing_path.clone(),
                    ordering(),
                )),
                logical::AccessStream::Distinct(logical::AccessDistinct::new(missing_path.clone())),
                logical::AccessStream::Window(logical::AccessWindow::new(
                    missing_path.clone(),
                    logical::AccessWindowRange::new(1, Some(2)).unwrap(),
                )),
                logical::AccessStream::Filter(logical::AccessFilter::new(
                    missing_path.clone(),
                    predicate(),
                )),
                logical::AccessStream::Pipeline(
                    logical::AccessPipeline::new(
                        missing_path,
                        ir::AtLeast::from_one(logical::StreamPipelineOp::Distinct),
                    )
                    .unwrap(),
                ),
            ] {
                assert!(access_stream_cursor(&stream, rule).is_err());
                assert!(access_count_plans(&stream, rule).is_err());
            }
            assert!(access_stream_cursor(
                &logical::AccessStream::Path(edge_path(missing_edge())),
                rule,
            )
            .is_err());

            let invalid_access_suffix = logical::AccessPipeline::new(
                node_path(ir::NodeAccessPlan::AllScan),
                ir::AtLeast::from_one(invalid_limit()),
            )
            .unwrap();
            assert!(access_pipeline_count(&invalid_access_suffix, rule).is_err());
            let invalid_access_cursor = logical::AccessPipeline::new(
                node_path(missing_node()),
                ir::AtLeast::from_one(logical::StreamPipelineOp::Distinct),
            )
            .unwrap();
            assert!(access_pipeline_count(&invalid_access_cursor, rule).is_err());
            let invalid_access_fold = logical::AccessPipeline::new(
                node_path(ir::NodeAccessPlan::AllScan),
                ir::AtLeast::from_one_and_rest(
                    invalid_limit(),
                    vec![logical::StreamPipelineOp::Distinct],
                ),
            )
            .unwrap();
            assert!(access_pipeline_count(&invalid_access_fold, rule).is_err());

            let invalid_root_suffix = logical::RootPipeline::new(
                access(node_path(ir::NodeAccessPlan::AllScan)),
                ir::AtLeast::from_one(invalid_limit()),
            )
            .unwrap();
            assert!(root_pipeline_count(&invalid_root_suffix, rule).is_err());
            assert!(count_plans(
                &logical::RootStream::Pipeline(Box::new(invalid_root_suffix)),
                rule,
            )
            .is_err());
            let invalid_root_cursor = logical::RootPipeline::new(
                logical::RootStream::Access(logical::AccessStream::Path(node_path(missing_node()))),
                ir::AtLeast::from_one(logical::StreamPipelineOp::Distinct),
            )
            .unwrap();
            assert!(root_pipeline_count(&invalid_root_cursor, rule).is_err());
            let invalid_root_fold = logical::RootPipeline::new(
                access(node_path(ir::NodeAccessPlan::AllScan)),
                ir::AtLeast::from_one_and_rest(
                    invalid_limit(),
                    vec![logical::StreamPipelineOp::Distinct],
                ),
            )
            .unwrap();
            assert!(root_pipeline_count(&invalid_root_fold, rule).is_err());

            let node_bitmap_error = ir::NodeAccessPlan::Union(ir::AtLeast::from_pair(
                node_source(missing_node()),
                node_source(node_equality(
                    "node_eq:User:role",
                    "role",
                    literal(PropertyValue::from("admin")),
                    catalog::IndexUniqueness::NonUnique,
                )),
            ));
            assert!(node_count_plans(
                &node_bitmap_error,
                exec::ExecCountWindowPlan::identity(),
                rule,
            )
            .is_err());
            let node_cursor_error = ir::NodeAccessPlan::Union(ir::AtLeast::from_pair(
                node_source(ir::NodeAccessPlan::AllScan),
                node_source(missing_node()),
            ));
            assert!(node_count_plans(
                &node_cursor_error,
                exec::ExecCountWindowPlan::identity(),
                rule,
            )
            .is_err());
            let node_residual_error = ir::NodeAccessPlan::ScanThenFilter {
                source: node_source(missing_node()),
                residual: predicate(),
            };
            assert!(node_count_plans(
                &node_residual_error,
                exec::ExecCountWindowPlan::identity(),
                rule,
            )
            .is_err());
            let node_intersection_error = ir::NodeAccessPlan::Intersect(ir::AtLeast::from_pair(
                node_source(missing_node()),
                node_source(node_range_plan()),
            ));
            assert!(node_count_plans(
                &node_intersection_error,
                exec::ExecCountWindowPlan::identity(),
                rule,
            )
            .is_err());
            let node_range_filter_error = ir::NodeAccessPlan::Intersect(ir::AtLeast::from_pair(
                node_source(node_range_plan()),
                node_source(missing_node()),
            ));
            assert!(node_count_plans(
                &node_range_filter_error,
                exec::ExecCountWindowPlan::identity(),
                rule,
            )
            .is_err());
            let node_fallback_error =
                ir::NodeAccessPlan::Intersect(ir::AtLeast::from_pair_and_rest(
                    node_source(node_range_plan()),
                    node_source(node_range_plan()),
                    vec![node_source(missing_node())],
                ));
            assert!(node_count_plans(
                &node_fallback_error,
                exec::ExecCountWindowPlan::identity(),
                rule,
            )
            .is_err());

            let edge_bitmap_error = ir::EdgeAccessPlan::Union(ir::AtLeast::from_pair(
                edge_source(missing_edge()),
                edge_source(edge_equality(
                    "edge_eq:LIKES:kind",
                    "kind",
                    literal(PropertyValue::from("friend")),
                )),
            ));
            assert!(edge_count_plans(
                &edge_bitmap_error,
                exec::ExecCountWindowPlan::identity(),
                rule,
            )
            .is_err());
            let edge_cursor_error = ir::EdgeAccessPlan::Union(ir::AtLeast::from_pair(
                edge_source(ir::EdgeAccessPlan::AllScan),
                edge_source(missing_edge()),
            ));
            assert!(edge_count_plans(
                &edge_cursor_error,
                exec::ExecCountWindowPlan::identity(),
                rule,
            )
            .is_err());
            let edge_residual_error = ir::EdgeAccessPlan::ScanThenFilter {
                source: edge_source(missing_edge()),
                residual: predicate(),
            };
            assert!(edge_count_plans(
                &edge_residual_error,
                exec::ExecCountWindowPlan::identity(),
                rule,
            )
            .is_err());
            let edge_intersection_error = ir::EdgeAccessPlan::Intersect(ir::AtLeast::from_pair(
                edge_source(missing_edge()),
                edge_source(edge_range_plan()),
            ));
            assert!(edge_count_plans(
                &edge_intersection_error,
                exec::ExecCountWindowPlan::identity(),
                rule,
            )
            .is_err());
            let edge_range_filter_error = ir::EdgeAccessPlan::Intersect(ir::AtLeast::from_pair(
                edge_source(edge_range_plan()),
                edge_source(missing_edge()),
            ));
            assert!(edge_count_plans(
                &edge_range_filter_error,
                exec::ExecCountWindowPlan::identity(),
                rule,
            )
            .is_err());
            let edge_fallback_error =
                ir::EdgeAccessPlan::Intersect(ir::AtLeast::from_pair_and_rest(
                    edge_source(edge_range_plan()),
                    edge_source(edge_range_plan()),
                    vec![edge_source(missing_edge())],
                ));
            assert!(edge_count_plans(
                &edge_fallback_error,
                exec::ExecCountWindowPlan::identity(),
                rule,
            )
            .is_err());
        });
    }

    #[test]
    fn pipeline_operator_matrix_normalizes_only_the_trailing_safe_region() {
        let dynamic = |param: &str| {
            ir::StreamBoundPlan::Expr(
                ir::StreamBoundExprPlan::new(helix_ast::expr::Expr::param(param)).unwrap(),
            )
        };
        let window_ops = vec![
            logical::StreamPipelineOp::Window {
                window: logical::AccessWindowRange::new(2, None).unwrap(),
            },
            logical::StreamPipelineOp::Window {
                window: logical::AccessWindowRange::new(1, Some(5)).unwrap(),
            },
            logical::StreamPipelineOp::Limit {
                count: ir::StreamBoundPlan::Literal(8),
            },
            logical::StreamPipelineOp::Skip {
                count: dynamic("skip"),
            },
            logical::StreamPipelineOp::Range {
                range: ir::StreamRangePlan::Literal(ir::StreamLiteralRange::new(1, 4).unwrap()),
            },
            logical::StreamPipelineOp::Range {
                range: ir::StreamRangePlan::Dynamic(
                    ir::StreamDynamicRange::new(dynamic("start"), dynamic("end")).unwrap(),
                ),
            },
            logical::StreamPipelineOp::Order {
                ordering: ordering(),
            },
        ];
        let (prefix, normalized) = trailing_count_window(&window_ops).unwrap();
        assert!(prefix.is_empty());
        assert_ne!(normalized, exec::ExecCountWindowPlan::identity());

        let semantic_ops = vec![
            logical::StreamPipelineOp::Filter {
                predicate: predicate(),
            },
            logical::StreamPipelineOp::Expand { plan: expand() },
            logical::StreamPipelineOp::VectorSearch {
                plan: restricted_vector(),
            },
            logical::StreamPipelineOp::TextSearch {
                plan: restricted_text(),
            },
            logical::StreamPipelineOp::Variable {
                op: logical::PureStreamVariableOp::Select(name("saved")),
            },
            logical::StreamPipelineOp::Distinct,
        ];
        for op in &semantic_ops {
            assert!(append_window(exec::ExecCountWindowPlan::identity(), op).is_err());
            assert!(fold_cursor(
                exec::ExecCountCursorPlan::NodeFullScan,
                std::slice::from_ref(op),
            )
            .is_ok());
        }
        assert!(matches!(
            fold_cursor(
                exec::ExecCountCursorPlan::NodeFullScan,
                &[logical::StreamPipelineOp::Order {
                    ordering: ordering(),
                }],
            )
            .unwrap(),
            exec::ExecCountCursorPlan::Order { .. }
        ));
        assert!(matches!(
            fold_cursor(
                exec::ExecCountCursorPlan::NodeFullScan,
                &[logical::StreamPipelineOp::Limit {
                    count: ir::StreamBoundPlan::Literal(2),
                }],
            )
            .unwrap(),
            exec::ExecCountCursorPlan::Window { .. }
        ));

        let mixed = vec![
            logical::StreamPipelineOp::Limit {
                count: ir::StreamBoundPlan::Literal(5),
            },
            logical::StreamPipelineOp::Filter {
                predicate: predicate(),
            },
            logical::StreamPipelineOp::Skip {
                count: ir::StreamBoundPlan::Literal(1),
            },
        ];
        let (prefix, normalized) = trailing_count_window(&mixed).unwrap();
        assert_eq!(prefix.len(), 2);
        assert_eq!(normalized.skip, exec::ExecUsizeExpr::Literal(1));
        let cursor = fold_cursor(exec::ExecCountCursorPlan::NodeFullScan, prefix).unwrap();
        assert!(matches!(
            cursor,
            exec::ExecCountCursorPlan::Filter { input, .. }
                if matches!(input.as_ref(), exec::ExecCountCursorPlan::Window { .. })
        ));

        let write = logical::StreamPipelineOp::VariableWrite {
            op: logical::StreamVariableWriteOp::Store(name("saved")),
        };
        assert!(append_window(exec::ExecCountWindowPlan::identity(), &write).is_err());
        assert!(fold_cursor(exec::ExecCountCursorPlan::NodeFullScan, &[write]).is_err());
    }

    #[test]
    fn window_expression_matrix_accepts_params_and_rejects_interpretive_expressions() {
        let literal_bound = ir::StreamBoundPlan::Literal(3);
        assert_eq!(
            bound_expr(&literal_bound).unwrap(),
            exec::ExecUsizeExpr::Literal(3)
        );
        let param_bound = ir::StreamBoundPlan::Expr(
            ir::StreamBoundExprPlan::new(helix_ast::expr::Expr::param("limit")).unwrap(),
        );
        assert_eq!(
            bound_expr(&param_bound).unwrap(),
            exec::ExecUsizeExpr::Param(name("limit"))
        );

        for expression in [
            helix_ast::expr::Expr::Id,
            helix_ast::expr::Expr::Timestamp,
            helix_ast::expr::Expr::DateTimeNow,
            helix_ast::expr::Expr::Constant(PropertyValue::I64(3)),
        ] {
            let Ok(expression) = ir::StreamBoundExprPlan::new(expression) else {
                continue;
            };
            assert!(bound_expr(&ir::StreamBoundPlan::Expr(expression)).is_err());
        }
    }

    #[test]
    fn access_pipeline_with_only_windows_becomes_a_direct_count() {
        let pipeline = logical::AccessPipeline::new(
            node_path(ir::NodeAccessPlan::AllScan),
            ir::AtLeast::from_one_and_rest(
                logical::StreamPipelineOp::Window {
                    window: logical::AccessWindowRange::new(1, None).unwrap(),
                },
                vec![
                    logical::StreamPipelineOp::Range {
                        range: ir::StreamRangePlan::Literal(
                            ir::StreamLiteralRange::new(2, 8).unwrap(),
                        ),
                    },
                    logical::StreamPipelineOp::Order {
                        ordering: ordering(),
                    },
                ],
            ),
        )
        .unwrap();
        assert!(matches!(
            plans(apply(
                logical::RootStream::Access(logical::AccessStream::Pipeline(pipeline)),
                context::ParamBindings::default(),
                BTreeSet::new(),
            ))
            .as_slice(),
            [exec::ExecCountPlan::NodeFullScan { .. }]
        ));
    }

    #[test]
    fn root_shape_matrix_uses_exact_scalar_row_and_runtime_dependencies() {
        let access_root = || access(node_path(ir::NodeAccessPlan::AllScan));
        let scalar_roots = [
            logical::RootStream::Project(Box::new(logical::StreamProject::new(
                access_root(),
                ir::ProjectionPlan::Id,
            ))),
            logical::RootStream::Cardinality(Box::new(logical::StreamCardinality::new(
                access_root(),
            ))),
            logical::RootStream::Aggregate(Box::new(logical::StreamAggregate::new(
                access_root(),
                ir::AggregatePlan::Group(name("status")),
            ))),
        ];
        for root in scalar_roots {
            assert!(matches!(
                plans(apply(
                    root,
                    context::ParamBindings::default(),
                    BTreeSet::new(),
                ))
                .as_slice(),
                [exec::ExecCountPlan::InputScalars { .. }]
            ));
        }

        assert!(matches!(
            plans(apply(
                logical::RootStream::VariableSource(logical::VariableSource::new(name("saved"))),
                context::ParamBindings::default(),
                BTreeSet::new(),
            ))
            .as_slice(),
            [exec::ExecCountPlan::RuntimeInput { .. }]
        ));
        assert!(matches!(
            plans(apply(
                logical::RootStream::VariableWrite(Box::new(logical::StreamVariableWrite::new(
                    access_root(),
                    logical::StreamVariableWriteOp::Store(name("saved")),
                ))),
                context::ParamBindings::default(),
                BTreeSet::new(),
            ))
            .as_slice(),
            [exec::ExecCountPlan::InputRows { .. }]
        ));
    }

    #[test]
    fn root_pipelines_preserve_access_runtime_and_barrier_cursor_shapes() {
        let pipeline = |input| {
            logical::RootPipeline::new(
                input,
                ir::AtLeast::from_one(logical::StreamPipelineOp::Distinct),
            )
            .unwrap()
        };
        for (input, expected_dependency) in [
            (
                access(node_path(ir::NodeAccessPlan::AllScan)),
                exec::ExecCountDependency::Direct,
            ),
            (
                logical::RootStream::VariableSource(logical::VariableSource::new(name("saved"))),
                exec::ExecCountDependency::Direct,
            ),
            (
                logical::RootStream::Project(Box::new(logical::StreamProject::new(
                    access(node_path(ir::NodeAccessPlan::AllScan)),
                    ir::ProjectionPlan::Id,
                ))),
                exec::ExecCountDependency::Rows,
            ),
        ] {
            let [plan] = plans(apply(
                logical::RootStream::Pipeline(Box::new(pipeline(input))),
                context::ParamBindings::default(),
                BTreeSet::new(),
            ))
            .try_into()
            .unwrap();
            assert_eq!(plan.dependency().unwrap(), expected_dependency);
        }

        let write_pipeline = logical::RootPipeline::new(
            access(node_path(ir::NodeAccessPlan::AllScan)),
            ir::AtLeast::from_one(logical::StreamPipelineOp::VariableWrite {
                op: logical::StreamVariableWriteOp::Store(name("saved")),
            }),
        )
        .unwrap();
        assert!(matches!(
            plans(apply(
                logical::RootStream::Pipeline(Box::new(write_pipeline)),
                context::ParamBindings::default(),
                BTreeSet::new(),
            ))
            .as_slice(),
            [exec::ExecCountPlan::InputRows { .. }]
        ));
    }

    #[test]
    fn count_plan_cursor_and_cost_matrix_cover_every_physical_family() {
        let mut direct = Vec::new();
        for source in [
            ir::NodeAccessPlan::Empty,
            ir::NodeAccessPlan::PointIds { ids: element_ids() },
            ir::NodeAccessPlan::FromParam {
                param: name("nodes"),
            },
            ir::NodeAccessPlan::FromVar {
                variable: name("nodes"),
            },
            ir::NodeAccessPlan::AllScan,
            ir::NodeAccessPlan::LabelScan {
                label: name("User"),
            },
            node_range_plan(),
            node_vector_search(),
            node_text_search(),
        ] {
            direct.extend(plans(apply(
                access(node_path(source)),
                context::ParamBindings::default(),
                BTreeSet::new(),
            )));
        }
        for source in [
            ir::EdgeAccessPlan::Empty,
            ir::EdgeAccessPlan::PointIds { ids: element_ids() },
            ir::EdgeAccessPlan::FromParam {
                param: name("edges"),
            },
            ir::EdgeAccessPlan::FromVar {
                variable: name("edges"),
            },
            ir::EdgeAccessPlan::AllScan,
            ir::EdgeAccessPlan::LabelScan {
                label: name("LIKES"),
            },
            edge_range_plan(),
            edge_vector_search(),
            edge_text_search(),
        ] {
            direct.extend(plans(apply(
                access(edge_path(source)),
                context::ParamBindings::default(),
                BTreeSet::new(),
            )));
        }
        for (source, params, late) in [
            (
                access(node_path(node_equality(
                    "node_eq:User:status",
                    "status",
                    literal(PropertyValue::from("active")),
                    catalog::IndexUniqueness::NonUnique,
                ))),
                context::ParamBindings::default(),
                BTreeSet::new(),
            ),
            (
                access(node_path(node_equality(
                    "node_eq:User:email",
                    "email",
                    literal(PropertyValue::from("alice@example.test")),
                    catalog::IndexUniqueness::Unique,
                ))),
                context::ParamBindings::default(),
                BTreeSet::new(),
            ),
            (
                access(node_path(node_equality(
                    "node_eq:User:status",
                    "status",
                    literal(PropertyValue::Null),
                    catalog::IndexUniqueness::NonUnique,
                ))),
                context::ParamBindings::default(),
                BTreeSet::new(),
            ),
            (
                access(node_path(node_equality(
                    "node_eq:User:status",
                    "status",
                    ir::IndexValue::Param(name("status")),
                    catalog::IndexUniqueness::NonUnique,
                ))),
                context::ParamBindings::default(),
                BTreeSet::from([name("status")]),
            ),
            (
                access(edge_path(edge_equality(
                    "edge_eq:LIKES:status",
                    "status",
                    literal(PropertyValue::from("active")),
                ))),
                context::ParamBindings::default(),
                BTreeSet::new(),
            ),
            (
                access(edge_path(edge_equality(
                    "edge_eq:LIKES:status",
                    "status",
                    literal(PropertyValue::Null),
                ))),
                context::ParamBindings::default(),
                BTreeSet::new(),
            ),
            (
                access(edge_path(edge_equality(
                    "edge_eq:LIKES:status",
                    "status",
                    ir::IndexValue::Param(name("status")),
                ))),
                context::ParamBindings::default(),
                BTreeSet::from([name("status")]),
            ),
        ] {
            direct.extend(plans(apply(source, params, late)));
        }
        direct.extend([
            exec::ExecCountPlan::RuntimeInput {
                input: exec::ExecRuntimeInputPlan::Variable(name("rows")),
                window: exec::ExecCountWindowPlan::identity(),
            },
            exec::ExecCountPlan::InputRows {
                window: exec::ExecCountWindowPlan::identity(),
            },
        ]);

        let scalar_input = exec::ExecCountPlan::InputScalars {
            window: exec::ExecCountWindowPlan::identity(),
        };

        let stats = context::StatsSnapshot::default()
            .with_node_label_cardinality(name("User"), 17)
            .with_edge_label_cardinality(name("LIKES"), 19)
            .with_node_eq_cardinality(
                catalog::ScopedPropertyKey::try_new("User", "status").unwrap(),
                3,
            )
            .with_node_eq_cardinality(
                catalog::ScopedPropertyKey::try_new("User", "email").unwrap(),
                1,
            )
            .with_edge_eq_cardinality(
                catalog::ScopedPropertyKey::try_new("LIKES", "status").unwrap(),
                4,
            )
            .with_node_range_cardinality(
                catalog::ScopedPropertyDirectionKey::try_new(
                    "User",
                    "age",
                    RangeIndexDirection::Asc,
                )
                .unwrap(),
                11,
            )
            .with_edge_range_cardinality(
                catalog::ScopedPropertyDirectionKey::try_new(
                    "LIKES",
                    "age",
                    RangeIndexDirection::Desc,
                )
                .unwrap(),
                13,
            );
        let storage = cost::StorageCostProfile::default();
        assert_eq!(
            count_cost(&scalar_input, &stats, &storage),
            count_cost(
                &exec::ExecCountPlan::InputRows {
                    window: exec::ExecCountWindowPlan::identity(),
                },
                &stats,
                &storage,
            )
        );
        let mut cursors = Vec::new();
        let stream = exec::ExecCountPlan::Stream(exec::ExecCountStreamPlan {
            cursor: exec::ExecCountCursorPlan::NodeFullScan,
            window: exec::ExecCountWindowPlan::identity(),
        });
        assert_eq!(
            count_cost(&stream, &stats, &storage),
            cursor_cost(&exec::ExecCountCursorPlan::NodeFullScan, &stats, &storage,)
        );
        cursors.push(count_plan_cursor(stream).unwrap());
        for plan in direct {
            let cost = count_cost(&plan, &stats, &storage);
            assert_eq!(cost, count_cost(&plan, &stats, &storage));
            cursors.push(count_plan_cursor(plan).unwrap());
        }

        let node_batch = exec::ExecNodeBitmapExpr::BatchedUnionRead {
            index: catalog::NodeEqualityIndexMeta::new(name("node_eq:User:status"))
                .try_into()
                .unwrap(),
            key: catalog::ScopedPropertyKey::try_new("User", "status").unwrap(),
            values: ir::AtLeast::from_pair(exec_indexed("active"), exec_indexed("inactive")),
        };
        let edge_batch = exec::ExecEdgeBitmapExpr::BatchedUnionRead {
            index: exec::ExecEdgeNonUniqueEqualityIndex::new(catalog::EdgeEqualityIndexMeta::new(
                name("edge_eq:LIKES:status"),
            )),
            key: catalog::ScopedPropertyKey::try_new("LIKES", "status").unwrap(),
            values: ir::AtLeast::from_pair(exec_indexed("active"), exec_indexed("inactive")),
        };
        cursors.extend([
            exec::ExecCountCursorPlan::NodeBitmap(exec::ExecNodeBitmapExpr::Union {
                driver: Box::new(node_batch),
                rest: ir::AtLeast::from_one(exec_node_point("pending")),
            }),
            exec::ExecCountCursorPlan::EdgeBitmap(exec::ExecEdgeBitmapExpr::Intersect {
                driver: Box::new(edge_batch),
                rest: ir::AtLeast::from_one(exec_edge_point("pending")),
            }),
            exec::ExecCountCursorPlan::EdgeBitmap(exec::ExecEdgeBitmapExpr::Union {
                driver: Box::new(exec_edge_point("active")),
                rest: ir::AtLeast::from_one(exec_edge_point("paused")),
            }),
            exec::ExecCountCursorPlan::Union {
                driver: Box::new(exec::ExecCountCursorPlan::NodeFullScan),
                rest: ir::AtLeast::from_one(exec::ExecCountCursorPlan::EdgeFullScan),
            },
            exec::ExecCountCursorPlan::Intersect {
                driver: Box::new(exec::ExecCountCursorPlan::NodeFullScan),
                rest: ir::AtLeast::from_one(exec::ExecCountCursorPlan::EdgeFullScan),
            },
            exec::ExecCountCursorPlan::Filter {
                input: Box::new(exec::ExecCountCursorPlan::NodeFullScan),
                predicate: predicate(),
            },
            exec::ExecCountCursorPlan::Window {
                input: Box::new(exec::ExecCountCursorPlan::NodeFullScan),
                window: exec::ExecCountWindowPlan::identity(),
            },
            exec::ExecCountCursorPlan::Order {
                input: Box::new(exec::ExecCountCursorPlan::NodeFullScan),
                plan: ir::OrderPlan::ExplicitSort(ordering()),
            },
            exec::ExecCountCursorPlan::Expand {
                input: Box::new(exec::ExecCountCursorPlan::NodeFullScan),
                plan: expand(),
            },
            exec::ExecCountCursorPlan::VectorSearch {
                input: Box::new(exec::ExecCountCursorPlan::NodeFullScan),
                plan: restricted_vector(),
            },
            exec::ExecCountCursorPlan::TextSearch {
                input: Box::new(exec::ExecCountCursorPlan::NodeFullScan),
                plan: restricted_text(),
            },
            exec::ExecCountCursorPlan::Variable {
                input: Box::new(exec::ExecCountCursorPlan::NodeFullScan),
                op: logical::PureStreamVariableOp::Select(name("saved")),
            },
            exec::ExecCountCursorPlan::Distinct {
                input: Box::new(exec::ExecCountCursorPlan::NodeFullScan),
                plan: exec::ExecCountDistinctPlan::HashRows,
            },
            exec::ExecCountCursorPlan::NodeAuthoritativeScan(
                exec::ExecNodeAuthoritativeScanPredicate::Predicate(predicate()),
            ),
            exec::ExecCountCursorPlan::EdgeAuthoritativeScan(
                exec::ExecEdgeAuthoritativeScanPredicate::Predicate(predicate()),
            ),
        ]);
        for cursor in cursors {
            let cost = cursor_cost(&cursor, &stats, &storage);
            assert_eq!(cost, cursor_cost(&cursor, &stats, &storage));
        }

        assert!(count_plan_cursor(exec::ExecCountPlan::Constant(1)).is_err());
        assert!(count_plan_cursor(exec::ExecCountPlan::InputScalars {
            window: exec::ExecCountWindowPlan::identity(),
        })
        .is_err());
    }
}
