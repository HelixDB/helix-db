use crate::helixc::parser::{
    HelixParser, ParserError, Rule,
    location::HasLoc,
    types::{
        Aggregate, BooleanOp, BooleanOpType, Closure, Exclude, Expression, ExpressionType,
        FieldAddition, FieldValue, FieldValueType, GraphStep, GraphStepType, GroupBy, IdType,
        MMRDistance, Object, OrderBy, OrderByType, RerankMMR, RerankRRF, ShortestPath,
        ShortestPathAStar, ShortestPathBFS, ShortestPathDijkstras, Step, StepType, Update,
    },
    utils::{PairTools, PairsTools},
};
use pest::iterators::Pair;

impl HelixParser {
    /// Parses an order by step
    ///
    /// #### Example
    /// ```rs
    /// ::ORDER<Asc>(_::{age})
    /// ```
    pub(super) fn parse_order_by(&self, pair: Pair<Rule>) -> Result<OrderBy, ParserError> {
        let mut inner = pair.clone().into_inner();
        let order_by_rule = inner.try_next_inner().try_next()?;
        let order_by_type = match order_by_rule.as_rule() {
            Rule::asc => OrderByType::Asc,
            Rule::desc => OrderByType::Desc,
            other => return Err(ParserError::from(format!(
                "Unexpected rule in parse_order_by: {:?}",
                other
            ))),
        };
        let expression = self.parse_expression(inner.try_next()?)?;
        Ok(OrderBy {
            loc: pair.loc(),
            order_by_type,
            expression: Box::new(expression),
        })
    }

    /// Parses a range step
    ///
    /// #### Example
    /// ```rs
    /// ::RANGE(1, 10)
    /// ```
    #[track_caller]
    pub(super) fn parse_range(
        &self,
        pair: Pair<Rule>,
    ) -> Result<(Expression, Expression), ParserError> {
        let mut inner = pair.into_inner();
        let start = self.parse_expression(inner.try_next()?)?;
        let end = self.parse_expression(inner.try_next()?)?;

        Ok((start, end))
    }

    /// Parses a boolean operation
    ///
    /// #### Example
    /// ```rs
    /// ::GT(1)
    /// ```
    pub(super) fn parse_bool_operation(&self, pair: Pair<Rule>) -> Result<BooleanOp, ParserError> {
        let inner = pair.clone().try_inner_next()?;
        let expr = match inner.as_rule() {
            Rule::GT => BooleanOp {
                loc: pair.loc(),
                op: BooleanOpType::GreaterThan(Box::new(
                    self.parse_expression(inner.try_inner_next()?)?,
                )),
            },
            Rule::GTE => BooleanOp {
                loc: pair.loc(),
                op: BooleanOpType::GreaterThanOrEqual(Box::new(
                    self.parse_expression(inner.try_inner_next()?)?,
                )),
            },
            Rule::LT => BooleanOp {
                loc: pair.loc(),
                op: BooleanOpType::LessThan(Box::new(
                    self.parse_expression(inner.try_inner_next()?)?,
                )),
            },
            Rule::LTE => BooleanOp {
                loc: pair.loc(),
                op: BooleanOpType::LessThanOrEqual(Box::new(
                    self.parse_expression(inner.try_inner_next()?)?,
                )),
            },
            Rule::EQ => BooleanOp {
                loc: pair.loc(),
                op: BooleanOpType::Equal(Box::new(self.parse_expression(inner.try_inner_next()?)?)),
            },
            Rule::NEQ => BooleanOp {
                loc: pair.loc(),
                op: BooleanOpType::NotEqual(Box::new(
                    self.parse_expression(inner.try_inner_next()?)?,
                )),
            },
            Rule::CONTAINS => BooleanOp {
                loc: pair.loc(),
                op: BooleanOpType::Contains(Box::new(
                    self.parse_expression(inner.try_inner_next()?)?,
                )),
            },
            Rule::IS_IN => BooleanOp {
                loc: pair.loc(),
                op: BooleanOpType::IsIn(Box::new(self.parse_expression(inner)?)),
            },
            _ => return Err(ParserError::from("Invalid boolean operation")),
        };
        Ok(expr)
    }

    /// Parses an update step
    ///
    /// #### Example
    /// ```rs
    /// ::UPDATE({age: 1})
    /// ```
    pub(super) fn parse_update(&self, pair: Pair<Rule>) -> Result<Update, ParserError> {
        let fields = self.parse_object_fields(pair.clone())?;
        Ok(Update {
            fields,
            loc: pair.loc(),
        })
    }

    /// Parses an object step
    ///
    /// #### Example
    /// ```rs
    /// ::{username: name}
    /// ```
    pub(super) fn parse_object_step(&self, pair: Pair<Rule>) -> Result<Object, ParserError> {
        let mut fields = Vec::new();
        let mut should_spread = false;
        for p in pair.clone().into_inner() {
            if p.as_rule() == Rule::spread_object {
                should_spread = true;
                continue;
            }
            let mut pairs = p.clone().into_inner();
            let prop_key = pairs.try_next()?.as_str().to_string();
            let field_addition = match pairs.next() {
                Some(p) => match p.as_rule() {
                    Rule::evaluates_to_anything => FieldValue {
                        loc: p.loc(),
                        value: FieldValueType::Expression(self.parse_expression(p)?),
                    },
                    Rule::anonymous_traversal => FieldValue {
                        loc: p.loc(),
                        value: FieldValueType::Traversal(Box::new(self.parse_anon_traversal(p)?)),
                    },
                    Rule::id_traversal => FieldValue {
                        loc: p.loc(),
                        value: FieldValueType::Traversal(Box::new(self.parse_traversal(p)?)),
                    },
                    Rule::mapping_field => FieldValue {
                        loc: p.loc(),
                        value: FieldValueType::Fields(self.parse_object_fields(p)?),
                    },
                    Rule::object_step => FieldValue {
                        loc: p.clone().loc(),
                        value: FieldValueType::Fields(self.parse_object_step(p.clone())?.fields),
                    },
                    _ => self.parse_new_field_value(p)?,
                },
                None if !prop_key.is_empty() => FieldValue {
                    loc: p.loc(),
                    value: FieldValueType::Identifier(prop_key.clone()),
                },
                None => FieldValue {
                    loc: p.loc(),
                    value: FieldValueType::Empty,
                },
            };
            fields.push(FieldAddition {
                loc: p.loc(),
                key: prop_key,
                value: field_addition,
            });
        }
        Ok(Object {
            loc: pair.loc(),
            fields,
            should_spread,
        })
    }

    /// Parses a closure step
    ///
    /// #### Example
    /// ```rs
    /// ::|user|{user_age: user::{age}}
    /// ```
    pub(super) fn parse_closure(&self, pair: Pair<Rule>) -> Result<Closure, ParserError> {
        let mut pairs = pair.clone().into_inner();
        let identifier = pairs.try_next()?.as_str().to_string();
        let object = self.parse_object_step(pairs.try_next()?)?;
        Ok(Closure {
            loc: pair.loc(),
            identifier,
            object,
        })
    }

    /// Parses an exclude step
    ///
    /// #### Example
    /// ```rs
    /// ::!{age, name}
    /// ```
    pub(super) fn parse_exclude(&self, pair: Pair<Rule>) -> Result<Exclude, ParserError> {
        let mut fields = Vec::new();
        for p in pair.clone().into_inner() {
            fields.push((p.loc(), p.as_str().to_string()));
        }
        Ok(Exclude {
            loc: pair.loc(),
            fields,
        })
    }

    pub(super) fn parse_aggregate(&self, pair: Pair<Rule>) -> Result<Aggregate, ParserError> {
        let loc = pair.loc();
        let identifiers = pair
            .into_inner()
            .map(|i| i.as_str().to_string())
            .collect::<Vec<_>>();

        Ok(Aggregate {
            loc,
            properties: identifiers,
        })
    }

    pub(super) fn parse_group_by(&self, pair: Pair<Rule>) -> Result<GroupBy, ParserError> {
        let loc = pair.loc();
        let identifiers = pair
            .into_inner()
            .map(|i| i.as_str().to_string())
            .collect::<Vec<_>>();

        Ok(GroupBy {
            loc,
            properties: identifiers,
        })
    }

    pub(super) fn parse_step(&self, pair: Pair<Rule>) -> Result<Step, ParserError> {
        let step_pair = pair.clone().try_inner_next()?;
        match step_pair.as_rule() {
            Rule::graph_step => Ok(Step {
                loc: step_pair.loc(),
                step: StepType::Node(self.parse_graph_step(step_pair)?),
            }),
            Rule::object_step => Ok(Step {
                loc: step_pair.loc(),
                step: StepType::Object(self.parse_object_step(step_pair)?),
            }),
            Rule::closure_step => Ok(Step {
                loc: step_pair.loc(),
                step: StepType::Closure(self.parse_closure(step_pair)?),
            }),
            Rule::where_step => Ok(Step {
                loc: step_pair.loc(),
                step: StepType::Where(Box::new(self.parse_expression(step_pair)?)),
            }),
            Rule::range_step => Ok(Step {
                loc: step_pair.loc(),
                step: StepType::Range(self.parse_range(step_pair)?),
            }),

            Rule::bool_operations => Ok(Step {
                loc: step_pair.loc(),
                step: StepType::BooleanOperation(self.parse_bool_operation(step_pair)?),
            }),
            Rule::count => Ok(Step {
                loc: step_pair.loc(),
                step: StepType::Count,
            }),
            Rule::ID => Ok(Step {
                loc: step_pair.loc(),
                step: StepType::Object(Object {
                    fields: vec![FieldAddition {
                        key: "id".to_string(),
                        value: FieldValue {
                            loc: step_pair.loc(),
                            value: FieldValueType::Identifier("id".to_string()),
                        },
                        loc: step_pair.loc(),
                    }],
                    should_spread: false,
                    loc: step_pair.loc(),
                }),
            }),
            Rule::update => Ok(Step {
                loc: step_pair.loc(),
                step: StepType::Update(self.parse_update(step_pair)?),
            }),
            Rule::exclude_field => Ok(Step {
                loc: step_pair.loc(),
                step: StepType::Exclude(self.parse_exclude(step_pair)?),
            }),
            Rule::AddE => Ok(Step {
                loc: step_pair.loc(),
                step: StepType::AddEdge(self.parse_add_edge(step_pair, true)?),
            }),
            Rule::order_by => Ok(Step {
                loc: step_pair.loc(),
                step: StepType::OrderBy(self.parse_order_by(step_pair)?),
            }),
            Rule::aggregate => Ok(Step {
                loc: step_pair.loc(),
                step: StepType::Aggregate(self.parse_aggregate(step_pair)?),
            }),
            Rule::group_by => Ok(Step {
                loc: step_pair.loc(),
                step: StepType::GroupBy(self.parse_group_by(step_pair)?),
            }),
            Rule::first => Ok(Step {
                loc: step_pair.loc(),
                step: StepType::First,
            }),
            Rule::rerank_rrf => Ok(Step {
                loc: step_pair.loc(),
                step: StepType::RerankRRF(self.parse_rerank_rrf(step_pair)?),
            }),
            Rule::rerank_mmr => Ok(Step {
                loc: step_pair.loc(),
                step: StepType::RerankMMR(self.parse_rerank_mmr(step_pair)?),
            }),
            _ => Err(ParserError::from(format!(
                "Unexpected step type: {:?}",
                step_pair.as_rule()
            ))),
        }
    }

    pub(super) fn parse_graph_step(&self, pair: Pair<Rule>) -> Result<GraphStep, ParserError> {
        let types = |pair: &Pair<Rule>| -> Result<String, ParserError> {
            pair.clone()
                .into_inner()
                .next()
                .map(|p| p.as_str().to_string())
                .ok_or_else(|| ParserError::from(format!("Expected type for {:?}", pair.as_rule())))
        };
        let pair = pair.clone().try_inner_next()?;
        let step = match pair.as_rule() {
            Rule::out_e => {
                let types = types(&pair)?;
                GraphStep {
                    loc: pair.loc(),
                    step: GraphStepType::OutE(types),
                }
            }
            Rule::in_e => {
                let types = types(&pair)?;
                GraphStep {
                    loc: pair.loc(),
                    step: GraphStepType::InE(types),
                }
            }
            Rule::from_n => GraphStep {
                loc: pair.loc(),
                step: GraphStepType::FromN,
            },
            Rule::to_n => GraphStep {
                loc: pair.loc(),
                step: GraphStepType::ToN,
            },
            Rule::from_v => GraphStep {
                loc: pair.loc(),
                step: GraphStepType::FromV,
            },
            Rule::to_v => GraphStep {
                loc: pair.loc(),
                step: GraphStepType::ToV,
            },
            Rule::out => {
                let types = types(&pair)?;
                GraphStep {
                    loc: pair.loc(),
                    step: GraphStepType::Out(types),
                }
            }
            Rule::in_nodes => {
                let types = types(&pair)?;
                GraphStep {
                    loc: pair.loc(),
                    step: GraphStepType::In(types),
                }
            }
            Rule::shortest_path => {
                let (type_arg, from, to) = match pair.clone().into_inner().try_fold(
                    (None, None, None),
                    |(type_arg, from, to), p| match p.as_rule() {
                        Rule::type_args => {
                            Ok((Some(p.try_inner_next()?.as_str().to_string()), from, to))
                        }
                        Rule::to_from => match p.into_inner().next() {
                            Some(p) => match p.as_rule() {
                                Rule::to => Ok((
                                    type_arg,
                                    from,
                                    Some(p.try_inner_next()?.as_str().to_string()),
                                )),
                                Rule::from => Ok((
                                    type_arg,
                                    Some(p.try_inner_next()?.as_str().to_string()),
                                    to,
                                )),
                                other => Err(ParserError::from(format!(
                                    "Unexpected rule in shortest_path to_from: {:?}",
                                    other
                                ))),
                            },
                            None => Ok((type_arg, from, to)),
                        },
                        _ => Ok((type_arg, from, to)),
                    },
                ) {
                    Ok((type_arg, from, to)) => (type_arg, from, to),
                    Err(e) => return Err(e),
                };
                GraphStep {
                    loc: pair.loc(),
                    step: GraphStepType::ShortestPath(ShortestPath {
                        loc: pair.loc(),
                        from: from.map(|id| IdType::Identifier {
                            value: id,
                            loc: pair.loc(),
                        }),
                        to: to.map(|id| IdType::Identifier {
                            value: id,
                            loc: pair.loc(),
                        }),
                        type_arg,
                    }),
                }
            }
            Rule::shortest_path_dijkstras => {
                let (type_arg, weight_expression, from, to) =
                    match pair.clone().into_inner().try_fold(
                        (None, None, None, None),
                        |(type_arg, weight_expr, from, to), p| match p.as_rule() {
                            Rule::type_args => Ok((
                                Some(p.try_inner_next()?.as_str().to_string()),
                                weight_expr,
                                from,
                                to,
                            )),
                            Rule::math_expression => {
                                // Parse the math_expression into an Expression
                                let expr = self.parse_math_expression(p)?;
                                Ok((type_arg, Some(expr), from, to))
                            }
                            Rule::to_from => match p.into_inner().next() {
                                Some(p) => match p.as_rule() {
                                    Rule::to => Ok((
                                        type_arg,
                                        weight_expr,
                                        from,
                                        Some(p.into_inner().next().unwrap().as_str().to_string()),
                                    )),
                                    Rule::from => Ok((
                                        type_arg,
                                        weight_expr,
                                        Some(p.into_inner().next().unwrap().as_str().to_string()),
                                        to,
                                    )),
                                    other => Err(ParserError::from(format!(
                                        "Unexpected rule in shortest_path_dijkstras to_from: {:?}",
                                        other
                                    ))),
                                },
                                None => Ok((type_arg, weight_expr, from, to)),
                            },
                            _ => Ok((type_arg, weight_expr, from, to)),
                        },
                    ) {
                        Ok((type_arg, weight_expr, from, to)) => (type_arg, weight_expr, from, to),
                        Err(e) => return Err(e),
                    };

                // Determine weight expression type
                let (inner_traversal, weight_expr_typed) = if let Some(expr) = weight_expression {
                    // Check if it's a simple property access or a complex expression
                    let weight_type = match &expr.expr {
                        ExpressionType::Traversal(_trav) => {
                            // For now, keep the traversal and create a Property weight expression
                            // TODO: Extract property name from traversal for simple cases
                            Some(crate::helixc::parser::types::WeightExpression::Expression(
                                Box::new(expr.clone()),
                            ))
                        }
                        ExpressionType::MathFunctionCall(_) => {
                            Some(crate::helixc::parser::types::WeightExpression::Expression(
                                Box::new(expr.clone()),
                            ))
                        }
                        _ => Some(crate::helixc::parser::types::WeightExpression::Expression(
                            Box::new(expr.clone()),
                        )),
                    };
                    (None, weight_type)
                } else {
                    (
                        None,
                        Some(crate::helixc::parser::types::WeightExpression::Default),
                    )
                };

                GraphStep {
                    loc: pair.loc(),
                    step: GraphStepType::ShortestPathDijkstras(ShortestPathDijkstras {
                        loc: pair.loc(),
                        from: from.map(|id| IdType::Identifier {
                            value: id,
                            loc: pair.loc(),
                        }),
                        to: to.map(|id| IdType::Identifier {
                            value: id,
                            loc: pair.loc(),
                        }),
                        type_arg,
                        inner_traversal,
                        weight_expr: weight_expr_typed,
                    }),
                }
            }
            Rule::shortest_path_bfs => {
                let (type_arg, from, to) = match pair.clone().into_inner().try_fold(
                    (None, None, None),
                    |(type_arg, from, to), p| match p.as_rule() {
                        Rule::type_args => Ok((
                            Some(p.into_inner().next().unwrap().as_str().to_string()),
                            from,
                            to,
                        )),
                        Rule::to_from => match p.into_inner().next() {
                            Some(p) => match p.as_rule() {
                                Rule::to => Ok((
                                    type_arg,
                                    from,
                                    Some(p.into_inner().next().unwrap().as_str().to_string()),
                                )),
                                Rule::from => Ok((
                                    type_arg,
                                    Some(p.into_inner().next().unwrap().as_str().to_string()),
                                    to,
                                )),
                                other => Err(ParserError::from(format!(
                                    "Unexpected rule in shortest_path_bfs to_from: {:?}",
                                    other
                                ))),
                            },
                            None => Ok((type_arg, from, to)),
                        },
                        _ => Ok((type_arg, from, to)),
                    },
                ) {
                    Ok((type_arg, from, to)) => (type_arg, from, to),
                    Err(e) => return Err(e),
                };
                GraphStep {
                    loc: pair.loc(),
                    step: GraphStepType::ShortestPathBFS(ShortestPathBFS {
                        loc: pair.loc(),
                        from: from.map(|id| IdType::Identifier {
                            value: id,
                            loc: pair.loc(),
                        }),
                        to: to.map(|id| IdType::Identifier {
                            value: id,
                            loc: pair.loc(),
                        }),
                        type_arg,
                    }),
                }
            }
            Rule::shortest_path_astar => {
                // Parse: ShortestPathAStar<Type>(weight_expr, "heuristic_property")
                let mut type_arg: Option<String> = None;
                let mut weight_expression: Option<Expression> = None;
                let mut heuristic_property: Option<String> = None;
                let mut from: Option<String> = None;
                let mut to: Option<String> = None;

                for inner_pair in pair.clone().into_inner() {
                    match inner_pair.as_rule() {
                        Rule::type_args => {
                            type_arg =
                                Some(inner_pair.into_inner().next().unwrap().as_str().to_string());
                        }
                        Rule::math_expression => {
                            weight_expression = Some(self.parse_expression(inner_pair)?);
                        }
                        Rule::string_literal => {
                            // Extract string content (remove quotes)
                            let literal = inner_pair.as_str();
                            heuristic_property = Some(literal[1..literal.len() - 1].to_string());
                        }
                        Rule::to_from => {
                            if let Some(p) = inner_pair.into_inner().next() {
                                match p.as_rule() {
                                    Rule::to => {
                                        to = Some(
                                            p.into_inner().next().unwrap().as_str().to_string(),
                                        );
                                    }
                                    Rule::from => {
                                        from = Some(
                                            p.into_inner().next().unwrap().as_str().to_string(),
                                        );
                                    }
                                    _ => {}
                                }
                            }
                        }
                        _ => {}
                    }
                }

                // Determine weight expression type
                let (inner_traversal, weight_expr_typed) = if let Some(expr) = weight_expression {
                    let weight_type = match &expr.expr {
                        ExpressionType::Traversal(_trav) => {
                            Some(crate::helixc::parser::types::WeightExpression::Expression(
                                Box::new(expr.clone()),
                            ))
                        }
                        ExpressionType::MathFunctionCall(_) => {
                            Some(crate::helixc::parser::types::WeightExpression::Expression(
                                Box::new(expr.clone()),
                            ))
                        }
                        _ => Some(crate::helixc::parser::types::WeightExpression::Expression(
                            Box::new(expr.clone()),
                        )),
                    };
                    (None, weight_type)
                } else {
                    (
                        None,
                        Some(crate::helixc::parser::types::WeightExpression::Default),
                    )
                };

                GraphStep {
                    loc: pair.loc(),
                    step: GraphStepType::ShortestPathAStar(ShortestPathAStar {
                        loc: pair.loc(),
                        from: from.map(|id| IdType::Identifier {
                            value: id,
                            loc: pair.loc(),
                        }),
                        to: to.map(|id| IdType::Identifier {
                            value: id,
                            loc: pair.loc(),
                        }),
                        type_arg,
                        inner_traversal,
                        weight_expr: weight_expr_typed,
                        heuristic_property: heuristic_property.unwrap_or_else(|| "h".to_string()),
                    }),
                }
            }

            Rule::search_vector => GraphStep {
                loc: pair.loc(),
                step: GraphStepType::SearchVector(self.parse_search_vector(pair)?),
            },
            _ => {
                return Err(ParserError::from(format!(
                    "Unexpected graph step type: {:?}",
                    pair.as_rule()
                )));
            }
        };
        Ok(step)
    }

    /// Parses a RerankRRF step
    ///
    /// #### Example
    /// ```rs
    /// ::RerankRRF(k: 60)
    /// ::RerankRRF()
    /// ```
    pub(super) fn parse_rerank_rrf(&self, pair: Pair<Rule>) -> Result<RerankRRF, ParserError> {
        let loc = pair.loc();
        let mut k = None;

        // Parse optional k parameter
        for inner in pair.into_inner() {
            // The grammar is: "k" ~ ":" ~ evaluates_to_number
            // We need to parse the evaluates_to_number part
            k = Some(self.parse_expression(inner)?);
        }

        Ok(RerankRRF { loc, k })
    }

    /// Parses a RerankMMR step
    ///
    /// #### Example
    /// ```rs
    /// ::RerankMMR(lambda: 0.7)
    /// ::RerankMMR(lambda: 0.5, distance: "euclidean")
    /// ```
    pub(super) fn parse_rerank_mmr(&self, pair: Pair<Rule>) -> Result<RerankMMR, ParserError> {
        let loc = pair.loc();
        let mut lambda = None;
        let mut distance = None;

        // Parse parameters
        let mut inner = pair.into_inner();

        // First parameter is always lambda (required)
        if let Some(lambda_expr) = inner.next() {
            lambda = Some(self.parse_expression(lambda_expr)?);
        }

        // Second parameter is optional distance
        if let Some(distance_pair) = inner.next() {
            let dist_str = match distance_pair.as_rule() {
                Rule::string_literal => {
                    // Remove quotes from string literal
                    let s = distance_pair.as_str();
                    s.trim_matches('"').to_string()
                }
                Rule::identifier => distance_pair.as_str().to_string(),
                _ => distance_pair.as_str().to_string(),
            };

            distance = Some(match dist_str.as_str() {
                "cosine" => MMRDistance::Cosine,
                "euclidean" => MMRDistance::Euclidean,
                "dotproduct" => MMRDistance::DotProduct,
                _ => MMRDistance::Identifier(dist_str),
            });
        }

        let lambda =
            lambda.ok_or_else(|| ParserError::from("lambda parameter required for RerankMMR"))?;

        Ok(RerankMMR {
            loc,
            lambda,
            distance,
        })
    }
}
