use crate::helixc::parser::{
    location::HasLoc, types::{
        Aggregate, BooleanOp, BooleanOpType, Closure, Exclude, Expression, ExpressionType, FieldAddition, FieldValue, FieldValueType, GraphStep, GraphStepType, GroupBy, IdType, Match, MatchStatement, MatchType, MatchValueType, MatchVariable, MatchVariableType, Object, Optional, OrderBy, OrderByType, SchemaMatchType, ShortestPath, ShortestPathBFS, ShortestPathDijkstras, Step, StepType, Update
    }, utils::{PairTools, PairsTools}, HelixParser, ParserError, Rule
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
        let order_by_type = match inner.try_next_inner().try_next()?.as_rule() {
            Rule::asc => OrderByType::Asc,
            Rule::desc => OrderByType::Desc,
            _ => unreachable!(),
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
            Rule::match_step => Ok(Step {
                loc: step_pair.loc(),
                step: StepType::Match(self.parse_match_step(step_pair)?),
            }),
            _ => Err(ParserError::from(format!(
                "Unexpected step type: {:?}",
                step_pair.as_rule()
            ))),
        }
    }

    pub(super) fn parse_match_step(&self, pair: Pair<Rule>) -> Result<Match, ParserError> {
        // get inner
        let step_loc = pair.loc();
        let mut inner = pair.into_inner();
        // match variable via peek
        let match_variable = match inner.peek() {
            Some(variable) => {
                let match_variable = match variable.into_inner().next() {
                    Some(inner) => match inner.as_rule() {
                        Rule::identifier => Some(MatchVariable {
                            loc: inner.loc(),
                            variable: MatchVariableType::Identifier(inner.as_str().to_string()),
                        }),
                        Rule::traversal | Rule::id_traversal => Some(MatchVariable {
                            loc: inner.loc(),
                            variable: MatchVariableType::Traversal(Box::new(
                                self.parse_traversal(inner)?,
                            )),
                        }),
                        Rule::anonymous_traversal => Some(MatchVariable {
                            loc: inner.loc(),
                            variable: MatchVariableType::Traversal(Box::new(
                                self.parse_anon_traversal(inner)?,
                            )),
                        }),
                        Rule::anon_variable => Some(MatchVariable {
                            loc: inner.loc(),
                            variable: MatchVariableType::Anonymous,
                        }),
                        _ => None,
                    },
                    None => panic!("poo"),
                };
                inner.next();
                match_variable
            }
            None => None,
        };
        // then get match statements via .next.inner
        let mut statements = Vec::new();
        for stmt in inner.next().ok_or(ParserError::Error)?.into_inner() {
            // for each statement do .inner
            let stmt_loc = stmt.loc();
            let mut inner = stmt.into_inner();
            // .next for match_type
            let match_type = inner
                .next()
                .ok_or(ParserError::Error)?
                .into_inner()
                .next()
                .unwrap();
            // .inner then match for exact
            let mt = match match_type.as_rule() {
                Rule::match_type_enum => {
                    let mut match_type_inner = match_type.into_inner();
                    let prefix = match_type_inner.next().unwrap();
                    let schema_type = match prefix.into_inner().next().unwrap().as_rule() {
                        Rule::node_type_prefix => SchemaMatchType::Node {
                            type_arg: match_type_inner.next().unwrap().as_str().to_string(),
                            identifier: match_type_inner.next().map(|i| i.as_str().to_string()),
                        },
                        Rule::edge_type_prefix => SchemaMatchType::Edge {
                            type_arg: match_type_inner.next().unwrap().as_str().to_string(),
                            identifier: match_type_inner.next().map(|i| i.as_str().to_string()),
                        },
                        Rule::vec_type_prefix => SchemaMatchType::Vector {
                            type_arg: match_type_inner.next().unwrap().as_str().to_string(),
                            identifier: match_type_inner.next().map(|i| i.as_str().to_string()),
                        },
                        _ => unreachable!(),
                    };
                    MatchType::SchemaType(schema_type)
                }
                Rule::boolean => MatchType::Boolean(match_type.as_str() == "true"),
                Rule::optional_type => {
                    let optional_inner = match_type.into_inner().next().unwrap();
                    MatchType::Optional(match optional_inner.as_rule() {
                        Rule::identifier => Optional::Some(optional_inner.as_str().to_string()),
                        Rule::none => Optional::None,
                        _ => unreachable!(),
                    })
                }
                Rule::anon_variable => MatchType::Anonymous,
                _ => unreachable!(),
            };
            // .next for match_value
            let match_value = inner
                .next()
                .ok_or(ParserError::Error)?
                .into_inner()
                .next()
                .unwrap();

            // .inner then match for exact
            let mv = match match_value.as_rule() {
                Rule::query_body => MatchValueType::Statements(self.parse_query_body(match_value)?),
                Rule::none => MatchValueType::None,
                Rule::anon_variable => MatchValueType::Anonymous,
                Rule::identifier => MatchValueType::Expression(Expression {
                    loc: match_value.loc(),
                    expr: ExpressionType::Identifier(match_value.as_str().to_string()),
                }),
                _ => {
                    println!("match_value: {:?}", match_value.as_rule());
                    MatchValueType::Expression(self.parse_expression(match_value)?)
                }
            };

            statements.push(MatchStatement {
                loc: stmt_loc,
                match_type: mt,
                match_value: mv,
            });
        }

        Ok(Match {
            loc: step_loc,
            variable: match_variable,
            statements,
        })
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
                                _ => unreachable!(),
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
                let (type_arg, inner_traversal, from, to) =
                    match pair.clone().into_inner().try_fold(
                        (None, None, None, None),
                        |(type_arg, inner_traversal, from, to), p| match p.as_rule() {
                            Rule::type_args => Ok((
                                Some(p.try_inner_next()?.as_str().to_string()),
                                inner_traversal,
                                from,
                                to,
                            )),
                            Rule::anonymous_traversal => Ok((
                                type_arg,
                                Some(self.parse_anon_traversal(p).unwrap()),
                                from,
                                to,
                            )),
                            Rule::to_from => match p.into_inner().next() {
                                Some(p) => match p.as_rule() {
                                    Rule::to => Ok((
                                        type_arg,
                                        inner_traversal,
                                        from,
                                        Some(p.into_inner().next().unwrap().as_str().to_string()),
                                    )),
                                    Rule::from => Ok((
                                        type_arg,
                                        inner_traversal,
                                        Some(p.into_inner().next().unwrap().as_str().to_string()),
                                        to,
                                    )),
                                    _ => unreachable!(),
                                },
                                None => Ok((type_arg, inner_traversal, from, to)),
                            },
                            _ => Ok((type_arg, inner_traversal, from, to)),
                        },
                    ) {
                        Ok((type_arg, inner_traversal, from, to)) => {
                            (type_arg, inner_traversal, from, to)
                        }
                        Err(e) => return Err(e),
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
                                _ => unreachable!(),
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
}
