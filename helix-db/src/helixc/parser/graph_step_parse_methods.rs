use crate::helixc::parser::{
    HelixParser, ParserError, Rule,
    location::HasLoc,
    types::{
        Aggregate, BooleanOp, BooleanOpType, Closure, Exclude, Expression, FieldAddition,
        FieldValue, FieldValueType, GraphStep, GraphStepType, GroupBy, IdType, Match,
        MatchStatement, MatchType, MatchValueType, MatchVariable, MatchVariableType, Object,
        Optional, OrderBy, OrderByType, ShortestPath, Step, StepType, Update,
    },
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
        let order_by_type = match inner.next().unwrap().into_inner().next().unwrap().as_rule() {
            Rule::asc => OrderByType::Asc,
            Rule::desc => OrderByType::Desc,
            _ => unreachable!(),
        };
        let expression = self.parse_expression(inner.next().unwrap())?;
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
    pub(super) fn parse_range(
        &self,
        pair: Pair<Rule>,
    ) -> Result<(Expression, Expression), ParserError> {
        let mut inner = pair.into_inner().next().unwrap().into_inner();
        let start = self.parse_expression(inner.next().unwrap())?;
        let end = self.parse_expression(inner.next().unwrap())?;

        Ok((start, end))
    }

    /// Parses a boolean operation
    ///
    /// #### Example
    /// ```rs
    /// ::GT(1)
    /// ```
    pub(super) fn parse_bool_operation(&self, pair: Pair<Rule>) -> Result<BooleanOp, ParserError> {
        let inner = pair.clone().into_inner().next().unwrap();
        let expr = match inner.as_rule() {
            Rule::GT => BooleanOp {
                loc: pair.loc(),
                op: BooleanOpType::GreaterThan(Box::new(
                    self.parse_expression(inner.into_inner().next().unwrap())?,
                )),
            },
            Rule::GTE => BooleanOp {
                loc: pair.loc(),
                op: BooleanOpType::GreaterThanOrEqual(Box::new(
                    self.parse_expression(inner.into_inner().next().unwrap())?,
                )),
            },
            Rule::LT => BooleanOp {
                loc: pair.loc(),
                op: BooleanOpType::LessThan(Box::new(
                    self.parse_expression(inner.into_inner().next().unwrap())?,
                )),
            },
            Rule::LTE => BooleanOp {
                loc: pair.loc(),
                op: BooleanOpType::LessThanOrEqual(Box::new(
                    self.parse_expression(inner.into_inner().next().unwrap())?,
                )),
            },
            Rule::EQ => BooleanOp {
                loc: pair.loc(),
                op: BooleanOpType::Equal(Box::new(
                    self.parse_expression(inner.into_inner().next().unwrap())?,
                )),
            },
            Rule::NEQ => BooleanOp {
                loc: pair.loc(),
                op: BooleanOpType::NotEqual(Box::new(
                    self.parse_expression(inner.into_inner().next().unwrap())?,
                )),
            },
            Rule::CONTAINS => BooleanOp {
                loc: pair.loc(),
                op: BooleanOpType::Contains(Box::new(
                    self.parse_expression(inner.into_inner().next().unwrap())?,
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
            let prop_key = pairs.next().unwrap().as_str().to_string();
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
        let identifier = pairs.next().unwrap().as_str().to_string();
        let object = self.parse_object_step(pairs.next().unwrap())?;
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
        let inner = pair.clone().into_inner().next().unwrap();
        match inner.as_rule() {
            Rule::graph_step => Ok(Step {
                loc: inner.loc(),
                step: StepType::Node(self.parse_graph_step(inner)),
            }),
            Rule::object_step => Ok(Step {
                loc: inner.loc(),
                step: StepType::Object(self.parse_object_step(inner)?),
            }),
            Rule::closure_step => Ok(Step {
                loc: inner.loc(),
                step: StepType::Closure(self.parse_closure(inner)?),
            }),
            Rule::where_step => Ok(Step {
                loc: inner.loc(),
                step: StepType::Where(Box::new(self.parse_expression(inner)?)),
            }),
            Rule::range_step => Ok(Step {
                loc: inner.loc(),
                step: StepType::Range(self.parse_range(pair)?),
            }),

            Rule::bool_operations => Ok(Step {
                loc: inner.loc(),
                step: StepType::BooleanOperation(self.parse_bool_operation(inner)?),
            }),
            Rule::count => Ok(Step {
                loc: inner.loc(),
                step: StepType::Count,
            }),
            Rule::ID => Ok(Step {
                loc: inner.loc(),
                step: StepType::Object(Object {
                    fields: vec![FieldAddition {
                        key: "id".to_string(),
                        value: FieldValue {
                            loc: pair.loc(),
                            value: FieldValueType::Identifier("id".to_string()),
                        },
                        loc: pair.loc(),
                    }],
                    should_spread: false,
                    loc: pair.loc(),
                }),
            }),
            Rule::update => Ok(Step {
                loc: inner.loc(),
                step: StepType::Update(self.parse_update(inner)?),
            }),
            Rule::exclude_field => Ok(Step {
                loc: inner.loc(),
                step: StepType::Exclude(self.parse_exclude(inner)?),
            }),
            Rule::AddE => Ok(Step {
                loc: inner.loc(),
                step: StepType::AddEdge(self.parse_add_edge(inner, true)?),
            }),
            Rule::order_by => Ok(Step {
                loc: inner.loc(),
                step: StepType::OrderBy(self.parse_order_by(inner)?),
            }),
            Rule::aggregate => Ok(Step {
                loc: inner.loc(),
                step: StepType::Aggregate(self.parse_aggregate(inner)?),
            }),
            Rule::group_by => Ok(Step {
                loc: inner.loc(),
                step: StepType::GroupBy(self.parse_group_by(inner)?),
            }),
            Rule::first => Ok(Step {
                loc: inner.loc(),
                step: StepType::First,
            }),
            Rule::match_step => Ok(Step {
                loc: inner.loc(),
                step: StepType::Match(self.parse_match_step(inner)?),
            }),
            _ => Err(ParserError::from(format!(
                "Unexpected step type: {:?}",
                inner.as_rule()
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
                Rule::match_type_enum => todo!(),
                Rule::boolean => MatchType::Boolean(match_type.as_str() == "true"),
                Rule::optional_type => {
                    let optional_inner = match_type.into_inner().next().unwrap();
                    MatchType::Optional(match optional_inner.as_rule() {
                        Rule::identifier => Optional::Some(optional_inner.as_str().to_string()),
                        Rule::none => Optional::None,
                        _ => unreachable!(),
                    })
                }
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

    pub(super) fn parse_graph_step(&self, pair: Pair<Rule>) -> GraphStep {
        let types = |pair: &Pair<Rule>| {
            pair.clone()
                .into_inner()
                .next()
                .map(|p| p.as_str().to_string())
                .ok_or_else(|| ParserError::from("Expected type".to_string()))
                .unwrap()
        };
        let pair = pair.into_inner().next().unwrap();
        match pair.as_rule() {
            Rule::out_e => {
                let types = types(&pair);
                GraphStep {
                    loc: pair.loc(),
                    step: GraphStepType::OutE(types),
                }
            }
            Rule::in_e => {
                let types = types(&pair);
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
                let types = types(&pair);
                GraphStep {
                    loc: pair.loc(),
                    step: GraphStepType::Out(types),
                }
            }
            Rule::in_nodes => {
                let types = types(&pair);
                GraphStep {
                    loc: pair.loc(),
                    step: GraphStepType::In(types),
                }
            }
            Rule::shortest_path => {
                let (type_arg, from, to) = pair.clone().into_inner().fold(
                    (None, None, None),
                    |(type_arg, from, to), p| match p.as_rule() {
                        Rule::type_args => (
                            Some(p.into_inner().next().unwrap().as_str().to_string()),
                            from,
                            to,
                        ),
                        Rule::to_from => match p.into_inner().next() {
                            Some(p) => match p.as_rule() {
                                Rule::to => (
                                    type_arg,
                                    from,
                                    Some(p.into_inner().next().unwrap().as_str().to_string()),
                                ),
                                Rule::from => (
                                    type_arg,
                                    Some(p.into_inner().next().unwrap().as_str().to_string()),
                                    to,
                                ),
                                _ => unreachable!(),
                            },
                            None => (type_arg, from, to),
                        },
                        _ => (type_arg, from, to),
                    },
                );
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
            Rule::search_vector => GraphStep {
                loc: pair.loc(),
                step: GraphStepType::SearchVector(self.parse_search_vector(pair).unwrap()),
            },
            _ => {
                unreachable!()
            }
        }
    }
}
