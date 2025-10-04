use crate::{
    helixc::parser::{
        HelixParser, ParserError, Rule,
        location::{HasLoc, Loc},
        types::{
            Assignment, BM25Search, Embed, EvaluatesToNumber, EvaluatesToNumberType,
            EvaluatesToString, ExistsExpression, Expression, ExpressionType, ForLoop, ForLoopVars,
            SearchVector, ValueType, VectorData,
        },
        utils::{PairTools, PairsTools},
    },
    protocol::value::Value,
};
use pest::iterators::{Pair, Pairs};

impl HelixParser {
    pub(super) fn parse_assignment(&self, pair: Pair<Rule>) -> Result<Assignment, ParserError> {
        let mut pairs = pair.clone().into_inner();
        let variable = pairs.try_next()?.as_str().to_string();
        let value = self.parse_expression(pairs.try_next()?)?;

        Ok(Assignment {
            variable,
            value,
            loc: pair.loc(),
        })
    }

    pub(super) fn parse_expression(&self, p: Pair<Rule>) -> Result<Expression, ParserError> {
        let pair = p.try_inner_next()?;

        match pair.as_rule() {
            Rule::traversal => Ok(Expression {
                loc: pair.loc(),
                expr: ExpressionType::Traversal(Box::new(self.parse_traversal(pair)?)),
            }),
            Rule::id_traversal => Ok(Expression {
                loc: pair.loc(),
                expr: ExpressionType::Traversal(Box::new(self.parse_traversal(pair)?)),
            }),

            Rule::anonymous_traversal => Ok(Expression {
                loc: pair.loc(),
                expr: ExpressionType::Traversal(Box::new(self.parse_anon_traversal(pair)?)),
            }),
            Rule::identifier => Ok(Expression {
                loc: pair.loc(),
                expr: ExpressionType::Identifier(pair.as_str().to_string()),
            }),
            Rule::string_literal => Ok(Expression {
                loc: pair.loc(),
                expr: ExpressionType::StringLiteral(self.parse_string_literal(pair)?),
            }),
            Rule::exists => {
                let loc = pair.loc();
                let mut inner = pair.into_inner();
                let negated = match inner.peek() {
                    Some(p) => p.as_rule() == Rule::negate,
                    None => false,
                };
                if negated {
                    inner.next();
                }
                let traversal = inner
                    .next()
                    .ok_or_else(|| ParserError::from("Missing traversal"))?;

                let expr = ExpressionType::Exists(ExistsExpression {
                    loc: loc.clone(),
                    expr: Box::new(Expression {
                        loc: loc.clone(),
                        expr: ExpressionType::Traversal(Box::new(match traversal.as_rule() {
                            Rule::anonymous_traversal => self.parse_anon_traversal(traversal)?,
                            Rule::id_traversal => self.parse_traversal(traversal)?,
                            Rule::traversal => self.parse_traversal(traversal)?,
                            _ => unreachable!(),
                        })),
                    }),
                });
                Ok(Expression {
                    loc: loc.clone(),
                    expr: match negated {
                        true => ExpressionType::Not(Box::new(Expression {
                            loc: loc.clone(),
                            expr,
                        })),
                        false => expr,
                    },
                })
            }
            Rule::integer => pair
                .as_str()
                .parse()
                .map(|i| Expression {
                    loc: pair.loc(),
                    expr: ExpressionType::IntegerLiteral(i),
                })
                .map_err(|_| ParserError::from("Invalid integer literal")),
            Rule::float => pair
                .as_str()
                .parse()
                .map(|f| Expression {
                    loc: pair.loc(),
                    expr: ExpressionType::FloatLiteral(f),
                })
                .map_err(|_| ParserError::from("Invalid float literal")),
            Rule::boolean => Ok(Expression {
                loc: pair.loc(),
                expr: ExpressionType::BooleanLiteral(pair.as_str() == "true"),
            }),
            Rule::array_literal => Ok(Expression {
                loc: pair.loc(),
                expr: ExpressionType::ArrayLiteral(self.parse_array_literal(pair)?),
            }),
            Rule::evaluates_to_bool => Ok(self.parse_boolean_expression(pair)?),
            Rule::AddN => Ok(Expression {
                loc: pair.loc(),
                expr: ExpressionType::AddNode(self.parse_add_node(pair)?),
            }),
            Rule::AddV => Ok(Expression {
                loc: pair.loc(),
                expr: ExpressionType::AddVector(self.parse_add_vector(pair)?),
            }),
            Rule::AddE => Ok(Expression {
                loc: pair.loc(),
                expr: ExpressionType::AddEdge(self.parse_add_edge(pair, false)?),
            }),
            Rule::search_vector => Ok(Expression {
                loc: pair.loc(),
                expr: ExpressionType::SearchVector(self.parse_search_vector(pair)?),
            }),
            Rule::none => Ok(Expression {
                loc: pair.loc(),
                expr: ExpressionType::Empty,
            }),
            Rule::bm25_search => Ok(Expression {
                loc: pair.loc(),
                expr: ExpressionType::BM25Search(self.parse_bm25_search(pair)?),
            }),
            _ => Err(ParserError::from(format!(
                "Unexpected expression type: {:?}",
                pair.as_rule()
            ))),
        }
    }

    pub(super) fn parse_boolean_expression(
        &self,
        pair: Pair<Rule>,
    ) -> Result<Expression, ParserError> {
        let expression = pair.try_inner_next()?;
        match expression.as_rule() {
            Rule::and => {
                let loc: Loc = expression.loc();
                let mut inner = expression.into_inner();
                let negated = match inner.peek() {
                    Some(p) => p.as_rule() == Rule::negate,
                    None => false,
                };
                if negated {
                    inner.next();
                }
                let exprs = self.parse_expression_vec(inner)?;
                Ok(Expression {
                    loc: loc.clone(),
                    expr: match negated {
                        true => ExpressionType::Not(Box::new(Expression {
                            loc,
                            expr: ExpressionType::And(exprs),
                        })),
                        false => ExpressionType::And(exprs),
                    },
                })
            }
            Rule::or => {
                let loc: Loc = expression.loc();
                let mut inner = expression.into_inner();
                let negated = match inner.peek() {
                    Some(p) => p.as_rule() == Rule::negate,
                    None => false,
                };
                if negated {
                    inner.next();
                }
                let exprs = self.parse_expression_vec(inner)?;
                Ok(Expression {
                    loc: loc.clone(),
                    expr: match negated {
                        true => ExpressionType::Not(Box::new(Expression {
                            loc,
                            expr: ExpressionType::Or(exprs),
                        })),
                        false => ExpressionType::Or(exprs),
                    },
                })
            }
            Rule::boolean => Ok(Expression {
                loc: expression.loc(),
                expr: ExpressionType::BooleanLiteral(expression.as_str() == "true"),
            }),
            Rule::exists => {
                let loc = expression.loc();
                let mut inner = expression.into_inner();
                let negated = match inner.peek() {
                    Some(p) => p.as_rule() == Rule::negate,
                    None => false,
                };
                if negated {
                    inner.next();
                }
                let traversal = inner
                    .next()
                    .ok_or_else(|| ParserError::from("Missing traversal"))?;
                let expr = ExpressionType::Exists(ExistsExpression {
                    loc: loc.clone(),
                    expr: Box::new(Expression {
                        loc: loc.clone(),
                        expr: ExpressionType::Traversal(Box::new(match traversal.as_rule() {
                            Rule::anonymous_traversal => self.parse_anon_traversal(traversal)?,
                            Rule::id_traversal => self.parse_traversal(traversal)?,
                            Rule::traversal => self.parse_traversal(traversal)?,
                            _ => unreachable!(),
                        })),
                    }),
                });
                Ok(Expression {
                    loc: loc.clone(),
                    expr: match negated {
                        true => ExpressionType::Not(Box::new(Expression {
                            loc: loc.clone(),
                            expr,
                        })),
                        false => expr,
                    },
                })
            }

            _ => unreachable!(),
        }
    }
    pub(super) fn parse_expression_vec(
        &self,
        pairs: Pairs<Rule>,
    ) -> Result<Vec<Expression>, ParserError> {
        let mut expressions = Vec::new();
        for p in pairs {
            match p.as_rule() {
                Rule::anonymous_traversal => {
                    expressions.push(Expression {
                        loc: p.loc(),
                        expr: ExpressionType::Traversal(Box::new(self.parse_anon_traversal(p)?)),
                    });
                }
                Rule::traversal => {
                    expressions.push(Expression {
                        loc: p.loc(),
                        expr: ExpressionType::Traversal(Box::new(self.parse_traversal(p)?)),
                    });
                }
                Rule::id_traversal => {
                    expressions.push(Expression {
                        loc: p.loc(),
                        expr: ExpressionType::Traversal(Box::new(self.parse_traversal(p)?)),
                    });
                }
                Rule::evaluates_to_bool => {
                    expressions.push(self.parse_boolean_expression(p)?);
                }
                _ => unreachable!(),
            }
        }
        Ok(expressions)
    }

    pub(super) fn parse_bm25_search(&self, pair: Pair<Rule>) -> Result<BM25Search, ParserError> {
        let mut pairs = pair.clone().into_inner();
        let vector_type = pairs.try_next()?.as_str().to_string();
        let query = match pairs.next() {
            Some(pair) => match pair.as_rule() {
                Rule::identifier => ValueType::Identifier {
                    value: pair.as_str().to_string(),
                    loc: pair.loc(),
                },
                Rule::string_literal => ValueType::Literal {
                    value: Value::String(pair.as_str().to_string()),
                    loc: pair.loc(),
                },
                _ => {
                    return Err(ParserError::from(format!(
                        "Unexpected rule in BM25Search: {:?}",
                        pair.as_rule()
                    )));
                }
            },
            None => {
                return Err(ParserError::from(format!(
                    "Unexpected rule in BM25Search: {:?}",
                    pair.as_rule()
                )));
            }
        };
        let k = Some(match pairs.next() {
            Some(pair) => match pair.as_rule() {
                Rule::identifier => EvaluatesToNumber {
                    loc: pair.loc(),
                    value: EvaluatesToNumberType::Identifier(pair.as_str().to_string()),
                },
                Rule::integer => EvaluatesToNumber {
                    loc: pair.loc(),
                    value: EvaluatesToNumberType::I32(
                        pair.as_str()
                            .to_string()
                            .parse::<i32>()
                            .map_err(|_| ParserError::from("Invalid integer value"))?,
                    ),
                },
                _ => {
                    return Err(ParserError::from(format!(
                        "Unexpected rule in BM25Search: {:?}",
                        pair.as_rule()
                    )));
                }
            },
            None => {
                return Err(ParserError::from(format!(
                    "Unexpected rule in BM25Search: {:?}",
                    pair.as_rule()
                )));
            }
        });

        Ok(BM25Search {
            loc: pair.loc(),
            type_arg: Some(vector_type),
            data: Some(query),
            k,
        })
    }

    pub(super) fn parse_for_loop(&self, pair: Pair<Rule>) -> Result<ForLoop, ParserError> {
        let mut pairs = pair.clone().into_inner();
        // parse the arguments
        let argument = pairs.try_next_inner().try_next()?;
        let argument_loc = argument.loc();
        let variable = match argument.as_rule() {
            Rule::object_destructuring => {
                let fields = argument
                    .into_inner()
                    .map(|p| (p.loc(), p.as_str().to_string()))
                    .collect();
                ForLoopVars::ObjectDestructuring {
                    fields,
                    loc: argument_loc,
                }
            }
            Rule::object_access => {
                let mut inner = argument.clone().into_inner();
                let object_name = inner.try_next()?.as_str().to_string();
                let field_name = inner.try_next()?.as_str().to_string();
                ForLoopVars::ObjectAccess {
                    name: object_name,
                    field: field_name,
                    loc: argument_loc,
                }
            }
            Rule::identifier => ForLoopVars::Identifier {
                name: argument.as_str().to_string(),
                loc: argument_loc,
            },
            _ => {
                return Err(ParserError::from(format!(
                    "Unexpected rule in ForLoop: {:?}",
                    argument.as_rule()
                )));
            }
        };

        // parse the in
        let in_ = pairs.try_next()?.clone();
        let in_variable = match in_.as_rule() {
            Rule::identifier => (in_.loc(), in_.as_str().to_string()),
            _ => {
                return Err(ParserError::from(format!(
                    "Unexpected rule in ForLoop: {:?}",
                    in_.as_rule()
                )));
            }
        };
        // parse the body
        let statements = self.parse_query_body(pairs.try_next()?)?;

        Ok(ForLoop {
            variable,
            in_variable,
            statements,
            loc: pair.loc(),
        })
    }

    pub(super) fn parse_search_vector(
        &self,
        pair: Pair<Rule>,
    ) -> Result<SearchVector, ParserError> {
        let mut vector_type = None;
        let mut data = None;
        let mut k: Option<EvaluatesToNumber> = None;
        let mut pre_filter = None;
        for p in pair.clone().into_inner() {
            match p.as_rule() {
                Rule::identifier_upper => {
                    vector_type = Some(p.as_str().to_string());
                }
                Rule::vector_data => {
                    let vector_data = p.clone().try_inner_next()?;
                    match vector_data.as_rule() {
                        Rule::identifier => {
                            data = Some(VectorData::Identifier(p.as_str().to_string()));
                        }
                        Rule::vec_literal => {
                            data = Some(VectorData::Vector(self.parse_vec_literal(p)?));
                        }
                        Rule::embed_method => {
                            let loc = vector_data.loc();
                            let inner = vector_data.try_inner_next()?;
                            data = Some(VectorData::Embed(Embed {
                                loc,
                                value: match inner.as_rule() {
                                    Rule::identifier => {
                                        EvaluatesToString::Identifier(inner.as_str().to_string())
                                    }
                                    Rule::string_literal => {
                                        EvaluatesToString::StringLiteral(inner.as_str().to_string())
                                    }
                                    _ => {
                                        return Err(ParserError::from(format!(
                                            "Unexpected rule in SearchV: {:?} => {:?}",
                                            inner.as_rule(),
                                            inner,
                                        )));
                                    }
                                },
                            }));
                        }
                        _ => {
                            return Err(ParserError::from(format!(
                                "Unexpected rule in SearchV: {:?} => {:?}",
                                vector_data.as_rule(),
                                vector_data,
                            )));
                        }
                    }
                }
                Rule::integer => {
                    k = Some(EvaluatesToNumber {
                        loc: p.loc(),
                        value: EvaluatesToNumberType::I32(
                            p.as_str()
                                .to_string()
                                .parse::<i32>()
                                .map_err(|_| ParserError::from("Invalid integer value"))?,
                        ),
                    });
                }
                Rule::identifier => {
                    k = Some(EvaluatesToNumber {
                        loc: p.loc(),
                        value: EvaluatesToNumberType::Identifier(p.as_str().to_string()),
                    });
                }
                Rule::pre_filter => {
                    pre_filter = Some(Box::new(self.parse_expression(p)?));
                }
                _ => {
                    return Err(ParserError::from(format!(
                        "Unexpected rule in SearchV: {:?} => {:?}",
                        p.as_rule(),
                        p,
                    )));
                }
            }
        }

        Ok(SearchVector {
            loc: pair.loc(),
            vector_type,
            data,
            k,
            pre_filter,
        })
    }
}
