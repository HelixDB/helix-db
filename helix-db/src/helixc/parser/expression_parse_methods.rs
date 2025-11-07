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

#[cfg(test)]
mod tests {
    use crate::helixc::parser::{write_to_temp_file, HelixParser};

    // ============================================================================
    // Literal Expression Tests
    // ============================================================================

    #[test]
    fn test_parse_integer_literal() {
        let source = r#"
            N::Person { name: String }

            QUERY testQuery() =>
                value <- 42
                RETURN value
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_float_literal() {
        let source = r#"
            N::Person { name: String }

            QUERY testQuery() =>
                value <- 3.14
                RETURN value
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_boolean_literal_true() {
        let source = r#"
            N::Person { name: String }

            QUERY testQuery() =>
                value <- true
                RETURN value
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_boolean_literal_false() {
        let source = r#"
            N::Person { name: String }

            QUERY testQuery() =>
                value <- false
                RETURN value
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_string_literal() {
        let source = r#"
            N::Person { name: String }

            QUERY testQuery() =>
                value <- "Hello World"
                RETURN value
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_array_literal() {
        let source = r#"
            N::Person { name: String }

            QUERY testQuery() =>
                values <- [1, 2, 3]
                RETURN values
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());
    }

    // ============================================================================
    // Boolean Expression Tests
    // ============================================================================

    #[test]
    fn test_parse_and_in_where_clause() {
        let source = r#"
            N::Person { name: String, age: U32 }

            QUERY testQuery(targetName: String, targetAge: U32) =>
                person <- N<Person>::WHERE(
                    AND(
                        _::{name}::EQ(targetName),
                        _::{age}::EQ(targetAge)
                    )
                )
                RETURN person
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_or_in_where_clause() {
        let source = r#"
            N::Person { name: String }

            QUERY testQuery(name1: String, name2: String) =>
                person <- N<Person>::WHERE(
                    OR(
                        _::{name}::EQ(name1),
                        _::{name}::EQ(name2)
                    )
                )
                RETURN person
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_exists_expression() {
        let source = r#"
            N::Person { name: String }
            E::Knows { From: Person, To: Person }

            QUERY testQuery(id: ID) =>
                person <- N<Person>(id)
                hasFriends <- EXISTS(person::Out<Knows>)
                RETURN hasFriends
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_not_exists_expression() {
        let source = r#"
            N::Person { name: String }
            E::Knows { From: Person, To: Person }

            QUERY testQuery(id: ID) =>
                person <- N<Person>(id)
                hasNoFriends <- !EXISTS(person::Out<Knows>)
                RETURN hasNoFriends
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_negated_and_in_where() {
        let source = r#"
            N::Person { name: String, active: Boolean }

            QUERY testQuery(targetName: String) =>
                person <- N<Person>::WHERE(
                    !AND(
                        _::{name}::EQ(targetName),
                        _::{active}::EQ(true)
                    )
                )
                RETURN person
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());
    }

    // ============================================================================
    // For Loop Tests
    // ============================================================================

    #[test]
    fn test_parse_for_loop_with_identifier() {
        let source = r#"
            N::Person { name: String }

            QUERY testQuery() =>
                people <- N<Person>
                FOR person IN people {
                    name <- person
                }
                RETURN "done"
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_for_loop_with_destructuring() {
        let source = r#"
            N::Person { name: String, age: U32 }

            QUERY testQuery() =>
                people <- N<Person>
                FOR {name, age} IN people {
                    value <- name
                }
                RETURN "done"
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_for_loop_with_object_access() {
        let source = r#"
            N::Person { name: String }

            QUERY testQuery() =>
                people <- N<Person>
                FOR person.name IN people {
                    value <- person
                }
                RETURN "done"
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());
    }

    // ============================================================================
    // BM25 Search Tests
    // ============================================================================

    #[test]
    fn test_parse_bm25_search_with_string_literal() {
        let source = r#"
            V::Document { content: String }

            QUERY searchDocs() =>
                docs <- SearchBM25<Document>("search query", 10)
                RETURN docs
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_bm25_search_with_identifier() {
        let source = r#"
            V::Document { content: String }

            QUERY searchDocs(query: String) =>
                docs <- SearchBM25<Document>(query, 10)
                RETURN docs
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_bm25_search_with_variable_k() {
        let source = r#"
            V::Document { content: String }

            QUERY searchDocs(query: String, limit: I32) =>
                docs <- SearchBM25<Document>(query, limit)
                RETURN docs
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());
    }

    // ============================================================================
    // Vector Search Tests
    // ============================================================================

    #[test]
    fn test_parse_vector_search_with_identifier() {
        let source = r#"
            V::Document { content: String, embedding: [F32] }

            QUERY searchSimilar(queryVec: [F32]) =>
                docs <- SearchV<Document>(queryVec, 10)
                RETURN docs
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_vector_search_with_embed() {
        let source = r#"
            V::Document { content: String, embedding: [F32] }

            QUERY searchSimilar(query: String) =>
                docs <- SearchV<Document>(Embed(query), 10)
                RETURN docs
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_vector_search_with_string_embed() {
        let source = r#"
            V::Document { content: String, embedding: [F32] }

            QUERY searchSimilar() =>
                docs <- SearchV<Document>(Embed("search query"), 10)
                RETURN docs
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());
    }

    // ============================================================================
    // Assignment Tests
    // ============================================================================

    #[test]
    fn test_parse_assignment_with_identifier() {
        let source = r#"
            N::Person { name: String }

            QUERY testQuery(inputName: String) =>
                name <- inputName
                RETURN name
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_assignment_with_traversal() {
        let source = r#"
            N::Person { name: String }

            QUERY testQuery(id: ID) =>
                person <- N<Person>(id)
                RETURN person
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());
    }

    // ============================================================================
    // Edge Cases and Complex Expressions
    // ============================================================================

    #[test]
    fn test_parse_none_expression() {
        let source = r#"
            N::Person { name: String }

            QUERY testQuery() =>
                value <- NONE
                RETURN value
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_nested_boolean_in_where() {
        let source = r#"
            N::Person { name: String, age: U32, active: Boolean }

            QUERY testQuery(name1: String, name2: String, minAge: U32) =>
                person <- N<Person>::WHERE(
                    AND(
                        OR(
                            _::{name}::EQ(name1),
                            _::{name}::EQ(name2)
                        ),
                        _::{age}::GT(minAge)
                    )
                )
                RETURN person
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_multiple_assignments() {
        let source = r#"
            N::Person { name: String }

            QUERY testQuery() =>
                val1 <- 10
                val2 <- 20
                val3 <- 30
                RETURN val1
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());
    }
}
