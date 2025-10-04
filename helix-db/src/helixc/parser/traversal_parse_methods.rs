use crate::{
    helixc::parser::{
        HelixParser, ParserError, Rule,
        location::HasLoc,
        types::{IdType, StartNode, Traversal, ValueType},
        utils::{PairTools, PairsTools},
    },
    protocol::value::Value,
};
use pest::iterators::{Pair, Pairs};

impl HelixParser {
    pub(super) fn parse_traversal(&self, pair: Pair<Rule>) -> Result<Traversal, ParserError> {
        let mut pairs = pair.clone().into_inner();
        let start = self.parse_start_node(
            pairs
                .next()
                .ok_or_else(|| ParserError::from(format!("Expected start node, got {pair:?}")))?,
        )?;
        let steps = pairs
            .map(|p| self.parse_step(p))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Traversal {
            start,
            steps,
            loc: pair.loc(),
        })
    }

    pub(super) fn parse_anon_traversal(&self, pair: Pair<Rule>) -> Result<Traversal, ParserError> {
        let pairs = pair.clone().into_inner();
        let start = StartNode::Anonymous;
        let steps = pairs
            .map(|p| self.parse_step(p))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Traversal {
            start,
            steps,
            loc: pair.loc(),
        })
    }

    pub(super) fn parse_start_node(&self, pair: Pair<Rule>) -> Result<StartNode, ParserError> {
        match pair.as_rule() {
            Rule::start_node => {
                let pairs = pair.into_inner();
                let mut node_type = String::new();
                let mut ids = None;
                for p in pairs {
                    match p.as_rule() {
                        Rule::type_args => {
                            node_type = p.try_inner_next()?.as_str().to_string();
                            // WATCH
                        }
                        Rule::id_args => {
                            let mut new_ids = Vec::new();
                            for id in p.into_inner() {
                                let id = id.try_inner_next()?;
                                let id_to_add = match id.as_rule() {
                                    Rule::identifier => IdType::Identifier {
                                        value: id.as_str().to_string(),
                                        loc: id.loc(),
                                    },
                                    Rule::string_literal => IdType::Literal {
                                        value: id.as_str().to_string(),
                                        loc: id.loc(),
                                    },
                                    _ => {
                                        return Err(ParserError::from(
                                            "Should be identifier or string literal",
                                        ));
                                    }
                                };
                                new_ids.push(id_to_add);
                            }
                            ids = Some(new_ids);
                        }
                        Rule::by_index => {
                            ids = Some({
                                let mut pairs: Pairs<'_, Rule> = p.clone().into_inner();
                                let index = pairs.try_next_inner().try_next()?;
                                let index = match index.as_rule() {
                                    Rule::identifier => IdType::Identifier {
                                        value: index.as_str().to_string(),
                                        loc: index.loc(),
                                    },
                                    Rule::string_literal => IdType::Literal {
                                        value: index.as_str().to_string(),
                                        loc: index.loc(),
                                    },
                                    other => {
                                        return Err(ParserError::from(format!(
                                            "Should be identifier or string literal: {other:?}"
                                        )));
                                    }
                                };
                                let value = match pairs.try_next_inner()?.next() {
                                    Some(val) => match val.as_rule() {
                                        Rule::identifier => ValueType::Identifier {
                                            value: val.as_str().to_string(),
                                            loc: val.loc(),
                                        },
                                        Rule::string_literal => ValueType::Literal {
                                            value: Value::from(val.as_str()),
                                            loc: val.loc(),
                                        },
                                        Rule::integer => ValueType::Literal {
                                            value: Value::from(
                                                val.as_str().parse::<i64>().map_err(|_| {
                                                    ParserError::from("Invalid integer value")
                                                })?,
                                            ),
                                            loc: val.loc(),
                                        },
                                        Rule::float => ValueType::Literal {
                                            value: Value::from(
                                                val.as_str().parse::<f64>().map_err(|_| {
                                                    ParserError::from("Invalid float value")
                                                })?,
                                            ),
                                            loc: val.loc(),
                                        },
                                        Rule::boolean => ValueType::Literal {
                                            value: Value::from(
                                                val.as_str().parse::<bool>().map_err(|_| {
                                                    ParserError::from("Invalid boolean value")
                                                })?,
                                            ),
                                            loc: val.loc(),
                                        },
                                        _ => {
                                            return Err(ParserError::from(
                                                "Should be identifier or string literal",
                                            ));
                                        }
                                    },
                                    _ => unreachable!(),
                                };
                                vec![IdType::ByIndex {
                                    index: Box::new(index),
                                    value: Box::new(value),
                                    loc: p.loc(),
                                }]
                            })
                        }
                        _ => unreachable!(),
                    }
                }
                Ok(StartNode::Node { node_type, ids })
            }
            Rule::start_edge => {
                let pairs = pair.into_inner();
                let mut edge_type = String::new();
                let mut ids = None;
                for p in pairs {
                    match p.as_rule() {
                        Rule::type_args => {
                            edge_type = p.try_inner_next()?.as_str().to_string();
                        }
                        Rule::id_args => {
                            let mut new_ids = Vec::new();
                            for id in p.into_inner() {
                                let id = id.try_inner_next()?;
                                let id_to_add = match id.as_rule() {
                                    Rule::identifier => IdType::Identifier {
                                        value: id.as_str().to_string(),
                                        loc: id.loc(),
                                    },
                                    Rule::string_literal => IdType::Literal {
                                        value: id.as_str().to_string(),
                                        loc: id.loc(),
                                    },
                                    _ => {
                                        return Err(ParserError::from(
                                            "Should be identifier or string literal",
                                        ));
                                    }
                                };
                                new_ids.push(id_to_add);
                            }
                            ids = Some(new_ids);
                        }
                        _ => unreachable!(),
                    }
                }
                Ok(StartNode::Edge { edge_type, ids })
            }
            Rule::identifier => Ok(StartNode::Identifier(pair.as_str().to_string())),
            Rule::search_vector => Ok(StartNode::SearchVector(self.parse_search_vector(pair)?)),
            Rule::start_vector => {
                let pairs = pair.into_inner();
                let mut vector_type = String::new();
                let mut ids = None;
                for p in pairs {
                    match p.as_rule() {
                        Rule::type_args => {
                            vector_type = p.try_inner_next()?.as_str().to_string();
                        }
                        Rule::id_args => {
                            let mut new_ids = Vec::new();
                            for id in p.into_inner() {
                                let id = id.try_inner_next()?;
                                let id_to_add = match id.as_rule() {
                                    Rule::identifier => IdType::Identifier {
                                        value: id.as_str().to_string(),
                                        loc: id.loc(),
                                    },
                                    Rule::string_literal => IdType::Literal {
                                        value: id.as_str().to_string(),
                                        loc: id.loc(),
                                    },
                                    _ => {
                                        return Err(ParserError::from(
                                            "Should be identifier or string literal",
                                        ));
                                    }
                                };
                                new_ids.push(id_to_add);
                            }
                            ids = Some(new_ids);
                        }
                        Rule::by_index => {
                            let mut new_ids = Vec::new();
                            for p in p.into_inner() {
                                let mut pairs = p.clone().into_inner();
                                let index_inner = pairs.try_next_inner()?.try_next()?;
                                let index = match index_inner.as_rule() {
                                    Rule::identifier => IdType::Identifier {
                                        value: index_inner.as_str().to_string(),
                                        loc: index_inner.loc(),
                                    },
                                    Rule::string_literal => IdType::Literal {
                                        value: index_inner.as_str().to_string(),
                                        loc: index_inner.loc(),
                                    },
                                    _ => {
                                        return Err(ParserError::from(
                                            "Should be identifier or string literal",
                                        ));
                                    }
                                };
                                let value_inner = pairs.try_next_inner()?.try_next()?;
                                let value = match value_inner.as_rule() {
                                    Rule::identifier => ValueType::Identifier {
                                        value: value_inner.as_str().to_string(),
                                        loc: value_inner.loc(),
                                    },
                                    Rule::string_literal => ValueType::Literal {
                                        value: Value::from(value_inner.as_str()),
                                        loc: value_inner.loc(),
                                    },
                                    Rule::integer => ValueType::Literal {
                                        value: Value::from(
                                            value_inner.as_str().parse::<i64>().map_err(|_| {
                                                ParserError::from("Invalid integer value")
                                            })?,
                                        ),
                                        loc: value_inner.loc(),
                                    },
                                    Rule::float => ValueType::Literal {
                                        value: Value::from(
                                            value_inner.as_str().parse::<f64>().map_err(|_| {
                                                ParserError::from("Invalid float value")
                                            })?,
                                        ),
                                        loc: value_inner.loc(),
                                    },
                                    Rule::boolean => ValueType::Literal {
                                        value: Value::from(
                                            value_inner.as_str().parse::<bool>().map_err(|_| {
                                                ParserError::from("Invalid boolean value")
                                            })?,
                                        ),
                                        loc: value_inner.loc(),
                                    },
                                    _ => {
                                        return Err(ParserError::from(
                                            "Should be identifier or literal",
                                        ));
                                    }
                                };
                                new_ids.push(IdType::ByIndex {
                                    index: Box::new(index),
                                    value: Box::new(value),
                                    loc: p.loc(),
                                });
                            }
                            ids = Some(new_ids);
                        }
                        _ => unreachable!(),
                    }
                }
                Ok(StartNode::Vector { vector_type, ids })
            }
            _ => Ok(StartNode::Anonymous),
        }
    }
}
