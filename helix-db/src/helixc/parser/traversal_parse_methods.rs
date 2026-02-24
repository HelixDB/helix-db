use crate::{
    helixc::parser::{
        HelixParser, ParserError, Rule,
        location::HasLoc,
        types::{IdType, StartNode, Traversal, ValueType},
        utils::{PairTools, PairsTools},
    },
    protocol::value::Value,
};
use pest::iterators::Pair;

impl HelixParser {
    pub(super) fn parse_traversal(&self, pair: Pair<Rule>) -> Result<Traversal, ParserError> {
        let loc = pair.loc();
        let pair_debug = format!("{pair:?}");
        let mut pairs = pair.into_inner();
        let start = self.parse_start_node(pairs.next().ok_or_else(|| {
            ParserError::from(format!("Expected start node, got {pair_debug}"))
        })?)?;
        let steps = pairs
            .map(|p| self.parse_step(p))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Traversal { start, steps, loc })
    }

    pub(super) fn parse_anon_traversal(&self, pair: Pair<Rule>) -> Result<Traversal, ParserError> {
        let loc = pair.loc();
        let pairs = pair.into_inner();
        let start = StartNode::Anonymous;
        let steps = pairs
            .map(|p| self.parse_step(p))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Traversal { start, steps, loc })
    }

    fn parse_id_selector(&self, pair: Pair<Rule>) -> Result<IdType, ParserError> {
        let loc = pair.loc();
        let id = pair.try_inner_next()?;
        match id.as_rule() {
            Rule::identifier => Ok(IdType::Identifier {
                value: id.as_str().to_string(),
                loc,
            }),
            Rule::string_literal => Ok(IdType::Literal {
                value: id.as_str().to_string(),
                loc,
            }),
            _ => Err(ParserError::from("Should be identifier or string literal")),
        }
    }

    fn parse_id_args_list(&self, pair: Pair<Rule>) -> Result<Vec<IdType>, ParserError> {
        pair.into_inner()
            .map(|id| self.parse_id_selector(id))
            .collect()
    }

    fn parse_by_index_value(&self, pair: Pair<Rule>) -> Result<ValueType, ParserError> {
        match pair.as_rule() {
            Rule::identifier => Ok(ValueType::Identifier {
                value: pair.as_str().to_string(),
                loc: pair.loc(),
            }),
            Rule::string_literal => Ok(ValueType::Literal {
                value: Value::from(pair.as_str()),
                loc: pair.loc(),
            }),
            Rule::integer => Ok(ValueType::Literal {
                value: Value::from(
                    pair.as_str()
                        .parse::<i64>()
                        .map_err(|_| ParserError::from("Invalid integer value"))?,
                ),
                loc: pair.loc(),
            }),
            Rule::float => Ok(ValueType::Literal {
                value: Value::from(
                    pair.as_str()
                        .parse::<f64>()
                        .map_err(|_| ParserError::from("Invalid float value"))?,
                ),
                loc: pair.loc(),
            }),
            Rule::boolean => Ok(ValueType::Literal {
                value: Value::from(
                    pair.as_str()
                        .parse::<bool>()
                        .map_err(|_| ParserError::from("Invalid boolean value"))?,
                ),
                loc: pair.loc(),
            }),
            _ => Err(ParserError::from("Should be identifier or literal")),
        }
    }

    fn parse_by_index_ids(&self, pair: Pair<Rule>) -> Result<Vec<IdType>, ParserError> {
        let loc = pair.loc();
        let mut pairs = pair.into_inner();

        let index_inner = pairs.try_next()?.try_inner_next()?;
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
                return Err(ParserError::from("Should be identifier or string literal"));
            }
        };

        let value_inner = pairs.try_next()?.try_inner_next()?;
        let value = self.parse_by_index_value(value_inner)?;

        Ok(vec![IdType::ByIndex {
            index: Box::new(index),
            value: Box::new(value),
            loc,
        }])
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
                        }
                        Rule::id_args => {
                            ids = Some(self.parse_id_args_list(p)?);
                        }
                        Rule::by_index => {
                            ids = Some(self.parse_by_index_ids(p)?);
                        }
                        other => {
                            return Err(ParserError::from(format!(
                                "Unexpected rule in start_node: {:?}",
                                other
                            )));
                        }
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
                            ids = Some(self.parse_id_args_list(p)?);
                        }
                        other => {
                            return Err(ParserError::from(format!(
                                "Unexpected rule in start_edge: {:?}",
                                other
                            )));
                        }
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
                            ids = Some(self.parse_id_args_list(p)?);
                        }
                        Rule::by_index => {
                            ids = Some(self.parse_by_index_ids(p)?);
                        }
                        other => {
                            return Err(ParserError::from(format!(
                                "Unexpected rule in start_vector: {:?}",
                                other
                            )));
                        }
                    }
                }
                Ok(StartNode::Vector { vector_type, ids })
            }
            _ => Ok(StartNode::Anonymous),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::helixc::parser::{
        HelixParser,
        types::{
            EvaluatesToNumberType, ExpressionType, GraphStepType, IdType, StartNode, StatementType,
            StepType, ValueType, VectorData, WeightExpression,
        },
        write_to_temp_file,
    };

    #[test]
    fn parses_search_vector_start_node() {
        let source = r#"
            V::Document { body: String }

            QUERY find(vec: [F64]) =>
                docs <- SearchV<Document>(vec, 10)
                RETURN docs
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).expect("parse should succeed");
        let stmt = &parsed.queries[0].statements[0];

        let StatementType::Assignment(assignment) = &stmt.statement else {
            panic!("expected assignment statement")
        };
        let ExpressionType::Traversal(traversal) = &assignment.value.expr else {
            panic!("expected traversal expression")
        };

        match &traversal.start {
            StartNode::SearchVector(search) => {
                assert_eq!(search.vector_type.as_deref(), Some("Document"));
                assert!(matches!(
                    search.data,
                    Some(VectorData::Identifier(ref value)) if value == "vec"
                ));
                assert!(matches!(
                    search.k.as_ref().map(|k| &k.value),
                    Some(EvaluatesToNumberType::I32(10))
                ));
            }
            other => panic!("expected search vector start node, got {other:?}"),
        }
    }

    #[test]
    fn parses_search_vector_graph_step() {
        let source = r#"
            V::Document { body: String }

            QUERY refine(vec: [F64]) =>
                docs <- V<Document>::SearchV<Document>(vec, 10)
                RETURN docs
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).expect("parse should succeed");
        let stmt = &parsed.queries[0].statements[0];

        let StatementType::Assignment(assignment) = &stmt.statement else {
            panic!("expected assignment statement")
        };
        let ExpressionType::Traversal(traversal) = &assignment.value.expr else {
            panic!("expected traversal expression")
        };

        let step = traversal
            .steps
            .first()
            .expect("expected one traversal step");
        let StepType::Node(graph_step) = &step.step else {
            panic!("expected node graph step")
        };

        match &graph_step.step {
            GraphStepType::SearchVector(search) => {
                assert_eq!(search.vector_type.as_deref(), Some("Document"));
                assert!(matches!(
                    search.data,
                    Some(VectorData::Identifier(ref value)) if value == "vec"
                ));
                assert!(matches!(
                    search.k.as_ref().map(|k| &k.value),
                    Some(EvaluatesToNumberType::I32(10))
                ));
            }
            other => panic!("expected search vector graph step, got {other:?}"),
        }
    }

    #[test]
    fn parses_node_start_by_index() {
        let source = r#"
            N::User { INDEX email: String, name: String }

            QUERY lookup(email: String) =>
                user <- N<User>({email: email})
                RETURN user
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).expect("parse should succeed");
        let stmt = &parsed.queries[0].statements[0];

        let StatementType::Assignment(assignment) = &stmt.statement else {
            panic!("expected assignment statement")
        };
        let ExpressionType::Traversal(traversal) = &assignment.value.expr else {
            panic!("expected traversal expression")
        };

        match &traversal.start {
            StartNode::Node { node_type, ids } => {
                assert_eq!(node_type, "User");
                let ids = ids.as_ref().expect("expected indexed selector");
                assert_eq!(ids.len(), 1);
                match &ids[0] {
                    IdType::ByIndex { index, value, .. } => {
                        assert!(matches!(
                            index.as_ref(),
                            IdType::Identifier { value, .. } if value == "email"
                        ));
                        assert!(matches!(
                            value.as_ref(),
                            ValueType::Identifier { value, .. } if value == "email"
                        ));
                    }
                    other => panic!("expected by-index selector, got {other:?}"),
                }
            }
            other => panic!("expected node start, got {other:?}"),
        }
    }

    #[test]
    fn parses_vector_start_by_index() {
        let source = r#"
            V::Document { content: String }

            QUERY lookup(doc_id: ID) =>
                doc <- V<Document>({id: doc_id})
                RETURN doc
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).expect("parse should succeed");
        let stmt = &parsed.queries[0].statements[0];

        let StatementType::Assignment(assignment) = &stmt.statement else {
            panic!("expected assignment statement")
        };
        let ExpressionType::Traversal(traversal) = &assignment.value.expr else {
            panic!("expected traversal expression")
        };

        match &traversal.start {
            StartNode::Vector { vector_type, ids } => {
                assert_eq!(vector_type, "Document");
                let ids = ids.as_ref().expect("expected indexed selector");
                assert_eq!(ids.len(), 1);
                match &ids[0] {
                    IdType::ByIndex { index, value, .. } => {
                        assert!(matches!(
                            index.as_ref(),
                            IdType::Identifier { value, .. } if value == "id"
                        ));
                        assert!(matches!(
                            value.as_ref(),
                            ValueType::Identifier { value, .. } if value == "doc_id"
                        ));
                    }
                    other => panic!("expected by-index selector, got {other:?}"),
                }
            }
            other => panic!("expected vector start, got {other:?}"),
        }
    }

    #[test]
    fn parses_shortest_path_variants() {
        let source = r#"
            N::User { name: String }
            E::Follows { From: User, To: User, Properties: { weight: F64 } }

            QUERY shortest(fromId: ID, toId: ID) =>
                p1 <- N<User>(fromId)::ShortestPath<User>::To(toId)
                p2 <- N<User>(fromId)::ShortestPathBFS<User>::To(toId)
                p3 <- N<User>(fromId)::ShortestPathDijkstras<User>(_::{weight})::To(toId)
                p4 <- N<User>(fromId)::ShortestPathAStar<User>(_::{weight}, "h")::To(toId)
                RETURN p1
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).expect("parse should succeed");

        let expect_variant = |index: usize| {
            let statement = &parsed.queries[0].statements[index];
            let StatementType::Assignment(assignment) = &statement.statement else {
                panic!("expected assignment statement")
            };
            let ExpressionType::Traversal(traversal) = &assignment.value.expr else {
                panic!("expected traversal expression")
            };
            let step = traversal
                .steps
                .first()
                .expect("expected one traversal step");
            let StepType::Node(graph_step) = &step.step else {
                panic!("expected node graph step")
            };
            &graph_step.step
        };

        assert!(matches!(expect_variant(0), GraphStepType::ShortestPath(_)));
        assert!(matches!(
            expect_variant(1),
            GraphStepType::ShortestPathBFS(_)
        ));

        assert!(matches!(
            expect_variant(2),
            GraphStepType::ShortestPathDijkstras(d) if matches!(d.weight_expr, Some(WeightExpression::Expression(_)))
        ));

        assert!(matches!(
            expect_variant(3),
            GraphStepType::ShortestPathAStar(a)
                if a.heuristic_property == "h"
                && matches!(a.weight_expr, Some(WeightExpression::Expression(_)))
        ));
    }
}
