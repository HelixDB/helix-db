use crate::helixc::parser::{
    HelixParser, ParserError, Rule,
    location::HasLoc,
    types::{AddEdge, AddNode, AddVector, Embed, EvaluatesToString, VectorData},
    utils::PairTools,
};
use pest::iterators::Pair;

impl HelixParser {
    pub(super) fn parse_add_vector(&self, pair: Pair<Rule>) -> Result<AddVector, ParserError> {
        let mut vector_type = None;
        let mut data = None;
        let mut fields = None;

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
                            let inner = vector_data.clone().try_inner_next()?;
                            data = Some(VectorData::Embed(Embed {
                                loc: vector_data.loc(),
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
                Rule::create_field => {
                    fields = Some(self.parse_property_assignments(p)?);
                }
                _ => {
                    return Err(ParserError::from(format!(
                        "Unexpected rule in AddV: {:?} => {:?}",
                        p.as_rule(),
                        p,
                    )));
                }
            }
        }

        Ok(AddVector {
            vector_type,
            data,
            fields,
            loc: pair.loc(),
        })
    }

    pub(super) fn parse_add_node(&self, pair: Pair<Rule>) -> Result<AddNode, ParserError> {
        let mut node_type = None;
        let mut fields = None;

        for p in pair.clone().into_inner() {
            match p.as_rule() {
                Rule::identifier_upper => {
                    node_type = Some(p.as_str().to_string());
                }
                Rule::create_field => {
                    fields = Some(self.parse_property_assignments(p)?);
                }
                _ => {
                    return Err(ParserError::from(format!(
                        "Unexpected rule in AddV: {:?} => {:?}",
                        p.as_rule(),
                        p,
                    )));
                }
            }
        }

        Ok(AddNode {
            node_type,
            fields,
            loc: pair.loc(),
        })
    }

    pub(super) fn parse_add_edge(
        &self,
        pair: Pair<Rule>,
        from_identifier: bool,
    ) -> Result<AddEdge, ParserError> {
        let mut edge_type = None;
        let mut fields = None;
        let mut connection = None;

        for p in pair.clone().into_inner() {
            match p.as_rule() {
                Rule::identifier_upper => {
                    edge_type = Some(p.as_str().to_string());
                }
                Rule::create_field => {
                    fields = Some(self.parse_property_assignments(p)?);
                }
                Rule::to_from => {
                    connection = Some(self.parse_to_from(p)?);
                }
                _ => {
                    return Err(ParserError::from(format!(
                        "Unexpected rule in AddE: {:?}",
                        p.as_rule()
                    )));
                }
            }
        }
        if edge_type.is_none() {
            return Err(ParserError::from("Missing edge type"));
        }
        if connection.is_none() {
            return Err(ParserError::from("Missing edge connection"));
        }
        Ok(AddEdge {
            edge_type,
            fields,
            connection: connection.ok_or_else(|| ParserError::from("Missing edge connection"))?,
            from_identifier,
            loc: pair.loc(),
        })
    }
}
