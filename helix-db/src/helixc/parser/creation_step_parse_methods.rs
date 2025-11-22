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

#[cfg(test)]
mod tests {
    use crate::helixc::parser::{HelixParser, write_to_temp_file};

    // ============================================================================
    // AddNode Tests
    // ============================================================================

    #[test]
    fn test_parse_add_node_basic() {
        let source = r#"
            N::Person { name: String }

            QUERY createPerson(name: String) =>
                person <- AddN<Person>({name: name})
                RETURN person
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_add_node_empty_fields() {
        let source = r#"
            N::Person { name: String }

            QUERY createPerson() =>
                person <- AddN<Person>()
                RETURN person
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_add_node_multiple_fields() {
        let source = r#"
            N::Person { name: String, age: U32, email: String }

            QUERY createPerson(name: String, age: U32, email: String) =>
                person <- AddN<Person>({name: name, age: age, email: email})
                RETURN person
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_add_node_with_literal_values() {
        let source = r#"
            N::Person { name: String, age: U32 }

            QUERY createPerson() =>
                person <- AddN<Person>({name: "Alice", age: 30})
                RETURN person
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());
    }

    // ============================================================================
    // AddEdge Tests
    // ============================================================================

    #[test]
    fn test_parse_add_edge_basic() {
        let source = r#"
            N::Person { name: String }
            E::Knows { From: Person, To: Person }

            QUERY createFriendship(id1: ID, id2: ID) =>
                person1 <- N<Person>(id1)
                person2 <- N<Person>(id2)
                AddE<Knows>::From(person1)::To(person2)
                RETURN "done"
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_add_edge_with_properties() {
        let source = r#"
            N::Person { name: String }
            E::Knows { From: Person, To: Person, Properties: { since: String } }

            QUERY createFriendship(id1: ID, id2: ID, since: String) =>
                person1 <- N<Person>(id1)
                person2 <- N<Person>(id2)
                AddE<Knows>({since: since})::From(person1)::To(person2)
                RETURN "done"
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_add_edge_from_to_order() {
        let source = r#"
            N::Person { name: String }
            E::Knows { From: Person, To: Person }

            QUERY createFriendship(id1: ID, id2: ID) =>
                person1 <- N<Person>(id1)
                person2 <- N<Person>(id2)
                AddE<Knows>::To(person2)::From(person1)
                RETURN "done"
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_add_edge_with_id_literals() {
        let source = r#"
            N::Person { name: String }
            E::Knows { From: Person, To: Person }

            QUERY createFriendship(fromId: ID) =>
                person <- N<Person>(fromId)
                AddE<Knows>::From(person)::To(fromId)
                RETURN "done"
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());
    }

    // ============================================================================
    // AddVector Tests
    // ============================================================================

    #[test]
    fn test_parse_add_vector_with_identifier() {
        let source = r#"
            V::Document { content: String, embedding: [F32] }

            QUERY addDoc(vector: [F32], content: String) =>
                doc <- AddV<Document>(vector, {content: content})
                RETURN doc
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_add_vector_with_embed() {
        let source = r#"
            V::Document { content: String, embedding: [F32] }

            QUERY addDoc(text: String) =>
                doc <- AddV<Document>(Embed(text), {content: text})
                RETURN doc
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_add_vector_with_string_embed() {
        let source = r#"
            V::Document { content: String, embedding: [F32] }

            QUERY addDoc() =>
                doc <- AddV<Document>(Embed("hello world"), {content: "hello world"})
                RETURN doc
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_add_vector_multiple_fields() {
        let source = r#"
            V::Document { content: String, title: String, embedding: [F32] }

            QUERY addDoc(vec: [F32], content: String, title: String) =>
                doc <- AddV<Document>(vec, {content: content, title: title})
                RETURN doc
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());
    }

    // ============================================================================
    // Complex Creation Tests
    // ============================================================================

    #[test]
    fn test_parse_create_node_and_edge() {
        let source = r#"
            N::Person { name: String }
            E::Knows { From: Person, To: Person }

            QUERY createRelationship(name1: String, name2: String) =>
                person1 <- AddN<Person>({name: name1})
                person2 <- AddN<Person>({name: name2})
                AddE<Knows>::From(person1)::To(person2)
                RETURN person1
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_multiple_node_creations() {
        let source = r#"
            N::Person { name: String }

            QUERY createPeople() =>
                p1 <- AddN<Person>({name: "Alice"})
                p2 <- AddN<Person>({name: "Bob"})
                p3 <- AddN<Person>({name: "Charlie"})
                RETURN p1
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_add_node_with_object_type_field() {
        let source = r#"
            N::Person { name: String, details: {age: U32, city: String} }

            QUERY createPerson(name: String, age: U32, city: String) =>
                person <- AddN<Person>({name: name})
                RETURN person
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());
    }

    // ============================================================================
    // Edge Cases
    // ============================================================================

    #[test]
    fn test_parse_add_node_empty_type() {
        let source = r#"
            N::EmptyNode {}

            QUERY createEmpty() =>
                node <- AddN<EmptyNode>()
                RETURN node
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_creation_with_traversal_result() {
        let source = r#"
            N::Person { name: String }
            E::Knows { From: Person, To: Person }

            QUERY addEdgeFromQuery(id: ID) =>
                person <- N<Person>(id)
                friend <- N<Person>
                AddE<Knows>::From(person)::To(friend)
                RETURN "done"
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_add_vector_no_extra_fields() {
        let source = r#"
            V::Document { embedding: [F32] }

            QUERY addDoc(vec: [F32]) =>
                doc <- AddV<Document>(vec)
                RETURN doc
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());
    }
}
