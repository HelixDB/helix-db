use crate::helixc::parser::{
    HelixParser, ParserError, Rule,
    location::HasLoc,
    types::{BuiltInMacro, Parameter, Query, Statement, StatementType},
};
use pest::iterators::Pair;
use std::collections::HashSet;

impl HelixParser {
    pub(super) fn parse_query_def(
        &self,
        pair: Pair<Rule>,
        filepath: String,
    ) -> Result<Query, ParserError> {
        let original_query = pair.clone().as_str().to_string();
        let mut pairs = pair.clone().into_inner();
        let built_in_macro = match pairs.peek() {
            Some(pair) if pair.as_rule() == Rule::built_in_macro => {
                let built_in_macro = match pair.into_inner().next() {
                    Some(pair) => match pair.as_rule() {
                        Rule::mcp_macro => Some(BuiltInMacro::MCP),
                        Rule::model_macro => match pair.into_inner().next() {
                            Some(model_name) => {
                                Some(BuiltInMacro::Model(model_name.as_str().to_string()))
                            }
                            None => {
                                return Err(ParserError::from("Model macro missing model name"));
                            }
                        },
                        _ => None,
                    },
                    _ => None,
                };
                pairs.next();
                built_in_macro
            }
            _ => None,
        };
        let name = pairs
            .next()
            .ok_or_else(|| ParserError::from("Expected query name"))?
            .as_str()
            .to_string();
        let parameters = self.parse_parameters(
            pairs
                .next()
                .ok_or_else(|| ParserError::from("Expected parameters block"))?,
        )?;
        let body = pairs
            .next()
            .ok_or_else(|| ParserError::from("Expected query body"))?;
        let statements = self.parse_query_body(body)?;
        let return_values = self.parse_return_statement(
            pairs
                .next()
                .ok_or_else(|| ParserError::from("Expected return statement"))?,
        )?;

        Ok(Query {
            built_in_macro,
            name,
            parameters,
            statements,
            return_values,
            original_query,
            loc: pair.loc_with_filepath(filepath),
        })
    }

    pub(super) fn parse_parameters(&self, pair: Pair<Rule>) -> Result<Vec<Parameter>, ParserError> {
        let mut seen = HashSet::new();
        pair.clone()
            .into_inner()
            .map(|p: Pair<'_, Rule>| -> Result<Parameter, ParserError> {
                let mut inner = p.into_inner();
                let name = {
                    let pair = inner
                        .next()
                        .ok_or_else(|| ParserError::from("Expected parameter name"))?;
                    (pair.loc(), pair.as_str().to_string())
                };

                // gets optional param
                let is_optional = inner
                    .peek()
                    .is_some_and(|p| p.as_rule() == Rule::optional_param);
                if is_optional {
                    inner.next();
                }

                // gets param type
                let param_type_outer = inner
                    .clone()
                    .next()
                    .ok_or_else(|| ParserError::from("Expected parameter type"))?;
                let param_type_pair = param_type_outer
                    .clone()
                    .into_inner()
                    .next()
                    .ok_or_else(|| ParserError::from("Expected parameter type definition"))?;
                let param_type_location = param_type_pair.loc();
                let param_type = self.parse_field_type(
                    // unwraps the param type to get the rule (array, object, named_type, etc)
                    param_type_pair,
                    Some(&self.source),
                )?;

                if seen.insert(name.1.clone()) {
                    Ok(Parameter {
                        name,
                        param_type: (param_type_location, param_type),
                        is_optional,
                        loc: pair.loc(),
                    })
                } else {
                    Err(ParserError::from(format!(
                        r#"Duplicate parameter name: {}
                            Please use unique parameter names.

                            Error happened at line {} column {} here: {}
                        "#,
                        name.1,
                        pair.line_col().0,
                        pair.line_col().1,
                        pair.as_str(),
                    )))
                }
            })
            .collect::<Result<Vec<_>, _>>()
    }

    pub(super) fn parse_query_body(&self, pair: Pair<Rule>) -> Result<Vec<Statement>, ParserError> {
        pair.into_inner()
            .map(|p| match p.as_rule() {
                // path_macro_stmt removed - now using distinct function names,
                Rule::get_stmt => Ok(Statement {
                    loc: p.loc(),
                    statement: StatementType::Assignment(self.parse_assignment(p)?),
                }),
                Rule::creation_stmt => Ok(Statement {
                    loc: p.loc(),
                    statement: StatementType::Expression(self.parse_expression(p)?),
                }),

                Rule::drop => {
                    let inner = p
                        .into_inner()
                        .next()
                        .ok_or_else(|| ParserError::from("Drop statement missing expression"))?;
                    Ok(Statement {
                        loc: inner.loc(),
                        statement: StatementType::Drop(self.parse_expression(inner)?),
                    })
                }

                Rule::for_loop => Ok(Statement {
                    loc: p.loc(),
                    statement: StatementType::ForLoop(self.parse_for_loop(p)?),
                }),
                _ => Err(ParserError::from(format!(
                    "Unexpected statement type in query body: {:?}",
                    p.as_rule()
                ))),
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::helixc::parser::{HelixParser, write_to_temp_file};

    // ============================================================================
    // Basic Query Parsing Tests
    // ============================================================================

    #[test]
    fn test_parse_simple_query() {
        let source = r#"
            N::Person { name: String }

            QUERY getUser(id: ID) =>
                user <- N<Person>(id)
                RETURN user
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());

        let parsed = result.unwrap();
        assert_eq!(parsed.queries.len(), 1);
        assert_eq!(parsed.queries[0].name, "getUser");
        assert_eq!(parsed.queries[0].parameters.len(), 1);
    }

    #[test]
    fn test_parse_query_with_multiple_parameters() {
        let source = r#"
            N::Person { name: String, age: U32 }

            QUERY findPerson(name: String, age: U32) =>
                person <- N<Person>
                RETURN person
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());

        let parsed = result.unwrap();
        assert_eq!(parsed.queries[0].parameters.len(), 2);
        assert_eq!(parsed.queries[0].parameters[0].name.1, "name");
        assert_eq!(parsed.queries[0].parameters[1].name.1, "age");
    }

    #[test]
    fn test_parse_query_with_optional_parameter() {
        let source = r#"
            N::Person { name: String }

            QUERY findPerson(name?: String) =>
                person <- N<Person>
                RETURN person
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());

        let parsed = result.unwrap();
        assert!(parsed.queries[0].parameters[0].is_optional);
    }

    #[test]
    fn test_parse_query_with_array_parameter() {
        let source = r#"
            N::Person { name: String }

            QUERY findPeople(names: [String]) =>
                people <- N<Person>
                RETURN people
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());

        let parsed = result.unwrap();
        assert!(matches!(
            parsed.queries[0].parameters[0].param_type.1,
            crate::helixc::parser::types::FieldType::Array(_)
        ));
    }

    // ============================================================================
    // Built-in Macro Tests
    // ============================================================================

    #[test]
    fn test_parse_query_with_mcp_macro() {
        let source = r#"
            N::Person { name: String }

            #[mcp]
            QUERY getUser(id: ID) =>
                user <- N<Person>(id)
                RETURN user
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());

        let parsed = result.unwrap();
        assert!(matches!(
            parsed.queries[0].built_in_macro,
            Some(BuiltInMacro::MCP)
        ));
    }

    #[test]
    fn test_parse_query_with_model_macro() {
        let source = r#"
            N::Person { name: String }

            #[model("gpt-4")]
            QUERY generateResponse(prompt: String) =>
                response <- N<Person>
                RETURN response
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());

        let parsed = result.unwrap();
        assert!(matches!(
            parsed.queries[0].built_in_macro,
            Some(BuiltInMacro::Model(_))
        ));
    }

    // ============================================================================
    // Query Body and Statements Tests
    // ============================================================================

    #[test]
    fn test_parse_query_with_multiple_statements() {
        let source = r#"
            N::Person { name: String }
            E::Knows { From: Person, To: Person }

            QUERY complexQuery(id: ID) =>
                user <- N<Person>(id)
                friends <- user::Out<Knows>
                RETURN friends
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());

        let parsed = result.unwrap();
        assert_eq!(parsed.queries[0].statements.len(), 2);
    }

    #[test]
    fn test_parse_query_with_creation_statement() {
        let source = r#"
            N::Person { name: String }

            QUERY createPerson(name: String) =>
                person <- AddN<Person>({name: name})
                RETURN person
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());

        let parsed = result.unwrap();
        assert_eq!(parsed.queries[0].statements.len(), 1);
    }

    // ============================================================================
    // Return Statement Tests
    // ============================================================================

    #[test]
    fn test_parse_query_with_multiple_return_values() {
        let source = r#"
            N::Person { name: String }

            QUERY getUsers() =>
                user1 <- N<Person>
                user2 <- N<Person>
                RETURN user1, user2
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());

        let parsed = result.unwrap();
        assert_eq!(parsed.queries[0].return_values.len(), 2);
    }

    // ============================================================================
    // Error Cases
    // ============================================================================

    #[test]
    fn test_parse_query_duplicate_parameter_names() {
        let source = r#"
            N::Person { name: String }

            QUERY badQuery(name: String, name: String) =>
                person <- N<Person>
                RETURN person
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_err());

        let err = result.unwrap_err();
        let err_string = err.to_string();
        assert!(err_string.contains("Duplicate"));
    }

    #[test]
    fn test_parse_query_missing_return() {
        let source = r#"
            N::Person { name: String }

            QUERY badQuery(id: ID) =>
                user <- N<Person>(id)
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_query_missing_parameters() {
        let source = r#"
            N::Person { name: String }

            QUERY badQuery =>
                user <- N<Person>
                RETURN user
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_err());
    }

    // ============================================================================
    // Edge Cases
    // ============================================================================

    #[test]
    fn test_parse_query_no_parameters() {
        let source = r#"
            N::Person { name: String }

            QUERY getAllUsers() =>
                users <- N<Person>
                RETURN users
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());

        let parsed = result.unwrap();
        assert_eq!(parsed.queries[0].parameters.len(), 0);
    }

    #[test]
    fn test_parse_multiple_queries() {
        let source = r#"
            N::Person { name: String }

            QUERY query1() =>
                users <- N<Person>
                RETURN users

            QUERY query2() =>
                users <- N<Person>
                RETURN users
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());

        let parsed = result.unwrap();
        assert_eq!(parsed.queries.len(), 2);
    }

    #[test]
    fn test_parse_query_with_drop_statement() {
        let source = r#"
            N::Person { name: String }
            E::Knows { From: Person, To: Person }

            QUERY removeConnection(id: ID) =>
                person <- N<Person>(id)
                DROP person::Out<Knows>
                RETURN person
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());

        let parsed = result.unwrap();
        assert_eq!(parsed.queries[0].statements.len(), 2);
    }

    #[test]
    fn test_parse_query_with_for_loop() {
        let source = r#"
            N::Person { name: String }

            QUERY processPeople(ids: [ID]) =>
                FOR id IN ids {
                    person <- N<Person>(id)
                }
                RETURN "done"
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());

        let parsed = result.unwrap();
        assert_eq!(parsed.queries[0].statements.len(), 1);
    }

    #[test]
    fn test_parse_query_mixed_optional_required_parameters() {
        let source = r#"
            N::Person { name: String, age: U32, email: String }

            QUERY findPerson(name: String, age?: U32, email?: String) =>
                person <- N<Person>
                RETURN person
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());

        let parsed = result.unwrap();
        assert_eq!(parsed.queries[0].parameters.len(), 3);
        assert!(!parsed.queries[0].parameters[0].is_optional);
        assert!(parsed.queries[0].parameters[1].is_optional);
        assert!(parsed.queries[0].parameters[2].is_optional);
    }

    #[test]
    fn test_parse_query_with_object_parameter() {
        let source = r#"
            N::Person { name: String, details: {age: U32, city: String} }

            QUERY createPerson(details: {age: U32, city: String}) =>
                person <- AddN<Person>({details: details})
                RETURN person
        "#;

        let content = write_to_temp_file(vec![source]);
        let result = HelixParser::parse_source(&content);
        assert!(result.is_ok());

        let parsed = result.unwrap();
        assert!(matches!(
            parsed.queries[0].parameters[0].param_type.1,
            crate::helixc::parser::types::FieldType::Object(_)
        ));
    }
}
