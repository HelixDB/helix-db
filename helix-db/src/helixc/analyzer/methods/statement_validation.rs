//! Semantic analyzer for Helix‑QL.

use crate::helixc::analyzer::error_codes::ErrorCode;
use crate::{
    generate_error,
    helixc::{
        analyzer::{
            Ctx, errors::push_query_err, methods::infer_expr_type::infer_expr_type, types::Type,
            utils::is_valid_identifier,
        },
        generator::{
            queries::Query as GeneratedQuery,
            statements::Statement as GeneratedStatement,
            statements::{
                Assignment as GeneratedAssignment, Drop as GeneratedDrop,
                ForEach as GeneratedForEach, ForLoopInVariable, ForVariable,
            },
            utils::GenRef,
        },
        parser::types::*,
    },
};
use paste::paste;
use std::collections::HashMap;

/// Validates the statements in the query used at the highest level to generate each statement in the query
///
/// # Arguments
///
/// * `ctx` - The context of the query
/// * `scope` - The scope of the query
/// * `original_query` - The original query
/// * `query` - The generated query
/// * `statement` - The statement to validate
///
/// # Returns
///
/// * `Option<GeneratedStatement>` - The validated statement to generate rust code for
pub(crate) fn validate_statements<'a>(
    ctx: &mut Ctx<'a>,
    scope: &mut HashMap<&'a str, Type>,
    original_query: &'a Query,
    query: &mut GeneratedQuery,
    statement: &'a Statement,
) -> Option<GeneratedStatement> {
    use StatementType::*;
    match &statement.statement {
        Assignment(assign) => {
            if scope.contains_key(assign.variable.as_str()) {
                generate_error!(
                    ctx,
                    original_query,
                    assign.loc.clone(),
                    E302,
                    &assign.variable
                );
            }

            let (rhs_ty, stmt) =
                infer_expr_type(ctx, &assign.value, scope, original_query, None, query);
            
            scope.insert(assign.variable.as_str(), rhs_ty);

            stmt.as_ref()?;

            let assignment = GeneratedStatement::Assignment(GeneratedAssignment {
                variable: GenRef::Std(assign.variable.clone()),
                value: Box::new(stmt.unwrap()),
            });
            Some(assignment)
        }

        Drop(expr) => {
            let (_, stmt) = infer_expr_type(ctx, expr, scope, original_query, None, query);
            stmt.as_ref()?;

            query.is_mut = true;
            if let Some(GeneratedStatement::Traversal(tr)) = stmt {
                Some(GeneratedStatement::Drop(GeneratedDrop { expression: tr }))
            } else {
                panic!("Drop should only be applied to traversals");
            }
        }

        Expression(expr) => {
            let (_, stmt) = infer_expr_type(ctx, expr, scope, original_query, None, query);
            stmt
        }

        // PARAMS DONT GET PARSED TO TYPE::ARRAY
        ForLoop(fl) => {
            if !scope.contains_key(fl.in_variable.1.as_str()) {
                generate_error!(ctx, original_query, fl.loc.clone(), E301, &fl.in_variable.1);
            }

            let mut body_scope = HashMap::new();
            let mut for_loop_in_variable: ForLoopInVariable = ForLoopInVariable::Empty;

            // Check if the in variable is a parameter
            let param = original_query
                .parameters
                .iter()
                .find(|p| p.name.1 == fl.in_variable.1);
            // if it is a parameter, add it to the body scope
            // else assume variable in scope and add it to the body scope
            let in_var_type = match param {
                Some(param) => {
                    for_loop_in_variable =
                        ForLoopInVariable::Parameter(GenRef::Std(fl.in_variable.1.clone()));
                    Type::from(param.param_type.1.clone())
                }
                None => match scope.get(fl.in_variable.1.as_str()) {
                    Some(fl_in_var_ty) => {
                        is_valid_identifier(
                            ctx,
                            original_query,
                            fl.loc.clone(),
                            fl.in_variable.1.as_str(),
                        );

                        for_loop_in_variable =
                            ForLoopInVariable::Identifier(GenRef::Std(fl.in_variable.1.clone()));
                        fl_in_var_ty.clone()
                    }
                    None => {
                        generate_error!(
                            ctx,
                            original_query,
                            fl.loc.clone(),
                            E301,
                            &fl.in_variable.1
                        );
                        Type::Unknown
                    }
                },
            };

            let mut for_variable: ForVariable = ForVariable::Empty;

            match &fl.variable {
                ForLoopVars::Identifier { name, loc: _ } => {
                    is_valid_identifier(ctx, original_query, fl.loc.clone(), name.as_str());
                    // Extract the inner type from the array type
                    let field_type = match &in_var_type {
                        Type::Array(inner) => inner.as_ref().clone(),
                        _ => {
                            // If not an array, generate error for non-iterable
                            generate_error!(
                                ctx,
                                original_query,
                                fl.in_variable.0.clone(),
                                E651,
                                &fl.in_variable.1
                            );
                            Type::Unknown
                        }
                    };
                    body_scope.insert(name.as_str(), field_type.clone());
                    scope.insert(name.as_str(), field_type);
                    for_variable = ForVariable::Identifier(GenRef::Std(name.clone()));
                }
                ForLoopVars::ObjectAccess { .. } => {
                    todo!()
                }
                ForLoopVars::ObjectDestructuring { fields, loc: _ } => {
                    match &param {
                        Some(p) => {
                            for_loop_in_variable =
                                ForLoopInVariable::Parameter(GenRef::Std(p.name.1.clone()));
                            match &p.param_type.1 {
                                FieldType::Array(inner) => match inner.as_ref() {
                                    FieldType::Object(param_fields) => {
                                        for (field_loc, field_name) in fields {
                                            if !param_fields.contains_key(field_name.as_str()) {
                                                generate_error!(
                                                    ctx,
                                                    original_query,
                                                    field_loc.clone(),
                                                    E652,
                                                    [field_name, &fl.in_variable.1],
                                                    [field_name, &fl.in_variable.1]
                                                );
                                            }
                                            let field_type = Type::from(
                                                param_fields
                                                    .get(field_name.as_str())
                                                    .unwrap()
                                                    .clone(),
                                            );
                                            body_scope.insert(field_name.as_str(), field_type.clone());
                                            scope.insert(field_name.as_str(), field_type);
                                        }
                                        for_variable = ForVariable::ObjectDestructure(
                                            fields
                                                .iter()
                                                .map(|(_, f)| GenRef::Std(f.clone()))
                                                .collect(),
                                        );
                                    }
                                    _ => {
                                        generate_error!(
                                            ctx,
                                            original_query,
                                            fl.in_variable.0.clone(),
                                            E653,
                                            [&fl.in_variable.1],
                                            [&fl.in_variable.1]
                                        );
                                    }
                                },

                                _ => {
                                    generate_error!(
                                        ctx,
                                        original_query,
                                        fl.in_variable.0.clone(),
                                        E651,
                                        &fl.in_variable.1
                                    );
                                }
                            }
                        }
                        None => match scope.get(fl.in_variable.1.as_str()) {
                            Some(Type::Array(object_arr)) => {
                                match object_arr.as_ref() {
                                    Type::Object(object) => {
                                        let mut obj_dest_fields = Vec::with_capacity(fields.len());
                                        let object = object.clone();
                                        for (_, field_name) in fields {
                                            let name = field_name.as_str();
                                            // adds non-param fields to scope
                                            let field_type = object.get(name).unwrap().clone();
                                            body_scope.insert(name, field_type.clone());
                                            scope.insert(name, field_type);
                                            obj_dest_fields.push(GenRef::Std(name.to_string()));
                                        }
                                        for_variable =
                                            ForVariable::ObjectDestructure(obj_dest_fields);
                                    }
                                    _ => {
                                        generate_error!(
                                            ctx,
                                            original_query,
                                            fl.in_variable.0.clone(),
                                            E653,
                                            [&fl.in_variable.1],
                                            [&fl.in_variable.1]
                                        );
                                    }
                                }
                            }
                            _ => {
                                generate_error!(
                                    ctx,
                                    original_query,
                                    fl.in_variable.0.clone(),
                                    E301,
                                    &fl.in_variable.1
                                );
                            }
                        },
                    }
                }
            }
            let mut statements = Vec::new();
            for body_stmt in &fl.statements {
                let stmt = validate_statements(ctx, scope, original_query, query, body_stmt);
                if let Some(s) = stmt {
                    statements.push(s);
                }
            }
            // body_scope.iter().for_each(|(k, _)| {
            //     scope.remove(k);
            // });

            let stmt = GeneratedStatement::ForEach(GeneratedForEach {
                for_variables: for_variable,
                in_variable: for_loop_in_variable,
                statements,
            });
            Some(stmt)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::helixc::parser::{write_to_temp_file, HelixParser};

    // ============================================================================
    // Assignment Validation Tests
    // ============================================================================

    #[test]
    fn test_duplicate_variable_assignment() {
        let source = r#"
            N::Person { name: String }

            QUERY test() =>
                person <- N<Person>
                person <- N<Person>
                RETURN person
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.iter().any(|d| d.error_code == ErrorCode::E302));
        assert!(diagnostics.iter().any(|d| d.message.contains("previously declared")));
    }

    #[test]
    fn test_valid_multiple_assignments_different_names() {
        let source = r#"
            N::Person { name: String }

            QUERY test() =>
                person1 <- N<Person>
                person2 <- N<Person>
                RETURN person1, person2
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(!diagnostics.iter().any(|d| d.error_code == ErrorCode::E302));
    }

    // ============================================================================
    // For Loop Validation Tests
    // ============================================================================

    #[test]
    fn test_for_loop_in_variable_not_in_scope() {
        let source = r#"
            N::Person { name: String }

            QUERY test() =>
                FOR p IN unknownList {
                    person <- N<Person>
                }
                RETURN "done"
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.iter().any(|d| d.error_code == ErrorCode::E301));
        assert!(diagnostics.iter().any(|d| d.message.contains("not in scope") && d.message.contains("unknownList")));
    }

    #[test]
    fn test_for_loop_with_valid_parameter() {
        let source = r#"
            N::Person { name: String }

            QUERY test(ids: [ID]) =>
                FOR id IN ids {
                    person <- N<Person>(id)
                }
                RETURN "done"
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(!diagnostics.iter().any(|d| d.error_code == ErrorCode::E301));
        assert!(!diagnostics.iter().any(|d| d.error_code == ErrorCode::E651));
    }

    #[test]
    fn test_for_loop_non_iterable_variable() {
        let source = r#"
            N::Person { name: String }

            QUERY test(id: ID) =>
                FOR p IN id {
                    person <- N<Person>
                }
                RETURN "done"
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.iter().any(|d| d.error_code == ErrorCode::E651));
        assert!(diagnostics.iter().any(|d| d.message.contains("not iterable")));
    }

    #[test]
    fn test_for_loop_with_object_destructuring() {
        let source = r#"
            N::Person { name: String, age: U32 }

            QUERY test(data: [{name: String, age: U32}]) =>
                FOR {name, age} IN data {
                    person <- AddN<Person>({name: name, age: age})
                }
                RETURN "done"
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        // This tests the for loop with object destructuring works
    }

    // ============================================================================
    // Drop Statement Tests
    // ============================================================================

    #[test]
    fn test_drop_statement_valid() {
        let source = r#"
            N::Person { name: String }
            E::Knows { From: Person, To: Person }

            QUERY test(id: ID) =>
                person <- N<Person>(id)
                DROP person::Out<Knows>
                RETURN person
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        // DROP statements should not produce scope errors
        assert!(!diagnostics.iter().any(|d| d.error_code == ErrorCode::E301));
    }

    #[test]
    fn test_drop_with_undefined_variable() {
        let source = r#"
            N::Person { name: String }

            QUERY test() =>
                DROP unknownVar
                RETURN "done"
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.iter().any(|d| d.error_code == ErrorCode::E301));
    }

    // ============================================================================
    // Expression Statement Tests
    // ============================================================================

    #[test]
    fn test_expression_statement_add_node() {
        let source = r#"
            N::Person { name: String }

            QUERY test() =>
                AddN<Person>({name: "Alice"})
                RETURN "created"
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        // Expression statements should not produce errors
        assert!(diagnostics.is_empty() || !diagnostics.iter().any(|d| d.error_code == ErrorCode::E301));
    }

    #[test]
    fn test_expression_statement_add_edge() {
        let source = r#"
            N::Person { name: String }
            E::Knows { From: Person, To: Person }

            QUERY test(id1: ID, id2: ID) =>
                p1 <- N<Person>(id1)
                p2 <- N<Person>(id2)
                AddE<Knows>::From(p1)::To(p2)
                RETURN "connected"
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(!diagnostics.iter().any(|d| d.error_code == ErrorCode::E301));
    }

    // ============================================================================
    // Complex Statement Tests
    // ============================================================================

    #[test]
    fn test_nested_for_loops() {
        let source = r#"
            N::Person { name: String }
            N::Company { name: String }

            QUERY test(peopleIds: [ID], companyIds: [ID]) =>
                FOR personId IN peopleIds {
                    FOR companyId IN companyIds {
                        person <- N<Person>(personId)
                    }
                }
                RETURN "done"
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        // Nested for loops should work
        assert!(!diagnostics.iter().any(|d| d.error_code == ErrorCode::E301));
    }

    #[test]
    fn test_assignment_with_property_access() {
        let source = r#"
            N::Person { name: String, age: U32 }

            QUERY test(id: ID) =>
                person <- N<Person>(id)
                name <- person::{name}
                age <- person::{age}
                RETURN name, age
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(!diagnostics.iter().any(|d| d.error_code == ErrorCode::E301 || d.error_code == ErrorCode::E302));
    }

    #[test]
    fn test_assignment_with_traversal_chain() {
        let source = r#"
            N::Person { name: String }
            N::Company { name: String }
            E::WorksAt { From: Person, To: Company }

            QUERY test(personId: ID) =>
                person <- N<Person>(personId)
                edges <- person::OutE<WorksAt>
                companies <- edges::ToN
                RETURN companies
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(!diagnostics.iter().any(|d| d.error_code == ErrorCode::E301 || d.error_code == ErrorCode::E302));
    }
}
