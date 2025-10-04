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
            let _ = match param {
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
                    let field_type = scope.get(name.as_str()).unwrap().clone();
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
