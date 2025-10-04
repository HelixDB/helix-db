//! Semantic analyzer for Helix‑QL.

use crate::generate_error;
use crate::helixc::analyzer::error_codes::ErrorCode;
use crate::helixc::{
    analyzer::{
        Ctx,
        errors::{push_query_err, push_query_warn},
        methods::{infer_expr_type::infer_expr_type, statement_validation::validate_statements},
        types::Type,
        utils::{gen_identifier_or_param, is_valid_identifier},
    },
    generator::{
        queries::{Parameter as GeneratedParameter, Query as GeneratedQuery},
        return_values::{ReturnValue, ReturnValueExpr},
        source_steps::SourceStep,
        statements::Statement as GeneratedStatement,
        traversal_steps::ShouldCollect,
        utils::{GenRef, GeneratedValue},
    },
    parser::{location::Loc, types::*},
};
use paste::paste;
use std::collections::HashMap;

pub(crate) fn validate_query<'a>(ctx: &mut Ctx<'a>, original_query: &'a Query) {
    let mut query = GeneratedQuery {
        name: original_query.name.clone(),
        ..Default::default()
    };

    if let Some(BuiltInMacro::Model(model_name)) = &original_query.built_in_macro {
        // handle model macro
        query.embedding_model_to_use = Some(model_name.clone());
    }

    // -------------------------------------------------
    // Parameter validation
    // -------------------------------------------------
    for param in &original_query.parameters {
        if let FieldType::Identifier(ref id) = param.param_type.1
            && is_valid_identifier(ctx, original_query, param.param_type.0.clone(), id.as_str())
            && !ctx.node_set.contains(id.as_str())
            && !ctx.edge_map.contains_key(id.as_str())
            && !ctx.vector_set.contains(id.as_str())
        {
            generate_error!(
                ctx,
                original_query,
                param.param_type.0.clone(),
                E209,
                &id,
                &param.name.1
            );
        }
        // constructs parameters and sub‑parameters for generator
        GeneratedParameter::unwrap_param(
            param.clone(),
            &mut query.parameters,
            &mut query.sub_parameters,
        );
    }

    // -------------------------------------------------
    // Statement‑by‑statement walk
    // -------------------------------------------------
    let mut scope: HashMap<&str, Type> = HashMap::new();
    for param in &original_query.parameters {
        scope.insert(
            param.name.1.as_str(),
            Type::from(param.param_type.1.clone()),
        );
    }
    for stmt in &original_query.statements {
        let statement = validate_statements(ctx, &mut scope, original_query, &mut query, stmt);
        if let Some(s) = statement {
            query.statements.push(s);
        } else {
            // given all erroneous statements are caught by the analyzer, this should never happen
            return;
        }
    }

    // -------------------------------------------------
    // Validate RETURN expressions
    // -------------------------------------------------
    if original_query.return_values.is_empty() {
        let end = original_query.loc.end;
        push_query_warn(
            ctx,
            original_query,
            Loc::new(
                original_query.loc.filepath.clone(),
                end,
                end,
                original_query.loc.span.clone(),
            ),
            ErrorCode::W101,
            "query has no RETURN clause".to_string(),
            "add `RETURN <expr>` at the end",
            None,
        );
    }
    for ret in &original_query.return_values {
        analyze_return_expr(ctx, original_query, &mut scope, &mut query, ret);
    }

    if let Some(BuiltInMacro::MCP) = &original_query.built_in_macro {
        if query.return_values.len() != 1 {
            generate_error!(
                ctx,
                original_query,
                original_query.loc.clone(),
                E401,
                &query.return_values.len().to_string()
            );
        }
        let return_name = query.return_values.first().unwrap().get_name();
        query.mcp_handler = Some(return_name);
    }

    ctx.output.queries.push(query);
}

fn analyze_return_expr<'a>(
    ctx: &mut Ctx<'a>,
    original_query: &'a Query,
    scope: &mut HashMap<&'a str, Type>,
    query: &mut GeneratedQuery,
    ret: &'a ReturnType,
) {
    match ret {
        ReturnType::Expression(expr) => {
            let (_, stmt) = infer_expr_type(ctx, expr, scope, original_query, None, query);

            if stmt.is_none() {
                return;
            }

            match stmt.unwrap() {
                GeneratedStatement::Traversal(traversal) => {
                    match &traversal.source_step.inner() {
                        SourceStep::Identifier(v) => {
                            is_valid_identifier(
                                ctx,
                                original_query,
                                expr.loc.clone(),
                                v.inner().as_str(),
                            );

                            // if is single object, need to handle it as a single object
                            // if is array, need to handle it as an array
                            match traversal.should_collect {
                                ShouldCollect::ToVec => {
                                    query.return_values.push(ReturnValue::new_named(
                                        GeneratedValue::Literal(GenRef::Literal(v.inner().clone())),
                                        ReturnValueExpr::Traversal(traversal.clone()),
                                    ));
                                }
                                ShouldCollect::ToObj => {
                                    query.return_values.push(ReturnValue::new_single_named(
                                        GeneratedValue::Literal(GenRef::Literal(v.inner().clone())),
                                        ReturnValueExpr::Traversal(traversal.clone()),
                                    ));
                                }
                                ShouldCollect::Try => {
                                    query.return_values.push(ReturnValue::new_aggregate_traversal(
                                        GeneratedValue::Literal(GenRef::Literal(v.inner().clone())),
                                        ReturnValueExpr::Traversal(traversal.clone()),
                                    ));
                                }
                                _ => {
                                    unreachable!()
                                }
                            }
                        }
                        _ => {
                            query.return_values.push(ReturnValue::new_unnamed(
                                ReturnValueExpr::Traversal(traversal.clone()),
                            ));
                        }
                    }
                }
                GeneratedStatement::Identifier(id) => {
                    is_valid_identifier(ctx, original_query, expr.loc.clone(), id.inner().as_str());
                    let identifier_end_type = match scope.get(id.inner().as_str()) {
                        Some(t) => t.clone(),
                        None => {
                            generate_error!(
                                ctx,
                                original_query,
                                expr.loc.clone(),
                                E301,
                                id.inner().as_str()
                            );
                            Type::Unknown
                        }
                    };
                    let value =
                        gen_identifier_or_param(original_query, id.inner().as_str(), false, true);

                    match identifier_end_type {
                        Type::Aggregate => {
                            query.return_values.push(ReturnValue::new_aggregate(
                                GeneratedValue::Aggregate(GenRef::Literal(id.inner().clone())),
                                value,
                            ));
                        }
                        Type::Scalar(_) | Type::Boolean  => {
                            query.return_values.push(ReturnValue::new_named_literal(
                                GeneratedValue::Literal(GenRef::Literal(id.inner().clone())),
                                value,
                            ));
                        }
                        Type::Node(_) | Type::Vector(_) | Type::Edge(_) => {
                            query.return_values.push(ReturnValue::new_single_named(
                                GeneratedValue::Literal(GenRef::Literal(id.inner().clone())),
                                ReturnValueExpr::Identifier(value),
                            ));
                        }
                        _ => {
                            query.return_values.push(ReturnValue::new_named(
                                GeneratedValue::Literal(GenRef::Literal(id.inner().clone())),
                                ReturnValueExpr::Identifier(value),
                            ));
                        }
                    }
                }
                GeneratedStatement::Literal(l) => {
                    query.return_values.push(ReturnValue::new_literal(
                        GeneratedValue::Literal(l.clone()),
                        GeneratedValue::Literal(l.clone()),
                    ));
                }
                GeneratedStatement::Empty => query.return_values = vec![],

                // given all erroneous statements are caught by the analyzer, this should never happen
                // all malformed statements (not gramatically correct) should be caught by the parser
                _ => unreachable!(),
            }
        }
        ReturnType::Array(values) => {
            let values = values
                .iter()
                .map(|object| process_return_object(ctx, original_query, scope, object, query))
                .collect::<Vec<ReturnValueExpr>>();
            query.return_values.push(ReturnValue::new_array(values));
        }
        ReturnType::Object(values) => {
            let values = values
                .iter()
                .map(|(key, value)| {
                    (
                        key.clone(),
                        process_return_object(ctx, original_query, scope, value, query),
                    )
                })
                .collect::<HashMap<String, ReturnValueExpr>>();
            query.return_values.push(ReturnValue::new_object(values));
        }
        ReturnType::Empty => {}
    }
}

fn process_return_object<'a>(
    ctx: &mut Ctx<'a>,
    original_query: &'a Query,
    scope: &mut HashMap<&'a str, Type>,
    return_type: &'a ReturnType,
    query: &mut GeneratedQuery,
) -> ReturnValueExpr {
    match return_type {
        ReturnType::Expression(expr) => {
            let (_, stmt) = infer_expr_type(ctx, expr, scope, original_query, None, query);
            match stmt.unwrap() {
                GeneratedStatement::Traversal(traversal) => {
                    match &traversal.source_step.inner() {
                        SourceStep::Identifier(v) => {
                            is_valid_identifier(
                                ctx,
                                original_query,
                                expr.loc.clone(),
                                v.inner().as_str(),
                            );

                            // if is single object, need to handle it as a single object
                            // if is array, need to handle it as an array
                            match traversal.should_collect {
                                ShouldCollect::ToVec => {
                                    ReturnValueExpr::Traversal(traversal.clone())
                                }
                                ShouldCollect::ToObj => {
                                    ReturnValueExpr::Traversal(traversal.clone())
                                }
                                _ => {
                                    unreachable!()
                                }
                            }
                        }
                        _ => ReturnValueExpr::Traversal(traversal.clone()),
                    }
                }
                GeneratedStatement::Identifier(id) => {
                    is_valid_identifier(ctx, original_query, expr.loc.clone(), id.inner().as_str());
                    let identifier_end_type = match scope.get(id.inner().as_str()) {
                        Some(t) => t.clone(),
                        None => {
                            generate_error!(
                                ctx,
                                original_query,
                                expr.loc.clone(),
                                E301,
                                id.inner().as_str()
                            );
                            Type::Unknown
                        }
                    };
                    let value =
                        gen_identifier_or_param(original_query, id.inner().as_str(), false, true);

                    match identifier_end_type {
                        Type::Scalar(_) | Type::Boolean => ReturnValueExpr::Identifier(value),
                        Type::Node(_) | Type::Vector(_) | Type::Edge(_) => {
                            ReturnValueExpr::Identifier(value)
                        }
                        _ => ReturnValueExpr::Identifier(value),
                    }
                }
                GeneratedStatement::Literal(l) => {
                    ReturnValueExpr::Value(GeneratedValue::Literal(l.clone()))
                }
                _ => unreachable!(),
            }
        }
        ReturnType::Array(values) => {
            let values = values
                .iter()
                .map(|value| process_return_object(ctx, original_query, scope, value, query))
                .collect::<Vec<ReturnValueExpr>>();
            ReturnValueExpr::Array(values)
        }
        ReturnType::Object(values) => {
            let values = values
                .iter()
                .map(|(key, value)| {
                    (
                        key.clone(),
                        process_return_object(ctx, original_query, scope, value, query),
                    )
                })
                .collect::<HashMap<String, ReturnValueExpr>>();
            ReturnValueExpr::Object(values)
        }
        _ => unreachable!(),
    }
}
