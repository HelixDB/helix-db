//! Semantic analyzer for Helix‑QL.
use crate::helixc::analyzer::error_codes::ErrorCode;
use crate::{
    generate_error,
    helixc::{
        analyzer::{
            analyzer::Ctx,
            errors::push_query_err,
            methods::{infer_expr_type::infer_expr_type, traversal_validation::validate_traversal},
            types::Type,
            utils::{
                FieldLookup, Variable, VariableAccess, gen_property_access, is_valid_identifier,
                validate_field_name_existence_for_item_type,
            },
        },
        generator::{
            queries::Query as GeneratedQuery,
            object_remapping_generation::{
                ExistsRemapping, IdentifierRemapping, ObjectRemapping, Remapping, RemappingType,
                TraversalRemapping, ValueRemapping,
            },
            source_steps::SourceStep,
            traversal_steps::{
                ShouldCollect, Step as GeneratedStep, Traversal as GeneratedTraversal,
                TraversalType,
            },
            statements::Statement,
            utils::{GenRef, Separator},
        },
        parser::{helix_parser::*, location::Loc},
    },
};
use paste::paste;
use std::{borrow::Cow, collections::HashMap};

/// Validates the object step (e.g. `::{ name }`)
///
/// # Arguments
///
/// * `ctx` - The context of the query
/// * `cur_ty` - The current type of the traversal
/// * `tr` - The traversal to validate
/// * `obj` - The object to validate
/// * `excluded` - The excluded fields
/// * `original_query` - The original query
/// * `gen_traversal` - The generated traversal
/// * `gen_query` - The generated query
/// * `scope` - The scope of the query
/// * `var_name` - The name of the variable that the property access is on if any
pub(crate) fn validate_object<'a>(
    ctx: &mut Ctx<'a>,
    cur_ty: &Type,
    _tr: &Traversal,
    obj: &'a Object,
    _excluded: &HashMap<&str, Loc>,
    original_query: &'a Query,
    gen_traversal: &mut GeneratedTraversal,
    gen_query: &mut GeneratedQuery,
    scope: &mut HashMap<&'a str, Type>,
    closure_variable: Option<Variable>,
) {
    match &cur_ty {
        Type::Node(Some(node_ty)) | Type::Nodes(Some(node_ty)) => {
            validate_property_access(
                ctx,
                obj,
                original_query,
                gen_query,
                scope,
                closure_variable,
                gen_traversal,
                cur_ty,
                ctx.node_fields.get(node_ty.as_str()).cloned(),
            );
        }
        Type::Edge(Some(edge_ty)) | Type::Edges(Some(edge_ty)) => {
            validate_property_access(
                ctx,
                obj,
                original_query,
                gen_query,
                scope,
                closure_variable,
                gen_traversal,
                cur_ty,
                ctx.edge_fields.get(edge_ty.as_str()).cloned(),
            );
        }
        Type::Vector(Some(vector_ty)) | Type::Vectors(Some(vector_ty)) => {
            validate_property_access(
                ctx,
                obj,
                original_query,
                gen_query,
                scope,
                closure_variable,
                gen_traversal,
                cur_ty,
                ctx.vector_fields.get(vector_ty.as_str()).cloned(),
            );
        }
        Type::Anonymous(ty) => {
            validate_object(
                ctx,
                ty,
                _tr,
                obj,
                _excluded,
                original_query,
                gen_traversal,
                gen_query,
                scope,
                closure_variable,
            );
        }
        _ => {
            generate_error!(
                ctx,
                original_query,
                obj.fields[0].value.loc.clone(),
                E203,
                &obj.fields[0].value.loc.span
            );
        }
    }
}

/// Parses the object remapping
///
/// # Arguments
///
/// * `ctx` - The context of the query
/// * `obj` - The object to parse
/// * `original_query` - The original query
/// * `gen_query` - The generated query
/// * `is_inner` - Whether the remapping is within another remapping
/// * `scope` - The scope of the query
/// * `var_name` - The name of the variable that the property access is on if any
/// * `parent_ty` - The type of the parent of the object remapping
///
/// # Returns
///
/// * `Remapping` - A struct representing the object remapping
pub(crate) fn parse_object_remapping<'a>(
    ctx: &mut Ctx<'a>,
    obj: &'a Vec<FieldAddition>,
    original_query: &'a Query,
    gen_query: &mut GeneratedQuery,
    is_inner: bool,
    scope: &mut HashMap<&'a str, Type>,
    closure_variable: Option<Variable>,
    parent_ty: Type,
    should_spread: bool,
) -> Remapping {
    let mut remappings = Vec::with_capacity(obj.len());

    for FieldAddition { key, value, .. } in obj {
        let remapping: RemappingType = match &value.value {
            // if the field value is a traversal then it is a TraversalRemapping
            FieldValueType::Traversal(traversal) => {
                let mut inner_traversal = GeneratedTraversal::default();
                validate_traversal(
                    ctx,
                    traversal,
                    scope,
                    original_query,
                    Some(parent_ty.clone()),
                    &mut inner_traversal,
                    gen_query,
                );
                match &traversal.start {
                    StartNode::Identifier(name) => {
                        if *name == closure_variable.get_variable_name() {
                            inner_traversal.traversal_type = TraversalType::NestedFrom(
                                GenRef::Std(closure_variable.get_variable_name()),
                            );
                        } else {
                            inner_traversal.traversal_type =
                                TraversalType::FromVar(GenRef::Std(name.to_string()));
                        }
                    }
                    _ => {
                        inner_traversal.traversal_type = TraversalType::NestedFrom(GenRef::Std(
                            closure_variable.get_variable_name(),
                        ));
                    }
                };

                match closure_variable.get_variable_ty() {
                    Type::Node(_) | Type::Edge(_) | Type::Vector(_) => {
                        inner_traversal.should_collect = ShouldCollect::ToVal;
                    }
                    Type::Nodes(_) | Type::Edges(_) | Type::Vectors(_) => {
                        inner_traversal.should_collect = ShouldCollect::ToVec;
                    }
                    _ => unreachable!(),
                }
                match &traversal.steps.last() {
                    Some(step) => match step.step {
                        StepType::Count | StepType::BooleanOperation(_) => {
                            RemappingType::ValueRemapping(ValueRemapping {
                                variable_name: closure_variable.get_variable_name(),
                                field_name: key.clone(),
                                value: GenRef::Std(inner_traversal.to_string()),
                                should_spread,
                            })
                        }
                        _ => RemappingType::TraversalRemapping(TraversalRemapping {
                            variable_name: closure_variable.get_variable_name(),
                            new_field: key.clone(),
                            new_value: inner_traversal,
                            should_spread,
                        }),
                    },
                    None => RemappingType::TraversalRemapping(TraversalRemapping {
                        variable_name: closure_variable.get_variable_name(),
                        new_field: key.clone(),
                        new_value: inner_traversal,
                        should_spread,
                    }),
                }
            }
            FieldValueType::Expression(expr) => match &expr.expr {
                ExpressionType::Traversal(traversal) => {
                    let mut inner_traversal = GeneratedTraversal::default();
                    validate_traversal(
                        ctx,
                        traversal,
                        scope,
                        original_query,
                        Some(parent_ty.clone()),
                        &mut inner_traversal,
                        gen_query,
                    );
                    match &traversal.start {
                        StartNode::Identifier(name) => {
                            if *name == closure_variable.get_variable_name() {
                                inner_traversal.traversal_type = TraversalType::NestedFrom(
                                    GenRef::Std(closure_variable.get_variable_name()),
                                );
                            } else {
                                inner_traversal.traversal_type =
                                    TraversalType::FromVar(GenRef::Std(name.to_string()));
                            }
                        }
                        _ => {
                            inner_traversal.traversal_type = TraversalType::NestedFrom(
                                GenRef::Std(closure_variable.get_variable_name()),
                            );
                        }
                    };
                    match closure_variable.get_variable_ty() {
                        Type::Node(_) | Type::Edge(_) | Type::Vector(_) => {
                            inner_traversal.should_collect = ShouldCollect::ToVal;
                        }
                        Type::Nodes(_) | Type::Edges(_) | Type::Vectors(_) => {
                            inner_traversal.should_collect = ShouldCollect::ToVec;
                        }
                        _ => unreachable!(),
                    }
                    RemappingType::TraversalRemapping(TraversalRemapping {
                        variable_name: closure_variable.get_variable_name(),
                        new_field: key.clone(),
                        new_value: inner_traversal,
                        should_spread,
                    })
                }
                ExpressionType::Exists(expr) => {
                    let (_, stmt) = infer_expr_type(
                        ctx,
                        expr,
                        scope,
                        original_query,
                        Some(parent_ty.clone()),
                        gen_query,
                    );
                    assert!(stmt.is_some());
                    assert!(matches!(stmt, Some(Statement::Traversal(_))));
                    let expr = match stmt.unwrap() {
                        Statement::Traversal(mut tr) => {
                            tr.traversal_type =
                                // TODO: FIX VALUE HERE
                                TraversalType::NestedFrom(GenRef::Std("val".to_string()));
                            tr
                        }
                        _ => unreachable!(),
                    };
                    RemappingType::Exists(ExistsRemapping {
                        variable_name: closure_variable.get_variable_name(),
                        remapping: expr,
                        should_spread,
                    })
                }
                ExpressionType::BooleanLiteral(bo_lit) => {
                    RemappingType::ValueRemapping(ValueRemapping {
                        variable_name: closure_variable.get_variable_name(),
                        field_name: key.clone(),
                        value: GenRef::Literal(bo_lit.to_string()),
                        should_spread,
                    })
                }
                ExpressionType::FloatLiteral(float) => {
                    RemappingType::ValueRemapping(ValueRemapping {
                        variable_name: closure_variable.get_variable_name(),
                        field_name: key.clone(),
                        value: GenRef::Literal(float.to_string()),
                        should_spread,
                    })
                }
                ExpressionType::StringLiteral(string) => {
                    RemappingType::ValueRemapping(ValueRemapping {
                        variable_name: closure_variable.get_variable_name(),
                        field_name: key.clone(),
                        value: GenRef::Literal(string.clone()),
                        should_spread,
                    })
                }
                ExpressionType::IntegerLiteral(integer) => {
                    RemappingType::ValueRemapping(ValueRemapping {
                        variable_name: closure_variable.get_variable_name(),
                        field_name: key.clone(),
                        value: GenRef::Literal(integer.to_string()),
                        should_spread,
                    })
                }
                ExpressionType::Identifier(identifier) => {
                    is_valid_identifier(
                        ctx,
                        original_query,
                        value.loc.clone(),
                        identifier.as_str(),
                    );
                    if scope.contains_key(identifier.as_str()) {
                        RemappingType::IdentifierRemapping(IdentifierRemapping {
                            variable_name: closure_variable.get_variable_name(),
                            field_name: key.clone(),
                            identifier_value: identifier.into(),
                            should_spread,
                        })
                    } else {
                        let (is_valid_field, item_type) = (
                            parent_ty.item_fields_contains_key(ctx, identifier.as_str()),
                            parent_ty.get_type_name(),
                        );
                        match is_valid_field {
                            true => RemappingType::TraversalRemapping(TraversalRemapping {
                                variable_name: closure_variable.get_variable_name(),
                                new_field: key.clone(),
                                new_value: GeneratedTraversal {
                                    traversal_type: TraversalType::NestedFrom(GenRef::Std(
                                        closure_variable.get_variable_name(),
                                    )),
                                    source_step: Separator::Empty(SourceStep::Anonymous),
                                    steps: vec![Separator::Period(gen_property_access(
                                        identifier.as_str(),
                                    ))],
                                    should_collect: ShouldCollect::ToVal,
                                },
                                should_spread,
                            }),
                            false => {
                                generate_error!(
                                    ctx,
                                    original_query,
                                    expr.loc.clone(),
                                    E202,
                                    &identifier,
                                    &parent_ty.kind_str(),
                                    &item_type
                                );
                                RemappingType::Empty
                            }
                        }
                    }
                }
                _ => {
                    generate_error!(
                        ctx,
                        original_query,
                        expr.loc.clone(),
                        E601,
                        &expr.expr.to_string()
                    );
                    RemappingType::Empty
                }
            },
            // if field value is identifier then push field remapping
            FieldValueType::Literal(lit) => {
                RemappingType::ValueRemapping(ValueRemapping {
                    variable_name: closure_variable.get_variable_name(),
                    field_name: key.clone(),
                    value: GenRef::from(lit.clone()), // TODO: Implement
                    should_spread,
                })
            }
            FieldValueType::Identifier(identifier) => {
                is_valid_identifier(ctx, original_query, value.loc.clone(), identifier.as_str());
                if scope.contains_key(identifier.as_str()) {
                    RemappingType::IdentifierRemapping(IdentifierRemapping {
                        variable_name: closure_variable.get_variable_name(),
                        field_name: key.clone(),
                        identifier_value: identifier.into(), // TODO: Implement
                        should_spread,
                    })
                } else {
                    let (is_valid_field, item_type) = match &parent_ty {
                        Type::Nodes(Some(ty)) | Type::Node(Some(ty)) => (
                            ctx.node_fields
                                .get(ty.as_str())
                                .unwrap()
                                .contains_key(identifier.as_str()),
                            ty.as_str(),
                        ),
                        Type::Edges(Some(ty)) | Type::Edge(Some(ty)) => (
                            ctx.edge_fields
                                .get(ty.as_str())
                                .unwrap()
                                .contains_key(identifier.as_str()),
                            ty.as_str(),
                        ),
                        Type::Vectors(Some(ty)) | Type::Vector(Some(ty)) => (
                            ctx.vector_fields
                                .get(ty.as_str())
                                .unwrap()
                                .contains_key(identifier.as_str()),
                            ty.as_str(),
                        ),
                        _ => unreachable!(),
                    };
                    match is_valid_field {
                        true => RemappingType::TraversalRemapping(TraversalRemapping {
                            variable_name: closure_variable.get_variable_name(),
                            new_field: key.clone(),
                            new_value: GeneratedTraversal {
                                traversal_type: TraversalType::NestedFrom(GenRef::Std(
                                    closure_variable.get_variable_name(),
                                )),
                                source_step: Separator::Empty(SourceStep::Anonymous),
                                steps: vec![Separator::Period(GeneratedStep::PropertyFetch(
                                    GenRef::Literal(identifier.to_string()),
                                ))],
                                should_collect: ShouldCollect::ToVec,
                            },
                            should_spread,
                        }),
                        false => {
                            generate_error!(
                                ctx,
                                original_query,
                                value.loc.clone(),
                                E202,
                                &identifier,
                                &parent_ty.kind_str(),
                                &item_type
                            );
                            RemappingType::Empty
                        }
                    }
                }
            }
            // if the field value is another object or closure then recurse (sub mapping would go where traversal would go)
            FieldValueType::Fields(fields) => {
                let remapping = parse_object_remapping(
                    ctx,
                    fields,
                    original_query,
                    gen_query,
                    true,
                    scope,
                    closure_variable.clone(),
                    parent_ty.clone(),
                    should_spread,
                );
                RemappingType::ObjectRemapping(ObjectRemapping {
                    variable_name: closure_variable.get_variable_name(),
                    field_name: key.clone(),
                    remapping,
                })
            } // object or closure
            FieldValueType::Empty => {
                generate_error!(ctx, original_query, obj[0].loc.clone(), E646);
                RemappingType::Empty
            } // err
        };
        // cast to a remapping type
        remappings.push(remapping);
    }

    Remapping {
        variable_name: closure_variable.get_variable_name(),
        is_inner,
        remappings,
        should_spread,
    }
}

/// Validates the property access
///
/// # Arguments
///
/// * `ctx` - The context of the query
/// * `obj` - The object to validate
/// * `original_query` - The original query
/// * `gen_query` - The generated query
/// * `scope` - The scope of the query
/// * `var_name` - The name of the variable that the property access is on if any
/// * `gen_traversal` - The generated traversal
/// * `cur_ty` - The current type of the traversal
/// * `fields` - The fields of the object
fn validate_property_access<'a>(
    ctx: &mut Ctx<'a>,
    obj: &'a Object,
    original_query: &'a Query,
    gen_query: &mut GeneratedQuery,
    scope: &mut HashMap<&'a str, Type>,
    closure_variable: Option<Variable>,
    gen_traversal: &mut GeneratedTraversal,
    cur_ty: &Type,
    fields: Option<HashMap<&'a str, Cow<'a, Field>>>,
) {
    match fields {
        Some(_) => {
            // if there is only one field then it is a property access
            // e.g. N<User>::{name}
            if obj.fields.len() == 1
                && matches!(obj.fields[0].value.value, FieldValueType::Identifier(_))
            {
                match &obj.fields[0].value.value {
                    FieldValueType::Identifier(lit) => {
                        is_valid_identifier(
                            ctx,
                            original_query,
                            obj.fields[0].value.loc.clone(),
                            lit.as_str(),
                        );
                        validate_field_name_existence_for_item_type(
                            ctx,
                            original_query,
                            obj.fields[0].value.loc.clone(),
                            cur_ty,
                            lit.as_str(),
                        );
                        gen_traversal
                            .steps
                            .push(Separator::Period(gen_property_access(lit.as_str())));
                        match cur_ty {
                            Type::Nodes(_) | Type::Edges(_) | Type::Vectors(_) => {
                                gen_traversal.should_collect = ShouldCollect::ToVec;
                            }
                            Type::Node(_) | Type::Edge(_) | Type::Vector(_) => {
                                gen_traversal.should_collect = ShouldCollect::ToVal;
                            }
                            _ => {
                                unreachable!()
                            }
                        }
                    }
                    _ => unreachable!(),
                }
            } else if !obj.fields.is_empty() {
                // if there are multiple fields then it is a field remapping
                // push object remapping where
                let remapping = match closure_variable {
                    Some(_) => parse_object_remapping(
                        ctx,
                        &obj.fields,
                        original_query,
                        gen_query,
                        false,
                        scope,
                        closure_variable,
                        cur_ty.clone(),
                        obj.should_spread,
                    ),
                    None => parse_object_remapping(
                        ctx,
                        &obj.fields,
                        original_query,
                        gen_query,
                        false,
                        scope,
                        Some(Variable::new("item".to_string(), cur_ty.clone())),
                        cur_ty.clone(),
                        obj.should_spread,
                    ),
                };

                gen_traversal
                    .steps
                    .push(Separator::Period(GeneratedStep::Remapping(remapping)));
            } else {
                // error
                generate_error!(ctx, original_query, obj.fields[0].value.loc.clone(), E645);
            }
        }
        None => {
            generate_error!(
                ctx,
                original_query,
                obj.fields[0].value.loc.clone(),
                E201,
                &cur_ty.get_type_name()
            );
        }
    }
}
