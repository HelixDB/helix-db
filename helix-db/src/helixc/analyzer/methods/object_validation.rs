//! Semantic analyzer for Helix‑QL.
use crate::helixc::{
    analyzer::{
        analyzer::Ctx,
        errors::push_query_err,
        methods::{infer_expr_type::infer_expr_type, traversal_validation::validate_traversal},
        types::Type,
        utils::is_valid_identifier,
    },
    generator::{
        generator_types::{Query as GeneratedQuery, Statement},
        object_remapping_generation::{
            ExistsRemapping, IdentifierRemapping, ObjectRemapping, Remapping, RemappingType,
            TraversalRemapping, ValueRemapping,
        },
        source_steps::SourceStep,
        traversal_steps::{
            ShouldCollect, Step as GeneratedStep, Traversal as GeneratedTraversal, TraversalType,
        },
        utils::{GenRef, Separator},
    },
    parser::{helix_parser::*, location::Loc},
};
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
    tr: &Traversal,
    obj: &'a Object,
    excluded: &HashMap<&str, Loc>,
    original_query: &'a Query,
    gen_traversal: &mut GeneratedTraversal,
    gen_query: &mut GeneratedQuery,
    scope: &mut HashMap<&'a str, Type>,
    var_name: Option<&str>,
) {
    match &cur_ty {
        Type::Node(Some(node_ty)) | Type::Nodes(Some(node_ty)) => {
            validate_property_access(
                ctx,
                obj,
                original_query,
                gen_query,
                scope,
                var_name,
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
                var_name,
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
                var_name,
                gen_traversal,
                cur_ty,
                ctx.vector_fields.get(vector_ty.as_str()).cloned(),
            );
        }
        Type::Anonymous(ty) => {
            validate_object(
                ctx,
                ty,
                tr,
                obj,
                excluded,
                original_query,
                gen_traversal,
                gen_query,
                scope,
                var_name,
            );
        }
        _ => {
            push_query_err(
                ctx,
                original_query,
                obj.fields[0].value.loc.clone(),
                "cannot access properties on this type".to_string(),
                "property access is only valid on nodes, edges and vectors",
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
    var_name: &str,
    parent_ty: Type,
) -> Remapping {
    let mut remappings = Vec::with_capacity(obj.len());

    for FieldAddition { key, value, .. } in obj {
        let remapping: RemappingType = match &value.value {
            // if the field value is a traversal then it is a TraversalRemapping
            FieldValueType::Traversal(traversal) => {
                let mut inner_traversal = GeneratedTraversal::default();
                validate_traversal(
                    ctx,
                    &traversal,
                    scope,
                    original_query,
                    Some(parent_ty.clone()),
                    &mut inner_traversal,
                    gen_query,
                );
                match &traversal.start {
                    StartNode::Identifier(name) => {
                        if name.to_string() == var_name {
                            inner_traversal.traversal_type =
                                TraversalType::NestedFrom(GenRef::Std(var_name.to_string()));
                        } else {
                            inner_traversal.traversal_type =
                                TraversalType::FromVar(GenRef::Std(name.to_string()));
                        }
                    }
                    _ => {
                        inner_traversal.traversal_type =
                            TraversalType::NestedFrom(GenRef::Std(var_name.to_string()));
                    }
                };

                match &traversal.steps.last() {
                    Some(step) => match step.step {
                        StepType::Count | StepType::BooleanOperation(_) => {
                            RemappingType::ValueRemapping(ValueRemapping {
                                variable_name: var_name.to_string(),
                                field_name: key.clone(),
                                value: GenRef::Std(inner_traversal.to_string()),
                            })
                        }
                        _ => RemappingType::TraversalRemapping(TraversalRemapping {
                            variable_name: var_name.to_string(),
                            new_field: key.clone(),
                            new_value: inner_traversal,
                        }),
                    },
                    None => RemappingType::TraversalRemapping(TraversalRemapping {
                        variable_name: var_name.to_string(),
                        new_field: key.clone(),
                        new_value: inner_traversal,
                    }),
                }
            }
            FieldValueType::Expression(expr) => match &expr.expr {
                ExpressionType::Traversal(traversal) => {
                    let mut inner_traversal = GeneratedTraversal::default();
                    validate_traversal(
                        ctx,
                        &traversal,
                        scope,
                        original_query,
                        Some(parent_ty.clone()),
                        &mut inner_traversal,
                        gen_query,
                    );
                    match &traversal.start {
                        StartNode::Identifier(name) => {
                            if name.to_string() == var_name {
                                inner_traversal.traversal_type =
                                    TraversalType::NestedFrom(GenRef::Std(var_name.to_string()));
                            } else {
                                inner_traversal.traversal_type =
                                    TraversalType::FromVar(GenRef::Std(name.to_string()));
                            }
                        }
                        _ => {
                            inner_traversal.traversal_type =
                                TraversalType::NestedFrom(GenRef::Std(var_name.to_string()));
                        }
                    };
                    RemappingType::TraversalRemapping(TraversalRemapping {
                        variable_name: var_name.to_string(),
                        new_field: key.clone(),
                        new_value: inner_traversal,
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
                                TraversalType::NestedFrom(GenRef::Std("val".to_string()));
                            tr
                        }
                        _ => unreachable!(),
                    };
                    RemappingType::Exists(ExistsRemapping {
                        variable_name: var_name.to_string(),
                        remapping: expr,
                    })
                }
                ExpressionType::BooleanLiteral(bo_lit) => {
                    RemappingType::ValueRemapping(ValueRemapping {
                        variable_name: var_name.to_string(),
                        field_name: key.clone(),
                        value: GenRef::Literal(bo_lit.to_string()),
                    })
                }
                ExpressionType::FloatLiteral(float) => {
                    RemappingType::ValueRemapping(ValueRemapping {
                        variable_name: var_name.to_string(),
                        field_name: key.clone(),
                        value: GenRef::Literal(float.to_string()),
                    })
                }
                ExpressionType::StringLiteral(string) => {
                    RemappingType::ValueRemapping(ValueRemapping {
                        variable_name: var_name.to_string(),
                        field_name: key.clone(),
                        value: GenRef::Literal(string.clone()),
                    })
                }
                ExpressionType::IntegerLiteral(integer) => {
                    RemappingType::ValueRemapping(ValueRemapping {
                        variable_name: var_name.to_string(),
                        field_name: key.clone(),
                        value: GenRef::Literal(integer.to_string()),
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
                            variable_name: var_name.to_string(),
                            field_name: key.clone(),
                            identifier_value: identifier.into(),
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
                                variable_name: var_name.to_string(),
                                new_field: key.clone(),
                                new_value: GeneratedTraversal {
                                    traversal_type: TraversalType::NestedFrom(GenRef::Std(
                                        var_name.to_string(),
                                    )),
                                    source_step: Separator::Empty(SourceStep::Anonymous),
                                    steps: vec![Separator::Period(GeneratedStep::PropertyFetch(
                                        GenRef::Literal(identifier.to_string()),
                                    ))],
                                    should_collect: ShouldCollect::ToVal,
                                },
                            }),
                            false => {
                                push_query_err(
                                    ctx,
                                    original_query,
                                    expr.loc.clone(),
                                    format!(
                                        "`{}` is not a field of type `{}` or is not a variable in scope",
                                        identifier, item_type
                                    ),
                                    "check the schema field names or declare the variable"
                                        .to_string(),
                                );
                                RemappingType::Empty
                            }
                        }
                    }
                }
                _ => {
                    push_query_err(
                        ctx,
                        original_query,
                        expr.loc.clone(),
                        "invalid expression".to_string(),
                        "invalid expression".to_string(),
                    );
                    RemappingType::Empty
                }
            },
            // if field value is identifier then push field remapping
            FieldValueType::Literal(lit) => {
                RemappingType::ValueRemapping(ValueRemapping {
                    variable_name: var_name.to_string(),
                    field_name: key.clone(),
                    value: GenRef::from(lit.clone()), // TODO: Implement
                })
            }
            FieldValueType::Identifier(identifier) => {
                is_valid_identifier(ctx, original_query, value.loc.clone(), identifier.as_str());
                if scope.contains_key(identifier.as_str()) {
                    RemappingType::IdentifierRemapping(IdentifierRemapping {
                        variable_name: var_name.to_string(),
                        field_name: key.clone(),
                        identifier_value: identifier.into(), // TODO: Implement
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
                            variable_name: var_name.to_string(),
                            new_field: key.clone(),
                            new_value: GeneratedTraversal {
                                traversal_type: TraversalType::NestedFrom(GenRef::Std(
                                    var_name.to_string(),
                                )),
                                source_step: Separator::Empty(SourceStep::Anonymous),
                                steps: vec![Separator::Period(GeneratedStep::PropertyFetch(
                                    GenRef::Literal(identifier.to_string()),
                                ))],
                                should_collect: ShouldCollect::ToVec,
                            },
                        }),
                        false => {
                            push_query_err(
                                ctx,
                                original_query,
                                value.loc.clone(),
                                format!(
                                    "`{}` is not a field of type `{}` or is not a variable in scope",
                                    identifier, item_type
                                ),
                                "check the schema field names or declare the variable".to_string(),
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
                    &fields,
                    original_query,
                    gen_query,
                    true,
                    scope,
                    var_name,
                    parent_ty.clone(),
                );
                RemappingType::ObjectRemapping(ObjectRemapping {
                    variable_name: var_name.to_string(),
                    field_name: key.clone(),
                    remapping,
                })
            } // object or closure
            FieldValueType::Empty => {
                push_query_err(
                    ctx,
                    original_query,
                    obj[0].loc.clone(),
                    "field value is empty".to_string(),
                    "field value must be a literal, identifier, traversal,or object".to_string(),
                );
                RemappingType::Empty
            } // err
        };
        // cast to a remapping type
        remappings.push(remapping);
    }

    Remapping {
        variable_name: var_name.to_string(),
        is_inner,
        remappings,
        should_spread: false,
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
    var_name: Option<&str>,
    gen_traversal: &mut GeneratedTraversal,
    cur_ty: &Type,
    fields: Option<HashMap<&'a str, Cow<'a, Field>>>,
) {
    assert!(fields.is_some());
    if let Some(fields) = fields {
        if let Some(_) = fields.get(cur_ty.get_type_name().as_str()).cloned() {
            // if there is only one field then it is a property access
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
                        gen_traversal
                            .steps
                            .push(Separator::Period(GeneratedStep::PropertyFetch(
                                GenRef::Literal(lit.clone()),
                            )));
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
            } else if obj.fields.len() > 0 {
                // if there are multiple fields then it is a field remapping
                // push object remapping where
                let remapping = match var_name {
                    Some(var_name) => parse_object_remapping(
                        ctx,
                        &obj.fields,
                        original_query,
                        gen_query,
                        false,
                        scope,
                        var_name,
                        cur_ty.clone(),
                    ),
                    None => parse_object_remapping(
                        ctx,
                        &obj.fields,
                        original_query,
                        gen_query,
                        false,
                        scope,
                        "item",
                        cur_ty.clone(),
                    ),
                };

                gen_traversal
                    .steps
                    .push(Separator::Period(GeneratedStep::Remapping(remapping)));
            } else {
                // error
                push_query_err(
                    ctx,
                    original_query,
                    obj.fields[0].value.loc.clone(),
                    "object must have at least one field".to_string(),
                    "object must have at least one field".to_string(),
                );
            }
        }
    }
}
