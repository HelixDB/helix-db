//! Semantic analyzer for Helix‑QL.
use crate::helixc::analyzer::error_codes::ErrorCode;
use crate::helixc::analyzer::utils::{
    DEFAULT_VAR_NAME, VariableInfo, is_in_scope, is_param, validate_id_type,
};
use crate::helixc::generator::utils::EmbedData;
use crate::{
    generate_error,
    helixc::{
        analyzer::{
            Ctx,
            errors::push_query_err,
            methods::traversal_validation::validate_traversal,
            types::Type,
            utils::{
                gen_id_access_or_param, gen_identifier_or_param, is_valid_identifier, type_in_scope,
            },
        },
        generator::{
            bool_ops::BoExp,
            queries::Query as GeneratedQuery,
            source_steps::{
                AddE, AddN, AddV, SearchBM25, SearchHybrid as GeneratedSearchHybrid,
                SearchVector as GeneratedSearchVector, SourceStep,
            },
            statements::Statement as GeneratedStatement,
            traversal_steps::{
                ShouldCollect, Step as GeneratedStep, Traversal as GeneratedTraversal,
                TraversalType, Where, WhereRef,
            },
            utils::{GenRef, GeneratedValue, Separator, VecData},
        },
        parser::types::*,
    },
    protocol::date::Date,
};
use paste::paste;
use std::collections::HashMap;

/// Infer the end type of an expression and returns the statement to generate from the expression
///
/// This function is used to infer the end type of an expression and returns the statement to generate from the expression
///
/// # Arguments
///
/// * `ctx` - The context of the query
/// * `expression` - The expression to infer the type of
/// * `scope` - The scope of the query
/// * `original_query` - The original query
/// * `parent_ty` - The parent type of the expression
/// * `gen_query` - The generated query
///
/// # Returns
///
/// * `(Type, Option<GeneratedStatement>)` - The end type of the expression and the statement to generate from the expression
pub(crate) fn infer_expr_type<'a>(
    ctx: &mut Ctx<'a>,
    expression: &'a Expression,
    scope: &mut HashMap<&'a str, VariableInfo>,
    original_query: &'a Query,
    parent_ty: Option<Type>,
    gen_query: &mut GeneratedQuery,
) -> (Type, Option<GeneratedStatement>) {
    use ExpressionType::*;
    let expr: &ExpressionType = &expression.expr;
    match expr {
        Identifier(name) => {
            is_valid_identifier(ctx, original_query, expression.loc.clone(), name.as_str());
            match scope.get(name.as_str()) {
                Some(var_info) => (
                    var_info.ty.clone(),
                    Some(GeneratedStatement::Identifier(GenRef::Std(name.clone()))),
                ),

                None => {
                    generate_error!(
                        ctx,
                        original_query,
                        expression.loc.clone(),
                        E301,
                        name.as_str()
                    );
                    (Type::Unknown, None)
                }
            }
        }

        IntegerLiteral(i) => (
            Type::Scalar(FieldType::I32),
            Some(GeneratedStatement::Literal(GenRef::Literal(i.to_string()))),
        ),
        FloatLiteral(f) => (
            Type::Scalar(FieldType::F64),
            Some(GeneratedStatement::Literal(GenRef::Literal(f.to_string()))),
        ),
        StringLiteral(s) => (
            Type::Scalar(FieldType::String),
            Some(GeneratedStatement::Literal(GenRef::Literal(s.to_string()))),
        ),
        BooleanLiteral(b) => (
            Type::Boolean,
            Some(GeneratedStatement::Literal(GenRef::Literal(b.to_string()))),
        ),
        // Gets expression type for each element in the array
        // Checks if all elements are of the same type
        // Returns the type of the array and the statements to generate from the array
        ArrayLiteral(a) => {
            let mut inner_array_ty = None;
            let result = a.iter().try_fold(Vec::new(), |mut stmts, e| {
                let (ty, stmt) =
                    infer_expr_type(ctx, e, scope, original_query, parent_ty.clone(), gen_query);
                let type_str = ty.kind_str();
                if let Some(inner_array_ty) = &inner_array_ty {
                    if inner_array_ty != &ty {
                        generate_error!(ctx, original_query, e.loc.clone(), E306, type_str);
                    }
                } else {
                    inner_array_ty = Some(ty);
                }
                match stmt {
                    Some(s) => {
                        stmts.push(s);
                        Ok(stmts)
                    }
                    None => {
                        generate_error!(ctx, original_query, e.loc.clone(), E306, type_str);
                        Err(())
                    }
                }
            });
            match result {
                Ok(stmts) => (
                    Type::Array(Box::new(inner_array_ty.unwrap_or(Type::Unknown))),
                    Some(GeneratedStatement::Array(stmts)),
                ),
                Err(()) => (Type::Unknown, Some(GeneratedStatement::Empty)),
            }
        }
        Traversal(tr) => {
            let mut gen_traversal = GeneratedTraversal::default();
            let final_ty = validate_traversal(
                ctx,
                tr,
                scope,
                original_query,
                parent_ty,
                &mut gen_traversal,
                gen_query,
            );
            let stmt = GeneratedStatement::Traversal(gen_traversal);

            if matches!(expr, Exists(_)) {
                (Type::Boolean, Some(stmt))
            } else if let Some(final_ty) = final_ty {
                (final_ty, Some(stmt))
            } else {
                generate_error!(ctx, original_query, tr.loc.clone(), E601, &tr.loc.span);
                (Type::Unknown, None)
            }
        }

        AddNode(add) => {
            if let Some(ref ty) = add.node_type {
                if !ctx.node_set.contains(ty.as_str()) {
                    generate_error!(ctx, original_query, add.loc.clone(), E101, ty.as_str());
                }
                let label = GenRef::Literal(ty.clone());

                let node_in_schema = match ctx.output.nodes.iter().find(|n| n.name == ty.as_str()) {
                    Some(node) => node.clone(),
                    None => {
                        generate_error!(ctx, original_query, add.loc.clone(), E101, ty.as_str());
                        return (Type::Node(None), None);
                    }
                };

                let default_properties = node_in_schema
                    .properties
                    .iter()
                    .filter_map(|p| p.default_value.clone().map(|v| (p.name.clone(), v)))
                    .collect::<Vec<(String, GeneratedValue)>>();

                // Validate fields of add node by traversing the fields
                // checking they exist in the schema, then checking their types
                let (properties, secondary_indices) = match &add.fields {
                    Some(fields_to_add) => {
                        let field_set_from_schema = ctx.node_fields.get(ty.as_str()).cloned();
                        if let Some(field_set) = field_set_from_schema {
                            for (field_name, field_value) in fields_to_add {
                                if !field_set.contains_key(field_name.as_str()) {
                                    generate_error!(
                                        ctx,
                                        original_query,
                                        add.loc.clone(),
                                        E202,
                                        field_name.as_str(),
                                        "node",
                                        ty.as_str()
                                    );
                                }
                                match field_value {
                                    ValueType::Identifier { value, loc } => {
                                        if is_valid_identifier(
                                            ctx,
                                            original_query,
                                            loc.clone(),
                                            value.as_str(),
                                        ) && !scope.contains_key(value.as_str())
                                        {
                                            generate_error!(
                                                ctx,
                                                original_query,
                                                loc.clone(),
                                                E301,
                                                value.as_str()
                                            );
                                        } else {
                                            // Variable is in scope, now validate field type
                                            match scope.get(value.as_str()) {
                                                Some(var_info) => {
                                                    match field_set.get(field_name.as_str()) {
                                                        Some(field) => {
                                                            let variable_type = &var_info.ty;
                                                            if variable_type
                                                                != &Type::from(
                                                                    field.field_type.clone(),
                                                                )
                                                            {
                                                                generate_error!(
                                                                    ctx,
                                                                    original_query,
                                                                    loc.clone(),
                                                                    E205,
                                                                    value.as_str(),
                                                                    &variable_type.to_string(),
                                                                    &field.field_type.to_string(),
                                                                    "node",
                                                                    ty.as_str()
                                                                );
                                                            }
                                                        }
                                                        None => {
                                                            // Field doesn't exist - error already generated above
                                                            // Skip further validation to prevent panic
                                                        }
                                                    }
                                                }
                                                None => {
                                                    // Variable not in scope - error already generated above
                                                }
                                            }
                                        }
                                    }
                                    ValueType::Literal { value, loc } => {
                                        match ctx.node_fields.get(ty.as_str()) {
                                            Some(fields) => match fields.get(field_name.as_str()) {
                                                Some(field) => {
                                                    let field_type = field.field_type.clone();
                                                    if field_type != *value {
                                                        generate_error!(
                                                            ctx,
                                                            original_query,
                                                            loc.clone(),
                                                            E205,
                                                            &value.inner_stringify(),
                                                            value.to_variant_string(),
                                                            &field_type.to_string(),
                                                            "node",
                                                            ty.as_str()
                                                        );
                                                    }
                                                }
                                                None => {
                                                    // Field doesn't exist - error already generated above
                                                }
                                            },
                                            None => {
                                                // Type doesn't exist - error already generated above
                                            }
                                        }
                                    }
                                    _ => {}
                                }
                            }
                        }
                        let mut properties = fields_to_add
                            .iter()
                            .map(|(field_name, value)| {
                                (
                                    field_name.clone(),
                                    match value {
                                        ValueType::Literal { value, loc } => {
                                            match ctx.node_fields.get(ty.as_str()) {
                                                Some(fields) => match fields
                                                    .get(field_name.as_str())
                                                {
                                                    Some(field) => {
                                                        match field.field_type == FieldType::Date {
                                                            true => match Date::new(value) {
                                                                Ok(date) => {
                                                                    GeneratedValue::Literal(
                                                                        GenRef::Literal(
                                                                            date.to_rfc3339(),
                                                                        ),
                                                                    )
                                                                }
                                                                Err(_) => {
                                                                    generate_error!(
                                                                        ctx,
                                                                        original_query,
                                                                        loc.clone(),
                                                                        E501,
                                                                        value.as_str()
                                                                    );
                                                                    GeneratedValue::Unknown
                                                                }
                                                            },
                                                            false => GeneratedValue::Literal(
                                                                GenRef::from(value.clone()),
                                                            ),
                                                        }
                                                    }
                                                    None => {
                                                        // Field doesn't exist - error already generated
                                                        GeneratedValue::Unknown
                                                    }
                                                },
                                                None => {
                                                    // Type doesn't exist - error already generated
                                                    GeneratedValue::Unknown
                                                }
                                            }
                                        }
                                        ValueType::Identifier { value, .. } => {
                                            gen_identifier_or_param(
                                                original_query,
                                                value,
                                                true,
                                                false,
                                            )
                                        }
                                        v => {
                                            generate_error!(
                                                ctx,
                                                original_query,
                                                add.loc.clone(),
                                                E206,
                                                &v.to_string()
                                            );
                                            GeneratedValue::Unknown
                                        }
                                    },
                                )
                            })
                            .collect::<HashMap<String, GeneratedValue>>();

                        for (field_name, default_value) in default_properties {
                            if !properties.contains_key(field_name.as_str()) {
                                properties.insert(field_name, default_value);
                            }
                        }

                        let secondary_indices = {
                            let secondary_indices = node_in_schema
                                .properties
                                .iter()
                                .filter_map(|p| {
                                    p.field_prefix.is_indexed().then_some(p.name.clone())
                                })
                                .collect::<Vec<_>>();
                            match secondary_indices.is_empty() {
                                true => None,
                                false => Some(secondary_indices),
                            }
                        };

                        (properties, secondary_indices)
                    }
                    None => (
                        default_properties.into_iter().fold(
                            HashMap::new(),
                            |mut acc, (field_name, default_value)| {
                                acc.insert(field_name, default_value);
                                acc
                            },
                        ),
                        None,
                    ),
                };

                let add_n = AddN {
                    label,
                    properties: Some(properties.into_iter().collect()),
                    secondary_indices,
                };

                let stmt = GeneratedStatement::Traversal(GeneratedTraversal {
                    source_step: Separator::Period(SourceStep::AddN(add_n)),
                    steps: vec![],
                    traversal_type: TraversalType::Mut,
                    should_collect: ShouldCollect::ToObj,
                    ..Default::default()
                });
                gen_query.is_mut = true;
                return (Type::Node(Some(ty.to_string())), Some(stmt));
            }
            generate_error!(
                ctx,
                original_query,
                add.loc.clone(),
                E304,
                ["node"],
                ["node"]
            );
            (Type::Node(None), None)
        }
        AddEdge(add) => {
            if let Some(ref ty) = add.edge_type {
                if !ctx.edge_map.contains_key(ty.as_str()) {
                    generate_error!(ctx, original_query, add.loc.clone(), E102, ty.as_str());
                }
                let label = GenRef::Literal(ty.clone());

                let edge_in_schema = match ctx.output.edges.iter().find(|e| e.name == ty.as_str()) {
                    Some(edge) => edge.clone(),
                    None => {
                        generate_error!(ctx, original_query, add.loc.clone(), E102, ty.as_str());
                        return (Type::Edge(None), None);
                    }
                };

                let default_properties = edge_in_schema
                    .properties
                    .iter()
                    .filter_map(|p| p.default_value.clone().map(|v| (p.name.clone(), v)))
                    .collect::<Vec<(String, GeneratedValue)>>();

                // Validate fields if both type and fields are present
                let properties = match &add.fields {
                    Some(fields) => {
                        // Get the field set before validation
                        let field_set = ctx.edge_fields.get(ty.as_str()).cloned();
                        if let Some(field_set) = field_set {
                            for (field_name, value) in fields {
                                if !field_set.contains_key(field_name.as_str()) {
                                    generate_error!(
                                        ctx,
                                        original_query,
                                        add.loc.clone(),
                                        E202,
                                        field_name.as_str(),
                                        "edge",
                                        ty.as_str()
                                    );
                                }

                                match value {
                                    ValueType::Identifier { value, loc } => {
                                        if is_valid_identifier(
                                            ctx,
                                            original_query,
                                            loc.clone(),
                                            value.as_str(),
                                        ) && !scope.contains_key(value.as_str())
                                        {
                                            generate_error!(
                                                ctx,
                                                original_query,
                                                loc.clone(),
                                                E301,
                                                value.as_str()
                                            );
                                        } else {
                                            // Variable is in scope, now validate field type
                                            match scope.get(value.as_str()) {
                                                Some(var_info) => {
                                                    match field_set.get(field_name.as_str()) {
                                                        Some(field) => {
                                                            let variable_type = &var_info.ty;
                                                            if variable_type
                                                                != &Type::from(
                                                                    field.field_type.clone(),
                                                                )
                                                            {
                                                                generate_error!(
                                                                    ctx,
                                                                    original_query,
                                                                    loc.clone(),
                                                                    E205,
                                                                    value.as_str(),
                                                                    &variable_type.to_string(),
                                                                    &field.field_type.to_string(),
                                                                    "edge",
                                                                    ty.as_str()
                                                                );
                                                            }
                                                        }
                                                        None => {
                                                            // Field doesn't exist - error already generated above
                                                        }
                                                    }
                                                }
                                                None => {
                                                    // Variable not in scope - error already generated above
                                                }
                                            }
                                        }
                                    }
                                    ValueType::Literal { value, loc } => {
                                        // check against type
                                        match ctx.edge_fields.get(ty.as_str()) {
                                            Some(fields) => match fields.get(field_name.as_str()) {
                                                Some(field) => {
                                                    let field_type = field.field_type.clone();
                                                    if field_type != *value {
                                                        generate_error!(
                                                            ctx,
                                                            original_query,
                                                            loc.clone(),
                                                            E205,
                                                            &value.inner_stringify(),
                                                            value.to_variant_string(),
                                                            &field_type.to_string(),
                                                            "edge",
                                                            ty.as_str()
                                                        );
                                                    }
                                                }
                                                None => {
                                                    // Field doesn't exist - error already generated above
                                                }
                                            },
                                            None => {
                                                // Type doesn't exist - error already generated above
                                            }
                                        }
                                    }
                                    _ => {}
                                }
                            }
                        }
                        let mut properties = fields
                            .iter()
                            .map(|(field_name, value)| {
                                (
                                    field_name.clone(),
                                    match value {
                                        ValueType::Literal { value, loc } => {
                                            match ctx.edge_fields.get(ty.as_str()) {
                                                Some(fields) => match fields
                                                    .get(field_name.as_str())
                                                {
                                                    Some(field) => {
                                                        match field.field_type == FieldType::Date {
                                                            true => match Date::new(value) {
                                                                Ok(date) => {
                                                                    GeneratedValue::Literal(
                                                                        GenRef::Literal(
                                                                            date.to_rfc3339(),
                                                                        ),
                                                                    )
                                                                }
                                                                Err(_) => {
                                                                    generate_error!(
                                                                        ctx,
                                                                        original_query,
                                                                        loc.clone(),
                                                                        E501,
                                                                        value.as_str()
                                                                    );
                                                                    GeneratedValue::Unknown
                                                                }
                                                            },
                                                            false => GeneratedValue::Literal(
                                                                GenRef::from(value.clone()),
                                                            ),
                                                        }
                                                    }
                                                    None => {
                                                        // Field doesn't exist - error already generated
                                                        GeneratedValue::Unknown
                                                    }
                                                },
                                                None => {
                                                    // Type doesn't exist - error already generated
                                                    GeneratedValue::Unknown
                                                }
                                            }
                                        }
                                        ValueType::Identifier { value, loc } => {
                                            is_valid_identifier(
                                                ctx,
                                                original_query,
                                                loc.clone(),
                                                value.as_str(),
                                            );
                                            gen_identifier_or_param(
                                                original_query,
                                                value.as_str(),
                                                false,
                                                true,
                                            )
                                        }
                                        v => {
                                            generate_error!(
                                                ctx,
                                                original_query,
                                                add.loc.clone(),
                                                E206,
                                                &v.to_string()
                                            );
                                            GeneratedValue::Unknown
                                        }
                                    },
                                )
                            })
                            .collect::<HashMap<String, GeneratedValue>>();

                        for (field_name, default_value) in default_properties.clone() {
                            if !properties.contains_key(field_name.as_str()) {
                                properties.insert(field_name, default_value);
                            }
                        }

                        Some(properties.into_iter().collect())
                    }
                    None => match default_properties.is_empty() {
                        true => None,
                        false => Some(default_properties),
                    },
                };

                let (to, to_is_plural) = match &add.connection.to_id {
                    Some(id) => match id {
                        IdType::Identifier { value, loc } => {
                            is_valid_identifier(ctx, original_query, loc.clone(), value.as_str());
                            // Validate that the identifier exists in scope or is a parameter
                            if !scope.contains_key(value.as_str())
                                && is_param(original_query, value.as_str()).is_none()
                            {
                                generate_error!(
                                    ctx,
                                    original_query,
                                    loc.clone(),
                                    E301,
                                    value.as_str()
                                );
                            }
                            // Validate that the identifier is of type ID
                            validate_id_type(
                                ctx,
                                original_query,
                                loc.clone(),
                                scope,
                                value.as_str(),
                            );
                            // Check if this variable is plural
                            let is_plural = scope
                                .get(value.as_str())
                                .map(|var_info| !var_info.is_single)
                                .unwrap_or(false);
                            let gen_value = if is_plural {
                                // For plural variables, just use the variable name without .id()
                                if let Some(param) = is_param(original_query, value.as_str()) {
                                    GeneratedValue::Parameter(match param.is_optional {
                                        true => GenRef::DeRef(format!(
                                            "data.{}.as_ref().ok_or_else(|| GraphError::ParamNotFound(\"{}\"))?",
                                            value, value
                                        )),
                                        false => GenRef::DeRef(format!("data.{}", value)),
                                    })
                                } else {
                                    GeneratedValue::Identifier(GenRef::Std(value.clone()))
                                }
                            } else {
                                gen_id_access_or_param(original_query, value.as_str())
                            };
                            (gen_value, is_plural)
                        }
                        IdType::Literal { value, loc: _ } => (
                            GeneratedValue::Literal(GenRef::Literal(value.clone())),
                            false,
                        ),
                        // Parser guarantees edge to_id is Identifier or Literal
                        _ => unreachable!("parser guarantees edge to_id is Identifier or Literal"),
                    },
                    _ => {
                        generate_error!(ctx, original_query, add.loc.clone(), E611);
                        (GeneratedValue::Unknown, false)
                    }
                };
                let (from, from_is_plural) = match &add.connection.from_id {
                    Some(id) => match id {
                        IdType::Identifier { value, loc } => {
                            is_valid_identifier(ctx, original_query, loc.clone(), value.as_str());
                            // Validate that the identifier exists in scope or is a parameter
                            if !scope.contains_key(value.as_str())
                                && is_param(original_query, value.as_str()).is_none()
                            {
                                generate_error!(
                                    ctx,
                                    original_query,
                                    loc.clone(),
                                    E301,
                                    value.as_str()
                                );
                            }
                            // Validate that the identifier is of type ID
                            validate_id_type(
                                ctx,
                                original_query,
                                loc.clone(),
                                scope,
                                value.as_str(),
                            );
                            // Check if this variable is plural
                            let is_plural = scope
                                .get(value.as_str())
                                .map(|var_info| !var_info.is_single)
                                .unwrap_or(false);
                            let gen_value = if is_plural {
                                // For plural variables, just use the variable name without .id()
                                if let Some(param) = is_param(original_query, value.as_str()) {
                                    GeneratedValue::Parameter(match param.is_optional {
                                        true => GenRef::DeRef(format!(
                                            "data.{}.as_ref().ok_or_else(|| GraphError::ParamNotFound(\"{}\"))?",
                                            value, value
                                        )),
                                        false => GenRef::DeRef(format!("data.{}", value)),
                                    })
                                } else {
                                    GeneratedValue::Identifier(GenRef::Std(value.clone()))
                                }
                            } else {
                                gen_id_access_or_param(original_query, value.as_str())
                            };
                            (gen_value, is_plural)
                        }
                        IdType::Literal { value, loc: _ } => (
                            GeneratedValue::Literal(GenRef::Literal(value.clone())),
                            false,
                        ),
                        // Parser guarantees edge from_id is Identifier or Literal
                        _ => unreachable!("parser guarantees edge from_id is Identifier or Literal"),
                    },
                    _ => {
                        generate_error!(ctx, original_query, add.loc.clone(), E612);
                        (GeneratedValue::Unknown, false)
                    }
                };
                let add_e = AddE {
                    to,
                    from,
                    label,
                    properties,
                    from_is_plural,
                    to_is_plural,
                    is_unique: edge_in_schema.is_unique,
                };
                // If either from or to is plural, use Standalone (no G::new_mut wrapper),
                // Empty separator (no period before it), and No collection (already done in iteration)
                let (final_traversal_result_type, traversal_type, separator, should_collect) =
                    if from_is_plural || to_is_plural {
                        (
                            Type::Edges(Some(ty.to_string())),
                            TraversalType::Standalone,
                            Separator::Empty(SourceStep::AddE(add_e)),
                            ShouldCollect::No,
                        )
                    } else {
                        (
                            Type::Edge(Some(ty.to_string())),
                            TraversalType::Mut,
                            Separator::Period(SourceStep::AddE(add_e)),
                            ShouldCollect::ToObj,
                        )
                    };
                let stmt = GeneratedStatement::Traversal(GeneratedTraversal {
                    source_step: separator,
                    steps: vec![],
                    traversal_type,
                    should_collect,
                    ..Default::default()
                });
                gen_query.is_mut = true;
                return (final_traversal_result_type, Some(stmt));
            }
            generate_error!(
                ctx,
                original_query,
                add.loc.clone(),
                E304,
                ["edge"],
                ["edge"]
            );
            (Type::Edge(None), None)
        }
        AddVector(add) => {
            if let Some(ref ty) = add.vector_type {
                if !ctx.vector_set.contains(ty.as_str()) {
                    generate_error!(ctx, original_query, add.loc.clone(), E103, ty.as_str());
                }
                let label = GenRef::Literal(ty.clone());

                let vector_in_schema = match ctx
                    .output
                    .vectors
                    .iter()
                    .find(|v| v.name == ty.as_str())
                {
                    Some(vector) => vector.clone(),
                    None => {
                        generate_error!(ctx, original_query, add.loc.clone(), E103, ty.as_str());
                        return (Type::Vector(None), None);
                    }
                };

                let default_properties = vector_in_schema
                    .properties
                    .iter()
                    .filter_map(|p| p.default_value.clone().map(|v| (p.name.clone(), v)))
                    .collect::<Vec<(String, GeneratedValue)>>();

                // Validate vector fields
                let properties = match &add.fields {
                    Some(fields) => {
                        let field_set = ctx.vector_fields.get(ty.as_str()).cloned();
                        if let Some(field_set) = field_set {
                            for (field_name, value) in fields {
                                if !field_set.contains_key(field_name.as_str()) {
                                    generate_error!(
                                        ctx,
                                        original_query,
                                        add.loc.clone(),
                                        E202,
                                        field_name.as_str(),
                                        "vector",
                                        ty.as_str()
                                    );
                                }
                                match value {
                                    ValueType::Identifier { value, loc } => {
                                        if is_valid_identifier(
                                            ctx,
                                            original_query,
                                            loc.clone(),
                                            value.as_str(),
                                        ) && !scope.contains_key(value.as_str())
                                        {
                                            generate_error!(
                                                ctx,
                                                original_query,
                                                loc.clone(),
                                                E301,
                                                value.as_str()
                                            );
                                        } else {
                                            // Variable is in scope, now validate field type
                                            match scope.get(value.as_str()) {
                                                Some(var_info) => {
                                                    match field_set.get(field_name.as_str()) {
                                                        Some(field) => {
                                                            let variable_type = &var_info.ty;
                                                            if variable_type
                                                                != &Type::from(
                                                                    field.field_type.clone(),
                                                                )
                                                            {
                                                                generate_error!(
                                                                    ctx,
                                                                    original_query,
                                                                    loc.clone(),
                                                                    E205,
                                                                    value.as_str(),
                                                                    &variable_type.to_string(),
                                                                    &field.field_type.to_string(),
                                                                    "vector",
                                                                    ty.as_str()
                                                                );
                                                            }
                                                        }
                                                        None => {
                                                            // Field doesn't exist - error already generated above
                                                        }
                                                    }
                                                }
                                                None => {
                                                    // Variable not in scope - error already generated above
                                                }
                                            }
                                        }
                                    }
                                    ValueType::Literal { value, loc } => {
                                        // check against type
                                        match ctx.vector_fields.get(ty.as_str()) {
                                            Some(fields) => match fields.get(field_name.as_str()) {
                                                Some(field) => {
                                                    let field_type = field.field_type.clone();
                                                    if field_type != *value {
                                                        generate_error!(
                                                            ctx,
                                                            original_query,
                                                            loc.clone(),
                                                            E205,
                                                            value.as_str(),
                                                            &value.to_variant_string(),
                                                            &field_type.to_string(),
                                                            "vector",
                                                            ty.as_str()
                                                        );
                                                    }
                                                }
                                                None => {
                                                    // Field doesn't exist - error already generated above
                                                }
                                            },
                                            None => {
                                                // Type doesn't exist - error already generated above
                                            }
                                        }
                                    }
                                    _ => {}
                                }
                            }
                        }
                        let mut properties = fields
                            .iter()
                            .map(|(field_name, value)| {
                                (
                                    field_name.clone(),
                                    match value {
                                        ValueType::Literal { value, loc } => {
                                            match ctx.vector_fields.get(ty.as_str()) {
                                                Some(fields) => match fields
                                                    .get(field_name.as_str())
                                                {
                                                    Some(field) => {
                                                        match field.field_type == FieldType::Date {
                                                            true => match Date::new(value) {
                                                                Ok(date) => {
                                                                    GeneratedValue::Literal(
                                                                        GenRef::Literal(
                                                                            date.to_rfc3339(),
                                                                        ),
                                                                    )
                                                                }
                                                                Err(_) => {
                                                                    generate_error!(
                                                                        ctx,
                                                                        original_query,
                                                                        loc.clone(),
                                                                        E501,
                                                                        value.as_str()
                                                                    );
                                                                    GeneratedValue::Unknown
                                                                }
                                                            },
                                                            false => GeneratedValue::Literal(
                                                                GenRef::from(value.clone()),
                                                            ),
                                                        }
                                                    }
                                                    None => {
                                                        // Field doesn't exist - error already generated
                                                        GeneratedValue::Unknown
                                                    }
                                                },
                                                None => {
                                                    // Type doesn't exist - error already generated
                                                    GeneratedValue::Unknown
                                                }
                                            }
                                        }
                                        ValueType::Identifier { value, loc } => {
                                            is_valid_identifier(
                                                ctx,
                                                original_query,
                                                loc.clone(),
                                                value.as_str(),
                                            );
                                            gen_identifier_or_param(
                                                original_query,
                                                value.as_str(),
                                                false,
                                                true,
                                            )
                                        }
                                        v => {
                                            generate_error!(
                                                ctx,
                                                original_query,
                                                add.loc.clone(),
                                                E206,
                                                &v.to_string()
                                            );
                                            GeneratedValue::Unknown
                                        }
                                    },
                                )
                            })
                            .collect::<HashMap<String, GeneratedValue>>();

                        for (field_name, default_value) in default_properties.clone() {
                            if !properties.contains_key(field_name.as_str()) {
                                properties.insert(field_name, default_value);
                            }
                        }

                        properties
                    }
                    None => default_properties.into_iter().fold(
                        HashMap::new(),
                        |mut acc, (field_name, default_value)| {
                            acc.insert(field_name, default_value);
                            acc
                        },
                    ),
                };
                if let Some(vec_data) = &add.data {
                    let vec = match vec_data {
                        VectorData::Vector(v) => {
                            VecData::Standard(GeneratedValue::Literal(GenRef::Ref(format!(
                                "[{}]",
                                v.iter()
                                    .map(|f| f.to_string())
                                    .collect::<Vec<String>>()
                                    .join(",")
                            ))))
                        }
                        VectorData::Identifier(i) => {
                            is_valid_identifier(ctx, original_query, add.loc.clone(), i.as_str());
                            // Check that the identifier is of type [F64]
                            if let Some(var_info) = scope.get(i.as_str()) {
                                let expected_type =
                                    Type::Array(Box::new(Type::Scalar(FieldType::F64)));
                                if var_info.ty != expected_type {
                                    generate_error!(
                                        ctx,
                                        original_query,
                                        add.loc.clone(),
                                        E205,
                                        i.as_str(),
                                        &var_info.ty.to_string(),
                                        "[F64]",
                                        "AddV",
                                        ty.as_str()
                                    );
                                }
                            } else {
                                generate_error!(
                                    ctx,
                                    original_query,
                                    add.loc.clone(),
                                    E301,
                                    i.as_str()
                                );
                            }
                            let id =
                                gen_identifier_or_param(original_query, i.as_str(), true, false);
                            VecData::Standard(id)
                        }
                        VectorData::Embed(e) => {
                            let embed_data = match &e.value {
                                EvaluatesToString::Identifier(i) => {
                                    type_in_scope(
                                        ctx,
                                        original_query,
                                        add.loc.clone(),
                                        scope,
                                        i.as_str(),
                                    );
                                    EmbedData {
                                        data: gen_identifier_or_param(
                                            original_query,
                                            i.as_str(),
                                            true,
                                            false,
                                        ),
                                        model_name: gen_query.embedding_model_to_use.clone(),
                                    }
                                }
                                EvaluatesToString::StringLiteral(s) => EmbedData {
                                    data: GeneratedValue::Literal(GenRef::Ref(s.clone())),
                                    model_name: gen_query.embedding_model_to_use.clone(),
                                },
                            };

                            VecData::Hoisted(gen_query.add_hoisted_embed(embed_data))
                        }
                    };
                    let add_v = AddV {
                        vec,
                        label,
                        properties: Some(properties.into_iter().collect()),
                    };
                    let stmt = GeneratedStatement::Traversal(GeneratedTraversal {
                        source_step: Separator::Period(SourceStep::AddV(add_v)),
                        steps: vec![],
                        traversal_type: TraversalType::Mut,
                        should_collect: ShouldCollect::ToObj,
                        ..Default::default()
                    });
                    gen_query.is_mut = true;
                    return (Type::Vector(Some(ty.to_string())), Some(stmt));
                }
            }
            generate_error!(
                ctx,
                original_query,
                add.loc.clone(),
                E304,
                ["vector"],
                ["vector"]
            );
            (Type::Vector(None), None)
        }

        // BatchAddVector(add) => {
        //     if let Some(ref ty) = add.vector_type {
        //         if !ctx.vector_set.contains(ty.as_str()) {
        //             push_query_err(ctx,
        //                 original_query,
        //                 add.loc.clone(),
        //                 format!("vector type `{}` has not been declared", ty),
        //                 format!("add a `V::{}` schema first", ty),
        //             );
        //         }
        //     }
        //     Type::Vector(add.vector_type.as_deref())
        // }
        SearchVector(sv) => {
            if let Some(ref ty) = sv.vector_type
                && !ctx.vector_set.contains(ty.as_str())
            {
                generate_error!(ctx, original_query, sv.loc.clone(), E103, ty.as_str());
            }
            let vec: VecData = match &sv.data {
                Some(VectorData::Vector(v)) => {
                    VecData::Standard(GeneratedValue::Literal(GenRef::Ref(format!(
                        "[{}]",
                        v.iter()
                            .map(|f| f.to_string())
                            .collect::<Vec<String>>()
                            .join(",")
                    ))))
                }
                Some(VectorData::Identifier(i)) => {
                    is_valid_identifier(ctx, original_query, sv.loc.clone(), i.as_str());
                    // Check that the identifier is of type [F64]
                    if let Some(var_type) =
                        type_in_scope(ctx, original_query, sv.loc.clone(), scope, i.as_str())
                    {
                        let expected_type = Type::Array(Box::new(Type::Scalar(FieldType::F64)));
                        if var_type != expected_type {
                            generate_error!(
                                ctx,
                                original_query,
                                sv.loc.clone(),
                                E205,
                                i.as_str(),
                                &var_type.to_string(),
                                "[F64]",
                                "SearchV",
                                sv.vector_type.as_deref().unwrap_or("unknown")
                            );
                        }
                    }
                    VecData::Standard(gen_identifier_or_param(
                        original_query,
                        i.as_str(),
                        true,
                        false,
                    ))
                }
                Some(VectorData::Embed(e)) => {
                    let embed_data = match &e.value {
                        EvaluatesToString::Identifier(i) => {
                            type_in_scope(ctx, original_query, sv.loc.clone(), scope, i.as_str());
                            EmbedData {
                                data: gen_identifier_or_param(
                                    original_query,
                                    i.as_str(),
                                    true,
                                    false,
                                ),
                                model_name: gen_query.embedding_model_to_use.clone(),
                            }
                        }
                        EvaluatesToString::StringLiteral(s) => EmbedData {
                            data: GeneratedValue::Literal(GenRef::Ref(s.clone())),
                            model_name: gen_query.embedding_model_to_use.clone(),
                        },
                    };

                    VecData::Hoisted(gen_query.add_hoisted_embed(embed_data))
                }
                _ => {
                    generate_error!(
                        ctx,
                        original_query,
                        sv.loc.clone(),
                        E305,
                        ["vector_data", "SearchV"],
                        ["vector_data"]
                    );
                    VecData::Unknown
                }
            };
            let k = match &sv.k {
                Some(k) => match &k.value {
                    EvaluatesToNumberType::I8(i) => {
                        GeneratedValue::Primitive(GenRef::Std(i.to_string()))
                    }
                    EvaluatesToNumberType::I16(i) => {
                        GeneratedValue::Primitive(GenRef::Std(i.to_string()))
                    }
                    EvaluatesToNumberType::I32(i) => {
                        GeneratedValue::Primitive(GenRef::Std(i.to_string()))
                    }
                    EvaluatesToNumberType::I64(i) => {
                        GeneratedValue::Primitive(GenRef::Std(i.to_string()))
                    }

                    EvaluatesToNumberType::U8(i) => {
                        GeneratedValue::Primitive(GenRef::Std(i.to_string()))
                    }
                    EvaluatesToNumberType::U16(i) => {
                        GeneratedValue::Primitive(GenRef::Std(i.to_string()))
                    }
                    EvaluatesToNumberType::U32(i) => {
                        GeneratedValue::Primitive(GenRef::Std(i.to_string()))
                    }
                    EvaluatesToNumberType::U64(i) => {
                        GeneratedValue::Primitive(GenRef::Std(i.to_string()))
                    }
                    EvaluatesToNumberType::U128(i) => {
                        GeneratedValue::Primitive(GenRef::Std(i.to_string()))
                    }
                    EvaluatesToNumberType::Identifier(i) => {
                        is_valid_identifier(ctx, original_query, sv.loc.clone(), i.as_str());
                        type_in_scope(ctx, original_query, sv.loc.clone(), scope, i.as_str());
                        gen_identifier_or_param(original_query, i, false, false)
                    }
                    _ => {
                        generate_error!(
                            ctx,
                            original_query,
                            sv.loc.clone(),
                            E305,
                            ["k", "SearchV"],
                            ["k"]
                        );
                        GeneratedValue::Unknown
                    }
                },
                None => {
                    generate_error!(ctx, original_query, sv.loc.clone(), E601, &sv.loc.span);
                    GeneratedValue::Unknown
                }
            };

            let pre_filter: Option<Vec<BoExp>> = match &sv.pre_filter {
                Some(expr) => {
                    let (_, stmt) = infer_expr_type(
                        ctx,
                        expr,
                        scope,
                        original_query,
                        Some(Type::Vector(sv.vector_type.clone())),
                        gen_query,
                    );
                    // Where/boolean ops don't change the element type,
                    // so `cur_ty` stays the same.
                    if stmt.is_none() {
                        return (Type::Vector(sv.vector_type.clone()), None);
                    }
                    let stmt = stmt.unwrap();
                    let mut gen_traversal = GeneratedTraversal {
                        traversal_type: TraversalType::FromIter(GenRef::Std("v".to_string())),
                        steps: vec![],
                        should_collect: ShouldCollect::ToVec,
                        source_step: Separator::Empty(SourceStep::Anonymous),
                        ..Default::default()
                    };
                    match stmt {
                        GeneratedStatement::Traversal(tr) => {
                            gen_traversal
                                .steps
                                .push(Separator::Period(GeneratedStep::Where(Where::Ref(
                                    WhereRef {
                                        expr: BoExp::Expr(tr),
                                    },
                                ))));
                        }
                        GeneratedStatement::BoExp(expr) => {
                            gen_traversal
                                .steps
                                .push(Separator::Period(GeneratedStep::Where(match expr {
                                    BoExp::Exists(mut traversal) => {
                                        traversal.should_collect = ShouldCollect::No;
                                        Where::Ref(WhereRef {
                                            expr: BoExp::Exists(traversal),
                                        })
                                    }
                                    _ => Where::Ref(WhereRef { expr }),
                                })));
                        }
                        // Pre-filter should produce Traversal or BoExp
                        _ => {
                            // Fall through - pre-filter will be None
                            return (Type::Vector(sv.vector_type.clone()), None);
                        }
                    }
                    Some(vec![BoExp::Expr(gen_traversal)])
                }
                None => None,
            };

            // Search returns nodes that contain the vectors
            (
                Type::Vectors(sv.vector_type.clone()),
                Some(GeneratedStatement::Traversal(GeneratedTraversal {
                    traversal_type: TraversalType::Ref,
                    steps: vec![],
                    should_collect: ShouldCollect::ToVec,
                    source_step: Separator::Period(SourceStep::SearchVector(
                        GeneratedSearchVector {
                            label: GenRef::Literal(sv.vector_type.clone().unwrap_or_default()),
                            vec,
                            k,
                            pre_filter,
                        },
                    )),
                    ..Default::default()
                })),
            )
        }
        And(exprs) => {
            let exprs = exprs
                .iter()
                .map(|expr| {
                    let (ty, stmt) = infer_expr_type(
                        ctx,
                        expr,
                        scope,
                        original_query,
                        parent_ty.clone(),
                        gen_query,
                    );

                    let Some(stmt) = stmt else {
                        return BoExp::Empty;
                    };
                    match stmt {
                        GeneratedStatement::BoExp(expr) => match expr {
                            BoExp::Exists(mut traversal) => {
                                traversal.should_collect = ShouldCollect::No;
                                BoExp::Exists(traversal)
                            }
                            BoExp::Not(inner_expr) => {
                                if let BoExp::Exists(mut traversal) = *inner_expr {
                                    traversal.should_collect = ShouldCollect::No;
                                    BoExp::Exists(traversal)
                                } else {
                                    BoExp::Not(inner_expr)
                                }
                            }
                            _ => expr,
                        },
                        GeneratedStatement::Traversal(tr) => BoExp::Expr(tr),
                        _ => {
                            generate_error!(
                                ctx,
                                original_query,
                                expr.loc.clone(),
                                E306,
                                ty.kind_str()
                            );
                            BoExp::Empty
                        }
                    }
                })
                .collect::<Vec<_>>();
            (
                Type::Boolean,
                Some(GeneratedStatement::BoExp(BoExp::And(exprs))),
            )
        }
        Or(exprs) => {
            let exprs = exprs
                .iter()
                .map(|expr| {
                    let (ty, stmt) = infer_expr_type(
                        ctx,
                        expr,
                        scope,
                        original_query,
                        parent_ty.clone(),
                        gen_query,
                    );

                    let Some(stmt) = stmt else {
                        return BoExp::Empty;
                    };
                    match stmt {
                        GeneratedStatement::BoExp(expr) => match expr {
                            BoExp::Exists(mut traversal) => {
                                traversal.should_collect = ShouldCollect::No;
                                BoExp::Exists(traversal)
                            }
                            BoExp::Not(inner_expr) => {
                                if let BoExp::Exists(mut traversal) = *inner_expr {
                                    traversal.should_collect = ShouldCollect::No;
                                    BoExp::Exists(traversal)
                                } else {
                                    BoExp::Not(inner_expr)
                                }
                            }
                            _ => expr,
                        },
                        GeneratedStatement::Traversal(tr) => BoExp::Expr(tr),
                        _ => {
                            generate_error!(
                                ctx,
                                original_query,
                                expr.loc.clone(),
                                E306,
                                ty.kind_str()
                            );
                            BoExp::Empty
                        }
                    }
                })
                .collect::<Vec<_>>();
            (
                Type::Boolean,
                Some(GeneratedStatement::BoExp(BoExp::Or(exprs))),
            )
        }
        Not(expr) => {
            let (ty, stmt) =
                infer_expr_type(ctx, expr, scope, original_query, parent_ty, gen_query);

            let Some(stmt) = stmt else {
                return (Type::Unknown, None);
            };
            match stmt {
                GeneratedStatement::BoExp(expr) => (
                    Type::Boolean,
                    Some(GeneratedStatement::BoExp(BoExp::Not(Box::new(expr)))),
                ),
                _ => {
                    generate_error!(ctx, original_query, expr.loc.clone(), E306, ty.kind_str());
                    (Type::Unknown, None)
                }
            }
        }
        Exists(expr) => {
            let (_, stmt) =
                infer_expr_type(ctx, &expr.expr, scope, original_query, parent_ty, gen_query);
            if stmt.is_none() {
                return (Type::Boolean, None);
            }
            let traversal = match stmt.unwrap() {
                GeneratedStatement::Traversal(mut tr) => {
                    // Only modify traversal_type if source is Identifier or Anonymous
                    match tr.source_step.inner() {
                        SourceStep::Identifier(id) => {
                            let source_variable = id.inner().clone();
                            // Check if the variable is single or plural to determine traversal type
                            let is_single = scope
                                .get(source_variable.as_str())
                                .map(|var_info| var_info.is_single)
                                .unwrap_or(false);

                            tr.traversal_type = if is_single {
                                TraversalType::FromSingle(GenRef::Std(source_variable))
                            } else {
                                TraversalType::FromIter(GenRef::Std(source_variable))
                            };
                        }
                        SourceStep::Anonymous => {
                            tr.traversal_type = TraversalType::FromSingle(GenRef::Std(
                                DEFAULT_VAR_NAME.to_string(),
                            ));
                        }
                        _ => {
                            // For AddN, AddV, AddE, SearchVector, etc., leave traversal_type unchanged (Ref)
                        }
                    }
                    tr.should_collect = ShouldCollect::No;
                    tr
                }
                // EXISTS expects a traversal expression
                _ => {
                    return (Type::Boolean, None);
                }
            };
            (
                Type::Boolean,
                Some(GeneratedStatement::BoExp(BoExp::Exists(traversal))),
            )
        }
        MathFunctionCall(_math_call) => {
            // Math function calls always return f64
            // TODO: Add proper type inference and validation for math function arguments
            (
                Type::Scalar(FieldType::F64),
                None, // Will be handled by generator
            )
        }
        Empty => (Type::Unknown, Some(GeneratedStatement::Empty)),
        BM25Search(bm25_search) => {
            if let Some(ref ty) = bm25_search.type_arg
                && !ctx.node_set.contains(ty.as_str())
            {
                generate_error!(
                    ctx,
                    original_query,
                    bm25_search.loc.clone(),
                    E101,
                    ty.as_str()
                );
            }
            let vec = match &bm25_search.data {
                Some(ValueType::Literal { value, loc: _ }) => {
                    GeneratedValue::Literal(GenRef::Std(value.inner_stringify()))
                }
                Some(ValueType::Identifier { value: i, loc: _ }) => {
                    is_valid_identifier(ctx, original_query, bm25_search.loc.clone(), i.as_str());

                    if is_in_scope(scope, i.as_str()) {
                        gen_identifier_or_param(original_query, i, true, false)
                    } else {
                        generate_error!(
                            ctx,
                            original_query,
                            bm25_search.loc.clone(),
                            E301,
                            i.as_str()
                        );
                        GeneratedValue::Unknown
                    }
                }
                _ => {
                    generate_error!(
                        ctx,
                        original_query,
                        bm25_search.loc.clone(),
                        E305,
                        ["vector_data", "SearchV"],
                        ["vector_data"]
                    );
                    GeneratedValue::Unknown
                }
            };
            let k = match &bm25_search.k {
                Some(k) => match &k.value {
                    EvaluatesToNumberType::I8(i) => {
                        GeneratedValue::Primitive(GenRef::Std(i.to_string()))
                    }
                    EvaluatesToNumberType::I16(i) => {
                        GeneratedValue::Primitive(GenRef::Std(i.to_string()))
                    }
                    EvaluatesToNumberType::I32(i) => {
                        GeneratedValue::Primitive(GenRef::Std(i.to_string()))
                    }
                    EvaluatesToNumberType::I64(i) => {
                        GeneratedValue::Primitive(GenRef::Std(i.to_string()))
                    }

                    EvaluatesToNumberType::U8(i) => {
                        GeneratedValue::Primitive(GenRef::Std(i.to_string()))
                    }
                    EvaluatesToNumberType::U16(i) => {
                        GeneratedValue::Primitive(GenRef::Std(i.to_string()))
                    }
                    EvaluatesToNumberType::U32(i) => {
                        GeneratedValue::Primitive(GenRef::Std(i.to_string()))
                    }
                    EvaluatesToNumberType::U64(i) => {
                        GeneratedValue::Primitive(GenRef::Std(i.to_string()))
                    }
                    EvaluatesToNumberType::U128(i) => {
                        GeneratedValue::Primitive(GenRef::Std(i.to_string()))
                    }
                    EvaluatesToNumberType::Identifier(i) => {
                        is_valid_identifier(
                            ctx,
                            original_query,
                            bm25_search.loc.clone(),
                            i.as_str(),
                        );
                        type_in_scope(
                            ctx,
                            original_query,
                            bm25_search.loc.clone(),
                            scope,
                            i.as_str(),
                        );
                        gen_identifier_or_param(original_query, i, false, false)
                    }
                    _ => {
                        generate_error!(
                            ctx,
                            original_query,
                            bm25_search.loc.clone(),
                            E305,
                            ["k", "SearchBM25"],
                            ["k"]
                        );
                        GeneratedValue::Unknown
                    }
                },
                None => {
                    generate_error!(
                        ctx,
                        original_query,
                        bm25_search.loc.clone(),
                        E601,
                        &bm25_search.loc.span
                    );
                    GeneratedValue::Unknown
                }
            };

            let search_bm25 = SearchBM25 {
                type_arg: GenRef::Literal(bm25_search.type_arg.clone().unwrap_or_default()),
                query: vec,
                k,
            };
            (
                Type::Nodes(bm25_search.type_arg.clone()),
                Some(GeneratedStatement::Traversal(GeneratedTraversal {
                    traversal_type: TraversalType::Ref,
                    steps: vec![],
                    should_collect: ShouldCollect::ToVec,
                    source_step: Separator::Period(SourceStep::SearchBM25(search_bm25)),
                    ..Default::default()
                })),
            )
        }
        SearchHybrid(sh) => {
            if let Some(ref ty) = sh.label
                && !ctx.vector_set.contains(ty.as_str())
            {
                generate_error!(ctx, original_query, sh.loc.clone(), E103, ty.as_str());
            }

            // Process vector_data (like SearchVector)
            let vec: VecData = match &sh.vector_data {
                Some(VectorData::Vector(v)) => {
                    VecData::Standard(GeneratedValue::Literal(GenRef::Ref(format!(
                        "[{}]",
                        v.iter()
                            .map(|f| f.to_string())
                            .collect::<Vec<String>>()
                            .join(",")
                    ))))
                }
                Some(VectorData::Identifier(i)) => {
                    is_valid_identifier(ctx, original_query, sh.loc.clone(), i.as_str());
                    if let Some(var_type) =
                        type_in_scope(ctx, original_query, sh.loc.clone(), scope, i.as_str())
                    {
                        let expected_type = Type::Array(Box::new(Type::Scalar(FieldType::F64)));
                        if var_type != expected_type {
                            generate_error!(
                                ctx,
                                original_query,
                                sh.loc.clone(),
                                E205,
                                i.as_str(),
                                &var_type.to_string(),
                                "[F64]",
                                "SearchHybrid",
                                sh.label.as_deref().unwrap_or("unknown")
                            );
                        }
                    }
                    VecData::Standard(gen_identifier_or_param(
                        original_query,
                        i.as_str(),
                        true,
                        false,
                    ))
                }
                Some(VectorData::Embed(e)) => {
                    let embed_data = match &e.value {
                        EvaluatesToString::Identifier(i) => {
                            type_in_scope(ctx, original_query, sh.loc.clone(), scope, i.as_str());
                            EmbedData {
                                data: gen_identifier_or_param(
                                    original_query,
                                    i.as_str(),
                                    true,
                                    false,
                                ),
                                model_name: gen_query.embedding_model_to_use.clone(),
                            }
                        }
                        EvaluatesToString::StringLiteral(s) => EmbedData {
                            data: GeneratedValue::Literal(GenRef::Ref(s.clone())),
                            model_name: gen_query.embedding_model_to_use.clone(),
                        },
                        EvaluatesToString::Arguments(args) => {
                            let concatenated = args.join(" ");
                            EmbedData {
                                data: GeneratedValue::Literal(GenRef::Ref(concatenated)),
                                model_name: gen_query.embedding_model_to_use.clone(),
                            }
                        }
                    };
                    VecData::Hoisted(gen_query.add_hoisted_embed(embed_data))
                }
                _ => {
                    generate_error!(
                        ctx,
                        original_query,
                        sh.loc.clone(),
                        E305,
                        ["vector_data", "SearchHybrid"],
                        ["vector_data"]
                    );
                    VecData::Unknown
                }
            };

            // Process query (like BM25Search)
            let query = match &sh.query {
                Some(ValueType::Literal { value, loc: _ }) => {
                    GeneratedValue::Literal(GenRef::Std(value.inner_stringify()))
                }
                Some(ValueType::Identifier { value: i, loc: _ }) => {
                    is_valid_identifier(ctx, original_query, sh.loc.clone(), i.as_str());
                    if is_in_scope(scope, i.as_str()) {
                        gen_identifier_or_param(original_query, i, true, false)
                    } else {
                        generate_error!(ctx, original_query, sh.loc.clone(), E301, i.as_str());
                        GeneratedValue::Unknown
                    }
                }
                _ => {
                    generate_error!(
                        ctx,
                        original_query,
                        sh.loc.clone(),
                        E305,
                        ["query", "SearchHybrid"],
                        ["query"]
                    );
                    GeneratedValue::Unknown
                }
            };

            // Process k
            let k = match &sh.k {
                Some(k) => match &k.value {
                    EvaluatesToNumberType::I8(i) => {
                        GeneratedValue::Primitive(GenRef::Std(i.to_string()))
                    }
                    EvaluatesToNumberType::I16(i) => {
                        GeneratedValue::Primitive(GenRef::Std(i.to_string()))
                    }
                    EvaluatesToNumberType::I32(i) => {
                        GeneratedValue::Primitive(GenRef::Std(i.to_string()))
                    }
                    EvaluatesToNumberType::I64(i) => {
                        GeneratedValue::Primitive(GenRef::Std(i.to_string()))
                    }
                    EvaluatesToNumberType::U8(i) => {
                        GeneratedValue::Primitive(GenRef::Std(i.to_string()))
                    }
                    EvaluatesToNumberType::U16(i) => {
                        GeneratedValue::Primitive(GenRef::Std(i.to_string()))
                    }
                    EvaluatesToNumberType::U32(i) => {
                        GeneratedValue::Primitive(GenRef::Std(i.to_string()))
                    }
                    EvaluatesToNumberType::U64(i) => {
                        GeneratedValue::Primitive(GenRef::Std(i.to_string()))
                    }
                    EvaluatesToNumberType::U128(i) => {
                        GeneratedValue::Primitive(GenRef::Std(i.to_string()))
                    }
                    EvaluatesToNumberType::Identifier(i) => {
                        is_valid_identifier(ctx, original_query, sh.loc.clone(), i.as_str());
                        type_in_scope(ctx, original_query, sh.loc.clone(), scope, i.as_str());
                        gen_identifier_or_param(original_query, i, false, false)
                    }
                    _ => {
                        generate_error!(
                            ctx,
                            original_query,
                            sh.loc.clone(),
                            E305,
                            ["k", "SearchHybrid"],
                            ["k"]
                        );
                        GeneratedValue::Unknown
                    }
                },
                None => {
                    generate_error!(ctx, original_query, sh.loc.clone(), E601, &sh.loc.span);
                    GeneratedValue::Unknown
                }
            };

            let search_hybrid = GeneratedSearchHybrid {
                label: GenRef::Literal(sh.label.clone().unwrap_or_default()),
                vec,
                query,
                k,
            };

            (
                Type::Vectors(sh.label.clone()),
                Some(GeneratedStatement::Traversal(GeneratedTraversal {
                    traversal_type: TraversalType::Ref,
                    steps: vec![],
                    should_collect: ShouldCollect::ToVec,
                    source_step: Separator::Period(SourceStep::SearchHybrid(search_hybrid)),
                    ..Default::default()
                })),
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::helixc::analyzer::error_codes::ErrorCode;
    use crate::helixc::parser::{HelixParser, write_to_temp_file};

    // ============================================================================
    // AddNode Expression Tests
    // ============================================================================

    #[test]
    fn test_add_node_valid() {
        let source = r#"
            N::Person { name: String, age: U32 }

            QUERY test(personName: String, personAge: U32) =>
                person <- AddN<Person>({name: personName, age: personAge})
                RETURN person
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.is_empty());
    }

    #[test]
    fn test_add_node_undeclared_type() {
        let source = r#"
            N::Person { name: String }

            QUERY test() =>
                company <- AddN<Company>({name: "Acme"})
                RETURN company
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.iter().any(|d| d.error_code == ErrorCode::E101));
    }

    // ============================================================================
    // AddEdge Expression Tests
    // ============================================================================

    #[test]
    fn test_add_edge_valid() {
        let source = r#"
            N::Person { name: String }
            E::Knows { From: Person, To: Person }

            QUERY test(id1: ID, id2: ID) =>
                person1 <- N<Person>(id1)
                person2 <- N<Person>(id2)
                edge <- AddE<Knows>::From(person1)::To(person2)
                RETURN edge
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.is_empty());
    }

    #[test]
    fn test_add_edge_with_unique_index_valid() {
        let source = r#"
                N::Person { name: String }
                E::Knows UNIQUE { From: Person, To: Person }

                QUERY test(id1: ID, id2: ID) =>
                    person1 <- N<Person>(id1)
                    person2 <- N<Person>(id2)
                    edge <- AddE<Knows>::From(person1)::To(person2)
                    RETURN edge
            "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.is_empty());
    }

    #[test]
    fn test_add_edge_undeclared_type() {
        let source = r#"
            N::Person { name: String }

            QUERY test(id1: ID, id2: ID) =>
                edge <- AddE<UndeclaredEdge>::From(id1)::To(id2)
                RETURN edge
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.iter().any(|d| d.error_code == ErrorCode::E102));
    }

    // ============================================================================
    // Array Literal Tests
    // ============================================================================

    #[test]
    fn test_array_literal_homogeneous() {
        let source = r#"
            N::Person { name: String }

            QUERY test() =>
                names <- ["Alice", "Bob", "Charlie"]
                RETURN names
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.is_empty());
    }

    #[test]
    fn test_array_literal_mixed_types() {
        let source = r#"
            N::Person { name: String }

            QUERY test() =>
                mixed <- ["string", 123]
                RETURN mixed
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.iter().any(|d| d.error_code == ErrorCode::E306));
    }

    // ============================================================================
    // Identifier Scope Tests
    // ============================================================================

    #[test]
    fn test_identifier_in_scope() {
        let source = r#"
            N::Person { name: String }

            QUERY test() =>
                person <- N<Person>
                samePerson <- person
                RETURN samePerson
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.is_empty());
    }

    #[test]
    fn test_identifier_out_of_scope() {
        let source = r#"
            N::Person { name: String }

            QUERY test() =>
                person <- unknownVariable
                RETURN person
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.iter().any(|d| d.error_code == ErrorCode::E301));
    }

    // ============================================================================
    // Creation with Type Mismatch Tests
    // ============================================================================

    #[test]
    fn test_add_node_type_mismatch() {
        let source = r#"
            N::Person { name: String, age: U32 }

            QUERY test() =>
                person <- AddN<Person>({name: "Alice", age: "not a number"})
                RETURN person
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.iter().any(|d| d.error_code == ErrorCode::E205));
    }

    // ============================================================================
    // Invalid Field Name Tests (E202)
    // ============================================================================

    #[test]
    fn test_add_node_invalid_field_name() {
        let source = r#"
            N::Person { name: String, age: U32 }

            QUERY test() =>
                person <- AddN<Person>({name: "Alice", invalidField: 42})
                RETURN person
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.iter().any(|d| d.error_code == ErrorCode::E202));
    }

    #[test]
    fn test_add_node_invalid_field_with_identifier() {
        let source = r#"
            N::Person { name: String, age: U32 }

            QUERY test(value: U32) =>
                person <- AddN<Person>({name: "Alice", wrongField: value})
                RETURN person
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.iter().any(|d| d.error_code == ErrorCode::E202));
    }

    #[test]
    fn test_add_edge_invalid_field_name() {
        let source = r#"
            N::Person { name: String }
            E::Knows { From: Person, To: Person, Properties: { since: U32 } }

            QUERY test(id1: ID, id2: ID) =>
                person1 <- N<Person>(id1)
                person2 <- N<Person>(id2)
                edge <- AddE<Knows>({since: 2020, badField: 123})::From(person1)::To(person2)
                RETURN edge
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.iter().any(|d| d.error_code == ErrorCode::E202));
    }

    #[test]
    fn test_add_edge_invalid_field_with_identifier() {
        let source = r#"
            N::Person { name: String }
            E::Knows { From: Person, To: Person, Properties: { since: U32 } }

            QUERY test(id1: ID, id2: ID, year: U32) =>
                person1 <- N<Person>(id1)
                person2 <- N<Person>(id2)
                edge <- AddE<Knows>({invalidField: year})::From(person1)::To(person2)
                RETURN edge
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.iter().any(|d| d.error_code == ErrorCode::E202));
    }

    #[test]
    fn test_add_vector_invalid_field_name() {
        let source = r#"
            V::Document { content: String, embedding: [F32] }

            QUERY test(vec: [F32]) =>
                doc <- AddV<Document>(vec, {content: "test", wrongField: "bad"})
                RETURN doc
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.iter().any(|d| d.error_code == ErrorCode::E202));
    }

    #[test]
    fn test_add_vector_invalid_field_with_identifier() {
        let source = r#"
            V::Document { content: String, embedding: [F32] }

            QUERY test(vec: [F32], text: String) =>
                doc <- AddV<Document>(vec, {content: text, badField: "invalid"})
                RETURN doc
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.iter().any(|d| d.error_code == ErrorCode::E202));
    }
}
