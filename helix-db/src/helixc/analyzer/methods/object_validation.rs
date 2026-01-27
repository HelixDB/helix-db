//! Semantic analyzer for Helix‑QL.
use crate::helixc::analyzer::utils::DEFAULT_VAR_NAME;
use crate::helixc::analyzer::{
    error_codes::ErrorCode, errors::push_query_err, utils::get_field_type_from_item_fields,
};
use crate::helixc::generator::source_steps::SourceStep;
use crate::helixc::parser::errors::ParserError;
use crate::{
    generate_error,
    helixc::{
        analyzer::{
            Ctx,
            types::Type,
            utils::{
                gen_property_access, is_valid_identifier,
                validate_field_name_existence_for_item_type,
            },
        },
        generator::{
            return_values::ReturnValueField,
            traversal_steps::{ShouldCollect, Traversal as GeneratedTraversal},
            utils::Separator,
        },
        parser::types::*,
    },
};
use paste::paste;
use std::{borrow::Cow, collections::HashMap};

/// Marks all Out/In steps with EdgeType::Vec in the traversal to fetch vector data
/// This should be called when the 'data' field is accessed on a Vector type
fn mark_vector_steps_for_data_fetch(gen_traversal: &mut GeneratedTraversal) {
    use crate::helixc::generator::traversal_steps::{EdgeType, Step};
    use crate::helixc::generator::utils::Separator;

    match &mut gen_traversal.source_step {
        Separator::Period(step)
        | Separator::Semicolon(step)
        | Separator::Empty(step)
        | Separator::Comma(step)
        | Separator::Newline(step) => match step {
            SourceStep::VFromID(v_from_id) => {
                v_from_id.get_vector_data = true;
            }
            SourceStep::VFromType(v_from_type) => {
                v_from_type.get_vector_data = true;
            }
            _ => {}
        },
    }

    for step_sep in &mut gen_traversal.steps {
        match step_sep {
            Separator::Period(step)
            | Separator::Semicolon(step)
            | Separator::Empty(step)
            | Separator::Comma(step)
            | Separator::Newline(step) => match step {
                Step::Out(out) if matches!(out.edge_type, EdgeType::Vec) => {
                    out.get_vector_data = true;
                }
                Step::In(in_step) if matches!(in_step.edge_type, EdgeType::Vec) => {
                    in_step.get_vector_data = true;
                }
                Step::ToV(to_v) => {
                    to_v.get_vector_data = true;
                }
                Step::FromV(from_v) => {
                    from_v.get_vector_data = true;
                }

                _ => {}
            },
        }
    }
}

/// Validates the object step (e.g. `::{ name }`)
///
/// # Arguments
///
/// * `ctx` - The context of the query
/// * `cur_ty` - The current type of the traversal
/// * `obj` - The object to validate
/// * `original_query` - The original query
/// * `gen_traversal` - The generated traversal
/// * `fields_out` - Output parameter to collect the fields being selected
/// * `scope` - The scope for variable lookups (needed for nested traversals)
/// * `gen_query` - The generated query (needed for nested traversals)
pub(crate) fn validate_object<'a>(
    ctx: &mut Ctx<'a>,
    cur_ty: &Type,
    obj: &'a Object,
    original_query: &'a Query,
    gen_traversal: &mut GeneratedTraversal,
    fields_out: &mut Vec<ReturnValueField>,
    scope: &mut std::collections::HashMap<&'a str, crate::helixc::analyzer::utils::VariableInfo>,
    gen_query: &mut crate::helixc::generator::queries::Query,
) -> Result<Type, ParserError> {
    match &cur_ty {
        Type::Node(Some(node_ty)) | Type::Nodes(Some(node_ty)) => validate_property_access(
            ctx,
            obj,
            original_query,
            gen_traversal,
            cur_ty,
            ctx.node_fields.get(node_ty.as_str()).cloned(),
            fields_out,
            scope,
            gen_query,
        ),
        Type::Edge(Some(edge_ty)) | Type::Edges(Some(edge_ty)) => validate_property_access(
            ctx,
            obj,
            original_query,
            gen_traversal,
            cur_ty,
            ctx.edge_fields.get(edge_ty.as_str()).cloned(),
            fields_out,
            scope,
            gen_query,
        ),
        Type::Vector(Some(vector_ty)) | Type::Vectors(Some(vector_ty)) => validate_property_access(
            ctx,
            obj,
            original_query,
            gen_traversal,
            cur_ty,
            ctx.vector_fields.get(vector_ty.as_str()).cloned(),
            fields_out,
            scope,
            gen_query,
        ),
        Type::Anonymous(ty) => validate_object(
            ctx,
            ty,
            obj,
            original_query,
            gen_traversal,
            fields_out,
            scope,
            gen_query,
        ),
        _ => {
            generate_error!(
                ctx,
                original_query,
                obj.fields[0].value.loc.clone(),
                E203,
                &obj.fields[0].value.loc.span
            );
            Ok(Type::Unknown)
        }
    }
}

/// Extracts the fields from an object selection
/// This is used when the query selects specific fields like N<User>::{id, name, email}
/// Returns true if the 'data' field was selected (for Vector types)
fn extract_fields_from_object<'a>(
    ctx: &mut Ctx<'a>,
    obj: &'a Vec<FieldAddition>,
    original_query: &'a Query,
    parent_ty: &Type,
    fields_out: &mut Vec<ReturnValueField>,
) -> bool {
    let mut data_field_accessed = false;

    for FieldAddition { key, value, .. } in obj {
        match &value.value {
            FieldValueType::Identifier(identifier) => {
                // Check if accessing 'data' field
                if identifier.as_str() == "data" {
                    data_field_accessed = true;
                }

                // Validate the field exists
                is_valid_identifier(ctx, original_query, value.loc.clone(), identifier.as_str());
                validate_field_name_existence_for_item_type(
                    ctx,
                    original_query,
                    value.loc.clone(),
                    parent_ty,
                    identifier.as_str(),
                );

                // Get the field type from the schema
                if let Some(field_type) =
                    get_field_type_from_item_fields(ctx, parent_ty, identifier.as_str())
                {
                    fields_out.push(ReturnValueField::new(
                        key.clone(),
                        format!("{}", field_type),
                    ));
                }
            }
            // For other field value types, we just track that the field was selected
            // The actual value will be computed at runtime
            _ => {
                // For now, we'll just track these as dynamic values
                // The code generator will handle extracting the actual values
                fields_out.push(ReturnValueField::new(key.clone(), "Value".to_string()));
            }
        }
    }

    data_field_accessed
}

/// Validates the property access
///
/// # Arguments
///
/// * `ctx` - The context of the query
/// * `obj` - The object to validate
/// * `original_query` - The original query
/// * `gen_traversal` - The generated traversal
/// * `cur_ty` - The current type of the traversal
/// * `fields` - The fields of the object from schema
/// * `fields_out` - Output parameter to collect selected fields
fn validate_property_access<'a>(
    ctx: &mut Ctx<'a>,
    obj: &'a Object,
    original_query: &'a Query,
    gen_traversal: &mut GeneratedTraversal,
    cur_ty: &Type,
    fields: Option<HashMap<&'a str, Cow<'a, Field>>>,
    fields_out: &mut Vec<ReturnValueField>,
    scope: &mut std::collections::HashMap<&'a str, crate::helixc::analyzer::utils::VariableInfo>,
    gen_query: &mut crate::helixc::generator::queries::Query,
) -> Result<Type, ParserError> {
    match fields {
        Some(_) => {
            // if there is only one field then it is a single property access
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
                        // Check if we're accessing the 'data' field on a Vector type
                        // If so, we need to mark vector traversal steps to fetch the data
                        if lit.as_str() == "data"
                            && matches!(cur_ty, Type::Vector(_) | Type::Vectors(_))
                        {
                            mark_vector_steps_for_data_fetch(gen_traversal);
                        }

                        gen_traversal
                            .steps
                            .push(Separator::Period(gen_property_access(lit.as_str())));

                        // Store the field name so nested traversal code generation can access it
                        gen_traversal.object_fields.push(lit.as_str().to_string());

                        // Mark that this traversal has an object step (for nested struct generation)
                        gen_traversal.has_object_step = true;

                        match cur_ty {
                            Type::Nodes(_) | Type::Edges(_) | Type::Vectors(_) => {
                                gen_traversal.should_collect = ShouldCollect::ToVec;
                            }
                            Type::Node(_) | Type::Edge(_) | Type::Vector(_) => {
                                gen_traversal.should_collect = ShouldCollect::ToObj;
                            }
                            other => {
                                // Property access requires Node, Edge, or Vector type
                                return Err(ParserError::ParseError(format!(
                                    "cannot access property on type '{}'",
                                    other.kind_str()
                                )));
                            }
                        }
                        let field_type = get_field_type_from_item_fields(ctx, cur_ty, lit.as_str());
                        Ok(Type::Scalar(field_type.ok_or(ParserError::ParseError(
                            "field is none".to_string(),
                        ))?))
                    }
                    // This branch is guarded by the outer `if` which checks for Identifier
                    // but add defensive handling in case the match pattern changes
                    other => Err(ParserError::ParseError(format!(
                        "expected identifier in property access, got: {:?}",
                        other
                    ))),
                }
            } else if !obj.fields.is_empty() {
                // Multiple fields selected - extract them for return value generation
                // e.g. N<User>::{id, name, email}
                let data_field_accessed = extract_fields_from_object(
                    ctx,
                    &obj.fields,
                    original_query,
                    cur_ty,
                    fields_out,
                );

                // If accessing 'data' field on Vector type, mark vector steps to fetch data
                if data_field_accessed && matches!(cur_ty, Type::Vector(_) | Type::Vectors(_)) {
                    mark_vector_steps_for_data_fetch(gen_traversal);
                }

                // Populate projection metadata for new struct-based return generation
                gen_traversal.has_object_step = true;
                gen_traversal.has_spread = obj.should_spread;

                // Collect field names and nested traversals
                for field_addition in &obj.fields {
                    match &field_addition.value.value {
                        FieldValueType::Identifier(id) => {
                            // Use the key (output field name), not the id (source property name)
                            gen_traversal.object_fields.push(field_addition.key.clone());
                            // Track the mapping from output name to source property name
                            gen_traversal
                                .field_name_mappings
                                .insert(field_addition.key.clone(), id.clone());
                        }
                        FieldValueType::Traversal(tr) => {
                            // Nested traversal - validate it now to get the type
                            use crate::helixc::analyzer::methods::traversal_validation::validate_traversal;
                            use crate::helixc::generator::traversal_steps::NestedTraversalInfo;
                            use crate::helixc::parser::types::StartNode;

                            // Check if this traversal starts with a closure parameter or anonymous identifier
                            // For example: usr::ID where usr is in scope, or _::In<Created>::ID
                            let (closure_param, closure_source) = match &tr.start {
                                StartNode::Identifier(ident) => {
                                    if let Some(var_info) = scope.get(ident.as_str()) {
                                        // Found a closure parameter - capture its name and the actual source variable
                                        let source_var = var_info
                                            .source_var
                                            .clone()
                                            .unwrap_or_else(|| ident.clone());
                                        (Some(ident.clone()), Some(source_var))
                                    } else {
                                        (None, None)
                                    }
                                }
                                StartNode::Anonymous => (
                                    Some(DEFAULT_VAR_NAME.to_string()),
                                    Some(DEFAULT_VAR_NAME.to_string()),
                                ),
                                _ => (None, None),
                            };

                            // Validate the nested traversal
                            let mut nested_gen_traversal =
                                crate::helixc::generator::traversal_steps::Traversal::default();

                            // Convert plural types to singular for nested traversals
                            // since _/identifier refers to individual items when iterating
                            let item_type = match cur_ty {
                                Type::Nodes(label) => Type::Node(label.clone()),
                                Type::Edges(label) => Type::Edge(label.clone()),
                                Type::Vectors(label) => Type::Vector(label.clone()),
                                _ => cur_ty.clone(),
                            };

                            let nested_type = validate_traversal(
                                ctx,
                                tr.as_ref(),
                                scope,
                                original_query,
                                Some(item_type),
                                &mut nested_gen_traversal,
                                gen_query,
                            );

                            // Check if this nested traversal ends with a Closure step
                            let own_closure_param =
                                tr.steps.last().and_then(|step| match &step.step {
                                    crate::helixc::parser::types::StepType::Closure(cl) => {
                                        Some(cl.identifier.clone())
                                    }
                                    _ => None,
                                });

                            let nested_info = NestedTraversalInfo {
                                traversal: Box::new(nested_gen_traversal),
                                return_type: nested_type.clone(),
                                field_name: field_addition.key.clone(),
                                parsed_traversal: Some(tr.clone()),
                                closure_param_name: closure_param,
                                closure_source_var: closure_source,
                                own_closure_param,
                            };
                            gen_traversal
                                .nested_traversals
                                .insert(field_addition.key.clone(), nested_info);
                            gen_traversal.object_fields.push(field_addition.key.clone());
                        }
                        FieldValueType::Expression(expr) => {
                            // Check if this expression contains a traversal
                            use crate::helixc::analyzer::methods::traversal_validation::validate_traversal;
                            use crate::helixc::generator::traversal_steps::NestedTraversalInfo;
                            use crate::helixc::parser::types::ExpressionType;

                            if let ExpressionType::Traversal(tr) = &expr.expr {
                                // Nested traversal within expression - validate it
                                let mut nested_gen_traversal =
                                    crate::helixc::generator::traversal_steps::Traversal::default();

                                // Convert plural types to singular for nested traversals
                                // since _/identifier refers to individual items when iterating
                                let item_type = match cur_ty {
                                    Type::Nodes(label) => Type::Node(label.clone()),
                                    Type::Edges(label) => Type::Edge(label.clone()),
                                    Type::Vectors(label) => Type::Vector(label.clone()),
                                    _ => cur_ty.clone(),
                                };

                                let nested_type = validate_traversal(
                                    ctx,
                                    tr.as_ref(),
                                    scope,
                                    original_query,
                                    Some(item_type),
                                    &mut nested_gen_traversal,
                                    gen_query,
                                );

                                // Check if this nested traversal ends with a Closure step
                                let own_closure_param =
                                    tr.steps.last().and_then(|step| match &step.step {
                                        crate::helixc::parser::types::StepType::Closure(cl) => {
                                            Some(cl.identifier.clone())
                                        }
                                        _ => None,
                                    });

                                let nested_info = NestedTraversalInfo {
                                    traversal: Box::new(nested_gen_traversal),
                                    return_type: nested_type,
                                    field_name: field_addition.key.clone(),
                                    parsed_traversal: Some(tr.clone()),
                                    closure_param_name: None, // Will be set by closure handling code
                                    closure_source_var: None, // Will be set by closure handling code
                                    own_closure_param,
                                };
                                gen_traversal
                                    .nested_traversals
                                    .insert(field_addition.key.clone(), nested_info);
                                gen_traversal.object_fields.push(field_addition.key.clone());
                            } else {
                                // Other expression types (identifiers, literals, etc.)
                                gen_traversal.object_fields.push(field_addition.key.clone());

                                // If this is an identifier expression, track the mapping
                                // e.g., "post: content" where content is parsed as Expression(Identifier("content"))
                                if let ExpressionType::Identifier(id) = &expr.expr {
                                    gen_traversal
                                        .field_name_mappings
                                        .insert(field_addition.key.clone(), id.clone());
                                }
                            }
                        }
                        _ => {
                            // Other field types (literals, etc.)
                            gen_traversal.object_fields.push(field_addition.key.clone());
                        }
                    }
                }

                // Set collection behavior based on current type
                match cur_ty {
                    Type::Nodes(_) | Type::Edges(_) | Type::Vectors(_) => {
                        gen_traversal.should_collect = ShouldCollect::ToVec;
                    }
                    Type::Node(_) | Type::Edge(_) | Type::Vector(_) => {
                        gen_traversal.should_collect = ShouldCollect::ToObj;
                    }
                    _ => {}
                }

                // Return the current type as we're just selecting fields from it
                Ok(cur_ty.clone())
            } else {
                // error - empty object
                generate_error!(ctx, original_query, obj.fields[0].value.loc.clone(), E645);
                Ok(Type::Unknown)
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
            Ok(Type::Unknown)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::helixc::parser::{HelixParser, write_to_temp_file};

    // ============================================================================
    // Property Access Tests
    // ============================================================================

    #[test]
    fn test_single_property_access() {
        let source = r#"
            N::Person { name: String, age: U32 }

            QUERY test(id: ID) =>
                person <- N<Person>(id)
                name <- person::{name}
                RETURN name
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.is_empty());
    }

    #[test]
    fn test_multiple_property_accesses() {
        let source = r#"
            N::Person { name: String, age: U32, email: String }

            QUERY test(id: ID) =>
                person <- N<Person>(id)
                name <- person::{name}
                age <- person::{age}
                email <- person::{email}
                RETURN name, age, email
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.is_empty());
    }

    #[test]
    fn test_implicit_id_field_access() {
        let source = r#"
            N::Person { name: String }

            QUERY test(id: ID) =>
                person <- N<Person>(id)
                personId <- person::{id}
                RETURN personId
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.is_empty());
    }
}
