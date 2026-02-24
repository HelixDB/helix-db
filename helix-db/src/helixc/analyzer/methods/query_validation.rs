//! Semantic analyzer for Helix‑QL.

use crate::generate_error;
use crate::helixc::analyzer::error_codes::ErrorCode;
use crate::helixc::generator::return_values::RustFieldType;
use crate::helixc::generator::utils::{GenRef, RustType};
use crate::helixc::{
    analyzer::{
        Ctx,
        errors::{push_query_err, push_query_warn},
        methods::{infer_expr_type::infer_expr_type, statement_validation::validate_statements},
        types::Type,
        utils::{VariableInfo, is_valid_identifier},
    },
    generator::{
        queries::{Parameter as GeneratedParameter, Query as GeneratedQuery},
        return_values::{
            ReturnFieldInfo, ReturnFieldSource, ReturnFieldType, ReturnValue, ReturnValueStruct,
        },
        source_steps::SourceStep,
        statements::Statement as GeneratedStatement,
        traversal_steps::{
            ComputedExpressionInfo, NestedTraversalInfo, ShouldCollect,
            Traversal as GeneratedTraversal,
        },
    },
    parser::{location::Loc, types::*},
};
use paste::paste;
use std::collections::HashMap;

/// Helper to capitalize first letter of a string
fn capitalize_first(s: &str) -> String {
    let mut chars = s.chars();
    match chars.next() {
        None => String::new(),
        Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
    }
}

struct ProjectionMetadataRef<'a> {
    has_object_step: bool,
    object_fields: &'a [String],
    has_spread: bool,
    excluded_fields: &'a [String],
    field_name_mappings: &'a HashMap<String, String>,
    nested_traversals: &'a HashMap<String, NestedTraversalInfo>,
    computed_expressions: &'a HashMap<String, ComputedExpressionInfo>,
}

fn projection_metadata_from_traversal<'a>(
    traversal: &'a GeneratedTraversal,
) -> ProjectionMetadataRef<'a> {
    ProjectionMetadataRef {
        has_object_step: traversal.has_object_step,
        object_fields: &traversal.object_fields,
        has_spread: traversal.has_spread,
        excluded_fields: &traversal.excluded_fields,
        field_name_mappings: &traversal.field_name_mappings,
        nested_traversals: &traversal.nested_traversals,
        computed_expressions: &traversal.computed_expressions,
    }
}

#[derive(Clone)]
struct ProjectionMetadataOwned {
    has_object_step: bool,
    object_fields: Vec<String>,
    has_spread: bool,
    excluded_fields: Vec<String>,
    field_name_mappings: HashMap<String, String>,
    nested_traversals: HashMap<String, NestedTraversalInfo>,
    computed_expressions: HashMap<String, ComputedExpressionInfo>,
}

impl ProjectionMetadataOwned {
    fn empty() -> Self {
        Self {
            has_object_step: false,
            object_fields: Vec::new(),
            has_spread: false,
            excluded_fields: Vec::new(),
            field_name_mappings: HashMap::new(),
            nested_traversals: HashMap::new(),
            computed_expressions: HashMap::new(),
        }
    }

    fn as_ref(&self) -> ProjectionMetadataRef<'_> {
        ProjectionMetadataRef {
            has_object_step: self.has_object_step,
            object_fields: &self.object_fields,
            has_spread: self.has_spread,
            excluded_fields: &self.excluded_fields,
            field_name_mappings: &self.field_name_mappings,
            nested_traversals: &self.nested_traversals,
            computed_expressions: &self.computed_expressions,
        }
    }
}

impl From<&GeneratedTraversal> for ProjectionMetadataOwned {
    fn from(value: &GeneratedTraversal) -> Self {
        Self {
            has_object_step: value.has_object_step,
            object_fields: value.object_fields.clone(),
            has_spread: value.has_spread,
            excluded_fields: value.excluded_fields.clone(),
            field_name_mappings: value.field_name_mappings.clone(),
            nested_traversals: value.nested_traversals.clone(),
            computed_expressions: value.computed_expressions.clone(),
        }
    }
}

impl From<&VariableInfo> for ProjectionMetadataOwned {
    fn from(value: &VariableInfo) -> Self {
        Self {
            has_object_step: value.has_object_step,
            object_fields: value.object_fields.clone(),
            has_spread: value.has_spread,
            excluded_fields: value.excluded_fields.clone(),
            field_name_mappings: value.field_name_mappings.clone(),
            nested_traversals: value.nested_traversals.clone(),
            computed_expressions: value.computed_expressions.clone(),
        }
    }
}

struct ReturnIr {
    field_name: String,
    inferred_type: Type,
    should_collect: ShouldCollect,
    source_variable: String,
    projection: ProjectionMetadataOwned,
    is_reused_variable: bool,
    is_collection: bool,
    closure_param_name: Option<String>,
    aggregate_properties: Vec<String>,
    is_count_aggregate: bool,
    legacy_literal_value: Option<GenRef<String>>,
}

fn return_ir_from_traversal(
    field_name: String,
    inferred_type: &Type,
    traversal: &GeneratedTraversal,
    legacy_literal_value: Option<GenRef<String>>,
) -> ReturnIr {
    let (aggregate_properties, is_count_aggregate) = match inferred_type {
        Type::Aggregate(info) => (info.properties.clone(), info.is_count),
        _ => (Vec::new(), false),
    };

    ReturnIr {
        source_variable: field_name.clone(),
        field_name,
        inferred_type: inferred_type.clone(),
        should_collect: traversal.should_collect.clone(),
        projection: ProjectionMetadataOwned::from(traversal),
        is_reused_variable: traversal.is_reused_variable,
        is_collection: matches!(
            inferred_type,
            Type::Nodes(_) | Type::Edges(_) | Type::Vectors(_)
        ),
        closure_param_name: traversal.closure_param_name.clone(),
        aggregate_properties,
        is_count_aggregate,
        legacy_literal_value,
    }
}

fn return_ir_from_identifier(
    field_name: String,
    inferred_type: Type,
    variable_info: Option<&VariableInfo>,
) -> ReturnIr {
    ReturnIr {
        source_variable: field_name.clone(),
        field_name,
        should_collect: ShouldCollect::No,
        inferred_type,
        projection: variable_info
            .map(ProjectionMetadataOwned::from)
            .unwrap_or_else(ProjectionMetadataOwned::empty),
        is_reused_variable: variable_info.is_some_and(|v| v.reference_count > 1),
        is_collection: variable_info.is_some_and(|v| !v.is_single),
        closure_param_name: None,
        aggregate_properties: Vec::new(),
        is_count_aggregate: false,
        legacy_literal_value: None,
    }
}

fn emit_return_from_ir(ctx: &Ctx, query: &mut GeneratedQuery, return_ir: ReturnIr) {
    let (rust_type, fields) = type_to_rust_string_and_fields(
        &return_ir.inferred_type,
        &return_ir.should_collect,
        ctx,
        &return_ir.field_name,
    );

    query.return_values.push((
        return_ir.field_name.clone(),
        ReturnValue {
            name: rust_type,
            fields,
            literal_value: return_ir.legacy_literal_value.clone(),
        },
    ));

    if matches!(
        return_ir.inferred_type,
        Type::Boolean | Type::Scalar(_) | Type::Count
    ) {
        let mut prim_struct = ReturnValueStruct::new(return_ir.field_name.clone());
        prim_struct.source_variable = return_ir.source_variable;
        prim_struct.is_primitive = true;
        prim_struct.primitive_literal_value = return_ir.legacy_literal_value;
        query.return_structs.push(prim_struct);
        return;
    }

    let struct_name_prefix = format!(
        "{}{}",
        capitalize_first(&query.name),
        capitalize_first(&return_ir.field_name)
    );
    let projection = return_ir.projection.as_ref();
    let return_fields = build_return_fields(
        ctx,
        &return_ir.inferred_type,
        &projection,
        &struct_name_prefix,
    );
    let struct_name = format!("{}ReturnType", struct_name_prefix);

    let (is_aggregate, is_group_by) = match &return_ir.inferred_type {
        Type::Aggregate(info) => (true, info.is_group_by),
        _ => (false, false),
    };

    query
        .return_structs
        .push(ReturnValueStruct::from_return_fields(
            struct_name,
            return_fields,
            return_ir.source_variable,
            return_ir.is_collection,
            return_ir.is_reused_variable,
            is_aggregate,
            is_group_by,
            return_ir.aggregate_properties,
            return_ir.is_count_aggregate,
            return_ir.closure_param_name,
        ));
}

fn scalar_property_literal_from_traversal(
    inferred_type: &Type,
    traversal: &GeneratedTraversal,
    field_name: &str,
) -> Option<GenRef<String>> {
    if !matches!(inferred_type, Type::Scalar(_)) || traversal.object_fields.is_empty() {
        return None;
    }

    let property_name = &traversal.object_fields[0];
    match traversal.should_collect {
        ShouldCollect::ToObj => {
            if property_name == "id" {
                Some(GenRef::Std(format!(
                    "uuid_str({}.id(), &arena)",
                    field_name
                )))
            } else if property_name == "label" {
                Some(GenRef::Std(format!("{}.label()", field_name)))
            } else {
                Some(GenRef::Std(format!(
                    "{}.get_property(\"{}\")",
                    field_name, property_name
                )))
            }
        }
        ShouldCollect::ToVec => {
            let iter_code = if property_name == "id" {
                format!(
                    "{}.iter().map(|item| uuid_str(item.id(), &arena)).collect::<Vec<_>>()",
                    field_name
                )
            } else if property_name == "label" {
                format!(
                    "{}.iter().map(|item| item.label()).collect::<Vec<_>>()",
                    field_name
                )
            } else {
                format!(
                    "{}.iter().map(|item| item.get_property(\"{}\")).collect::<Vec<_>>()",
                    field_name, property_name
                )
            };
            Some(GenRef::Std(iter_code))
        }
        _ => None,
    }
}

fn emit_literal_return(query: &mut GeneratedQuery, field_name: String, literal: GenRef<String>) {
    query.return_values.push((
        field_name.clone(),
        ReturnValue {
            name: "Value".to_string(),
            fields: vec![],
            literal_value: Some(literal.clone()),
        },
    ));

    let mut prim_struct = ReturnValueStruct::new(field_name.clone());
    prim_struct.source_variable = field_name;
    prim_struct.is_primitive = true;
    prim_struct.primitive_literal_value = Some(literal);
    query.return_structs.push(prim_struct);
}

/// Build unified field list for return types
/// This handles all cases: simple schema, projections, spread, nested traversals
fn build_return_fields(
    ctx: &Ctx,
    inferred_type: &Type,
    projection: &ProjectionMetadataRef<'_>,
    struct_name_prefix: &str,
) -> Vec<ReturnFieldInfo> {
    let mut fields = Vec::new();

    // Handle aggregate types specially
    if let Type::Aggregate(info) = inferred_type {
        // All aggregates have a key field (the grouping key from HashMap)
        fields.push(ReturnFieldInfo::new_implicit(
            "key".to_string(),
            RustFieldType::Primitive(GenRef::Std(RustType::String)),
        ));

        // Add fields for each grouped property
        // We need to get the source type's schema to determine property types
        let (_schema_fields, _item_type) = match info.source_type.as_ref() {
            Type::Node(Some(label)) | Type::Nodes(Some(label)) => {
                (ctx.node_fields.get(label.as_str()), "node")
            }
            Type::Edge(Some(label)) | Type::Edges(Some(label)) => {
                (ctx.edge_fields.get(label.as_str()), "edge")
            }
            Type::Vector(Some(label)) | Type::Vectors(Some(label)) => {
                (ctx.vector_fields.get(label.as_str()), "vector")
            }
            _ => (None, "unknown"),
        };

        // Add each grouped property as a field
        for prop_name in &info.properties {
            fields.push(ReturnFieldInfo::new_schema(
                prop_name.clone(),
                RustFieldType::OptionValue,
            ));
        }

        // Add count field
        fields.push(ReturnFieldInfo::new_implicit(
            "count".to_string(),
            RustFieldType::Primitive(GenRef::Std(RustType::I32)),
        ));

        // For non-COUNT aggregates, add items field with nested struct
        if !info.is_count {
            // Build nested struct for the items
            let items_struct_name = format!("{}Items", struct_name_prefix);
            // Recursively build fields for the source type
            let item_fields = build_return_fields(
                ctx,
                info.source_type.as_ref(),
                projection,
                &items_struct_name,
            );

            // Create field with proper nested_struct_name to avoid conflicts
            fields.push(ReturnFieldInfo {
                name: "items".to_string(),
                field_type: ReturnFieldType::Nested(item_fields),
                source: ReturnFieldSource::NestedTraversal {
                    traversal_expr: String::new(),
                    traversal_code: None,
                    nested_struct_name: Some(format!("{}ReturnType", items_struct_name)),
                    traversal_type: None,
                    closure_param_name: None,
                    closure_source_var: None,
                    accessed_field_name: None,
                    own_closure_param: None,
                    requires_full_traversal: false,
                    is_first: false,
                },
            });
        }

        return fields;
    }

    // Get schema type name if this is a schema type
    let schema_type = match inferred_type {
        Type::Node(Some(label)) | Type::Nodes(Some(label)) => Some((label.as_str(), "node")),
        Type::Edge(Some(label)) | Type::Edges(Some(label)) => Some((label.as_str(), "edge")),
        Type::Vector(Some(label)) | Type::Vectors(Some(label)) => Some((label.as_str(), "vector")),
        _ => None,
    };

    // Step 1: Add implicit fields if this is a schema type
    if let Some((label, item_type)) = schema_type {
        // Helper to find which output field name maps to a given property
        // e.g., for property "id", might return Some("file_id") if there's a mapping file_id -> ID
        let find_output_for_property = |property: &str| -> Option<String> {
            // First check if any object_field maps to this property via field_name_mappings
            for output_name in projection.object_fields {
                if let Some(prop) = projection.field_name_mappings.get(output_name)
                    && prop.to_lowercase() == property.to_lowercase()
                {
                    return Some(output_name.clone());
                }
                // Also check if the output_name itself matches (identity mapping)
                if output_name.to_lowercase() == property.to_lowercase() {
                    return Some(output_name.clone());
                }
            }
            None
        };

        // If has_object_step, only add implicit fields if they're explicitly selected OR has_spread
        // Otherwise, add all implicit fields (default behavior)
        let should_add_field = |field_name: &str| {
            // Exclude if field is in excluded_fields
            if projection.excluded_fields.contains(&field_name.to_string()) {
                return false;
            }
            // If has object step, only include if explicitly selected (possibly with remapping) OR has_spread
            if projection.has_object_step {
                find_output_for_property(field_name).is_some() || projection.has_spread
            } else {
                true
            }
        };

        // Add id and label if no object step OR if explicitly selected
        if should_add_field("id") {
            // Check if id is remapped to a different output name
            if let Some(output_name) = find_output_for_property("id") {
                if output_name != "id" {
                    // Remapped: e.g., file_id: ID
                    fields.push(ReturnFieldInfo::new_implicit_with_property(
                        output_name,
                        "id".to_string(),
                        RustFieldType::Primitive(GenRef::RefLT("a", RustType::Str)),
                    ));
                } else {
                    fields.push(ReturnFieldInfo::new_implicit(
                        "id".to_string(),
                        RustFieldType::Primitive(GenRef::RefLT("a", RustType::Str)),
                    ));
                }
            } else if !projection.has_object_step || projection.has_spread {
                // No object step or has spread means return all fields
                fields.push(ReturnFieldInfo::new_implicit(
                    "id".to_string(),
                    RustFieldType::Primitive(GenRef::RefLT("a", RustType::Str)),
                ));
            }
        }
        if should_add_field("label") {
            if let Some(output_name) = find_output_for_property("label") {
                if output_name != "label" {
                    fields.push(ReturnFieldInfo::new_implicit_with_property(
                        output_name,
                        "label".to_string(),
                        RustFieldType::Primitive(GenRef::RefLT("a", RustType::Str)),
                    ));
                } else {
                    fields.push(ReturnFieldInfo::new_implicit(
                        "label".to_string(),
                        RustFieldType::Primitive(GenRef::RefLT("a", RustType::Str)),
                    ));
                }
            } else if !projection.has_object_step || projection.has_spread {
                fields.push(ReturnFieldInfo::new_implicit(
                    "label".to_string(),
                    RustFieldType::Primitive(GenRef::RefLT("a", RustType::Str)),
                ));
            }
        }

        // Add type-specific implicit fields
        if item_type == "edge" {
            if should_add_field("from_node") {
                if let Some(output_name) = find_output_for_property("from_node") {
                    if output_name != "from_node" {
                        fields.push(ReturnFieldInfo::new_implicit_with_property(
                            output_name,
                            "from_node".to_string(),
                            RustFieldType::Primitive(GenRef::RefLT("a", RustType::Str)),
                        ));
                    } else {
                        fields.push(ReturnFieldInfo::new_implicit(
                            "from_node".to_string(),
                            RustFieldType::Primitive(GenRef::RefLT("a", RustType::Str)),
                        ));
                    }
                } else if !projection.has_object_step || projection.has_spread {
                    fields.push(ReturnFieldInfo::new_implicit(
                        "from_node".to_string(),
                        RustFieldType::Primitive(GenRef::RefLT("a", RustType::Str)),
                    ));
                }
            }
            if should_add_field("to_node") {
                if let Some(output_name) = find_output_for_property("to_node") {
                    if output_name != "to_node" {
                        fields.push(ReturnFieldInfo::new_implicit_with_property(
                            output_name,
                            "to_node".to_string(),
                            RustFieldType::Primitive(GenRef::RefLT("a", RustType::Str)),
                        ));
                    } else {
                        fields.push(ReturnFieldInfo::new_implicit(
                            "to_node".to_string(),
                            RustFieldType::Primitive(GenRef::RefLT("a", RustType::Str)),
                        ));
                    }
                } else if !projection.has_object_step || projection.has_spread {
                    fields.push(ReturnFieldInfo::new_implicit(
                        "to_node".to_string(),
                        RustFieldType::Primitive(GenRef::RefLT("a", RustType::Str)),
                    ));
                }
            }
        } else if item_type == "vector" {
            if should_add_field("data") {
                if let Some(output_name) = find_output_for_property("data") {
                    if output_name != "data" {
                        fields.push(ReturnFieldInfo::new_implicit_with_property(
                            output_name,
                            "data".to_string(),
                            RustFieldType::RefArray(RustType::F64),
                        ));
                    } else {
                        fields.push(ReturnFieldInfo::new_implicit(
                            "data".to_string(),
                            RustFieldType::RefArray(RustType::F64),
                        ));
                    }
                } else if !projection.has_object_step || projection.has_spread {
                    fields.push(ReturnFieldInfo::new_implicit(
                        "data".to_string(),
                        RustFieldType::RefArray(RustType::F64),
                    ));
                }
            }
            if should_add_field("score") {
                if let Some(output_name) = find_output_for_property("score") {
                    if output_name != "score" {
                        fields.push(ReturnFieldInfo::new_implicit_with_property(
                            output_name,
                            "score".to_string(),
                            RustFieldType::Primitive(GenRef::Std(RustType::F64)),
                        ));
                    } else {
                        fields.push(ReturnFieldInfo::new_implicit(
                            "score".to_string(),
                            RustFieldType::Primitive(GenRef::Std(RustType::F64)),
                        ));
                    }
                } else if !projection.has_object_step || projection.has_spread {
                    fields.push(ReturnFieldInfo::new_implicit(
                        "score".to_string(),
                        RustFieldType::Primitive(GenRef::Std(RustType::F64)),
                    ));
                }
            }
        }

        // Step 2: Add schema fields based on projection mode
        let schema_fields = match item_type {
            "node" => ctx.node_fields.get(label),
            "edge" => ctx.edge_fields.get(label),
            "vector" => ctx.vector_fields.get(label),
            _ => None,
        };

        // Helper to check if a property is an implicit field
        let is_implicit_field = |prop: &str| -> bool {
            let lower = prop.to_lowercase();
            matches!(
                lower.as_str(),
                "id" | "label" | "from_node" | "to_node" | "data" | "score"
            )
        };

        if let Some(schema_fields) = schema_fields {
            if projection.has_object_step {
                // Projection mode - only include selected fields
                for field_name in projection.object_fields {
                    // Skip if it's a nested traversal (handled separately)
                    if projection.nested_traversals.contains_key(field_name) {
                        continue;
                    }

                    // Skip if it's a computed expression (handled separately)
                    if projection.computed_expressions.contains_key(field_name) {
                        continue;
                    }

                    // Look up the actual property name from the mapping
                    let property_name = projection
                        .field_name_mappings
                        .get(field_name)
                        .unwrap_or(field_name);

                    // Skip implicit fields (already handled above)
                    if is_implicit_field(property_name) {
                        continue;
                    }

                    if let Some(_field) = schema_fields.get(property_name.as_str()) {
                        // If property_name != field_name, we need to track the mapping
                        if property_name != field_name {
                            fields.push(ReturnFieldInfo::new_schema_with_property(
                                field_name.clone(),    // output field name ("post")
                                property_name.clone(), // source property name ("content")
                                RustFieldType::OptionValue,
                            ));
                        } else {
                            fields.push(ReturnFieldInfo::new_schema(
                                field_name.clone(),
                                RustFieldType::OptionValue,
                            ));
                        }
                    }
                }

                // If has_spread, add all remaining schema fields
                if projection.has_spread {
                    for (field_name, _field) in schema_fields.iter() {
                        // Skip if output name already exists
                        let already_exists = fields.iter().any(|f| f.name == *field_name);
                        // Skip if this source property was remapped to a different output name
                        let already_remapped = projection
                            .field_name_mappings
                            .values()
                            .any(|source_prop| source_prop == field_name);
                        let already_covered_by_nested =
                            projection.nested_traversals.values().any(|info| {
                                info.traversal
                                    .object_fields
                                    .iter()
                                    .any(|f| f.to_lowercase() == field_name.to_lowercase())
                            });
                        if already_exists || already_remapped || already_covered_by_nested {
                            continue;
                        }
                        // Skip if excluded
                        if projection.excluded_fields.contains(&field_name.to_string()) {
                            continue;
                        }

                        // Check if this is an implicit field - if so, use the correct type
                        let is_implicit_field = matches!(
                            *field_name,
                            "id" | "label" | "from_node" | "to_node" | "data" | "score"
                        );

                        if is_implicit_field {
                            let rust_type = match *field_name {
                                "data" => RustFieldType::RefArray(RustType::F64),
                                "score" => RustFieldType::Primitive(GenRef::Std(RustType::F64)),
                                _ => RustFieldType::Primitive(GenRef::RefLT("a", RustType::Str)),
                            };
                            fields.push(ReturnFieldInfo::new_implicit(
                                field_name.to_string(),
                                rust_type,
                            ));
                        } else {
                            fields.push(ReturnFieldInfo::new_schema(
                                field_name.to_string(),
                                RustFieldType::OptionValue,
                            ));
                        }
                    }
                }
            } else {
                // No projection - include all schema fields except excluded ones
                for (field_name, _field) in schema_fields.iter() {
                    // Skip implicit fields (already added)
                    if *field_name == "id"
                        || *field_name == "label"
                        || *field_name == "from_node"
                        || *field_name == "to_node"
                        || *field_name == "data"
                        || *field_name == "score"
                    {
                        continue;
                    }
                    // Skip if excluded
                    if projection.excluded_fields.contains(&field_name.to_string()) {
                        continue;
                    }
                    fields.push(ReturnFieldInfo::new_schema(
                        field_name.to_string(),
                        RustFieldType::OptionValue,
                    ));
                }
            }
        }
    }

    // Step 3: Add nested traversals
    for (field_name, nested_info) in projection.nested_traversals {
        // For nested traversals, extract the return type and build nested fields
        if let Some(ref return_type) = nested_info.return_type {
            // Check if this is a scalar type or needs a struct
            match return_type {
                Type::Count => {
                    // Check if this is a variable reference (e.g., `count: count_var`)
                    // Variable references have closure_source_var set but no graph steps and no object step
                    let is_variable_reference = nested_info.closure_source_var.is_some()
                        && !nested_info.traversal.has_graph_steps()
                        && !nested_info.traversal.has_object_step;

                    let trav_code = if is_variable_reference {
                        String::new()
                    } else {
                        nested_info.traversal.format_steps_only()
                    };
                    let accessed_field_name = nested_info.traversal.object_fields.first().cloned();
                    fields.push(ReturnFieldInfo {
                        name: field_name.clone(),
                        field_type: ReturnFieldType::Simple(RustFieldType::Value),
                        source: ReturnFieldSource::NestedTraversal {
                            traversal_expr: format!("nested_traversal_{}", field_name),
                            traversal_code: Some(trav_code),
                            nested_struct_name: None,
                            traversal_type: Some(nested_info.traversal.traversal_type.clone()),
                            closure_param_name: nested_info.closure_param_name.clone(),
                            closure_source_var: nested_info.closure_source_var.clone(),
                            accessed_field_name,
                            own_closure_param: nested_info.own_closure_param.clone(),
                            requires_full_traversal: nested_info.traversal.has_graph_steps(),
                            is_first: false,
                        },
                    });
                }
                Type::Scalar(_scalar_ty) => {
                    // Check if the traversal is accessing an implicit field
                    // For nested traversals like usr::ID, we need to check what field is actually accessed
                    let accessed_field = nested_info.traversal.object_fields.first(); // Get the first (and usually only) field being accessed
                    let is_implicit = accessed_field
                        .map(|f| {
                            matches!(
                                f.as_str(),
                                "id" | "label"
                                    | "from_node"
                                    | "to_node"
                                    | "data"
                                    | "score"
                                    | "ID"
                                    | "Label" // Also check capitalized versions
                            )
                        })
                        .unwrap_or(!nested_info.traversal.has_object_step);

                    // Check if this has graph steps AND object step - if so, generate nested struct like Node/Edge case
                    if nested_info.traversal.has_graph_steps()
                        && nested_info.traversal.has_object_step
                    {
                        // Generate nested struct for single-field object access with graph navigation
                        let nested_prefix =
                            format!("{}{}", struct_name_prefix, capitalize_first(field_name));

                        // Build the nested fields from the object_fields
                        let mut nested_field_infos = Vec::new();
                        for obj_field in &nested_info.traversal.object_fields {
                            // Check if it's an implicit field
                            let field_type = if matches!(
                                obj_field.as_str(),
                                "id" | "ID" | "label" | "Label" | "from_node" | "to_node"
                            ) {
                                RustFieldType::Primitive(GenRef::RefLT("a", RustType::Str))
                            } else if obj_field == "data" {
                                RustFieldType::RefArray(RustType::F64)
                            } else if obj_field == "score" {
                                RustFieldType::Primitive(GenRef::Std(RustType::F64))
                            } else {
                                RustFieldType::OptionValue
                            };

                            // Determine if this is an implicit or schema field
                            let source = if matches!(
                                obj_field.as_str(),
                                "id" | "ID"
                                    | "label"
                                    | "Label"
                                    | "from_node"
                                    | "to_node"
                                    | "data"
                                    | "score"
                            ) {
                                ReturnFieldSource::ImplicitField {
                                    property_name: None,
                                }
                            } else {
                                ReturnFieldSource::SchemaField {
                                    property_name: None,
                                }
                            };

                            nested_field_infos.push(ReturnFieldInfo {
                                name: obj_field.clone(),
                                field_type: ReturnFieldType::Simple(field_type),
                                source,
                            });
                        }

                        let nested_struct_name = format!("{}ReturnType", nested_prefix);
                        let is_first =
                            matches!(nested_info.traversal.should_collect, ShouldCollect::ToObj);

                        fields.push(ReturnFieldInfo {
                            name: field_name.clone(),
                            field_type: ReturnFieldType::Nested(nested_field_infos),
                            source: ReturnFieldSource::NestedTraversal {
                                traversal_expr: format!("nested_traversal_{}", field_name),
                                // Use format_steps_without_property_fetch for scalar types so the
                                // property access is handled in the struct mapping, not as a traversal step
                                traversal_code: Some(
                                    nested_info.traversal.format_steps_without_property_fetch(),
                                ),
                                nested_struct_name: Some(nested_struct_name),
                                traversal_type: Some(nested_info.traversal.traversal_type.clone()),
                                closure_param_name: nested_info.closure_param_name.clone(),
                                closure_source_var: nested_info.closure_source_var.clone(),
                                accessed_field_name: None,
                                own_closure_param: nested_info.own_closure_param.clone(),
                                requires_full_traversal: true,
                                is_first,
                            },
                        });
                    } else {
                        // Simple property access - no graph steps OR no object step
                        // If this traversal has graph steps, check if ::FIRST was used
                        let rust_type = if nested_info.traversal.has_graph_steps() {
                            // Check if ::FIRST was used (should_collect is ToObj)
                            if matches!(nested_info.traversal.should_collect, ShouldCollect::ToObj)
                            {
                                RustFieldType::OptionValue // ::FIRST returns Option<&'a Value>
                            } else {
                                RustFieldType::Vec(Box::new(RustFieldType::Value))
                            }
                        } else if is_implicit {
                            // Use the appropriate type based on the implicit field
                            match accessed_field.map(|s| s.as_str()) {
                                Some("data") => RustFieldType::RefArray(RustType::F64),
                                Some("score") => {
                                    RustFieldType::Primitive(GenRef::Std(RustType::F64))
                                }
                                Some("id") | Some("ID") | Some("label") | Some("Label")
                                | Some("from_node") | Some("to_node") | None => {
                                    RustFieldType::Primitive(GenRef::RefLT("a", RustType::Str))
                                }
                                _ => RustFieldType::OptionValue,
                            }
                        } else {
                            RustFieldType::OptionValue
                        };

                        // Check if this is a variable reference (e.g., `scalar: scalar_var`)
                        // Variable references have closure_source_var set but no graph steps and no object step
                        let is_variable_reference = nested_info.closure_source_var.is_some()
                            && !nested_info.traversal.has_graph_steps()
                            && !nested_info.traversal.has_object_step;

                        let trav_code = if is_variable_reference {
                            String::new()
                        } else {
                            nested_info.traversal.format_steps_only()
                        };
                        // Extract the accessed field name from object_fields
                        let accessed_field_name =
                            nested_info.traversal.object_fields.first().cloned();
                        fields.push(ReturnFieldInfo {
                            name: field_name.clone(),
                            field_type: ReturnFieldType::Simple(rust_type),
                            source: ReturnFieldSource::NestedTraversal {
                                traversal_expr: format!("nested_traversal_{}", field_name),
                                traversal_code: Some(trav_code),
                                nested_struct_name: None,
                                traversal_type: Some(nested_info.traversal.traversal_type.clone()),
                                closure_param_name: nested_info.closure_param_name.clone(),
                                closure_source_var: nested_info.closure_source_var.clone(),
                                accessed_field_name,
                                own_closure_param: nested_info.own_closure_param.clone(),
                                requires_full_traversal: nested_info.traversal.has_graph_steps(),
                                is_first: false,
                            },
                        });
                    }
                }
                Type::Node(_)
                | Type::Edge(_)
                | Type::Vector(_)
                | Type::Nodes(_)
                | Type::Edges(_)
                | Type::Vectors(_) => {
                    // Check if this is a variable reference (e.g., `user: u` in a closure)
                    // Variable references have closure_source_var set but no graph steps and no object step
                    let is_variable_reference = nested_info.closure_source_var.is_some()
                        && !nested_info.traversal.has_graph_steps()
                        && !nested_info.traversal.has_object_step;

                    // Check if there's property access (object step) - if not, just return TraversalValue
                    if !nested_info.traversal.has_object_step && !is_variable_reference {
                        // No property access - return simple TraversalValue type
                        let rust_type = match return_type {
                            Type::Nodes(_) | Type::Edges(_) | Type::Vectors(_) => {
                                RustFieldType::Vec(Box::new(RustFieldType::TraversalValue))
                            }
                            _ => RustFieldType::TraversalValue,
                        };

                        fields.push(ReturnFieldInfo {
                            name: field_name.clone(),
                            field_type: ReturnFieldType::Simple(rust_type),
                            source: ReturnFieldSource::NestedTraversal {
                                traversal_expr: format!("nested_traversal_{}", field_name),
                                traversal_code: Some(nested_info.traversal.format_steps_only()),
                                nested_struct_name: None,
                                traversal_type: Some(nested_info.traversal.traversal_type.clone()),
                                closure_param_name: nested_info.closure_param_name.clone(),
                                closure_source_var: nested_info.closure_source_var.clone(),
                                accessed_field_name: None,
                                own_closure_param: nested_info.own_closure_param.clone(),
                                requires_full_traversal: nested_info.traversal.has_graph_steps(),
                                is_first: false,
                            },
                        });
                    } else {
                        // Has property access or variable reference - complex types need nested structs
                        let nested_prefix =
                            format!("{}{}", struct_name_prefix, capitalize_first(field_name));
                        let nested_projection =
                            projection_metadata_from_traversal(&nested_info.traversal);
                        let nested_fields = build_return_fields(
                            ctx,
                            return_type,
                            &nested_projection,
                            &nested_prefix,
                        );
                        let nested_struct_name = format!("{}ReturnType", nested_prefix);
                        let is_first =
                            matches!(nested_info.traversal.should_collect, ShouldCollect::ToObj);

                        // For variable references (empty source step), use empty string for traversal_code
                        let traversal_code = if is_variable_reference {
                            String::new()
                        } else {
                            nested_info.traversal.format_steps_only()
                        };

                        fields.push(ReturnFieldInfo {
                            name: field_name.clone(),
                            field_type: ReturnFieldType::Nested(nested_fields),
                            source: ReturnFieldSource::NestedTraversal {
                                traversal_expr: format!("nested_traversal_{}", field_name),
                                traversal_code: Some(traversal_code),
                                nested_struct_name: Some(nested_struct_name),
                                traversal_type: Some(nested_info.traversal.traversal_type.clone()),
                                closure_param_name: nested_info.closure_param_name.clone(),
                                closure_source_var: nested_info.closure_source_var.clone(),
                                accessed_field_name: None,
                                own_closure_param: nested_info.own_closure_param.clone(),
                                requires_full_traversal: nested_info.traversal.has_graph_steps(),
                                is_first,
                            },
                        });
                    }
                }
                _ => {
                    // Other types - use placeholder
                    fields.push(ReturnFieldInfo {
                        name: field_name.clone(),
                        field_type: ReturnFieldType::Simple(RustFieldType::Value),
                        source: ReturnFieldSource::NestedTraversal {
                            traversal_expr: format!("nested_traversal_{}", field_name),
                            traversal_code: Some(nested_info.traversal.format_steps_only()),
                            nested_struct_name: None,
                            traversal_type: Some(nested_info.traversal.traversal_type.clone()),
                            closure_param_name: nested_info.closure_param_name.clone(),
                            closure_source_var: nested_info.closure_source_var.clone(),
                            accessed_field_name: None,
                            own_closure_param: nested_info.own_closure_param.clone(),
                            requires_full_traversal: nested_info.traversal.has_graph_steps(),
                            is_first: false,
                        },
                    });
                }
            }
        } else {
            // Type not yet determined - create placeholder
            // This will be filled in during a later pass
            fields.push(ReturnFieldInfo {
                name: field_name.clone(),
                field_type: ReturnFieldType::Simple(RustFieldType::Value),
                source: ReturnFieldSource::NestedTraversal {
                    traversal_expr: format!("nested_traversal_{}", field_name),
                    traversal_code: None,
                    nested_struct_name: None,
                    traversal_type: None,
                    closure_param_name: None,
                    closure_source_var: None,
                    accessed_field_name: None,
                    own_closure_param: None,
                    requires_full_traversal: false,
                    is_first: false,
                },
            });
        }
    }

    // Step 4: Add computed expression fields
    for (field_name, computed_info) in projection.computed_expressions {
        fields.push(ReturnFieldInfo {
            name: field_name.clone(),
            field_type: ReturnFieldType::Simple(RustFieldType::Value),
            source: ReturnFieldSource::ComputedExpression {
                expression: computed_info.expression.clone(),
            },
        });
    }

    fields
}

/// Process object literal return types and create struct definitions
/// This handles RETURN { field1: expr1, field2: { ... }, field3: [...] } syntax
///
/// Compatibility behavior: complex object/array returns are wrapped under a top-level
/// `response` key and emitted as a literal JSON expression.
fn process_object_literal<'a>(
    ctx: &mut Ctx<'a>,
    _original_query: &'a Query,
    scope: &mut HashMap<&'a str, VariableInfo>,
    query: &mut GeneratedQuery,
    object_fields: &HashMap<String, ReturnType>,
    _struct_name: String,
) {
    // Build JSON construction code recursively
    fn build_json_code<'a>(
        ctx: &Ctx<'a>,
        obj_fields: &HashMap<String, ReturnType>,
        scope: &HashMap<&str, VariableInfo>,
    ) -> String {
        let mut json_parts = Vec::new();

        for (field_name, return_type) in obj_fields {
            let field_value = match return_type {
                ReturnType::Expression(expr) => {
                    match &expr.expr {
                        ExpressionType::Traversal(trav) => {
                            // Handle traversal like app::{name}
                            // Extract variable name from start node
                            let var_name = match &trav.start {
                                crate::helixc::parser::types::StartNode::Identifier(id) => {
                                    id.clone()
                                }
                                _ => "unknown".to_string(),
                            };

                            // Check if there's an Object step to extract property name
                            if let Some(step) = trav.steps.first() {
                                if let crate::helixc::parser::types::StepType::Object(obj) =
                                    &step.step
                                {
                                    // Extract the first field name from the object step
                                    if let Some(field) = obj.fields.first() {
                                        let prop_name = &field.key;

                                        // Generate appropriate access code based on property
                                        if prop_name == "id" {
                                            format!("uuid_str({}.id(), &arena)", var_name)
                                        } else if prop_name == "label" {
                                            format!("{}.label()", var_name)
                                        } else {
                                            format!("{}.get_property(\"{}\")", var_name, prop_name)
                                        }
                                    } else {
                                        // No fields in object step
                                        format!("json!({})", var_name)
                                    }
                                } else {
                                    // Not an Object step, just return the variable
                                    format!("json!({})", var_name)
                                }
                            } else {
                                // No steps, just the identifier
                                format!("json!({})", var_name)
                            }
                        }
                        ExpressionType::Identifier(id) => {
                            // Look up the variable type in scope and generate property extraction
                            if let Some(var_info) = scope.get(id.as_str()) {
                                build_identifier_json(ctx, id, &var_info.ty)
                            } else {
                                // Fallback if not in scope
                                format!("json!({})", id)
                            }
                        }
                        _ => "serde_json::Value::Null".to_string(),
                    }
                }
                ReturnType::Object(nested_obj) => {
                    // Recursively build nested object
                    let nested_json = build_json_code(ctx, nested_obj, scope);
                    format!("json!({})", nested_json)
                }
                ReturnType::Array(arr) => {
                    // Build array
                    let mut array_parts = Vec::new();
                    for elem in arr {
                        match elem {
                            ReturnType::Expression(expr) => {
                                match &expr.expr {
                                    ExpressionType::Identifier(id) => {
                                        // Look up the variable type and generate property extraction
                                        if let Some(var_info) = scope.get(id.as_str()) {
                                            array_parts.push(build_identifier_json(
                                                ctx,
                                                id,
                                                &var_info.ty,
                                            ));
                                        } else {
                                            // Fallback
                                            array_parts.push(format!("json!({})", id));
                                        }
                                    }
                                    ExpressionType::Traversal(trav) => {
                                        // Handle traversal in array
                                        let var_name = match &trav.start {
                                            crate::helixc::parser::types::StartNode::Identifier(
                                                id,
                                            ) => id.clone(),
                                            _ => "unknown".to_string(),
                                        };

                                        // Check for object step
                                        if let Some(step) = trav.steps.first() {
                                            if let crate::helixc::parser::types::StepType::Object(
                                                obj,
                                            ) = &step.step
                                            {
                                                if let Some(field) = obj.fields.first() {
                                                    let prop_name = &field.key;
                                                    if prop_name == "id" {
                                                        array_parts.push(format!(
                                                            "uuid_str({}.id(), &arena)",
                                                            var_name
                                                        ));
                                                    } else if prop_name == "label" {
                                                        array_parts
                                                            .push(format!("{}.label()", var_name));
                                                    } else {
                                                        array_parts.push(format!(
                                                            "{}.get_property(\"{}\")",
                                                            var_name, prop_name
                                                        ));
                                                    }
                                                } else {
                                                    array_parts
                                                        .push(format!("json!({})", var_name));
                                                }
                                            } else {
                                                array_parts.push(format!("json!({})", var_name));
                                            }
                                        } else {
                                            array_parts.push(format!("json!({})", var_name));
                                        }
                                    }
                                    _ => {
                                        array_parts.push("serde_json::Value::Null".to_string());
                                    }
                                }
                            }
                            ReturnType::Object(obj) => {
                                let nested_json = build_json_code(ctx, obj, scope);
                                array_parts.push(format!("json!({})", nested_json));
                            }
                            _ => {
                                array_parts.push("serde_json::Value::Null".to_string());
                            }
                        }
                    }
                    format!("json!([{}])", array_parts.join(", "))
                }
                ReturnType::Empty => "serde_json::Value::Null".to_string(),
            };

            json_parts.push(format!("\"{}\": {}", field_name, field_value));
        }

        format!("{{\n        {}\n    }}", json_parts.join(",\n        "))
    }

    // Helper function to build JSON for an identifier based on its type
    fn build_identifier_json(ctx: &Ctx, var_name: &str, ty: &Type) -> String {
        match ty {
            Type::Node(Some(label)) => {
                // Look up the node schema to get its properties
                if let Some(node_fields) = ctx.node_fields.get(label.as_str()) {
                    let mut props = vec![
                        format!("\"id\": uuid_str({}.id(), &arena)", var_name),
                        format!("\"label\": {}.label()", var_name),
                    ];

                    for (prop_name, _prop_type) in node_fields.iter() {
                        // Skip implicit fields that are accessed via methods, not get_property
                        if *prop_name == "id" || *prop_name == "label" {
                            continue;
                        }
                        props.push(format!(
                            "\"{}\":  {}.get_property(\"{}\")",
                            prop_name, var_name, prop_name
                        ));
                    }

                    format!("json!({{\n        {}\n    }})", props.join(",\n        "))
                } else {
                    // Fallback if schema not found
                    format!(
                        "json!({{\"id\": uuid_str({}.id(), &arena), \"label\": {}.label()}})",
                        var_name, var_name
                    )
                }
            }
            Type::Edge(Some(label)) => {
                // Similar for edges
                if let Some(edge_fields) = ctx.edge_fields.get(label.as_str()) {
                    let mut props = vec![
                        format!("\"id\": uuid_str({}.id(), &arena)", var_name),
                        format!("\"label\": {}.label()", var_name),
                    ];

                    for (prop_name, _prop_type) in edge_fields.iter() {
                        // Skip implicit fields
                        if *prop_name == "id" || *prop_name == "label" {
                            continue;
                        }
                        props.push(format!(
                            "\"{}\":  {}.get_property(\"{}\")",
                            prop_name, var_name, prop_name
                        ));
                    }

                    format!("json!({{\n        {}\n    }})", props.join(",\n        "))
                } else {
                    format!(
                        "json!({{\"id\": uuid_str({}.id(), &arena), \"label\": {}.label()}})",
                        var_name, var_name
                    )
                }
            }
            _ => {
                // For other types (Node(None), Edge(None), primitives, etc), just use json! macro
                format!("json!({})", var_name)
            }
        }
    }

    let json_code = build_json_code(ctx, object_fields, scope);

    emit_literal_return(
        query,
        "response".to_string(),
        crate::helixc::generator::utils::GenRef::Std(format!("json!({})", json_code)),
    );
}

/// Helper function to get Rust type string from analyzer Type and populate return value fields
fn type_to_rust_string_and_fields(
    ty: &Type,
    should_collect: &ShouldCollect,
    ctx: &Ctx,
    _field_name: &str,
) -> (
    String,
    Vec<crate::helixc::generator::return_values::ReturnValueField>,
) {
    match (ty, should_collect) {
        // For single nodes/vectors/edges, generate a proper struct based on schema
        (Type::Node(Some(label)), ShouldCollect::ToObj | ShouldCollect::No) => {
            let type_name = format!("{}ReturnType", label);
            let mut fields = vec![
                crate::helixc::generator::return_values::ReturnValueField::new(
                    "id".to_string(),
                    "ID".to_string(),
                )
                .with_implicit(true),
                crate::helixc::generator::return_values::ReturnValueField::new(
                    "label".to_string(),
                    "String".to_string(),
                )
                .with_implicit(true),
            ];

            // Add properties from schema (skip id and label as they're already added)
            if let Some(node_fields) = ctx.node_fields.get(label.as_str()) {
                for (prop_name, field) in node_fields {
                    if *prop_name != "id" && *prop_name != "label" {
                        fields.push(
                            crate::helixc::generator::return_values::ReturnValueField::new(
                                prop_name.to_string(),
                                format!("{}", field.field_type),
                            ),
                        );
                    }
                }
            }
            (type_name, fields)
        }
        (Type::Edge(Some(label)), ShouldCollect::ToObj | ShouldCollect::No) => {
            let type_name = format!("{}ReturnType", label);
            let mut fields = vec![
                crate::helixc::generator::return_values::ReturnValueField::new(
                    "id".to_string(),
                    "ID".to_string(),
                )
                .with_implicit(true),
                crate::helixc::generator::return_values::ReturnValueField::new(
                    "label".to_string(),
                    "String".to_string(),
                )
                .with_implicit(true),
            ];

            if let Some(edge_fields) = ctx.edge_fields.get(label.as_str()) {
                for (prop_name, field) in edge_fields {
                    if *prop_name != "id" && *prop_name != "label" {
                        fields.push(
                            crate::helixc::generator::return_values::ReturnValueField::new(
                                prop_name.to_string(),
                                format!("{}", field.field_type),
                            ),
                        );
                    }
                }
            }
            (type_name, fields)
        }
        (Type::Vector(Some(label)), ShouldCollect::ToObj | ShouldCollect::No) => {
            let type_name = format!("{}ReturnType", label);
            let mut fields = vec![
                crate::helixc::generator::return_values::ReturnValueField::new(
                    "id".to_string(),
                    "ID".to_string(),
                )
                .with_implicit(true),
                crate::helixc::generator::return_values::ReturnValueField::new(
                    "label".to_string(),
                    "String".to_string(),
                )
                .with_implicit(true),
            ];

            if let Some(vector_fields) = ctx.vector_fields.get(label.as_str()) {
                for (prop_name, field) in vector_fields {
                    if *prop_name != "id" && *prop_name != "label" {
                        fields.push(
                            crate::helixc::generator::return_values::ReturnValueField::new(
                                prop_name.to_string(),
                                format!("{}", field.field_type),
                            ),
                        );
                    }
                }
            }
            (type_name, fields)
        }
        // For Vec types, we still need Vec<TypeName>
        (Type::Node(Some(label)), ShouldCollect::ToVec) => {
            (format!("Vec<{}ReturnType>", label), vec![])
        }
        (Type::Edge(Some(label)), ShouldCollect::ToVec) => {
            (format!("Vec<{}ReturnType>", label), vec![])
        }
        (Type::Vector(Some(label)), ShouldCollect::ToVec) => {
            (format!("Vec<{}ReturnType>", label), vec![])
        }
        // Fallbacks for None labels
        (Type::Node(None), _) | (Type::Edge(None), _) | (Type::Vector(None), _) => {
            ("".to_string(), vec![])
        }
        (Type::Scalar(s), _) => (format!("{}", s), vec![]),
        (Type::Boolean, _) => ("bool".to_string(), vec![]),
        (Type::Array(inner), _) => {
            let (inner_type, _) =
                type_to_rust_string_and_fields(inner, &ShouldCollect::No, ctx, _field_name);
            (format!("Vec<{}>", inner_type), vec![])
        }
        (Type::Aggregate(_info), _) => {
            // For aggregates, return HashMap type since that's what group_by/aggregate_by returns
            // The actual struct fields will be generated later in build_return_fields
            ("HashMap<String, AggregateItem>".to_string(), vec![])
        }
        _ => ("".to_string(), vec![]),
    }
}

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
            &original_query.name,
            param.clone(),
            &mut query.parameters,
            &mut query.sub_parameters,
        );
    }

    // -------------------------------------------------
    // Statement‑by‑statement walk
    // -------------------------------------------------
    let mut scope: HashMap<&str, VariableInfo> = HashMap::new();
    for param in &original_query.parameters {
        let param_type = Type::from(param.param_type.1.clone());
        // Parameters are singular unless they're array types (Nodes, Edges, Vectors, etc.)
        let is_single = !matches!(
            param_type,
            Type::Nodes(_) | Type::Edges(_) | Type::Vectors(_)
        );
        scope.insert(
            param.name.1.as_str(),
            VariableInfo::new(param_type, is_single),
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
        } else {
            let return_name = query.return_values.first().unwrap().0.clone();
            query.mcp_handler = Some(return_name);
        }
    }

    ctx.output.queries.push(query);
}

fn analyze_return_expr<'a>(
    ctx: &mut Ctx<'a>,
    original_query: &'a Query,
    scope: &mut HashMap<&'a str, VariableInfo>,
    query: &mut GeneratedQuery,
    ret: &'a ReturnType,
) {
    match ret {
        ReturnType::Expression(expr) => {
            let (inferred_type, stmt) =
                infer_expr_type(ctx, expr, scope, original_query, None, query);

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

                            let field_name = v.inner().clone();

                            let literal_value = scalar_property_literal_from_traversal(
                                &inferred_type,
                                &traversal,
                                &field_name,
                            );

                            let return_ir = return_ir_from_traversal(
                                field_name,
                                &inferred_type,
                                &traversal,
                                literal_value,
                            );
                            emit_return_from_ir(ctx, query, return_ir);

                            // Note: Map closures are no longer injected here.
                            // Mapping will happen at response construction time instead.
                        }
                        _ => {
                            let field_name = "data".to_string();

                            let return_ir = return_ir_from_traversal(
                                field_name,
                                &inferred_type,
                                &traversal,
                                None,
                            );
                            emit_return_from_ir(ctx, query, return_ir);

                            // Generate map closure (direct return, no variable assignment to update)
                            // Map closure will be used during return generation phase
                        }
                    }
                }
                GeneratedStatement::Identifier(id) => {
                    is_valid_identifier(ctx, original_query, expr.loc.clone(), id.inner().as_str());
                    let identifier_end_type = match scope.get(id.inner().as_str()) {
                        Some(var_info) => var_info.ty.clone(),
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

                    let field_name = id.inner().clone();

                    let return_ir = return_ir_from_identifier(
                        field_name,
                        identifier_end_type,
                        scope.get(id.inner().as_str()),
                    );
                    emit_return_from_ir(ctx, query, return_ir);

                    // Note: Map closures are no longer injected here.
                    // Mapping will happen at response construction time instead.
                } // end GeneratedStatement::Identifier
                GeneratedStatement::Literal(l) => {
                    emit_literal_return(query, "data".to_string(), l.clone());
                }
                GeneratedStatement::Empty => {
                    query.return_values = vec![];
                    query.return_structs = vec![];
                }

                // These statement types are not valid in return expressions
                // ForEach, Drop, Assignment, BoExp, and Array cannot be returned directly
                GeneratedStatement::ForEach(_)
                | GeneratedStatement::Drop(_)
                | GeneratedStatement::Assignment(_)
                | GeneratedStatement::BoExp(_)
                | GeneratedStatement::Array(_) => {
                    // Silently ignore - error should have been caught earlier
                }
            }
        }
        ReturnType::Array(values) => {
            // For arrays, check if they contain simple expressions (identifiers/traversals)
            // or complex nested structures
            let is_simple_array = values
                .iter()
                .all(|v| matches!(v, ReturnType::Expression(_)));

            if is_simple_array {
                // Process each element as a separate return value
                for return_expr in values {
                    analyze_return_expr(ctx, original_query, scope, query, return_expr);
                }
            } else {
                // Complex nested array/object structure
                // Wrap in an object with a single "data" field for now
                let mut object_fields = HashMap::new();
                object_fields.insert("data".to_string(), ReturnType::Array(values.clone()));

                let struct_name = format!("{}ReturnType", capitalize_first(&query.name));
                process_object_literal(
                    ctx,
                    original_query,
                    scope,
                    query,
                    &object_fields,
                    struct_name,
                );

                // Note: compatibility wrapper handling is emitted via shared literal-return helper
            }
        }
        ReturnType::Object(values) => {
            // Check if this is a simple object with only expression values
            let is_simple_object = values
                .values()
                .all(|v| matches!(v, ReturnType::Expression(_)));

            if is_simple_object {
                // Process each field in the object
                for return_expr in values.values() {
                    // Recursively analyze each field's return expression
                    analyze_return_expr(ctx, original_query, scope, query, return_expr);
                }
            } else {
                // Complex nested object - use new object literal processing
                let struct_name = format!("{}ReturnType", capitalize_first(&query.name));
                process_object_literal(ctx, original_query, scope, query, values, struct_name);

                // Note: compatibility wrapper handling is emitted via shared literal-return helper
            }
        }
        ReturnType::Empty => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::helixc::parser::{HelixParser, write_to_temp_file};

    // ============================================================================
    // Parameter Validation Tests
    // ============================================================================

    #[test]
    fn test_unknown_parameter_type() {
        let source = r#"
            N::Person { name: String }

            QUERY test(data: UnknownType) =>
                p <- N<Person>
                RETURN p
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.iter().any(|d| d.error_code == ErrorCode::E209));
        assert!(
            diagnostics
                .iter()
                .any(|d| d.message.contains("unknown type") && d.message.contains("UnknownType"))
        );
    }

    #[test]
    fn test_valid_array_parameter_type() {
        let source = r#"
            N::Person { name: String }

            QUERY createPeople(names: [String]) =>
                p <- N<Person>
                RETURN p
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        // Should not have E209 errors for valid array parameter type
        assert!(!diagnostics.iter().any(|d| d.error_code == ErrorCode::E209));
    }

    // ============================================================================
    // Variable Scope Tests
    // ============================================================================

    #[test]
    fn test_variable_not_in_scope() {
        let source = r#"
            N::Person { name: String }

            QUERY test() =>
                p <- N<Person>
                RETURN unknownVar
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.iter().any(|d| d.error_code == ErrorCode::E301));
        assert!(
            diagnostics
                .iter()
                .any(|d| d.message.contains("not in scope") && d.message.contains("unknownVar"))
        );
    }

    #[test]
    fn test_parameter_in_scope() {
        let source = r#"
            N::Person { name: String }

            QUERY test(id: ID) =>
                p <- N<Person>(id)
                RETURN p
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(!diagnostics.iter().any(|d| d.error_code == ErrorCode::E301));
    }

    #[test]
    fn test_assigned_variable_in_scope() {
        let source = r#"
            N::Person { name: String }

            QUERY test() =>
                p <- N<Person>
                result <- p::{name}
                RETURN result
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(!diagnostics.iter().any(|d| d.error_code == ErrorCode::E301));
    }

    // ============================================================================
    // MCP Macro Validation Tests
    // ============================================================================

    #[test]
    fn test_mcp_query_single_return_valid() {
        let source = r#"
            N::Person { name: String }

            #[mcp]
            QUERY getPerson(id: ID) =>
                person <- N<Person>(id)
                RETURN person
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(!diagnostics.iter().any(|d| d.error_code == ErrorCode::E401));
    }

    #[test]
    fn test_mcp_query_multiple_returns_invalid() {
        let source = r#"
            N::Person { name: String }

            #[mcp]
            QUERY getPerson() =>
                p1 <- N<Person>
                p2 <- N<Person>
                RETURN p1, p2
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.iter().any(|d| d.error_code == ErrorCode::E401));
        assert!(
            diagnostics
                .iter()
                .any(|d| d.message.contains("MCP query must return a single value"))
        );
    }

    #[test]
    fn test_non_mcp_query_multiple_returns_valid() {
        let source = r#"
            N::Person { name: String }

            QUERY getPeople() =>
                p1 <- N<Person>
                p2 <- N<Person>
                RETURN p1, p2
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        // Non-MCP queries can return multiple values
        assert!(!diagnostics.iter().any(|d| d.error_code == ErrorCode::E401));
    }

    // ============================================================================
    // Return Value Tests
    // ============================================================================

    #[test]
    fn test_return_literal_value() {
        let source = r#"
            N::Person { name: String }

            QUERY test() =>
                p <- N<Person>
                RETURN "success"
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, generated) = result.unwrap();
        // Should not have errors for returning literal
        assert!(!diagnostics.iter().any(|d| d.error_code == ErrorCode::E301));

        let query = generated
            .queries
            .iter()
            .find(|q| q.name == "test")
            .expect("query should be generated");
        assert_eq!(query.return_values.len(), 1);
        assert_eq!(query.return_structs.len(), 1);
        assert!(query.return_structs[0].is_primitive);
        assert!(query.return_structs[0].primitive_literal_value.is_some());
    }

    #[test]
    fn test_return_multiple_values() {
        let source = r#"
            N::Person { name: String }

            QUERY test() =>
                p1 <- N<Person>
                p2 <- N<Person>
                RETURN p1, p2
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(!diagnostics.iter().any(|d| d.error_code == ErrorCode::E301));
    }

    #[test]
    fn test_return_object() {
        let source = r#"
            N::Person { name: String }

            QUERY test() =>
                p <- N<Person>
                RETURN {person: p, status: "found"}
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(!diagnostics.iter().any(|d| d.error_code == ErrorCode::E301));
    }

    #[test]
    fn test_complex_return_object_keeps_response_wrapper_compatibility() {
        let source = r#"
            N::Person { name: String }

            QUERY test() =>
                p <- N<Person>
                RETURN { payload: { person: p, status: "found" } }
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, generated) = result.unwrap();
        assert!(!diagnostics.iter().any(|d| d.error_code == ErrorCode::E301));

        let query = generated
            .queries
            .iter()
            .find(|q| q.name == "test")
            .expect("query should exist");
        assert!(query.use_struct_returns);
        assert_eq!(query.return_values.len(), 1);
        assert_eq!(query.return_values[0].0, "response");
        assert_eq!(query.return_structs.len(), 1);
        assert!(query.return_structs[0].is_primitive);
        assert_eq!(query.return_structs[0].source_variable, "response");
        assert!(query.return_structs[0].primitive_literal_value.is_some());
    }

    #[test]
    fn test_complex_return_array_keeps_response_wrapper_compatibility() {
        let source = r#"
            N::Person { name: String }

            QUERY test() =>
                p <- N<Person>
                RETURN [{ person: p }]
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, generated) = result.unwrap();
        assert!(!diagnostics.iter().any(|d| d.error_code == ErrorCode::E301));

        let query = generated
            .queries
            .iter()
            .find(|q| q.name == "test")
            .expect("query should exist");
        assert!(query.use_struct_returns);
        assert_eq!(query.return_values.len(), 1);
        assert_eq!(query.return_values[0].0, "response");
        assert_eq!(query.return_structs.len(), 1);
        assert!(query.return_structs[0].is_primitive);
        assert_eq!(query.return_structs[0].source_variable, "response");
        assert!(query.return_structs[0].primitive_literal_value.is_some());
    }

    // ============================================================================
    // Model Macro Tests
    // ============================================================================

    #[test]
    fn test_model_macro_sets_embedding_model() {
        let source = r#"
            V::Document { content: String, embedding: [F32] }

            #[model("gpt-4")]
            QUERY addDoc(text: String) =>
                doc <- AddV<Document>(Embed(text), {content: text})
                RETURN doc
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, generated) = result.unwrap();
        // Model macro should be processed without errors
        assert!(
            diagnostics.is_empty() || !diagnostics.iter().any(|d| d.error_code == ErrorCode::E301)
        );

        // Check that the generated query has the embedding model set
        assert_eq!(generated.queries.len(), 1);
        // Model name includes quotes from parsing
        assert_eq!(
            generated.queries[0].embedding_model_to_use,
            Some("\"gpt-4\"".to_string())
        );
    }

    // ============================================================================
    // Complex Query Tests
    // ============================================================================

    #[test]
    fn test_query_with_traversal_and_filtering() {
        let source = r#"
            N::Person { name: String, age: U32 }
            E::Knows { From: Person, To: Person }

            QUERY getFriends(id: ID, minAge: U32) =>
                person <- N<Person>(id)
                friends <- person::Out<Knows>::WHERE(_::{age}::GT(minAge))
                RETURN friends
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        // Complex queries should not have scope errors
        assert!(!diagnostics.iter().any(|d| d.error_code == ErrorCode::E301));
    }

    #[test]
    fn test_query_with_multiple_assignments() {
        let source = r#"
            N::Person { name: String }
            N::Company { name: String }
            E::WorksAt { From: Person, To: Company }

            QUERY getEmployees(companyId: ID) =>
                company <- N<Company>(companyId)
                edges <- company::InE<WorksAt>
                people <- edges::FromN
                RETURN people
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(!diagnostics.iter().any(|d| d.error_code == ErrorCode::E301));
    }

    #[test]
    fn test_query_returning_property_access() {
        let source = r#"
            N::Person { name: String, email: String }

            QUERY getEmail(id: ID) =>
                person <- N<Person>(id)
                email <- person::{email}
                RETURN email
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(!diagnostics.iter().any(|d| d.error_code == ErrorCode::E301));
    }
}
