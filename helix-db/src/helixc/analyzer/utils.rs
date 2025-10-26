//! Semantic analyzer for Helix‑QL.
use crate::helixc::analyzer::error_codes::ErrorCode;
use crate::{
    generate_error,
    helixc::{
        analyzer::{Ctx, errors::push_query_err, types::Type},
        generator::{
            traversal_steps::Step,
            utils::{GenRef, GeneratedValue},
        },
        parser::{location::Loc, types::*},
    },
};
use paste::paste;
use std::collections::HashMap;

pub(super) const DEFAULT_VAR_NAME: &str = "val";

pub(super) fn is_valid_identifier(
    ctx: &mut Ctx,
    original_query: &Query,
    loc: Loc,
    name: &str,
) -> bool {
    match name {
        "true" | "false" | "NONE" | "String" | "Boolean" | "F32" | "F64" | "I8" | "I16" | "I32"
        | "I64" | "U8" | "U16" | "U32" | "U64" | "U128" | "Uuid" | "Date" => {
            generate_error!(ctx, original_query, loc.clone(), E105, name);
            false
        }
        _ => true,
    }
}

pub(super) fn is_param<'a>(q: &'a Query, name: &str) -> Option<&'a Parameter> {
    q.parameters.iter().find(|p| p.name.1 == *name)
}

pub(super) fn check_identifier_is_fieldtype(
    ctx: &mut Ctx,
    original_query: &Query,
    loc: Loc,
    scope: &HashMap<&str, Type>,
    identifier_name: &str,
    field_type: FieldType,
) -> Option<()> {
    if let Some(scope_type) = scope.get(identifier_name)
        && scope_type != &Type::from(&field_type)
    {
        generate_error!(
            ctx,
            original_query,
            loc,
            E210,
            identifier_name,
            &field_type.to_string()
        );
        return None;
    }

    Some(())
}

pub(super) fn gen_identifier_or_param(
    original_query: &Query,
    name: &str,
    should_ref: bool,
    _should_clone: bool,
) -> GeneratedValue {
    if let Some(param) = is_param(original_query, name) {
        GeneratedValue::Parameter(match (should_ref, param.is_optional) {
            (true, false) => GenRef::Ref(format!("data.{name}")),
            // std here because the as_ref returns a reference to the value
            (true, true) => GenRef::Std(format!(
                "data.{name}.as_ref().ok_or_else(|| GraphError::ParamNotFound(\"{name}\"))?"
            )),
            (false, false) => GenRef::Std(format!("data.{name}.clone()")),
            (false, true) => GenRef::Std(format!(
                "data.{name}.as_ref().ok_or_else(|| GraphError::ParamNotFound(\"{name}\"))?.clone()"
            )),
        })
    } else {
        GeneratedValue::Identifier(if should_ref {
            GenRef::Ref(name.to_string())
        } else {
            GenRef::Std(format!("{name}.clone()"))
        })
    }
}

pub(super) fn gen_id_access_or_param(original_query: &Query, name: &str) -> GeneratedValue {
    if let Some(param) = is_param(original_query, name) {
        GeneratedValue::Parameter(match param.is_optional {
            true => GenRef::DeRef(format!(
                "data.{name}.as_ref().ok_or_else(|| GraphError::ParamNotFound(\"{name}\"))?"
            )),
            false => GenRef::DeRef(format!("data.{name}")),
        })
    } else {
        GeneratedValue::Identifier(GenRef::Std(format!("{name}.id()")))
    }
}

pub(super) fn is_in_scope(scope: &HashMap<&str, Type>, name: &str) -> bool {
    scope.contains_key(name)
}

pub(super) fn type_in_scope(
    ctx: &mut Ctx,
    original_query: &Query,
    loc: Loc,
    scope: &HashMap<&str, Type>,
    name: &str,
) -> Option<Type> {
    match scope.get(name) {
        Some(ty) => Some(ty.clone()),
        None => {
            generate_error!(ctx, original_query, loc.clone(), E301, name);
            None
        }
    }
}

pub(super) fn field_exists_on_item_type(
    ctx: &mut Ctx,
    original_query: &Query,
    item_type: Type,
    fields: Vec<(&str, &Loc)>,
) {
    for (key, loc) in fields {
        if !item_type.item_fields_contains_key(ctx, key) {
            generate_error!(
                ctx,
                original_query,
                loc.clone(),
                E202,
                key,
                item_type.kind_str(),
                &item_type.get_type_name()
            );
        }
    }
}

#[allow(unused)]
pub(super) fn get_singular_type(ty: Type) -> Type {
    match ty {
        Type::Nodes(node_type) => Type::Node(node_type),
        Type::Edges(edge_type) => Type::Edge(edge_type),
        Type::Vectors(vector_type) => Type::Vector(vector_type),
        Type::Node(_) => ty,
        Type::Edge(_) => ty,
        Type::Vector(_) => ty,
        _ => unreachable!("shouldve been caught eariler"),
    }
}

pub(super) fn validate_field_name_existence_for_item_type(
    ctx: &mut Ctx,
    original_query: &Query,
    loc: Loc,
    item_type: &Type,
    name: &str,
) {
    if !item_type.item_fields_contains_key(ctx, name) {
        generate_error!(
            ctx,
            original_query,
            loc.clone(),
            E202,
            name,
            item_type.kind_str(),
            &item_type.get_type_name()
        );
    }
}

pub(super) fn get_field_type_from_item_fields(
    ctx: &mut Ctx,
    item_type: &Type,
    name: &str,
) -> Option<FieldType> {
    item_type.get_field_type_from_item_fields(ctx, name)
}

pub(super) fn gen_property_access(name: &str) -> Step {
    match name {
        "id" => Step::PropertyFetch(GenRef::Literal("id".to_string())),
        "ID" => Step::PropertyFetch(GenRef::Literal("id".to_string())),
        n => Step::PropertyFetch(GenRef::Literal(n.to_string())),
    }
}

#[allow(unused)]
#[derive(Clone)]
pub(super) struct Variable {
    pub name: String,
    pub ty: Type,
}

impl Variable {
    pub fn new(name: String, ty: Type) -> Self {
        Self { name, ty }
    }
}

#[allow(unused)]
pub(super) trait VariableAccess {
    fn get_variable_name(&self) -> String;
    fn get_variable_ty(&self) -> &Type;
}

impl VariableAccess for Option<Variable> {
    fn get_variable_name(&self) -> String {
        match self {
            Some(v) => v.name.clone(),
            None => "var".to_string(),
        }
    }

    fn get_variable_ty(&self) -> &Type {
        match self {
            Some(v) => &v.ty,
            None => &Type::Unknown,
        }
    }
}

pub(super) trait FieldLookup {
    fn item_fields_contains_key(&self, ctx: &Ctx, key: &str) -> bool;
    fn item_fields_contains_key_with_type(&self, ctx: &Ctx, key: &str) -> (bool, String);
    fn get_field_type_from_item_fields(&self, ctx: &Ctx, key: &str) -> Option<FieldType>;
}

impl FieldLookup for Type {
    fn item_fields_contains_key(&self, ctx: &Ctx, key: &str) -> bool {
        match self {
            Type::Node(Some(node_type)) | Type::Nodes(Some(node_type)) => ctx
                .node_fields
                .get(node_type.as_str())
                .map(|fields| match key {
                    "id" | "ID" | "label" => true,
                    _ => fields.contains_key(key),
                })
                .unwrap_or(true),
            Type::Edge(Some(edge_type)) | Type::Edges(Some(edge_type)) => ctx
                .edge_fields
                .get(edge_type.as_str())
                .map(|fields| match key {
                    "id" | "ID" | "label" | "from_node" | "to_node" => true,
                    _ => fields.contains_key(key),
                })
                .unwrap_or(true),
            Type::Vector(Some(vector_type)) | Type::Vectors(Some(vector_type)) => ctx
                .vector_fields
                .get(vector_type.as_str())
                .map(|fields| match key {
                    "id" | "ID" | "label" | "data" | "score" => true,
                    _ => fields.contains_key(key),
                })
                .unwrap_or(true),
            _ => unreachable!("shouldve been caught eariler"),
        }
    }

    fn item_fields_contains_key_with_type(&self, ctx: &Ctx, key: &str) -> (bool, String) {
        let (is_valid_field, item_type) = match self {
            Type::Node(Some(node_type)) | Type::Nodes(Some(node_type)) => (
                ctx.node_fields
                    .get(node_type.as_str())
                    .map(|fields| match key {
                        "id" | "ID" | "label" => true,
                        _ => fields.contains_key(key),
                    })
                    .unwrap_or(true),
                node_type.as_str(),
            ),
            Type::Edge(Some(edge_type)) | Type::Edges(Some(edge_type)) => (
                ctx.edge_fields
                    .get(edge_type.as_str())
                    .map(|fields| match key {
                        "id" | "ID" | "label" | "from_node" | "to_node" => true,
                        _ => fields.contains_key(key),
                    })
                    .unwrap_or(true),
                edge_type.as_str(),
            ),
            Type::Vector(Some(vector_type)) | Type::Vectors(Some(vector_type)) => (
                ctx.vector_fields
                    .get(vector_type.as_str())
                    .map(|fields| match key {
                        "id" | "ID" | "label" | "data" | "score" => true,
                        _ => fields.contains_key(key),
                    })
                    .unwrap_or(true),
                vector_type.as_str(),
            ),
            _ => unreachable!("shouldve been caught eariler"),
        };

        (is_valid_field, item_type.to_string())
    }

    fn get_field_type_from_item_fields(&self, ctx: &Ctx, key: &str) -> Option<FieldType> {
        match self {
            Type::Node(Some(node_type)) | Type::Nodes(Some(node_type)) => ctx
                .node_fields
                .get(node_type.as_str())
                .map(|fields| match key {
                    "id" | "ID" => Some(FieldType::Uuid),
                    "label" => Some(FieldType::String),
                    _ => fields
                        .get(key)
                        .map(|field| Some(field.field_type.clone()))
                        .unwrap_or(None),
                })
                .unwrap_or(None),
            Type::Edge(Some(edge_type)) | Type::Edges(Some(edge_type)) => ctx
                .edge_fields
                .get(edge_type.as_str())
                .map(|fields| match key {
                    "id" | "ID" => Some(FieldType::Uuid),
                    "label" => Some(FieldType::String),
                    "from_node" | "to_node" => Some(FieldType::Uuid),
                    _ => fields
                        .get(key)
                        .map(|field| Some(field.field_type.clone()))
                        .unwrap_or(None),
                })
                .unwrap_or(None),

            Type::Vector(Some(vector_type)) | Type::Vectors(Some(vector_type)) => ctx
                .vector_fields
                .get(vector_type.as_str())
                .map(|fields| match key {
                    "id" | "ID" => Some(FieldType::Uuid),
                    "label" => Some(FieldType::String),
                    "data" => Some(FieldType::Array(Box::new(FieldType::F64))),
                    "score" => Some(FieldType::F64),
                    _ => fields
                        .get(key)
                        .map(|field| Some(field.field_type.clone()))
                        .unwrap_or(None),
                })
                .unwrap_or(None),
            _ => unreachable!("shouldve been caught eariler"),
        }
    }
}
