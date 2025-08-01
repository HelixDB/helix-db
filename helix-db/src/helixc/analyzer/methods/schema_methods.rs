use std::{borrow::Cow, collections::HashMap};

use crate::helixc::{
    analyzer::{analyzer::Ctx, error_codes::ErrorCode, errors::push_schema_err},
    parser::{
        helix_parser::{Field, FieldPrefix, FieldType, Source},
        location::Loc,
    },
};

type FieldLookup<'a> = HashMap<&'a str, HashMap<&'a str, Cow<'a, Field>>>;

pub(crate) fn build_field_lookups<'a>(
    src: &'a Source,
) -> (FieldLookup<'a>, FieldLookup<'a>, FieldLookup<'a>) {
    let node_fields = src
        .node_schemas
        .iter()
        .map(|n| {
            let mut props = n
                .fields
                .iter()
                .map(|f| (f.name.as_str(), Cow::Borrowed(f)))
                .collect::<HashMap<&str, Cow<'a, Field>>>();
            props.insert(
                "id",
                Cow::Owned(Field {
                    prefix: FieldPrefix::Empty,
                    defaults: None,
                    name: "id".to_string(),
                    field_type: FieldType::Uuid,
                    loc: Loc::empty(),
                }),
            );
            (n.name.1.as_str(), props)
        })
        .collect();

    let edge_fields = src
        .edge_schemas
        .iter()
        .map(|e| {
            let mut props: HashMap<_, _> = e
                .properties
                .as_ref()
                .map(|v| {
                    v.iter()
                        .map(|f| (f.name.as_str(), Cow::Borrowed(f)))
                        .collect()
                })
                .unwrap_or_default();
            props.insert(
                "id",
                Cow::Owned(Field {
                    prefix: FieldPrefix::Empty,
                    defaults: None,
                    name: "id".to_string(),
                    field_type: FieldType::Uuid,
                    loc: Loc::empty(),
                }),
            );
            (e.name.1.as_str(), props)
        })
        .collect();

    let vector_fields = src
        .vector_schemas
        .iter()
        .map(|v| {
            let mut props = v
                .fields
                .iter()
                .map(|f| (f.name.as_str(), Cow::Borrowed(f)))
                .collect::<HashMap<&str, Cow<'a, Field>>>();
            props.insert(
                "id",
                Cow::Owned(Field {
                    prefix: FieldPrefix::Empty,
                    defaults: None,
                    name: "id".to_string(),
                    field_type: FieldType::Uuid,
                    loc: Loc::empty(),
                }),
            );
            (v.name.as_str(), props)
        })
        .collect();

    (node_fields, edge_fields, vector_fields)
}

pub(crate) fn check_schema(ctx: &mut Ctx) {
    for edge in &ctx.src.edge_schemas {
        if !ctx.node_set.contains(edge.from.1.as_str())
            && !ctx.vector_set.contains(edge.from.1.as_str())
        {
            push_schema_err(
                ctx,
                edge.from.0.clone(),
                ErrorCode::E106,
                format!(
                    "use of undeclared node or vector type `{}` in schema",
                    edge.from.1
                ),
                Some(format!(
                    "declare `{}` in the schema before using it in an edge",
                    edge.from.1
                )),
            );
        }
        if !ctx.node_set.contains(edge.to.1.as_str())
            && !ctx.vector_set.contains(edge.to.1.as_str())
        {
            push_schema_err(
                ctx,
                edge.to.0.clone(),
                ErrorCode::E106,
                format!(
                    "use of undeclared node or vector type `{}` in schema",
                    edge.to.1
                ),
                Some(format!(
                    "declare `{}` in the schema before using it in an edge",
                    edge.to.1
                )),
            );
        }
        if let Some(v) = edge.properties.as_ref() {
            v.iter().for_each(|f| {
                if f.name.to_lowercase() == "id" {
                    push_schema_err(
                        ctx,
                        f.loc.clone(),
                        ErrorCode::E204,
                        format!("field `{}` is a reserved field name", f.name),
                        Some("rename the field".to_string()),
                    );
                }
            })
        }
        ctx.output.edges.push(edge.clone().into());
    }
    for node in &ctx.src.node_schemas {
        node.fields.iter().for_each(|f| {
            if f.name.to_lowercase() == "id" {
                push_schema_err(
                    ctx,
                    f.loc.clone(),
                    ErrorCode::E204,
                    format!("field `{}` is a reserved field name", f.name),
                    Some("rename the field".to_string()),
                );
            }
        });
        ctx.output.nodes.push(node.clone().into());
    }
    for vector in &ctx.src.vector_schemas {
        vector.fields.iter().for_each(|f: &Field| {
            if f.name.to_lowercase() == "id" {
                push_schema_err(
                    ctx,
                    f.loc.clone(),
                    ErrorCode::E204,
                    format!("field `{}` is a reserved field name", f.name),
                    Some("rename the field".to_string()),
                );
            }
        });
        ctx.output.vectors.push(vector.clone().into());
    }
}
