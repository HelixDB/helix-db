use std::{borrow::Cow, collections::HashMap};

use crate::helixc::{
    analyzer::{analyzer::Ctx, error_codes::ErrorCode, errors::push_schema_err},
    parser::helix_parser::{Field, Source},
};

type FieldLookup<'a> = HashMap<&'a str, HashMap<&'a str, Cow<'a, Field>>>;

pub(crate) struct SchemaVersionMap<'a>(
    HashMap<usize, (FieldLookup<'a>, FieldLookup<'a>, FieldLookup<'a>)>,
);

impl<'a> SchemaVersionMap<'a> {
    pub fn get_latest(&self) -> (FieldLookup<'a>, FieldLookup<'a>, FieldLookup<'a>) {
        self.0.get(self.0.keys().max().unwrap()).unwrap().clone()
    }

    pub fn inner(&self) -> &HashMap<usize, (FieldLookup<'a>, FieldLookup<'a>, FieldLookup<'a>)> {
        &self.0
    }
}

pub(crate) fn build_field_lookups<'a>(src: &'a Source) -> SchemaVersionMap<'a> {
    SchemaVersionMap(
        src.get_schemas_in_order()
            .iter()
            .map(|schema| {
                let node_fields = schema
                    .node_schemas
                    .iter()
                    .map(|n| {
                        (
                            n.name.1.as_str(),
                            n.fields
                                .iter()
                                .map(|f| (f.name.as_str(), Cow::Borrowed(f)))
                                .collect::<HashMap<&str, Cow<'a, Field>>>(),
                        )
                    })
                    .collect();

                let edge_fields = schema
                    .edge_schemas
                    .iter()
                    .map(|e| {
                        (
                            e.name.1.as_str(),
                            e.properties
                                .as_ref()
                                .map(|v| {
                                    v.iter()
                                        .map(|f| (f.name.as_str(), Cow::Borrowed(f)))
                                        .collect::<HashMap<&str, Cow<'a, Field>>>()
                                })
                                .unwrap_or_default(),
                        )
                    })
                    .collect();

                let vector_fields = schema
                    .vector_schemas
                    .iter()
                    .map(|v| {
                        (
                            v.name.as_str(),
                            v.fields
                                .iter()
                                .map(|f| (f.name.as_str(), Cow::Borrowed(f)))
                                .collect::<HashMap<&str, Cow<'a, Field>>>(),
                        )
                    })
                    .collect();

                (schema.version.1, (node_fields, edge_fields, vector_fields))
            })
            .collect(),
    )
}

pub(crate) fn check_schema(ctx: &mut Ctx) {
    for edge in &ctx.src.get_latest_schema().edge_schemas {
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
    for node in &ctx.src.get_latest_schema().node_schemas {
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
    for vector in &ctx.src.get_latest_schema().vector_schemas {
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
