use crate::{
    helixc::{
        analyzer::analyzer::Ctx,
        generator::{
            migrations::{
                GeneratedMigration, GeneratedMigrationItemMapping,
                GeneratedMigrationPropertyMapping,
            },
            utils::{GenRef, GeneratedValue, Separator},
        },
        parser::helix_parser::{
            FieldValueType, Migration, MigrationItem, MigrationPropertyMapping,
        },
    },
    protocol::value::Value,
};

pub(crate) fn validate_migration(ctx: &mut Ctx, migration: &Migration) {
    // check from version exists
    if !ctx
        .all_schemas
        .inner()
        .contains_key(&migration.from_version.1)
    {
        // schema error - version does not exist
    }
    // check to version exists and is 1 greater than from version
    if !ctx
        .all_schemas
        .inner()
        .contains_key(&migration.to_version.1)
    {
        // schema error - version does not exist
    }

    let (from_node_fields, from_edge_fields, from_vector_fields) = ctx
        .all_schemas
        .inner()
        .get(&migration.from_version.1)
        .unwrap();
    let (to_node_fields, to_edge_fields, to_vector_fields) = ctx
        .all_schemas
        .inner()
        .get(&migration.to_version.1)
        .unwrap();

    // for each migration item mapping
    let mut item_mappings = Vec::new();
    for item in &migration.body {
        // // get from fields and to fields and check they exist in respective versions
        // println!("item: {:?}", item);

        let from_fields = match match &item.from_item {
            (_, MigrationItem::Node(node)) => from_node_fields.get(node.as_str()),
            (_, MigrationItem::Edge(edge)) => from_edge_fields.get(edge.as_str()),
            (_, MigrationItem::Vector(vector)) => from_vector_fields.get(vector.as_str()),
        } {
            Some(fields) => fields,
            None => {
                // schema error - item does not exist
                println!(
                    "item does not exist: {:?}, {:?}",
                    item.from_item, from_node_fields
                );
                panic!("item does not exist");
            }
        };

        let to_fields = match match &item.to_item {
            (_, MigrationItem::Node(node)) => to_node_fields.get(node.as_str()),
            (_, MigrationItem::Edge(edge)) => to_edge_fields.get(edge.as_str()),
            (_, MigrationItem::Vector(vector)) => to_vector_fields.get(vector.as_str()),
        } {
            Some(fields) => fields,
            None => {
                // schema error - item does not exist
                panic!("item does not exist");
            }
        };

        // for now assert that from and to fields are the same type
        // TODO: add support for migrating actual item types
        if item.from_item.1 != item.to_item.1 {
            // schema error - item types do not match
            panic!("item types do not match");
        }

        let mut generated_migration_item_mapping = GeneratedMigrationItemMapping {
            from_item: item.from_item.1.inner().to_string(),
            to_item: item.to_item.1.inner().to_string(),
            remappings: Vec::new(),
            should_spread: true,
        };

        for MigrationPropertyMapping {
            property_name,
            property_value,
            default,
            cast,
            loc: _,
        } in &item.remappings
        {

            // check the new property exists in to version schema
            let to_property_field = match to_fields.get(property_name.1.as_str()) {
                Some(field) => field,
                None => {
                    // schema error - property does not exist in to version
                    panic!("property does not exist in to version");
                }
            };

            // check the property value is valid for the new field type

            match &property_value.value {
                // if property value is a literal, check it is valid for the new field type
                FieldValueType::Literal(literal) => {
                    if to_property_field.field_type != *literal {
                        // schema error - property value is not valid for the new field type
                        panic!("property value is not valid for the new field type");
                    }
                }
                FieldValueType::Identifier(identifier) => {
                    // check the identifier is valid for the new field type
                    if from_fields.get(identifier.as_str()).is_none() {
                        // schema error - identifier does not exist in from version
                        panic!("identifier does not exist in from version");
                    }
                }
                _ => todo!(),
            }

            // check default value is valid for the new field type
            if let Some(default) = &default {
                if to_property_field.field_type != *default {
                    // schema error - default value is not valid for the new field type
                    panic!("default value is not valid for the new field type");
                }
            }

            // check the cast is valid for the new field type
            if let Some(cast) = &cast {
                if to_property_field.field_type != cast.cast_to {
                    // schema error - cast is not valid for the new field type
                    panic!("cast is not valid for the new field type");
                }
            }

            // // warnings if name is same
            // // warnings if numeric type cast is smaller than existing type

            // generate migration

            match &cast {
                Some(cast) => {
                    generated_migration_item_mapping
                        .remappings
                        .push(Separator::Semicolon(
                            GeneratedMigrationPropertyMapping::FieldTypeCast {
                                field: GeneratedValue::Literal(GenRef::Literal(
                                    property_name.1.to_string(),
                                )),
                                cast: cast.cast_to.clone().into(),
                            },
                        ))
                }

                None => {
                    match &property_value.value {
                        FieldValueType::Literal(literal) => {
                            if to_property_field.field_type != *literal {
                                // schema error - property value is not valid for the new field type
                                panic!("property value is not valid for the new field type");
                            }
                            generated_migration_item_mapping
                                .remappings
                                .push(Separator::Semicolon(
                                    GeneratedMigrationPropertyMapping::FieldAdditionFromValue {
                                        new_field: GeneratedValue::Literal(GenRef::Literal(
                                            property_name.1.to_string(),
                                        )),
                                        value: GeneratedValue::Literal(match literal {
                                            Value::String(s) => GenRef::Literal(s.to_string()),
                                            other => GenRef::Std(other.to_string()),
                                        }),
                                    },
                                ));
                        }
                        FieldValueType::Identifier(identifier) => {
                            if from_fields.get(identifier.as_str()).is_none() {
                                // schema error - identifier does not exist in from version
                                panic!("identifier does not exist in from version");
                            }
                            generated_migration_item_mapping
                                .remappings
                                .push(Separator::Semicolon(
                                    GeneratedMigrationPropertyMapping::FieldAdditionFromOldField {
                                        old_field: match &property_value.value {
                                            FieldValueType::Literal(literal) => {
                                                GeneratedValue::Literal(GenRef::Literal(
                                                    literal.to_string(),
                                                ))
                                            }
                                            FieldValueType::Identifier(identifier) => {
                                                GeneratedValue::Identifier(GenRef::Literal(
                                                    identifier.to_string(),
                                                ))
                                            }
                                            _ => todo!(),
                                        },
                                        new_field: GeneratedValue::Literal(GenRef::Literal(
                                            property_name.1.to_string(),
                                        )),
                                    },
                                ));
                        }
                        _ => todo!(),
                    }
                }
            };
        }

        item_mappings.push(generated_migration_item_mapping);
    }

    ctx.output.migrations.push(GeneratedMigration {
        from_version: migration.from_version.1.to_string(),
        to_version: migration.to_version.1.to_string(),
        body: item_mappings,
    });
}
