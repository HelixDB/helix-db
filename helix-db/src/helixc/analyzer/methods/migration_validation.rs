use crate::{
    helixc::{
        analyzer::{Ctx, error_codes::ErrorCode, errors::push_schema_err},
        generator::{
            migrations::{
                GeneratedMigration, GeneratedMigrationItemMapping,
                GeneratedMigrationPropertyMapping,
            },
            utils::{GenRef, GeneratedValue, Separator},
        },
        parser::types::{
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
        push_schema_err(
            ctx,
            migration.from_version.0.clone(),
            ErrorCode::E108,
            format!("Migration references non-existent schema version: {}", migration.from_version.1),
            Some("Ensure the schema version exists before referencing it in a migration".into()),
        );
        return;
    }
    // check to version exists and is 1 greater than from version
    if !ctx
        .all_schemas
        .inner()
        .contains_key(&migration.to_version.1)
    {
        push_schema_err(
            ctx,
            migration.to_version.0.clone(),
            ErrorCode::E108,
            format!("Migration references non-existent schema version: {}", migration.to_version.1),
            Some("Ensure the schema version exists before referencing it in a migration".into()),
        );
        return;
    }

    // We've already validated these exist above, so unwrap is safe here
    // Clone the fields to avoid borrow checker issues when we need to mutably borrow ctx later
    let (from_node_fields, from_edge_fields, from_vector_fields) = ctx
        .all_schemas
        .inner()
        .get(&migration.from_version.1)
        .expect("Schema version was validated to exist")
        .clone();

    let (to_node_fields, to_edge_fields, to_vector_fields) = ctx
        .all_schemas
        .inner()
        .get(&migration.to_version.1)
        .expect("Schema version was validated to exist")
        .clone();

    // for each migration item mapping
    let mut item_mappings = Vec::new();
    for item in &migration.body {
        // get from fields and to fields and check they exist in respective versions

        let from_fields = match match &item.from_item {
            (_, MigrationItem::Node(node)) => from_node_fields.get(node.as_str()),
            (_, MigrationItem::Edge(edge)) => from_edge_fields.get(edge.as_str()),
            (_, MigrationItem::Vector(vector)) => from_vector_fields.get(vector.as_str()),
        } {
            Some(fields) => fields,
            None => {
                let item_name = item.from_item.1.inner();
                push_schema_err(
                    ctx,
                    item.from_item.0.clone(),
                    ErrorCode::E201,
                    format!("Migration item '{item_name}' does not exist in schema version {}", migration.from_version.1),
                    Some(format!("Ensure '{item_name}' is defined in the source schema")),
                );
                continue;
            }
        };

        let to_fields = match match &item.to_item {
            (_, MigrationItem::Node(node)) => to_node_fields.get(node.as_str()),
            (_, MigrationItem::Edge(edge)) => to_edge_fields.get(edge.as_str()),
            (_, MigrationItem::Vector(vector)) => to_vector_fields.get(vector.as_str()),
        } {
            Some(fields) => fields,
            None => {
                let item_name = item.to_item.1.inner();
                push_schema_err(
                    ctx,
                    item.to_item.0.clone(),
                    ErrorCode::E201,
                    format!("Migration item '{item_name}' does not exist in schema version {}", migration.to_version.1),
                    Some(format!("Ensure '{item_name}' is defined in the target schema")),
                );
                continue;
            }
        };

        // for now assert that from and to fields are the same type
        // TODO: add support for migrating actual item types
        if item.from_item.1 != item.to_item.1 {
            push_schema_err(
                ctx,
                item.loc.clone(),
                ErrorCode::E205,
                format!("Migration item types do not match: '{}' to '{}'",
                    item.from_item.1.inner(), item.to_item.1.inner()),
                Some("Migration between different item types is not yet supported".into()),
            );
            continue;
        }

        let mut generated_migration_item_mapping = GeneratedMigrationItemMapping {
            from_item: item.from_item.1.inner().to_string(),
            to_item: item.to_item.1.inner().to_string(),
            remappings: Vec::new(),
            should_spread: item.should_spread,
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
                    push_schema_err(
                        ctx,
                        property_name.0.clone(),
                        ErrorCode::E202,
                        format!("Property '{}' does not exist in target schema for '{}'",
                            property_name.1, item.to_item.1.inner()),
                        Some(format!("Ensure property '{}' is defined in the target schema", property_name.1)),
                    );
                    continue;
                }
            };

            // check the property value is valid for the new field type

            match &property_value.value {
                // if property value is a literal, check it is valid for the new field type
                FieldValueType::Literal(literal) => {
                    if to_property_field.field_type != *literal {
                        push_schema_err(
                            ctx,
                            property_value.loc.clone(),
                            ErrorCode::E205,
                            format!("Property value type mismatch: expected '{}' but got '{}'",
                                to_property_field.field_type, literal.to_variant_string()),
                            Some("Ensure the property value type matches the field type in the target schema".into()),
                        );
                        continue;
                    }
                }
                FieldValueType::Identifier(identifier) => {
                    // check the identifier is valid for the new field type
                    if from_fields.get(identifier.as_str()).is_none() {
                        push_schema_err(
                            ctx,
                            property_value.loc.clone(),
                            ErrorCode::E202,
                            format!("Identifier '{}' does not exist in source schema for '{}'",
                                identifier, item.from_item.1.inner()),
                            Some(format!("Ensure '{identifier}' is a valid field in the source schema")),
                        );
                        continue;
                    }
                }
                _ => {
                    push_schema_err(
                        ctx,
                        property_value.loc.clone(),
                        ErrorCode::E206,
                        "Unsupported property value type in migration".into(),
                        Some("Only literal values and identifiers are supported in migrations".into()),
                    );
                    continue;
                }
            }

            // check default value is valid for the new field type
            if let Some(default) = &default
                && to_property_field.field_type != *default {
                    push_schema_err(
                        ctx,
                        property_value.loc.clone(),
                        ErrorCode::E205,
                        format!("Default value type mismatch: expected '{}' but got '{:?}'",
                            to_property_field.field_type, default),
                        Some("Ensure the default value type matches the field type in the target schema".into()),
                    );
                    continue;
            }

            // check the cast is valid for the new field type
            if let Some(cast) = &cast
                && to_property_field.field_type != cast.cast_to {
                    push_schema_err(
                        ctx,
                        cast.loc.clone(),
                        ErrorCode::E205,
                        format!("Cast target type mismatch: expected '{}' but got '{}'",
                            to_property_field.field_type, cast.cast_to),
                        Some("Ensure the cast target type matches the field type in the target schema".into()),
                    );
                    continue;
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
                            // Already validated above, can proceed with generation
                            generated_migration_item_mapping
                                .remappings
                                .push(Separator::Semicolon(
                                    GeneratedMigrationPropertyMapping::FieldAdditionFromValue {
                                        new_field_name: GeneratedValue::Literal(GenRef::Literal(
                                            property_name.1.to_string(),
                                        )),
                                        new_field_type: to_property_field.field_type.clone(),
                                        value: GeneratedValue::Literal(match literal {
                                            Value::String(s) => GenRef::Literal(s.to_string()),
                                            other => GenRef::Std(other.inner_stringify()),
                                        }),
                                    },
                                ));
                        }
                        FieldValueType::Identifier(_identifier) => {
                            // Already validated above, can proceed with generation
                            generated_migration_item_mapping
                                .remappings
                                .push(Separator::Semicolon(
                                    GeneratedMigrationPropertyMapping::FieldAdditionFromOldField {
                                        old_field: match &property_value.value {
                                            FieldValueType::Literal(literal) => {
                                                GeneratedValue::Literal(GenRef::Literal(
                                                    literal.inner_stringify(),
                                                ))
                                            }
                                            FieldValueType::Identifier(identifier) => {
                                                GeneratedValue::Identifier(GenRef::Literal(
                                                    identifier.to_string(),
                                                ))
                                            }
                                            _ => {
                                                // This case should have been caught and continued earlier
                                                GeneratedValue::Unknown
                                            }
                                        },
                                        new_field: GeneratedValue::Literal(GenRef::Literal(
                                            property_name.1.to_string(),
                                        )),
                                    },
                                ));
                        }
                        _ => {
                            // This case should have been caught and continued earlier in validation
                            // Just skip generation for unsupported types
                        }
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
