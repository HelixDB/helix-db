use crate::{
    helixc::{
        generator::utils::{GeneratedValue, Separator},
        parser::types::FieldType,
    },
    protocol::value::casting::CastType,
};

#[derive(Debug)]
pub struct GeneratedMigration {
    pub from_version: String,
    pub to_version: String,
    pub body: Vec<GeneratedMigrationItemMapping>,
}
#[derive(Debug)]
pub struct GeneratedMigrationItemMapping {
    pub from_item: String,
    pub to_item: String,
    pub remappings: Vec<Separator<GeneratedMigrationPropertyMapping>>,
    pub should_spread: bool,
}

#[derive(Debug)]
pub enum GeneratedMigrationPropertyMapping {
    FieldAdditionFromOldField {
        old_field: GeneratedValue,
        new_field: GeneratedValue,
    },
    FieldAdditionFromValue {
        new_field_name: GeneratedValue,
        new_field_type: FieldType,
        value: GeneratedValue,
    },
    FieldTypeCast {
        field: GeneratedValue,
        cast: CastType,
    },
}

impl std::fmt::Display for GeneratedMigration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for item in self.body.iter() {
            writeln!(
                f,
                "#[migration({}, {} -> {})]",
                item.from_item, self.from_version, self.to_version
            )?;
            writeln!(
                f,
                "pub fn migration_{}_{}_{}(mut props: HashMap<String, Value>) -> HashMap<String, Value> {{",
                item.from_item.to_ascii_lowercase(),
                self.from_version,
                self.to_version
            )?;
            writeln!(f, "let mut new_props = HashMap::new();")?;
            for remapping in item.remappings.iter() {
                writeln!(f, "{remapping}")?;
            }
            writeln!(f, "new_props")?;
            writeln!(f, "}}")?;
        }
        Ok(())
    }
}

impl std::fmt::Display for GeneratedMigrationPropertyMapping {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::FieldAdditionFromOldField {
                old_field,
                new_field,
            } => write!(
                f,
                "field_addition_from_old_field!(&mut props, &mut new_props, {new_field}, {old_field})"
            ),
            Self::FieldAdditionFromValue {
                new_field_name,
                new_field_type,
                value,
            } => {
                write!(
                    f,
                    "field_addition_from_value!(&mut new_props, {new_field_name}, {new_field_type}, {value})"
                )
            }
            Self::FieldTypeCast { field, cast } => {
                write!(
                    f,
                    "field_type_cast!(&mut props, &mut new_props, {field}, {cast})"
                )
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::helixc::generator::utils::GenRef;

    // ============================================================================
    // GeneratedMigrationPropertyMapping Tests
    // ============================================================================

    #[test]
    fn test_field_addition_from_old_field() {
        let mapping = GeneratedMigrationPropertyMapping::FieldAdditionFromOldField {
            old_field: GeneratedValue::Literal(GenRef::Literal("old_name".to_string())),
            new_field: GeneratedValue::Literal(GenRef::Literal("new_name".to_string())),
        };
        let output = format!("{}", mapping);
        assert!(output.contains("field_addition_from_old_field!"));
        assert!(output.contains("\"new_name\""));
        assert!(output.contains("\"old_name\""));
    }

    #[test]
    fn test_field_addition_from_value() {
        let mapping = GeneratedMigrationPropertyMapping::FieldAdditionFromValue {
            new_field_name: GeneratedValue::Literal(GenRef::Literal("status".to_string())),
            new_field_type: FieldType::String,
            value: GeneratedValue::Literal(GenRef::Literal("active".to_string())),
        };
        let output = format!("{}", mapping);
        assert!(output.contains("field_addition_from_value!"));
        assert!(output.contains("\"status\""));
        assert!(output.contains("\"active\""));
    }

    #[test]
    fn test_field_type_cast() {
        let mapping = GeneratedMigrationPropertyMapping::FieldTypeCast {
            field: GeneratedValue::Literal(GenRef::Literal("count".to_string())),
            cast: CastType::U32,
        };
        let output = format!("{}", mapping);
        assert!(output.contains("field_type_cast!"));
        assert!(output.contains("\"count\""));
    }

    // ============================================================================
    // GeneratedMigration Tests
    // ============================================================================

    #[test]
    fn test_migration_display_basic() {
        let migration = GeneratedMigration {
            from_version: "v1".to_string(),
            to_version: "v2".to_string(),
            body: vec![GeneratedMigrationItemMapping {
                from_item: "User".to_string(),
                to_item: "User".to_string(),
                remappings: vec![Separator::Semicolon(
                    GeneratedMigrationPropertyMapping::FieldAdditionFromOldField {
                        old_field: GeneratedValue::Literal(GenRef::Literal("name".to_string())),
                        new_field: GeneratedValue::Literal(GenRef::Literal(
                            "full_name".to_string(),
                        )),
                    },
                )],
                should_spread: false,
            }],
        };

        let output = format!("{}", migration);
        assert!(output.contains("#[migration(User, v1 -> v2)]"));
        assert!(output.contains("pub fn migration_user_v1_v2"));
        assert!(output.contains("HashMap<String, Value>"));
    }

    #[test]
    fn test_migration_function_signature() {
        let migration = GeneratedMigration {
            from_version: "v1".to_string(),
            to_version: "v2".to_string(),
            body: vec![GeneratedMigrationItemMapping {
                from_item: "Product".to_string(),
                to_item: "Product".to_string(),
                remappings: vec![],
                should_spread: false,
            }],
        };

        let output = format!("{}", migration);
        assert!(output.contains("migration_product_v1_v2"));
        assert!(output.contains("mut props: HashMap<String, Value>"));
        assert!(output.contains("let mut new_props = HashMap::new();"));
        assert!(output.contains("new_props"));
    }

    #[test]
    fn test_migration_with_multiple_remappings() {
        let migration = GeneratedMigration {
            from_version: "1".to_string(),
            to_version: "2".to_string(),
            body: vec![GeneratedMigrationItemMapping {
                from_item: "Node".to_string(),
                to_item: "Node".to_string(),
                remappings: vec![
                    Separator::Semicolon(
                        GeneratedMigrationPropertyMapping::FieldAdditionFromOldField {
                            old_field: GeneratedValue::Literal(GenRef::Literal("a".to_string())),
                            new_field: GeneratedValue::Literal(GenRef::Literal("b".to_string())),
                        },
                    ),
                    Separator::Semicolon(
                        GeneratedMigrationPropertyMapping::FieldAdditionFromValue {
                            new_field_name: GeneratedValue::Literal(GenRef::Literal(
                                "c".to_string(),
                            )),
                            new_field_type: FieldType::Boolean,
                            value: GeneratedValue::Primitive(GenRef::Std("true".to_string())),
                        },
                    ),
                ],
                should_spread: false,
            }],
        };

        let output = format!("{}", migration);
        assert!(output.contains("field_addition_from_old_field!"));
        assert!(output.contains("field_addition_from_value!"));
    }

    #[test]
    fn test_migration_empty_remappings() {
        let migration = GeneratedMigration {
            from_version: "v1".to_string(),
            to_version: "v2".to_string(),
            body: vec![GeneratedMigrationItemMapping {
                from_item: "Empty".to_string(),
                to_item: "Empty".to_string(),
                remappings: vec![],
                should_spread: false,
            }],
        };

        let output = format!("{}", migration);
        assert!(output.contains("#[migration(Empty, v1 -> v2)]"));
        assert!(output.contains("migration_empty_v1_v2"));
    }

    #[test]
    fn test_migration_item_name_lowercase() {
        let migration = GeneratedMigration {
            from_version: "v1".to_string(),
            to_version: "v2".to_string(),
            body: vec![GeneratedMigrationItemMapping {
                from_item: "MyEntity".to_string(),
                to_item: "MyEntity".to_string(),
                remappings: vec![],
                should_spread: false,
            }],
        };

        let output = format!("{}", migration);
        // Function name should be lowercase
        assert!(output.contains("migration_myentity_v1_v2"));
    }

    #[test]
    fn test_migration_version_format() {
        let migration = GeneratedMigration {
            from_version: "2024_01".to_string(),
            to_version: "2024_02".to_string(),
            body: vec![GeneratedMigrationItemMapping {
                from_item: "Schema".to_string(),
                to_item: "Schema".to_string(),
                remappings: vec![],
                should_spread: false,
            }],
        };

        let output = format!("{}", migration);
        assert!(output.contains("2024_01 -> 2024_02"));
        assert!(output.contains("migration_schema_2024_01_2024_02"));
    }

    #[test]
    fn test_migration_with_type_cast() {
        let mapping = GeneratedMigrationPropertyMapping::FieldTypeCast {
            field: GeneratedValue::Literal(GenRef::Literal("age".to_string())),
            cast: CastType::I64,
        };
        let output = format!("{}", mapping);
        assert!(output.contains("field_type_cast!"));
        assert!(output.contains("&mut props"));
        assert!(output.contains("&mut new_props"));
    }

    #[test]
    fn test_migration_multiple_items() {
        let migration = GeneratedMigration {
            from_version: "v1".to_string(),
            to_version: "v2".to_string(),
            body: vec![
                GeneratedMigrationItemMapping {
                    from_item: "User".to_string(),
                    to_item: "User".to_string(),
                    remappings: vec![],
                    should_spread: false,
                },
                GeneratedMigrationItemMapping {
                    from_item: "Post".to_string(),
                    to_item: "Post".to_string(),
                    remappings: vec![],
                    should_spread: false,
                },
            ],
        };

        let output = format!("{}", migration);
        assert!(output.contains("migration_user_v1_v2"));
        assert!(output.contains("migration_post_v1_v2"));
    }

    #[test]
    fn test_field_addition_format() {
        let mapping = GeneratedMigrationPropertyMapping::FieldAdditionFromOldField {
            old_field: GeneratedValue::Primitive(GenRef::Std("old_id".to_string())),
            new_field: GeneratedValue::Primitive(GenRef::Std("id".to_string())),
        };
        let output = format!("{}", mapping);
        assert!(output.contains("&mut props"));
        assert!(output.contains("&mut new_props"));
        assert!(output.contains("old_id"));
    }
}
