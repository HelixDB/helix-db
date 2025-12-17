use core::fmt;
use std::fmt::Display;

use crate::helixc::{
    generator::{
        tsdisplay::ToTypeScript,
        utils::{GeneratedType, GeneratedValue},
    },
    parser::types::FieldPrefix,
};

#[derive(Clone)]
pub struct NodeSchema {
    pub name: String,
    pub properties: Vec<SchemaProperty>,
}
impl Display for NodeSchema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "pub struct {} {{", self.name)?;
        for property in &self.properties {
            writeln!(f, "    pub {}: {},", property.name, property.field_type)?;
        }
        writeln!(f, "}}")
    }
}
impl ToTypeScript for NodeSchema {
    fn to_typescript(&self) -> String {
        let mut result = format!("interface {} {{\n", self.name);
        result.push_str("  id: string;\n");

        for property in &self.properties {
            result.push_str(&format!(
                "  {}: {};\n",
                property.name,
                match &property.field_type {
                    GeneratedType::RustType(t) => t.to_ts(),
                    _ => unreachable!(),
                }
            ));
        }

        result.push_str("}\n");
        result
    }
}

#[derive(Clone)]
pub struct EdgeSchema {
    pub name: String,
    pub from: String,
    pub to: String,
    pub is_unique: bool,
    pub properties: Vec<SchemaProperty>,
}
impl Display for EdgeSchema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "pub struct {} {{", self.name)?;
        writeln!(f, "    pub from: {},", self.from)?;
        writeln!(f, "    pub to: {},", self.to)?;
        for property in &self.properties {
            writeln!(f, "    pub {}: {},", property.name, property.field_type)?;
        }
        writeln!(f, "}}")
    }
}
impl ToTypeScript for VectorSchema {
    fn to_typescript(&self) -> String {
        let mut result = format!("interface {} {{\n", self.name);
        result.push_str("  id: string;\n");
        result.push_str("  data: Array<number>;\n");

        for property in &self.properties {
            result.push_str(&format!(
                "  {}: {};\n",
                property.name,
                match &property.field_type {
                    GeneratedType::RustType(t) => t.to_ts(),
                    _ => unreachable!(),
                }
            ));
        }

        result.push_str("}\n");
        result
    }
}
#[derive(Clone)]
pub struct VectorSchema {
    pub name: String,
    pub properties: Vec<SchemaProperty>,
}
impl Display for VectorSchema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "pub struct {} {{", self.name)?;
        for property in &self.properties {
            writeln!(f, "    pub {}: {},", property.name, property.field_type)?;
        }
        writeln!(f, "}}")
    }
}
impl ToTypeScript for EdgeSchema {
    fn to_typescript(&self) -> String {
        let properties_str = self
            .properties
            .iter()
            .map(|p| {
                format!(
                    "    {}: {}",
                    p.name,
                    match &p.field_type {
                        GeneratedType::RustType(t) => t.to_ts(),
                        _ => unreachable!(),
                    }
                )
            })
            .collect::<Vec<_>>()
            .join(";");

        format!(
            "interface {} {{\n  id: string;\n  from: {};\n  to: {};\n  properties: {{\n\t{}\n}};\n}}\n",
            self.name, self.from, self.to, properties_str
        )
    }
}

#[derive(Clone)]
pub struct SchemaProperty {
    pub name: String,
    pub field_type: GeneratedType,
    pub default_value: Option<GeneratedValue>,
    // pub is_optional: bool,
    pub field_prefix: FieldPrefix,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::helixc::generator::utils::RustType;

    // ============================================================================
    // NodeSchema Tests
    // ============================================================================

    #[test]
    fn test_node_schema_display_simple() {
        let schema = NodeSchema {
            name: "Person".to_string(),
            properties: vec![
                SchemaProperty {
                    name: "name".to_string(),
                    field_type: GeneratedType::RustType(RustType::String),
                    default_value: None,
                    field_prefix: FieldPrefix::Empty,
                },
                SchemaProperty {
                    name: "age".to_string(),
                    field_type: GeneratedType::RustType(RustType::U32),
                    default_value: None,
                    field_prefix: FieldPrefix::Empty,
                },
            ],
        };

        let output = format!("{}", schema);
        assert!(output.contains("pub struct Person {"));
        assert!(output.contains("pub name: String,"));
        assert!(output.contains("pub age: u32,"));
    }

    #[test]
    fn test_node_schema_display_empty_properties() {
        let schema = NodeSchema {
            name: "Empty".to_string(),
            properties: vec![],
        };

        let output = format!("{}", schema);
        assert!(output.contains("pub struct Empty {"));
        assert!(output.contains("}"));
    }

    #[test]
    fn test_node_schema_typescript_generation() {
        let schema = NodeSchema {
            name: "User".to_string(),
            properties: vec![
                SchemaProperty {
                    name: "email".to_string(),
                    field_type: GeneratedType::RustType(RustType::String),
                    default_value: None,
                    field_prefix: FieldPrefix::Empty,
                },
                SchemaProperty {
                    name: "active".to_string(),
                    field_type: GeneratedType::RustType(RustType::Bool),
                    default_value: None,
                    field_prefix: FieldPrefix::Empty,
                },
            ],
        };

        let output = schema.to_typescript();
        assert!(output.contains("interface User {"));
        assert!(output.contains("id: string;"));
        assert!(output.contains("email: string;"));
        assert!(output.contains("active: boolean;"));
    }

    #[test]
    fn test_node_schema_with_numeric_types() {
        let schema = NodeSchema {
            name: "Stats".to_string(),
            properties: vec![
                SchemaProperty {
                    name: "count".to_string(),
                    field_type: GeneratedType::RustType(RustType::I32),
                    default_value: None,
                    field_prefix: FieldPrefix::Empty,
                },
                SchemaProperty {
                    name: "score".to_string(),
                    field_type: GeneratedType::RustType(RustType::F64),
                    default_value: None,
                    field_prefix: FieldPrefix::Empty,
                },
            ],
        };

        let output = format!("{}", schema);
        assert!(output.contains("pub count: i32,"));
        assert!(output.contains("pub score: f64,"));
    }

    // ============================================================================
    // EdgeSchema Tests
    // ============================================================================

    #[test]
    fn test_edge_schema_display_simple() {
        let schema = EdgeSchema {
            name: "Knows".to_string(),
            from: "Person".to_string(),
            to: "Person".to_string(),
            properties: vec![SchemaProperty {
                name: "since".to_string(),
                field_type: GeneratedType::RustType(RustType::U32),
                default_value: None,
                field_prefix: FieldPrefix::Empty,
            }],
            is_unique: false,
        };

        let output = format!("{}", schema);
        assert!(output.contains("pub struct Knows {"));
        assert!(output.contains("pub from: Person,"));
        assert!(output.contains("pub to: Person,"));
        assert!(output.contains("pub since: u32,"));
    }

    #[test]
    fn test_edge_schema_display_no_properties() {
        let schema = EdgeSchema {
            name: "Follows".to_string(),
            from: "User".to_string(),
            to: "User".to_string(),
            properties: vec![],
            is_unique: false,
        };

        let output = format!("{}", schema);
        assert!(output.contains("pub struct Follows {"));
        assert!(output.contains("pub from: User,"));
        assert!(output.contains("pub to: User,"));
    }

    #[test]
    fn test_edge_schema_typescript_generation() {
        let schema = EdgeSchema {
            name: "WorksAt".to_string(),
            from: "Person".to_string(),
            to: "Company".to_string(),
            properties: vec![SchemaProperty {
                name: "role".to_string(),
                field_type: GeneratedType::RustType(RustType::String),
                default_value: None,
                field_prefix: FieldPrefix::Empty,
            }],
            is_unique: false,
        };

        let output = schema.to_typescript();
        assert!(output.contains("interface WorksAt {"));
        assert!(output.contains("from: Person;"));
        assert!(output.contains("to: Company;"));
        assert!(output.contains("role: string"));
    }

    #[test]
    fn test_edge_schema_with_multiple_properties() {
        let schema = EdgeSchema {
            name: "Rated".to_string(),
            from: "User".to_string(),
            to: "Movie".to_string(),
            is_unique: false,
            properties: vec![
                SchemaProperty {
                    name: "rating".to_string(),
                    field_type: GeneratedType::RustType(RustType::F32),
                    default_value: None,
                    field_prefix: FieldPrefix::Empty,
                },
                SchemaProperty {
                    name: "comment".to_string(),
                    field_type: GeneratedType::RustType(RustType::String),
                    default_value: None,
                    field_prefix: FieldPrefix::Empty,
                },
            ],
        };

        let output = format!("{}", schema);
        assert!(output.contains("pub rating: f32,"));
        assert!(output.contains("pub comment: String,"));
    }

    // ============================================================================
    // VectorSchema Tests
    // ============================================================================

    #[test]
    fn test_vector_schema_display_simple() {
        let schema = VectorSchema {
            name: "Embedding".to_string(),
            properties: vec![SchemaProperty {
                name: "metadata".to_string(),
                field_type: GeneratedType::RustType(RustType::String),
                default_value: None,
                field_prefix: FieldPrefix::Empty,
            }],
        };

        let output = format!("{}", schema);
        assert!(output.contains("pub struct Embedding {"));
        assert!(output.contains("pub metadata: String,"));
    }

    #[test]
    fn test_vector_schema_display_empty_properties() {
        let schema = VectorSchema {
            name: "Vector".to_string(),
            properties: vec![],
        };

        let output = format!("{}", schema);
        assert!(output.contains("pub struct Vector {"));
        assert!(output.contains("}"));
    }

    #[test]
    fn test_vector_schema_typescript_generation() {
        let schema = VectorSchema {
            name: "DocVector".to_string(),
            properties: vec![
                SchemaProperty {
                    name: "source".to_string(),
                    field_type: GeneratedType::RustType(RustType::String),
                    default_value: None,
                    field_prefix: FieldPrefix::Empty,
                },
                SchemaProperty {
                    name: "chunk_index".to_string(),
                    field_type: GeneratedType::RustType(RustType::U32),
                    default_value: None,
                    field_prefix: FieldPrefix::Empty,
                },
            ],
        };

        let output = schema.to_typescript();
        assert!(output.contains("interface DocVector {"));
        assert!(output.contains("id: string;"));
        assert!(output.contains("data: Array<number>;"));
        assert!(output.contains("source: string;"));
        assert!(output.contains("chunk_index: number;"));
    }

    #[test]
    fn test_vector_schema_with_bool_property() {
        let schema = VectorSchema {
            name: "FeatureVector".to_string(),
            properties: vec![SchemaProperty {
                name: "is_normalized".to_string(),
                field_type: GeneratedType::RustType(RustType::Bool),
                default_value: None,
                field_prefix: FieldPrefix::Empty,
            }],
        };

        let output = format!("{}", schema);
        assert!(output.contains("pub is_normalized: bool,"));
    }
}
