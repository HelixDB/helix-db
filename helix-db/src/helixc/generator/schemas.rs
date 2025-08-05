use core::fmt;
use std::fmt::Display;

use crate::helixc::{generator::{tsdisplay::ToTypeScript, utils::{GeneratedType, GeneratedValue}}, parser::helix_parser::FieldPrefix};

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
    pub is_index: FieldPrefix,
}