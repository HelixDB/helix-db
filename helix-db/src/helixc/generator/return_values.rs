use core::fmt;
use std::fmt::Display;

use crate::helixc::generator::utils::RustType;

use super::utils::GenRef;

/// Represents a return value field with enhanced metadata
#[derive(Clone, Debug)]
pub struct ReturnValueField {
    pub name: String,
    pub field_type: String,
    pub is_implicit: bool,         // id, label, from_node, to_node, data, score
    pub is_nested_traversal: bool, // Whether this field contains a nested traversal
    pub nested_struct_name: Option<String>, // Name of nested struct type if applicable
}

impl ReturnValueField {
    pub fn new(name: String, field_type: String) -> Self {
        Self {
            name,
            field_type,
            is_implicit: false,
            is_nested_traversal: false,
            nested_struct_name: None,
        }
    }

    pub fn with_implicit(mut self, is_implicit: bool) -> Self {
        self.is_implicit = is_implicit;
        self
    }

    pub fn with_nested_traversal(mut self, nested_struct_name: String) -> Self {
        self.is_nested_traversal = true;
        self.nested_struct_name = Some(nested_struct_name);
        self
    }
}

/// Represents a generated struct for return types
#[derive(Clone, Debug)]
pub struct ReturnValueStruct {
    pub name: String,
    pub fields: Vec<ReturnValueField>,
    pub has_lifetime: bool,         // Whether to add 'a lifetime parameter
    pub is_query_return_type: bool, // True for the main QueryReturnType
    pub is_collection: bool,        // True if this returns Vec<T>, false for single T
    pub is_aggregate: bool,         // True for aggregate/group_by returns
    pub is_group_by: bool,          // True for GROUP_BY, false for AGGREGATE_BY
    pub source_variable: String,    // Variable name this struct is built from
    pub is_reused_variable: bool,   // True if source variable is referenced multiple times
    pub field_infos: Vec<ReturnFieldInfo>, // Original field info for nested struct generation
    pub aggregate_properties: Vec<String>, // Properties to group by (for closure-style aggregates)
    pub is_count_aggregate: bool,   // True for COUNT mode aggregates
    pub closure_param_name: Option<String>, // HQL closure parameter name (e.g., "e" from entries::|e|)
}

impl ReturnValueStruct {
    pub fn new(name: String) -> Self {
        Self {
            name,
            fields: Vec::new(),
            has_lifetime: true,
            is_query_return_type: false,
            is_collection: false,
            is_aggregate: false,
            is_group_by: false,
            source_variable: String::new(),
            is_reused_variable: false,
            field_infos: Vec::new(),
            aggregate_properties: Vec::new(),
            is_count_aggregate: false,
            closure_param_name: None,
        }
    }

    pub fn with_fields(mut self, fields: Vec<ReturnValueField>) -> Self {
        self.fields = fields;
        self
    }

    pub fn with_lifetime(mut self, has_lifetime: bool) -> Self {
        self.has_lifetime = has_lifetime;
        self
    }

    pub fn as_query_return_type(mut self) -> Self {
        self.is_query_return_type = true;
        self
    }

    pub fn with_collection(mut self, is_collection: bool) -> Self {
        self.is_collection = is_collection;
        self
    }

    pub fn with_source_variable(mut self, source_variable: String) -> Self {
        self.source_variable = source_variable;
        self
    }

    pub fn with_reused_variable(mut self, is_reused: bool) -> Self {
        self.is_reused_variable = is_reused;
        self
    }

    /// Generate the struct definition as a string
    pub fn generate_struct_def(&self) -> String {
        let mut output = String::new();

        // Generate derive attributes
        output.push_str("#[derive(Serialize, Default)]\n");

        // Generate struct declaration
        if self.has_lifetime {
            output.push_str(&format!("pub struct {}<'a> {{\n", self.name));
        } else {
            output.push_str(&format!("pub struct {} {{\n", self.name));
        }

        // Generate fields
        for field in &self.fields {
            if self.has_lifetime {
                output.push_str(&format!("    pub {}: {},\n", field.name, field.field_type));
            } else {
                // Remove lifetime parameters if not needed
                let field_type = field.field_type.replace("<'a>", "");
                output.push_str(&format!("    pub {}: {},\n", field.name, field_type));
            }
        }

        output.push_str("}\n");
        output
    }

    /// Generate code to construct an instance of this struct from the source variable
    pub fn generate_struct_construction(&self) -> String {
        let singular_var = self.source_variable.trim_end_matches('s');

        // Generate the struct construction body
        let mut body = format!("{} {{\n", self.name);

        for field in &self.fields {
            let field_value = if field.name == "id" {
                format!("uuid_str({}.id(), &arena)", singular_var)
            } else if field.name == "label" {
                format!("{}.label()", singular_var)
            } else if field.name == "from_node" {
                format!("uuid_str({}.from_node(), &arena)", singular_var)
            } else if field.name == "to_node" {
                format!("uuid_str({}.to_node(), &arena)", singular_var)
            } else if field.name == "data" {
                format!("{}.data()", singular_var)
            } else if field.name == "score" {
                format!("{}.score()", singular_var)
            } else if field.is_nested_traversal {
                // Nested traversal - will be populated by nested G::new() call
                "/* nested traversal */".to_string()
            } else {
                // Regular schema field
                format!("{}.get_property(\"{}\").unwrap()", singular_var, field.name)
            };

            body.push_str(&format!("        {}: {},\n", field.name, field_value));
        }

        body.push_str("    }");

        // Wrap in .map() for collections
        if self.is_collection {
            let iter_method = if self.is_reused_variable {
                "iter().cloned()"
            } else {
                "into_iter()"
            };

            format!(
                "{}.{}.map(|{}| {}).collect::<Vec<_>>()",
                self.source_variable, iter_method, singular_var, body
            )
        } else {
            // Single item - just construct the struct directly
            body.replace(singular_var, &self.source_variable)
        }
    }

    /// Create a ReturnValueStruct from unified field information
    pub fn from_return_fields(
        name: String,
        field_infos: Vec<ReturnFieldInfo>,
        source_variable: String,
        is_collection: bool,
        is_reused: bool,
        is_aggregate: bool,
        is_group_by: bool,
        aggregate_properties: Vec<String>,
        is_count_aggregate: bool,
        closure_param_name: Option<String>,
    ) -> Self {
        // First, recursively build nested structs to determine if they have lifetimes
        let mut nested_has_lifetime = std::collections::HashMap::new();
        for field_info in &field_infos {
            if let ReturnFieldType::Nested(nested_fields) = &field_info.field_type {
                // Use the nested_struct_name from the source if available, otherwise fall back to field name
                let nested_name = if let ReturnFieldSource::NestedTraversal {
                    nested_struct_name: Some(name),
                    ..
                } = &field_info.source
                {
                    name.clone()
                } else {
                    format!("{}ReturnType", capitalize_first(&field_info.name))
                };
                let nested_struct = Self::from_return_fields(
                    nested_name.clone(),
                    nested_fields.clone(),
                    "item".to_string(),
                    false,
                    false,
                    false,      // Nested structs are not aggregates
                    false,      // Not group_by
                    Vec::new(), // No aggregate properties for nested structs
                    false,      // Not count aggregate
                    None,       // Nested structs don't have their own closure param
                );
                nested_has_lifetime.insert(nested_name, nested_struct.has_lifetime);
            }
        }

        let fields = field_infos
            .iter()
            .map(|field_info| {
                let (field_type, _is_nested, nested_name) = match &field_info.field_type {
                    ReturnFieldType::Simple(ty) => (ty.to_string(), false, None),
                    ReturnFieldType::Nested(_) => {
                        // Nested fields become Vec<NestedTypeName> or NestedTypeName (if is_first)
                        // Use the nested_struct_name from the source if available, otherwise fall back to field name
                        let (nested_type_name, is_first) =
                            if let ReturnFieldSource::NestedTraversal {
                                nested_struct_name: Some(name),
                                is_first,
                                ..
                            } = &field_info.source
                            {
                                (name.clone(), *is_first)
                            } else {
                                (
                                    format!("{}ReturnType", capitalize_first(&field_info.name)),
                                    false,
                                )
                            };
                        let has_lt = nested_has_lifetime
                            .get(&nested_type_name)
                            .copied()
                            .unwrap_or(false);
                        // For ::FIRST, use single struct type; otherwise use Vec
                        let type_ref = if is_first {
                            if has_lt {
                                format!("{}<'a>", nested_type_name)
                            } else {
                                nested_type_name.clone()
                            }
                        } else if has_lt {
                            format!("Vec<{}<'a>>", nested_type_name)
                        } else {
                            format!("Vec<{}>", nested_type_name)
                        };
                        (type_ref, true, Some(nested_type_name))
                    }
                };

                ReturnValueField {
                    name: field_info.name.clone(),
                    field_type,
                    is_implicit: matches!(
                        field_info.source,
                        ReturnFieldSource::ImplicitField { .. }
                    ),
                    is_nested_traversal: matches!(
                        field_info.source,
                        ReturnFieldSource::NestedTraversal { .. }
                    ),
                    nested_struct_name: nested_name,
                }
            })
            .collect::<Vec<_>>();

        // Check if any field contains a lifetime parameter
        let has_lifetime = fields.iter().any(|f| f.field_type.contains("'a"));

        let mut struct_def = ReturnValueStruct::new(name);
        struct_def.has_lifetime = has_lifetime;
        struct_def.fields = fields;
        struct_def.source_variable = source_variable;
        struct_def.is_collection = is_collection;
        struct_def.is_aggregate = is_aggregate;
        struct_def.is_group_by = is_group_by;
        struct_def.is_reused_variable = is_reused;
        struct_def.field_infos = field_infos; // Store for nested generation
        struct_def.aggregate_properties = aggregate_properties;
        struct_def.is_count_aggregate = is_count_aggregate;
        struct_def.closure_param_name = closure_param_name;
        struct_def
    }

    /// Recursively generate all struct definitions (including nested ones)
    pub fn generate_all_struct_defs(&self) -> String {
        let mut output = String::new();

        // First, generate nested struct definitions
        for field_info in &self.field_infos {
            if let ReturnFieldType::Nested(nested_fields) = &field_info.field_type {
                // Use the nested_struct_name from the source if available, otherwise fall back to field name
                let nested_name = if let ReturnFieldSource::NestedTraversal {
                    nested_struct_name: Some(name),
                    ..
                } = &field_info.source
                {
                    name.clone()
                } else {
                    format!("{}ReturnType", capitalize_first(&field_info.name))
                };
                let nested_struct = ReturnValueStruct::from_return_fields(
                    nested_name,
                    nested_fields.clone(),
                    "item".to_string(), // Placeholder - actual value comes from traversal
                    false,              // Nested items are not collections themselves
                    false,              // Not reused
                    false,              // Nested structs are not aggregates
                    false,              // Not group_by
                    Vec::new(),         // No aggregate properties for nested structs
                    false,              // Not count aggregate
                    None,               // Nested structs don't have their own closure param
                );
                // Recursively generate nested struct defs
                output.push_str(&nested_struct.generate_all_struct_defs());
                output.push_str("\n\n");
            }
        }

        // Then generate this struct's definition
        output.push_str(&self.generate_struct_def());
        output
    }
}

/// Helper function to capitalize the first letter of a string
fn capitalize_first(s: &str) -> String {
    let mut chars = s.chars();
    match chars.next() {
        None => String::new(),
        Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
    }
}

/// Generate the body of a map closure for struct construction
/// Returns: "StructName { id: uuid_str(val.id(), &arena), ... }"
/// Note: Always uses "val" as the variable name to match closure parameter
pub fn generate_map_closure_body(
    struct_name: &str,
    fields: &[ReturnValueField],
    _singular_var: &str,
) -> String {
    let mut body = format!("{} {{\n", struct_name);

    for field in fields {
        let field_value = if field.name == "id" {
            "uuid_str(val.id(), &arena)".to_string()
        } else if field.name == "label" {
            "val.label()".to_string()
        } else if field.name == "from_node" {
            "uuid_str(val.from_node(), &arena)".to_string()
        } else if field.name == "to_node" {
            "uuid_str(val.to_node(), &arena)".to_string()
        } else if field.name == "data" {
            "val.data()".to_string()
        } else if field.name == "score" {
            "val.score()".to_string()
        } else if field.is_nested_traversal {
            // Nested traversal - will be populated by nested G::new() call
            "/* TODO: nested traversal */".to_string()
        } else {
            // Regular schema field - return Option directly, no unwrap
            format!("val.get_property(\"{}\")", field.name)
        };

        body.push_str(&format!("            {}: {},\n", field.name, field_value));
    }

    body.push_str("        }");
    body
}

/// Represents how a return value should be constructed
#[derive(Clone, Debug)]
pub enum ReturnValueConstruction {
    /// Map a traversal result to a struct
    MapTraversal {
        variable_name: String,
        struct_name: String,
        field_mappings: Vec<FieldMapping>,
    },
    /// Construct a struct from existing variables
    DirectConstruction {
        struct_name: String,
        field_assignments: Vec<(String, String)>, // field_name -> expression
    },
    /// A literal value (for backwards compatibility)
    Literal { value: GenRef<String> },
}

#[derive(Clone, Debug)]
pub struct FieldMapping {
    pub field_name: String,
    pub source: FieldSource,
}

#[derive(Clone, Debug)]
pub enum FieldSource {
    /// Call .id() on the value
    Id,
    /// Call .label() on the value
    Label,
    /// Call .get_property(name) on the value
    Property(String),
    /// Nested traversal that needs to be executed
    NestedTraversal {
        traversal_expr: String,
        inner_mapping: Box<ReturnValueConstruction>,
    },
    /// Reference to another variable
    Variable(String),
    /// A literal value
    Literal(String),
}

/// Unified field information for return types
#[derive(Clone, Debug)]
pub struct ReturnFieldInfo {
    pub name: String,
    pub field_type: ReturnFieldType,
    pub source: ReturnFieldSource,
}

#[derive(Clone, Debug, PartialEq)]
pub enum RustFieldType {
    OptionValue,
    Value,
    TraversalValue,
    Vec(Box<RustFieldType>),
    RefArray(RustType),
    Primitive(GenRef<RustType>),
}

impl Display for RustFieldType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RustFieldType::OptionValue => write!(f, "Option<&'a Value>"),
            RustFieldType::Value => write!(f, "Value"),
            RustFieldType::TraversalValue => write!(f, "TraversalValue<'a>"),
            RustFieldType::Vec(ty) => write!(f, "Vec<{ty}>"),
            RustFieldType::RefArray(ty) => write!(f, "&'a [{ty}]"),
            RustFieldType::Primitive(ty) => write!(f, "{ty}"),
        }
    }
}

#[derive(Clone, Debug)]
pub enum ReturnFieldType {
    /// Simple type like "&'a str", "Option<&'a Value>", etc.
    Simple(RustFieldType),
    /// Nested object with its own fields (for nested traversals)
    Nested(Vec<ReturnFieldInfo>),
}

#[derive(Clone, Debug)]
pub enum ReturnFieldSource {
    /// Field from the schema (node/edge/vector properties)
    /// property_name is the source property name if different from output field name
    SchemaField { property_name: Option<String> },
    /// Implicit field (id, label, from_node, to_node, data, score)
    /// property_name is the source property name if different from output field name
    ImplicitField { property_name: Option<String> },
    /// User-defined field in custom object
    UserDefined,
    /// Result of a nested traversal expression
    NestedTraversal {
        traversal_expr: String,
        traversal_code: Option<String>, // Generated traversal code for response
        nested_struct_name: Option<String>, // Name of the nested struct type
        traversal_type: Option<super::traversal_steps::TraversalType>, // The actual traversal type for source extraction
        closure_param_name: Option<String>, // Closure parameter if in closure context
        closure_source_var: Option<String>, // Actual variable for the closure parameter
        accessed_field_name: Option<String>, // For simple property access, the field being accessed (e.g., "name" for usr::{name})
        own_closure_param: Option<String>, // This traversal's own closure parameter if it ends with a Closure step
        requires_full_traversal: bool, // True if traversal has graph navigation steps (Out, In, COUNT, etc.)
        is_first: bool,                // True if ::FIRST was used (should_collect = ToObj)
    },
}

impl ReturnFieldInfo {
    pub fn new_implicit(name: String, field_type: RustFieldType) -> Self {
        Self {
            name,
            field_type: ReturnFieldType::Simple(field_type),
            source: ReturnFieldSource::ImplicitField {
                property_name: None,
            },
        }
    }

    /// Create an implicit field with a different source property name
    /// e.g., output "file_id" from property "ID"
    pub fn new_implicit_with_property(
        name: String,
        property_name: String,
        field_type: RustFieldType,
    ) -> Self {
        Self {
            name,
            field_type: ReturnFieldType::Simple(field_type),
            source: ReturnFieldSource::ImplicitField {
                property_name: Some(property_name),
            },
        }
    }

    pub fn new_schema(name: String, field_type: RustFieldType) -> Self {
        Self {
            name,
            field_type: ReturnFieldType::Simple(field_type),
            source: ReturnFieldSource::SchemaField {
                property_name: None,
            },
        }
    }

    /// Create a schema field with a different source property name
    /// e.g., output "post" from property "content"
    pub fn new_schema_with_property(
        name: String,
        property_name: String,
        field_type: RustFieldType,
    ) -> Self {
        Self {
            name,
            field_type: ReturnFieldType::Simple(field_type),
            source: ReturnFieldSource::SchemaField {
                property_name: Some(property_name),
            },
        }
    }

    pub fn new_nested(name: String, fields: Vec<ReturnFieldInfo>, traversal_expr: String) -> Self {
        Self {
            name,
            field_type: ReturnFieldType::Nested(fields),
            source: ReturnFieldSource::NestedTraversal {
                traversal_expr,
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
        }
    }

    pub fn new_user_defined(name: String, field_type: RustFieldType) -> Self {
        Self {
            name,
            field_type: ReturnFieldType::Simple(field_type),
            source: ReturnFieldSource::UserDefined,
        }
    }
}

/// Legacy ReturnValue structure for backwards compatibility
pub struct ReturnValue {
    pub name: String,
    pub fields: Vec<ReturnValueField>,
    pub literal_value: Option<GenRef<String>>,
}

impl Display for ReturnValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "#[derive(Serialize)]")?;
        writeln!(f, "pub struct {} {{", self.name)?;
        for field in &self.fields {
            writeln!(f, "    pub {}: {},", field.name, field.field_type)?;
        }
        writeln!(f, "}}")?;
        Ok(())
    }
}
