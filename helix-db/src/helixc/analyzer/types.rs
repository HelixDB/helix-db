use std::collections::HashMap;

use crate::helixc::{
    generator::{
        queries::Parameter as GeneratedParameter,
        schemas::{
            EdgeSchema as GeneratedEdgeSchema, NodeSchema as GeneratedNodeSchema, SchemaProperty,
            VectorSchema as GeneratedVectorSchema,
        },
        utils::{GenRef, GeneratedType, GeneratedValue, RustType as GeneratedRustType},
    },
    parser::types::{DefaultValue, EdgeSchema, FieldType, NodeSchema, Parameter, VectorSchema},
};

impl From<NodeSchema> for GeneratedNodeSchema {
    fn from(generated: NodeSchema) -> Self {
        GeneratedNodeSchema {
            name: generated.name.1,
            properties: generated
                .fields
                .into_iter()
                .map(|f| SchemaProperty {
                    name: f.name,
                    field_type: f.field_type.into(),
                    default_value: f.defaults.map(|d| d.into()),
                    is_index: f.prefix,
                })
                .collect(),
        }
    }
}

impl From<EdgeSchema> for GeneratedEdgeSchema {
    fn from(generated: EdgeSchema) -> Self {
        GeneratedEdgeSchema {
            name: generated.name.1,
            from: generated.from.1,
            to: generated.to.1,
            properties: generated.properties.map_or(vec![], |fields| {
                fields
                    .into_iter()
                    .map(|f| SchemaProperty {
                        name: f.name,
                        field_type: f.field_type.into(),
                        default_value: f.defaults.map(|d| d.into()),
                        is_index: f.prefix,
                    })
                    .collect()
            }),
        }
    }
}

impl From<VectorSchema> for GeneratedVectorSchema {
    fn from(generated: VectorSchema) -> Self {
        GeneratedVectorSchema {
            name: generated.name,
            properties: generated
                .fields
                .into_iter()
                .map(|f| SchemaProperty {
                    name: f.name,
                    field_type: f.field_type.into(),
                    default_value: f.defaults.map(|d| d.into()),
                    is_index: f.prefix,
                })
                .collect(),
        }
    }
}

impl GeneratedParameter {
    pub fn unwrap_param(
        param: Parameter,
        parameters: &mut Vec<GeneratedParameter>,
        sub_parameters: &mut Vec<(String, Vec<GeneratedParameter>)>,
    ) {
        match param.param_type.1 {
            FieldType::Identifier(ref id) => {
                parameters.push(GeneratedParameter {
                    name: param.name.1,
                    field_type: GeneratedType::Variable(GenRef::Std(id.clone())),
                    is_optional: param.is_optional,
                });
            }
            FieldType::Array(inner) => match inner.as_ref() {
                FieldType::Object(obj) => {
                    unwrap_object(format!("{}Data", param.name.1), obj, sub_parameters);
                    parameters.push(GeneratedParameter {
                        name: param.name.1.clone(),
                        field_type: GeneratedType::Vec(Box::new(GeneratedType::Object(
                            GenRef::Std(format!("{}Data", param.name.1)),
                        ))),
                        is_optional: param.is_optional,
                    });
                }
                param_type => {
                    parameters.push(GeneratedParameter {
                        name: param.name.1,
                        field_type: GeneratedType::Vec(Box::new(param_type.clone().into())),
                        is_optional: param.is_optional,
                    });
                }
            },
            FieldType::Object(obj) => {
                unwrap_object(format!("{}Data", param.name.1), &obj, sub_parameters);
                parameters.push(GeneratedParameter {
                    name: param.name.1.clone(),
                    field_type: GeneratedType::Variable(GenRef::Std(format!(
                        "{}Data",
                        param.name.1
                    ))),
                    is_optional: param.is_optional,
                });
            }
            param_type => {
                parameters.push(GeneratedParameter {
                    name: param.name.1,
                    field_type: param_type.into(),
                    is_optional: param.is_optional,
                });
            }
        }
    }
}

fn unwrap_object(
    name: String,
    obj: &HashMap<String, FieldType>,
    sub_parameters: &mut Vec<(String, Vec<GeneratedParameter>)>,
) {
    let sub_param = (
        name,
        obj.iter()
            .map(|(field_name, field_type)| match field_type {
                FieldType::Object(obj) => {
                    unwrap_object(format!("{field_name}Data"), obj, sub_parameters);
                    GeneratedParameter {
                        name: field_name.clone(),
                        field_type: GeneratedType::Object(GenRef::Std(format!("{field_name}Data"))),
                        is_optional: false,
                    }
                }
                FieldType::Array(inner) => match inner.as_ref() {
                    FieldType::Object(obj) => {
                        unwrap_object(format!("{field_name}Data"), obj, sub_parameters);
                        GeneratedParameter {
                            name: field_name.clone(),
                            field_type: GeneratedType::Vec(Box::new(GeneratedType::Object(
                                GenRef::Std(format!("{field_name}Data")),
                            ))),
                            is_optional: false,
                        }
                    }
                    _ => GeneratedParameter {
                        name: field_name.clone(),
                        field_type: GeneratedType::from(field_type.clone()),
                        is_optional: false,
                    },
                },
                _ => GeneratedParameter {
                    name: field_name.clone(),
                    field_type: GeneratedType::from(field_type.clone()),
                    is_optional: false,
                },
            })
            .collect(),
    );
    sub_parameters.push(sub_param);
}
impl From<FieldType> for GeneratedType {
    fn from(generated: FieldType) -> Self {
        match generated {
            FieldType::String => GeneratedType::RustType(GeneratedRustType::String),
            FieldType::F32 => GeneratedType::RustType(GeneratedRustType::F32),
            FieldType::F64 => GeneratedType::RustType(GeneratedRustType::F64),
            FieldType::I8 => GeneratedType::RustType(GeneratedRustType::I8),
            FieldType::I16 => GeneratedType::RustType(GeneratedRustType::I16),
            FieldType::I32 => GeneratedType::RustType(GeneratedRustType::I32),
            FieldType::I64 => GeneratedType::RustType(GeneratedRustType::I64),
            FieldType::U8 => GeneratedType::RustType(GeneratedRustType::U8),
            FieldType::U16 => GeneratedType::RustType(GeneratedRustType::U16),
            FieldType::U32 => GeneratedType::RustType(GeneratedRustType::U32),
            FieldType::U64 => GeneratedType::RustType(GeneratedRustType::U64),
            FieldType::U128 => GeneratedType::RustType(GeneratedRustType::U128),
            FieldType::Boolean => GeneratedType::RustType(GeneratedRustType::Bool),
            FieldType::Uuid => GeneratedType::RustType(GeneratedRustType::Uuid),
            FieldType::Date => GeneratedType::RustType(GeneratedRustType::Date),
            FieldType::Array(inner) => GeneratedType::Vec(Box::new(GeneratedType::from(*inner))),
            FieldType::Identifier(ref id) => GeneratedType::Variable(GenRef::Std(id.clone())),
            // FieldType::Object(obj) => GeneratedType::Object(
            //     obj.iter()
            //         .map(|(name, field_type)| {
            //             (name.clone(), GeneratedType::from(field_type.clone()))
            //         })
            //         .collect(),
            // ),
            _ => {
                unimplemented!()
            }
        }
    }
}

impl From<DefaultValue> for GeneratedValue {
    fn from(generated: DefaultValue) -> Self {
        match generated {
            DefaultValue::String(s) => GeneratedValue::Primitive(GenRef::Std(s)),
            DefaultValue::F32(f) => GeneratedValue::Primitive(GenRef::Std(f.to_string())),
            DefaultValue::F64(f) => GeneratedValue::Primitive(GenRef::Std(f.to_string())),
            DefaultValue::I8(i) => GeneratedValue::Primitive(GenRef::Std(i.to_string())),
            DefaultValue::I16(i) => GeneratedValue::Primitive(GenRef::Std(i.to_string())),
            DefaultValue::I32(i) => GeneratedValue::Primitive(GenRef::Std(i.to_string())),
            DefaultValue::I64(i) => GeneratedValue::Primitive(GenRef::Std(i.to_string())),
            DefaultValue::U8(i) => GeneratedValue::Primitive(GenRef::Std(i.to_string())),
            DefaultValue::U16(i) => GeneratedValue::Primitive(GenRef::Std(i.to_string())),
            DefaultValue::U32(i) => GeneratedValue::Primitive(GenRef::Std(i.to_string())),
            DefaultValue::U64(i) => GeneratedValue::Primitive(GenRef::Std(i.to_string())),
            DefaultValue::U128(i) => GeneratedValue::Primitive(GenRef::Std(i.to_string())),
            DefaultValue::Boolean(b) => GeneratedValue::Primitive(GenRef::Std(b.to_string())),
            DefaultValue::Now => GeneratedValue::Primitive(GenRef::Std(
                "chrono::Utc::now().to_rfc3339()".to_string(),
            )),
            DefaultValue::Empty => GeneratedValue::Unknown,
        }
    }
}

/// Metadata for GROUPBY and AGGREGATE_BY operations
#[derive(Debug, Clone)]
pub struct AggregateInfo {
    pub source_type: Box<Type>, // Original type being aggregated (Node, Edge, Vector)
    pub properties: Vec<String>, // Properties being grouped by
    pub is_count: bool,         // true for COUNT mode
    pub is_group_by: bool,      // true for GROUP_BY, false for AGGREGATE_BY
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum Type {
    Aggregate(AggregateInfo),
    Node(Option<String>),
    Nodes(Option<String>),
    Edge(Option<String>),
    Edges(Option<String>),
    Vector(Option<String>),
    Vectors(Option<String>),
    Scalar(FieldType),
    Object(HashMap<String, Type>),
    Array(Box<Type>),
    Anonymous(Box<Type>),
    Boolean,
    Unknown,
}

impl Type {
    pub fn kind_str(&self) -> &'static str {
        match self {
            Type::Aggregate(_) => "aggregate",
            Type::Node(_) => "node",
            Type::Nodes(_) => "nodes",
            Type::Edge(_) => "edge",
            Type::Edges(_) => "edges",
            Type::Vector(_) => "vector",
            Type::Vectors(_) => "vectors",
            Type::Scalar(_) => "scalar",
            Type::Object(_) => "object",
            Type::Array(_) => "array",
            Type::Boolean => "boolean",
            Type::Unknown => "unknown",
            Type::Anonymous(ty) => ty.kind_str(),
        }
    }

    pub fn get_type_name(&self) -> String {
        match self {
            Type::Aggregate(_) => "aggregate".to_string(),
            Type::Node(Some(name)) => name.clone(),
            Type::Nodes(Some(name)) => name.clone(),
            Type::Edge(Some(name)) => name.clone(),
            Type::Edges(Some(name)) => name.clone(),
            Type::Vector(Some(name)) => name.clone(),
            Type::Vectors(Some(name)) => name.clone(),
            Type::Scalar(ft) => ft.to_string(),
            Type::Anonymous(ty) => ty.get_type_name(),
            Type::Array(ty) => ty.get_type_name(),
            Type::Boolean => "boolean".to_string(),
            Type::Unknown => "unknown".to_string(),
            Type::Object(fields) => {
                let field_names = fields.keys().cloned().collect::<Vec<_>>();
                format!("object({})", field_names.join(", "))
            }
            _ => unreachable!(),
        }
    }

    /// Recursively strip <code>Anonymous</code> layers and return the base type.
    pub fn base(&self) -> &Type {
        match self {
            Type::Anonymous(inner) => inner.base(),
            _ => self,
        }
    }

    #[allow(dead_code)]
    /// Same, but returns an owned clone for convenience.
    pub fn cloned_base(&self) -> Type {
        match self {
            Type::Anonymous(inner) => inner.cloned_base(),
            _ => self.clone(),
        }
    }

    #[allow(dead_code)]
    pub fn is_numeric(&self) -> bool {
        matches!(
            self,
            Type::Scalar(
                FieldType::I8
                    | FieldType::I16
                    | FieldType::I32
                    | FieldType::I64
                    | FieldType::U8
                    | FieldType::U16
                    | FieldType::U32
                    | FieldType::U64
                    | FieldType::U128
                    | FieldType::F32
                    | FieldType::F64,
            )
        )
    }

    pub fn is_integer(&self) -> bool {
        matches!(
            self,
            Type::Scalar(
                FieldType::I8
                    | FieldType::I16
                    | FieldType::I32
                    | FieldType::I64
                    | FieldType::U8
                    | FieldType::U16
                    | FieldType::U32
                    | FieldType::U64
                    | FieldType::U128
            )
        )
    }

    pub fn into_single(self) -> Type {
        match self {
            Type::Scalar(ft) => Type::Scalar(ft),
            Type::Object(fields) => Type::Object(fields),
            Type::Boolean => Type::Boolean,
            Type::Unknown => Type::Unknown,
            Type::Anonymous(inner) => Type::Anonymous(Box::new(inner.into_single())),
            Type::Aggregate(info) => Type::Aggregate(info),
            Type::Node(name) => Type::Node(name),
            Type::Nodes(name) => Type::Node(name),
            Type::Edge(name) => Type::Edge(name),
            Type::Edges(name) => Type::Edge(name),
            Type::Vector(name) => Type::Vector(name),
            Type::Vectors(name) => Type::Vector(name),
            Type::Array(inner) => *inner,
        }
    }
}

impl PartialEq for Type {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Type::Scalar(ft), Type::Scalar(other_ft)) => ft == other_ft,
            (Type::Object(fields), Type::Object(other_fields)) => fields == other_fields,
            (Type::Boolean, Type::Boolean) => true,
            (Type::Unknown, Type::Unknown) => true,
            (Type::Anonymous(inner), Type::Anonymous(other_inner)) => inner == other_inner,
            (Type::Node(name), Type::Node(other_name)) => name == other_name,
            (Type::Nodes(name), Type::Nodes(other_name)) => name == other_name,
            (Type::Edge(name), Type::Edge(other_name)) => name == other_name,
            (Type::Edges(name), Type::Edges(other_name)) => name == other_name,
            (Type::Vector(name), Type::Vector(other_name)) => name == other_name,
            (Type::Vectors(name), Type::Vectors(other_name)) => name == other_name,
            (Type::Array(inner), Type::Array(other_inner)) => inner == other_inner,
            (Type::Vector(name), Type::Vectors(other_name)) => name == other_name,
            _ => unreachable!(),
        }
    }
}

impl From<FieldType> for Type {
    fn from(ft: FieldType) -> Self {
        use FieldType::*;
        match ft {
            String | Boolean | F32 | F64 | I8 | I16 | I32 | I64 | U8 | U16 | U32 | U64 | U128
            | Uuid | Date => Type::Scalar(ft.clone()),
            Array(inner_ft) => Type::Array(Box::new(Type::from(*inner_ft))),
            Object(obj) => Type::Object(obj.into_iter().map(|(k, v)| (k, Type::from(v))).collect()),
            Identifier(id) => Type::Scalar(FieldType::Identifier(id)),
        }
    }
}

impl From<&FieldType> for Type {
    fn from(ft: &FieldType) -> Self {
        use FieldType::*;
        match ft {
            String | Boolean | F32 | F64 | I8 | I16 | I32 | I64 | U8 | U16 | U32 | U64 | U128
            | Uuid | Date => Type::Scalar(ft.clone()),
            Array(inner_ft) => Type::Array(Box::new(Type::from(*inner_ft.clone()))),
            Object(obj) => Type::Object(
                obj.iter()
                    .map(|(k, v)| (k.clone(), Type::from(v)))
                    .collect(),
            ),
            Identifier(id) => Type::Scalar(FieldType::Identifier(id.clone())),
        }
    }
}

impl std::fmt::Display for Type {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
