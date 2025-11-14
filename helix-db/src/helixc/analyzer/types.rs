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
        Self {
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
        Self {
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
        Self {
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
        parameters: &mut Vec<Self>,
        sub_parameters: &mut Vec<(String, Vec<Self>)>,
    ) {
        match param.param_type.1 {
            FieldType::Identifier(ref id) => {
                parameters.push(Self {
                    name: param.name.1,
                    field_type: GeneratedType::Variable(GenRef::Std(id.clone())),
                    is_optional: param.is_optional,
                });
            }
            FieldType::Array(inner) => match inner.as_ref() {
                FieldType::Object(obj) => {
                    unwrap_object(format!("{}Data", param.name.1), obj, sub_parameters);
                    parameters.push(Self {
                        name: param.name.1.clone(),
                        field_type: GeneratedType::Vec(Box::new(GeneratedType::Object(
                            GenRef::Std(format!("{}Data", param.name.1)),
                        ))),
                        is_optional: param.is_optional,
                    });
                }
                param_type => {
                    parameters.push(Self {
                        name: param.name.1,
                        field_type: GeneratedType::Vec(Box::new(param_type.clone().into())),
                        is_optional: param.is_optional,
                    });
                }
            },
            FieldType::Object(obj) => {
                unwrap_object(format!("{}Data", param.name.1), &obj, sub_parameters);
                parameters.push(Self {
                    name: param.name.1.clone(),
                    field_type: GeneratedType::Variable(GenRef::Std(format!(
                        "{}Data",
                        param.name.1
                    ))),
                    is_optional: param.is_optional,
                });
            }
            param_type => {
                parameters.push(Self {
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
            FieldType::String => Self::RustType(GeneratedRustType::String),
            FieldType::F32 => Self::RustType(GeneratedRustType::F32),
            FieldType::F64 => Self::RustType(GeneratedRustType::F64),
            FieldType::I8 => Self::RustType(GeneratedRustType::I8),
            FieldType::I16 => Self::RustType(GeneratedRustType::I16),
            FieldType::I32 => Self::RustType(GeneratedRustType::I32),
            FieldType::I64 => Self::RustType(GeneratedRustType::I64),
            FieldType::U8 => Self::RustType(GeneratedRustType::U8),
            FieldType::U16 => Self::RustType(GeneratedRustType::U16),
            FieldType::U32 => Self::RustType(GeneratedRustType::U32),
            FieldType::U64 => Self::RustType(GeneratedRustType::U64),
            FieldType::U128 => Self::RustType(GeneratedRustType::U128),
            FieldType::Boolean => Self::RustType(GeneratedRustType::Bool),
            FieldType::Uuid => Self::RustType(GeneratedRustType::Uuid),
            FieldType::Date => Self::RustType(GeneratedRustType::Date),
            FieldType::Array(inner) => Self::Vec(Box::new(Self::from(*inner))),
            FieldType::Identifier(ref id) => Self::Variable(GenRef::Std(id.clone())),
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
            DefaultValue::String(s) => Self::Primitive(GenRef::Std(s)),
            DefaultValue::F32(f) => Self::Primitive(GenRef::Std(f.to_string())),
            DefaultValue::F64(f) => Self::Primitive(GenRef::Std(f.to_string())),
            DefaultValue::I8(i) => Self::Primitive(GenRef::Std(i.to_string())),
            DefaultValue::I16(i) => Self::Primitive(GenRef::Std(i.to_string())),
            DefaultValue::I32(i) => Self::Primitive(GenRef::Std(i.to_string())),
            DefaultValue::I64(i) => Self::Primitive(GenRef::Std(i.to_string())),
            DefaultValue::U8(i) => Self::Primitive(GenRef::Std(i.to_string())),
            DefaultValue::U16(i) => Self::Primitive(GenRef::Std(i.to_string())),
            DefaultValue::U32(i) => Self::Primitive(GenRef::Std(i.to_string())),
            DefaultValue::U64(i) => Self::Primitive(GenRef::Std(i.to_string())),
            DefaultValue::U128(i) => Self::Primitive(GenRef::Std(i.to_string())),
            DefaultValue::Boolean(b) => Self::Primitive(GenRef::Std(b.to_string())),
            DefaultValue::Now => Self::Primitive(GenRef::Std(
                "chrono::Utc::now().to_rfc3339()".to_string(),
            )),
            DefaultValue::Empty => Self::Unknown,
        }
    }
}

/// Metadata for GROUPBY and AGGREGATE_BY operations
#[derive(Debug, Clone)]
pub struct AggregateInfo {
    pub source_type: Box<Type>,   // Original type being aggregated (Node, Edge, Vector)
    pub properties: Vec<String>,  // Properties being grouped by
    pub is_count: bool,           // true for COUNT mode
    pub is_group_by: bool,        // true for GROUP_BY, false for AGGREGATE_BY
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
            Self::Aggregate(_) => "aggregate",
            Self::Node(_) => "node",
            Self::Nodes(_) => "nodes",
            Self::Edge(_) => "edge",
            Self::Edges(_) => "edges",
            Self::Vector(_) => "vector",
            Self::Vectors(_) => "vectors",
            Self::Scalar(_) => "scalar",
            Self::Object(_) => "object",
            Self::Array(_) => "array",
            Self::Boolean => "boolean",
            Self::Unknown => "unknown",
            Self::Anonymous(ty) => ty.kind_str(),
        }
    }

    pub fn get_type_name(&self) -> String {
        match self {
            Self::Aggregate(_) => "aggregate".to_string(),
            Self::Node(Some(name)) => name.clone(),
            Self::Nodes(Some(name)) => name.clone(),
            Self::Edge(Some(name)) => name.clone(),
            Self::Edges(Some(name)) => name.clone(),
            Self::Vector(Some(name)) => name.clone(),
            Self::Vectors(Some(name)) => name.clone(),
            Self::Scalar(ft) => ft.to_string(),
            Self::Anonymous(ty) => ty.get_type_name(),
            Self::Array(ty) => ty.get_type_name(),
            Self::Boolean => "boolean".to_string(),
            Self::Unknown => "unknown".to_string(),
            Self::Object(fields) => {
                let field_names = fields.keys().cloned().collect::<Vec<_>>();
                format!("object({})", field_names.join(", "))
            }
            _ => unreachable!(),
        }
    }

    /// Recursively strip <code>Anonymous</code> layers and return the base type.
    pub fn base(&self) -> &Self {
        match self {
            Self::Anonymous(inner) => inner.base(),
            _ => self,
        }
    }

    #[allow(dead_code)]
    /// Same, but returns an owned clone for convenience.
    pub fn cloned_base(&self) -> Self {
        match self {
            Self::Anonymous(inner) => inner.cloned_base(),
            _ => self.clone(),
        }
    }

    #[allow(dead_code)]
    pub fn is_numeric(&self) -> bool {
        matches!(
            self,
            Self::Scalar(
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
            Self::Scalar(
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

    pub fn into_single(self) -> Self {
        match self {
            Self::Scalar(ft) => Self::Scalar(ft),
            Self::Object(fields) => Self::Object(fields),
            Self::Boolean => Self::Boolean,
            Self::Unknown => Self::Unknown,
            Self::Anonymous(inner) => Self::Anonymous(Box::new(inner.into_single())),
            Self::Aggregate(info) => Self::Aggregate(info),
            Self::Node(name) => Self::Node(name),
            Self::Nodes(name) => Self::Node(name),
            Self::Edge(name) => Self::Edge(name),
            Self::Edges(name) => Self::Edge(name),
            Self::Vector(name) => Self::Vector(name),
            Self::Vectors(name) => Self::Vector(name),
            Self::Array(inner) => *inner,
        }
    }
}

impl PartialEq for Type {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Scalar(ft), Self::Scalar(other_ft)) => ft == other_ft,
            (Self::Object(fields), Self::Object(other_fields)) => fields == other_fields,
            (Self::Boolean, Self::Boolean) => true,
            (Self::Unknown, Self::Unknown) => true,
            (Self::Anonymous(inner), Self::Anonymous(other_inner)) => inner == other_inner,
            (Self::Node(name), Self::Node(other_name)) => name == other_name,
            (Self::Nodes(name), Self::Nodes(other_name)) => name == other_name,
            (Self::Edge(name), Self::Edge(other_name)) => name == other_name,
            (Self::Edges(name), Self::Edges(other_name)) => name == other_name,
            (Self::Vector(name), Self::Vector(other_name)) => name == other_name,
            (Self::Vectors(name), Self::Vectors(other_name)) => name == other_name,
            (Self::Array(inner), Self::Array(other_inner)) => inner == other_inner,
            (Self::Vector(name), Self::Vectors(other_name)) => name == other_name,
            _ => unreachable!(),
        }
    }
}

impl From<FieldType> for Type {
    fn from(ft: FieldType) -> Self {
        use FieldType::*;
        match ft {
            String | Boolean | F32 | F64 | I8 | I16 | I32 | I64 | U8 | U16 | U32 | U64 | U128
            | Uuid | Date => Self::Scalar(ft.clone()),
            Array(inner_ft) => Self::Array(Box::new(Self::from(*inner_ft))),
            Object(obj) => Self::Object(obj.into_iter().map(|(k, v)| (k, Self::from(v))).collect()),
            Identifier(id) => Self::Scalar(FieldType::Identifier(id)),
        }
    }
}

impl From<&FieldType> for Type {
    fn from(ft: &FieldType) -> Self {
        use FieldType::*;
        match ft {
            String | Boolean | F32 | F64 | I8 | I16 | I32 | I64 | U8 | U16 | U32 | U64 | U128
            | Uuid | Date => Self::Scalar(ft.clone()),
            Array(inner_ft) => Self::Array(Box::new(Self::from(*inner_ft.clone()))),
            Object(obj) => Self::Object(obj.iter().map(|(k, v)| (k.clone(), Self::from(v))).collect()),
            Identifier(id) => Self::Scalar(FieldType::Identifier(id.clone())),
        }
    }
}

impl std::fmt::Display for Type {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
