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
    parser::helix_parser::{
        DefaultValue, EdgeSchema, FieldType, NodeSchema, Parameter, VectorSchema,
    },
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
                println!("unimplemented: {generated:?}");
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

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) enum Type {
    Node(Option<String>),
    Nodes(Option<String>),
    Edge(Option<String>),
    Edges(Option<String>),
    Vector(Option<String>),
    Vectors(Option<String>),
    Scalar(FieldType),
    Object(HashMap<String, Type>),
    Anonymous(Box<Type>),
    Boolean,
    Unknown,
}

impl Type {
    pub fn kind_str(&self) -> &'static str {
        match self {
            Type::Node(_) => "node",
            Type::Nodes(_) => "nodes",
            Type::Edge(_) => "edge",
            Type::Edges(_) => "edges",
            Type::Vector(_) => "vector",
            Type::Vectors(_) => "vectors",
            Type::Scalar(_) => "scalar",
            Type::Object(_) => "object",
            Type::Boolean => "boolean",
            Type::Unknown => "unknown",
            Type::Anonymous(ty) => ty.kind_str(),
        }
    }

    pub fn get_type_name(&self) -> String {
        match self {
            Type::Node(Some(name)) => name.clone(),
            Type::Nodes(Some(name)) => name.clone(),
            Type::Edge(Some(name)) => name.clone(),
            Type::Edges(Some(name)) => name.clone(),
            Type::Vector(Some(name)) => name.clone(),
            Type::Vectors(Some(name)) => name.clone(),
            Type::Scalar(ft) => ft.to_string(),
            Type::Anonymous(ty) => ty.get_type_name(),
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
        // TODO: never used?
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
}

impl From<FieldType> for Type {
    fn from(ft: FieldType) -> Self {
        use FieldType::*;
        match ft {
            String | Boolean | F32 | F64 | I8 | I16 | I32 | I64 | U8 | U16 | U32 | U64 | U128
            | Uuid | Date => Type::Scalar(ft.clone()),
            Array(inner_ft) => Type::from(*inner_ft),
            _ => Type::Unknown,
        }
    }
}
