use std::collections::HashMap;

use crate::helixc::{
    generator::{
        generator_types::{
            Assignment as GeneratedAssignment, EdgeSchema as GeneratedEdgeSchema,
            NodeSchema as GeneratedNodeSchema, Parameter as GeneratedParameter, SchemaProperty,
            Statement as GeneratedStatement, VectorSchema as GeneratedVectorSchema,
        },
        traversal_steps::Traversal as GeneratedTraversal,
        utils::{GenRef, GeneratedType, GeneratedValue, RustType as GeneratedRustType},
    },
    parser::helix_parser::{
        Assignment, DefaultValue, EdgeSchema, FieldPrefix, FieldType, NodeSchema, Parameter,
        VectorSchema,
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
                    });
                }
                param_type => {
                    parameters.push(GeneratedParameter {
                        name: param.name.1,
                        field_type: GeneratedType::Vec(Box::new(param_type.clone().into())),
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
                });
            }
            param_type => {
                parameters.push(GeneratedParameter {
                    name: param.name.1,
                    field_type: param_type.into(),
                });
            }
        }
    }
}

impl GeneratedStatement {
    // pub fn unwrap_assignment(assignment: Assignment, statements: &mut Vec<GeneratedStatement>) {
    //     let generated_assignment = GeneratedStatement::Assignment(GeneratedAssignment {
    //         variable: assignment.variable.clone(),
    //         value: assignment.value.into(),
    //     });
    //     statements.push(generated_assignment);
    // }

    fn unwrap_traversal(traversal: GeneratedTraversal, statements: &mut Vec<GeneratedStatement>) {}
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
                    unwrap_object(format!("{}Data", field_name), obj, sub_parameters);
                    GeneratedParameter {
                        name: field_name.clone(),
                        field_type: GeneratedType::Object(GenRef::Std(format!(
                            "{}Data",
                            field_name
                        ))),
                    }
                }
                FieldType::Array(inner) => match inner.as_ref() {
                    FieldType::Object(obj) => {
                        unwrap_object(format!("{}Data", field_name), obj, sub_parameters);
                        GeneratedParameter {
                            name: field_name.clone(),
                            field_type: GeneratedType::Vec(Box::new(GeneratedType::Object(
                                GenRef::Std(format!("{}Data", field_name)),
                            ))),
                        }
                    }
                    _ => GeneratedParameter {
                        name: field_name.clone(),
                        field_type: GeneratedType::from(field_type.clone()),
                    },
                },
                _ => GeneratedParameter {
                    name: field_name.clone(),
                    field_type: GeneratedType::from(field_type.clone()),
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
                println!("unimplemented: {:?}", generated);
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
            DefaultValue::Empty => GeneratedValue::Unknown,
        }
    }
}
