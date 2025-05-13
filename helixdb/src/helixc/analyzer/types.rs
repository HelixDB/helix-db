use std::collections::HashMap;

use crate::helixc::{
    generator::new::generator_types::{
        Assignment as GeneratedAssignment, EdgeSchema as GeneratedEdgeSchema, GeneratedType,
        NodeSchema as GeneratedNodeSchema, Parameter as GeneratedParameter,
        RustType as GeneratedRustType, Statement as GeneratedStatement,
        VectorSchema as GeneratedVectorSchema,
    },
    parser::helix_parser::{
        Assignment, EdgeSchema, FieldType, NodeSchema, Parameter, VectorSchema,
    },
};

impl From<NodeSchema> for GeneratedNodeSchema {
    fn from(generated: NodeSchema) -> Self {
        GeneratedNodeSchema {
            name: generated.name.1,
            properties: generated
                .fields
                .into_iter()
                .map(|f| (f.name, f.field_type.into()))
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
                    .map(|f| (f.name, f.field_type.into()))
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
                .map(|f| (f.name, f.field_type.into()))
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
                    field_type: GeneratedType::Variable(id.clone()),
                });
            }
            FieldType::Array(inner) => match inner.as_ref() {
                FieldType::Object(obj) => {
                    sub_parameters.push((
                        format!("{}Data", param.name.1),
                        obj.iter()
                            .map(|(name, field_type)| GeneratedParameter {
                                name: name.clone(),
                                field_type: field_type.clone().into(),
                            })
                            .collect(),
                    ));
                }
                param_type => {
                    parameters.push(GeneratedParameter {
                        name: param.name.1,
                        field_type: param_type.clone().into(),
                    });
                }
            },
            FieldType::Object(obj) => {
                sub_parameters.push((
                    format!("{}Data", param.name.1),
                    obj.iter()
                        .map(|(name, field_type)| GeneratedParameter {
                            name: name.clone(),
                            field_type: field_type.clone().into(),
                        })
                        .collect(),
                ));
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
    pub fn unwrap_assignment(assignment: Assignment, statements: &mut Vec<GeneratedStatement>) {
        let generated_assignment = GeneratedStatement::Assignment(GeneratedAssignment {
            variable: assignment.variable.clone(),
            value: assignment.value.into(),
        });
        statements.push(generated_assignment);
    }

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
                    unwrap_object(field_name.clone(), obj, sub_parameters);
                    GeneratedParameter {
                        name: field_name.clone(),
                        field_type: GeneratedType::Object(field_name.clone()),
                    }
                }
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
            FieldType::Identifier(ref id) => GeneratedType::Variable(id.clone()),
            _ => unimplemented!(),
        }
    }
}
