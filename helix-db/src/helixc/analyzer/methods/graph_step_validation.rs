//! Semantic analyzer for Helix‑QL.
use crate::helixc::analyzer::error_codes::ErrorCode;
use crate::helixc::analyzer::utils::type_in_scope;
use crate::helixc::generator::utils::{EmbedData, Precision as GeneratedPrecision};
use crate::{
    generate_error,
    helix_engine::traversal_core::ops::source::add_e::EdgeType,
    helixc::{
        analyzer::{
            Ctx,
            errors::push_query_err,
            types::Type,
            utils::{gen_identifier_or_param, is_valid_identifier},
        },
        generator::{
            queries::Query as GeneratedQuery,
            traversal_steps::{
                In as GeneratedIn, InE as GeneratedInE, Out as GeneratedOut, OutE as GeneratedOutE,
                SearchVectorStep, ShortestPath as GeneratedShortestPath,
                ShortestPathBFS as GeneratedShortestPathBFS,
                ShortestPathDijkstras as GeneratedShortestPathDijkstras, ShouldCollect,
                Step as GeneratedStep, Traversal as GeneratedTraversal,
            },
            utils::{GenRef, GeneratedValue, Separator, VecData},
        },
        parser::types::*,
    },
};
use paste::paste;
use std::collections::HashMap;

/// Check that a graph‑navigation step is allowed for the current element
/// kind and return the post‑step kind.
///
/// # Arguments
///
/// * `ctx` - The context of the query
/// * `gs` - The graph step to apply
/// * `cur_ty` - The current type of the traversal
/// * `original_query` - The original query
/// * `traversal` - The generated traversal
/// * `scope` - The scope of the query
///
/// # Returns
///
/// * `Option<Type>` - The resulting type of applying the graph step
pub(crate) fn apply_graph_step<'a>(
    ctx: &mut Ctx<'a>,
    gs: &'a GraphStep,
    cur_ty: &Type,
    original_query: &'a Query,
    traversal: &mut GeneratedTraversal,
    scope: &mut HashMap<&'a str, Type>,
    gen_query: &mut GeneratedQuery,
) -> Option<Type> {
    use GraphStepType::*;
    match (&gs.step, cur_ty.base()) {
        // Node‑to‑Edge
        (
            OutE(label),
            Type::Nodes(Some(node_label))
            | Type::Node(Some(node_label))
            | Type::Vectors(Some(node_label))
            | Type::Vector(Some(node_label)),
        ) => {
            traversal
                .steps
                .push(Separator::Period(GeneratedStep::OutE(GeneratedOutE {
                    label: GenRef::Literal(label.clone()),
                })));
            traversal.should_collect = ShouldCollect::ToVec;
            let edge = match ctx.edge_map.get(label.as_str()) {
                Some(e) => e,
                None => {
                    generate_error!(ctx, original_query, gs.loc.clone(), E102, label.as_str());
                    return None;
                }
            };
            match edge.from.1 == node_label.clone() {
                true => Some(Type::Edges(Some(label.to_string()))),
                false => {
                    generate_error!(
                        ctx,
                        original_query,
                        gs.loc.clone(),
                        E207,
                        label.as_str(),
                        "node",
                        node_label.as_str()
                    );
                    None
                }
            }
        }
        (
            InE(label),
            Type::Nodes(Some(node_label))
            | Type::Node(Some(node_label))
            | Type::Vectors(Some(node_label))
            | Type::Vector(Some(node_label)),
        ) => {
            traversal
                .steps
                .push(Separator::Period(GeneratedStep::InE(GeneratedInE {
                    label: GenRef::Literal(label.clone()),
                })));
            traversal.should_collect = ShouldCollect::ToVec;
            let edge = match ctx.edge_map.get(label.as_str()) {
                Some(e) => e,
                None => {
                    generate_error!(ctx, original_query, gs.loc.clone(), E102, label.as_str());
                    return None;
                }
            };

            match edge.to.1 == node_label.clone() {
                true => Some(Type::Edges(Some(label.to_string()))),
                false => {
                    generate_error!(ctx, original_query, gs.loc.clone(), E102, label.as_str());
                    None
                }
            }
        }

        // Node‑to‑Node
        (
            Out(label),
            Type::Nodes(Some(node_label))
            | Type::Node(Some(node_label))
            | Type::Vectors(Some(node_label))
            | Type::Vector(Some(node_label)),
        ) => {
            let edge_type = match ctx.edge_map.get(label.as_str()) {
                Some(edge) => {
                    if ctx.node_set.contains(edge.to.1.as_str()) {
                        EdgeType::Node
                    } else if ctx.vector_set.contains(edge.to.1.as_str()) {
                        EdgeType::Vec
                    } else {
                        generate_error!(ctx, original_query, gs.loc.clone(), E102, label);
                        return None;
                    }
                }
                None => {
                    generate_error!(ctx, original_query, gs.loc.clone(), E102, label.as_str());
                    return None;
                }
            };
            traversal
                .steps
                .push(Separator::Period(GeneratedStep::Out(GeneratedOut {
                    edge_type: GenRef::Ref(edge_type.to_string()),
                    label: GenRef::Literal(label.clone()),
                })));
            traversal.should_collect = ShouldCollect::ToVec;
            let edge = match ctx.edge_map.get(label.as_str()) {
                Some(e) => e,
                None => {
                    generate_error!(ctx, original_query, gs.loc.clone(), E102, label.as_str());
                    return None;
                }
            };
            match edge.from.1 == node_label.clone() {
                true => {
                    if EdgeType::Node == edge_type {
                        Some(Type::Nodes(Some(edge.to.1.clone())))
                    } else if EdgeType::Vec == edge_type {
                        Some(Type::Vectors(Some(edge.to.1.clone())))
                    } else {
                        None
                    }
                }
                false => {
                    generate_error!(
                        ctx,
                        original_query,
                        gs.loc.clone(),
                        E207,
                        label.as_str(),
                        "node",
                        node_label.as_str()
                    );
                    None
                }
            }
        }

        (
            In(label),
            Type::Nodes(Some(node_label))
            | Type::Node(Some(node_label))
            | Type::Vectors(Some(node_label))
            | Type::Vector(Some(node_label)),
        ) => {
            let edge_type = match ctx.edge_map.get(label.as_str()) {
                Some(edge) => {
                    if ctx.node_set.contains(edge.from.1.as_str()) {
                        EdgeType::Node
                    } else if ctx.vector_set.contains(edge.from.1.as_str()) {
                        EdgeType::Vec
                    } else {
                        generate_error!(ctx, original_query, gs.loc.clone(), E102, label.as_str());
                        return None;
                    }
                }
                None => {
                    generate_error!(ctx, original_query, gs.loc.clone(), E102, label.as_str());
                    return None;
                }
            };

            traversal
                .steps
                .push(Separator::Period(GeneratedStep::In(GeneratedIn {
                    edge_type: GenRef::Ref(edge_type.to_string()),
                    label: GenRef::Literal(label.clone()),
                })));
            traversal.should_collect = ShouldCollect::ToVec;
            let edge = match ctx.edge_map.get(label.as_str()) {
                Some(e) => e,
                None => {
                    generate_error!(ctx, original_query, gs.loc.clone(), E102, label.as_str());
                    return None;
                }
            };

            match edge.to.1 == node_label.clone() {
                true => {
                    if EdgeType::Node == edge_type {
                        Some(Type::Nodes(Some(edge.from.1.clone())))
                    } else if EdgeType::Vec == edge_type {
                        Some(Type::Vectors(Some(edge.from.1.clone())))
                    } else {
                        None
                    }
                }
                false => {
                    generate_error!(
                        ctx,
                        original_query,
                        gs.loc.clone(),
                        E207,
                        label.as_str(),
                        "node",
                        node_label.as_str()
                    );
                    None
                }
            }
        }

        // Edge‑to‑Node
        (FromN, Type::Edges(Some(edge_ty)) | Type::Edge(Some(edge_ty))) => {
            let new_ty = if let Some(edge_schema) = ctx.edge_map.get(edge_ty.as_str()) {
                let node_type = &edge_schema.from.1;
                if !ctx.node_set.contains(node_type.as_str()) {
                    generate_error!(ctx, original_query, gs.loc.clone(), E623, edge_ty);
                }
                match cur_ty {
                    Type::Edges(_) => Some(Type::Nodes(Some(node_type.clone()))),
                    Type::Edge(_) => Some(Type::Node(Some(node_type.clone()))),
                    _ => None,
                }
            } else {
                None
            };
            traversal
                .steps
                .push(Separator::Period(GeneratedStep::FromN));
            // Preserve collection type: multiple edges -> multiple nodes, single edge -> single node
            match cur_ty {
                Type::Edges(_) => traversal.should_collect = ShouldCollect::ToVec,
                Type::Edge(_) => traversal.should_collect = ShouldCollect::ToObj,
                _ => {}
            }
            new_ty
        }
        (ToN, Type::Edges(Some(edge_ty)) | Type::Edge(Some(edge_ty))) => {
            let new_ty = if let Some(edge_schema) = ctx.edge_map.get(edge_ty.as_str()) {
                let node_type = &edge_schema.to.1;
                if !ctx.node_set.contains(node_type.as_str()) {
                    generate_error!(ctx, original_query, gs.loc.clone(), E624, edge_ty);
                }
                match cur_ty {
                    Type::Edges(_) => Some(Type::Nodes(Some(node_type.clone()))),
                    Type::Edge(_) => Some(Type::Node(Some(node_type.clone()))),
                    _ => None,
                }
            } else {
                None
            };
            traversal.steps.push(Separator::Period(GeneratedStep::ToN));
            // Preserve collection type: multiple edges -> multiple nodes, single edge -> single node
            match cur_ty {
                Type::Edges(_) => traversal.should_collect = ShouldCollect::ToVec,
                Type::Edge(_) => traversal.should_collect = ShouldCollect::ToObj,
                _ => {}
            }
            new_ty
        }
        (FromV, Type::Edges(Some(edge_ty)) | Type::Edge(Some(edge_ty))) => {
            // Get the source vector type from the edge schema
            let new_ty = if let Some(edge_schema) = ctx.edge_map.get(edge_ty.as_str()) {
                let source_type = &edge_schema.from.1;
                if !ctx.vector_set.contains(source_type.as_str()) {
                    generate_error!(ctx, original_query, gs.loc.clone(), E625, edge_ty);
                }
                match cur_ty {
                    Type::Edges(_) => Some(Type::Vectors(Some(source_type.clone()))),
                    Type::Edge(_) => Some(Type::Vector(Some(source_type.clone()))),
                    _ => None,
                }
            } else {
                None
            };
            traversal
                .steps
                .push(Separator::Period(GeneratedStep::FromV));
            // Preserve collection type: multiple edges -> multiple vectors, single edge -> single vector
            match cur_ty {
                Type::Edges(_) => traversal.should_collect = ShouldCollect::ToVec,
                Type::Edge(_) => traversal.should_collect = ShouldCollect::ToObj,
                _ => {}
            }
            new_ty
        }
        (ToV, Type::Edges(Some(edge_ty)) | Type::Edge(Some(edge_ty))) => {
            // Get the target vector type from the edge schema
            let new_ty = if let Some(edge_schema) = ctx.edge_map.get(edge_ty.as_str()) {
                let target_type = &edge_schema.to.1;
                if !ctx.vector_set.contains(target_type.as_str()) {
                    generate_error!(ctx, original_query, gs.loc.clone(), E626, edge_ty);
                }
                match cur_ty {
                    Type::Edges(_) => Some(Type::Vectors(Some(target_type.clone()))),
                    Type::Edge(_) => Some(Type::Vector(Some(target_type.clone()))),
                    _ => None,
                }
            } else {
                None
            };
            traversal.steps.push(Separator::Period(GeneratedStep::ToV));
            // Preserve collection type: multiple edges -> multiple vectors, single edge -> single vector
            match cur_ty {
                Type::Edges(_) => traversal.should_collect = ShouldCollect::ToVec,
                Type::Edge(_) => traversal.should_collect = ShouldCollect::ToObj,
                _ => {}
            }
            new_ty
        }
        (ShortestPath(sp), Type::Nodes(_) | Type::Node(_)) => {
            let type_arg = sp.type_arg.clone().map(GenRef::Literal);

            // ShortestPath always uses BFS for backward compatibility
            let algorithm = None; // Will default to BFS in the generator

            traversal
                .steps
                .push(Separator::Period(GeneratedStep::ShortestPath(
                    match (sp.from.clone(), sp.to.clone()) {
                        (Some(from), Some(to)) => GeneratedShortestPath {
                            label: type_arg,
                            from: Some(GenRef::from(from)),
                            to: Some(GenRef::from(to)),
                            algorithm,
                        },
                        (Some(from), None) => GeneratedShortestPath {
                            label: type_arg,
                            from: Some(GenRef::from(from)),
                            to: None,
                            algorithm,
                        },
                        (None, Some(to)) => GeneratedShortestPath {
                            label: type_arg,
                            from: None,
                            to: Some(GenRef::from(to)),
                            algorithm,
                        },
                        (None, None) => panic!("Invalid shortest path"),
                    },
                )));
            traversal.should_collect = ShouldCollect::ToVec;
            Some(Type::Unknown)
        }
        (ShortestPathDijkstras(sp), Type::Nodes(_) | Type::Node(_)) => {
            let type_arg = sp.type_arg.clone().map(GenRef::Literal);

            // Extract weight property from anonymous traversal
            let weight_property = if let Some(ref inner_traversal) = sp.inner_traversal {
                // Check if traversal is _::{property}
                if let StartNode::Anonymous = inner_traversal.start
                    && inner_traversal.steps.len() == 1
                    && let StepType::Object(ref obj) = inner_traversal.steps[0].step
                    && obj.fields.len() == 1
                    && !obj.should_spread
                {
                    // For _::{weight}, the key is "weight"
                    Some(obj.fields[0].key.clone())
                } else {
                    None
                }
            } else {
                None
            };

            // If we have an inner traversal but couldn't extract a simple property, it's an error
            if sp.inner_traversal.is_some() && weight_property.is_none() {
                generate_error!(
                    ctx,
                    original_query,
                    sp.loc.clone(),
                    E202,
                    "complex weight expression",
                    "simple property access",
                    "ShortestPathDijkstras"
                );
                return Some(Type::Unknown);
            }

            // Validate edge type and weight property if provided
            if let Some(ref edge_type) = sp.type_arg {
                if !ctx.edge_map.contains_key(edge_type.as_str()) {
                    generate_error!(
                        ctx,
                        original_query,
                        sp.loc.clone(),
                        E102,
                        edge_type.as_str()
                    );
                } else if let Some(ref weight_prop) = weight_property {
                    // Check if the weight property exists on the edge
                    if let Some(edge_fields) = ctx.edge_fields.get(edge_type.as_str()) {
                        if let Some(field) = edge_fields.get(weight_prop.as_str()) {
                            // Validate that the weight property is numeric
                            match &field.field_type {
                                crate::helixc::parser::types::FieldType::F32
                                | crate::helixc::parser::types::FieldType::F64
                                | crate::helixc::parser::types::FieldType::I8
                                | crate::helixc::parser::types::FieldType::I16
                                | crate::helixc::parser::types::FieldType::I32
                                | crate::helixc::parser::types::FieldType::I64
                                | crate::helixc::parser::types::FieldType::U8
                                | crate::helixc::parser::types::FieldType::U16
                                | crate::helixc::parser::types::FieldType::U32
                                | crate::helixc::parser::types::FieldType::U64
                                | crate::helixc::parser::types::FieldType::U128 => {
                                    // Valid numeric type for weight
                                }
                                _ => {
                                    // Weight property must be numeric
                                    generate_error!(
                                        ctx,
                                        original_query,
                                        sp.loc.clone(),
                                        E202,
                                        weight_prop.as_str(),
                                        "numeric edge",
                                        edge_type.as_str()
                                    );
                                }
                            }
                        } else {
                            generate_error!(
                                ctx,
                                original_query,
                                sp.loc.clone(),
                                E202,
                                weight_prop.as_str(),
                                "edge",
                                edge_type.as_str()
                            );
                        }
                    }
                }
            }

            traversal
                .steps
                .push(Separator::Period(GeneratedStep::ShortestPathDijkstras(
                    match (sp.from.clone(), sp.to.clone()) {
                        (Some(from), Some(to)) => GeneratedShortestPathDijkstras {
                            label: type_arg,
                            from: Some(GenRef::from(from)),
                            to: Some(GenRef::from(to)),
                            weight_property: weight_property.clone().map(GenRef::Literal),
                        },
                        (Some(from), None) => GeneratedShortestPathDijkstras {
                            label: type_arg,
                            from: Some(GenRef::from(from)),
                            to: None,
                            weight_property: weight_property.clone().map(GenRef::Literal),
                        },
                        (None, Some(to)) => GeneratedShortestPathDijkstras {
                            label: type_arg,
                            from: None,
                            to: Some(GenRef::from(to)),
                            weight_property: weight_property.clone().map(GenRef::Literal),
                        },
                        (None, None) => panic!("Invalid shortest path dijkstras"),
                    },
                )));
            traversal.should_collect = ShouldCollect::ToVec;
            Some(Type::Unknown)
        }
        (ShortestPathBFS(sp), Type::Nodes(_) | Type::Node(_)) => {
            let type_arg = sp.type_arg.clone().map(GenRef::Literal);

            traversal
                .steps
                .push(Separator::Period(GeneratedStep::ShortestPathBFS(
                    match (sp.from.clone(), sp.to.clone()) {
                        (Some(from), Some(to)) => GeneratedShortestPathBFS {
                            label: type_arg,
                            from: Some(GenRef::from(from)),
                            to: Some(GenRef::from(to)),
                        },
                        (Some(from), None) => GeneratedShortestPathBFS {
                            label: type_arg,
                            from: Some(GenRef::from(from)),
                            to: None,
                        },
                        (None, Some(to)) => GeneratedShortestPathBFS {
                            label: type_arg,
                            from: None,
                            to: Some(GenRef::from(to)),
                        },
                        (None, None) => panic!("Invalid shortest path bfs"),
                    },
                )));
            traversal.should_collect = ShouldCollect::ToVec;
            Some(Type::Unknown)
        }
        (SearchVector(sv), Type::Vectors(Some(vector_ty)) | Type::Vector(Some(vector_ty))) => {
            if !(matches!(cur_ty, Type::Vector(_)) || matches!(cur_ty, Type::Vectors(_))) {
                generate_error!(
                    ctx,
                    original_query,
                    sv.loc.clone(),
                    E603,
                    &cur_ty.get_type_name(),
                    cur_ty.kind_str()
                );
            }
            if let Some(ref ty) = sv.vector_type
                && !ctx.vector_set.contains(ty.as_str())
            {
                generate_error!(ctx, original_query, sv.loc.clone(), E103, ty.as_str());
            }
            let vec = match &sv.data {
                Some(VectorData::Vector(v)) => {
                    VecData::Standard(GeneratedValue::Literal(GenRef::Ref(format!(
                        "[{}]",
                        v.iter()
                            .map(|f| f.to_string())
                            .collect::<Vec<String>>()
                            .join(",")
                    ))))
                }
                Some(VectorData::Identifier(i)) => {
                    is_valid_identifier(ctx, original_query, sv.loc.clone(), i.as_str());
                    // if is in params then use data.
                    let _ = type_in_scope(ctx, original_query, sv.loc.clone(), scope, i.as_str());
                    let value = gen_identifier_or_param(original_query, i.as_str(), true, false);
                    VecData::Standard(value)
                }
                Some(VectorData::Embed(e)) => {
                    let embed_data = match &e.value {
                        EvaluatesToString::Identifier(i) => EmbedData {
                            data: gen_identifier_or_param(original_query, i.as_str(), true, false),
                            model_name: gen_query.embedding_model_to_use.clone(),
                        },
                        EvaluatesToString::StringLiteral(s) => EmbedData {
                            data: GeneratedValue::Literal(GenRef::Ref(s.clone())),
                            model_name: gen_query.embedding_model_to_use.clone(),
                        },
                    };
                    let name = gen_query.add_hoisted_embed(embed_data);

                    VecData::Hoisted(name)
                }
                _ => {
                    generate_error!(
                        ctx,
                        original_query,
                        sv.loc.clone(),
                        E305,
                        ["vector_data", "SearchV"],
                        ["vector_data"]
                    );
                    VecData::Standard(GeneratedValue::Unknown)
                }
            };
            let k = match &sv.k {
                Some(k) => match &k.value {
                    EvaluatesToNumberType::I8(i) => {
                        GeneratedValue::Primitive(GenRef::Std(i.to_string()))
                    }
                    EvaluatesToNumberType::I16(i) => {
                        GeneratedValue::Primitive(GenRef::Std(i.to_string()))
                    }
                    EvaluatesToNumberType::I32(i) => {
                        GeneratedValue::Primitive(GenRef::Std(i.to_string()))
                    }
                    EvaluatesToNumberType::I64(i) => {
                        GeneratedValue::Primitive(GenRef::Std(i.to_string()))
                    }
                    EvaluatesToNumberType::U8(i) => {
                        GeneratedValue::Primitive(GenRef::Std(i.to_string()))
                    }
                    EvaluatesToNumberType::U16(i) => {
                        GeneratedValue::Primitive(GenRef::Std(i.to_string()))
                    }
                    EvaluatesToNumberType::U32(i) => {
                        GeneratedValue::Primitive(GenRef::Std(i.to_string()))
                    }
                    EvaluatesToNumberType::U64(i) => {
                        GeneratedValue::Primitive(GenRef::Std(i.to_string()))
                    }
                    EvaluatesToNumberType::U128(i) => {
                        GeneratedValue::Primitive(GenRef::Std(i.to_string()))
                    }
                    EvaluatesToNumberType::Identifier(i) => {
                        is_valid_identifier(ctx, original_query, sv.loc.clone(), i.as_str());
                        gen_identifier_or_param(original_query, i, false, true)
                    }
                    _ => {
                        generate_error!(
                            ctx,
                            original_query,
                            sv.loc.clone(),
                            E305,
                            ["k", "SearchV"],
                            ["k"]
                        );
                        GeneratedValue::Unknown
                    }
                },
                None => {
                    generate_error!(
                        ctx,
                        original_query,
                        sv.loc.clone(),
                        E305,
                        ["k", "SearchV"],
                        ["k"]
                    );
                    GeneratedValue::Unknown
                }
            };

            let vector_in_schema = ctx
                .src
                .get_latest_schema()
                .unwrap()
                .vector_schemas
                .iter()
                .find(|v| &v.name == sv.vector_type.as_ref().unwrap())
                .cloned()
                .unwrap();

               let vec = match vector_in_schema.precision {
                Precision::F64 => GenRef::Std(GeneratedPrecision::F64(vec)),
                Precision::F32 => GenRef::Std(GeneratedPrecision::F32(vec)),
                Precision::F16 => GenRef::Std(GeneratedPrecision::F16(vec)),
            };
            // Search returns nodes that contain the vectors

            // Some(GeneratedStatement::Traversal(GeneratedTraversal {
            //     traversal_type: TraversalType::Ref,
            //     steps: vec![],
            //     should_collect: ShouldCollect::ToVec,
            //     source_step: Separator::Period(SourceStep::SearchVector(
            //         GeneratedSearchVector { vec, k, pre_filter },
            //     )),
            // }))
            traversal
                .steps
                .push(Separator::Period(GeneratedStep::SearchVector(
                    SearchVectorStep { vec, k },
                )));
            // traversal.traversal_type = TraversalType::Ref;
            traversal.should_collect = ShouldCollect::ToVec;
            Some(Type::Vectors(Some(vector_ty.clone())))
        }
        // Anything else is illegal
        _ => {
            generate_error!(ctx, original_query, gs.loc.clone(), E601, &gs.loc.span);
            None
        }
    }
}
