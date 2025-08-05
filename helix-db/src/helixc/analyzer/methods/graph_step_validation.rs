//! Semantic analyzer for Helix‑QL.
use crate::helixc::analyzer::error_codes::ErrorCode;
use crate::helixc::analyzer::utils::type_in_scope;
use crate::{
    generate_error,
    helix_engine::graph_core::ops::source::add_e::EdgeType,
    helixc::{
        analyzer::{
            analyzer::Ctx,
            errors::push_query_err,
            types::Type,
            utils::{gen_identifier_or_param, is_valid_identifier},
        },
        generator::{
            traversal_steps::{
                In as GeneratedIn, InE as GeneratedInE, Out as GeneratedOut, OutE as GeneratedOutE,
                SearchVectorStep, ShortestPath as GeneratedShortestPath, ShouldCollect,
                Step as GeneratedStep, Traversal as GeneratedTraversal,
            },
            utils::{GenRef, GeneratedValue, Separator, VecData},
            queries::Query as GeneratedQuery,
        },
        parser::helix_parser::*,
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
            let edge = ctx.edge_map.get(label.as_str());
            if edge.is_none() {
                generate_error!(ctx, original_query, gs.loc.clone(), E102, label.as_str());
                return None;
            }
            match edge.unwrap().from.1 == node_label.clone() {
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
            let edge = ctx.edge_map.get(label.as_str());
            if edge.is_none() {
                generate_error!(ctx, original_query, gs.loc.clone(), E102, label.as_str());
                return None;
            }

            match edge.unwrap().to.1 == node_label.clone() {
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
            let edge = ctx.edge_map.get(label.as_str());
            // assert!(edge.is_some()); // make sure is caught
            if edge.is_none() {
                generate_error!(ctx, original_query, gs.loc.clone(), E102, label.as_str());
                return None;
            }
            match edge.unwrap().from.1 == node_label.clone() {
                true => {
                    if EdgeType::Node == edge_type {
                        Some(Type::Nodes(Some(edge.unwrap().to.1.clone())))
                    } else if EdgeType::Vec == edge_type {
                        Some(Type::Vectors(Some(edge.unwrap().to.1.clone())))
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
            let edge = ctx.edge_map.get(label.as_str());
            // assert!(edge.is_some());
            if edge.is_none() {
                generate_error!(ctx, original_query, gs.loc.clone(), E102, label.as_str());
                return None;
            }

            match edge.unwrap().to.1 == node_label.clone() {
                true => {
                    if EdgeType::Node == edge_type {
                        Some(Type::Nodes(Some(edge.unwrap().from.1.clone())))
                    } else if EdgeType::Vec == edge_type {
                        Some(Type::Vectors(Some(edge.unwrap().from.1.clone())))
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
            traversal.should_collect = ShouldCollect::ToVec;
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
            traversal.should_collect = ShouldCollect::ToVec;
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
            traversal.should_collect = ShouldCollect::ToVec;
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
            traversal.should_collect = ShouldCollect::ToVec;
            new_ty
        }
        (ShortestPath(sp), Type::Nodes(_) | Type::Node(_)) => {
            let type_arg = sp.type_arg.clone().map(GenRef::Literal);
            // check edge type is valid
            traversal
                .steps
                .push(Separator::Period(GeneratedStep::ShortestPath(
                    match (sp.from.clone(), sp.to.clone()) {
                        // TODO: get rid of clone
                        (Some(from), Some(to)) => GeneratedShortestPath {
                            label: type_arg,
                            from: Some(GenRef::from(from)),
                            to: Some(GenRef::from(to)),
                        },
                        (Some(from), None) => GeneratedShortestPath {
                            label: type_arg,
                            from: Some(GenRef::from(from)),
                            to: None,
                        },
                        (None, Some(to)) => GeneratedShortestPath {
                            label: type_arg,
                            from: None,
                            to: Some(GenRef::from(to)),
                        },
                        (None, None) => panic!("Invalid shortest path"),
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
            if let Some(ref ty) = sv.vector_type {
                if !ctx.vector_set.contains(ty.as_str()) {
                    generate_error!(ctx, original_query, sv.loc.clone(), E103, ty.as_str());
                }
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
                    let value = gen_identifier_or_param(original_query, i.as_str(), false, true);
                    VecData::Standard(value)
                }
                Some(VectorData::Embed(e)) => match &e.value {
                    EvaluatesToString::Identifier(i) => VecData::Embed {
                        data: gen_identifier_or_param(original_query, i.as_str(), true, false),
                        model_name: gen_query.embedding_model_to_use.clone(),
                    },
                    EvaluatesToString::StringLiteral(s) => VecData::Embed {
                        data: GeneratedValue::Literal(GenRef::Ref(s.clone())),
                        model_name: gen_query.embedding_model_to_use.clone(),
                    },
                },
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
