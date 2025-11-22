//! Semantic analyzer for Helix‑QL.
use crate::helixc::analyzer::error_codes::ErrorCode;
use crate::helixc::analyzer::utils::{VariableInfo, type_in_scope};
use crate::helixc::generator::traversal_steps::EdgeType;
use crate::helixc::generator::utils::EmbedData;
use crate::{
    generate_error,
    helixc::{
        analyzer::{
            Ctx,
            errors::push_query_err,
            types::Type,
            utils::{gen_identifier_or_param, is_valid_identifier},
        },
        generator::{
            math_functions::{ExpressionContext, generate_math_expr},
            queries::Query as GeneratedQuery,
            traversal_steps::{
                FromV as GeneratedFromV, In as GeneratedIn, InE as GeneratedInE,
                Out as GeneratedOut, OutE as GeneratedOutE, SearchVectorStep,
                ShortestPath as GeneratedShortestPath,
                ShortestPathAStar as GeneratedShortestPathAStar,
                ShortestPathBFS as GeneratedShortestPathBFS,
                ShortestPathDijkstras as GeneratedShortestPathDijkstras, ShouldCollect,
                Step as GeneratedStep, ToV as GeneratedToV, Traversal as GeneratedTraversal,
                WeightCalculation,
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
    scope: &mut HashMap<&'a str, VariableInfo>,
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
                    edge_type: edge_type.clone(),
                    label: GenRef::Literal(label.clone()),
                    get_vector_data: false, // Will be updated if 'data' field is accessed
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
                    edge_type: edge_type.clone(),
                    label: GenRef::Literal(label.clone()),
                    get_vector_data: false, // Will be updated if 'data' field is accessed
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
                .push(Separator::Period(GeneratedStep::FromV(GeneratedFromV {
                    get_vector_data: false,
                })));
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
            traversal
                .steps
                .push(Separator::Period(GeneratedStep::ToV(GeneratedToV {
                    get_vector_data: false,
                })));
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
                        (None, None) => {
                            generate_error!(
                                ctx,
                                original_query,
                                sp.loc.clone(),
                                E627,
                                "ShortestPath"
                            );
                            return None;
                        }
                    },
                )));
            traversal.should_collect = ShouldCollect::ToVec;
            Some(Type::Unknown)
        }
        (ShortestPathDijkstras(sp), Type::Nodes(_) | Type::Node(_)) => {
            let type_arg = sp.type_arg.clone().map(GenRef::Literal);

            // Convert weight_expr to WeightCalculation for generator
            let weight_calculation = match &sp.weight_expr {
                Some(WeightExpression::Property(prop)) => {
                    WeightCalculation::Property(GenRef::Literal(prop.clone()))
                }
                Some(WeightExpression::Expression(expr)) => {
                    // Generate Rust code for the math expression
                    match generate_math_expr(expr, ExpressionContext::WeightCalculation) {
                        Ok(math_expr) => WeightCalculation::Expression(format!("{}", math_expr)),
                        Err(e) => {
                            generate_error!(
                                ctx,
                                original_query,
                                sp.loc.clone(),
                                E202,
                                &format!("Failed to generate weight expression: {}", e),
                                "valid math expression",
                                "ShortestPathDijkstras"
                            );
                            WeightCalculation::Default
                        }
                    }
                }
                Some(WeightExpression::Default) | None => WeightCalculation::Default,
            };

            // Extract weight property for validation (if it's a simple property)
            let weight_property = match &sp.weight_expr {
                Some(WeightExpression::Property(prop)) => Some(prop.clone()),
                _ => None,
            };

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
                            weight_calculation: weight_calculation.clone(),
                        },
                        (Some(from), None) => GeneratedShortestPathDijkstras {
                            label: type_arg,
                            from: Some(GenRef::from(from)),
                            to: None,
                            weight_calculation: weight_calculation.clone(),
                        },
                        (None, Some(to)) => GeneratedShortestPathDijkstras {
                            label: type_arg,
                            from: None,
                            to: Some(GenRef::from(to)),
                            weight_calculation: weight_calculation.clone(),
                        },
                        (None, None) => {
                            generate_error!(
                                ctx,
                                original_query,
                                sp.loc.clone(),
                                E627,
                                "ShortestPathDijkstras"
                            );
                            return None;
                        }
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
                        (None, None) => {
                            generate_error!(
                                ctx,
                                original_query,
                                sp.loc.clone(),
                                E627,
                                "ShortestPathBFS"
                            );
                            return None;
                        }
                    },
                )));
            traversal.should_collect = ShouldCollect::ToVec;
            Some(Type::Unknown)
        }
        (ShortestPathAStar(sp), Type::Nodes(_) | Type::Node(_)) => {
            let type_arg = sp.type_arg.clone().map(GenRef::Literal);

            // Generate weight calculation
            let weight_calculation = match &sp.weight_expr {
                Some(WeightExpression::Property(prop)) => {
                    WeightCalculation::Property(GenRef::Literal(prop.clone()))
                }
                Some(WeightExpression::Expression(expr)) => {
                    match generate_math_expr(expr, ExpressionContext::WeightCalculation) {
                        Ok(math_expr) => WeightCalculation::Expression(format!("{}", math_expr)),
                        Err(e) => {
                            generate_error!(
                                ctx,
                                original_query,
                                sp.loc.clone(),
                                E202,
                                &format!("Failed to generate weight expression: {}", e),
                                "valid math expression",
                                "ShortestPathAStar"
                            );
                            WeightCalculation::Default
                        }
                    }
                }
                Some(WeightExpression::Default) | None => WeightCalculation::Default,
            };

            let heuristic_property = GenRef::Literal(sp.heuristic_property.clone());

            traversal
                .steps
                .push(Separator::Period(GeneratedStep::ShortestPathAStar(match (
                    sp.from.clone(),
                    sp.to.clone(),
                ) {
                    (Some(from), Some(to)) => GeneratedShortestPathAStar {
                        label: type_arg,
                        from: Some(GenRef::from(from)),
                        to: Some(GenRef::from(to)),
                        weight_calculation,
                        heuristic_property,
                    },
                    (Some(from), None) => GeneratedShortestPathAStar {
                        label: type_arg,
                        from: Some(GenRef::from(from)),
                        to: None,
                        weight_calculation,
                        heuristic_property,
                    },
                    (None, Some(to)) => GeneratedShortestPathAStar {
                        label: type_arg,
                        from: None,
                        to: Some(GenRef::from(to)),
                        weight_calculation,
                        heuristic_property,
                    },
                    (None, None) => panic!("Invalid shortest path astar"),
                })));
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
                        type_in_scope(ctx, original_query, sv.loc.clone(), scope, i.as_str());
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

#[cfg(test)]
mod tests {
    use crate::helixc::analyzer::error_codes::ErrorCode;
    use crate::helixc::parser::{HelixParser, write_to_temp_file};

    // ============================================================================
    // Edge Direction Validation Tests
    // ============================================================================

    #[test]
    fn test_out_edge_correct_direction() {
        let source = r#"
            N::Person { name: String }
            E::Knows { From: Person, To: Person }

            QUERY test(id: ID) =>
                person <- N<Person>(id)
                friends <- person::Out<Knows>
                RETURN friends
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.is_empty());
    }

    #[test]
    fn test_in_edge_correct_direction() {
        let source = r#"
            N::Person { name: String }
            E::Follows { From: Person, To: Person }

            QUERY test(id: ID) =>
                person <- N<Person>(id)
                followers <- person::In<Follows>
                RETURN followers
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.is_empty());
    }

    #[test]
    fn test_out_edge_wrong_node_type() {
        let source = r#"
            N::Person { name: String }
            N::Company { name: String }
            E::WorksAt { From: Person, To: Company }

            QUERY test(id: ID) =>
                company <- N<Company>(id)
                employees <- company::Out<WorksAt>
                RETURN employees
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.iter().any(|d| d.error_code == ErrorCode::E207));
    }

    // ============================================================================
    // Edge-to-Node Conversion Tests
    // ============================================================================

    #[test]
    fn test_out_edge_to_target_node() {
        let source = r#"
            N::Person { name: String }
            N::Company { name: String }
            E::WorksAt { From: Person, To: Company }

            QUERY test(id: ID) =>
                person <- N<Person>(id)
                edges <- person::OutE<WorksAt>
                companies <- edges::ToN
                RETURN companies
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.is_empty());
    }

    #[test]
    fn test_out_edge_to_source_node() {
        let source = r#"
            N::Person { name: String }
            N::Company { name: String }
            E::WorksAt { From: Person, To: Company }

            QUERY test(id: ID) =>
                person <- N<Person>(id)
                edges <- person::OutE<WorksAt>
                source <- edges::FromN
                RETURN source
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.is_empty());
    }

    #[test]
    fn test_in_edge_to_source_node() {
        let source = r#"
            N::Person { name: String }
            E::Follows { From: Person, To: Person }

            QUERY test(id: ID) =>
                person <- N<Person>(id)
                edges <- person::InE<Follows>
                followers <- edges::FromN
                RETURN followers
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.is_empty());
    }

    // ============================================================================
    // Multi-Type Graph Tests
    // ============================================================================

    #[test]
    fn test_multi_type_traversal() {
        let source = r#"
            N::Person { name: String }
            N::Company { name: String }
            E::WorksAt { From: Person, To: Company }
            E::LocatedIn { From: Company, To: Person }

            QUERY test(id: ID) =>
                person <- N<Person>(id)
                companies <- person::Out<WorksAt>
                locations <- companies::Out<LocatedIn>
                RETURN locations
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.is_empty());
    }

    #[test]
    fn test_bidirectional_edges() {
        let source = r#"
            N::Person { name: String }
            E::Knows { From: Person, To: Person }

            QUERY test(id: ID) =>
                person <- N<Person>(id)
                outgoing <- person::Out<Knows>
                incoming <- person::In<Knows>
                RETURN outgoing, incoming
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.is_empty());
    }

    // ============================================================================
    // Edge Type Validation Tests
    // ============================================================================

    #[test]
    fn test_undeclared_edge_in_out_traversal() {
        let source = r#"
            N::Person { name: String }

            QUERY test(id: ID) =>
                person <- N<Person>(id)
                related <- person::Out<UndeclaredEdge>
                RETURN related
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.iter().any(|d| d.error_code == ErrorCode::E102));
    }

    #[test]
    fn test_undeclared_edge_in_out_e_traversal() {
        let source = r#"
            N::Person { name: String }

            QUERY test(id: ID) =>
                person <- N<Person>(id)
                edges <- person::OutE<UndeclaredEdge>
                RETURN edges
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.iter().any(|d| d.error_code == ErrorCode::E102));
    }

    // ============================================================================
    // Complex Traversal Pattern Tests
    // ============================================================================

    #[test]
    fn test_edge_chain_from_to() {
        let source = r#"
            N::Person { name: String }
            N::Company { name: String }
            E::WorksAt { From: Person, To: Company }

            QUERY test(personId: ID, companyId: ID) =>
                person <- N<Person>(personId)
                company <- N<Company>(companyId)
                personEdges <- person::OutE<WorksAt>
                companyEdges <- company::InE<WorksAt>
                personCompanies <- personEdges::ToN
                companyPeople <- companyEdges::FromN
                RETURN personCompanies, companyPeople
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.is_empty());
    }
}
