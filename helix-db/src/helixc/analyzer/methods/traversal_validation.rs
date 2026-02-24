use crate::helixc::analyzer::error_codes::*;
use crate::helixc::analyzer::utils::{
    DEFAULT_VAR_NAME, VariableInfo, check_identifier_is_fieldtype, validate_embed_string_type,
};
use crate::helixc::generator::bool_ops::{Contains, IsIn, PropertyEq, PropertyNeq};
use crate::helixc::generator::source_steps::{SearchVector, VFromID, VFromType};
use crate::helixc::generator::traversal_steps::{AggregateBy, GroupBy};
use crate::helixc::generator::utils::{EmbedData, VecData};
use crate::{
    generate_error,
    helixc::{
        analyzer::{
            Ctx,
            errors::push_query_err,
            methods::{
                exclude_validation::validate_exclude, graph_step_validation::apply_graph_step,
                infer_expr_type::infer_expr_type, object_validation::validate_object,
            },
            types::{AggregateInfo, Type},
            utils::{
                field_exists_on_item_type, gen_identifier_or_param, get_singular_type,
                is_valid_identifier, type_in_scope,
            },
        },
        generator::{
            bool_ops::{BoExp, BoolOp, Eq, Gt, Gte, Lt, Lte, Neq},
            queries::Query as GeneratedQuery,
            source_steps::{EFromID, EFromType, NFromID, NFromIndex, NFromType, SourceStep},
            statements::Statement as GeneratedStatement,
            traversal_steps::{
                OrderBy, Range, ShouldCollect, Step as GeneratedStep,
                Traversal as GeneratedTraversal, TraversalType, Where, WhereRef,
            },
            utils::{GenRef, GeneratedValue, Order, Separator},
        },
        parser::{location::Loc, types::*},
    },
    protocol::value::Value,
};
use indexmap::IndexMap;
use paste::paste;
use std::collections::HashMap;

/// Check if a property name is a reserved property and return its expected type
fn get_reserved_property_type(prop_name: &str, item_type: &Type) -> Option<FieldType> {
    match prop_name {
        "id" | "ID" | "Id" => Some(FieldType::Uuid),
        "label" | "Label" => Some(FieldType::String),
        "version" | "Version" => Some(FieldType::I8),
        "from_node" | "fromNode" | "FromNode" => {
            // Only valid for edges
            match item_type {
                Type::Edge(_) | Type::Edges(_) => Some(FieldType::Uuid),
                _ => None,
            }
        }
        "to_node" | "toNode" | "ToNode" => {
            // Only valid for edges
            match item_type {
                Type::Edge(_) | Type::Edges(_) => Some(FieldType::Uuid),
                _ => None,
            }
        }
        "deleted" | "Deleted" => {
            // Only valid for vectors
            match item_type {
                Type::Vector(_) | Type::Vectors(_) => Some(FieldType::Boolean),
                _ => None,
            }
        }
        "level" | "Level" => {
            // Only valid for vectors
            match item_type {
                Type::Vector(_) | Type::Vectors(_) => Some(FieldType::U64),
                _ => None,
            }
        }
        "distance" | "Distance" => {
            // Only valid for vectors
            match item_type {
                Type::Vector(_) | Type::Vectors(_) => Some(FieldType::F64),
                _ => None,
            }
        }
        "data" | "Data" => {
            // Only valid for vectors
            match item_type {
                Type::Vector(_) | Type::Vectors(_) => {
                    Some(FieldType::Array(Box::new(FieldType::F64)))
                }
                _ => None,
            }
        }
        _ => None,
    }
}

/// Checks if a traversal is a "simple" property access (no graph navigation steps)
/// and returns the variable name and property name if so.
///
/// A simple traversal is one that only accesses properties on an already-bound variable,
/// without any graph navigation (Out, In, etc.). For example: `toUser::{login}`
///
/// Returns: Some((variable_name, property_name)) if simple, None otherwise
fn is_simple_property_traversal(tr: &Traversal) -> Option<(String, String)> {
    // Check if the start is an identifier (not a type-based query)
    let var_name = match &tr.start {
        StartNode::Identifier(id) => id.clone(),
        _ => return None,
    };

    // Check if there's exactly one step and it's an Object (property access)
    if tr.steps.len() != 1 {
        return None;
    }

    // Check if the single step is an Object step (property access like {login})
    match &tr.steps[0].step {
        StepType::Object(obj) => {
            // Check if it's a simple property fetch (single field, no spread)
            if obj.fields.len() == 1 && !obj.should_spread {
                let field = &obj.fields[0];
                // Check if it's a simple field selection (Empty or Identifier, not a complex expression)
                match &field.value.value {
                    FieldValueType::Empty => return Some((var_name, field.key.clone())),
                    FieldValueType::Identifier(identifier) => {
                        return Some((var_name, identifier.clone()));
                    }
                    _ => return None,
                }
            }
            None
        }
        _ => None,
    }
}

fn mutation_field_name_locs(fields: &[FieldAddition]) -> Vec<(&str, &Loc)> {
    fields
        .iter()
        .map(|field| (field.key.as_str(), &field.loc))
        .collect()
}

fn normalize_mutation_field_value<'a>(
    ctx: &mut Ctx<'a>,
    original_query: &'a Query,
    scope: &HashMap<&'a str, VariableInfo>,
    field_value: &FieldValue,
) -> GeneratedValue {
    match &field_value.value {
        FieldValueType::Identifier(identifier) => {
            is_valid_identifier(
                ctx,
                original_query,
                field_value.loc.clone(),
                identifier.as_str(),
            );
            type_in_scope(
                ctx,
                original_query,
                field_value.loc.clone(),
                scope,
                identifier.as_str(),
            );
            gen_identifier_or_param(original_query, identifier.as_str(), true, true)
        }
        FieldValueType::Literal(literal) => match literal {
            Value::String(value) => GeneratedValue::Literal(GenRef::Literal(value.clone())),
            other => GeneratedValue::Primitive(GenRef::Std(other.inner_stringify())),
        },
        FieldValueType::Expression(expression) => match &expression.expr {
            ExpressionType::Identifier(identifier) => {
                is_valid_identifier(
                    ctx,
                    original_query,
                    expression.loc.clone(),
                    identifier.as_str(),
                );
                type_in_scope(
                    ctx,
                    original_query,
                    expression.loc.clone(),
                    scope,
                    identifier.as_str(),
                );
                gen_identifier_or_param(original_query, identifier.as_str(), true, true)
            }
            ExpressionType::StringLiteral(value) => {
                GeneratedValue::Literal(GenRef::Literal(value.to_string()))
            }
            ExpressionType::IntegerLiteral(value) => {
                GeneratedValue::Primitive(GenRef::Std(value.to_string()))
            }
            ExpressionType::FloatLiteral(value) => {
                GeneratedValue::Primitive(GenRef::Std(value.to_string()))
            }
            ExpressionType::BooleanLiteral(value) => {
                GeneratedValue::Primitive(GenRef::Std(value.to_string()))
            }
            other => {
                generate_error!(
                    ctx,
                    original_query,
                    expression.loc.clone(),
                    E206,
                    &format!("{:?}", other)
                );
                GeneratedValue::Unknown
            }
        },
        other => {
            generate_error!(
                ctx,
                original_query,
                field_value.loc.clone(),
                E206,
                &format!("{:?}", other)
            );
            GeneratedValue::Unknown
        }
    }
}

fn normalize_mutation_properties<'a>(
    ctx: &mut Ctx<'a>,
    original_query: &'a Query,
    scope: &HashMap<&'a str, VariableInfo>,
    fields: &[FieldAddition],
) -> Vec<(String, GeneratedValue)> {
    fields
        .iter()
        .map(|field| {
            (
                field.key.clone(),
                normalize_mutation_field_value(ctx, original_query, scope, &field.value),
            )
        })
        .collect()
}

fn upsert_source_and_plural(traversal_type: &TraversalType) -> (Option<GenRef<String>>, bool) {
    match traversal_type {
        TraversalType::FromSingle(var) => (Some(var.clone()), false),
        TraversalType::FromIter(var) => (Some(var.clone()), true),
        _ => (None, true),
    }
}

fn finalize_mutation_step(
    cur_ty: &mut Type,
    gen_traversal: &mut GeneratedTraversal,
    excluded: &mut HashMap<&str, Loc>,
) {
    *cur_ty = cur_ty.clone().into_single();
    gen_traversal.should_collect = ShouldCollect::No;
    excluded.clear();
}

fn normalize_upsert_edge_connection_value<'a>(
    ctx: &mut Ctx<'a>,
    original_query: &'a Query,
    scope: &HashMap<&'a str, VariableInfo>,
    endpoint: Option<&IdType>,
    loc: &Loc,
    missing_message: &str,
) -> GeneratedValue {
    match endpoint {
        Some(IdType::Identifier { value, loc }) => {
            is_valid_identifier(ctx, original_query, loc.clone(), value.as_str());
            type_in_scope(ctx, original_query, loc.clone(), scope, value.as_str());
            gen_identifier_or_param(original_query, value.as_str(), false, false)
        }
        Some(IdType::Literal { value, .. }) => {
            GeneratedValue::Literal(GenRef::Literal(value.clone()))
        }
        Some(_) => unreachable!("parser guarantees UpsertE endpoints are Identifier or Literal"),
        None => {
            generate_error!(ctx, original_query, loc.clone(), E601, missing_message);
            GeneratedValue::Unknown
        }
    }
}

fn validate_update_step<'a>(
    ctx: &mut Ctx<'a>,
    original_query: &'a Query,
    scope: &HashMap<&'a str, VariableInfo>,
    cur_ty: &mut Type,
    update: &Update,
    gen_traversal: &mut GeneratedTraversal,
    excluded: &mut HashMap<&str, Loc>,
) -> Option<Type> {
    match &*cur_ty {
        Type::Node(Some(_)) | Type::Nodes(Some(_)) | Type::Edge(Some(_)) | Type::Edges(Some(_)) => {
            field_exists_on_item_type(
                ctx,
                original_query,
                get_singular_type(cur_ty.clone()),
                mutation_field_name_locs(&update.fields),
            );
        }
        other => {
            generate_error!(
                ctx,
                original_query,
                update.loc.clone(),
                E604,
                &other.get_type_name()
            );
            return Some(cur_ty.clone());
        }
    }

    gen_traversal.traversal_type = TraversalType::Update(Some(normalize_mutation_properties(
        ctx,
        original_query,
        scope,
        &update.fields,
    )));
    finalize_mutation_step(cur_ty, gen_traversal, excluded);
    None
}

fn validate_upsert_step<'a>(
    ctx: &mut Ctx<'a>,
    original_query: &'a Query,
    scope: &HashMap<&'a str, VariableInfo>,
    cur_ty: &mut Type,
    upsert: &Upsert,
    gen_traversal: &mut GeneratedTraversal,
    excluded: &mut HashMap<&str, Loc>,
) -> Option<Type> {
    let source = match &gen_traversal.traversal_type {
        TraversalType::FromSingle(var) | TraversalType::FromIter(var) => Some(var.clone()),
        _ => None,
    };

    let label = match &*cur_ty {
        Type::Node(Some(ty))
        | Type::Nodes(Some(ty))
        | Type::Edge(Some(ty))
        | Type::Edges(Some(ty))
        | Type::Vector(Some(ty))
        | Type::Vectors(Some(ty)) => {
            field_exists_on_item_type(
                ctx,
                original_query,
                get_singular_type(cur_ty.clone()),
                mutation_field_name_locs(&upsert.fields),
            );
            ty.clone()
        }
        other => {
            generate_error!(
                ctx,
                original_query,
                upsert.loc.clone(),
                E604,
                &other.get_type_name()
            );
            return Some(cur_ty.clone());
        }
    };

    gen_traversal.traversal_type = TraversalType::Upsert {
        source,
        label,
        properties: Some(normalize_mutation_properties(
            ctx,
            original_query,
            scope,
            &upsert.fields,
        )),
    };
    finalize_mutation_step(cur_ty, gen_traversal, excluded);
    None
}

fn validate_upsert_n_step<'a>(
    ctx: &mut Ctx<'a>,
    original_query: &'a Query,
    scope: &HashMap<&'a str, VariableInfo>,
    cur_ty: &mut Type,
    upsert: &UpsertN,
    gen_traversal: &mut GeneratedTraversal,
    gen_query: &mut GeneratedQuery,
    excluded: &mut HashMap<&str, Loc>,
) -> Option<Type> {
    let (source, source_is_plural) = upsert_source_and_plural(&gen_traversal.traversal_type);

    let label = match &*cur_ty {
        Type::Node(Some(ty)) | Type::Nodes(Some(ty)) => {
            field_exists_on_item_type(
                ctx,
                original_query,
                Type::Node(Some(ty.clone())),
                mutation_field_name_locs(&upsert.fields),
            );
            ty.clone()
        }
        other => {
            generate_error!(
                ctx,
                original_query,
                upsert.loc.clone(),
                E604,
                &format!(
                    "UpsertN requires a Node type, found {:?}",
                    other.get_type_name()
                )
            );
            return Some(cur_ty.clone());
        }
    };

    gen_query.is_mut = true;
    gen_traversal.traversal_type = TraversalType::UpsertN {
        source,
        source_is_plural,
        label,
        properties: Some(normalize_mutation_properties(
            ctx,
            original_query,
            scope,
            &upsert.fields,
        )),
    };
    finalize_mutation_step(cur_ty, gen_traversal, excluded);
    None
}

fn validate_upsert_e_step<'a>(
    ctx: &mut Ctx<'a>,
    original_query: &'a Query,
    scope: &HashMap<&'a str, VariableInfo>,
    cur_ty: &mut Type,
    upsert: &UpsertE,
    gen_traversal: &mut GeneratedTraversal,
    gen_query: &mut GeneratedQuery,
    excluded: &mut HashMap<&str, Loc>,
) -> Option<Type> {
    let (source, source_is_plural) = upsert_source_and_plural(&gen_traversal.traversal_type);

    let label = match &*cur_ty {
        Type::Edge(Some(ty)) | Type::Edges(Some(ty)) => {
            if !upsert.fields.is_empty() {
                field_exists_on_item_type(
                    ctx,
                    original_query,
                    Type::Edge(Some(ty.clone())),
                    mutation_field_name_locs(&upsert.fields),
                );
            }
            ty.clone()
        }
        other => {
            generate_error!(
                ctx,
                original_query,
                upsert.loc.clone(),
                E604,
                &format!(
                    "UpsertE requires an Edge type, found {:?}",
                    other.get_type_name()
                )
            );
            return Some(cur_ty.clone());
        }
    };

    let from = normalize_upsert_edge_connection_value(
        ctx,
        original_query,
        scope,
        upsert.connection.from_id.as_ref(),
        &upsert.loc,
        "Missing From() for UpsertE",
    );
    let to = normalize_upsert_edge_connection_value(
        ctx,
        original_query,
        scope,
        upsert.connection.to_id.as_ref(),
        &upsert.loc,
        "Missing To() for UpsertE",
    );

    gen_query.is_mut = true;
    gen_traversal.traversal_type = TraversalType::UpsertE {
        source,
        source_is_plural,
        label,
        properties: Some(normalize_mutation_properties(
            ctx,
            original_query,
            scope,
            &upsert.fields,
        )),
        from,
        to,
    };
    finalize_mutation_step(cur_ty, gen_traversal, excluded);
    None
}

fn validate_upsert_v_step<'a>(
    ctx: &mut Ctx<'a>,
    original_query: &'a Query,
    scope: &HashMap<&'a str, VariableInfo>,
    cur_ty: &mut Type,
    upsert: &UpsertV,
    gen_traversal: &mut GeneratedTraversal,
    gen_query: &mut GeneratedQuery,
    excluded: &mut HashMap<&str, Loc>,
) -> Option<Type> {
    let (source, source_is_plural) = upsert_source_and_plural(&gen_traversal.traversal_type);

    let label = match &*cur_ty {
        Type::Vector(Some(ty)) | Type::Vectors(Some(ty)) => {
            field_exists_on_item_type(
                ctx,
                original_query,
                Type::Vector(Some(ty.clone())),
                mutation_field_name_locs(&upsert.fields),
            );
            ty.clone()
        }
        other => {
            generate_error!(
                ctx,
                original_query,
                upsert.loc.clone(),
                E604,
                &format!(
                    "UpsertV requires a Vector type, found {:?}",
                    other.get_type_name()
                )
            );
            return Some(cur_ty.clone());
        }
    };

    let vec_data = match &upsert.data {
        Some(VectorData::Identifier(id)) => {
            is_valid_identifier(ctx, original_query, upsert.loc.clone(), id.as_str());
            if let Some(var_info) = scope.get(id.as_str()) {
                let expected_type = Type::Array(Box::new(Type::Scalar(FieldType::F64)));
                if var_info.ty != expected_type {
                    generate_error!(
                        ctx,
                        original_query,
                        upsert.loc.clone(),
                        E205,
                        id.as_str(),
                        &var_info.ty.to_string(),
                        "[F64]",
                        "UpsertV",
                        &label
                    );
                }
            } else {
                generate_error!(ctx, original_query, upsert.loc.clone(), E301, id.as_str());
            }
            Some(VecData::Standard(gen_identifier_or_param(
                original_query,
                id.as_str(),
                true,
                false,
            )))
        }
        Some(VectorData::Vector(vec)) => {
            let vec_str = format!(
                "&[{}]",
                vec.iter()
                    .map(|f| f.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            Some(VecData::Standard(GeneratedValue::Primitive(GenRef::Ref(
                vec_str,
            ))))
        }
        Some(VectorData::Embed(embed)) => {
            let embed_data = match &embed.value {
                EvaluatesToString::Identifier(id) => {
                    is_valid_identifier(ctx, original_query, embed.loc.clone(), id.as_str());
                    type_in_scope(ctx, original_query, embed.loc.clone(), scope, id.as_str());
                    validate_embed_string_type(
                        ctx,
                        original_query,
                        embed.loc.clone(),
                        scope,
                        id.as_str(),
                    );
                    EmbedData {
                        data: gen_identifier_or_param(original_query, id.as_str(), true, false),
                        model_name: gen_query.embedding_model_to_use.clone(),
                    }
                }
                EvaluatesToString::StringLiteral(value) => EmbedData {
                    data: GeneratedValue::Literal(GenRef::Ref(value.clone())),
                    model_name: gen_query.embedding_model_to_use.clone(),
                },
            };
            Some(VecData::Hoisted(gen_query.add_hoisted_embed(embed_data)))
        }
        None => None,
    };

    gen_query.is_mut = true;
    gen_traversal.traversal_type = TraversalType::UpsertV {
        source,
        source_is_plural,
        label,
        properties: Some(normalize_mutation_properties(
            ctx,
            original_query,
            scope,
            &upsert.fields,
        )),
        vec_data,
    };
    finalize_mutation_step(cur_ty, gen_traversal, excluded);
    None
}

fn validate_search_vector_start<'a>(
    ctx: &mut Ctx<'a>,
    sv: &'a crate::helixc::parser::types::SearchVector,
    scope: &mut HashMap<&'a str, VariableInfo>,
    original_query: &'a Query,
    gen_traversal: &mut GeneratedTraversal,
    gen_query: &mut GeneratedQuery,
) -> Option<Type> {
    if let Some(ref ty) = sv.vector_type
        && !ctx.vector_set.contains(ty.as_str())
    {
        generate_error!(ctx, original_query, sv.loc.clone(), E103, ty.as_str());
    }

    let vec: VecData = match &sv.data {
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
            let _ = type_in_scope(ctx, original_query, sv.loc.clone(), scope, i.as_str());
            VecData::Standard(gen_identifier_or_param(
                original_query,
                i.as_str(),
                true,
                false,
            ))
        }
        Some(VectorData::Embed(e)) => {
            let embed_data = match &e.value {
                EvaluatesToString::Identifier(i) => {
                    type_in_scope(ctx, original_query, sv.loc.clone(), scope, i.as_str());
                    validate_embed_string_type(
                        ctx,
                        original_query,
                        sv.loc.clone(),
                        scope,
                        i.as_str(),
                    );
                    EmbedData {
                        data: gen_identifier_or_param(original_query, i.as_str(), true, false),
                        model_name: gen_query.embedding_model_to_use.clone(),
                    }
                }
                EvaluatesToString::StringLiteral(s) => EmbedData {
                    data: GeneratedValue::Literal(GenRef::Ref(s.clone())),
                    model_name: gen_query.embedding_model_to_use.clone(),
                },
            };

            VecData::Hoisted(gen_query.add_hoisted_embed(embed_data))
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
            VecData::Unknown
        }
    };

    let k = match &sv.k {
        Some(k) => match &k.value {
            EvaluatesToNumberType::I8(i) => GeneratedValue::Primitive(GenRef::Std(i.to_string())),
            EvaluatesToNumberType::I16(i) => GeneratedValue::Primitive(GenRef::Std(i.to_string())),
            EvaluatesToNumberType::I32(i) => GeneratedValue::Primitive(GenRef::Std(i.to_string())),
            EvaluatesToNumberType::I64(i) => GeneratedValue::Primitive(GenRef::Std(i.to_string())),
            EvaluatesToNumberType::U8(i) => GeneratedValue::Primitive(GenRef::Std(i.to_string())),
            EvaluatesToNumberType::U16(i) => GeneratedValue::Primitive(GenRef::Std(i.to_string())),
            EvaluatesToNumberType::U32(i) => GeneratedValue::Primitive(GenRef::Std(i.to_string())),
            EvaluatesToNumberType::U64(i) => GeneratedValue::Primitive(GenRef::Std(i.to_string())),
            EvaluatesToNumberType::U128(i) => GeneratedValue::Primitive(GenRef::Std(i.to_string())),
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
            generate_error!(ctx, original_query, sv.loc.clone(), E601, &sv.loc.span);
            GeneratedValue::Unknown
        }
    };

    let pre_filter: Option<Vec<BoExp>> = match &sv.pre_filter {
        Some(expr) => {
            let (_, stmt) = infer_expr_type(
                ctx,
                expr,
                scope,
                original_query,
                Some(Type::Vector(sv.vector_type.clone())),
                gen_query,
            );

            let Some(stmt) = stmt else {
                return None;
            };

            let mut gen_traversal = GeneratedTraversal {
                traversal_type: TraversalType::FromIter(GenRef::Std("v".to_string())),
                steps: vec![],
                should_collect: ShouldCollect::ToVec,
                source_step: Separator::Empty(SourceStep::Anonymous),
                ..Default::default()
            };

            if let Some(where_expr) = boexp_from_where_stmt(
                ctx,
                original_query,
                expr.loc.clone(),
                stmt,
                None,
                false,
                false,
                None,
            ) {
                gen_traversal
                    .steps
                    .push(Separator::Period(GeneratedStep::Where(Where::Ref(
                        WhereRef { expr: where_expr },
                    ))));
                Some(vec![BoExp::Expr(gen_traversal)])
            } else {
                None
            }
        }
        None => None,
    };

    gen_traversal.traversal_type = TraversalType::Ref;
    gen_traversal.should_collect = ShouldCollect::ToVec;

    let label = match &sv.vector_type {
        Some(vt) => GenRef::Literal(vt.clone()),
        None => {
            generate_error!(
                ctx,
                original_query,
                sv.loc.clone(),
                E601,
                "search vector requires vector_type"
            );
            return None;
        }
    };

    gen_traversal.source_step = Separator::Period(SourceStep::SearchVector(SearchVector {
        label,
        vec,
        k,
        pre_filter,
    }));

    Some(Type::Vectors(sv.vector_type.clone()))
}

fn apply_rerank_rrf_step<'a>(
    ctx: &mut Ctx<'a>,
    original_query: &'a Query,
    scope: &HashMap<&'a str, VariableInfo>,
    rerank_rrf: &RerankRRF,
    gen_traversal: &mut GeneratedTraversal,
) {
    let k = rerank_rrf.k.as_ref().map(|k_expr| match &k_expr.expr {
        ExpressionType::Identifier(id) => {
            is_valid_identifier(ctx, original_query, k_expr.loc.clone(), id.as_str());
            type_in_scope(ctx, original_query, k_expr.loc.clone(), scope, id.as_str());
            gen_identifier_or_param(original_query, id.as_str(), false, true)
        }
        ExpressionType::IntegerLiteral(val) => {
            GeneratedValue::Primitive(GenRef::Std(val.to_string()))
        }
        ExpressionType::FloatLiteral(val) => {
            GeneratedValue::Primitive(GenRef::Std(val.to_string()))
        }
        _ => {
            generate_error!(
                ctx,
                original_query,
                k_expr.loc.clone(),
                E206,
                &k_expr.expr.to_string()
            );
            GeneratedValue::Unknown
        }
    });

    gen_traversal
        .steps
        .push(Separator::Period(GeneratedStep::RerankRRF(
            crate::helixc::generator::traversal_steps::RerankRRF { k },
        )));
}

fn apply_rerank_mmr_step<'a>(
    ctx: &mut Ctx<'a>,
    original_query: &'a Query,
    scope: &HashMap<&'a str, VariableInfo>,
    rerank_mmr: &RerankMMR,
    gen_traversal: &mut GeneratedTraversal,
) {
    let lambda = match &rerank_mmr.lambda.expr {
        ExpressionType::Identifier(id) => {
            is_valid_identifier(
                ctx,
                original_query,
                rerank_mmr.lambda.loc.clone(),
                id.as_str(),
            );
            type_in_scope(
                ctx,
                original_query,
                rerank_mmr.lambda.loc.clone(),
                scope,
                id.as_str(),
            );
            Some(gen_identifier_or_param(
                original_query,
                id.as_str(),
                false,
                true,
            ))
        }
        ExpressionType::FloatLiteral(val) => {
            Some(GeneratedValue::Primitive(GenRef::Std(val.to_string())))
        }
        ExpressionType::IntegerLiteral(val) => {
            Some(GeneratedValue::Primitive(GenRef::Std(val.to_string())))
        }
        _ => {
            generate_error!(
                ctx,
                original_query,
                rerank_mmr.lambda.loc.clone(),
                E206,
                &rerank_mmr.lambda.expr.to_string()
            );
            None
        }
    };

    let distance = if let Some(MMRDistance::Identifier(id)) = &rerank_mmr.distance {
        is_valid_identifier(ctx, original_query, rerank_mmr.loc.clone(), id.as_str());
        type_in_scope(
            ctx,
            original_query,
            rerank_mmr.loc.clone(),
            scope,
            id.as_str(),
        );
        Some(crate::helixc::generator::traversal_steps::MMRDistanceMethod::Identifier(id.clone()))
    } else {
        rerank_mmr.distance.as_ref().map(|d| match d {
            MMRDistance::Cosine => {
                crate::helixc::generator::traversal_steps::MMRDistanceMethod::Cosine
            }
            MMRDistance::Euclidean => {
                crate::helixc::generator::traversal_steps::MMRDistanceMethod::Euclidean
            }
            MMRDistance::DotProduct => {
                crate::helixc::generator::traversal_steps::MMRDistanceMethod::DotProduct
            }
            MMRDistance::Identifier(id) => {
                crate::helixc::generator::traversal_steps::MMRDistanceMethod::Identifier(id.clone())
            }
        })
    };

    gen_traversal
        .steps
        .push(Separator::Period(GeneratedStep::RerankMMR(
            crate::helixc::generator::traversal_steps::RerankMMR { lambda, distance },
        )));
}

fn resolve_identifier_start<'a>(
    ctx: &mut Ctx<'a>,
    tr: &'a Traversal,
    identifier: &str,
    scope: &mut HashMap<&'a str, VariableInfo>,
    original_query: &'a Query,
    gen_traversal: &mut GeneratedTraversal,
) -> Type {
    match is_valid_identifier(ctx, original_query, tr.loc.clone(), identifier) {
        true => {
            if let Some(var_info) = scope.get_mut(identifier) {
                var_info.increment_reference();

                if var_info.reference_count > 1 {
                    gen_traversal.is_reused_variable = true;
                }

                gen_traversal.traversal_type = if var_info.is_single {
                    TraversalType::FromSingle(GenRef::Std(identifier.to_string()))
                } else {
                    TraversalType::FromIter(GenRef::Std(identifier.to_string()))
                };
                gen_traversal.source_step =
                    Separator::Empty(SourceStep::Identifier(GenRef::Std(identifier.to_string())));
                var_info.ty.clone()
            } else {
                generate_error!(ctx, original_query, tr.loc.clone(), E301, identifier);
                Type::Unknown
            }
        }
        false => Type::Unknown,
    }
}

fn resolve_anonymous_start<'a>(
    ctx: &mut Ctx<'a>,
    tr: &'a Traversal,
    original_query: &'a Query,
    parent_ty: Option<Type>,
    gen_traversal: &mut GeneratedTraversal,
) -> Option<Type> {
    let Some(parent) = parent_ty else {
        generate_error!(
            ctx,
            original_query,
            tr.loc.clone(),
            E601,
            "anonymous traversal requires parent type"
        );
        return None;
    };

    gen_traversal.traversal_type =
        TraversalType::FromSingle(GenRef::Std(DEFAULT_VAR_NAME.to_string()));
    gen_traversal.source_step = Separator::Empty(SourceStep::Anonymous);
    Some(parent)
}

fn resolve_node_start<'a>(
    ctx: &mut Ctx<'a>,
    tr: &'a Traversal,
    node_type: &str,
    ids: &Option<Vec<IdType>>,
    scope: &mut HashMap<&'a str, VariableInfo>,
    original_query: &'a Query,
    gen_traversal: &mut GeneratedTraversal,
) -> Option<Type> {
    if !ctx.node_set.contains(node_type) {
        generate_error!(ctx, original_query, tr.loc.clone(), E101, node_type);
        return None;
    }

    let ty = if let Some(ids) = ids {
        assert!(ids.len() == 1, "multiple ids not supported yet");
        match ids.first().cloned() {
            Some(id) => match id {
                IdType::ByIndex { index, value, loc } => {
                    is_valid_identifier(
                        ctx,
                        original_query,
                        loc.clone(),
                        index.to_string().as_str(),
                    );
                    let corresponding_field = ctx
                        .node_fields
                        .get(node_type)
                        .cloned()
                        .ok_or_else(|| {
                            generate_error!(ctx, original_query, loc.clone(), E201, node_type);
                        })
                        .unwrap_or_else(|_| {
                            generate_error!(ctx, original_query, loc.clone(), E201, node_type);
                            IndexMap::default()
                        });

                    match corresponding_field
                        .iter()
                        .find(|(name, _)| name.to_string() == *index.to_string())
                    {
                        Some((_, field)) => {
                            if !field.is_indexed() {
                                generate_error!(
                                    ctx,
                                    original_query,
                                    loc.clone(),
                                    E208,
                                    [&index.to_string(), node_type],
                                    [node_type]
                                );
                            } else if let ValueType::Literal { ref value, ref loc } = *value
                                && !field.field_type.eq(value)
                            {
                                generate_error!(
                                    ctx,
                                    original_query,
                                    loc.clone(),
                                    E205,
                                    &value.inner_stringify(),
                                    &value.to_variant_string(),
                                    &field.field_type.to_string(),
                                    "node",
                                    node_type
                                );
                            }
                        }
                        None => {
                            generate_error!(
                                ctx,
                                original_query,
                                loc.clone(),
                                E208,
                                [&index.to_string(), node_type],
                                [node_type]
                            );
                        }
                    };
                    gen_traversal.source_step =
                        Separator::Period(SourceStep::NFromIndex(NFromIndex {
                            label: GenRef::Literal(node_type.to_string()),
                            index: GenRef::Literal(match *index {
                                IdType::Identifier { value, loc: _ } => value,
                                _ => unreachable!("parser guarantees index is Identifier"),
                            }),
                            key: match *value {
                                ValueType::Identifier { value, loc } => {
                                    if is_valid_identifier(
                                        ctx,
                                        original_query,
                                        loc.clone(),
                                        value.as_str(),
                                    ) && !scope.contains_key(value.as_str())
                                    {
                                        generate_error!(
                                            ctx,
                                            original_query,
                                            loc.clone(),
                                            E301,
                                            value.as_str()
                                        );
                                    }
                                    gen_identifier_or_param(
                                        original_query,
                                        value.as_str(),
                                        true,
                                        false,
                                    )
                                }
                                ValueType::Literal { value, loc: _ } => {
                                    GeneratedValue::Primitive(GenRef::Ref(match value {
                                        Value::String(s) => format!("\"{s}\""),
                                        other => other.inner_stringify(),
                                    }))
                                }
                                _ => {
                                    unreachable!("parser guarantees value is Identifier or Literal")
                                }
                            },
                        }));
                    gen_traversal.should_collect = ShouldCollect::ToObj;
                    gen_traversal.traversal_type = TraversalType::Ref;
                    Type::Node(Some(node_type.to_string()))
                }
                IdType::Identifier { value: i, loc } => {
                    gen_traversal.source_step = Separator::Period(SourceStep::NFromID(NFromID {
                        id: {
                            is_valid_identifier(ctx, original_query, loc.clone(), i.as_str());
                            let _ =
                                type_in_scope(ctx, original_query, loc.clone(), scope, i.as_str());
                            let value =
                                gen_identifier_or_param(original_query, i.as_str(), true, false);
                            check_identifier_is_fieldtype(
                                ctx,
                                original_query,
                                loc.clone(),
                                scope,
                                i.as_str(),
                                FieldType::Uuid,
                            )?;
                            value.inner().clone()
                        },
                        label: GenRef::Literal(node_type.to_string()),
                    }));
                    gen_traversal.traversal_type = TraversalType::Ref;
                    gen_traversal.should_collect = ShouldCollect::ToObj;
                    Type::Node(Some(node_type.to_string()))
                }
                IdType::Literal { value: s, loc: _ } => {
                    gen_traversal.source_step = Separator::Period(SourceStep::NFromID(NFromID {
                        id: GenRef::Ref(s.clone()),
                        label: GenRef::Literal(node_type.to_string()),
                    }));
                    gen_traversal.traversal_type = TraversalType::Ref;
                    gen_traversal.should_collect = ShouldCollect::ToObj;
                    Type::Node(Some(node_type.to_string()))
                }
            },
            None => {
                generate_error!(ctx, original_query, tr.loc.clone(), E601, "missing id");
                Type::Unknown
            }
        }
    } else {
        gen_traversal.source_step = Separator::Period(SourceStep::NFromType(NFromType {
            label: GenRef::Literal(node_type.to_string()),
        }));
        gen_traversal.traversal_type = TraversalType::Ref;
        Type::Nodes(Some(node_type.to_string()))
    };

    Some(ty)
}

fn resolve_edge_start<'a>(
    ctx: &mut Ctx<'a>,
    tr: &'a Traversal,
    edge_type: &str,
    ids: &Option<Vec<IdType>>,
    scope: &mut HashMap<&'a str, VariableInfo>,
    original_query: &'a Query,
    gen_traversal: &mut GeneratedTraversal,
) -> Type {
    if !ctx.edge_map.contains_key(edge_type) {
        generate_error!(ctx, original_query, tr.loc.clone(), E102, edge_type);
    }

    if let Some(ids) = ids {
        assert!(ids.len() == 1, "multiple ids not supported yet");
        gen_traversal.source_step = Separator::Period(SourceStep::EFromID(EFromID {
            id: match ids.first().cloned() {
                Some(id) => match id {
                    IdType::Identifier { value: i, loc } => {
                        is_valid_identifier(ctx, original_query, loc.clone(), i.as_str());
                        let _ = type_in_scope(ctx, original_query, loc.clone(), scope, i.as_str());
                        let value =
                            gen_identifier_or_param(original_query, i.as_str(), true, false);
                        value.inner().clone()
                    }
                    IdType::Literal { value: s, loc: _ } => GenRef::Std(s),
                    _ => unreachable!("parser guarantees edge ID is Identifier or Literal"),
                },
                None => {
                    generate_error!(ctx, original_query, tr.loc.clone(), E601, "missing id");
                    GenRef::Unknown
                }
            },
            label: GenRef::Literal(edge_type.to_string()),
        }));
        gen_traversal.traversal_type = TraversalType::Ref;
        gen_traversal.should_collect = ShouldCollect::ToObj;
        Type::Edge(Some(edge_type.to_string()))
    } else {
        gen_traversal.source_step = Separator::Period(SourceStep::EFromType(EFromType {
            label: GenRef::Literal(edge_type.to_string()),
        }));
        gen_traversal.traversal_type = TraversalType::Ref;
        Type::Edges(Some(edge_type.to_string()))
    }
}

fn resolve_vector_start<'a>(
    ctx: &mut Ctx<'a>,
    tr: &'a Traversal,
    vector_type: &str,
    ids: &Option<Vec<IdType>>,
    scope: &mut HashMap<&'a str, VariableInfo>,
    original_query: &'a Query,
    gen_traversal: &mut GeneratedTraversal,
) -> Type {
    if !ctx.vector_set.contains(vector_type) {
        generate_error!(ctx, original_query, tr.loc.clone(), E103, vector_type);
    }

    if let Some(ids) = ids {
        assert!(ids.len() == 1, "multiple ids not supported yet");
        gen_traversal.source_step = Separator::Period(SourceStep::VFromID(VFromID {
            get_vector_data: false,
            id: match ids.first().cloned() {
                Some(id) => match id {
                    IdType::Identifier { value: i, loc } => {
                        is_valid_identifier(ctx, original_query, loc.clone(), i.as_str());
                        let _ = type_in_scope(ctx, original_query, loc.clone(), scope, i.as_str());
                        let value =
                            gen_identifier_or_param(original_query, i.as_str(), true, false);
                        value.inner().clone()
                    }
                    IdType::Literal { value: s, loc: _ } => GenRef::Std(s),
                    _ => unreachable!("parser guarantees vector ID is Identifier or Literal"),
                },
                None => {
                    generate_error!(ctx, original_query, tr.loc.clone(), E601, "missing id");
                    GenRef::Unknown
                }
            },
            label: GenRef::Literal(vector_type.to_string()),
        }));
        gen_traversal.traversal_type = TraversalType::Ref;
        gen_traversal.should_collect = ShouldCollect::ToObj;
        Type::Vector(Some(vector_type.to_string()))
    } else {
        gen_traversal.source_step = Separator::Period(SourceStep::VFromType(VFromType {
            label: GenRef::Literal(vector_type.to_string()),
            get_vector_data: false,
        }));
        gen_traversal.traversal_type = TraversalType::Ref;
        Type::Vectors(Some(vector_type.to_string()))
    }
}

fn resolve_traversal_start<'a>(
    ctx: &mut Ctx<'a>,
    tr: &'a Traversal,
    scope: &mut HashMap<&'a str, VariableInfo>,
    original_query: &'a Query,
    parent_ty: Option<Type>,
    gen_traversal: &mut GeneratedTraversal,
    gen_query: &mut GeneratedQuery,
) -> Option<Type> {
    match &tr.start {
        StartNode::Node { node_type, ids } => resolve_node_start(
            ctx,
            tr,
            node_type.as_str(),
            ids,
            scope,
            original_query,
            gen_traversal,
        ),
        StartNode::Edge { edge_type, ids } => Some(resolve_edge_start(
            ctx,
            tr,
            edge_type.as_str(),
            ids,
            scope,
            original_query,
            gen_traversal,
        )),
        StartNode::Vector { vector_type, ids } => Some(resolve_vector_start(
            ctx,
            tr,
            vector_type.as_str(),
            ids,
            scope,
            original_query,
            gen_traversal,
        )),
        StartNode::Identifier(identifier) => Some(resolve_identifier_start(
            ctx,
            tr,
            identifier.as_str(),
            scope,
            original_query,
            gen_traversal,
        )),
        StartNode::Anonymous => {
            resolve_anonymous_start(ctx, tr, original_query, parent_ty, gen_traversal)
        }
        StartNode::SearchVector(sv) => {
            validate_search_vector_start(ctx, sv, scope, original_query, gen_traversal, gen_query)
        }
    }
}

fn normalize_where_boexp_for_collection(expr: BoExp, normalize_not_exists: bool) -> BoExp {
    match expr {
        BoExp::Not(inner_expr) if normalize_not_exists => {
            if let BoExp::Exists(mut traversal) = *inner_expr {
                traversal.should_collect = ShouldCollect::No;
                BoExp::Not(Box::new(BoExp::Exists(traversal)))
            } else {
                BoExp::Not(inner_expr)
            }
        }
        BoExp::Not(inner_expr) => BoExp::Not(inner_expr),
        BoExp::Exists(mut traversal) => {
            traversal.should_collect = ShouldCollect::No;
            BoExp::Exists(traversal)
        }
        other => other,
    }
}

fn boexp_from_where_stmt<'a>(
    ctx: &mut Ctx<'a>,
    original_query: &'a Query,
    expr_loc: Loc,
    stmt: GeneratedStatement,
    traversal_type_for_error: Option<&str>,
    strict_boolean_traversal: bool,
    normalize_not_exists: bool,
    unexpected_stmt_message: Option<&str>,
) -> Option<BoExp> {
    match stmt {
        GeneratedStatement::Traversal(tr) => {
            if strict_boolean_traversal {
                let last_is_bool_op = tr
                    .steps
                    .last()
                    .is_some_and(|s| matches!(s.inner(), GeneratedStep::BoolOp(_)));
                if !last_is_bool_op {
                    generate_error!(
                        ctx,
                        original_query,
                        expr_loc,
                        E659,
                        traversal_type_for_error.unwrap_or("unknown")
                    );
                    return None;
                }
            }
            Some(BoExp::Expr(tr))
        }
        GeneratedStatement::BoExp(expr) => Some(normalize_where_boexp_for_collection(
            expr,
            normalize_not_exists,
        )),
        _ => {
            if let Some(message) = unexpected_stmt_message {
                generate_error!(ctx, original_query, expr_loc, E655, message);
            }
            None
        }
    }
}

fn apply_where_step<'a>(
    ctx: &mut Ctx<'a>,
    expr: &'a Expression,
    scope: &mut HashMap<&'a str, VariableInfo>,
    original_query: &'a Query,
    cur_ty: &Type,
    gen_query: &mut GeneratedQuery,
    gen_traversal: &mut GeneratedTraversal,
) -> Option<Type> {
    let (ty, stmt) = infer_expr_type(
        ctx,
        expr,
        scope,
        original_query,
        Some(cur_ty.clone()),
        gen_query,
    );

    let Some(stmt) = stmt else {
        return Some(cur_ty.clone());
    };

    if let Some(where_expr) = boexp_from_where_stmt(
        ctx,
        original_query,
        expr.loc.clone(),
        stmt,
        Some(ty.kind_str()),
        true,
        true,
        Some("unexpected statement type in Where clause"),
    ) {
        gen_traversal
            .steps
            .push(Separator::Period(GeneratedStep::Where(Where::Ref(
                WhereRef { expr: where_expr },
            ))));
    }

    None
}

fn comparison_value_from_expr<'a>(
    ctx: &mut Ctx<'a>,
    original_query: &'a Query,
    scope: &HashMap<&'a str, VariableInfo>,
    expr: &Expression,
) -> GeneratedValue {
    match &expr.expr {
        ExpressionType::IntegerLiteral(i) => GeneratedValue::Primitive(GenRef::Std(i.to_string())),
        ExpressionType::FloatLiteral(f) => GeneratedValue::Primitive(GenRef::Std(f.to_string())),
        ExpressionType::Identifier(i) => {
            is_valid_identifier(ctx, original_query, expr.loc.clone(), i.as_str());
            type_in_scope(ctx, original_query, expr.loc.clone(), scope, i.as_str());
            gen_identifier_or_param(original_query, i.as_str(), false, true)
        }
        other => {
            generate_error!(
                ctx,
                original_query,
                expr.loc.clone(),
                E655,
                &format!("unexpected expression type in comparison: {:?}", other)
            );
            GeneratedValue::Unknown
        }
    }
}

fn apply_boolean_operation_step<'a>(
    ctx: &mut Ctx<'a>,
    b_op: &'a BooleanOp,
    previous_step: Option<&StepType>,
    scope: &mut HashMap<&'a str, VariableInfo>,
    original_query: &'a Query,
    parent_ty: Option<Type>,
    cur_ty: &Type,
    gen_query: &mut GeneratedQuery,
    gen_traversal: &mut GeneratedTraversal,
) -> Option<Type> {
    let Some(step) = previous_step else {
        generate_error!(
            ctx,
            original_query,
            b_op.loc.clone(),
            E657,
            "BooleanOperation"
        );
        return Some(cur_ty.clone());
    };

    let property_type = match &b_op.op {
        BooleanOpType::LessThanOrEqual(expr)
        | BooleanOpType::LessThan(expr)
        | BooleanOpType::GreaterThanOrEqual(expr)
        | BooleanOpType::GreaterThan(expr)
        | BooleanOpType::Equal(expr)
        | BooleanOpType::NotEqual(expr)
        | BooleanOpType::Contains(expr) => {
            match infer_expr_type(
                ctx,
                expr,
                scope,
                original_query,
                Some(cur_ty.clone()),
                gen_query,
            ) {
                (Type::Scalar(ft), _) => ft.clone(),
                (Type::Boolean, _) => FieldType::Boolean,
                (Type::Count, _) => FieldType::I64,
                (field_type, _) => {
                    generate_error!(
                        ctx,
                        original_query,
                        b_op.loc.clone(),
                        E621,
                        &b_op.loc.span,
                        field_type.kind_str()
                    );
                    return Some(field_type);
                }
            }
        }
        BooleanOpType::IsIn(expr) => {
            // IS_IN expects an array argument
            match infer_expr_type(
                ctx,
                expr,
                scope,
                original_query,
                Some(cur_ty.clone()),
                gen_query,
            ) {
                (Type::Array(boxed_ty), _) => match *boxed_ty {
                    Type::Scalar(ft) => ft,
                    _ => {
                        generate_error!(
                            ctx,
                            original_query,
                            b_op.loc.clone(),
                            E621,
                            &b_op.loc.span,
                            "non-scalar array elements"
                        );
                        return Some(Type::Unknown);
                    }
                },
                (field_type, _) => {
                    generate_error!(
                        ctx,
                        original_query,
                        b_op.loc.clone(),
                        E621,
                        &b_op.loc.span,
                        field_type.kind_str()
                    );
                    return Some(field_type);
                }
            }
        }
        _ => return Some(cur_ty.clone()),
    };

    // get type of field name
    let field_name = match step {
        StepType::Object(obj) => {
            let fields = &obj.fields;
            assert!(fields.len() == 1);
            Some(fields[0].value.value.clone())
        }
        _ => None,
    };
    if let Some(FieldValueType::Identifier(field_name)) = &field_name {
        is_valid_identifier(ctx, original_query, b_op.loc.clone(), field_name.as_str());
        match cur_ty {
            Type::Scalar(ft) => {
                if ft != &property_type {
                    generate_error!(
                        ctx,
                        original_query,
                        b_op.loc.clone(),
                        E622,
                        field_name,
                        cur_ty.kind_str(),
                        &cur_ty.get_type_name(),
                        &ft.to_string(),
                        &property_type.to_string()
                    );
                }
            }
            Type::Nodes(Some(node_ty)) | Type::Node(Some(node_ty)) => {
                // Check if this is a reserved property first
                if let Some(reserved_type) = get_reserved_property_type(field_name.as_str(), cur_ty)
                {
                    // Validate the type matches
                    if let FieldType::Array(inner_type) = &property_type {
                        if reserved_type != **inner_type {
                            generate_error!(
                                ctx,
                                original_query,
                                b_op.loc.clone(),
                                E622,
                                field_name,
                                cur_ty.kind_str(),
                                &cur_ty.get_type_name(),
                                &reserved_type.to_string(),
                                &property_type.to_string()
                            );
                        }
                    } else if reserved_type != property_type {
                        generate_error!(
                            ctx,
                            original_query,
                            b_op.loc.clone(),
                            E622,
                            field_name,
                            cur_ty.kind_str(),
                            &cur_ty.get_type_name(),
                            &reserved_type.to_string(),
                            &property_type.to_string()
                        );
                    }
                } else {
                    // Not a reserved property, check schema fields
                    let field_type = ctx
                        .node_fields
                        .get(node_ty.as_str())
                        .and_then(|field_set| field_set.get(field_name.as_str()))
                        .map(|field| field.field_type.clone());
                    match field_type {
                        Some(field_type) => {
                            if let FieldType::Array(inner_type) = &property_type {
                                if field_type != **inner_type {
                                    generate_error!(
                                        ctx,
                                        original_query,
                                        b_op.loc.clone(),
                                        E622,
                                        field_name,
                                        cur_ty.kind_str(),
                                        &cur_ty.get_type_name(),
                                        &field_type.to_string(),
                                        &property_type.to_string()
                                    );
                                }
                            } else if field_type != property_type {
                                generate_error!(
                                    ctx,
                                    original_query,
                                    b_op.loc.clone(),
                                    E622,
                                    field_name,
                                    cur_ty.kind_str(),
                                    &cur_ty.get_type_name(),
                                    &field_type.to_string(),
                                    &property_type.to_string()
                                );
                            }
                        }
                        None => {
                            generate_error!(
                                ctx,
                                original_query,
                                b_op.loc.clone(),
                                E202,
                                field_name,
                                cur_ty.kind_str(),
                                node_ty
                            );
                        }
                    }
                }
            }
            Type::Edges(Some(edge_ty)) | Type::Edge(Some(edge_ty)) => {
                // Check if this is a reserved property first
                if let Some(reserved_type) = get_reserved_property_type(field_name.as_str(), cur_ty)
                {
                    // Validate the type matches
                    if reserved_type != property_type {
                        generate_error!(
                            ctx,
                            original_query,
                            b_op.loc.clone(),
                            E622,
                            field_name,
                            cur_ty.kind_str(),
                            &cur_ty.get_type_name(),
                            &reserved_type.to_string(),
                            &property_type.to_string()
                        );
                    }
                } else {
                    // Not a reserved property, check schema fields
                    let field_type = ctx
                        .edge_fields
                        .get(edge_ty.as_str())
                        .and_then(|field_set| field_set.get(field_name.as_str()))
                        .map(|field| field.field_type.clone());
                    match field_type {
                        Some(field_type) => {
                            if field_type != property_type {
                                generate_error!(
                                    ctx,
                                    original_query,
                                    b_op.loc.clone(),
                                    E622,
                                    field_name,
                                    cur_ty.kind_str(),
                                    &cur_ty.get_type_name(),
                                    &field_type.to_string(),
                                    &property_type.to_string()
                                );
                            }
                        }
                        None => {
                            generate_error!(
                                ctx,
                                original_query,
                                b_op.loc.clone(),
                                E202,
                                field_name,
                                cur_ty.kind_str(),
                                edge_ty
                            );
                        }
                    }
                }
            }
            Type::Vectors(Some(sv)) | Type::Vector(Some(sv)) => {
                // Check if this is a reserved property first
                if let Some(reserved_type) = get_reserved_property_type(field_name.as_str(), cur_ty)
                {
                    // Validate the type matches
                    if reserved_type != property_type {
                        generate_error!(
                            ctx,
                            original_query,
                            b_op.loc.clone(),
                            E622,
                            field_name,
                            cur_ty.kind_str(),
                            &cur_ty.get_type_name(),
                            &reserved_type.to_string(),
                            &property_type.to_string()
                        );
                    }
                } else {
                    // Not a reserved property, check schema fields
                    let field_type = ctx
                        .vector_fields
                        .get(sv.as_str())
                        .and_then(|field_set| field_set.get(field_name.as_str()))
                        .map(|field| field.field_type.clone());
                    match field_type {
                        Some(field_type) => {
                            if field_type != property_type {
                                generate_error!(
                                    ctx,
                                    original_query,
                                    b_op.loc.clone(),
                                    E622,
                                    field_name,
                                    cur_ty.kind_str(),
                                    &cur_ty.get_type_name(),
                                    &field_type.to_string(),
                                    &property_type.to_string()
                                );
                            }
                        }
                        None => {
                            generate_error!(
                                ctx,
                                original_query,
                                b_op.loc.clone(),
                                E202,
                                field_name,
                                cur_ty.kind_str(),
                                sv
                            );
                        }
                    }
                }
            }
            _ => {
                generate_error!(
                    ctx,
                    original_query,
                    b_op.loc.clone(),
                    E621,
                    &b_op.loc.span,
                    cur_ty.kind_str()
                );
            }
        }
    }

    // Where/boolean ops don't change the element type,
    // so `cur_ty` stays the same.
    let op = match &b_op.op {
        BooleanOpType::LessThanOrEqual(expr) => {
            let v = comparison_value_from_expr(ctx, original_query, scope, expr);
            BoolOp::Lte(Lte {
                left: GeneratedValue::Primitive(GenRef::Std("*v".to_string())),
                right: v,
            })
        }
        BooleanOpType::LessThan(expr) => {
            let v = comparison_value_from_expr(ctx, original_query, scope, expr);
            BoolOp::Lt(Lt {
                left: GeneratedValue::Primitive(GenRef::Std("*v".to_string())),
                right: v,
            })
        }
        BooleanOpType::GreaterThanOrEqual(expr) => {
            let v = comparison_value_from_expr(ctx, original_query, scope, expr);
            BoolOp::Gte(Gte {
                left: GeneratedValue::Primitive(GenRef::Std("*v".to_string())),
                right: v,
            })
        }
        BooleanOpType::GreaterThan(expr) => {
            let v = comparison_value_from_expr(ctx, original_query, scope, expr);
            BoolOp::Gt(Gt {
                left: GeneratedValue::Primitive(GenRef::Std("*v".to_string())),
                right: v,
            })
        }
        BooleanOpType::Equal(expr) => {
            // Check if the right-hand side is a simple property traversal
            if let ExpressionType::Traversal(traversal) = &expr.expr {
                if let Some((var, property)) = is_simple_property_traversal(traversal) {
                    // Use PropertyEq for simple traversals to avoid unnecessary G::from_iter
                    BoolOp::PropertyEq(PropertyEq { var, property })
                } else {
                    // Complex traversal - parse normally
                    let mut gen_traversal = GeneratedTraversal::default();
                    validate_traversal(
                        ctx,
                        traversal,
                        scope,
                        original_query,
                        parent_ty.clone(),
                        &mut gen_traversal,
                        gen_query,
                    );
                    gen_traversal.should_collect = ShouldCollect::ToValue;
                    let v = GeneratedValue::Traversal(Box::new(gen_traversal));
                    BoolOp::Eq(Eq {
                        left: GeneratedValue::Primitive(GenRef::Std("*v".to_string())),
                        right: v,
                    })
                }
            } else {
                let v = match &expr.expr {
                    ExpressionType::BooleanLiteral(b) => {
                        GeneratedValue::Primitive(GenRef::Std(b.to_string()))
                    }
                    ExpressionType::IntegerLiteral(i) => {
                        GeneratedValue::Primitive(GenRef::Std(i.to_string()))
                    }
                    ExpressionType::FloatLiteral(f) => {
                        GeneratedValue::Primitive(GenRef::Std(f.to_string()))
                    }
                    ExpressionType::StringLiteral(s) => {
                        GeneratedValue::Primitive(GenRef::Literal(s.to_string()))
                    }
                    ExpressionType::Identifier(i) => {
                        is_valid_identifier(ctx, original_query, expr.loc.clone(), i.as_str());
                        type_in_scope(ctx, original_query, expr.loc.clone(), scope, i.as_str());
                        gen_identifier_or_param(original_query, i.as_str(), false, true)
                    }
                    other => {
                        generate_error!(
                            ctx,
                            original_query,
                            expr.loc.clone(),
                            E655,
                            &format!("unexpected expression type in equality: {:?}", other)
                        );
                        GeneratedValue::Unknown
                    }
                };
                BoolOp::Eq(Eq {
                    left: GeneratedValue::Primitive(GenRef::Std("*v".to_string())),
                    right: v,
                })
            }
        }
        BooleanOpType::NotEqual(expr) => {
            // Check if the right-hand side is a simple property traversal
            if let ExpressionType::Traversal(traversal) = &expr.expr {
                if let Some((var, property)) = is_simple_property_traversal(traversal) {
                    // Use PropertyNeq for simple traversals to avoid unnecessary G::from_iter
                    BoolOp::PropertyNeq(PropertyNeq { var, property })
                } else {
                    // Complex traversal - parse normally
                    let mut gen_traversal = GeneratedTraversal::default();
                    validate_traversal(
                        ctx,
                        traversal,
                        scope,
                        original_query,
                        parent_ty.clone(),
                        &mut gen_traversal,
                        gen_query,
                    );
                    gen_traversal.should_collect = ShouldCollect::ToValue;
                    let v = GeneratedValue::Traversal(Box::new(gen_traversal));
                    BoolOp::Neq(Neq {
                        left: GeneratedValue::Primitive(GenRef::Std("*v".to_string())),
                        right: v,
                    })
                }
            } else {
                let v = match &expr.expr {
                    ExpressionType::BooleanLiteral(b) => {
                        GeneratedValue::Primitive(GenRef::Std(b.to_string()))
                    }
                    ExpressionType::IntegerLiteral(i) => {
                        GeneratedValue::Primitive(GenRef::Std(i.to_string()))
                    }
                    ExpressionType::FloatLiteral(f) => {
                        GeneratedValue::Primitive(GenRef::Std(f.to_string()))
                    }
                    ExpressionType::StringLiteral(s) => {
                        GeneratedValue::Primitive(GenRef::Literal(s.to_string()))
                    }
                    ExpressionType::Identifier(i) => {
                        is_valid_identifier(ctx, original_query, expr.loc.clone(), i.as_str());
                        type_in_scope(ctx, original_query, expr.loc.clone(), scope, i.as_str());
                        gen_identifier_or_param(original_query, i.as_str(), false, true)
                    }
                    other => {
                        generate_error!(
                            ctx,
                            original_query,
                            expr.loc.clone(),
                            E655,
                            &format!("unexpected expression type in inequality: {:?}", other)
                        );
                        GeneratedValue::Unknown
                    }
                };
                BoolOp::Neq(Neq {
                    left: GeneratedValue::Primitive(GenRef::Std("*v".to_string())),
                    right: v,
                })
            }
        }
        BooleanOpType::Contains(expr) => {
            let v = match &expr.expr {
                ExpressionType::Identifier(i) => {
                    is_valid_identifier(ctx, original_query, expr.loc.clone(), i.as_str());
                    type_in_scope(ctx, original_query, expr.loc.clone(), scope, i.as_str());
                    gen_identifier_or_param(original_query, i.as_str(), true, false)
                }
                ExpressionType::BooleanLiteral(b) => {
                    GeneratedValue::Primitive(GenRef::Std(b.to_string()))
                }
                ExpressionType::IntegerLiteral(i) => {
                    GeneratedValue::Primitive(GenRef::Std(i.to_string()))
                }
                ExpressionType::FloatLiteral(f) => {
                    GeneratedValue::Primitive(GenRef::Std(f.to_string()))
                }
                ExpressionType::StringLiteral(s) => {
                    GeneratedValue::Primitive(GenRef::Literal(s.to_string()))
                }
                other => {
                    generate_error!(
                        ctx,
                        original_query,
                        expr.loc.clone(),
                        E655,
                        &format!("unexpected expression type in contains: {:?}", other)
                    );
                    GeneratedValue::Unknown
                }
            };
            BoolOp::Contains(Contains { value: v })
        }
        BooleanOpType::IsIn(expr) => {
            let v = match &expr.expr {
                ExpressionType::Identifier(i) => {
                    is_valid_identifier(ctx, original_query, expr.loc.clone(), i.as_str());
                    type_in_scope(ctx, original_query, expr.loc.clone(), scope, i.as_str());
                    gen_identifier_or_param(original_query, i.as_str(), true, false)
                }
                ExpressionType::ArrayLiteral(a) => GeneratedValue::Array(GenRef::Std(
                    a.iter()
                        .map(|e| {
                            let v = match &e.expr {
                                ExpressionType::BooleanLiteral(b) => {
                                    GeneratedValue::Primitive(GenRef::Std(b.to_string()))
                                }
                                ExpressionType::IntegerLiteral(i) => {
                                    GeneratedValue::Primitive(GenRef::Std(i.to_string()))
                                }
                                ExpressionType::FloatLiteral(f) => {
                                    GeneratedValue::Primitive(GenRef::Std(f.to_string()))
                                }
                                ExpressionType::StringLiteral(s) => {
                                    GeneratedValue::Primitive(GenRef::Literal(s.to_string()))
                                }
                                // Other expression types in arrays are not supported for IS_IN
                                _ => GeneratedValue::Unknown,
                            };
                            v.to_string()
                        })
                        .collect::<Vec<_>>()
                        .join(", "),
                )),
                other => {
                    generate_error!(
                        ctx,
                        original_query,
                        expr.loc.clone(),
                        E655,
                        &format!("unexpected expression type in IS_IN: {:?}", other)
                    );
                    GeneratedValue::Unknown
                }
            };
            BoolOp::IsIn(IsIn { value: v })
        }
        other => {
            // Other boolean operations should have been handled above
            generate_error!(
                ctx,
                original_query,
                b_op.loc.clone(),
                E655,
                &format!("unexpected boolean operation type: {:?}", other)
            );
            return Some(cur_ty.clone());
        }
    };

    gen_traversal
        .steps
        .push(Separator::Period(GeneratedStep::BoolOp(op)));
    gen_traversal.should_collect = ShouldCollect::No;

    None
}

/// Validates the traversal and returns the end type of the traversal
///
/// This method also builds the generated traversal (`gen_traversal`) as it analyzes the traversal
///
/// - `gen_query`: is used to set the query to being a mutating query if necessary.
///   This is then used to determine the transaction type to use.
///
/// - `parent_ty`: is used with anonymous traversals to keep track of the parent type that the anonymous traversal is nested in.
pub(crate) fn validate_traversal<'a>(
    ctx: &mut Ctx<'a>,
    tr: &'a Traversal,
    scope: &mut HashMap<&'a str, VariableInfo>,
    original_query: &'a Query,
    parent_ty: Option<Type>,
    gen_traversal: &mut GeneratedTraversal,
    gen_query: &mut GeneratedQuery,
) -> Option<Type> {
    let mut previous_step = None;
    let mut cur_ty = resolve_traversal_start(
        ctx,
        tr,
        scope,
        original_query,
        parent_ty.clone(),
        gen_traversal,
        gen_query,
    )?;

    // Track excluded fields for property validation
    let mut excluded: HashMap<&str, Loc> = HashMap::new();

    // Stream through the steps
    let number_of_steps = match tr.steps.len() {
        0 => 0,
        n => n - 1,
    };

    for (i, graph_step) in tr.steps.iter().enumerate() {
        let step = &graph_step.step;
        match step {
            StepType::Node(gs) | StepType::Edge(gs) => {
                match apply_graph_step(
                    ctx,
                    gs,
                    &cur_ty,
                    original_query,
                    gen_traversal,
                    scope,
                    gen_query,
                ) {
                    Some(new_ty) => {
                        cur_ty = new_ty;
                    }
                    None => { /* error already recorded */ }
                }
                excluded.clear(); // Traversal to a new element resets exclusions
            }
            StepType::First => {
                cur_ty = cur_ty.clone().into_single();
                excluded.clear();
                gen_traversal.should_collect = ShouldCollect::ToObj;
            }

            StepType::Count => {
                cur_ty = Type::Count;
                excluded.clear();
                gen_traversal
                    .steps
                    .push(Separator::Period(GeneratedStep::Count));
                gen_traversal.should_collect = ShouldCollect::No;
            }

            StepType::Exclude(ex) => {
                // checks if exclude is either the last step or the step before an object remapping or closure
                // i.e. you cant have `N<Type>::!{field1}::Out<Label>`
                let is_last_step = i == number_of_steps;
                let next_is_projection = i < number_of_steps
                    && matches!(
                        tr.steps[i + 1].step,
                        StepType::Closure(_) | StepType::Object(_)
                    );

                if !(is_last_step || next_is_projection) {
                    generate_error!(ctx, original_query, ex.loc.clone(), E644);
                }
                validate_exclude(ctx, &cur_ty, tr, ex, &excluded, original_query);
                for (_, key) in &ex.fields {
                    excluded.insert(key.as_str(), ex.loc.clone());
                    gen_traversal.excluded_fields.push(key.clone());
                }
            }

            StepType::Object(obj) => {
                // For intermediate object steps, we don't track fields for return values
                // Fields are only tracked when this traversal is used in a RETURN statement
                let mut fields_out = vec![];
                match validate_object(
                    ctx,
                    &cur_ty,
                    obj,
                    original_query,
                    gen_traversal,
                    &mut fields_out,
                    scope,
                    gen_query,
                ) {
                    Ok(new_ty) => cur_ty = new_ty,
                    Err(_) => {
                        // Error already recorded (e.g. E202 for invalid field).
                        // Continue with Unknown so we don't emit a redundant E601.
                        cur_ty = Type::Unknown;
                    }
                }
            }

            StepType::Where(expr) => {
                if let Some(result_ty) = apply_where_step(
                    ctx,
                    expr,
                    scope,
                    original_query,
                    &cur_ty,
                    gen_query,
                    gen_traversal,
                ) {
                    return Some(result_ty);
                }
            }
            StepType::Intersect(expr) => {
                let (ty, stmt) = infer_expr_type(
                    ctx,
                    expr,
                    scope,
                    original_query,
                    Some(cur_ty.clone()),
                    gen_query,
                );
                if stmt.is_none() {
                    return Some(cur_ty.clone());
                }
                let stmt = stmt.unwrap();
                match stmt {
                    GeneratedStatement::Traversal(tr) => {
                        gen_traversal
                            .steps
                            .push(Separator::Period(GeneratedStep::Intersect(
                                crate::helixc::generator::traversal_steps::Intersect {
                                    traversal: tr,
                                },
                            )));
                        // The result type changes to whatever the sub-traversal returns
                        cur_ty = ty;
                    }
                    _ => {
                        generate_error!(
                            ctx,
                            original_query,
                            expr.loc.clone(),
                            E655,
                            "INTERSECT requires a traversal expression"
                        );
                    }
                }
            }
            StepType::BooleanOperation(b_op) => {
                if let Some(result_ty) = apply_boolean_operation_step(
                    ctx,
                    b_op,
                    previous_step.as_ref(),
                    scope,
                    original_query,
                    parent_ty.clone(),
                    &cur_ty,
                    gen_query,
                    gen_traversal,
                ) {
                    return Some(result_ty);
                }
            }
            StepType::Aggregate(aggr) => {
                let properties = aggr
                    .properties
                    .iter()
                    .map(|p| GenRef::Std(format!("\"{}\".to_string()", p.clone())))
                    .collect::<Vec<_>>();
                let should_count = matches!(previous_step, Some(StepType::Count));
                let _ = gen_traversal.steps.pop();

                // Capture aggregate metadata before replacing cur_ty
                let property_names = aggr.properties.clone();
                cur_ty = Type::Aggregate(AggregateInfo {
                    source_type: Box::new(cur_ty.clone()),
                    properties: property_names,
                    is_count: should_count,
                    is_group_by: false, // This is AGGREGATE_BY
                });

                gen_traversal.should_collect = ShouldCollect::Try;
                gen_traversal
                    .steps
                    .push(Separator::Period(GeneratedStep::AggregateBy(AggregateBy {
                        properties,
                        should_count,
                    })))
            }
            StepType::GroupBy(gb) => {
                let properties = gb
                    .properties
                    .iter()
                    .map(|p| GenRef::Std(format!("\"{}\".to_string()", p.clone())))
                    .collect::<Vec<_>>();
                let should_count = matches!(previous_step, Some(StepType::Count));
                let _ = gen_traversal.steps.pop();

                // Capture aggregate metadata before replacing cur_ty
                let property_names = gb.properties.clone();
                cur_ty = Type::Aggregate(AggregateInfo {
                    source_type: Box::new(cur_ty.clone()),
                    properties: property_names,
                    is_count: should_count,
                    is_group_by: true, // This is GROUP_BY
                });

                gen_traversal.should_collect = ShouldCollect::Try;
                gen_traversal
                    .steps
                    .push(Separator::Period(GeneratedStep::GroupBy(GroupBy {
                        properties,
                        should_count,
                    })))
            }
            StepType::Update(update) => {
                if let Some(result_ty) = validate_update_step(
                    ctx,
                    original_query,
                    scope,
                    &mut cur_ty,
                    update,
                    gen_traversal,
                    &mut excluded,
                ) {
                    return Some(result_ty);
                }
            }

            StepType::Upsert(upsert) => {
                if let Some(result_ty) = validate_upsert_step(
                    ctx,
                    original_query,
                    scope,
                    &mut cur_ty,
                    upsert,
                    gen_traversal,
                    &mut excluded,
                ) {
                    return Some(result_ty);
                }
            }

            StepType::UpsertN(upsert) => {
                if let Some(result_ty) = validate_upsert_n_step(
                    ctx,
                    original_query,
                    scope,
                    &mut cur_ty,
                    upsert,
                    gen_traversal,
                    gen_query,
                    &mut excluded,
                ) {
                    return Some(result_ty);
                }
            }

            StepType::UpsertE(upsert) => {
                if let Some(result_ty) = validate_upsert_e_step(
                    ctx,
                    original_query,
                    scope,
                    &mut cur_ty,
                    upsert,
                    gen_traversal,
                    gen_query,
                    &mut excluded,
                ) {
                    return Some(result_ty);
                }
            }

            StepType::UpsertV(upsert) => {
                if let Some(result_ty) = validate_upsert_v_step(
                    ctx,
                    original_query,
                    scope,
                    &mut cur_ty,
                    upsert,
                    gen_traversal,
                    gen_query,
                    &mut excluded,
                ) {
                    return Some(result_ty);
                }
            }

            StepType::AddEdge(add) => {
                if let Some(ref ty) = add.edge_type
                    && !ctx.edge_map.contains_key(ty.as_str())
                {
                    generate_error!(ctx, original_query, add.loc.clone(), E102, ty);
                }
                cur_ty = Type::Edges(add.edge_type.clone());
                excluded.clear();
            }

            StepType::Range((start, end)) => {
                let (start, end) = match (&start.expr, &end.expr) {
                    (ExpressionType::Identifier(i), ExpressionType::Identifier(j)) => {
                        is_valid_identifier(ctx, original_query, start.loc.clone(), i.as_str());
                        is_valid_identifier(ctx, original_query, end.loc.clone(), j.as_str());

                        let ty = type_in_scope(
                            ctx,
                            original_query,
                            start.loc.clone(),
                            scope,
                            i.as_str(),
                        );
                        if let Some(ty) = ty
                            && !ty.is_integer()
                        {
                            generate_error!(
                                ctx,
                                original_query,
                                start.loc.clone(),
                                E633,
                                [&start.loc.span, &ty.get_type_name()],
                                [i.as_str()]
                            );
                            return Some(cur_ty.clone());
                        };
                        let ty =
                            type_in_scope(ctx, original_query, end.loc.clone(), scope, j.as_str());
                        if let Some(ty) = ty
                            && !ty.is_integer()
                        {
                            generate_error!(
                                ctx,
                                original_query,
                                end.loc.clone(),
                                E633,
                                [&end.loc.span, &ty.get_type_name()],
                                [j.as_str()]
                            );
                            return Some(cur_ty.clone());
                        }
                        (
                            gen_identifier_or_param(original_query, i.as_str(), false, true),
                            gen_identifier_or_param(original_query, j.as_str(), false, true),
                        )
                    }
                    (ExpressionType::IntegerLiteral(i), ExpressionType::IntegerLiteral(j)) => (
                        GeneratedValue::Primitive(GenRef::Std(i.to_string())),
                        GeneratedValue::Primitive(GenRef::Std(j.to_string())),
                    ),
                    (ExpressionType::Identifier(i), ExpressionType::IntegerLiteral(j)) => {
                        is_valid_identifier(ctx, original_query, start.loc.clone(), i.as_str());

                        let ty = type_in_scope(
                            ctx,
                            original_query,
                            start.loc.clone(),
                            scope,
                            i.as_str(),
                        );
                        if let Some(ty) = ty
                            && !ty.is_integer()
                        {
                            generate_error!(
                                ctx,
                                original_query,
                                start.loc.clone(),
                                E633,
                                [&start.loc.span, &ty.get_type_name()],
                                [i.as_str()]
                            );
                            return Some(cur_ty.clone());
                        }

                        (
                            gen_identifier_or_param(original_query, i.as_str(), false, true),
                            GeneratedValue::Primitive(GenRef::Std(j.to_string())),
                        )
                    }
                    (ExpressionType::IntegerLiteral(i), ExpressionType::Identifier(j)) => {
                        is_valid_identifier(ctx, original_query, end.loc.clone(), j.as_str());
                        let ty =
                            type_in_scope(ctx, original_query, end.loc.clone(), scope, j.as_str());
                        if let Some(ty) = ty
                            && !ty.is_integer()
                        {
                            generate_error!(
                                ctx,
                                original_query,
                                end.loc.clone(),
                                E633,
                                [&end.loc.span, &ty.get_type_name()],
                                [j.as_str()]
                            );
                            return Some(cur_ty.clone());
                        }
                        (
                            GeneratedValue::Primitive(GenRef::Std(i.to_string())),
                            gen_identifier_or_param(original_query, j.as_str(), false, true),
                        )
                    }
                    (ExpressionType::Identifier(_) | ExpressionType::IntegerLiteral(_), other) => {
                        generate_error!(
                            ctx,
                            original_query,
                            start.loc.clone(),
                            E633,
                            [&start.loc.span, &other.to_string()],
                            [&other.to_string()]
                        );
                        return Some(cur_ty.clone());
                    }
                    (other, ExpressionType::Identifier(_) | ExpressionType::IntegerLiteral(_)) => {
                        generate_error!(
                            ctx,
                            original_query,
                            start.loc.clone(),
                            E633,
                            [&start.loc.span, &other.to_string()],
                            [&other.to_string()]
                        );
                        return Some(cur_ty.clone());
                    }
                    (start_expr, end_expr) => {
                        // Both start and end must be integers or identifiers
                        generate_error!(
                            ctx,
                            original_query,
                            start.loc.clone(),
                            E633,
                            [&format!("({}, {})", start_expr, end_expr), "non-integer"],
                            ["start and end"]
                        );
                        return Some(cur_ty.clone());
                    }
                };
                gen_traversal
                    .steps
                    .push(Separator::Period(GeneratedStep::Range(Range {
                        start,
                        end,
                    })));
            }
            StepType::OrderBy(order_by) => {
                // verify property access
                let (_, stmt) = infer_expr_type(
                    ctx,
                    &order_by.expression,
                    scope,
                    original_query,
                    Some(cur_ty.clone()),
                    gen_query,
                );

                if stmt.is_none() {
                    return Some(cur_ty.clone());
                }
                match stmt.unwrap() {
                    GeneratedStatement::Traversal(traversal) => {
                        gen_traversal
                            .steps
                            .push(Separator::Period(GeneratedStep::OrderBy(OrderBy {
                                traversal,
                                order: match order_by.order_by_type {
                                    OrderByType::Asc => Order::Asc,
                                    OrderByType::Desc => Order::Desc,
                                },
                            })));
                        gen_traversal.should_collect = ShouldCollect::ToVec;
                    }
                    _ => {
                        // OrderBy requires a traversal expression
                        generate_error!(
                            ctx,
                            original_query,
                            order_by.expression.loc.clone(),
                            E655,
                            "OrderBy expected traversal expression"
                        );
                    }
                }
            }
            StepType::Closure(cl) => {
                if i != number_of_steps {
                    generate_error!(ctx, original_query, cl.loc.clone(), E641);
                }
                // Add identifier to a temporary scope so inner uses pass
                // For closures iterating over collections, singularize the type
                let was_collection =
                    matches!(cur_ty, Type::Nodes(_) | Type::Edges(_) | Type::Vectors(_));
                let closure_param_type = match &cur_ty {
                    Type::Nodes(label) => Type::Node(label.clone()),
                    Type::Edges(label) => Type::Edge(label.clone()),
                    Type::Vectors(label) => Type::Vector(label.clone()),
                    other => other.clone(),
                };

                // Extract the source variable name from the current traversal
                let closure_source_var = match &gen_traversal.source_step {
                    Separator::Empty(SourceStep::Identifier(var))
                    | Separator::Period(SourceStep::Identifier(var))
                    | Separator::Newline(SourceStep::Identifier(var)) => var.inner().clone(),
                    _ => {
                        // For other source types, try to extract from traversal_type
                        match &gen_traversal.traversal_type {
                            TraversalType::FromSingle(var) | TraversalType::FromIter(var) => {
                                var.inner().clone()
                            }
                            _ => String::new(),
                        }
                    }
                };

                // Closure parameters are always singular (they represent individual items during iteration)
                scope.insert(
                    cl.identifier.as_str(),
                    VariableInfo::new_with_source(
                        closure_param_type.clone(),
                        true,
                        closure_source_var.clone(),
                    ),
                );
                let obj = &cl.object;
                let mut fields_out = vec![];
                // Pass the singular type to validate_object so nested traversals use the correct type
                match validate_object(
                    ctx,
                    &closure_param_type,
                    obj,
                    original_query,
                    gen_traversal,
                    &mut fields_out,
                    scope,
                    gen_query,
                ) {
                    Ok(new_ty) => cur_ty = new_ty,
                    Err(_) => {
                        // Error already recorded (e.g. E202 for invalid field).
                        // Continue with Unknown so we don't emit a redundant E601.
                        cur_ty = Type::Unknown;
                    }
                }

                // Tag the main traversal with the closure parameter name
                gen_traversal.closure_param_name = Some(cl.identifier.clone());

                // Tag all nested traversals with closure context
                for (_field_name, nested_info) in gen_traversal.nested_traversals.iter_mut() {
                    nested_info.closure_param_name = Some(cl.identifier.clone());
                    nested_info.closure_source_var = Some(closure_source_var.clone());
                }

                // If we were iterating over a collection, ensure should_collect stays as ToVec
                // validate_object may have set it to ToObj because we passed a singular type
                if was_collection {
                    gen_traversal.should_collect = ShouldCollect::ToVec;
                    // Also convert the return type back to collection type
                    // This ensures is_collection flag is set correctly in query_validation.rs
                    cur_ty = match cur_ty {
                        Type::Node(label) => Type::Nodes(label),
                        Type::Edge(label) => Type::Edges(label),
                        Type::Vector(label) => Type::Vectors(label),
                        other => other,
                    };
                }

                scope.remove(cl.identifier.as_str());
            }
            StepType::RerankRRF(rerank_rrf) => {
                apply_rerank_rrf_step(ctx, original_query, scope, rerank_rrf, gen_traversal);
            }
            StepType::RerankMMR(rerank_mmr) => {
                apply_rerank_mmr_step(ctx, original_query, scope, rerank_mmr, gen_traversal);
            }
        }
        previous_step = Some(step.clone());
    }
    match gen_traversal.traversal_type {
        TraversalType::Mut | TraversalType::Update(_) | TraversalType::Upsert { .. } => {
            gen_query.is_mut = true;
        }
        _ => {}
    }
    Some(cur_ty)
}

#[cfg(test)]
mod tests {
    use crate::helixc::analyzer::error_codes::ErrorCode;
    use crate::helixc::parser::{HelixParser, write_to_temp_file};

    // ============================================================================
    // Start Node Validation Tests
    // ============================================================================

    #[test]
    fn test_undeclared_node_type() {
        let source = r#"
            N::Person { name: String }

            QUERY test() =>
                company <- N<Company>
                RETURN company
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.iter().any(|d| d.error_code == ErrorCode::E101));
    }

    #[test]
    fn test_undeclared_edge_type() {
        let source = r#"
            N::Person { name: String }
            E::Knows { From: Person, To: Person }

            QUERY test(id: ID) =>
                person <- N<Person>(id)
                edges <- person::OutE<WorksAt>
                RETURN edges
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.iter().any(|d| d.error_code == ErrorCode::E102));
    }

    #[test]
    fn test_undeclared_vector_type() {
        let source = r#"
            N::Person { name: String }

            QUERY test() =>
                docs <- V<Document>
                RETURN docs
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.iter().any(|d| d.error_code == ErrorCode::E103));
    }

    #[test]
    fn test_node_with_id_parameter() {
        let source = r#"
            N::Person { name: String }

            QUERY test(id: ID) =>
                person <- N<Person>(id)
                RETURN person
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(!diagnostics.iter().any(|d| d.error_code == ErrorCode::E301));
    }

    #[test]
    fn test_node_with_undefined_id_variable() {
        let source = r#"
            N::Person { name: String }

            QUERY test() =>
                person <- N<Person>(unknownId)
                RETURN person
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.iter().any(|d| d.error_code == ErrorCode::E301));
    }

    #[test]
    fn test_node_without_id() {
        let source = r#"
            N::Person { name: String }

            QUERY test() =>
                people <- N<Person>
                RETURN people
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.is_empty());
    }

    #[test]
    fn test_identifier_start_node() {
        let source = r#"
            N::Person { name: String }

            QUERY test() =>
                person <- N<Person>
                samePerson <- person
                RETURN samePerson
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.is_empty());
    }

    #[test]
    fn test_identifier_not_in_scope() {
        let source = r#"
            N::Person { name: String }

            QUERY test() =>
                person <- unknownVariable
                RETURN person
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.iter().any(|d| d.error_code == ErrorCode::E301));
    }

    // ============================================================================
    // Traversal Step Tests
    // ============================================================================

    #[test]
    fn test_valid_out_traversal() {
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
    fn test_property_access() {
        let source = r#"
            N::Person { name: String, age: U32 }

            QUERY test(id: ID) =>
                person <- N<Person>(id)
                name <- person::{name}
                RETURN name
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.is_empty());
    }

    // Note: Property errors are caught during object validation, not traversal validation
    // Removing test_property_not_exists as it requires different assertion approach

    // ============================================================================
    // Where Clause Tests
    // ============================================================================

    #[test]
    fn test_where_with_property_equals() {
        let source = r#"
            N::Person { name: String, age: U32 }

            QUERY test(targetAge: U32) =>
                people <- N<Person>::WHERE(_::{age}::EQ(targetAge))
                RETURN people
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.is_empty());
    }

    #[test]
    fn test_where_with_property_greater_than() {
        let source = r#"
            N::Person { name: String, age: U32 }

            QUERY test(minAge: U32) =>
                people <- N<Person>::WHERE(_::{age}::GT(minAge))
                RETURN people
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.is_empty());
    }

    // ============================================================================
    // Mutation Step Tests
    // ============================================================================

    #[test]
    fn test_update_with_identifier_value() {
        let source = r#"
            N::Person { name: String }

            QUERY test(id: ID, newName: String) =>
                updated <- N<Person>(id)::UPDATE({name: newName})
                RETURN updated
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.is_empty());
    }

    #[test]
    fn test_update_with_unsupported_expression_emits_e206() {
        let source = r#"
            N::Person { name: String }

            QUERY test(id: ID) =>
                updated <- N<Person>(id)::UPDATE({name: ["invalid"]})
                RETURN updated
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.iter().any(|d| d.error_code == ErrorCode::E206));
    }

    #[test]
    fn test_upsert_n_with_unsupported_expression_emits_e206() {
        let source = r#"
            N::Person { name: String }

            QUERY test(id: ID) =>
                updated <- N<Person>(id)::UpsertN({name: ["invalid"]})
                RETURN updated
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.iter().any(|d| d.error_code == ErrorCode::E206));
    }

    // ============================================================================
    // Chained Traversal Tests
    // ============================================================================

    #[test]
    fn test_chained_edge_traversal() {
        let source = r#"
            N::Person { name: String }
            E::Knows { From: Person, To: Person }

            QUERY test(id: ID) =>
                person <- N<Person>(id)
                edges <- person::OutE<Knows>
                targets <- edges::ToN
                RETURN targets
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.is_empty());
    }

    #[test]
    fn test_multi_hop_traversal() {
        let source = r#"
            N::Person { name: String }
            E::Knows { From: Person, To: Person }

            QUERY test(id: ID) =>
                friends <- N<Person>(id)::Out<Knows>
                friendsOfFriends <- friends::Out<Knows>
                RETURN friendsOfFriends
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.is_empty());
    }

    // ============================================================================
    // Complex Query Tests
    // ============================================================================

    #[test]
    fn test_complex_query_with_multiple_steps() {
        let source = r#"
            N::Person { name: String, age: U32 }
            E::Knows { From: Person, To: Person }

            QUERY test(id: ID, minAge: U32) =>
                person <- N<Person>(id)
                friends <- person::Out<Knows>::WHERE(_::{age}::GT(minAge))
                names <- friends::{name}
                RETURN names
        "#;

        let content = write_to_temp_file(vec![source]);
        let parsed = HelixParser::parse_source(&content).unwrap();
        let result = crate::helixc::analyzer::analyze(&parsed);

        assert!(result.is_ok());
        let (diagnostics, _) = result.unwrap();
        assert!(diagnostics.is_empty());
    }
}
