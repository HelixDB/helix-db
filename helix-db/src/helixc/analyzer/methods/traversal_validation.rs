use crate::helixc::analyzer::error_codes::*;
use crate::helixc::generator::source_steps::SearchVector;
use crate::helixc::generator::utils::VecData;
use crate::{
    generate_error,
    helixc::{
        analyzer::{
            analyzer::Ctx,
            errors::push_query_err,
            methods::{
                exclude_validation::validate_exclude, graph_step_validation::apply_graph_step,
                infer_expr_type::infer_expr_type, object_validation::validate_object,
            },
            types::Type,
            utils::{
                Variable, field_exists_on_item_type, gen_identifier_or_param, is_valid_identifier,
                type_in_scope,
            },
        },
        generator::{
            bool_op::{BoolOp, Eq, Gt, Gte, Lt, Lte, Neq},
            generator_types::{BoExp, Query as GeneratedQuery, Statement as GeneratedStatement},
            object_remapping_generation::{ExcludeField, Remapping, RemappingType},
            source_steps::{EFromID, EFromType, NFromID, NFromIndex, NFromType, SourceStep},
            traversal_steps::{
                OrderBy, Range, ShouldCollect, Step as GeneratedStep,
                Traversal as GeneratedTraversal, TraversalType, Where, WhereRef,
            },
            utils::{GenRef, GeneratedValue, Order, Separator},
        },
        parser::{helix_parser::*, location::Loc},
    },
    protocol::value::Value,
};
use paste::paste;
use std::collections::HashMap;

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
    scope: &mut HashMap<&'a str, Type>,
    original_query: &'a Query,
    parent_ty: Option<Type>,
    gen_traversal: &mut GeneratedTraversal,
    gen_query: &mut GeneratedQuery,
) -> Type {
    let mut previous_step = None;
    let mut cur_ty = match &tr.start {
        StartNode::Node { node_type, ids } => {
            if !ctx.node_set.contains(node_type.as_str()) {
                generate_error!(ctx, original_query, tr.loc.clone(), E101, node_type);
            }
            if let Some(ids) = ids {
                assert!(ids.len() == 1, "multiple ids not supported yet");
                // check id exists in scope
                match ids[0].clone() {
                    IdType::ByIndex { index, value, loc } => {
                        is_valid_identifier(
                            ctx,
                            original_query,
                            loc.clone(),
                            index.to_string().as_str(),
                        );
                        let corresponding_field = ctx.node_fields.get(node_type.as_str()).cloned();
                        match corresponding_field {
                            Some(node_fields) => {
                                match node_fields
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
                                        } else if let ValueType::Literal { ref value, ref loc } =
                                            *value
                                        {
                                            if !field.field_type.eq(value) {
                                                generate_error!(
                                                    ctx,
                                                    original_query,
                                                    loc.clone(),
                                                    E205,
                                                    &value.to_string(),
                                                    &field.field_type.to_string(),
                                                    "node",
                                                    node_type
                                                );
                                            }
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
                                }
                            }
                            None => unreachable!(),
                        };
                        gen_traversal.source_step =
                            Separator::Period(SourceStep::NFromIndex(NFromIndex {
                                index: GenRef::Literal(match *index {
                                    IdType::Identifier { value, loc: _ } => value,
                                    // would be caught by the parser
                                    _ => unreachable!(),
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
                                        GeneratedValue::Primitive(GenRef::Std(match value {
                                            Value::String(s) => format!("\"{s}\""),
                                            Value::I8(i) => i.to_string(),
                                            Value::I16(i) => i.to_string(),
                                            Value::I32(i) => i.to_string(),
                                            Value::I64(i) => i.to_string(),
                                            Value::U8(i) => i.to_string(),
                                            Value::U16(i) => i.to_string(),
                                            Value::U32(i) => i.to_string(),
                                            Value::U64(i) => i.to_string(),
                                            Value::U128(i) => i.to_string(),
                                            Value::F32(i) => i.to_string(),
                                            Value::F64(i) => i.to_string(),
                                            Value::Boolean(b) => b.to_string(),
                                            _ => unreachable!(),
                                        }))
                                    }
                                    _ => unreachable!(),
                                },
                            }));
                        gen_traversal.should_collect = ShouldCollect::ToVal;
                        gen_traversal.traversal_type = TraversalType::Ref;
                        Type::Node(Some(node_type.to_string()))
                    }
                    IdType::Identifier { value: i, loc } => {
                        gen_traversal.source_step =
                            Separator::Period(SourceStep::NFromID(NFromID {
                                id: {
                                    is_valid_identifier(
                                        ctx,
                                        original_query,
                                        loc.clone(),
                                        i.as_str(),
                                    );
                                    let _ = type_in_scope(
                                        ctx,
                                        original_query,
                                        loc.clone(),
                                        scope,
                                        i.as_str(),
                                    );
                                    let value = gen_identifier_or_param(
                                        original_query,
                                        i.as_str(),
                                        true,
                                        false,
                                    );
                                    value.inner().clone()
                                },
                                label: GenRef::Literal(node_type.clone()),
                            }));
                        gen_traversal.traversal_type = TraversalType::Ref;
                        gen_traversal.should_collect = ShouldCollect::ToVal;
                        Type::Node(Some(node_type.to_string()))
                    }
                    IdType::Literal { value: s, loc: _ } => {
                        gen_traversal.source_step =
                            Separator::Period(SourceStep::NFromID(NFromID {
                                id: GenRef::Ref(s),
                                label: GenRef::Literal(node_type.clone()),
                            }));
                        gen_traversal.traversal_type = TraversalType::Ref;
                        gen_traversal.should_collect = ShouldCollect::ToVal;
                        Type::Node(Some(node_type.to_string()))
                    }
                }
            } else {
                gen_traversal.source_step = Separator::Period(SourceStep::NFromType(NFromType {
                    label: GenRef::Literal(node_type.clone()),
                }));
                gen_traversal.traversal_type = TraversalType::Ref;
                Type::Nodes(Some(node_type.to_string()))
            }
        }
        StartNode::Edge { edge_type, ids } => {
            if !ctx.edge_map.contains_key(edge_type.as_str()) {
                generate_error!(ctx, original_query, tr.loc.clone(), E102, edge_type);
            }
            if let Some(ids) = ids {
                assert!(ids.len() == 1, "multiple ids not supported yet");
                gen_traversal.source_step = Separator::Period(SourceStep::EFromID(EFromID {
                    id: match ids[0].clone() {
                        IdType::Identifier { value: i, loc } => {
                            is_valid_identifier(ctx, original_query, loc.clone(), i.as_str());
                            let _ =
                                type_in_scope(ctx, original_query, loc.clone(), scope, i.as_str());
                            let value =
                                gen_identifier_or_param(original_query, i.as_str(), true, false);
                            value.inner().clone()
                        }
                        IdType::Literal { value: s, loc: _ } => GenRef::Std(s),
                        _ => unreachable!(),
                    },
                    label: GenRef::Literal(edge_type.clone()),
                }));
                gen_traversal.traversal_type = TraversalType::Ref;
                gen_traversal.should_collect = ShouldCollect::ToVal;
                Type::Edge(Some(edge_type.to_string()))
            } else {
                gen_traversal.source_step = Separator::Period(SourceStep::EFromType(EFromType {
                    label: GenRef::Literal(edge_type.clone()),
                }));
                gen_traversal.traversal_type = TraversalType::Ref;
                Type::Edges(Some(edge_type.to_string()))
            }
        }

        StartNode::Identifier(identifier) => {
            match is_valid_identifier(ctx, original_query, tr.loc.clone(), identifier.as_str()) {
                true => scope.get(identifier.as_str()).cloned().map_or_else(
                    || {
                        generate_error!(
                            ctx,
                            original_query,
                            tr.loc.clone(),
                            E301,
                            identifier.as_str()
                        );
                        Type::Unknown
                    },
                    |var_type| {
                        gen_traversal.traversal_type =
                            TraversalType::FromVar(GenRef::Std(identifier.clone()));
                        gen_traversal.source_step = Separator::Empty(SourceStep::Identifier(
                            GenRef::Std(identifier.clone()),
                        ));
                        var_type.clone()
                    },
                ),
                false => Type::Unknown,
            }
        }
        // anonymous will be the traversal type rather than the start type
        StartNode::Anonymous => {
            let parent = parent_ty.unwrap();
            gen_traversal.traversal_type = TraversalType::FromVar(GenRef::Std("val".to_string())); // TODO: ensure this default is stable
            gen_traversal.source_step = Separator::Empty(SourceStep::Anonymous);
            parent
        }
        StartNode::SearchVector(sv) => {
            if let Some(ref ty) = sv.vector_type {
                if !ctx.vector_set.contains(ty.as_str()) {
                    generate_error!(ctx, original_query, sv.loc.clone(), E103, ty.as_str());
                }
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
                    // if is in params then use data.
                    let _ = type_in_scope(ctx, original_query, sv.loc.clone(), scope, i.as_str());
                    VecData::Standard(gen_identifier_or_param(
                        original_query,
                        i.as_str(),
                        true,
                        false,
                    ))
                }
                Some(VectorData::Embed(e)) => {
                    match &e.value {
                        EvaluatesToString::Identifier(i) => VecData::Embed(
                            gen_identifier_or_param(original_query, i.as_str(), true, false),
                        ),
                        EvaluatesToString::StringLiteral(s) => {
                            VecData::Embed(GeneratedValue::Literal(GenRef::Ref(s.clone())))
                        }
                    }
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
                    // Where/boolean ops don't change the element type,
                    // so `cur_ty` stays the same.
                    assert!(stmt.is_some());
                    let stmt = stmt.unwrap();
                    let mut gen_traversal = GeneratedTraversal {
                        traversal_type: TraversalType::NestedFrom(GenRef::Std("v".to_string())),
                        steps: vec![],
                        should_collect: ShouldCollect::ToVec,
                        source_step: Separator::Empty(SourceStep::Anonymous),
                    };
                    match stmt {
                        GeneratedStatement::Traversal(tr) => {
                            gen_traversal
                                .steps
                                .push(Separator::Period(GeneratedStep::Where(Where::Ref(
                                    WhereRef {
                                        expr: BoExp::Expr(tr),
                                    },
                                ))));
                        }
                        GeneratedStatement::BoExp(expr) => {
                            gen_traversal
                                .steps
                                .push(Separator::Period(GeneratedStep::Where(match expr {
                                    BoExp::Exists(mut tr) => {
                                        tr.should_collect = ShouldCollect::No;
                                        Where::Ref(WhereRef {
                                            expr: BoExp::Exists(tr),
                                        })
                                    }
                                    _ => Where::Ref(WhereRef { expr }),
                                })));
                        }
                        _ => unreachable!(),
                    }
                    Some(vec![BoExp::Expr(gen_traversal)])
                }
                None => None,
            };

            gen_traversal.traversal_type = TraversalType::Ref;
            gen_traversal.should_collect = ShouldCollect::ToVec;
            gen_traversal.source_step = Separator::Period(SourceStep::SearchVector(SearchVector {
                label: GenRef::Literal(sv.vector_type.clone().unwrap()),
                vec,
                k,
                pre_filter,
            }));
            // Search returns nodes that contain the vectors
            Type::Vectors(sv.vector_type.clone())
        }
    };

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
                match apply_graph_step(ctx, gs, &cur_ty, original_query, gen_traversal, scope) {
                    Some(new_ty) => {
                        cur_ty = new_ty;
                    }
                    None => { /* error already recorded */ }
                }
                excluded.clear(); // Traversal to a new element resets exclusions
            }

            StepType::Count => {
                cur_ty = Type::Scalar(FieldType::I64);
                excluded.clear();
                gen_traversal
                    .steps
                    .push(Separator::Period(GeneratedStep::Count));
                gen_traversal.should_collect = ShouldCollect::No;
            }

            StepType::Exclude(ex) => {
                // checks if exclude is either the last step or the step before an object remapping or closure
                // i.e. you cant have `N<Type>::!{field1}::Out<Label>`
                if !(i == number_of_steps
                    || (i != number_of_steps - 1
                        && (!matches!(tr.steps[i + 1].step, StepType::Closure(_))
                            || !matches!(tr.steps[i + 1].step, StepType::Object(_)))))
                {
                    generate_error!(ctx, original_query, ex.loc.clone(), E644);
                }
                validate_exclude(ctx, &cur_ty, tr, ex, &excluded, original_query);
                for (_, key) in &ex.fields {
                    excluded.insert(key.as_str(), ex.loc.clone());
                }
                gen_traversal
                    .steps
                    .push(Separator::Period(GeneratedStep::Remapping(Remapping {
                        variable_name: "item".to_string(), // TODO: Change to start var
                        is_inner: false,
                        should_spread: false,
                        remappings: vec![RemappingType::ExcludeField(ExcludeField {
                            variable_name: "item".to_string(), // TODO: Change to start var
                            fields_to_exclude: ex
                                .fields
                                .iter()
                                .map(|(_, field)| GenRef::Literal(field.clone()))
                                .collect(),
                        })],
                    })));
            }

            StepType::Object(obj) => {
                // TODO: Fix issue with step count being incorrect (i think its counting each field as a step)
                // if i != number_of_steps {
                //     println!("{} {}", i, number_of_steps);
                //     push_query_err(ctx,
                //         original_query,
                //         obj.loc.clone(),
                //         "object is only valid as the last step in a traversal".to_string(),
                //         "move the object to the end of the traversal",
                //     );
                // }
                validate_object(
                    ctx,
                    &cur_ty,
                    tr,
                    obj,
                    &excluded,
                    original_query,
                    gen_traversal,
                    gen_query,
                    scope,
                    None,
                );
            }

            StepType::Where(expr) => {
                let (_, stmt) = infer_expr_type(
                    ctx,
                    expr,
                    scope,
                    original_query,
                    Some(cur_ty.clone()),
                    gen_query,
                );
                // Where/boolean ops don't change the element type,
                // so `cur_ty` stays the same.
                assert!(stmt.is_some());
                let stmt = stmt.unwrap();
                match stmt {
                    GeneratedStatement::Traversal(tr) => {
                        gen_traversal
                            .steps
                            .push(Separator::Period(GeneratedStep::Where(Where::Ref(
                                WhereRef {
                                    expr: BoExp::Expr(tr),
                                },
                            ))));
                    }
                    GeneratedStatement::BoExp(expr) => {
                        gen_traversal
                            .steps
                            .push(Separator::Period(GeneratedStep::Where(match expr {
                                BoExp::Exists(mut tr) => {
                                    tr.should_collect = ShouldCollect::No;
                                    Where::Ref(WhereRef {
                                        expr: BoExp::Exists(tr),
                                    })
                                }
                                _ => Where::Ref(WhereRef { expr }),
                            })));
                    }
                    _ => unreachable!(),
                }
            }
            StepType::BooleanOperation(b_op) => {
                let step = previous_step.unwrap();
                let property_type = match &b_op.op {
                    BooleanOpType::LessThanOrEqual(expr)
                    | BooleanOpType::LessThan(expr)
                    | BooleanOpType::GreaterThanOrEqual(expr)
                    | BooleanOpType::GreaterThan(expr)
                    | BooleanOpType::Equal(expr)
                    | BooleanOpType::NotEqual(expr) => {
                        match infer_expr_type(
                            ctx,
                            expr,
                            scope,
                            original_query,
                            Some(cur_ty.clone()),
                            gen_query,
                        ) {
                            (Type::Scalar(ft), _) => ft.clone(),
                            (field_type, _) => {
                                generate_error!(
                                    ctx,
                                    original_query,
                                    b_op.loc.clone(),
                                    E621,
                                    &b_op.loc.span,
                                    field_type.kind_str()
                                );
                                return field_type;
                            }
                        }
                    }
                    _ => return cur_ty.clone(),
                };

                // get type of field name
                let field_name = match step {
                    StepType::Object(obj) => {
                        let fields = obj.fields;
                        assert!(fields.len() == 1);
                        Some(fields[0].value.value.clone())
                    }
                    _ => None,
                };
                if let Some(FieldValueType::Identifier(field_name)) = &field_name {
                    is_valid_identifier(ctx, original_query, b_op.loc.clone(), field_name.as_str());
                    match &cur_ty {
                        Type::Nodes(Some(node_ty)) | Type::Node(Some(node_ty)) => {
                            let field_set = ctx.node_fields.get(node_ty.as_str()).cloned();
                            if let Some(field_set) = field_set {
                                match field_set.get(field_name.as_str()) {
                                    Some(field) => {
                                        if field.field_type != property_type {
                                            generate_error!(
                                                ctx,
                                                original_query,
                                                b_op.loc.clone(),
                                                E622,
                                                field_name,
                                                cur_ty.kind_str(),
                                                &cur_ty.get_type_name(),
                                                &field.field_type.to_string(),
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
                            let field_set = ctx.edge_fields.get(edge_ty.as_str()).cloned();
                            if let Some(field_set) = field_set {
                                match field_set.get(field_name.as_str()) {
                                    Some(field) => {
                                        if field.field_type != property_type {
                                            generate_error!(
                                                ctx,
                                                original_query,
                                                b_op.loc.clone(),
                                                E622,
                                                field_name,
                                                cur_ty.kind_str(),
                                                &cur_ty.get_type_name(),
                                                &field.field_type.to_string(),
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
                            let field_set = ctx.vector_fields.get(sv.as_str()).cloned();
                            if let Some(field_set) = field_set {
                                match field_set.get(field_name.as_str()) {
                                    Some(field) => {
                                        if field.field_type != property_type {
                                            generate_error!(
                                                ctx,
                                                original_query,
                                                b_op.loc.clone(),
                                                E622,
                                                field_name,
                                                cur_ty.kind_str(),
                                                &cur_ty.get_type_name(),
                                                &field.field_type.to_string(),
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

                // ctx.infer_expr_type(expr, scope, q);
                // Where/boolean ops don't change the element type,
                // so `cur_ty` stays the same.
                let op = match &b_op.op {
                    BooleanOpType::LessThanOrEqual(expr) => {
                        // assert!()
                        let v = match &expr.expr {
                            ExpressionType::IntegerLiteral(i) => {
                                GeneratedValue::Primitive(GenRef::Std(i.to_string()))
                            }
                            ExpressionType::FloatLiteral(f) => {
                                GeneratedValue::Primitive(GenRef::Std(f.to_string()))
                            }
                            ExpressionType::Identifier(i) => {
                                is_valid_identifier(
                                    ctx,
                                    original_query,
                                    expr.loc.clone(),
                                    i.as_str(),
                                );
                                gen_identifier_or_param(original_query, i.as_str(), false, true)
                            }
                            _ => unreachable!("Cannot reach here"),
                        };
                        BoolOp::Lte(Lte { value: v })
                    }
                    BooleanOpType::LessThan(expr) => {
                        let v = match &expr.expr {
                            ExpressionType::IntegerLiteral(i) => {
                                GeneratedValue::Primitive(GenRef::Std(i.to_string()))
                            }
                            ExpressionType::FloatLiteral(f) => {
                                GeneratedValue::Primitive(GenRef::Std(f.to_string()))
                            }
                            ExpressionType::Identifier(i) => {
                                is_valid_identifier(
                                    ctx,
                                    original_query,
                                    expr.loc.clone(),
                                    i.as_str(),
                                );
                                gen_identifier_or_param(original_query, i.as_str(), false, true)
                            }
                            _ => unreachable!("Cannot reach here"),
                        };
                        BoolOp::Lt(Lt { value: v })
                    }
                    BooleanOpType::GreaterThanOrEqual(expr) => {
                        let v = match &expr.expr {
                            ExpressionType::IntegerLiteral(i) => {
                                GeneratedValue::Primitive(GenRef::Std(i.to_string()))
                            }
                            ExpressionType::FloatLiteral(f) => {
                                GeneratedValue::Primitive(GenRef::Std(f.to_string()))
                            }
                            ExpressionType::Identifier(i) => {
                                is_valid_identifier(
                                    ctx,
                                    original_query,
                                    expr.loc.clone(),
                                    i.as_str(),
                                );
                                gen_identifier_or_param(original_query, i.as_str(), false, true)
                            }
                            _ => unreachable!("Cannot reach here"),
                        };
                        BoolOp::Gte(Gte { value: v })
                    }
                    BooleanOpType::GreaterThan(expr) => {
                        let v = match &expr.expr {
                            ExpressionType::IntegerLiteral(i) => {
                                GeneratedValue::Primitive(GenRef::Std(i.to_string()))
                            }
                            ExpressionType::FloatLiteral(f) => {
                                GeneratedValue::Primitive(GenRef::Std(f.to_string()))
                            }
                            ExpressionType::Identifier(i) => {
                                is_valid_identifier(
                                    ctx,
                                    original_query,
                                    expr.loc.clone(),
                                    i.as_str(),
                                );
                                gen_identifier_or_param(original_query, i.as_str(), false, true)
                            }
                            _ => unreachable!("Cannot reach here"),
                        };
                        BoolOp::Gt(Gt { value: v })
                    }
                    BooleanOpType::Equal(expr) => {
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
                                is_valid_identifier(
                                    ctx,
                                    original_query,
                                    expr.loc.clone(),
                                    i.as_str(),
                                );
                                gen_identifier_or_param(original_query, i.as_str(), false, true)
                            }
                            _ => unreachable!("Cannot reach here"),
                        };
                        BoolOp::Eq(Eq { value: v })
                    }
                    BooleanOpType::NotEqual(expr) => {
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
                                is_valid_identifier(
                                    ctx,
                                    original_query,
                                    expr.loc.clone(),
                                    i.as_str(),
                                );
                                gen_identifier_or_param(original_query, i.as_str(), false, true)
                            }
                            _ => unreachable!("Cannot reach here"),
                        };
                        BoolOp::Neq(Neq { value: v })
                    }
                    _ => unreachable!("shouldve been caught eariler"),
                };
                gen_traversal
                    .steps
                    .push(Separator::Period(GeneratedStep::BoolOp(op)));
                gen_traversal.should_collect = ShouldCollect::No;
            }

            StepType::Update(update) => {
                // if type == node, edge, vector then update is valid
                // otherwise it is invalid

                // Update returns the same type (nodes/edges) it started with.
                match tr.steps.iter().nth_back(1) {
                    Some(step) => match &step.step {
                        StepType::Node(gs) => {
                            let node_type = gs.get_item_type().unwrap();
                            field_exists_on_item_type(
                                ctx,
                                original_query,
                                Type::Node(Some(node_type.clone())),
                                update
                                    .fields
                                    .iter()
                                    .map(|field| (field.key.as_str(), &field.loc))
                                    .collect(),
                            );
                        }

                        StepType::Edge(gs) => {
                            let edge_type = gs.get_item_type().unwrap();
                            field_exists_on_item_type(
                                ctx,
                                original_query,
                                Type::Edge(Some(edge_type)),
                                update
                                    .fields
                                    .iter()
                                    .map(|field| (field.key.as_str(), &field.loc))
                                    .collect(),
                            );
                        }
                        _ => {
                            // TODO: maybe use cur_ty instead of update.loc.span?
                            generate_error!(
                                ctx,
                                original_query,
                                update.loc.clone(),
                                E604,
                                &update.loc.span
                            );
                            return cur_ty.clone();
                        }
                    },
                    None => match &tr.start {
                        StartNode::Node { node_type, .. } => {
                            field_exists_on_item_type(
                                ctx,
                                original_query,
                                Type::Node(Some(node_type.clone())),
                                update
                                    .fields
                                    .iter()
                                    .map(|field| (field.key.as_str(), &field.loc))
                                    .collect(),
                            );
                        }
                        StartNode::Edge { edge_type, .. } => {
                            field_exists_on_item_type(
                                ctx,
                                original_query,
                                Type::Edge(Some(edge_type.clone())),
                                update
                                    .fields
                                    .iter()
                                    .map(|field| (field.key.as_str(), &field.loc))
                                    .collect(),
                            );
                        }
                        _ => {
                            // maybe use cur_ty instead of update.loc.span?
                            generate_error!(
                                ctx,
                                original_query,
                                update.loc.clone(),
                                E604,
                                &update.loc.span
                            );
                            return cur_ty.clone();
                        }
                    },
                };
                gen_traversal.traversal_type = TraversalType::Update(Some(
                    update
                        .fields
                        .iter()
                        .map(|field| {
                            (
                                field.key.clone(),
                                match &field.value.value {
                                    FieldValueType::Identifier(i) => {
                                        is_valid_identifier(
                                            ctx,
                                            original_query,
                                            field.value.loc.clone(),
                                            i.as_str(),
                                        );
                                        gen_identifier_or_param(
                                            original_query,
                                            i.as_str(),
                                            true,
                                            true,
                                        )
                                    }
                                    FieldValueType::Literal(l) => match l {
                                        Value::String(s) => {
                                            GeneratedValue::Literal(GenRef::Literal(s.clone()))
                                        }
                                        other => GeneratedValue::Primitive(GenRef::Std(
                                            other.to_string(),
                                        )),
                                    },
                                    FieldValueType::Expression(e) => match &e.expr {
                                        ExpressionType::Identifier(i) => {
                                            is_valid_identifier(
                                                ctx,
                                                original_query,
                                                e.loc.clone(),
                                                i.as_str(),
                                            );
                                            gen_identifier_or_param(
                                                original_query,
                                                i.as_str(),
                                                true,
                                                true,
                                            )
                                        }
                                        ExpressionType::StringLiteral(i) => {
                                            GeneratedValue::Primitive(GenRef::Std(i.to_string()))
                                        }

                                        ExpressionType::IntegerLiteral(i) => {
                                            GeneratedValue::Primitive(GenRef::Std(i.to_string()))
                                        }
                                        ExpressionType::FloatLiteral(i) => {
                                            GeneratedValue::Primitive(GenRef::Std(i.to_string()))
                                        }
                                        v => {
                                            println!("ID {v:?}");
                                            panic!("expr be primitive or value")
                                        }
                                    },
                                    v => {
                                        println!("{v:?}");
                                        panic!("Should be primitive or value")
                                    }
                                },
                            )
                        })
                        .collect(),
                ));
                gen_traversal.should_collect = ShouldCollect::No;
                excluded.clear();
            }

            StepType::AddEdge(add) => {
                if let Some(ref ty) = add.edge_type {
                    if !ctx.edge_map.contains_key(ty.as_str()) {
                        generate_error!(ctx, original_query, add.loc.clone(), E102, ty);
                    }
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
                        if let Some(ty) = ty {
                            if !ty.is_integer() {
                                generate_error!(
                                    ctx,
                                    original_query,
                                    start.loc.clone(),
                                    E633,
                                    [&start.loc.span, &ty.get_type_name()],
                                    [i.as_str()]
                                );
                                return cur_ty.clone(); // Not sure if this should be here
                            }
                        };
                        let ty =
                            type_in_scope(ctx, original_query, end.loc.clone(), scope, j.as_str());
                        if let Some(ty) = ty {
                            if !ty.is_integer() {
                                generate_error!(
                                    ctx,
                                    original_query,
                                    end.loc.clone(),
                                    E633,
                                    [&end.loc.span, &ty.get_type_name()],
                                    [j.as_str()]
                                );
                                return cur_ty.clone(); // Not sure if this should be here
                            }
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
                        if let Some(ty) = ty {
                            if !ty.is_integer() {
                                generate_error!(
                                    ctx,
                                    original_query,
                                    start.loc.clone(),
                                    E633,
                                    [&start.loc.span, &ty.get_type_name()],
                                    [i.as_str()]
                                );
                                return cur_ty.clone(); // Not sure if this should be here
                            }
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
                        if let Some(ty) = ty {
                            if !ty.is_integer() {
                                generate_error!(
                                    ctx,
                                    original_query,
                                    end.loc.clone(),
                                    E633,
                                    [&end.loc.span, &ty.get_type_name()],
                                    [j.as_str()]
                                );
                                return cur_ty.clone();
                            }
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
                        return cur_ty.clone();
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
                        return cur_ty.clone();
                    }
                    _ => unreachable!("shouldve been caught eariler"),
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

                assert!(stmt.is_some());
                match stmt.unwrap() {
                    GeneratedStatement::Traversal(traversal) => {
                        let property = match &traversal.steps.last() {
                            Some(step) => match &step.inner() {
                                GeneratedStep::PropertyFetch(property) => property.clone(),
                                _ => unreachable!("Cannot reach here"),
                            },
                            None => unreachable!("Cannot reach here"),
                        };
                        gen_traversal
                            .steps
                            .push(Separator::Period(GeneratedStep::OrderBy(OrderBy {
                                property,
                                order: match order_by.order_by_type {
                                    OrderByType::Asc => Order::Asc,
                                    OrderByType::Desc => Order::Desc,
                                },
                            })));
                        gen_traversal.should_collect = ShouldCollect::ToVec;
                    }
                    _ => unreachable!("Cannot reach here"),
                }
            }
            StepType::Closure(cl) => {
                if i != number_of_steps {
                    generate_error!(ctx, original_query, cl.loc.clone(), E641);
                }
                // Add identifier to a temporary scope so inner uses pass
                scope.insert(cl.identifier.as_str(), cur_ty.clone()); // If true then already exists so return error
                let obj = &cl.object;
                validate_object(
                    ctx,
                    &cur_ty,
                    tr,
                    obj,
                    &excluded,
                    original_query,
                    gen_traversal,
                    gen_query,
                    scope,
                    Some(Variable::new(cl.identifier.clone(), cur_ty.clone())),
                );

                // gen_traversal
                //     .steps
                //     .push(Separator::Period(GeneratedStep::Remapping(Remapping {
                //         is_inner: false,
                //         should_spread: false,
                //         variable_name: cl.identifier.clone(),
                //         remappings: (),
                //     })));
                scope.remove(cl.identifier.as_str());
                // gen_traversal.traversal_type =
                //     TraversalType::Nested(GenRef::Std(var));
            }
        }
        previous_step = Some(step.clone());
    }
    match gen_traversal.traversal_type {
        TraversalType::Mut | TraversalType::Update(_) => {
            gen_query.is_mut = true;
        }
        _ => {}
    }
    cur_ty
}
