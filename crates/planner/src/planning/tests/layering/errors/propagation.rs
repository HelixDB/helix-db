use super::*;

#[test]
fn top_level_context_still_errors_outside_sub_traversals() {
    let batch = ReadBatch {
        entries: vec![BatchEntry::Query(Box::new(NamedQuery {
            name: Some("invalid".to_string()),
            root: AstNode::Context,
            condition: None,
        }))],
        returns: Vec::new(),
    };
    let err = plan_read_checked(&batch, &PlannerContext::default()).unwrap_err();

    assert_eq!(err, PlannerError::UnboundContext);
}

#[test]
fn followup_batch_entry_propagates_planning_errors() {
    let batch = ReadBatch {
        entries: vec![
            BatchEntry::Query(Box::new(NamedQuery {
                name: Some("valid".to_string()),
                root: AstNode::Nodes {
                    reference: NodeRef::all(),
                },
                condition: None,
            })),
            BatchEntry::Query(Box::new(NamedQuery {
                name: Some("invalid".to_string()),
                root: *invalid_param_node_source(),
                condition: None,
            })),
        ],
        returns: Vec::new(),
    };

    assert_eq!(
        plan_read_checked(&batch, &PlannerContext::default()).unwrap_err(),
        PlannerError::InvalidEmptyName {
            field: NameField::Param
        }
    );
}

#[test]
fn wrapper_inputs_propagate_child_planning_errors() {
    let cases: &[(&str, AstWrapper)] = &[
        ("out", |input| AstNode::Out {
            input,
            label: Some("FOLLOWS".to_string()),
        }),
        ("in", |input| AstNode::In {
            input,
            label: Some("FOLLOWS".to_string()),
        }),
        ("both", |input| AstNode::Both {
            input,
            label: Some("FOLLOWS".to_string()),
        }),
        ("out_e", |input| AstNode::OutE {
            input,
            label: Some("FOLLOWS".to_string()),
        }),
        ("in_e", |input| AstNode::InE {
            input,
            label: Some("FOLLOWS".to_string()),
        }),
        ("both_e", |input| AstNode::BothE {
            input,
            label: Some("FOLLOWS".to_string()),
        }),
        ("out_n", |input| AstNode::OutN { input }),
        ("in_n", |input| AstNode::InN { input }),
        ("other_n", |input| AstNode::OtherN { input }),
        ("has", |input| AstNode::Has {
            input,
            property: "active".to_string(),
            value: PropertyValue::Bool(true),
        }),
        ("has_label", |input| AstNode::HasLabel {
            input,
            label: "User".to_string(),
        }),
        ("has_key", |input| AstNode::HasKey {
            input,
            property: "active".to_string(),
        }),
        ("where", |input| AstNode::Where {
            input,
            predicate: Predicate::eq("active", true),
        }),
        ("dedup", |input| AstNode::Dedup { input }),
        ("within", |input| AstNode::Within {
            input,
            variable: "visited".to_string(),
        }),
        ("without", |input| AstNode::Without {
            input,
            variable: "visited".to_string(),
        }),
        ("edge_has", |input| AstNode::EdgeHas {
            input,
            property: "active".to_string(),
            value: PropertyInput::from(true),
        }),
        ("edge_has_label", |input| AstNode::EdgeHasLabel {
            input,
            label: "FOLLOWS".to_string(),
        }),
        ("limit", |input| AstNode::Limit {
            input,
            count: StreamBound::Literal(5),
        }),
        ("skip", |input| AstNode::Skip {
            input,
            count: StreamBound::Literal(2),
        }),
        ("range", |input| AstNode::Range {
            input,
            start: StreamBound::Literal(1),
            end: StreamBound::Literal(5),
        }),
        ("as", |input| AstNode::As {
            input,
            name: "users".to_string(),
        }),
        ("store", |input| AstNode::Store {
            input,
            name: "users".to_string(),
        }),
        ("select", |input| AstNode::Select {
            input,
            name: "users".to_string(),
        }),
        ("bind", |input| AstNode::Bind {
            input,
            name: "user".to_string(),
        }),
        ("inject", |input| AstNode::Inject {
            input: Some(input),
            variable: "users".to_string(),
        }),
        ("count", |input| AstNode::Count { input }),
        ("exists", |input| AstNode::Exists { input }),
        ("id", |input| AstNode::Id { input }),
        ("label", |input| AstNode::Label { input }),
        ("values", |input| AstNode::Values {
            input,
            properties: vec!["name".to_string()],
        }),
        ("value_map", |input| AstNode::ValueMap {
            input,
            properties: Some(vec!["name".to_string()]),
        }),
        ("project", |input| AstNode::Project {
            input,
            projections: vec![Projection::property("name", "name")],
        }),
        ("project_bindings", |input| AstNode::ProjectBindings {
            input,
            projections: vec![BindingProjection::current("$id", "id")],
            distinct: true,
        }),
        ("edge_properties", |input| AstNode::EdgeProperties { input }),
        ("add_n", |input| AstNode::AddN {
            input: Some(input),
            label: "User".to_string(),
            properties: vec![("name".to_string(), PropertyInput::from("alice"))],
        }),
        ("add_e", |input| AstNode::AddE {
            input,
            label: "FOLLOWS".to_string(),
            to: NodeRef::id(42),
            properties: vec![("since".to_string(), PropertyInput::from(2024i64))],
        }),
        ("set_property", |input| AstNode::SetProperty {
            input,
            name: "name".to_string(),
            value: PropertyInput::from("alice"),
        }),
        ("remove_property", |input| AstNode::RemoveProperty {
            input,
            name: "name".to_string(),
        }),
        ("drop", |input| AstNode::Drop { input }),
        ("drop_edge", |input| AstNode::DropEdge {
            input,
            to: NodeRef::id(42),
        }),
        ("drop_edge_labeled", |input| AstNode::DropEdgeLabeled {
            input,
            to: NodeRef::id(42),
            label: "FOLLOWS".to_string(),
        }),
        ("drop_edge_by_id", |input| AstNode::DropEdgeById {
            input: Some(input),
            edges: EdgeRef::id(7),
        }),
        ("order_by", |input| AstNode::OrderBy {
            input,
            property: "name".to_string(),
            order: Order::Asc,
        }),
        ("order_by_multiple", |input| AstNode::OrderByMultiple {
            input,
            orderings: vec![("name".to_string(), Order::Asc)],
        }),
        ("repeat", |input| AstNode::Repeat {
            input,
            config: RepeatConfig::new(sub().out(Some("FOLLOWS"))),
        }),
        ("union", |input| AstNode::Union {
            input,
            traversals: vec![sub().out(Some("FOLLOWS")), sub().in_(Some("MENTIONS"))],
        }),
        ("choose", |input| AstNode::Choose {
            input,
            condition: Predicate::eq("active", true),
            then_traversal: sub().out(Some("FOLLOWS")),
            else_traversal: Some(sub().in_(Some("MENTIONS"))),
        }),
        ("coalesce", |input| AstNode::Coalesce {
            input,
            traversals: vec![sub().out(Some("FOLLOWS"))],
        }),
        ("optional", |input| AstNode::Optional {
            input,
            traversal: sub().out(Some("FOLLOWS")),
        }),
        ("group", |input| AstNode::Group {
            input,
            property: "tenant_id".to_string(),
        }),
        ("group_count", |input| AstNode::GroupCount {
            input,
            property: "tenant_id".to_string(),
        }),
        ("aggregate_by", |input| AstNode::AggregateBy {
            input,
            function: AggregateFunction::Count,
            property: "tenant_id".to_string(),
        }),
        ("fold", |input| AstNode::Fold { input }),
        ("unfold", |input| AstNode::Unfold { input }),
        ("path", |input| AstNode::Path { input }),
        ("simple_path", |input| AstNode::SimplePath { input }),
        ("with_sack", |input| AstNode::WithSack {
            input,
            initial: PropertyValue::I64(0),
        }),
        ("sack_set", |input| AstNode::SackSet {
            input,
            property: "score".to_string(),
        }),
        ("sack_add", |input| AstNode::SackAdd {
            input,
            property: "score".to_string(),
        }),
        ("sack_get", |input| AstNode::SackGet { input }),
    ];

    for (name, wrap) in cases {
        assert_eq!(
            plan_read_checked(
                &raw_read((*wrap)(invalid_param_node_source())),
                &PlannerContext::default()
            )
            .unwrap_err(),
            PlannerError::InvalidEmptyName {
                field: NameField::Param
            },
            "{name} should propagate the child planning error"
        );
    }
}

#[test]
fn branch_sub_traversals_propagate_body_planning_errors() {
    let invalid = invalid_sub_traversal();
    let cases = [
        AstNode::Repeat {
            input: boxed_nodes_root(),
            config: RepeatConfig::new(invalid.clone()),
        },
        AstNode::Union {
            input: boxed_nodes_root(),
            traversals: vec![sub().out(Some("FOLLOWS")), invalid.clone()],
        },
        AstNode::Choose {
            input: boxed_nodes_root(),
            condition: Predicate::eq("active", true),
            then_traversal: invalid.clone(),
            else_traversal: None,
        },
        AstNode::Choose {
            input: boxed_nodes_root(),
            condition: Predicate::eq("active", true),
            then_traversal: sub().out(Some("FOLLOWS")),
            else_traversal: Some(invalid.clone()),
        },
        AstNode::Coalesce {
            input: boxed_nodes_root(),
            traversals: vec![invalid.clone()],
        },
        AstNode::Optional {
            input: boxed_nodes_root(),
            traversal: invalid,
        },
    ];

    for root in cases {
        assert_eq!(
            plan_read_checked(&raw_read(root), &PlannerContext::default()).unwrap_err(),
            PlannerError::InvalidEmptyName {
                field: NameField::Param
            }
        );
    }
}
