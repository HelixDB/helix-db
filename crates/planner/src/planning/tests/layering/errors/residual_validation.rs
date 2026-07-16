use super::*;

#[test]
fn indexed_and_empty_candidate_residuals_reject_empty_predicate_fields_from_raw_ast() {
    let cases = [
        (
            AstNode::NodesWhere {
                predicate: Predicate::and(vec![
                    Predicate::eq("$label", "User"),
                    Predicate::eq_param("username", "name"),
                    Predicate::has_key(String::new()),
                ]),
            },
            ctx(builtin_label_indexes()
                .with_node_eq(ScopedPropertyKey::try_new("User", "username").unwrap())),
        ),
        (
            AstNode::NodesWhere {
                predicate: Predicate::and(vec![
                    Predicate::eq("$label", "User"),
                    Predicate::eq(String::new(), "alice"),
                ]),
            },
            PlannerContext::default(),
        ),
        (
            AstNode::NodesWhere {
                predicate: Predicate::and(vec![
                    Predicate::eq("$label", "User"),
                    Predicate::gte(String::new(), 21),
                ]),
            },
            PlannerContext::default(),
        ),
        (
            AstNode::NodesWhere {
                predicate: Predicate::and(vec![
                    Predicate::has_key("username"),
                    Predicate::has_key(String::new()),
                ]),
            },
            PlannerContext::default(),
        ),
        (
            AstNode::EdgesWhere {
                predicate: Predicate::and(vec![
                    Predicate::eq("$label", "FOLLOWS"),
                    Predicate::eq_param("since", "year"),
                    Predicate::has_key(String::new()),
                ]),
            },
            ctx(builtin_label_indexes()
                .with_edge_eq(ScopedPropertyKey::try_new("FOLLOWS", "since").unwrap())),
        ),
        (
            AstNode::EdgesWhere {
                predicate: Predicate::and(vec![
                    Predicate::eq("$label", "FOLLOWS"),
                    Predicate::eq(String::new(), "active"),
                ]),
            },
            PlannerContext::default(),
        ),
        (
            AstNode::EdgesWhere {
                predicate: Predicate::and(vec![
                    Predicate::eq("$label", "FOLLOWS"),
                    Predicate::lt(String::new(), 50),
                ]),
            },
            PlannerContext::default(),
        ),
        (
            AstNode::EdgesWhere {
                predicate: Predicate::and(vec![
                    Predicate::has_key("since"),
                    Predicate::has_key(String::new()),
                ]),
            },
            PlannerContext::default(),
        ),
    ];

    for (root, context) in cases {
        assert_eq!(
            plan_read_checked(&raw_read(root), &context).unwrap_err(),
            PlannerError::InvalidEmptyName {
                field: NameField::Property
            }
        );
    }
}

#[test]
fn variable_filter_pushdown_propagates_index_candidate_errors() {
    let root = AstNode::Where {
        input: Box::new(AstNode::Within {
            input: boxed_nodes_root(),
            variable: "allowed".to_string(),
        }),
        predicate: Predicate::and(vec![
            Predicate::eq("$label", "User"),
            Predicate::eq(String::new(), "alice"),
        ]),
    };

    assert_eq!(
        plan_read_checked(&raw_read(root), &ctx(builtin_label_indexes())).unwrap_err(),
        PlannerError::InvalidEmptyName {
            field: NameField::Property
        }
    );
}
