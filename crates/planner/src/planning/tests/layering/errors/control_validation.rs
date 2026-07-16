use super::*;

#[test]
fn union_requires_at_least_two_branch_traversals() {
    assert_eq!(
        [BranchOp::Union, BranchOp::Coalesce].map(|op| op.to_string()),
        ["union", "coalesce"]
    );

    let empty_union = read_batch().var_as(
        "invalid",
        g().n(NodeRef::all())
            .union(Vec::<helix_ast::traversal::SubTraversal>::new()),
    );
    let single_union = read_batch().var_as(
        "invalid",
        g().n(NodeRef::all())
            .union(vec![sub().out(Some("FOLLOWS"))]),
    );

    assert_eq!(
        plan_read_checked(&empty_union, &PlannerContext::default()).unwrap_err(),
        PlannerError::InvalidBranchArity {
            op: BranchOp::Union,
            min: 2,
            actual: 0,
        }
    );
    assert_eq!(
        plan_read_checked(&single_union, &PlannerContext::default()).unwrap_err(),
        PlannerError::InvalidBranchArity {
            op: BranchOp::Union,
            min: 2,
            actual: 1,
        }
    );
}

#[test]
fn coalesce_requires_at_least_one_branch_traversal() {
    let empty_coalesce = read_batch().var_as(
        "invalid",
        g().n(NodeRef::all())
            .coalesce(Vec::<helix_ast::traversal::SubTraversal>::new()),
    );

    assert_eq!(
        plan_read_checked(&empty_coalesce, &PlannerContext::default()).unwrap_err(),
        PlannerError::InvalidBranchArity {
            op: BranchOp::Coalesce,
            min: 1,
            actual: 0,
        }
    );
}

#[test]
fn repeat_emit_predicate_requires_after_emit_mode() {
    for emit in [EmitBehavior::None, EmitBehavior::Before, EmitBehavior::All] {
        let batch = read_batch().var_as(
            "invalid",
            g().n(NodeRef::all()).repeat(RepeatConfig {
                traversal: sub().out(Some("FOLLOWS")),
                times: None,
                until: None,
                emit,
                emit_predicate: Some(Predicate::eq("active", true)),
                max_depth: 5,
            }),
        );

        assert_eq!(
            plan_read_checked(&batch, &PlannerContext::default()).unwrap_err(),
            PlannerError::InvalidRepeatEmit { emit }
        );
    }
}

#[test]
fn repeat_counts_reject_zero_literals_from_raw_ast() {
    assert_eq!(
        [RepeatCountField::Times, RepeatCountField::MaxDepth].map(|field| field.to_string()),
        ["times", "max_depth"]
    );

    let cases = [
        (
            RepeatConfig {
                traversal: sub().out(Some("FOLLOWS")),
                times: Some(0),
                until: None,
                emit: EmitBehavior::None,
                emit_predicate: None,
                max_depth: 5,
            },
            RepeatCountField::Times,
        ),
        (
            RepeatConfig {
                traversal: sub().out(Some("FOLLOWS")),
                times: None,
                until: None,
                emit: EmitBehavior::None,
                emit_predicate: None,
                max_depth: 0,
            },
            RepeatCountField::MaxDepth,
        ),
        (
            RepeatConfig {
                traversal: sub().out(Some("FOLLOWS")),
                times: Some(0),
                until: Some(Predicate::eq("done", true)),
                emit: EmitBehavior::None,
                emit_predicate: None,
                max_depth: 5,
            },
            RepeatCountField::Times,
        ),
    ];

    for (config, field) in cases {
        let batch = raw_read(AstNode::Repeat {
            input: boxed_nodes_root(),
            config,
        });

        assert_eq!(
            plan_read_checked(&batch, &PlannerContext::default()).unwrap_err(),
            PlannerError::InvalidRepeatCount { field, actual: 0 }
        );
    }
}

#[test]
fn repeat_times_or_until_propagates_invalid_until_predicate() {
    let batch = raw_read(AstNode::Repeat {
        input: boxed_nodes_root(),
        config: RepeatConfig {
            traversal: sub().out(Some("FOLLOWS")),
            times: Some(2),
            until: Some(Predicate::has_key(String::new())),
            emit: EmitBehavior::None,
            emit_predicate: None,
            max_depth: 5,
        },
    });

    assert_eq!(
        plan_read_checked(&batch, &PlannerContext::default()).unwrap_err(),
        PlannerError::InvalidEmptyName {
            field: NameField::Property
        }
    );
}

#[test]
fn order_requires_at_least_one_key() {
    let batch = read_batch().var_as("invalid", plan_order_by_multiple_without_keys());

    assert_eq!(
        plan_read_checked(&batch, &PlannerContext::default()).unwrap_err(),
        PlannerError::InvalidOrderKeys
    );
}

#[test]
fn order_rejects_duplicate_keys_from_raw_ast() {
    let batch = raw_read(AstNode::OrderByMultiple {
        input: boxed_nodes_root(),
        orderings: vec![
            ("age".to_string(), Order::Asc),
            ("age".to_string(), Order::Desc),
        ],
    });

    assert_eq!(
        plan_read_checked(&batch, &PlannerContext::default()).unwrap_err(),
        PlannerError::DuplicateOrderKey {
            property: NonEmptyString::new("age").unwrap()
        }
    );
}
