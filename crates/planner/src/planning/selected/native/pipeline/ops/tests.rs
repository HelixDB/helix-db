use super::*;
use crate::{error, ir, logical};
use helix_ast::expr::{Expr, StreamBound};
use helix_ast::traversal::{AstNode, Order};
use helix_ast::value::PropertyInput;

fn input() -> Box<AstNode> {
    Box::new(AstNode::Context)
}

fn parse(root: &AstNode) -> Result<NativePipelineRoot<'_>, error::PlannerError> {
    pipeline_op_from_ast(root)
}

fn pipeline_op(root: &AstNode) -> NativePipelineOp<'_> {
    match parse(root).unwrap() {
        NativePipelineRoot::Pipeline(op) => op,
        NativePipelineRoot::NotPipeline => panic!("expected pipeline op"),
    }
}

fn family_op(result: Result<contract::NativePipelineOpMatch<'_>, error::PlannerError>) -> bool {
    result.is_ok_and(|parsed| matches!(parsed, contract::NativePipelineOpMatch::Op(_)))
}

fn family_miss(result: Result<contract::NativePipelineOpMatch<'_>, error::PlannerError>) -> bool {
    result.is_ok_and(|parsed| matches!(parsed, contract::NativePipelineOpMatch::NotThisFamily))
}

#[test]
fn pipeline_op_family_probes_return_typed_matches_and_misses() {
    assert!(family_op(expansion::pipeline_op_from_ast(&AstNode::Out {
        input: input(),
        label: None,
    })));
    assert!(family_op(filter::pipeline_op_from_ast(&AstNode::HasKey {
        input: input(),
        property: "email".to_owned(),
    })));
    assert!(family_op(bounds::pipeline_op_from_ast(&AstNode::Dedup {
        input: input(),
    })));
    assert!(family_op(variable::pipeline_op_from_ast(&AstNode::Bind {
        input: input(),
        name: "users".to_owned(),
    })));

    assert!(family_miss(expansion::pipeline_op_from_ast(
        &AstNode::Context
    )));
    assert!(family_miss(filter::pipeline_op_from_ast(&AstNode::Context)));
    assert!(family_miss(bounds::pipeline_op_from_ast(&AstNode::Context)));
    assert!(family_miss(variable::pipeline_op_from_ast(
        &AstNode::Inject {
            input: None,
            variable: "seed".to_owned(),
        }
    )));
}

#[test]
fn pipeline_op_contract_preserves_input_and_expansion_payload() {
    let root = AstNode::Out {
        input: input(),
        label: Some("LIKES".to_owned()),
    };
    let parsed = pipeline_op(&root);
    let (input, op) = parsed.into_parts();

    assert!(matches!(input, AstNode::Context));
    assert!(matches!(
        op,
        logical::StreamPipelineOp::Expand { plan }
            if matches!(plan.direction, ir::ExpandDirection::Out)
                && matches!(plan.output, ir::ExpandOutput::Nodes)
                && matches!(plan.label, ir::ExpandLabelPlan::Label(ref label) if label.as_ref() == "LIKES")
    ));
}

#[test]
fn pipeline_op_contract_covers_filter_bound_order_and_variable_families() {
    [
        AstNode::EdgeHas {
            input: input(),
            property: "active".to_owned(),
            value: PropertyInput::from(true),
        },
        AstNode::HasKey {
            input: input(),
            property: "email".to_owned(),
        },
    ]
    .into_iter()
    .for_each(|root| {
        let parsed = pipeline_op(&root);
        assert!(matches!(
            parsed.into_parts().1,
            logical::StreamPipelineOp::Filter { .. }
        ));
    });

    let limit_root = AstNode::Limit {
        input: input(),
        count: StreamBound::expr(Expr::param("limit")),
    };
    let limit = pipeline_op(&limit_root);
    assert!(matches!(
        limit.into_parts().1,
        logical::StreamPipelineOp::Limit {
            count: ir::StreamBoundPlan::Expr(_)
        }
    ));

    let order_root = AstNode::OrderByMultiple {
        input: input(),
        orderings: vec![
            ("age".to_owned(), Order::Asc),
            ("name".to_owned(), Order::Desc),
        ],
    };
    let order = pipeline_op(&order_root);
    assert!(matches!(
        order.into_parts().1,
        logical::StreamPipelineOp::Order { ordering }
            if ordering.as_ref().len() == 2
    ));

    let within_root = AstNode::Within {
        input: input(),
        variable: "allowed".to_owned(),
    };
    let within = pipeline_op(&within_root);
    assert!(matches!(
        within.into_parts().1,
        logical::StreamPipelineOp::Variable {
            op: logical::PureStreamVariableOp::Within(variable)
        } if variable.as_ref() == "allowed"
    ));

    let store_root = AstNode::Store {
        input: input(),
        name: "saved".to_owned(),
    };
    let store = pipeline_op(&store_root);
    assert!(matches!(
        store.into_parts().1,
        logical::StreamPipelineOp::VariableWrite {
            op: logical::StreamVariableWriteOp::Store(variable)
        } if variable.as_ref() == "saved"
    ));
}

#[test]
fn pipeline_op_contract_rejects_non_pipeline_roots_and_invalid_payloads() {
    assert!(matches!(
        parse(&AstNode::Context).unwrap(),
        NativePipelineRoot::NotPipeline
    ));
    assert!(parse(&AstNode::Inject {
        input: None,
        variable: "seed".to_owned(),
    })
    .is_ok_and(|root| matches!(root, NativePipelineRoot::NotPipeline)));

    assert!(matches!(
        parse(&AstNode::Out {
            input: input(),
            label: Some(String::new()),
        }),
        Err(error::PlannerError::InvalidEmptyName {
            field: ir::NameField::Label
        })
    ));
    assert!(matches!(
        parse(&AstNode::Limit {
            input: input(),
            count: StreamBound::expr(Expr::val(-1)),
        }),
        Err(error::PlannerError::InvalidStreamBoundExpression { .. })
    ));
    assert!(matches!(
        parse(&AstNode::Within {
            input: input(),
            variable: String::new(),
        }),
        Err(error::PlannerError::InvalidEmptyName {
            field: ir::NameField::Variable
        })
    ));
}
