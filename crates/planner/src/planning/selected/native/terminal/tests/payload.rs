use super::super::{NativeTerminalPayload, NativeTerminalRoot};
use super::support;
use crate::{error, ir};
use helix_ast::projection::{BindingProjection, Projection};
use helix_ast::traversal::{AggregateFunction, AstNode};
use helix_ast::value::PropertyValue;

#[test]
fn terminal_payload_contract_preserves_input_and_payload_family() {
    let context = || Box::new(AstNode::Context);
    let payloads = [
        AstNode::Count { input: context() },
        AstNode::Exists { input: context() },
        AstNode::Id { input: context() },
        AstNode::Label { input: context() },
        AstNode::Values {
            input: context(),
            properties: vec!["name".to_owned()],
        },
        AstNode::ValueMap {
            input: context(),
            properties: Some(vec!["name".to_owned()]),
        },
        AstNode::Project {
            input: context(),
            projections: vec![Projection::property("name", "display")],
        },
        AstNode::ProjectBindings {
            input: context(),
            projections: vec![BindingProjection::current("$id", "id")],
            distinct: false,
        },
        AstNode::EdgeProperties { input: context() },
        AstNode::Group {
            input: context(),
            property: "kind".to_owned(),
        },
        AstNode::GroupCount {
            input: context(),
            property: "kind".to_owned(),
        },
        AstNode::AggregateBy {
            input: context(),
            function: AggregateFunction::Count,
            property: "kind".to_owned(),
        },
        AstNode::Fold { input: context() },
        AstNode::Unfold { input: context() },
        AstNode::Path { input: context() },
        AstNode::SimplePath { input: context() },
        AstNode::WithSack {
            input: context(),
            initial: PropertyValue::from(1),
        },
        AstNode::SackSet {
            input: context(),
            property: "score".to_owned(),
        },
        AstNode::SackAdd {
            input: context(),
            property: "score".to_owned(),
        },
        AstNode::SackGet { input: context() },
    ];

    payloads.into_iter().for_each(|root| {
        let terminal_op = support::terminal_payload(&root).unwrap();
        let (input, payload) = terminal_op.into_parts();
        assert!(matches!(input, AstNode::Context));
        assert!(matches!(
            payload,
            NativeTerminalPayload::Project(_)
                | NativeTerminalPayload::Aggregate(_)
                | NativeTerminalPayload::Reserved(_)
        ));
    });
}

#[test]
fn terminal_payload_contract_preserves_projection_details() {
    let root = AstNode::ProjectBindings {
        input: Box::new(AstNode::Context),
        projections: vec![BindingProjection::current("$id", "id")],
        distinct: true,
    };
    let terminal_op = support::terminal_payload(&root).unwrap();
    let (input, payload) = terminal_op.into_parts();

    assert!(matches!(input, AstNode::Context));
    assert!(matches!(
        payload,
        NativeTerminalPayload::Project(ir::ProjectionPlan::ProjectBindings {
            dedup: ir::ProjectionDedupMode::Distinct,
            ..
        })
    ));
}

#[test]
fn terminal_payload_contract_preserves_aggregate_and_reserved_details() {
    let aggregate = AstNode::AggregateBy {
        input: Box::new(AstNode::Context),
        function: AggregateFunction::Sum,
        property: "score".to_owned(),
    };
    let aggregate_payload = support::terminal_payload(&aggregate)
        .unwrap()
        .into_parts()
        .1;
    assert!(matches!(
        aggregate_payload,
        NativeTerminalPayload::Aggregate(ir::AggregatePlan::AggregateBy {
            function: AggregateFunction::Sum,
            property,
        }) if property.as_ref() == "score"
    ));

    let reserved = AstNode::SackAdd {
        input: Box::new(AstNode::Context),
        property: "score".to_owned(),
    };
    let reserved_payload = support::terminal_payload(&reserved).unwrap().into_parts().1;
    assert!(matches!(
        reserved_payload,
        NativeTerminalPayload::Reserved(ir::ReservedOp::SackAdd(property))
            if property.as_ref() == "score"
    ));
}

#[test]
fn terminal_payload_contract_rejects_non_terminals_and_invalid_payloads() {
    assert!(matches!(
        support::payload(&AstNode::Context).unwrap(),
        NativeTerminalRoot::NotTerminal
    ));

    let empty_values_ast = AstNode::Values {
        input: Box::new(AstNode::Context),
        properties: Vec::new(),
    };
    let empty_values = support::payload(&empty_values_ast);
    assert!(matches!(
        empty_values,
        Err(error::PlannerError::InvalidProjectionArity {
            op: error::ProjectionOp::Values,
            min: 1,
            actual: 0
        })
    ));

    let empty_group_ast = AstNode::Group {
        input: Box::new(AstNode::Context),
        property: String::new(),
    };
    let empty_group = support::payload(&empty_group_ast);
    assert!(matches!(
        empty_group,
        Err(error::PlannerError::InvalidEmptyName {
            field: ir::NameField::Property
        })
    ));

    let empty_sack_ast = AstNode::SackSet {
        input: Box::new(AstNode::Context),
        property: String::new(),
    };
    let empty_sack = support::payload(&empty_sack_ast);
    assert!(matches!(
        empty_sack,
        Err(error::PlannerError::InvalidEmptyName {
            field: ir::NameField::Property
        })
    ));
}
