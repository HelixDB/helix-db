pub(super) use std::collections::{BTreeMap, BTreeSet};

pub(super) use helix_ast::batch::{BatchEntry, NamedQuery, ReadBatch};
pub(super) use helix_ast::expr::{Expr, StreamBound};
pub(super) use helix_ast::graph::NodeRef;
pub(super) use helix_ast::traversal::{AggregateFunction, AstNode, Order};
pub(super) use helix_ast::value::PropertyValue;
pub(super) use helix_planner::{context, exec, ir, planning};

pub(super) use super::super::super::test_support;
pub(super) use super::super::super::{ElementRef, ExecutionRow, ExecutionScalar, ExecutionValue};
pub(super) use super::super::bounds::{eval_stream_bound, limit_rows, skip_rows, slice_rows};
pub(super) use super::super::sets::{
    bind_rows, distinct_rows, filter_within_rows, filter_without_rows, merge_streams,
};
pub(super) use crate::encoding::property::property_value::PropertyValue as DbPropertyValue;

pub(super) fn name(value: &str) -> ir::NonEmptyString {
    ir::NonEmptyString::new(value).expect("valid test name")
}

pub(super) fn row(id: u64) -> ExecutionRow {
    ExecutionRow::current(ElementRef::Node(id))
}

pub(super) fn rows(ids: &[u64]) -> Vec<ExecutionRow> {
    ids.iter().copied().map(row).collect()
}

pub(super) fn row_ids(rows: Vec<ExecutionRow>) -> Vec<u64> {
    rows.into_iter()
        .map(|row| match row.current.expect("row current element") {
            ElementRef::Node(id) => id,
            ElementRef::Edge(id) => panic!("expected node row, got edge {id}"),
        })
        .collect()
}

pub(super) fn ids_value(ids: &[u64]) -> PropertyValue {
    PropertyValue::I64Array(ids.iter().map(|id| *id as i64).collect())
}

pub(super) fn property_names(names: Vec<&str>) -> ir::PropertyNames {
    ir::PropertyNames::new(
        ir::AtLeast::<_, 1>::try_from_vec(names.into_iter().map(name).collect())
            .expect("test property list is non-empty"),
    )
    .expect("test property list has unique names")
}

pub(super) fn binding_projection_items(
    items: Vec<ir::BindingProjectionPlan>,
) -> ir::BindingProjectionItems {
    ir::BindingProjectionItems::new(
        ir::AtLeast::<_, 1>::try_from_vec(items)
            .expect("test binding projection list is non-empty"),
    )
    .expect("test binding projection aliases are unique")
}

pub(super) fn binding_refs(
    items: Vec<ir::BindingValueRefPlan>,
) -> ir::AtLeast<ir::BindingValueRefPlan, 1> {
    ir::AtLeast::<_, 1>::try_from_vec(items).expect("test binding refs are non-empty")
}

pub(super) fn order_keys(property: &str, order: Order) -> ir::OrderKeys {
    ir::OrderKeys::new(ir::AtLeast::<_, 1>::from_one(ir::OrderKey {
        property: name(property),
        order,
    }))
    .expect("test order keys are unique")
}

pub(super) fn node_access_step(id: usize, param: ir::NonEmptyString) -> exec::ExecStep {
    test_support::step(
        id,
        Vec::new(),
        exec::ExecOp::Access {
            plan: Box::new(exec::ExecAccessPlan::Node(
                exec::ExecNodeAccessPlan::FromParam { param },
            )),
        },
    )
}

pub(super) fn edge_access_step(id: usize, param: ir::NonEmptyString) -> exec::ExecStep {
    test_support::step(
        id,
        Vec::new(),
        exec::ExecOp::Access {
            plan: Box::new(exec::ExecAccessPlan::Edge(
                exec::ExecEdgeAccessPlan::FromParam { param },
            )),
        },
    )
}
