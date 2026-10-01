pub(super) use std::collections::{BTreeMap, BTreeSet};

pub(super) use helix_ast::batch::{BatchEntry, NamedQuery, ReadBatch};
pub(super) use helix_ast::expr::{Expr, Predicate, StreamBound};
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

pub(super) use helix_planner::catalog;

use super::super::filter::RECORD_BATCH_ROWS;

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

/// Repetitions of [`traversal_pattern`] that make [`traversal_rows`] span
/// [`TRAVERSAL_BATCHES`] record batches for the read-count assertions.
pub(super) const TRAVERSAL_REPEATS: usize = RECORD_BATCH_ROWS / 8 + 1;

/// Stored-record batches spanned by [`traversal_rows`]; each batch reads a
/// distinct element's record at most once.
pub(super) const TRAVERSAL_BATCHES: usize = 2;

pub(super) struct Fixture {
    pub(super) db: crate::HelixDB,
    /// `Attribute` nodes with kind `B`, `A`, and no kind. Titles contain `x`
    /// on every node except `attribute_b_plain` and `note_b`. Every
    /// `Attribute` has a distinct `uid` (`a1` to `a4` in field order), and
    /// `attribute_b` and `attribute_a` have status `on` and `off`.
    pub(super) attribute_b: u64,
    pub(super) attribute_a: u64,
    pub(super) attribute_none: u64,
    /// `Attribute` node with kind `B` whose title has no `x`. Only the
    /// membership tests' `fused_residual_reads_each_record_once` streams it.
    pub(super) attribute_b_plain: u64,
    /// Nodes of other labels with kind `B` and `A`.
    pub(super) note_b: u64,
    pub(super) note_a: u64,
    pub(super) group: u64,
    /// Edges with kind `B` and `A`.
    pub(super) edge_b: u64,
    pub(super) edge_a: u64,
}

pub(super) async fn fixture(name: &str) -> Fixture {
    let db = test_support::open_db_with_config(
        test_support::in_memory_config(name)
            .with_equality_index("Attribute", "kind")
            .with_equality_index("Attribute", "status")
            .with_unique_equality_index("Attribute", "uid")
            .with_range_index("Attribute", "rank"),
    )
    .await;
    let node = |label: &'static str,
                kind: Option<&'static str>,
                rank: i64,
                title: &'static str,
                extra: Vec<(&'static str, &'static str)>| {
        let db = &db;
        async move {
            let mut properties = vec![
                ("rank", PropertyValue::I64(rank)),
                ("title", PropertyValue::from(title)),
            ];
            properties.extend(kind.map(|kind| ("kind", PropertyValue::from(kind))));
            properties.extend(
                extra
                    .into_iter()
                    .map(|(name, value)| (name, PropertyValue::from(value))),
            );
            test_support::add_node_with_properties(db, label, properties).await
        }
    };
    let attribute_b = node(
        "Attribute",
        Some("B"),
        1,
        "xb",
        vec![("uid", "a1"), ("status", "on")],
    )
    .await;
    let attribute_a = node(
        "Attribute",
        Some("A"),
        5,
        "xa",
        vec![("uid", "a2"), ("status", "off")],
    )
    .await;
    let attribute_none = node("Attribute", None, 9, "x", vec![("uid", "a3")]).await;
    let attribute_b_plain = node("Attribute", Some("B"), 2, "plain", vec![("uid", "a4")]).await;
    let note_b = node("Note", Some("B"), 1, "note", Vec::new()).await;
    let note_a = node("Note", Some("A"), 5, "xn", Vec::new()).await;
    let group = node("Group", None, 0, "xg", Vec::new()).await;
    let edge_b = test_support::add_edge_with_properties(
        &db,
        group,
        attribute_b,
        "LINK",
        vec![("kind", PropertyValue::from("B"))],
    )
    .await;
    let edge_a = test_support::add_edge_with_properties(
        &db,
        group,
        attribute_a,
        "LINK",
        vec![("kind", PropertyValue::from("A"))],
    )
    .await;
    Fixture {
        db,
        attribute_b,
        attribute_a,
        attribute_none,
        attribute_b_plain,
        note_b,
        note_a,
        group,
        edge_b,
        edge_a,
    }
}

pub(super) fn kind_equality(value: ir::IndexValue) -> ir::NodeAccessSourcePlan {
    attribute_equality("kind", catalog::IndexUniqueness::NonUnique, value)
}

/// Equality set on an indexed `Attribute` property.
pub(super) fn attribute_equality(
    property: &str,
    uniqueness: catalog::IndexUniqueness,
    value: ir::IndexValue,
) -> ir::NodeAccessSourcePlan {
    let key = catalog::ScopedPropertyKey::try_new("Attribute", property).unwrap();
    ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::EqualityIndex {
        index: catalog::IndexCatalogSnapshot::default()
            .with_node_eq(key.clone())
            .node_eq[&key]
            .clone()
            .with_uniqueness(uniqueness),
        key,
        value,
    })
    .unwrap()
}

pub(super) fn literal(value: impl Into<helix_ast::value::PropertyValue>) -> ir::IndexValue {
    ir::IndexValue::Literal(ir::SecondaryIndexLiteral::new(value.into()).unwrap())
}

pub(super) fn membership(set: ir::NodeAccessSourcePlan, predicate: Predicate) -> exec::ExecOp {
    exec::ExecOp::IndexMembership {
        plan: Box::new(exec::ExecNodeIndexMembershipPlan::from(
            &ir::NodeIndexMembershipPlan::new(
                set,
                ir::PredicatePlan::new(predicate).unwrap(),
                None,
            )
            .unwrap(),
        )),
    }
}

/// Membership of the whole `predicate` whose set matches evaluate `residual`.
pub(super) fn fused(
    set: ir::NodeAccessSourcePlan,
    predicate: Predicate,
    residual: Predicate,
) -> exec::ExecOp {
    exec::ExecOp::IndexMembership {
        plan: Box::new(exec::ExecNodeIndexMembershipPlan::from(
            &ir::NodeIndexMembershipPlan::new(
                set,
                ir::PredicatePlan::new(predicate).unwrap(),
                Some(ir::PredicatePlan::new(residual).unwrap()),
            )
            .unwrap(),
        )),
    }
}

/// Membership decided by the `$label` bitmaps of `predicate`'s label domain.
pub(super) fn label_membership(predicate: Predicate, residual: Option<Predicate>) -> exec::ExecOp {
    exec::ExecOp::IndexMembership {
        plan: Box::new(exec::ExecNodeIndexMembershipPlan::from(
            &ir::NodeIndexMembershipPlan::labels(
                ir::PredicatePlan::new(predicate).unwrap(),
                residual.map(|residual| ir::PredicatePlan::new(residual).unwrap()),
            )
            .unwrap(),
        )),
    }
}

pub(super) fn filter(predicate: Predicate) -> exec::ExecOp {
    exec::ExecOp::Filter {
        predicate: ir::PredicatePlan::new(predicate).unwrap(),
    }
}

/// Rows behind a traversal: every node row carries a path through `group`, a
/// binding, and a sack, and the stream repeats elements.
pub(super) fn traversal_pattern(fixture: &Fixture) -> Vec<ExecutionRow> {
    let node = |id| {
        let mut row = ExecutionRow::current(ElementRef::Node(fixture.group));
        row.bindings
            .insert(name("group"), ElementRef::Node(fixture.group));
        row.set_current(ElementRef::Node(id));
        row.set_sack(DbPropertyValue::I64(id as i64));
        row
    };
    vec![
        node(fixture.attribute_b),
        node(fixture.note_b),
        node(fixture.attribute_a),
        node(fixture.attribute_none),
        node(fixture.note_a),
        node(fixture.group),
        node(fixture.attribute_b),
        ExecutionRow::current(ElementRef::Edge(fixture.edge_b)),
        ExecutionRow::current(ElementRef::Edge(fixture.edge_a)),
        ExecutionRow::empty(),
        node(fixture.note_b),
    ]
}

/// [`traversal_pattern`] repeated across [`TRAVERSAL_BATCHES`] record batches.
pub(super) fn traversal_rows(fixture: &Fixture) -> Vec<ExecutionRow> {
    repeated(traversal_pattern(fixture))
}

/// `pattern` repeated [`TRAVERSAL_REPEATS`] times, spanning
/// [`TRAVERSAL_BATCHES`] record batches.
pub(super) fn repeated(pattern: Vec<ExecutionRow>) -> Vec<ExecutionRow> {
    let rows = pattern
        .iter()
        .cycle()
        .take(pattern.len() * TRAVERSAL_REPEATS)
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(rows.len().div_ceil(RECORD_BATCH_ROWS), TRAVERSAL_BATCHES);
    rows
}

/// Membership sets `db` resolved from secondary indexes. The per-row
/// fallback keeps the same rows, so only this count shows a set was read.
pub(super) fn resolved(db: &crate::HelixDB) -> usize {
    db.inner
        .resolved_index_memberships
        .load(std::sync::atomic::Ordering::Relaxed)
}
