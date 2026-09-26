//! Node index membership and batched residual filter contracts.
//!
//! Each membership result is compared with the per-row filter over the same
//! input, so the index path can only change how rows are decided, never which
//! rows survive.

use helix_ast::expr::Predicate;
use helix_planner::catalog;

use super::super::super::ExecutionContext;
use super::support::*;

struct Fixture {
    db: crate::HelixDB,
    /// `Attribute` nodes with kind `B`, `A`, and no kind.
    attribute_b: u64,
    attribute_a: u64,
    attribute_none: u64,
    /// Nodes of other labels with kind `B` and `A`.
    note_b: u64,
    note_a: u64,
    group: u64,
    /// Edges with kind `B` and `A`.
    edge_b: u64,
    edge_a: u64,
}

async fn fixture(name: &str) -> Fixture {
    let db = test_support::open_db_with_config(
        test_support::in_memory_config(name)
            .with_equality_index("Attribute", "kind")
            .with_range_index("Attribute", "rank"),
    )
    .await;
    let node = |label: &'static str, kind: Option<&'static str>, rank: i64| {
        let db = &db;
        async move {
            let mut properties = vec![("rank", PropertyValue::I64(rank))];
            properties.extend(kind.map(|kind| ("kind", PropertyValue::from(kind))));
            test_support::add_node_with_properties(db, label, properties).await
        }
    };
    let attribute_b = node("Attribute", Some("B"), 1).await;
    let attribute_a = node("Attribute", Some("A"), 5).await;
    let attribute_none = node("Attribute", None, 9).await;
    let note_b = node("Note", Some("B"), 1).await;
    let note_a = node("Note", Some("A"), 5).await;
    let group = node("Group", None, 0).await;
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
        note_b,
        note_a,
        group,
        edge_b,
        edge_a,
    }
}

fn kind_equality(value: ir::IndexValue) -> ir::NodeAccessSourcePlan {
    let key = catalog::ScopedPropertyKey::try_new("Attribute", "kind").unwrap();
    ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::EqualityIndex {
        index: catalog::IndexCatalogSnapshot::default()
            .with_node_eq(key.clone())
            .node_eq[&key]
            .clone(),
        key,
        value,
    })
    .unwrap()
}

fn literal(value: impl Into<helix_ast::value::PropertyValue>) -> ir::IndexValue {
    ir::IndexValue::Literal(ir::SecondaryIndexLiteral::new(value.into()).unwrap())
}

fn membership(set: ir::NodeAccessSourcePlan, predicate: Predicate) -> exec::ExecOp {
    exec::ExecOp::IndexMembership {
        plan: Box::new(exec::ExecNodeIndexMembershipPlan::from(
            &ir::NodeIndexMembershipPlan::new(set, ir::PredicatePlan::new(predicate).unwrap())
                .unwrap(),
        )),
    }
}

fn filter(predicate: Predicate) -> exec::ExecOp {
    exec::ExecOp::Filter {
        predicate: ir::PredicatePlan::new(predicate).unwrap(),
    }
}

/// Rows behind a traversal: every node row carries a path through `group`, a
/// binding, and a sack, and the stream repeats elements.
fn traversal_rows(fixture: &Fixture) -> Vec<ExecutionRow> {
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

async fn run(
    fixture: &Fixture,
    op: &exec::ExecOp,
    input: Vec<ExecutionRow>,
    params: context::ParamBindings,
) -> (crate::Result<ExecutionValue>, usize) {
    let mut ctx = ExecutionContext::new(&fixture.db, params);
    ctx.enable_request_read_view().await.unwrap();
    let result = ctx.execute_op(op, ExecutionValue::Stream(input)).await;
    let reads = ctx.projection_read_snapshot().property_gets;
    ctx.close_request_read_view().unwrap();
    (result, reads)
}

/// Run membership and the per-row filter it replaces, assert identical rows,
/// and return the stored-record reads of the membership run.
async fn assert_matches_filter(
    fixture: &Fixture,
    membership: &exec::ExecOp,
    predicate: Predicate,
    input: Vec<ExecutionRow>,
    params: context::ParamBindings,
) -> (Vec<ExecutionRow>, usize) {
    let (expected, _) = run(fixture, &filter(predicate), input.clone(), params.clone()).await;
    let (actual, reads) = run(fixture, membership, input, params).await;
    let expected = expected.unwrap();
    assert_eq!(actual.unwrap(), expected);
    let ExecutionValue::Stream(rows) = expected else {
        panic!("filter returns rows");
    };
    (rows, reads)
}

fn current_ids(rows: &[ExecutionRow]) -> Vec<Option<ElementRef>> {
    rows.iter().map(|row| row.current.clone()).collect()
}

#[tokio::test]
async fn membership_matches_filter_on_mixed_streams_without_label_record_reads() {
    let fixture = fixture("membership-mixed-streams").await;
    let predicate = Predicate::eq("kind", "B");
    let (rows, reads) = assert_matches_filter(
        &fixture,
        &membership(kind_equality(literal("B")), predicate.clone()),
        predicate,
        traversal_rows(&fixture),
        context::ParamBindings::default(),
    )
    .await;

    assert_eq!(
        current_ids(&rows),
        [
            Some(ElementRef::Node(fixture.attribute_b)),
            Some(ElementRef::Node(fixture.note_b)),
            Some(ElementRef::Node(fixture.attribute_b)),
            Some(ElementRef::Edge(fixture.edge_b)),
            Some(ElementRef::Node(fixture.note_b)),
        ]
    );
    assert!(rows[0].bindings.contains_key(&name("group")));
    assert_eq!(rows[0].path.elements().len(), 2);
    // Only the other-label nodes and the edges read records, once each.
    assert_eq!(reads, 5);

    let (_, reads) = assert_matches_filter(
        &fixture,
        &membership(kind_equality(literal("B")), Predicate::eq("kind", "B")),
        Predicate::eq("kind", "B"),
        [
            fixture.attribute_b,
            fixture.attribute_a,
            fixture.attribute_none,
        ]
        .into_iter()
        .cycle()
        .take(600)
        .map(|id| ExecutionRow::current(ElementRef::Node(id)))
        .collect(),
        context::ParamBindings::default(),
    )
    .await;
    assert_eq!(reads, 0);
}

#[tokio::test]
async fn label_scoped_membership_rejects_other_nodes_without_reading_them() {
    let fixture = fixture("membership-label-scoped").await;
    let predicate = Predicate::and(vec![
        Predicate::eq("$label", "Attribute"),
        Predicate::eq("kind", "B"),
    ]);
    let (rows, reads) = assert_matches_filter(
        &fixture,
        &membership(kind_equality(literal("B")), predicate.clone()),
        predicate,
        traversal_rows(&fixture),
        context::ParamBindings::default(),
    )
    .await;

    assert_eq!(
        current_ids(&rows),
        [
            Some(ElementRef::Node(fixture.attribute_b)),
            Some(ElementRef::Node(fixture.attribute_b)),
        ]
    );
    // Only the two edges evaluate the predicate.
    assert_eq!(reads, 2);
}

#[tokio::test]
async fn membership_sets_for_in_and_range_match_filter() {
    let fixture = fixture("membership-in-range").await;
    let is_in = Predicate::is_in(
        "kind",
        helix_ast::value::PropertyValue::StringArray(vec!["A".into(), "B".into()]),
    );
    let union = ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::Union(
        ir::AtLeast::<_, 2>::from_pair(kind_equality(literal("A")), kind_equality(literal("B"))),
    ))
    .unwrap();
    let (rows, _) = assert_matches_filter(
        &fixture,
        &membership(union, is_in.clone()),
        is_in,
        traversal_rows(&fixture),
        context::ParamBindings::default(),
    )
    .await;
    assert_eq!(rows.len(), 8);

    let key = catalog::ScopedPropertyDirectionKey::try_new(
        "Attribute",
        "rank",
        helix_ast::index::RangeIndexDirection::Asc,
    )
    .unwrap();
    let range = ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::RangeIndex {
        index: catalog::IndexCatalogSnapshot::default()
            .with_node_range(key.clone())
            .node_range[&key]
            .clone(),
        key,
        range: ir::IndexRange::Lower {
            lower: ir::IndexBound::Inclusive(ir::RangeIndexValue::literal(5_i64.into()).unwrap()),
        },
        iteration: ir::RangeScanIteration::Forward,
    })
    .unwrap();
    let predicate = Predicate::gte("rank", 5);
    let (rows, reads) = assert_matches_filter(
        &fixture,
        &membership(range, predicate.clone()),
        predicate,
        traversal_rows(&fixture),
        context::ParamBindings::default(),
    )
    .await;
    assert_eq!(
        current_ids(&rows),
        [
            Some(ElementRef::Node(fixture.attribute_a)),
            Some(ElementRef::Node(fixture.attribute_none)),
            Some(ElementRef::Node(fixture.note_a)),
        ]
    );
    assert_eq!(reads, 5);
}

#[tokio::test]
async fn runtime_parameters_needing_authoritative_scans_fall_back_to_rows() {
    let fixture = fixture("membership-runtime-params").await;
    // Unreached label nodes with a null kind would be read by an authoritative
    // scan; per-row fallback never reads them.
    for _ in 0..4 {
        test_support::add_node_with_properties(&fixture.db, "Attribute", Vec::new()).await;
    }
    let param = name("kind");
    let equality = membership(
        kind_equality(ir::IndexValue::Param(param.clone())),
        Predicate::eq_param("kind", "kind"),
    );
    let bind = |value: helix_ast::value::PropertyValue| {
        context::ParamBindings::default().with_value(param.clone(), value)
    };
    let (_, indexed_reads) = assert_matches_filter(
        &fixture,
        &equality,
        Predicate::eq_param("kind", "kind"),
        traversal_rows(&fixture),
        bind("B".into()),
    )
    .await;
    assert_eq!(indexed_reads, 5);
    for value in [
        helix_ast::value::PropertyValue::Null,
        helix_ast::value::PropertyValue::array([helix_ast::value::PropertyValue::from("B")]),
    ] {
        let (_, reads) = assert_matches_filter(
            &fixture,
            &equality,
            Predicate::eq_param("kind", "kind"),
            traversal_rows(&fixture),
            bind(value),
        )
        .await;
        // Every distinct stream element, never the unreached label nodes.
        assert_eq!(reads, 8);
    }
    let (_, nan_reads) = assert_matches_filter(
        &fixture,
        &equality,
        Predicate::eq_param("kind", "kind"),
        traversal_rows(&fixture),
        bind(helix_ast::value::PropertyValue::F64(f64::NAN)),
    )
    .await;
    assert_eq!(nan_reads, 5);

    let kinds = name("kinds");
    let domain = membership(
        kind_equality(ir::IndexValue::ParamSet(ir::RuntimeEqualitySet::new(
            kinds.clone(),
            std::num::NonZeroUsize::new(2).unwrap(),
        ))),
        Predicate::is_in_param("kind", "kinds"),
    );
    for (values, reads) in [(vec!["A", "B"], 5), (vec!["A", "B", "C"], 8)] {
        let (_, actual) = assert_matches_filter(
            &fixture,
            &domain,
            Predicate::is_in_param("kind", "kinds"),
            traversal_rows(&fixture),
            context::ParamBindings::default().with_value(
                kinds.clone(),
                helix_ast::value::PropertyValue::StringArray(
                    values.into_iter().map(str::to_owned).collect(),
                ),
            ),
        )
        .await;
        assert_eq!(actual, reads);
    }
    let (_, reads) = assert_matches_filter(
        &fixture,
        &domain,
        Predicate::is_in_param("kind", "kinds"),
        traversal_rows(&fixture),
        context::ParamBindings::default().with_value(
            kinds,
            helix_ast::value::PropertyValue::array([
                helix_ast::value::PropertyValue::from("B"),
                helix_ast::value::PropertyValue::Null,
            ]),
        ),
    )
    .await;
    assert_eq!(reads, 8);

    let (missing, _) = run(
        &fixture,
        &equality,
        traversal_rows(&fixture),
        context::ParamBindings::default(),
    )
    .await;
    assert!(missing.is_err());
}

#[tokio::test]
async fn unavailable_indexes_fall_back_and_corrupt_identities_fail_closed() {
    let fixture = fixture("membership-unavailable-index").await;
    let key = catalog::ScopedPropertyKey::try_new("Attribute", "color").unwrap();
    let unindexed = ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::EqualityIndex {
        index: catalog::IndexCatalogSnapshot::default()
            .with_node_eq(key.clone())
            .node_eq[&key]
            .clone(),
        key,
        value: literal("red"),
    })
    .unwrap();
    let (_, reads) = assert_matches_filter(
        &fixture,
        &membership(unindexed, Predicate::eq("color", "red")),
        Predicate::eq("color", "red"),
        traversal_rows(&fixture),
        context::ParamBindings::default(),
    )
    .await;
    assert_eq!(reads, 8);

    let corrupt = ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::EqualityIndex {
        index: catalog::NodeEqualityIndexMeta::try_new("not-a-planner-identity").unwrap(),
        key: catalog::ScopedPropertyKey::try_new("Attribute", "kind").unwrap(),
        value: literal("B"),
    })
    .unwrap();
    let corrupt = membership(corrupt, Predicate::eq("kind", "B"));
    let (error, _) = run(
        &fixture,
        &corrupt,
        traversal_rows(&fixture),
        context::ParamBindings::default(),
    )
    .await;
    assert!(matches!(
        error,
        Err(crate::error::HelixDbError::IndexCatalogCorruption(_))
    ));

    // Streams without node rows never resolve the (corrupt) set.
    for input in [
        Vec::new(),
        vec![
            ExecutionRow::current(ElementRef::Edge(fixture.edge_b)),
            ExecutionRow::empty(),
        ],
    ] {
        let expected = input
            .iter()
            .filter(|row| row.current.is_some())
            .cloned()
            .collect::<Vec<_>>();
        let (result, _) = run(&fixture, &corrupt, input, context::ParamBindings::default()).await;
        assert_eq!(result.unwrap(), ExecutionValue::Stream(expected));
    }
}

#[tokio::test]
async fn pull_regions_and_count_cursors_share_membership_semantics() {
    let fixture = fixture("membership-pull-count").await;
    let ids = [
        fixture.attribute_a,
        fixture.note_b,
        fixture.attribute_b,
        fixture.attribute_b,
        fixture.note_a,
    ];
    let params = context::ParamBindings::default().with_value(name("ids"), ids_value(&ids));
    let predicate = Predicate::eq("kind", "B");
    let op = membership(kind_equality(literal("B")), predicate.clone());
    for (terminal, expected) in [
        (
            exec::ExecOp::Limit {
                count: ir::StreamBoundPlan::Literal(2),
            },
            ExecutionValue::Stream(vec![
                ExecutionRow::current(ElementRef::Node(fixture.note_b)),
                ExecutionRow::current(ElementRef::Node(fixture.attribute_b)),
            ]),
        ),
        (
            exec::ExecOp::Count {
                plan: Box::new(exec::ExecCountPlan::InputRows {
                    window: exec::ExecCountWindowPlan::identity(),
                }),
            },
            ExecutionValue::Count(3),
        ),
    ] {
        let plan = test_support::executable(
            ir::PlanKind::Read,
            vec![
                node_access_step(1, name("ids")),
                test_support::step(2, vec![exec::ExecStepId::new(1).unwrap()], op.clone()),
                test_support::step(3, vec![exec::ExecStepId::new(2).unwrap()], terminal),
            ],
            3,
        );
        let result = fixture.db.execute(&plan, params.clone()).await.unwrap();
        assert_eq!(result.last, Some(expected));
    }

    let exec::ExecOp::IndexMembership { plan } = op else {
        unreachable!("membership helper builds membership");
    };
    let count = exec::ExecCountPlan::Stream(exec::ExecCountStreamPlan {
        cursor: exec::ExecCountCursorPlan::IndexMembership {
            input: Box::new(exec::ExecCountCursorPlan::NodeRuntimeInput(
                exec::ExecRuntimeInputPlan::Param(name("ids")),
            )),
            plan,
        },
        window: exec::ExecCountWindowPlan::identity(),
    });
    let plan = test_support::executable(
        ir::PlanKind::Read,
        vec![test_support::step(
            1,
            Vec::new(),
            exec::ExecOp::Count {
                plan: Box::new(count),
            },
        )],
        1,
    );
    let result = fixture.db.execute(&plan, params).await.unwrap();
    assert_eq!(result.last, Some(ExecutionValue::Count(3)));
}

#[tokio::test]
async fn filter_batches_record_reads_once_per_distinct_element() {
    let fixture = fixture("filter-batched-reads").await;
    let input = [fixture.attribute_b, fixture.note_b, fixture.attribute_a]
        .into_iter()
        .cycle()
        .take(700)
        .map(|id| ExecutionRow::current(ElementRef::Node(id)))
        .collect::<Vec<_>>();
    let mut ctx = ExecutionContext::new(&fixture.db, context::ParamBindings::default());
    ctx.enable_request_read_view().await.unwrap();
    let rows = ctx
        .execute_op(
            &filter(Predicate::eq("kind", "B")),
            ExecutionValue::Stream(input.clone()),
        )
        .await
        .unwrap();
    let ExecutionValue::Stream(rows) = rows else {
        panic!("filter returns rows");
    };
    assert_eq!(rows.len(), 467);
    // Three batches, each reading the three distinct records once.
    assert_eq!(ctx.projection_read_snapshot().property_gets, 9);
    assert_eq!(ctx.pull_work.snapshot().multi_get_keys, 9);
    assert_eq!(ctx.pull_work.snapshot().raw_gets, 0);
    ctx.close_request_read_view().unwrap();

    // A leading row-local conjunct keeps records unread for rows it rejects.
    let mut ctx = ExecutionContext::new(&fixture.db, context::ParamBindings::default());
    ctx.enable_request_read_view().await.unwrap();
    ctx.execute_op(
        &filter(Predicate::and(vec![
            Predicate::eq("$id", fixture.note_b as i64),
            Predicate::eq("kind", "B"),
        ])),
        ExecutionValue::Stream(input[..3].to_vec()),
    )
    .await
    .unwrap();
    assert_eq!(ctx.projection_read_snapshot().property_gets, 1);
    assert_eq!(ctx.pull_work.snapshot().multi_get_keys, 0);
    ctx.close_request_read_view().unwrap();
}
