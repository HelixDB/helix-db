//! Node index membership and batched residual filter contracts.
//!
//! Each membership result is compared with the per-row filter over the same
//! input, so the index path can only change how rows are decided, never which
//! rows survive.

use helix_ast::expr::Predicate;
use helix_planner::catalog;

use super::super::super::ExecutionContext;
use super::super::RECORD_BATCH_ROWS;
use super::support::*;

/// Repetitions of [`traversal_pattern`] that make [`traversal_rows`] span
/// [`TRAVERSAL_BATCHES`] record batches for the read-count assertions.
const TRAVERSAL_REPEATS: usize = RECORD_BATCH_ROWS / 8 + 1;

/// Stored-record batches spanned by [`traversal_rows`]; each batch reads a
/// distinct element's record at most once.
const TRAVERSAL_BATCHES: usize = 2;

struct Fixture {
    db: crate::HelixDB,
    /// `Attribute` nodes with kind `B`, `A`, and no kind. Titles contain `x`
    /// on every node except `attribute_b_plain` and `note_b`.
    attribute_b: u64,
    attribute_a: u64,
    attribute_none: u64,
    /// `Attribute` node with kind `B` whose title has no `x`. Only
    /// [`fused_residual_reads_each_record_once`] streams it.
    attribute_b_plain: u64,
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
    let node = |label: &'static str, kind: Option<&'static str>, rank: i64, title: &'static str| {
        let db = &db;
        async move {
            let mut properties = vec![
                ("rank", PropertyValue::I64(rank)),
                ("title", PropertyValue::from(title)),
            ];
            properties.extend(kind.map(|kind| ("kind", PropertyValue::from(kind))));
            test_support::add_node_with_properties(db, label, properties).await
        }
    };
    let attribute_b = node("Attribute", Some("B"), 1, "xb").await;
    let attribute_a = node("Attribute", Some("A"), 5, "xa").await;
    let attribute_none = node("Attribute", None, 9, "x").await;
    let attribute_b_plain = node("Attribute", Some("B"), 2, "plain").await;
    let note_b = node("Note", Some("B"), 1, "note").await;
    let note_a = node("Note", Some("A"), 5, "xn").await;
    let group = node("Group", None, 0, "xg").await;
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
fn fused(set: ir::NodeAccessSourcePlan, predicate: Predicate, residual: Predicate) -> exec::ExecOp {
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
fn label_membership(predicate: Predicate, residual: Option<Predicate>) -> exec::ExecOp {
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

fn filter(predicate: Predicate) -> exec::ExecOp {
    exec::ExecOp::Filter {
        predicate: ir::PredicatePlan::new(predicate).unwrap(),
    }
}

/// Rows behind a traversal: every node row carries a path through `group`, a
/// binding, and a sack, and the stream repeats elements.
fn traversal_pattern(fixture: &Fixture) -> Vec<ExecutionRow> {
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
fn traversal_rows(fixture: &Fixture) -> Vec<ExecutionRow> {
    repeated(traversal_pattern(fixture))
}

/// `pattern` repeated [`TRAVERSAL_REPEATS`] times, spanning
/// [`TRAVERSAL_BATCHES`] record batches.
fn repeated(pattern: Vec<ExecutionRow>) -> Vec<ExecutionRow> {
    let rows = pattern
        .iter()
        .cycle()
        .take(pattern.len() * TRAVERSAL_REPEATS)
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(rows.len().div_ceil(RECORD_BATCH_ROWS), TRAVERSAL_BATCHES);
    rows
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

/// Membership sets `db` resolved from secondary indexes. The per-row
/// fallback keeps the same rows, so only this count shows a set was read.
fn resolved(db: &crate::HelixDB) -> usize {
    db.inner
        .resolved_index_memberships
        .load(std::sync::atomic::Ordering::Relaxed)
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

    assert_eq!(rows.len(), 5 * TRAVERSAL_REPEATS);
    assert_eq!(
        current_ids(&rows[..5]),
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
    // Only the other-label nodes and the edges read records, once per batch.
    assert_eq!(reads, 5 * TRAVERSAL_BATCHES);

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

    assert_eq!(rows.len(), 2 * TRAVERSAL_REPEATS);
    assert_eq!(
        current_ids(&rows[..2]),
        [
            Some(ElementRef::Node(fixture.attribute_b)),
            Some(ElementRef::Node(fixture.attribute_b)),
        ]
    );
    // Only the two edges evaluate the predicate.
    assert_eq!(reads, 2 * TRAVERSAL_BATCHES);
}

#[tokio::test]
async fn membership_sets_for_in_match_filter() {
    let fixture = fixture("membership-in").await;
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
    assert_eq!(rows.len(), 8 * TRAVERSAL_REPEATS);
}

/// Validated plans never carry a range set, but the executable contract is
/// serializable. A range set still evaluates every row instead of scanning
/// and verifying the label's range, and runtime bounds without a range
/// encoding keep the per-row result instead of failing the request.
#[tokio::test]
async fn range_sets_evaluate_rows_without_scanning_the_range() {
    let fixture = fixture("membership-range").await;
    let key = catalog::ScopedPropertyDirectionKey::try_new(
        "Attribute",
        "rank",
        helix_ast::index::RangeIndexDirection::Asc,
    )
    .unwrap();
    let range = |range: ir::IndexRange, predicate: Predicate| exec::ExecOp::IndexMembership {
        plan: Box::new(exec::ExecNodeIndexMembershipPlan {
            set: exec::ExecNodeMembershipSet::Index {
                set: exec::ExecNodeSecondarySetPlan::Range(exec::ExecNodeSecondaryRangePlan {
                    index: catalog::IndexCatalogSnapshot::default()
                        .with_node_range(key.clone())
                        .node_range[&key]
                        .clone(),
                    key: key.clone(),
                    range,
                    iteration: ir::RangeScanIteration::Forward,
                }),
                label: name("Attribute"),
                outside_label: ir::NodeMembershipOutsideLabel::Evaluate,
            },
            predicate: ir::PredicatePlan::new(predicate).unwrap(),
            residual: None,
        }),
    };
    let lower = |value: ir::RangeIndexValue| ir::IndexRange::Lower {
        lower: ir::IndexBound::Inclusive(value),
    };
    let min = name("min");
    let (rows, reads) = assert_matches_filter(
        &fixture,
        &range(
            lower(ir::RangeIndexValue::literal(5_i64.into()).unwrap()),
            Predicate::gte("rank", 5),
        ),
        Predicate::gte("rank", 5),
        traversal_rows(&fixture),
        context::ParamBindings::default(),
    )
    .await;
    assert_eq!(
        current_ids(&rows[..3]),
        [
            Some(ElementRef::Node(fixture.attribute_a)),
            Some(ElementRef::Node(fixture.attribute_none)),
            Some(ElementRef::Node(fixture.note_a)),
        ]
    );
    // Every distinct stream element evaluates the predicate in each batch.
    assert_eq!(reads, 8 * TRAVERSAL_BATCHES);

    let late_bound = range(
        lower(ir::RangeIndexValue::Param(min.clone())),
        Predicate::gte_param("rank", "min"),
    );
    let mixed = range(
        ir::IndexRange::Between(
            ir::IndexBetweenRange::new(
                ir::IndexBound::Inclusive(ir::RangeIndexValue::Param(min.clone())),
                ir::IndexBound::Inclusive(ir::RangeIndexValue::Param(name("max"))),
            )
            .unwrap(),
        ),
        Predicate::between(
            "rank",
            helix_ast::value::PropertyInput::param("min"),
            helix_ast::value::PropertyInput::param("max"),
        ),
    );
    for (op, predicate, params, kept) in [
        (
            &late_bound,
            Predicate::gte_param("rank", "min"),
            context::ParamBindings::default().with_value(min.clone(), 5_i64),
            3,
        ),
        (
            &late_bound,
            Predicate::gte_param("rank", "min"),
            context::ParamBindings::default()
                .with_value(min.clone(), helix_ast::value::PropertyValue::Null),
            0,
        ),
        (
            &late_bound,
            Predicate::gte_param("rank", "min"),
            context::ParamBindings::default()
                .with_value(min.clone(), helix_ast::value::PropertyValue::F64(f64::NAN)),
            0,
        ),
        (
            &late_bound,
            Predicate::gte_param("rank", "min"),
            context::ParamBindings::default().with_value(min.clone(), true),
            0,
        ),
        (
            &mixed,
            Predicate::between(
                "rank",
                helix_ast::value::PropertyInput::param("min"),
                helix_ast::value::PropertyInput::param("max"),
            ),
            context::ParamBindings::default()
                .with_value(min.clone(), 1_i64)
                .with_value(name("max"), "z"),
            0,
        ),
    ] {
        let mut ctx = ExecutionContext::new(&fixture.db, params.clone());
        ctx.enable_request_read_view().await.unwrap();
        let actual = ctx
            .execute_op(op, ExecutionValue::Stream(traversal_rows(&fixture)))
            .await
            .unwrap();
        assert_eq!(
            ctx.range_reads
                .entries
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );
        ctx.close_request_read_view().unwrap();
        let (expected, _) = run(
            &fixture,
            &filter(predicate),
            traversal_rows(&fixture),
            params,
        )
        .await;
        let expected = expected.unwrap();
        assert_eq!(actual, expected);
        let ExecutionValue::Stream(expected) = expected else {
            panic!("filter returns rows");
        };
        assert_eq!(expected.len(), kept * TRAVERSAL_REPEATS);
    }
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
    assert_eq!(indexed_reads, 5 * TRAVERSAL_BATCHES);
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
        assert_eq!(reads, 8 * TRAVERSAL_BATCHES);
    }
    let (_, nan_reads) = assert_matches_filter(
        &fixture,
        &equality,
        Predicate::eq_param("kind", "kind"),
        traversal_rows(&fixture),
        bind(helix_ast::value::PropertyValue::F64(f64::NAN)),
    )
    .await;
    assert_eq!(nan_reads, 5 * TRAVERSAL_BATCHES);

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
        assert_eq!(actual, reads * TRAVERSAL_BATCHES);
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
    assert_eq!(reads, 8 * TRAVERSAL_BATCHES);

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
async fn unbound_parameters_fail_only_when_a_row_reads_them() {
    let fixture = fixture("membership-unbound-params").await;
    // Node rows, none of them `Attribute`: the per-row filter rejects every
    // row on `$label` and never reads `$kind`.
    let rows = [fixture.note_b, fixture.note_a, fixture.group]
        .into_iter()
        .cycle()
        .take(3)
        .map(|id| ExecutionRow::current(ElementRef::Node(id)))
        .collect::<Vec<_>>();
    let scoped = |predicate: Predicate| {
        Predicate::and(vec![Predicate::eq("$label", "Attribute"), predicate])
    };
    let sets = [
        (
            kind_equality(ir::IndexValue::Param(name("kind"))),
            scoped(Predicate::eq_param("kind", "kind")),
        ),
        (
            kind_equality(ir::IndexValue::ParamSet(ir::RuntimeEqualitySet::new(
                name("kinds"),
                std::num::NonZeroUsize::new(2).unwrap(),
            ))),
            scoped(Predicate::is_in_param("kind", "kinds")),
        ),
    ];
    for (set, predicate) in sets {
        let before = resolved(&fixture.db);
        let (kept, _) = assert_matches_filter(
            &fixture,
            &membership(set, predicate.clone()),
            predicate,
            rows.clone(),
            context::ParamBindings::default(),
        )
        .await;
        assert!(kept.is_empty());
        // The unbound parameter left the set unresolved: every row was decided
        // by the per-row filter, which never read the parameter.
        assert_eq!(resolved(&fixture.db), before);
    }
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
    assert_eq!(reads, 8 * TRAVERSAL_BATCHES);

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

    // Streams without node rows never resolve the (corrupt) set and evaluate
    // every row instead.
    for input in [
        Vec::new(),
        vec![
            ExecutionRow::current(ElementRef::Edge(fixture.edge_b)),
            ExecutionRow::empty(),
        ],
    ] {
        let (expected, _) = run(
            &fixture,
            &filter(Predicate::eq("kind", "B")),
            input.clone(),
            context::ParamBindings::default(),
        )
        .await;
        let (result, _) = run(&fixture, &corrupt, input, context::ParamBindings::default()).await;
        assert_eq!(result.unwrap(), expected.unwrap());
    }
    // Any node row resolves the set, so the corrupt identity fails closed.
    for input in [
        traversal_pattern(&fixture),
        vec![ExecutionRow::current(ElementRef::Node(fixture.note_a))],
    ] {
        let (error, _) = run(&fixture, &corrupt, input, context::ParamBindings::default()).await;
        assert!(matches!(
            error,
            Err(crate::error::HelixDbError::IndexCatalogCorruption(_))
        ));
    }
}

#[tokio::test]
async fn pull_regions_and_count_cursors_share_membership_semantics() {
    let fixture = fixture("membership-pull-count").await;
    // Three of every five node rows match. Every pull resolves the set on
    // its first node row and decides the remaining rows from it.
    let pattern = [
        fixture.attribute_a,
        fixture.note_b,
        fixture.attribute_b,
        fixture.attribute_b,
        fixture.note_a,
    ];
    let ids = pattern
        .into_iter()
        .cycle()
        .take(60 * pattern.len())
        .collect::<Vec<_>>();
    let matching = |count| {
        ExecutionValue::Stream(
            ids.iter()
                .filter(|id| ![fixture.attribute_a, fixture.note_a].contains(*id))
                .take(count)
                .map(|id| ExecutionRow::current(ElementRef::Node(*id)))
                .collect(),
        )
    };
    let params = context::ParamBindings::default().with_value(name("ids"), ids_value(&ids));
    let predicate = Predicate::eq("kind", "B");
    let op = membership(kind_equality(literal("B")), predicate.clone());
    let limit = |count| exec::ExecOp::Limit {
        count: ir::StreamBoundPlan::Literal(count),
    };
    for (terminal, expected, resolves) in [
        // Two matches stop after four node rows, all decided by the set.
        (limit(2), matching(2), 1),
        (limit(170), matching(170), 1),
        (
            exec::ExecOp::Count {
                plan: Box::new(exec::ExecCountPlan::InputRows {
                    window: exec::ExecCountWindowPlan::identity(),
                }),
            },
            ExecutionValue::Count(180),
            1,
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
        let before = resolved(&fixture.db);
        let result = fixture.db.execute(&plan, params.clone()).await.unwrap();
        assert_eq!(result.last, Some(expected));
        assert_eq!(resolved(&fixture.db) - before, resolves);
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
    let before = resolved(&fixture.db);
    let result = fixture.db.execute(&plan, params).await.unwrap();
    assert_eq!(result.last, Some(ExecutionValue::Count(180)));
    assert_eq!(resolved(&fixture.db) - before, 1);
}

#[tokio::test]
async fn pull_cursors_resolve_membership_on_their_first_node_row() {
    let fixture = fixture("membership-pull-first-node").await;
    let ids = [fixture.attribute_b, fixture.note_b, fixture.attribute_a]
        .into_iter()
        .cycle()
        .take(3 * RECORD_BATCH_ROWS)
        .collect::<Vec<_>>();
    let corrupt = membership(
        ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::EqualityIndex {
            index: catalog::NodeEqualityIndexMeta::try_new("not-a-planner-identity").unwrap(),
            key: catalog::ScopedPropertyKey::try_new("Attribute", "kind").unwrap(),
            value: literal("B"),
        })
        .unwrap(),
        Predicate::eq("kind", "B"),
    );
    let limited = |count| {
        test_support::executable(
            ir::PlanKind::Read,
            vec![
                node_access_step(1, name("ids")),
                test_support::step(2, vec![exec::ExecStepId::new(1).unwrap()], corrupt.clone()),
                test_support::step(
                    3,
                    vec![exec::ExecStepId::new(2).unwrap()],
                    exec::ExecOp::Limit {
                        count: ir::StreamBoundPlan::Literal(count),
                    },
                ),
            ],
            3,
        )
    };

    // Even the shortest pull resolves the set on its first node row, so the
    // corrupt identity fails closed.
    for count in [1, 170] {
        let error = fixture
            .db
            .execute(
                &limited(count),
                context::ParamBindings::default().with_value(name("ids"), ids_value(&ids)),
            )
            .await
            .unwrap_err();
        assert!(
            matches!(error, crate::error::HelixDbError::IndexCatalogCorruption(_)),
            "{error:?}"
        );
    }

    // A cursor without node rows never resolves it.
    let before = resolved(&fixture.db);
    let result = fixture
        .db
        .execute(
            &limited(1),
            context::ParamBindings::default().with_value(name("ids"), ids_value(&[])),
        )
        .await
        .unwrap();
    assert_eq!(result.last, Some(ExecutionValue::Stream(Vec::new())));
    assert_eq!(resolved(&fixture.db), before);
}

#[tokio::test]
async fn resolved_memberships_are_reused_across_executions_of_one_plan() {
    let fixture = fixture("membership-reuse").await;
    let predicate = Predicate::eq("kind", "B");
    let op = membership(kind_equality(literal("B")), predicate.clone());
    let exec::ExecOp::IndexMembership { plan } = &op else {
        unreachable!("the membership helper builds membership");
    };
    let other = membership(kind_equality(literal("A")), Predicate::eq("kind", "A"));
    let mut ctx = ExecutionContext::new(&fixture.db, context::ParamBindings::default());
    ctx.enable_request_read_view().await.unwrap();

    // A branch body runs the same plan once per parent row; every run after
    // the first reuses the resolved set.
    let (expected, _) = run(
        &fixture,
        &filter(predicate),
        traversal_rows(&fixture),
        context::ParamBindings::default(),
    )
    .await;
    let expected = expected.unwrap();
    for _ in 0..3 {
        let rows = ctx
            .execute_op(&op, ExecutionValue::Stream(traversal_rows(&fixture)))
            .await
            .unwrap();
        assert_eq!(rows, expected);
    }
    assert_eq!(ctx.prepared_memberships.len(), 1);
    assert_eq!(resolved(&fixture.db), 1);
    let first = ctx.cached_index_membership(plan).await.unwrap();
    assert!(std::sync::Arc::ptr_eq(
        &first,
        &ctx.cached_index_membership(plan).await.unwrap()
    ));
    assert_eq!(resolved(&fixture.db), 1);

    // Another plan resolves its own set, and clearing forgets both.
    ctx.execute_op(&other, ExecutionValue::Stream(traversal_rows(&fixture)))
        .await
        .unwrap();
    assert_eq!(ctx.prepared_memberships.len(), 2);
    ctx.prepared_memberships.clear();
    assert!(!std::sync::Arc::ptr_eq(
        &first,
        &ctx.cached_index_membership(plan).await.unwrap()
    ));
    assert_eq!(resolved(&fixture.db), 3);
    ctx.close_request_read_view().unwrap();
}

/// Every `ForEach` frame clears the cache, so a body that decides a node row
/// resolves its membership set once per frame, however few rows it streams.
/// This pins the known per-frame cost until the cache survives frames whose
/// parameters the plan does not read.
#[tokio::test]
async fn foreach_frames_resolve_membership_once_each() {
    let fixture = fixture("membership-foreach-frames").await;
    let item = name("item");
    let items = name("items");
    let body = test_support::subplan(
        vec![
            node_access_step(1, item.clone()),
            test_support::step(
                2,
                vec![exec::ExecStepId::new(1).unwrap()],
                membership(kind_equality(literal("B")), Predicate::eq("kind", "B")),
            ),
        ],
        2,
    );
    let frames = [fixture.attribute_b, fixture.note_b, fixture.attribute_a];
    let mut ctx = ExecutionContext::new(
        &fixture.db,
        context::ParamBindings::default().with_value(
            items.clone(),
            PropertyValue::array(frames.map(|id| {
                PropertyValue::object([(item.to_string(), PropertyValue::I64(id as i64))])
            })),
        ),
    );
    ctx.enable_request_read_view().await.unwrap();
    let last = ctx.execute_foreach(&items, &body).await.unwrap();
    // The last frame's `attribute_a` has kind `A`.
    assert_eq!(last, ExecutionValue::Stream(Vec::new()));
    assert_eq!(resolved(&fixture.db), frames.len());
    assert_eq!(ctx.prepared_memberships.len(), 0);
    ctx.close_request_read_view().unwrap();
}

/// A NaN constant makes a plan unequal to itself, so no lookup could find a
/// stored entry. Such a plan resolves on every execution instead of keeping
/// one set per execution until the request ends.
#[tokio::test]
async fn plans_unequal_to_themselves_resolve_without_being_stored() {
    let fixture = fixture("membership-nan-reuse").await;
    let predicate = Predicate::is_in(
        "kind",
        PropertyValue::array([PropertyValue::F64(f64::NAN), PropertyValue::from("B")]),
    );
    let op = membership(kind_equality(literal("B")), predicate.clone());
    let exec::ExecOp::IndexMembership { plan } = &op else {
        unreachable!("the membership helper builds membership");
    };
    assert_ne!(*plan, plan.clone());
    let (expected, _) = run(
        &fixture,
        &filter(predicate),
        traversal_rows(&fixture),
        context::ParamBindings::default(),
    )
    .await;
    let expected = expected.unwrap();
    let mut ctx = ExecutionContext::new(&fixture.db, context::ParamBindings::default());
    ctx.enable_request_read_view().await.unwrap();
    for _ in 0..3 {
        let rows = ctx
            .execute_op(&op, ExecutionValue::Stream(traversal_rows(&fixture)))
            .await
            .unwrap();
        assert_eq!(rows, expected);
    }
    assert_eq!(ctx.prepared_memberships.len(), 0);
    assert_eq!(resolved(&fixture.db), 3);
    ctx.close_request_read_view().unwrap();
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

/// Run `plan` in one request and return its root value and stored-record
/// reads.
async fn run_plan(
    fixture: &Fixture,
    plan: &exec::ExecutablePlan,
    params: context::ParamBindings,
) -> (ExecutionValue, usize) {
    let mut ctx = ExecutionContext::new(&fixture.db, params);
    ctx.enable_request_read_view().await.unwrap();
    ctx.execute_steps(
        plan.steps(),
        plan.execution_order(),
        plan.root(),
        plan.execution_program(),
    )
    .await
    .unwrap();
    let reads = ctx.projection_read_snapshot().property_gets;
    let value = ctx
        .finish(plan.root(), &exec::ExecutableReturns::None)
        .unwrap()
        .last
        .expect("root result");
    ctx.close_request_read_view().unwrap();
    (value, reads)
}

/// `ids` behind a param node access, then `op`, then `terminal`.
fn pull_plan(op: exec::ExecOp, terminal: exec::ExecOp) -> exec::ExecutablePlan {
    test_support::executable(
        ir::PlanKind::Read,
        vec![
            node_access_step(1, name("ids")),
            test_support::step(2, vec![exec::ExecStepId::new(1).unwrap()], op),
            test_support::step(3, vec![exec::ExecStepId::new(2).unwrap()], terminal),
        ],
        3,
    )
}

#[tokio::test]
async fn narrow_streams_resolve_the_set_and_read_no_label_records() {
    let fixture = fixture("membership-narrow").await;
    let predicate = Predicate::eq("kind", "B");
    let op = membership(kind_equality(literal("B")), predicate.clone());
    for ids in [
        vec![fixture.attribute_b],
        vec![
            fixture.attribute_b,
            fixture.attribute_a,
            fixture.attribute_none,
        ],
    ] {
        let before = resolved(&fixture.db);
        let (rows, reads) = assert_matches_filter(
            &fixture,
            &op,
            predicate.clone(),
            ids.iter()
                .map(|id| ExecutionRow::current(ElementRef::Node(*id)))
                .collect(),
            context::ParamBindings::default(),
        )
        .await;
        assert_eq!(
            current_ids(&rows),
            [Some(ElementRef::Node(fixture.attribute_b))]
        );
        assert_eq!(reads, 0);
        assert_eq!(resolved(&fixture.db) - before, 1);
    }
}

#[tokio::test]
async fn streams_without_node_rows_never_resolve() {
    let fixture = fixture("membership-node-free").await;
    let predicate = Predicate::eq("kind", "B");
    let corrupt = membership(
        ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::EqualityIndex {
            index: catalog::NodeEqualityIndexMeta::try_new("not-a-planner-identity").unwrap(),
            key: catalog::ScopedPropertyKey::try_new("Attribute", "kind").unwrap(),
            value: literal("B"),
        })
        .unwrap(),
        predicate.clone(),
    );
    for op in [
        membership(kind_equality(literal("B")), predicate.clone()),
        corrupt,
        label_membership(Predicate::eq("$label", "Attribute"), None),
    ] {
        for input in [
            Vec::new(),
            vec![
                ExecutionRow::current(ElementRef::Edge(fixture.edge_b)),
                ExecutionRow::current(ElementRef::Edge(fixture.edge_a)),
            ],
            vec![ExecutionRow::empty(), ExecutionRow::empty()],
        ] {
            let before = resolved(&fixture.db);
            let exec::ExecOp::IndexMembership { plan } = &op else {
                unreachable!("the helpers build membership");
            };
            assert_matches_filter(
                &fixture,
                &op,
                plan.predicate.predicate().clone(),
                input,
                context::ParamBindings::default(),
            )
            .await;
            assert_eq!(resolved(&fixture.db), before);
        }
    }
}

#[tokio::test]
async fn fused_residual_reads_each_record_once() {
    let fixture = fixture("membership-fused-residual").await;
    let title = Predicate::contains("title", "x");
    let predicate = Predicate::and(vec![Predicate::eq("kind", "B"), title.clone()]);
    let op = fused(kind_equality(literal("B")), predicate.clone(), title);
    let mut pattern = traversal_pattern(&fixture);
    pattern.push(ExecutionRow::current(ElementRef::Node(
        fixture.attribute_b_plain,
    )));
    let before = resolved(&fixture.db);
    let (rows, reads) = assert_matches_filter(
        &fixture,
        &op,
        predicate,
        repeated(pattern),
        context::ParamBindings::default(),
    )
    .await;
    assert_eq!(resolved(&fixture.db) - before, 1);
    // Per pattern: both `attribute_b` rows pass their residual, and
    // `attribute_b_plain` fails it; `note_b` fails its title and every other
    // row its kind.
    assert_eq!(rows.len(), 2 * TRAVERSAL_REPEATS);
    // Each batch reads the two set matches for their residual and the notes,
    // group, and edges for the whole predicate, once each. `attribute_a` and
    // `attribute_none` are dropped unread.
    assert_eq!(reads, 7 * TRAVERSAL_BATCHES);

    // A pull cursor reads each evaluated row's record at most once and never
    // reads a label node outside the set.
    let ids = [
        fixture.attribute_b,
        fixture.attribute_a,
        fixture.note_b,
        fixture.attribute_b_plain,
        fixture.attribute_none,
        fixture.note_a,
    ]
    .repeat(3);
    let evaluated = ids
        .iter()
        .filter(|id| ![fixture.attribute_a, fixture.attribute_none].contains(*id))
        .count();
    let (value, reads) = run_plan(
        &fixture,
        &pull_plan(
            op,
            exec::ExecOp::Limit {
                count: ir::StreamBoundPlan::Literal(ids.len()),
            },
        ),
        context::ParamBindings::default().with_value(name("ids"), ids_value(&ids)),
    )
    .await;
    assert_eq!(
        value,
        ExecutionValue::Stream(
            [fixture.attribute_b; 3]
                .into_iter()
                .map(|id| ExecutionRow::current(ElementRef::Node(id)))
                .collect()
        )
    );
    assert!(reads <= evaluated, "{reads} reads for {evaluated} rows");
}

#[tokio::test]
async fn label_set_membership_decides_nodes_from_label_bitmaps() {
    let fixture = fixture("membership-label-set").await;
    let attribute = Predicate::eq("$label", "Attribute");
    let before = resolved(&fixture.db);
    let (rows, reads) = assert_matches_filter(
        &fixture,
        &label_membership(attribute.clone(), None),
        attribute.clone(),
        traversal_rows(&fixture),
        context::ParamBindings::default(),
    )
    .await;
    assert_eq!(rows.len(), 4 * TRAVERSAL_REPEATS);
    // Only the two edges read their records.
    assert_eq!(reads, 2 * TRAVERSAL_BATCHES);
    assert_eq!(resolved(&fixture.db) - before, 1);

    let either = Predicate::is_in(
        "$label",
        PropertyValue::StringArray(vec!["Attribute".to_owned(), "Note".to_owned()]),
    );
    let (rows, reads) = assert_matches_filter(
        &fixture,
        &label_membership(either.clone(), None),
        either,
        traversal_rows(&fixture),
        context::ParamBindings::default(),
    )
    .await;
    assert_eq!(rows.len(), 7 * TRAVERSAL_REPEATS);
    assert_eq!(reads, 2 * TRAVERSAL_BATCHES);

    // Attribute nodes evaluate their residual and edges the whole predicate;
    // every other node is dropped unread.
    let kind = Predicate::eq("kind", "B");
    let predicate = Predicate::and(vec![attribute, kind.clone()]);
    let (rows, reads) = assert_matches_filter(
        &fixture,
        &label_membership(predicate.clone(), Some(kind)),
        predicate,
        traversal_rows(&fixture),
        context::ParamBindings::default(),
    )
    .await;
    assert_eq!(rows.len(), 2 * TRAVERSAL_REPEATS);
    assert_eq!(reads, (3 + 2) * TRAVERSAL_BATCHES);
}

#[tokio::test]
async fn counts_over_fully_indexed_memberships_read_no_records() {
    let fixture = fixture("membership-count-reads").await;
    let count = |predicate: Predicate| exec::ExecOp::Count {
        plan: Box::new(exec::ExecCountPlan::Stream(exec::ExecCountStreamPlan {
            cursor: exec::ExecCountCursorPlan::IndexMembership {
                input: Box::new(exec::ExecCountCursorPlan::NodeRuntimeInput(
                    exec::ExecRuntimeInputPlan::Param(name("ids")),
                )),
                plan: Box::new(exec::ExecNodeIndexMembershipPlan::from(
                    &ir::NodeIndexMembershipPlan::new(
                        kind_equality(literal("B")),
                        ir::PredicatePlan::new(predicate).unwrap(),
                        None,
                    )
                    .unwrap(),
                )),
            },
            window: exec::ExecCountWindowPlan::identity(),
        })),
    };
    let execute = |op: exec::ExecOp, ids: Vec<u64>| {
        let db = &fixture.db;
        async move {
            let mut ctx = ExecutionContext::new(
                db,
                context::ParamBindings::default().with_value(name("ids"), ids_value(&ids)),
            );
            ctx.enable_request_read_view().await.unwrap();
            let before = resolved(db);
            let value = ctx
                .execute_op(&op, ExecutionValue::Stream(Vec::new()))
                .await
                .unwrap();
            let reads = ctx.projection_read_snapshot().property_gets;
            ctx.close_request_read_view().unwrap();
            (value, reads, resolved(db) - before)
        }
    };

    let attributes = [
        fixture.attribute_b,
        fixture.attribute_a,
        fixture.attribute_none,
    ]
    .repeat(200);
    let reject = Predicate::and(vec![
        Predicate::eq("$label", "Attribute"),
        Predicate::eq("kind", "B"),
    ]);
    assert_eq!(
        execute(count(reject), attributes).await,
        (ExecutionValue::Count(200), 0, 1)
    );

    // Under the Evaluate policy only the other-label nodes read records.
    let mixed = [
        fixture.attribute_b,
        fixture.note_b,
        fixture.attribute_a,
        fixture.note_a,
    ]
    .repeat(50);
    assert_eq!(
        execute(count(Predicate::eq("kind", "B")), mixed).await,
        (ExecutionValue::Count(100), 100, 1)
    );
}

#[tokio::test]
async fn pull_limit_and_exists_resolve_the_set() {
    let fixture = fixture("membership-pull-short").await;
    let op = membership(kind_equality(literal("B")), Predicate::eq("kind", "B"));
    let ids = [
        fixture.attribute_a,
        fixture.attribute_b,
        fixture.attribute_none,
    ]
    .repeat(100);
    for (terminal, expected) in [
        (
            exec::ExecOp::Limit {
                count: ir::StreamBoundPlan::Literal(1),
            },
            ExecutionValue::Stream(vec![ExecutionRow::current(ElementRef::Node(
                fixture.attribute_b,
            ))]),
        ),
        (
            exec::ExecOp::Project {
                projection: ir::ProjectionPlan::Exists,
            },
            ExecutionValue::Bool(true),
        ),
    ] {
        let before = resolved(&fixture.db);
        let (value, reads) = run_plan(
            &fixture,
            &pull_plan(op.clone(), terminal),
            context::ParamBindings::default().with_value(name("ids"), ids_value(&ids)),
        )
        .await;
        assert_eq!(value, expected);
        assert_eq!(reads, 0);
        assert_eq!(resolved(&fixture.db) - before, 1);
    }
}
