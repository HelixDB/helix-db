//! Membership set retention across `ForEach` frames and request writes.
//!
//! Every retained set is checked against a fresh per-row filter of the same
//! predicate in the same request state, so retention can only change how
//! often a set is read, never which rows survive. The resolve counter shows
//! whether a set was kept or read again.

use super::super::super::ExecutionContext;
use super::support::*;

fn step_id(id: usize) -> exec::ExecStepId {
    exec::ExecStepId::new(id).unwrap()
}

/// Node rows of `ids`, in order.
fn nodes(ids: &[u64]) -> Vec<ExecutionRow> {
    rows(ids)
}

fn node_id(id: u64) -> PropertyValue {
    PropertyValue::I64(id as i64)
}

/// `ForEach` items, one object of `fields` per frame.
fn items<const N: usize>(frames: Vec<[(&str, PropertyValue); N]>) -> PropertyValue {
    PropertyValue::array(frames.into_iter().map(PropertyValue::object))
}

/// Body that streams the node bound to `item` through `op`.
fn item_body(op: exec::ExecOp) -> exec::ExecutableSubplan {
    test_support::subplan(
        vec![
            node_access_step(1, name("item")),
            test_support::step(2, vec![step_id(1)], op),
        ],
        2,
    )
}

/// `ForEach` over the `param` items running `body` per frame.
fn foreach(param: &str, body: exec::ExecutableSubplan) -> exec::ExecOp {
    exec::ExecOp::ForEach {
        param: name(param),
        body: Box::new(body),
    }
}

fn membership_plan(op: &exec::ExecOp) -> &exec::ExecNodeIndexMembershipPlan {
    let exec::ExecOp::IndexMembership { plan } = op else {
        panic!("expected an index membership operation");
    };
    plan
}

/// Run `op` through the context's membership cache and a fresh per-row
/// filter of its predicate in the same request state, assert that both keep
/// the same rows, and return them.
///
/// `execute_op` bypasses the step visibility barrier, so pending writes are
/// flushed first, as `execute_step` would.
async fn exact(
    ctx: &mut ExecutionContext<'_>,
    op: &exec::ExecOp,
    input: &[ExecutionRow],
) -> ExecutionValue {
    ctx.flush_active_index_mutations().await.unwrap();
    let expected = ctx
        .execute_op(
            &filter(membership_plan(op).predicate.predicate().clone()),
            ExecutionValue::Stream(input.to_vec()),
        )
        .await
        .unwrap();
    let actual = ctx
        .execute_op(op, ExecutionValue::Stream(input.to_vec()))
        .await
        .unwrap();
    assert_eq!(actual, expected);
    actual
}

async fn read_context(fixture: &Fixture, params: context::ParamBindings) -> ExecutionContext<'_> {
    let mut ctx = ExecutionContext::new(&fixture.db, params);
    ctx.enable_request_read_view().await.unwrap();
    ctx
}

async fn run_op(
    ctx: &mut ExecutionContext<'_>,
    op: &exec::ExecOp,
) -> crate::Result<ExecutionValue> {
    ctx.execute_op(op, ExecutionValue::Stream(Vec::new())).await
}

fn stream(ids: &[u64]) -> ExecutionValue {
    ExecutionValue::Stream(nodes(ids))
}

/// A frame binding only the streamed node keeps a literal set: the body
/// resolves it once for every frame.
#[tokio::test]
async fn foreach_frames_keep_sets_no_frame_rebinds() {
    let fixture = fixture("retention-foreach-unrelated").await;
    let op = membership(kind_equality(literal("B")), Predicate::eq("kind", "B"));
    let frames = [fixture.attribute_a, fixture.note_b, fixture.attribute_b];
    let mut ctx = read_context(
        &fixture,
        context::ParamBindings::default().with_value(
            name("items"),
            items(frames.map(|id| [("item", node_id(id))]).to_vec()),
        ),
    )
    .await;
    let last = run_op(&mut ctx, &foreach("items", item_body(op.clone())))
        .await
        .unwrap();
    assert_eq!(resolved(&fixture.db), 1);
    assert_eq!(ctx.prepared_memberships.len(), 1);
    assert_eq!(last, stream(&[fixture.attribute_b]));
    assert_eq!(
        last,
        exact(&mut ctx, &op, &nodes(&[fixture.attribute_b])).await
    );
    assert_eq!(resolved(&fixture.db), 1);
    ctx.close_request_read_view().unwrap();
}

/// Parameters read only by the predicate or residual are evaluated per row,
/// so rebinding them keeps the set while every frame still sees its own
/// value.
#[tokio::test]
async fn residual_only_parameters_keep_the_set_across_frames() {
    let fixture = fixture("retention-foreach-residual").await;
    let title = Predicate::contains_param("title", "needle");
    let predicate = Predicate::and(vec![Predicate::eq("kind", "B"), title.clone()]);
    let op = fused(kind_equality(literal("B")), predicate.clone(), title);
    let frames = vec![
        (fixture.attribute_b, "xb"),
        (fixture.attribute_b, "plain"),
        (fixture.attribute_b_plain, "plain"),
        (fixture.note_b, "note"),
        (fixture.attribute_a, "xa"),
    ];
    let bind = |frames: &[(u64, &str)]| {
        items(
            frames
                .iter()
                .map(|(id, needle)| {
                    [
                        ("item", node_id(*id)),
                        ("needle", PropertyValue::from(*needle)),
                    ]
                })
                .collect(),
        )
    };
    let mut ctx = read_context(
        &fixture,
        context::ParamBindings::default().with_value(name("items"), bind(&frames)),
    )
    .await;
    run_op(&mut ctx, &foreach("items", item_body(op.clone())))
        .await
        .unwrap();
    assert_eq!(resolved(&fixture.db), 1);

    // Each frame, run alone against the warm cache, keeps exactly the rows
    // the per-row filter keeps under that frame's needle.
    let mut kept = 0;
    for frame in frames {
        ctx.params.values.insert(name("items"), bind(&[frame]));
        let actual = run_op(&mut ctx, &foreach("items", item_body(op.clone())))
            .await
            .unwrap();
        let expected = run_op(
            &mut ctx,
            &foreach("items", item_body(filter(predicate.clone()))),
        )
        .await
        .unwrap();
        assert_eq!(actual, expected);
        kept += usize::from(actual != ExecutionValue::Stream(Vec::new()));
    }
    assert_eq!(kept, 3);
    assert_eq!(resolved(&fixture.db), 1);
    ctx.close_request_read_view().unwrap();
}

/// A set read from a frame parameter resolves once per frame, and a frame
/// never sees the set of the previous binding.
#[tokio::test]
async fn set_parameters_resolve_once_per_frame() {
    let fixture = fixture("retention-foreach-set-param").await;
    let predicate = Predicate::eq_param("kind", "kind");
    let op = membership(
        kind_equality(ir::IndexValue::Param(name("kind"))),
        predicate.clone(),
    );
    let bind = |frames: &[(u64, &str)]| {
        items(
            frames
                .iter()
                .map(|(id, kind)| [("item", node_id(*id)), ("kind", PropertyValue::from(*kind))])
                .collect(),
        )
    };
    let mut ctx = read_context(
        &fixture,
        context::ParamBindings::default().with_value(
            name("items"),
            bind(&[
                (fixture.attribute_b, "B"),
                (fixture.attribute_b, "A"),
                (fixture.attribute_b, "B"),
            ]),
        ),
    )
    .await;
    let last = run_op(&mut ctx, &foreach("items", item_body(op.clone())))
        .await
        .unwrap();
    assert_eq!(resolved(&fixture.db), 3);
    assert_eq!(last, stream(&[fixture.attribute_b]));
    assert_eq!(ctx.prepared_memberships.len(), 0);

    // A set kept from the `B` frame would drop `attribute_a` in the `A` frame.
    ctx.params.values.insert(
        name("items"),
        bind(&[(fixture.attribute_a, "B"), (fixture.attribute_a, "A")]),
    );
    let last = run_op(&mut ctx, &foreach("items", item_body(op.clone())))
        .await
        .unwrap();
    assert_eq!(last, stream(&[fixture.attribute_a]));
    assert_eq!(
        last,
        run_op(&mut ctx, &foreach("items", item_body(filter(predicate))))
            .await
            .unwrap()
    );
    assert_eq!(resolved(&fixture.db), 5);
    ctx.close_request_read_view().unwrap();
}

/// Runs `contract` on a 16 MiB stack: nested `ForEach` futures overflow the
/// default test thread stack in debug builds.
fn high_stack<F: std::future::Future<Output = ()> + 'static>(contract: fn() -> F) {
    std::thread::Builder::new()
        .stack_size(16 * 1024 * 1024)
        .spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(contract());
        })
        .unwrap()
        .join()
        .unwrap();
}

/// An outer frame's set survives the inner frames that do not rebind its
/// parameter, and the binding restored on exit resolves its own set.
#[test]
fn nested_frames_compose_and_restore_the_outer_binding() {
    high_stack(nested_frames_contract);
}

async fn nested_frames_contract() {
    let fixture = fixture("retention-foreach-nested").await;
    let op = membership(
        kind_equality(ir::IndexValue::Param(name("kind"))),
        Predicate::eq_param("kind", "kind"),
    );
    let inner = |id: u64| PropertyValue::object([("item".to_string(), node_id(id))]);
    let group = |kind: &str, ids: [u64; 3]| {
        [
            ("kind", PropertyValue::from(kind)),
            ("items", PropertyValue::array(ids.map(inner))),
        ]
    };
    let (b, a) = (fixture.attribute_b, fixture.attribute_a);
    let mut ctx = read_context(
        &fixture,
        context::ParamBindings::default()
            .with_value(
                name("groups"),
                items(vec![group("B", [b, a, b]), group("A", [b, a, a])]),
            )
            .with_value(name("kind"), PropertyValue::from("B")),
    )
    .await;
    let nested = |op: exec::ExecOp| {
        foreach(
            "groups",
            test_support::subplan(
                vec![test_support::step(
                    1,
                    Vec::new(),
                    foreach("items", item_body(op)),
                )],
                1,
            ),
        )
    };
    let last = run_op(&mut ctx, &nested(op.clone())).await.unwrap();
    // One resolve per outer frame, however many inner frames read it.
    assert_eq!(resolved(&fixture.db), 2);
    assert_eq!(last, stream(&[a]));
    assert_eq!(
        last,
        run_op(
            &mut ctx,
            &nested(filter(Predicate::eq_param("kind", "kind")))
        )
        .await
        .unwrap()
    );
    assert_eq!(ctx.prepared_memberships.len(), 0);
    assert_eq!(
        ctx.params.values.get(&name("kind")),
        Some(&PropertyValue::from("B"))
    );
    assert_eq!(exact(&mut ctx, &op, &nodes(&[b, a])).await, stream(&[b]));
    assert_eq!(resolved(&fixture.db), 3);
    ctx.close_request_read_view().unwrap();
}

/// A set resolved before the loop from a parameter a frame binds is
/// forgotten by that frame and resolved again after the loop under the
/// restored binding; an unrelated set survives the whole loop.
#[tokio::test]
async fn frames_forget_entry_sets_they_rebind_and_keep_the_rest() {
    let fixture = fixture("retention-foreach-entry-exit").await;
    let dynamic = membership(
        kind_equality(ir::IndexValue::Param(name("kind"))),
        Predicate::eq_param("kind", "kind"),
    );
    let literal_b = membership(kind_equality(literal("B")), Predicate::eq("kind", "B"));
    let all = nodes(&[
        fixture.attribute_b,
        fixture.attribute_a,
        fixture.note_a,
        fixture.group,
    ]);
    let mut ctx = read_context(
        &fixture,
        context::ParamBindings::default()
            .with_value(
                name("items"),
                items(vec![
                    [
                        ("item", node_id(fixture.attribute_b)),
                        ("kind", PropertyValue::from("B")),
                    ],
                    [
                        ("item", node_id(fixture.attribute_a)),
                        ("kind", PropertyValue::from("B")),
                    ],
                ]),
            )
            .with_value(name("kind"), PropertyValue::from("A")),
    )
    .await;
    exact(&mut ctx, &dynamic, &all).await;
    exact(&mut ctx, &literal_b, &all).await;
    assert_eq!(
        (resolved(&fixture.db), ctx.prepared_memberships.len()),
        (2, 2)
    );

    let last = run_op(&mut ctx, &foreach("items", item_body(dynamic.clone())))
        .await
        .unwrap();
    assert_eq!(last, ExecutionValue::Stream(Vec::new()));
    assert_eq!(
        (resolved(&fixture.db), ctx.prepared_memberships.len()),
        (4, 1)
    );
    assert_eq!(
        exact(&mut ctx, &dynamic, &all).await,
        stream(&[fixture.attribute_a, fixture.note_a])
    );
    exact(&mut ctx, &literal_b, &all).await;
    assert_eq!(resolved(&fixture.db), 5);
    ctx.close_request_read_view().unwrap();
}

#[tokio::test]
async fn empty_foreach_keeps_every_set() {
    let fixture = fixture("retention-foreach-empty").await;
    let op = membership(kind_equality(literal("B")), Predicate::eq("kind", "B"));
    let mut ctx = read_context(
        &fixture,
        context::ParamBindings::default()
            .with_value(name("items"), PropertyValue::Array(Vec::new())),
    )
    .await;
    exact(&mut ctx, &op, &nodes(&[fixture.attribute_b])).await;
    assert_eq!(
        run_op(&mut ctx, &foreach("items", item_body(op.clone())))
            .await
            .unwrap(),
        ExecutionValue::Stream(Vec::new())
    );
    assert_eq!(ctx.prepared_memberships.len(), 1);
    exact(&mut ctx, &op, &nodes(&[fixture.attribute_b])).await;
    assert_eq!(resolved(&fixture.db), 1);
    ctx.close_request_read_view().unwrap();
}

/// A failed frame restores every binding, and the failed operation forgets
/// every set.
#[tokio::test]
async fn failed_frames_restore_bindings_and_forget_every_set() {
    let fixture = fixture("retention-foreach-error").await;
    let op = membership(kind_equality(literal("B")), Predicate::eq("kind", "B"));
    let frames = items(vec![
        [("item", node_id(fixture.attribute_b))],
        [("other", node_id(fixture.attribute_a))],
    ]);
    let mut ctx = read_context(
        &fixture,
        context::ParamBindings::default().with_value(name("items"), frames.clone()),
    )
    .await;
    exact(&mut ctx, &op, &nodes(&[fixture.attribute_b])).await;
    let error = run_op(&mut ctx, &foreach("items", item_body(op.clone())))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("is not bound"), "{error}");
    assert_eq!(ctx.params.values.get(&name("items")), Some(&frames));
    assert_eq!(ctx.params.values.get(&name("item")), None);
    assert_eq!(ctx.params.values.get(&name("other")), None);
    assert_eq!(ctx.prepared_memberships.len(), 0);
    ctx.close_request_read_view().unwrap();
}

/// A deadline that expires anywhere in the loop fails the operation and
/// forgets every set.
#[tokio::test]
async fn deadlines_inside_frames_forget_every_set() {
    let fixture = fixture("retention-foreach-deadline").await;
    let op = membership(kind_equality(literal("B")), Predicate::eq("kind", "B"));
    let frames = items(
        [
            fixture.attribute_b,
            fixture.attribute_a,
            fixture.attribute_b,
        ]
        .map(|id| [("item", node_id(id))])
        .to_vec(),
    );
    let mut expired = 0;
    for checks in 0.. {
        let mut ctx = read_context(
            &fixture,
            context::ParamBindings::default().with_value(name("items"), frames.clone()),
        )
        .await;
        exact(&mut ctx, &op, &nodes(&[fixture.attribute_a])).await;
        ctx.fail_deadline_after(checks);
        match run_op(&mut ctx, &foreach("items", item_body(op.clone()))).await {
            Err(crate::HelixDbError::QueryDeadlineExceeded) => {
                assert_eq!(ctx.prepared_memberships.len(), 0);
                expired += 1;
            }
            Err(error) => panic!("unexpected error {error:?}"),
            Ok(last) => {
                assert_eq!(last, stream(&[fixture.attribute_b]));
                assert_eq!(ctx.prepared_memberships.len(), 1);
                break;
            }
        }
    }
    // The loop checks its deadline before every frame, at least.
    assert!(expired >= 3, "{expired} expiring runs");
}

async fn write_context(fixture: &Fixture, params: context::ParamBindings) -> ExecutionContext<'_> {
    let mut ctx = ExecutionContext::new(&fixture.db, params);
    ctx.enable_request_write_scope().await.unwrap();
    ctx
}

/// Run one mutation over `input` and return the node IDs it streams.
async fn mutate(
    ctx: &mut ExecutionContext<'_>,
    plan: exec::ExecMutationPlan,
    input: &[ExecutionRow],
) -> crate::Result<Vec<ExecutionRow>> {
    let value = ctx
        .execute_op(
            &exec::ExecOp::Mutation { plan },
            ExecutionValue::Stream(input.to_vec()),
        )
        .await?;
    let ExecutionValue::Stream(rows) = value else {
        panic!("mutations stream their rows");
    };
    Ok(rows)
}

fn add_node(label: &str, properties: Vec<(&str, PropertyValue)>) -> exec::ExecMutationPlan {
    exec::ExecMutationPlan::AddNodeSource {
        label: name(label),
        properties: test_support::assignments(properties),
    }
}

fn set_property(property: &str, value: impl Into<PropertyValue>) -> exec::ExecMutationPlan {
    exec::ExecMutationPlan::SetProperty {
        name: name(property),
        value: ir::PropertyInputPlan::Value(value.into()),
    }
}

fn created_id(rows: Vec<ExecutionRow>) -> u64 {
    let [row] = rows.try_into().expect("one created node");
    let Some(ElementRef::Node(id)) = row.current else {
        panic!("a node create streams its node");
    };
    id
}

/// One request-transaction write whose footprint the retention matrix
/// checks.
#[derive(Debug, Clone, Copy)]
enum Write {
    CreateNote,
    AddEdge,
    DropEdge,
    SetTitle,
    SetUnindexed,
    SetKindUnchanged,
    SetKind,
    RemoveKind,
    SetUid,
    CreateAttribute,
    DropAttribute,
    Relabel,
}

impl Write {
    /// Apply the write, returning the node it created or `None`, and the
    /// node it deleted or `None`.
    async fn apply(
        self,
        ctx: &mut ExecutionContext<'_>,
        fixture: &Fixture,
    ) -> (Option<u64>, Option<u64>) {
        let (plan, input) = match self {
            Self::CreateNote => (
                add_node("Note", vec![("kind", PropertyValue::from("B"))]),
                Vec::new(),
            ),
            Self::AddEdge => (
                exec::ExecMutationPlan::AddEdge {
                    label: name("LINK"),
                    to: ir::NodeTargetPlan::PointIds {
                        ids: test_support::ids(vec![fixture.attribute_a]),
                    },
                    properties: test_support::assignments(vec![("kind", PropertyValue::from("B"))]),
                },
                nodes(&[fixture.note_b]),
            ),
            Self::DropEdge => (
                exec::ExecMutationPlan::Drop,
                vec![ExecutionRow::current(ElementRef::Edge(fixture.edge_b))],
            ),
            Self::SetTitle => (
                set_property("title", "plain"),
                nodes(&[fixture.attribute_b]),
            ),
            Self::SetUnindexed => (set_property("color", "red"), nodes(&[fixture.attribute_b])),
            Self::SetKindUnchanged => (set_property("kind", "B"), nodes(&[fixture.attribute_b])),
            Self::SetKind => (set_property("kind", "B"), nodes(&[fixture.attribute_a])),
            Self::RemoveKind => (
                exec::ExecMutationPlan::RemoveProperty { name: name("kind") },
                nodes(&[fixture.attribute_b]),
            ),
            Self::SetUid => (set_property("uid", "a9"), nodes(&[fixture.attribute_b])),
            Self::CreateAttribute => (
                add_node(
                    "Attribute",
                    vec![
                        ("kind", PropertyValue::from("B")),
                        ("uid", PropertyValue::from("a5")),
                        ("title", PropertyValue::from("x")),
                    ],
                ),
                Vec::new(),
            ),
            Self::DropAttribute => (exec::ExecMutationPlan::Drop, nodes(&[fixture.attribute_b])),
            Self::Relabel => (
                set_property("$label", "Attribute"),
                nodes(&[fixture.note_b]),
            ),
        };
        let rows = mutate(ctx, plan, &input).await.unwrap();
        match self {
            Self::CreateNote | Self::CreateAttribute => (Some(created_id(rows)), None),
            Self::DropAttribute => (None, Some(fixture.attribute_b)),
            Self::AddEdge
            | Self::DropEdge
            | Self::SetTitle
            | Self::SetUnindexed
            | Self::SetKindUnchanged
            | Self::SetKind
            | Self::RemoveKind
            | Self::SetUid
            | Self::Relabel => (None, None),
        }
    }
}

/// Live nodes of the fixture plus `created`, without `deleted`, and the
/// edge no write deletes.
fn live_rows(fixture: &Fixture, created: Option<u64>, deleted: Option<u64>) -> Vec<ExecutionRow> {
    [
        fixture.attribute_b,
        fixture.attribute_a,
        fixture.attribute_none,
        fixture.attribute_b_plain,
        fixture.note_b,
        fixture.note_a,
        fixture.group,
    ]
    .into_iter()
    .chain(created)
    .filter(|id| Some(*id) != deleted)
    .map(|id| ExecutionRow::current(ElementRef::Node(id)))
    .chain([ExecutionRow::current(ElementRef::Edge(fixture.edge_a))])
    .collect()
}

/// A write forgets exactly the sets whose footprint it reaches: every kept
/// set still equals the per-row filter, and only forgotten sets resolve
/// again.
#[test]
fn writes_forget_only_sets_whose_footprint_they_reach() {
    high_stack(writes_forget_only_sets_whose_footprint_they_reach_contract);
}

async fn writes_forget_only_sets_whose_footprint_they_reach_contract() {
    let title = Predicate::contains("title", "x");
    let sets = [
        membership(kind_equality(literal("B")), Predicate::eq("kind", "B")),
        fused(
            kind_equality(literal("B")),
            Predicate::and(vec![Predicate::eq("kind", "B"), title.clone()]),
            title,
        ),
        membership(
            attribute_equality("uid", catalog::IndexUniqueness::Unique, literal("a1")),
            Predicate::eq("uid", "a1"),
        ),
        label_membership(Predicate::eq("$label", "Attribute"), None),
    ];
    for (write, resolves) in [
        (Write::CreateNote, [0, 0, 0, 0]),
        (Write::AddEdge, [0, 0, 0, 0]),
        (Write::DropEdge, [0, 0, 0, 0]),
        (Write::SetTitle, [0, 0, 0, 0]),
        (Write::SetUnindexed, [0, 0, 0, 0]),
        (Write::SetKindUnchanged, [0, 0, 0, 0]),
        (Write::SetKind, [1, 1, 0, 0]),
        (Write::RemoveKind, [1, 1, 0, 0]),
        (Write::SetUid, [0, 0, 1, 0]),
        (Write::CreateAttribute, [1, 1, 1, 1]),
        (Write::DropAttribute, [1, 1, 1, 1]),
        (Write::Relabel, [1, 1, 1, 1]),
    ] {
        let fixture = fixture(&format!("retention-write-{write:?}")).await;
        let mut ctx = write_context(&fixture, context::ParamBindings::default()).await;
        let before = live_rows(&fixture, None, None);
        let mut warm = Vec::new();
        for set in &sets {
            warm.push(exact(&mut ctx, set, &before).await);
        }
        assert_eq!(resolved(&fixture.db), sets.len(), "{write:?}");

        let (created, deleted) = write.apply(&mut ctx, &fixture).await;
        let after = live_rows(&fixture, created, deleted);
        let mut actual = Vec::new();
        let mut changed = Vec::new();
        for (set, warm) in sets.iter().zip(&warm) {
            let start = resolved(&fixture.db);
            let rows = exact(&mut ctx, set, &after).await;
            actual.push(resolved(&fixture.db) - start);
            changed.push(rows != *warm);
        }
        assert_eq!(actual, resolves, "{write:?}");
        if matches!(write, Write::SetTitle) {
            // The kept set evaluates the new title per row.
            assert_eq!(changed, [false, true, false, false]);
        }
        ctx.abort_request_write_scope();
    }
}

/// Many writes before the next read, including a batch create, forget a set
/// once.
#[test]
fn repeated_writes_resolve_a_set_again_once() {
    high_stack(repeated_writes_contract);
}

async fn repeated_writes_contract() {
    let fixture = fixture("retention-write-repeated").await;
    let op = membership(kind_equality(literal("B")), Predicate::eq("kind", "B"));
    let mut ctx = write_context(&fixture, context::ParamBindings::default()).await;
    let mut input = live_rows(&fixture, None, None);
    exact(&mut ctx, &op, &input).await;

    let batch = mutate(
        &mut ctx,
        exec::ExecMutationPlan::AddNodeFromInput {
            label: name("Attribute"),
            properties: test_support::assignments(vec![("kind", PropertyValue::from("B"))]),
        },
        &nodes(&[fixture.group, fixture.group, fixture.group]),
    )
    .await
    .unwrap();
    assert_eq!(batch.len(), 3);
    input.extend(batch);
    exact(&mut ctx, &op, &input).await;
    assert_eq!(resolved(&fixture.db), 2);

    for (plan, rows) in [
        (set_property("title", "u"), nodes(&[fixture.attribute_b])),
        (set_property("title", "v"), nodes(&[fixture.attribute_a])),
        (set_property("kind", "B"), nodes(&[fixture.attribute_a])),
        (
            add_node("Attribute", vec![("kind", PropertyValue::from("A"))]),
            Vec::new(),
        ),
        (set_property("kind", "A"), nodes(&[fixture.attribute_b])),
    ] {
        input.extend(mutate(&mut ctx, plan, &rows).await.unwrap());
    }
    assert_eq!(ctx.prepared_memberships.len(), 0);
    exact(&mut ctx, &op, &input).await;
    exact(&mut ctx, &op, &input).await;
    assert_eq!(resolved(&fixture.db), 3);
    ctx.commit_request_write_scope().await.unwrap();
    assert_eq!(ctx.prepared_memberships.len(), 0);
}

/// A write forgets a union or intersection when it reaches any of its
/// leaves.
#[test]
fn composite_sets_are_forgotten_by_any_leaf() {
    high_stack(composite_sets_contract);
}

async fn composite_sets_contract() {
    let fixture = fixture("retention-write-composite").await;
    let union = membership(
        ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::Union(ir::AtLeast::from_pair(
            kind_equality(literal("B")),
            kind_equality(ir::IndexValue::Param(name("k"))),
        )))
        .unwrap(),
        Predicate::or(vec![
            Predicate::eq("kind", "B"),
            Predicate::eq_param("kind", "k"),
        ]),
    );
    let intersect = membership(
        ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::Intersect(ir::AtLeast::from_pair(
            kind_equality(literal("B")),
            attribute_equality(
                "status",
                catalog::IndexUniqueness::NonUnique,
                ir::IndexValue::Param(name("s")),
            ),
        )))
        .unwrap(),
        Predicate::and(vec![
            Predicate::eq("kind", "B"),
            Predicate::eq_param("status", "s"),
        ]),
    );
    let mut ctx = write_context(
        &fixture,
        context::ParamBindings::default()
            .with_value(name("k"), PropertyValue::from("A"))
            .with_value(name("s"), PropertyValue::from("on")),
    )
    .await;
    let input = live_rows(&fixture, None, None);
    exact(&mut ctx, &union, &input).await;
    exact(&mut ctx, &intersect, &input).await;
    assert_eq!(resolved(&fixture.db), 2);

    // A status write reaches only the intersection.
    for (id, status) in [
        (fixture.attribute_b, "off"),
        (fixture.attribute_b_plain, "on"),
    ] {
        mutate(&mut ctx, set_property("status", status), &nodes(&[id]))
            .await
            .unwrap();
        exact(&mut ctx, &union, &input).await;
        exact(&mut ctx, &intersect, &input).await;
    }
    assert_eq!(resolved(&fixture.db), 4);
    // A kind write reaches both.
    mutate(
        &mut ctx,
        set_property("kind", "A"),
        &nodes(&[fixture.attribute_b_plain]),
    )
    .await
    .unwrap();
    assert_eq!(ctx.prepared_memberships.len(), 0);
    exact(&mut ctx, &union, &input).await;
    exact(&mut ctx, &intersect, &input).await;
    assert_eq!(resolved(&fixture.db), 6);
    ctx.abort_request_write_scope();
}

/// Runtime bindings that resolve per row or to an empty set, and runtime
/// domains within and over their bound, stay exact across writes.
#[test]
fn runtime_bindings_stay_exact_across_writes() {
    high_stack(runtime_bindings_contract);
}

async fn runtime_bindings_contract() {
    let equality = membership(
        kind_equality(ir::IndexValue::Param(name("kind"))),
        Predicate::eq_param("kind", "kind"),
    );
    let domain = membership(
        kind_equality(ir::IndexValue::ParamSet(ir::RuntimeEqualitySet::new(
            name("kinds"),
            std::num::NonZeroUsize::new(2).unwrap(),
        ))),
        Predicate::is_in_param("kind", "kinds"),
    );
    let strings = |values: &[&str]| {
        PropertyValue::StringArray(values.iter().map(|value| value.to_string()).collect())
    };
    for (op, param, value, resolves) in [
        // Null resolves per row; NaN resolves an empty set and the label.
        (&equality, "kind", PropertyValue::Null, 0),
        (&equality, "kind", PropertyValue::F64(f64::NAN), 1),
        (&domain, "kinds", strings(&["A", "B"]), 1),
        (&domain, "kinds", strings(&["A", "B", "C"]), 0),
    ] {
        let fixture = fixture("retention-write-runtime").await;
        let mut ctx = write_context(
            &fixture,
            context::ParamBindings::default().with_value(name(param), value.clone()),
        )
        .await;
        let input = live_rows(&fixture, None, None);
        exact(&mut ctx, op, &input).await;
        assert_eq!(ctx.prepared_memberships.len(), 1, "{value:?}");
        // An unrelated write keeps the entry, a kind write forgets it.
        mutate(
            &mut ctx,
            set_property("title", "u"),
            &nodes(&[fixture.attribute_a]),
        )
        .await
        .unwrap();
        exact(&mut ctx, op, &input).await;
        assert_eq!(resolved(&fixture.db), resolves, "{value:?}");
        mutate(
            &mut ctx,
            set_property("kind", "A"),
            &nodes(&[fixture.attribute_b]),
        )
        .await
        .unwrap();
        assert_eq!(ctx.prepared_memberships.len(), 0, "{value:?}");
        exact(&mut ctx, op, &input).await;
        assert_eq!(resolved(&fixture.db), 2 * resolves, "{value:?}");
        ctx.abort_request_write_scope();
    }

    // Rebinding in frames stays exact whatever each frame binds.
    let fixture = fixture("retention-write-runtime-frames").await;
    let frames = items(
        [
            PropertyValue::F64(f64::NAN),
            PropertyValue::from("B"),
            PropertyValue::Null,
            PropertyValue::from("A"),
        ]
        .into_iter()
        .map(|kind| [("item", node_id(fixture.attribute_a)), ("kind", kind)])
        .collect(),
    );
    let mut ctx = write_context(
        &fixture,
        context::ParamBindings::default().with_value(name("items"), frames),
    )
    .await;
    mutate(
        &mut ctx,
        set_property("kind", "B"),
        &nodes(&[fixture.attribute_b_plain]),
    )
    .await
    .unwrap();
    ctx.flush_active_index_mutations().await.unwrap();
    assert_eq!(
        run_op(&mut ctx, &foreach("items", item_body(equality.clone())))
            .await
            .unwrap(),
        stream(&[fixture.attribute_a])
    );
    ctx.abort_request_write_scope();
}

/// A set on a property without an Active index falls back to per-row
/// evaluation. The fallback reads no index, and follows the same footprint.
#[test]
fn per_row_fallbacks_follow_the_same_footprint() {
    high_stack(per_row_fallback_contract);
}

async fn per_row_fallback_contract() {
    let fixture = fixture("retention-write-fallback").await;
    let op = membership(
        attribute_equality("color", catalog::IndexUniqueness::NonUnique, literal("red")),
        Predicate::eq("color", "red"),
    );
    let mut ctx = write_context(&fixture, context::ParamBindings::default()).await;
    let mut input = live_rows(&fixture, None, None);
    exact(&mut ctx, &op, &input).await;
    assert_eq!(ctx.prepared_memberships.len(), 1);
    input.extend(
        mutate(
            &mut ctx,
            add_node("Note", vec![("color", PropertyValue::from("red"))]),
            &[],
        )
        .await
        .unwrap(),
    );
    assert_eq!(ctx.prepared_memberships.len(), 1);
    exact(&mut ctx, &op, &input).await;
    mutate(
        &mut ctx,
        set_property("color", "red"),
        &nodes(&[fixture.attribute_a]),
    )
    .await
    .unwrap();
    assert_eq!(ctx.prepared_memberships.len(), 0);
    let ExecutionValue::Stream(rows) = exact(&mut ctx, &op, &input).await else {
        panic!("membership streams rows");
    };
    assert_eq!(rows.len(), 2);
    assert_eq!(resolved(&fixture.db), 0);
    ctx.abort_request_write_scope();
}

/// Opening, committing, or aborting the request transaction, an isolated
/// mutation scope, and a failed mutation each forget every set.
#[test]
fn transaction_boundaries_and_failures_forget_every_set() {
    high_stack(transaction_boundaries_contract);
}

async fn transaction_boundaries_contract() {
    let fixture = fixture("retention-write-boundaries").await;
    let op = membership(kind_equality(literal("B")), Predicate::eq("kind", "B"));
    let input = live_rows(&fixture, None, None);

    let mut ctx = write_context(&fixture, context::ParamBindings::default()).await;
    exact(&mut ctx, &op, &input).await;
    ctx.abort_request_write_scope();
    assert_eq!(ctx.prepared_memberships.len(), 0);

    let mut ctx = write_context(&fixture, context::ParamBindings::default()).await;
    exact(&mut ctx, &op, &input).await;
    ctx.commit_request_write_scope().await.unwrap();
    assert_eq!(ctx.prepared_memberships.len(), 0);

    // A failed mutation drops its staged writes with the scope.
    let mut ctx = write_context(&fixture, context::ParamBindings::default()).await;
    exact(&mut ctx, &op, &input).await;
    let missing = mutate(
        &mut ctx,
        exec::ExecMutationPlan::AddEdge {
            label: name("LINK"),
            to: ir::NodeTargetPlan::PointIds {
                ids: test_support::ids(vec![u64::MAX >> 1]),
            },
            properties: test_support::assignments(vec![("kind", PropertyValue::from("B"))]),
        },
        &nodes(&[fixture.attribute_b]),
    )
    .await;
    assert!(missing.is_err());
    assert_eq!(ctx.prepared_memberships.len(), 0);

    // A mutation outside the request transaction commits its own scope.
    let mut ctx = read_context(&fixture, context::ParamBindings::default()).await;
    exact(&mut ctx, &op, &input).await;
    mutate(&mut ctx, set_property("title", "u"), &nodes(&[fixture.group]))
        .await
        .unwrap();
    assert_eq!(ctx.prepared_memberships.len(), 0);
    exact(&mut ctx, &op, &input).await;
    ctx.close_request_read_view().unwrap();
    // Sets read before the request transaction opens came from another
    // snapshot.
    assert_eq!(ctx.prepared_memberships.len(), 1);
    ctx.enable_request_write_scope().await.unwrap();
    assert_eq!(ctx.prepared_memberships.len(), 0);
    ctx.abort_request_write_scope();
}
