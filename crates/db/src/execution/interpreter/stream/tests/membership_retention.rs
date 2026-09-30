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
