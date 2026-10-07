//! Index-set vector search fusion. The planner's filtered node vector
//! searches reach the scheduler as an ID-set access feeding a vector search;
//! run fused, they return exactly what running the two steps returns, and
//! every shape that could observe the difference runs unfused.
use helix_ast::{batch, expr, graph, query, traversal, value};
use helix_planner::{catalog, planning};

use super::*;
use crate::encoding::keys::scope::DataScope;
use crate::search;

/// Twelve `Doc` nodes. The node of rank `r` sits at `[r, r % 3]`, so its
/// squared distance from the origin grows with `r`; it is in category `a`
/// when `r` is even and `open` while `r < 6`.
async fn seed(config: test_support::TestDbConfig) -> (HelixDB, Vec<u64>) {
    let db = test_support::open_db_with_config(
        config
            .with_node_vector_index(
                "Doc",
                "embedding",
                2,
                search::vector::VectorDistanceMetric::Euclidean,
            )
            .with_equality_index("Doc", "category")
            .with_equality_index("Doc", "status")
            .with_range_index("Doc", "rank"),
    )
    .await;
    let seed = (0..12_i64)
        .fold(batch::write_batch(), |write, rank| {
            write.var_as(
                &format!("d{rank}"),
                traversal::g().add_n(
                    "Doc",
                    vec![
                        (
                            "embedding",
                            value::PropertyInput::from(vec![rank as f32, (rank % 3) as f32]),
                        ),
                        (
                            "category",
                            value::PropertyInput::from(if rank % 2 == 0 { "a" } else { "b" }),
                        ),
                        (
                            "status",
                            value::PropertyInput::from(if rank < 6 { "open" } else { "closed" }),
                        ),
                        ("rank", value::PropertyInput::from(rank)),
                    ],
                ),
            )
        })
        .returning((0..12).map(|rank| format!("d{rank}")));
    let created = db.query(query::QueryRequest::write(seed)).await.unwrap();
    let ids = (0..12)
        .map(|rank| created[format!("d{rank}")][0]["$id"].as_u64().unwrap())
        .collect();
    (db, ids)
}

fn docs(predicate: expr::Predicate) -> traversal::Traversal<traversal::OnNodes> {
    traversal::g().n_with_label_where("Doc", predicate)
}

fn search(source: traversal::Traversal<traversal::OnNodes>, k: usize) -> batch::ReadBatch {
    batch::read_batch()
        .var_as(
            "hits",
            source.vector_search("Doc", "embedding", vec![0.0, 0.0], k, None),
        )
        .returning(["hits"])
}

async fn plan_read(db: &HelixDB, read: &batch::ReadBatch) -> exec::ExecutablePlan {
    let prepared = db
        .planner_context_scoped_prepared(
            context::ParamBindings::default(),
            DataScope::LegacyUnscoped,
        )
        .await
        .unwrap();
    planning::plan_read_batch(read, prepared.context()).unwrap()
}

/// How a test runs a plan's DAG.
#[derive(Debug, Clone, Copy)]
enum Run {
    /// Through the scheduler, which fuses what it can.
    Scheduled,
    /// Every step on its own in execution order, as the scheduler ran them
    /// before fusion.
    StepByStep,
}

/// Runs `plan` in a read view and returns its bound `hits` and `others`, and
/// the work it counted.
async fn run(
    db: &HelixDB,
    plan: &exec::ExecutablePlan,
    budget: Option<crate::query_resources::Budget>,
    how: Run,
) -> (
    Result<Vec<ExecutionValue>>,
    crate::execution::interpreter::pull::metrics::WorkSnapshot,
) {
    let mut ctx = ExecutionContext::new(db, context::ParamBindings::default());
    ctx.enable_request_read_view().await.unwrap();
    ctx.row_memory = budget;
    let ran = match how {
        Run::Scheduled => {
            ctx.execute_steps(
                plan.steps(),
                plan.execution_order(),
                plan.root(),
                plan.execution_program(),
            )
            .await
        }
        Run::StepByStep => step_by_step(&mut ctx, plan).await,
    };
    let bound = ran.map(|()| {
        ["hits", "others"]
            .into_iter()
            .filter_map(|variable| ctx.variables.get(&named(variable)).cloned())
            .collect()
    });
    (bound, ctx.pull_work.snapshot())
}

async fn step_by_step(ctx: &mut ExecutionContext<'_>, plan: &exec::ExecutablePlan) -> Result<()> {
    ctx.initialize_step_output_uses(plan.steps(), plan.root())?;
    let steps = by_id(plan.steps());
    for id in plan.execution_order().step_ids() {
        let step = steps[&id];
        let value = ctx.execute_step(step, None).await?;
        ctx.record_step_output(step, value);
    }
    Ok(())
}

/// The nodes of `ranks` in order, each with a distance.
fn assert_hits(hits: &ExecutionValue, ids: &[u64], ranks: &[usize]) {
    let ExecutionValue::Stream(rows) = hits else {
        panic!("vector search returns rows: {hits:?}");
    };
    assert_eq!(
        rows.iter()
            .map(|row| row.current.clone())
            .collect::<Vec<_>>(),
        ranks
            .iter()
            .map(|rank| Some(ElementRef::Node(ids[*rank])))
            .collect::<Vec<_>>()
    );
    assert!(rows.iter().all(|row| matches!(
        row.virtual_properties.get(&named("$distance")),
        Some(DbPropertyValue::F64(_))
    )));
}

/// The access kind of the plan's only access step, which feeds its only
/// vector search.
fn fused_access(plan: &exec::ExecutablePlan) -> &exec::ExecNodeAccessPlan {
    let [access, search] = plan.steps() else {
        panic!("expected an access and a search: {:#?}", plan.steps());
    };
    let (exec::ExecOp::Access { plan: access_plan }, exec::ExecOp::VectorSearch { .. }) =
        (&access.op, &search.op)
    else {
        panic!("expected an access and a search: {:#?}", plan.steps());
    };
    let exec::ExecAccessPlan::Node(access_plan) = access_plan.as_ref() else {
        panic!("expected a node access: {access_plan:?}");
    };
    assert_eq!(search.dependencies, vec![access.id]);
    let fused = fusion::plan(plan.steps(), plan.root(), plan.execution_program());
    assert!(matches!(fused.get(&access.id), Some(fusion::Role::Source)));
    assert!(matches!(
        fused.get(&search.id),
        Some(fusion::Role::Search(_))
    ));
    access_plan
}

/// Label-only, equality and secondary-set filters all reach the interpreter
/// as one index-set access feeding one node vector search, which the
/// scheduler fuses. Fused, each returns exactly the rows, order and distances
/// of its two steps run one by one, which are the exact nearest members of
/// its set, and reads no access rows. A set smaller than `k` returns all of
/// it, and an empty set returns nothing.
#[tokio::test]
async fn fused_searches_match_their_steps_for_every_index_set() {
    let (db, ids) = seed(test_support::in_memory_config("fused-vector-equivalence")).await;
    type Kind = fn(&exec::ExecNodeAccessPlan) -> bool;
    let label: Kind = |access| matches!(access, exec::ExecNodeAccessPlan::LabelScan { .. });
    let bitmap: Kind = |access| matches!(access, exec::ExecNodeAccessPlan::Bitmap { .. });
    let secondary: Kind = |access| matches!(access, exec::ExecNodeAccessPlan::SecondarySet { .. });
    let cases: [(
        &str,
        traversal::Traversal<traversal::OnNodes>,
        usize,
        Kind,
        &[usize],
    ); 8] = [
        (
            "label",
            traversal::g().n_with_label("Doc"),
            4,
            label,
            &[0, 1, 2, 3],
        ),
        (
            "equality",
            docs(expr::Predicate::eq("category", "a")),
            4,
            bitmap,
            &[0, 2, 4, 6],
        ),
        (
            "intersection smaller than k",
            docs(expr::Predicate::and(vec![
                expr::Predicate::eq("category", "a"),
                expr::Predicate::eq("status", "open"),
            ])),
            50,
            secondary,
            &[0, 2, 4],
        ),
        (
            "union",
            docs(expr::Predicate::or(vec![
                expr::Predicate::eq("category", "b"),
                expr::Predicate::eq("status", "closed"),
            ])),
            4,
            secondary,
            &[1, 3, 5, 6],
        ),
        (
            "equality and range",
            docs(expr::Predicate::and(vec![
                expr::Predicate::eq("category", "a"),
                expr::Predicate::gt("rank", 3_i64),
            ])),
            3,
            secondary,
            &[4, 6, 8],
        ),
        (
            "range membership",
            docs(expr::Predicate::is_in(
                "rank",
                value::PropertyValue::I64Array(vec![9, 3, 7]),
            )),
            4,
            secondary,
            &[3, 7, 9],
        ),
        (
            "empty equality",
            docs(expr::Predicate::eq("category", "none")),
            4,
            bitmap,
            &[],
        ),
        (
            "empty intersection",
            docs(expr::Predicate::and(vec![
                expr::Predicate::eq("category", "b"),
                expr::Predicate::eq("status", "none"),
            ])),
            4,
            secondary,
            &[],
        ),
    ];
    for (name, source, k, kind, ranks) in cases {
        let read = search(source, k);
        let plan = plan_read(&db, &read).await;
        let access = fused_access(&plan);
        assert!(kind(access), "{name}: {:#?}", plan.steps());
        let pulls_rows = !matches!(access, exec::ExecNodeAccessPlan::SecondarySet { .. });

        let (fused, fused_work) = run(&db, &plan, None, Run::Scheduled).await;
        let (unfused, unfused_work) = run(&db, &plan, None, Run::StepByStep).await;
        let fused = fused.unwrap();
        assert_eq!(fused, unfused.unwrap(), "{name}");
        assert_hits(&fused[0], &ids, ranks);
        assert_eq!(fused_work.fused_vector_searches, 1, "{name}");
        assert_eq!(unfused_work.fused_vector_searches, 0, "{name}");
        // Unfused label and bitmap accesses pull one row per ID.
        assert_eq!(fused_work.source_visits, 0, "{name}");
        if pulls_rows {
            assert!(unfused_work.source_visits >= ranks.len(), "{name}");
        }

        // The public request takes the same path.
        let response = db.query(query::QueryRequest::read(read)).await.unwrap();
        assert_eq!(
            response["hits"]
                .as_array()
                .unwrap()
                .iter()
                .map(|hit| hit["$id"].as_u64().unwrap())
                .collect::<Vec<_>>(),
            ranks.iter().map(|rank| ids[*rank]).collect::<Vec<_>>(),
            "{name}"
        );
    }
}

/// The set is read when the search runs: deleted nodes are gone, updated
/// filter values and vectors are current, and fused results still match the
/// steps run one by one.
#[tokio::test]
async fn fused_searches_see_deleted_and_updated_nodes() {
    let (db, ids) = seed(test_support::in_memory_config("fused-vector-mutated")).await;
    for request in [
        batch::write_batch().var_as(
            "dropped",
            traversal::g().n(graph::NodeRef::from(ids[0])).drop(),
        ),
        batch::write_batch().var_as(
            "moved",
            traversal::g()
                .n(graph::NodeRef::from(ids[2]))
                .set_property("category", "b"),
        ),
        batch::write_batch().var_as(
            "far",
            traversal::g()
                .n(graph::NodeRef::from(ids[4]))
                .set_property("embedding", vec![100.0_f32, 100.0]),
        ),
    ] {
        db.query(query::QueryRequest::write(request)).await.unwrap();
    }
    for (source, ranks) in [
        (traversal::g().n_with_label("Doc"), [1, 2, 3, 5]),
        (docs(expr::Predicate::eq("category", "a")), [6, 8, 10, 4]),
    ] {
        let plan = plan_read(&db, &search(source, 4)).await;
        fused_access(&plan);
        let (fused, work) = run(&db, &plan, None, Run::Scheduled).await;
        let fused = fused.unwrap();
        assert_eq!(work.fused_vector_searches, 1);
        assert_eq!(
            fused,
            run(&db, &plan, None, Run::StepByStep).await.0.unwrap()
        );
        assert_hits(&fused[0], &ids, &ranks);
    }
}

/// Joins two planned reads into one DAG whose root stores both results as
/// `hits`, so their steps can share stages. Batch statements run in order,
/// so the planner never puts two of them in one stage.
fn join(first: &exec::ExecutablePlan, second: &exec::ExecutablePlan) -> Vec<exec::ExecStep> {
    let offset = first.steps().len();
    let mut steps = first
        .steps()
        .iter()
        .cloned()
        .chain(second.steps().iter().map(|step| {
            exec::ExecStep {
                id: id(step.id.get() + offset),
                dependencies: step
                    .dependencies
                    .iter()
                    .map(|dependency| id(dependency.get() + offset))
                    .collect(),
                ..step.clone()
            }
        }))
        .map(|step| exec::ExecStep {
            output: ir::BatchOutputPlan::Discard,
            ..step
        })
        .collect::<Vec<_>>();
    // A stored variable is a pull boundary, so neither read joins a region
    // through the root.
    steps.push(test_support::step(
        steps.len() + 1,
        vec![first.root(), id(second.root().get() + offset)],
        exec::ExecOp::Variable {
            op: exec::ExecVariableOp::Stream(ir::StreamVariableOp::Store(named("hits"))),
        },
    ));
    steps
}

fn parallel_stages(plan: &exec::ExecutablePlan) -> usize {
    plan.execution_order()
        .stages()
        .iter()
        .filter(|stage| matches!(stage, exec::ExecExecutionStage::Parallel(_)))
        .count()
}

/// Fused accesses in a parallel stage, and fused searches in one, each run
/// in their own step context and match their steps.
#[tokio::test]
async fn parallel_stages_fuse_each_search() {
    let config = test_support::in_memory_config("fused-vector-parallel");
    let (writer, ids) = seed(config.clone()).await;
    writer.flush_writer().await.unwrap();
    let reader = test_support::open_reader_with_config(config).await;
    assert!(!reader.is_writer_mode());
    let steps = join(
        &plan_read(
            &reader,
            &search(docs(expr::Predicate::eq("category", "a")), 2),
        )
        .await,
        &plan_read(
            &reader,
            &search(docs(expr::Predicate::eq("category", "b")), 2),
        )
        .await,
    );
    // The planner runs searches alone after their parallel accesses; a
    // pipelined search pair runs in parallel too.
    for (search_schedule, parallel) in [
        (exec::ExecSchedule::Barrier, 1),
        (exec::ExecSchedule::Pipeline, 2),
    ] {
        let mut steps = steps.clone();
        assert_eq!(steps[1].schedule, exec::ExecSchedule::Barrier);
        steps[1].schedule = search_schedule.clone();
        steps[3].schedule = search_schedule;
        let plan = test_support::executable(ir::PlanKind::Read, steps, 5);
        assert_eq!(parallel_stages(&plan), parallel);
        assert_eq!(
            fusion::plan(plan.steps(), plan.root(), plan.execution_program()).len(),
            4
        );

        let (fused, work) = run(&reader, &plan, None, Run::Scheduled).await;
        assert_eq!(work.fused_vector_searches, 2);
        let fused = fused.unwrap();
        assert_eq!(
            fused,
            run(&reader, &plan, None, Run::StepByStep).await.0.unwrap()
        );
        assert_hits(&fused[0], &ids, &[0, 2, 1, 3]);
    }
}

/// A fused pair sharing stages with a pull region runs through the region
/// stage paths, serial on a writer and parallel on a reader, and still
/// matches its steps.
#[tokio::test]
async fn fused_pairs_run_beside_pull_regions() {
    let config = test_support::in_memory_config("fused-vector-regions");
    let (writer, ids) = seed(config.clone()).await;
    let fused_read = plan_read(
        &writer,
        &search(docs(expr::Predicate::eq("category", "a")), 2),
    )
    .await;
    let region_read = plan_read(
        &writer,
        &batch::read_batch()
            .var_as(
                "hits",
                docs(expr::Predicate::eq("category", "b"))
                    .vector_search("Doc", "embedding", vec![0.0, 0.0], 3, None)
                    .limit(2),
            )
            .returning(["hits"]),
    )
    .await;
    assert_eq!(region_read.execution_program().regions().count(), 1);
    let steps = join(&fused_read, &region_read);
    let root = steps.len();
    writer.flush_writer().await.unwrap();
    let reader = test_support::open_reader_with_config(config).await;
    for db in [&writer, &reader] {
        for search_schedule in [exec::ExecSchedule::Barrier, exec::ExecSchedule::Pipeline] {
            let mut steps = steps.clone();
            steps[1].schedule = search_schedule;
            let plan = test_support::executable(ir::PlanKind::Read, steps, root);
            assert!(parallel_stages(&plan) >= 1);
            assert!(plan.execution_order().stages().iter().any(|stage| stage
                .iter()
                .any(|id| plan.execution_program().is_absorbed(id))
                && stage
                    .iter()
                    .any(|id| id == plan.steps()[0].id || id == plan.steps()[1].id)));
            assert_eq!(
                fusion::plan(plan.steps(), plan.root(), plan.execution_program()).len(),
                2
            );

            let (fused, work) = run(db, &plan, None, Run::Scheduled).await;
            assert_eq!(work.fused_vector_searches, 1);
            let fused = fused.unwrap();
            assert_eq!(
                fused,
                run(db, &plan, None, Run::StepByStep).await.0.unwrap()
            );
            assert_hits(&fused[0], &ids, &[0, 2, 1, 3]);
        }
    }
}

/// Under a request memory budget the fused set is admitted while the search
/// runs and released after it. A budget too small for the set fails the
/// request as running the steps does.
#[tokio::test]
async fn fused_searches_are_admitted_under_the_request_budget() {
    let (db, ids) = seed(test_support::in_memory_config("fused-vector-budget")).await;
    for (source, ranks) in [
        (traversal::g().n_with_label("Doc"), &[0, 1]),
        (docs(expr::Predicate::eq("category", "a")), &[0, 2]),
        (
            docs(expr::Predicate::and(vec![
                expr::Predicate::eq("category", "a"),
                expr::Predicate::eq("status", "open"),
            ])),
            &[0, 2],
        ),
    ] {
        let plan = plan_read(&db, &search(source, 2)).await;
        fused_access(&plan);
        let budget = crate::query_resources::Budget::new(64 << 20);
        let (fused, work) = run(&db, &plan, Some(budget.clone()), Run::Scheduled).await;
        let fused = fused.unwrap();
        assert_eq!(work.fused_vector_searches, 1);
        assert!(budget.peak() > 0, "the set is admitted");
        assert_eq!(budget.available(), 64 << 20, "and released");
        let (unfused, _) = run(
            &db,
            &plan,
            Some(crate::query_resources::Budget::new(64 << 20)),
            Run::StepByStep,
        )
        .await;
        assert_eq!(fused, unfused.unwrap());
        assert_hits(&fused[0], &ids, ranks);

        let (fused, _) = run(
            &db,
            &plan,
            Some(crate::query_resources::Budget::new(1)),
            Run::Scheduled,
        )
        .await;
        let (unfused, _) = run(
            &db,
            &plan,
            Some(crate::query_resources::Budget::new(1)),
            Run::StepByStep,
        )
        .await;
        let fused = fused.expect_err("a one-byte budget cannot hold the set");
        assert!(
            matches!(fused, HelixDbError::QueryMemoryLimitExceeded),
            "{fused}"
        );
        assert_eq!(fused.to_string(), unfused.unwrap_err().to_string());
    }
}

/// Searches whose access is not an index ID set, that sit in a pull region,
/// or that run in a request that writes, run as planned.
#[tokio::test]
async fn planned_shapes_that_must_not_fuse_run_unfused() {
    let (db, ids) = seed(test_support::in_memory_config("fused-vector-unfused")).await;
    for (name, read, ranks) in [
        (
            "range scan",
            search(docs(expr::Predicate::gt("rank", 8_i64)), 2),
            &[9, 10][..],
        ),
        (
            "filtered label",
            search(docs(expr::Predicate::eq("unindexed", "x")), 2),
            &[][..],
        ),
        (
            "null equality",
            search(
                docs(expr::Predicate::eq("category", value::PropertyValue::Null)),
                2,
            ),
            &[][..],
        ),
        (
            "pull region",
            batch::read_batch()
                .var_as(
                    "hits",
                    docs(expr::Predicate::eq("category", "a"))
                        .vector_search("Doc", "embedding", vec![0.0, 0.0], 3, None)
                        .limit(2),
                )
                .returning(["hits"]),
            &[0, 2][..],
        ),
    ] {
        let plan = plan_read(&db, &read).await;
        assert!(
            fusion::plan(plan.steps(), plan.root(), plan.execution_program()).is_empty(),
            "{name}: {:#?}",
            plan.steps()
        );
        let (scheduled, work) = run(&db, &plan, None, Run::Scheduled).await;
        let scheduled = scheduled.unwrap();
        assert_eq!(work.fused_vector_searches, 0, "{name}");
        assert_eq!(
            scheduled,
            run(&db, &plan, None, Run::StepByStep).await.0.unwrap(),
            "{name}"
        );
        assert_hits(&scheduled[0], &ids, ranks);
    }

    let write = batch::write_batch()
        .var_as(
            "created",
            traversal::g().add_n(
                "Doc",
                vec![
                    ("embedding", value::PropertyInput::from(vec![0.5_f32, 0.0])),
                    ("category", value::PropertyInput::from("a")),
                ],
            ),
        )
        .var_as(
            "hits",
            docs(expr::Predicate::eq("category", "a")).vector_search(
                "Doc",
                "embedding",
                vec![0.0, 0.0],
                2,
                None,
            ),
        )
        .returning(["created", "hits"]);
    let prepared = db
        .planner_context_scoped_prepared(
            context::ParamBindings::default(),
            DataScope::LegacyUnscoped,
        )
        .await
        .unwrap();
    let plan = planning::plan_write_batch(&write, prepared.context()).unwrap();
    assert!(plan.steps().iter().any(|step| matches!(
        &step.op,
        exec::ExecOp::Access { plan } if matches!(
            plan.as_ref(),
            exec::ExecAccessPlan::Node(exec::ExecNodeAccessPlan::Bitmap { .. })
        )
    )));
    assert!(fusion::plan(plan.steps(), plan.root(), plan.execution_program()).is_empty());
    let response = db.query(query::QueryRequest::write(write)).await.unwrap();
    let created = response["created"][0]["$id"].as_u64().unwrap();
    assert_eq!(
        response["hits"]
            .as_array()
            .unwrap()
            .iter()
            .map(|hit| hit["$id"].as_u64().unwrap())
            .collect::<Vec<_>>(),
        vec![ids[0], created]
    );
}

/// The pairing rule on its own: starting from a planned fused pair, every
/// change that lets something else observe the access, skips one half, or
/// leaves the node ID-set and node-search shapes unpairs it.
#[tokio::test]
async fn pairing_requires_an_exclusive_unconditional_index_set_and_node_search() {
    let (db, _) = seed(test_support::in_memory_config("fused-vector-pairing")).await;
    let plan = plan_read(&db, &search(docs(expr::Predicate::eq("category", "a")), 2)).await;
    fused_access(&plan);
    let planned = plan.steps().to_vec();
    let (source, searched) = (planned[0].id, planned[1].id);
    let root = plan.root();
    assert_eq!(root, searched);
    let program = exec::ExecProgram::default();
    let pairs = |steps: &[exec::ExecStep], root| fusion::plan(steps, root, &program).len();
    assert_eq!(pairs(&planned, root), 2);
    assert_eq!(pairs(&planned[..1], source), 0, "an access alone");

    let consumer = move |condition| exec::ExecStep {
        condition,
        ..test_support::step(3, vec![source], exec::ExecOp::Noop)
    };
    let conditional =
        exec::ExecCondition::Variable(ir::BatchVariableConditionPlan::VarNotEmpty(named("gate")));
    let edge_search = {
        let exec::ExecOp::VectorSearch { plan } = &planned[1].op else {
            unreachable!()
        };
        let ir::RestrictedVectorSearchPlan::Nodes {
            index,
            query_vector,
            k,
            ..
        } = plan.as_ref()
        else {
            unreachable!()
        };
        exec::ExecOp::VectorSearch {
            plan: Box::new(ir::RestrictedVectorSearchPlan::Edges {
                key: catalog::EdgeSearchIndexKey::try_new("Doc", "embedding").unwrap(),
                index: index.clone(),
                query_vector: query_vector.clone(),
                k: k.clone(),
            }),
        }
    };
    let access = |plan| exec::ExecOp::Access {
        plan: Box::new(plan),
    };
    let exec::ExecOp::Access { plan: bitmap } = &planned[0].op else {
        unreachable!()
    };
    let bitmap = bitmap.as_ref().clone();
    type Change = Box<dyn Fn(&mut Vec<exec::ExecStep>)>;
    let changes: Vec<(&str, Change)> = vec![
        (
            "second consumer",
            Box::new(move |steps| steps.push(consumer(exec::ExecCondition::Always))),
        ),
        (
            "condition observer",
            Box::new(move |steps| {
                steps.push(exec::ExecStep {
                    dependencies: Vec::new(),
                    ..consumer(exec::ExecCondition::PreviousStepNotEmpty { dependency: source })
                })
            }),
        ),
        (
            "bound access",
            Box::new(|steps| steps[0].output = ir::BatchOutputPlan::Bind(named("docs"))),
        ),
        ("conditional access", {
            let conditional = conditional.clone();
            Box::new(move |steps| steps[0].condition = conditional.clone())
        }),
        ("conditional search", {
            let conditional = conditional.clone();
            Box::new(move |steps| steps[1].condition = conditional.clone())
        }),
        (
            "second search input",
            Box::new(move |steps| {
                steps.push(test_support::step(3, Vec::new(), exec::ExecOp::Noop));
                steps[1].dependencies.push(id(3));
            }),
        ),
        (
            "search over a filter",
            Box::new(move |steps| {
                steps.push(test_support::step(3, vec![source], exec::ExecOp::Noop));
                steps[1].dependencies = vec![id(3)];
            }),
        ),
        (
            "edge search",
            Box::new(move |steps| steps[1].op = edge_search.clone()),
        ),
        (
            "full scan",
            Box::new(move |steps| {
                steps[0].op = access(exec::ExecAccessPlan::Node(
                    exec::ExecNodeAccessPlan::AllScan,
                ))
            }),
        ),
        (
            "limited set",
            Box::new(move |steps| {
                steps[0].op = access(bitmap.clone().limited_by(exec::ExecAccessLimit::Static(
                    properties::PositiveUsize::new(1).unwrap(),
                )))
            }),
        ),
        (
            "edge set",
            Box::new(move |steps| {
                steps[0].op = access(exec::ExecAccessPlan::Edge(
                    exec::ExecEdgeAccessPlan::LabelScan {
                        label: named("Doc"),
                    },
                ))
            }),
        ),
        (
            "writing step",
            Box::new(|steps| {
                steps.push(test_support::step(
                    3,
                    Vec::new(),
                    exec::ExecOp::Mutation {
                        plan: exec::ExecMutationPlan::Drop,
                    },
                ))
            }),
        ),
    ];
    for (name, change) in changes {
        let mut steps = planned.clone();
        change(&mut steps);
        assert_eq!(pairs(&steps, root), 0, "{name}");
    }
    assert_eq!(pairs(&planned, source), 0, "the access as the root");

    // A pull region drives its own steps.
    let limited = plan_read(
        &db,
        &batch::read_batch()
            .var_as(
                "hits",
                docs(expr::Predicate::eq("category", "a"))
                    .vector_search("Doc", "embedding", vec![0.0, 0.0], 3, None)
                    .limit(2),
            )
            .returning(["hits"]),
    )
    .await;
    let limited_source = limited
        .steps()
        .iter()
        .find(|step| matches!(step.op, exec::ExecOp::Access { .. }))
        .unwrap()
        .id;
    assert!(limited.execution_program().is_absorbed(limited_source));
    assert!(fusion::plan(limited.steps(), limited.root(), limited.execution_program()).is_empty());
}
