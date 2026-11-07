use std::borrow::Cow;
use std::num::NonZeroU64;

use proptest::prelude::*;

use super::super::NeighborSet;
use super::super::{free_in_background, SessionGraph, VECTOR_BUILD_SESSION_BACKGROUND_DROP_BYTES};
use super::*;
use crate::encoding::v2::keys::scope::DataScope;
use crate::index_lifecycle::IndexElementKind;
use crate::search::vector::distance::Euclidean;
use crate::search::vector::{Connections, Layer0Connections, VectorGenerationIdentity};

type Graph = SharedGraph<Euclidean>;
type Overlay = MutationOpCache<Euclidean>;

fn item(value: f32) -> Arc<Item<'static, Euclidean>> {
    let values = [value, value];
    let vector = UnalignedVector::<<Euclidean as Distance>::VectorCodec>::from_slice(&values);
    Arc::new(
        Item::<Euclidean> {
            header: Euclidean::new_header(&vector),
            vector: Cow::Borrowed(&vector),
        }
        .into_owned(),
    )
}

fn overlay() -> Overlay {
    Overlay::with_degree_limits(8, 4)
        .expect("fixture degree limits are valid")
        .into_build_session_cache()
}

fn row(layer: u16, node_id: NodeId) -> NeighborRowId {
    Overlay::node_row_id(layer, node_id)
}

fn nodes(ids: &[NodeId]) -> foldhash::HashSet<NodeId> {
    ids.iter().copied().collect()
}

fn identity(name: &str) -> VectorGenerationIdentity {
    VectorGenerationIdentity::try_new(
        DataScope::LegacyUnscoped,
        1,
        name.to_string(),
        1,
        NonZeroU64::MIN,
        1,
        IndexElementKind::Node,
        VectorDimension::try_new(2).expect("fixture dimension is valid"),
    )
    .expect("fixture generation identity is valid")
}

fn state(jobs: usize, window: usize) -> RunState {
    RunState {
        progress: Mutex::new(RunProgress {
            slots: (0..jobs).map(|_| JobSlot::Waiting).collect(),
            next: 0,
            planned: 0,
        }),
        planner_advanced: Condvar::new(),
        job_finished: Notify::new(),
        cancelled: Arc::new(AtomicBool::new(false)),
        window,
    }
}

fn run(state: RunState) -> PresearchRun<Euclidean> {
    PresearchRun {
        run: Arc::new(state),
        workers: Vec::new(),
        changed: Arc::default(),
    }
}

fn memo(node_id: NodeId, distance: f32) -> DistanceMemo {
    DistanceMemo {
        node_id,
        distances: [((0, 9), distance)].into_iter().collect(),
    }
}

#[test]
fn workers_stay_within_what_the_machine_can_spare() {
    let workers = default_presearch_workers().map_or(0, NonZeroUsize::get);
    assert!(workers <= PRESEARCH_MAX_WORKERS);
}

#[test]
fn only_a_replay_serves_distances_and_only_a_capture_keeps_them() {
    let mut compute = BeamDistances::Compute;
    compute.record(0, 1, 0.5);
    assert_eq!(compute.memoized(0, 1), None);
    assert!(!compute.cancelled());

    let cancelled = Arc::new(AtomicBool::new(false));
    let mut capture = BeamDistances::Capture(DistanceCapture {
        memo: DistanceMemo {
            node_id: 7,
            distances: foldhash::HashMap::default(),
        },
        excluded: Arc::new(nodes(&[2])),
        cancelled: Arc::clone(&cancelled),
    });
    capture.record(1, 1, 0.5);
    capture.record(1, 2, 0.25);
    assert_eq!(capture.memoized(1, 1), None, "a capture serves nothing");
    assert!(!capture.cancelled());
    cancelled.store(true, Ordering::Relaxed);
    assert!(capture.cancelled());

    let BeamDistances::Capture(capture) = capture else {
        unreachable!("the fixture captures");
    };
    let replay = BeamDistances::Replay(capture.memo);
    assert_eq!(replay.memoized(1, 1), Some(0.5));
    assert_eq!(replay.memoized(0, 1), None, "a hit proves its own layer");
    assert_eq!(
        replay.memoized(1, 2),
        None,
        "a changed node is never shared"
    );
    assert!(!replay.cancelled());
}

#[test]
fn absorbing_keeps_only_unchanged_positive_state() {
    let mut read = overlay();
    read.put_item(0, 1, Some(item(1.0)), 16);
    read.put_item(0, 2, Some(item(2.0)), 16);
    read.put_item(0, 3, None, 0);
    read.install_loaded_neighbor(row(0, 1), NeighborRowValue::KnownAbsent);
    read.install_loaded_neighbor(row(0, 2), NeighborRowValue::KnownAbsent);
    read.install_loaded_neighbor(row(1, 4), NeighborRowValue::KnownAbsent);
    read.install_loaded_neighbor(row(0, 5), NeighborRowValue::KnownAbsent);

    let mut graph = Graph::default();
    let retired = graph.absorb(
        &[read],
        &nodes(&[2]),
        &BTreeSet::from([row(0, 5)]),
        usize::MAX,
    );
    assert!(retired.is_none());

    assert_eq!(graph.item(0, 1).map(|(_, age)| age), Some(GraphAge::Young));
    assert!(graph.item(0, 2).is_none(), "a changed node stays out");
    assert!(
        graph.item(0, 3).is_none(),
        "absence is the planner's to prove"
    );
    assert!(graph.neighbors(row(0, 1)).is_some());
    assert!(graph.neighbors(row(0, 2)).is_none(), "a changed node's row");
    assert!(graph.neighbors(row(0, 5)).is_none(), "a changed row");
    assert!(graph.neighbors(row(1, 4)).is_some());
    assert_eq!(graph.max_layer, 1);
}

#[test]
fn forgetting_reaches_every_layer_of_both_generations() {
    let mut read = overlay();
    read.put_item(0, 1, Some(item(1.0)), 16);
    read.put_item(2, 1, Some(item(1.0)), 16);
    read.install_loaded_neighbor(row(2, 1), NeighborRowValue::KnownAbsent);
    let mut graph = Graph::default();
    let _ = graph.absorb(&[read], &nodes(&[]), &BTreeSet::new(), usize::MAX);
    // Age it, then read the same node again.
    let mut read = overlay();
    read.put_item(1, 1, Some(item(1.0)), 16);
    let _ = graph.absorb(&[read], &nodes(&[]), &BTreeSet::new(), 0);
    assert_eq!(graph.item(0, 1).map(|(_, age)| age), Some(GraphAge::Old));
    assert_eq!(graph.item(1, 1).map(|(_, age)| age), Some(GraphAge::Young));

    graph.forget(&nodes(&[1]), &BTreeSet::new());

    assert_eq!(graph.charged_bytes(), 0);
    assert!((0..=2).all(|layer| graph.item(layer, 1).is_none()));
    assert!(graph.neighbors(row(2, 1)).is_none());
}

#[test]
fn a_full_generation_ages_and_the_next_rotation_retires_it() {
    let budget = 2 * (16 + VECTOR_BUILD_SESSION_ITEM_OVERHEAD_BYTES);
    let absorb_one = |graph: &mut Graph, node_id| {
        let mut read = overlay();
        read.put_item(0, node_id, Some(item(1.0)), 16);
        graph
            .absorb(&[read], &nodes(&[]), &BTreeSet::new(), budget)
            .is_some()
    };
    let mut graph = Graph::default();
    assert!(!absorb_one(&mut graph, 1));
    assert!(!absorb_one(&mut graph, 2));
    assert!(absorb_one(&mut graph, 3), "the third read would overflow");
    assert_eq!(
        graph.item(1 - 1, 1).map(|(_, age)| age),
        Some(GraphAge::Old)
    );
    assert_eq!(graph.item(0, 3).map(|(_, age)| age), Some(GraphAge::Young));
    assert!(graph.charged_bytes() <= shared_graph_budget(budget * 8 / 3));
}

proptest! {
    /// Charges stay exact through any mix of reads, ages, and forgets, because
    /// session eviction trusts them.
    #[test]
    fn charges_always_equal_what_the_graph_holds(
        steps in proptest::collection::vec((0_u64..6, 0_u16..3, any::<bool>(), any::<bool>()), 1..64),
        budget in 300_usize..4_000,
    ) {
        let mut graph = Graph::default();
        for (node_id, layer, is_read, with_row) in steps {
            if is_read {
                let mut read = overlay();
                read.put_item(layer, node_id, Some(item(1.0)), 16);
                if with_row {
                    read.install_loaded_neighbor(row(layer, node_id), NeighborRowValue::KnownAbsent);
                }
                let _ = graph.absorb(&[read], &nodes(&[]), &BTreeSet::new(), budget);
            } else {
                graph.forget(&nodes(&[node_id]), &BTreeSet::new());
            }
            let charged = [&graph.young, &graph.old]
                .into_iter()
                .map(|generation| {
                    generation.items.len() * (16 + VECTOR_BUILD_SESSION_ITEM_OVERHEAD_BYTES)
                        + generation.neighbors.values().map(neighbor_charge).sum::<usize>()
                })
                .sum::<usize>();
            prop_assert_eq!(graph.charged_bytes(), charged);
        }
    }
}

#[test]
fn claims_run_in_order_and_stop_at_the_window() {
    let state = Arc::new(state(5, 2));
    assert_eq!(state.claim(), Some(0));
    assert_eq!(state.claim(), Some(1));
    let waiting = {
        let state = Arc::clone(&state);
        std::thread::spawn(move || state.claim())
    };
    std::thread::sleep(std::time::Duration::from_millis(20));
    assert!(!waiting.is_finished(), "job 2 is past the window");
    {
        let mut progress = state.progress.lock();
        progress.planned = 1;
        state.planner_advanced.notify_all();
    }
    assert_eq!(waiting.join().unwrap(), Some(2));
}

#[test]
fn cancelling_releases_a_worker_waiting_at_the_window() {
    let state = Arc::new(state(5, 1));
    assert_eq!(state.claim(), Some(0));
    let waiting = {
        let state = Arc::clone(&state);
        std::thread::spawn(move || state.claim())
    };
    std::thread::sleep(std::time::Duration::from_millis(20));
    state.cancel();
    assert_eq!(waiting.join().unwrap(), None);
    assert_eq!(state.claim(), None, "nothing starts after cancellation");
}

#[tokio::test]
async fn the_planner_overtakes_a_job_no_worker_started() {
    let run = run(state(2, 2));
    assert!(run.take(0).await.is_none());
    assert_eq!(run.run.claim(), Some(1), "a worker skips the planned job");
}

#[tokio::test]
async fn the_planner_takes_a_finished_memo_once() {
    let run = run(state(1, 1));
    assert_eq!(run.run.claim(), Some(0));
    run.run.finish(0, Some(memo(3, 0.5)));
    assert_eq!(run.take(0).await.map(|memo| memo.node_id()), Some(3));
    assert!(run.take(0).await.is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_planner_waits_for_a_running_job() {
    let run = Arc::new(run(state(1, 1)));
    assert_eq!(run.run.claim(), Some(0));
    let planner = {
        let run = Arc::clone(&run);
        tokio::spawn(async move { run.take(0).await })
    };
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    assert!(!planner.is_finished(), "the job is still running");
    run.run.finish(0, Some(memo(3, 0.5)));
    assert_eq!(planner.await.unwrap().map(|memo| memo.node_id()), Some(3));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_panicking_worker_never_strands_the_planner() {
    let run = Arc::new(run(state(1, 1)));
    let state = Arc::clone(&run.run);
    let worker = std::thread::spawn(move || {
        let job = state.claim().unwrap();
        let _guard = JobGuard { run: &state, job };
        panic!("injected worker failure");
    });
    assert!(worker.join().is_err());
    assert!(run.take(0).await.is_none());
}

#[tokio::test]
async fn a_failed_job_leaves_the_planner_to_score() {
    let run = run(state(1, 1));
    assert_eq!(run.run.claim(), Some(0));
    run.run.finish(0, None);
    assert!(run.take(0).await.is_none());
}

#[tokio::test]
async fn a_position_without_a_job_takes_nothing() {
    let run = run(state(1, 1));
    run.run.progress.lock().slots[0] = JobSlot::Planned;
    assert!(run.take(0).await.is_none());
}

#[test]
fn a_finished_guard_keeps_its_memo() {
    let state = state(1, 1);
    let job = state.claim().unwrap();
    JobGuard { run: &state, job }.finish(Some(memo(3, 0.5)));
    assert!(matches!(
        state.progress.lock().slots[0],
        JobSlot::Finished(Some(_))
    ));
}

#[test]
fn dropping_a_run_cancels_it() {
    let run = run(state(1, 1));
    let state = Arc::clone(&run.run);
    drop(run);
    assert_eq!(state.claim(), None);
}

#[test]
fn an_overlay_stays_within_its_payload_budget() {
    let mut read = overlay();
    for node_id in 0..8 {
        read.put_item(0, node_id, Some(item(1.0)), 16);
    }
    read.bound_payload(32);
    assert!(read.retained_payload_bytes <= 32);
    assert_eq!(read.item_count(), 2);
    read.install_loaded_neighbor(row(0, 1), NeighborRowValue::KnownAbsent);
    read.bound_payload(0);
    assert_eq!(read.item_count(), 0);
}

#[test]
fn a_worker_reads_rows_and_keeps_only_what_the_next_rotation_drops() {
    let mut read = overlay();
    read.put_item(0, 1, Some(item(1.0)), 16);
    read.install_loaded_neighbor(row(0, 1), NeighborRowValue::KnownAbsent);
    let mut graph = Graph::default();
    let _ = graph.absorb(&[read], &nodes(&[]), &BTreeSet::new(), usize::MAX);

    let graph = Arc::new(graph);
    let upper = Connections::try_new(4).unwrap();
    let layer0 = Layer0Connections::try_new(8, upper).unwrap();
    let limits = MutationDegreeLimits {
        upper,
        conventional_layer0: layer0,
        layer0,
    };
    let mut worker = Overlay::shared_overlay(limits, Arc::clone(&graph));
    assert!(worker.item(0, 1).flatten().is_some());
    assert!(worker.contains_neighbor(row(0, 1)));
    assert_eq!(worker.item_count(), 0, "a young entry needs no copy");
    worker.release_shared_graph();
    assert!(!worker.contains_neighbor(row(0, 1)));

    let mut aged = Graph::default();
    let mut read = overlay();
    read.put_item(0, 1, Some(item(1.0)), 16);
    let _ = aged.absorb(&[read], &nodes(&[]), &BTreeSet::new(), usize::MAX);
    let _ = aged.absorb(&[overlay()], &nodes(&[]), &BTreeSet::new(), 0);
    let mut worker = Overlay::shared_overlay(limits, Arc::new(aged));
    assert!(worker.item(0, 1).flatten().is_some());
    assert_eq!(
        worker.item_count(),
        1,
        "an old entry stays for another round"
    );
}

#[test]
fn the_planner_reads_items_but_never_shared_rows() {
    let mut read = overlay();
    read.put_item(0, 1, Some(item(1.0)), 16);
    read.install_loaded_neighbor(row(0, 1), NeighborRowValue::KnownAbsent);
    let mut graph = Graph::default();
    let _ = graph.absorb(&[read], &nodes(&[]), &BTreeSet::new(), usize::MAX);

    let mut planner = overlay();
    planner.shared = SharedGraphAccess::Items(Arc::new(graph));
    assert!(planner.item(0, 1).flatten().is_some());
    assert_eq!(planner.item_count(), 0);
    assert!(!planner.contains_neighbor(row(0, 1)));
}

#[test]
fn the_session_charges_its_graph_and_drops_one_that_no_longer_fits() {
    let name = identity("presearch-budget");
    let mut session = VectorBuildSession::<Euclidean>::new(NonZeroU64::new(1 << 20).unwrap());
    let (graph, overlay_bytes) = session.begin_presearch(name.clone(), &nodes(&[]));
    assert_eq!(overlay_bytes, overlay_budget(1 << 20));
    drop(graph);
    let empty = session.retained_bytes().unwrap();

    let mut read = overlay();
    read.put_item(0, 1, Some(item(1.0)), 16);
    session.settle_presearch(&[read], &nodes(&[]), true);
    let charge = 16 + VECTOR_BUILD_SESSION_ITEM_OVERHEAD_BYTES;
    assert_eq!(session.retained_bytes().unwrap(), empty + charge);

    session.shrink_to(1).unwrap();
    assert_eq!(session.retained_bytes().unwrap(), empty);
    assert!(matches!(session.graph, SessionGraph::None));
}

#[test]
fn another_generation_starts_from_an_empty_graph() {
    let mut session = VectorBuildSession::<Euclidean>::new(NonZeroU64::new(1 << 20).unwrap());
    let (graph, _) = session.begin_presearch(identity("first"), &nodes(&[]));
    drop(graph);
    let mut read = overlay();
    read.put_item(0, 1, Some(item(1.0)), 16);
    session.settle_presearch(&[read], &nodes(&[]), true);

    let (graph, _) = session.begin_presearch(identity("second"), &nodes(&[]));
    assert!(graph.item(0, 1).is_none());
}

#[test]
fn a_new_attempt_forgets_its_batch_before_workers_start() {
    let name = identity("presearch-batch");
    let mut session = VectorBuildSession::<Euclidean>::new(NonZeroU64::new(1 << 20).unwrap());
    let (graph, _) = session.begin_presearch(name.clone(), &nodes(&[]));
    drop(graph);
    let mut read = overlay();
    read.put_item(0, 1, Some(item(1.0)), 16);
    read.put_item(0, 2, Some(item(2.0)), 16);
    session.settle_presearch(&[read], &nodes(&[]), true);

    let (graph, _) = session.begin_presearch(name, &nodes(&[1]));
    assert!(graph.item(0, 1).is_none());
    assert!(graph.item(0, 2).is_some());
}

#[test]
fn settling_forgets_rows_the_attempt_changed_and_any_unused_memo() {
    let name = identity("presearch-settle");
    let mut session = VectorBuildSession::<Euclidean>::new(NonZeroU64::new(1 << 20).unwrap());
    let (graph, _) = session.begin_presearch(name.clone(), &nodes(&[]));
    drop(graph);
    let mut read = overlay();
    read.install_loaded_neighbor(row(0, 1), NeighborRowValue::KnownAbsent);
    read.install_loaded_neighbor(row(0, 2), NeighborRowValue::KnownAbsent);
    session.settle_presearch(&[read], &nodes(&[]), true);

    let (graph, _) = session.begin_presearch(name.clone(), &nodes(&[]));
    drop(graph);
    session.record_entity_changes(&name, 9, [row(0, 1)]);
    session.offer_distance_memo(Some(memo(9, 0.5)));
    session.settle_presearch(&[], &nodes(&[]), true);

    assert!(session.next_memo.is_none());
    let SessionGraph::Resting { graph, .. } = &session.graph else {
        panic!("the settled graph rests");
    };
    assert!(graph.neighbors(row(0, 1)).is_none());
    assert!(graph.neighbors(row(0, 2)).is_some());
}

#[test]
fn a_worker_panic_costs_the_graph_but_not_the_session() {
    let name = identity("presearch-panic");
    let mut session = VectorBuildSession::<Euclidean>::new(NonZeroU64::new(1 << 20).unwrap());
    let (graph, _) = session.begin_presearch(name, &nodes(&[]));
    drop(graph);
    session.settle_presearch(&[], &nodes(&[]), false);
    assert!(matches!(session.graph, SessionGraph::None));
}

#[test]
fn a_graph_still_shared_is_dropped_rather_than_mutated() {
    let name = identity("presearch-shared");
    let mut session = VectorBuildSession::<Euclidean>::new(NonZeroU64::new(1 << 20).unwrap());
    let (graph, _) = session.begin_presearch(name, &nodes(&[]));
    session.settle_presearch(&[], &nodes(&[]), true);
    assert!(matches!(session.graph, SessionGraph::None));
    assert_eq!(Arc::strong_count(&graph), 1);
}

#[test]
fn the_planner_lets_go_of_the_graph_so_it_survives_settling() {
    let name = identity("presearch-planner");
    let mut session = VectorBuildSession::<Euclidean>::new(NonZeroU64::new(1 << 20).unwrap());
    let (graph, _) = session.begin_presearch(name.clone(), &nodes(&[]));
    drop(graph);

    let mut cache = session.take_cache(&name, 8, 4).unwrap();
    assert!(matches!(cache.shared, SharedGraphAccess::Items(_)));
    // An entry keeps the cache attached, and so whatever it still holds.
    cache.put_item(0, 1, Some(item(1.0)), 16);
    session.restore_cache(name, cache);

    session.settle_presearch(&[], &nodes(&[]), true);
    assert!(matches!(session.graph, SessionGraph::Resting { .. }));
}

#[test]
fn a_change_outside_an_attempt_drops_the_resting_graph() {
    let name = identity("presearch-unsettled");
    let mut session = VectorBuildSession::<Euclidean>::new(NonZeroU64::new(1 << 20).unwrap());
    let (graph, _) = session.begin_presearch(name.clone(), &nodes(&[]));
    drop(graph);
    session.settle_presearch(&[], &nodes(&[]), true);
    assert!(matches!(session.graph, SessionGraph::Resting { .. }));

    let cache = session.take_cache(&name, 8, 4).unwrap();
    assert!(
        matches!(cache.shared, SharedGraphAccess::None),
        "nothing plans against a resting graph"
    );
    session.restore_cache(name.clone(), cache);

    session.record_entity_changes(&name, 1, []);
    assert!(matches!(session.graph, SessionGraph::None));
}

#[test]
fn a_session_with_no_attempt_journals_no_rows() {
    let name = identity("presearch-build");
    let mut session = VectorBuildSession::<Euclidean>::new(NonZeroU64::new(1 << 20).unwrap());
    session.record_entity_changes(&name, 1, [row(0, 1)]);
    assert!(matches!(session.graph, SessionGraph::None));
}

#[test]
fn a_large_retired_graph_frees_off_the_caller() {
    let held = Arc::new(());
    let weak = Arc::downgrade(&held);
    free_in_background(held, VECTOR_BUILD_SESSION_BACKGROUND_DROP_BYTES);
    let freed = std::time::Instant::now();
    while weak.strong_count() > 0 {
        assert!(freed.elapsed() < std::time::Duration::from_secs(10));
        std::thread::yield_now();
    }
}

#[test]
fn settling_refreshes_the_graph_from_what_the_attempt_planned() {
    let name = identity("presearch-refresh");
    let mut session = VectorBuildSession::<Euclidean>::new(NonZeroU64::new(1 << 20).unwrap());
    let (graph, _) = session.begin_presearch(name.clone(), &nodes(&[]));
    drop(graph);
    let mut read = overlay();
    read.install_loaded_neighbor(row(0, 2), NeighborRowValue::KnownAbsent);
    read.install_loaded_neighbor(row(0, 3), NeighborRowValue::KnownAbsent);
    session.settle_presearch(&[read], &nodes(&[]), true);

    let (graph, _) = session.begin_presearch(name.clone(), &nodes(&[1]));
    drop(graph);
    let mut planned = session.take_cache(&name, 8, 4).unwrap();
    planned.put_item(0, 1, Some(item(5.0)), 16);
    planned.put_item(2, 1, Some(item(5.0)), 16);
    let present = NeighborSet::try_from_deployed(2, planned.degree_limit(0), vec![1]).unwrap();
    planned.install_loaded_neighbor(row(0, 1), NeighborRowValue::KnownAbsent);
    planned.install_loaded_neighbor(row(0, 2), NeighborRowValue::Present(present.clone()));
    session.restore_cache(name.clone(), planned);
    session.record_entity_changes(&name, 1, [row(0, 2), row(0, 3)]);
    session.settle_presearch(&[], &nodes(&[1]), true);

    let SessionGraph::Resting { graph, .. } = &session.graph else {
        panic!("the settled graph rests");
    };
    assert!(
        graph.item(0, 1).is_some(),
        "a changed node comes back current"
    );
    assert!(graph.item(2, 1).is_some(), "at every layer it holds");
    assert_eq!(graph.max_layer, 2);
    assert!(graph.neighbors(row(0, 1)).is_some(), "with its own row");
    assert_eq!(
        graph.neighbors(row(0, 2)).map(|(value, _)| value),
        Some(&NeighborRowValue::Present(present)),
        "a changed row comes back current"
    );
    assert!(
        graph.neighbors(row(0, 3)).is_none(),
        "a row the planner no longer holds stays forgotten"
    );
}

#[test]
fn a_dropped_session_frees_its_graph_with_its_other_state() {
    let name = identity("presearch-drop");
    let mut session = VectorBuildSession::<Euclidean>::new(NonZeroU64::new(1 << 30).unwrap());
    let (graph, _) = session.begin_presearch(name, &nodes(&[]));
    drop(graph);
    let mut read = overlay();
    read.put_item(
        0,
        1,
        Some(item(1.0)),
        VECTOR_BUILD_SESSION_BACKGROUND_DROP_BYTES,
    );
    session.settle_presearch(&[read], &nodes(&[]), true);

    let freeing = session
        .free_state_in_background()
        .expect("a large session frees on its own thread");
    freeing.join().unwrap();
    assert!(matches!(session.graph, SessionGraph::None));
}
