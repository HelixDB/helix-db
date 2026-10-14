//! Batch-write benchmark over a synthetic `Group -[HAS]-> Item` graph.
//!
//! `Item` has equality indexes on the broad `kind` (four values) and `status`
//! (three values), so every value is a large bitmap. Each workload request
//! filters the items of a group, reached by an expansion, on
//! `$label == Item && kind == k1 && status == active && uid == <probe>`: the
//! index membership set is the `kind` and `status` bitmaps, and the probe is a
//! per-row residual. Every matched item gets a `LINK` edge to the request's
//! target item, and the harness checks the target's `LINK` count against its
//! own model of the fixture after every request.
//!
//! Workloads, each timed at every batch size (items per request):
//! - `a`: a `ForEach` whose frames filter, then link existing nodes. Only
//!   edges are written, so no frame can change a membership set.
//! - `b`: a `ForEach` whose frames first create an `Item` (indexed label and
//!   properties) in a group, then filter that group: every frame's write can
//!   change the set, and some frames probe the item they just created.
//! - `c`: one statement pair per item, without `ForEach`: an update of the
//!   unindexed `touched` property of an `Item`, then the filter and link.
//!   Each statement probes its own residual parameter, but every filter
//!   decides the same `kind` and `status` set, and the update reads no index
//!   the set does, so a server that keeps sets across writes reads it once
//!   per request, and one that forgets them on every write reads it once per
//!   statement.
//!
//! Modes (`BENCH_HTTP_URL` or an embedded backend, see `main.rs`):
//! - `batch-load`: indexes, groups, then items in `ForEach` chunks
//!   (`BATCH_LOAD_CHUNK`, `BATCH_LOAD_CONCURRENCY`), then a full check of the
//!   loaded graph against the model.
//! - `batch-run`: needs a fresh copy of a `batch-load` database with the same
//!   `BATCH_ITEMS` and `BATCH_GROUP_SIZE`; it fails when the graph differs
//!   from the model. Runs `BATCH_ROUNDS` rounds of every workload
//!   (`BATCH_WORKLOADS`, default `a,b,c`) and batch size (`BATCH_SIZES`,
//!   default `1,10,100,500`). A cell sends `BATCH_WARMUP` unmeasured requests,
//!   then `BATCH_ITEM_BUDGET / size` requests clamped to
//!   `BATCH_MIN_REQUESTS..=BATCH_MAX_REQUESTS`.
//!
//! Every cell of every round prints a `BATCH_RESULT {json}` line, and the run
//! ends with a summary across rounds and `BATCH_RUN_OK`. Only query features
//! of the v0.0.7 server are used, so old and new servers run the same requests.
//!
//! ```text
//! BENCH_HTTP_URL=http://127.0.0.1:8090 BATCH_ITEMS=300000 scoped_search_bench batch-load
//! BENCH_HTTP_URL=http://127.0.0.1:8090 BATCH_ITEMS=300000 scoped_search_bench batch-run
//! ```

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use futures::stream::{self, StreamExt};
use helix_ast::prelude::*;

use crate::fixture::{self, Backend, Rng};

/// Seed of the loaded graph; the workload draws use a derived seed.
const SEED: u64 = 0x5EED_BA7C;
const TARGET_KIND: Kind = Kind::K1;
const TARGET_STATUS: Status = Status::Active;
/// Concurrent reads when the harness reads the loaded graph back.
const READ_CONCURRENCY: usize = 16;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Kind {
    K0,
    K1,
    K2,
    K3,
}

impl Kind {
    /// Shares of loaded items: even the rarest value is a tenth of the label.
    const WEIGHTS: [(Self, f64); 4] = [
        (Self::K0, 0.4),
        (Self::K1, 0.3),
        (Self::K2, 0.2),
        (Self::K3, 0.1),
    ];

    const fn name(self) -> &'static str {
        match self {
            Self::K0 => "k0",
            Self::K1 => "k1",
            Self::K2 => "k2",
            Self::K3 => "k3",
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Status {
    Active,
    Pending,
    Archived,
}

impl Status {
    const WEIGHTS: [(Self, f64); 3] = [
        (Self::Active, 0.6),
        (Self::Pending, 0.3),
        (Self::Archived, 0.1),
    ];

    const fn name(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Pending => "pending",
            Self::Archived => "archived",
        }
    }
}

/// One value of `weighted`, drawn in proportion to its weight.
fn draw<T: Copy>(rng: &mut Rng, weighted: &[(T, f64)]) -> T {
    let mut draw = rng.unit();
    weighted
        .iter()
        .find(|(_, weight)| {
            draw -= weight;
            draw < 0.0
        })
        .or(weighted.last())
        .map(|(value, _)| *value)
        .expect("weights are not empty")
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
enum Workload {
    /// `ForEach` frames that filter after an expansion and link existing nodes.
    LinkExisting,
    /// `ForEach` frames that create an item, then filter its group.
    CreateItem,
    /// Statement pairs: an unindexed property update, then the filter.
    WriteThenFilter,
}

impl Workload {
    const fn code(self) -> &'static str {
        match self {
            Self::LinkExisting => "a",
            Self::CreateItem => "b",
            Self::WriteThenFilter => "c",
        }
    }

    const fn describe(self) -> &'static str {
        match self {
            Self::LinkExisting => "foreach filter + link existing",
            Self::CreateItem => "foreach create item + filter",
            Self::WriteThenFilter => "unindexed update + filter",
        }
    }

    fn parse(code: &str) -> Self {
        match code.trim() {
            "a" => Self::LinkExisting,
            "b" => Self::CreateItem,
            "c" => Self::WriteThenFilter,
            other => panic!("BATCH_WORKLOADS entries are a, b or c, not {other}"),
        }
    }
}

/// Fixture size and run shape, from the environment.
pub struct Options {
    items: usize,
    group_size: usize,
    load_chunk: usize,
    load_concurrency: usize,
    index_deadline: Duration,
    workloads: Vec<Workload>,
    sizes: Vec<usize>,
    rounds: usize,
    warmup: usize,
    item_budget: usize,
    min_requests: usize,
    max_requests: usize,
}

impl Options {
    pub fn from_env() -> Self {
        let list = |name: &str, default: &str| {
            std::env::var(name)
                .unwrap_or_else(|_| default.to_string())
                .split(',')
                .map(str::to_string)
                .collect::<Vec<_>>()
        };
        let options = Self {
            items: fixture::env_or("BATCH_ITEMS", 300_000),
            group_size: fixture::env_or("BATCH_GROUP_SIZE", 200),
            load_chunk: fixture::env_or("BATCH_LOAD_CHUNK", 2_000),
            load_concurrency: fixture::env_or("BATCH_LOAD_CONCURRENCY", 2),
            index_deadline: Duration::from_secs(fixture::env_or("BENCH_INDEX_TIMEOUT_SECS", 3_600)),
            workloads: list("BATCH_WORKLOADS", "a,b,c")
                .iter()
                .map(|code| Workload::parse(code))
                .collect(),
            sizes: list("BATCH_SIZES", "1,10,100,500")
                .iter()
                .map(|size| {
                    size.trim()
                        .parse()
                        .unwrap_or_else(|_| panic!("BATCH_SIZES entry {size} is not a number"))
                })
                .collect(),
            rounds: fixture::env_or("BATCH_ROUNDS", 3),
            warmup: fixture::env_or("BATCH_WARMUP", 2),
            item_budget: fixture::env_or("BATCH_ITEM_BUDGET", 5_000),
            min_requests: fixture::env_or("BATCH_MIN_REQUESTS", 10),
            max_requests: fixture::env_or("BATCH_MAX_REQUESTS", 200),
        };
        assert!(
            options.items > 0 && options.group_size > 0,
            "BATCH_ITEMS and BATCH_GROUP_SIZE are positive"
        );
        assert!(
            options.load_chunk > 0 && options.load_concurrency > 0,
            "load chunks and concurrency are positive"
        );
        assert!(
            options.sizes.iter().all(|size| *size > 0),
            "batch sizes are positive"
        );
        assert!(
            options.min_requests > 0 && options.min_requests <= options.max_requests,
            "BATCH_MIN_REQUESTS is in 1..=BATCH_MAX_REQUESTS"
        );
        options
    }

    fn groups(&self) -> usize {
        self.items.div_ceil(self.group_size)
    }

    fn requests(&self, size: usize) -> usize {
        (self.item_budget / size).clamp(self.min_requests, self.max_requests)
    }
}

#[derive(Clone, Copy)]
struct ItemRecord {
    group: usize,
    kind: Kind,
    status: Status,
}

/// The harness's own copy of the graph: the oracle every request is checked against.
struct Model {
    /// Every item by `uid`: the loaded items, then the items the run created.
    items: Vec<ItemRecord>,
    /// Item `uid`s of each group.
    members: Vec<Vec<usize>>,
    loaded: usize,
}

impl Model {
    fn generate(options: &Options) -> Self {
        let mut rng = Rng(SEED);
        let mut model = Self {
            items: Vec::with_capacity(options.items),
            members: vec![Vec::new(); options.groups()],
            loaded: options.items,
        };
        (0..options.items).for_each(|_| {
            let group = rng.below(options.groups());
            let kind = draw(&mut rng, &Kind::WEIGHTS);
            let status = draw(&mut rng, &Status::WEIGHTS);
            model.push(ItemRecord {
                group,
                kind,
                status,
            });
        });
        model
    }

    fn push(&mut self, item: ItemRecord) -> usize {
        let uid = self.items.len();
        self.members[item.group].push(uid);
        self.items.push(item);
        uid
    }

    /// Whether the filter of an expansion from `group` keeps item `uid`.
    fn matches(&self, uid: usize, group: usize) -> bool {
        let item = self.items[uid];
        item.group == group && item.kind == TARGET_KIND && item.status == TARGET_STATUS
    }

    /// A probe for a filter over `group`: half the draws take an item of the
    /// group the filter keeps (when there is one), a quarter any item of the
    /// group, and a quarter any item, which is almost always in another group
    /// and so never reached by the expansion.
    fn probe(&self, rng: &mut Rng, group: usize) -> usize {
        let draw = rng.unit();
        let members = &self.members[group];
        let matching = members
            .iter()
            .copied()
            .filter(|uid| self.matches(*uid, group))
            .collect::<Vec<_>>();
        match (draw, matching.is_empty(), members.is_empty()) {
            (draw, false, _) if draw < 0.5 => matching[rng.below(matching.len())],
            (draw, _, false) if draw < 0.75 => members[rng.below(members.len())],
            _ => rng.below(self.items.len()),
        }
    }
}

/// Node ids of the loaded graph, read back and checked against the model.
struct Ids {
    groups: Vec<u64>,
    /// Node id of every loaded item, by `uid`.
    items: Vec<u64>,
}

fn int(value: u64) -> QueryValue {
    QueryValue::I64(i64::try_from(value).expect("ids and uids fit in i64"))
}

fn text(value: &str) -> QueryValue {
    QueryValue::String(value.to_string())
}

fn frame<const N: usize>(fields: [(&str, QueryValue); N]) -> QueryValue {
    QueryValue::Object(
        fields
            .into_iter()
            .map(|(name, value)| (name.to_string(), value))
            .collect(),
    )
}

/// `ForEach` body that creates the item of frame fields `uid`, `kind` and
/// `status` in the group whose node id is `group`.
fn create_item_body() -> WriteBatch {
    write_batch()
        .var_as(
            "item",
            g().add_n(
                "Item",
                vec![
                    ("uid", PropertyInput::param("uid")),
                    ("kind", PropertyInput::param("kind")),
                    ("status", PropertyInput::param("status")),
                ],
            ),
        )
        .var_as(
            "has",
            g().n(NodeRef::param("group")).add_e(
                "HAS",
                NodeRef::var("item"),
                Vec::<(String, PropertyInput)>::new(),
            ),
        )
}

/// Links the items of `group` the filter keeps for parameter `probe` to the
/// node id in parameter `target`, tagging each edge with parameter `req`.
fn filter_and_link(group: NodeRef, probe: &str) -> Traversal<OnNodes, WriteEnabled> {
    g().n(group)
        .out(Some("HAS"))
        .where_(Predicate::and(vec![
            Predicate::eq("$label", "Item"),
            Predicate::eq("kind", TARGET_KIND.name()),
            Predicate::eq("status", TARGET_STATUS.name()),
            Predicate::eq_param("uid", probe),
        ]))
        .add_e(
            "LINK",
            NodeRef::param("target"),
            vec![("req", PropertyInput::param("req"))],
        )
}

/// Sends a write, retrying transaction conflicts (a conflicted transaction is
/// rolled back, so a retry is safe). Returns the latency of the attempt that
/// succeeded and the number of retries; any other error panics.
async fn send(backend: &Backend, request: &QueryRequest) -> (Duration, u32) {
    for attempt in 1u32.. {
        let started = Instant::now();
        match backend.query(request.clone()).await {
            Ok(_) => return (started.elapsed(), attempt - 1),
            Err(error) if error.contains("transaction conflict") && attempt < 20 => {
                tokio::time::sleep(Duration::from_millis(25 * u64::from(attempt))).await;
            }
            Err(error) => panic!("write request failed after {attempt} attempts: {error}"),
        }
    }
    unreachable!("the retry loop returns or panics")
}

async fn ensure_indexes(backend: &Backend, deadline: Duration) {
    let names = ["kind", "status"];
    let receipts = backend
        .query(QueryRequest::write(
            write_batch()
                .var_as(
                    "kind",
                    g().create_index_if_not_exists(IndexSpec::node_equality("Item", "kind")),
                )
                .var_as(
                    "status",
                    g().create_index_if_not_exists(IndexSpec::node_equality("Item", "status")),
                )
                .returning(names),
        ))
        .await
        .unwrap();
    fixture::wait_for_operations(backend, &receipts, &names, deadline).await;
}

/// Reads the graph back and checks it is exactly the loaded model: every
/// group, and every item with its group, `kind` and `status`. A database a
/// run has already written to fails, since its items differ.
async fn read_ids(backend: &Backend, model: &Model) -> Ids {
    let started = Instant::now();
    let response = backend
        .query(fixture::read(g().n_with_label("Group").project(vec![
            PropertyProjection::renamed("$id", "id"),
            PropertyProjection::new("name"),
        ])))
        .await
        .unwrap();
    let mut groups = vec![None; model.members.len()];
    response["r"].as_array().unwrap().iter().for_each(|row| {
        let name = row["name"].as_str().unwrap();
        let Some(group) = name
            .strip_prefix("group-")
            .and_then(|index| index.parse::<usize>().ok())
            .filter(|group| *group < groups.len())
        else {
            panic!("unexpected group {row}");
        };
        assert!(groups[group].is_none(), "group {name} exists twice");
        groups[group] = row["id"].as_u64();
    });
    let groups = groups
        .into_iter()
        .enumerate()
        .map(|(group, id)| id.unwrap_or_else(|| panic!("group-{group} is missing")))
        .collect::<Vec<_>>();

    let responses = stream::iter(groups.iter().copied().enumerate())
        .map(|(group, id)| async move {
            let response = backend
                .query(fixture::read(
                    g().n(NodeRef::id(id)).out(Some("HAS")).project(vec![
                        PropertyProjection::renamed("$id", "id"),
                        PropertyProjection::new("uid"),
                        PropertyProjection::new("kind"),
                        PropertyProjection::new("status"),
                    ]),
                ))
                .await
                .unwrap();
            (group, response)
        })
        .buffer_unordered(READ_CONCURRENCY)
        .collect::<Vec<_>>()
        .await;
    let mut items = vec![None; model.loaded];
    responses.iter().for_each(|(group, response)| {
        let rows = response["r"].as_array().unwrap();
        assert_eq!(
            rows.len(),
            model.members[*group].len(),
            "group-{group} has {} items, the model {}: batch-run needs a fresh copy of a batch-load database with the same BATCH_ITEMS and BATCH_GROUP_SIZE",
            rows.len(),
            model.members[*group].len()
        );
        rows.iter().for_each(|row| {
            let Some(uid) = row["uid"]
                .as_u64()
                .and_then(|uid| usize::try_from(uid).ok())
                .filter(|uid| *uid < model.loaded)
            else {
                panic!("group-{group} has an item the model does not: {row}");
            };
            let expected = model.items[uid];
            assert!(
                expected.group == *group
                    && row["kind"] == expected.kind.name()
                    && row["status"] == expected.status.name()
                    && items[uid].is_none(),
                "item {row} of group-{group} differs from the model (group-{}, {}, {})",
                expected.group,
                expected.kind.name(),
                expected.status.name()
            );
            items[uid] = row["id"].as_u64();
        });
    });
    let items = items
        .into_iter()
        .enumerate()
        .map(|(uid, id)| id.unwrap_or_else(|| panic!("item {uid} is missing")))
        .collect();
    println!(
        "graph matches the model: {} groups, {} items ({:.1}s)",
        groups.len(),
        model.loaded,
        started.elapsed().as_secs_f64()
    );
    Ids { groups, items }
}

pub async fn load(backend: &Backend, options: &Options) {
    let started = Instant::now();
    let existing = backend
        .query(fixture::read(g().n_with_label("Group").count()))
        .await
        .unwrap();
    assert_eq!(existing["r"], 0, "batch-load needs an empty database");
    ensure_indexes(backend, options.index_deadline).await;
    let model = Model::generate(options);

    let groups = (0..options.groups())
        .map(|group| frame([("name", text(&fixture::group_name(group)))]))
        .collect();
    send(
        backend,
        &QueryRequest::write(write_batch().for_each_param(
            "frames",
            write_batch().var_as(
                "group",
                g().add_n("Group", vec![("name", PropertyInput::param("name"))]),
            ),
        ))
        .with_parameter_value("frames", QueryValue::Array(groups)),
    )
    .await;
    let response = backend
        .query(fixture::read(g().n_with_label("Group").project(vec![
            PropertyProjection::renamed("$id", "id"),
            PropertyProjection::new("name"),
        ])))
        .await
        .unwrap();
    let group_ids = response["r"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| {
            (
                row["name"].as_str().unwrap().to_string(),
                row["id"].as_u64().unwrap(),
            )
        })
        .collect::<BTreeMap<_, _>>();

    let requests = model
        .items
        .chunks(options.load_chunk)
        .enumerate()
        .map(|(chunk, items)| {
            let frames = items
                .iter()
                .enumerate()
                .map(|(offset, item)| {
                    frame([
                        ("uid", int((chunk * options.load_chunk + offset) as u64)),
                        ("kind", text(item.kind.name())),
                        ("status", text(item.status.name())),
                        ("group", int(group_ids[&fixture::group_name(item.group)])),
                    ])
                })
                .collect();
            QueryRequest::write(write_batch().for_each_param("frames", create_item_body()))
                .with_parameter_value("frames", QueryValue::Array(frames))
        })
        .collect::<Vec<_>>();
    let chunks = requests.len();
    stream::iter(requests)
        .map(|request| async move { send(backend, &request).await })
        .buffer_unordered(options.load_concurrency)
        .enumerate()
        .for_each(|(done, _)| async move {
            if (done + 1) % 20 == 0 || done + 1 == chunks {
                let items = ((done + 1) * options.load_chunk).min(options.items);
                let elapsed = started.elapsed().as_secs_f64();
                println!(
                    "loaded {items} items in {elapsed:.0}s ({:.0}/s)",
                    items as f64 / elapsed
                );
            }
        })
        .await;
    if let Backend::Embedded { db, .. } = backend {
        db.flush_writer().await.unwrap();
    }
    read_ids(backend, &model).await;
    println!(
        "BATCH_LOAD_OK items={} groups={} in {:.0}s",
        options.items,
        options.groups(),
        started.elapsed().as_secs_f64()
    );
}

/// One request of a workload and the `LINK` edges it must add to its target.
struct Planned {
    request: QueryRequest,
    links: usize,
}

fn plan(workload: Workload, model: &mut Model, ids: &Ids, rng: &mut Rng, size: usize) -> Planned {
    let groups = model.members.len();
    match workload {
        Workload::LinkExisting => {
            let (frames, links) = (0..size).fold((Vec::new(), 0), |(mut frames, links), _| {
                let group = rng.below(groups);
                let probe = model.probe(rng, group);
                frames.push(frame([
                    ("group", int(ids.groups[group])),
                    ("probe", int(probe as u64)),
                ]));
                (frames, links + usize::from(model.matches(probe, group)))
            });
            let body =
                write_batch().var_as("link", filter_and_link(NodeRef::param("group"), "probe"));
            Planned {
                request: QueryRequest::write(write_batch().for_each_param("frames", body))
                    .with_parameter_value("frames", QueryValue::Array(frames)),
                links,
            }
        }
        Workload::CreateItem => {
            let (frames, links) = (0..size).fold((Vec::new(), 0), |(mut frames, links), _| {
                let group = rng.below(groups);
                // Half the created items are kept by the filter.
                let (kind, status) = match rng.unit() < 0.5 {
                    true => (TARGET_KIND, TARGET_STATUS),
                    false => (draw(rng, &Kind::WEIGHTS), draw(rng, &Status::WEIGHTS)),
                };
                let uid = model.push(ItemRecord {
                    group,
                    kind,
                    status,
                });
                // Two in five frames probe the item they have just created.
                let probe = match rng.unit() < 0.4 {
                    true => uid,
                    false => model.probe(rng, group),
                };
                frames.push(frame([
                    ("group", int(ids.groups[group])),
                    ("uid", int(uid as u64)),
                    ("kind", text(kind.name())),
                    ("status", text(status.name())),
                    ("probe", int(probe as u64)),
                ]));
                (frames, links + usize::from(model.matches(probe, group)))
            });
            let body = create_item_body()
                .var_as("link", filter_and_link(NodeRef::param("group"), "probe"));
            Planned {
                request: QueryRequest::write(write_batch().for_each_param("frames", body))
                    .with_parameter_value("frames", QueryValue::Array(frames)),
                links,
            }
        }
        Workload::WriteThenFilter => {
            let (batch, parameters, links) = (0..size).fold(
                (write_batch(), Vec::new(), 0),
                |(batch, mut parameters, links), unit| {
                    let group = rng.below(groups);
                    let probe = model.probe(rng, group);
                    let touched = rng.below(model.loaded);
                    parameters.extend([
                        (format!("x{unit}"), int(ids.items[touched])),
                        (format!("g{unit}"), int(ids.groups[group])),
                        (format!("p{unit}"), int(probe as u64)),
                    ]);
                    let batch = batch
                        .var_as(
                            &format!("touch{unit}"),
                            g().n(NodeRef::param(format!("x{unit}")))
                                .set_property("touched", PropertyInput::param("req")),
                        )
                        .var_as(
                            &format!("link{unit}"),
                            filter_and_link(
                                NodeRef::param(format!("g{unit}")),
                                &format!("p{unit}"),
                            ),
                        );
                    (
                        batch,
                        parameters,
                        links + usize::from(model.matches(probe, group)),
                    )
                },
            );
            Planned {
                request: parameters
                    .into_iter()
                    .fold(QueryRequest::write(batch), |request, (name, value)| {
                        request.with_parameter_value(name, value)
                    }),
                links,
            }
        }
    }
}

/// Measured requests of one workload and batch size in one round.
struct Cell {
    latencies: Vec<f64>,
    items: usize,
    retries: u32,
}

impl Cell {
    fn seconds(&self) -> f64 {
        self.latencies.iter().sum::<f64>() / 1_000.0
    }

    fn items_per_second(&self) -> f64 {
        self.items as f64 / self.seconds()
    }
}

pub async fn run(backend: &Backend, options: &Options) {
    let mut model = Model::generate(options);
    ensure_indexes(backend, options.index_deadline).await;
    let ids = read_ids(backend, &model).await;
    let mut rng = Rng(SEED ^ 0xA5A5_A5A5);
    // Every request links to a distinct loaded item, so its `LINK` in-edges
    // are exactly the ones that request added.
    let mut targets = (0..model.loaded).collect::<Vec<_>>();
    (1..targets.len()).rev().for_each(|index| {
        let other = rng.below(index + 1);
        targets.swap(index, other);
    });
    let mut targets = targets.into_iter();
    let mut request_number = 0u64;
    let mut cells = BTreeMap::<(Workload, usize), Vec<Cell>>::new();
    let started = Instant::now();
    for round in 1..=options.rounds {
        for workload in options.workloads.iter().copied() {
            for size in options.sizes.iter().copied() {
                let requests = options.requests(size);
                let mut cell = Cell {
                    latencies: Vec::with_capacity(requests),
                    items: 0,
                    retries: 0,
                };
                for index in 0..options.warmup + requests {
                    let target = targets.next().expect(
                        "the run sends fewer requests than there are loaded items; raise BATCH_ITEMS",
                    );
                    request_number += 1;
                    let Planned { request, links } =
                        plan(workload, &mut model, &ids, &mut rng, size);
                    let request = request
                        .with_parameter_value("target", int(ids.items[target]))
                        .with_parameter_value("req", int(request_number));
                    let (latency, retries) = send(backend, &request).await;
                    let response = backend
                        .query(fixture::read(
                            g().n(NodeRef::id(ids.items[target]))
                                .in_e(Some("LINK"))
                                .count(),
                        ))
                        .await
                        .unwrap();
                    assert_eq!(
                        response["r"],
                        links,
                        "workload {} size {size} request {request_number}: LINK edges to item {target} differ from the oracle",
                        workload.code()
                    );
                    if index >= options.warmup {
                        cell.latencies.push(latency.as_secs_f64() * 1_000.0);
                        cell.items += size;
                        cell.retries += retries;
                    }
                }
                let result = serde_json::json!({
                    "workload": workload.code(),
                    "size": size,
                    "round": round,
                    "requests": cell.latencies.len(),
                    "p50_ms": crate::percentile(&cell.latencies, 50),
                    "p95_ms": crate::percentile(&cell.latencies, 95),
                    "p99_ms": crate::percentile(&cell.latencies, 99),
                    "items_per_s": cell.items_per_second(),
                    "retries": cell.retries,
                });
                println!("BATCH_RESULT {result}");
                cells.entry((workload, size)).or_default().push(cell);
            }
        }
    }
    println!(
        "\n{:<34} {:>5} {:>6} {:>9} {:>9} {:>9} {:>10} {:>18} {:>7}",
        "workload",
        "size",
        "reqs",
        "p50 ms",
        "p95 ms",
        "p99 ms",
        "items/s",
        "round p50 spread",
        "retries"
    );
    cells.iter().for_each(|((workload, size), rounds)| {
        let latencies = rounds
            .iter()
            .flat_map(|cell| cell.latencies.iter().copied())
            .collect::<Vec<_>>();
        let items = rounds.iter().map(|cell| cell.items).sum::<usize>();
        let seconds = rounds.iter().map(Cell::seconds).sum::<f64>();
        let round_p50s = rounds
            .iter()
            .map(|cell| crate::percentile(&cell.latencies, 50))
            .collect::<Vec<_>>();
        let (low, high) = round_p50s
            .iter()
            .fold((f64::INFINITY, 0.0_f64), |(low, high), p50| {
                (low.min(*p50), high.max(*p50))
            });
        println!(
            "{:<34} {size:>5} {:>6} {:>9.2} {:>9.2} {:>9.2} {:>10.0} {:>17.1}% {:>7}",
            format!("{} {}", workload.code(), workload.describe()),
            latencies.len(),
            crate::percentile(&latencies, 50),
            crate::percentile(&latencies, 95),
            crate::percentile(&latencies, 99),
            items as f64 / seconds,
            (high - low) / crate::percentile(&round_p50s, 50) * 100.0,
            rounds.iter().map(|cell| cell.retries).sum::<u32>(),
        );
    });
    println!(
        "BATCH_RUN_OK requests={request_number} created_items={} in {:.0}s",
        model.items.len() - model.loaded,
        started.elapsed().as_secs_f64()
    );
}
