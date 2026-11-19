//! A store that needs both the label identity copy and the datetime string
//! conversion: one background worker finishes both, whatever order their
//! steps interleave in, while the writer serves graph writes. A failing
//! migration backs off alone: a failing label step does not slow the
//! conversion, a failing migration job does not slow the label steps but does
//! hold the conversion back, and the conversion waits while the writer's flush
//! backlog is high.

use std::sync::Arc;
use std::time::Duration;

use helix_ast::batch::{read_batch, write_batch};
use helix_ast::expr::Predicate;
use helix_ast::graph::NodeRef;
use helix_ast::query::{QueryParamType, QueryRequest, QueryValue};
use helix_ast::traversal::g;
use helix_ast::value::{DateTime, PropertyInput};
use serde_json::json;
use slatedb::object_store::{memory::InMemory, ObjectStore};
use slatedb::DbReadOps;
use tokio::time::Instant;

use super::{
    process_background_step, BackgroundMigrations, BackgroundStep, MigrationRetries,
    MigrationWorkerSupervisor, Retry, MAX_FAILURE_BACKOFF,
};
use crate::config::{self, DbConfig, MigrationTuning, SecondaryIndexDefinition};
use crate::encoding::indexes::{label, PropertyIndexKey};
use crate::encoding::keys::scope::{DataScope, TenantId};
use crate::encoding::keys::NodePropertyKey;
use crate::encoding::property::{encode_properties, Property};
use crate::encoding::v2::keys::{DataKey, DataKeyKind};
use crate::encoding::v2::legacy::label_hash;
use crate::encoding::v2::values::indexes::SecondaryEqualityBitmapValue;
use crate::error::HelixDbError;
use crate::index_lifecycle::IndexStorageVersion;
use crate::migrations::{datetime_strings, indexes::label_identity, startup};
use crate::query_service::HelixQueryService;
use crate::search::vector::gated_wal::{GatedWalStore, WalUploads};
use crate::{HelixDB, HelixStorage};

const DB_PATH: &str = "label-and-datetime-migrations";
const PEOPLE: usize = 4;
const EVENTS: usize = 8;

fn scopes() -> [DataScope; 2] {
    [
        DataScope::LegacyUnscoped,
        DataScope::Tenant(TenantId::from_ulid_str("0000000000000000000000000A").unwrap()),
    ]
}

/// Event `index`'s instant, spelled at +02:00 as a parameter-era write stored it.
fn event_at(index: usize) -> String {
    format!("2026-{:02}-01T{:02}:30:00+02:00", index + 1, index)
}

fn attends_since(index: usize) -> String {
    format!("2025-12-{:02}T08:00:00Z", index + 1)
}

fn canonical(rfc3339: &str) -> String {
    DateTime::parse_rfc3339(rfc3339)
        .unwrap()
        .to_rfc3339()
        .unwrap()
}

fn tuned(worker: config::MigrationWorkerMode) -> DbConfig {
    // Two rows per datetime batch, so its phases span several steps that
    // interleave with the label steps.
    DbConfig::new().with_migration_tuning(
        MigrationTuning::default()
            .with_batch_rows(config::MigrationBatchRows::new(2).unwrap())
            .with_worker_mode(worker),
    )
}

/// People attending events in `scope`, every datetime stored as its string.
async fn write_graph(db: &HelixDB, scope: DataScope) {
    let mut batch = write_batch();
    for person in 0..PEOPLE {
        batch = batch.var_as(
            &format!("p{person}"),
            g().add_n(
                "Person",
                vec![("name", PropertyInput::from(format!("p{person}")))],
            ),
        );
    }
    for event in 0..EVENTS {
        batch = batch
            .var_as(
                &format!("e{event}"),
                g().add_n(
                    "Event",
                    vec![
                        ("name", PropertyInput::from(format!("e{event}"))),
                        ("at", PropertyInput::from(event_at(event))),
                    ],
                ),
            )
            .var_as(
                &format!("a{event}"),
                g().n(NodeRef::var(format!("p{}", event % PEOPLE))).add_e(
                    "ATTENDS",
                    NodeRef::var(format!("e{event}")),
                    vec![("since", PropertyInput::from(attends_since(event)))],
                ),
            );
    }
    db.query_scoped(
        QueryRequest::write(batch.returning(Vec::<String>::new())),
        scope,
    )
    .await
    .expect("fixture graph writes");
}

/// One person attending one event, written while the migrations run, with
/// its datetimes typed. A write that read the store before the label copy
/// published conflicts with publication, so it is taken again, as a client
/// retries a conflict.
async fn write_late(db: &HelixDB, index: usize) {
    let request = || {
        QueryRequest::write(
            write_batch()
                .var_as(
                    "person",
                    g().add_n(
                        "Person",
                        vec![("name", PropertyInput::from(format!("late-p{index}")))],
                    ),
                )
                .var_as(
                    "event",
                    g().add_n(
                        "Event",
                        vec![
                            ("name", PropertyInput::from(format!("late-e{index}"))),
                            (
                                "at",
                                PropertyInput::from(
                                    DateTime::parse_rfc3339(&format!(
                                        "2026-0{}-15T12:00:00Z",
                                        index + 1
                                    ))
                                    .unwrap(),
                                ),
                            ),
                        ],
                    ),
                )
                .var_as(
                    "attends",
                    g().n(NodeRef::var("person")).add_e(
                        "ATTENDS",
                        NodeRef::var("event"),
                        vec![(
                            "since",
                            PropertyInput::from(
                                DateTime::parse_rfc3339("2026-01-01T00:00:00Z").unwrap(),
                            ),
                        )],
                    ),
                )
                .returning(Vec::<String>::new()),
        )
    };
    for _ in 0..100 {
        match db.query(request()).await {
            Ok(_) => return,
            Err(error) if error.is_transaction_conflict() => {}
            Err(error) => panic!("a graph write during the migrations fails: {error}"),
        }
    }
    panic!("a graph write during the migrations kept conflicting");
}

/// Every label-served read shape: node and edge label scans and labeled
/// expansion in both directions.
async fn label_reads(db: &HelixDB, scope: DataScope) -> serde_json::Value {
    db.query_scoped(
        QueryRequest::read(
            read_batch()
                .var_as("people", g().n_with_label("Person").count())
                .var_as("events", g().n_with_label("Event").count())
                .var_as("attends", g().e_with_label("ATTENDS").count())
                .var_as(
                    "attended",
                    g().n_with_label("Person").out(Some("ATTENDS")).count(),
                )
                .var_as(
                    "attendees",
                    g().n_with_label("Event").in_(Some("ATTENDS")).count(),
                )
                .returning(["people", "events", "attends", "attended", "attendees"]),
        ),
        scope,
    )
    .await
    .expect("label reads execute")
}

/// `(name, at)` of every event in `scope`, by a label scan, sorted by name.
async fn scanned_events(db: &HelixDB, scope: DataScope) -> Vec<(String, String)> {
    let response = db
        .query_scoped(
            QueryRequest::read(
                read_batch()
                    .var_as(
                        "rows",
                        g().n_with_label("Event").value_map(None::<Vec<String>>),
                    )
                    .returning(["rows"]),
            ),
            scope,
        )
        .await
        .unwrap();
    let mut rows = response["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| {
            (
                row["name"].as_str().unwrap().to_owned(),
                row["at"].as_str().unwrap().to_owned(),
            )
        })
        .collect::<Vec<_>>();
    rows.sort();
    rows
}

async fn edge_since(db: &HelixDB, scope: DataScope) -> Vec<String> {
    let response = db
        .query_scoped(
            QueryRequest::read(
                read_batch()
                    .var_as("e", g().e_with_label("ATTENDS").edge_properties())
                    .returning(["e"]),
            ),
            scope,
        )
        .await
        .unwrap();
    let mut since = response["e"]
        .as_array()
        .unwrap()
        .iter()
        .map(|edge| edge["since"].as_str().unwrap().to_owned())
        .collect::<Vec<_>>();
    since.sort();
    since
}

/// Hash-keyed label rows left in any scope.
async fn legacy_label_rows(reader: &(impl DbReadOps + Sync)) -> usize {
    let mut rows = 0;
    for scope in crate::index_lifecycle::queue::recovery::discover_scopes(reader)
        .await
        .unwrap()
    {
        for prefix in label_hash::LegacyLabelScanPrefix::ALL {
            let mut scan = reader
                .scan_prefix(DataKey::data_prefix(scope, prefix.to_bytes()), ..)
                .await
                .unwrap();
            while scan.next().await.unwrap().is_some() {
                rows += 1;
            }
        }
    }
    rows
}

/// Names of the events after `bound`, through the range index on `at`.
async fn indexed_after(service: &HelixQueryService, bound: &str) -> Vec<String> {
    let response = service
        .execute_query(
            QueryRequest::read(
                read_batch()
                    .var_as(
                        "hits",
                        g().n_with_label_where("Event", Predicate::gt_param("at", "since"))
                            .values(vec!["name"]),
                    )
                    .returning(["hits"]),
            )
            .with_typed_parameter(
                "since",
                QueryParamType::DateTime,
                QueryValue::String(bound.to_owned()),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert!(
        !response
            .diagnostics()
            .insights
            .iter()
            .any(|insight| matches!(
                insight,
                helix_planner::diagnostics::PlannerInsight::MissingIndex(_)
            )),
        "the range index serves the filter"
    );
    let mut names = response.json()["hits"]
        .as_array()
        .unwrap()
        .iter()
        .map(|hit| hit["name"].as_str().unwrap().to_owned())
        .collect::<Vec<_>>();
    names.sort();
    names
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_worker_finishes_the_label_copy_and_the_datetime_conversion() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    // A store as a binary before both migrations left it: hash-keyed label
    // rows, and every datetime stored as its RFC3339 string, which a range
    // index on `at` holds as a string.
    let db = HelixDB::open_with_object_store_and_config(
        DB_PATH,
        Arc::clone(&store),
        tuned(config::MigrationWorkerMode::Disabled),
    )
    .await
    .unwrap();
    db.install_index_for_tests(
        SecondaryIndexDefinition::node_range("Event", "at")
            .unwrap()
            .try_into()
            .unwrap(),
    )
    .await
    .unwrap();
    for scope in scopes() {
        write_graph(&db, scope).await;
    }
    label_identity::make_legacy_label_fixture(&db.inner_db())
        .await
        .unwrap();
    db.close().await.unwrap();

    let db = Arc::new(
        HelixDB::open_with_object_store_and_config(
            DB_PATH,
            Arc::clone(&store),
            tuned(config::MigrationWorkerMode::Background),
        )
        .await
        .unwrap(),
    );
    let inner = db.inner_db();
    // Before its background steps, the open has moved the store to the copy
    // and converted nothing: label reads still resolve through hash rows.
    for scope in scopes() {
        assert_eq!(
            label_reads(&db, scope).await,
            json!({
                "people": PEOPLE, "events": EVENTS, "attends": EVENTS,
                "attended": EVENTS, "attendees": EVENTS,
            })
        );
    }

    // Graph writes while both migrations run.
    tokio::join!(db.wait_for_background_migrations(), async {
        for index in 0..2 {
            write_late(&db, index).await;
        }
    });

    // The label copy published and its cleanup removed every hash row.
    assert_eq!(
        startup::storage_version(inner.as_ref()).await.unwrap(),
        IndexStorageVersion::LABEL_IDENTITY
    );
    assert_eq!(legacy_label_rows(inner.as_ref()).await, 0);

    // The datetime conversion finished every scope and element.
    let HelixStorage::Writer(writer) = db.storage() else {
        panic!("writer");
    };
    let job = datetime_strings::load(writer.db()).await.unwrap().unwrap();
    assert!(job.is_completed(), "{job:?}");
    let report = job.report();
    assert_eq!(
        report.converted_values,
        u64::try_from(2 * EVENTS * scopes().len()).unwrap(),
        "{report:?}"
    );
    assert_eq!(report.unique_conflicts, 0);
    assert!(report.skipped_properties.is_empty(), "{report:?}");
    assert!(report.skipped_scopes.is_empty());
    assert_eq!(report.undecodable_rows, 0);

    for (position, scope) in scopes().into_iter().enumerate() {
        let late = if position == 0 { 2 } else { 0 };
        assert_eq!(
            label_reads(&db, scope).await,
            json!({
                "people": PEOPLE + late, "events": EVENTS + late,
                "attends": EVENTS + late, "attended": EVENTS + late,
                "attendees": EVENTS + late,
            }),
            "{scope:?}"
        );
        let mut expected = (0..EVENTS)
            .map(|event| (format!("e{event}"), canonical(&event_at(event))))
            .chain((0..late).map(|index| {
                (
                    format!("late-e{index}"),
                    format!("2026-0{}-15T12:00:00.000Z", index + 1),
                )
            }))
            .collect::<Vec<_>>();
        expected.sort();
        assert_eq!(scanned_events(&db, scope).await, expected, "{scope:?}");
        let mut since = (0..EVENTS)
            .map(|event| canonical(&attends_since(event)))
            .chain((0..late).map(|_| "2026-01-01T00:00:00.000Z".to_owned()))
            .collect::<Vec<_>>();
        since.sort();
        assert_eq!(edge_since(&db, scope).await, since, "{scope:?}");
    }

    // The range index holds every converted datetime: each bound selects
    // exactly what a scan of the stored values does.
    let service = HelixQueryService::new(Arc::clone(&db));
    let scanned = scanned_events(&db, DataScope::LegacyUnscoped).await;
    for bound in [
        "2025-01-01T00:00:00Z",
        "2026-01-01T00:00:00Z",
        // Event 3's instant in UTC, which its +02:00 spelling sorted after.
        "2026-04-01T01:30:00Z",
        "2026-06-01T03:30:00+02:00",
        "2026-08-01T05:30:00Z",
        "2027-01-01T00:00:00Z",
    ] {
        let instant = DateTime::parse_rfc3339(bound).unwrap();
        let expected = scanned
            .iter()
            .filter(|(_, at)| DateTime::parse_rfc3339(at).unwrap() > instant)
            .map(|(name, _)| name.clone())
            .collect::<Vec<_>>();
        assert_eq!(indexed_after(&service, bound).await, expected, "{bound}");
    }

    db.close().await.unwrap();
}

/// One background step of every migration with the clock held at `now`, as
/// the worker takes it.
async fn step(
    db: &HelixDB,
    labels: &mut label_identity::LabelCopySession,
    retries: &mut MigrationRetries,
    now: Instant,
) -> BackgroundStep {
    let HelixStorage::Writer(writer) = db.storage() else {
        panic!("writer");
    };
    process_background_step(
        writer,
        &Arc::downgrade(&db.inner),
        db.config().db().migrations(),
        BackgroundMigrations::All,
        labels,
        retries,
        || now,
    )
    .await
}

/// The first failure backoff of `db`'s migrations: its idle interval.
fn initial_backoff(db: &HelixDB) -> Duration {
    Duration::from_millis(db.config().db().migrations().idle_interval_millis().get())
}

/// A background worker of `db` on `tuning`, with no handle on the database
/// it keeps alive, as the open starts one.
fn start_worker(db: &HelixDB, tuning: MigrationTuning) -> MigrationWorkerSupervisor {
    let HelixStorage::Writer(writer) = db.storage() else {
        panic!("writer");
    };
    MigrationWorkerSupervisor::start_if_enabled(
        Arc::clone(writer),
        Arc::downgrade(&db.inner),
        tuning
            .with_worker_mode(config::MigrationWorkerMode::Background)
            .with_active_interval(config::MigrationActiveIntervalMillis::new(1).unwrap()),
        BackgroundMigrations::All,
    )
    .expect("background tuning starts a worker")
}

async fn conversion(db: &HelixDB) -> Option<datetime_strings::Job> {
    let HelixStorage::Writer(writer) = db.storage() else {
        panic!("writer");
    };
    datetime_strings::load(writer.db()).await.unwrap()
}

/// Every unscoped event and attendance as written: datetimes still strings.
async fn assert_unconverted(db: &HelixDB) {
    let mut events = (0..EVENTS)
        .map(|event| (format!("e{event}"), event_at(event)))
        .collect::<Vec<_>>();
    events.sort();
    assert_eq!(scanned_events(db, DataScope::LegacyUnscoped).await, events);
    let mut since = (0..EVENTS).map(attends_since).collect::<Vec<_>>();
    since.sort();
    assert_eq!(edge_since(db, DataScope::LegacyUnscoped).await, since);
}

/// The conversion finished and every unscoped datetime is stored as one.
async fn assert_converted(db: &HelixDB) {
    let job = conversion(db).await.expect("the conversion ran");
    assert!(job.is_completed(), "{job:?}");
    assert_eq!(
        job.report().converted_values,
        u64::try_from(2 * EVENTS).unwrap(),
        "{job:?}"
    );
    let mut events = (0..EVENTS)
        .map(|event| (format!("e{event}"), canonical(&event_at(event))))
        .collect::<Vec<_>>();
    events.sort();
    assert_eq!(scanned_events(db, DataScope::LegacyUnscoped).await, events);
    let mut since = (0..EVENTS)
        .map(|event| canonical(&attends_since(event)))
        .collect::<Vec<_>>();
    since.sort();
    assert_eq!(edge_since(db, DataScope::LegacyUnscoped).await, since);
}

/// A store whose label copy fails closed on every step, here on a label too
/// long for any exact key, and whose unscoped datetimes are still strings.
/// Returns the long label's two rows: deleting them lets the copy finish.
async fn failing_label_store(store: &Arc<dyn ObjectStore>) -> [bytes::Bytes; 2] {
    let db = HelixDB::open_with_object_store_and_config(
        DB_PATH,
        Arc::clone(store),
        tuned(config::MigrationWorkerMode::Disabled),
    )
    .await
    .unwrap();
    write_graph(&db, DataScope::LegacyUnscoped).await;
    let inner = db.inner_db();
    label_identity::make_legacy_label_fixture(&inner)
        .await
        .unwrap();
    let long = "L".repeat(label::MAX_LABEL_LEN + 1);
    let property = DataKey::Data {
        scope: DataScope::LegacyUnscoped,
        kind: DataKeyKind::NodeProperty(NodePropertyKey::new(9_999)),
    }
    .to_bytes();
    crate::transaction::put_unbatched(
        &inner,
        &property,
        &encode_properties(&[Property::string("$label", &long)]),
    )
    .await
    .unwrap();
    let hash_label = DataKey::Data {
        scope: DataScope::LegacyUnscoped,
        kind: DataKeyKind::PropertyIndex(PropertyIndexKey::Equality(
            label_hash::LegacyNodeLabelKey::for_label(&long),
        )),
    }
    .to_bytes();
    crate::transaction::put_unbatched(
        &inner,
        &hash_label,
        &SecondaryEqualityBitmapValue::new(roaring::RoaringTreemap::from_iter([9_999])).encode(),
    )
    .await
    .unwrap();
    db.close().await.unwrap();
    [property, hash_label]
}

/// A store with hash label rows to copy and a migration job that fails on
/// every step, on a source row it cannot decode, which it returns: deleting
/// that row lets the job finish.
async fn failing_job_store(store: &Arc<dyn ObjectStore>) -> bytes::Bytes {
    let db = HelixDB::open_with_object_store_and_config(
        DB_PATH,
        Arc::clone(store),
        tuned(config::MigrationWorkerMode::Disabled),
    )
    .await
    .unwrap();
    write_graph(&db, DataScope::LegacyUnscoped).await;
    let inner = db.inner_db();
    label_identity::make_legacy_label_fixture(&inner)
        .await
        .unwrap();
    crate::migrations::ensure_migration_job(
        &inner,
        DataScope::LegacyUnscoped,
        crate::migrations::MigrationId::GraphFormatV1Cleanup,
        crate::migrations::MigrationMode::Background,
    )
    .await
    .unwrap();
    let malformed =
        crate::migrations::MigrationStage::LegacyEdgePairs.prefix(DataScope::LegacyUnscoped);
    inner
        .put(&malformed, bytes::Bytes::from_static(b"malformed"))
        .await
        .unwrap();
    db.close().await.unwrap();
    malformed
}

/// A label copy that fails on every attempt backs off alone. With the clock
/// held still, every step after its first failure skips it and the datetime
/// conversion finishes at one batch a step; the copy is attempted again only
/// at each retry time, twice as far apart each failure, up to the cap.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failing_label_step_backs_off_alone_beside_the_datetime_conversion() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    failing_label_store(&store).await;
    let db = HelixDB::open_with_object_store_and_config(
        DB_PATH,
        Arc::clone(&store),
        tuned(config::MigrationWorkerMode::Disabled),
    )
    .await
    .unwrap();
    let initial = initial_backoff(&db);
    let mut labels = label_identity::LabelCopySession::new();
    let mut retries = MigrationRetries::default();
    let start = Instant::now();

    let first = step(&db, &mut labels, &mut retries, start).await;
    assert!(
        matches!(
            first,
            BackgroundStep::Active {
                failure: Some(HelixDbError::MigrationRequired { .. })
            }
        ),
        "the copy fails while the conversion advances: {first:?}"
    );
    let backing_off = Retry::At {
        at: start + initial,
        delay: initial,
    };
    assert_eq!(retries.label_copy, backing_off);

    // The clock never moves, so no step waits on the copy's backoff.
    let mut steps = 1;
    loop {
        match step(&db, &mut labels, &mut retries, start).await {
            BackgroundStep::Active { failure: None } => {}
            BackgroundStep::RetryAt { at, failure: None } => {
                assert_eq!(at, start + initial, "only the copy waits");
                break;
            }
            other @ (BackgroundStep::Active { failure: Some(_) }
            | BackgroundStep::RetryAt {
                failure: Some(_), ..
            }
            | BackgroundStep::Finished) => panic!("only the copy failed, and it waits: {other:?}"),
        }
        assert_eq!(retries.label_copy, backing_off, "the copy is not retried");
        steps += 1;
        assert!(steps < 100, "the datetime conversion stalled");
    }
    assert_converted(&db).await;
    assert!(matches!(
        retries,
        MigrationRetries {
            job: Retry::Ready,
            pacing: Retry::Ready,
            label_copy: Retry::At { .. },
            label_cleanup: Retry::Ready,
            datetimes: Retry::Ready,
        }
    ));

    // Each retry fails again and waits twice as long, up to the cap; a step
    // before the retry time does not attempt the copy.
    let mut now = start + initial;
    let mut delay = initial;
    for _ in 0..8 {
        delay = (delay * 2).min(MAX_FAILURE_BACKOFF);
        let retried = step(&db, &mut labels, &mut retries, now).await;
        let BackgroundStep::RetryAt {
            at,
            failure: Some(HelixDbError::MigrationRequired { .. }),
        } = retried
        else {
            panic!("the retry fails again: {retried:?}");
        };
        assert_eq!(at, now + delay);
        assert_eq!(retries.label_copy, Retry::At { at, delay });
        let early = step(
            &db,
            &mut labels,
            &mut retries,
            at - Duration::from_millis(1),
        )
        .await;
        assert!(
            matches!(early, BackgroundStep::RetryAt { at: next, failure: None } if next == at),
            "a step before the retry time skips the copy: {early:?}"
        );
        now = at;
    }
    assert_eq!(delay, MAX_FAILURE_BACKOFF, "the backoff reached its cap");
    // The copy never published.
    assert_eq!(
        startup::storage_version(db.inner_db().as_ref())
            .await
            .unwrap(),
        IndexStorageVersion::LABEL_COPY
    );
    db.close().await.unwrap();
}

/// A failure's retry delay starts when its step ends, not when the step
/// started, so a slow batch beside the failure does not use up its backoff.
/// A migration a step skipped as not yet due still waits, even if its retry
/// time passed while the step ran: the step reports that time, not that every
/// migration finished.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failure_backs_off_from_the_end_of_its_step() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    failing_label_store(&store).await;
    let db = HelixDB::open_with_object_store_and_config(
        DB_PATH,
        Arc::clone(&store),
        tuned(config::MigrationWorkerMode::Disabled),
    )
    .await
    .unwrap();
    let HelixStorage::Writer(writer) = db.storage() else {
        panic!("writer");
    };
    let initial = initial_backoff(&db);
    let mut labels = label_identity::LabelCopySession::new();
    let mut retries = MigrationRetries::default();
    // A clock that reads `started` before the step's migrations run and
    // `ended` once they have.
    let slow_step = |started: Instant, ended: Instant| {
        let reads = std::cell::Cell::new(0_u8);
        move || {
            reads.set(reads.get() + 1);
            if reads.get() == 1 {
                started
            } else {
                ended
            }
        }
    };
    let start = Instant::now();

    // The copy fails while the conversion's batch takes half the backoff more.
    let ended = start + initial * 3 / 2;
    let first = process_background_step(
        writer,
        &Arc::downgrade(&db.inner),
        db.config().db().migrations(),
        BackgroundMigrations::All,
        &mut labels,
        &mut retries,
        slow_step(start, ended),
    )
    .await;
    assert!(
        matches!(first, BackgroundStep::Active { failure: Some(_) }),
        "the copy fails while the conversion advances: {first:?}"
    );
    let backing_off = Retry::At {
        at: ended + initial,
        delay: initial,
    };
    assert_eq!(
        retries.label_copy, backing_off,
        "the delay starts at the end"
    );

    // The conversion finishes while the copy waits.
    let mut steps = 1;
    while matches!(
        step(&db, &mut labels, &mut retries, start).await,
        BackgroundStep::Active { failure: None }
    ) {
        steps += 1;
        assert!(steps < 100, "the datetime conversion stalled");
    }
    assert_converted(&db).await;
    assert_eq!(retries.label_copy, backing_off, "the copy is not retried");

    // A step that starts before the copy's retry time and ends after it
    // skips the copy, which still waits.
    let skipped = process_background_step(
        writer,
        &Arc::downgrade(&db.inner),
        db.config().db().migrations(),
        BackgroundMigrations::All,
        &mut labels,
        &mut retries,
        slow_step(start, ended + 2 * initial),
    )
    .await;
    assert!(
        matches!(
            skipped,
            BackgroundStep::RetryAt { at, failure: None } if at == ended + initial
        ),
        "the skipped copy is still pending: {skipped:?}"
    );
    assert_eq!(retries.label_copy, backing_off, "the copy is not retried");
    db.close().await.unwrap();
}

/// A migration job that fails on every attempt backs off alone: the label
/// cleanup, one row a step, finishes while it waits. It still holds the
/// datetime conversion back, which never rewrites rows an unfinished job
/// moves, whether the job is failing or waiting on its backoff. Once it
/// succeeds, its backoff clears and every migration finishes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failing_migration_job_backs_off_alone_and_blocks_the_datetime_conversion() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let malformed = failing_job_store(&store).await;
    let db = HelixDB::open_with_object_store_and_config(
        DB_PATH,
        Arc::clone(&store),
        tuned(config::MigrationWorkerMode::Disabled),
    )
    .await
    .unwrap();
    let initial = initial_backoff(&db);
    let mut labels = label_identity::LabelCopySession::with_cleanup_rows(1);
    let mut retries = MigrationRetries::default();
    let start = Instant::now();

    let first = step(&db, &mut labels, &mut retries, start).await;
    assert!(
        matches!(first, BackgroundStep::Active { failure: Some(_) }),
        "the job fails while the label steps advance: {first:?}"
    );
    let backing_off = Retry::At {
        at: start + initial,
        delay: initial,
    };
    assert_eq!(retries.job, backing_off);

    // With the clock held still the job waits, and the label steps finish.
    let mut steps = 1;
    loop {
        match step(&db, &mut labels, &mut retries, start).await {
            BackgroundStep::Active { failure: None } => {}
            BackgroundStep::RetryAt { at, failure: None } => {
                assert_eq!(at, start + initial, "only the job waits");
                break;
            }
            other @ (BackgroundStep::Active { failure: Some(_) }
            | BackgroundStep::RetryAt {
                failure: Some(_), ..
            }
            | BackgroundStep::Finished) => panic!("only the job failed, and it waits: {other:?}"),
        }
        assert_eq!(retries.job, backing_off, "the job is not retried");
        assert!(conversion(&db).await.is_none(), "the conversion waits");
        steps += 1;
        assert!(steps < 100, "the label steps stalled");
    }
    assert!(steps > 2, "the cleanup took several steps: {steps}");
    let inner = db.inner_db();
    assert_eq!(
        startup::storage_version(inner.as_ref()).await.unwrap(),
        IndexStorageVersion::LABEL_IDENTITY
    );
    assert_eq!(legacy_label_rows(inner.as_ref()).await, 0);

    // The retry fails again and waits twice as long; the conversion waits.
    let retried = step(&db, &mut labels, &mut retries, start + initial).await;
    assert!(
        matches!(
            retried,
            BackgroundStep::RetryAt { at, failure: Some(_) } if at == start + 3 * initial
        ),
        "{retried:?}"
    );
    assert!(conversion(&db).await.is_none(), "the conversion waits");
    assert_unconverted(&db).await;

    inner.delete(&malformed).await.unwrap();
    let now = start + 3 * initial;
    let mut steps = 0;
    loop {
        match step(&db, &mut labels, &mut retries, now).await {
            BackgroundStep::Active { failure: None } => {}
            BackgroundStep::Finished => break,
            other @ (BackgroundStep::Active { failure: Some(_) }
            | BackgroundStep::RetryAt { .. }) => panic!("every migration succeeds: {other:?}"),
        }
        assert_eq!(retries.job, Retry::Ready, "the job's backoff cleared");
        steps += 1;
        assert!(steps < 1_000, "the migrations stalled");
    }
    assert_converted(&db).await;
    db.close().await.unwrap();
}

/// A migration that fails again after it recovered starts its backoff over
/// from the first delay, not from where its earlier failures left it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_recovered_migration_restarts_its_backoff() {
    let db = HelixDB::open_with_object_store_and_config(
        DB_PATH,
        Arc::new(InMemory::new()),
        tuned(config::MigrationWorkerMode::Disabled),
    )
    .await
    .unwrap();
    write_graph(&db, DataScope::LegacyUnscoped).await;
    while db.process_migration_once().await.unwrap() {}
    let initial = initial_backoff(&db);
    let inner = db.inner_db();
    // A conversion progress row that does not decode fails every attempt.
    let corrupt = || async {
        inner
            .put(
                datetime_strings::job_key(),
                bytes::Bytes::from_static(b"corrupt"),
            )
            .await
            .unwrap();
    };
    let mut labels = label_identity::LabelCopySession::new();
    let mut retries = MigrationRetries::default();
    let start = Instant::now();

    corrupt().await;
    step(&db, &mut labels, &mut retries, start).await;
    step(&db, &mut labels, &mut retries, start + initial).await;
    assert_eq!(
        retries.datetimes,
        Retry::At {
            at: start + 3 * initial,
            delay: 2 * initial,
        }
    );

    // A fresh progress row: the next attempt succeeds and clears the backoff.
    inner.delete(datetime_strings::job_key()).await.unwrap();
    let recovered = start + 3 * initial;
    let advanced = step(&db, &mut labels, &mut retries, recovered).await;
    assert!(
        matches!(advanced, BackgroundStep::Active { failure: None }),
        "{advanced:?}"
    );
    assert_eq!(retries.datetimes, Retry::Ready);

    // Failing again, it waits the first delay, not four times it.
    corrupt().await;
    let failed = step(&db, &mut labels, &mut retries, recovered).await;
    assert!(
        matches!(
            failed,
            BackgroundStep::RetryAt { at, failure: Some(_) } if at == recovered + initial
        ),
        "{failed:?}"
    );
    assert_eq!(
        retries.datetimes,
        Retry::At {
            at: recovered + initial,
            delay: initial,
        }
    );
    db.close().await.unwrap();
}

/// A datetime conversion due to retry while the migration jobs back off
/// waits on the jobs: the step reports the jobs' retry time, not the
/// conversion's, which has passed, so the worker does not wake at once and
/// spin until the jobs' retry.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_conversion_waiting_on_backing_off_jobs_waits_for_their_retry() {
    let db = HelixDB::open_with_object_store_and_config(
        DB_PATH,
        Arc::new(InMemory::new()),
        tuned(config::MigrationWorkerMode::Disabled),
    )
    .await
    .unwrap();
    write_graph(&db, DataScope::LegacyUnscoped).await;
    while db.process_migration_once().await.unwrap() {}
    let initial = initial_backoff(&db);
    let inner = db.inner_db();
    let mut labels = label_identity::LabelCopySession::new();
    let mut retries = MigrationRetries::default();
    let start = Instant::now();

    // The conversion fails on a progress row that does not decode.
    inner
        .put(
            datetime_strings::job_key(),
            bytes::Bytes::from_static(b"corrupt"),
        )
        .await
        .unwrap();
    step(&db, &mut labels, &mut retries, start).await;
    assert_eq!(
        retries.datetimes,
        Retry::At {
            at: start + initial,
            delay: initial,
        }
    );

    // The migration jobs fail from the conversion's retry time on, on a job
    // record that does not decode.
    inner
        .put(
            crate::migrations::MigrationJobKey::new(
                DataScope::LegacyUnscoped,
                crate::migrations::MigrationId::GraphFormatV1Cleanup,
            )
            .as_ref(),
            bytes::Bytes::from_static(b"corrupt"),
        )
        .await
        .unwrap();
    let now = start + initial;
    let failed = step(&db, &mut labels, &mut retries, now).await;
    assert!(
        matches!(
            failed,
            BackgroundStep::RetryAt { at, failure: Some(_) } if at == now + initial
        ),
        "{failed:?}"
    );
    // Later, before the jobs' retry, nothing runs and the step still waits
    // for the jobs.
    let waiting = step(&db, &mut labels, &mut retries, now + initial / 2).await;
    assert!(
        matches!(
            waiting,
            BackgroundStep::RetryAt { at, failure: None } if at == now + initial
        ),
        "{waiting:?}"
    );
    assert_eq!(
        retries.datetimes,
        Retry::At {
            at: start + initial,
            delay: initial,
        },
        "the conversion waits on the jobs, untried"
    );
    db.close().await.unwrap();
}

/// The worker converts datetimes at its active pace beside a label copy that
/// fails on every attempt, with a failure backoff far longer than the whole
/// conversion takes. It does not exit while the copy backs off, and exits on
/// its own once the copy, able to finish again, has.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_worker_paces_each_migration_on_its_own_backoff_and_exits_once_all_finish() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let long_label = failing_label_store(&store).await;
    let db = HelixDB::open_with_object_store_and_config(
        DB_PATH,
        Arc::clone(&store),
        tuned(config::MigrationWorkerMode::Disabled),
    )
    .await
    .unwrap();
    // A shared backoff would wait 1 s, 2 s, 4 s, and so on between the
    // conversion's many batches, so it could not finish within the timeout.
    let worker = start_worker(
        &db,
        db.config()
            .db()
            .migrations()
            .with_idle_interval(config::MigrationIdleIntervalMillis::new(1_000).unwrap()),
    );
    tokio::time::timeout(Duration::from_secs(10), async {
        while !conversion(&db).await.is_some_and(|job| job.is_completed()) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the conversion does not wait on the label copy's backoff");
    assert_converted(&db).await;
    assert!(
        !worker.handle.is_finished(),
        "the worker waits on the backing-off copy"
    );

    let inner = db.inner_db();
    for key in long_label {
        inner.delete(key).await.unwrap();
    }
    tokio::time::timeout(Duration::from_secs(30), async {
        while !worker.handle.is_finished() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the worker exits once the copy finishes");
    assert_eq!(
        startup::storage_version(inner.as_ref()).await.unwrap(),
        IndexStorageVersion::LABEL_IDENTITY
    );
    assert_eq!(legacy_label_rows(inner.as_ref()).await, 0);
    worker.stop().await;
    db.close().await.unwrap();
}

/// While WAL uploads are held and the writer's flush backlog is past the
/// pacing's high mark, a step holds the datetime conversion back: it writes
/// nothing, not even its progress, and reports more to do. Once uploads
/// resume and the backlog drains, the steps finish it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_datetime_conversion_waits_while_the_flush_backlog_is_high() {
    let gate = Arc::new(GatedWalStore::new());
    let mut settings = DbConfig::new().slate().to_writer_settings(None);
    settings.max_unflushed_bytes = 4 * 1024 * 1024;
    settings.l0_sst_size_bytes = 1024 * 1024;
    let db = HelixDB::open_with_object_store_and_config(
        DB_PATH,
        Arc::clone(&gate) as Arc<dyn ObjectStore>,
        tuned(config::MigrationWorkerMode::Disabled).with_slate_settings(settings),
    )
    .await
    .unwrap();
    write_graph(&db, DataScope::LegacyUnscoped).await;
    // Finish every other migration a new store starts while uploads still
    // flow, so the held steps below have only the conversion to hold back.
    while db.process_migration_once().await.unwrap() {}
    assert!(conversion(&db).await.is_none());

    // Writes that do not wait for their WAL upload fill the backlog to half
    // the unflushed budget: past the high mark (a quarter), short of the
    // limit at which SlateDB holds every write back.
    gate.uploads.send_replace(WalUploads::Held);
    let inner = db.inner_db();
    let backlog = || {
        let unflushed = inner.unflushed_bytes().unwrap();
        (
            unflushed
                .immutable_memtable_bytes
                .saturating_add(unflushed.wal_bytes),
            unflushed.max_unflushed_bytes,
        )
    };
    let mut filler = 0_u32;
    while backlog().0 < backlog().1 / 2 {
        inner
            .put_with_options(
                crate::migrations::scoped_metadata_key(
                    DataScope::LegacyUnscoped,
                    format!("backlog_filler:{filler}").as_bytes(),
                ),
                vec![0_u8; 64 * 1024],
                &slatedb::config::PutOptions::default(),
                &slatedb::config::WriteOptions {
                    await_durable: false,
                    ..slatedb::config::WriteOptions::default()
                },
            )
            .await
            .unwrap();
        filler += 1;
        assert!(filler < 1_000, "held WAL uploads build a backlog");
    }

    let mut labels = label_identity::LabelCopySession::new();
    let mut retries = MigrationRetries::default();
    for _ in 0..5 {
        let held = tokio::time::timeout(
            Duration::from_secs(10),
            step(&db, &mut labels, &mut retries, Instant::now()),
        )
        .await
        .expect("a held-back step does not wait on the backlog");
        assert!(
            matches!(held, BackgroundStep::Active { failure: None }),
            "a held-back step has more to do: {held:?}"
        );
        assert!(
            conversion(&db).await.is_none(),
            "the conversion wrote nothing"
        );
    }
    assert_unconverted(&db).await;

    gate.uploads.send_replace(WalUploads::Open);
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            match step(&db, &mut labels, &mut retries, Instant::now()).await {
                BackgroundStep::Active { failure: None } => {}
                BackgroundStep::Finished => break,
                other @ (BackgroundStep::Active { failure: Some(_) }
                | BackgroundStep::RetryAt { .. }) => panic!("every step succeeds: {other:?}"),
            }
        }
    })
    .await
    .expect("the steps finish once the backlog drains");
    assert_converted(&db).await;
    db.close().await.unwrap();
}
