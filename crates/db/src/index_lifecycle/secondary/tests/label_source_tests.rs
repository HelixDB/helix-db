//! Secondary builds read only their label's rows, through the label bitmap
//! ([`crate::index_lifecycle::label_source`]): steps scale with the label,
//! not the graph, and the generation matches what a scan of every row
//! builds ([`assert_generation_equals_source`] derives it from every row).

use super::*;
use crate::index_lifecycle::label_source::testing;

/// A deterministic xorshift sequence, so a failing seed replays.
struct Seeded(u64);

impl Seeded {
    fn next(&mut self, below: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % below
    }
}

fn labelled(label: &str, email: &str) -> Vec<Property> {
    vec![
        Property::string("$label", label),
        Property::string("email", email),
    ]
}

/// Drives a build to its end with `limits`, counting the steps taken in
/// Scan.
async fn drive_build(
    db: &Db,
    driver: &SecondaryIndexDriver,
    operation_id: IndexOperationId,
    claim_sequence: &mut u64,
    limits: SearchIndexBatchLimits,
) -> (u32, CommittedOperationStep) {
    let mut scan_steps = 0;
    for _ in 0..10_000 {
        let scanning = matches!(
            build_stage(db, operation_id).await,
            SecondaryBuildStage::Scan(_)
        );
        let step = drive_one_with_limits(db, driver, operation_id, claim_sequence, limits).await;
        scan_steps += u32::from(scanning);
        if step != CommittedOperationStep::Progressed {
            return (scan_steps, step);
        }
    }
    panic!("the build did not end")
}

/// Puts `count` rows of `filler` before `ids` of `User`, every row below
/// `count` that `ids` does not name being `Other`.
async fn seed(
    db: &Db,
    count: u64,
    users: impl Fn(u64) -> bool,
) -> BTreeMap<u64, Vec<Property>> {
    let scope = DataScope::LegacyUnscoped;
    let mut sources = BTreeMap::new();
    for id in 0..count {
        let label = if users(id) { "User" } else { "Other" };
        let properties = labelled(label, &format!("{id}@example.com"));
        put_source(db, scope, IndexElementKind::Node, id, &properties).await;
        sources.insert(id, properties);
    }
    sources
}

fn email_index() -> ValidatedDynamicIndexDefinition {
    validated(SecondaryIndexDefinition::node_equality("User", "email").expect("definition"))
}

#[tokio::test]
async fn an_empty_label_among_many_rows_reaches_catch_up_in_one_step() {
    let db = test_db("label-source-empty-label").await;
    let sources = seed(&db, 5_000, |_| false).await;
    let definition = email_index();
    let (operation_id, index_id, generation) =
        create_build(&db, DataScope::LegacyUnscoped, &definition, 4_999).await;
    let driver = SecondaryIndexDriver::new(Arc::new(IndexScopeGates::default()));
    let mut claim_sequence = 1;
    drive_one(&db, &driver, operation_id, &mut claim_sequence).await;
    let SecondaryBuildStage::CatchUp(progress) = build_stage(&db, operation_id).await else {
        panic!("one step finishes the scan of an empty label");
    };
    assert_eq!(progress.counters, OperationCounters::default());
    assert_eq!(
        drive_to_terminal(&db, &driver, operation_id, &mut claim_sequence).await,
        CommittedOperationStep::Completed
    );
    assert_generation_equals_source(
        &db,
        DataScope::LegacyUnscoped,
        &definition,
        index_id,
        generation,
        &sources,
    )
    .await;
    db.close().await.expect("database closes");
}

#[tokio::test]
async fn a_small_label_among_many_rows_scans_in_one_step() {
    let db = test_db("label-source-small-label").await;
    let sources = seed(&db, 5_000, |id| id % 500 == 7).await;
    let definition = email_index();
    let (operation_id, index_id, generation) =
        create_build(&db, DataScope::LegacyUnscoped, &definition, 4_999).await;
    let driver = SecondaryIndexDriver::new(Arc::new(IndexScopeGates::default()));
    let mut claim_sequence = 1;
    drive_one(&db, &driver, operation_id, &mut claim_sequence).await;
    let SecondaryBuildStage::CatchUp(progress) = build_stage(&db, operation_id).await else {
        panic!("one step scans ten rows");
    };
    assert_eq!(progress.counters.entities, 10);
    assert_eq!(
        drive_to_terminal(&db, &driver, operation_id, &mut claim_sequence).await,
        CommittedOperationStep::Completed
    );
    assert_generation_equals_source(
        &db,
        DataScope::LegacyUnscoped,
        &definition,
        index_id,
        generation,
        &sources,
    )
    .await;
    db.close().await.expect("database closes");
}

#[tokio::test]
async fn each_of_many_labels_scans_only_its_own_rows() {
    let db = test_db("label-source-many-labels").await;
    let scope = DataScope::LegacyUnscoped;
    let labels = ["User", "Account", "Order", "Invoice"];
    let mut sources = BTreeMap::new();
    for id in 0..1_200_u64 {
        let label = labels[usize::try_from(id % 4).expect("index fits")];
        let properties = labelled(label, &format!("{id}@example.com"));
        put_source(&db, scope, IndexElementKind::Node, id, &properties).await;
        sources.insert(id, properties);
    }
    let limits = batch_limits(64, 1 << 20, 1 << 20, 1 << 30);
    for label in labels {
        let definition =
            validated(SecondaryIndexDefinition::node_equality(label, "email").expect("definition"));
        let (operation_id, index_id, generation) =
            create_build(&db, scope, &definition, 1_199).await;
        let driver = SecondaryIndexDriver::new(Arc::new(IndexScopeGates::default()));
        let mut claim_sequence = 1;
        let (scan_steps, end) =
            drive_build(&db, &driver, operation_id, &mut claim_sequence, limits).await;
        assert_eq!(end, CommittedOperationStep::Completed);
        // 300 rows at 64 a step; a scan of every row took 19.
        assert_eq!(scan_steps, 5, "{label}");
        assert_generation_equals_source(&db, scope, &definition, index_id, generation, &sources)
            .await;
    }
    db.close().await.expect("database closes");
}

/// Random graphs of several labels, built under several batch limits,
/// match what a scan of every row builds. Each graph has members whose row
/// is gone (in long runs too), a member whose row carries another label (as
/// a label-hash collision leaves), rows above the build's bound, id gaps,
/// and rows of other labels too large for any batch, which no longer block
/// the build.
#[tokio::test]
async fn builds_match_a_scan_of_every_row_on_random_graphs() {
    let scope = DataScope::LegacyUnscoped;
    let node = IndexElementKind::Node;
    for seed_value in 1..=6_u64 {
        let mut random = Seeded(0x9E37_79B9_7F4A_7C15 ^ seed_value);
        let db = test_db(&format!("label-source-differential-{seed_value}")).await;
        let mut sources = BTreeMap::new();
        let mut id = 0_u64;
        let mut absent = Vec::new();
        for _ in 0..1_500 {
            id += 1 + random.next(3);
            if random.next(40) == 0 {
                // A long run of members whose rows are gone.
                let run = 1 + random.next(600);
                absent.extend(id..id + run);
                id += run;
                continue;
            }
            let properties = match random.next(10) {
                0..=3 => labelled("User", &format!("{}@example.com", random.next(400))),
                4 => vec![Property::string("$label", "User")],
                5 if random.next(30) == 0 => labelled("Other", &"x".repeat(9_000)),
                _ => labelled("Other", &format!("{id}@example.com")),
            };
            put_source(&db, scope, node, id, &properties).await;
            sources.insert(id, properties);
        }
        let upper = id;
        // Rows above the bound write after the build's cut: a build leaves
        // them to their deltas, which this graph never has.
        for above in upper + 1..upper + 20 {
            put_source(&db, scope, node, above, &labelled("User", "late@example.com")).await;
        }
        let transaction = db
            .begin(IsolationLevel::Snapshot)
            .await
            .expect("bitmap write begins");
        absent
            .iter()
            .for_each(|&missing| testing::stage_member(&transaction, scope, node, "User", missing));
        let collided = *sources
            .iter()
            .find(|(_, properties)| properties[0].value.as_str() == Some("Other"))
            .expect("the graph has another label")
            .0;
        testing::stage_member(&transaction, scope, node, "User", collided);
        transaction.commit().await.expect("bitmap members commit");

        let limits = match seed_value % 3 {
            0 => SearchIndexBackfillLimits::default().batch(),
            1 => batch_limits(37, 1 << 20, 1 << 20, 1 << 30),
            // About 4 KiB: every 256-id chunk crosses the input bound.
            _ => batch_limits(1_024, 4_096, 1 << 20, 1 << 30),
        };
        let definition = email_index();
        let (operation_id, index_id, generation) =
            create_build(&db, scope, &definition, upper).await;
        let driver = SecondaryIndexDriver::new(Arc::new(IndexScopeGates::default()));
        let mut claim_sequence = 1;
        let (scan_steps, end) =
            drive_build(&db, &driver, operation_id, &mut claim_sequence, limits).await;
        assert_eq!(end, CommittedOperationStep::Completed, "seed {seed_value}");
        assert!(scan_steps >= 1);
        sources.retain(|&id, _| id <= upper);
        assert_generation_equals_source(&db, scope, &definition, index_id, generation, &sources)
            .await;
        db.close().await.expect("database closes");
    }
}

/// A row of the label too large for a batch still blocks the build, as it
/// did when every row was scanned.
#[tokio::test]
async fn an_oversized_row_of_the_label_still_blocks_the_build() {
    let db = test_db("label-source-oversized-member").await;
    let scope = DataScope::LegacyUnscoped;
    put_source(
        &db,
        scope,
        IndexElementKind::Node,
        3,
        &labelled("User", &"x".repeat(9_000)),
    )
    .await;
    let (operation_id, _, _) = create_build(&db, scope, &email_index(), 3).await;
    let driver = SecondaryIndexDriver::new(Arc::new(IndexScopeGates::default()));
    let mut claim_sequence = 1;
    assert_eq!(
        drive_one_with_limits(
            &db,
            &driver,
            operation_id,
            &mut claim_sequence,
            batch_limits(1_024, 4_096, 1 << 20, 1 << 30),
        )
        .await,
        CommittedOperationStep::Blocked
    );
    db.close().await.expect("database closes");
}

/// Writes during the scan move rows into and out of the label, delete them
/// and create them at, below and above the bound; the build converges on
/// the rows as they end.
#[tokio::test]
async fn writes_racing_the_scan_converge() {
    let scope = DataScope::LegacyUnscoped;
    let node = IndexElementKind::Node;
    for definition in [
        SecondaryIndexDefinition::node_equality("User", "email"),
        SecondaryIndexDefinition::node_unique_equality("User", "email"),
        SecondaryIndexDefinition::node_range("User", "email"),
    ] {
        let definition = validated(definition.expect("definition"));
        let db = test_db("label-source-racing-writes").await;
        let mut sources = seed(&db, 600, |id| id % 3 == 0).await;
        let (operation_id, index_id, generation) =
            create_build(&db, scope, &definition, 599).await;
        let driver = SecondaryIndexDriver::new(Arc::new(IndexScopeGates::default()));
        let mut claim_sequence = 1;
        let limits = batch_limits(25, 1 << 20, 1 << 20, 1 << 30);
        let mut random = Seeded(0xDEAD_BEEF);
        let mut next_id = 600;
        let mut write = 0;
        let end = loop {
            for _ in 0..3 {
                write += 1;
                let (id, after) = match random.next(5) {
                    // Into the label, or out of it.
                    0 => (random.next(600), labelled("User", &format!("in-{write}@x"))),
                    1 => (random.next(600), labelled("Other", &format!("out-{write}@x"))),
                    // A delete.
                    2 => (random.next(600), Vec::new()),
                    // A create below the bound, where a row was deleted.
                    3 => (random.next(600), labelled("User", &format!("re-{write}@x"))),
                    // A create above the bound.
                    _ => {
                        next_id += 1;
                        (next_id, labelled("User", &format!("new-{write}@x")))
                    }
                };
                let before = sources.get(&id).cloned().unwrap_or_default();
                mutate_source(&db, scope, node, id, &before, &after)
                    .await
                    .expect("the write records its delta");
                if after.is_empty() {
                    sources.remove(&id);
                } else {
                    sources.insert(id, after);
                }
            }
            match drive_one_with_limits(&db, &driver, operation_id, &mut claim_sequence, limits)
                .await
            {
                CommittedOperationStep::Progressed => {}
                end => break end,
            }
        };
        assert_eq!(end, CommittedOperationStep::Completed);
        assert_generation_equals_source(&db, scope, &definition, index_id, generation, &sources)
            .await;
        db.close().await.expect("database closes");
    }
}

/// A crash at every outbox boundary of a Scan step, followed by a reopen,
/// loses nothing: the step either committed whole or left its checkpoint.
#[test]
fn a_crash_at_every_scan_failpoint_resumes_after_reopen() {
    crate::test_isolation::in_own_process(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("runtime builds")
            .block_on(crash_at_every_scan_failpoint());
    });
}

async fn crash_at_every_scan_failpoint() {
    use crate::index_lifecycle::failpoints::{self, IndexOutboxFailpoint};

    let store = Arc::new(InMemory::new());
    let path = "label-source-scan-crashes";
    let open = || {
        Db::builder(path, store.clone())
            .with_merge_operator(Arc::new(crate::merge_operator::HelixMergeOperator::new()))
            .build()
    };
    let mut db = open().await.expect("crash test database opens");
    bootstrap_writer(&db).await.expect("database bootstraps");
    let scope = DataScope::LegacyUnscoped;
    let sources = seed(&db, 2_000, |id| id % 2 == 0).await;
    let definition = validated(
        SecondaryIndexDefinition::node_unique_equality("User", "email").expect("definition"),
    );
    let (operation_id, index_id, generation) = create_build(&db, scope, &definition, 1_999).await;
    let driver = SecondaryIndexDriver::new(Arc::new(IndexScopeGates::default()));
    let limits = batch_limits(40, 1 << 20, 1 << 20, 1 << 30);
    let mut claim_sequence = 1;
    let crashed_epoch = WriterEpoch::from_bytes([0x5C; 16]).expect("writer epoch is non-nil");
    for failpoint in [
        IndexOutboxFailpoint::ClaimAfter,
        IndexOutboxFailpoint::BatchReadBefore,
        IndexOutboxFailpoint::BatchReadAfter,
        IndexOutboxFailpoint::PhysicalStagingBefore,
        IndexOutboxFailpoint::PhysicalStagingAfter,
        IndexOutboxFailpoint::CheckpointStagingBefore,
        IndexOutboxFailpoint::CheckpointStagingAfter,
        IndexOutboxFailpoint::CommitBefore,
        IndexOutboxFailpoint::CommitAfter,
    ] {
        assert!(
            matches!(
                build_stage(&db, operation_id).await,
                SecondaryBuildStage::Scan(_)
            ),
            "{failpoint:?} strikes mid-Scan"
        );
        failpoints::inject_for_operation_once(failpoint, operation_id)
            .expect("failpoint installs");
        let OperationPointerObservation::Eligible(eligible) =
            observe_operation_pointer(&db, operation_id, crashed_epoch, NOW_MILLIS + 1)
                .await
                .expect("operation pointer is readable")
        else {
            panic!("the next Scan step is eligible");
        };
        let claimed = claim_operation(
            &db,
            &eligible,
            crashed_epoch,
            ClaimSequence::new(claim_sequence).expect("claim sequence is non-zero"),
            NOW_MILLIS + 1,
            ClaimPermission::Normal,
        )
        .await;
        claim_sequence += 1;
        // A failpoint in the claim leaves nothing to execute.
        let Ok(Some(claimed)) = claimed else {
            assert!(failpoints::was_triggered(), "{failpoint:?} fired in the claim");
            drop(db);
            db = open().await.expect("database reopens after the crash");
            drive_one_with_limits(&db, &driver, operation_id, &mut claim_sequence, limits).await;
            continue;
        };
        let _ = execute_claimed_step(&db, &claimed, &driver, limits, NOW_MILLIS + 1).await;
        assert!(failpoints::was_triggered(), "{failpoint:?} fired");
        drop(db);
        db = open().await.expect("database reopens after the crash");
        // One ordinary step after each crash, so every crash lands on a
        // build that has scanned further.
        drive_one_with_limits(&db, &driver, operation_id, &mut claim_sequence, limits).await;
    }
    let (_, end) = drive_build(&db, &driver, operation_id, &mut claim_sequence, limits).await;
    assert_eq!(end, CommittedOperationStep::Completed);
    assert_generation_equals_source(&db, scope, &definition, index_id, generation, &sources)
        .await;
    db.close().await.expect("database closes");
}

/// Progress keeps its shape across binaries. A cursor this binary leaves is
/// the property key of the last id it took, at or below the bound, which the
/// repository's checks accept on read, and the label's rows up to it are all
/// applied: an older binary's scan of every row resumes after it and covers
/// the rest. A cursor an older binary left on another label's row resumes
/// here after that row's id.
#[tokio::test]
async fn scan_progress_resumes_across_binaries_in_both_directions() {
    let scope = DataScope::LegacyUnscoped;
    let node = IndexElementKind::Node;
    let definition = email_index();
    let ValidatedDynamicIndexDefinition::Secondary(secondary) = &definition else {
        unreachable!("secondary fixture")
    };
    let db = test_db("label-source-cross-binary").await;
    let sources = seed(&db, 900, |id| id % 3 == 1).await;
    let (operation_id, index_id, generation) = create_build(&db, scope, &definition, 899).await;
    let driver = SecondaryIndexDriver::new(Arc::new(IndexScopeGates::default()));
    let limits = batch_limits(50, 1 << 20, 1 << 20, 1 << 30);

    // This binary's cursor: rows 1, 4, ... 148 are the label's first fifty.
    let mut claim_sequence = 1;
    drive_one_with_limits(&db, &driver, operation_id, &mut claim_sequence, limits).await;
    let SecondaryBuildStage::Scan(progress) = build_stage(&db, operation_id).await else {
        panic!("fifty of three hundred rows leave more to scan");
    };
    assert_eq!(progress.cursor, Some(source_cursor(scope, node, 148)));
    assert_eq!(generation_rows(&db, scope, RecordKind::AppliedState, index_id, generation).await.len(), 50);

    // An older binary's cursor, on row 299 of another label: the next step
    // resumes after id 299, at the label's row 301.
    let operation = read_operation(&db, scope, operation_id)
        .await
        .expect("operation reads")
        .expect("operation exists");
    let older = SourceScanProgress {
        inclusive_upper_bound: progress.inclusive_upper_bound.clone(),
        cursor: Some(source_cursor(scope, node, 299)),
        counters: progress.counters,
    };
    let transaction = db
        .begin(IsolationLevel::SerializableSnapshot)
        .await
        .expect("step begins");
    let result = scan_source(&db, &transaction, scope, &operation, secondary, &older, limits)
        .await
        .expect("the resumed step stages");
    transaction.commit().await.expect("the resumed step commits");
    let SecondaryBuildStage::Scan(resumed) = progressed_stage(&result) else {
        panic!("fifty more rows leave more to scan");
    };
    // Rows 301, 304, ... 448.
    assert_eq!(resumed.cursor, Some(source_cursor(scope, node, 448)));
    assert_eq!(generation_rows(&db, scope, RecordKind::AppliedState, index_id, generation).await.len(), 100);
    let label_rows = sources
        .iter()
        .filter(|(id, properties)| {
            canonical_value(secondary, properties, IndexEntityId::new(**id))
                .is_ok_and(|value| value.is_some())
        })
        .filter(|(id, _)| **id <= 148 || (300..=448).contains(*id))
        .count();
    assert_eq!(label_rows, 100);
    db.close().await.expect("database closes");
}
