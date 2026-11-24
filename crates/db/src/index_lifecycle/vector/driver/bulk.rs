//! In-memory bulk build of one unpartitioned vector generation.
//!
//! A serial build ([`super::scan_source`]) inserts one source entity at a time
//! and reads the stored graph for every insertion. A bulk build does the work
//! of the serial `Scan` stage under a stage of its own, in three passes:
//!
//! 1. **Read.** A background task reads the label's rows once, run by run,
//!    into memory ([`BulkVectors`]).
//! 2. **Link.** A dedicated OS thread links the whole graph on the configured
//!    construction threads ([`BulkGraph::construct`]).
//! 3. **Write.** Each lifecycle step rereads the next source rows, plans on a
//!    few threads the rows of every node whose source still holds the vector
//!    it was linked for, and commits the plans its limits admit with the scan
//!    cursor advanced past exactly their source rows.
//!
//! Bulk builds are off unless the configuration turns them on
//! ([`VectorBulkBuildMemory`]); see "Older binaries" for why.
//!
//! # Source
//!
//! Both reading passes read what the serial scan reads: the rows of the
//! members of the build's label, taken from the label's bitmap up to the
//! build's source bound ([`label_source`]). Pass 1 takes the members once,
//! from the bitmap of one snapshot it then drops, and reads their rows
//! through [`LabelSource::over`] in runs of at most [`READ_RUN_IDS`] ids and
//! [`READ_RUN_BYTES`], each under a snapshot of its own ([`SourceRows`]): no
//! snapshot or transaction is held across runs, because SlateDB keeps the
//! write set of every commit made while one is open. Pass 3 opens the serial
//! scan's own step source ([`LabelSource::open`]) on each step's snapshot. So
//! a member whose row is gone is skipped, a member that carries another
//! label is read and stores nothing, and the scan counters count the rows a
//! serial scan of the same label counts.
//!
//! A source with fewer vectors than a bulk build is started for is built
//! serially. A label with fewer members than that is one, without reading a
//! row; otherwise the rows of its first members are read, eight for each of
//! those vectors and no more than one run of pass 1 reads
//! ([`READ_RUN_BYTES`]), before the build waits for anything, so a small
//! build never queues behind a large one and the step that reads them holds
//! the lane for one bounded read; a source those rows do not settle is read
//! by pass 1, which counts. One in which pass 1 finds no vector has no graph:
//! the step that collects the pass ends the scan, as a serial scan of the
//! same rows would.
//!
//! # Stored state
//!
//! A build is created at the serial `Scan` stage. The step that starts a bulk
//! attempt stores the `BulkBuild` stage instead
//! ([`VectorBuildStage::BulkBuild`], stage byte `0x07`): the serial stage's
//! scan progress and the number of bulk attempts started for the operation
//! since it was last at the serial stage. It is the operation's stage for the
//! whole attempt and for every later one:
//!
//! | stored stage | cursor | meaning |
//! |---|---|---|
//! | `Scan` | absent | nothing is stored; no process has chosen a path |
//! | `Scan` | present | a serial scan |
//! | `BulkBuild` | absent | an attempt waits for its count to be durable, reads or links, or the rows of an abandoned one are being deleted |
//! | `BulkBuild` | present | pass 3 of the attempt the count names |
//!
//! The stage is left in two ways only. Forward, by the step that stages the
//! last node and the metadata row and stores `ValidateDescriptor` with them,
//! or that finds the source holds no vector. Back, to `Scan`, only after a
//! step of the same process scanned the generation's applied state and its
//! namespace from their start and found nothing, and nothing but that
//! process's steps writes to a generation being built: by the serial scan's
//! own first committed step, or by a step that stores `Scan` without a
//! cursor, which a build takes before it blocks and when bulk builds are
//! off. So rows of a partly written bulk graph are stored under the
//! `BulkBuild` stage and no other, which is what an older binary cannot read
//! (see "Older binaries").
//!
//! Every row is written by the typed writers a serial insert uses; only the
//! edges differ from the ones a serial build would choose. The scan counters
//! under either stage are plain progress of the scan that stores rows: zero
//! while an attempt reads and links, pass 3's own while it writes, reset by
//! the step that begins to delete an abandoned attempt, and the serial
//! scan's from zero after a build leaves the bulk path. A build over `N`
//! entities ends its scan at `N`, however it got there. A step that stores
//! the scan at its start says so to the outbox ([`StepCounters::Restarted`]),
//! which otherwise takes falling counters for corruption.
//!
//! # Memory
//!
//! A build reserves its memory before it reads a row and holds the
//! reservation until its graph is stored or dropped. The source's size is
//! exact: the label's members up to the build's bound, counted in one
//! snapshot and read from that set alone, so no later write can add a row
//! to what the build holds. The reservation is
//! [`BulkParameters::bytes_per_vector`] for each member, which the read pass trims to the vectors it found,
//! [`FIXED_ALLOWANCE_BYTES`] for what the passes make the process hold beside
//! the vectors and the graph, and an allowance for the writes SlateDB buffers
//! ([`WriteAhead`]).
//!
//! Pass 3 writes faster than SlateDB flushes, and SlateDB stalls writers
//! only at `max_unflushed_bytes` of encoded rows (1 GiB by default), which
//! memtables of small rows hold in up to three times as much memory. A build
//! that can reserve that takes the whole buffer and is done sooner by what
//! the buffer holds. One that cannot writes only while the memtables hold
//! less than two [`WRITE_WINDOW_BYTES`], whoever wrote what they hold, and
//! reserves only those two windows: it starts a flush at one window and
//! waits for it at two, so it writes no faster than SlateDB flushes and adds
//! nothing to a buffer other writers have filled. Rows those writers buffer
//! are theirs: the configuration counts the buffer at its encoded size, with
//! or without a build, and rows of a few bytes hold up to twice that again,
//! which no reservation of a build that paces itself covers.
//!
//! The budget every build of a process shares is
//! [`VectorBulkBuildMemory::budget_bytes`], taken against what the rest of
//! the process holds ([`BuildMemory`]). That is measured only while no build
//! holds a reservation, because a running build's memory cannot be told from
//! it, and what the builds are found to have left resident when the last of
//! them releases is taken to be theirs, memory the allocator kept and the
//! next build reuses, not the rest of the process's. So a build that fitted
//! before another ran fits after it. The allocator may hand that memory
//! back later, which looks the same as the rest of the process shrinking, so
//! the rest is never taken to hold less than it held before the builds
//! while that much is resident at all.
//!
//! A build that no budget would hold scans serially before it reads a row.
//! One that only the reservations of running builds keep out waits for them,
//! holding its turn for passes 1 and 2 so that later builds queue behind it:
//! a waiting build holds no memory, and the builds it waits for are in pass
//! 3, which needs no turn, so nothing waits in a cycle. With nothing
//! reserved, a build that does not fit scans serially rather than wait for
//! the rest of the process to shrink.
//!
//! # Waiting
//!
//! A build waits for its turn, for the memory of running builds, for its
//! own passes 1 and 2, and, when it paces its writes, for a flush. None of
//! these waits runs on a lifecycle step: the step that finds the build
//! waiting commits the stage unchanged and parks the operation
//! ([`Parking`]): that commit releases its claim, and the worker offers it
//! no step until what it waits for wakes it (the turn or memory released,
//! the passes or the flush ended) or [`PARK_LIMIT`] passes. A waiting build
//! therefore takes the vector lane for one short step when it starts to wait
//! and one each time it is woken, and other vector operations run as if it
//! were not queued. What it waits for is in its status
//! ([`VectorBuildReports`]) and logged once.
//!
//! # Crashes
//!
//! The graph lives only in this process, so a process that finds the
//! `BulkBuild` stage stored and holds no graph for it deletes whatever the
//! generation has stored, under that stage, before it does anything else.
//! The first deleting step also restarts the stored scan. The generation's
//! applied-state rows go first, then its physical rows. Each step deletes a
//! bounded batch and hands the next one the last row it deleted
//! ([`DiscardResume`]), in memory only, so a step does not read the
//! tombstones of every step before it. Only a step that scans both from
//! their start and finds nothing ends the deletion.
//!
//! A step whose commit fails leaves the graph at its last confirmed position
//! ([`BulkBuildCommit`]), and the next step repeats it. What this process
//! holds of a build is used only while the stored stage is the one its own
//! last step left; otherwise it looks at the stored stage again as a
//! restarted process does. A scan this process decided to run serially
//! needs no such check: only its own serial steps move it, because a database
//! has one writer and SlateDB fences a writer another process opened over, so
//! a process that lost the operation cannot commit. A partially linked graph
//! is never activated: validation follows only the step that stages the last
//! node and the metadata row together.
//!
//! # Retries
//!
//! The stored count is the number of bulk attempts started for the operation
//! since its record was last at the serial stage. A process starts an
//! attempt by committing the count raised by one, with the stage
//! ([`Begun::Commit`]), while it already holds the attempt's turn and
//! memory. The next step waits for that commit to be durable before it reads
//! a source row, because a progressed step does not wait for its WAL upload:
//! an attempt that kills its process has been counted, however soon it does.
//!
//! A build whose count has reached [`BulkBuildAttempts::LIMIT`] starts no
//! other. Once nothing of the last attempt is stored it scans serially, and
//! the status says why ([`VectorSerialBuildReason::EarlierAttemptsFailed`]).
//! The serial scan's first committed step stores the `Scan` stage with its
//! cursor, which every later process continues serially: the count is not
//! needed again, and no process can take the build for a new one.
//!
//! What counts is every attempt that ended without the build finishing its
//! bulk stage or leaving it: the process was killed, crashed, was stopped or
//! restarted, or lost the database to another writer; or it dropped the
//! attempt itself and started over (a run of pass 1 that still failed after
//! its retries, a source row repaired while it was reported, a stored stage
//! that moved without its graph). Each cost a read of the source, which is
//! what the limit bounds. What does not count: a run of pass 1 that failed
//! with an error the outbox would retry and was read again in place, up to
//! [`READ_RUN_RETRIES`] times a run ([`SourceRows`]), which costs that run
//! and no more; a write step whose commit did not land, which is repeated;
//! an upload of the count that is retried; an attempt that never started (a
//! build that waits for its turn or for memory, one whose counting commit
//! did not land, one that goes serial before it reads); and anything before
//! the record was last at the serial stage, because a build that returns
//! there (it blocked, see "Blocked builds", or bulk builds were turned off)
//! stores no count and starts again at one. An attempt that fails where a
//! serial build may not is not tried again at all (see "Failures"), and
//! neither is one whose source changed too much (see "What unlinks cost").
//! The read that sizes a build before any attempt runs on a lifecycle step
//! and is never read again in place: its failure is the step's.
//!
//! The limit is three. A build the OOM killer ends takes its server with it
//! every time, so it must stop soon; and a bulk build takes a small fraction
//! of a serial one's time, so three lost attempts cost less than the serial
//! build they fall back to. A build of 50 million vectors runs for hours and
//! may well be restarted once or twice for unrelated reasons, a deploy and a
//! node replacement; it keeps the bulk path through both, where a limit of
//! two would send it to a serial build of days after the second.
//!
//! A process that is asked to stop does not give its attempt back, although
//! it could still commit. Nothing tells a routine deploy from the stop an
//! orchestrator sends a process that fails its health checks or holds too
//! much memory, which is the same signal: a build that makes its process
//! unhealthy would then be restarted without bound, the loop the limit
//! exists to end. Nor could the first attempt be given back from pass 3,
//! whose rows are stored under the bulk stage, which holds no count of zero.
//! So three restarts of any kind inside one build's bulk stage send it
//! serial, and an operator who wants the bulk path back has two ways:
//! aborting the build and creating the index again, and starting the server
//! once with bulk builds off, which stores the `Scan` stage without a
//! cursor.
//!
//! # Blocked builds
//!
//! A blocked operation keeps its record until an operator retries or aborts
//! it, and has no queue entry, so a binary from before bulk builds would open
//! over it and then fail every request that reads the record (see "Older
//! binaries"). A build therefore never blocks at the `BulkBuild` stage while
//! it can leave it: it first deletes what the attempt stored, then stores
//! the `Scan` stage without a cursor, and blocks in the step after that
//! ([`Blocking`]), so the blocked record is one every binary reads. This
//! holds for a source row the serial scan would block on (invalid or
//! oversized), from whichever pass meets it, with the same blocker at the
//! same entity; for a node no step can store; and for the serial scan that
//! takes a build over from the bulk stage, when its first step blocks or
//! fails instead of storing the serial stage with its first rows. The row is
//! read again through the blocking step's transaction, and a row repaired or
//! deleted by then blocks nothing: the build starts over.
//!
//! A blocked build holds no graph and no reservation: a blocker waits for an
//! operator, for a time no other build should wait with it. Its retry starts
//! from the serial stage, as a new build does, with a first attempt.
//!
//! One build stays blocked at the `BulkBuild` stage: one whose stored rows
//! cannot be deleted, because the deletion itself blocks or fails. Aborting
//! it is the way out.
//!
//! # Failures
//!
//! A bulk attempt can fail in ways a serial build cannot: a construction
//! or staging thread that does not start or panics, a write step that
//! panics on an invariant of the in-memory graph ([`BulkBuilds::write`]), a
//! graph an unlink left torn, an upload of the attempt's count that fails
//! for good. Such a failure never blocks the build, because the serial path
//! could finish it, and it is not tried again, because it would likely fail
//! the same way, in this process and after an operator's retry alike. The
//! step that meets it commits nothing, the next ones delete what the attempt
//! stored, under the bulk stage, and the serial scan then builds the index
//! at once ([`BulkBuilds::fall_back`]; the status says
//! [`VectorSerialBuildReason::BulkAttemptFailed`]). The failure is logged as
//! an error, a defect of this path, except the upload storage refused, which
//! is logged as that. A step that fails while it deletes leaves the build
//! deleting, with what it does afterwards: its retry goes on, and no other
//! attempt starts in between. A failure the serial scan then meets itself
//! blocks the build under the serial stage, as it would have without a bulk
//! attempt. A process restarted before the serial scan's first commit finds
//! the bulk stage stored and starts the next attempt, which the limit of
//! attempts bounds.
//!
//! # Older binaries
//!
//! A binary from before bulk builds knows only the serial scan, which would
//! take a partly written bulk graph for a scan that has stored no vector
//! yet, insert the remaining entities beside rows whose links name nodes it
//! never stores, and activate the result. It cannot: it knows no stage
//! `0x07`, fails to decode the operation record, and every writer open
//! decodes every queued operation record
//! ([`crate::index_lifecycle::outbox::reconcile_operation_queue`]) before it
//! runs a step, so such a binary refuses to open the database as a writer
//! for as long as any build is at the `BulkBuild` stage. It writes no row of
//! the build and leaves the operation record as it is; what its open does
//! write is SlateDB's own (the writer fence, and the replayed WAL as
//! tables). That fence is the cost: the open that fails still takes the
//! database from the writer that was running the build, which must be
//! restarted. A reader-role process of such a binary opens and serves; only
//! the status of that one operation fails to decode.
//!
//! So bulk builds are off by default, and are turned on only once every
//! process that can open the database as a writer runs a binary that knows
//! the stage. While they are off this binary stores no `BulkBuild` stage.
//!
//! An operator who must go back to such a binary while a build is at the
//! `BulkBuild` stage (its status reports stage `scan` with
//! `vector_build_path.path` `bulk`, from any process, for as long as that
//! stage is what a restart would find: see
//! [`crate::index_lifecycle::status_record`]) first takes the build off the
//! bulk path with this binary, in one of two ways:
//!
//! - Start it with bulk builds off
//!   ([`VectorBulkBuildMemory::Off`]). The build's next steps delete what is
//!   stored and then store the `Scan` stage, whatever the source holds and
//!   however many attempts were counted; the build goes on serially, here or
//!   under the older binary.
//! - Abort the build. An abort stores the cleanup progress every binary
//!   reads, and cleanup deletes the partly written graph like any other.
//!
//! A build that finished its bulk stage needs neither: `ValidateDescriptor`
//! and everything after it are stages every binary reads.
//!
//! # Source changes
//!
//! Pass 3 reads source rows as every build does (see "Build source reads" in
//! [`crate::index_lifecycle::queue`]), later than pass 1 did. A node is
//! stored only while its source vector hashes to the value pass 1 linked it
//! for. An entity changed, deleted, added or moved to or from the label in
//! between has a queued operation that inserts or removes it after
//! activation, so its node is removed from the in-memory graph
//! ([`BulkGraph::unlink`]) and never stored, and the rows of already stored
//! neighbors that linked to it are staged again in the same step.
//!
//! Pass 1 reads its runs at different moments, none earlier than the
//! snapshot the members were taken in. That changes nothing above: whichever
//! version of a row a run read, the row was written after the build was
//! created, so it has a queued operation whenever it differs from what pass
//! 3 finds, and pass 3 stores the node only when it finds that very vector.
//! A member added after the members were taken is read by neither pass 1
//! nor, as a node, by pass 3, and reaches the index through its operation.
//!
//! ## What unlinks cost
//!
//! An unlink relinks every neighbor of the node it removes
//! ([`BulkGraph::unlink`]) on the lifecycle step's task: 4.3 ms measured at
//! 128 dimensions and 20.9 ms at 1,536, at the default degree, where a whole
//! bulk build costs 0.14 ms and 0.26 ms a vector that did not change. Two
//! bounds keep unlinks from costing more than the build they save.
//!
//! A step ends once its unlinks have taken [`UNLINK_STEP_TIME`], whether
//! their entities are gone or hold other vectors, and makes no more of them
//! than the entities it may read. Its unlinks therefore hold the family's
//! lane for that long and one unlink more, at any dimension, and the next
//! step goes on where this one stopped.
//!
//! A build whose source changed too much gives its attempt up
//! ([`gives_up`]). A bulk build of `n` vectors of which a share `f` changed
//! costs `n * (b + f * u)`, for `b` a vector built and `u` an unlink, and a
//! serial one `n * s`; the changed entities' queued operations cost both the
//! same. Measured on S3, a million vectors of 128 dimensions: `b` 0.14 ms,
//! `s` 1.77 ms, `u` 3.6 to 4.3 ms. A quarter of a million of 1,536
//! dimensions: `b` 0.26 ms, `s` 3.14 ms, `u` 20.9 ms. So bulk is the slower
//! of the two past a changed share of 38% to 45% at 128 dimensions and of
//! 14% at 1,536. The rule takes one share for every dimension, an eighth,
//! just under the lower of the two: where unlinks cost most, a build stops
//! paying for them before a serial build would have been the faster, and
//! where they cost less it gives up sooner than it had to, for the serial
//! build it would have been without this path.
//!
//! The share is judged on what is left to do, not on what is spent. Once
//! the pass has made [`UNLINKS_BEFORE_JUDGING`] unlinks, it gives up if the
//! nodes it has not passed would hold, at the share it has unlinked so far,
//! more unlinks than an eighth of the graph. So a pass with less than an
//! eighth of its nodes left always finishes, and a source that changed at a
//! steady share above an eighth is given up after about a thousand unlinks,
//! which take seconds, not after an eighth of the graph, which would take
//! longer than the serial build. The build then deletes what the attempt
//! stored and scans serially at once, without another attempt; nothing
//! failed, and the status says
//! [`VectorSerialBuildReason::TooManyRowsChanged`].
//!
//! # Scaling
//!
//! - The graph and its vectors are never spilled, so a label whose members
//!   do not fit the budget is built serially. The reservation counts
//!   members, so a label most of whose members hold no vector is
//!   overestimated by that ratio until pass 1 has read it.
//! - The label's bitmap is read whole, once for the size of the source and
//!   by every step of pass 3, as every step of a serial scan does; see
//!   [`label_source`] for what that costs past 100 million members.
//! - Passes 1 and 2 run for one build at a time, so construction threads are
//!   never oversubscribed.
//! - A restarted attempt reads the whole source again: nothing of passes 1
//!   and 2 is kept across processes.
//! - Pass 1 reads a label whose ids are dense in its key range by scans that
//!   keep at most [`READ_AHEAD_BYTES`] of fetches in flight across the SSTs
//!   the manifest holds when each scan opens ([`read_tuning`]), and a sparse
//!   one by point reads of up to [`READ_POINT_IDS`] rows, one after another:
//!   a sparse label of rows far smaller than a storage block reads slower
//!   than its bytes would suggest.
//! - Pass 3 scans the source a second time and writes every row from one
//!   lifecycle step at a time, bounded by the step limits. Rows and cursor
//!   commit in one transaction, so the step's commit is the only writer; only
//!   planning a step's rows is spread over [`STAGING_THREADS`] threads. Each
//!   step opens its own source, sized by what the last step admitted, and
//!   drops what it fetched beyond the rows it admits.
//! - A pass 3 that paces itself writes no faster than SlateDB flushes
//!   memtables, which stops while level 0 is full and compaction is behind,
//!   and writes nothing while other writers keep the memtables above two
//!   windows: under sustained ingest at SlateDB's own limit it waits for the
//!   ingest to end.
//! - Unlinking relinks every neighbor of the unlinked node from their
//!   neighborhood, as the serial delete does, on the lifecycle step's task,
//!   one node after another: a step's unlinks are bounded in time and a
//!   build's in share (see "What unlinks cost"). A build that keeps under
//!   the share still pays for every unlink: a tenth of a million vectors
//!   of 1,536 dimensions changed adds about 35 minutes to a build of under
//!   eight. A step that will unlink several nodes relinks the neighbors of
//!   the first against the others before it unlinks those too: which nodes
//!   a step unlinks is known only as it reads, and how many only when its
//!   time is up. A source rewritten wholesale between pass 1 and pass 3 is
//!   given up after about a thousand unlinks and built serially.
//! - Pass 1 holds the rows of one run, up to [`READ_RUN_BYTES`] and the
//!   rows one more read takes, beside the vectors of the runs before it.
//! - Deleting a partial graph costs a delete per stored row, and then one
//!   scan over every tombstone to prove the namespace empty, in a single
//!   step's transaction: about 2,600 entities a second measured on S3, three
//!   times as long as the rows took to write. A process restarted while it
//!   deletes pays that scan once more, over the tombstones written so far.
//!   Aborting a partly written build deletes through the cleanup every build
//!   shares, which was slower still: 46 to 60 minutes for a fifth of a
//!   million-vector graph.
//! - A build whose operation never runs another step in this process keeps
//!   its reservation, and builds waiting for it keep waiting. Every way an
//!   operation leaves its scan (completion, a blocker, an abort) runs a step
//!   or a commit here that drops it.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use bytes::Bytes;
use slatedb::{Db, DbTransaction, IsolationLevel};

use super::{
    corruption, delete_delta_and_applied_rows, delete_physical_namespace, deterministic_layer,
    lifecycle_write_measurement, measurement_error, operation_error, progressed_build,
    stage_applied, AppliedStateTransition, CleanupWorkOutcome, IndexStateVectorPhysical,
    PhysicalCleanupOutcome, VectorBatchAccounting, VectorPlanTarget, VectorStepResult,
};
use crate::config::{
    self, IndexLifecycleScanTuning, SearchIndexBatchLimits, VectorBulkBuildMemory,
    VectorBulkBuildTuning,
};
use crate::encoding::v2::keys::scope::DataScope;
use crate::encoding::v2::keys::RecordKind;
use crate::encoding::v2::values::property::view;
use crate::encoding::NodeId;
use crate::error::{HelixDbError, Result};
use crate::index_lifecycle::label_source::{self, LabelRead, LabelSource};
use crate::index_lifecycle::outbox::{
    self, CommittedStepState, IndexOperationStepResult, StepCounters, VectorPlanningUsage,
};
use crate::index_lifecycle::vector::{stored_vector_document, VectorIndexedDocument};
use crate::index_lifecycle::worker::IndexWorkerWakeHandle;
use crate::index_lifecycle::{
    BulkBuildAttempts, BulkBuildProgress, IndexCursor, IndexElementKind, IndexEntityId,
    IndexGenerationId, IndexId, IndexOperationBlocker, IndexOperationId, IndexOperationRecord,
    IndexRecordV2, OperationCounters, PrefixScanProgress, SourceScanProgress,
    ValidatedVectorIndexDefinition, VectorBuildPath, VectorBuildStage, VectorBulkBuildPhase,
    VectorBulkBuildWait, VectorPhysicalIndexId, VectorPhysicalLayout, VectorSerialBuildReason,
};
use crate::search::vector::{
    self, distance, BulkConstruction, BulkGraph, BulkParameters, BulkVectors, Distance, LinkChange,
    MeasuredVectorTransaction, PlannedVectorMutation, ValidatedVectorBuildGenerationHandle,
    VectorCleanupRow, VectorIndex, VectorIndexConfig, VectorWriteMeasurement, VectorWriteRecorder,
};

/// Longest a parked operation goes without a step: a wake that never came
/// delays a build by this much, not for good.
const PARK_LIMIT: Duration = Duration::from_secs(30);

/// Least time between two looks at the process's resident memory that the
/// steps of running builds ask for: a look reads `/proc/self/status`.
const OBSERVE_EVERY: Duration = Duration::from_millis(250);

/// Memory a build makes the process hold beside its per-vector bytes and the
/// writes SlateDB buffers, in the pass that holds most.
///
/// What a pass allocates itself is small: the fetches pass 1 keeps in flight
/// (at most [`READ_AHEAD_BYTES`] and one run of [`READ_RUN_BYTES`]), the
/// partly filled last vector chunk, the search buffers of the construction
/// threads, and the rows of one write step. Nearly all of the allowance is
/// memory the process has freed and the allocator has not returned to the OS:
/// buffers of the source reads, which read the source twice, and of the
/// flushes and compactions the written rows go through. It follows the bytes
/// that pass through, not the size of the source or the number of threads.
/// Beside their vectors, their graph and the block cache, builds of 250,000
/// vectors of 1,536 dimensions that wrote two windows ahead held 2.6 to 3.6
/// GiB at their peak, windows included, in six runs on 2 to 64 cores, and a
/// build of a million vectors of 128 dimensions 1.9 GiB.
const FIXED_ALLOWANCE_BYTES: u64 = 4 << 30;

/// Memtable bytes at which a build that paces its writes starts a flush, and
/// half of those at which it waits for one. SlateDB freezes a memtable at
/// this size by default, so a waited flush writes the SST the memtable would
/// have become anyway.
const WRITE_WINDOW_BYTES: u64 = 64 << 20;

/// Times its encoded size a row holds in a memtable and in the SST it is
/// flushed into together: a row of a few bytes holds up to three, most of it
/// in its skip-list node.
const WRITE_BUFFER_FOOTPRINT: u64 = 3;

/// Per-vector allowance for the arrays pass 1 grows by doubling (entity ID,
/// source hash, layer and distance header: at most 21 bytes a vector), which
/// hold up to three times their content while one moves.
const GROWTH_SLACK_BYTES: u64 = 48;

/// Most bytes the fetches of one pass-1 scan hold in flight, across every
/// SST the scan merges; see [`read_tuning`].
const READ_AHEAD_BYTES: usize = 256 << 20;

/// Most a pass-1 scan reads ahead per fetch.
const SOURCE_READ_AHEAD_BYTES: usize = 4 << 20;

/// Least a pass-1 scan reads ahead per fetch: SlateDB's own default for a
/// tuned lifecycle scan.
const SOURCE_MIN_READ_AHEAD_BYTES: usize = 256 << 10;

/// Most fetches a bulk build's source scan keeps in flight for one SST.
const SOURCE_FETCH_TASKS: usize = 16;

/// Most label members pass 1 takes for one run of row reads. A run chooses
/// between one scan and point reads by how dense its own ids are.
const READ_RUN_IDS: usize = 1 << 16;

/// Row bytes after which a pass-1 run ends and the next one starts.
const READ_RUN_BYTES: u64 = 64 << 20;

/// Times a pass-1 run is read again after a failure the outbox would retry,
/// before the failure ends the attempt.
const READ_RUN_RETRIES: u32 = 3;

/// Wait before the first retry of a pass-1 run: the retries of one run wait
/// three seconds together.
const READ_RUN_RETRY_DELAY: Duration = Duration::from_millis(500);

/// Most rows one point read of pass 1 fetches, where a serial step reads
/// [`label_source::POINT_CHUNK_IDS`]: pass 1 has no step limit to overrun.
const READ_POINT_IDS: usize = 256;

/// Most threads that plan the node rows of one pass-3 step.
const STAGING_THREADS: usize = 4;

/// Fewest nodes of a pass-3 step that are worth a planning thread.
const NODES_PER_STAGING_THREAD: usize = 32;

/// Time one pass-3 step spends unlinking before it ends.
///
/// An unlink runs on the step's task, which holds the family's lane and the
/// operation's claim, and costs from milliseconds to tens of them with the
/// dimension ([`BulkGraph::unlink`]). A step always makes its first unlink
/// and makes no other once this much time went into them, so a step's
/// unlinks take this long and one unlink more, whatever the dimension.
const UNLINK_STEP_TIME: Duration = Duration::from_millis(250);

/// Unlinks a write pass makes before it judges whether its source changed
/// too much to finish in bulk ([`gives_up`]): enough for the share it has
/// seen to stand for the rest, and few enough to cost seconds.
const UNLINKS_BEFORE_JUDGING: u32 = 1_024;

/// A write pass gives its attempt up once the nodes it still has to pass
/// hold, at the share it has unlinked so far, more unlinks than one in this
/// many of the graph's nodes (see "Source changes" in the module
/// documentation for the measurements behind an eighth).
const GIVES_UP_PER_NODES: u32 = 8;

/// Operations whose build path [`VectorBuildReports`] remembers.
const REMEMBERED_BUILD_PATHS: usize = 256;

/// Label members whose rows a build reads, for each vector a bulk build is
/// started for, to learn whether its source holds that many.
const PROBE_ROWS_PER_VECTOR: u64 = 8;

/// What this process's bulk attempt of one operation is doing.
#[derive(Debug, Clone)]
enum Activity {
    Waiting(VectorBulkBuildWait),
    /// Passes 1 and 2, which count as they go.
    Preparing(Arc<PreparationProgress>),
    Writing {
        nodes_written: u64,
        nodes: u64,
        paused_for_write_buffer: bool,
    },
    Discarding {
        rows_deleted: u64,
    },
}

/// Counts of one attempt's passes 1 and 2, which run off the lifecycle step.
#[derive(Debug, Default)]
struct PreparationProgress {
    /// Label members the attempt reads.
    members: u64,
    members_read: AtomicU64,
    /// Vectors the read pass found, set once it ends and linking starts.
    vectors: OnceLock<u64>,
    vectors_linked: AtomicU64,
}

/// What the vector builds of this process report beside their stored
/// progress: the path each took, and what its bulk attempt is doing now.
///
/// Only that a build is on the bulk path is stored, with its stage and its
/// attempt count. Why a build scans serially, that a finished build was a
/// bulk one, and what a running attempt is doing is known to the process
/// that runs it: unknown after a restart until the operation's next scan
/// step, and to processes that only read.
#[derive(Debug, Default)]
pub(crate) struct VectorBuildReports {
    recent: parking_lot::Mutex<VecDeque<(IndexOperationId, VectorBuildPath)>>,
    active: parking_lot::Mutex<HashMap<IndexOperationId, Activity>>,
    /// Operations a scan step of this process found stored at the bulk
    /// stage, each with how many steps did: until a status read finds the
    /// applied and the durable record equal off that stage, the durable one
    /// may still be at it.
    bulk_stage: parking_lot::Mutex<VecDeque<(IndexOperationId, u64)>>,
    /// Where a test stops status reads ([`Self::before_status_records`]).
    #[cfg(test)]
    pub(crate) status_gate: parking_lot::Mutex<Option<Arc<StatusGate>>>,
}

/// A test's hold on status reads, between their first look at the sightings
/// and their read of the operation's records.
#[cfg(test)]
#[derive(Debug)]
pub(crate) struct StatusGate {
    /// One permit for each status read that has reached the gate.
    pub(crate) arrived: tokio::sync::Semaphore,
    /// One permit lets one status read through.
    pub(crate) open: tokio::sync::Semaphore,
}

impl VectorBuildReports {
    /// Stops a status read at the gate a test set, until the test lets it
    /// through: steps the worker takes meanwhile happen between the two
    /// looks at the sightings.
    #[cfg(test)]
    pub(crate) async fn before_status_records(&self) {
        let gate = self.status_gate.lock().clone();
        let Some(gate) = gate else {
            return;
        };
        gate.arrived.add_permits(1);
        gate.open
            .acquire()
            .await
            .expect("the status gate stays open")
            .forget();
    }

    /// Path the latest scan of `operation_id` in this process took.
    pub(crate) fn path(&self, operation_id: IndexOperationId) -> Option<VectorBuildPath> {
        self.recent
            .lock()
            .iter()
            .find(|(recorded, _)| *recorded == operation_id)
            .map(|(_, path)| *path)
    }

    /// What this process's bulk attempt of `operation_id` is doing, or
    /// `None` when it runs none.
    pub(crate) fn phase(&self, operation_id: IndexOperationId) -> Option<VectorBulkBuildPhase> {
        let activity = self.active.lock().get(&operation_id).cloned()?;
        Some(match activity {
            Activity::Waiting(waiting_for) => VectorBulkBuildPhase::Waiting { waiting_for },
            Activity::Preparing(progress) => match progress.vectors.get() {
                None => VectorBulkBuildPhase::Reading {
                    members_read: progress
                        .members_read
                        .load(Ordering::Relaxed)
                        .min(progress.members),
                    members: progress.members,
                },
                Some(vectors) => VectorBulkBuildPhase::Linking {
                    vectors_linked: progress
                        .vectors_linked
                        .load(Ordering::Relaxed)
                        .min(*vectors),
                    vectors: *vectors,
                },
            },
            Activity::Writing {
                nodes_written,
                nodes,
                paused_for_write_buffer,
            } => VectorBulkBuildPhase::Writing {
                nodes_written,
                nodes,
                paused_for_write_buffer,
            },
            Activity::Discarding { rows_deleted } => {
                VectorBulkBuildPhase::Discarding { rows_deleted }
            }
        })
    }

    /// How many scan steps of this process found `operation_id` stored at
    /// the bulk stage, or `None` when none did since the durable record was
    /// last read off it: the applied record is then the one a restart finds,
    /// as far as the bulk stage goes.
    pub(crate) fn bulk_stage_sightings(&self, operation_id: IndexOperationId) -> Option<u64> {
        self.bulk_stage
            .lock()
            .iter()
            .find(|(seen, _)| *seen == operation_id)
            .map(|(_, sightings)| *sightings)
    }

    /// Forgets `operation_id` once a status read that began at `sightings`
    /// found its applied and durable records equal off the bulk stage: what
    /// a restart finds cannot return to that stage without another step of
    /// this process finding the operation there, which counts again.
    pub(crate) fn durably_left_bulk_stage(&self, operation_id: IndexOperationId, sightings: u64) {
        self.bulk_stage
            .lock()
            .retain(|seen| *seen != (operation_id, sightings));
    }

    /// Counts a scan step that found `operation_id` stored at the bulk
    /// stage.
    fn found_at_bulk_stage(&self, operation_id: IndexOperationId) {
        let mut seen = self.bulk_stage.lock();
        let Some((_, sightings)) = seen.iter_mut().find(|(seen, _)| *seen == operation_id) else {
            if seen.len() == REMEMBERED_BUILD_PATHS {
                seen.pop_front();
            }
            seen.push_back((operation_id, 1));
            return;
        };
        *sightings += 1;
    }

    fn record(&self, operation_id: IndexOperationId, path: VectorBuildPath) {
        let mut recent = self.recent.lock();
        recent.retain(|(recorded, _)| *recorded != operation_id);
        if recent.len() == REMEMBERED_BUILD_PATHS {
            recent.pop_front();
        }
        recent.push_back((operation_id, path));
    }

    /// Replaces what the attempt of `operation_id` is doing; `None` when the
    /// process runs none.
    fn publish(&self, operation_id: IndexOperationId, activity: Option<Activity>) {
        let mut active = self.active.lock();
        match activity {
            Some(activity) => active.insert(operation_id, activity),
            None => active.remove(&operation_id),
        };
    }
}

/// Operations whose next step could only wait, and until when at the latest.
///
/// The worker offers a parked operation no step and ends a claim slice that
/// reaches one ([`crate::index_lifecycle::outbox::IndexOperationDriver::parked_until`]).
/// Whatever a build waits for removes its entry and wakes the worker, so a
/// build parks before it looks at what it waits for: a release between the
/// look and the entry would otherwise wake nobody.
pub(super) struct Parking {
    parked: parking_lot::Mutex<HashMap<IndexOperationId, Instant>>,
    worker: IndexWorkerWakeHandle,
    /// What a test runs at the moment every parked operation is woken.
    #[cfg(test)]
    woken: parking_lot::Mutex<Option<Box<dyn Fn() + Send + Sync>>>,
}

impl Parking {
    fn new(worker: IndexWorkerWakeHandle) -> Self {
        Self {
            parked: parking_lot::Mutex::new(HashMap::new()),
            worker,
            #[cfg(test)]
            woken: parking_lot::Mutex::new(None),
        }
    }

    /// When the worker offers `operation_id` a step again at the latest, or
    /// `None` when it is not parked.
    pub(super) fn until(&self, operation_id: IndexOperationId) -> Option<Instant> {
        self.parked.lock().get(&operation_id).copied()
    }

    fn park(&self, operation_id: IndexOperationId) {
        self.parked
            .lock()
            .insert(operation_id, Instant::now() + PARK_LIMIT);
    }

    /// Removes the entry of an operation whose step found nothing to wait
    /// for. Nobody needs waking: the operation is running a step.
    fn clear(&self, operation_id: IndexOperationId) {
        self.parked.lock().remove(&operation_id);
    }

    /// Lets `operation_id` step again.
    fn wake(&self, operation_id: IndexOperationId) {
        if self.parked.lock().remove(&operation_id).is_some() {
            self.worker.wake();
        }
    }

    /// Lets every parked operation step again: something several may wait
    /// for was released.
    fn wake_all(&self) {
        let mut parked = self.parked.lock();
        if parked.is_empty() {
            return;
        }
        parked.clear();
        drop(parked);
        #[cfg(test)]
        match &*self.woken.lock() {
            None => {}
            Some(woken) => woken(),
        }
        self.worker.wake();
    }
}

/// Wakes one operation when dropped: held by a background task the
/// operation waits for, so the task's end wakes it however the task ends.
struct WakeOnDrop {
    parking: Arc<Parking>,
    operation_id: IndexOperationId,
}

impl Drop for WakeOnDrop {
    fn drop(&mut self) {
        self.parking.wake(self.operation_id);
    }
}

/// The turn for passes 1 and 2, which one build holds at a time. Dropping it
/// wakes the builds that wait for it.
///
/// The fields drop in this order: the permit goes back before the waiting
/// builds are woken. Woken first, a build stepped on another thread could
/// find the turn still taken and park again, with no wake to come when the
/// permit went back a moment later: it would wait out [`PARK_LIMIT`] beside
/// a free turn.
struct Turn {
    _permit: tokio::sync::OwnedSemaphorePermit,
    _woken: WakeAllOnDrop,
}

/// Wakes every parked operation when dropped.
struct WakeAllOnDrop(Arc<Parking>);

impl Drop for WakeAllOnDrop {
    fn drop(&mut self) {
        self.0.wake_all();
    }
}

/// How far ahead of SlateDB's memtable flushes a build's write pass runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WriteAhead {
    /// As far as SlateDB lets any writer: until its write buffer is full and
    /// stalls them. The build finishes sooner by what the buffer holds.
    Unbounded,
    /// While the memtables hold less than two windows of
    /// [`WRITE_WINDOW_BYTES`], whoever wrote them: the build starts a flush
    /// at one window and waits for it at two.
    Windowed,
}

/// Where the memory figures of the process come from.
#[derive(Debug, Clone)]
enum HostMemory {
    /// The process's own: its cgroup or machine ceiling and its resident
    /// anonymous memory.
    Process,
    /// Figures a test sets.
    #[cfg(test)]
    Fixed {
        ceiling: Option<u64>,
        resident: Arc<AtomicU64>,
    },
}

impl HostMemory {
    fn ceiling(&self) -> Option<u64> {
        match self {
            Self::Process => config::memory_ceiling_bytes(),
            #[cfg(test)]
            Self::Fixed { ceiling, .. } => *ceiling,
        }
    }

    fn resident(&self) -> Option<u64> {
        match self {
            Self::Process => config::resident_anonymous_bytes(),
            #[cfg(test)]
            Self::Fixed { resident, .. } => Some(resident.load(Ordering::Relaxed)),
        }
    }

    /// Least time between two looks at the resident memory that the steps
    /// of running builds ask for ([`BuildMemory::observe`]).
    fn observe_every(&self) -> Duration {
        match self {
            Self::Process => OBSERVE_EVERY,
            #[cfg(test)]
            Self::Fixed { .. } => Duration::ZERO,
        }
    }
}

/// What the builds hold of the budget, and what they are taken to have done
/// to the process's resident memory.
#[derive(Debug, Default)]
struct Ledger {
    /// Bytes the running builds reserve together.
    reserved: u64,
    /// The most the builds are taken to hold or to have left: what the last
    /// builds left that was still resident when `reserved` last left zero,
    /// which these builds reuse, or the most `reserved` has been since,
    /// whichever is more.
    peak: u64,
    /// What the rest of the process was taken to hold when `reserved` last
    /// left zero ([`MemoryReservation::admit`]). `None` before the first
    /// build and where the platform reports no resident memory.
    rest: Option<u64>,
    /// Resident memory the builds left behind when `reserved` last returned
    /// to zero: what the process held then above the larger of its
    /// configured memory and `rest`, and no more than `peak`.
    left: u64,
    /// The least the rest of the process is known to have held while builds
    /// held reservations: the most that was resident beyond `peak` at a
    /// moment the ledger looked. Zero while no build runs.
    floor: u64,
    /// When a step of a running build last had the ledger look.
    observed: Option<Instant>,
}

impl Ledger {
    /// Looks at `resident` while builds hold reservations. The builds that
    /// run and the ones that ran with them hold, or left, no more than they
    /// reserved at once, in memory the builds before them left where that
    /// is still resident, so what is resident beyond `peak` is the rest of
    /// the process's.
    fn look(&mut self, resident: Option<u64>) {
        let Some(resident) = resident else {
            return;
        };
        self.floor = self.floor.max(resident.saturating_sub(self.peak));
    }
}

/// What the bulk builds of one process may reserve together, and have.
///
/// The budget is [`VectorBulkBuildMemory::budget_bytes`] against what the
/// rest of the process holds. While a build holds a reservation that cannot
/// be measured, because the build's own memory is resident too and holds
/// less than it reserved for most of its run, so the rest is taken to hold
/// what it held when the first of the running builds was admitted. When the
/// last of them releases, the process still holds memory the builds freed
/// and the allocator kept; whatever is resident then above the level the
/// builds started from is theirs ([`Ledger::left`]), no more than they
/// reserved at once, and is left out of the rest of the process for as long
/// as no build runs. So the next build is measured against the process the
/// builds found, not against what they left in it, and reuses what they
/// left.
///
/// The rest of the process may grow while builds run, and resident memory
/// does not say whose a byte is. What it does say is a lower bound. The
/// builds are taken to account for at most [`Ledger::peak`]: the most they
/// reserved at once since the first of them was admitted, or what the builds
/// before them had left resident at that moment, whichever is more, since
/// they run in that memory. Whatever is resident beyond it is the rest's
/// ([`Ledger::floor`]). The ledger looks whenever a build is admitted or
/// releases and, at most every [`OBSERVE_EVERY`], when a build steps
/// ([`Self::observe`]), which is while builds write and hold most. Growth it
/// sees is the rest's from then on: it is not counted as left by the builds,
/// and it lowers the budget of the builds that follow. The look latches: the
/// most the rest was seen to hold stands, though it was held for a moment
/// only, until less than that is resident, so a spike costs the next builds
/// budget until the allocator has returned the memory. That is the safe
/// side of not knowing whether the rest shrank.
///
/// Growth it cannot see is growth that stays inside that bound: as much as
/// the bound exceeds what the builds really hold at each look. That is not
/// only the part of a reservation a build leaves unused. It is the whole of
/// the larger of the peak reservation and what earlier builds left wherever
/// the running builds hold little of it: a small build after a large one, a
/// build that has released most of its reservation. That much is taken for
/// memory the builds left, and the next budget is too large by it. The error
/// does not add up from one build to the next: the next builds' own run
/// shows the rest again, with everything missed before, as soon as more is
/// resident than their bound. It is not corrected while no build runs, and
/// cannot be
/// from resident memory: memory the allocator keeps after a build freed it
/// and memory the rest of the process uses are both resident, and only the
/// allocator tells them apart. What covers the error is the share of the
/// ceiling every automatic budget keeps free. An operator sees the budget
/// and the rest it was measured against in the log line of each
/// reservation; a process that still grows past its ceiling during a build
/// is killed, which counts the attempt (see "Retries").
///
/// Memory the builds left may go back to the OS while no build runs, and
/// resident memory that fell by it would make the rest of the process look
/// smaller by as much. So the rest is taken to hold at least what it held
/// before the last builds or was seen to hold during them, or all that is
/// resident where that is less: a rest that did shrink is seen once what the
/// builds left is gone too, and until then the budget is the one they were
/// admitted under.
struct BuildMemory {
    policy: VectorBulkBuildMemory,
    /// Bytes the configuration lets the rest of the process hold.
    configured_bytes: u64,
    /// Encoded bytes SlateDB buffers before it stalls writers, which
    /// `configured_bytes` counts once.
    write_buffer_bytes: u64,
    host: HostMemory,
    ledger: parking_lot::Mutex<Ledger>,
    parking: Arc<Parking>,
}

impl BuildMemory {
    /// The budget were the rest of the process to hold only what its
    /// configuration bounds: the most any build can ever reserve.
    fn capacity(&self) -> u64 {
        self.policy
            .budget_bytes(self.host.ceiling(), self.configured_bytes, None)
    }

    /// Memory a build's write pass makes SlateDB hold: the write buffer's
    /// rows beyond the encoded size the configuration counts them at, or two
    /// windows of rows where that is less.
    fn write_allowance(&self, write_ahead: WriteAhead) -> u64 {
        let whole_buffer = self
            .write_buffer_bytes
            .saturating_mul(WRITE_BUFFER_FOOTPRINT - 1);
        match write_ahead {
            WriteAhead::Unbounded => whole_buffer,
            WriteAhead::Windowed => {
                whole_buffer.min(2 * WRITE_BUFFER_FOOTPRINT * WRITE_WINDOW_BYTES)
            }
        }
    }

    /// Bytes the running builds reserve together.
    #[cfg(test)]
    fn reserved(&self) -> u64 {
        self.ledger.lock().reserved
    }

    /// Has the ledger look at what is resident, for a step of a running
    /// build: at most once every [`OBSERVE_EVERY`], and not while no build
    /// holds a reservation.
    fn observe(&self) {
        let mut ledger = self.ledger.lock();
        let recent = ledger
            .observed
            .is_some_and(|at| at.elapsed() < self.host.observe_every());
        if ledger.reserved == 0 || recent {
            return;
        }
        ledger.observed = Some(Instant::now());
        ledger.look(self.host.resident());
    }

    /// Releases `bytes` of the builds' reservations and wakes the builds
    /// that wait for memory.
    fn release(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        let mut ledger = self.ledger.lock();
        let resident = self.host.resident();
        ledger.look(resident);
        ledger.reserved -= bytes;
        if ledger.reserved == 0 {
            // The rest held what it was admitted against, or more where the
            // ledger saw more: that stands for the builds that follow.
            let floor = std::mem::take(&mut ledger.floor);
            ledger.rest = ledger.rest.map(|rest| rest.max(floor));
            let before = self.configured_bytes.max(ledger.rest.unwrap_or(0));
            ledger.left = resident
                .map_or(0, |resident| resident.saturating_sub(before))
                .min(ledger.peak);
        }
        drop(ledger);
        self.parking.wake_all();
    }
}

/// Answer to a build that asks for its memory.
enum Admission {
    Reserved(MemoryReservation),
    /// Running builds hold the bytes and will release them.
    Busy,
    /// Nothing the builds can free would make room.
    Refused {
        /// The budget the build did not fit.
        budget_bytes: u64,
        /// The most the budget can ever be.
        capacity_bytes: u64,
    },
}

/// Bytes one build holds of the budget every bulk build shares.
struct MemoryReservation {
    memory: Arc<BuildMemory>,
    bytes: u64,
    /// The budget the build was admitted under, and what the rest of the
    /// process was taken to hold for it (`None` where the platform reports
    /// no resident memory): what an operator reads in the log.
    budget_bytes: u64,
    rest_bytes: Option<u64>,
}

impl MemoryReservation {
    /// Reserves `bytes` of `memory`, all of them or none.
    ///
    /// A build that cannot reserve is [`Admission::Busy`] only while other
    /// builds hold a reservation and it fits the budget's capacity: they
    /// release theirs, whereas the rest of the process may never shrink.
    fn admit(memory: &Arc<BuildMemory>, bytes: u64) -> Admission {
        let capacity_bytes = memory.capacity();
        let mut ledger = memory.ledger.lock();
        let idle = ledger.reserved == 0;
        let resident = memory.host.resident();
        let rest = if idle {
            // What the builds left may have gone back to the OS since, so
            // the rest holds no less than before them while that much is
            // resident.
            let before = ledger.rest.unwrap_or(0);
            resident.map(|resident| {
                resident
                    .saturating_sub(ledger.left)
                    .max(before.min(resident))
            })
        } else {
            // Builds run: the rest holds what it did when the first of them
            // was admitted, or more where the ledger sees more.
            ledger.look(resident);
            let floor = ledger.floor;
            ledger.rest.map(|rest| rest.max(floor))
        };
        let budget_bytes =
            memory
                .policy
                .budget_bytes(memory.host.ceiling(), memory.configured_bytes, rest);
        let Some(reserved) = ledger
            .reserved
            .checked_add(bytes)
            .filter(|reserved| bytes <= capacity_bytes && *reserved <= budget_bytes)
        else {
            return if idle || bytes > capacity_bytes {
                Admission::Refused {
                    budget_bytes,
                    capacity_bytes,
                }
            } else {
                Admission::Busy
            };
        };
        if idle {
            ledger.rest = rest;
            // What is resident beyond the rest is what the last builds left:
            // a build smaller than that runs in it, and leaves it.
            ledger.peak = resident
                .zip(rest)
                .map_or(0, |(resident, rest)| resident.saturating_sub(rest));
        }
        ledger.reserved = reserved;
        ledger.peak = ledger.peak.max(reserved);
        Admission::Reserved(Self {
            memory: Arc::clone(memory),
            bytes,
            budget_bytes,
            rest_bytes: rest,
        })
    }

    /// Releases what the reservation holds beyond `bytes`.
    fn shrink_to(&mut self, bytes: u64) {
        let released = self.bytes.saturating_sub(bytes);
        self.bytes -= released;
        self.memory.release(released);
    }
}

impl Drop for MemoryReservation {
    fn drop(&mut self) {
        self.memory.release(self.bytes);
    }
}

/// Source rows one attempt's pass 1 read, and the bytes it read them in.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct ReadCounts {
    entities: u64,
    input_bytes: u64,
}

/// The scan stage an operation stores: one of the two whose steps run here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum StoredScan {
    /// The serial scan's stage, which every build is created at. Nothing a
    /// bulk attempt wrote is stored under it.
    Serial(SourceScanProgress),
    /// The bulk build's stage.
    Bulk(BulkBuildProgress),
}

impl StoredScan {
    /// The scan progress either stage holds.
    fn progress(&self) -> &SourceScanProgress {
        match self {
            Self::Serial(progress) | Self::Bulk(BulkBuildProgress { scan: progress, .. }) => {
                progress
            }
        }
    }

    /// Bulk attempts started for the operation: none under the serial stage.
    fn attempts(&self) -> Option<BulkBuildAttempts> {
        match self {
            Self::Serial(_) => None,
            Self::Bulk(progress) => Some(progress.attempts),
        }
    }

    /// Whether exactly `attempt` is stored.
    fn is_bulk(&self, attempt: &BulkBuildProgress) -> bool {
        match self {
            Self::Serial(_) => false,
            Self::Bulk(progress) => progress == attempt,
        }
    }

    /// Whether the serial stage is stored with nothing read: the stage a
    /// build that left the bulk path stores.
    fn is_unstarted(&self) -> bool {
        match self {
            Self::Serial(progress) => progress.cursor.is_none(),
            Self::Bulk(_) => false,
        }
    }

    /// The stage as it is stored.
    fn stage(&self) -> VectorBuildStage {
        match self {
            Self::Serial(progress) => VectorBuildStage::Scan(progress.clone()),
            Self::Bulk(progress) => VectorBuildStage::BulkBuild(progress.clone()),
        }
    }
}

/// The scan of `progress`'s source with nothing read: no cursor and no
/// counters.
fn scan_from_start(progress: &SourceScanProgress) -> SourceScanProgress {
    SourceScanProgress {
        inclusive_upper_bound: progress.inclusive_upper_bound.clone(),
        cursor: None,
        counters: OperationCounters::default(),
    }
}

/// A linked graph of any metric an index is built for.
pub(super) enum LinkedGraph {
    Cosine(Box<BulkGraph<distance::Cosine>>),
    Euclidean(Box<BulkGraph<distance::Euclidean>>),
    Manhattan(Box<BulkGraph<distance::Manhattan>>),
}

impl LinkedGraph {
    /// Nodes the graph was linked with.
    fn len(&self) -> usize {
        match self {
            Self::Cosine(graph) => graph.len(),
            Self::Euclidean(graph) => graph.len(),
            Self::Manhattan(graph) => graph.len(),
        }
    }
}

impl From<Box<BulkGraph<distance::Cosine>>> for LinkedGraph {
    fn from(graph: Box<BulkGraph<distance::Cosine>>) -> Self {
        Self::Cosine(graph)
    }
}

impl From<Box<BulkGraph<distance::Euclidean>>> for LinkedGraph {
    fn from(graph: Box<BulkGraph<distance::Euclidean>>) -> Self {
        Self::Euclidean(graph)
    }
}

impl From<Box<BulkGraph<distance::Manhattan>>> for LinkedGraph {
    fn from(graph: Box<BulkGraph<distance::Manhattan>>) -> Self {
        Self::Manhattan(graph)
    }
}

/// The graph of a build in pass 3.
enum GraphSlot {
    /// The graph the stored rows were planned from.
    Whole(LinkedGraph),
    /// A step holds the graph, or an unlink failed or panicked part-way and
    /// took it along: no later step can read a graph in that state.
    Taken,
}

/// An attempt whose count this process has committed and not yet seen
/// stored: it holds the attempt's turn and memory and has read nothing.
struct Starting {
    /// The stage the attempt runs under, with its count.
    attempt: BulkBuildProgress,
    parameters: BulkParameters,
    reserved: Reserved,
    turn: Turn,
    /// The members the attempt reads, which its memory was reserved for.
    source: Source,
    progress: Arc<PreparationProgress>,
}

/// Passes 1 and 2 of one build, running off the lifecycle step.
struct Preparation {
    task: tokio::task::JoinHandle<()>,
    /// The passes' outcome, sent before the task wakes the operation.
    done: tokio::sync::oneshot::Receiver<Result<Prepared>>,
    cancelled: Arc<AtomicBool>,
    /// The stage stored while the passes run: nothing of the generation is.
    attempt: BulkBuildProgress,
    progress: Arc<PreparationProgress>,
}

impl Drop for Preparation {
    fn drop(&mut self) {
        self.cancelled.store(true, Ordering::Relaxed);
        self.task.abort();
    }
}

/// Outcome of passes 1 and 2.
enum Prepared {
    /// The source cannot or need not be bulk built.
    Serial(VectorSerialBuildReason),
    /// The serial scan would block on the row of `entity_id` at `key`.
    Blocked {
        key: Bytes,
        entity_id: IndexEntityId,
    },
    /// The source holds no vector: there is no graph to link or to store.
    Empty { read: ReadCounts },
    /// Every source vector is linked.
    Linked {
        graph: LinkedGraph,
        reservation: MemoryReservation,
        write_ahead: WriteAhead,
    },
}

/// Pass 3 of one build.
struct Writing {
    graph: GraphSlot,
    /// Nodes the graph was linked with.
    nodes: u32,
    /// Link changes of unlinked nodes not yet confirmed stored, oldest first.
    journal: Vec<LinkChange>,
    /// The stage the stored rows match.
    committed: BulkBuildProgress,
    /// First node the stored rows do not cover: every node below it is stored
    /// or unlinked.
    position: u32,
    /// Source bytes the last step admitted, which sizes the next step's scan.
    step_input_bytes: u64,
    write_ahead: WriteAhead,
    /// The flush a pacing build started and has not seen end.
    flushing: Option<tokio::sync::oneshot::Receiver<std::result::Result<(), slatedb::Error>>>,
    /// Whether the last step wrote nothing because the memtables were full.
    paused: bool,
    started: Instant,
    unlink: UnlinkBounds,
    /// What the pass has unlinked, as of its last step.
    unlinks: Unlinks,
    /// The longest a step of the pass has taken.
    longest_step: Duration,
    _reservation: MemoryReservation,
}

/// What unlinking may cost a build's write pass.
#[derive(Debug, Clone, Copy)]
struct UnlinkBounds {
    /// [`UNLINK_STEP_TIME`].
    step_time: Duration,
    /// [`UNLINKS_BEFORE_JUDGING`].
    judged_after: u32,
}

/// The unlinks of a write pass.
#[derive(Debug, Clone, Copy, Default)]
struct Unlinks {
    /// Nodes the pass unlinked.
    nodes: u32,
    /// Nodes the pass had read the entities of, or found them gone, when it
    /// last unlinked: every unlinked node is one of them.
    passed: u32,
    /// Time the unlinks took.
    time: Duration,
}

/// Whether a write pass that unlinked `unlinked` of the first `passed` of
/// its graph's `nodes` gives its attempt up for a serial scan.
///
/// It does once it has made `judged_after` unlinks and the nodes it has not
/// passed would hold, at the share it unlinked so far, more unlinks than one
/// in [`GIVES_UP_PER_NODES`] of the graph's nodes: finishing in bulk would
/// then cost more than the serial scan it falls back to. What the pass has
/// spent already does not count, so a pass with few nodes left finishes
/// however many of the others it unlinked.
fn gives_up(nodes: u32, passed: u32, unlinked: u32, judged_after: u32) -> bool {
    let left = nodes.saturating_sub(passed);
    unlinked >= judged_after.max(1)
        && u128::from(unlinked) * u128::from(left) * u128::from(GIVES_UP_PER_NODES)
            > u128::from(nodes) * u128::from(passed)
}

/// Last row a committed step deleted of a partially written graph.
///
/// Every row at or before it, in the order the rows are deleted, is gone. It
/// is never stored: without it a step scans from the start.
pub(crate) enum DiscardResume {
    /// Key of an applied-state row.
    AppliedState(Bytes),
    /// A physical row; no applied-state row is left.
    Physical(VectorCleanupRow),
}

/// What of a bulk attempt failed where a serial scan may not.
#[derive(Debug, Clone, Copy)]
enum Failed {
    /// The attempt itself: a defect of the bulk path.
    Attempt,
    /// The upload of the attempt's count, which storage refused for good:
    /// the attempt never started.
    CountUpload,
}

/// Why a build blocks once it has left the bulk stage.
enum Blocking {
    /// The source row of `entity_id` at `key`, if it still blocks when the
    /// blocking step reads it: with the blocker the serial scan would give
    /// it, or else `otherwise`, the blocker of a row the serial scan reads
    /// and no step could store.
    Row {
        key: Bytes,
        entity_id: IndexEntityId,
        otherwise: Option<IndexOperationBlocker>,
    },
    /// No source row decides it: no step can store the graph's metadata
    /// row.
    Unconditional(IndexOperationBlocker),
    /// The build failed: the outbox records the failure as the blocker.
    Failure(HelixDbError),
}

/// What a build does once nothing of its generation is stored.
enum AfterDiscard {
    /// It starts another attempt, or scans serially.
    Attempt,
    /// It scans serially, for this reason: its bulk attempt failed where a
    /// serial scan may not ([`BulkBuilds::fall_back`]), or its source
    /// changed too much to finish in bulk ([`gives_up`]).
    Serial(VectorSerialBuildReason),
    /// It stores the serial stage and blocks.
    Block(Blocking),
}

/// Where one operation's scan stands in this process.
#[derive(Default)]
enum BuildState {
    /// No step of this process has looked at the scan yet.
    #[default]
    Undecided,
    /// The generation's rows are deleted until a step finds none.
    Discarding {
        physical_index_id: VectorPhysicalIndexId,
        resume: Option<DiscardResume>,
        /// Rows the committed steps of this process deleted.
        deleted: u64,
        then: AfterDiscard,
    },
    /// Nothing of the generation is stored; the build waits its turn for
    /// passes 1 and 2, and then for its memory.
    Fresh {
        physical_index_id: VectorPhysicalIndexId,
        /// The stage stored while nothing of the generation is.
        stored: StoredScan,
        /// Whether the source is known to hold the vectors a bulk build is
        /// started for, as far as its first rows tell.
        probed: bool,
        /// What the build holds, which says what its last step found it
        /// waiting for.
        held: Held,
    },
    Starting(Box<Starting>),
    Preparing(Preparation),
    Writing(Box<Writing>),
    /// The build committed the serial stage without a cursor, with nothing
    /// stored, and blocks in its next step.
    Blocking(Blocking),
    /// The serial scan runs every remaining step, and nothing a bulk attempt
    /// wrote is stored.
    Serial,
}

impl BuildState {
    /// What the status reports of a build in this state.
    fn activity(&self) -> Option<Activity> {
        match self {
            Self::Undecided | Self::Blocking(_) | Self::Serial => None,
            Self::Discarding { deleted, .. } => Some(Activity::Discarding {
                rows_deleted: *deleted,
            }),
            Self::Fresh { held, .. } => held.waiting_for().map(Activity::Waiting),
            Self::Starting(starting) => Some(Activity::Preparing(Arc::clone(&starting.progress))),
            Self::Preparing(preparation) => {
                Some(Activity::Preparing(Arc::clone(&preparation.progress)))
            }
            Self::Writing(writing) => Some(Activity::Writing {
                nodes_written: u64::from(writing.position),
                nodes: u64::from(writing.nodes),
                paused_for_write_buffer: writing.paused,
            }),
        }
    }
}

/// What a build that has nothing stored holds while it waits to start.
enum Held {
    /// Nothing, and no step has found it waiting.
    Nothing,
    /// Nothing: another build has the turn for passes 1 and 2.
    AwaitingTurn,
    /// The turn: running builds hold the memory it needs.
    Turn(Turn),
}

impl Held {
    /// What the build waits for, if a step found it waiting.
    fn waiting_for(&self) -> Option<VectorBulkBuildWait> {
        match self {
            Self::Nothing => None,
            Self::AwaitingTurn => Some(VectorBulkBuildWait::Turn),
            Self::Turn(_) => Some(VectorBulkBuildWait::Memory),
        }
    }
}

/// What a committed step of a bulk build made true of the stored rows.
pub(crate) enum BulkBuildCommit {
    /// A pass-3 step stored rows.
    Stored {
        /// Stage and graph position the next step starts from, or `None`
        /// once the whole graph is stored.
        next: Option<(BulkBuildProgress, u32)>,
        /// Leading journal entries the step staged or found nothing to stage
        /// for.
        journal_applied: usize,
    },
    /// A step deleted rows of a partially written graph.
    Discarded {
        /// Where the next deleting step resumes.
        resume: Option<DiscardResume>,
        /// Rows the step deleted.
        deleted: u64,
    },
}

/// Everything one step of either scan stage is bound to.
pub(super) struct ScanStep<'a> {
    pub(super) db: &'a Db,
    pub(super) transaction: &'a DbTransaction,
    pub(super) scope: DataScope,
    pub(super) operation: &'a IndexOperationRecord,
    pub(super) record: &'a IndexRecordV2,
    pub(super) definition: &'a ValidatedVectorIndexDefinition,
    pub(super) stored: &'a StoredScan,
    pub(super) limits: SearchIndexBatchLimits,
    pub(super) scan_tuning: IndexLifecycleScanTuning,
    pub(super) simhasher_registry: &'a Arc<vector::SimHasherRegistry>,
    pub(super) batch_reads: crate::batch_reads::BatchReads,
}

/// Who ran one step of a scan stage.
pub(super) enum ScanOutcome {
    /// The bulk build ran the step.
    Bulk(Box<VectorStepResult>),
    /// The serial scan must run it.
    Serial {
        /// Where the scan goes on from: the stored progress under the serial
        /// stage, and the start of the source for a build that leaves the
        /// bulk stage, which has nothing stored. The serial scan's first
        /// committed step is what stores the serial stage again.
        progress: SourceScanProgress,
        /// Whether that step's counters continue the stored ones.
        counters: StepCounters,
    },
}

/// What one pass over a build's state came to.
enum Stepped {
    /// The step has its outcome.
    Ran(ScanOutcome),
    /// The build waits: the step commits the stage unchanged and the
    /// operation stays parked.
    Parked,
}

/// What a build that has nothing stored does next.
enum Begun {
    /// It waits, parked, for what its [`Held`] says.
    Waiting,
    /// It moves to this state within the same step.
    Next(BuildState),
    /// The step commits this stage, whose scan is at its start, and the
    /// build is in this state if the commit lands.
    Commit(VectorBuildStage, BuildState),
}

/// How much one run of a pass-1 read takes, and how it is read again.
#[derive(Debug, Clone, Copy)]
struct ReadRun {
    /// Most label members the run takes.
    ids: usize,
    /// Row bytes after which the run ends.
    bytes: u64,
    /// How long a failed run waits before its first retry; the `n`th waits
    /// `n` times as long.
    retry_delay: Duration,
}

/// Bulk builds of one database's vector driver.
pub(super) struct BulkBuilds {
    tuning: VectorBulkBuildTuning,
    /// One slot per operation with a scan step in this process. A slot lives
    /// until its operation steps past the scan or blocks.
    builds: parking_lot::Mutex<HashMap<IndexOperationId, Arc<tokio::sync::Mutex<BuildState>>>>,
    memory: Arc<BuildMemory>,
    /// Admits one build to passes 1 and 2 at a time.
    preparing: Arc<tokio::sync::Semaphore>,
    /// [`WRITE_WINDOW_BYTES`], which tests shrink.
    write_window_bytes: u64,
    /// [`READ_RUN_IDS`], [`READ_RUN_BYTES`] and [`READ_RUN_RETRY_DELAY`],
    /// which tests shrink.
    read_run: ReadRun,
    /// [`UNLINK_STEP_TIME`] and [`UNLINKS_BEFORE_JUDGING`], which tests
    /// shrink.
    unlink: UnlinkBounds,
    reports: Arc<VectorBuildReports>,
    parking: Arc<Parking>,
    #[cfg(test)]
    faults: Arc<TestFaults>,
    /// Steps that found their build waiting. A test cannot tell one from a
    /// step that ran by looking for the parked operation afterwards: what
    /// the build waited for may have woken it by then.
    #[cfg(test)]
    parked_steps: AtomicU64,
}

impl BulkBuilds {
    /// `configured_memory_bytes` is what the configuration lets the rest of
    /// the process hold ([`crate::config::DbConfig::configured_memory_bytes`]),
    /// and `write_buffer_bytes` the part of it that is SlateDB's
    /// `max_unflushed_bytes`. `worker` wakes the lifecycle worker when a
    /// build that waited can step again.
    pub(super) fn new(
        tuning: VectorBulkBuildTuning,
        configured_memory_bytes: u64,
        write_buffer_bytes: u64,
        worker: IndexWorkerWakeHandle,
    ) -> Self {
        let parking = Arc::new(Parking::new(worker));
        Self {
            tuning,
            builds: parking_lot::Mutex::new(HashMap::new()),
            memory: Arc::new(BuildMemory {
                policy: tuning.memory(),
                configured_bytes: configured_memory_bytes,
                write_buffer_bytes,
                host: HostMemory::Process,
                ledger: parking_lot::Mutex::new(Ledger::default()),
                parking: Arc::clone(&parking),
            }),
            preparing: Arc::new(tokio::sync::Semaphore::new(1)),
            write_window_bytes: WRITE_WINDOW_BYTES,
            read_run: ReadRun {
                ids: READ_RUN_IDS,
                bytes: READ_RUN_BYTES,
                retry_delay: READ_RUN_RETRY_DELAY,
            },
            unlink: UnlinkBounds {
                step_time: UNLINK_STEP_TIME,
                judged_after: UNLINKS_BEFORE_JUDGING,
            },
            reports: Arc::new(VectorBuildReports::default()),
            parking,
            #[cfg(test)]
            faults: Arc::default(),
            #[cfg(test)]
            parked_steps: AtomicU64::new(0),
        }
    }

    pub(super) fn reports(&self) -> Arc<VectorBuildReports> {
        Arc::clone(&self.reports)
    }

    /// When the worker offers `operation_id` a step again at the latest, or
    /// `None` when its build does not wait.
    pub(super) fn parked_until(&self, operation_id: IndexOperationId) -> Option<Instant> {
        self.parking.until(operation_id)
    }

    /// Drops the graph and the reservation this process holds for
    /// `operation_id`, cancelling its passes 1 and 2. A later scan step of the
    /// operation starts from what is stored.
    pub(super) fn forget(&self, operation_id: IndexOperationId) {
        self.builds.lock().remove(&operation_id);
        self.reports.publish(operation_id, None);
        self.parking.clear(operation_id);
    }

    /// Runs one step of a bulk build, or tells the caller to run the serial
    /// scan.
    ///
    /// The outbox runs one step of an operation at a time and reports each
    /// commit through [`Self::confirm`] before the next step. A step never
    /// waits: one that finds its build waiting commits the stored stage
    /// unchanged and leaves the operation parked ([`Parking`]). An error
    /// leaves the stored rows and a graph in pass 3 as the last confirmed
    /// step left them, except that a graph an unlink left torn is gone, and
    /// the next step deletes the stored rows. A failure that would block the
    /// operation at the bulk stage is not returned from that stage: the step
    /// reports a transient failure, the build deletes what is stored and
    /// returns to the serial stage, and the failure is returned from there
    /// (see "Blocked builds" in the module documentation).
    pub(super) async fn scan_step<D: Distance>(&self, step: &ScanStep<'_>) -> Result<ScanOutcome>
    where
        LinkedGraph: From<Box<BulkGraph<D>>>,
    {
        let operation_id = step.operation.operation_id();
        // A step is when builds hold most: the ledger looks at the process.
        self.memory.observe();
        match step.stored {
            StoredScan::Serial(_) => {}
            StoredScan::Bulk(_) => self.reports.found_at_bulk_stage(operation_id),
        }
        let slot = Arc::clone(self.builds.lock().entry(operation_id).or_default());
        let mut state = slot.lock().await;
        let stepped = self.advance::<D>(step, &mut state).await;
        self.reports.publish(operation_id, state.activity());
        match stepped {
            Ok(Stepped::Parked) => {
                #[cfg(test)]
                self.parked_steps.fetch_add(1, Ordering::Relaxed);
                Ok(ScanOutcome::Bulk(Box::new(waiting(step))))
            }
            Ok(Stepped::Ran(outcome)) => {
                self.parking.clear(operation_id);
                Ok(outcome)
            }
            Err(error) => {
                self.parking.clear(operation_id);
                Err(error)
            }
        }
    }

    /// Moves `state` until the step has an outcome.
    async fn advance<D: Distance>(
        &self,
        step: &ScanStep<'_>,
        state: &mut BuildState,
    ) -> Result<Stepped>
    where
        LinkedGraph: From<Box<BulkGraph<D>>>,
    {
        let operation_id = step.operation.operation_id();
        let ran = |result: VectorStepResult| Ok(Stepped::Ran(ScanOutcome::Bulk(Box::new(result))));
        'steps: loop {
            let next = match &mut *state {
                BuildState::Serial => {
                    return Ok(Stepped::Ran(match step.stored {
                        StoredScan::Serial(progress) => ScanOutcome::Serial {
                            progress: progress.clone(),
                            counters: StepCounters::Continued,
                        },
                        StoredScan::Bulk(progress) => ScanOutcome::Serial {
                            progress: scan_from_start(&progress.scan),
                            counters: StepCounters::Restarted,
                        },
                    }));
                }
                BuildState::Undecided => self.decide(step),
                BuildState::Discarding {
                    physical_index_id,
                    resume,
                    ..
                } => match step.stored {
                    // Only the bulk stage has rows to delete.
                    StoredScan::Serial(_) => Ok(BuildState::Undecided),
                    StoredScan::Bulk(stored) => {
                        let discarded =
                            discard::<D>(step, stored, *physical_index_id, resume.as_ref()).await;
                        #[cfg(test)]
                        let discarded = self.faults.discard_fault().map_or(discarded, Err);
                        match discarded {
                            Ok(Some(result)) => return ran(result),
                            // A build whose rows cannot be deleted cannot
                            // leave the bulk stage: the failure is returned
                            // under it. The build stays as it is, with what
                            // it does once the rows are gone, so the step's
                            // retry goes on deleting and does not start
                            // another attempt instead.
                            Err(error) => return Err(error),
                            Ok(None) => {
                                let BuildState::Discarding {
                                    physical_index_id,
                                    then,
                                    ..
                                } = std::mem::take(&mut *state)
                                else {
                                    continue;
                                };
                                match then {
                                    AfterDiscard::Attempt => Ok(BuildState::Fresh {
                                        physical_index_id,
                                        stored: step.stored.clone(),
                                        probed: false,
                                        held: Held::Nothing,
                                    }),
                                    AfterDiscard::Serial(reason) => {
                                        Ok(self.serial(operation_id, reason))
                                    }
                                    // Nothing is stored: the serial stage
                                    // may be, and the next step blocks under
                                    // it.
                                    AfterDiscard::Block(blocking) => {
                                        *state = BuildState::Blocking(blocking);
                                        return ran(VectorStepResult::ordinary(progressed_build(
                                            VectorBuildStage::Scan(scan_from_start(&stored.scan)),
                                        ))
                                        .with_counters(StepCounters::Restarted));
                                    }
                                }
                            }
                        }
                    }
                },
                // Something else moved the scan this build was to start, and
                // may have stored rows.
                BuildState::Fresh { stored, .. } if *stored != *step.stored => {
                    Ok(BuildState::Undecided)
                }
                BuildState::Fresh {
                    physical_index_id,
                    probed,
                    held,
                    ..
                } => {
                    let waited_for = held.waiting_for();
                    match self
                        .begin::<D>(step, *physical_index_id, probed, held)
                        .await
                    {
                        Ok(Begun::Waiting) => {
                            let waiting_for = held.waiting_for();
                            if waiting_for != waited_for {
                                tracing::info!(
                                    operation_id = %operation_id.as_uuid(),
                                    ?waiting_for,
                                    "bulk vector build waits"
                                );
                            }
                            return Ok(Stepped::Parked);
                        }
                        Ok(Begun::Next(next)) => Ok(next),
                        Ok(Begun::Commit(stage, next)) => {
                            *state = next;
                            return ran(VectorStepResult::ordinary(progressed_build(stage))
                                .with_counters(StepCounters::Restarted));
                        }
                        Err(error) => Err(error),
                    }
                }
                // The commit that counted the attempt did not land, so no
                // attempt has started.
                BuildState::Starting(starting) if !step.stored.is_bulk(&starting.attempt) => {
                    Ok(BuildState::Undecided)
                }
                BuildState::Starting(_) => {
                    // The count must outlive this process before the attempt
                    // can end it, and a progressed step does not wait for its
                    // WAL upload. An upload the outbox retries leaves the
                    // attempt as it is, to start at the next step; any other
                    // failure is the attempt's.
                    let flushed = step.db.flush().await.map_err(HelixDbError::from);
                    #[cfg(test)]
                    let flushed = self.faults.flush_fault().map_or(flushed, Err);
                    match flushed {
                        Ok(()) => {}
                        Err(error) if !blocks(&error) => return Err(error),
                        // Storage refused the upload for good. The attempt
                        // cannot start, and the stored count names it, so
                        // the build leaves the bulk stage for a serial scan,
                        // which meets the same storage.
                        Err(error) => {
                            return self.fell_back(step, state, error, Failed::CountUpload);
                        }
                    }
                    let BuildState::Starting(starting) = std::mem::take(&mut *state) else {
                        continue 'steps;
                    };
                    let Starting {
                        attempt,
                        parameters,
                        reserved,
                        turn,
                        source,
                        progress,
                    } = *starting;
                    let cancelled = Arc::new(AtomicBool::new(false));
                    let (sender, done) = tokio::sync::oneshot::channel();
                    let wake = WakeOnDrop {
                        parking: Arc::clone(&self.parking),
                        operation_id,
                    };
                    let preparing = prepare::<D>(
                        source,
                        parameters,
                        self.tuning,
                        reserved,
                        turn,
                        Arc::clone(&cancelled),
                        Arc::clone(&progress),
                    );
                    let task = tokio::spawn(async move {
                        // Dropped last, after the outcome is sent or its
                        // sender is gone: the woken step finds either.
                        let _wake = wake;
                        let sender = sender;
                        // Nobody receives once the build was dropped.
                        let _ = sender.send(preparing.await);
                    });
                    Ok(BuildState::Preparing(Preparation {
                        task,
                        done,
                        cancelled,
                        attempt,
                        progress,
                    }))
                }
                BuildState::Preparing(preparation)
                    if !step.stored.is_bulk(&preparation.attempt) =>
                {
                    Ok(BuildState::Undecided)
                }
                BuildState::Preparing(preparation) => {
                    // Parked before the look: the passes wake the operation
                    // when they end.
                    self.parking.park(operation_id);
                    let prepared = match preparation.done.try_recv() {
                        Err(tokio::sync::oneshot::error::TryRecvError::Empty) => {
                            return Ok(Stepped::Parked);
                        }
                        Err(tokio::sync::oneshot::error::TryRecvError::Closed) => {
                            Err(HelixDbError::InvariantViolation(
                                "bulk vector build preparation stopped without an outcome"
                                    .to_string(),
                            ))
                        }
                        Ok(prepared) => prepared,
                    };
                    // The passes ended, so every outcome replaces the state.
                    match prepared {
                        Ok(Prepared::Serial(reason)) => Ok(self.serial(operation_id, reason)),
                        Ok(Prepared::Blocked { key, entity_id }) => {
                            let blocker = source_blocker(step, &key, entity_id).await;
                            #[cfg(test)]
                            let blocker = self.faults.row_read_fault().map_or(blocker, Err);
                            let pending = match blocker {
                                Ok(blocker) => Ok(blocker.is_some()),
                                // The row cannot be read for now. The block
                                // stays pending: the blocking step reads the
                                // row again, where dropping it would cost
                                // another read of the whole source.
                                Err(error) if !blocks(&error) => Ok(true),
                                Err(error) => Err(error),
                            };
                            match pending {
                                Ok(true) => self.leave(
                                    step,
                                    Blocking::Row {
                                        key,
                                        entity_id,
                                        otherwise: None,
                                    },
                                ),
                                // The row was repaired or deleted since the
                                // pass read it: the source is read again, by
                                // another attempt.
                                Ok(false) => Ok(BuildState::Undecided),
                                Err(error) => Err(error),
                            }
                        }
                        // A scan that found nothing to index is finished, with
                        // the counters a serial scan of the same rows ends at.
                        Ok(Prepared::Empty { read }) => {
                            self.reports.record(operation_id, VectorBuildPath::Bulk);
                            *state = BuildState::Undecided;
                            return ran(VectorStepResult::ordinary(progressed_build(
                                VectorBuildStage::ValidateDescriptor(PrefixScanProgress {
                                    cursor: None,
                                    counters: OperationCounters {
                                        entities: read.entities,
                                        input_bytes: read.input_bytes,
                                        ..OperationCounters::default()
                                    },
                                }),
                            )));
                        }
                        Ok(Prepared::Linked {
                            graph,
                            reservation,
                            write_ahead,
                        }) => {
                            self.reports.record(operation_id, VectorBuildPath::Bulk);
                            Ok(BuildState::Writing(Box::new(Writing {
                                nodes: graph.len() as u32,
                                graph: GraphSlot::Whole(graph),
                                journal: Vec::new(),
                                committed: preparation.attempt.clone(),
                                position: 0,
                                step_input_bytes: 0,
                                write_ahead,
                                flushing: None,
                                paused: false,
                                started: Instant::now(),
                                unlink: self.unlink,
                                unlinks: Unlinks::default(),
                                longest_step: Duration::ZERO,
                                _reservation: reservation,
                            })))
                        }
                        Err(error) => Err(error),
                    }
                }
                // The stored scan moved without this graph, which an
                // unreported commit can do: the stored rows are discarded.
                BuildState::Writing(writing) if !step.stored.is_bulk(&writing.committed) => {
                    Ok(BuildState::Undecided)
                }
                BuildState::Writing(writing) => 'writing: {
                    if writing.write_ahead == WriteAhead::Windowed {
                        // Parked before the look: the flush wakes the
                        // operation when it ends.
                        self.parking.park(operation_id);
                        match self.pace(step, writing) {
                            Ok(true) => writing.paused = false,
                            Ok(false) => {
                                writing.paused = true;
                                return Ok(Stepped::Parked);
                            }
                            // A failure the outbox retries leaves the build
                            // where it is.
                            Err(error) if !blocks(&error) => return Err(error),
                            Err(error) => break 'writing Err(error),
                        }
                    }
                    let Unlinks {
                        nodes,
                        passed,
                        time,
                    } = writing.unlinks;
                    if gives_up(writing.nodes, passed, nodes, writing.unlink.judged_after) {
                        tracing::warn!(
                            operation_id = %operation_id.as_uuid(),
                            nodes = writing.nodes,
                            nodes_passed = passed,
                            nodes_unlinked = nodes,
                            unlink_ms = time.as_millis() as u64,
                            longest_step_ms = writing.longest_step.as_millis() as u64,
                            "bulk vector build's source changed too much to finish in bulk: \
                             the attempt is discarded and the build scans serially"
                        );
                        break 'writing self
                            .give_up(step, VectorSerialBuildReason::TooManyRowsChanged);
                    }
                    let GraphSlot::Whole(graph) =
                        std::mem::replace(&mut writing.graph, GraphSlot::Taken)
                    else {
                        // An unlink stopped part-way: the stored rows are
                        // discarded.
                        break 'writing Ok(BuildState::Undecided);
                    };
                    // The graph's own metric picks the distance, so no step
                    // can read it as another's.
                    let (graph, stepped) = match graph {
                        LinkedGraph::Cosine(mut graph) => {
                            let stepped = self.write(step, &mut graph, writing).await;
                            (LinkedGraph::Cosine(graph), stepped)
                        }
                        LinkedGraph::Euclidean(mut graph) => {
                            let stepped = self.write(step, &mut graph, writing).await;
                            (LinkedGraph::Euclidean(graph), stepped)
                        }
                        LinkedGraph::Manhattan(mut graph) => {
                            let stepped = self.write(step, &mut graph, writing).await;
                            (LinkedGraph::Manhattan(graph), stepped)
                        }
                    };
                    match stepped {
                        // The graph is gone with the failure.
                        Err(StepFailure::Torn(error)) => Err(error),
                        Err(StepFailure::Intact(error)) => {
                            writing.graph = GraphSlot::Whole(graph);
                            // A failure the outbox retries leaves the build
                            // where it is.
                            if !blocks(&error) {
                                return Err(error);
                            }
                            Err(error)
                        }
                        Ok(Written::Step(result)) => {
                            writing.graph = GraphSlot::Whole(graph);
                            return ran(*result);
                        }
                        // The step staged nothing, so the same step begins
                        // to delete what the earlier ones stored.
                        Ok(Written::Blocked(blocking)) => self.leave(step, blocking),
                    }
                }
                BuildState::Blocking(_) if !step.stored.is_unstarted() => Ok(BuildState::Undecided),
                BuildState::Blocking(blocking) => {
                    // A row blocks only while it is still what it was, so it
                    // is read first: a read the outbox retries leaves the
                    // block pending for the step's retry, where dropping it
                    // would start another attempt.
                    let row = match blocking {
                        Blocking::Row { key, .. } => {
                            #[cfg(test)]
                            self.faults.row_read_fault().map_or(Ok(()), Err)?;
                            step.transaction.get(&*key).await?
                        }
                        Blocking::Unconditional(_) | Blocking::Failure(_) => None,
                    };
                    let BuildState::Blocking(blocking) = std::mem::take(&mut *state) else {
                        continue;
                    };
                    match blocking {
                        Blocking::Failure(error) => return Err(error),
                        Blocking::Unconditional(blocker) => {
                            return ran(VectorStepResult::ordinary(
                                IndexOperationStepResult::Blocked(blocker),
                            ));
                        }
                        Blocking::Row {
                            key,
                            entity_id,
                            otherwise,
                        } => {
                            let Some(value) = row else {
                                continue;
                            };
                            let Some(blocker) = row_blocker(
                                step.definition,
                                step.limits.max_input_bytes().get(),
                                entity_id,
                                &key,
                                &value,
                                &mut view::Scratch::new(),
                            )
                            .or(otherwise) else {
                                continue;
                            };
                            return ran(VectorStepResult::ordinary(
                                IndexOperationStepResult::Blocked(blocker),
                            ));
                        }
                    }
                }
            };
            match next {
                Ok(next) => *state = next,
                // A failure the outbox retries, or that ends the process's
                // hold on the database, changes nothing stored: the build
                // starts from the stored stage again.
                Err(error) if !blocks(&error) => {
                    *state = BuildState::Undecided;
                    return Err(error);
                }
                // The failure would block the operation.
                Err(error) => return self.fell_back(step, state, error, Failed::Attempt),
            }
        }
    }

    /// The outcome of a step in which the build's bulk attempt failed with
    /// `error`, which would block the operation. The step may have staged
    /// rows, so it commits nothing and reports a transient failure; the
    /// next ones delete what is stored and scan serially
    /// ([`Self::fall_back`]).
    fn fell_back(
        &self,
        step: &ScanStep<'_>,
        state: &mut BuildState,
        error: HelixDbError,
        failed: Failed,
    ) -> Result<Stepped> {
        match self.fall_back(step, error, failed) {
            Ok(falling_back) => {
                *state = falling_back;
                Ok(Stepped::Ran(ScanOutcome::Bulk(Box::new(
                    VectorStepResult::ordinary(IndexOperationStepResult::TransientFailure),
                ))))
            }
            Err(error) => {
                *state = BuildState::Undecided;
                Err(error)
            }
        }
    }

    /// One pass-3 step, with the time it took.
    ///
    /// A step that panics (a staging thread that does not start or panics,
    /// an invariant of the graph that does not hold) fails as a torn one: the
    /// graph is not used again, and the failure is the bulk attempt's, which
    /// takes the build to a serial scan. Left to unwind, the panic would end
    /// the worker's cycle, and a panic that repeats would cost every attempt
    /// the build has before the serial scan took over.
    async fn write<D: Distance>(
        &self,
        step: &ScanStep<'_>,
        graph: &mut BulkGraph<D>,
        writing: &mut Writing,
    ) -> std::result::Result<Written, StepFailure> {
        use futures::FutureExt;
        let started = Instant::now();
        let stepped = std::panic::AssertUnwindSafe(async {
            #[cfg(test)]
            self.faults.before_write();
            write_step(step, graph, writing).await
        })
        .catch_unwind()
        .await
        .unwrap_or_else(|panic| {
            let message = panic
                .downcast_ref::<&str>()
                .copied()
                .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
                .unwrap_or("a panic without a message");
            Err(StepFailure::Torn(HelixDbError::InvariantViolation(
                format!("bulk vector write step panicked: {message}"),
            )))
        });
        writing.longest_step = writing.longest_step.max(started.elapsed());
        stepped
    }

    /// The outcome of a step in which the serial scan, taking the build over
    /// from the bulk stage, failed with `error`: a failure that would block
    /// the operation is returned only once the build has left that stage
    /// ([`Self::failed`]), and this step reports a transient one.
    pub(super) async fn serial_scan_failed(
        &self,
        step: &ScanStep<'_>,
        error: HelixDbError,
    ) -> Result<VectorStepResult> {
        if !blocks(&error) {
            return Err(error);
        }
        let operation_id = step.operation.operation_id();
        let slot = Arc::clone(self.builds.lock().entry(operation_id).or_default());
        let mut state = slot.lock().await;
        *state = self.failed(step, error)?;
        Ok(VectorStepResult::ordinary(
            IndexOperationStepResult::TransientFailure,
        ))
    }

    /// Applies a committed step to what this process holds of its build.
    ///
    /// A commit of a state the process has since dropped changes nothing.
    pub(super) async fn confirm(&self, operation_id: IndexOperationId, commit: BulkBuildCommit) {
        let Some(slot) = self.builds.lock().get(&operation_id).cloned() else {
            return;
        };
        let mut state = slot.lock().await;
        match commit {
            BulkBuildCommit::Discarded {
                resume: next,
                deleted: step_deleted,
            } => {
                let BuildState::Discarding {
                    resume, deleted, ..
                } = &mut *state
                else {
                    return;
                };
                *resume = next;
                *deleted = deleted.saturating_add(step_deleted);
            }
            BulkBuildCommit::Stored {
                next,
                journal_applied,
            } => {
                let BuildState::Writing(writing) = &mut *state else {
                    return;
                };
                writing.journal.drain(..journal_applied);
                let Some((progress, position)) = next else {
                    tracing::info!(
                        operation_id = %operation_id.as_uuid(),
                        elapsed_ms = writing.started.elapsed().as_millis() as u64,
                        nodes_unlinked = writing.unlinks.nodes,
                        unlink_ms = writing.unlinks.time.as_millis() as u64,
                        longest_step_ms = writing.longest_step.as_millis() as u64,
                        "bulk vector build stored its graph"
                    );
                    drop(state);
                    self.forget(operation_id);
                    return;
                };
                writing.committed = progress;
                writing.position = position;
            }
        }
        self.reports.publish(operation_id, state.activity());
    }

    fn serial(
        &self,
        operation_id: IndexOperationId,
        reason: VectorSerialBuildReason,
    ) -> BuildState {
        self.reports
            .record(operation_id, VectorBuildPath::Serial(reason));
        tracing::info!(
            operation_id = %operation_id.as_uuid(),
            ?reason,
            "vector build scans serially"
        );
        BuildState::Serial
    }

    /// The state of a build that must block for `blocking`: under the bulk
    /// stage it first deletes what is stored and returns to the serial
    /// stage, and under the serial stage, where nothing of a bulk attempt is
    /// stored, it blocks in this step.
    fn leave(&self, step: &ScanStep<'_>, blocking: Blocking) -> Result<BuildState> {
        match step.stored {
            StoredScan::Serial(_) => Ok(BuildState::Blocking(blocking)),
            StoredScan::Bulk(_) => {
                let IndexStateVectorPhysical { layout } =
                    IndexStateVectorPhysical::from_record(step.record)?;
                let VectorPhysicalLayout::Unpartitioned { physical_index_id } = layout else {
                    return Err(corruption(
                        "bulk vector build targets a partitioned generation",
                    ));
                };
                Ok(BuildState::Discarding {
                    physical_index_id,
                    resume: None,
                    deleted: 0,
                    then: AfterDiscard::Block(blocking),
                })
            }
        }
    }

    /// The state of a build that gives its bulk attempt up for a serial scan
    /// with `reason`, though nothing failed: it deletes what the attempt
    /// stored and scans serially at once, in this process, without another
    /// attempt.
    fn give_up(&self, step: &ScanStep<'_>, reason: VectorSerialBuildReason) -> Result<BuildState> {
        let IndexStateVectorPhysical { layout } =
            IndexStateVectorPhysical::from_record(step.record)?;
        let VectorPhysicalLayout::Unpartitioned { physical_index_id } = layout else {
            return Err(corruption(
                "bulk vector build targets a partitioned generation",
            ));
        };
        Ok(BuildState::Discarding {
            physical_index_id,
            resume: None,
            deleted: 0,
            then: AfterDiscard::Serial(reason),
        })
    }

    /// The state of a build whose bulk attempt failed with `error`, which
    /// would block the operation: nothing a serial scan does went wrong, so
    /// the build deletes what the attempt stored and scans serially at once,
    /// in this process, without another attempt. The serial scan finishes
    /// the build, or fails for itself and blocks under its own stage.
    ///
    /// A failure of the bulk path is a defect, logged as one. Trying again
    /// would likely fail the same way: a construction thread that panics on
    /// this source does so every time, and an operator's retry of a blocked
    /// build would start at attempt one and repeat it, so the index could
    /// not be built until bulk builds were turned off. An upload of the
    /// attempt's count that storage refuses is no defect of this path and is
    /// logged for what it is ([`Failed`]).
    ///
    /// Returns the failure itself where the build cannot leave the bulk
    /// stage: its record names no unpartitioned generation to delete from.
    fn fall_back(
        &self,
        step: &ScanStep<'_>,
        error: HelixDbError,
        failed: Failed,
    ) -> Result<BuildState> {
        let operation_id = step.operation.operation_id();
        let layout = match (
            step.stored,
            IndexStateVectorPhysical::from_record(step.record),
        ) {
            // Nothing of a bulk attempt is stored under the serial stage.
            (StoredScan::Serial(_), _) => None,
            (StoredScan::Bulk(_), Ok(IndexStateVectorPhysical { layout })) => Some(layout),
            (StoredScan::Bulk(_), Err(_)) => return Err(error),
        };
        let falling_back = match layout {
            None => self.serial(operation_id, VectorSerialBuildReason::BulkAttemptFailed),
            Some(VectorPhysicalLayout::Unpartitioned { physical_index_id }) => {
                BuildState::Discarding {
                    physical_index_id,
                    resume: None,
                    deleted: 0,
                    then: AfterDiscard::Serial(VectorSerialBuildReason::BulkAttemptFailed),
                }
            }
            Some(VectorPhysicalLayout::Partitioned) => return Err(error),
        };
        match failed {
            Failed::Attempt => tracing::error!(
                operation_id = %operation_id.as_uuid(),
                %error,
                "bulk vector build failed where a serial build may not: this is a defect. \
                 The attempt is discarded and the build scans serially"
            ),
            Failed::CountUpload => tracing::warn!(
                operation_id = %operation_id.as_uuid(),
                %error,
                "storage refused the upload of a bulk vector build's attempt count: \
                 the attempt does not start and the build scans serially"
            ),
        }
        Ok(falling_back)
    }

    /// The state of a build whose serial scan, taking it over from the bulk
    /// stage, failed with `error`, which would block the operation: it
    /// leaves the bulk stage first, and the failure is returned from the
    /// serial stage. Returns the failure itself where the build cannot
    /// leave: the serial stage is stored already, so the failure blocks
    /// under it, or the stored rows cannot be deleted.
    fn failed(&self, step: &ScanStep<'_>, error: HelixDbError) -> Result<BuildState> {
        let operation_id = step.operation.operation_id();
        let (StoredScan::Bulk(_), Ok(IndexStateVectorPhysical { layout })) = (
            step.stored,
            IndexStateVectorPhysical::from_record(step.record),
        ) else {
            return Err(error);
        };
        let VectorPhysicalLayout::Unpartitioned { physical_index_id } = layout else {
            return Err(error);
        };
        tracing::warn!(
            operation_id = %operation_id.as_uuid(),
            %error,
            "vector build's serial scan failed: it leaves the bulk stage before the failure blocks it"
        );
        Ok(BuildState::Discarding {
            physical_index_id,
            resume: None,
            deleted: 0,
            then: AfterDiscard::Block(Blocking::Failure(error)),
        })
    }

    /// Whether a build that paces its writes may write another step: the
    /// memtables, whoever filled them, hold less than two windows.
    ///
    /// Collects a flush that ended and starts one once the memtables hold a
    /// window. `false` leaves a flush in flight, which wakes the operation.
    fn pace(&self, step: &ScanStep<'_>, writing: &mut Writing) -> Result<bool> {
        let flushed = writing.flushing.as_mut().map(|flush| flush.try_recv());
        match flushed {
            None | Some(Err(tokio::sync::oneshot::error::TryRecvError::Empty)) => {}
            Some(Ok(flushed)) => {
                writing.flushing = None;
                flushed?;
            }
            Some(Err(tokio::sync::oneshot::error::TryRecvError::Closed)) => {
                writing.flushing = None;
                return Err(HelixDbError::InvariantViolation(
                    "bulk vector build flush stopped without an outcome".to_string(),
                ));
            }
        }
        let unflushed = step.db.unflushed_bytes()?;
        let buffered = (unflushed.active_memtable_bytes as u64)
            .saturating_add(unflushed.immutable_memtable_bytes as u64);
        if buffered < self.write_window_bytes {
            return Ok(true);
        }
        if writing.flushing.is_none() {
            let (sender, flushed) = tokio::sync::oneshot::channel();
            let wake = WakeOnDrop {
                parking: Arc::clone(&self.parking),
                operation_id: step.operation.operation_id(),
            };
            let db = step.db.clone();
            tokio::spawn(async move {
                let _wake = wake;
                let sender = sender;
                let _ = sender.send(
                    db.flush_with_options(slatedb::config::FlushOptions {
                        flush_type: slatedb::config::FlushType::MemTable,
                    })
                    .await,
                );
            });
            writing.flushing = Some(flushed);
        }
        Ok(buffered < 2 * self.write_window_bytes)
    }

    /// Chooses what a scan this process has not run a step of does first,
    /// from the stage alone.
    ///
    /// The serial stage with a cursor is a serial scan to continue, and
    /// without one a build nothing is stored for. Whatever the bulk stage
    /// left stored is no use to this process, which holds no graph: it is
    /// deleted before anything else.
    fn decide(&self, step: &ScanStep<'_>) -> Result<BuildState> {
        let operation_id = step.operation.operation_id();
        let IndexStateVectorPhysical { layout } =
            IndexStateVectorPhysical::from_record(step.record)?;
        match (layout, step.stored) {
            (VectorPhysicalLayout::Partitioned, StoredScan::Serial(_)) => {
                Ok(self.serial(operation_id, VectorSerialBuildReason::TenantPartitioned))
            }
            (VectorPhysicalLayout::Partitioned, StoredScan::Bulk(_)) => Err(corruption(
                "bulk vector build targets a partitioned generation",
            )),
            (VectorPhysicalLayout::Unpartitioned { .. }, StoredScan::Serial(progress))
                if progress.cursor.is_some() =>
            {
                Ok(self.serial(operation_id, VectorSerialBuildReason::ResumedSerialScan))
            }
            (VectorPhysicalLayout::Unpartitioned { physical_index_id }, StoredScan::Serial(_)) => {
                Ok(BuildState::Fresh {
                    physical_index_id,
                    stored: step.stored.clone(),
                    probed: false,
                    held: Held::Nothing,
                })
            }
            (VectorPhysicalLayout::Unpartitioned { physical_index_id }, StoredScan::Bulk(_)) => {
                Ok(BuildState::Discarding {
                    physical_index_id,
                    resume: None,
                    deleted: 0,
                    then: AfterDiscard::Attempt,
                })
            }
        }
    }

    /// Starts an attempt at a generation that has nothing stored, or takes
    /// the build off the bulk path.
    ///
    /// An attempt starts by committing the bulk stage with its count
    /// ([`Begun::Commit`]), once the build holds its turn, which it keeps in
    /// `held` across the steps that find it waiting, and its memory.
    /// `probed` keeps across those steps that the source's first rows were
    /// read.
    async fn begin<D: Distance>(
        &self,
        step: &ScanStep<'_>,
        physical_index_id: VectorPhysicalIndexId,
        probed: &mut bool,
        held: &mut Held,
    ) -> Result<Begun>
    where
        LinkedGraph: From<Box<BulkGraph<D>>>,
    {
        let operation_id = step.operation.operation_id();
        if self.tuning.memory() == VectorBulkBuildMemory::Off {
            return Ok(match step.stored {
                StoredScan::Serial(_) => Begun::Next(
                    self.serial(operation_id, VectorSerialBuildReason::BulkBuildsDisabled),
                ),
                // The way back for an operator: the serial stage is stored
                // whatever the source holds, and the next step scans under
                // it. No attempt can start in between, so the build cannot
                // come back here.
                StoredScan::Bulk(stored) => {
                    tracing::info!(
                        operation_id = %operation_id.as_uuid(),
                        attempts = stored.attempts.get(),
                        "bulk vector builds are off: the build returns to its serial stage"
                    );
                    Begun::Commit(
                        VectorBuildStage::Scan(scan_from_start(&stored.scan)),
                        BuildState::Undecided,
                    )
                }
            });
        }
        // The count this attempt would be stored with.
        let attempts = match step.stored.attempts() {
            None => BulkBuildAttempts::FIRST,
            Some(started) => {
                let Some(next) = started
                    .next()
                    .filter(|_| started.get() < BulkBuildAttempts::LIMIT)
                else {
                    tracing::warn!(
                        operation_id = %operation_id.as_uuid(),
                        attempts = started.get(),
                        "no bulk attempt of the vector build stored its graph"
                    );
                    return Ok(Begun::Next(self.serial(
                        operation_id,
                        VectorSerialBuildReason::EarlierAttemptsFailed,
                    )));
                };
                next
            }
        };
        let handle = step.generation::<D>(physical_index_id)?;
        let config = VectorIndexConfig::from_v2_definition(
            step.definition,
            handle.generation().physical_name(),
        );
        let parameters = BulkParameters::try_from_config(&config)?;
        // The label's members now, in the snapshot the attempt would read:
        // exactly the rows its memory is reserved for.
        let source = step.source(self.read_run).await?;
        #[cfg(test)]
        let source = Source {
            faults: Arc::clone(&self.faults),
            ..source
        };
        let rows = source.members.len();
        // A source too small for a bulk build is found out before the build
        // waits for a turn or for memory, so it never queues behind another.
        let min_vectors = self.tuning.min_vectors().max(1);
        if rows < min_vectors || (!*probed && source.holds_fewer_vectors_than(min_vectors).await?) {
            return Ok(Begun::Next(
                self.serial(operation_id, VectorSerialBuildReason::TooFewVectors),
            ));
        }
        *probed = true;
        // `BulkGraph::construct` takes fewer than `u32::MAX` vectors.
        if rows >= u64::from(u32::MAX) {
            return Ok(Begun::Next(
                self.serial(operation_id, VectorSerialBuildReason::OverMemoryBudget),
            ));
        }
        // Parked before each look, so the release of what the build waits
        // for wakes it; the step clears the entry when it does not wait.
        self.parking.park(operation_id);
        let turn = match std::mem::replace(held, Held::AwaitingTurn) {
            Held::Turn(turn) => turn,
            Held::Nothing | Held::AwaitingTurn => {
                let Ok(permit) = Arc::clone(&self.preparing).try_acquire_owned() else {
                    return Ok(Begun::Waiting);
                };
                Turn {
                    _permit: permit,
                    _woken: WakeAllOnDrop(Arc::clone(&self.parking)),
                }
            }
        };
        let bytes_per_vector = parameters
            .bytes_per_vector::<D>(step.definition.dimension() as usize)
            .saturating_add(GROWTH_SLACK_BYTES);
        let reserve = |write_ahead| {
            MemoryReservation::admit(
                &self.memory,
                build_bytes(
                    bytes_per_vector,
                    rows,
                    self.memory.write_allowance(write_ahead),
                ),
            )
        };
        // The whole write buffer where it fits now, else two windows of it.
        let (reservation, write_ahead) = match reserve(WriteAhead::Unbounded) {
            Admission::Reserved(reservation) => (reservation, WriteAhead::Unbounded),
            Admission::Busy | Admission::Refused { .. } => match reserve(WriteAhead::Windowed) {
                Admission::Reserved(reservation) => (reservation, WriteAhead::Windowed),
                Admission::Busy => {
                    *held = Held::Turn(turn);
                    return Ok(Begun::Waiting);
                }
                Admission::Refused {
                    budget_bytes,
                    capacity_bytes,
                } => {
                    // What an operator needs to size the machine or the
                    // budget: the status says only that the build is serial.
                    tracing::info!(
                        operation_id = %operation_id.as_uuid(),
                        label_members = rows,
                        needed_bytes = build_bytes(
                            bytes_per_vector,
                            rows,
                            self.memory.write_allowance(WriteAhead::Windowed),
                        ),
                        budget_bytes,
                        capacity_bytes,
                        "bulk vector build does not fit its memory budget"
                    );
                    return Ok(Begun::Next(
                        self.serial(operation_id, VectorSerialBuildReason::OverMemoryBudget),
                    ));
                }
            },
        };
        tracing::info!(
            operation_id = %operation_id.as_uuid(),
            attempt = attempts.get(),
            attempt_limit = BulkBuildAttempts::LIMIT,
            label_members = rows,
            reserved_bytes = reservation.bytes,
            budget_bytes = reservation.budget_bytes,
            rest_bytes = ?reservation.rest_bytes,
            ?write_ahead,
            "bulk vector build reserved its memory"
        );
        let attempt = BulkBuildProgress {
            scan: scan_from_start(step.stored.progress()),
            attempts,
        };
        Ok(Begun::Commit(
            VectorBuildStage::BulkBuild(attempt.clone()),
            BuildState::Starting(Box::new(Starting {
                attempt,
                parameters,
                reserved: Reserved {
                    reservation,
                    bytes_per_vector,
                    write_bytes: self.memory.write_allowance(write_ahead),
                    write_ahead,
                },
                turn,
                progress: Arc::new(PreparationProgress {
                    members: rows,
                    ..PreparationProgress::default()
                }),
                source,
            })),
        ))
    }
}

/// Whether the outbox would block the operation for `error`, rather than
/// retry the step or stop the worker.
fn blocks(error: &HelixDbError) -> bool {
    match outbox::DriverFailure::of(error) {
        outbox::DriverFailure::Blocks => true,
        outbox::DriverFailure::Stops | outbox::DriverFailure::Retried(_) => false,
    }
}

/// Bytes a build of `vectors` vectors reserves when its write pass makes
/// SlateDB hold `write_bytes`.
fn build_bytes(bytes_per_vector: u64, vectors: u64, write_bytes: u64) -> u64 {
    vectors
        .saturating_mul(bytes_per_vector)
        .saturating_add(FIXED_ALLOWANCE_BYTES)
        .saturating_add(write_bytes)
}

/// The memory a build was admitted with.
struct Reserved {
    reservation: MemoryReservation,
    bytes_per_vector: u64,
    write_ahead: WriteAhead,
    /// [`BuildMemory::write_allowance`] of `write_ahead`.
    write_bytes: u64,
}

impl ScanStep<'_> {
    /// The build's source as it is now: the label's members up to the
    /// build's bound, in a snapshot taken after the build's creation
    /// committed, so every later write to a row of theirs has a queued
    /// operation. The snapshot is dropped with this call: the rows are read
    /// later, each run under a snapshot of its own ([`SourceRows`]).
    async fn source(&self, read_run: ReadRun) -> Result<Source> {
        let kind = self.definition.element_kind();
        let upper = label_source::upper_id(self.scope, kind, self.stored.progress())?;
        let snapshot = self.db.begin(IsolationLevel::Snapshot).await?;
        let layout = label_source::label_layout(&snapshot).await?;
        let mut members = label_source::label_members(
            &snapshot,
            layout,
            kind,
            self.definition.label().as_str(),
            self.scope,
        )
        .await?;
        drop(snapshot);
        members.remove_range((std::ops::Bound::Excluded(upper), std::ops::Bound::Unbounded));
        Ok(Source {
            db: self.db.clone(),
            scope: self.scope,
            kind,
            operation_id: self.operation.operation_id(),
            index_id: self.operation.index_id(),
            generation: self.operation.generation(),
            definition: self.definition.clone(),
            members,
            max_input_bytes: self.limits.max_input_bytes().get(),
            read_run,
            #[cfg(test)]
            faults: Arc::default(),
        })
    }

    /// Handle of the unpartitioned generation this step builds.
    fn generation<D: Distance>(
        &self,
        physical_index_id: VectorPhysicalIndexId,
    ) -> Result<ValidatedVectorBuildGenerationHandle> {
        ValidatedVectorBuildGenerationHandle::try_from_building::<D>(
            self.scope,
            self.record,
            self.operation.operation_id(),
            physical_index_id,
        )
        .map_err(|error| corruption(error.to_string()))
    }
}

/// A step that commits the stage as it is stored.
fn waiting(step: &ScanStep<'_>) -> VectorStepResult {
    VectorStepResult::ordinary(progressed_build(step.stored.stage()))
}

/// Why the serial scan would block on the source row of `entity_id`, if it
/// would: the row is larger than a step may read, or does not decode to what
/// the index's definition requires. `None` for any other row.
fn row_blocker(
    definition: &ValidatedVectorIndexDefinition,
    max_input_bytes: u64,
    entity_id: IndexEntityId,
    key: &[u8],
    value: &[u8],
    scratch: &mut view::Scratch,
) -> Option<IndexOperationBlocker> {
    let entity_kind = definition.element_kind();
    let observed = key.len().saturating_add(value.len()) as u64;
    if observed > max_input_bytes {
        return Some(IndexOperationBlocker::OversizedEntity {
            entity_kind,
            entity_id,
            observed,
            limit: max_input_bytes,
        });
    }
    stored_vector_document(definition, value, scratch)
        .is_err()
        .then_some(IndexOperationBlocker::InvalidSourceData {
            entity_kind,
            entity_id,
        })
}

/// The blocker of the source row pass 1 stopped at, read through the step's
/// transaction as the serial scan reads a row it blocks on, or `None` when
/// the row is gone or readable by now.
async fn source_blocker(
    step: &ScanStep<'_>,
    key: &Bytes,
    entity_id: IndexEntityId,
) -> Result<Option<IndexOperationBlocker>> {
    let Some(value) = step.transaction.get(key).await? else {
        return Ok(None);
    };
    Ok(row_blocker(
        step.definition,
        step.limits.max_input_bytes().get(),
        entity_id,
        key,
        &value,
        &mut view::Scratch::new(),
    ))
}

/// Deletes one bounded batch of what the generation has stored, or returns
/// `None` when this step read the generation's applied-state rows and its
/// physical namespace from their start and found neither.
///
/// Every other step keeps the bulk stage and its attempt count and stores the
/// scan as restarted: the rows its cursor and counters stood for are going.
/// `resume` is the position the last committed deleting step of this process
/// left; a step given none scans from the start.
async fn discard<D: Distance>(
    step: &ScanStep<'_>,
    stored: &BulkBuildProgress,
    physical_index_id: VectorPhysicalIndexId,
    resume: Option<&DiscardResume>,
) -> Result<Option<VectorStepResult>> {
    let blocked = |blocker| VectorStepResult::ordinary(IndexOperationStepResult::Blocked(blocker));
    let deleted = |next, counters: OperationCounters| {
        if resume.is_none() {
            tracing::info!(
                operation_id = %step.operation.operation_id().as_uuid(),
                attempts = stored.attempts.get(),
                "deleting a partially written bulk vector graph"
            );
        }
        VectorStepResult {
            committed: Some(CommittedStepState::VectorBulkBuild(
                BulkBuildCommit::Discarded {
                    resume: next,
                    deleted: counters.output_operations,
                },
            )),
            // The rows the stored counters stood for are going.
            counters: StepCounters::Restarted,
            ..VectorStepResult::ordinary(progressed_build(VectorBuildStage::BulkBuild(
                BulkBuildProgress {
                    scan: scan_from_start(&stored.scan),
                    attempts: stored.attempts,
                },
            )))
        }
    };
    let nothing = OperationCounters::default();
    let (applied_resume, physical_resume) = match resume {
        None => (None, None),
        Some(DiscardResume::AppliedState(key)) => (Some(key), None),
        Some(DiscardResume::Physical(row)) => (None, Some(row)),
    };
    // A physical position follows a scan of the applied state from its start
    // that found nothing, and no later step writes applied state.
    if physical_resume.is_none() {
        match delete_delta_and_applied_rows(
            step.transaction,
            step.scope,
            step.operation,
            &[RecordKind::AppliedState],
            applied_resume,
            nothing,
            step.limits,
        )
        .await?
        {
            CleanupWorkOutcome::Blocked(blocker) => return Ok(Some(blocked(blocker))),
            CleanupWorkOutcome::Progress {
                counters,
                exhausted: true,
                resume: _,
            } if counters == nothing => {}
            CleanupWorkOutcome::Progress {
                counters, resume, ..
            } => {
                return Ok(Some(deleted(
                    resume.map(DiscardResume::AppliedState),
                    counters,
                )));
            }
        }
    }
    let handle = step.generation::<D>(physical_index_id)?;
    match delete_physical_namespace::<D>(
        step.transaction,
        handle.generation(),
        None,
        physical_resume,
        step.definition.element_kind(),
        nothing,
        step.limits,
    )
    .await?
    {
        PhysicalCleanupOutcome::Blocked(blocker) => Ok(Some(blocked(blocker))),
        PhysicalCleanupOutcome::Progress {
            counters,
            namespace_empty: true,
            ..
        } if counters == nothing => Ok(None),
        PhysicalCleanupOutcome::Progress {
            counters, resume, ..
        } => Ok(Some(deleted(resume.map(DiscardResume::Physical), counters))),
    }
}

/// What passes 1 and 2 read, owned so they outlive the step that starts them.
///
/// It holds no snapshot and no transaction: while one is open SlateDB keeps
/// the write set of every later commit, memory no build reserves, and counts
/// those commits' merge operands against a quota past which writes fail. A
/// read of the whole source can take minutes, so each run of it opens a
/// snapshot of its own and drops it ([`Source::read`]).
struct Source {
    db: Db,
    scope: DataScope,
    kind: IndexElementKind,
    operation_id: IndexOperationId,
    index_id: IndexId,
    generation: IndexGenerationId,
    definition: ValidatedVectorIndexDefinition,
    /// The label's members up to the build's source bound, as of the
    /// snapshot they were taken in ([`ScanStep::source`]).
    members: roaring::RoaringTreemap,
    max_input_bytes: u64,
    read_run: ReadRun,
    #[cfg(test)]
    faults: Arc<TestFaults>,
}

/// Failures and pauses a test puts into the passes of a driver's builds.
#[cfg(test)]
#[derive(Default)]
struct TestFaults {
    /// Pass-1 runs that fail, before they read, with an error the outbox
    /// retries.
    read: std::sync::atomic::AtomicUsize,
    /// Pass-1 runs that fail, before they read, with an error that would
    /// block the operation, before any of `read`.
    read_blocked: std::sync::atomic::AtomicUsize,
    /// Permits pass-1 runs take before they read, one each: a test that
    /// sets it lets runs through one at a time.
    read_gate: parking_lot::Mutex<Option<Arc<tokio::sync::Semaphore>>>,
    /// Uploads of an attempt's count that fail with an error that would
    /// block the operation.
    flush: std::sync::atomic::AtomicUsize,
    /// Uploads of an attempt's count that fail with an error the outbox
    /// retries, before any of `flush`.
    flush_retried: std::sync::atomic::AtomicUsize,
    /// Steps that delete a partly written graph and fail with an error the
    /// outbox retries.
    discard: std::sync::atomic::AtomicUsize,
    /// Reads of a row that blocks a build that fail with an error the
    /// outbox retries.
    row_read: std::sync::atomic::AtomicUsize,
    /// Pass-3 steps that panic before they read.
    write_panics: std::sync::atomic::AtomicUsize,
    /// Runs read, by the probe and by pass 1.
    runs: std::sync::atomic::AtomicUsize,
}

#[cfg(test)]
impl TestFaults {
    /// Takes one of `faults`, if any is left.
    fn take(faults: &std::sync::atomic::AtomicUsize) -> bool {
        faults
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |faults| {
                faults.checked_sub(1)
            })
            .is_ok()
    }

    /// Waits for the run's permit, then fails the run if a fault is set.
    async fn before_read(&self) -> Result<()> {
        self.runs.fetch_add(1, Ordering::Relaxed);
        let gate = self.read_gate.lock().clone();
        match gate {
            None => {}
            Some(gate) => gate
                .acquire()
                .await
                .expect("the read gate stays open")
                .forget(),
        }
        Self::take(&self.read_blocked)
            .then(|| HelixDbError::InvariantViolation("injected pass-1 read fault".to_string()))
            .or_else(|| {
                Self::take(&self.read).then(|| {
                    HelixDbError::TransactionConflict("injected pass-1 read fault".to_string())
                })
            })
            .map_or(Ok(()), Err)
    }

    /// The failure of the next step that deletes a partly written graph, if
    /// one is set.
    fn discard_fault(&self) -> Option<HelixDbError> {
        Self::take(&self.discard)
            .then(|| HelixDbError::TransactionConflict("injected discard fault".to_string()))
    }

    /// The failure of the next read of a blocking row, if one is set.
    fn row_read_fault(&self) -> Option<HelixDbError> {
        Self::take(&self.row_read)
            .then(|| HelixDbError::TransactionConflict("injected row read fault".to_string()))
    }

    /// Panics if a pass-3 panic is set.
    fn before_write(&self) {
        assert!(!Self::take(&self.write_panics), "injected pass-3 panic");
    }

    /// The failure of the next upload of an attempt's count, if one is set.
    fn flush_fault(&self) -> Option<HelixDbError> {
        Self::take(&self.flush_retried)
            .then(|| HelixDbError::TransactionConflict("injected flush fault".to_string()))
            .or_else(|| {
                Self::take(&self.flush)
                    .then(|| HelixDbError::InvariantViolation("injected flush fault".to_string()))
            })
    }
}

/// What one run of a pass-1 read came to.
struct Run {
    /// The present rows of the ids the run read, in ascending id order.
    rows: Vec<label_source::SourceRow>,
    /// The last id the run read or passed when it ended with ids left to
    /// read; `None` when it read every id it was given and none is left.
    passed: Option<u64>,
}

impl Source {
    /// Whether the source certainly holds fewer than `min_vectors` vectors:
    /// its rows ended before that many were found.
    ///
    /// Reads the rows of at most [`PROBE_ROWS_PER_VECTOR`] members for each
    /// of the vectors, so a small source is read whole and a large one is
    /// not, and no more than [`ReadRun::bytes`] of them in all, what one run
    /// of pass 1 reads: the probe runs on a lifecycle step, which holds the
    /// operation's claim and its lane, so it reads that much and one read
    /// more whatever the threshold is set to and however large the rows
    /// are. `false` also when those rows gave no answer, or one of them is a
    /// row the scan would block on, which the read pass reports.
    async fn holds_fewer_vectors_than(&self, min_vectors: u64) -> Result<bool> {
        let probed = min_vectors.saturating_mul(PROBE_ROWS_PER_VECTOR);
        let whole = self.members.len() <= probed;
        // A run is read whole before its first row is looked at, so the
        // probe's runs take no more ids than the answer needs vectors. A run
        // that fails is not read again here, with a wait, but by the step
        // the outbox offers after it.
        let mut rows = SourceRows::new(
            self,
            self.members
                .iter()
                .take(usize::try_from(probed).unwrap_or(usize::MAX))
                .collect(),
            usize::try_from(min_vectors).unwrap_or(usize::MAX),
            self.read_run.bytes,
            0,
        );
        let mut scratch = view::Scratch::new();
        let mut vectors = 0_u64;
        // Imperative: the read ends at the first answer.
        loop {
            let Some(row) = rows.next().await? else {
                // The rows ended, or the bytes the probe may read did.
                return Ok(whole && rows.left.is_empty());
            };
            if row.key.len().saturating_add(row.value.len()) as u64 > self.max_input_bytes {
                return Ok(false);
            }
            let Ok(document) = stored_vector_document(&self.definition, &row.value, &mut scratch)
            else {
                return Ok(false);
            };
            vectors += u64::from(document.is_some());
            if vectors >= min_vectors {
                return Ok(false);
            }
        }
    }

    /// Scan tuning of the next pass-1 run, bounded by the SSTs the manifest
    /// holds now.
    fn read_tuning(&self) -> IndexLifecycleScanTuning {
        let manifest = self.db.manifest();
        read_tuning(
            manifest.l0().len()
                + manifest.compacted().len()
                + manifest
                    .segments()
                    .iter()
                    .map(|segment| segment.l0().len() + segment.compacted().len())
                    .sum::<usize>(),
        )
    }

    /// Reads the rows of `taken`, ascending ids of the source's members,
    /// under a snapshot opened here and dropped before this returns, as a
    /// step of the serial scan reads its own ([`LabelSource::over`]): by one
    /// scan where the ids are dense in their key range, and by point reads
    /// otherwise, until `bytes` are read. `more` says whether the source
    /// holds ids after them.
    ///
    /// Runs are therefore read at different moments. A row may be newer than
    /// the member set: its node is stored only if pass 3 still finds the
    /// vector this read (see "Source changes" in the module documentation).
    async fn read(&self, taken: Vec<u64>, more: bool, bytes: u64) -> Result<Run> {
        #[cfg(test)]
        self.faults.before_read().await?;
        let snapshot = self.db.begin(IsolationLevel::Snapshot).await?;
        let mut run = LabelSource::over(
            &snapshot,
            self.scope,
            self.kind,
            taken,
            more,
            label_source::ReadBounds {
                input_bytes: bytes,
                point_chunk_ids: std::num::NonZeroUsize::new(READ_POINT_IDS)
                    .expect("a point read fetches a row"),
            },
            self.read_tuning(),
        )
        .await?;
        let mut rows = Vec::new();
        // Imperative: the run ends where its source says.
        loop {
            match run.next().await? {
                LabelRead::Row(row) => rows.push(row),
                LabelRead::Exhausted => return Ok(Run { rows, passed: None }),
                LabelRead::Paused(cursor) => {
                    return Ok(Run {
                        rows,
                        passed: Some(label_source::cursor_id(
                            self.scope,
                            self.kind,
                            cursor.as_bytes(),
                        )?),
                    });
                }
            }
        }
    }
}

/// Scan tuning of a pass-1 scan that merges at most `ssts` SSTs: level-0
/// tables and sorted runs, of which a scan reads one table at a time.
///
/// SlateDB keeps up to the tuned number of fetches of the tuned size in
/// flight for each SST a scan merges, so a fixed tuning holds memory in
/// proportion to the shape of the LSM tree. This one divides
/// [`READ_AHEAD_BYTES`] among the SSTs: each gets up to
/// [`SOURCE_FETCH_TASKS`] fetches of up to [`SOURCE_READ_AHEAD_BYTES`], fewer
/// and smaller as there are more, and never less than one fetch of
/// [`SOURCE_MIN_READ_AHEAD_BYTES`]. The fetches of one scan therefore hold at
/// most [`READ_AHEAD_BYTES`] together, or that least fetch for each SST where
/// that is more (past a thousand SSTs).
fn read_tuning(ssts: usize) -> IndexLifecycleScanTuning {
    let share = READ_AHEAD_BYTES / ssts.max(1);
    let read_ahead =
        (share / SOURCE_FETCH_TASKS).clamp(SOURCE_MIN_READ_AHEAD_BYTES, SOURCE_READ_AHEAD_BYTES);
    let tasks = (share / read_ahead).clamp(1, SOURCE_FETCH_TASKS);
    IndexLifecycleScanTuning::try_new(read_ahead, tasks).expect("neither bound is zero")
}

/// The present rows of some of a source's members, in ascending id order,
/// read in runs.
///
/// Each run takes up to `run_ids` of the ids left, at most
/// [`ReadRun::ids`], and reads their rows whole under a snapshot of its own
/// ([`Source::read`]); it ends once it has read [`ReadRun::bytes`], and the
/// next one starts behind the last id it read or passed. So no snapshot
/// outlives a run, and the rows of at most one run are held beside the
/// vectors already taken from the runs before it.
///
/// A run that fails with an error the outbox would retry (storage that is
/// unavailable for now) is read again in place, `retries` times at most,
/// [`ReadRun::retry_delay`] longer apart each time: the runs before it are
/// kept, and the failure reaches the caller only after the last retry. Pass
/// 1, a background task that would lose every run it has read, retries
/// [`READ_RUN_RETRIES`] times; the size probe, which runs on a lifecycle
/// step, never does.
struct SourceRows<'a> {
    source: &'a Source,
    /// Ids no run has read or passed yet.
    left: roaring::RoaringTreemap,
    /// Most ids a run takes.
    run_ids: usize,
    /// Row bytes the runs may still read together: the read ends, with ids
    /// left, once they are spent.
    bytes: u64,
    /// Times a failed run is read again in place.
    retries: u32,
    /// Rows of the last run not handed out yet.
    rows: std::vec::IntoIter<label_source::SourceRow>,
}

impl<'a> SourceRows<'a> {
    fn new(
        source: &'a Source,
        ids: roaring::RoaringTreemap,
        run_ids: usize,
        bytes: u64,
        retries: u32,
    ) -> Self {
        Self {
            source,
            left: ids,
            run_ids: run_ids.clamp(1, source.read_run.ids.max(1)),
            bytes,
            retries,
            rows: Vec::new().into_iter(),
        }
    }

    /// The next present row, or `None` once every id is read or the bytes
    /// the runs may read are spent, which leaves ids in `left`.
    async fn next(&mut self) -> Result<Option<label_source::SourceRow>> {
        // Imperative: a run that is used up starts the next, and a run may
        // hold no row.
        loop {
            let Some(row) = self.rows.next() else {
                if self.left.is_empty() || self.bytes == 0 {
                    return Ok(None);
                }
                self.rows = self.run().await?.into_iter();
                continue;
            };
            return Ok(Some(row));
        }
    }

    /// Reads the next run, again after a failure the outbox would retry,
    /// and takes its ids off the ids left.
    async fn run(&mut self) -> Result<Vec<label_source::SourceRow>> {
        let mut failures = 0_u32;
        // Imperative: a failed read is repeated with the same ids.
        loop {
            let taken = self.left.iter().take(self.run_ids).collect::<Vec<_>>();
            let more = self.left.len() > taken.len() as u64;
            let bytes = self.bytes.min(self.source.read_run.bytes);
            match self.source.read(taken, more, bytes).await {
                Ok(Run { rows, passed }) => {
                    match passed {
                        Some(passed) => {
                            self.left.remove_range(..=passed);
                        }
                        None => self.left.clear(),
                    }
                    let read = rows
                        .iter()
                        .map(|row| row.key.len().saturating_add(row.value.len()) as u64)
                        .sum::<u64>();
                    self.bytes = self.bytes.saturating_sub(read);
                    return Ok(rows);
                }
                Err(error) => {
                    let retried = match outbox::DriverFailure::of(&error) {
                        outbox::DriverFailure::Retried(_) => failures < self.retries,
                        outbox::DriverFailure::Stops | outbox::DriverFailure::Blocks => false,
                    };
                    if !retried {
                        return Err(error);
                    }
                    failures += 1;
                    tracing::warn!(
                        operation_id = %self.source.operation_id.as_uuid(),
                        %error,
                        failures,
                        retries = self.retries,
                        "bulk vector build reads a run of its source again"
                    );
                    tokio::time::sleep(self.source.read_run.retry_delay * failures).await;
                }
            }
        }
    }
}

/// Passes 1 and 2: reads the row of every member `source` holds, run by
/// run, and links their vectors on a dedicated thread.
///
/// A row the serial scan would block on ends the attempt with
/// [`Prepared::Blocked`]. Nothing is stored while the passes run.
async fn prepare<D: Distance>(
    source: Source,
    parameters: BulkParameters,
    tuning: VectorBulkBuildTuning,
    reserved: Reserved,
    turn: Turn,
    cancelled: Arc<AtomicBool>,
    progress: Arc<PreparationProgress>,
) -> Result<Prepared>
where
    LinkedGraph: From<Box<BulkGraph<D>>>,
{
    let started = Instant::now();
    let Reserved {
        mut reservation,
        bytes_per_vector,
        write_ahead,
        write_bytes,
    } = reserved;
    let dimension = source.definition.dimension() as usize;
    let mut vectors = BulkVectors::<D>::new(dimension);
    let mut scratch = view::Scratch::new();
    let members = source.members.len();
    let mut rows = SourceRows::new(
        &source,
        source.members.clone(),
        source.read_run.ids,
        u64::MAX,
        READ_RUN_RETRIES,
    );
    let mut counts = ReadCounts::default();
    // Imperative: any row can end the pass.
    loop {
        let Some(row) = rows.next().await? else {
            break;
        };
        // The rows [`row_blocker`] names, decoded once here.
        let input_bytes = row.key.len().saturating_add(row.value.len()) as u64;
        if input_bytes > source.max_input_bytes {
            return Ok(Prepared::Blocked {
                key: row.key,
                entity_id: row.entity_id,
            });
        }
        counts.entities += 1;
        counts.input_bytes = counts.input_bytes.saturating_add(input_bytes);
        progress
            .members_read
            .store(counts.entities, Ordering::Relaxed);
        let Ok(document) = stored_vector_document(&source.definition, &row.value, &mut scratch)
        else {
            return Ok(Prepared::Blocked {
                key: row.key,
                entity_id: row.entity_id,
            });
        };
        let Some(document) = document else {
            continue;
        };
        vectors.push(
            row.entity_id.get(),
            document.vector(),
            deterministic_layer(
                source.index_id,
                source.generation,
                &source.definition,
                row.entity_id,
                &document,
            ),
        );
    }
    drop(rows);
    let operation_id = source.operation_id;
    drop(source);
    // Only a source too large to count before it was read gets here with
    // fewer vectors than a bulk build is started for.
    let count = vectors.len();
    if count == 0 {
        return Ok(Prepared::Empty { read: counts });
    }
    if (count as u64) < tuning.min_vectors() {
        return Ok(Prepared::Serial(VectorSerialBuildReason::TooFewVectors));
    }
    reservation.shrink_to(build_bytes(bytes_per_vector, count as u64, write_bytes));
    tracing::info!(
        operation_id = %operation_id.as_uuid(),
        vectors = count,
        source_rows = counts.entities,
        label_members = members,
        reserved_bytes = reservation.bytes,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "bulk vector build read its source"
    );
    // The reservation covers a vector for every member, so the vectors fit.
    progress
        .vectors
        .set(count as u64)
        .expect("the read pass ends once");
    let linking = Instant::now();
    let (sender, receiver) = tokio::sync::oneshot::channel();
    let linked = Arc::clone(&progress);
    std::thread::Builder::new()
        .name("vector-bulk-build".to_string())
        .spawn(move || {
            let constructed = BulkGraph::construct(
                vectors,
                parameters,
                tuning.construction_threads(),
                &cancelled,
                &linked.vectors_linked,
            )
            .map(|construction| (construction, reservation));
            // Nobody receives once the build was dropped.
            let _ = sender.send(constructed);
            drop(turn);
        })
        .map_err(|error| {
            HelixDbError::InvariantViolation(format!(
                "bulk vector construction thread did not start: {error}"
            ))
        })?;
    let (construction, reservation) = receiver.await.map_err(|_| {
        HelixDbError::InvariantViolation(
            "bulk vector construction thread stopped without a graph".to_string(),
        )
    })??;
    match construction {
        BulkConstruction::Built(graph) => {
            tracing::info!(
                operation_id = %operation_id.as_uuid(),
                vectors = count,
                threads = tuning.construction_threads().get(),
                elapsed_ms = linking.elapsed().as_millis() as u64,
                "bulk vector build linked its graph"
            );
            Ok(Prepared::Linked {
                graph: LinkedGraph::from(graph),
                reservation,
                write_ahead,
            })
        }
        // Only a dropped build raises the flag, and it awaits nothing.
        BulkConstruction::Cancelled => Err(HelixDbError::InvariantViolation(
            "bulk vector construction was cancelled while its build waited".to_string(),
        )),
    }
}

/// Why a pass-3 step stopped before the graph was stored.
enum Halt {
    /// The step's limits are reached; the next step continues.
    BatchFull,
    /// The step staged nothing and the build must block.
    Blocked(Blocking),
    Failed(HelixDbError),
    /// An unlink failed part-way: the graph no longer matches the stored
    /// rows.
    Torn(HelixDbError),
}

impl From<HelixDbError> for Halt {
    fn from(error: HelixDbError) -> Self {
        Self::Failed(error)
    }
}

impl From<slatedb::Error> for Halt {
    fn from(error: slatedb::Error) -> Self {
        Self::Failed(error.into())
    }
}

/// Why a pass-3 step failed.
enum StepFailure {
    /// The graph and the stored rows are as the last confirmed step left
    /// them.
    Intact(HelixDbError),
    /// An unlink failed part-way, so the graph must not be used again.
    Torn(HelixDbError),
}

impl From<HelixDbError> for StepFailure {
    fn from(error: HelixDbError) -> Self {
        Self::Intact(error)
    }
}

impl From<slatedb::Error> for StepFailure {
    fn from(error: slatedb::Error) -> Self {
        Self::Intact(error.into())
    }
}

/// One source row a pass-3 step read.
struct ReadRow {
    key: Bytes,
    input_bytes: u64,
    /// Graph position once the row is admitted.
    position: u32,
    /// The node the row stores and its entity; a row without one stores
    /// nothing.
    node: Option<(u32, IndexEntityId, VectorIndexedDocument)>,
}

/// How a pass-3 step's read ended.
enum ReadEnd {
    /// The step's limits ended it: the scan resumes behind the last row the
    /// step admits.
    Limits,
    /// The source paused behind this cursor with members left: every id up
    /// to it is read or holds no row, and the nodes of those are unlinked.
    Paused(IndexCursor),
    /// The source and the graph both ended.
    Finished,
}

/// What a pass-3 step read before it staged a node.
struct SourceRead {
    rows: Vec<ReadRow>,
    /// Graph position once every row is admitted: past the nodes unlinked
    /// behind the last row.
    position: u32,
    end: ReadEnd,
}

/// One pass-3 step's view of its build.
struct WriteBatch<'a, D: Distance> {
    step: &'a ScanStep<'a>,
    index: VectorIndex<D>,
    config: VectorIndexConfig,
    target: VectorPlanTarget<'a>,
    /// Snapshot transaction that takes every staged vector row and is never
    /// committed; rows reach `step.transaction` only as admitted plans.
    planning: &'a DbTransaction,
    /// Records the journal and metadata rows staged into `planning`.
    write: MeasuredVectorTransaction<'a>,
    /// Admitted node plans, which `write` does not record.
    nodes: VectorWriteMeasurement,
    graph: &'a mut BulkGraph<D>,
    journal: &'a mut Vec<LinkChange>,
    accounting: VectorBatchAccounting,
    cursor: Option<IndexCursor>,
    position: u32,
    journal_applied: usize,
    /// Unlinks the step made, and the time they took.
    unlinked: usize,
    unlink_time: Duration,
    unlink_step_time: Duration,
    /// First node the step's read has not decided on, as far as the step
    /// knows: the nodes before it are staged, left to a later step with
    /// their rows, or unlinked, and every node the step unlinked is one of
    /// them even when its read failed afterwards.
    reached: u32,
}

impl<D: Distance> WriteBatch<'_, D> {
    /// A step blocks only while this holds, because a blocked step commits
    /// what it staged with unchanged progress.
    fn nothing_staged(&self) -> bool {
        self.accounting.is_empty() && self.accounting.vector_writes.operations() == 0
    }

    /// Every vector write the step has staged, with `unrecorded` for a node
    /// plan `write` did not record.
    ///
    /// Node plans, journal rows and the metadata row never share a key. Two
    /// journal entries of one stored neighbor stage its row twice, and the
    /// sum counts both, which only fills the step sooner.
    fn cumulative(
        &self,
        unrecorded: VectorWriteMeasurement,
    ) -> std::result::Result<VectorWriteMeasurement, Halt> {
        let recorded = self.write.measurement().map_err(measurement_error)?;
        Ok(VectorWriteMeasurement::from_exact_parts(
            recorded
                .operations()
                .saturating_add(self.nodes.operations())
                .saturating_add(unrecorded.operations()),
            recorded
                .encoded_bytes()
                .saturating_add(self.nodes.encoded_bytes())
                .saturating_add(unrecorded.encoded_bytes()),
        ))
    }

    /// Applies `plan` and the `lifecycle` (operations, bytes) staged beside
    /// it to the step's transaction if the step's limits admit `cumulative`,
    /// the step's vector writes with this plan, and returns the plan's bytes.
    ///
    /// A plan no step can admit blocks unconditionally; a caller that plans
    /// for a source row names the row instead ([`Self::for_row`]).
    fn admit(
        &self,
        plan: PlannedVectorMutation,
        cumulative: VectorWriteMeasurement,
        lifecycle: (u64, u64),
        entity_id: IndexEntityId,
    ) -> std::result::Result<u64, Halt> {
        let limits = self.step.limits;
        let bytes = plan.measurement().encoded_bytes();
        let (observed, limit) = if bytes > limits.max_single_vector_output_bytes().get() {
            (bytes, limits.max_single_vector_output_bytes().get())
        } else if !self
            .accounting
            .can_admit_output(cumulative, lifecycle.0, lifecycle.1)
        {
            (
                cumulative
                    .encoded_bytes()
                    .saturating_add(self.accounting.lifecycle_bytes)
                    .saturating_add(lifecycle.1),
                limits.max_output_bytes().get(),
            )
        } else {
            plan.apply_to(self.step.transaction)?;
            return Ok(bytes);
        };
        Err(if self.nothing_staged() {
            Halt::Blocked(Blocking::Unconditional(
                IndexOperationBlocker::OversizedEntity {
                    entity_kind: self.step.definition.element_kind(),
                    entity_id,
                    observed,
                    limit,
                },
            ))
        } else {
            Halt::BatchFull
        })
    }

    /// The halt of a plan made for the source row of `entity_id` at `key`:
    /// one no step can admit blocks for as long as that row is there.
    fn for_row(halt: Halt, key: Bytes, entity_id: IndexEntityId) -> Halt {
        match halt {
            Halt::Blocked(Blocking::Unconditional(blocker)) => Halt::Blocked(Blocking::Row {
                key,
                entity_id,
                otherwise: Some(blocker),
            }),
            halt @ (Halt::BatchFull
            | Halt::Blocked(Blocking::Row { .. } | Blocking::Failure(_))
            | Halt::Failed(_)
            | Halt::Torn(_)) => halt,
        }
    }

    /// Stages again the rows that journal entries changed for nodes already
    /// stored. A node not yet stored is staged with its current links when
    /// the scan reaches it, which is why the step stages no node before its
    /// last unlink.
    fn restage_journal(&mut self) -> std::result::Result<(), Halt> {
        // Imperative: each admitted change moves the limits the next meets.
        loop {
            let Some(change) = self.journal.get(self.journal_applied).copied() else {
                return Ok(());
            };
            if change.source < self.position && !self.graph.is_unlinked(change.source) {
                let checkpoint = self.write.checkpoint();
                self.index
                    .stage_bulk_link_change(&self.write, self.graph, change)?;
                let plan = self
                    .write
                    .plan_since(checkpoint)
                    .map_err(measurement_error)?;
                let cumulative = self.cumulative(VectorWriteMeasurement::zero())?;
                let entity_id = IndexEntityId::new(self.graph.entity_id(change.source));
                let bytes = self
                    .admit(plan, cumulative, (0, 0), entity_id)
                    .map_err(|halt| {
                        Self::for_row(
                            halt,
                            label_source::source_key(
                                self.step.scope,
                                self.step.definition.element_kind(),
                                entity_id.get(),
                            ),
                            entity_id,
                        )
                    })?;
                self.accounting.admit_vector_writes(cumulative, bytes);
            }
            self.journal_applied += 1;
        }
    }

    /// Unlinks, in memory, every node from `position` up to `before`, or to
    /// the end of the graph: the scan passed where their entities were.
    /// Returns whether it got there within the step's unlink allowance
    /// ([`Self::unlink`]).
    fn unlink_absent(
        &mut self,
        position: &mut u32,
        before: Option<NodeId>,
    ) -> std::result::Result<bool, Halt> {
        let end = before.map_or(self.graph.len() as u32, |entity_id| {
            self.graph
                .position(entity_id)
                .unwrap_or_else(|following| following)
        });
        while *position < end {
            if !self.graph.is_unlinked(*position) && !self.unlink(*position)? {
                return Ok(false);
            }
            *position += 1;
        }
        Ok(true)
    }

    /// Unlinks `node`, in memory, if the step's unlink allowance has room,
    /// and returns whether it did. A failure leaves the graph torn.
    ///
    /// An unlink runs here, on the step's task, and costs milliseconds
    /// ([`BulkGraph::unlink`]). A step makes its first unlink whatever it
    /// costs, so every step that meets one moves on, and another only while
    /// it has made fewer than the entities it may read and spent less than
    /// [`UNLINK_STEP_TIME`] on them: whether the node's entity is gone or
    /// holds another vector.
    fn unlink(&mut self, node: u32) -> std::result::Result<bool, Halt> {
        let room = self.unlinked == 0
            || (self.unlinked < self.step.limits.max_entities().get()
                && self.unlink_time < self.unlink_step_time);
        if !room {
            return Ok(false);
        }
        let started = Instant::now();
        self.graph.unlink(node, self.journal).map_err(Halt::Torn)?;
        self.unlink_time += started.elapsed();
        self.unlinked += 1;
        // Whatever becomes of the step's read, the pass has passed the node.
        self.reached = self.reached.max(node + 1);
        Ok(true)
    }

    /// Reads the next rows of the step's source, which holds at most the
    /// step's entities, under its input limit, and unlinks every node whose
    /// entity is gone or no longer holds the vector it was linked for.
    ///
    /// Nothing is staged and only the graph and the journal change, so a row
    /// read here and not admitted afterwards is read again by the next step,
    /// which finds its unlinks already made.
    async fn read(
        &mut self,
        mut rows: Option<LabelSource<'_, DbTransaction>>,
    ) -> std::result::Result<SourceRead, Halt> {
        let step = self.step;
        let kind = step.definition.element_kind();
        let mut scratch = view::Scratch::new();
        // Counts rows as they are read; `self.accounting` counts them as they
        // are admitted.
        let mut reading = VectorBatchAccounting::new(OperationCounters::default(), step.limits);
        let mut read = SourceRead {
            rows: Vec::new(),
            position: self.position,
            end: ReadEnd::Limits,
        };
        // Bytes the vector rows of the nodes read so far hold at least.
        let mut vector_bytes = 0_u64;
        // Imperative: every row moves the limits and the graph position.
        loop {
            // A source with no member left ends like an exhausted one.
            let ended = match rows.as_mut() {
                None => LabelRead::Exhausted,
                Some(rows) => rows.next().await?,
            };
            let row = match ended {
                LabelRead::Row(row) => row,
                LabelRead::Exhausted => {
                    if self.unlink_absent(&mut read.position, None)? {
                        read.end = ReadEnd::Finished;
                    }
                    return Ok(read);
                }
                // Every id up to the cursor that gave no row holds none.
                LabelRead::Paused(cursor) => {
                    let passed = label_source::cursor_id(step.scope, kind, cursor.as_bytes())?;
                    let reached = match passed.checked_add(1) {
                        Some(next) => self.unlink_absent(&mut read.position, Some(next))?,
                        None => self.unlink_absent(&mut read.position, None)?,
                    };
                    if reached {
                        read.end = ReadEnd::Paused(cursor);
                    }
                    return Ok(read);
                }
            };
            let entity_id = row.entity_id;
            let input_bytes = row.key.len().saturating_add(row.value.len()) as u64;
            // A row blocks only a step that has nothing to commit.
            let idle = read.rows.is_empty() && self.nothing_staged();
            // A row past the step's input limit is not decoded.
            let readable = reading
                .can_admit_input(input_bytes)
                .then(|| stored_vector_document(step.definition, &row.value, &mut scratch).ok())
                .flatten();
            let Some(document) = readable else {
                if !idle {
                    return Ok(read);
                }
                // The oversized row of a step that read nothing else, or a
                // row that does not decode: the serial scan blocks on both.
                return Err(Halt::Blocked(Blocking::Row {
                    key: row.key,
                    entity_id,
                    otherwise: None,
                }));
            };
            if !self.unlink_absent(&mut read.position, Some(entity_id.get()))? {
                return Ok(read);
            }
            let node = self.graph.position(entity_id.get()).ok();
            if node.is_some_and(|node| node != read.position) {
                return Err(Halt::Failed(HelixDbError::InvariantViolation(
                    "bulk vector write position passed a source entity".to_string(),
                )));
            }
            // An entity that appeared or changed after pass 1 read the source
            // has a queued operation that indexes it after activation.
            let current = node.zip(document).filter(|(node, document)| {
                !self.graph.is_unlinked(*node)
                    && BulkVectors::<D>::source_hash(document.vector())
                        == self.graph.source_hash(*node)
            });
            // A node whose unlink the step has no room for ends the read: the
            // next step reads its row again.
            let unlinked = node
                .filter(|node| current.is_none() && !self.graph.is_unlinked(*node))
                .map(|node| self.unlink(node))
                .transpose()?;
            if unlinked == Some(false) {
                return Ok(read);
            }
            // A vector row holds its vector, so nodes past this bound cannot
            // be admitted and are left to the next step unstaged.
            vector_bytes =
                vector_bytes.saturating_add(current.as_ref().map_or(0, |(_, document)| {
                    std::mem::size_of_val(document.vector()) as u64
                }));
            if vector_bytes > step.limits.max_output_bytes().get()
                && read.rows.iter().any(|row| row.node.is_some())
            {
                return Ok(read);
            }
            reading.admit(input_bytes, VectorWriteMeasurement::zero(), 0, 0, 0)?;
            read.position = node.map_or(read.position, |node| node + 1);
            read.rows.push(ReadRow {
                key: row.key,
                input_bytes,
                position: read.position,
                node: current.map(|(node, document)| (node, entity_id, document)),
            });
        }
    }

    /// Plans the rows of every node `read` holds, in row order, on up to
    /// [`STAGING_THREADS`] threads.
    ///
    /// A node's rows depend on the graph and its vector alone and nothing
    /// unlinks while this runs, so the plans are the ones one thread makes.
    fn plan(&self, read: &SourceRead) -> Result<Vec<PlannedVectorMutation>> {
        let nodes = read
            .rows
            .iter()
            .filter_map(|row| row.node.as_ref())
            .map(|(node, _, document)| (*node, document.vector()))
            .collect::<Vec<_>>();
        let threads = STAGING_THREADS
            .min(nodes.len() / NODES_PER_STAGING_THREAD)
            .max(1);
        let (index, planning, graph) = (&self.index, self.planning, &*self.graph);
        let plan = |nodes: &[(u32, &[f32])]| {
            // Writes reach `planning` only to be measured; each thread records
            // its own.
            let write = VectorWriteRecorder::new().bind(planning);
            nodes
                .iter()
                .map(|&(node, vector)| {
                    let checkpoint = write.checkpoint();
                    index.stage_bulk_node(&write, graph, node, vector)?;
                    write.plan_since(checkpoint).map_err(measurement_error)
                })
                .collect::<Result<Vec<_>>>()
        };
        let plan = &plan;
        // Scoped threads on the step's own task: a step's nodes take
        // milliseconds to plan, as they did on one thread.
        std::thread::scope(|scope| {
            let mut chunks = nodes.chunks(nodes.len().div_ceil(threads).max(1));
            let first = chunks.next();
            let spawned = chunks
                .map(|chunk| scope.spawn(move || plan(chunk)))
                .collect::<Vec<_>>();
            first
                .map(plan)
                .into_iter()
                .chain(
                    spawned
                        .into_iter()
                        // A staging thread's panic is the step's, which
                        // fails for it ([`BulkBuilds::write`]).
                        .map(|worker| {
                            worker
                                .join()
                                .unwrap_or_else(|panic| std::panic::resume_unwind(panic))
                        }),
                )
                .try_fold(Vec::with_capacity(nodes.len()), |mut plans, planned| {
                    plans.extend(planned?);
                    Ok(plans)
                })
        })
    }

    /// Stores nodes in source order until the graph is stored (`Ok`) or the
    /// step must end.
    ///
    /// The step reads and unlinks first ([`Self::read`]), plans the rows of
    /// the nodes it read in parallel ([`Self::plan`]), and then admits the
    /// plans in source order, so the cursor, the position and the counters
    /// cover exactly the rows whose plans the step's transaction holds.
    async fn store(
        &mut self,
        rows: Option<LabelSource<'_, DbTransaction>>,
    ) -> std::result::Result<(), Halt> {
        let step = self.step;
        let kind = step.definition.element_kind();
        self.restage_journal()?;
        let read = self.read(rows).await?;
        self.reached = self.reached.max(read.position);
        let mut plans = self.plan(&read)?.into_iter();
        self.restage_journal()?;
        // Imperative: every admitted row moves the limits the next meets.
        for row in read.rows {
            let cursor = IndexCursor::try_new(row.key.clone()).map_err(operation_error)?;
            let Some((_, entity_id, document)) = row.node else {
                self.accounting
                    .admit(row.input_bytes, self.accounting.vector_writes, 0, 0, 0)?;
                self.position = row.position;
                self.cursor = Some(cursor);
                continue;
            };
            let plan = plans.next().ok_or_else(|| {
                HelixDbError::InvariantViolation(
                    "bulk vector write step planned fewer nodes than it read".to_string(),
                )
            })?;
            let planned = plan.measurement();
            let lifecycle = lifecycle_write_measurement(
                &self.target,
                kind,
                entity_id,
                AppliedStateTransition::Put(document.partition()),
                None,
                &[],
            )?;
            let cumulative = self.cumulative(planned)?;
            let bytes = self
                .admit(plan, cumulative, lifecycle, entity_id)
                .map_err(|halt| Self::for_row(halt, row.key, entity_id))?;
            self.nodes = VectorWriteMeasurement::from_exact_parts(
                self.nodes.operations().saturating_add(planned.operations()),
                self.nodes
                    .encoded_bytes()
                    .saturating_add(planned.encoded_bytes()),
            );
            stage_applied(
                step.transaction,
                step.scope,
                step.operation,
                kind,
                entity_id,
                Some(document.partition().clone()),
            )?;
            self.accounting
                .admit(row.input_bytes, cumulative, bytes, lifecycle.0, lifecycle.1)?;
            self.position = row.position;
            self.cursor = Some(cursor);
        }
        self.position = read.position;
        match read.end {
            ReadEnd::Limits => return Err(Halt::BatchFull),
            // Every row the source read is admitted, so the scan resumes
            // behind the last id it took, as the serial scan's does.
            ReadEnd::Paused(cursor) => {
                self.cursor = Some(cursor);
                return Err(Halt::BatchFull);
            }
            ReadEnd::Finished => {}
        }
        let checkpoint = self.write.checkpoint();
        self.index
            .stage_bulk_metadata(&self.write, self.graph, self.config.clone())
            .await?;
        let plan = self
            .write
            .plan_since(checkpoint)
            .map_err(measurement_error)?;
        let cumulative = self.cumulative(VectorWriteMeasurement::zero())?;
        let bytes = self.admit(plan, cumulative, (0, 0), IndexEntityId::initial())?;
        self.accounting.admit_vector_writes(cumulative, bytes);
        Ok(())
    }
}

/// What a pass-3 step came to.
enum Written {
    /// The step's result, to commit.
    Step(Box<VectorStepResult>),
    /// The step staged nothing and the build must block.
    Blocked(Blocking),
}

/// Pass 3: stages the next nodes of `graph` in source order and the scan
/// progress that covers them.
///
/// Blockers follow [`super::scan_source`]: a row that cannot be admitted
/// blocks only a step that staged nothing, which the step reports instead of
/// a result ([`Written::Blocked`]): the build leaves the bulk stage before it
/// blocks.
///
/// Every step but the last stores the bulk stage with the attempt's count.
/// The last stages the metadata row and leaves the bulk path for validation,
/// in one commit: the first stored stage an older binary can read again.
async fn write_step<D: Distance>(
    step: &ScanStep<'_>,
    graph: &mut BulkGraph<D>,
    writing: &mut Writing,
) -> std::result::Result<Written, StepFailure> {
    let Writing {
        journal,
        committed,
        position,
        step_input_bytes,
        unlink,
        unlinks,
        ..
    } = writing;
    let progress = &committed.scan;
    let IndexStateVectorPhysical { layout } = IndexStateVectorPhysical::from_record(step.record)?;
    let VectorPhysicalLayout::Unpartitioned { physical_index_id } = layout else {
        return Err(corruption("bulk vector build targets a partitioned generation").into());
    };
    let target = VectorPlanTarget::build(step.scope, step.operation, step.record)?;
    let handle = target.physical::<D>(physical_index_id)?;
    // Source rows are read untracked and vector rows are staged here before
    // the step's transaction admits them, as in the serial scan.
    let planning = step.db.begin(IsolationLevel::Snapshot).await?;
    // A step drops whatever its source fetched ahead of the rows it admits,
    // so a scan fetches about what the last step admitted, in more ranges at
    // a time once the configured ones no longer cover that.
    let configured = step.scan_tuning;
    let expected = usize::try_from(*step_input_bytes).unwrap_or(usize::MAX);
    let tasks = expected
        .div_ceil(configured.read_ahead_bytes().get())
        .clamp(
            configured.max_fetch_tasks().get(),
            configured.max_fetch_tasks().get().max(SOURCE_FETCH_TASKS),
        );
    let read_ahead = configured.read_ahead_bytes().get().max(expected / tasks);
    let rows = LabelSource::open(
        &planning,
        step.scope,
        step.definition.element_kind(),
        step.definition.label().as_str(),
        progress,
        step.limits,
        IndexLifecycleScanTuning::try_new(read_ahead, tasks)
            .map_err(|error| HelixDbError::InvariantViolation(error.to_string()))?,
    )
    .await?;
    let recorder = VectorWriteRecorder::new();
    let mut batch = WriteBatch {
        step,
        index: VectorIndex::<D>::from_generation(&handle)
            .with_simhasher_registry(Arc::clone(step.simhasher_registry))
            .with_batch_reads(step.batch_reads),
        config: VectorIndexConfig::from_v2_definition(step.definition, handle.physical_name()),
        target,
        planning: &planning,
        write: recorder.bind(&planning),
        nodes: VectorWriteMeasurement::zero(),
        graph,
        journal,
        accounting: VectorBatchAccounting::new(progress.counters, step.limits),
        cursor: progress.cursor.clone(),
        position: *position,
        journal_applied: 0,
        unlinked: 0,
        unlink_time: Duration::ZERO,
        unlink_step_time: unlink.step_time,
        reached: *position,
    };
    let halted = batch.store(rows).await;
    let WriteBatch {
        accounting,
        cursor,
        position,
        journal_applied,
        unlinked,
        unlink_time,
        reached,
        ..
    } = batch;
    // The unlinks are made in the graph whatever becomes of the step.
    *unlinks = Unlinks {
        nodes: unlinks.nodes.saturating_add(unlinked as u32),
        passed: unlinks.passed.max(reached),
        time: unlinks.time + unlink_time,
    };
    let stored = match halted {
        Ok(()) => true,
        Err(Halt::BatchFull) => false,
        Err(Halt::Blocked(blocking)) => return Ok(Written::Blocked(blocking)),
        Err(Halt::Failed(error)) => return Err(StepFailure::Intact(error)),
        Err(Halt::Torn(error)) => return Err(StepFailure::Torn(error)),
    };
    // A hint only, so it follows the step whether or not its commit lands.
    *step_input_bytes = accounting.input_bytes;
    let planned_writes = accounting.vector_writes.operations();
    // The counters are this pass's alone, as a serial scan of the same rows
    // counts them: pass 1 stored nothing of what it read.
    let (counters, single_vector_output_bytes) = accounting.finish_with_max()?;
    let (stage, next) = if stored {
        (
            VectorBuildStage::ValidateDescriptor(PrefixScanProgress {
                cursor: None,
                counters,
            }),
            None,
        )
    } else {
        let next = BulkBuildProgress {
            scan: SourceScanProgress {
                inclusive_upper_bound: progress.inclusive_upper_bound.clone(),
                cursor,
                counters,
            },
            attempts: committed.attempts,
        };
        (
            VectorBuildStage::BulkBuild(next.clone()),
            Some((next, position)),
        )
    };
    Ok(Written::Step(Box::new(VectorStepResult {
        result: progressed_build(stage),
        single_vector_output_bytes,
        physical_operations: 0,
        output_bytes: 0,
        vector_planning: VectorPlanningUsage {
            planned_writes,
            ..VectorPlanningUsage::default()
        },
        counters: StepCounters::Continued,
        committed: Some(CommittedStepState::VectorBulkBuild(
            BulkBuildCommit::Stored {
                next,
                journal_applied,
            },
        )),
    })))
}

#[cfg(test)]
#[path = "../../../../tests/unit/vector_bulk_build_contracts.rs"]
mod contract_tests;
