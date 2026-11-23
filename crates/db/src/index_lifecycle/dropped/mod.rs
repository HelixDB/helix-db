//! Dropped generations whose rows compactions discard.
//!
//! A drop, or the abort of a build, that completes on a store at
//! [`IndexStorageVersion::DROPPED_GENERATIONS`] does not delete the
//! generation's rows one by one. It deletes the few rows no filter may match,
//! and in the transaction that moves the index record to `Dropped` it writes
//! one global *dropped-generation entry*. From then on the rows are garbage
//! that nothing reads, and compactions discard them. An aborted build ends
//! in the `Dropped` state a drop ends in, and its ids are as final: a
//! recreated index builds the next generation under new physical ids. What
//! this module says of a drop holds for an abort unless it says otherwise.
//!
//! # Contract
//!
//! - **Creation.** A cleanup that leaves rows to compactions ends with
//!   [`LeftToCompactions`](super::outbox::IndexOperationStepResult::LeftToCompactions),
//!   and the one function that commits a step calls [`stage_entry`] in the
//!   record's `Dropped` transaction: the only way an entry is created. The
//!   entry commits at the sequence the record does, so a view that still
//!   reads the generation's rows is older than the entry. The same function
//!   tells the writer's [`Registry`] once the commit is durable, so an entry
//!   is never stored without the retirer being woken to arm it.
//! - **Arming.** A writer opens with an empty registry and discards nothing
//!   for an entry until [`Registry::refresh`], which a background pass runs,
//!   has read it through a snapshot, cross-checked it
//!   against the catalog and the id watermarks, and flushed the memtable. The
//!   snapshot's sequence is the entry's *gate*: it is at or after the entry's
//!   commit. The flush makes the `Dropped` record durable in a sorted table
//!   before any compaction output can lack the rows, so no manifest shows the
//!   rows gone beside a record that still serves them.
//! - **Discarding.** A compaction job discards an armed entry's rows only
//!   when its `retention_min_seq` has reached the entry's gate, which means
//!   no snapshot or transaction of the writer older than the drop is open.
//!   Each job decides once, when its filters are created. Rows are dropped
//!   outright, merge operands and tombstones included: nothing reads them, so
//!   nothing needs a tombstone to shadow an older copy.
//! - **Cross-check.** An entry never names rows of a generation that is
//!   building, active, aborting or dropping. [`Registry::refresh`] checks
//!   every stored entry it has not armed against the catalog and the id
//!   watermarks, and *quarantines* one that fails: its rows are kept, the
//!   store opens, and the entry is reported until a later refresh finds it
//!   consistent or [`retire::Retirer::remove_quarantined`] deletes it on
//!   request. No allocation hands out an id an entry names, armed or
//!   quarantined: [`unclaimed_index_id`] and [`unclaimed_generation`] skip
//!   past them, so no entry can fail the creation of an index either. A
//!   vector physical id has no such skip: an entry is keyed by generation,
//!   not by physical id, so it would scan every entry on every allocation.
//!   The cross-check gives the same guarantee, because an armed entry names
//!   only ids below the watermark, which are never allocated again, and the
//!   transaction that allocates an id above it also writes the record or the
//!   partition mapping that makes the cross-check quarantine an entry naming
//!   that id.
//! - **Retirement.** [`retire`] deletes an entry once no table of the store
//!   can hold a row of it; see that module for the proof. Compactions the
//!   scheduler picks retire entries as they pass. A restart forgets the
//!   proof and starts it again from every table the store then has.
//! - **Reclaim.** [`reclaim`] rewrites the sorted runs that hold the rows:
//!   every one of them on request, and by itself those in which the rows
//!   are a large enough part to be worth it, once the store has been idle.
//!   It rewrites no run that holds none, deletes no entry itself, and never
//!   takes the compactor's last free slot. A store that is never idle keeps
//!   its dropped rows until its own compactions rewrite the runs they are
//!   in.
//!
//! # What a drop still deletes itself
//!
//! - **Unpartitioned vector.** The rows of its physical id that no filter
//!   matches, in one bounded step. The entry records the physical id.
//! - **Partitioned vector.** The same rows of each partition's physical id,
//!   one cleanup step per partition. The partition mappings stay: they are
//!   the only list of the generation's physical ids, so the cross-check and
//!   every filter read them, and [`retire`] deletes them with the entry.
//! - **Secondary and text.** Nothing: every row is keyed by the generation.
//!   Text split blobs in object storage are not rows and are retained, as
//!   they were before.
//!
//! A cleanup an older image had started row by row, before the store was
//! admitted, is handed to an entry at its next step on an admitted store,
//! whatever it had deleted: the entry covers what is left. Rows are deleted
//! one by one in three cases only, and each says so where it does it:
//!
//! - **A store that does not admit entries.** Images that do not know the
//!   entries can still open it, so nothing but the drop would remove the
//!   rows.
//! - **A generation adopted from legacy vector storage.** It is never given
//!   an entry: its physical id was not allocated by this catalog and its
//!   rows may be shared with a legacy index. Its drop deletes every row one
//!   by one. The abort of its build deletes the directory and the
//!   generation-keyed rows the build added, one by one, and hands the
//!   legacy rows back untouched.
//! - **An abandoned attempt of a bulk vector build.** A process that finds
//!   a build at the bulk stage and holds no graph for it deletes what the
//!   attempt stored, applied state first and then the physical rows, before
//!   the build goes on (see "Crashes" in the module documentation of
//!   `index_lifecycle/vector/driver/bulk.rs`). The generation is still
//!   building and will be built again under the same physical id, so
//!   it cannot be given an entry: the cross-check would refuse one, and a
//!   filter armed for it would discard the rows of the next attempt. On an
//!   admitted store too, that deletion removes every row itself, the lanes a
//!   filter may match included. An abort that arrives while it runs, or
//!   while an attempt writes, goes through the ordinary abort: it deletes
//!   the rows no filter matches and its entry covers whatever else the
//!   attempts stored, since they write only the rows a serial build writes.
//!
//! Whether a drop leaves an entry depends on the store alone, not on the
//! writer that runs it. A writer that runs no compaction worker in its own
//! process still leaves entries, and does nothing more with them: it runs
//! no retirement task, so it arms none, discards no row and retires
//! nothing. Its entries wait for a writer that compacts in process; see
//! [`DroppedStorageCompactor::None`].
//!
//! # What does not scale
//!
//! Every armed entry costs one hash-set member per owner in each compaction
//! filter; a partitioned vector generation has one owner per partition. A
//! refresh scans the entry prefix; when it finds entries it has not armed it
//! reads, once for each scope they name, that scope's index records and, for
//! vector entries, its partition mappings, and one metadata key per physical
//! id. Every entry is new to a writer that opens, so an open pays that once.
//! A quarantined entry is not armed, so every refresh pays it again for the
//! scopes quarantined entries name, for as long as one is stored.
//! Dropping a partitioned vector generation still costs one cleanup commit
//! per partition, and its mappings stay until the entry retires. An entry
//! lives as long as a table that held its rows when the proof started. The
//! rows of an old index sit in the oldest sorted run, which the scheduler
//! rewrites rarely; when they are too small a part of it to be worth its
//! rewrite the writer leaves them, and the entry stays for as long as that
//! run does unless a caller asks for a reclaim.

pub(crate) mod filter;
pub(crate) mod ownership;
pub(crate) mod reclaim;
pub(crate) mod retire;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use slatedb::config::{FlushOptions, FlushType};
use slatedb::object_store::ObjectStore;
use slatedb::{CompactionJobContext, Db, DbReadOps};

use crate::encoding::v2::keys::indexes::vector::{VectorIndexMetadataKey, VectorKey};
use crate::encoding::v2::keys::scope::DataScope;
use crate::encoding::v2::keys::{
    DataKey, DataKeyKind, GlobalKey, GlobalKind, ManagedIndexKey, RecordKind, ScopedKey,
};
use crate::encoding::v2::values::{
    decode_index_record, decode_metadata_value, decode_partition_mapping, encode_metadata_value,
};
use crate::error::{HelixDbError, Result};

use self::ownership::OwnerKey;
use super::failpoints::{self, IndexOutboxFailpoint};
use super::{
    repository, DroppedGenerationRows, DroppedGenerationValue, IndexDefinitionFamily,
    IndexGenerationId, IndexId, IndexStorageVersion, IndexV2MetadataValue, PhysicalGeneration,
    VectorPhysicalIndexId,
};

/// Moves a store at [`IndexStorageVersion::LABEL_IDENTITY`] to
/// [`IndexStorageVersion::DROPPED_GENERATIONS`] in one commit that writes
/// only the marker; the two versions share every other row.
///
/// The move is automatic, and two callers perform it: a writer open that is
/// authorized to migrate (an embedded open, or a managed open with a
/// controlled-migration intent), once it has started or found finished the
/// label identity copy; and the background migration worker of any writer,
/// a managed failover writer included, right after its copy has published
/// version 7. A pristine managed store is bootstrapped at the current
/// version, and a new embedded database ends its first open there.
///
/// The move is one way. Binaries that support at most version 7 refuse the
/// store from then on: they would neither decode a dropped-generation entry
/// nor discard its rows. So every reader must run an image that knows
/// version 8 before any writer that knows it opens the store.
///
/// A store below version 7 is left alone: one whose label identity copy is
/// still running is admitted by the background worker in a second commit
/// after the copy publishes, or by the next migration-authorized open when
/// the process dies between the two. A store already admitted is left alone
/// too.
pub(crate) async fn admit(db: &Db) -> Result<()> {
    if crate::migrations::startup::storage_version(db).await? != IndexStorageVersion::LABEL_IDENTITY
    {
        return Ok(());
    }
    crate::migrations::startup::advance_storage_version(
        db,
        IndexStorageVersion::LABEL_IDENTITY..IndexStorageVersion::DROPPED_GENERATIONS,
        IndexStorageVersion::DROPPED_GENERATIONS,
        |_| Ok(()),
    )
    .await?;
    tracing::info!("index storage admits dropped-generation entries");
    Ok(())
}

/// The embedded compactor of a writer at `path`, whose compactions discard
/// the rows of `registry`'s armed entries; `None` when `settings` run no
/// compactor.
///
/// Must be called on the runtime the compactor is to run on.
pub(crate) fn compactor(
    settings: &slatedb::Settings,
    path: &str,
    object_store: Arc<dyn ObjectStore>,
    registry: Arc<Registry>,
) -> Option<slatedb::CompactorBuilder<String>> {
    Some(
        slatedb::CompactorBuilder::new(path.to_string(), object_store)
            .with_options(compactor_options(settings)?)
            .with_compaction_filter_supplier(Arc::new(filter::DroppedRowsFilterSupplier::new(
                registry,
            ))),
    )
}

/// The options of the embedded compactor of a writer opened with `settings`,
/// with trivial moves off whatever `settings` say; `None` when they run no
/// compactor.
///
/// A trivial move re-links tables into a run without creating a filter, so
/// a reclaim made of one would rewrite nothing and give no byte back, and
/// the tables of a manifest would change without its [`retire::Shape`]
/// moving. SlateDB also runs one in its coordinator, outside the check that
/// refuses a submitted compaction which shares a source with one in flight:
/// with trivial moves on, that check would not see every compaction.
pub(crate) fn compactor_options(
    settings: &slatedb::Settings,
) -> Option<slatedb::config::CompactorOptions> {
    let mut options = settings.compactor_options.clone()?;
    options.enable_trivial_move = false;
    Some(options)
}

/// How many compactions a writer opened with `settings` runs at a time in
/// its own process, behind the filter [`compactor`] installs; `None` when it
/// does not run every compaction of its store there.
///
/// [`retire`] rests on the writer compacting in process: a compaction
/// another process ran would replace tables without consulting this writer's
/// [`Registry`]. SlateDB runs a compaction in whichever worker claims it,
/// and a writer's embedded worker is the only one Helix starts; a writer
/// configured without it leaves its compactions to workers it cannot see, so
/// it retires nothing. The count is the smaller of what the coordinator
/// schedules and what the worker claims: [`reclaim`] never takes the last
/// free one of them.
pub(crate) fn compaction_slots(settings: &slatedb::Settings) -> Option<usize> {
    let options = settings.compactor_options.as_ref()?;
    let worker = options.worker.as_ref()?;
    Some(
        options
            .max_concurrent_compactions
            .min(worker.max_concurrent_compactions),
    )
}

/// One generation of one logical index, as a dropped-generation entry names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct DroppedGeneration {
    pub(crate) index_id: IndexId,
    pub(crate) generation: IndexGenerationId,
}

impl DroppedGeneration {
    fn key(self) -> Bytes {
        ManagedIndexKey::Global {
            kind: GlobalKey::DroppedGeneration {
                index_id: self.index_id,
                generation: self.generation,
            },
        }
        .to_bytes()
    }
}

/// Whether the store admits dropped-generation entries.
///
/// A store below [`IndexStorageVersion::DROPPED_GENERATIONS`] can still be
/// opened by a binary that does not know the entries, so its drops delete
/// their rows one by one.
pub(crate) async fn admitted(read: &(impl DbReadOps + Sync)) -> Result<bool> {
    Ok(crate::migrations::startup::storage_version(read).await?
        >= IndexStorageVersion::DROPPED_GENERATIONS)
}

/// Moves a bootstrapped fixture store to
/// [`IndexStorageVersion::DROPPED_GENERATIONS`], as an open does.
#[cfg(test)]
pub(crate) async fn admit_fixture(db: &Db) {
    db.put(
        ManagedIndexKey::Global {
            kind: GlobalKey::StorageVersion,
        }
        .to_bytes(),
        encode_metadata_value(&IndexV2MetadataValue::StorageVersion(
            IndexStorageVersion::LABEL_IDENTITY,
        )),
    )
    .await
    .expect("fixture storage version is written");
    admit(db)
        .await
        .expect("fixture store admits dropped-generation entries");
    assert!(admitted(db).await.expect("storage version reads"));
}

/// Puts a fixture store that holds no dropped-generation entry back at
/// [`IndexStorageVersion::LABEL_IDENTITY`], as a store that has only been
/// served by writers that do not move it: its drops and aborts delete their
/// rows one by one. An open with leave to migrate admits it again.
///
/// # Errors
///
/// An entry is stored, which an image at that version could not read; or a
/// failed read or write.
#[cfg(feature = "index-lifecycle-testing")]
pub(crate) async fn unadmit_fixture(db: &Db) -> Result<()> {
    if !load_entries(db).await?.is_empty() {
        return Err(HelixDbError::InvariantViolation(
            "a store that holds a dropped-generation entry cannot refuse them".to_string(),
        ));
    }
    db.put(
        ManagedIndexKey::Global {
            kind: GlobalKey::StorageVersion,
        }
        .to_bytes(),
        encode_metadata_value(&IndexV2MetadataValue::StorageVersion(
            IndexStorageVersion::LABEL_IDENTITY,
        )),
    )
    .await
    .map_err(HelixDbError::from_storage_commit)?;
    Ok(())
}

/// Compacts on request until `db` holds no dropped-generation entry, for
/// tests that count the rows drops leave behind.
///
/// SlateDB publishes its oldest open reader only when it flushes a memtable
/// that holds rows, so a pass that finds the rows retained is followed by
/// the write of one node no index covers. The writer stops reading the
/// discarded rows at its next manifest poll, which may be after this returns.
#[cfg(any(test, feature = "index-lifecycle-testing"))]
pub(crate) async fn reclaim_fixture(db: &crate::HelixDB) {
    use helix_ast::value::PropertyInput;
    use helix_ast::{batch::write_batch, query::QueryRequest, traversal::g};

    use self::reclaim::DroppedStorageReclaim;

    tokio::time::timeout(std::time::Duration::from_secs(60), async {
        loop {
            match db
                .reclaim_dropped_index_storage(std::time::Duration::from_secs(30))
                .await
                .expect("the reclaim runs")
            {
                DroppedStorageReclaim::Idle => break,
                // Rewritten, or lost to a compaction the scheduler picked:
                // the next call tells whether an entry is left. A caller is
                // never declined.
                DroppedStorageReclaim::Compacted { .. }
                | DroppedStorageReclaim::Declined { .. }
                | DroppedStorageReclaim::Incomplete { .. } => {}
                DroppedStorageReclaim::Retained { .. } => {
                    Box::pin(db.query(QueryRequest::write(write_batch().var_as(
                        "flushed",
                        g().add_n("DroppedReclaimFixture", Vec::<(&str, PropertyInput)>::new()),
                    ))))
                    .await
                    .expect("the fixture node is written");
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("every dropped generation is retired in time");
}

/// Stages the entry of a generation whose record `transaction` moves to
/// `Dropped`.
///
/// The caller has already deleted, in this or an earlier transaction, every
/// row of the generation that [`ownership::owned_row`] does not match, and
/// has checked [`admitted`] in `transaction`.
pub(crate) fn stage_entry(
    transaction: &impl crate::transaction::Mutation,
    entry: DroppedGeneration,
    value: DroppedGenerationValue,
) -> Result<()> {
    failpoints::trip(IndexOutboxFailpoint::DroppedEntryStagingBefore)?;
    transaction.put(
        entry.key(),
        encode_metadata_value(&IndexV2MetadataValue::DroppedGeneration(value)),
    )?;
    Ok(())
}

/// Loads every entry visible to `read`, in key order.
pub(crate) async fn load_entries(
    read: &(impl DbReadOps + Sync),
) -> Result<Vec<(DroppedGeneration, DroppedGenerationValue)>> {
    let mut rows = read
        .scan_prefix(GlobalKey::logical_prefix(GlobalKind::DroppedGeneration), ..)
        .await?;
    let mut entries = Vec::new();
    while let Some(row) = rows.next().await? {
        let GlobalKey::DroppedGeneration {
            index_id,
            generation,
        } = GlobalKey::parse_from_slice(&row.key)?
        else {
            return Err(HelixDbError::IndexCatalogCorruption(
                "dropped-generation prefix yielded a different typed key".to_string(),
            ));
        };
        let IndexV2MetadataValue::DroppedGeneration(value) = decode_metadata_value(&row.value)?
        else {
            return Err(HelixDbError::IndexCatalogCorruption(
                "dropped-generation key contains another value kind".to_string(),
            ));
        };
        entries.push((
            DroppedGeneration {
                index_id,
                generation,
            },
            value,
        ));
    }
    Ok(entries)
}

/// The first logical index id at or after `from` that no entry names, armed
/// or quarantined: compactions may discard the rows of an id an armed entry
/// names, and an id a quarantined entry names is left to that entry.
///
/// A consistent store has no entry at or above the watermark, so this reads
/// nothing past the first key. Each entry it skips costs one more.
///
/// # Errors
///
/// A failed read, and [`HelixDbError::IdentifierExhausted`] when entries
/// name every id up to the last.
pub(crate) async fn unclaimed_index_id(
    read: &(impl DbReadOps + Sync),
    from: IndexId,
) -> Result<IndexId> {
    let first = from.get().to_be_bytes();
    let mut rows = read
        .scan_prefix(
            GlobalKey::logical_prefix(GlobalKind::DroppedGeneration),
            first.as_slice()..,
        )
        .await?;
    let mut candidate = from;
    // Imperative: the scan ends at the first id no entry names.
    while let Some(row) = rows.next().await? {
        let GlobalKey::DroppedGeneration { index_id, .. } = GlobalKey::parse_from_slice(&row.key)?
        else {
            return Err(HelixDbError::IndexCatalogCorruption(
                "dropped-generation prefix yielded a different typed key".to_string(),
            ));
        };
        match index_id.cmp(&candidate) {
            // Another generation of an id already skipped.
            std::cmp::Ordering::Less => {}
            std::cmp::Ordering::Equal => candidate = candidate.checked_next()?,
            std::cmp::Ordering::Greater => break,
        }
    }
    Ok(candidate)
}

/// The first generation of `index_id` at or after `from` that no entry
/// names, armed or quarantined; see [`unclaimed_index_id`].
///
/// # Errors
///
/// A failed read, and [`HelixDbError::IdentifierExhausted`] when entries
/// name every generation up to the last.
pub(crate) async fn unclaimed_generation(
    read: &(impl DbReadOps + Sync),
    index_id: IndexId,
    from: IndexGenerationId,
) -> Result<IndexGenerationId> {
    let prefix = [
        &GlobalKey::logical_prefix(GlobalKind::DroppedGeneration)[..],
        &index_id.get().to_be_bytes(),
    ]
    .concat();
    let first = from.get().to_be_bytes();
    let mut rows = read.scan_prefix(prefix, first.as_slice()..).await?;
    let mut candidate = from;
    // Imperative: the scan ends at the first generation no entry names.
    while let Some(row) = rows.next().await? {
        let GlobalKey::DroppedGeneration { generation, .. } =
            GlobalKey::parse_from_slice(&row.key)?
        else {
            return Err(HelixDbError::IndexCatalogCorruption(
                "dropped-generation prefix yielded a different typed key".to_string(),
            ));
        };
        if generation != candidate {
            break;
        }
        candidate = candidate.checked_next()?;
    }
    Ok(candidate)
}

/// What in the catalog contradicts a dropped-generation entry.
///
/// ```
/// use db::DroppedEntryContradiction;
///
/// assert_eq!(
///     DroppedEntryContradiction::GenerationStillOwned.to_string(),
///     "names a generation its index record still owns",
/// );
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DroppedEntryContradiction {
    /// The logical index id was never allocated.
    IndexAboveWatermark,
    /// A vector physical index id was never allocated.
    PhysicalIndexAboveWatermark,
    /// A partition of another generation maps to one of its vector physical
    /// indexes.
    PhysicalIndexMappedByAnotherGeneration,
    /// Its scope has no record of the index.
    IndexWithoutRecord,
    /// The index is of another family than the entry's rows.
    AnotherFamily,
    /// The generation is building, active, aborting or dropping.
    GenerationStillOwned,
    /// The index has not reached the generation.
    GenerationNotReached,
    /// A live generation owns one of its vector physical indexes.
    PhysicalIndexStillOwned,
    /// One of its vector physical indexes has a metadata row, which the drop
    /// deleted: the id is in use again.
    PhysicalIndexDescribed,
    /// Among its partition mappings is one of another generation.
    MappingOfAnotherGeneration,
    /// Two of its partitions map to one vector physical index.
    PhysicalIndexMappedTwice,
}

impl std::fmt::Display for DroppedEntryContradiction {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::IndexAboveWatermark => "names an index id above the watermark",
            Self::PhysicalIndexAboveWatermark => {
                "names a vector physical index id above the watermark"
            }
            Self::PhysicalIndexMappedByAnotherGeneration => {
                "names a vector physical index another generation's partition maps to"
            }
            Self::IndexWithoutRecord => "names an index its scope has no record of",
            Self::AnotherFamily => "names an index of another family",
            Self::GenerationStillOwned => "names a generation its index record still owns",
            Self::GenerationNotReached => "names a generation its index has not reached",
            Self::PhysicalIndexStillOwned => {
                "names a vector physical index an index record still owns"
            }
            Self::PhysicalIndexDescribed => "names a vector physical index that has a metadata row",
            Self::MappingOfAnotherGeneration => "has a partition mapping of another generation",
            Self::PhysicalIndexMappedTwice => {
                "has two partitions mapped to one vector physical index"
            }
        })
    }
}

use DroppedEntryContradiction as Contradiction;

/// What the catalog records of one index, as far as an entry is checked
/// against it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RecordFacts {
    family: IndexDefinitionFamily,
    /// The generation the record's state names, live or not.
    generation: IndexGenerationId,
    /// The generation that is building, active, aborting or dropping.
    live: Option<IndexGenerationId>,
}

/// Which generations map a partition to one vector physical id.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MappedBy {
    One(DroppedGeneration),
    Several,
}

/// What the cross-check reads of one scope, once for every entry that names
/// the scope.
#[derive(Debug, Default)]
struct ScopeFacts {
    /// Every index record of the scope. Records are never deleted, so a
    /// dropped generation's own record is among them.
    records: HashMap<IndexId, RecordFacts>,
    /// The vector physical ids a live unpartitioned generation owns.
    owned: HashSet<VectorPhysicalIndexId>,
    /// The generations whose partition mappings name a physical id, for
    /// every physical id an entry under check names.
    mapped: HashMap<VectorPhysicalIndexId, MappedBy>,
}

impl ScopeFacts {
    /// Reads the index records of `scope` and, when entries name the vector
    /// physical ids `named`, the partition mappings that map to one of them.
    async fn load(
        read: &(impl DbReadOps + Sync),
        scope: DataScope,
        named: &HashSet<VectorPhysicalIndexId>,
    ) -> Result<Self> {
        let mut facts = Self::default();
        let mut records = read
            .scan_prefix(
                ManagedIndexKey::data_prefix(
                    scope,
                    ScopedKey::logical_prefix(RecordKind::IndexRecord),
                ),
                ..,
            )
            .await?;
        while let Some(row) = records.next().await? {
            let record = decode_index_record(&row.value)?;
            let live = record.state().physical();
            facts.records.insert(
                record.index_id(),
                RecordFacts {
                    family: record.definition().family(),
                    generation: record.state().generation(),
                    live: live.map(|physical| physical.generation()),
                },
            );
            facts.owned.extend(live.and_then(|physical| match physical {
                PhysicalGeneration::Vector { layout, .. } => layout.physical_index_id(),
                PhysicalGeneration::Secondary { .. } | PhysicalGeneration::Text { .. } => None,
            }));
        }
        if named.is_empty() {
            return Ok(facts);
        }
        // A partitioned generation's physical ids are in its mappings, not
        // in its index record.
        let mut mappings = read
            .scan_prefix(
                ManagedIndexKey::data_prefix(
                    scope,
                    ScopedKey::logical_prefix(RecordKind::VectorPartitionMapping),
                ),
                ..,
            )
            .await?;
        while let Some(row) = mappings.next().await? {
            let mapping = super::expect_typed_value(
                decode_partition_mapping(&row.value),
                "vector partition mapping key contains another value kind",
            )?;
            if !named.contains(&mapping.physical_index_id) {
                continue;
            }
            let by = DroppedGeneration {
                index_id: mapping.index_id,
                generation: mapping.generation,
            };
            facts
                .mapped
                .entry(mapping.physical_index_id)
                .and_modify(|mapped| {
                    if *mapped != MappedBy::One(by) {
                        *mapped = MappedBy::Several;
                    }
                })
                .or_insert(MappedBy::One(by));
        }
        Ok(facts)
    }

    /// Checks `entry`, whose rows are of `family` and whose vector physical
    /// ids are `physical_index_ids`, against these facts and the id
    /// watermarks.
    fn check(
        &self,
        entry: DroppedGeneration,
        family: IndexDefinitionFamily,
        physical_index_ids: &[VectorPhysicalIndexId],
        next_physical_index_id: Option<VectorPhysicalIndexId>,
    ) -> std::result::Result<(), Contradiction> {
        if physical_index_ids
            .iter()
            .any(|id| next_physical_index_id.is_none_or(|next_id| *id >= next_id))
        {
            return Err(Contradiction::PhysicalIndexAboveWatermark);
        }
        if physical_index_ids.iter().any(|id| {
            self.mapped
                .get(id)
                .is_some_and(|mapped| *mapped != MappedBy::One(entry))
        }) {
            return Err(Contradiction::PhysicalIndexMappedByAnotherGeneration);
        }
        // Index records are never deleted, so the drop's own record is there.
        let Some(record) = self.records.get(&entry.index_id) else {
            return Err(Contradiction::IndexWithoutRecord);
        };
        if record.family != family {
            return Err(Contradiction::AnotherFamily);
        }
        // A recreated index builds a strictly later generation.
        if record.live.is_some_and(|live| live <= entry.generation) {
            return Err(Contradiction::GenerationStillOwned);
        }
        if record.generation < entry.generation {
            return Err(Contradiction::GenerationNotReached);
        }
        if physical_index_ids.iter().any(|id| self.owned.contains(id)) {
            return Err(Contradiction::PhysicalIndexStillOwned);
        }
        Ok(())
    }
}

/// Cross-checks `entries` against the catalog and the id watermarks as `read`
/// sees them. Returns, for each, every owner whose rows the entry gives up,
/// or what contradicts the entry.
///
/// The cost is linear in `entries` and in the catalog: each scope's index
/// records and partition mappings are read once however many entries name
/// the scope. An error is a failed read and says nothing about any entry.
async fn cross_check(
    read: &(impl DbReadOps + Sync),
    entries: Vec<(DroppedGeneration, DroppedGenerationValue)>,
) -> Result<
    Vec<(
        DroppedGeneration,
        DroppedGenerationValue,
        std::result::Result<Vec<OwnerKey>, Contradiction>,
    )>,
> {
    if entries.is_empty() {
        return Ok(Vec::new());
    }
    let next_index_id = repository::load_logical_index_watermark(read)
        .await?
        .next_id;
    // Imperative: each entry may await the scan of its own mappings.
    let mut named = Vec::with_capacity(entries.len());
    for (entry, value) in entries {
        let (family, physical_index_ids) = match value.rows {
            DroppedGenerationRows::UnpartitionedVector { physical_index_id } => {
                (IndexDefinitionFamily::Vector, Ok(vec![physical_index_id]))
            }
            DroppedGenerationRows::PartitionedVector => (
                IndexDefinitionFamily::Vector,
                partition_physical_index_ids(read, value.scope, entry).await?,
            ),
            DroppedGenerationRows::Secondary => (IndexDefinitionFamily::Secondary, Ok(Vec::new())),
            DroppedGenerationRows::Text => (IndexDefinitionFamily::Text, Ok(Vec::new())),
        };
        named.push((entry, value, family, physical_index_ids));
    }
    let mut named_by_scope = HashMap::<DataScope, HashSet<VectorPhysicalIndexId>>::new();
    for (_, value, _, physical_index_ids) in &named {
        named_by_scope
            .entry(value.scope)
            .or_default()
            .extend(physical_index_ids.iter().flatten().copied());
    }
    let next_physical_index_id = if named_by_scope.values().all(HashSet::is_empty) {
        None
    } else {
        Some(
            repository::load_vector_physical_watermark(read)
                .await?
                .next_id,
        )
    };
    let mut scopes = HashMap::with_capacity(named_by_scope.len());
    for (scope, named) in &named_by_scope {
        scopes.insert(*scope, ScopeFacts::load(read, *scope, named).await?);
    }
    // Imperative: each entry that passed awaits one read per physical id.
    let mut checked = Vec::with_capacity(named.len());
    for (entry, value, family, physical_index_ids) in named {
        let verdict = if entry.index_id >= next_index_id {
            Err(Contradiction::IndexAboveWatermark)
        } else {
            physical_index_ids.and_then(|physical_index_ids| {
                scopes[&value.scope]
                    .check(entry, family, &physical_index_ids, next_physical_index_id)
                    .map(|()| physical_index_ids)
            })
        };
        let verdict = match verdict {
            Ok(physical_index_ids) => {
                // The drop deleted these rows itself, so one that exists
                // belongs to a namespace created after it.
                let mut described = false;
                for physical_index_id in &physical_index_ids {
                    let metadata = DataKey::Data {
                        scope: value.scope,
                        kind: DataKeyKind::Vector(VectorKey::IndexMetadata(
                            VectorIndexMetadataKey::new(physical_index_id.get()),
                        )),
                    }
                    .to_bytes();
                    described = described || read.get(metadata).await?.is_some();
                }
                if described {
                    Err(Contradiction::PhysicalIndexDescribed)
                } else {
                    Ok(std::iter::once(OwnerKey::Generation {
                        scope: value.scope,
                        index_id: entry.index_id.get(),
                        generation: entry.generation.get(),
                    })
                    .chain(physical_index_ids.into_iter().map(|physical_index_id| {
                        OwnerKey::PhysicalIndex {
                            scope: value.scope,
                            physical_index_id: physical_index_id.get(),
                        }
                    }))
                    .collect())
                }
            }
            Err(contradiction) => Err(contradiction),
        };
        checked.push((entry, value, verdict));
    }
    Ok(checked)
}

/// The physical ids of a dropped partitioned vector generation, from the
/// partition mappings its drop left in place, or what in them contradicts
/// the entry.
async fn partition_physical_index_ids(
    read: &(impl DbReadOps + Sync),
    scope: DataScope,
    entry: DroppedGeneration,
) -> Result<std::result::Result<Vec<VectorPhysicalIndexId>, Contradiction>> {
    let mut rows = read
        .scan_prefix(partition_mappings_prefix(scope, entry), ..)
        .await?;
    let mut physical_index_ids = HashSet::new();
    while let Some(row) = rows.next().await? {
        let mapping = super::expect_typed_value(
            decode_partition_mapping(&row.value),
            "vector partition mapping key contains another value kind",
        )?;
        if mapping.index_id != entry.index_id || mapping.generation != entry.generation {
            return Ok(Err(Contradiction::MappingOfAnotherGeneration));
        }
        if !physical_index_ids.insert(mapping.physical_index_id) {
            return Ok(Err(Contradiction::PhysicalIndexMappedTwice));
        }
    }
    Ok(Ok(physical_index_ids.into_iter().collect()))
}

/// The prefix of every partition mapping of one generation.
fn partition_mappings_prefix(scope: DataScope, entry: DroppedGeneration) -> Bytes {
    ManagedIndexKey::data_prefix(
        scope,
        ScopedKey::generation_prefix(
            RecordKind::VectorPartitionMapping,
            entry.index_id,
            entry.generation,
        ),
    )
}

/// One table of the store, by the id of its file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct TableId(pub(crate) u128);

/// One compaction of the store, by the id of its `.compactions` record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct CompactionId(pub(crate) u128);

/// A set with at least one member: what a proof still waits for. A proof
/// that waits for nothing is a different state, never an empty set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Waiting<T: Eq + std::hash::Hash>(HashSet<T>);

impl<T: Eq + std::hash::Hash> Waiting<T> {
    /// `members`, unless there is none.
    pub(crate) fn new(members: HashSet<T>) -> Option<Self> {
        (!members.is_empty()).then_some(Self(members))
    }

    /// The members; never empty.
    pub(crate) fn members(&self) -> &HashSet<T> {
        &self.0
    }
}

/// The tables a proof still waits for, each with an estimate of the stored
/// bytes of the entry's rows in it. Never empty: a proof that waits for no
/// table is complete.
///
/// The estimate is what [`retire`] read of the table when the proof found
/// it: exact to one block at each end of each owner's rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Tracked(HashMap<TableId, u64>);

impl Tracked {
    /// `tables`, unless there is none.
    pub(crate) fn new(tables: HashMap<TableId, u64>) -> Option<Self> {
        (!tables.is_empty()).then_some(Self(tables))
    }

    /// The tables and the estimated bytes of the entry's rows in each; never
    /// empty.
    pub(crate) fn tables(&self) -> &HashMap<TableId, u64> {
        &self.0
    }

    /// Those of the tables that are among `stored`, unless none is.
    pub(crate) fn still(&self, stored: &HashSet<TableId>) -> Option<Self> {
        Self::new(
            self.0
                .iter()
                .filter(|(table, _)| stored.contains(table))
                .map(|(table, bytes)| (*table, *bytes))
                .collect(),
        )
    }
}

/// How far [`retire`] has got in proving that no table holds a row of one
/// armed entry. The steps and why each is sound are in that module.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Proof {
    /// Nothing is known: the entry was just armed, or a compaction kept its
    /// rows since.
    Unproven,
    /// These compactions were in flight when the proof started, so they may
    /// write rows a filter of theirs kept before the entry was armed.
    Draining { compactions: Waiting<CompactionId> },
    /// These tables held rows of the entry once every compaction above had
    /// finished. Every later table is free of them.
    Tracking { tables: Tracked },
}

/// An entry this writer discards rows for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Armed {
    /// Sequence of the snapshot the entry was read through; at or after the
    /// entry's commit.
    pub(crate) gate: u64,
    /// Scope and row layout the entry records.
    pub(crate) value: DroppedGenerationValue,
    /// Every owner whose rows the entry gives up.
    pub(crate) owners: Vec<OwnerKey>,
    /// Filters created since the entry was armed that kept its rows, because
    /// their job's retention sequence was below the gate. Each one restarts
    /// the proof, and a proof step applies only while this is unchanged.
    pub(crate) kept: u64,
    /// How far the proof that its rows are gone has got.
    pub(crate) proof: Proof,
    /// When this writer armed the entry.
    pub(crate) since: Instant,
    /// When the proof last moved forward: the entry was armed, its proof
    /// took a step, or a table it tracks was compacted away.
    pub(crate) progressed: Instant,
}

/// An entry that failed its cross-check. It holds no owner, so no filter can
/// discard a row for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Quarantined {
    /// Scope and row layout the entry records.
    pub(crate) value: DroppedGenerationValue,
    /// What in the catalog contradicts the entry.
    pub(crate) contradiction: Contradiction,
    /// When this writer first quarantined the entry.
    pub(crate) since: Instant,
}

/// What a writer does with one stored entry.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Entry {
    /// Compactions discard its rows.
    Armed(Armed),
    /// Compactions keep its rows.
    Quarantined(Quarantined),
}

impl Entry {
    /// Whether the entry's proof tracks tables.
    const fn tracks(&self) -> bool {
        match self {
            Self::Armed(Armed {
                proof: Proof::Tracking { .. },
                ..
            }) => true,
            Self::Armed(Armed {
                proof: Proof::Unproven | Proof::Draining { .. },
                ..
            })
            | Self::Quarantined(_) => false,
        }
    }
}

/// The rows a dropped-generation entry records as given up.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DroppedEntryRows {
    /// A secondary index generation.
    Secondary,
    /// A text index generation.
    Text,
    /// An unpartitioned vector index generation stored under this vector
    /// physical index id.
    UnpartitionedVector {
        /// The vector physical index id the entry names.
        physical_index_id: u64,
    },
    /// A partitioned vector index generation, whose partition mappings list
    /// its vector physical index ids.
    PartitionedVector,
}

impl std::fmt::Display for DroppedEntryRows {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Secondary => formatter.write_str("secondary"),
            Self::Text => formatter.write_str("text"),
            Self::UnpartitionedVector { physical_index_id } => {
                write!(formatter, "vector, physical index {physical_index_id}")
            }
            Self::PartitionedVector => formatter.write_str("partitioned vector"),
        }
    }
}

impl From<DroppedGenerationRows> for DroppedEntryRows {
    fn from(rows: DroppedGenerationRows) -> Self {
        match rows {
            DroppedGenerationRows::Secondary => Self::Secondary,
            DroppedGenerationRows::Text => Self::Text,
            DroppedGenerationRows::UnpartitionedVector { physical_index_id } => {
                Self::UnpartitionedVector {
                    physical_index_id: physical_index_id.get(),
                }
            }
            DroppedGenerationRows::PartitionedVector => Self::PartitionedVector,
        }
    }
}

/// A dropped index generation whose entry the catalog contradicts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuarantinedDroppedGeneration {
    /// The logical index the entry names.
    pub index_id: u64,
    /// The generation of that index the entry names.
    pub generation: u64,
    /// The data scope the entry records.
    pub scope: DataScope,
    /// The rows the entry records as given up. They are kept.
    pub rows: DroppedEntryRows,
    /// What in the catalog contradicts the entry.
    pub contradiction: DroppedEntryContradiction,
    /// When this writer first found the entry contradicted. A writer that
    /// opens starts the clock again.
    pub since: Instant,
}

/// How a writer's last background pass over its dropped-generation entries
/// ended.
///
/// ```
/// use std::time::Instant;
///
/// use db::DroppedStoragePass;
///
/// let now = Instant::now();
/// let failed = DroppedStoragePass::Failed {
///     at: now,
///     since: now,
///     error: "object store unavailable".to_string(),
/// };
/// assert_ne!(failed, DroppedStoragePass::Completed { at: now });
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DroppedStoragePass {
    /// It read the store.
    Completed {
        /// When it ended.
        at: Instant,
    },
    /// It failed. The writer tries again by itself, at growing intervals.
    Failed {
        /// When it ended.
        at: Instant,
        /// When the first of the passes that have failed in a row ended.
        since: Instant,
        /// What it failed with.
        error: String,
    },
}

/// What a writer's own compactor does for dropped index generations, which
/// follows from how many compactions it runs at a time in its process.
///
/// ```
/// use db::DroppedStorageCompactor;
///
/// assert_eq!(DroppedStorageCompactor::of(None), DroppedStorageCompactor::None);
/// assert_eq!(DroppedStorageCompactor::of(Some(1)), DroppedStorageCompactor::SingleSlot);
/// assert_eq!(DroppedStorageCompactor::of(Some(4)), DroppedStorageCompactor::Reclaims);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DroppedStorageCompactor {
    /// Several compactions at a time: the writer arms entries, its
    /// compactions discard their rows, and it rewrites the sorted runs that
    /// hold them by itself once it has been idle.
    #[default]
    Reclaims,
    /// One compaction at a time: the writer arms entries and its ordinary
    /// compactions discard their rows, but it never rewrites a sorted run
    /// for them, by itself or on request: that would take its only slot.
    SingleSlot,
    /// No compaction worker in this process: the writer arms no entry,
    /// discards no row and counts nothing. The entries its drops leave wait
    /// for a writer that compacts in process.
    None,
}

impl DroppedStorageCompactor {
    /// The kind of a compactor that runs `slots` compactions at a time in
    /// the writer's process; `None` when it runs none there.
    pub const fn of(slots: Option<usize>) -> Self {
        match slots {
            None => Self::None,
            Some(0 | 1) => Self::SingleSlot,
            Some(_) => Self::Reclaims,
        }
    }
}

/// What a writer knows of the dropped index generations whose entries are
/// still stored, as of its last look. Every count is of generations unless
/// it says otherwise.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DroppedIndexStorage {
    /// What this writer's compactor does for dropped generations. A writer
    /// whose compactor is [`DroppedStorageCompactor::None`] reports nothing
    /// else: it never looks.
    pub compactor: DroppedStorageCompactor,
    /// Armed, with no proof under way: just dropped, or a compaction kept
    /// their rows since.
    pub unproven: usize,
    /// Of `unproven`, those whose rows a compaction has kept since they were
    /// armed, because its retention sequence was behind the drop: a snapshot
    /// or transaction older than the drop was open, or the compaction had
    /// been planned before the drop.
    pub retained: usize,
    /// Armed, waiting for the compactions that were in flight when their
    /// proof started.
    pub draining: usize,
    /// Armed, waiting for the tables that held their rows to be compacted.
    pub tracking: usize,
    /// Distinct tables the `tracking` generations wait for.
    pub tracked_tables: usize,
    /// An estimate of the stored bytes of the `tracking` generations' rows
    /// in those tables: about what the store gets back when they are
    /// compacted. It is summed generation by generation, so a block two of
    /// them have rows in counts twice, and it can exceed the tables' size.
    /// Rows of generations not yet `tracking` are not counted.
    pub tracked_bytes: u64,
    /// The part of that the writer leaves where it is; `None` when a reclaim
    /// would rewrite every table the generations wait for.
    pub left_in_place: Option<reclaim::LeftInPlace>,
    /// When this writer armed the armed generation it has waited on longest.
    /// A writer that opens arms every stored entry again.
    pub waiting_since: Option<Instant>,
    /// When the proof last moved for the generation, among those not yet
    /// `tracking`, whose proof has stood still longest. Such a proof waits
    /// for a pass, for compactions in flight, or for a reader older than the
    /// drop to close, each of which should end by itself.
    pub stalled_since: Option<Instant>,
    /// When the proof last moved for the `tracking` generation whose proof
    /// has stood still longest: how long its rows have waited for a
    /// compaction to reach them.
    pub tracking_since: Option<Instant>,
    /// How this writer's last background pass ended; `None` before its
    /// first, which arms the entries the store holds, and always on a writer
    /// that runs none.
    pub last_pass: Option<DroppedStoragePass>,
    /// Entries the catalog contradicts. Their rows are kept.
    pub quarantined: Vec<QuarantinedDroppedGeneration>,
}

impl DroppedIndexStorage {
    /// Every dropped generation whose entry is still stored.
    pub fn entries(&self) -> usize {
        self.unproven + self.draining + self.tracking + self.quarantined.len()
    }
}

/// What the compactions of this process have done, as far as the filters
/// they created tell: enough to know, without reading object storage, that
/// the compactor has had nothing to do.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct CompactionActivity {
    /// Filters created and filters dropped, in total. Unchanged between two
    /// reads means no compaction job started or ended in between.
    pub(crate) events: u64,
    /// Filters alive: zero when no compaction job is running.
    pub(crate) running: usize,
}

/// What [`Registry::remove_quarantined`] found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Unquarantined {
    /// The entry was quarantined and is forgotten.
    Removed,
    /// The entry is armed.
    Armed,
    /// The registry knows no such entry.
    Absent,
}

#[derive(Debug, Default)]
struct State {
    entries: HashMap<DroppedGeneration, Entry>,
    /// How many of `entries` have a proof that tracks tables. Kept beside
    /// them so that asking whether any does, which the background task does
    /// at every write, reads one number.
    tracking: usize,
    compactions: CompactionActivity,
    /// How the last background pass ended.
    last_pass: Option<DroppedStoragePass>,
    /// What the writer's compactor does; see [`Registry::compacts`].
    compactor: DroppedStorageCompactor,
}

/// The dropped generations one writer knows of, and what its compactions do
/// with each.
///
/// Shared by the compaction filter supplier, which reads it once per filter,
/// and [`retire`], which arms entries and removes the ones it retires.
#[derive(Debug, Default)]
pub(crate) struct Registry {
    state: parking_lot::Mutex<State>,
    /// Wakes the retirer; see [`Self::changed`].
    changed: tokio::sync::Notify,
}

impl Registry {
    /// Cross-checks every stored entry that is not armed, arms the ones that
    /// pass and quarantines the rest.
    ///
    /// Entries are read, cross-checked and gated through one snapshot, and
    /// armed only after a memtable flush that follows it; see the module
    /// contract. A quarantined entry is checked again by every refresh, so
    /// one that a later commit makes consistent is armed then.
    ///
    /// # Errors
    ///
    /// A failed read or flush. Nothing is armed or quarantined then.
    pub(crate) async fn refresh(&self, db: &Db) -> Result<()> {
        let snapshot = db.snapshot().await?;
        let gate = snapshot.seq();
        let stored = load_entries(snapshot.as_ref()).await?;
        let unarmed = {
            let state = self.state.lock();
            stored
                .into_iter()
                .filter(|(entry, _)| !matches!(state.entries.get(entry), Some(Entry::Armed(_))))
                .collect::<Vec<_>>()
        };
        let checked = cross_check(snapshot.as_ref(), unarmed).await?;
        drop(snapshot);
        if checked.iter().any(|(_, _, verdict)| verdict.is_ok()) {
            failpoints::trip(IndexOutboxFailpoint::DroppedEntryArmBefore)?;
            db.flush_with_options(FlushOptions {
                flush_type: FlushType::MemTable,
            })
            .await?;
        }
        let now = Instant::now();
        let mut state = self.state.lock();
        let mut armed = 0usize;
        for (entry, value, verdict) in checked {
            let previous = state.entries.get(&entry);
            let next = match verdict {
                Ok(owners) => {
                    armed += 1;
                    Entry::Armed(Armed {
                        gate,
                        value,
                        owners,
                        kept: 0,
                        proof: Proof::Unproven,
                        since: now,
                        progressed: now,
                    })
                }
                Err(contradiction) => Entry::Quarantined(Quarantined {
                    value,
                    contradiction,
                    // The clock of an entry that stays quarantined runs on.
                    since: match previous {
                        Some(Entry::Quarantined(quarantined)) => quarantined.since,
                        Some(Entry::Armed(_)) | None => now,
                    },
                }),
            };
            match (&next, previous) {
                (Entry::Quarantined(quarantined), previous) if previous != Some(&next) => {
                    tracing::error!(
                        index_id = entry.index_id.get(),
                        generation = entry.generation.get(),
                        contradiction = %quarantined.contradiction,
                        "quarantined a dropped-generation entry: compactions keep its rows"
                    );
                }
                (Entry::Armed(_), Some(Entry::Quarantined(_))) => {
                    tracing::info!(
                        index_id = entry.index_id.get(),
                        generation = entry.generation.get(),
                        "a quarantined dropped-generation entry is consistent again"
                    );
                }
                (Entry::Armed(_) | Entry::Quarantined(_), _) => {}
            }
            // What a refresh puts in tracks no table.
            let replaced = state.entries.insert(entry, next);
            state.tracking -= usize::from(replaced.as_ref().is_some_and(Entry::tracks));
        }
        if armed > 0 {
            tracing::info!(entries = armed, gate, "armed dropped-generation entries");
        }
        Ok(())
    }

    /// Every armed entry.
    pub(crate) fn armed(&self) -> Vec<(DroppedGeneration, Armed)> {
        self.state
            .lock()
            .entries
            .iter()
            .filter_map(|(entry, state)| match state {
                Entry::Armed(armed) => Some((*entry, armed.clone())),
                Entry::Quarantined(_) => None,
            })
            .collect()
    }

    /// Every entry this writer knows of, counted by what it waits for.
    /// [`DroppedIndexStorage::left_in_place`] is not filled in: it takes the
    /// writer's manifest and a rule, which [`reclaim::storage`] has.
    pub(crate) fn storage(&self) -> DroppedIndexStorage {
        let state = self.state.lock();
        let mut storage = DroppedIndexStorage {
            compactor: state.compactor,
            last_pass: state.last_pass.clone(),
            ..DroppedIndexStorage::default()
        };
        let mut tracked = HashSet::new();
        let oldest = |oldest: Option<Instant>, instant: Instant| {
            Some(oldest.map_or(instant, |oldest| oldest.min(instant)))
        };
        for (entry, state) in &state.entries {
            match state {
                Entry::Armed(armed) => {
                    storage.waiting_since = oldest(storage.waiting_since, armed.since);
                    match &armed.proof {
                        Proof::Unproven => {
                            storage.unproven += 1;
                            storage.retained += usize::from(armed.kept > 0);
                            storage.stalled_since = oldest(storage.stalled_since, armed.progressed);
                        }
                        Proof::Draining { .. } => {
                            storage.draining += 1;
                            storage.stalled_since = oldest(storage.stalled_since, armed.progressed);
                        }
                        Proof::Tracking { tables } => {
                            storage.tracking += 1;
                            storage.tracked_bytes += tables.tables().values().sum::<u64>();
                            tracked.extend(tables.tables().keys().copied());
                            storage.tracking_since =
                                oldest(storage.tracking_since, armed.progressed);
                        }
                    }
                }
                Entry::Quarantined(quarantined) => {
                    storage.quarantined.push(QuarantinedDroppedGeneration {
                        index_id: entry.index_id.get(),
                        generation: entry.generation.get(),
                        scope: quarantined.value.scope,
                        rows: quarantined.value.rows.into(),
                        contradiction: quarantined.contradiction,
                        since: quarantined.since,
                    });
                }
            }
        }
        storage.tracked_tables = tracked.len();
        storage
            .quarantined
            .sort_unstable_by_key(|entry| (entry.index_id, entry.generation));
        assert_eq!(
            state.tracking, storage.tracking,
            "the count of tracking proofs follows every change of a proof"
        );
        storage
    }

    /// Records what the writer's compactor does for dropped generations.
    /// The open that knows the compactor's settings says so once.
    pub(crate) fn compacts(&self, compactor: DroppedStorageCompactor) {
        self.state.lock().compactor = compactor;
    }

    /// Whether some armed entry's proof tracks tables: only then does it
    /// matter to a proof which tables the manifest holds. Reads one number,
    /// however many entries there are.
    pub(crate) fn tracks_tables(&self) -> bool {
        self.state.lock().tracking > 0
    }

    /// The estimated bytes of dropped rows in each table a proof tracks,
    /// for the entries whose gate is at or below `retention`: the rows a
    /// compaction planned now would discard.
    pub(crate) fn discardable_tracked(&self, retention: u64) -> HashMap<TableId, u64> {
        self.state
            .lock()
            .entries
            .values()
            .filter_map(|state| match state {
                Entry::Armed(Armed {
                    gate,
                    proof: Proof::Tracking { tables },
                    ..
                }) if *gate <= retention => Some(tables),
                Entry::Armed(_) | Entry::Quarantined(_) => None,
            })
            .flat_map(|tables| tables.tables())
            .fold(HashMap::new(), |mut tracked, (table, bytes)| {
                *tracked.entry(*table).or_default() += bytes;
                tracked
            })
    }

    /// Whether a table some armed entry's proof tracks is missing from
    /// `stored`: only then can a look at the store retire a tracked entry.
    pub(crate) fn tracked_table_left(&self, stored: &HashSet<TableId>) -> bool {
        self.state.lock().entries.values().any(|state| match state {
            Entry::Armed(Armed {
                proof: Proof::Tracking { tables },
                ..
            }) => !tables.tables().keys().all(|table| stored.contains(table)),
            Entry::Armed(Armed {
                proof: Proof::Unproven | Proof::Draining { .. },
                ..
            })
            | Entry::Quarantined(_) => false,
        })
    }

    /// The owners whose rows one compaction job may discard, decided once for
    /// the filter being created: every armed entry whose gate the job's
    /// `retention_min_seq` has reached. A job that reports no retention
    /// sequence discards nothing.
    ///
    /// An armed entry whose rows the filter keeps loses its proof: the job
    /// may write those rows into a table no proof step has seen. The caller
    /// owes one [`Self::filter_dropped`] for the filter it creates.
    fn discardable(&self, context: &CompactionJobContext) -> HashSet<OwnerKey> {
        let mut state = self.state.lock();
        state.compactions.events += 1;
        state.compactions.running += 1;
        let mut discardable = HashSet::new();
        let mut kept = false;
        let State {
            entries, tracking, ..
        } = &mut *state;
        for entry in entries.values_mut() {
            let tracked = entry.tracks();
            let Entry::Armed(armed) = entry else {
                continue;
            };
            if context
                .retention_min_seq
                .is_some_and(|retention| armed.gate <= retention)
            {
                discardable.extend(armed.owners.iter().copied());
            } else {
                armed.kept += 1;
                armed.proof = Proof::Unproven;
                *tracking -= usize::from(tracked);
                kept = true;
            }
        }
        drop(state);
        if kept {
            self.changed.notify_one();
        }
        discardable
    }

    /// Records that a filter [`Self::discardable`] was called for is gone:
    /// its job ended, or was abandoned.
    fn filter_dropped(&self) {
        let mut state = self.state.lock();
        state.compactions.events += 1;
        debug_assert!(
            state.compactions.running > 0,
            "every dropped filter was created through this registry"
        );
        // Saturating: this runs as a filter is dropped, where a panic
        // during an unwind would abort the process.
        state.compactions.running = state.compactions.running.saturating_sub(1);
        // A proof that tracks tables is told when one leaves the manifest;
        // any other may wait for the compaction that just ended.
        let waiting = state.entries.values().any(|state| match state {
            Entry::Armed(Armed {
                proof: Proof::Unproven | Proof::Draining { .. },
                ..
            }) => true,
            Entry::Armed(Armed {
                proof: Proof::Tracking { .. },
                ..
            })
            | Entry::Quarantined(_) => false,
        });
        drop(state);
        if waiting {
            self.changed.notify_one();
        }
    }

    /// What this process's compactions have done so far.
    pub(crate) fn compactions(&self) -> CompactionActivity {
        self.state.lock().compactions
    }

    /// Tells the retirer that a stored entry or the catalog it is checked
    /// against may have changed: an index operation committed its last
    /// step. [`outbox`](super::outbox) calls this from the one function
    /// that commits a step, after the commit that may have stored an entry.
    pub(crate) fn operation_committed(&self) {
        self.changed.notify_one();
    }

    /// Records how a background pass ended: with `failure`, or by reading
    /// the store.
    pub(crate) fn pass_ended(&self, failure: Option<String>) {
        let at = Instant::now();
        let mut state = self.state.lock();
        state.last_pass = Some(match failure {
            None => DroppedStoragePass::Completed { at },
            Some(error) => DroppedStoragePass::Failed {
                at,
                since: match &state.last_pass {
                    Some(DroppedStoragePass::Failed { since, .. }) => *since,
                    Some(DroppedStoragePass::Completed { .. }) | None => at,
                },
                error,
            },
        });
    }

    /// Completes once something a retirement pass depends on has changed
    /// in this process since the last call completed: an entry may have been
    /// stored, the catalog moved, a filter kept an armed entry's rows, or a
    /// compaction ended while an armed entry's proof was not yet tracking
    /// tables. One waiter at a time.
    pub(crate) async fn changed(&self) {
        self.changed.notified().await;
    }

    /// Records that the proof of `entry` has reached `proof`, unless a filter
    /// kept the entry's rows since the caller read `kept`, or the entry is
    /// no longer armed. Returns whether the step applied.
    pub(crate) fn advance(&self, entry: DroppedGeneration, kept: u64, proof: Proof) -> bool {
        let mut state = self.state.lock();
        let State {
            entries, tracking, ..
        } = &mut *state;
        let Some(Entry::Armed(armed)) = entries.get_mut(&entry) else {
            return false;
        };
        if armed.kept != kept {
            return false;
        }
        if armed.proof != proof {
            let tracks = |proof: &Proof| match proof {
                Proof::Tracking { .. } => 1,
                Proof::Unproven | Proof::Draining { .. } => 0,
            };
            *tracking = *tracking - tracks(&armed.proof) + tracks(&proof);
            armed.proof = proof;
            armed.progressed = Instant::now();
        }
        true
    }

    /// Forgets `entry`, whose rows are proven gone, unless a filter kept the
    /// entry's rows since the caller read `kept`, or the entry is no longer
    /// armed. Returns whether it was forgotten: only then may the caller
    /// delete the stored entry.
    pub(crate) fn retire(&self, entry: DroppedGeneration, kept: u64) -> bool {
        let mut state = self.state.lock();
        let Some(Entry::Armed(armed)) = state.entries.get(&entry) else {
            return false;
        };
        if armed.kept != kept {
            return false;
        }
        let retired = state.entries.remove(&entry);
        state.tracking -= usize::from(retired.as_ref().is_some_and(Entry::tracks));
        true
    }

    /// Forgets `entry` if it is quarantined. Only after
    /// [`Unquarantined::Removed`] may the caller delete the stored entry: an
    /// armed entry is never deleted this way, its rows are being discarded.
    pub(crate) fn remove_quarantined(&self, entry: DroppedGeneration) -> Unquarantined {
        let mut state = self.state.lock();
        match state.entries.get(&entry) {
            Some(Entry::Quarantined(_)) => {
                state.entries.remove(&entry);
                Unquarantined::Removed
            }
            Some(Entry::Armed(_)) => Unquarantined::Armed,
            None => Unquarantined::Absent,
        }
    }
}

#[cfg(test)]
mod tests;
