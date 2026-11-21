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
//! - **Creation.** [`stage_entry`] in the record's `Dropped` transaction is
//!   the only way an entry is created. The entry commits at the sequence the
//!   record does, so a view that still reads the generation's rows is older
//!   than the entry.
//! - **Arming.** A writer discards nothing for an entry until
//!   [`Registry::refresh`] has read it through a snapshot, cross-checked it
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
//!   building, active, aborting or dropping. [`Registry::refresh`] fails for
//!   an entry that does, which fails the open; [`ensure_index_unclaimed`],
//!   [`ensure_generation_unclaimed`] and [`ensure_physical_index_unclaimed`]
//!   fail the allocation of an id an entry names.
//! - **Deletion.** [`reclaim`] deletes an entry only after a compaction of
//!   every sorted table, planned in this process while the entry was armed
//!   and discardable, has completed. A restart forgets that proof and
//!   compacts again.
//!
//! # What a drop still deletes itself
//!
//! - **Unpartitioned vector.** The rows of its physical id that no filter
//!   matches, in one bounded step. The entry records the physical id.
//! - **Partitioned vector.** The same rows of each partition's physical id,
//!   one cleanup step per partition. The partition mappings stay: they are
//!   the only list of the generation's physical ids, so the cross-check and
//!   every filter read them, and [`reclaim`] deletes them with the entry.
//! - **Secondary and text.** Nothing: every row is keyed by the generation.
//!   Text split blobs in object storage are not rows and are retained, as
//!   they were before.
//!
//! Two kinds of cleanup still delete row by row: one that had deleted rows
//! before the store was admitted; and a vector cleanup whose first physical
//! id has more unfiltered rows than one step deletes, which the persisted
//! progress cannot tell from the first. Both finish on an admitted store, so
//! each still leaves an entry, which then names rows that are already gone.
//!
//! A generation adopted from legacy vector storage is never given an entry:
//! its physical id was not allocated by this catalog and its rows may be
//! shared with a legacy index. Its drop deletes every row one by one. The
//! abort of its build deletes the directory and the generation-keyed rows
//! the build added, one by one, and hands the legacy rows back untouched.
//!
//! Whether a drop leaves an entry depends on the store alone, not on the
//! writer that runs it. A writer opened without a compactor still leaves
//! entries and arms nothing that matters: it compacts nothing at all, and the
//! next writer that embeds a compactor discards the rows.
//!
//! # What does not scale
//!
//! Every armed entry costs one hash-set member per owner in each compaction
//! filter; a partitioned vector generation has one owner per partition. A
//! refresh scans the entry prefix and, for each new entry, the index records
//! of its scope; for a new vector entry it also scans the scope's partition
//! mappings and reads one metadata key per physical id. Every entry is new
//! to a writer that opens, so an open costs those scans once per stored
//! entry. Allocating a vector physical id scans every entry. Dropping a
//! partitioned vector generation still costs one cleanup commit per
//! partition, and its mappings stay until the reclaim. Entries leave only
//! through a reclaim, so one whose rows are a small share of the store stays
//! until a larger drop or a request triggers one.

pub(crate) mod filter;
pub(crate) mod ownership;
pub(crate) mod reclaim;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use bytes::Bytes;
use slatedb::compactor::CompactionStatus;
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
/// only the marker; the two versions share every other row. Binaries that
/// support at most version 7 refuse the store from then on: they would
/// neither decode a dropped-generation entry nor discard its rows.
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
    let mut options = settings.compactor_options.clone()?;
    // SlateDB's default, pinned: a trivial move re-links tables into a run
    // without creating a filter, so a reclaim made of one would discard
    // nothing, prove nothing, and repeat for ever.
    options.enable_trivial_move = false;
    Some(
        slatedb::CompactorBuilder::new(path.to_string(), object_store)
            .with_options(options)
            .with_compaction_filter_supplier(Arc::new(filter::DroppedRowsFilterSupplier::new(
                registry,
            ))),
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

fn violation(entry: DroppedGeneration, reason: &str) -> HelixDbError {
    HelixDbError::IndexCatalogCorruption(format!(
        "dropped-generation entry for index {} generation {} {reason}",
        entry.index_id.get(),
        entry.generation.get()
    ))
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

/// Reclaims on request until `db` holds no dropped-generation entry, for
/// tests that count the rows drops leave behind.
///
/// SlateDB publishes its oldest open reader only when it flushes a memtable
/// that holds rows, so a pass that finds the rows retained is followed by
/// the write of one node no index covers. The writer stops reading the
/// discarded rows at its next manifest poll, which may be after this returns.
#[cfg(test)]
pub(crate) async fn reclaim_fixture(db: &crate::HelixDB) {
    use helix_ast::value::PropertyInput;
    use helix_ast::{batch::write_batch, query::QueryRequest, traversal::g};

    use self::reclaim::DroppedStorageReclaim;

    tokio::time::timeout(std::time::Duration::from_secs(60), async {
        loop {
            match db
                .reclaim_dropped_index_storage()
                .await
                .expect("the reclaim pass runs")
            {
                DroppedStorageReclaim::Idle => break,
                // Lost its compaction to one the scheduler picked, or done:
                // the next pass tells.
                DroppedStorageReclaim::Unproven { .. }
                | DroppedStorageReclaim::Reclaimed { .. } => {}
                DroppedStorageReclaim::Retained { .. } => {
                    Box::pin(db.query(QueryRequest::write(write_batch().var_as(
                        "flushed",
                        g().add_n("DroppedReclaimFixture", Vec::<(&str, PropertyInput)>::new()),
                    ))))
                    .await
                    .expect("the fixture node is written");
                }
                outcome @ DroppedStorageReclaim::BelowShare { .. } => {
                    panic!("a requested pass ignores the share: {outcome:?}")
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("every dropped generation is reclaimed in time");
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

/// Fails when an entry names any generation of the freshly allocated
/// `index_id`: compactions would discard the rows the new index writes.
pub(crate) async fn ensure_index_unclaimed(
    read: &(impl DbReadOps + Sync),
    index_id: IndexId,
) -> Result<()> {
    let prefix = [
        &GlobalKey::logical_prefix(GlobalKind::DroppedGeneration)[..],
        &index_id.get().to_be_bytes(),
    ]
    .concat();
    let mut rows = read.scan_prefix(prefix, ..).await?;
    if rows.next().await?.is_some() {
        return Err(HelixDbError::IndexCatalogCorruption(format!(
            "a dropped-generation entry names index {}, which was never allocated",
            index_id.get()
        )));
    }
    Ok(())
}

/// Fails when an entry names the generation a recreated index is about to
/// build.
pub(crate) async fn ensure_generation_unclaimed(
    read: &(impl DbReadOps + Sync),
    entry: DroppedGeneration,
) -> Result<()> {
    if read.get(entry.key()).await?.is_some() {
        return Err(violation(entry, "names a generation that was never built"));
    }
    Ok(())
}

/// Fails when the entry of an unpartitioned vector generation names the
/// freshly allocated `physical_index_id`.
///
/// The physical ids of a dropped partitioned generation are listed by its
/// partition mappings, not by its entry, so this check does not see them:
/// [`Registry::refresh`] bounds them by the watermark instead.
pub(crate) async fn ensure_physical_index_unclaimed(
    read: &(impl DbReadOps + Sync),
    physical_index_id: VectorPhysicalIndexId,
) -> Result<()> {
    let claimed = load_entries(read)
        .await?
        .into_iter()
        .find(|(_, value)| match value.rows {
            DroppedGenerationRows::UnpartitionedVector {
                physical_index_id: dropped,
            } => dropped == physical_index_id,
            DroppedGenerationRows::PartitionedVector
            | DroppedGenerationRows::Secondary
            | DroppedGenerationRows::Text => false,
        });
    let Some((entry, _)) = claimed else {
        return Ok(());
    };
    Err(violation(
        entry,
        "names a vector physical index id that was never allocated",
    ))
}

/// Cross-checks one entry against the catalog and the id watermarks as
/// `read` sees them, and returns every owner whose rows the entry gives up.
async fn cross_checked_owners(
    read: &(impl DbReadOps + Sync),
    entry: DroppedGeneration,
    value: DroppedGenerationValue,
) -> Result<Vec<OwnerKey>> {
    let scope = value.scope;
    if entry.index_id
        >= repository::load_logical_index_watermark(read)
            .await?
            .next_id
    {
        return Err(violation(entry, "names an index id above the watermark"));
    }
    let (family, physical_index_ids) = match value.rows {
        DroppedGenerationRows::UnpartitionedVector { physical_index_id } => (
            IndexDefinitionFamily::Vector,
            HashSet::from([physical_index_id]),
        ),
        DroppedGenerationRows::PartitionedVector => (
            IndexDefinitionFamily::Vector,
            partition_physical_index_ids(read, scope, entry).await?,
        ),
        DroppedGenerationRows::Secondary => (IndexDefinitionFamily::Secondary, HashSet::new()),
        DroppedGenerationRows::Text => (IndexDefinitionFamily::Text, HashSet::new()),
    };
    if !physical_index_ids.is_empty() {
        let next_id = repository::load_vector_physical_watermark(read)
            .await?
            .next_id;
        if physical_index_ids.iter().any(|id| *id >= next_id) {
            return Err(violation(
                entry,
                "names a vector physical index id above the watermark",
            ));
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
            if physical_index_ids.contains(&mapping.physical_index_id)
                && (mapping.index_id != entry.index_id || mapping.generation != entry.generation)
            {
                return Err(violation(
                    entry,
                    "names a vector physical index another generation's partition maps to",
                ));
            }
        }
    }
    // The drop deleted these rows itself, so one that exists belongs to a
    // namespace created after it.
    for physical_index_id in &physical_index_ids {
        let metadata = DataKey::Data {
            scope,
            kind: DataKeyKind::Vector(VectorKey::IndexMetadata(VectorIndexMetadataKey::new(
                physical_index_id.get(),
            ))),
        }
        .to_bytes();
        if read.get(metadata).await?.is_some() {
            return Err(violation(
                entry,
                "names a vector physical index that has a metadata row",
            ));
        }
    }
    let records =
        ManagedIndexKey::data_prefix(scope, ScopedKey::logical_prefix(RecordKind::IndexRecord));
    let mut rows = read.scan_prefix(records, ..).await?;
    let mut recorded = false;
    while let Some(row) = rows.next().await? {
        let record = decode_index_record(&row.value)?;
        let live = record.state().physical();
        if record.index_id() == entry.index_id {
            recorded = true;
            if record.definition().family() != family {
                return Err(violation(entry, "names an index of another family"));
            }
            // A recreated index builds a strictly later generation.
            if live.is_some_and(|physical| physical.generation() <= entry.generation) {
                return Err(violation(
                    entry,
                    "names a generation its index record still owns",
                ));
            }
            if record.state().generation() < entry.generation {
                return Err(violation(
                    entry,
                    "names a generation its index has not reached",
                ));
            }
        }
        let live_physical_index_id = live.and_then(|physical| match physical {
            PhysicalGeneration::Vector { layout, .. } => layout.physical_index_id(),
            PhysicalGeneration::Secondary { .. } | PhysicalGeneration::Text { .. } => None,
        });
        if live_physical_index_id.is_some_and(|id| physical_index_ids.contains(&id)) {
            return Err(violation(
                entry,
                "names a vector physical index an index record still owns",
            ));
        }
    }
    // Index records are never deleted, so the drop's own record is there.
    if !recorded {
        return Err(violation(
            entry,
            "names an index its scope has no record of",
        ));
    }
    Ok(std::iter::once(OwnerKey::Generation {
        scope,
        index_id: entry.index_id.get(),
        generation: entry.generation.get(),
    })
    .chain(
        physical_index_ids
            .into_iter()
            .map(|physical_index_id| OwnerKey::PhysicalIndex {
                scope,
                physical_index_id: physical_index_id.get(),
            }),
    )
    .collect())
}

/// The physical ids of a dropped partitioned vector generation, from the
/// partition mappings its drop left in place.
async fn partition_physical_index_ids(
    read: &(impl DbReadOps + Sync),
    scope: DataScope,
    entry: DroppedGeneration,
) -> Result<HashSet<VectorPhysicalIndexId>> {
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
            return Err(violation(
                entry,
                "has a partition mapping of another generation",
            ));
        }
        if !physical_index_ids.insert(mapping.physical_index_id) {
            return Err(violation(
                entry,
                "has two partitions mapped to one vector physical index",
            ));
        }
    }
    Ok(physical_index_ids)
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
}

/// The filters created for one destination run while a reclaim compaction
/// was awaited, each as the entries it could discard.
#[derive(Debug)]
struct Watch {
    destination: u32,
    filters: Vec<HashSet<DroppedGeneration>>,
}

#[derive(Debug)]
enum State {
    /// Compactions discard the rows of armed entries.
    Filtering {
        armed: HashMap<DroppedGeneration, Armed>,
        watch: Option<Watch>,
    },
    /// A cross-check failed after the open: compactions discard nothing until
    /// the process restarts and the open fails on the same entry.
    Halted,
}

/// The dropped generations one writer's compactions discard rows for.
///
/// Shared by the compaction filter supplier, which reads it once per filter,
/// and the reclaimer, which arms and disarms entries.
#[derive(Debug)]
pub(crate) struct Registry {
    state: parking_lot::Mutex<State>,
}

impl Default for Registry {
    fn default() -> Self {
        Self {
            state: parking_lot::Mutex::new(State::Filtering {
                armed: HashMap::new(),
                watch: None,
            }),
        }
    }
}

impl Registry {
    /// Arms every stored entry that is not armed yet.
    ///
    /// Fails, arming none of the new entries, when one of them names rows a
    /// live generation owns, and from then on the registry discards nothing
    /// and every refresh fails: the entry is wrong until an operator repairs
    /// the store. Entries are read, cross-checked and gated through one
    /// snapshot, and armed only after a memtable flush that follows it; see
    /// the module contract.
    pub(crate) async fn refresh(&self, db: &Db) -> Result<()> {
        let snapshot = db.snapshot().await?;
        let gate = snapshot.seq();
        let stored = load_entries(snapshot.as_ref()).await?;
        let unarmed = match &*self.state.lock() {
            State::Filtering { armed, .. } => stored
                .into_iter()
                .filter(|(entry, _)| !armed.contains_key(entry))
                .collect::<Vec<_>>(),
            State::Halted => {
                return Err(HelixDbError::IndexCatalogCorruption(
                    "a dropped-generation entry failed its cross-check; compactions discard nothing until the store is repaired"
                        .to_string(),
                ))
            }
        };
        if unarmed.is_empty() {
            return Ok(());
        }
        let mut arming = Vec::with_capacity(unarmed.len());
        for (entry, value) in unarmed {
            let owners = cross_checked_owners(snapshot.as_ref(), entry, value)
                .await
                .inspect_err(|error| {
                    // A failed read says nothing about the entry.
                    if matches!(error, HelixDbError::IndexCatalogCorruption(_)) {
                        tracing::error!(%error, "compactions discard nothing from now on");
                        *self.state.lock() = State::Halted;
                    }
                })?;
            arming.push((
                entry,
                Armed {
                    gate,
                    value,
                    owners,
                },
            ));
        }
        drop(snapshot);
        failpoints::trip(IndexOutboxFailpoint::DroppedEntryArmBefore)?;
        db.flush_with_options(FlushOptions {
            flush_type: FlushType::MemTable,
        })
        .await?;
        match &mut *self.state.lock() {
            State::Filtering { armed, .. } => {
                tracing::info!(
                    entries = arming.len(),
                    gate,
                    "armed dropped-generation entries"
                );
                armed.extend(arming);
            }
            State::Halted => {}
        }
        Ok(())
    }

    /// Every armed entry.
    pub(crate) fn armed(&self) -> Vec<(DroppedGeneration, Armed)> {
        match &*self.state.lock() {
            State::Filtering { armed, .. } => armed
                .iter()
                .map(|(entry, armed)| (*entry, armed.clone()))
                .collect(),
            State::Halted => Vec::new(),
        }
    }

    /// The owners whose rows one compaction job may discard, decided once for
    /// the filter being created: every armed entry whose gate the job's
    /// `retention_min_seq` has reached. A job that reports no retention
    /// sequence discards nothing.
    fn discardable(&self, context: &CompactionJobContext) -> HashSet<OwnerKey> {
        let State::Filtering { armed, watch } = &mut *self.state.lock() else {
            return HashSet::new();
        };
        let discardable = armed
            .iter()
            .filter(|(_, armed)| {
                context
                    .retention_min_seq
                    .is_some_and(|retention| armed.gate <= retention)
            })
            .collect::<Vec<_>>();
        match watch {
            Some(watch) if watch.destination == context.destination => watch
                .filters
                .push(discardable.iter().map(|(entry, _)| **entry).collect()),
            Some(_) | None => {}
        }
        discardable
            .into_iter()
            .flat_map(|(_, armed)| armed.owners.iter().copied())
            .collect()
    }

    /// Starts recording the filters created for compactions into
    /// `destination`, replacing any earlier watch, until the returned
    /// [`Watching`] is consumed or dropped.
    ///
    /// The registry holds one watch: callers watch one at a time, as the
    /// reclaimer's pass lock makes them.
    fn watch(&self, destination: u32) -> Watching<'_> {
        match &mut *self.state.lock() {
            State::Filtering { watch, .. } => {
                *watch = Some(Watch {
                    destination,
                    filters: Vec::new(),
                });
            }
            State::Halted => {}
        }
        Watching(self)
    }

    /// Ends the watch and returns the entries that every filter created for
    /// its destination could discard. Empty when no filter was created.
    fn unwatch(&self) -> HashSet<DroppedGeneration> {
        let State::Filtering { watch, .. } = &mut *self.state.lock() else {
            return HashSet::new();
        };
        let Some(watch) = watch.take() else {
            return HashSet::new();
        };
        watch
            .filters
            .into_iter()
            .reduce(|proven, filter| proven.intersection(&filter).copied().collect())
            .unwrap_or_default()
    }

    /// Forgets entries whose rows a completed reclaim has discarded.
    fn disarm(&self, entries: &HashSet<DroppedGeneration>) {
        match &mut *self.state.lock() {
            State::Filtering { armed, .. } => armed.retain(|entry, _| !entries.contains(entry)),
            State::Halted => {}
        }
    }
}

/// A running [`Registry::watch`]. Dropping it ends the watch, so a reclaim
/// pass that is cancelled while its compaction runs leaves none behind.
#[must_use]
struct Watching<'a>(&'a Registry);

impl Watching<'_> {
    /// Ends the watch and returns the entries whose rows the watched
    /// compaction, last seen with `status`, has discarded from every table it
    /// read: those every filter created for its destination could discard,
    /// and none unless the compaction completed. A job that failed, was
    /// lost, or is still running may have written nothing its filters
    /// decided.
    fn proven(self, status: Option<CompactionStatus>) -> HashSet<DroppedGeneration> {
        let discardable = self.0.unwatch();
        match status {
            Some(CompactionStatus::Completed) => discardable,
            Some(
                CompactionStatus::Submitted
                | CompactionStatus::Scheduled
                | CompactionStatus::Running
                | CompactionStatus::Compacted
                | CompactionStatus::Failed,
            )
            | None => HashSet::new(),
        }
    }
}

impl Drop for Watching<'_> {
    fn drop(&mut self) {
        self.0.unwatch();
    }
}

#[cfg(test)]
mod tests;
