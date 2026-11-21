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
//!   building, active, aborting or dropping. [`Registry::refresh`] checks
//!   every stored entry it has not armed against the catalog and the id
//!   watermarks, and *quarantines* one that fails: its rows are kept, the
//!   store opens, and the entry is reported until a later refresh finds it
//!   consistent. [`ensure_index_unclaimed`] and
//!   [`ensure_generation_unclaimed`] fail the allocation of an id an entry
//!   names. A vector physical id has no such check: an entry is keyed by
//!   generation, not by physical id, so the check would scan every entry on
//!   every allocation. The cross-check gives the same guarantee, because an
//!   armed entry names only ids below the watermark, which are never
//!   allocated again, and the transaction that allocates an id above it also
//!   writes the record or the partition mapping that makes the cross-check
//!   quarantine an entry naming that id.
//! - **Retirement.** [`retire`] deletes an entry once no table of the store
//!   can hold a row of it; see that module for the proof. Compactions the
//!   scheduler picks retire entries as they pass; nothing compacts on their
//!   behalf. A restart forgets the proof and starts it again from every
//!   table the store then has.
//! - **Reclaim.** [`reclaim`] compacts on request, for a caller who wants the
//!   bytes back before the scheduler gets to them. It deletes no entry
//!   itself.
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
//! Two kinds of cleanup still delete row by row: one that had deleted rows
//! before the store was admitted; and a vector cleanup whose first physical
//! id has more unfiltered rows than one step deletes, which the persisted
//! progress cannot tell from the first. Both finish on an admitted store, so
//! each still leaves an entry, which then names rows that are already gone
//! and retires at the first pass that looks.
//!
//! A generation adopted from legacy vector storage is never given an entry:
//! its physical id was not allocated by this catalog and its rows may be
//! shared with a legacy index. Its drop deletes every row one by one. The
//! abort of its build deletes the directory and the generation-keyed rows
//! the build added, one by one, and hands the legacy rows back untouched.
//!
//! Whether a drop leaves an entry depends on the store alone, not on the
//! writer that runs it. A writer that runs no compaction worker in its own
//! process still leaves entries and arms them, but discards and retires
//! nothing: its entries wait for a writer that compacts in process.
//!
//! # What does not scale
//!
//! Every armed entry costs one hash-set member per owner in each compaction
//! filter; a partitioned vector generation has one owner per partition. A
//! refresh scans the entry prefix; when it finds entries it has not armed it
//! reads, once for each scope they name, that scope's index records and, for
//! vector entries, its partition mappings, and one metadata key per physical
//! id. Every entry is new to a writer that opens, so an open pays that once.
//! Dropping a partitioned vector generation still costs one cleanup commit
//! per partition, and its mappings stay until the entry retires. An entry
//! lives as long as a table that held its rows when the proof started: the
//! rows of a large index sit in the oldest sorted run, which the scheduler
//! rewrites rarely, so such an entry can stay for as long as that run does
//! unless a caller asks for a reclaim.

pub(crate) mod filter;
pub(crate) mod ownership;
pub(crate) mod reclaim;
pub(crate) mod retire;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

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
    // without creating a filter, so a requested reclaim made of one would
    // rewrite nothing and give no byte back.
    options.enable_trivial_move = false;
    Some(
        slatedb::CompactorBuilder::new(path.to_string(), object_store)
            .with_options(options)
            .with_compaction_filter_supplier(Arc::new(filter::DroppedRowsFilterSupplier::new(
                registry,
            ))),
    )
}

/// Whether a writer opened with `settings` runs every compaction of its
/// store in its own process, behind the filter [`compactor`] installs.
///
/// [`retire`] rests on that: a compaction another process ran would replace
/// tables without consulting this writer's [`Registry`]. SlateDB runs a
/// compaction in whichever worker claims it, and a writer's embedded worker
/// is the only one Helix starts; a writer configured without it leaves its
/// compactions to workers it cannot see, so it retires nothing.
pub(crate) fn compacts_in_process(settings: &slatedb::Settings) -> bool {
    settings
        .compactor_options
        .as_ref()
        .is_some_and(|options| options.worker.is_some())
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

/// Compacts on request until `db` holds no dropped-generation entry, for
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
                .reclaim_dropped_index_storage(std::time::Duration::from_secs(30))
                .await
                .expect("the reclaim runs")
            {
                DroppedStorageReclaim::Idle => break,
                // Rewritten, or lost to a compaction the scheduler picked:
                // the next call tells whether an entry is left.
                DroppedStorageReclaim::Compacted { .. }
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

/// Why an entry failed its cross-check.
pub(crate) type Contradiction = &'static str;

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
            return Err("names a vector physical index id above the watermark");
        }
        if physical_index_ids.iter().any(|id| {
            self.mapped
                .get(id)
                .is_some_and(|mapped| *mapped != MappedBy::One(entry))
        }) {
            return Err("names a vector physical index another generation's partition maps to");
        }
        // Index records are never deleted, so the drop's own record is there.
        let Some(record) = self.records.get(&entry.index_id) else {
            return Err("names an index its scope has no record of");
        };
        if record.family != family {
            return Err("names an index of another family");
        }
        // A recreated index builds a strictly later generation.
        if record.live.is_some_and(|live| live <= entry.generation) {
            return Err("names a generation its index record still owns");
        }
        if record.generation < entry.generation {
            return Err("names a generation its index has not reached");
        }
        if physical_index_ids.iter().any(|id| self.owned.contains(id)) {
            return Err("names a vector physical index an index record still owns");
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
            Err("names an index id above the watermark")
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
                    Err("names a vector physical index that has a metadata row")
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
            return Ok(Err("has a partition mapping of another generation"));
        }
        if !physical_index_ids.insert(mapping.physical_index_id) {
            return Ok(Err(
                "has two partitions mapped to one vector physical index",
            ));
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

/// How far [`retire`] has got in proving that no table holds a row of one
/// armed entry. The steps and why each is sound are in that module.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Proof {
    /// Nothing is known: the entry was just armed, or a compaction kept its
    /// rows since.
    Unproven,
    /// These compactions were in flight when the proof started, so they may
    /// write rows a filter of theirs kept before the entry was armed. Never
    /// empty.
    Draining { compactions: HashSet<CompactionId> },
    /// These tables held rows of the entry once every compaction above had
    /// finished. Every later table is free of them. Never empty.
    Tracking { tables: HashSet<TableId> },
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
}

/// An entry that failed its cross-check. It holds no owner, so no filter can
/// discard a row for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Quarantined {
    /// Scope and row layout the entry records.
    pub(crate) value: DroppedGenerationValue,
    /// What in the catalog contradicts the entry.
    pub(crate) contradiction: Contradiction,
}

/// What a writer does with one stored entry.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Entry {
    /// Compactions discard its rows.
    Armed(Armed),
    /// Compactions keep its rows.
    Quarantined(Quarantined),
}

/// A dropped index generation whose entry the catalog contradicts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuarantinedDroppedGeneration {
    /// The logical index the entry names.
    pub index_id: u64,
    /// The generation of that index the entry names.
    pub generation: u64,
    /// What in the catalog contradicts the entry.
    pub contradiction: &'static str,
}

/// What a writer knows of the dropped index generations whose entries are
/// still stored, as of its last look. Every count is of generations unless
/// it says otherwise.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DroppedIndexStorage {
    /// Armed, with no proof under way: just dropped, or a compaction kept
    /// their rows for a reader older than the drop.
    pub unproven: usize,
    /// Of `unproven`, those whose rows a compaction has kept for an older
    /// reader since they were armed.
    pub retained: usize,
    /// Armed, waiting for the compactions that were in flight when their
    /// proof started.
    pub draining: usize,
    /// Armed, waiting for the tables that held their rows to be compacted.
    pub tracking: usize,
    /// Distinct tables the `tracking` generations wait for.
    pub tracked_tables: usize,
    /// Entries the catalog contradicts. Their rows are kept.
    pub quarantined: Vec<QuarantinedDroppedGeneration>,
}

impl DroppedIndexStorage {
    /// Every dropped generation whose entry is still stored.
    pub fn entries(&self) -> usize {
        self.unproven + self.draining + self.tracking + self.quarantined.len()
    }
}

/// The dropped generations one writer knows of, and what its compactions do
/// with each.
///
/// Shared by the compaction filter supplier, which reads it once per filter,
/// and [`retire`], which arms entries and removes the ones it retires.
#[derive(Debug, Default)]
pub(crate) struct Registry {
    entries: parking_lot::Mutex<HashMap<DroppedGeneration, Entry>>,
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
            let entries = self.entries.lock();
            stored
                .into_iter()
                .filter(|(entry, _)| !matches!(entries.get(entry), Some(Entry::Armed(_))))
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
        let mut entries = self.entries.lock();
        let mut armed = 0usize;
        for (entry, value, verdict) in checked {
            let next = match verdict {
                Ok(owners) => {
                    armed += 1;
                    Entry::Armed(Armed {
                        gate,
                        value,
                        owners,
                        kept: 0,
                        proof: Proof::Unproven,
                    })
                }
                Err(contradiction) => Entry::Quarantined(Quarantined {
                    value,
                    contradiction,
                }),
            };
            match (&next, entries.insert(entry, next.clone())) {
                (Entry::Quarantined(quarantined), previous) if previous.as_ref() != Some(&next) => {
                    tracing::error!(
                        index_id = entry.index_id.get(),
                        generation = entry.generation.get(),
                        contradiction = quarantined.contradiction,
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
        }
        if armed > 0 {
            tracing::info!(entries = armed, gate, "armed dropped-generation entries");
        }
        Ok(())
    }

    /// Every armed entry.
    pub(crate) fn armed(&self) -> Vec<(DroppedGeneration, Armed)> {
        self.entries
            .lock()
            .iter()
            .filter_map(|(entry, state)| match state {
                Entry::Armed(armed) => Some((*entry, armed.clone())),
                Entry::Quarantined(_) => None,
            })
            .collect()
    }

    /// Every entry this writer knows of, counted by what it waits for.
    pub(crate) fn storage(&self) -> DroppedIndexStorage {
        let entries = self.entries.lock();
        let mut storage = DroppedIndexStorage::default();
        let mut tracked = HashSet::new();
        for (entry, state) in entries.iter() {
            match state {
                Entry::Armed(armed) => match &armed.proof {
                    Proof::Unproven => {
                        storage.unproven += 1;
                        storage.retained += usize::from(armed.kept > 0);
                    }
                    Proof::Draining { .. } => storage.draining += 1,
                    Proof::Tracking { tables } => {
                        storage.tracking += 1;
                        tracked.extend(tables.iter().copied());
                    }
                },
                Entry::Quarantined(quarantined) => {
                    storage.quarantined.push(QuarantinedDroppedGeneration {
                        index_id: entry.index_id.get(),
                        generation: entry.generation.get(),
                        contradiction: quarantined.contradiction,
                    });
                }
            }
        }
        storage.tracked_tables = tracked.len();
        storage
            .quarantined
            .sort_unstable_by_key(|entry| (entry.index_id, entry.generation));
        storage
    }

    /// The owners whose rows one compaction job may discard, decided once for
    /// the filter being created: every armed entry whose gate the job's
    /// `retention_min_seq` has reached. A job that reports no retention
    /// sequence discards nothing.
    ///
    /// An armed entry whose rows the filter keeps loses its proof: the job
    /// may write those rows into a table no proof step has seen.
    fn discardable(&self, context: &CompactionJobContext) -> HashSet<OwnerKey> {
        let mut entries = self.entries.lock();
        let mut discardable = HashSet::new();
        for state in entries.values_mut() {
            let Entry::Armed(armed) = state else {
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
            }
        }
        discardable
    }

    /// Records that the proof of `entry` has reached `proof`, unless a filter
    /// kept the entry's rows since the caller read `kept`, or the entry is
    /// no longer armed. Returns whether the step applied.
    pub(crate) fn advance(&self, entry: DroppedGeneration, kept: u64, proof: Proof) -> bool {
        let mut entries = self.entries.lock();
        let Some(Entry::Armed(armed)) = entries.get_mut(&entry) else {
            return false;
        };
        if armed.kept != kept {
            return false;
        }
        armed.proof = proof;
        true
    }

    /// Forgets `entry`, whose rows are proven gone, unless a filter kept the
    /// entry's rows since the caller read `kept`, or the entry is no longer
    /// armed. Returns whether it was forgotten: only then may the caller
    /// delete the stored entry.
    pub(crate) fn retire(&self, entry: DroppedGeneration, kept: u64) -> bool {
        let mut entries = self.entries.lock();
        let Some(Entry::Armed(armed)) = entries.get(&entry) else {
            return false;
        };
        if armed.kept != kept {
            return false;
        }
        entries.remove(&entry);
        true
    }
}

#[cfg(test)]
mod tests;
