//! The compaction filter that discards the rows of dropped generations.
//!
//! SlateDB creates one filter per subcompaction of a job, and every filter of
//! a job sees the same job context. A filter decides its owners once, when it
//! is created, and never looks at the registry again, so a job cannot start
//! discarding half-way through its output.

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use slatedb::{
    CompactionFilter, CompactionFilterDecision, CompactionFilterError, CompactionFilterSupplier,
    CompactionJobContext, RowEntry,
};

use super::ownership::{self, OwnerKey};
use super::Registry;

/// Creates the filters of one writer's compactor.
#[derive(Debug)]
pub(crate) struct DroppedRowsFilterSupplier {
    registry: Arc<Registry>,
}

impl DroppedRowsFilterSupplier {
    /// A supplier whose filters discard what `registry` allows when each is
    /// created.
    pub(crate) fn new(registry: Arc<Registry>) -> Self {
        Self { registry }
    }
}

#[async_trait]
impl CompactionFilterSupplier for DroppedRowsFilterSupplier {
    async fn create_compaction_filter(
        &self,
        context: &CompactionJobContext,
    ) -> Result<Box<dyn CompactionFilter>, CompactionFilterError> {
        Ok(Box::new(Counted {
            filter: DroppedRowsFilter::new(self.registry.discardable(context)),
            registry: Arc::clone(&self.registry),
        }))
    }
}

/// A filter the registry counts as alive until it is dropped, which is how a
/// writer knows without reading object storage whether a compaction of its
/// process is running. SlateDB drops a filter when its job ends, fails or
/// is abandoned.
struct Counted {
    filter: DroppedRowsFilter,
    registry: Arc<Registry>,
}

impl Drop for Counted {
    fn drop(&mut self) {
        self.registry.filter_dropped();
    }
}

/// Discards every row whose key names one of a fixed set of owners.
#[derive(Debug)]
struct DroppedRowsFilter {
    discardable: HashSet<OwnerKey>,
    /// Owner prefix of the last owned row seen, and whether its rows are
    /// discarded. Rows of one owner are contiguous, so most rows of a dropped
    /// generation are decided by this one comparison.
    last_prefix: Vec<u8>,
    last_discarded: bool,
}

impl DroppedRowsFilter {
    fn new(discardable: HashSet<OwnerKey>) -> Self {
        Self {
            discardable,
            last_prefix: Vec::new(),
            last_discarded: false,
        }
    }

    /// Whether the row under `key` is discarded.
    fn discards(&mut self, key: &[u8]) -> bool {
        if self.discardable.is_empty() {
            return false;
        }
        // An owner prefix is never empty, so the initial state matches nothing.
        if !self.last_prefix.is_empty() && key.starts_with(&self.last_prefix) {
            return self.last_discarded;
        }
        let Some(row) = ownership::owned_row(key) else {
            return false;
        };
        self.last_prefix.clear();
        self.last_prefix.extend_from_slice(row.prefix);
        self.last_discarded = self.discardable.contains(&row.owner);
        self.last_discarded
    }
}

#[async_trait]
impl CompactionFilter for Counted {
    async fn filter(
        &mut self,
        entry: &RowEntry,
    ) -> Result<CompactionFilterDecision, CompactionFilterError> {
        // Dropped outright whatever the row holds: a value, a merge operand
        // or a tombstone of a dropped generation is equally unread.
        Ok(if self.filter.discards(&entry.key) {
            CompactionFilterDecision::Drop
        } else {
            CompactionFilterDecision::Keep
        })
    }

    async fn on_compaction_end(&mut self) -> Result<(), CompactionFilterError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encoding::v2::keys::scope::{DataScope, TenantId};

    const TENANT: DataScope = DataScope::Tenant(TenantId::from_u128(7));

    fn hot(scope: DataScope, physical_index_id: u64, suffix: &[u8]) -> Vec<u8> {
        let mut key = Vec::new();
        scope.encode_key_prefix(&mut key);
        key.push(0xF0);
        key.extend_from_slice(&physical_index_id.to_be_bytes());
        key.extend_from_slice(suffix);
        key
    }

    fn generation(scope: DataScope, kind: u8, index_id: u64, generation: u64) -> Vec<u8> {
        let mut key = Vec::new();
        scope.encode_key_prefix(&mut key);
        key.extend_from_slice(&[0x06, kind]);
        key.extend_from_slice(&index_id.to_be_bytes());
        key.extend_from_slice(&generation.to_be_bytes());
        key.extend_from_slice(b"row");
        key
    }

    #[test]
    fn a_filter_with_no_owner_keeps_every_row() {
        let mut filter = DroppedRowsFilter::new(HashSet::new());
        assert!(!filter.discards(&hot(TENANT, 4, b"a")));
        assert!(!filter.discards(&generation(TENANT, 0x03, 1, 1)));
    }

    #[test]
    fn rows_are_discarded_by_exact_scope_and_owner() {
        let mut filter = DroppedRowsFilter::new(HashSet::from([
            OwnerKey::PhysicalIndex {
                scope: TENANT,
                physical_index_id: 4,
            },
            OwnerKey::Generation {
                scope: TENANT,
                index_id: 9,
                generation: 2,
            },
        ]));
        // Interleaved so every decision follows a different remembered owner.
        assert!(filter.discards(&hot(TENANT, 4, b"a")));
        assert!(filter.discards(&hot(TENANT, 4, b"b")));
        assert!(!filter.discards(&hot(TENANT, 5, b"a")));
        assert!(!filter.discards(&hot(TENANT, 5, b"b")));
        assert!(filter.discards(&hot(TENANT, 4, b"c")));
        assert!(!filter.discards(&hot(DataScope::LegacyUnscoped, 4, b"a")));
        assert!(!filter.discards(&hot(DataScope::Tenant(TenantId::from_u128(8)), 4, b"a")));
        assert!(filter.discards(&generation(TENANT, 0x03, 9, 2)));
        assert!(filter.discards(&generation(TENANT, 0x04, 9, 2)));
        assert!(!filter.discards(&generation(TENANT, 0x03, 9, 3)));
        assert!(!filter.discards(&generation(TENANT, 0x03, 8, 2)));
        assert!(!filter.discards(&generation(DataScope::LegacyUnscoped, 0x03, 9, 2)));
        // Kinds keyed by the same ids that no generation owns.
        assert!(!filter.discards(&generation(TENANT, 0x0F, 9, 2)));
        assert!(!filter.discards(&generation(TENANT, 0x14, 9, 2)));
        assert!(!filter.discards(&generation(TENANT, 0x15, 9, 2)));
        assert!(filter.discards(&generation(TENANT, 0x05, 9, 2)));
    }

    #[test]
    fn a_remembered_owner_never_decides_a_row_that_is_not_owned() {
        let mut filter = DroppedRowsFilter::new(HashSet::from([OwnerKey::PhysicalIndex {
            scope: DataScope::LegacyUnscoped,
            physical_index_id: 4,
        }]));
        assert!(filter.discards(&hot(DataScope::LegacyUnscoped, 4, b"a")));
        // Graph, core vector, catalog and global rows after a discarded one.
        assert!(!filter.discards(&[0x02, 0xF0, 0, 0, 0, 0, 0, 0, 0, 4]));
        assert!(!filter.discards(&[0x03, 0x03, 0, 0, 0, 0, 0, 0, 0, 4, 0x01]));
        assert!(!filter.discards(&generation(DataScope::LegacyUnscoped, 0x01, 4, 4)));
        let mut global = vec![0xFE; 17];
        global.extend_from_slice(&[0x0D, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 1]);
        assert!(!filter.discards(&global));
        assert!(filter.discards(&hot(DataScope::LegacyUnscoped, 4, b"b")));
    }
}
