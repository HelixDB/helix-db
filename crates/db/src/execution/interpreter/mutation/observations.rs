//! Sorted observation keys admit ID collection and typed key buffers before use.
use crate::{encoding::v2::keys, query_resources};
use helix_planner::relational::allocation;
use std::collections::BTreeSet;

#[derive(Clone, Copy)]
pub(super) enum Kind {
    Nodes,
    Edges,
}
impl Kind {
    fn keys(
        self,
        id: u64,
        scope: keys::scope::DataScope,
    ) -> impl Iterator<Item = keys::DataKey<'static>> {
        let (first, second) = match self {
            Self::Nodes => (
                keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(id)),
                None,
            ),
            Self::Edges => (
                keys::DataKeyKind::EdgeEndpoints(keys::EdgeEndpointsKey::new(id)),
                Some(keys::DataKeyKind::EdgePropertyById(
                    keys::EdgePropertyByIdKey::new(id),
                )),
            ),
        };
        std::iter::once(first)
            .chain(second)
            .map(move |kind| keys::DataKey::Data { scope, kind })
    }
}

pub(super) struct RowKeys {
    pub(super) ids: BTreeSet<u64>,
    pub(super) keys: Vec<bytes::Bytes>,
    _memory: Option<query_resources::Reservation>,
}
impl RowKeys {
    pub(super) fn new(
        ids: impl IntoIterator<Item = u64>,
        kind: Kind,
        scope: keys::scope::DataScope,
        budget: Option<&query_resources::Budget>,
    ) -> crate::error::Result<Self> {
        let mut memory = budget.map(|budget| budget.reserve(0)).transpose()?;
        let unique = match memory.as_mut() {
            None => ids.into_iter().collect::<BTreeSet<_>>(),
            Some(memory) => {
                let mut unique = BTreeSet::new();
                // Per-entry insertion lets admission precede every allocation,
                // without the bulk constructor's unbounded sorting temporary.
                for id in ids {
                    if unique.contains(&id) {
                        continue;
                    }
                    memory.resize(allocation::btree_bytes::<u64, ()>(
                        unique.len().saturating_add(1),
                    ))?;
                    unique.insert(id);
                }
                unique
            }
        };
        if unique.is_empty() {
            return Ok(Self {
                ids: unique,
                keys: Vec::new(),
                _memory: memory,
            });
        }
        // These closed key variants have fixed-width IDs. Typed codecs own
        // their full scoped lengths; no wire offsets are duplicated here.
        let count = kind.keys(0, scope).count();
        let per_id = kind.keys(0, scope).fold(0_usize, |bytes, key| {
            bytes
                .saturating_add(key.encoded_len())
                .saturating_add(size_of::<bytes::Bytes>())
        });
        memory
            .as_mut()
            .map(|memory| {
                memory.resize(
                    allocation::btree_bytes::<u64, ()>(unique.len())
                        .saturating_add(unique.len().saturating_mul(per_id)),
                )
            })
            .transpose()?;
        let mut keys = Vec::with_capacity(unique.len().saturating_mul(count));
        for id in &unique {
            keys.extend(kind.keys(*id, scope).map(|key| key.to_bytes()));
        }
        Ok(Self {
            ids: unique,
            keys,
            _memory: memory,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn observation_keys_admit_unique_ids_and_preserve_typed_scoped_order() {
        let tenant = keys::scope::TenantId::from_ulid_str("00000000000000000000000001").unwrap();
        for scope in [
            keys::scope::DataScope::LegacyUnscoped,
            keys::scope::DataScope::Tenant(tenant),
        ] {
            for kind in [Kind::Nodes, Kind::Edges] {
                let budget = query_resources::Budget::new(512 * 1024);
                let (rows, allocations) = crate::allocation_testing::observe(|| {
                    RowKeys::new(
                        (0..1024).map(|i| i % 257).chain([u64::MAX]),
                        kind,
                        scope,
                        Some(&budget),
                    )
                    .unwrap()
                });
                assert!(allocations.bytes <= budget.peak());
                assert_eq!(rows.ids.len(), 258);
                assert_eq!(
                    rows.keys,
                    rows.ids
                        .iter()
                        .flat_map(|id| kind.keys(*id, scope).map(|key| key.to_bytes()))
                        .collect::<Vec<_>>()
                );
                let (copy, allocation) = crate::allocation_testing::observe(|| {
                    RowKeys::new([0, 0, 0], kind, scope, None).unwrap()
                });
                assert!(allocation.bytes > 0);
                assert_eq!(
                    copy.keys,
                    kind.keys(0, scope)
                        .map(|key| key.to_bytes())
                        .collect::<Vec<_>>()
                );
                drop(rows);
                assert_eq!(budget.available(), 512 * 1024);
                let budget = query_resources::Budget::new(0);
                let (result, allocation) = crate::allocation_testing::observe(|| {
                    RowKeys::new([1], kind, scope, Some(&budget))
                });
                assert!(matches!(
                    result,
                    Err(crate::HelixDbError::QueryMemoryLimitExceeded)
                ));
                assert_eq!(allocation.allocations, 0);
                assert!(RowKeys::new([], kind, scope, Some(&budget))
                    .unwrap()
                    .keys
                    .is_empty());
                // One ID fits, but its additional typed key buffers do not.
                let budget = query_resources::Budget::new(allocation::btree_bytes::<u64, ()>(1));
                assert!(matches!(
                    RowKeys::new([1], kind, scope, Some(&budget)),
                    Err(crate::HelixDbError::QueryMemoryLimitExceeded)
                ));
                assert_eq!(budget.available(), allocation::btree_bytes::<u64, ()>(1));
            }
        }
    }
}
