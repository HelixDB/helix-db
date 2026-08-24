//! Executable KV read contracts for graph element access.
//!
//! This module owns the boundary between planner `KvReadPlan` keyspace ADTs and
//! the physical SlateDB key layout used by the interpreter.

use bytes::Bytes;
use helix_planner::{exec, properties};

use super::super::{ElementRef, ExecutionContext, ExecutionRow, ExecutionValue};
use crate::encoding::keys;
use crate::error::Result;

impl<'db> ExecutionContext<'db> {
    pub(in crate::execution::interpreter) async fn execute_kv_read(
        &mut self,
        read: &exec::KvReadPlan,
    ) -> Result<ExecutionValue> {
        match read {
            exec::KvReadPlan::Get { key } => {
                let (element, id, physical_key) = physical_element_key(self.tenant_scope, key);
                let value = self.get_raw(&physical_key).await?;
                Ok(ExecutionValue::Stream(
                    value
                        .is_some()
                        .then(|| ExecutionRow::current(element_ref(element, id)))
                        .into_iter()
                        .collect(),
                ))
            }
            exec::KvReadPlan::MultiGet(plan) => {
                let keyed = plan
                    .keyed_original_positions()
                    .map(|(key, original_position)| {
                        let (element, id, physical_key) =
                            physical_element_key(self.tenant_scope, key);
                        (original_position, element, id, physical_key)
                    })
                    .collect::<Vec<_>>();
                let physical_keys = keyed
                    .iter()
                    .map(|(_, _, _, physical_key)| physical_key)
                    .collect::<Vec<_>>();
                let values = self.multi_get_raw(&physical_keys).await?;
                let mut rows = keyed
                    .into_iter()
                    .zip(values)
                    .filter_map(|((original_position, element, id, _), value)| {
                        value.is_some().then_some({
                            (
                                original_position,
                                ExecutionRow::current(element_ref(element, id)),
                            )
                        })
                    })
                    .collect::<Vec<_>>();
                rows.sort_by_key(|(original_position, _)| *original_position);
                Ok(ExecutionValue::Stream(
                    rows.into_iter().map(|(_, row)| row).collect(),
                ))
            }
            exec::KvReadPlan::RangeScan {
                keyspace,
                start,
                end,
                limit,
            } => {
                let keyspace = *keyspace;
                let (start, end) = element_range_bounds(keyspace, start, end);
                let ids = self
                    .scan_raw_range_limited(start, end, limit.map(|limit| limit.get()))
                    .await?
                    .into_iter()
                    .filter_map(|(key, _)| parse_element_id(keyspace, &key))
                    .collect::<Vec<_>>();
                self.element_rows(keyspace, ids).await
            }
            exec::KvReadPlan::PrefixScan {
                keyspace,
                prefix,
                limit,
            } => {
                let keyspace = *keyspace;
                let mut physical_prefix = element_prefix(keyspace);
                physical_prefix.extend_from_slice(prefix.as_ref());
                let ids = self
                    .scan_raw_prefix_limited(
                        Bytes::from(physical_prefix),
                        limit.map(|limit| limit.get()),
                    )
                    .await?
                    .into_iter()
                    .filter_map(|(key, _)| parse_element_id(keyspace, &key))
                    .collect::<Vec<_>>();
                self.element_rows(keyspace, ids).await
            }
        }
    }

    pub(in crate::execution::interpreter) async fn scan_element_ids(
        &self,
        keyspace: exec::ElementKeyspace,
        limit: Option<properties::PositiveUsize>,
    ) -> Result<Vec<u64>> {
        Ok(self
            .scan_raw_range_limited(
                Bytes::from(element_prefix(keyspace)),
                Bytes::from(element_prefix_end(keyspace)),
                limit.map(|limit| limit.get()),
            )
            .await?
            .into_iter()
            .filter_map(|(key, _)| parse_element_id(keyspace, &key))
            .collect())
    }

    async fn element_rows(
        &self,
        keyspace: exec::ElementKeyspace,
        ids: Vec<u64>,
    ) -> Result<ExecutionValue> {
        match keyspace {
            exec::ElementKeyspace::NodeProperty => self.node_rows(ids).await,
            exec::ElementKeyspace::EdgeEndpoints => self.edge_rows(ids).await,
        }
    }
}

fn physical_element_key(
    scope: crate::encoding::keys::scope::DataScope,
    key: &exec::KvKey,
) -> (exec::ElementKeyspace, u64, Bytes) {
    let keyspace = key.keyspace();
    let id = key.id();
    let kind = match keyspace {
        exec::ElementKeyspace::NodeProperty => {
            keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(id))
        }
        exec::ElementKeyspace::EdgeEndpoints => {
            keys::DataKeyKind::EdgeEndpoints(keys::EdgeEndpointsKey::new(id))
        }
    };
    let physical = keys::DataKey::Data { scope, kind }.to_bytes();
    (keyspace, id, physical)
}

fn element_prefix(keyspace: exec::ElementKeyspace) -> Vec<u8> {
    match keyspace {
        exec::ElementKeyspace::NodeProperty => vec![keys::KeyPrefix::NodeProperty.as_u8()],
        exec::ElementKeyspace::EdgeEndpoints => vec![keys::KeyPrefix::EdgeEndpoints.as_u8()],
    }
}

fn element_prefix_end(keyspace: exec::ElementKeyspace) -> Vec<u8> {
    match keyspace {
        exec::ElementKeyspace::NodeProperty => vec![keys::KeyPrefix::PropertyIndex.as_u8()],
        exec::ElementKeyspace::EdgeEndpoints => vec![keys::KeyPrefix::EdgePairIndex.as_u8()],
    }
}

fn element_range_bounds(
    keyspace: exec::ElementKeyspace,
    start: &exec::KvKeyBound,
    end: &exec::KvKeyBound,
) -> (Bytes, Bytes) {
    let start = match start {
        exec::KvKeyBound::Unbounded => Bytes::from(element_prefix(keyspace)),
        exec::KvKeyBound::Included(key) => Bytes::from(element_bound_key(keyspace, key)),
        exec::KvKeyBound::Excluded(key) => Bytes::from(element_bound_key_after(keyspace, key)),
    };
    let end = match end {
        exec::KvKeyBound::Unbounded => Bytes::from(element_prefix_end(keyspace)),
        exec::KvKeyBound::Included(key) => Bytes::from(element_bound_key_after(keyspace, key)),
        exec::KvKeyBound::Excluded(key) => Bytes::from(element_bound_key(keyspace, key)),
    };
    (start, end)
}

fn element_bound_key(keyspace: exec::ElementKeyspace, key: &exec::KvBoundKey) -> Vec<u8> {
    let mut bytes = element_prefix(keyspace);
    bytes.extend_from_slice(key.bytes());
    bytes
}

fn element_bound_key_after(keyspace: exec::ElementKeyspace, key: &exec::KvBoundKey) -> Vec<u8> {
    let mut bytes = element_bound_key(keyspace, key);
    bytes.push(0xFF);
    bytes
}

fn parse_element_id(keyspace: exec::ElementKeyspace, key: &[u8]) -> Option<u64> {
    match keys::DataKeyKind::parse_from_slice(key).ok()? {
        keys::DataKeyKind::NodeProperty(key) if keyspace == exec::ElementKeyspace::NodeProperty => {
            Some(key.node_id())
        }
        keys::DataKeyKind::EdgeEndpoints(key)
            if keyspace == exec::ElementKeyspace::EdgeEndpoints =>
        {
            Some(key.edge_id())
        }
        keys::DataKeyKind::Adjacency(_)
        | keys::DataKeyKind::EdgePropertyPair(..)
        | keys::DataKeyKind::EdgePropertyById(_)
        | keys::DataKeyKind::NodeProperty(_)
        | keys::DataKeyKind::PropertyIndex(_)
        | keys::DataKeyKind::EdgeEndpoints(_)
        | keys::DataKeyKind::EdgePairIndex(..)
        | keys::DataKeyKind::Vector(_)
        | keys::DataKeyKind::IndexMetadata(_) => None,
    }
}

fn element_ref(keyspace: exec::ElementKeyspace, id: u64) -> ElementRef {
    match keyspace {
        exec::ElementKeyspace::NodeProperty => ElementRef::Node(id),
        exec::ElementKeyspace::EdgeEndpoints => ElementRef::Edge(id),
    }
}

#[cfg(test)]
mod tests {
    use helix_planner::context::ParamBindings;

    use super::super::super::test_support;
    use super::*;

    #[tokio::test]
    async fn prefix_scan_limits_node_rows_and_edge_id_scan_uses_edge_keyspace() {
        let db = test_support::open_db("kv-prefix-scan").await;
        let first = test_support::add_node_with_properties(&db, "Node", Vec::new()).await;
        let second = test_support::add_node_with_properties(&db, "Node", Vec::new()).await;
        let edge = test_support::add_edge(&db, first, second, "LINK").await;
        let mut context = ExecutionContext::new(&db, ParamBindings::default());

        let rows = context
            .execute_kv_read(&exec::KvReadPlan::PrefixScan {
                keyspace: exec::ElementKeyspace::NodeProperty,
                prefix: helix_planner::ir::AtLeast::from_one_and_rest(0, Vec::new()),
                limit: Some(properties::PositiveUsize::new(1).unwrap()),
            })
            .await
            .unwrap();
        assert_eq!(
            rows,
            ExecutionValue::Stream(vec![ExecutionRow::current(ElementRef::Node(first))])
        );
        assert_eq!(
            context
                .scan_element_ids(exec::ElementKeyspace::EdgeEndpoints, None)
                .await
                .unwrap(),
            vec![edge]
        );
    }

    #[test]
    fn element_id_parser_rejects_typed_metadata_keys() {
        let key = keys::DataKey::Data {
            scope: keys::scope::DataScope::LegacyUnscoped,
            kind: keys::DataKeyKind::IndexMetadata(keys::metadata::MetadataKey::new(b"catalog")),
        }
        .to_bytes();

        assert_eq!(
            parse_element_id(exec::ElementKeyspace::NodeProperty, &key),
            None
        );
    }
}
