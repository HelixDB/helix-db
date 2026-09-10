//! Bounded node batches from production-selected access primitives. The cursor
//! owns its bitmap or storage iterator; projection/aggregation owns downstream
//! state. It never reopens a scan to simulate pagination.
use super::{memory, push_row, ExecutionContext, Limits, Result, RowBuffer};
use crate::encoding::v2::keys;
use helix_planner::{exec, relational as r};
use slatedb::DbReadOps;

pub(super) enum NodeCursor {
    Scan(slatedb::DbIterator),
    Indexed {
        ids: roaring::treemap::IntoIter,
        verify_existence: bool,
        _memory: memory::Reservation,
    },
}

impl ExecutionContext<'_> {
    /// Only open a cursor when its exact selected primitive supports iteration.
    /// Other native access contracts continue through their existing executor.
    pub(super) async fn node_cursor(&self, operation: &exec::ExecOp) -> Result<Option<NodeCursor>> {
        let source = if matches!(
            operation,
            exec::ExecOp::KvRead(exec::KvReadPlan::RangeScan {
                keyspace: exec::ElementKeyspace::NodeProperty,
                start: exec::KvKeyBound::Unbounded,
                end: exec::KvKeyBound::Unbounded,
                limit: None,
            })
        ) {
            &exec::ExecNodeAccessPlan::AllScan
        } else {
            let exec::ExecOp::Access { plan } = operation else {
                return Ok(None);
            };
            let exec::ExecAccessPlan::Node(source) = plan.as_ref() else {
                return Ok(None);
            };
            source
        };
        let (bitmap, verify_existence) = match source {
            exec::ExecNodeAccessPlan::AllScan => {
                let prefix = keys::DataKey::data_prefix(
                    self.tenant_scope,
                    bytes::Bytes::from(vec![keys::KeyPrefix::NodeProperty.as_u8()]),
                );
                self.check_execution_deadline()?;
                let cursor = match (self.active_write_tx(), self.request_read_view()) {
                    (Some(active), _) => active
                        .txn
                        .scan_prefix(prefix, ..)
                        .await
                        .map_err(crate::HelixDbError::from)?,
                    (None, Some(view)) => view
                        .scan_prefix(prefix, ..)
                        .await
                        .map_err(crate::HelixDbError::from)?,
                    (None, None) => {
                        return Err(crate::HelixDbError::InvariantViolation(
                            "node cursor has no request snapshot".into(),
                        )
                        .into())
                    }
                };
                self.row_budget()
                    .record_reads(crate::cypher::StorageReadUsage {
                        scans: 1,
                        ..Default::default()
                    });
                return Ok(Some(NodeCursor::Scan(cursor)));
            }
            exec::ExecNodeAccessPlan::LabelScan { label } => (
                self.lookup_equality_index_set(
                    "$label",
                    &crate::encoding::v2::values::property::property_value::PropertyValue::String(
                        label.as_ref().to_owned(),
                    ),
                )
                .await?,
                true,
            ),
            exec::ExecNodeAccessPlan::Bitmap { bitmap } => (self.node_bitmap(bitmap).await?, false),
            exec::ExecNodeAccessPlan::Unique {
                lookup,
                verification,
            } => (
                self.verified_node_unique_owner(lookup, verification)
                    .await?
                    .into_iter()
                    .collect(),
                false,
            ),
            exec::ExecNodeAccessPlan::Empty => (roaring::RoaringTreemap::new(), false),
            exec::ExecNodeAccessPlan::FromParam { .. }
            | exec::ExecNodeAccessPlan::FromVar { .. }
            | exec::ExecNodeAccessPlan::AuthoritativeScan { .. }
            | exec::ExecNodeAccessPlan::DynamicEquality { .. }
            | exec::ExecNodeAccessPlan::DynamicMembership { .. }
            | exec::ExecNodeAccessPlan::RangeIndex { .. }
            | exec::ExecNodeAccessPlan::SecondarySet { .. }
            | exec::ExecNodeAccessPlan::VectorSearch { .. }
            | exec::ExecNodeAccessPlan::TextSearch { .. } => return Ok(None),
        };
        // Roaring exposes allocated container capacity. Include conservative
        // container-vector and treemap-node overhead as well as payload bytes.
        let memory = self.row_budget().reserve(memory::bitmap_bytes(&bitmap))?;
        Ok(Some(NodeCursor::Indexed {
            ids: bitmap.into_iter(),
            verify_existence,
            _memory: memory,
        }))
    }

    pub(super) fn node_id_batches<'a>(
        &'a self,
        cursor: NodeCursor,
        width: usize,
        slot: r::Slot,
        limits: Limits,
    ) -> impl futures::Stream<Item = Result<memory::Rows>> + 'a {
        futures::stream::try_unfold(cursor, move |mut cursor| async move {
            loop {
                self.check_execution_deadline()?;
                let _ids_memory = self
                    .row_budget()
                    .reserve(limits.batch_rows.saturating_mul(size_of::<u64>()))?;
                let mut ids = Vec::with_capacity(limits.batch_rows);
                match &mut cursor {
                    NodeCursor::Scan(cursor) => {
                        while ids.len() < limits.batch_rows {
                            let Some(row) =
                                cursor.next().await.map_err(crate::HelixDbError::from)?
                            else {
                                break;
                            };
                            self.check_execution_deadline()?;
                            self.row_budget()
                                .record_reads(crate::cypher::StorageReadUsage {
                                    scan_rows: 1,
                                    ..Default::default()
                                });
                            let _raw_memory = self
                                .row_budget()
                                .reserve(row.key.len().saturating_add(row.value.len()))?;
                            let Some(key) = self.tenant_scope.strip_key(&row.key) else {
                                return Err(crate::HelixDbError::InvariantViolation(
                                    "node scan escaped its tenant prefix".into(),
                                )
                                .into());
                            };
                            let Ok(keys::DataKeyKind::NodeProperty(key)) =
                                keys::DataKeyKind::parse_from_slice(key)
                            else {
                                continue;
                            };
                            ids.push(key.node_id());
                        }
                    }
                    NodeCursor::Indexed {
                        ids: source,
                        verify_existence,
                        ..
                    } => {
                        ids.extend(source.by_ref().take(limits.batch_rows));
                        if *verify_existence && !ids.is_empty() {
                            ids = self
                                .node_row_vec(ids)
                                .await?
                                .into_iter()
                                .filter_map(|row| {
                                    let Some(super::super::ElementRef::Node(id)) = row.current
                                    else {
                                        return None;
                                    };
                                    Some(id)
                                })
                                .collect();
                            // Missing legacy label-index owners do not end the cursor.
                            if ids.is_empty() {
                                continue;
                            }
                        }
                    }
                }
                if ids.is_empty() {
                    return Ok(None);
                }
                let mut candidates = RowBuffer::new(self.row_budget())?;
                for id in ids {
                    let mut row = vec![r::Value::Null; width];
                    row[slot.0 as usize] = r::Value::Entity(r::Entity::Node(id));
                    push_row(&mut candidates, row, limits)?;
                }

                return Ok(Some((candidates.finish(), cursor)));
            }
        })
    }
}
