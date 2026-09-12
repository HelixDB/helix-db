//! Bounded node batches from production-selected access primitives. The cursor
//! owns its bitmap or storage iterator; projection/aggregation owns downstream
//! state. It never reopens a scan to simulate pagination.
use super::{memory, ExecutionContext, Limits, Result, RowBuffer};
use crate::encoding::v2::keys;
use crate::query_resources::bitmap;
use helix_planner::{exec, relational as r};
use slatedb::DbReadOps;

pub(super) enum NodeCursor {
    Scan(slatedb::DbIterator),
    Indexed {
        ids: bitmap::IntoIter,
        verify_existence: bool,
    },
}

impl ExecutionContext<'_> {
    /// Only open a cursor when its exact selected primitive supports iteration.
    /// Other native access contracts continue through their existing executor.
    pub(super) async fn node_cursor(&self, operation: &exec::ExecOp) -> Result<Option<NodeCursor>> {
        let Some(source) = operation.node_cursor_access() else {
            return Ok(None);
        };
        let (bitmap, verify_existence) = match source {
            exec::ExecNodeCursor::AllScan => {
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
            exec::ExecNodeCursor::LabelScan { label } => (
                self.lookup_equality_index_set(
                    "$label",
                    &crate::encoding::v2::values::property::property_value::PropertyValue::String(
                        label.as_ref().to_owned(),
                    ),
                )
                .await?,
                true,
            ),
            exec::ExecNodeCursor::Bitmap { bitmap } => (self.node_bitmap(bitmap).await?, false),
            exec::ExecNodeCursor::Unique {
                lookup,
                verification,
            } => (
                match self
                    .verified_node_unique_owner(lookup, verification)
                    .await?
                {
                    Some(id) => bitmap::Bitmap::singleton(id, Some(self.row_budget()))?,
                    None => bitmap::Bitmap::empty(Some(self.row_budget()))?,
                },
                false,
            ),
            exec::ExecNodeCursor::Empty => (bitmap::Bitmap::empty(Some(self.row_budget()))?, false),
        };
        Ok(Some(NodeCursor::Indexed {
            ids: bitmap.into_iter(),
            verify_existence,
        }))
    }

    pub(super) fn node_id_batches<'a>(
        &'a self,
        cursor: NodeCursor,
        width: usize,
        slot: r::Slot,
        limits: Limits,
    ) -> impl futures::Stream<Item = Result<memory::Rows>> + 'a {
        futures::stream::try_unfold(cursor, move |cursor| {
            cursor.next_batch(self, width, slot, limits)
        })
    }
}

impl NodeCursor {
    /// Consume one continuation. An error drops its iterator and reservations;
    /// a successful batch returns the only owner of the remaining source.
    pub(super) async fn next_batch(
        mut self,
        context: &ExecutionContext<'_>,
        width: usize,
        slot: r::Slot,
        limits: Limits,
    ) -> Result<Option<(memory::Rows, Self)>> {
        let cursor = &mut self;
        // Existence probes and source ID buffers stay bounded even when the
        // downstream operator requests a larger row batch.
        let batch_rows = limits.batch_rows.min(512);
        assert!(batch_rows > 0, "execution validates nonzero batch widths");

        loop {
            context.check_execution_deadline()?;
            let _ids_memory = context
                .row_budget()
                .reserve(batch_rows.saturating_mul(size_of::<u64>()))?;
            let mut ids = Vec::with_capacity(batch_rows);
            match cursor {
                NodeCursor::Scan(cursor) => {
                    while ids.len() < batch_rows {
                        let Some(row) = cursor.next().await.map_err(crate::HelixDbError::from)?
                        else {
                            break;
                        };
                        context.check_execution_deadline()?;
                        context
                            .row_budget()
                            .record_reads(crate::cypher::StorageReadUsage {
                                scan_rows: 1,
                                ..Default::default()
                            });
                        let _raw_memory = context
                            .row_budget()
                            .reserve(row.key.len().saturating_add(row.value.len()))?;
                        let Some(key) = context.tenant_scope.strip_key(&row.key) else {
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
                    ids.extend(source.by_ref().take(batch_rows));
                    if *verify_existence && !ids.is_empty() {
                        // Only existence is needed here. Admit typed key
                        // payloads, Bytes handles/control blocks and returned
                        // handles before allocation; raw values retain their
                        // own storage guards. The pinned Bytes shared owner has
                        // a pointer, capacity and reference count (three words).
                        // Native traversal rows and paths
                        // would be discarded immediately after this check.
                        let key_bytes = context.tenant_scope.encoded_len()
                            + keys::NodePropertyKey::new(0).encoded_len();
                        let _probe_memory =
                            context.row_budget().reserve(ids.len().saturating_mul(
                                key_bytes
                                    + size_of::<bytes::Bytes>()
                                    + 3 * size_of::<usize>()
                                    + size_of::<Option<bytes::Bytes>>(),
                            ))?;
                        let keys = ids
                            .iter()
                            .map(|id| {
                                context.storage_key(keys::DataKeyKind::NodeProperty(
                                    keys::NodePropertyKey::new(*id),
                                ))
                            })
                            .collect::<Vec<_>>();
                        let values = context
                            .row_budget()
                            .admitted_future(context.multi_get_raw(&keys))?
                            .await?;
                        assert_eq!(values.len(), ids.len(), "one result per existence key");
                        let mut values = values.into_iter();
                        ids.retain(|_| values.next().expect("one result per ID").is_some());
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
            let mut candidates = RowBuffer::new(context.row_budget())?;
            for id in ids {
                candidates.push_with(
                    size_of::<r::Row>().saturating_add(width.saturating_mul(size_of::<r::Value>())),
                    || {
                        let mut row = vec![r::Value::Null; width];
                        row[slot.0 as usize] = r::Value::Entity(r::Entity::Node(id));
                        row
                    },
                )?;
            }

            return Ok(Some((candidates.finish(), self)));
        }
    }
}
