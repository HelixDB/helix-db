//! Incremental fixed-length expansion shared by native traversal and graph rows.
//! Index values are decoded on demand; stopping a cursor releases unread values,
//! compressed neighbors and their reservations. Errors consume the cursor, so a
//! partially advanced storage batch cannot be retried as a valid continuation.
use super::super::{ExecutionContext, Result};
use crate::encoding::keys;
use crate::encoding::keys::indexes::EdgeDirection;
use crate::query_resources::{adjacency, bitmap, Reservation};
use helix_planner::ir;

pub(in crate::execution::interpreter) struct Cursor {
    pairs: Pairs,
    values: std::vec::IntoIter<Option<bytes::Bytes>>,
    current: Option<bitmap::IntoIter>,
    input_memory: Option<Reservation>,
    batch_size: usize,
}

enum Pairs {
    Empty,
    #[cfg(test)]
    Fixture(std::vec::IntoIter<(u64, u64)>),
    Adjacency {
        source: u64,
        neighbors: adjacency::IntoNeighbors,
        both: bool,
    },
    Labeled {
        source: u64,
        outgoing: Option<bitmap::IntoIter>,
        incoming: Option<bitmap::IntoIter>,
        both: bool,
    },
}

impl Iterator for Pairs {
    type Item = (u64, u64);

    fn next(&mut self) -> Option<Self::Item> {
        let (source, direction, neighbor, both) = match self {
            Self::Empty => return None,
            #[cfg(test)]
            Self::Fixture(pairs) => return pairs.next(),
            Self::Adjacency {
                source,
                neighbors,
                both,
            } => {
                let (direction, neighbor) = neighbors.next()?;
                (*source, direction, neighbor, *both)
            }
            Self::Labeled {
                source,
                outgoing,
                incoming,
                both,
            } => match outgoing.as_mut().and_then(Iterator::next) {
                Some(neighbor) => (*source, EdgeDirection::Out, neighbor, *both),
                None => (
                    *source,
                    EdgeDirection::In,
                    incoming.as_mut().and_then(Iterator::next)?,
                    *both,
                ),
            },
        };
        match direction {
            EdgeDirection::Out => Some((source, neighbor)),
            // A self-loop's pair occurs in both neighbor directions. Skip the
            // second occurrence; parallel edges in its bitmap remain distinct.
            EdgeDirection::In if both && source == neighbor => self.next(),
            EdgeDirection::In => Some((neighbor, source)),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let upper = match self {
            Self::Empty => Some(0),
            #[cfg(test)]
            Self::Fixture(pairs) => pairs.size_hint().1,
            Self::Adjacency { neighbors, .. } => neighbors.size_hint().1,
            Self::Labeled {
                outgoing, incoming, ..
            } => outgoing
                .as_ref()
                .map_or(Some(0), |ids| ids.size_hint().1)
                .zip(incoming.as_ref().map_or(Some(0), |ids| ids.size_hint().1))
                .and_then(|(a, b)| a.checked_add(b)),
        };
        (0, upper)
    }
}

pub(in crate::execution::interpreter) struct IdBatch {
    ids: Vec<u64>,
    _memory: Option<Reservation>,
}

impl IdBatch {
    pub(in crate::execution::interpreter) fn ids(&self) -> &[u64] {
        &self.ids
    }
}

impl Cursor {
    #[cfg(test)]
    pub(in crate::execution::interpreter) fn from_pairs(
        pairs: Vec<(u64, u64)>,
        batch_size: usize,
    ) -> Self {
        Self {
            pairs: Pairs::Fixture(pairs.into_iter()),
            ..Self::empty(batch_size)
        }
    }

    pub(in crate::execution::interpreter) async fn collect(
        mut self,
        context: &ExecutionContext<'_>,
        filter: Option<&roaring::RoaringTreemap>,
    ) -> Result<bitmap::Bitmap> {
        let mut output = bitmap::Builder::new(context.row_memory.as_ref())?;
        loop {
            let Some((batch, next)) = Box::pin(self.next_batch(context)).await? else {
                break;
            };
            self = next;
            for id in batch.ids() {
                if filter.is_none_or(|filter| filter.contains(*id)) {
                    output.insert(*id)?;
                }
            }
        }
        Ok(output.finish())
    }

    pub(in crate::execution::interpreter) fn empty(batch_size: usize) -> Self {
        Self {
            pairs: Pairs::Empty,
            values: Vec::new().into_iter(),
            current: None,
            input_memory: None,
            batch_size: batch_size.clamp(1, 512),
        }
    }

    pub(in crate::execution::interpreter) async fn new(
        context: &ExecutionContext<'_>,
        source: u64,
        direction: ir::ExpandDirection,
        label: &ir::ExpandLabelPlan,
        target: Option<u64>,
        batch_size: usize,
    ) -> Result<Self> {
        context.check_execution_deadline()?;
        let mut cursor = Self::empty(batch_size);
        let Some(target) = target else {
            let both = direction == ir::ExpandDirection::Both;
            cursor.pairs = match label {
                ir::ExpandLabelPlan::Any => {
                    let key = context.storage_key(keys::DataKeyKind::Adjacency(
                        keys::AdjacencyKey::new(source),
                    ));
                    let Some(value) = context.get_raw(&key).await? else {
                        return Ok(cursor);
                    };
                    let selected = match direction {
                        ir::ExpandDirection::Out => Some(EdgeDirection::Out),
                        ir::ExpandDirection::In => Some(EdgeDirection::In),
                        ir::ExpandDirection::Both => None,
                    };
                    Pairs::Adjacency {
                        source,
                        neighbors: adjacency::Adjacency::decode(
                            &value,
                            context.row_memory.as_ref(),
                        )?
                        .into_neighbors(selected),
                        both,
                    }
                }
                ir::ExpandLabelPlan::Label(label) => {
                    let outgoing = if direction != ir::ExpandDirection::In {
                        Some(
                            context
                                .lookup_out_neighbors_by_label(source, label.as_ref())
                                .await?
                                .into_iter(),
                        )
                    } else {
                        None
                    };
                    let incoming = if direction != ir::ExpandDirection::Out {
                        Some(
                            context
                                .lookup_in_neighbors_by_label(source, label.as_ref())
                                .await?
                                .into_iter(),
                        )
                    } else {
                        None
                    };
                    Pairs::Labeled {
                        source,
                        outgoing,
                        incoming,
                        both,
                    }
                }
            };
            return Ok(cursor);
        };
        let ids = match direction {
            ir::ExpandDirection::Out => context.lookup_edge_pair_index(source, target).await?,
            ir::ExpandDirection::In => context.lookup_edge_pair_index(target, source).await?,
            ir::ExpandDirection::Both => {
                let forward = context.lookup_edge_pair_index(source, target).await?;
                if source == target {
                    forward
                } else {
                    forward.union(context.lookup_edge_pair_index(target, source).await?)?
                }
            }
        };
        cursor.current = Some(ids.into_iter());
        Ok(cursor)
    }

    pub(in crate::execution::interpreter) async fn next_batch(
        mut self,
        context: &ExecutionContext<'_>,
    ) -> Result<Option<(IdBatch, Self)>> {
        context.check_execution_deadline()?;
        let memory = context
            .row_memory
            .as_ref()
            .map(|budget| {
                budget.reserve(
                    size_of::<IdBatch>().saturating_add(self.batch_size * size_of::<u64>()),
                )
            })
            .transpose()?;
        let mut output = IdBatch {
            ids: Vec::with_capacity(self.batch_size),
            _memory: memory,
        };
        while output.ids.len() < self.batch_size {
            match self.current.as_mut().and_then(Iterator::next) {
                Some(id) => {
                    output.ids.push(id);
                    continue;
                }
                None => self.current = None,
            }
            match self.values.by_ref().flatten().next() {
                Some(value) => {
                    context.check_execution_deadline()?;
                    self.current = Some(
                        bitmap::Bitmap::decode_builtin(&value, context.row_memory.as_ref())?
                            .into_iter(),
                    );
                    continue;
                }
                None => self.input_memory = None,
            }
            let Some(first) = self.pairs.next() else {
                break;
            };
            context.check_execution_deadline()?;
            let count = 1 + self
                .pairs
                .size_hint()
                .1
                .unwrap_or(self.batch_size - 1)
                .min(self.batch_size - 1);
            let key_bytes = context.tenant_scope.encoded_len()
                + keys::EdgePairIndexKey::new(first.0, first.1).encoded_len();
            self.input_memory = context
                .row_memory
                .as_ref()
                .map(|budget| {
                    budget.reserve(
                        count.saturating_mul(key_bytes + 3 * size_of::<bytes::Bytes>() + 64),
                    )
                })
                .transpose()?;
            let mut keys = Vec::with_capacity(count);
            for (from, to) in
                std::iter::once(first).chain(self.pairs.by_ref().take(self.batch_size - 1))
            {
                keys.push(context.storage_key(keys::DataKeyKind::EdgePairIndex(
                    keys::EdgePairIndexKey::new(from, to),
                )));
            }
            self.values = context.multi_get_raw(&keys).await?.into_iter();
        }
        Ok((!output.ids.is_empty()).then_some((output, self)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::interpreter::test_support;

    #[tokio::test]
    async fn malformed_candidate_consumes_its_state_and_releases_all_admission() {
        let db = test_support::open_db("malformed-streamed-edge-candidate").await;
        let mut context =
            ExecutionContext::new(&db, helix_planner::context::ParamBindings::default());
        context.row_memory = Some(crate::query_resources::Budget::new(1024 * 1024));
        db.inner_db()
            .put(
                context.storage_key(keys::DataKeyKind::EdgePairIndex(
                    keys::EdgePairIndexKey::new(1, 2),
                )),
                bytes::Bytes::from_static(b"invalid bitmap"),
            )
            .await
            .unwrap();
        context.enable_request_read_view().await.unwrap();
        let cursor = Cursor::from_pairs(vec![(1, 2), (1, 3)], 1);
        assert!(Box::pin(cursor.next_batch(&context)).await.is_err());
        let budget = context.row_memory.as_ref().unwrap();
        assert_eq!(budget.available(), 1024 * 1024);
        assert_eq!(budget.reads().multi_get_keys, 1);
        assert_eq!(budget.reads().multi_get_batches, 1);
        context.fail_deadline_after(0);
        let cursor = Cursor::from_pairs(vec![(1, 3)], 1);
        assert!(Box::pin(cursor.next_batch(&context)).await.is_err());
        assert_eq!(budget.available(), 1024 * 1024);
        assert_eq!(budget.reads().multi_get_keys, 1);
        context.close_request_read_view().unwrap();
        db.close().await.unwrap();
    }
}
