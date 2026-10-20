//! Relationship scans bind an unbound typed relationship and both endpoints
//! from the edge bitmaps of its types. Every parent row replays the shared
//! bitmaps; each batch of edge IDs reads its stored endpoints with one multi-get.
use super::{memory, row_bytes, ExecutionContext, Limits, Result, RowBuffer};
use crate::encoding::indexes;
use crate::query_resources::bitmap;
use helix_planner::relational as r;
use std::collections::BTreeMap;
use std::sync::Arc;

impl ExecutionContext<'_> {
    /// The nonempty edge bitmaps of the relationship's types, one per stored
    /// label key. An edge has one type, so the bitmaps are disjoint and are
    /// replayed in turn rather than combined. A topology flush makes staged
    /// edges visible, and deletes remove label memberships in the same
    /// transaction, so they name exactly the edges a typed expansion reaches.
    /// Only the label key's type hash is trusted, as native label scans do.
    pub(super) async fn relationship_scan_ids(
        &self,
        relationship: &r::RelationshipPattern,
    ) -> Result<Arc<[bitmap::Bitmap]>> {
        let types = relationship
            .types
            .iter()
            .map(|label| (indexes::hash_property_value(label), label))
            .collect::<BTreeMap<_, _>>();
        let mut ids = Vec::with_capacity(types.len());
        for label in types.into_values() {
            self.check_execution_deadline()?;
            let edges = self.lookup_global_edge_label_index(label).await?;
            if !edges.is_empty() {
                ids.push(edges);
            }
        }
        Ok(ids.into())
    }

    /// Bind every scanned edge to each parent row, parent by parent. The first
    /// batch reads `first` edges and later ones double up to the batch width.
    /// An outgoing pattern binds `from` to the stored source, an incoming one
    /// to the stored target, and an undirected one binds both orientations of
    /// every edge but a self-loop. An edge whose endpoints are gone is skipped.
    pub(super) fn relationship_scan_batches<'a>(
        &'a self,
        rows: memory::Rows,
        pattern: &'a r::Pattern,
        relationship: usize,
        ids: Arc<[bitmap::Bitmap]>,
        first: usize,
        limits: Limits,
    ) -> impl futures::Stream<Item = Result<memory::Rows>> + 'a {
        let relationship = &pattern.relationships[relationship];
        let (from, to, slot) = (
            relationship.from.0 as usize,
            relationship.to.0 as usize,
            relationship.slot.0 as usize,
        );
        // The current parent's next bitmap and the lowest edge ID it has not
        // read there. Passing the last bitmap ends the parent.
        futures::stream::try_unfold(
            (
                rows,
                ids,
                0_usize,
                0_usize,
                0_u64,
                first.clamp(1, limits.batch_rows),
            ),
            move |(mut rows, ids, mut parent, mut kind, mut start, mut batch)| {
                // Only an active poll owns the async storage-operation state.
                let poll = async move {
                    loop {
                        self.check_execution_deadline()?;
                        if parent == rows.len() {
                            return Ok(None);
                        }
                        let Some(kind_ids) = ids.get(kind) else {
                            rows.release_rows(parent..parent + 1);
                            (parent, kind, start) = (parent + 1, 0, 0);
                            continue;
                        };
                        // Includes edge IDs, endpoint keys and decoded endpoints.
                        // Admit before Vec growth.
                        let _batch_memory = self.row_budget().reserve(batch.saturating_mul(128))?;
                        // Collecting would size the vector from the bitmap's size
                        // hint, which visits every remaining container.
                        let mut edges = Vec::with_capacity(batch);
                        let mut remaining = kind_ids.iter();
                        remaining.advance_to(start);
                        edges.extend(remaining.take(batch));
                        // A short batch ends this bitmap.
                        (kind, start) = match edges.last().and_then(|last| last.checked_add(1)) {
                            Some(next) if edges.len() == batch => (kind, next),
                            Some(_) | None => (kind + 1, 0),
                        };
                        if edges.is_empty() {
                            continue;
                        }
                        let endpoints = self
                            .row_budget()
                            .admitted_future(self.edge_endpoints_batch(&edges))?
                            .await?;
                        assert_eq!(endpoints.len(), edges.len(), "one endpoint result per edge");
                        let row = &rows[parent];
                        assert!(
                            [from, to, slot]
                                .iter()
                                .all(|slot| row[*slot] == r::Value::Null),
                            "a relationship scan binds unbound slots"
                        );
                        let mut output = RowBuffer::new(self.row_budget())?;
                        for (id, endpoints) in edges.into_iter().zip(endpoints) {
                            let Some((source, target)) = endpoints else {
                                continue;
                            };
                            let edge = r::Value::Entity(r::Entity::Relationship(id));
                            if pattern
                                .relationships
                                .iter()
                                .any(|other| row[other.slot.0 as usize] == edge)
                            {
                                continue;
                            }
                            let forward = (relationship.direction != r::Direction::Incoming)
                                .then_some((source, target));
                            let backward = (relationship.direction == r::Direction::Incoming
                                || (relationship.direction == r::Direction::Undirected
                                    && source != target))
                                .then_some((target, source));
                            for (start_node, end_node) in forward.into_iter().chain(backward) {
                                // One variable at both ends matches only self-loops.
                                if from == to && start_node != end_node {
                                    continue;
                                }
                                // Bound slots hold null; admit the parent payload
                                // before copying unrelated nested values.
                                output.push_with(row_bytes(row), || {
                                    let mut next = row.clone();
                                    next[from] = r::Value::Entity(r::Entity::Node(start_node));
                                    next[to] = r::Value::Entity(r::Entity::Node(end_node));
                                    next[slot] = edge.clone();
                                    next
                                })?;
                            }
                        }
                        batch = batch.saturating_mul(2).min(limits.batch_rows);
                        if output.len() > 0 {
                            return Ok(Some((
                                output.finish(),
                                (rows, ids, parent, kind, start, batch),
                            )));
                        }
                    }
                };
                match self.row_budget().admitted_future(poll) {
                    Ok(poll) => futures::future::Either::Left(poll),
                    Err(error) => futures::future::Either::Right(async move { Err(error.into()) }),
                }
            },
        )
    }
}
