//! Admission for authoritative property rows pending in a write transaction.
//! A replacement releases only the transaction's old reference; index consumers
//! may still own that version. Tombstones retain bookkeeping but no payload.
use crate::{
    encoding::v2::keys::scope::DataScope,
    error::Result,
    index_lifecycle::graph_mutation::GraphEntity,
    query_resources::{self, properties},
};
use std::{collections::BTreeMap, sync::Arc};

#[derive(Default)]
pub(super) struct Pending(Option<Admitted>);
impl std::fmt::Debug for Pending {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PendingPropertyWrites")
            .field(
                "entries",
                &self.0.as_ref().map_or(0, |state| state.rows.len()),
            )
            .finish()
    }
}
struct Admitted {
    rows: BTreeMap<GraphEntity, Option<Arc<query_resources::Reservation>>>,
    memory: query_resources::Reservation,
}
impl Pending {
    /// Admit before building a key or changing the backend write batch. Guards
    /// are installed after the backend accepts the write, while the previous
    /// version remains admitted throughout replacement.
    pub(super) fn stage(
        &mut self,
        transaction: &slatedb::DbTransaction,
        scope: DataScope,
        entity: GraphEntity,
        value: Option<properties::Encoded>,
        budget: Option<&query_resources::Budget>,
    ) -> Result<()> {
        self.admit(scope, entity, budget)?;
        let key = entity.property_key(scope);
        let memory = match value {
            Some(value) => {
                transaction.put_bytes(key, value.bytes)?;
                value.memory
            }
            None => {
                transaction.delete(key)?;
                None
            }
        };
        let Some(admitted) = &mut self.0 else {
            return Ok(());
        };
        admitted.rows.insert(entity, memory);
        Ok(())
    }
    fn admit(
        &mut self,
        scope: DataScope,
        entity: GraphEntity,
        budget: Option<&query_resources::Budget>,
    ) -> Result<()> {
        let Some(budget) = budget else {
            return Ok(());
        };
        let admitted = match &mut self.0 {
            Some(admitted) => admitted,
            slot @ None => slot.insert(Admitted {
                rows: BTreeMap::new(),
                memory: budget.reserve(0)?,
            }),
        };
        if !admitted.rows.contains_key(&entity) {
            use helix_planner::relational::allocation;
            let count = admitted.rows.len().saturating_add(1);
            // The pinned backend stores a B-tree of Bytes keys and one
            // inline WriteOp (value handle + PutOptions) per property key,
            // then clones it at commit. Sixteen words cover the operation
            // and SmallVec headers. Key bytes, sharing headers, and the
            // copying delete API receive a separate per-key allowance.
            // Index/topology batches and backend read tracking are separate
            // owners, not claimed by this property-row ledger.
            let bytes = allocation::btree_bytes::<
                GraphEntity,
                Option<Arc<query_resources::Reservation>>,
            >(count)
            .saturating_add(
                allocation::btree_bytes::<bytes::Bytes, [usize; 16]>(count).saturating_mul(2),
            )
            .saturating_add(
                count.saturating_mul(
                    entity
                        .property_key_len(scope)
                        .saturating_mul(2)
                        .saturating_add(size_of::<[usize; 8]>()),
                ),
            );
            admitted.memory.resize(bytes)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
