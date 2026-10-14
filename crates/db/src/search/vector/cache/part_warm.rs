//! One-shot warm of vector search rows into SlateDB's object-store tier.
//!
//! A search reads layer-0 neighbour rows, payloads and SimHash directory
//! entries at random. With a cold object-store tier, the first touch of each
//! part is a whole-part download inside the search's chain of dependent
//! reads, so the first searches after a restart wait for the downloads one by
//! one. Streaming those rows once after open moves the downloads off the
//! query path. Rows are discarded and never enter the block cache: only the
//! object-store tier fills, and later searches read the parts locally.
//!
//! Known limits: node and edge records (read to check hits and to project
//! them) are not warmed; neither are entry-candidate rows, which a search
//! reads only to recover from a deleted entry point; generations that become
//! Active after the warm are not warmed; and parts replaced by a later
//! compaction are cold again unless the writer caches the SSTs it writes.

use slatedb::DbReadOps;

use crate::search::vector::storage::{PartWarm, PartWarmFailure, VectorRowKeyspace, VectorRows};
use crate::search::vector::ValidatedVectorGenerationHandle;

/// What one warm pass read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct VectorPartWarmSummary {
    /// Targets warmed completely, or up to the budget.
    pub(crate) warmed_targets: usize,
    /// Targets whose rows could not be read; each was logged and skipped.
    pub(crate) failed_targets: usize,
    /// Bytes charged over every target, failed ones included (see
    /// [`PartWarm`]), and whether the budget ran out.
    pub(crate) read: PartWarm,
}

/// Streams the search rows of every target through `read`, in target order,
/// until they are all read or `budget` bytes have been charged.
///
/// Best effort: a target that fails is logged, charged what it read, and
/// skipped, and the pass goes on with the next one. SlateDB reads
/// `read_ahead` bytes, one object-store part, ahead of each scan.
pub(crate) async fn warm_object_store_parts<R>(
    read: &R,
    targets: &[ValidatedVectorGenerationHandle],
    read_ahead: usize,
    budget: u64,
) -> VectorPartWarmSummary
where
    R: DbReadOps + Send + Sync + ?Sized,
{
    let mut summary = VectorPartWarmSummary {
        warmed_targets: 0,
        failed_targets: 0,
        read: PartWarm::Complete(0),
    };
    for target in targets {
        let PartWarm::Complete(charged) = summary.read else {
            break;
        };
        let keyspace = VectorRowKeyspace::from_allocated(
            target.physical_name().to_owned(),
            target.identity().physical_index_id(),
            target.scope(),
        );
        let warmed = VectorRows::new(read, &keyspace)
            .warm_object_store_parts(
                target.has_simhash_directory(),
                read_ahead,
                budget.saturating_sub(charged),
            )
            .await;
        summary.read = match warmed {
            Ok(PartWarm::Complete(target_charged)) => {
                summary.warmed_targets += 1;
                PartWarm::Complete(charged.saturating_add(target_charged))
            }
            Ok(PartWarm::BudgetExhausted(target_charged)) => {
                summary.warmed_targets += 1;
                PartWarm::BudgetExhausted(charged.saturating_add(target_charged))
            }
            Err(PartWarmFailure {
                charged: target_charged,
                error,
            }) => {
                tracing::warn!(
                    physical_index_id = target.physical_index_id(),
                    %error,
                    "skipping a vector index the object-store warm could not read"
                );
                summary.failed_targets += 1;
                let charged = charged.saturating_add(target_charged);
                match charged < budget {
                    true => PartWarm::Complete(charged),
                    false => PartWarm::BudgetExhausted(charged),
                }
            }
        };
    }
    summary
}
