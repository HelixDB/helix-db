//! Read-only audit of the label bitmaps that label scans and index builds
//! read in place of the rows.
//!
//! Every node and edge write keeps its label's bitmap in the same
//! transaction as its row, so each bitmap should hold every row carrying its
//! label. A row missing from its bitmap is invisible to label scans and to
//! index builds; a member whose row is gone or carries another label is
//! harmless, since every reader checks the row. [`audit`] compares the two
//! at one snapshot and changes nothing: repairing a bitmap would be a change
//! to stored data.
//!
//! It reads the bitmaps that snapshot's reads and builds read: the hash rows
//! until the label identity copy publishes, the exact rows after
//! ([`super::label_source::label_layout`]). Exact rows hold one label each;
//! a hash row is shared by every label with its hash, so its other labels'
//! rows count as extra there.

use std::collections::BTreeMap;

use roaring::RoaringTreemap;
use slatedb::DbReadOps;

use crate::encoding::v2::keys::scope::DataScope;
use crate::encoding::v2::values::property::view;
use crate::error::Result;
use crate::index_lifecycle::IndexElementKind;

/// How one label's bitmap compares with the rows carrying the label.
#[derive(Debug, Clone, PartialEq)]
pub struct LabelBitmapFinding {
    /// Whether the label is a node or an edge label.
    pub kind: IndexElementKind,
    /// The label.
    pub label: String,
    /// Rows carrying the label.
    pub rows: u64,
    /// Ids of rows carrying the label that its bitmap lacks: label scans and
    /// index builds miss them.
    pub missing: RoaringTreemap,
    /// Bitmap members whose row is gone or carries another label, which every
    /// reader skips. While the store reads hash rows, rows of a label whose
    /// hash equals this one's count here.
    pub extra: u64,
}

/// Every label's bitmap compared with the rows carrying the label, at one
/// snapshot.
///
/// ```
/// use db::index_lifecycle::LabelBitmapAudit;
///
/// // A store with no labelled row has nothing missing.
/// assert!(LabelBitmapAudit::default().is_complete());
/// ```
#[derive(Debug, Clone, PartialEq, Default)]
pub struct LabelBitmapAudit {
    /// One finding per label a row carries: node labels, then edge labels,
    /// each in label order.
    pub findings: Vec<LabelBitmapFinding>,
    /// Rows whose properties could not be decoded, so their label is unknown.
    pub unreadable_rows: u64,
}

impl LabelBitmapAudit {
    /// Whether every row carrying a label is in that label's bitmap.
    pub fn is_complete(&self) -> bool {
        self.findings
            .iter()
            .all(|finding| finding.missing.is_empty())
    }
}

/// Compares each label bitmap of `scope` with the rows `reader` holds, in
/// `reader`'s own label layout.
///
/// Reads every node and edge row of the scope once, decoding only `$label`,
/// and one bitmap per label carried; holds one id set per label. `reader`
/// should be a snapshot, or concurrent writes show as differences.
pub(crate) async fn audit(
    reader: &(impl DbReadOps + Send + Sync),
    scope: DataScope,
) -> Result<LabelBitmapAudit> {
    let layout = super::label_source::label_layout(reader).await?;
    let mut audit = LabelBitmapAudit::default();
    let mut scratch = view::Scratch::new();
    for kind in [IndexElementKind::Node, IndexElementKind::Edge] {
        let mut labels = BTreeMap::<String, RoaringTreemap>::new();
        // The audit reads every row once: it leaves the block cache to queries.
        let mut rows = reader
            .scan_prefix_with_options(
                super::secondary::source_prefix(scope, kind),
                ..,
                &slatedb::config::ScanOptions::default().with_cache_blocks(false),
            )
            .await?;
        while let Some(row) = rows.next().await? {
            // A retired `[from][to]` edge row is no entity.
            let Some(id) = super::secondary::source_entity(scope, kind, &row.key)? else {
                continue;
            };
            let Ok(properties) =
                view::decode_selected(&row.value, &mut scratch, |name| name == "$label")
            else {
                audit.unreadable_rows += 1;
                continue;
            };
            let Some(label) = properties
                .iter()
                .find_map(|property| property.value.as_str())
            else {
                continue;
            };
            labels.entry(label.to_owned()).or_default().insert(id.get());
        }
        for (label, ids) in labels {
            let members =
                super::label_source::label_members(reader, layout, kind, &label, scope).await?;
            audit.findings.push(LabelBitmapFinding {
                kind,
                rows: ids.len(),
                missing: &ids - &members,
                extra: (&members - &ids).len(),
                label,
            });
        }
    }
    Ok(audit)
}

#[cfg(test)]
#[path = "../../tests/unit/index_lifecycle_label_audit.rs"]
mod tests;
