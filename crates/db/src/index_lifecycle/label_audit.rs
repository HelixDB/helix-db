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
    /// reader skips. Rows of a label whose hash equals this one's count here.
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

/// Compares each label bitmap of `scope` with the rows `reader` holds.
///
/// Reads every node and edge row of the scope once, decoding only `$label`,
/// and one bitmap per label carried; holds one id set per label. `reader`
/// should be a snapshot, or concurrent writes show as differences.
pub(crate) async fn audit(
    reader: &(impl DbReadOps + Send + Sync),
    scope: DataScope,
) -> Result<LabelBitmapAudit> {
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
            let members = match kind {
                IndexElementKind::Node => {
                    crate::search::lookup_node_label_index_scoped(reader, &label, scope).await?
                }
                IndexElementKind::Edge => {
                    crate::search::lookup_global_edge_label_index_scoped(reader, &label, scope)
                        .await?
                }
            };
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
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use slatedb::object_store::memory::InMemory;
    use slatedb::{Db, IsolationLevel};

    use super::*;
    use crate::encoding::property::Property;
    use crate::encoding::v2::keys::{DataKey, DataKeyKind};
    use crate::encoding::v2::legacy::edge_property_pair::LegacyEdgePropertyPairKey;
    use crate::index_lifecycle::label_source::testing;

    async fn test_db(name: &str) -> Db {
        Db::builder(name, Arc::new(InMemory::new()))
            .with_merge_operator(Arc::new(crate::merge_operator::HelixMergeOperator::new()))
            .build()
            .await
            .expect("audit test database opens")
    }

    fn labelled(label: &str) -> Vec<Property> {
        vec![
            Property::string("$label", label),
            Property::string("name", "x"),
        ]
    }

    async fn run(db: &Db, scope: DataScope) -> LabelBitmapAudit {
        let snapshot = db.snapshot().await.expect("snapshot opens");
        audit(snapshot.as_ref(), scope).await.expect("audit reads")
    }

    async fn commit(db: &Db, stage: impl FnOnce(&slatedb::DbTransaction)) {
        let transaction = db
            .begin(IsolationLevel::Snapshot)
            .await
            .expect("write begins");
        stage(&transaction);
        transaction.commit().await.expect("write commits");
    }

    fn finding<'a>(audit: &'a LabelBitmapAudit, kind: IndexElementKind, label: &str) -> &'a LabelBitmapFinding {
        audit
            .findings
            .iter()
            .find(|finding| finding.kind == kind && finding.label == label)
            .expect("the label has a finding")
    }

    #[tokio::test]
    async fn a_store_whose_rows_are_all_in_their_bitmaps_is_complete() {
        let db = test_db("label-audit-clean").await;
        let scope = DataScope::LegacyUnscoped;
        for id in [1, 2, 3] {
            testing::put_source_row(&db, scope, IndexElementKind::Node, id, &labelled("User"))
                .await;
        }
        testing::put_source_row(&db, scope, IndexElementKind::Node, 4, &labelled("Post")).await;
        testing::put_source_row(&db, scope, IndexElementKind::Edge, 4, &labelled("FOLLOWS"))
            .await;
        // A retired `[from][to]` edge row is no entity and is not audited.
        db.put(
            DataKey::Data {
                scope,
                kind: DataKeyKind::EdgePropertyPair(LegacyEdgePropertyPairKey::new(1, 2)),
            }
            .to_bytes(),
            crate::encoding::v2::values::property::encode_properties(&labelled("FOLLOWS")),
        )
        .await
        .expect("legacy row writes");

        let audit = run(&db, scope).await;
        assert!(audit.is_complete());
        assert_eq!(audit.unreadable_rows, 0);
        assert_eq!(
            audit
                .findings
                .iter()
                .map(|finding| (finding.kind, finding.label.as_str(), finding.rows))
                .collect::<Vec<_>>(),
            [
                (IndexElementKind::Node, "Post", 1),
                (IndexElementKind::Node, "User", 3),
                (IndexElementKind::Edge, "FOLLOWS", 1),
            ]
        );
        assert!(audit.findings.iter().all(|finding| finding.extra == 0));
    }

    #[tokio::test]
    async fn rows_missing_from_their_bitmap_and_stale_members_are_reported() {
        let db = test_db("label-audit-gaps").await;
        let scope = DataScope::LegacyUnscoped;
        for id in [1, 2, 3] {
            testing::put_source_row(&db, scope, IndexElementKind::Node, id, &labelled("User"))
                .await;
        }
        testing::put_source_row(&db, scope, IndexElementKind::Edge, 7, &labelled("LIKES")).await;
        commit(&db, |transaction| {
            testing::stage_departure(transaction, scope, IndexElementKind::Node, "User", 2);
            testing::stage_departure(transaction, scope, IndexElementKind::Edge, "LIKES", 7);
            // A member without a row, and one whose row carries another label.
            testing::stage_member(transaction, scope, IndexElementKind::Node, "User", 99);
            testing::stage_member(transaction, scope, IndexElementKind::Node, "User", 5);
        })
        .await;
        testing::put_source_row(&db, scope, IndexElementKind::Node, 5, &labelled("Post")).await;
        testing::put_raw_source_row(
            &db,
            scope,
            IndexElementKind::Node,
            6,
            "User",
            Bytes::from_static(b"not a property row"),
        )
        .await;

        let audit = run(&db, scope).await;
        assert!(!audit.is_complete());
        assert_eq!(audit.unreadable_rows, 1);
        let user = finding(&audit, IndexElementKind::Node, "User");
        assert_eq!(user.rows, 3);
        assert_eq!(user.missing.iter().collect::<Vec<_>>(), [2]);
        assert_eq!(user.extra, 3, "99, 5 and the unreadable 6");
        let likes = finding(&audit, IndexElementKind::Edge, "LIKES");
        assert_eq!(likes.missing.iter().collect::<Vec<_>>(), [7]);
        assert!(finding(&audit, IndexElementKind::Node, "Post").missing.is_empty());
    }

    #[tokio::test]
    async fn the_audit_reads_only_its_scope() {
        let db = test_db("label-audit-scopes").await;
        let tenant = DataScope::Tenant(crate::encoding::v2::keys::scope::TenantId::from_u128(7));
        testing::put_source_row(&db, tenant, IndexElementKind::Node, 1, &labelled("User")).await;
        commit(&db, |transaction| {
            testing::stage_departure(transaction, tenant, IndexElementKind::Node, "User", 1);
        })
        .await;
        testing::put_source_row(
            &db,
            DataScope::LegacyUnscoped,
            IndexElementKind::Node,
            1,
            &labelled("User"),
        )
        .await;

        assert!(run(&db, DataScope::LegacyUnscoped).await.is_complete());
        let tenant_audit = run(&db, tenant).await;
        assert_eq!(
            finding(&tenant_audit, IndexElementKind::Node, "User")
                .missing
                .iter()
                .collect::<Vec<_>>(),
            [1]
        );
    }
}
