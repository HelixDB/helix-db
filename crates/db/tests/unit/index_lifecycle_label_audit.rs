//! Contracts for the read-only label bitmap audit.

use std::sync::Arc;

use bytes::Bytes;
use slatedb::object_store::memory::InMemory;
use slatedb::{Db, IsolationLevel};

use super::*;
use crate::encoding::property::Property;
use crate::encoding::v2::keys::{DataKey, DataKeyKind};
use crate::encoding::v2::legacy::edge_property_pair::LegacyEdgePropertyPairKey;
use crate::index_lifecycle::label_source::testing;
use crate::migrations::LabelStage;

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

fn finding<'a>(
    audit: &'a LabelBitmapAudit,
    kind: IndexElementKind,
    label: &str,
) -> &'a LabelBitmapFinding {
    audit
        .findings
        .iter()
        .find(|finding| finding.kind == kind && finding.label == label)
        .expect("the label has a finding")
}

/// Writes `store`'s rows at `stage` of the label identity migration: the
/// rows before the stage, then those after it, so each stage's bitmaps hold
/// rows written in both its layouts.
async fn staged_store(
    name: &str,
    stage: LabelStage,
    before: &[(IndexElementKind, u64, &str)],
    after: &[(IndexElementKind, u64, &str)],
) -> Db {
    let db = test_db(&format!("{name}-{stage:?}")).await;
    LabelStage::rewind(&db).await;
    for &(kind, id, label) in before {
        testing::put_source_row(&db, DataScope::LegacyUnscoped, kind, id, &labelled(label)).await;
    }
    stage.reach(&db).await;
    for &(kind, id, label) in after {
        testing::put_source_row(&db, DataScope::LegacyUnscoped, kind, id, &labelled(label)).await;
    }
    db
}

/// A store whose rows are all in their bitmaps is complete at every stage
/// of the label identity migration.
#[tokio::test]
async fn a_store_whose_rows_are_all_in_their_bitmaps_is_complete() {
    let scope = DataScope::LegacyUnscoped;
    let (node, edge) = (IndexElementKind::Node, IndexElementKind::Edge);
    for stage in LabelStage::ALL {
        let db = staged_store(
            "label-audit-clean",
            stage,
            &[(node, 1, "User"), (node, 2, "User"), (edge, 4, "FOLLOWS")],
            &[(node, 3, "User"), (node, 4, "Post"), (edge, 5, "FOLLOWS")],
        )
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
        assert!(audit.is_complete(), "{stage:?}: {audit:?}");
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
                (IndexElementKind::Edge, "FOLLOWS", 2),
            ],
            "{stage:?}"
        );
        assert!(
            audit.findings.iter().all(|finding| finding.extra == 0),
            "{stage:?}: {audit:?}"
        );
    }
}

/// Rows missing from the bitmaps a stage reads, and members that are no row
/// of the label, are reported at every stage of the label identity
/// migration.
#[tokio::test]
async fn rows_missing_from_their_bitmap_and_stale_members_are_reported() {
    let scope = DataScope::LegacyUnscoped;
    let (node, edge) = (IndexElementKind::Node, IndexElementKind::Edge);
    for stage in LabelStage::ALL {
        let db = staged_store(
            "label-audit-gaps",
            stage,
            &[(node, 1, "User"), (node, 2, "User"), (edge, 7, "LIKES")],
            &[(node, 3, "User")],
        )
        .await;
        let transaction = db
            .begin(IsolationLevel::Snapshot)
            .await
            .expect("write begins");
        testing::stage_departure(&transaction, scope, node, "User", 2).await;
        testing::stage_departure(&transaction, scope, edge, "LIKES", 7).await;
        // A member without a row, and one whose row carries another label.
        testing::stage_member(&transaction, scope, node, "User", 99).await;
        testing::stage_member(&transaction, scope, node, "User", 5).await;
        transaction.commit().await.expect("write commits");
        testing::put_source_row(&db, scope, node, 5, &labelled("Post")).await;
        testing::put_raw_source_row(
            &db,
            scope,
            node,
            6,
            "User",
            Bytes::from_static(b"not a property row"),
        )
        .await;

        let audit = run(&db, scope).await;
        assert!(!audit.is_complete(), "{stage:?}");
        assert_eq!(audit.unreadable_rows, 1);
        let user = finding(&audit, node, "User");
        assert_eq!(user.rows, 3);
        assert_eq!(user.missing.iter().collect::<Vec<_>>(), [2], "{stage:?}");
        assert_eq!(user.extra, 3, "{stage:?}: 99, 5 and the unreadable 6");
        let likes = finding(&audit, edge, "LIKES");
        assert_eq!(likes.missing.iter().collect::<Vec<_>>(), [7], "{stage:?}");
        assert!(finding(&audit, node, "Post").missing.is_empty());
    }
}

#[tokio::test]
async fn the_audit_reads_only_its_scope() {
    let db = test_db("label-audit-scopes").await;
    let tenant = DataScope::Tenant(crate::encoding::v2::keys::scope::TenantId::from_u128(7));
    testing::put_source_row(&db, tenant, IndexElementKind::Node, 1, &labelled("User")).await;
    let transaction = db
        .begin(IsolationLevel::Snapshot)
        .await
        .expect("write begins");
    testing::stage_departure(&transaction, tenant, IndexElementKind::Node, "User", 1).await;
    transaction.commit().await.expect("write commits");
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
