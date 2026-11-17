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

#[tokio::test]
async fn a_store_whose_rows_are_all_in_their_bitmaps_is_complete() {
    let db = test_db("label-audit-clean").await;
    let scope = DataScope::LegacyUnscoped;
    for id in [1, 2, 3] {
        testing::put_source_row(&db, scope, IndexElementKind::Node, id, &labelled("User")).await;
    }
    testing::put_source_row(&db, scope, IndexElementKind::Node, 4, &labelled("Post")).await;
    testing::put_source_row(&db, scope, IndexElementKind::Edge, 4, &labelled("FOLLOWS")).await;
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
        testing::put_source_row(&db, scope, IndexElementKind::Node, id, &labelled("User")).await;
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
    assert!(finding(&audit, IndexElementKind::Node, "Post")
        .missing
        .is_empty());
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
