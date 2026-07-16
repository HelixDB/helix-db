//! Non-ignored production-entry scale gate for Index V2.
//!
//! Enable `production-scale` to run the fixed 100,000-entity contract. The
//! feature gate keeps ordinary unit and coverage jobs bounded; the test itself
//! is intentionally not ignored and cannot silently reduce the acceptance
//! shape through an environment variable.

/// Builds, queries, and drops secondary/text indexes at the release shape.
#[tokio::test(flavor = "multi_thread")]
async fn index_v2_secondary_text_and_tenant_scale_contracts() {
    db::production_coverage::index_v2_secondary_text_scale_contracts().await;
}

/// Reproduces text CREATE/search/DROP without waiting for the full scale fixture.
#[tokio::test(flavor = "multi_thread")]
async fn index_v2_text_drop_smoke() {
    db::production_coverage::index_v2_text_drop_smoke().await;
}

/// Reproduces text CREATE/search/DROP after multi-split compaction.
#[tokio::test(flavor = "multi_thread")]
async fn index_v2_text_drop_multi_split_smoke() {
    db::production_coverage::index_v2_text_drop_multi_split_smoke().await;
}

/// Builds, queries, and drops the 128D f32 vector index at the release shape.
#[tokio::test(flavor = "multi_thread")]
async fn index_v2_vector_scale_contract() {
    db::production_coverage::index_v2_vector_scale_contracts().await;
}
