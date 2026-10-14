#![recursion_limit = "256"]

//! Non-ignored production-entry scale gate for Index V2.
//!
//! Enable `production-scale` to run the fixed release and CI-sized contracts.
//! The feature gate keeps ordinary unit and coverage jobs bounded; every test
//! is intentionally not ignored and cannot silently reduce its acceptance
//! shape through an environment variable.

/// Builds, queries, and drops secondary/text indexes at the release shape.
#[tokio::test(flavor = "multi_thread")]
async fn index_lifecycle_secondary_text_and_tenant_scale_contracts() {
    db::production_coverage::index_lifecycle_secondary_text_scale_contracts().await;
}

/// Reproduces text CREATE/search/DROP without waiting for the full scale fixture.
#[tokio::test(flavor = "multi_thread")]
async fn index_lifecycle_text_drop_smoke() {
    db::production_coverage::index_lifecycle_text_drop_smoke().await;
}

/// Reproduces text CREATE/search/DROP after multi-split compaction.
#[tokio::test(flavor = "multi_thread")]
async fn index_lifecycle_text_drop_multi_split_smoke() {
    db::production_coverage::index_lifecycle_text_drop_multi_split_smoke().await;
}

/// Builds, queries, and drops the 128D f32 vector index at the release shape.
#[tokio::test(flavor = "multi_thread")]
async fn index_lifecycle_vector_scale_contract() {
    db::production_coverage::index_lifecycle_vector_scale_contracts().await;
}

/// Builds, queries, drops, and residue-checks 8k 128D vectors within bounded CI.
#[tokio::test(flavor = "multi_thread")]
async fn index_lifecycle_vector_ci_contract() {
    db::production_coverage::index_lifecycle_vector_ci_contracts().await;
}

/// Measures equality-index prefilter -> one hop -> 1536D DBpedia vector search.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "manual 50k by 1536 DBpedia traversal-vector performance gate"]
async fn traversal_vector_prefilter_scale_contract() {
    Box::pin(db::production_coverage::traversal_vector_prefilter_scale_contract()).await;
}

/// Inserts 1M DBpedia vectors, then measures four one-hop indexed prefilters.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "manual disk-backed 1M by 1536 DBpedia traversal-vector benchmark"]
async fn traversal_vector_prefilter_1m_scale_contract() {
    Box::pin(db::production_coverage::traversal_vector_prefilter_1m_scale_contract()).await;
}

/// Blocks, retries, drops, and aborts every family at one configured batch.
#[tokio::test(flavor = "multi_thread")]
async fn index_lifecycle_blocked_limit_scale_contracts() {
    db::production_coverage::index_lifecycle_blocked_limit_scale_contracts().await;
}
