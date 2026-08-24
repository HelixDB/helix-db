//! Public boundary for ordered writer-startup migrations.

use std::sync::Arc;

use slatedb::Db;

use crate::encoding::v2::keys::scope::DataScope;
use crate::{DbConfig, HelixDB, HelixWriter, Result};

/// Runs every migration that must finish before the runtime is constructed.
pub(crate) async fn prepare_writer(db: Arc<Db>, config: &DbConfig) -> Result<HelixWriter> {
    super::bootstrap_writer(&db).await?;
    super::super::preflight_legacy_vector_reservations(&db).await?;
    let writer = HelixWriter::new(db, config.id_lease_size());
    super::super::run_blocking_startup_migration(&writer, config.migrations()).await?;
    crate::index_lifecycle::outbox::reconcile_legacy_reader_coordination_operations(
        writer.db(),
        DataScope::LegacyUnscoped,
    )
    .await?;
    crate::index_lifecycle::outbox::reconcile_operation_queue(writer.db()).await?;
    Ok(writer)
}

/// Runs migrations that require the complete runtime and lifecycle drivers.
pub(crate) async fn finish_writer(db: &HelixDB) -> Result<()> {
    super::super::migrate_legacy_definitions(db).await?;
    Box::pin(super::super::migrate_active_vector_simhash_directories(db)).await
}
