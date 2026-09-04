//! Public boundary for ordered writer-startup migrations.

#[cfg(any(test, feature = "production-coverage"))]
use std::sync::Arc;

#[cfg(any(test, feature = "production-coverage"))]
use slatedb::Db;

#[cfg(any(test, feature = "production-coverage"))]
use crate::encoding::v2::keys::scope::DataScope;
#[cfg(any(test, feature = "production-coverage"))]
use crate::{DbConfig, HelixWriter};
use crate::{HelixDB, Result};

/// Runs every migration that must finish before the runtime is constructed.
#[cfg(any(test, feature = "production-coverage"))]
pub(crate) async fn prepare_writer(db: Arc<Db>, config: &DbConfig) -> Result<HelixWriter> {
    super::bootstrap_writer(&db).await?;
    super::super::preflight_legacy_vector_reservations(&db).await?;
    let writer = HelixWriter::new(
        db,
        config.id_lease_size(),
        config.id_lease_refill_threshold(),
    );
    let prepare_result: Result<()> = async {
        super::super::run_blocking_startup_migration(&writer, config.migrations()).await?;
        crate::index_lifecycle::outbox::reconcile_legacy_reader_coordination_operations(
            writer.db(),
            DataScope::LegacyUnscoped,
        )
        .await?;
        crate::index_lifecycle::outbox::reconcile_operation_queue(writer.db()).await?;
        Ok(())
    }
    .await;
    writer.close_on_open_error(prepare_result).await?;
    Ok(writer)
}

/// Runs migrations that require the complete runtime and lifecycle drivers.
pub(crate) async fn finish_writer(db: &HelixDB) -> Result<()> {
    super::super::migrate_legacy_definitions(db).await?;
    Box::pin(super::super::migrate_active_vector_simhash_directories(db)).await
}
