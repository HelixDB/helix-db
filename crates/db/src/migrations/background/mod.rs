//! Owned runtime worker for durable background migrations.

mod worker;

pub(crate) use worker::MigrationWorkerSupervisor;
