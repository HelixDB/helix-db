//! Ordered writer-startup migration pipeline.

mod bootstrap;
mod pipeline;

pub(crate) use bootstrap::{
    advance_storage_version, bootstrap_managed_writer, bootstrap_writer,
    require_current_managed_writer, storage_version, stored_storage_version,
};
pub(crate) use pipeline::finish_writer;
#[cfg(test)]
pub(crate) use pipeline::prepare_writer;
