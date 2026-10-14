//! Ordered writer-startup migration pipeline.

mod bootstrap;
mod pipeline;

pub(crate) use bootstrap::{
    bootstrap_managed_writer, bootstrap_writer, require_current_managed_writer,
};
pub(crate) use pipeline::finish_writer;
#[cfg(any(test, feature = "production-coverage"))]
pub(crate) use pipeline::prepare_writer;
