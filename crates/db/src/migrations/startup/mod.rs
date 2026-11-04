//! Ordered writer-startup migration pipeline.

mod bootstrap;
mod pipeline;

pub(crate) use bootstrap::{
    bootstrap_managed_writer, bootstrap_writer, require_current_managed_writer,
};
pub(crate) use pipeline::finish_writer;
#[cfg(test)]
pub(crate) use pipeline::prepare_writer;
