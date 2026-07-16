//! Source-rooted native stream-shape recognition.
//!
//! `family` owns the source/wrapper/non-access classification, `operation`
//! owns wrapper append semantics, `dispatch` owns recursive orchestration, and
//! `result` owns the access-stream recognition ADT.

mod dispatch;
mod family;
mod operation;
mod result;
#[cfg(test)]
mod tests;

pub(super) use dispatch::native_access_stream_from_ast;
pub(super) use result::NativeAccessStreamRoot;
