//! Literal stream-window composition for native access streams.

use super::NativeAccessStream;
use crate::logical;

impl NativeAccessStream {
    pub(super) fn push_window<F>(&mut self, compose: F)
    where
        F: FnOnce(logical::AccessWindowRange) -> logical::AccessWindowRange,
    {
        let (prefix, base) = match self.ops.pop() {
            Some(logical::StreamPipelineOp::Window { window }) => (None, window),
            Some(op) => (Some(op), logical::AccessWindowRange::identity()),
            None => (None, logical::AccessWindowRange::identity()),
        };
        if let Some(op) = prefix {
            self.ops.push(op);
        }
        let window = compose(base);
        if !window.is_identity() {
            self.ops.push(logical::StreamPipelineOp::Window { window });
        }
    }
}
