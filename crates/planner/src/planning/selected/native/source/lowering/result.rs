//! Native source-stream recognition result.

use crate::planning::selected::native::stream;

/// Native source stream recognition result.
pub(in crate::planning::selected::native) enum NativeSourceStreamRoot {
    /// The AST root is a validated source stream.
    Source(stream::NativeAccessStream),
    /// The AST root is not a source stream.
    NotSource,
}
