//! Access-stream recognition result.

use crate::planning::selected::native::stream;

/// Native access-stream recognition result.
pub(in crate::planning::selected::native) enum NativeAccessStreamRoot {
    /// The AST root is a validated access-rooted stream.
    Stream(stream::NativeAccessStream),
    /// The AST root is not access-rooted.
    NotAccessStream,
}
