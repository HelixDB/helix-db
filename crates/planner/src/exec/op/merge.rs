use serde::{Deserialize, Serialize};

/// How a native executable merge step combines dependency outputs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecMergeMode {
    /// Concatenate dependency outputs, restoring dependency order when the
    /// schedule requests order preservation.
    Concat,
    /// Set-union dependency outputs.
    Union,
    /// Set-intersect dependency outputs.
    Intersect,
}
