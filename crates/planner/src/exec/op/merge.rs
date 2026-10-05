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
    /// Set-union element streams, emitted in ascending element order.
    ///
    /// Index-served sources and label scans deliver their elements in ID
    /// order, so a union of index sets each filtered by its own residual uses
    /// this mode to deliver the same order a scan of the label would, however
    /// its branches order their rows.
    OrderedUnion,
    /// Set-intersect dependency outputs.
    Intersect,
}
