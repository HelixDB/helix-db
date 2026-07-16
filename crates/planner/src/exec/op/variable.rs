use serde::{Deserialize, Serialize};

use crate::ir;

/// Native executable variable operation.
///
/// Source injection has no input stream, while stream variables always depend
/// on one DAG dependency. Splitting those cases here keeps input-less
/// `Within`/`Select` style states unrepresentable in executable IR.
///
/// ```
/// use helix_planner::exec::ExecVariableOp;
/// use helix_planner::ir::NonEmptyString;
///
/// let op = ExecVariableOp::SourceInject {
///     variable: NonEmptyString::new("seed").unwrap(),
/// };
/// assert!(matches!(op, ExecVariableOp::SourceInject { .. }));
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecVariableOp {
    /// Inject a variable as a source stream.
    SourceInject { variable: ir::NonEmptyString },
    /// Apply a variable operation to an existing stream.
    Stream(ir::StreamVariableOp),
}
