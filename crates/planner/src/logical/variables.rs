//! Logical variable stream contracts.
//!
//! Variable operations are split into side-effect-free stream operations and
//! state-writing terminal operations so invalid effect placement is not
//! representable in pipeline contracts.

use serde::{Deserialize, Serialize};

use crate::ir;

/// Source-inject a previously materialized variable.
///
/// The variable name is a [`ir::NonEmptyString`], so source-injection contracts
/// cannot represent an empty variable reference.
///
/// ```
/// use helix_planner::ir::NonEmptyString;
/// use helix_planner::logical::VariableSource;
///
/// let source = VariableSource::new(NonEmptyString::new("users").unwrap());
/// assert_eq!(source.variable().as_ref(), "users");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VariableSource {
    variable: ir::NonEmptyString,
}

impl VariableSource {
    /// Build a variable source from a validated variable name.
    pub const fn new(variable: ir::NonEmptyString) -> Self {
        Self { variable }
    }

    /// Variable to source-inject.
    pub const fn variable(&self) -> &ir::NonEmptyString {
        &self.variable
    }
}

/// Side-effect-free stream variable operation allowed inside an access-backed
/// pipeline.
///
/// `As` and `Store` are intentionally absent because they write observable
/// variable state and must be modeled as barriers before Cascades can reorder
/// or natively select them.
///
/// ```
/// use helix_planner::ir::{NonEmptyString, StreamVariableOp};
/// use helix_planner::logical::PureStreamVariableOp;
///
/// let selected = StreamVariableOp::Select(NonEmptyString::new("users").unwrap());
/// let op = PureStreamVariableOp::from_stream_op(&selected).unwrap();
/// assert_eq!(op.to_stream_op(), selected);
///
/// let stored = StreamVariableOp::Store(NonEmptyString::new("users").unwrap());
/// assert!(PureStreamVariableOp::from_stream_op(&stored).is_none());
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PureStreamVariableOp {
    /// Select a previously stored variable stream.
    Select(ir::NonEmptyString),
    /// Bind the current row in row-local state.
    Bind(ir::NonEmptyString),
    /// Inject a variable into the current stream.
    Inject(ir::NonEmptyString),
    /// Keep rows inside a variable set.
    Within(ir::NonEmptyString),
    /// Keep rows outside a variable set.
    Without(ir::NonEmptyString),
}

impl PureStreamVariableOp {
    /// Convert a physical stream variable operation when it is side-effect-free.
    pub fn from_stream_op(op: &ir::StreamVariableOp) -> Option<Self> {
        match op {
            ir::StreamVariableOp::Select(variable) => Some(Self::Select(variable.clone())),
            ir::StreamVariableOp::Bind(variable) => Some(Self::Bind(variable.clone())),
            ir::StreamVariableOp::Inject(variable) => Some(Self::Inject(variable.clone())),
            ir::StreamVariableOp::Within(variable) => Some(Self::Within(variable.clone())),
            ir::StreamVariableOp::Without(variable) => Some(Self::Without(variable.clone())),
            ir::StreamVariableOp::As(_) | ir::StreamVariableOp::Store(_) => None,
        }
    }

    /// Convert back to the executable stream variable operation.
    pub fn to_stream_op(&self) -> ir::StreamVariableOp {
        match self {
            Self::Select(variable) => ir::StreamVariableOp::Select(variable.clone()),
            Self::Bind(variable) => ir::StreamVariableOp::Bind(variable.clone()),
            Self::Inject(variable) => ir::StreamVariableOp::Inject(variable.clone()),
            Self::Within(variable) => ir::StreamVariableOp::Within(variable.clone()),
            Self::Without(variable) => ir::StreamVariableOp::Without(variable.clone()),
        }
    }

    /// Whether this operation preserves the exact input cardinality.
    pub const fn preserves_cardinality(&self) -> bool {
        matches!(self, Self::Bind(_))
    }

    /// Whether this operation preserves the input upper cardinality bound.
    pub const fn preserves_upper_bound(&self) -> bool {
        matches!(self, Self::Bind(_) | Self::Within(_) | Self::Without(_))
    }
}

/// State-writing stream variable operation.
///
/// `Select`, `Bind`, `Inject`, `Within`, and `Without` are intentionally
/// absent because they do not write observable variable state and may be
/// represented by [`PureStreamVariableOp`] inside access-backed pipelines.
///
/// ```
/// use helix_planner::ir::{NonEmptyString, StreamVariableOp};
/// use helix_planner::logical::StreamVariableWriteOp;
///
/// let stored = StreamVariableOp::Store(NonEmptyString::new("users").unwrap());
/// let op = StreamVariableWriteOp::from_stream_op(&stored).unwrap();
/// assert_eq!(op.to_stream_op(), stored);
///
/// let selected = StreamVariableOp::Select(NonEmptyString::new("users").unwrap());
/// assert!(StreamVariableWriteOp::from_stream_op(&selected).is_none());
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamVariableWriteOp {
    /// Assign the current stream to a variable.
    As(ir::NonEmptyString),
    /// Store the current stream in a variable.
    Store(ir::NonEmptyString),
}

impl StreamVariableWriteOp {
    /// Convert a physical stream variable operation when it writes state.
    pub fn from_stream_op(op: &ir::StreamVariableOp) -> Option<Self> {
        match op {
            ir::StreamVariableOp::As(variable) => Some(Self::As(variable.clone())),
            ir::StreamVariableOp::Store(variable) => Some(Self::Store(variable.clone())),
            ir::StreamVariableOp::Select(_)
            | ir::StreamVariableOp::Bind(_)
            | ir::StreamVariableOp::Inject(_)
            | ir::StreamVariableOp::Within(_)
            | ir::StreamVariableOp::Without(_) => None,
        }
    }

    /// Convert back to the executable stream variable operation.
    pub fn to_stream_op(&self) -> ir::StreamVariableOp {
        match self {
            Self::As(variable) => ir::StreamVariableOp::As(variable.clone()),
            Self::Store(variable) => ir::StreamVariableOp::Store(variable.clone()),
        }
    }
}
