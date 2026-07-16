//! Supported executable root-stream inputs.
//!
//! A `RootStream` is intentionally narrower than a generic stream expression:
//! every variant keeps the concrete payload needed to lower a native executable
//! DAG without consulting a legacy physical tree.

use serde::{Deserialize, Serialize};

use super::{
    RootBranch, RootMutation, RootPipeline, RootRepeat, StreamAggregate, StreamProject,
    StreamReserved, StreamVariableWrite,
};
use crate::logical::{AccessStream, VariableSource};
use crate::properties;

/// Supported executable root stream with enough payload for selected lowering.
///
/// ```
/// use helix_planner::ir::NonEmptyString;
/// use helix_planner::logical::{RootStream, VariableSource};
///
/// let stream = RootStream::VariableSource(VariableSource::new(
///     NonEmptyString::new("seed").unwrap(),
/// ));
///
/// assert!(matches!(stream, RootStream::VariableSource(_)));
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RootStream {
    /// Access-backed stream.
    Access(AccessStream),
    /// Variable/source injection stream.
    VariableSource(VariableSource),
    /// Mutation stream.
    Mutation(Box<RootMutation>),
    /// Branch control-flow stream.
    Branch(Box<RootBranch>),
    /// Repeat control-flow stream.
    Repeat(Box<RootRepeat>),
    /// Composed root-stream pipeline.
    Pipeline(Box<RootPipeline>),
    /// Reserved stream terminal that can feed a later root operator.
    Reserved(Box<StreamReserved>),
    /// Projection terminal that can feed a later root operator.
    Project(Box<StreamProject>),
    /// Aggregation terminal that can feed a later root operator.
    Aggregate(Box<StreamAggregate>),
    /// State-writing variable terminal that can feed a later root operator.
    VariableWrite(Box<StreamVariableWrite>),
}

impl RootStream {
    /// Effect introduced by the stream source.
    pub fn effect(&self) -> properties::EffectKind {
        match self {
            Self::Access(access) => access.effect(),
            Self::VariableSource(_) => properties::EffectKind::Pure,
            Self::Mutation(_) | Self::Branch(_) | Self::Repeat(_) => {
                properties::EffectKind::Barrier
            }
            Self::Pipeline(pipeline) => pipeline.effect(),
            Self::Reserved(reserved) => reserved.effect(),
            Self::Project(project) => project.effect(),
            Self::Aggregate(aggregate) => aggregate.effect(),
            Self::VariableWrite(_) => properties::EffectKind::Barrier,
        }
    }
}
