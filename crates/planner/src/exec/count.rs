//! Exact executable cardinality contracts.
//!
//! Count plans are physical programs. Each variant names the primitive the
//! interpreter must execute; the interpreter must not replace it with a
//! different access, batching, ordering, or materialization strategy.

use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize};

use crate::{catalog, ir};

/// Failure to construct a physical equality contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecEqualityContractError {
    /// A non-unique node index was required.
    ExpectedNodeNonUnique,
    /// A unique node index was required.
    ExpectedNodeUnique,
    /// A reflexive, physically indexed equality value was required.
    ExpectedIndexedValue,
}

impl std::fmt::Display for ExecEqualityContractError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ExpectedNodeNonUnique => f.write_str("expected a non-unique node equality index"),
            Self::ExpectedNodeUnique => f.write_str("expected a unique node equality index"),
            Self::ExpectedIndexedValue => {
                f.write_str("expected a reflexive physically indexed equality value")
            }
        }
    }
}

impl std::error::Error for ExecEqualityContractError {}

/// Node equality index proven to be non-unique.
///
/// ```
/// use helix_planner::{catalog, exec, ir};
///
/// let metadata = catalog::NodeEqualityIndexMeta::new(
///     ir::NonEmptyString::new("user_email").unwrap(),
/// );
/// let index = exec::ExecNodeNonUniqueEqualityIndex::try_from(metadata).unwrap();
/// assert_eq!(index.metadata().index_id.as_ref(), "user_email");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct ExecNodeNonUniqueEqualityIndex(catalog::NodeEqualityIndexMeta);

impl ExecNodeNonUniqueEqualityIndex {
    /// Borrow the validated catalog metadata.
    pub const fn metadata(&self) -> &catalog::NodeEqualityIndexMeta {
        &self.0
    }

    /// Consume the wrapper.
    pub fn into_metadata(self) -> catalog::NodeEqualityIndexMeta {
        self.0
    }
}

impl TryFrom<catalog::NodeEqualityIndexMeta> for ExecNodeNonUniqueEqualityIndex {
    type Error = ExecEqualityContractError;

    fn try_from(value: catalog::NodeEqualityIndexMeta) -> Result<Self, Self::Error> {
        match value.uniqueness {
            catalog::IndexUniqueness::NonUnique => Ok(Self(value)),
            catalog::IndexUniqueness::Unique => {
                Err(ExecEqualityContractError::ExpectedNodeNonUnique)
            }
        }
    }
}

impl<'de> Deserialize<'de> for ExecNodeNonUniqueEqualityIndex {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        catalog::NodeEqualityIndexMeta::deserialize(deserializer)?
            .try_into()
            .map_err(D::Error::custom)
    }
}

/// Node equality index proven to be unique.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct ExecNodeUniqueEqualityIndex(catalog::NodeEqualityIndexMeta);

impl ExecNodeUniqueEqualityIndex {
    /// Borrow the validated catalog metadata.
    pub const fn metadata(&self) -> &catalog::NodeEqualityIndexMeta {
        &self.0
    }

    /// Consume the wrapper.
    pub fn into_metadata(self) -> catalog::NodeEqualityIndexMeta {
        self.0
    }
}

impl TryFrom<catalog::NodeEqualityIndexMeta> for ExecNodeUniqueEqualityIndex {
    type Error = ExecEqualityContractError;

    fn try_from(value: catalog::NodeEqualityIndexMeta) -> Result<Self, Self::Error> {
        match value.uniqueness {
            catalog::IndexUniqueness::Unique => Ok(Self(value)),
            catalog::IndexUniqueness::NonUnique => {
                Err(ExecEqualityContractError::ExpectedNodeUnique)
            }
        }
    }
}

impl<'de> Deserialize<'de> for ExecNodeUniqueEqualityIndex {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        catalog::NodeEqualityIndexMeta::deserialize(deserializer)?
            .try_into()
            .map_err(D::Error::custom)
    }
}

/// Edge equality index validated by the edge catalog contract.
///
/// Edge equality indexes are currently always non-unique; the distinct wrapper
/// prevents a future node/edge metadata mix-up at bitmap call sites.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ExecEdgeNonUniqueEqualityIndex(catalog::EdgeEqualityIndexMeta);

impl ExecEdgeNonUniqueEqualityIndex {
    /// Wrap validated edge equality metadata.
    pub const fn new(metadata: catalog::EdgeEqualityIndexMeta) -> Self {
        Self(metadata)
    }

    /// Borrow the validated catalog metadata.
    pub const fn metadata(&self) -> &catalog::EdgeEqualityIndexMeta {
        &self.0
    }

    /// Consume the wrapper.
    pub fn into_metadata(self) -> catalog::EdgeEqualityIndexMeta {
        self.0
    }
}

/// Equality value proven to have a reflexive physical index encoding.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(transparent)]
pub struct ExecIndexedEqualityValue(ir::SecondaryIndexLiteral);

impl ExecIndexedEqualityValue {
    /// Borrow the validated literal.
    pub const fn literal(&self) -> &ir::SecondaryIndexLiteral {
        &self.0
    }

    /// Consume the wrapper.
    pub fn into_literal(self) -> ir::SecondaryIndexLiteral {
        self.0
    }
}

impl TryFrom<ir::SecondaryIndexLiteral> for ExecIndexedEqualityValue {
    type Error = ExecEqualityContractError;

    fn try_from(value: ir::SecondaryIndexLiteral) -> Result<Self, Self::Error> {
        match value.semantics() {
            ir::LiteralEqualityIndexValueSemantics::Indexed => Ok(Self(value)),
            ir::LiteralEqualityIndexValueSemantics::AuthoritativeNull
            | ir::LiteralEqualityIndexValueSemantics::NonReflexive => {
                Err(ExecEqualityContractError::ExpectedIndexedValue)
            }
        }
    }
}

impl<'de> Deserialize<'de> for ExecIndexedEqualityValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        ir::SecondaryIndexLiteral::deserialize(deserializer)?
            .try_into()
            .map_err(D::Error::custom)
    }
}

/// Small executable arithmetic expression for count windows.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecUsizeExpr {
    /// Literal value.
    Literal(usize),
    /// Non-negative integer request parameter.
    Param(ir::NonEmptyString),
    /// Minimum of two expressions.
    Min(Box<Self>, Box<Self>),
    /// Saturating addition.
    SaturatingAdd(Box<Self>, Box<Self>),
    /// Saturating subtraction.
    SaturatingSub(Box<Self>, Box<Self>),
}

/// Maximum arithmetic nodes accepted by one executable window expression.
pub const MAX_EXEC_USIZE_EXPR_NODES: usize = 64;

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum ExecUsizeExprSerde {
    Literal(usize),
    Param(ir::NonEmptyString),
    Min(Box<Self>, Box<Self>),
    SaturatingAdd(Box<Self>, Box<Self>),
    SaturatingSub(Box<Self>, Box<Self>),
}

impl From<ExecUsizeExprSerde> for ExecUsizeExpr {
    fn from(value: ExecUsizeExprSerde) -> Self {
        match value {
            ExecUsizeExprSerde::Literal(value) => Self::Literal(value),
            ExecUsizeExprSerde::Param(value) => Self::Param(value),
            ExecUsizeExprSerde::Min(left, right) => {
                Self::Min(Box::new((*left).into()), Box::new((*right).into()))
            }
            ExecUsizeExprSerde::SaturatingAdd(left, right) => {
                Self::SaturatingAdd(Box::new((*left).into()), Box::new((*right).into()))
            }
            ExecUsizeExprSerde::SaturatingSub(left, right) => {
                Self::SaturatingSub(Box::new((*left).into()), Box::new((*right).into()))
            }
        }
    }
}

impl<'de> Deserialize<'de> for ExecUsizeExpr {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let expression: Self = ExecUsizeExprSerde::deserialize(deserializer)?.into();
        if expression.node_count() > MAX_EXEC_USIZE_EXPR_NODES {
            return Err(D::Error::custom(format!(
                "executable usize expression exceeds {MAX_EXEC_USIZE_EXPR_NODES} nodes"
            )));
        }
        Ok(expression)
    }
}

impl ExecUsizeExpr {
    /// Construct a literal.
    pub const fn literal(value: usize) -> Self {
        Self::Literal(value)
    }

    /// Construct and simplify a minimum expression.
    pub fn min(left: Self, right: Self) -> Self {
        if left == right {
            return left;
        }
        match (&left, &right) {
            (Self::Literal(left), Self::Literal(right)) => Self::Literal((*left).min(*right)),
            (Self::Literal(0), _) | (_, Self::Literal(0)) => Self::Literal(0),
            _ => Self::Min(Box::new(left), Box::new(right)),
        }
    }

    /// Construct and simplify saturating addition.
    pub fn saturating_add(left: Self, right: Self) -> Self {
        match (&left, &right) {
            (Self::Literal(left), Self::Literal(right)) => {
                Self::Literal(left.saturating_add(*right))
            }
            (Self::Literal(0), _) => right,
            (_, Self::Literal(0)) => left,
            _ => Self::SaturatingAdd(Box::new(left), Box::new(right)),
        }
    }

    /// Construct and simplify saturating subtraction.
    pub fn saturating_sub(left: Self, right: Self) -> Self {
        if left == right {
            return Self::Literal(0);
        }
        match (&left, &right) {
            (Self::Literal(left), Self::Literal(right)) => {
                Self::Literal(left.saturating_sub(*right))
            }
            (Self::Literal(0), _) => Self::Literal(0),
            (_, Self::Literal(0)) => left,
            _ => Self::SaturatingSub(Box::new(left), Box::new(right)),
        }
    }

    /// Number of nodes in this expression.
    pub fn node_count(&self) -> usize {
        match self {
            Self::Literal(_) | Self::Param(_) => 1,
            Self::Min(left, right)
            | Self::SaturatingAdd(left, right)
            | Self::SaturatingSub(left, right) => 1usize
                .saturating_add(left.node_count())
                .saturating_add(right.node_count()),
        }
    }

    /// Evaluate using a caller-provided parameter resolver.
    pub fn evaluate<E>(
        &self,
        resolve: &mut impl FnMut(&ir::NonEmptyString) -> Result<usize, E>,
    ) -> Result<usize, E> {
        match self {
            Self::Literal(value) => Ok(*value),
            Self::Param(param) => resolve(param),
            Self::Min(left, right) => Ok(left.evaluate(resolve)?.min(right.evaluate(resolve)?)),
            Self::SaturatingAdd(left, right) => Ok(left
                .evaluate(resolve)?
                .saturating_add(right.evaluate(resolve)?)),
            Self::SaturatingSub(left, right) => Ok(left
                .evaluate(resolve)?
                .saturating_sub(right.evaluate(resolve)?)),
        }
    }
}

/// Canonical upper bound for a count.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecCountTake {
    /// Consume all matching rows.
    All,
    /// Consume at most the evaluated amount.
    AtMost(ExecUsizeExpr),
}

/// Canonical count window.
///
/// ```
/// use helix_planner::exec::{ExecCountTake, ExecCountWindowPlan, ExecUsizeExpr};
///
/// let window = ExecCountWindowPlan::identity()
///     .then_skip(ExecUsizeExpr::literal(100))
///     .then_limit(ExecUsizeExpr::literal(10));
/// assert_eq!(window.skip, ExecUsizeExpr::literal(100));
/// assert_eq!(window.take, ExecCountTake::AtMost(ExecUsizeExpr::literal(10)));
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecCountWindowPlan {
    /// Matches discarded before counting.
    pub skip: ExecUsizeExpr,
    /// Maximum counted matches.
    pub take: ExecCountTake,
}

impl ExecCountWindowPlan {
    /// Identity window.
    pub const fn identity() -> Self {
        Self {
            skip: ExecUsizeExpr::Literal(0),
            take: ExecCountTake::All,
        }
    }

    /// Append a limit using sequential stream semantics.
    pub fn then_limit(self, count: ExecUsizeExpr) -> Self {
        let take = match self.take {
            ExecCountTake::All => ExecCountTake::AtMost(count),
            ExecCountTake::AtMost(existing) => {
                ExecCountTake::AtMost(ExecUsizeExpr::min(existing, count))
            }
        };
        Self {
            skip: self.skip,
            take,
        }
    }

    /// Append a skip using sequential stream semantics.
    pub fn then_skip(self, count: ExecUsizeExpr) -> Self {
        match self.take {
            ExecCountTake::All => Self {
                skip: ExecUsizeExpr::saturating_add(self.skip, count),
                take: ExecCountTake::All,
            },
            ExecCountTake::AtMost(existing) => {
                let consumed = ExecUsizeExpr::min(existing.clone(), count);
                Self {
                    skip: ExecUsizeExpr::saturating_add(self.skip, consumed.clone()),
                    take: ExecCountTake::AtMost(ExecUsizeExpr::saturating_sub(existing, consumed)),
                }
            }
        }
    }

    /// Append a half-open range using sequential stream semantics.
    pub fn then_range(self, start: ExecUsizeExpr, end: ExecUsizeExpr) -> Self {
        self.then_skip(start.clone())
            .then_limit(ExecUsizeExpr::saturating_sub(end, start))
    }

    /// Evaluate the final cardinality for an already-computed source count.
    pub fn apply<E>(
        &self,
        cardinality: usize,
        resolve: &mut impl FnMut(&ir::NonEmptyString) -> Result<usize, E>,
    ) -> Result<usize, E> {
        let after_skip = cardinality.saturating_sub(self.skip.evaluate(resolve)?);
        match &self.take {
            ExecCountTake::All => Ok(after_skip),
            ExecCountTake::AtMost(take) => Ok(after_skip.min(take.evaluate(resolve)?)),
        }
    }
}

impl Default for ExecCountWindowPlan {
    fn default() -> Self {
        Self::identity()
    }
}

/// Exact node non-unique equality bitmap program.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecNodeBitmapExpr {
    /// Read one bitmap row.
    PointRead {
        /// Proven non-unique index.
        index: ExecNodeNonUniqueEqualityIndex,
        /// Indexed property.
        key: catalog::ScopedPropertyKey,
        /// Indexed literal.
        value: ExecIndexedEqualityValue,
    },
    /// Read and union at least two bitmap rows with one multi-get.
    BatchedUnionRead {
        /// Proven non-unique index.
        index: ExecNodeNonUniqueEqualityIndex,
        /// Indexed property.
        key: catalog::ScopedPropertyKey,
        /// Planner-selected values in request order.
        values: ir::AtLeast<ExecIndexedEqualityValue, 2>,
    },
    /// Union children in encoded order.
    Union {
        /// First child.
        driver: Box<Self>,
        /// Remaining children.
        rest: ir::AtLeast<Self, 1>,
    },
    /// Intersect children in encoded order.
    Intersect {
        /// First child.
        driver: Box<Self>,
        /// Remaining children.
        rest: ir::AtLeast<Self, 1>,
    },
}

/// Exact edge non-unique equality bitmap program.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecEdgeBitmapExpr {
    /// Read one bitmap row.
    PointRead {
        /// Edge equality index.
        index: ExecEdgeNonUniqueEqualityIndex,
        /// Indexed property.
        key: catalog::ScopedPropertyKey,
        /// Indexed literal.
        value: ExecIndexedEqualityValue,
    },
    /// Read and union at least two bitmap rows with one multi-get.
    BatchedUnionRead {
        /// Edge equality index.
        index: ExecEdgeNonUniqueEqualityIndex,
        /// Indexed property.
        key: catalog::ScopedPropertyKey,
        /// Planner-selected values in request order.
        values: ir::AtLeast<ExecIndexedEqualityValue, 2>,
    },
    /// Union children in encoded order.
    Union {
        /// First child.
        driver: Box<Self>,
        /// Remaining children.
        rest: ir::AtLeast<Self, 1>,
    },
    /// Intersect children in encoded order.
    Intersect {
        /// First child.
        driver: Box<Self>,
        /// Remaining children.
        rest: ir::AtLeast<Self, 1>,
    },
}

/// Node bitmap count.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecNodeBitmapCountPlan {
    /// Exact bitmap expression.
    pub bitmap: ExecNodeBitmapExpr,
    /// Normalized count window.
    pub window: ExecCountWindowPlan,
}

/// Edge bitmap count.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecEdgeBitmapCountPlan {
    /// Exact bitmap expression.
    pub bitmap: ExecEdgeBitmapExpr,
    /// Normalized count window.
    pub window: ExecCountWindowPlan,
}

/// Exact unique-owner point read.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecNodeUniqueOwnerReadPlan {
    /// Proven unique index.
    pub index: ExecNodeUniqueEqualityIndex,
    /// Indexed property.
    pub key: catalog::ScopedPropertyKey,
    /// Indexed literal.
    pub value: ExecIndexedEqualityValue,
}

/// Exact authoritative verification of a unique node owner.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecNodeAuthoritativeVerificationPlan {
    /// Property expected on the authoritative node.
    pub key: catalog::ScopedPropertyKey,
    /// Value expected on the authoritative node.
    pub value: ExecIndexedEqualityValue,
}

/// Verified unique node count.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecNodeUniqueCountPlan {
    /// Owner lookup.
    pub lookup: ExecNodeUniqueOwnerReadPlan,
    /// Authoritative graph verification.
    pub verification: ExecNodeAuthoritativeVerificationPlan,
    /// Normalized count window.
    pub window: ExecCountWindowPlan,
}

/// Exact verified node range driver.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecNodeVerifiedRangeScanPlan {
    /// Logical index identity.
    pub index: catalog::NodeRangeIndexMeta,
    /// Property and selected physical direction.
    pub key: catalog::ScopedPropertyDirectionKey,
    /// Exact bounds.
    pub range: ir::IndexRange,
}

/// Exact verified edge range driver.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecEdgeVerifiedRangeScanPlan {
    /// Logical index identity.
    pub index: catalog::EdgeRangeIndexMeta,
    /// Property and selected physical direction.
    pub key: catalog::ScopedPropertyDirectionKey,
    /// Exact bounds.
    pub range: ir::IndexRange,
}

/// Bitmap membership program for a node range count.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecNodeRangeMembershipPlan {
    /// Accept every verified range match.
    All,
    /// Test bitmap filters in encoded order.
    BitmapFilters(ir::AtLeast<ExecNodeBitmapExpr, 1>),
}

/// Bitmap membership program for an edge range count.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecEdgeRangeMembershipPlan {
    /// Accept every verified range match.
    All,
    /// Test bitmap filters in encoded order.
    BitmapFilters(ir::AtLeast<ExecEdgeBitmapExpr, 1>),
}

/// Streaming verified node range count.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecNodeRangeCountPlan {
    /// Selected range driver.
    pub driver: ExecNodeVerifiedRangeScanPlan,
    /// Exact membership program.
    pub membership: ExecNodeRangeMembershipPlan,
    /// Normalized count window.
    pub window: ExecCountWindowPlan,
}

/// Streaming verified edge range count.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecEdgeRangeCountPlan {
    /// Selected range driver.
    pub driver: ExecEdgeVerifiedRangeScanPlan,
    /// Exact membership program.
    pub membership: ExecEdgeRangeMembershipPlan,
    /// Normalized count window.
    pub window: ExecCountWindowPlan,
}

/// Exact authoritative node scan predicate.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecNodeAuthoritativeScanPredicate {
    /// Property is absent or null on a node with the scoped label.
    NullEquality { key: catalog::ScopedPropertyKey },
    /// Evaluate a residual predicate authoritatively.
    Predicate(ir::PredicatePlan),
}

/// Exact authoritative edge scan predicate.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecEdgeAuthoritativeScanPredicate {
    /// Property is absent or null on an edge with the scoped label.
    NullEquality { key: catalog::ScopedPropertyKey },
    /// Evaluate a residual predicate authoritatively.
    Predicate(ir::PredicatePlan),
}

/// Authoritative node scan count.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecNodeScanCountPlan {
    /// Exact predicate.
    pub predicate: ExecNodeAuthoritativeScanPredicate,
    /// Normalized count window.
    pub window: ExecCountWindowPlan,
}

/// Authoritative edge scan count.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecEdgeScanCountPlan {
    /// Exact predicate.
    pub predicate: ExecEdgeAuthoritativeScanPredicate,
    /// Normalized count window.
    pub window: ExecCountWindowPlan,
}

/// Exact runtime input source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecRuntimeInputPlan {
    /// Request parameter.
    Param(ir::NonEmptyString),
    /// Previously bound variable.
    Variable(ir::NonEmptyString),
}

/// Exact node vector-search source.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecNodeVectorSearchCountPlan {
    /// Search index key.
    pub key: catalog::NodeSearchIndexKey,
    /// Search execution identity.
    pub index: ir::SearchIndexPlan,
    /// Query vector.
    pub query_vector: ir::VectorQueryInputPlan,
    /// Search result limit.
    pub k: ir::SearchLimitPlan,
    /// Normalized count window.
    pub window: ExecCountWindowPlan,
}

/// Exact edge vector-search source.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecEdgeVectorSearchCountPlan {
    /// Search index key.
    pub key: catalog::EdgeSearchIndexKey,
    /// Search execution identity.
    pub index: ir::SearchIndexPlan,
    /// Query vector.
    pub query_vector: ir::VectorQueryInputPlan,
    /// Search result limit.
    pub k: ir::SearchLimitPlan,
    /// Normalized count window.
    pub window: ExecCountWindowPlan,
}

/// Exact node text-search source.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecNodeTextSearchCountPlan {
    /// Search index key.
    pub key: catalog::NodeSearchIndexKey,
    /// Search execution identity.
    pub index: ir::SearchIndexPlan,
    /// Query text.
    pub query_text: ir::TextQueryInputPlan,
    /// Search result limit.
    pub k: ir::SearchLimitPlan,
    /// Normalized count window.
    pub window: ExecCountWindowPlan,
}

/// Exact edge text-search source.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecEdgeTextSearchCountPlan {
    /// Search index key.
    pub key: catalog::EdgeSearchIndexKey,
    /// Search execution identity.
    pub index: ir::SearchIndexPlan,
    /// Query text.
    pub query_text: ir::TextQueryInputPlan,
    /// Search result limit.
    pub k: ir::SearchLimitPlan,
    /// Normalized count window.
    pub window: ExecCountWindowPlan,
}

/// Explicit exception permitting runtime equality classification for a node.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecNodeDynamicEqualityCountPlan {
    /// Catalog index metadata, including node uniqueness.
    pub index: catalog::NodeEqualityIndexMeta,
    /// Indexed property.
    pub key: catalog::ScopedPropertyKey,
    /// Genuinely late-bound parameter.
    pub param: ir::NonEmptyString,
    /// Normalized count window.
    pub window: ExecCountWindowPlan,
}

/// Explicit exception permitting runtime equality classification for an edge.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecEdgeDynamicEqualityCountPlan {
    /// Catalog index metadata.
    pub index: catalog::EdgeEqualityIndexMeta,
    /// Indexed property.
    pub key: catalog::ScopedPropertyKey,
    /// Genuinely late-bound parameter.
    pub param: ir::NonEmptyString,
    /// Normalized count window.
    pub window: ExecCountWindowPlan,
}

/// Explicit row-distinct algorithm used by a count cursor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecCountDistinctPlan {
    /// Hash-based row identity set.
    HashRows,
    /// Adjacent deduplication over planner-proven identity ordering.
    OrderedRows,
}

/// Recursive, exactly ordered row cursor used when count still needs identity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecCountCursorPlan {
    /// Statically empty cursor.
    EmptyRows,
    /// Rows supplied by the count step dependency.
    InputRows,
    /// Node equality bitmap IDs.
    NodeBitmap(ExecNodeBitmapExpr),
    /// Edge equality bitmap IDs.
    EdgeBitmap(ExecEdgeBitmapExpr),
    /// Verified unique node owner.
    NodeUnique {
        /// Owner lookup.
        lookup: ExecNodeUniqueOwnerReadPlan,
        /// Authoritative verification.
        verification: ExecNodeAuthoritativeVerificationPlan,
    },
    /// Verified node range IDs.
    NodeRange(ExecNodeVerifiedRangeScanPlan),
    /// Verified edge range IDs.
    EdgeRange(ExecEdgeVerifiedRangeScanPlan),
    /// Authoritative node predicate scan.
    NodeAuthoritativeScan(ExecNodeAuthoritativeScanPredicate),
    /// Authoritative edge predicate scan.
    EdgeAuthoritativeScan(ExecEdgeAuthoritativeScanPredicate),
    /// Verified node point reads.
    NodePointReads(ir::ElementIds),
    /// Verified edge point reads.
    EdgePointReads(ir::ElementIds),
    /// Node runtime input.
    NodeRuntimeInput(ExecRuntimeInputPlan),
    /// Edge runtime input.
    EdgeRuntimeInput(ExecRuntimeInputPlan),
    /// Runtime rows whose element kind is intentionally unconstrained.
    RuntimeInput(ExecRuntimeInputPlan),
    /// Full node scan.
    NodeFullScan,
    /// Full edge scan.
    EdgeFullScan,
    /// Node label bitmap.
    NodeLabelBitmap(ir::NonEmptyString),
    /// Edge label bitmap.
    EdgeLabelBitmap(ir::NonEmptyString),
    /// Node vector-search rows.
    NodeVectorSearch {
        /// Search key.
        key: catalog::NodeSearchIndexKey,
        /// Search execution identity.
        index: ir::SearchIndexPlan,
        /// Query vector.
        query_vector: ir::VectorQueryInputPlan,
        /// Result limit.
        k: ir::SearchLimitPlan,
    },
    /// Edge vector-search rows.
    EdgeVectorSearch {
        /// Search key.
        key: catalog::EdgeSearchIndexKey,
        /// Search execution identity.
        index: ir::SearchIndexPlan,
        /// Query vector.
        query_vector: ir::VectorQueryInputPlan,
        /// Result limit.
        k: ir::SearchLimitPlan,
    },
    /// Node text-search rows.
    NodeTextSearch {
        /// Search key.
        key: catalog::NodeSearchIndexKey,
        /// Search execution identity.
        index: ir::SearchIndexPlan,
        /// Query text.
        query_text: ir::TextQueryInputPlan,
        /// Result limit.
        k: ir::SearchLimitPlan,
    },
    /// Edge text-search rows.
    EdgeTextSearch {
        /// Search key.
        key: catalog::EdgeSearchIndexKey,
        /// Search execution identity.
        index: ir::SearchIndexPlan,
        /// Query text.
        query_text: ir::TextQueryInputPlan,
        /// Result limit.
        k: ir::SearchLimitPlan,
    },
    /// Explicit node runtime equality dispatch exception.
    NodeDynamicEquality {
        /// Catalog index metadata.
        index: catalog::NodeEqualityIndexMeta,
        /// Indexed property.
        key: catalog::ScopedPropertyKey,
        /// Genuinely late-bound parameter.
        param: ir::NonEmptyString,
    },
    /// Explicit edge runtime equality dispatch exception.
    EdgeDynamicEquality {
        /// Catalog index metadata.
        index: catalog::EdgeEqualityIndexMeta,
        /// Indexed property.
        key: catalog::ScopedPropertyKey,
        /// Genuinely late-bound parameter.
        param: ir::NonEmptyString,
    },
    /// Materialized row union in encoded child order.
    Union {
        /// First child.
        driver: Box<Self>,
        /// Remaining children.
        rest: ir::AtLeast<Self, 1>,
    },
    /// Materialized row intersection in encoded child order.
    Intersect {
        /// First child.
        driver: Box<Self>,
        /// Remaining children.
        rest: ir::AtLeast<Self, 1>,
    },
    /// Authoritative predicate filter.
    Filter {
        /// Input cursor.
        input: Box<Self>,
        /// Predicate.
        predicate: ir::PredicatePlan,
    },
    /// Positioned canonical window that could not cross a semantic barrier.
    Window {
        /// Input cursor.
        input: Box<Self>,
        /// Window.
        window: ExecCountWindowPlan,
    },
    /// Required ordering that could not be proven irrelevant.
    Order {
        /// Input cursor.
        input: Box<Self>,
        /// Exact order plan.
        plan: ir::OrderPlan,
    },
    /// Graph expansion.
    Expand {
        /// Input cursor.
        input: Box<Self>,
        /// Exact expansion.
        plan: ir::ExpandPlan,
    },
    /// Restricted vector ranking.
    VectorSearch {
        /// Input cursor.
        input: Box<Self>,
        /// Exact restricted search.
        plan: Box<ir::RestrictedVectorSearchPlan>,
    },
    /// Restricted text ranking.
    TextSearch {
        /// Input cursor.
        input: Box<Self>,
        /// Exact restricted search.
        plan: Box<ir::RestrictedTextSearchPlan>,
    },
    /// Side-effect-free stream variable operation.
    Variable {
        /// Input cursor.
        input: Box<Self>,
        /// Exact variable operation.
        op: crate::logical::PureStreamVariableOp,
    },
    /// Explicit distinct algorithm.
    Distinct {
        /// Input cursor.
        input: Box<Self>,
        /// Selected algorithm.
        plan: ExecCountDistinctPlan,
    },
}

/// Streaming count over an exact recursive cursor.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecCountStreamPlan {
    /// Exact cursor program.
    pub cursor: ExecCountCursorPlan,
    /// Final normalized count window.
    pub window: ExecCountWindowPlan,
}

/// Fully physical count operation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecCountPlan {
    /// Final constant result. Windows have already been proven irrelevant or folded.
    Constant(usize),
    /// Node equality bitmap cardinality.
    NodeBitmap(ExecNodeBitmapCountPlan),
    /// Edge equality bitmap cardinality.
    EdgeBitmap(ExecEdgeBitmapCountPlan),
    /// Verified unique node owner cardinality.
    NodeUnique(ExecNodeUniqueCountPlan),
    /// Streaming verified node range cardinality.
    NodeRange(ExecNodeRangeCountPlan),
    /// Streaming verified edge range cardinality.
    EdgeRange(ExecEdgeRangeCountPlan),
    /// Authoritative node scan cardinality.
    NodeAuthoritativeScan(ExecNodeScanCountPlan),
    /// Authoritative edge scan cardinality.
    EdgeAuthoritativeScan(ExecEdgeScanCountPlan),
    /// Verified static node point reads.
    NodePointReads {
        /// Unique IDs in planner order.
        ids: ir::ElementIds,
        /// Normalized count window.
        window: ExecCountWindowPlan,
    },
    /// Verified static edge point reads.
    EdgePointReads {
        /// Unique IDs in planner order.
        ids: ir::ElementIds,
        /// Normalized count window.
        window: ExecCountWindowPlan,
    },
    /// Node IDs supplied at runtime.
    NodeRuntimeInput {
        /// Exact runtime source.
        input: ExecRuntimeInputPlan,
        /// Normalized count window.
        window: ExecCountWindowPlan,
    },
    /// Edge IDs supplied at runtime.
    EdgeRuntimeInput {
        /// Exact runtime source.
        input: ExecRuntimeInputPlan,
        /// Normalized count window.
        window: ExecCountWindowPlan,
    },
    /// Rows supplied directly by a request parameter or materialized variable.
    RuntimeInput {
        /// Exact runtime source.
        input: ExecRuntimeInputPlan,
        /// Normalized count window.
        window: ExecCountWindowPlan,
    },
    /// Full authoritative node scan.
    NodeFullScan { window: ExecCountWindowPlan },
    /// Full authoritative edge scan.
    EdgeFullScan { window: ExecCountWindowPlan },
    /// Node label bitmap cardinality.
    NodeLabelBitmap {
        /// Label.
        label: ir::NonEmptyString,
        /// Normalized count window.
        window: ExecCountWindowPlan,
    },
    /// Edge label bitmap cardinality.
    EdgeLabelBitmap {
        /// Label.
        label: ir::NonEmptyString,
        /// Normalized count window.
        window: ExecCountWindowPlan,
    },
    /// Node vector search cardinality.
    NodeVectorSearch(ExecNodeVectorSearchCountPlan),
    /// Edge vector search cardinality.
    EdgeVectorSearch(ExecEdgeVectorSearchCountPlan),
    /// Node text search cardinality.
    NodeTextSearch(ExecNodeTextSearchCountPlan),
    /// Edge text search cardinality.
    EdgeTextSearch(ExecEdgeTextSearchCountPlan),
    /// Explicit node runtime equality dispatch exception.
    NodeDynamicEquality(ExecNodeDynamicEqualityCountPlan),
    /// Explicit edge runtime equality dispatch exception.
    EdgeDynamicEquality(ExecEdgeDynamicEqualityCountPlan),
    /// Recursive exact cursor fallback for identity-sensitive pipelines and sets.
    Stream(ExecCountStreamPlan),
    /// Count rows emitted by a preserved effect barrier.
    InputRows { window: ExecCountWindowPlan },
    /// Count scalar items emitted by a nested scalar terminal.
    InputScalars { window: ExecCountWindowPlan },
}

/// Runtime input shape required by an exact count program.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecCountDependency {
    /// The count program performs all reads itself.
    Direct,
    /// The count program consumes rows from exactly one selected dependency.
    Rows,
    /// The count program consumes scalar items from exactly one selected dependency.
    Scalars,
}

/// Invalid dependency shape encoded by a recursive count cursor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecCountDependencyError {
    /// More than one cursor leaf requested dependency rows.
    MultipleRowInputs,
}

/// Invalid cross-field state in an executable count program.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecCountValidationError {
    /// The recursive cursor requested its one row dependency more than once.
    MultipleRowInputs,
    /// A window arithmetic expression exceeds its executable size bound.
    WindowExpressionTooLarge,
    /// Unique lookup and authoritative verification describe different values.
    UniqueVerificationMismatch,
}

impl ExecCountPlan {
    /// Validate recursive dependency, window, and verification invariants.
    pub fn validate(&self) -> Result<(), ExecCountValidationError> {
        self.validated_dependency().map(|_| ())
    }

    /// Validate every invariant and return the exact selected dependency contract.
    pub fn validated_dependency(&self) -> Result<ExecCountDependency, ExecCountValidationError> {
        let dependency = self
            .dependency()
            .map_err(|_| ExecCountValidationError::MultipleRowInputs)?;
        validate_count_plan(self)?;
        Ok(dependency)
    }

    /// Validate and return the exact selected dependency contract.
    pub fn dependency(&self) -> Result<ExecCountDependency, ExecCountDependencyError> {
        match self {
            Self::InputRows { .. } => Ok(ExecCountDependency::Rows),
            Self::InputScalars { .. } => Ok(ExecCountDependency::Scalars),
            Self::Stream(plan) => Ok(if cursor_row_input_count(&plan.cursor)? == 0 {
                ExecCountDependency::Direct
            } else {
                ExecCountDependency::Rows
            }),
            Self::Constant(_)
            | Self::NodeBitmap(_)
            | Self::EdgeBitmap(_)
            | Self::NodeUnique(_)
            | Self::NodeRange(_)
            | Self::EdgeRange(_)
            | Self::NodeAuthoritativeScan(_)
            | Self::EdgeAuthoritativeScan(_)
            | Self::NodePointReads { .. }
            | Self::EdgePointReads { .. }
            | Self::NodeRuntimeInput { .. }
            | Self::EdgeRuntimeInput { .. }
            | Self::RuntimeInput { .. }
            | Self::NodeFullScan { .. }
            | Self::EdgeFullScan { .. }
            | Self::NodeLabelBitmap { .. }
            | Self::EdgeLabelBitmap { .. }
            | Self::NodeVectorSearch(_)
            | Self::EdgeVectorSearch(_)
            | Self::NodeTextSearch(_)
            | Self::EdgeTextSearch(_)
            | Self::NodeDynamicEquality(_)
            | Self::EdgeDynamicEquality(_) => Ok(ExecCountDependency::Direct),
        }
    }
}

fn validate_count_plan(plan: &ExecCountPlan) -> Result<(), ExecCountValidationError> {
    match plan {
        ExecCountPlan::Constant(_) => Ok(()),
        ExecCountPlan::NodeBitmap(plan) => validate_window(&plan.window),
        ExecCountPlan::EdgeBitmap(plan) => validate_window(&plan.window),
        ExecCountPlan::NodeUnique(plan) => {
            if plan.lookup.key != plan.verification.key
                || plan.lookup.value != plan.verification.value
            {
                return Err(ExecCountValidationError::UniqueVerificationMismatch);
            }
            validate_window(&plan.window)
        }
        ExecCountPlan::NodeRange(plan) => validate_window(&plan.window),
        ExecCountPlan::EdgeRange(plan) => validate_window(&plan.window),
        ExecCountPlan::NodeAuthoritativeScan(plan) => validate_window(&plan.window),
        ExecCountPlan::EdgeAuthoritativeScan(plan) => validate_window(&plan.window),
        ExecCountPlan::NodePointReads { window, .. }
        | ExecCountPlan::EdgePointReads { window, .. }
        | ExecCountPlan::NodeRuntimeInput { window, .. }
        | ExecCountPlan::EdgeRuntimeInput { window, .. }
        | ExecCountPlan::RuntimeInput { window, .. }
        | ExecCountPlan::NodeFullScan { window }
        | ExecCountPlan::EdgeFullScan { window }
        | ExecCountPlan::NodeLabelBitmap { window, .. }
        | ExecCountPlan::EdgeLabelBitmap { window, .. }
        | ExecCountPlan::InputRows { window }
        | ExecCountPlan::InputScalars { window } => validate_window(window),
        ExecCountPlan::NodeVectorSearch(plan) => validate_window(&plan.window),
        ExecCountPlan::EdgeVectorSearch(plan) => validate_window(&plan.window),
        ExecCountPlan::NodeTextSearch(plan) => validate_window(&plan.window),
        ExecCountPlan::EdgeTextSearch(plan) => validate_window(&plan.window),
        ExecCountPlan::NodeDynamicEquality(plan) => validate_window(&plan.window),
        ExecCountPlan::EdgeDynamicEquality(plan) => validate_window(&plan.window),
        ExecCountPlan::Stream(plan) => {
            validate_cursor(&plan.cursor)?;
            validate_window(&plan.window)
        }
    }
}

fn validate_cursor(cursor: &ExecCountCursorPlan) -> Result<(), ExecCountValidationError> {
    match cursor {
        ExecCountCursorPlan::NodeUnique {
            lookup,
            verification,
        } if lookup.key != verification.key || lookup.value != verification.value => {
            Err(ExecCountValidationError::UniqueVerificationMismatch)
        }
        ExecCountCursorPlan::Union { driver, rest }
        | ExecCountCursorPlan::Intersect { driver, rest } => {
            validate_cursor(driver)?;
            rest.iter().try_for_each(validate_cursor)
        }
        ExecCountCursorPlan::Filter { input, .. }
        | ExecCountCursorPlan::Order { input, .. }
        | ExecCountCursorPlan::Expand { input, .. }
        | ExecCountCursorPlan::VectorSearch { input, .. }
        | ExecCountCursorPlan::TextSearch { input, .. }
        | ExecCountCursorPlan::Variable { input, .. }
        | ExecCountCursorPlan::Distinct { input, .. } => validate_cursor(input),
        ExecCountCursorPlan::Window { input, window } => {
            validate_cursor(input)?;
            validate_window(window)
        }
        ExecCountCursorPlan::EmptyRows
        | ExecCountCursorPlan::InputRows
        | ExecCountCursorPlan::NodeBitmap(_)
        | ExecCountCursorPlan::EdgeBitmap(_)
        | ExecCountCursorPlan::NodeUnique { .. }
        | ExecCountCursorPlan::NodeRange(_)
        | ExecCountCursorPlan::EdgeRange(_)
        | ExecCountCursorPlan::NodeAuthoritativeScan(_)
        | ExecCountCursorPlan::EdgeAuthoritativeScan(_)
        | ExecCountCursorPlan::NodePointReads(_)
        | ExecCountCursorPlan::EdgePointReads(_)
        | ExecCountCursorPlan::NodeRuntimeInput(_)
        | ExecCountCursorPlan::EdgeRuntimeInput(_)
        | ExecCountCursorPlan::RuntimeInput(_)
        | ExecCountCursorPlan::NodeFullScan
        | ExecCountCursorPlan::EdgeFullScan
        | ExecCountCursorPlan::NodeLabelBitmap(_)
        | ExecCountCursorPlan::EdgeLabelBitmap(_)
        | ExecCountCursorPlan::NodeVectorSearch { .. }
        | ExecCountCursorPlan::EdgeVectorSearch { .. }
        | ExecCountCursorPlan::NodeTextSearch { .. }
        | ExecCountCursorPlan::EdgeTextSearch { .. }
        | ExecCountCursorPlan::NodeDynamicEquality { .. }
        | ExecCountCursorPlan::EdgeDynamicEquality { .. } => Ok(()),
    }
}

fn validate_window(window: &ExecCountWindowPlan) -> Result<(), ExecCountValidationError> {
    if window.skip.node_count() > MAX_EXEC_USIZE_EXPR_NODES
        || matches!(
            &window.take,
            ExecCountTake::AtMost(take) if take.node_count() > MAX_EXEC_USIZE_EXPR_NODES
        )
    {
        return Err(ExecCountValidationError::WindowExpressionTooLarge);
    }
    Ok(())
}

fn cursor_row_input_count(cursor: &ExecCountCursorPlan) -> Result<usize, ExecCountDependencyError> {
    let count = match cursor {
        ExecCountCursorPlan::InputRows => 1,
        ExecCountCursorPlan::Union { driver, rest }
        | ExecCountCursorPlan::Intersect { driver, rest } => {
            let mut count = cursor_row_input_count(driver)?;
            for child in rest {
                count = count.saturating_add(cursor_row_input_count(child)?);
                if count > 1 {
                    return Err(ExecCountDependencyError::MultipleRowInputs);
                }
            }
            count
        }
        ExecCountCursorPlan::Filter { input, .. }
        | ExecCountCursorPlan::Window { input, .. }
        | ExecCountCursorPlan::Order { input, .. }
        | ExecCountCursorPlan::Expand { input, .. }
        | ExecCountCursorPlan::VectorSearch { input, .. }
        | ExecCountCursorPlan::TextSearch { input, .. }
        | ExecCountCursorPlan::Variable { input, .. }
        | ExecCountCursorPlan::Distinct { input, .. } => cursor_row_input_count(input)?,
        ExecCountCursorPlan::EmptyRows
        | ExecCountCursorPlan::NodeBitmap(_)
        | ExecCountCursorPlan::EdgeBitmap(_)
        | ExecCountCursorPlan::NodeUnique { .. }
        | ExecCountCursorPlan::NodeRange(_)
        | ExecCountCursorPlan::EdgeRange(_)
        | ExecCountCursorPlan::NodeAuthoritativeScan(_)
        | ExecCountCursorPlan::EdgeAuthoritativeScan(_)
        | ExecCountCursorPlan::NodePointReads(_)
        | ExecCountCursorPlan::EdgePointReads(_)
        | ExecCountCursorPlan::NodeRuntimeInput(_)
        | ExecCountCursorPlan::EdgeRuntimeInput(_)
        | ExecCountCursorPlan::RuntimeInput(_)
        | ExecCountCursorPlan::NodeFullScan
        | ExecCountCursorPlan::EdgeFullScan
        | ExecCountCursorPlan::NodeLabelBitmap(_)
        | ExecCountCursorPlan::EdgeLabelBitmap(_)
        | ExecCountCursorPlan::NodeVectorSearch { .. }
        | ExecCountCursorPlan::EdgeVectorSearch { .. }
        | ExecCountCursorPlan::NodeTextSearch { .. }
        | ExecCountCursorPlan::EdgeTextSearch { .. }
        | ExecCountCursorPlan::NodeDynamicEquality { .. }
        | ExecCountCursorPlan::EdgeDynamicEquality { .. } => 0,
    };
    Ok(count)
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use helix_ast::{
        expr::Predicate,
        index::RangeIndexDirection,
        traversal::Order,
        value::{PropertyInput, PropertyValue},
    };
    use proptest::prelude::*;

    use super::*;

    fn name(value: &str) -> ir::NonEmptyString {
        ir::NonEmptyString::new(value).unwrap()
    }

    fn literal(value: usize) -> ExecUsizeExpr {
        ExecUsizeExpr::Literal(value)
    }

    fn indexed(value: &str) -> ExecIndexedEqualityValue {
        ExecIndexedEqualityValue::try_from(
            ir::SecondaryIndexLiteral::new(PropertyValue::from(value)).unwrap(),
        )
        .unwrap()
    }

    fn node_index() -> ExecNodeNonUniqueEqualityIndex {
        catalog::NodeEqualityIndexMeta::new(name("node_eq:User:status"))
            .try_into()
            .unwrap()
    }

    fn edge_index() -> ExecEdgeNonUniqueEqualityIndex {
        ExecEdgeNonUniqueEqualityIndex::new(catalog::EdgeEqualityIndexMeta::new(name(
            "edge_eq:LIKES:status",
        )))
    }

    fn node_point(value: &str) -> ExecNodeBitmapExpr {
        ExecNodeBitmapExpr::PointRead {
            index: node_index(),
            key: catalog::ScopedPropertyKey::try_new("User", "status").unwrap(),
            value: indexed(value),
        }
    }

    fn edge_point(value: &str) -> ExecEdgeBitmapExpr {
        ExecEdgeBitmapExpr::PointRead {
            index: edge_index(),
            key: catalog::ScopedPropertyKey::try_new("LIKES", "status").unwrap(),
            value: indexed(value),
        }
    }

    fn unique_parts() -> (
        ExecNodeUniqueOwnerReadPlan,
        ExecNodeAuthoritativeVerificationPlan,
    ) {
        let key = catalog::ScopedPropertyKey::try_new("User", "email").unwrap();
        let value = indexed("alice@example.test");
        (
            ExecNodeUniqueOwnerReadPlan {
                index: catalog::NodeEqualityIndexMeta::new(name("node_eq:User:email"))
                    .with_uniqueness(catalog::IndexUniqueness::Unique)
                    .try_into()
                    .unwrap(),
                key: key.clone(),
                value: value.clone(),
            },
            ExecNodeAuthoritativeVerificationPlan { key, value },
        )
    }

    fn node_range() -> ExecNodeVerifiedRangeScanPlan {
        ExecNodeVerifiedRangeScanPlan {
            index: catalog::NodeRangeIndexMeta::try_new("node_range:User:age:asc").unwrap(),
            key: catalog::ScopedPropertyDirectionKey::try_new(
                "User",
                "age",
                RangeIndexDirection::Asc,
            )
            .unwrap(),
            range: ir::IndexRange::All,
        }
    }

    fn edge_range() -> ExecEdgeVerifiedRangeScanPlan {
        ExecEdgeVerifiedRangeScanPlan {
            index: catalog::EdgeRangeIndexMeta::try_new("edge_range:LIKES:age:desc").unwrap(),
            key: catalog::ScopedPropertyDirectionKey::try_new(
                "LIKES",
                "age",
                RangeIndexDirection::Desc,
            )
            .unwrap(),
            range: ir::IndexRange::All,
        }
    }

    fn ids() -> ir::ElementIds {
        ir::ElementIds::new(ir::AtLeast::from_one_and_rest(1, vec![2])).unwrap()
    }

    fn search_index() -> ir::SearchIndexPlan {
        ir::SearchIndexPlan {
            index_id: name("search-index"),
            tenant: ir::SearchTenantPlan::Unscoped,
        }
    }

    fn search_limit() -> ir::SearchLimitPlan {
        ir::SearchLimitPlan::Literal(NonZeroUsize::MIN)
    }

    fn vector_input() -> ir::VectorQueryInputPlan {
        ir::VectorQueryInputPlan::new(PropertyInput::from(vec![1.0_f32])).unwrap()
    }

    fn text_input() -> ir::TextQueryInputPlan {
        ir::TextQueryInputPlan::new(PropertyInput::from("needle")).unwrap()
    }

    fn predicate() -> ir::PredicatePlan {
        ir::PredicatePlan::new(Predicate::has_key("status")).unwrap()
    }

    fn cursor_leaves() -> Vec<ExecCountCursorPlan> {
        let (lookup, verification) = unique_parts();
        vec![
            ExecCountCursorPlan::EmptyRows,
            ExecCountCursorPlan::InputRows,
            ExecCountCursorPlan::NodeBitmap(node_point("active")),
            ExecCountCursorPlan::EdgeBitmap(edge_point("active")),
            ExecCountCursorPlan::NodeUnique {
                lookup,
                verification,
            },
            ExecCountCursorPlan::NodeRange(node_range()),
            ExecCountCursorPlan::EdgeRange(edge_range()),
            ExecCountCursorPlan::NodeAuthoritativeScan(
                ExecNodeAuthoritativeScanPredicate::NullEquality {
                    key: catalog::ScopedPropertyKey::try_new("User", "status").unwrap(),
                },
            ),
            ExecCountCursorPlan::EdgeAuthoritativeScan(
                ExecEdgeAuthoritativeScanPredicate::Predicate(predicate()),
            ),
            ExecCountCursorPlan::NodePointReads(ids()),
            ExecCountCursorPlan::EdgePointReads(ids()),
            ExecCountCursorPlan::NodeRuntimeInput(ExecRuntimeInputPlan::Param(name("nodes"))),
            ExecCountCursorPlan::EdgeRuntimeInput(ExecRuntimeInputPlan::Variable(name("edges"))),
            ExecCountCursorPlan::RuntimeInput(ExecRuntimeInputPlan::Param(name("rows"))),
            ExecCountCursorPlan::NodeFullScan,
            ExecCountCursorPlan::EdgeFullScan,
            ExecCountCursorPlan::NodeLabelBitmap(name("User")),
            ExecCountCursorPlan::EdgeLabelBitmap(name("LIKES")),
            ExecCountCursorPlan::NodeVectorSearch {
                key: catalog::NodeSearchIndexKey::try_new("User", "embedding").unwrap(),
                index: search_index(),
                query_vector: vector_input(),
                k: search_limit(),
            },
            ExecCountCursorPlan::EdgeVectorSearch {
                key: catalog::EdgeSearchIndexKey::try_new("LIKES", "embedding").unwrap(),
                index: search_index(),
                query_vector: vector_input(),
                k: search_limit(),
            },
            ExecCountCursorPlan::NodeTextSearch {
                key: catalog::NodeSearchIndexKey::try_new("User", "body").unwrap(),
                index: search_index(),
                query_text: text_input(),
                k: search_limit(),
            },
            ExecCountCursorPlan::EdgeTextSearch {
                key: catalog::EdgeSearchIndexKey::try_new("LIKES", "body").unwrap(),
                index: search_index(),
                query_text: text_input(),
                k: search_limit(),
            },
            ExecCountCursorPlan::NodeDynamicEquality {
                index: catalog::NodeEqualityIndexMeta::new(name("node_eq:User:status")),
                key: catalog::ScopedPropertyKey::try_new("User", "status").unwrap(),
                param: name("status"),
            },
            ExecCountCursorPlan::EdgeDynamicEquality {
                index: catalog::EdgeEqualityIndexMeta::new(name("edge_eq:LIKES:status")),
                key: catalog::ScopedPropertyKey::try_new("LIKES", "status").unwrap(),
                param: name("status"),
            },
        ]
    }

    fn wrapped_cursors() -> Vec<ExecCountCursorPlan> {
        let direct = || ExecCountCursorPlan::NodeFullScan;
        vec![
            ExecCountCursorPlan::Union {
                driver: Box::new(direct()),
                rest: ir::AtLeast::from_one(ExecCountCursorPlan::EdgeFullScan),
            },
            ExecCountCursorPlan::Intersect {
                driver: Box::new(direct()),
                rest: ir::AtLeast::from_one(ExecCountCursorPlan::NodeLabelBitmap(name("User"))),
            },
            ExecCountCursorPlan::Filter {
                input: Box::new(direct()),
                predicate: predicate(),
            },
            ExecCountCursorPlan::Window {
                input: Box::new(direct()),
                window: ExecCountWindowPlan::identity().then_limit(literal(3)),
            },
            ExecCountCursorPlan::Order {
                input: Box::new(direct()),
                plan: ir::OrderPlan::ExplicitSort(ir::OrderKeys::from(ir::OrderKey {
                    property: name("age"),
                    order: Order::Asc,
                })),
            },
            ExecCountCursorPlan::Expand {
                input: Box::new(direct()),
                plan: ir::ExpandPlan {
                    direction: ir::ExpandDirection::Out,
                    output: ir::ExpandOutput::Nodes,
                    label: ir::ExpandLabelPlan::Any,
                },
            },
            ExecCountCursorPlan::VectorSearch {
                input: Box::new(direct()),
                plan: Box::new(ir::RestrictedVectorSearchPlan::Nodes {
                    key: catalog::NodeSearchIndexKey::try_new("User", "embedding").unwrap(),
                    index: search_index(),
                    query_vector: vector_input(),
                    k: search_limit(),
                }),
            },
            ExecCountCursorPlan::TextSearch {
                input: Box::new(direct()),
                plan: Box::new(ir::RestrictedTextSearchPlan::Edges {
                    key: catalog::EdgeSearchIndexKey::try_new("LIKES", "body").unwrap(),
                    index: search_index(),
                    query_text: text_input(),
                    k: search_limit(),
                }),
            },
            ExecCountCursorPlan::Variable {
                input: Box::new(direct()),
                op: crate::logical::PureStreamVariableOp::Select(name("saved")),
            },
            ExecCountCursorPlan::Distinct {
                input: Box::new(direct()),
                plan: ExecCountDistinctPlan::OrderedRows,
            },
        ]
    }

    #[test]
    fn node_index_wrappers_reject_the_wrong_uniqueness() {
        let non_unique = catalog::NodeEqualityIndexMeta::new(name("non_unique"));
        let unique = catalog::NodeEqualityIndexMeta::new(name("unique"))
            .with_uniqueness(catalog::IndexUniqueness::Unique);

        assert!(ExecNodeNonUniqueEqualityIndex::try_from(non_unique.clone()).is_ok());
        assert_eq!(
            ExecNodeUniqueEqualityIndex::try_from(non_unique),
            Err(ExecEqualityContractError::ExpectedNodeUnique)
        );
        assert!(ExecNodeUniqueEqualityIndex::try_from(unique.clone()).is_ok());
        assert_eq!(
            ExecNodeNonUniqueEqualityIndex::try_from(unique),
            Err(ExecEqualityContractError::ExpectedNodeNonUnique)
        );
    }

    #[test]
    fn node_index_wrapper_deserialization_revalidates_uniqueness() {
        let unique = catalog::NodeEqualityIndexMeta::new(name("unique"))
            .with_uniqueness(catalog::IndexUniqueness::Unique);
        let json = serde_json::to_string(&unique).unwrap();
        let value = serde_json::to_value(&unique).unwrap();

        assert!(serde_json::from_str::<ExecNodeUniqueEqualityIndex>(&json).is_ok());
        assert!(serde_json::from_str::<ExecNodeNonUniqueEqualityIndex>(&json).is_err());
        assert!(serde_json::from_value::<ExecNodeUniqueEqualityIndex>(value).is_ok());
        assert!(serde_json::from_str::<ExecNodeUniqueEqualityIndex>("{}").is_err());
        assert!(serde_json::from_str::<ExecNodeNonUniqueEqualityIndex>("{}").is_err());
    }

    #[test]
    fn indexed_value_rejects_null_and_non_reflexive_literals() {
        let indexed = ir::SecondaryIndexLiteral::new(PropertyValue::from("alice")).unwrap();
        let null = ir::SecondaryIndexLiteral::new(PropertyValue::Null).unwrap();
        let nan = ir::SecondaryIndexLiteral::new(PropertyValue::F64(f64::NAN)).unwrap();

        assert!(ExecIndexedEqualityValue::try_from(indexed).is_ok());
        assert_eq!(
            ExecIndexedEqualityValue::try_from(null),
            Err(ExecEqualityContractError::ExpectedIndexedValue)
        );
        assert_eq!(
            ExecIndexedEqualityValue::try_from(nan),
            Err(ExecEqualityContractError::ExpectedIndexedValue)
        );
    }

    #[test]
    fn indexed_value_deserialization_revalidates_semantics() {
        let null = ir::SecondaryIndexLiteral::new(PropertyValue::Null).unwrap();
        let json = serde_json::to_string(&null).unwrap();

        assert!(serde_json::from_str::<ExecIndexedEqualityValue>(&json).is_err());
        assert!(
            serde_json::from_value::<ExecIndexedEqualityValue>(serde_json::json!(null)).is_err()
        );
        assert!(serde_json::from_str::<ExecIndexedEqualityValue>("{}").is_err());
    }

    #[test]
    fn expression_constructors_fold_every_arithmetic_family() {
        assert_eq!(ExecUsizeExpr::min(literal(4), literal(2)), literal(2));
        assert_eq!(
            ExecUsizeExpr::min(literal(0), ExecUsizeExpr::Param(name("n"))),
            literal(0)
        );
        assert_eq!(
            ExecUsizeExpr::saturating_add(literal(usize::MAX), literal(1)),
            literal(usize::MAX)
        );
        assert_eq!(
            ExecUsizeExpr::saturating_add(literal(0), literal(7)),
            literal(7)
        );
        assert_eq!(
            ExecUsizeExpr::saturating_sub(literal(2), literal(7)),
            literal(0)
        );
        assert_eq!(
            ExecUsizeExpr::saturating_sub(literal(7), literal(0)),
            literal(7)
        );
        assert_eq!(
            ExecUsizeExpr::saturating_sub(literal(7), literal(7)),
            literal(0)
        );
        assert_eq!(
            ExecUsizeExpr::saturating_add(literal(0), ExecUsizeExpr::Param(name("n"))),
            ExecUsizeExpr::Param(name("n"))
        );
        assert_eq!(
            ExecUsizeExpr::saturating_sub(literal(0), ExecUsizeExpr::Param(name("n"))),
            literal(0)
        );
        assert_eq!(
            ExecUsizeExpr::saturating_add(ExecUsizeExpr::Param(name("n")), literal(0)),
            ExecUsizeExpr::Param(name("n"))
        );
        assert_eq!(
            ExecUsizeExpr::saturating_sub(ExecUsizeExpr::Param(name("n")), literal(0)),
            ExecUsizeExpr::Param(name("n"))
        );
    }

    #[test]
    fn expressions_evaluate_parameters_and_saturating_operations() {
        let expression = ExecUsizeExpr::saturating_sub(
            ExecUsizeExpr::saturating_add(ExecUsizeExpr::Param(name("n")), literal(usize::MAX)),
            ExecUsizeExpr::min(ExecUsizeExpr::Param(name("m")), literal(8)),
        );
        let mut resolve = |name: &ir::NonEmptyString| -> Result<usize, ()> {
            Ok(match name.as_ref() {
                "n" => 10,
                "m" => 3,
                _ => return Err(()),
            })
        };

        assert_eq!(expression.evaluate(&mut resolve), Ok(usize::MAX - 3));
        assert_eq!(expression.node_count(), 7);
        assert_eq!(
            ExecUsizeExpr::Param(name("unknown")).evaluate(&mut resolve),
            Err(())
        );
    }

    #[test]
    fn windows_compose_with_sequential_slice_semantics() {
        let window = ExecCountWindowPlan::identity()
            .then_skip(literal(100))
            .then_limit(literal(10))
            .then_skip(literal(3))
            .then_range(literal(2), literal(5));
        let mut resolve = |_name: &ir::NonEmptyString| -> Result<usize, ()> { Err(()) };
        assert_eq!(resolve(&name("unused")), Err(()));

        assert_eq!(window.skip, literal(105));
        assert_eq!(window.take, ExecCountTake::AtMost(literal(3)));
        assert_eq!(window.apply(200, &mut resolve), Ok(3));
        assert_eq!(window.apply(104, &mut resolve), Ok(0));
    }

    #[test]
    fn unbounded_skip_and_limit_zero_are_canonical() {
        let skipped = ExecCountWindowPlan::identity().then_skip(literal(4));
        let limited = skipped.then_limit(literal(0));
        let mut resolve = |_name: &ir::NonEmptyString| -> Result<usize, ()> { Err(()) };
        assert_eq!(resolve(&name("unused")), Err(()));

        assert_eq!(limited.skip, literal(4));
        assert_eq!(limited.take, ExecCountTake::AtMost(literal(0)));
        assert_eq!(limited.apply(usize::MAX, &mut resolve), Ok(0));
    }

    #[derive(Debug, Clone)]
    enum WindowOp {
        Skip(usize),
        Limit(usize),
        Range(usize, usize),
    }

    fn window_value() -> impl Strategy<Value = usize> {
        prop_oneof![0usize..300, Just(usize::MAX)]
    }

    fn window_op() -> impl Strategy<Value = WindowOp> {
        prop_oneof![
            window_value().prop_map(WindowOp::Skip),
            window_value().prop_map(WindowOp::Limit),
            (window_value(), window_value()).prop_map(|(start, end)| WindowOp::Range(start, end)),
        ]
    }

    proptest! {
        #[test]
        fn arbitrary_window_sequences_match_sequential_slice_semantics(
            input_len in 0usize..200,
            ops in prop::collection::vec(window_op(), 0..40),
        ) {
            let mut plan = ExecCountWindowPlan::identity();
            let mut oracle_len = input_len;

            for op in ops {
                match op {
                    WindowOp::Skip(skip) => {
                        plan = plan.then_skip(literal(skip));
                        oracle_len = oracle_len.saturating_sub(skip);
                    }
                    WindowOp::Limit(limit) => {
                        plan = plan.then_limit(literal(limit));
                        oracle_len = oracle_len.min(limit);
                    }
                    WindowOp::Range(start, end) => {
                        plan = plan.then_range(literal(start), literal(end));
                        oracle_len = oracle_len
                            .saturating_sub(start)
                            .min(end.saturating_sub(start));
                    }
                }
            }

            let mut resolve = |_name: &ir::NonEmptyString| -> Result<usize, ()> { Ok(0) };
            prop_assert_eq!(resolve(&name("unused")), Ok(0));
            prop_assert_eq!(plan.apply(input_len, &mut resolve), Ok(oracle_len));
        }
    }

    #[test]
    fn every_arithmetic_expression_deserializes_through_its_validated_shape() {
        for expression in [
            ExecUsizeExpr::Literal(1),
            ExecUsizeExpr::Param(name("n")),
            ExecUsizeExpr::Min(Box::new(literal(1)), Box::new(literal(2))),
            ExecUsizeExpr::SaturatingAdd(Box::new(literal(1)), Box::new(literal(2))),
            ExecUsizeExpr::SaturatingSub(Box::new(literal(2)), Box::new(literal(1))),
        ] {
            let json = serde_json::to_string(&expression).unwrap();
            assert_eq!(
                serde_json::from_str::<ExecUsizeExpr>(&json).unwrap(),
                expression
            );
            assert_eq!(
                serde_json::from_value::<ExecUsizeExpr>(serde_json::to_value(&expression).unwrap())
                    .unwrap(),
                expression
            );
        }
        assert!(serde_json::from_str::<ExecUsizeExpr>("{}").is_err());
    }

    #[test]
    fn count_contract_round_trips() {
        let plan = ExecCountPlan::InputRows {
            window: ExecCountWindowPlan::identity().then_limit(ExecUsizeExpr::Param(name("limit"))),
        };
        let json = serde_json::to_string(&plan).unwrap();

        assert_eq!(serde_json::from_str::<ExecCountPlan>(&json).unwrap(), plan);
    }

    #[test]
    fn dependency_contract_rejects_multiple_recursive_input_leaves() {
        let plan = ExecCountPlan::Stream(ExecCountStreamPlan {
            cursor: ExecCountCursorPlan::Union {
                driver: Box::new(ExecCountCursorPlan::InputRows),
                rest: ir::AtLeast::from_one(ExecCountCursorPlan::InputRows),
            },
            window: ExecCountWindowPlan::identity(),
        });

        assert_eq!(
            plan.dependency(),
            Err(ExecCountDependencyError::MultipleRowInputs)
        );
        assert_eq!(
            ExecCountPlan::Constant(0).dependency(),
            Ok(ExecCountDependency::Direct)
        );
        assert_eq!(
            ExecCountPlan::InputScalars {
                window: ExecCountWindowPlan::identity(),
            }
            .dependency(),
            Ok(ExecCountDependency::Scalars)
        );
    }

    #[test]
    fn executable_expression_deserialization_rejects_oversized_trees() {
        let mut expression = ExecUsizeExpr::literal(0);
        for value in 0..33 {
            expression = ExecUsizeExpr::SaturatingAdd(
                Box::new(expression),
                Box::new(ExecUsizeExpr::literal(value)),
            );
        }
        assert!(expression.node_count() > MAX_EXEC_USIZE_EXPR_NODES);
        let json = serde_json::to_string(&expression).unwrap();

        let error = serde_json::from_str::<ExecUsizeExpr>(&json).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("executable usize expression exceeds 64 nodes"),
            "{error}"
        );
    }

    #[test]
    fn program_validation_rejects_oversized_programmatic_windows() {
        let mut expression = ExecUsizeExpr::literal(0);
        for value in 0..33 {
            expression = ExecUsizeExpr::SaturatingAdd(
                Box::new(expression),
                Box::new(ExecUsizeExpr::literal(value)),
            );
        }
        let plan = ExecCountPlan::InputRows {
            window: ExecCountWindowPlan {
                skip: expression,
                take: ExecCountTake::All,
            },
        };

        assert_eq!(
            plan.validate(),
            Err(ExecCountValidationError::WindowExpressionTooLarge)
        );
    }

    #[test]
    fn program_validation_rejects_mismatched_unique_verification() {
        let index = ExecNodeUniqueEqualityIndex::try_from(
            catalog::NodeEqualityIndexMeta::new(name("node_eq:User:email"))
                .with_uniqueness(catalog::IndexUniqueness::Unique),
        )
        .unwrap();
        let value = ExecIndexedEqualityValue::try_from(
            ir::SecondaryIndexLiteral::new(PropertyValue::from("alice@example.com")).unwrap(),
        )
        .unwrap();
        let plan = ExecCountPlan::NodeUnique(ExecNodeUniqueCountPlan {
            lookup: ExecNodeUniqueOwnerReadPlan {
                index,
                key: catalog::ScopedPropertyKey::try_new("User", "email").unwrap(),
                value: value.clone(),
            },
            verification: ExecNodeAuthoritativeVerificationPlan {
                key: catalog::ScopedPropertyKey::try_new("User", "other_email").unwrap(),
                value,
            },
            window: ExecCountWindowPlan::identity(),
        });

        assert_eq!(
            plan.validate(),
            Err(ExecCountValidationError::UniqueVerificationMismatch)
        );

        let (lookup, mut verification) = unique_parts();
        verification.value = indexed("bob@example.com");
        let plan = ExecCountPlan::NodeUnique(ExecNodeUniqueCountPlan {
            lookup,
            verification,
            window: ExecCountWindowPlan::identity(),
        });
        assert_eq!(
            plan.validate(),
            Err(ExecCountValidationError::UniqueVerificationMismatch)
        );
    }

    #[test]
    fn equality_wrappers_expose_owned_and_borrowed_validated_payloads() {
        let non_unique = node_index();
        assert_eq!(
            non_unique.metadata().index_id.as_ref(),
            "node_eq:User:status"
        );
        assert_eq!(
            non_unique.into_metadata().uniqueness,
            catalog::IndexUniqueness::NonUnique
        );

        let (lookup, _) = unique_parts();
        assert_eq!(
            lookup.index.metadata().uniqueness,
            catalog::IndexUniqueness::Unique
        );
        assert_eq!(
            lookup.index.into_metadata().uniqueness,
            catalog::IndexUniqueness::Unique
        );

        let edge = edge_index();
        assert_eq!(edge.metadata().index_id.as_ref(), "edge_eq:LIKES:status");
        assert_eq!(
            edge.into_metadata().index_id.as_ref(),
            "edge_eq:LIKES:status"
        );

        let value = indexed("active");
        assert_eq!(
            value.literal().as_property_value(),
            &PropertyValue::from("active")
        );
        assert_eq!(
            value.into_literal().as_property_value(),
            &PropertyValue::from("active")
        );

        for error in [
            ExecEqualityContractError::ExpectedNodeNonUnique,
            ExecEqualityContractError::ExpectedNodeUnique,
            ExecEqualityContractError::ExpectedIndexedValue,
        ] {
            assert!(!error.to_string().is_empty());
            let error: &dyn std::error::Error = &error;
            assert!(error.source().is_none());
        }
    }

    #[test]
    fn arithmetic_unsimplified_branches_and_default_window_are_executable() {
        let n = ExecUsizeExpr::Param(name("n"));
        let m = ExecUsizeExpr::Param(name("m"));
        assert!(matches!(
            ExecUsizeExpr::min(n.clone(), m.clone()),
            ExecUsizeExpr::Min(_, _)
        ));
        assert!(matches!(
            ExecUsizeExpr::saturating_add(n.clone(), m.clone()),
            ExecUsizeExpr::SaturatingAdd(_, _)
        ));
        assert!(matches!(
            ExecUsizeExpr::saturating_sub(n, m),
            ExecUsizeExpr::SaturatingSub(_, _)
        ));
        assert_eq!(
            ExecCountWindowPlan::default(),
            ExecCountWindowPlan::identity()
        );
    }

    #[test]
    fn every_cursor_variant_round_trips_and_validates() {
        let cursors = cursor_leaves()
            .into_iter()
            .chain(wrapped_cursors())
            .collect::<Vec<_>>();
        for cursor in cursors {
            let plan = ExecCountPlan::Stream(ExecCountStreamPlan {
                cursor,
                window: ExecCountWindowPlan::identity(),
            });
            let json = serde_json::to_string(&plan).unwrap();
            assert_eq!(serde_json::from_str::<ExecCountPlan>(&json).unwrap(), plan);
            assert!(plan.validate().is_ok());
        }

        let dependency_wrappers = [
            ExecCountCursorPlan::Filter {
                input: Box::new(ExecCountCursorPlan::InputRows),
                predicate: predicate(),
            },
            ExecCountCursorPlan::Window {
                input: Box::new(ExecCountCursorPlan::InputRows),
                window: ExecCountWindowPlan::identity(),
            },
            ExecCountCursorPlan::Order {
                input: Box::new(ExecCountCursorPlan::InputRows),
                plan: ir::OrderPlan::ExplicitSort(ir::OrderKeys::from(ir::OrderKey {
                    property: name("age"),
                    order: Order::Desc,
                })),
            },
            ExecCountCursorPlan::Expand {
                input: Box::new(ExecCountCursorPlan::InputRows),
                plan: ir::ExpandPlan {
                    direction: ir::ExpandDirection::Both,
                    output: ir::ExpandOutput::Edges,
                    label: ir::ExpandLabelPlan::Any,
                },
            },
            ExecCountCursorPlan::Variable {
                input: Box::new(ExecCountCursorPlan::InputRows),
                op: crate::logical::PureStreamVariableOp::Bind(name("row")),
            },
            ExecCountCursorPlan::Distinct {
                input: Box::new(ExecCountCursorPlan::InputRows),
                plan: ExecCountDistinctPlan::HashRows,
            },
        ];
        for cursor in dependency_wrappers {
            let plan = ExecCountPlan::Stream(ExecCountStreamPlan {
                cursor,
                window: ExecCountWindowPlan::identity(),
            });
            assert_eq!(plan.dependency(), Ok(ExecCountDependency::Rows));
        }
    }

    #[test]
    fn every_direct_plan_variant_round_trips_validates_and_is_dependency_free() {
        let window = ExecCountWindowPlan::identity();
        let (lookup, verification) = unique_parts();
        let node_batch = ExecNodeBitmapExpr::BatchedUnionRead {
            index: node_index(),
            key: catalog::ScopedPropertyKey::try_new("User", "status").unwrap(),
            values: ir::AtLeast::from_pair(indexed("active"), indexed("inactive")),
        };
        let edge_batch = ExecEdgeBitmapExpr::BatchedUnionRead {
            index: edge_index(),
            key: catalog::ScopedPropertyKey::try_new("LIKES", "status").unwrap(),
            values: ir::AtLeast::from_pair(indexed("active"), indexed("inactive")),
        };
        let plans = vec![
            ExecCountPlan::Constant(7),
            ExecCountPlan::NodeBitmap(ExecNodeBitmapCountPlan {
                bitmap: ExecNodeBitmapExpr::Union {
                    driver: Box::new(node_batch),
                    rest: ir::AtLeast::from_one(node_point("pending")),
                },
                window: window.clone(),
            }),
            ExecCountPlan::EdgeBitmap(ExecEdgeBitmapCountPlan {
                bitmap: ExecEdgeBitmapExpr::Intersect {
                    driver: Box::new(edge_batch),
                    rest: ir::AtLeast::from_one(edge_point("pending")),
                },
                window: window.clone(),
            }),
            ExecCountPlan::NodeUnique(ExecNodeUniqueCountPlan {
                lookup,
                verification,
                window: window.clone(),
            }),
            ExecCountPlan::NodeRange(ExecNodeRangeCountPlan {
                driver: node_range(),
                membership: ExecNodeRangeMembershipPlan::BitmapFilters(ir::AtLeast::from_one(
                    node_point("active"),
                )),
                window: window.clone(),
            }),
            ExecCountPlan::EdgeRange(ExecEdgeRangeCountPlan {
                driver: edge_range(),
                membership: ExecEdgeRangeMembershipPlan::BitmapFilters(ir::AtLeast::from_one(
                    edge_point("active"),
                )),
                window: window.clone(),
            }),
            ExecCountPlan::NodeAuthoritativeScan(ExecNodeScanCountPlan {
                predicate: ExecNodeAuthoritativeScanPredicate::Predicate(predicate()),
                window: window.clone(),
            }),
            ExecCountPlan::EdgeAuthoritativeScan(ExecEdgeScanCountPlan {
                predicate: ExecEdgeAuthoritativeScanPredicate::NullEquality {
                    key: catalog::ScopedPropertyKey::try_new("LIKES", "status").unwrap(),
                },
                window: window.clone(),
            }),
            ExecCountPlan::NodePointReads {
                ids: ids(),
                window: window.clone(),
            },
            ExecCountPlan::EdgePointReads {
                ids: ids(),
                window: window.clone(),
            },
            ExecCountPlan::NodeRuntimeInput {
                input: ExecRuntimeInputPlan::Param(name("nodes")),
                window: window.clone(),
            },
            ExecCountPlan::EdgeRuntimeInput {
                input: ExecRuntimeInputPlan::Variable(name("edges")),
                window: window.clone(),
            },
            ExecCountPlan::RuntimeInput {
                input: ExecRuntimeInputPlan::Param(name("rows")),
                window: window.clone(),
            },
            ExecCountPlan::NodeFullScan {
                window: window.clone(),
            },
            ExecCountPlan::EdgeFullScan {
                window: window.clone(),
            },
            ExecCountPlan::NodeLabelBitmap {
                label: name("User"),
                window: window.clone(),
            },
            ExecCountPlan::EdgeLabelBitmap {
                label: name("LIKES"),
                window: window.clone(),
            },
            ExecCountPlan::NodeVectorSearch(ExecNodeVectorSearchCountPlan {
                key: catalog::NodeSearchIndexKey::try_new("User", "embedding").unwrap(),
                index: search_index(),
                query_vector: vector_input(),
                k: search_limit(),
                window: window.clone(),
            }),
            ExecCountPlan::EdgeVectorSearch(ExecEdgeVectorSearchCountPlan {
                key: catalog::EdgeSearchIndexKey::try_new("LIKES", "embedding").unwrap(),
                index: search_index(),
                query_vector: vector_input(),
                k: search_limit(),
                window: window.clone(),
            }),
            ExecCountPlan::NodeTextSearch(ExecNodeTextSearchCountPlan {
                key: catalog::NodeSearchIndexKey::try_new("User", "body").unwrap(),
                index: search_index(),
                query_text: text_input(),
                k: search_limit(),
                window: window.clone(),
            }),
            ExecCountPlan::EdgeTextSearch(ExecEdgeTextSearchCountPlan {
                key: catalog::EdgeSearchIndexKey::try_new("LIKES", "body").unwrap(),
                index: search_index(),
                query_text: text_input(),
                k: search_limit(),
                window: window.clone(),
            }),
            ExecCountPlan::NodeDynamicEquality(ExecNodeDynamicEqualityCountPlan {
                index: catalog::NodeEqualityIndexMeta::new(name("node_eq:User:status")),
                key: catalog::ScopedPropertyKey::try_new("User", "status").unwrap(),
                param: name("status"),
                window: window.clone(),
            }),
            ExecCountPlan::EdgeDynamicEquality(ExecEdgeDynamicEqualityCountPlan {
                index: catalog::EdgeEqualityIndexMeta::new(name("edge_eq:LIKES:status")),
                key: catalog::ScopedPropertyKey::try_new("LIKES", "status").unwrap(),
                param: name("status"),
                window: window.clone(),
            }),
            ExecCountPlan::InputRows {
                window: window.clone(),
            },
            ExecCountPlan::InputScalars { window },
        ];

        for plan in plans {
            let json = serde_json::to_string(&plan).unwrap();
            assert_eq!(serde_json::from_str::<ExecCountPlan>(&json).unwrap(), plan);
            assert!(plan.validate().is_ok());
            if !matches!(
                plan,
                ExecCountPlan::InputRows { .. } | ExecCountPlan::InputScalars { .. }
            ) {
                assert_eq!(plan.dependency(), Ok(ExecCountDependency::Direct));
            }
        }
    }

    #[test]
    fn nested_validation_rejects_each_recursive_invalid_state() {
        let (lookup, mut verification) = unique_parts();
        verification.value = indexed("bob@example.test");
        let invalid_unique = ExecCountCursorPlan::NodeUnique {
            lookup,
            verification,
        };
        let plan = ExecCountPlan::Stream(ExecCountStreamPlan {
            cursor: ExecCountCursorPlan::Union {
                driver: Box::new(ExecCountCursorPlan::NodeFullScan),
                rest: ir::AtLeast::from_one(invalid_unique),
            },
            window: ExecCountWindowPlan::identity(),
        });
        assert_eq!(
            plan.validate(),
            Err(ExecCountValidationError::UniqueVerificationMismatch)
        );

        let mut too_large = ExecUsizeExpr::literal(0);
        for value in 0..33 {
            too_large = ExecUsizeExpr::SaturatingAdd(
                Box::new(too_large),
                Box::new(ExecUsizeExpr::literal(value)),
            );
        }
        let plan = ExecCountPlan::Stream(ExecCountStreamPlan {
            cursor: ExecCountCursorPlan::Window {
                input: Box::new(ExecCountCursorPlan::EmptyRows),
                window: ExecCountWindowPlan {
                    skip: ExecUsizeExpr::literal(0),
                    take: ExecCountTake::AtMost(too_large),
                },
            },
            window: ExecCountWindowPlan::identity(),
        });
        assert_eq!(
            plan.validate(),
            Err(ExecCountValidationError::WindowExpressionTooLarge)
        );
    }

    #[test]
    fn arithmetic_and_recursive_error_propagation_is_exhaustive() {
        for expression in [
            ExecUsizeExpr::Min(
                Box::new(ExecUsizeExpr::Param(name("missing"))),
                Box::new(literal(1)),
            ),
            ExecUsizeExpr::Min(
                Box::new(literal(1)),
                Box::new(ExecUsizeExpr::Param(name("missing"))),
            ),
            ExecUsizeExpr::SaturatingAdd(
                Box::new(ExecUsizeExpr::Param(name("missing"))),
                Box::new(literal(1)),
            ),
            ExecUsizeExpr::SaturatingAdd(
                Box::new(literal(1)),
                Box::new(ExecUsizeExpr::Param(name("missing"))),
            ),
            ExecUsizeExpr::SaturatingSub(
                Box::new(ExecUsizeExpr::Param(name("missing"))),
                Box::new(literal(1)),
            ),
            ExecUsizeExpr::SaturatingSub(
                Box::new(literal(1)),
                Box::new(ExecUsizeExpr::Param(name("missing"))),
            ),
        ] {
            let mut resolve = |_name: &ir::NonEmptyString| -> Result<usize, ()> { Err(()) };
            assert_eq!(expression.evaluate(&mut resolve), Err(()));
        }
        for window in [
            ExecCountWindowPlan {
                skip: ExecUsizeExpr::Param(name("missing")),
                take: ExecCountTake::All,
            },
            ExecCountWindowPlan {
                skip: literal(0),
                take: ExecCountTake::AtMost(ExecUsizeExpr::Param(name("missing"))),
            },
        ] {
            let mut resolve = |_name: &ir::NonEmptyString| -> Result<usize, ()> { Err(()) };
            assert_eq!(window.apply(10, &mut resolve), Err(()));
        }

        let invalid_pair = || ExecCountCursorPlan::Union {
            driver: Box::new(ExecCountCursorPlan::InputRows),
            rest: ir::AtLeast::from_one(ExecCountCursorPlan::InputRows),
        };
        for cursor in [
            ExecCountCursorPlan::Union {
                driver: Box::new(invalid_pair()),
                rest: ir::AtLeast::from_one(ExecCountCursorPlan::EmptyRows),
            },
            ExecCountCursorPlan::Union {
                driver: Box::new(ExecCountCursorPlan::EmptyRows),
                rest: ir::AtLeast::from_one(invalid_pair()),
            },
            ExecCountCursorPlan::Filter {
                input: Box::new(invalid_pair()),
                predicate: predicate(),
            },
        ] {
            let plan = ExecCountPlan::Stream(ExecCountStreamPlan {
                cursor,
                window: ExecCountWindowPlan::identity(),
            });
            assert_eq!(
                plan.dependency(),
                Err(ExecCountDependencyError::MultipleRowInputs)
            );
        }

        let (lookup, mut verification) = unique_parts();
        verification.key = catalog::ScopedPropertyKey::try_new("User", "other_email").unwrap();
        let plan = ExecCountPlan::Stream(ExecCountStreamPlan {
            cursor: ExecCountCursorPlan::NodeUnique {
                lookup,
                verification,
            },
            window: ExecCountWindowPlan::identity(),
        });
        assert_eq!(
            plan.validate(),
            Err(ExecCountValidationError::UniqueVerificationMismatch)
        );

        let invalid_unique = || {
            let (lookup, mut verification) = unique_parts();
            verification.value = indexed("other@example.test");
            ExecCountCursorPlan::NodeUnique {
                lookup,
                verification,
            }
        };
        for cursor in [
            ExecCountCursorPlan::Union {
                driver: Box::new(invalid_unique()),
                rest: ir::AtLeast::from_one(ExecCountCursorPlan::EmptyRows),
            },
            ExecCountCursorPlan::Window {
                input: Box::new(invalid_unique()),
                window: ExecCountWindowPlan::identity(),
            },
        ] {
            assert_eq!(
                ExecCountPlan::Stream(ExecCountStreamPlan {
                    cursor,
                    window: ExecCountWindowPlan::identity(),
                })
                .validate(),
                Err(ExecCountValidationError::UniqueVerificationMismatch)
            );
        }
    }
}
