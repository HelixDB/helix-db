//! A validated bound on source demand through consecutive total projections.
//! This proof stops at the first window with LIMIT. All preceding projections
//! preserve one row per input except for SKIP, whose offsets compose by addition.

use super::{pipeline::Termination, Expression, Operator, Query, Result, Value};

/// Derived only from a validated query. It cannot be deserialized independently
/// of the operator contracts that establish its early-termination proof.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct InputWindow {
    projection: usize,
    skips: Vec<Expression>,
    limit: Expression,
    termination: Termination,
}

impl InputWindow {
    pub(super) fn expression_heap_bytes(&self, heap: impl Fn(&Expression) -> usize) -> usize {
        self.skips.iter().fold(
            self.skips
                .capacity()
                .saturating_mul(size_of::<Expression>())
                .saturating_add(heap(&self.limit)),
            |bytes, expression| bytes.saturating_add(heap(expression)),
        )
    }
    pub(super) fn map_expressions(&self, map: impl Fn(&Expression) -> Expression) -> Self {
        Self {
            projection: self.projection,
            skips: self.skips.iter().map(&map).collect(),
            limit: map(&self.limit),
            termination: self.termination,
        }
    }
    pub(super) fn for_initial_source(query: &Query) -> Option<Self> {
        // A correlated source can contain later failing input expressions.
        // Only the initial source has the required single-input proof.
        let termination = match query.operators().first()? {
            Operator::Unwind { .. } => Termination::AfterFirstBatch,
            Operator::Match {
                pattern,
                predicate: None,
                ..
            } => {
                let mut constraints = pattern
                    .nodes
                    .iter()
                    .flat_map(|node| &node.properties)
                    .chain(
                        pattern
                            .relationships
                            .iter()
                            .flat_map(|edge| &edge.properties),
                    );
                if constraints.clone().next().is_none() {
                    Termination::BeforeInput
                } else if constraints.all(|(_, expression)| {
                    matches!(
                        expression,
                        Expression::Literal(_) | Expression::Parameter(_)
                    )
                }) {
                    // A complete match has evaluated every immutable constraint.
                    // Parameters cannot fail on a later row once that succeeds.
                    // Demand at least one result even for LIMIT 0, so a missing
                    // parameter is never hidden by skipping source evaluation.
                    Termination::AfterFirstBatch
                } else {
                    return None;
                }
            }
            Operator::Match { .. }
            | Operator::Filter(_)
            | Operator::Project { .. }
            | Operator::Create(_)
            | Operator::Update(_)
            | Operator::Delete { .. } => return None,
        };
        let mut skips = Vec::new();
        for (projection, operator) in query.operators().iter().enumerate().skip(1) {
            let Operator::Project { skip, limit, .. } = operator else {
                return None;
            };
            if !query.contracts()[projection].is_total_projection() {
                return None;
            }
            skips.extend(skip.iter().cloned());
            let Some(limit) = limit else {
                continue;
            };
            return Some(Self {
                projection,
                skips,
                limit: limit.clone(),
                termination,
            });
        }
        None
    }

    /// Absolute operator index whose remaining output allowance bounds input.
    pub fn projection(&self) -> usize {
        self.projection
    }

    pub fn termination(&self) -> Termination {
        self.termination
    }

    /// Evaluate offsets in clause order without creating an arithmetic query
    /// expression. Saturation bounds demand and must not introduce an observable
    /// integer-overflow error into otherwise valid SKIP/LIMIT clauses.
    pub fn demand(&self, mut evaluate: impl FnMut(&Expression) -> Result<Value>) -> Result<usize> {
        let skip = self.skips.iter().try_fold(0_usize, |offset, expression| {
            Ok::<_, super::QueryError>(
                offset.saturating_add(super::nonnegative(&evaluate(expression)?)?),
            )
        })?;
        let demand = skip.saturating_add(super::nonnegative(&evaluate(&self.limit)?)?);
        Ok(match self.termination {
            Termination::AfterFirstBatch => demand.max(1),
            Termination::Drain | Termination::BeforeInput => demand,
        })
    }

    /// A constant upper bound for costing, without evaluating parameters or
    /// admitting expression errors during optimizer exploration.
    pub(super) fn literal_demand(&self) -> Option<u64> {
        self.skips
            .iter()
            .chain(std::iter::once(&self.limit))
            .try_fold(0_u64, |offset, expression| {
                let Expression::Literal(Value::Integer(value)) = expression else {
                    return None;
                };
                Some(offset.saturating_add(u64::try_from(*value).ok()?))
            })
            .map(|demand| match self.termination {
                Termination::AfterFirstBatch => demand.max(1),
                Termination::Drain | Termination::BeforeInput => demand,
            })
    }
}
