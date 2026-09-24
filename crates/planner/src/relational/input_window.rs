//! A validated bound on source demand through consecutive total projections.
//! Every window in the proven prefix can stop its source. Literal downstream
//! windows also tighten source demand; dynamic windows retain their existing
//! consumer-side evaluation order.

use super::{pipeline::Termination, Expression, Operator, Query, Result, Value};

/// Derived only from a validated query. It cannot be deserialized independently
/// of the operator contracts that establish its early-termination proof.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct InputWindow {
    projection: usize,
    last_projection: usize,
    skips: Vec<Expression>,
    limit: Expression,
    downstream_demand: usize,
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
            last_projection: self.last_projection,
            skips: self.skips.iter().map(&map).collect(),
            limit: map(&self.limit),
            downstream_demand: self.downstream_demand,
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
        let mut window: Option<Self> = None;
        let mut downstream_skips = Some(0_usize);
        for (projection, operator) in query.operators().iter().enumerate().skip(1) {
            let Operator::Project { skip, limit, .. } = operator else {
                break;
            };
            if !query.contracts()[projection].is_total_projection() {
                break;
            }
            let Some(window) = &mut window else {
                skips.extend(skip.iter().cloned());
                let Some(limit) = limit else {
                    continue;
                };
                window = Some(Self {
                    projection,
                    last_projection: projection,
                    skips: std::mem::take(&mut skips),
                    limit: limit.clone(),
                    downstream_demand: usize::MAX,
                    termination,
                });
                continue;
            };
            if limit.is_some() {
                window.last_projection = projection;
            }
            // Do not evaluate later parameters or failing expressions ahead
            // of their existing consumer boundary. Once such a window is
            // reached, only the consumer's remaining counters may stop us.
            let Some(skipped) = downstream_skips else {
                continue;
            };
            let literal = |expression: &Option<Expression>, default| match expression {
                None => Some(default),
                Some(Expression::Literal(Value::Integer(value))) => usize::try_from(*value).ok(),
                _ => None,
            };
            let Some((skip, limit)) = literal(skip, 0).zip(literal(limit, usize::MAX)) else {
                downstream_skips = None;
                continue;
            };
            let skipped = skipped.saturating_add(skip);
            downstream_skips = Some(skipped);
            // Each tail window caps its input at cumulative SKIP + LIMIT. An
            // earlier cap still wins when a later skip exhausts that output.
            window.downstream_demand = window.downstream_demand.min(skipped.saturating_add(limit));
        }
        window
    }

    /// Absolute index of the first bounded projection and its validation point.
    pub fn projection(&self) -> usize {
        self.projection
    }

    /// Last bounded projection in the consecutive total prefix. Exhausting any
    /// window through this index permits stopping after downstream continuations
    /// finish; an earlier window may run out before this one receives a row.
    pub fn last_projection(&self) -> usize {
        self.last_projection
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
        let limit = super::nonnegative(&evaluate(&self.limit)?)?;
        let demand = skip.saturating_add(limit.min(self.downstream_demand));
        Ok(match self.termination {
            Termination::AfterFirstBatch => demand.max(1),
            Termination::Drain | Termination::BeforeInput => demand,
        })
    }

    /// A constant upper bound for costing, without evaluating parameters or
    /// admitting expression errors during optimizer exploration.
    pub(super) fn literal_demand(&self) -> Option<u64> {
        let skip = self.skips.iter().try_fold(0_u64, |offset, expression| {
            let Expression::Literal(Value::Integer(value)) = expression else {
                return None;
            };
            Some(offset.saturating_add(u64::try_from(*value).ok()?))
        })?;
        let Expression::Literal(Value::Integer(limit)) = &self.limit else {
            return None;
        };
        let demand = skip.saturating_add(
            u64::try_from(*limit)
                .ok()?
                .min(self.downstream_demand as u64),
        );
        Some(match self.termination {
            Termination::AfterFirstBatch => demand.max(1),
            Termination::Drain | Termination::BeforeInput => demand,
        })
    }
}
