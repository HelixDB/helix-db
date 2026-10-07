//! Normalize mixed Cypher aggregates into shared aggregate and scalar programs.
//! Indexes borrow expression owners and bound exact-identity candidate work.
use helix_planner::{digest, relational as r};
use std::{collections::BTreeMap, ops::ControlFlow};

const MAX_IDENTITY_CANDIDATES: usize = 8;

/// Consume a validated Cypher aggregating projection into direct aggregate
/// states followed by ordinary scalar expressions. Input binding IDs remain
/// stable. The second program preserves public column order; the shared Query
/// constructor validates both programs against their actual incoming schemas.
pub(super) fn split_projection(
    projections: Vec<r::Projection>,
    bindings: &mut Vec<r::Binding>,
) -> r::Result<(r::ProjectionProgram, r::ProjectionProgram)> {
    split_projection_with(projections, bindings, aggregate_identity)
}

fn split_projection_with(
    projections: Vec<r::Projection>,
    bindings: &mut Vec<r::Binding>,
    mut identity: impl FnMut(&r::Expression) -> r::Result<(digest::PlanDigest, Vec<u64>)>,
) -> r::Result<(r::ProjectionProgram, r::ProjectionProgram)> {
    enum Part {
        Group(r::Projection),
        Aggregate(r::Projection),
    }
    let mut groups = Vec::new();
    let mut parts = Vec::with_capacity(projections.len());
    for projection in projections {
        if projection.expression.has_aggregate() {
            parts.push(Part::Aggregate(projection));
        } else {
            parts.push(Part::Group(r::Projection {
                slot: projection.slot,
                expression: r::Expression::Slot(projection.slot),
            }));
            groups.push(projection);
        }
    }
    let mut states: Vec<r::Projection> = Vec::new();
    let mut identities = BTreeMap::<digest::PlanDigest, Vec<StateIdentity>>::new();
    let mut post = Vec::with_capacity(parts.len());
    for part in parts {
        let projection = match part {
            Part::Aggregate(projection) => projection,
            Part::Group(projection) => {
                post.push(projection);
                continue;
            }
        };
        let preferred = matches!(projection.expression, r::Expression::Aggregate { .. })
            .then_some(projection.slot);
        let expression = projection.expression.rewrite_owned(&mut |expression| {
            if matches!(expression, r::Expression::Aggregate { .. }) {
                let (digest, float_bits) = identity(&expression)?;
                let existing = identities.get(&digest).and_then(|bucket| {
                    bucket
                        .iter()
                        .find(|candidate| {
                            candidate.float_bits == float_bits
                                && states[candidate.index].expression == expression
                        })
                        .map(|candidate| &states[candidate.index])
                });
                let slot = match existing {
                    Some(state) => state.slot,
                    None => {
                        if identities
                            .get(&digest)
                            .is_some_and(|bucket| bucket.len() >= MAX_IDENTITY_CANDIDATES)
                        {
                            return Err(r::QueryError::compile(
                                "ResourceLimit",
                                "AggregateIdentityBudget",
                                "aggregate identity collision budget exhausted",
                            ));
                        }
                        let slot = match preferred {
                            Some(slot) => slot,
                            None => {
                                if bindings.len() >= 4096 {
                                    return Err(r::QueryError::compile(
                                        "ResourceLimit",
                                        "TooManyBindings",
                                        "aggregate extraction exceeds the binding budget",
                                    ));
                                }
                                let value_type = expression.value_type(bindings)?;
                                let slot = r::Slot(bindings.len() as u32);
                                bindings.push(r::Binding {
                                    name: format!("_aggregate_{}", slot.0),
                                    kind: r::BindingType::Scalar,
                                    nullable: true,
                                    value_type,
                                });
                                slot
                            }
                        };
                        let index = states.len();
                        states.push(r::Projection { slot, expression });
                        identities
                            .entry(digest)
                            .or_default()
                            .push(StateIdentity { float_bits, index });
                        return Ok(ControlFlow::Break(r::Expression::Slot(slot)));
                    }
                };
                return Ok(ControlFlow::Break(r::Expression::Slot(slot)));
            }
            let recognized = matches!(expression, r::Expression::Slot(_))
                || matches!(&expression, r::Expression::Property(base, _) if matches!(base.as_ref(), r::Expression::Slot(_)));
            if recognized {
                let grouped = groups.iter().find(|group| group.expression == expression);
                let Some(grouped) = grouped else {
                    if matches!(expression, r::Expression::Slot(_)) {
                        return Err(r::QueryError::compile(
                            "SyntaxError",
                            "AmbiguousAggregationExpression",
                            "unbound grouping dependency",
                        ));
                    }
                    return Ok(ControlFlow::Continue(expression));
                };
                return Ok(ControlFlow::Break(r::Expression::Slot(grouped.slot)));
            }
            let r::Expression::HasLabel(slot, label) = expression else {
                return Ok(ControlFlow::Continue(expression));
            };
            let Some(grouped) = groups
                .iter()
                .find(|group| group.expression == r::Expression::Slot(slot))
            else {
                return Err(r::QueryError::compile(
                    "SyntaxError",
                    "AmbiguousAggregationExpression",
                    "unbound label grouping dependency",
                ));
            };
            Ok(ControlFlow::Break(r::Expression::HasLabel(grouped.slot, label)))
        })?;
        post.push(r::Projection {
            slot: projection.slot,
            expression,
        });
    }
    Ok((
        r::ProjectionProgram::new(groups.into_iter().chain(states).collect())?,
        r::ProjectionProgram::new(post)?,
    ))
}

// PartialEq handles complete expression shape and all ordinary payloads. Exact
// float bits additionally distinguish signed zero, including inside literals.
// Digest equality only selects candidates; it never authorizes substitution.
fn aggregate_identity(expression: &r::Expression) -> r::Result<(digest::PlanDigest, Vec<u64>)> {
    expression.validate_shape()?;
    fn floats(value: &r::Value, bits: &mut Vec<u64>) {
        match value {
            r::Value::Float(value) => bits.push(value.to_bits()),
            r::Value::List(values) => values.iter().for_each(|value| floats(value, bits)),
            r::Value::Map(values) => values.values().for_each(|value| floats(value, bits)),
            r::Value::Null
            | r::Value::Boolean(_)
            | r::Value::Integer(_)
            | r::Value::String(_)
            | r::Value::Entity(_)
            | r::Value::Path(_) => {}
        }
    }
    let mut bits = Vec::new();
    expression.visit(&mut |node| {
        let r::Expression::Literal(value) = node else {
            return;
        };
        floats(value, &mut bits);
    });
    Ok((
        digest::PlanDigest::for_tagged_value("aggregate:v1", expression),
        bits,
    ))
}

struct StateIdentity {
    float_bits: Vec<u64>,
    index: usize,
}
struct Candidate<'a, T> {
    float_bits: Vec<u64>,
    expression: &'a r::Expression,
    target: T,
}

/// Borrowed exact-expression lookup. Hash buckets select candidates only, and
/// at most eight distinct candidates are admitted for any digest. Expression
/// depth is validated before hashing; payloads stay with their projection owner.
pub(super) struct ExpressionIndex<'a, T> {
    buckets: BTreeMap<digest::PlanDigest, Vec<Candidate<'a, T>>>,
}
impl<'a, T: Copy> ExpressionIndex<'a, T> {
    pub(super) fn new(items: impl Iterator<Item = (&'a r::Expression, T)>) -> r::Result<Self> {
        let mut index = Self {
            buckets: BTreeMap::new(),
        };
        for (expression, target) in items {
            index.insert(expression, target, aggregate_identity(expression)?)?;
        }
        Ok(index)
    }
    fn insert(
        &mut self,
        expression: &'a r::Expression,
        target: T,
        (digest, bits): (digest::PlanDigest, Vec<u64>),
    ) -> r::Result<()> {
        let bucket = self.buckets.entry(digest).or_default();
        if bucket
            .iter()
            .any(|candidate| candidate.float_bits == bits && candidate.expression == expression)
        {
            return Ok(());
        }
        if bucket.len() >= MAX_IDENTITY_CANDIDATES {
            return Err(r::QueryError::compile(
                "ResourceLimit",
                "AggregateIdentityBudget",
                "expression identity collision budget exhausted",
            ));
        }
        bucket.push(Candidate {
            float_bits: bits,
            expression,
            target,
        });
        Ok(())
    }
    fn find_identity(
        &self,
        expression: &r::Expression,
        (digest, bits): (digest::PlanDigest, Vec<u64>),
    ) -> Option<T> {
        let bucket = self.buckets.get(&digest)?;
        bucket
            .iter()
            .find(|candidate| candidate.float_bits == bits && candidate.expression == expression)
            .map(|candidate| candidate.target)
    }
    pub(super) fn find(&self, expression: &r::Expression) -> r::Result<Option<T>> {
        Ok(self.find_identity(expression, aggregate_identity(expression)?))
    }
}

#[cfg(test)]
mod tests;
