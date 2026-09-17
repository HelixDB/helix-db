//! Derived operator contracts. Scope, correlation, multiplicity and barriers are
//! computed once at the validated query boundary and shared with planning.
use super::{Binding, Effect, Expression, Operator, Slot, ValueType};
use std::collections::BTreeSet;

mod schema;
pub use schema::RowSchema;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub struct ColumnType {
    pub value_type: ValueType,
    pub nullable: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub enum Multiplicity {
    PreservesRows,
    MayReduceRows,
    MayMultiplyRows,
    GroupsRows,
}

/// Boundaries across which reordering must not change null extension, writes,
/// grouping or user-visible ordering. These are independent of storage effects.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub enum Boundary {
    OptionalMatch,
    Mutation,
    Aggregate,
    Distinct,
    Ordering,
    Window,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub enum Correlation {
    Independent,
    Bound(crate::ir::AtLeast<Slot, 1>),
}

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct OperatorContract {
    input: RowSchema,
    output: RowSchema,
    references: BTreeSet<Slot>,
    correlation: Correlation,
    effect: Effect,
    multiplicity: Multiplicity,
    boundaries: Vec<Boundary>,
    total_projection: bool,
}

impl OperatorContract {
    pub fn input(&self) -> &RowSchema {
        &self.input
    }
    pub fn output(&self) -> &RowSchema {
        &self.output
    }
    pub fn references(&self) -> &BTreeSet<Slot> {
        &self.references
    }
    pub fn correlation(&self) -> &Correlation {
        &self.correlation
    }
    pub fn effect(&self) -> Effect {
        self.effect
    }
    pub fn multiplicity(&self) -> Multiplicity {
        self.multiplicity
    }
    pub fn boundaries(&self) -> &[Boundary] {
        &self.boundaries
    }
    /// Parameters are total only after request-wide parameter validation. Property
    /// reads, arithmetic and functions remain potentially failing operations.
    pub fn is_total_projection(&self) -> bool {
        self.total_projection
    }

    pub(super) fn derive(
        operator: &Operator,
        input_schema: &RowSchema,
        output: &BTreeSet<Slot>,
        bindings: &[Binding],
    ) -> Self {
        let input = input_schema;
        // Query::new derives every scope from one immutable binding catalog.
        debug_assert!(input.iter().all(|(slot, column)| {
            let binding = &bindings[slot.0 as usize];
            column.value_type == binding.value_type && (!binding.nullable || column.nullable)
        }));
        let mut references = operator
            .expressions()
            .into_iter()
            .flat_map(Expression::slots)
            .collect::<BTreeSet<_>>();
        let mut boundaries = Vec::new();
        let mut effect = Effect::Read;
        let mut total_projection = false;
        let multiplicity = match operator {
            Operator::Match {
                pattern, optional, ..
            } => {
                references.extend(
                    pattern
                        .slots()
                        .into_iter()
                        .filter(|slot| input.get(*slot).is_some()),
                );
                if *optional {
                    boundaries.push(Boundary::OptionalMatch);
                }
                Multiplicity::MayMultiplyRows
            }
            Operator::Unwind { .. } => Multiplicity::MayMultiplyRows,
            Operator::Filter(_) => Multiplicity::MayReduceRows,
            Operator::Project {
                items,
                distinct,
                ordering,
                predicate,
                skip,
                limit,
            } => {
                let aggregate = items.iter().any(|item| item.expression.has_aggregate());
                if aggregate {
                    boundaries.push(Boundary::Aggregate);
                }
                if *distinct {
                    boundaries.push(Boundary::Distinct);
                }
                if !ordering.is_empty() {
                    boundaries.push(Boundary::Ordering);
                }
                if skip.is_some() || limit.is_some() {
                    boundaries.push(Boundary::Window);
                }
                total_projection = !aggregate
                    && !distinct
                    && ordering.is_empty()
                    && predicate.is_none()
                    && items.iter().all(|item| {
                        matches!(
                            item.expression,
                            Expression::Slot(_) | Expression::Literal(_) | Expression::Parameter(_)
                        )
                    });
                if aggregate {
                    Multiplicity::GroupsRows
                } else if *distinct || predicate.is_some() || skip.is_some() || limit.is_some() {
                    Multiplicity::MayReduceRows
                } else {
                    Multiplicity::PreservesRows
                }
            }
            Operator::Create(pattern) => {
                references.extend(
                    pattern
                        .slots()
                        .into_iter()
                        .filter(|slot| input.get(*slot).is_some()),
                );
                effect = Effect::Write;
                boundaries.push(Boundary::Mutation);
                Multiplicity::PreservesRows
            }
            Operator::Update(updates) => {
                for update in updates {
                    let entity = match update {
                        super::PropertyMutation::Set { entity, .. }
                        | super::PropertyMutation::Remove { entity, .. }
                        | super::PropertyMutation::Replace { entity, .. }
                        | super::PropertyMutation::Extend { entity, .. } => *entity,
                    };
                    references.insert(entity);
                }
                effect = Effect::Write;
                boundaries.push(Boundary::Mutation);
                Multiplicity::PreservesRows
            }
            Operator::Delete { .. } => {
                effect = Effect::Write;
                boundaries.push(Boundary::Mutation);
                Multiplicity::PreservesRows
            }
        };
        let correlation = if let Operator::Match { .. } = operator {
            crate::ir::AtLeast::try_from_vec(
                references
                    .iter()
                    .filter(|slot| input.get(**slot).is_some())
                    .copied()
                    .collect(),
            )
            .map(Correlation::Bound)
            .unwrap_or(Correlation::Independent)
        } else {
            Correlation::Independent
        };
        let output_schema = input.derive(
            output,
            matches!(operator, Operator::Match { optional: true, .. }),
        );
        Self {
            input: input_schema.clone(),
            output: output_schema,
            references,
            correlation,
            effect,
            multiplicity,
            boundaries,
            total_projection,
        }
    }
}
