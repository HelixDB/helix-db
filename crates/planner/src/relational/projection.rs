//! Shared simultaneous row projection. Frontends supply resolved expressions;
//! the program owns column order, unique destinations, and input dependencies.
//! All expressions observe the incoming row, including when a destination
//! shadows an input slot. A failed expression produces no partially updated row.
use super::{Expression, ExpressionInput, QueryError, Result, Slot};
use std::{collections::BTreeSet, future::Future};

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Projection<E = Expression> {
    pub slot: Slot,
    pub expression: E,
}

/// A relative row program. Construction and deserialization reject duplicate
/// destinations; attaching it to a query requires `validate_input` against the
/// incoming schema. The serialized form remains an ordered projection list.
///
/// ```
/// use helix_planner::relational as r;
/// use std::collections::BTreeSet;
/// let program = r::ProjectionProgram::new(vec![r::Projection {
///     slot: r::Slot(1), expression: r::Expression::Slot(r::Slot(0)),
/// }]).unwrap();
/// assert!(program.validate_input(&BTreeSet::new()).is_err());
/// program.validate_input(&BTreeSet::from([r::Slot(0)])).unwrap();
/// assert_eq!(program.outputs(), &BTreeSet::from([r::Slot(1)]));
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct ProjectionProgram<E = Expression> {
    items: Vec<Projection<E>>,
    references: BTreeSet<Slot>,
    outputs: BTreeSet<Slot>,
}

impl<E: ExpressionInput> ProjectionProgram<E> {
    pub fn new(items: Vec<Projection<E>>) -> Result<Self> {
        let mut outputs = BTreeSet::new();
        let mut references = BTreeSet::new();
        for item in &items {
            if !outputs.insert(item.slot) {
                return Err(QueryError::compile(
                    "InternalPlannerError",
                    "InvalidSchema",
                    "projection output slots must be unique",
                ));
            }
            item.expression.validate()?;
            references.extend(item.expression.references());
        }
        Ok(Self {
            items,
            references,
            outputs,
        })
    }

    pub fn validate_input(&self, input: &BTreeSet<Slot>) -> Result<()> {
        if !self.references.is_subset(input) {
            return Err(QueryError::compile(
                "InternalPlannerError",
                "UnboundSlot",
                "projection references an undefined incoming row slot",
            ));
        }
        Ok(())
    }
}

impl<E> ProjectionProgram<E> {
    pub fn references(&self) -> &BTreeSet<Slot> {
        &self.references
    }
    pub fn outputs(&self) -> &BTreeSet<Slot> {
        &self.outputs
    }

    /// Evaluate in declaration order against an evaluator's immutable input.
    /// The evaluator admits its output vector before allocation, retains the
    /// budget for earlier values, and checks cancellation between expressions.
    /// Callers install destinations only after the entire evaluation succeeds.
    pub async fn evaluate<V: ProjectionEvaluator<E>>(
        &self,
        evaluator: &mut V,
    ) -> std::result::Result<Vec<V::Value>, V::Error> {
        evaluator.prepare(self.items.len())?;
        let mut values = Vec::with_capacity(self.items.len());
        for item in &self.items {
            values.push(evaluator.evaluate(&item.expression).await?);
        }
        Ok(values)
    }
}

/// Frontend semantic adapter. Native optional properties may return `None`;
/// Cypher expressions return a value, including an explicit null. The program
/// never conflates those outcomes or changes the evaluator's error order.
pub trait ProjectionEvaluator<E> {
    type Value;
    type Error;
    fn prepare(&mut self, columns: usize) -> std::result::Result<(), Self::Error>;
    fn evaluate(
        &mut self,
        expression: &E,
    ) -> impl Future<Output = std::result::Result<Self::Value, Self::Error>> + Send;
}

impl<E> std::ops::Deref for ProjectionProgram<E> {
    type Target = [Projection<E>];
    fn deref(&self) -> &Self::Target {
        &self.items
    }
}
impl<'a, E> IntoIterator for &'a ProjectionProgram<E> {
    type Item = &'a Projection<E>;
    type IntoIter = std::slice::Iter<'a, Projection<E>>;
    fn into_iter(self) -> Self::IntoIter {
        self.items.iter()
    }
}
impl<E: serde::Serialize> serde::Serialize for ProjectionProgram<E> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        self.items.serialize(serializer)
    }
}
impl<'de, E: serde::Deserialize<'de> + ExpressionInput> serde::Deserialize<'de>
    for ProjectionProgram<E>
{
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        Self::new(Vec::<Projection<E>>::deserialize(deserializer)?)
            .map_err(serde::de::Error::custom)
    }
}
