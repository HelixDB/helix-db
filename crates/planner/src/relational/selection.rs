//! Common selection program. Frontend adapters decide predicate semantics;
//! this kernel preserves order, multiplicity, and the first evaluation failure.
use super::{Expression, ExpressionInput, QueryError, Result, Slot};
use std::{collections::BTreeSet, future::Future, sync::Arc};

/// Three-valued predicate result. Native boolean predicates only produce True
/// or False; Cypher null predicates produce Unknown and do not retain the row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Selection {
    True,
    False,
    Unknown,
}
impl From<Option<bool>> for Selection {
    fn from(value: Option<bool>) -> Self {
        match value {
            Some(true) => Self::True,
            Some(false) => Self::False,
            None => Self::Unknown,
        }
    }
}

/// Validated scalar predicate and its input dependencies. Deserialization
/// rebuilds dependencies; the wire representation remains the expression.
///
/// ```
/// use helix_planner::relational as r;
/// use std::collections::BTreeSet;
/// let predicate = r::SelectionProgram::new(r::Expression::Slot(r::Slot(0))).unwrap();
/// assert!(predicate.validate_input(&BTreeSet::new()).is_err());
/// predicate.validate_input(&BTreeSet::from([r::Slot(0)])).unwrap();
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct SelectionProgram<E = Expression> {
    expression: Arc<E>,
    references: BTreeSet<Slot>,
}
impl<E: ExpressionInput> SelectionProgram<E> {
    pub fn new(expression: E) -> Result<Self> {
        expression.validate()?;
        let references = expression.references();
        Ok(Self {
            expression: Arc::new(expression),
            references,
        })
    }
    pub fn validate_input(&self, input: &BTreeSet<Slot>) -> Result<()> {
        if !self.references.is_subset(input) {
            return Err(QueryError::compile(
                "InternalPlannerError",
                "UnboundSlot",
                "selection references an undefined incoming row slot",
            ));
        }
        Ok(())
    }
}
impl<E> SelectionProgram<E> {
    pub fn expression(&self) -> &E {
        &self.expression
    }
    pub fn references(&self) -> &BTreeSet<Slot> {
        &self.references
    }

    /// Evaluate once per row in input order. Retention moves rows without
    /// cloning. The adapter owns destination storage and its admission policy.
    pub async fn select<I, V>(
        &self,
        input: I,
        evaluator: &mut V,
    ) -> std::result::Result<(), V::Error>
    where
        I: IntoIterator<Item = V::Row>,
        V: SelectionEvaluator<E>,
    {
        for row in input {
            if evaluator.evaluate(&row, &self.expression).await? == Selection::True {
                evaluator.retain(row)?;
            }
        }
        Ok(())
    }
}

/// Storage/evaluation adapter. Hydrate an input batch before selection when the
/// storage model permits it. Check cancellation and resource limits here.
pub trait SelectionEvaluator<E> {
    type Row;
    type Error;
    fn evaluate(
        &mut self,
        row: &Self::Row,
        expression: &E,
    ) -> impl Future<Output = std::result::Result<Selection, Self::Error>> + Send;
    fn retain(&mut self, row: Self::Row) -> std::result::Result<(), Self::Error>;
}
impl<E> std::ops::Deref for SelectionProgram<E> {
    type Target = E;
    fn deref(&self) -> &E {
        &self.expression
    }
}
impl<E: serde::Serialize> serde::Serialize for SelectionProgram<E> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        self.expression.serialize(serializer)
    }
}
impl<'de, E: serde::Deserialize<'de> + ExpressionInput> serde::Deserialize<'de>
    for SelectionProgram<E>
{
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        Self::new(E::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}
