//! Incremental aggregation state shared by scalar evaluation and row execution.
//! DISTINCT retains unique keys only; ordinary numeric aggregates retain O(1)
//! values regardless of input cardinality.
use super::{evaluation, Aggregate, Binary, GroupingKey, QueryError, Result, Value};
use std::collections::HashSet;

#[derive(Debug)]
enum State {
    Count(i64),
    Sum(Value),
    Average {
        sum: Value,
        count: i64,
    },
    Minimum(Value),
    Maximum(Value),
    Collect {
        values: Vec<Value>,
        payload_bytes: usize,
    },
}
#[derive(Debug)]
enum Deduplication {
    All,
    Distinct {
        seen: HashSet<GroupingKey>,
        bytes: usize,
    },
}

/// Each call admits retained values before extending state. Callers also account
/// the returned byte count in their request-owned memory ledger.
#[derive(Debug)]
pub struct Accumulator {
    state: State,
    deduplication: Deduplication,
}
impl Accumulator {
    pub fn new(function: Aggregate, distinct: bool) -> Self {
        Self {
            state: match function {
                Aggregate::Count => State::Count(0),
                Aggregate::Sum => State::Sum(Value::Integer(0)),
                Aggregate::Avg => State::Average {
                    sum: Value::Integer(0),
                    count: 0,
                },
                Aggregate::Min => State::Minimum(Value::Null),
                Aggregate::Max => State::Maximum(Value::Null),
                Aggregate::Collect => State::Collect {
                    values: Vec::new(),
                    payload_bytes: 0,
                },
            },
            deduplication: if distinct {
                Deduplication::Distinct {
                    seen: HashSet::new(),
                    bytes: 0,
                }
            } else {
                Deduplication::All
            },
        }
    }
    pub fn allocated_bytes(&self) -> usize {
        let values = match &self.state {
            State::Count(_) => 0,
            State::Sum(value)
            | State::Minimum(value)
            | State::Maximum(value)
            | State::Average { sum: value, .. } => value.allocated_bytes(),
            State::Collect {
                values,
                payload_bytes,
            } => payload_bytes.saturating_add(
                values
                    .capacity()
                    .saturating_sub(values.len())
                    .saturating_mul(size_of::<Value>()),
            ),
        };
        size_of::<Self>()
            .saturating_add(values)
            .saturating_add(match &self.deduplication {
                Deduplication::All => 0,
                Deduplication::Distinct { bytes, .. } => *bytes,
            })
    }
    pub fn push(&mut self, value: Value, max_items: usize, max_bytes: usize) -> Result<()> {
        value.validate_shape()?;
        if value == Value::Null {
            return Ok(());
        }
        if let Deduplication::Distinct { seen, .. } = &self.deduplication
            && seen.contains(&GroupingKey::new(value.clone())?)
        {
            return Ok(());
        }
        let retains = matches!(
            self.state,
            State::Collect { .. } | State::Minimum(_) | State::Maximum(_)
        );
        let distinct = matches!(self.deduplication, Deduplication::Distinct { .. });
        let collection_growth = match &self.state {
            State::Collect { values, .. } if values.len() == values.capacity() => {
                values.capacity().max(4).saturating_mul(size_of::<Value>())
            }
            _ => 0,
        };
        let additional = value
            .allocated_bytes()
            .saturating_add(128)
            .saturating_mul(usize::from(retains) + usize::from(distinct))
            .saturating_add(collection_growth);
        if self.allocated_bytes().saturating_add(additional) > max_bytes {
            return Err(QueryError::runtime(
                "ResourceLimit",
                "MemoryLimit",
                "aggregation state exceeds the memory budget",
            ));
        }
        // Compute fallible numeric transitions before mutating either the
        // accumulator or its distinct-key set. A rejected input leaves it usable.
        let numeric = match &self.state {
            State::Sum(sum) | State::Average { sum, .. } => {
                if !matches!(value, Value::Integer(_) | Value::Float(_)) {
                    return Err(evaluation::type_error("numeric aggregate requires numbers"));
                }
                Some(evaluation::binary(Binary::Add, sum.clone(), value.clone())?)
            }
            _ => None,
        };
        let next_count = match &self.state {
            State::Count(count) | State::Average { count, .. } => {
                Some(count.checked_add(1).ok_or_else(evaluation::overflow)?)
            }
            _ => None,
        };
        if matches!(&self.state,State::Collect { values, .. } if values.len() >= max_items) {
            return Err(QueryError::runtime(
                "ResourceLimit",
                "CollectionLimit",
                "collect exceeds the collection budget",
            ));
        }
        if let Deduplication::Distinct { seen, bytes } = &mut self.deduplication {
            if seen.len() >= max_items {
                return Err(QueryError::runtime(
                    "ResourceLimit",
                    "CollectionLimit",
                    "distinct aggregation exceeds the collection budget",
                ));
            }
            *bytes = bytes
                .saturating_add(value.allocated_bytes())
                .saturating_add(128);
            seen.insert(GroupingKey::new(value.clone())?);
        }
        match &mut self.state {
            State::Count(count) => *count = next_count.expect("count transition was checked"),
            State::Sum(sum) => *sum = numeric.expect("sum transition was checked"),
            State::Average { sum, count } => {
                *sum = numeric.expect("average transition was checked");
                *count = next_count.expect("count transition was checked");
            }
            State::Minimum(current)
                if *current == Value::Null || value.total_cmp(current).is_lt() =>
            {
                *current = value
            }
            State::Maximum(current)
                if *current == Value::Null || value.total_cmp(current).is_gt() =>
            {
                *current = value
            }
            State::Minimum(_) | State::Maximum(_) => {}
            State::Collect {
                values,
                payload_bytes,
            } => {
                *payload_bytes = payload_bytes.saturating_add(value.allocated_bytes());
                values.push(value);
            }
        }
        Ok(())
    }
    pub fn finish(self) -> Result<Value> {
        Ok(match self.state {
            State::Count(count) => Value::Integer(count),
            State::Sum(value) | State::Minimum(value) | State::Maximum(value) => value,
            State::Average { count: 0, .. } => Value::Null,
            State::Average { sum, count } => {
                evaluation::binary(Binary::Divide, sum, Value::Float(count as f64))?
            }
            State::Collect { values, .. } => Value::List(values),
        })
    }
}
