//! Incremental aggregation state shared by scalar evaluation and row execution.
//! DISTINCT retains unique keys only; ordinary numeric aggregates retain O(1)
//! values regardless of input cardinality.
use super::{evaluation, Aggregate, Binary, GroupingKey, QueryError, Result, Value};
use std::collections::HashSet;

mod average;

#[derive(Debug)]
enum State {
    Count(i64),
    Sum(Value),
    Average(average::Average),
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
                Aggregate::Avg => State::Average(average::Average::new()),
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
            State::Count(_) | State::Average(_) => 0,
            State::Sum(value) | State::Minimum(value) | State::Maximum(value) => {
                value.allocated_bytes()
            }
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
                Deduplication::Distinct { seen, bytes } => {
                    bytes.saturating_add(super::allocation::hash_table_retained_bytes::<
                        GroupingKey,
                        (),
                    >(seen.len()))
                }
            })
    }
    pub fn push(&mut self, value: Value, max_items: usize, max_bytes: usize) -> Result<()> {
        self.push_with_admission(value, max_items, max_bytes, |_| Ok(()))
    }

    /// Admit the next state's conservative ownership bound before allocating or
    /// mutating it. Null and duplicate inputs need no callback. A validation or
    /// admission error leaves this accumulator unchanged; the input is consumed.
    /// The producer separately owns admission for the input value.
    ///
    /// ```
    /// use helix_planner::relational::{Accumulator, Aggregate, QueryError, Value};
    /// let mut count = Accumulator::new(Aggregate::Count, false);
    /// let mut admitted = 0;
    /// count.push_with_admission::<QueryError>(Value::Integer(1), 10, 1024, |bytes| {
    ///     admitted = bytes;
    ///     Ok(())
    /// }).unwrap();
    /// assert!(admitted >= count.allocated_bytes());
    /// assert_eq!(count.finish().unwrap(), Value::Integer(1));
    /// ```
    pub fn push_with_admission<E: From<QueryError>>(
        &mut self,
        value: Value,
        max_items: usize,
        max_bytes: usize,
        admit: impl FnOnce(usize) -> std::result::Result<(), E>,
    ) -> std::result::Result<(), E> {
        // Collect adds one logical level. Reject before updating either state
        // or deduplication, so finish cannot produce an excessively deep value.
        value.validate_runtime_shape(
            usize::from(matches!(self.state, State::Collect { .. })),
            200_000,
        )?;
        if value == Value::Null {
            return Ok(());
        }
        let value = match &self.deduplication {
            Deduplication::Distinct { seen, .. } => {
                // The input is already owned and admitted by its producer.
                // Probe with that owner; only retained unique state may copy
                // the payload, after the admission check below.
                let key = GroupingKey::new(value)?;
                if seen.contains(&key) {
                    return Ok(());
                }
                key.into_value()
            }
            Deduplication::All => value,
        };
        let retains = matches!(
            self.state,
            State::Collect { .. } | State::Minimum(_) | State::Maximum(_)
        );

        let collection_growth = match &self.state {
            State::Collect { values, .. } if values.len() == values.capacity() => {
                values.capacity().max(4).saturating_mul(size_of::<Value>())
            }
            _ => 0,
        };
        let distinct_growth = match &self.deduplication {
            Deduplication::All => 0,
            Deduplication::Distinct { seen, .. } => {
                let next = super::allocation::hash_table_retained_bytes::<GroupingKey, ()>(
                    seen.len().saturating_add(1),
                );
                // During rehashing the old table remains live; the existing
                // state already owns its bound, and admission adds the new one.
                let table = if seen.len() == seen.capacity() {
                    next
                } else {
                    next.saturating_sub(super::allocation::hash_table_retained_bytes::<
                        GroupingKey,
                        (),
                    >(seen.len()))
                };
                value.allocated_bytes().saturating_add(table)
            }
        };
        let additional = value
            .allocated_bytes()
            .saturating_mul(usize::from(retains))
            .saturating_add(collection_growth)
            .saturating_add(distinct_growth);
        if self.allocated_bytes().saturating_add(additional) > max_bytes {
            return Err(QueryError::runtime(
                "ResourceLimit",
                "MemoryLimit",
                "aggregation state exceeds the memory budget",
            )
            .into());
        }
        // Compute fallible numeric transitions before mutating either the
        // accumulator or its distinct-key set. A rejected input leaves it usable.
        let numeric = match &self.state {
            State::Sum(sum) => {
                if !matches!(value, Value::Integer(_) | Value::Float(_)) {
                    return Err(evaluation::type_error("numeric aggregate requires numbers").into());
                }
                Some(evaluation::binary(Binary::Add, sum.clone(), value.clone())?)
            }
            _ => None,
        };
        let next_average = match &self.state {
            State::Average(average) => Some(average.next(&value)?),
            _ => None,
        };
        let next_count = match &self.state {
            State::Count(count) => Some(count.checked_add(1).ok_or_else(evaluation::overflow)?),
            _ => None,
        };
        if matches!(&self.state,State::Collect { values, .. } if values.len() >= max_items) {
            return Err(QueryError::runtime(
                "ResourceLimit",
                "CollectionLimit",
                "collect exceeds the collection budget",
            )
            .into());
        }
        if matches!(&self.deduplication, Deduplication::Distinct { seen, .. } if seen.len() >= max_items)
        {
            return Err(QueryError::runtime(
                "ResourceLimit",
                "CollectionLimit",
                "distinct aggregation exceeds the collection budget",
            )
            .into());
        }
        admit(self.allocated_bytes().saturating_add(additional))?;
        if let Deduplication::Distinct { seen, bytes } = &mut self.deduplication {
            seen.try_reserve(1).map_err(|_| {
                QueryError::runtime(
                    "ResourceLimit",
                    "MemoryLimit",
                    "distinct aggregation allocation failed",
                )
            })?;
            seen.insert(GroupingKey::new(value.clone())?);
            *bytes = bytes.saturating_add(value.allocated_bytes());
        }
        match &mut self.state {
            State::Count(count) => *count = next_count.expect("count transition was checked"),
            State::Sum(sum) => *sum = numeric.expect("sum transition was checked"),
            State::Average(average) => {
                *average = next_average.expect("average transition was checked");
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
            State::Average(average) => average.finish(),
            State::Collect { values, .. } => Value::List(values),
        })
    }
}
