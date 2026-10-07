//! Admission for the three parameter representations used by the two frontends.
//! Preflight borrows incoming values; conversion starts only after the complete
//! construction bound fits. Runtime execution keeps only the retained debit.
use super::{context, ir, r, Result};
use helix_ast::{query, value};
use std::collections::BTreeMap;

pub(super) struct Prepared {
    pub bindings: context::ParamBindings,
    pub values: BTreeMap<String, r::Value>,
    pub footprint: Footprint,
}
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct Footprint {
    retained: usize,
    scratch: usize,
}
impl Footprint {
    pub(super) fn retained(self) -> usize {
        self.retained
    }
    pub(super) fn construction(self) -> usize {
        self.retained.saturating_add(self.scratch)
    }
}
#[derive(Default)]
struct Payload {
    retained: usize,
    scratch: usize,
}

pub(super) fn prepare(
    input: BTreeMap<String, query::QueryValue>,
    memory_bytes: usize,
) -> Result<Prepared> {
    let footprint = preflight(&input)?;
    if footprint.construction() > memory_bytes {
        return Err(r::QueryError::runtime(
            "ResourceLimit",
            "MemoryLimit",
            "parameter representations exceed the query memory budget",
        )
        .into());
    }
    let mut bindings = context::ParamBindings::default();
    let mut values = BTreeMap::new();
    for (name, value) in input {
        let key = ir::NonEmptyString::new(name.clone()).expect("parameter names passed preflight");
        bindings
            .values
            .insert(key.clone(), value::PropertyValue::from(&value));
        values.insert(name, super::parameter_value(&value));
        bindings.query_values.insert(key, value);
    }
    Ok(Prepared {
        bindings,
        values,
        footprint,
    })
}

fn preflight(input: &BTreeMap<String, query::QueryValue>) -> r::Result<Footprint> {
    if input.is_empty() {
        // Conversion drops even a pruned incoming root and creates empty maps
        // without allocation. Do not debit fictitious fresh empty roots.
        return Ok(Footprint::default());
    }
    let mut retained =
        r::allocation::btree_bytes::<ir::NonEmptyString, value::PropertyValue>(input.len())
            .saturating_add(r::allocation::btree_bytes::<
                ir::NonEmptyString,
                query::QueryValue,
            >(input.len()))
            .saturating_add(r::allocation::btree_bytes::<String, r::Value>(input.len()));
    // IntoIter can retain source map nodes while destination maps grow. Nested
    // map conversions may also allocate temporary entry vectors for collect().
    let mut scratch = r::allocation::btree_bytes::<String, query::QueryValue>(input.len());
    for (name, value) in input {
        if name.is_empty() {
            return Err(r::QueryError::compile(
                "SyntaxError",
                "InvalidParameter",
                "parameter name cannot be empty",
            ));
        }
        let mut count = 0;
        let payload = payload(value, 0, &mut count)?;
        retained = retained
            .saturating_add(name.capacity())
            .saturating_add(name.len().saturating_mul(2))
            .saturating_add(payload.retained);
        scratch = scratch.saturating_add(payload.scratch);
    }
    Ok(Footprint { retained, scratch })
}

fn payload(value: &query::QueryValue, depth: usize, count: &mut usize) -> r::Result<Payload> {
    use query::QueryValue as Q;
    use value::PropertyValue as P;
    *count = count.saturating_add(1);
    if depth >= r::MAX_EXPRESSION_DEPTH || *count > 200_000 {
        return Err(r::QueryError::compile(
            "ResourceLimit",
            "ValueDepth",
            "parameter exceeds structural limits",
        ));
    }
    // Recursion is bounded before descent. Wide inputs never allocate a pending
    // work vector proportional to their node count, unlike breadth-first work.
    match value {
        Q::Null | Q::Bool(_) | Q::I64(_) | Q::F64(_) | Q::F32(_) => Ok(Payload::default()),
        Q::String(value) => Ok(Payload {
            retained: value
                .capacity()
                .saturating_add(value.len().saturating_mul(2)),
            scratch: 0,
        }),
        Q::Array(values) => values.iter().try_fold(
            Payload {
                retained: values
                    .capacity()
                    .saturating_mul(size_of::<Q>())
                    .saturating_add(
                        values
                            .len()
                            .saturating_mul(size_of::<P>() + size_of::<r::Value>()),
                    ),
                scratch: 0,
            },
            |mut total, value| {
                let child = payload(value, depth + 1, count)?;
                total.retained = total.retained.saturating_add(child.retained);
                total.scratch = total.scratch.saturating_add(child.scratch);
                Ok(total)
            },
        ),
        Q::Object(values) => {
            let fresh = if values.is_empty() {
                0
            } else {
                r::allocation::btree_bytes::<String, P>(values.len())
                    .saturating_add(r::allocation::btree_bytes::<String, r::Value>(values.len()))
            };
            values.iter().try_fold(
                Payload {
                    retained: r::allocation::btree_bytes::<String, Q>(values.len())
                        .saturating_add(fresh),
                    scratch: values
                        .len()
                        .saturating_mul(size_of::<(String, P)>() + size_of::<(String, r::Value)>()),
                },
                |mut total, (name, value)| {
                    let child = payload(value, depth + 1, count)?;
                    total.retained = total
                        .retained
                        .saturating_add(name.capacity())
                        .saturating_add(name.len().saturating_mul(2))
                        .saturating_add(child.retained);
                    total.scratch = total.scratch.saturating_add(child.scratch);
                    Ok(total)
                },
            )
        }
    }
}

#[cfg(test)]
#[path = "tests/admission.rs"]
mod tests;
