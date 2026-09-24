//! Scalar semantics over an already materialized batch. No storage I/O occurs here.
use super::{
    Aggregate, Binary, Entity, Expression, Function, GraphValues, QueryError, Result, Unary, Value,
};
use std::{cmp::Ordering, collections::BTreeMap};

mod formatting;
mod integer;

pub type Row = Vec<Value>;

/// Immutable evaluation context; aggregate arguments use the same scalar rules.
#[derive(Clone, Copy)]
pub struct Evaluation<'a> {
    pub row: &'a [Value],
    pub parameters: &'a BTreeMap<String, Value>,
    pub graph: &'a dyn GraphValues,
    pub group: Option<&'a [Row]>,
    pub max_collection_items: usize,
    pub max_value_bytes: usize,
}

impl Evaluation<'_> {
    /// Evaluate a sized expression sequence in declaration order under one
    /// memory allowance. Earlier payloads and the vector's slots remain live
    /// while later expressions run. Empty sequences allocate nothing.
    pub fn eval_sequence<'e>(
        &self,
        expressions: impl ExactSizeIterator<Item = &'e Expression>,
    ) -> Result<Vec<Value>> {
        let count = expressions.len();
        if count == 0 {
            return Ok(Vec::new());
        }
        let mut remaining = self.remaining(
            size_of::<Vec<Value>>().saturating_add(count.saturating_mul(size_of::<Value>())),
        )?;
        let mut values = Vec::with_capacity(count);
        for expression in expressions {
            let value = remaining.eval(expression)?;
            remaining =
                remaining.remaining(value.allocated_bytes().saturating_sub(size_of::<Value>()))?;
            assert!(values.len() < count, "exact expression count");
            values.push(value);
        }
        Ok(values)
    }

    /// Stream the range generator in UNWIND without allocating a list. Other
    /// expressions retain scalar evaluation and null/list coercion semantics.
    pub fn unwind(&self, expression: &Expression) -> Result<UnwindValues> {
        if let Expression::Function(Function::Range, arguments) = expression {
            let mut remaining = self.remaining(
                size_of::<Vec<Value>>()
                    .saturating_add(arguments.len().saturating_mul(size_of::<Value>())),
            )?;
            let mut values = Vec::with_capacity(arguments.len());
            for expression in arguments {
                let value = remaining.eval(expression)?;
                remaining = remaining
                    .remaining(value.allocated_bytes().saturating_sub(size_of::<Value>()))?;
                values.push(value);
            }
            if values.first() == Some(&Value::Null) {
                return Ok(UnwindValues::values(Vec::new()));
            }
            return IntegerRange::new(&values).map(|range| UnwindValues {
                source: UnwindSource::Range(range),
                allocated_bytes: size_of::<UnwindValues>(),
            });
        }
        let values = match self.eval(expression)? {
            Value::Null => Vec::new(),
            Value::List(values) => values,
            value => vec![value],
        };
        Ok(UnwindValues::values(values))
    }

    pub fn eval(&self, expression: &Expression) -> Result<Value> {
        use Expression as E;
        let value = match expression {
            E::Literal(v) => {
                v.validate_depth()?;
                self.remaining(v.allocated_bytes())?;
                v.clone()
            }
            E::Slot(slot) => {
                let value = self.row.get(slot.0 as usize).ok_or_else(|| {
                    QueryError::runtime(
                        "InternalPlannerError",
                        "InvalidSlot",
                        "row schema does not contain a referenced slot",
                    )
                })?;
                value.validate_depth()?;
                self.remaining(value.allocated_bytes())?;
                value.clone()
            }
            E::Parameter(name) => {
                let value = self.parameters.get(name).ok_or_else(|| {
                    QueryError::runtime(
                        "ParameterMissing",
                        "MissingParameter",
                        format!("missing parameter ${name}"),
                    )
                })?;
                value.validate_depth()?;
                self.remaining(value.allocated_bytes())?;
                value.clone()
            }
            E::Property(value, key) => match self.eval(value)? {
                Value::Null => Value::Null,
                Value::Map(mut map) => map.remove(key).unwrap_or(Value::Null),
                Value::Entity(entity) => {
                    let value = self.graph.property(entity, key)?;
                    value.validate_depth()?;
                    self.remaining(value.allocated_bytes())?;
                    value.clone()
                }
                _ => {
                    return Err(type_error(
                        "property access requires a map, node, or relationship",
                    ))
                }
            },
            E::Index(value, index) => {
                let value = self.eval(value)?;
                let index = self.remaining(value.allocated_bytes())?.eval(index)?;
                match (value, index) {
                    (Value::Null, _) | (_, Value::Null) => Value::Null,
                    // This list is an owned temporary. Moving its selected element
                    // avoids cloning nested values; the remaining list is discarded.
                    (Value::List(mut values), Value::Integer(index)) => {
                        position(index, values.len())
                            .map(|i| values.swap_remove(i))
                            .unwrap_or(Value::Null)
                    }
                    (Value::List(_), _) => {
                        return Err(QueryError::runtime(
                            "TypeError",
                            "ListElementAccessByNonInteger",
                            "list index must be an integer",
                        ))
                    }
                    (Value::Map(mut values), Value::String(key)) => {
                        values.remove(&key).unwrap_or(Value::Null)
                    }
                    (Value::Entity(entity), Value::String(key)) => {
                        let value = self.graph.property(entity, &key)?;
                        value.validate_depth()?;
                        self.remaining(key.capacity().saturating_add(value.allocated_bytes()))?;
                        value.clone()
                    }
                    (Value::Map(_) | Value::Entity(_), _) => {
                        return Err(QueryError::runtime(
                            "TypeError",
                            "MapElementAccessByNonString",
                            "map index must be a string",
                        ))
                    }
                    _ => return Err(type_error("value is not indexable")),
                }
            }
            E::Slice { value, start, end } => {
                let value = self.eval(value)?;
                if value == Value::Null {
                    return Ok(Value::Null);
                }
                let remaining = self.remaining(value.allocated_bytes())?;
                let Value::List(mut values) = value else {
                    return Err(type_error("slicing requires a list"));
                };
                let bound = |x: &Option<Box<E>>, default: usize| -> Result<Option<usize>> {
                    let Some(x) = x else {
                        return Ok(Some(default));
                    };
                    match remaining.eval(x)? {
                        Value::Null => Ok(None),
                        Value::Integer(i) => Ok(Some(slice_position(i, values.len()))),
                        _ => Err(type_error("slice bounds must be integers")),
                    }
                };
                let (Some(start), Some(end)) = (bound(start, 0)?, bound(end, values.len())?) else {
                    return Ok(Value::Null);
                };
                Value::List(if end <= start {
                    Vec::new()
                } else {
                    // Reuse the owned buffer without duplicating nested values.
                    values.truncate(end);
                    values.drain(..start);
                    values
                })
            }
            E::Unary(op, x) => {
                let x = self.eval(x)?;
                match op {
                    Unary::IsNull => Value::Boolean(x == Value::Null),
                    Unary::IsNotNull => Value::Boolean(x != Value::Null),
                    Unary::Not => truth(x.truth()?.map(|x| !x)),
                    Unary::Positive => match x {
                        Value::Null | Value::Integer(_) | Value::Float(_) => x,
                        _ => return Err(type_error("unary + requires a number")),
                    },
                    Unary::Negate => match x {
                        Value::Null => Value::Null,
                        Value::Integer(i) => Value::Integer(i.checked_neg().ok_or_else(overflow)?),
                        Value::Float(f) => Value::Float(-f),
                        _ => return Err(type_error("unary - requires a number")),
                    },
                }
            }
            E::Binary(op, left, right) => {
                let left = self.eval(left)?;
                if *op == Binary::And && left.truth()? == Some(false) {
                    return Ok(Value::Boolean(false));
                }
                if *op == Binary::Or && left.truth()? == Some(true) {
                    return Ok(Value::Boolean(true));
                }
                let right = self.remaining(left.allocated_bytes())?.eval(right)?;
                let input_bytes = left
                    .allocated_bytes()
                    .saturating_add(right.allocated_bytes());
                let remaining = self.remaining(input_bytes)?;
                if *op == Binary::Add {
                    remaining.add(left, right)?
                } else {
                    binary(*op, left, right)?
                }
            }
            E::List(xs) => Value::List(self.arguments(xs)?),
            E::Map(xs) => {
                let mut bytes = size_of::<Value>()
                    .saturating_add(super::allocation::btree_bytes::<String, Value>(xs.len()));
                self.remaining(bytes)?;
                let mut values = BTreeMap::new();
                for (key, expression) in xs {
                    let value = self.remaining(bytes)?.eval(expression)?;
                    bytes = bytes
                        .saturating_add(key.capacity())
                        .saturating_add(value.allocated_bytes().saturating_sub(size_of::<Value>()));
                    self.remaining(bytes)?;
                    values.insert(key.clone(), value);
                }
                Value::Map(values)
            }
            E::HasLabel(slot, label) => match self.row.get(slot.0 as usize) {
                Some(Value::Null) => Value::Null,
                Some(Value::Entity(entity @ Entity::Node(_))) => {
                    Value::Boolean(self.graph.label(*entity)? == Some(label.as_str()))
                }
                _ => return Err(type_error("label test requires a node")),
            },
            E::Case {
                branches,
                otherwise,
            } => {
                for (condition, value) in branches {
                    if self.eval(condition)?.truth()? == Some(true) {
                        return self.eval(value);
                    }
                }
                self.eval(otherwise)?
            }
            E::SimpleCase(case) => {
                let operand = self.eval(&case.operand)?;
                // Keep the operand admitted while evaluating each comparison.
                // Both temporaries are released before the selected result.
                let remaining = self.remaining(operand.allocated_bytes())?;
                for (comparison, value) in &case.branches {
                    let matches = operand.equals(&remaining.eval(comparison)?) == Some(true);
                    if matches {
                        drop(operand);
                        return self.eval(value);
                    }
                }
                drop(operand);
                self.eval(&case.otherwise)?
            }
            E::Function(Function::Coalesce, args) => {
                for argument in args {
                    let v = self.eval(argument)?;
                    if v != Value::Null {
                        return Ok(v);
                    }
                }
                Value::Null
            }
            E::Function(function, args) => {
                let args = self.arguments(args)?;
                let bytes = args.iter().fold(0_usize, |bytes, value| {
                    bytes.saturating_add(value.allocated_bytes())
                });
                self.remaining(bytes)?.function(*function, args)?
            }
            E::Aggregate {
                function,
                argument,
                distinct,
            } => self.aggregate(*function, argument.as_deref(), *distinct)?,
        };
        // Inputs can be individually valid while a new list/map adds a level.
        // Enforce the runtime contract across clause boundaries as well as ASTs.
        value.validate_depth()?;
        self.remaining(value.allocated_bytes())?;
        Ok(value)
    }

    /// Both operands are already owned and admitted. Only a growing buffer or
    /// a new output needs additional space; payloads move without cloning.
    fn add(&self, left: Value, right: Value) -> Result<Value> {
        Ok(match (left, right) {
            (Value::Null, _) | (_, Value::Null) => Value::Null,
            (Value::List(left), Value::List(right)) if left.is_empty() => Value::List(right),
            (Value::List(mut left), Value::List(right)) => {
                self.grow_list(&mut left, right.len())?;
                left.extend(right);
                Value::List(left)
            }
            (Value::List(mut values), value) => {
                self.grow_list(&mut values, 1)?;
                values.push(value);
                Value::List(values)
            }
            (value, Value::List(mut values)) => {
                self.grow_list(&mut values, 1)?;
                values.insert(0, value);
                Value::List(values)
            }
            (Value::String(left), Value::String(right)) if left.is_empty() => Value::String(right),
            (Value::String(mut left), Value::String(right)) => {
                self.grow_string(&mut left, right.len())?;
                left.push_str(&right);
                Value::String(left)
            }
            (Value::String(mut text), number @ (Value::Integer(_) | Value::Float(_))) => {
                let formatted = formatting::ScalarText::new(&number)?;
                self.grow_string(&mut text, formatted.bytes())?;
                formatted.write_to(&mut text);
                Value::String(text)
            }
            (number @ (Value::Integer(_) | Value::Float(_)), Value::String(mut text)) => {
                let formatted = formatting::ScalarText::new(&number)?;
                self.grow_string(&mut text, formatted.bytes())?;
                formatted.write_to(&mut text);
                // Move the complete formatted suffix to the front without
                // allocating a second string. Both rotation boundaries are
                // UTF-8 boundaries; checked reconstruction keeps this safe.
                let mut bytes = text.into_bytes();
                bytes.rotate_right(formatted.bytes());
                Value::String(
                    String::from_utf8(bytes).expect("rotation preserves complete strings"),
                )
            }
            (left, right) => return binary(Binary::Add, left, right),
        })
    }

    fn grow_list(&self, values: &mut Vec<Value>, additional: usize) -> Result<()> {
        let count = values.len().saturating_add(additional);
        if count > values.capacity() {
            self.remaining(
                size_of::<Value>().saturating_add(count.saturating_mul(size_of::<Value>())),
            )?;
            values.reserve_exact(additional);
        }
        Ok(())
    }

    fn grow_string(&self, text: &mut String, additional: usize) -> Result<()> {
        let bytes = text.len().saturating_add(additional);
        if bytes > text.capacity() {
            self.remaining(size_of::<Value>().saturating_add(bytes))?;
            text.reserve_exact(additional);
        }
        Ok(())
    }

    fn remaining(&self, bytes: usize) -> Result<Self> {
        // Saturated estimates and unaddressable buffers must fail even when
        // an embedded caller provides an effectively unlimited allowance.
        let max_value_bytes = self
            .max_value_bytes
            .checked_sub(bytes)
            .filter(|_| bytes <= isize::MAX as usize)
            .ok_or_else(|| {
                QueryError::runtime(
                    "ResourceLimit",
                    "MemoryLimit",
                    "expression temporaries exceed the query memory budget",
                )
            })?;
        Ok(Self {
            max_value_bytes,
            ..*self
        })
    }

    fn arguments(&self, expressions: &[Expression]) -> Result<Vec<Value>> {
        if expressions.len() > self.max_collection_items {
            return Err(QueryError::runtime(
                "ResourceLimit",
                "CollectionLimit",
                "expression exceeds collection budget",
            ));
        }
        let mut bytes = expressions.len().saturating_mul(size_of::<Value>());
        self.remaining(bytes)?;
        let mut values = Vec::with_capacity(expressions.len());
        for expression in expressions {
            let value = self.remaining(bytes)?.eval(expression)?;
            bytes =
                bytes.saturating_add(value.allocated_bytes().saturating_sub(size_of::<Value>()));
            self.remaining(bytes)?;
            values.push(value);
        }
        Ok(values)
    }

    /// Materialize a prepared entity's property map after admitting its complete
    /// owned representation. Shared by properties() and graph-to-map updates.
    pub fn properties(&self, entity: Entity) -> Result<BTreeMap<String, Value>> {
        let properties = self.graph.properties(entity)?;
        let mut bytes = size_of::<Value>()
            .saturating_add(super::allocation::btree_bytes::<String, Value>(
                properties.len(),
            ));
        self.remaining(bytes)?;
        for (key, value) in properties {
            let value = value.as_ref().map_err(Clone::clone)?;
            value.validate_runtime_shape(1, usize::MAX)?;
            bytes = bytes
                .saturating_add(key.len())
                .saturating_add(value.allocated_bytes().saturating_sub(size_of::<Value>()));
            self.remaining(bytes)?;
        }
        properties
            .iter()
            .map(|(key, value)| value.clone().map(|value| (key.clone(), value)))
            .collect()
    }

    fn function(&self, function: Function, mut args: Vec<Value>) -> Result<Value> {
        use Function as F;
        let first = args
            .first_mut()
            .ok_or_else(|| type_error("missing function argument"))?;
        // Function arguments are already owned and admitted. Move the first
        // argument instead of cloning an entire map/list for size(), head(), etc.
        let first = std::mem::replace(first, Value::Null);
        if function == F::Exists {
            return Ok(Value::Boolean(first != Value::Null));
        }
        if first == Value::Null {
            return Ok(Value::Null);
        }
        Ok(match function {
            F::Id => match first {
                Value::Entity(Entity::Node(id) | Entity::Relationship(id)) => {
                    Value::Integer(i64::try_from(id).map_err(|_| overflow())?)
                }
                _ => return Err(type_error("id requires a graph entity")),
            },
            F::Type => match first {
                Value::Entity(entity @ Entity::Relationship(_)) => {
                    let label = self.graph.label(entity)?.unwrap_or_default();
                    self.remaining(size_of::<Value>().saturating_add(label.len()))?;
                    Value::String(label.into())
                }
                _ => return Err(type_error("type requires a relationship")),
            },
            F::Labels => match first {
                Value::Entity(entity @ Entity::Node(_)) => {
                    let label = self.graph.label(entity)?;
                    self.remaining(size_of::<Value>().saturating_add(
                        label.map_or(0, |s| size_of::<Value>().saturating_add(s.len())),
                    ))?;
                    Value::List(label.into_iter().map(|s| Value::String(s.into())).collect())
                }
                _ => return Err(type_error("labels requires a node")),
            },
            F::Properties => match first {
                Value::Map(_) => first,
                Value::Entity(entity) => Value::Map(self.properties(entity)?),
                _ => return Err(type_error("properties requires a map or graph entity")),
            },
            F::Keys => match first {
                Value::Map(map) => {
                    self.remaining(
                        size_of::<Value>()
                            .saturating_add(map.len().saturating_mul(size_of::<Value>())),
                    )?;
                    // Move owned keys directly into the admitted output slots.
                    let mut keys = Vec::with_capacity(map.len());
                    keys.extend(map.into_keys().map(Value::String));
                    Value::List(keys)
                }
                Value::Entity(entity) => {
                    let keys = self.graph.keys(entity)?;
                    let bytes = keys.clone().fold(size_of::<Value>(), |bytes, key| {
                        bytes
                            .saturating_add(size_of::<Value>())
                            .saturating_add(key.len())
                    });
                    self.remaining(bytes)?;
                    let mut values = Vec::with_capacity(keys.len());
                    values.extend(keys.cloned().map(Value::String));
                    Value::List(values)
                }
                _ => return Err(type_error("keys requires a map or graph entity")),
            },
            F::Size | F::Length => Value::Integer(match first {
                Value::String(s) => s.chars().count(),
                Value::List(xs) => xs.len(),
                Value::Path(p) => p.relationships().len(),
                _ => return Err(type_error("expected a string, list, or path")),
            } as i64),
            F::Nodes => match first {
                Value::Path(p) => {
                    self.remaining(
                        p.nodes()
                            .len()
                            .saturating_add(1)
                            .saturating_mul(size_of::<Value>()),
                    )?;
                    Value::List(
                        p.nodes()
                            .iter()
                            .map(|id| Value::Entity(Entity::Node(*id)))
                            .collect(),
                    )
                }
                _ => return Err(type_error("nodes requires a path")),
            },
            F::Relationships => match first {
                Value::Path(p) => {
                    self.remaining(
                        p.relationships()
                            .len()
                            .saturating_add(1)
                            .saturating_mul(size_of::<Value>()),
                    )?;
                    Value::List(
                        p.relationships()
                            .iter()
                            .map(|id| Value::Entity(Entity::Relationship(*id)))
                            .collect(),
                    )
                }
                _ => return Err(type_error("relationships requires a path")),
            },
            F::Head | F::Last => match first {
                Value::List(mut xs) => if function == F::Head {
                    xs.into_iter().next()
                } else {
                    xs.pop()
                }
                .unwrap_or(Value::Null),
                _ => return Err(type_error("head and last require a list")),
            },
            F::ToString => match first {
                Value::String(_) => first,
                _ => {
                    let formatted = formatting::ScalarText::new(&first)?;
                    self.remaining(size_of::<Value>().saturating_add(formatted.bytes()))?;
                    let mut output = String::with_capacity(formatted.bytes());
                    formatted.write_to(&mut output);
                    Value::String(output)
                }
            },
            F::ToInteger => match first {
                Value::Integer(_) => first,
                Value::Float(f) => integer::from_float(f)
                    .map(Value::Integer)
                    .unwrap_or(Value::Null),
                Value::String(s) => s
                    .trim()
                    .parse::<i64>()
                    .ok()
                    .or_else(|| integer::from_decimal(&s))
                    .map(Value::Integer)
                    .unwrap_or(Value::Null),
                _ => return Err(type_error("toInteger requires a string or number")),
            },
            F::ToFloat => match first {
                Value::Float(_) => first,
                Value::Integer(i) => Value::Float(i as f64),
                Value::String(s) => s
                    .trim()
                    .parse::<f64>()
                    .ok()
                    .map(Value::Float)
                    .unwrap_or(Value::Null),
                _ => return Err(type_error("toFloat requires a string or number")),
            },
            F::ToBoolean => match first {
                Value::Boolean(_) => first,
                Value::String(s) => match s.trim() {
                    text if text.eq_ignore_ascii_case("true") => Value::Boolean(true),
                    text if text.eq_ignore_ascii_case("false") => Value::Boolean(false),
                    _ => Value::Null,
                },
                _ => return Err(type_error("toBoolean requires a string or boolean")),
            },
            F::Abs => match first {
                Value::Integer(i) => Value::Integer(i.checked_abs().ok_or_else(overflow)?),
                Value::Float(f) => Value::Float(f.abs()),
                _ => return Err(type_error("abs requires a number")),
            },
            F::Range => {
                args[0] = first;
                let range = IntegerRange::new(&args)?;
                let start = range.next.expect("a new range has its initial candidate");
                let step = i128::from(range.step.get());
                let distance = (i128::from(range.end) - i128::from(start)) * step.signum();
                let count = if distance < 0 {
                    0
                } else {
                    distance / step.abs() + 1
                };
                // Reject oversized collections before reserving their exact
                // output. UNWIND retains the allocation-free generator path.
                if count as u128 > self.max_collection_items as u128 {
                    return Err(QueryError::runtime(
                        "ResourceLimit",
                        "CollectionLimit",
                        "range exceeds the collection budget",
                    ));
                }
                let count = usize::try_from(count).expect("count fits the collection limit");
                self.remaining(count.saturating_add(1).saturating_mul(size_of::<Value>()))?;
                let mut values = Vec::with_capacity(count);
                for value in range {
                    assert!(values.len() < count, "validated range cardinality");
                    values.push(value);
                }
                assert_eq!(values.len(), count, "validated range cardinality");
                Value::List(values)
            }
            F::Reverse => match first {
                Value::List(mut xs) => {
                    xs.reverse();
                    Value::List(xs)
                }
                Value::String(s) => {
                    self.remaining(size_of::<Value>().saturating_add(s.len()))?;
                    let mut reversed = String::with_capacity(s.len());
                    reversed.extend(s.chars().rev());
                    Value::String(reversed)
                }
                _ => return Err(type_error("reverse requires a string or list")),
            },
            F::Trim | F::Ltrim | F::Rtrim => {
                let Value::String(s) = first else {
                    return Err(type_error("string function requires a string"));
                };
                let trimmed = match function {
                    F::Trim => s.trim(),
                    F::Ltrim => s.trim_start(),
                    F::Rtrim => s.trim_end(),
                    _ => unreachable!(),
                };
                self.remaining(size_of::<Value>().saturating_add(trimmed.len()))?;
                Value::String(trimmed.to_owned())
            }
            F::ToLower | F::ToUpper => {
                let Value::String(mut s) = first else {
                    return Err(type_error("string function requires a string"));
                };
                if s.is_ascii() {
                    if function == F::ToLower {
                        s.make_ascii_lowercase();
                    } else {
                        s.make_ascii_uppercase();
                    }
                    Value::String(s)
                } else {
                    // Keep contextual Unicode mapping in the standard library.
                    // Its output initially reserves the input's byte length.
                    // allocation_bounds guards these pinned-toolchain capacity
                    // assumptions independently of this evaluator.
                    self.remaining(size_of::<Value>().saturating_add(s.len()))?;
                    let bytes = s.chars().fold(0_usize, |bytes, c| {
                        bytes.saturating_add(if function == F::ToLower {
                            c.to_lowercase().map(char::len_utf8).sum::<usize>()
                        } else {
                            c.to_uppercase().map(char::len_utf8).sum::<usize>()
                        })
                    });
                    // The contextual sigma forms have equal UTF-8 length.
                    // If conversion grows, admit geometric buffer growth and
                    // the minimum byte-vector capacity before allocating it.
                    if bytes > s.len() {
                        self.remaining(
                            size_of::<Value>().saturating_add(bytes.saturating_mul(2).max(8)),
                        )?;
                    }
                    Value::String(if function == F::ToLower {
                        s.to_lowercase()
                    } else {
                        s.to_uppercase()
                    })
                }
            }
            F::Substring => {
                let Value::String(s) = first else {
                    return Err(type_error("substring requires a string"));
                };
                let start = nonnegative(
                    args.get(1)
                        .ok_or_else(|| type_error("missing substring offset"))?,
                )?;
                let len = args
                    .get(2)
                    .map(nonnegative)
                    .transpose()?
                    .unwrap_or(usize::MAX);
                // Resolve character offsets into a borrowed UTF-8 slice before
                // admitting and copying its exact bytes. No temporary character
                // collection or geometrically growing output buffer is needed.
                let start = s
                    .char_indices()
                    .nth(start)
                    .map_or(s.len(), |(offset, _)| offset);
                let bytes = s[start..]
                    .char_indices()
                    .nth(len)
                    .map_or(s.len() - start, |(offset, _)| offset);
                self.remaining(size_of::<Value>().saturating_add(bytes))?;
                Value::String(s[start..start + bytes].to_owned())
            }
            F::Coalesce | F::Exists => unreachable!("handled before strict function evaluation"),
        })
    }

    fn aggregate(
        &self,
        function: Aggregate,
        argument: Option<&Expression>,
        distinct: bool,
    ) -> Result<Value> {
        let Some(group) = self.group else {
            return Err(type_error("aggregate requires a grouped input"));
        };
        let mut accumulator = super::Accumulator::new(function, distinct);
        for row in group {
            let value = match argument {
                None => Value::Integer(1),
                Some(expression) => Self {
                    row,
                    group: None,
                    ..self.remaining(accumulator.allocated_bytes())?
                }
                .eval(expression)?,
            };
            accumulator.push(value, self.max_collection_items, self.max_value_bytes)?;
        }
        accumulator.finish()
    }
}

impl super::ProjectionEvaluator<Expression> for Evaluation<'_> {
    type Value = Value;
    type Error = QueryError;

    fn prepare(&mut self, columns: usize) -> Result<()> {
        *self = self.remaining(
            size_of::<Vec<Value>>().saturating_add(columns.saturating_mul(size_of::<Value>())),
        )?;
        Ok(())
    }

    async fn evaluate(&mut self, expression: &Expression) -> Result<Value> {
        let value = self.eval(expression)?;
        // Earlier projected values remain alive while the next expression runs.
        // Slot storage was admitted by prepare; retain the owned payload here.
        *self = self.remaining(value.allocated_bytes().saturating_sub(size_of::<Value>()))?;
        Ok(value)
    }
}

pub fn nonnegative(value: &Value) -> Result<usize> {
    match value {
        Value::Integer(i) => usize::try_from(*i).map_err(|_| {
            QueryError::runtime(
                "SyntaxError",
                "NegativeIntegerArgument",
                "expected a nonnegative integer",
            )
        }),
        _ => Err(QueryError::runtime(
            "SyntaxError",
            "InvalidArgumentType",
            "expected an integer",
        )),
    }
}

fn position(index: i64, len: usize) -> Option<usize> {
    let index = if index < 0 {
        (len as i128) + i128::from(index)
    } else {
        i128::from(index)
    };
    usize::try_from(index).ok().filter(|i| *i < len)
}
fn slice_position(index: i64, len: usize) -> usize {
    let index = if index < 0 {
        len as i128 + i128::from(index)
    } else {
        i128::from(index)
    };
    index.clamp(0, len as i128) as usize
}
fn truth(value: Option<bool>) -> Value {
    value.map(Value::Boolean).unwrap_or(Value::Null)
}
pub(super) fn type_error(message: &str) -> QueryError {
    QueryError::runtime("TypeError", "InvalidArgumentType", message)
}
pub(super) fn overflow() -> QueryError {
    QueryError::runtime(
        "ArithmeticError",
        "NumberOutOfRange",
        "integer result is outside the signed 64-bit range",
    )
}
pub(super) fn binary(op: Binary, a: Value, b: Value) -> Result<Value> {
    use Binary as B;
    if matches!(op, B::And | B::Or | B::Xor) {
        let (a, b) = (a.truth()?, b.truth()?);
        return Ok(truth(match op {
            B::And => match (a, b) {
                (Some(false), _) | (_, Some(false)) => Some(false),
                (Some(true), Some(true)) => Some(true),
                _ => None,
            },
            B::Or => match (a, b) {
                (Some(true), _) | (_, Some(true)) => Some(true),
                (Some(false), Some(false)) => Some(false),
                _ => None,
            },
            B::Xor => a.zip(b).map(|(a, b)| a ^ b),
            _ => unreachable!(),
        }));
    }
    if op == B::In {
        if b == Value::Null {
            return Ok(Value::Null);
        }
        let Value::List(xs) = b else {
            return Err(type_error("IN requires a list"));
        };
        let mut unknown = false;
        for x in xs {
            match a.equals(&x) {
                Some(true) => return Ok(Value::Boolean(true)),
                None => unknown = true,
                Some(false) => {}
            }
        }
        return Ok(if unknown {
            Value::Null
        } else {
            Value::Boolean(false)
        });
    }
    if matches!(op, B::Equal | B::NotEqual) {
        return Ok(truth(a.equals(&b).map(|equal| {
            if op == B::Equal {
                equal
            } else {
                !equal
            }
        })));
    }
    if a == Value::Null || b == Value::Null {
        return Ok(Value::Null);
    }
    if matches!(op, B::Less | B::LessEqual | B::Greater | B::GreaterEqual) {
        if matches!(
            (&a, &b),
            (
                Value::Integer(_) | Value::Float(_),
                Value::Integer(_) | Value::Float(_)
            )
        ) && (matches!(&a,Value::Float(f) if f.is_nan())
            || matches!(&b,Value::Float(f) if f.is_nan()))
        {
            return Ok(Value::Boolean(false));
        }
        let comparison = predicate_order(&a, &b);
        return Ok(truth(comparison.map(|o| match op {
            B::Less => o == Ordering::Less,
            B::LessEqual => o != Ordering::Greater,
            B::Greater => o == Ordering::Greater,
            B::GreaterEqual => o != Ordering::Less,
            _ => unreachable!(),
        })));
    }
    if matches!(op, B::StartsWith | B::EndsWith | B::Contains) {
        return Ok(match (&a, &b) {
            (Value::String(a), Value::String(b)) => Value::Boolean(match op {
                B::StartsWith => a.starts_with(b),
                B::EndsWith => a.ends_with(b),
                B::Contains => a.contains(b),
                _ => unreachable!(),
            }),
            _ => Value::Null,
        });
    }
    if let (Value::Integer(a), Value::Integer(b)) = (&a, &b) {
        if matches!(op, B::Divide | B::Modulo) && *b == 0 {
            return Err(QueryError::runtime(
                "ArithmeticError",
                "DivisionByZero",
                "integer division by zero",
            ));
        }
        let value = match op {
            B::Add => a.checked_add(*b),
            B::Subtract => a.checked_sub(*b),
            B::Multiply => a.checked_mul(*b),
            B::Divide => a.checked_div(*b),
            B::Modulo => a.checked_rem(*b),
            B::Power => return Ok(Value::Float((*a as f64).powf(*b as f64))),
            _ => return Err(type_error("invalid numeric operation")),
        };
        return Ok(Value::Integer(value.ok_or_else(overflow)?));
    }
    let float = |v: Value| match v {
        Value::Integer(i) => Ok(i as f64),
        Value::Float(f) => Ok(f),
        _ => Err(type_error("arithmetic requires numbers")),
    };
    let (a, b) = (float(a)?, float(b)?);
    Ok(Value::Float(match op {
        B::Add => a + b,
        B::Subtract => a - b,
        B::Multiply => a * b,
        B::Divide => a / b,
        B::Modulo => a % b,
        B::Power => a.powf(b),
        _ => return Err(type_error("invalid arithmetic operation")),
    }))
}

fn predicate_order(a: &Value, b: &Value) -> Option<Ordering> {
    match (a, b) {
        (Value::Integer(_) | Value::Float(_), Value::Integer(_) | Value::Float(_)) => {
            a.number_cmp(b)
        }
        (Value::String(a), Value::String(b)) => Some(a.cmp(b)),
        (Value::Boolean(a), Value::Boolean(b)) => Some(a.cmp(b)),
        (Value::List(a), Value::List(b)) => {
            for (a, b) in a.iter().zip(b) {
                let comparison = predicate_order(a, b)?;
                if !comparison.is_eq() {
                    return Some(comparison);
                }
            }
            Some(a.len().cmp(&b.len()))
        }
        _ => None,
    }
}

/// A validated integer generator. Overflow at the endpoint terminates normally.
pub struct IntegerRange {
    next: Option<i64>,
    end: i64,
    step: std::num::NonZeroI64,
}
impl IntegerRange {
    fn new(arguments: &[Value]) -> Result<Self> {
        let [Value::Integer(start), Value::Integer(end), rest @ ..] = arguments else {
            return Err(QueryError::runtime(
                "ArgumentError",
                "InvalidArgumentType",
                "range requires integer endpoints",
            ));
        };
        let step = match rest {
            [] => 1,
            [Value::Integer(step)] => *step,
            _ => {
                return Err(QueryError::runtime(
                    "ArgumentError",
                    "InvalidArgumentType",
                    "range requires an integer step",
                ))
            }
        };
        let step = std::num::NonZeroI64::new(step).ok_or_else(|| {
            QueryError::runtime(
                "ArgumentError",
                "NumberOutOfRange",
                "range step cannot be zero",
            )
        })?;
        Ok(Self {
            next: Some(*start),
            end: *end,
            step,
        })
    }
}
impl Iterator for IntegerRange {
    type Item = Value;
    fn next(&mut self) -> Option<Self::Item> {
        let value = self.next?;
        if self.step.get() > 0 && value > self.end || self.step.get() < 0 && value < self.end {
            self.next = None;
            return None;
        }
        self.next = value.checked_add(self.step.get());
        Some(Value::Integer(value))
    }
}

/// Owned expansion state with its original allocation bound. The reservation
/// remains conservative as values move out; IntoIter alone would lose the
/// source vector's spare-capacity information.
pub struct UnwindValues {
    source: UnwindSource,
    allocated_bytes: usize,
}
enum UnwindSource {
    Values(std::vec::IntoIter<Value>),
    Range(IntegerRange),
}
impl UnwindValues {
    fn values(values: Vec<Value>) -> Self {
        let allocated_bytes = values.iter().fold(
            size_of::<Self>().saturating_add(values.capacity().saturating_mul(size_of::<Value>())),
            |bytes, value| {
                bytes.saturating_add(value.allocated_bytes().saturating_sub(size_of::<Value>()))
            },
        );
        Self {
            source: UnwindSource::Values(values.into_iter()),
            allocated_bytes,
        }
    }
    /// Original owned allocation, including nested values and spare capacity.
    pub fn allocated_bytes(&self) -> usize {
        self.allocated_bytes
    }
}
impl Iterator for UnwindValues {
    type Item = Value;
    fn next(&mut self) -> Option<Value> {
        match &mut self.source {
            UnwindSource::Values(values) => values.next(),
            UnwindSource::Range(range) => range.next(),
        }
    }
}

#[cfg(test)]
mod unwind_allocation_tests {
    use super::*;

    #[test]
    fn retained_unwind_bound_includes_spare_and_nested_allocations() {
        let mut text = String::with_capacity(128);
        text.push('x');
        let mut nested = Vec::with_capacity(40);
        nested.push(Value::String(text));
        let mut values = Vec::with_capacity(64);
        values.push(Value::List(nested));
        let mut values = UnwindValues::values(values);
        let bound = values.allocated_bytes();
        assert!(bound >= size_of::<UnwindValues>() + 104 * size_of::<Value>() + 128);
        assert_eq!(
            values.next(),
            Some(Value::List(vec![Value::String("x".into())]))
        );
        assert_eq!(values.allocated_bytes(), bound);
        assert_eq!(values.next(), None);
    }
}
