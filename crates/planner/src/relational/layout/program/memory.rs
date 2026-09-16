//! Retained physical-program accounting. Original logical queries and native
//! executable DAGs remain shared compilation inputs. Their construction budget
//! is separate; this bound covers the derived view while a request executes.
use super::RowProgram;
use crate::relational as r;

impl RowProgram {
    /// Retained layout and compiled-view allocation bound. Charge this before
    /// execution storage access and keep it through result preparation and commit.
    /// Compilation precedes this admission; this is not a compiler-allocation cap.
    pub fn retained_layout_bytes(&self) -> usize {
        let layout = self.query.source.layout();
        let bytes = size_of::<Self>()
            .saturating_add(2 * size_of::<usize>())
            .saturating_add(size_of::<r::RowLayout>() + 2 * size_of::<usize>())
            .saturating_add(
                layout
                    .cells
                    .capacity()
                    .saturating_mul(size_of::<Option<r::RowCell>>()),
            );
        let Some(mapped) = &self.query.mapped else {
            return bytes;
        };
        let bytes = mapped.operators.iter().fold(
            bytes.saturating_add(
                mapped
                    .operators
                    .capacity()
                    .saturating_mul(size_of::<r::Operator>()),
            ),
            |bytes, operator| bytes.saturating_add(operator_heap(operator)),
        );
        let bytes = mapped.returns.iter().fold(
            bytes.saturating_add(
                mapped
                    .returns
                    .capacity()
                    .saturating_mul(size_of::<(String, r::Slot)>()),
            ),
            |bytes, (name, _)| bytes.saturating_add(name.capacity()),
        );
        let bytes = self.matches.values().fold(
            bytes
                .saturating_add(
                    size_of::<std::collections::BTreeMap<usize, r::MatchPlan>>()
                        + 2 * size_of::<usize>(),
                )
                .saturating_add(r::allocation::btree_bytes::<usize, r::MatchPlan>(
                    self.matches.len(),
                )),
            |bytes, plan| {
                let bytes = bytes
                    .saturating_add(
                        plan.sources
                            .capacity()
                            .saturating_mul(size_of::<r::PlannedNode>()),
                    )
                    .saturating_add(
                        plan.steps
                            .capacity()
                            .saturating_mul(size_of::<r::MatchStep>()),
                    )
                    .saturating_add(r::allocation::btree_bytes::<r::Slot, ()>(
                        plan.incoming.len(),
                    ));
                plan.steps.iter().fold(bytes, |bytes, step| {
                    bytes.saturating_add(match step {
                        r::MatchStep::Scan(_) | r::MatchStep::Expand { .. } => 0,
                        r::MatchStep::HashJoin {
                            property,
                            probe_property,
                            ..
                        } => property
                            .capacity()
                            .saturating_add(probe_property.capacity()),
                        // These immutable identifiers were cloned once by relocation;
                        // String::clone allocates their length, not source spare capacity.
                        r::MatchStep::IndexLookup(lookup) => lookup
                            .index
                            .index_id
                            .len()
                            .saturating_add(lookup.key.label.len())
                            .saturating_add(lookup.key.property.len()),
                    })
                })
            },
        );
        let windows = self
            .windows
            .as_ref()
            .expect("mapped program has mapped window proofs");
        windows.values().fold(
            bytes.saturating_add(r::allocation::btree_bytes::<usize, r::InputWindow>(
                windows.len(),
            )),
            |bytes, window| bytes.saturating_add(window.expression_heap_bytes(expression_heap)),
        )
    }
}

fn expression_heap(expression: &r::Expression) -> usize {
    use r::Expression as E;
    match expression {
        E::Literal(value) => value
            .allocated_bytes()
            .saturating_sub(size_of::<r::Value>()),
        E::Slot(_) => 0,
        E::Parameter(name) | E::HasLabel(_, name) => name.capacity(),
        E::Property(value, key) => size_of::<E>()
            .saturating_add(expression_heap(value))
            .saturating_add(key.capacity()),
        E::Unary(_, value) => size_of::<E>().saturating_add(expression_heap(value)),
        E::Index(left, right) | E::Binary(_, left, right) => (2 * size_of::<E>())
            .saturating_add(expression_heap(left))
            .saturating_add(expression_heap(right)),
        E::Slice { value, start, end } => {
            std::iter::once(value)
                .chain(start)
                .chain(end)
                .fold(0_usize, |bytes, value| {
                    bytes
                        .saturating_add(size_of::<E>())
                        .saturating_add(expression_heap(value))
                })
        }
        E::Aggregate { argument, .. } => argument.as_ref().map_or(0, |value| {
            size_of::<E>().saturating_add(expression_heap(value))
        }),
        E::Function(_, values) | E::List(values) => values.iter().fold(
            values.capacity().saturating_mul(size_of::<E>()),
            |bytes, value| bytes.saturating_add(expression_heap(value)),
        ),
        E::Map(values) => values.iter().fold(
            values.capacity().saturating_mul(size_of::<(String, E)>()),
            |bytes, (name, value)| {
                bytes
                    .saturating_add(name.capacity())
                    .saturating_add(expression_heap(value))
            },
        ),
        E::Case {
            branches,
            otherwise,
        } => branches.iter().fold(
            branches
                .capacity()
                .saturating_mul(size_of::<(E, E)>())
                .saturating_add(size_of::<E>())
                .saturating_add(expression_heap(otherwise)),
            |bytes, (when, then)| {
                bytes
                    .saturating_add(expression_heap(when))
                    .saturating_add(expression_heap(then))
            },
        ),
        E::SimpleCase(case) => case.branches.iter().fold(
            size_of::<r::SimpleCase<E>>()
                .saturating_add(case.branches.capacity().saturating_mul(size_of::<(E, E)>()))
                .saturating_add(expression_heap(&case.operand))
                .saturating_add(expression_heap(&case.otherwise)),
            |bytes, (when, then)| {
                bytes
                    .saturating_add(expression_heap(when))
                    .saturating_add(expression_heap(then))
            },
        ),
    }
}

fn selection_heap(predicate: &r::SelectionProgram) -> usize {
    (size_of::<r::Expression>() + 2 * size_of::<usize>())
        .saturating_add(expression_heap(predicate.expression()))
        .saturating_add(r::allocation::btree_bytes::<r::Slot, ()>(
            predicate.references().len(),
        ))
}

fn property_heap(properties: &Vec<(String, r::Expression)>) -> usize {
    properties.iter().fold(
        properties
            .capacity()
            .saturating_mul(size_of::<(String, r::Expression)>()),
        |bytes, (key, value)| {
            bytes
                .saturating_add(key.capacity())
                .saturating_add(expression_heap(value))
        },
    )
}

fn pattern_heap(pattern: &r::Pattern) -> usize {
    let bytes = pattern.nodes.iter().fold(
        pattern
            .nodes
            .capacity()
            .saturating_mul(size_of::<r::NodePattern>()),
        |bytes, node| {
            bytes
                .saturating_add(node.label.as_ref().map_or(0, String::capacity))
                .saturating_add(property_heap(&node.properties))
        },
    );
    let bytes = pattern.relationships.iter().fold(
        bytes.saturating_add(
            pattern
                .relationships
                .capacity()
                .saturating_mul(size_of::<r::RelationshipPattern>()),
        ),
        |bytes, edge| {
            edge.types.iter().fold(
                bytes
                    .saturating_add(edge.types.capacity().saturating_mul(size_of::<String>()))
                    .saturating_add(property_heap(&edge.properties)),
                |bytes, kind| bytes.saturating_add(kind.capacity()),
            )
        },
    );
    pattern.paths.iter().fold(
        bytes.saturating_add(
            pattern
                .paths
                .capacity()
                .saturating_mul(size_of::<r::PathPattern>()),
        ),
        |bytes, path| {
            bytes.saturating_add(
                path.nodes
                    .capacity()
                    .saturating_add(path.relationships.capacity())
                    .saturating_mul(size_of::<r::Slot>()),
            )
        },
    )
}

fn operator_heap(operator: &r::Operator) -> usize {
    match operator {
        r::Operator::Create(pattern) => pattern_heap(pattern),
        r::Operator::Match {
            pattern, predicate, ..
        } => pattern_heap(pattern).saturating_add(predicate.as_ref().map_or(0, selection_heap)),
        r::Operator::Filter(predicate) => selection_heap(predicate),
        r::Operator::Unwind { expression, .. } => expression_heap(expression),
        r::Operator::Project {
            items,
            ordering,
            predicate,
            skip,
            limit,
            ..
        } => {
            let bytes = items.iter().fold(
                items
                    .capacity()
                    .saturating_mul(size_of::<r::Projection>())
                    .saturating_add(r::allocation::btree_bytes::<r::Slot, ()>(
                        items.references().len(),
                    ))
                    .saturating_add(r::allocation::btree_bytes::<r::Slot, ()>(
                        items.outputs().len(),
                    )),
                |bytes, item| bytes.saturating_add(expression_heap(&item.expression)),
            );
            ordering.iter().fold(
                bytes
                    .saturating_add(ordering.capacity().saturating_mul(size_of::<r::Ordering>()))
                    .saturating_add(predicate.as_ref().map_or(0, selection_heap))
                    .saturating_add(skip.as_ref().map_or(0, expression_heap))
                    .saturating_add(limit.as_ref().map_or(0, expression_heap)),
                |bytes, order| bytes.saturating_add(expression_heap(&order.expression)),
            )
        }
        r::Operator::Update(updates) => updates.iter().fold(
            updates
                .capacity()
                .saturating_mul(size_of::<r::PropertyMutation>()),
            |bytes, update| {
                bytes.saturating_add(match update {
                    r::PropertyMutation::Set { key, value, .. } => {
                        key.capacity().saturating_add(expression_heap(value))
                    }
                    r::PropertyMutation::Remove { key, .. } => key.capacity(),
                    r::PropertyMutation::Replace { properties, .. }
                    | r::PropertyMutation::Extend { properties, .. } => expression_heap(properties),
                })
            },
        ),
        r::Operator::Delete { entities, .. } => entities.iter().fold(
            entities
                .capacity()
                .saturating_mul(size_of::<r::Expression>()),
            |bytes, value| bytes.saturating_add(expression_heap(value)),
        ),
    }
}
