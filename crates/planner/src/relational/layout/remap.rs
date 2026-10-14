//! Relocate a validated program without changing its logical query or native
//! access programs. Relationship ordinals and native executable step IDs belong
//! to different index spaces and are deliberately preserved.
use super::RowLayout;
use crate::relational as r;

impl RowLayout {
    pub(super) fn slot(&self, slot: r::Slot) -> r::Slot {
        r::Slot(
            u32::try_from(
                self.cell(slot)
                    .expect("validated live binding has a cell")
                    .index(),
            )
            .expect("physical width is bounded by the logical binding catalog"),
        )
    }

    pub(super) fn expression(&self, expression: &r::Expression) -> r::Expression {
        expression
            .rewrite(&mut |expression| {
                Ok(match expression {
                    r::Expression::Slot(slot) => Some(r::Expression::Slot(self.slot(*slot))),
                    r::Expression::HasLabel(slot, label) => {
                        Some(r::Expression::HasLabel(self.slot(*slot), label.clone()))
                    }
                    _ => None,
                })
            })
            .expect("validated slot relocation cannot fail")
    }

    fn predicate(&self, predicate: &r::SelectionProgram) -> r::SelectionProgram {
        r::SelectionProgram::new(self.expression(predicate.expression()))
            .expect("relocating validated slots preserves expression validity")
    }

    fn pattern(&self, pattern: &r::Pattern) -> r::Pattern {
        r::Pattern {
            nodes: pattern
                .nodes
                .iter()
                .map(|node| r::NodePattern {
                    slot: self.slot(node.slot),
                    label: node.label.clone(),
                    properties: node
                        .properties
                        .iter()
                        .map(|(key, value)| (key.clone(), self.expression(value)))
                        .collect(),
                })
                .collect(),
            relationships: pattern
                .relationships
                .iter()
                .map(|edge| r::RelationshipPattern {
                    slot: self.slot(edge.slot),
                    from: self.slot(edge.from),
                    to: self.slot(edge.to),
                    direction: edge.direction,
                    types: edge.types.clone(),
                    properties: edge
                        .properties
                        .iter()
                        .map(|(key, value)| (key.clone(), self.expression(value)))
                        .collect(),
                })
                .collect(),
            paths: pattern
                .paths
                .iter()
                .map(|path| r::PathPattern {
                    slot: self.slot(path.slot),
                    nodes: path.nodes.iter().map(|slot| self.slot(*slot)).collect(),
                    relationships: path
                        .relationships
                        .iter()
                        .map(|slot| self.slot(*slot))
                        .collect(),
                })
                .collect(),
        }
    }

    pub(super) fn operator(&self, operator: &r::Operator) -> r::Operator {
        match operator {
            r::Operator::Match {
                pattern,
                optional,
                predicate,
            } => r::Operator::Match {
                pattern: self.pattern(pattern),
                optional: *optional,
                predicate: predicate
                    .as_ref()
                    .map(|predicate| self.predicate(predicate)),
            },
            r::Operator::Create(pattern) => r::Operator::Create(self.pattern(pattern)),
            r::Operator::Filter(predicate) => r::Operator::Filter(self.predicate(predicate)),
            r::Operator::Unwind { expression, slot } => r::Operator::Unwind {
                expression: self.expression(expression),
                slot: self.slot(*slot),
            },
            r::Operator::Project {
                items,
                distinct,
                ordering,
                predicate,
                skip,
                limit,
            } => r::Operator::Project {
                items: r::ProjectionProgram::new(
                    items
                        .iter()
                        .map(|item| r::Projection {
                            slot: self.slot(item.slot),
                            expression: self.expression(&item.expression),
                        })
                        .collect(),
                )
                .expect("simultaneous projection outputs retain distinct cells"),
                distinct: *distinct,
                ordering: ordering
                    .iter()
                    .map(|order| r::Ordering {
                        expression: self.expression(&order.expression),
                        descending: order.descending,
                    })
                    .collect(),
                predicate: predicate
                    .as_ref()
                    .map(|predicate| self.predicate(predicate)),
                skip: skip.as_ref().map(|value| self.expression(value)),
                limit: limit.as_ref().map(|value| self.expression(value)),
            },
            r::Operator::Update(updates) => r::Operator::Update(
                updates
                    .iter()
                    .map(|update| match update {
                        r::PropertyMutation::Set { entity, key, value } => {
                            r::PropertyMutation::Set {
                                entity: self.slot(*entity),
                                key: key.clone(),
                                value: self.expression(value),
                            }
                        }
                        r::PropertyMutation::Remove { entity, key } => {
                            r::PropertyMutation::Remove {
                                entity: self.slot(*entity),
                                key: key.clone(),
                            }
                        }
                        r::PropertyMutation::Replace { entity, properties } => {
                            r::PropertyMutation::Replace {
                                entity: self.slot(*entity),
                                properties: self.expression(properties),
                            }
                        }
                        r::PropertyMutation::Extend { entity, properties } => {
                            r::PropertyMutation::Extend {
                                entity: self.slot(*entity),
                                properties: self.expression(properties),
                            }
                        }
                    })
                    .collect(),
            ),
            r::Operator::Delete { entities, detach } => r::Operator::Delete {
                entities: entities
                    .iter()
                    .map(|value| self.expression(value))
                    .collect(),
                detach: *detach,
            },
        }
    }

    pub(super) fn match_plan(&self, plan: &r::MatchPlan) -> r::MatchPlan {
        r::MatchPlan {
            sources: plan
                .sources
                .iter()
                .map(|source| r::PlannedNode {
                    slot: self.slot(source.slot),
                    access: source.access.clone(),
                    estimated_rows: source.estimated_rows,
                })
                .collect(),
            steps: plan
                .steps
                .iter()
                .map(|step| match step {
                    r::MatchStep::Scan(slot) => r::MatchStep::Scan(self.slot(*slot)),
                    r::MatchStep::IndexLookup(lookup) => {
                        r::MatchStep::IndexLookup(r::PatternLookup {
                            slot: self.slot(lookup.slot),
                            probe: self.slot(lookup.probe),
                            ..lookup.clone()
                        })
                    }
                    r::MatchStep::Expand {
                        relationship,
                        from,
                        to,
                        reverse,
                    } => r::MatchStep::Expand {
                        relationship: *relationship,
                        from: self.slot(*from),
                        to: self.slot(*to),
                        reverse: *reverse,
                    },
                    r::MatchStep::HashJoin {
                        slot,
                        property,
                        probe,
                        probe_property,
                    } => r::MatchStep::HashJoin {
                        slot: self.slot(*slot),
                        property: property.clone(),
                        probe: self.slot(*probe),
                        probe_property: probe_property.clone(),
                    },
                })
                .collect(),
            incoming: plan.incoming.iter().map(|slot| self.slot(*slot)).collect(),
            cartesian_products: plan.cartesian_products,
            estimated_rows: plan.estimated_rows,
        }
    }
}
