//! Pattern-order alternatives owned by the production Cascades memo. A candidate
//! changes only traversal priority within one MATCH; predicates, relationship
//! uniqueness, correlations and optional boundaries remain with its row operator.
use super::{MatchStep, QueryError, Result, Slot};
use crate::{catalog, cost, logical, optimizer, physical, properties, rules};
use std::collections::BTreeSet;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PatternSource {
    pub slot: Slot,
    pub rows: u64,
    pub access_cost: cost::CostVector,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PatternExpansion {
    pub from: Slot,
    pub to: Slot,
    pub relationship: Slot,
    /// Estimated adjacency rows per source endpoint in each direction.
    pub forward_rows: u64,
    pub reverse_rows: u64,
}

/// A property equijoin whose null/error semantics were proven by the frontend
/// independent row contract before adding it to a graph candidate.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PatternEquality {
    pub left: Slot,
    pub left_key: String,
    pub right: Slot,
    pub right_key: String,
}

/// Equality access correlated with a previously resolved scalar binding. The
/// probe is a slot, so evaluating it cannot expose a later expression error.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PatternLookup {
    pub slot: Slot,
    pub probe: Slot,
    pub index: catalog::NodeEqualityIndexMeta,
    pub key: catalog::ScopedPropertyKey,
    pub estimated_rows: u64,
}

/// A validated graph conjunction and traversal priorities. Deserialization checks
/// the same endpoint/permutation contract as construction.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(try_from = "PatternOrderInput")]
pub struct GraphPatternOrder {
    sources: Vec<PatternSource>,
    relationships: Vec<PatternExpansion>,
    incoming: BTreeSet<Slot>,
    source_priority: Vec<usize>,
    relationship_priority: Vec<usize>,
    equality: Option<PatternEquality>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    lookups: Vec<PatternLookup>,
}
#[derive(serde::Deserialize)]
struct PatternOrderInput {
    sources: Vec<PatternSource>,
    relationships: Vec<PatternExpansion>,
    incoming: BTreeSet<Slot>,
    source_priority: Vec<usize>,
    relationship_priority: Vec<usize>,
    equality: Option<PatternEquality>,
    #[serde(default)]
    lookups: Vec<PatternLookup>,
}
impl TryFrom<PatternOrderInput> for GraphPatternOrder {
    type Error = QueryError;
    fn try_from(input: PatternOrderInput) -> Result<Self> {
        let mut result = Self::new(input.sources, input.relationships, input.incoming)?;
        for (priority, length) in [
            (&input.source_priority, result.sources.len()),
            (&input.relationship_priority, result.relationships.len()),
        ] {
            if priority.len() != length
                || priority.iter().copied().collect::<BTreeSet<_>>() != (0..length).collect()
            {
                return Err(QueryError::compile(
                    "InternalPlannerError",
                    "InvalidPatternOrder",
                    "pattern priorities must be permutations",
                ));
            }
        }
        if let Some(equality) = input.equality {
            result = result.with_equality(equality)?;
        }
        result = result.with_lookups(input.lookups)?;
        result.source_priority = input.source_priority;
        result.relationship_priority = input.relationship_priority;
        Ok(result)
    }
}

impl GraphPatternOrder {
    pub fn new(
        mut sources: Vec<PatternSource>,
        relationships: Vec<PatternExpansion>,
        incoming: BTreeSet<Slot>,
    ) -> Result<Self> {
        let nodes = sources.iter().map(|s| s.slot).collect::<BTreeSet<_>>();
        if sources.is_empty()
            || sources.len() > 4096
            || relationships.len() > 4096
            || nodes.len() != sources.len()
            || relationships
                .iter()
                .any(|r| !nodes.contains(&r.from) || !nodes.contains(&r.to))
        {
            return Err(QueryError::compile(
                "InternalPlannerError",
                "InvalidPatternOrder",
                "pattern sources must be unique and contain every endpoint",
            ));
        }
        sources.sort_by_key(|s| (s.rows, s.slot));
        Ok(Self {
            source_priority: (0..sources.len()).collect(),
            relationship_priority: (0..relationships.len()).collect(),
            sources,
            relationships,
            incoming,
            equality: None,
            lookups: Vec::new(),
        })
    }

    pub fn with_equality(mut self, equality: PatternEquality) -> Result<Self> {
        if equality.left == equality.right
            || equality.left_key.is_empty()
            || equality.right_key.is_empty()
            || [equality.left, equality.right]
                .iter()
                .any(|slot| !self.sources.iter().any(|s| s.slot == *slot))
        {
            return Err(QueryError::compile(
                "InternalPlannerError",
                "InvalidPatternEquality",
                "join keys require distinct node bindings and nonempty property names",
            ));
        }
        self.equality = Some(equality);
        Ok(self)
    }

    /// Only incoming probes and unbound pattern nodes are legal lookup endpoints.
    pub fn with_lookups(mut self, lookups: Vec<PatternLookup>) -> Result<Self> {
        let mut targets = BTreeSet::new();
        if lookups.iter().any(|lookup| {
            !targets.insert(lookup.slot)
                || self.incoming.contains(&lookup.slot)
                || !self.incoming.contains(&lookup.probe)
                || !self.sources.iter().any(|source| source.slot == lookup.slot)
        }) {
            return Err(QueryError::compile(
                "InternalPlannerError",
                "InvalidPatternLookup",
                "correlated lookups require an incoming probe and a distinct unbound node",
            ));
        }
        self.lookups = lookups;
        Ok(self)
    }

    /// At most two neighbors per rule invocation keeps allocation and scheduling
    /// bounded even for wide patterns. Memo deduplication closes both rotations;
    /// global rule/memo/time budgets bound their combined exploration.
    fn neighbors(&self) -> Vec<Self> {
        let mut neighbors = Vec::with_capacity(2);
        if self.source_priority.len() > 1 {
            let mut next = self.clone();
            next.source_priority.rotate_left(1);
            neighbors.push(next);
        }
        if self.relationship_priority.len() > 1 {
            let mut next = self.clone();
            next.relationship_priority.rotate_left(1);
            neighbors.push(next);
        }
        neighbors
    }

    /// Deterministic executable order, including expansion into bound endpoints.
    /// Estimates affect cost only; zero/unknown statistics never prune graph data.
    pub fn schedule(&self, storage: &cost::StorageCostProfile) -> PatternSchedule {
        let mut known = self.incoming.clone();
        let mut remaining: BTreeSet<_> = (0..self.relationships.len()).collect();
        let mut steps = Vec::with_capacity(self.sources.len() + self.relationships.len());
        let mut cost = cost::CostVector::ZERO;
        let mut rows = 1_u64;
        let mut cartesian_products = 0;
        loop {
            if let Some(index) = self.relationship_priority.iter().copied().find(|index| {
                let rel = &self.relationships[*index];
                remaining.contains(index) && (known.contains(&rel.from) || known.contains(&rel.to))
            }) {
                remaining.remove(&index);
                let rel = &self.relationships[index];
                let reverse = !known.contains(&rel.from);
                let (from, to, fanout) = if reverse {
                    (rel.to, rel.from, rel.reverse_rows)
                } else {
                    (rel.from, rel.to, rel.forward_rows)
                };
                cost = cost.serial(
                    storage
                        .range_scan(cost::EstimatedRows::rows(fanout))
                        .saturating_mul(rows),
                );
                if !known.contains(&to) {
                    rows = rows.saturating_mul(fanout);
                }
                cost = cost.serial(storage.stream_operator(cost::EstimatedRows::rows(rows)));
                steps.push(MatchStep::Expand {
                    relationship: index,
                    from,
                    to,
                    reverse,
                });
                known.insert(to);
                known.insert(rel.relationship);
                continue;
            }
            let Some(source) = self
                .source_priority
                .iter()
                .map(|i| &self.sources[*i])
                .find(|source| !known.contains(&source.slot))
            else {
                break;
            };
            let join = self.equality.as_ref().and_then(|equality| {
                if equality.left == source.slot && known.contains(&equality.right) {
                    Some((equality.right, &equality.left_key, &equality.right_key))
                } else if equality.right == source.slot && known.contains(&equality.left) {
                    Some((equality.left, &equality.right_key, &equality.left_key))
                } else {
                    None
                }
            });
            if let Some(lookup) = self
                .lookups
                .iter()
                .find(|lookup| lookup.slot == source.slot)
            {
                let cardinality = cost::EstimatedRows::rows(lookup.estimated_rows);
                let lookup_cost = match lookup.index.uniqueness {
                    catalog::IndexUniqueness::Unique => storage.unique_equality_lookup(cardinality),
                    catalog::IndexUniqueness::NonUnique => storage
                        .bitmap_equality_lookup(cardinality)
                        .serial(storage.authoritative_verification(cardinality)),
                };
                cost = cost.serial(lookup_cost.saturating_mul(rows));
                rows = rows.saturating_mul(lookup.estimated_rows);
                steps.push(MatchStep::IndexLookup(lookup.clone()));
                known.insert(source.slot);
                continue;
            }
            cost = cost.serial(source.access_cost);
            if let Some((probe, property, probe_property)) = join {
                cost =
                    cost.serial(storage.stream_operator(cost::EstimatedRows::rows(
                        rows.saturating_add(source.rows),
                    )));
                // Without NDV statistics use the larger input as a heuristic
                // estimate, never as a correctness bound or a data truncation.
                rows = rows.max(source.rows);
                cost.peak_memory = cost
                    .peak_memory
                    .max(cost::ByteEstimate::bytes(source.rows.saturating_mul(128)));
                steps.push(MatchStep::HashJoin {
                    slot: source.slot,
                    property: property.clone(),
                    probe,
                    probe_property: probe_property.clone(),
                });
            } else {
                if self.sources.iter().any(|s| known.contains(&s.slot)) {
                    cartesian_products += 1;
                }
                rows = rows.saturating_mul(source.rows);
                cost = cost.serial(storage.stream_operator(cost::EstimatedRows::rows(rows)));
                steps.push(MatchStep::Scan(source.slot));
            }
            known.insert(source.slot);
        }
        assert!(
            remaining.is_empty(),
            "validated connected endpoints must be scheduled"
        );
        PatternSchedule {
            steps,
            cost,
            estimated_rows: rows,
            cartesian_products,
        }
    }
}

pub struct PatternSchedule {
    pub steps: Vec<MatchStep>,
    pub cost: cost::CostVector,
    pub estimated_rows: u64,
    pub cartesian_products: usize,
}

pub struct GraphPatternImplementationRule {
    metadata: rules::RuleMetadata,
}
impl Default for GraphPatternImplementationRule {
    fn default() -> Self {
        Self {
            metadata: rules::RuleMetadata::new(
                rules::RuleId::known(rules::KnownRuleId::SeedGraphPattern),
                rules::RuleKind::Implementation,
            ),
        }
    }
}
impl optimizer::OptimizerRule for GraphPatternImplementationRule {
    fn metadata(&self) -> &rules::RuleMetadata {
        &self.metadata
    }
    fn apply(&self, input: optimizer::RuleInput<'_>) -> optimizer::RuleResult {
        let logical::LogicalExpr::GraphPattern(pattern) = input.expr else {
            return optimizer::RuleResult::NotApplicable;
        };
        let schedule = pattern.schedule(input.storage);
        if !pattern.lookups.is_empty() {
            let mut scan = pattern.clone();
            scan.lookups.clear();
            let scan_cost = scan.schedule(input.storage).cost;
            return optimizer::RuleResult::Applied(optimizer::RuleEffect::Physical(
                crate::ir::AtLeast::try_from_vec(vec![
                    physical::PhysicalAlternative::new(
                        physical::PhysicalExpr::GraphPattern(pattern.clone()),
                        properties::DeliveredProperties::unknown(),
                        schedule.cost,
                    ),
                    physical::PhysicalAlternative::new(
                        physical::PhysicalExpr::GraphPattern(scan),
                        properties::DeliveredProperties::unknown(),
                        scan_cost,
                    ),
                ])
                .expect("indexed and scan implementations"),
            ));
        }
        optimizer::RuleResult::Applied(optimizer::RuleEffect::Physical(
            crate::ir::AtLeast::from_one(physical::PhysicalAlternative::new(
                physical::PhysicalExpr::GraphPattern(pattern.clone()),
                properties::DeliveredProperties::unknown(),
                schedule.cost,
            )),
        ))
    }
}

pub struct GraphPatternExplorationRule {
    metadata: rules::RuleMetadata,
}
impl Default for GraphPatternExplorationRule {
    fn default() -> Self {
        Self {
            metadata: rules::RuleMetadata::new(
                rules::RuleId::known(rules::KnownRuleId::GraphPatternOrder),
                rules::RuleKind::Exploration,
            ),
        }
    }
}
impl optimizer::OptimizerRule for GraphPatternExplorationRule {
    fn metadata(&self) -> &rules::RuleMetadata {
        &self.metadata
    }
    fn apply(&self, input: optimizer::RuleInput<'_>) -> optimizer::RuleResult {
        let logical::LogicalExpr::GraphPattern(pattern) = input.expr else {
            return optimizer::RuleResult::NotApplicable;
        };
        let Some(neighbors) = crate::ir::AtLeast::try_from_vec(
            pattern
                .neighbors()
                .into_iter()
                .map(logical::LogicalExpr::GraphPattern)
                .collect(),
        ) else {
            return optimizer::RuleResult::NotApplicable;
        };
        optimizer::RuleResult::Applied(optimizer::RuleEffect::Logical(neighbors))
    }
}
