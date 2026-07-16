//! Provenance contracts for selected executable roots.
//!
//! Selection provenance is shared by every selected root family. Keeping it in
//! its own module makes lineage and trace invariants independent from mutation,
//! control-flow, stream, and batch payload details.

use crate::{memo, rules};

/// Memo provenance for a selected optimizer physical alternative.
///
/// ```
/// use helix_planner::{exec, memo, rules};
///
/// let rule_id = rules::RuleId::new("source_access").unwrap();
/// let provenance = exec::SelectedOptimizerProvenance::new(
///     rule_id.clone(),
///     memo::MemoGroupId::new(1).unwrap(),
///     memo::MemoExprId::new(2).unwrap(),
///     memo::PhysicalAlternativeId::new(3).unwrap(),
///     memo::MemoChildGroups::empty(),
/// );
///
/// assert_eq!(provenance.rule_id(), &rule_id);
/// assert_eq!(provenance.memo_summary(), "group=1 expr=2 alternative=3 children=[]");
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectedOptimizerProvenance {
    rule_id: rules::RuleId,
    group: memo::MemoGroupId,
    source_expr: memo::MemoExprId,
    alternative: memo::PhysicalAlternativeId,
    source_children: memo::MemoChildGroups,
}

impl SelectedOptimizerProvenance {
    /// Build provenance for a selected physical alternative.
    pub fn new(
        rule_id: rules::RuleId,
        group: memo::MemoGroupId,
        source_expr: memo::MemoExprId,
        alternative: memo::PhysicalAlternativeId,
        source_children: memo::MemoChildGroups,
    ) -> Self {
        Self {
            rule_id,
            group,
            source_expr,
            alternative,
            source_children,
        }
    }

    /// Stable ID of the implementation rule that produced the selected alternative.
    pub const fn rule_id(&self) -> &rules::RuleId {
        &self.rule_id
    }

    /// Selected memo group.
    pub const fn group(&self) -> memo::MemoGroupId {
        self.group
    }

    /// Source memo expression that produced the selected alternative.
    pub const fn source_expr(&self) -> memo::MemoExprId {
        self.source_expr
    }

    /// Selected physical alternative ID inside the memo group.
    pub const fn alternative(&self) -> memo::PhysicalAlternativeId {
        self.alternative
    }

    /// Child memo groups referenced by the source expression.
    pub fn source_children(&self) -> &[memo::MemoGroupId] {
        self.source_children.as_slice()
    }

    /// Typed child memo groups referenced by the source expression.
    pub const fn source_child_groups(&self) -> &memo::MemoChildGroups {
        &self.source_children
    }

    /// Stable compact string for planner traces.
    pub fn memo_summary(&self) -> String {
        format!(
            "group={} expr={} alternative={} children={}",
            self.group.get(),
            self.source_expr.get(),
            self.alternative.get(),
            self.source_children.summary()
        )
    }
}

/// Provenance for a selected executable root.
///
/// ```
/// use helix_planner::{exec, memo, rules};
///
/// let optimizer = exec::SelectedOptimizerProvenance::new(
///     rules::RuleId::new("source_access").unwrap(),
///     memo::MemoGroupId::new(1).unwrap(),
///     memo::MemoExprId::new(1).unwrap(),
///     memo::PhysicalAlternativeId::new(1).unwrap(),
///     memo::MemoChildGroups::empty(),
/// );
/// let provenance = exec::SelectedRootProvenance::from_optimizer(optimizer);
///
/// assert_eq!(provenance.optimizer_rule_id().as_ref(), "source_access");
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectedRootProvenance {
    optimizer: SelectedOptimizerProvenance,
}

impl SelectedRootProvenance {
    /// Build optimizer provenance.
    pub fn from_optimizer(provenance: SelectedOptimizerProvenance) -> Self {
        Self {
            optimizer: provenance,
        }
    }

    /// Return optimizer provenance.
    pub const fn optimizer(&self) -> &SelectedOptimizerProvenance {
        &self.optimizer
    }

    /// Return the optimizer rule ID.
    pub const fn optimizer_rule_id(&self) -> &rules::RuleId {
        self.optimizer().rule_id()
    }
}

#[cfg(test)]
pub(super) fn test_selected_root_provenance() -> SelectedRootProvenance {
    SelectedRootProvenance::from_optimizer(SelectedOptimizerProvenance::new(
        rules::RuleId::new("test_selected_root").expect("test rule id is non-empty"),
        memo::MemoGroupId::new(1).expect("test group id is non-zero"),
        memo::MemoExprId::new(1).expect("test expression id is non-zero"),
        memo::PhysicalAlternativeId::new(1).expect("test alternative id is non-zero"),
        memo::MemoChildGroups::empty(),
    ))
}
