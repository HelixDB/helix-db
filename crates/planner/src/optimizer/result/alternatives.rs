//! Retained physical alternative records.
//!
//! These records are the stable output of implementation-rule retention before
//! recursive best-plan selection chooses among them.

use serde::{Deserialize, Serialize};

use crate::{cost, memo, physical};

use super::super::provenance;
use super::index;

/// Physical alternatives collected for one memo group.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GroupAlternatives {
    /// Memo group.
    pub group: memo::MemoGroupId,
    /// Retained alternatives.
    pub alternatives: Vec<PhysicalAlternativeEntry>,
}

/// Physical alternative retained in a memo group.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PhysicalAlternativeEntry {
    /// Stable one-based ID inside the memo group.
    pub id: memo::PhysicalAlternativeId,
    /// Logical memo expression that produced this implementation.
    pub source_expr: memo::MemoExprId,
    /// Rule that produced this implementation.
    pub provenance: provenance::RuleProvenance,
    /// Physical implementation selected by an implementation rule.
    pub alternative: physical::PhysicalAlternative,
}

/// Best physical implementation resolved to the logical memo expression that
/// produced it.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SelectedPhysicalAlternative<'a> {
    /// Memo group selected for this result.
    pub group: memo::MemoGroupId,
    /// Retained physical alternative entry.
    pub entry: &'a PhysicalAlternativeEntry,
    /// Logical expression that produced the physical alternative.
    pub source_expr: &'a memo::MemoExpr,
    /// Full selected execution cost, including selected child groups that
    /// execute as separate selected roots.
    pub selected_cost: cost::CostVector,
}

#[derive(Debug, Clone, PartialEq)]
pub(in crate::optimizer) struct PendingPhysicalAlternative {
    pub(in crate::optimizer) source_expr: memo::MemoExprId,
    pub(in crate::optimizer) provenance: provenance::RuleProvenance,
    pub(in crate::optimizer) alternative: physical::PhysicalAlternative,
}

pub(super) struct IndexedGroupAlternatives {
    pub(super) groups: Vec<GroupAlternatives>,
    pub(super) index: index::PhysicalAlternativeIndex,
}

pub(super) fn group_alternatives(
    physical: std::collections::BTreeMap<memo::MemoGroupId, Vec<PendingPhysicalAlternative>>,
) -> IndexedGroupAlternatives {
    let mut group_indexes = std::collections::BTreeMap::new();
    let groups = physical
        .into_iter()
        .enumerate()
        .map(|(index, (group, alternatives))| {
            group_indexes.insert(group, index);
            GroupAlternatives {
                group,
                alternatives: memo::PhysicalAlternativeId::sequential()
                    .zip(alternatives)
                    .map(|(id, entry)| PhysicalAlternativeEntry {
                        id,
                        source_expr: entry.source_expr,
                        provenance: entry.provenance,
                        alternative: entry.alternative,
                    })
                    .collect(),
            }
        })
        .collect();
    IndexedGroupAlternatives {
        groups,
        index: index::PhysicalAlternativeIndex::from_generated_group_indexes(group_indexes),
    }
}
