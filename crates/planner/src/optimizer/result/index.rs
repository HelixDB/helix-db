//! Private physical-alternative indexes for optimizer-result selection.
//!
//! `OptimizationResult` serializes the ordered physical records, while this
//! module owns the derived group lookup used by recursive best-plan selection.

use std::collections::BTreeMap;

use crate::memo;

use super::alternatives::GroupAlternatives;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(super) struct PhysicalAlternativeIndex {
    group_indexes: BTreeMap<memo::MemoGroupId, usize>,
}

impl PhysicalAlternativeIndex {
    pub(super) fn from_generated_group_indexes(
        group_indexes: BTreeMap<memo::MemoGroupId, usize>,
    ) -> Self {
        Self { group_indexes }
    }

    pub(super) fn from_groups(groups: &[GroupAlternatives]) -> Result<Self, PhysicalRecordError> {
        let mut group_indexes = BTreeMap::new();
        for (index, group) in groups.iter().enumerate() {
            if let Some(first_index) = group_indexes.insert(group.group, index) {
                return Err(PhysicalRecordError::DuplicateGroup {
                    group: group.group,
                    first_index,
                    duplicate_index: index,
                });
            }
            validate_alternative_ids(group)?;
        }
        Ok(Self { group_indexes })
    }

    pub(super) fn group<'a>(
        &self,
        groups: &'a [GroupAlternatives],
        group: memo::MemoGroupId,
    ) -> Option<&'a GroupAlternatives> {
        self.group_indexes
            .get(&group)
            .and_then(|index| groups.get(*index))
            .filter(|candidate| candidate.group == group)
    }
}

fn validate_alternative_ids(group: &GroupAlternatives) -> Result<(), PhysicalRecordError> {
    for (index, alternative) in group.alternatives.iter().enumerate() {
        let expected = index + 1;
        if alternative.id.get() != expected {
            return Err(PhysicalRecordError::NonSequentialAlternative {
                group: group.group,
                expected,
                actual: alternative.id.get(),
            });
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PhysicalRecordError {
    DuplicateGroup {
        group: memo::MemoGroupId,
        first_index: usize,
        duplicate_index: usize,
    },
    NonSequentialAlternative {
        group: memo::MemoGroupId,
        expected: usize,
        actual: usize,
    },
}

impl std::fmt::Display for PhysicalRecordError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DuplicateGroup {
                group,
                first_index,
                duplicate_index,
            } => write!(
                f,
                "physical alternatives for memo group {} are duplicated at indexes {} and {}",
                group.get(),
                first_index,
                duplicate_index
            ),
            Self::NonSequentialAlternative {
                group,
                expected,
                actual,
            } => write!(
                f,
                "physical alternative IDs for memo group {} must be sequential: expected {}, got {}",
                group.get(),
                expected,
                actual
            ),
        }
    }
}
