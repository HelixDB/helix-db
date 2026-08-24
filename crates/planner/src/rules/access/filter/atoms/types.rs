//! Typed access-filter index-plan payloads.

use crate::ir;

#[derive(Debug, Clone, PartialEq)]
pub(in crate::rules::access::filter) enum AccessFilterIndexAtom {
    Equality {
        property: ir::NonEmptyString,
        domain: AccessEqualityDomain,
    },
    Range {
        property: ir::NonEmptyString,
        range: ir::IndexRange,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub(in crate::rules::access::filter) enum AccessEqualityDomain {
    One(ir::IndexValue),
    Many(ir::AtLeast<ir::IndexValue, 2>),
    Runtime(ir::RuntimeEqualitySet),
}

#[derive(Debug, Clone, PartialEq)]
pub(in crate::rules::access::filter) struct AccessFilterIndexAtoms(
    ir::AtLeast<AccessFilterIndexAtom, 1>,
);

impl AccessFilterIndexAtoms {
    pub(super) fn new(atoms: Vec<AccessFilterIndexAtom>) -> Result<Self, EmptyIndexAtoms> {
        ir::AtLeast::<_, 1>::try_from_vec(atoms)
            .map(Self)
            .ok_or(EmptyIndexAtoms)
    }

    pub(in crate::rules::access::filter) fn try_map_ref<U, E, F>(
        &self,
        f: F,
    ) -> Result<ir::AtLeast<U, 1>, E>
    where
        F: FnMut(&AccessFilterIndexAtom) -> Result<U, E>,
    {
        self.0.try_map_ref(f)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct EmptyIndexAtoms;

impl AsRef<[AccessFilterIndexAtom]> for AccessFilterIndexAtoms {
    fn as_ref(&self) -> &[AccessFilterIndexAtom] {
        self.0.as_ref()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(in crate::rules::access::filter) enum AccessFilterIndexPlan {
    Conjunction(AccessFilterIndexAtoms),
    Disjunction(AccessFilterIndexBranches),
}

#[derive(Debug, Clone, PartialEq)]
pub(in crate::rules::access::filter) struct AccessFilterIndexBranches(
    ir::AtLeast<AccessFilterIndexAtoms, 2>,
);

impl AccessFilterIndexBranches {
    pub(super) fn new(branches: Vec<AccessFilterIndexAtoms>) -> Result<Self, TooFewIndexBranches> {
        ir::AtLeast::<_, 2>::try_from_vec(branches)
            .map(Self)
            .ok_or(TooFewIndexBranches)
    }

    pub(in crate::rules::access::filter) fn try_map_ref<U, E, F>(
        &self,
        f: F,
    ) -> Result<ir::AtLeast<U, 2>, E>
    where
        F: FnMut(&AccessFilterIndexAtoms) -> Result<U, E>,
    {
        self.0.try_map_ref(f)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct TooFewIndexBranches;

#[derive(Debug, Clone, PartialEq)]
pub(in crate::rules::access::filter) enum AccessFilterIndexPlanMatch {
    Planned(AccessFilterIndexPlan),
    NotIndexable(AccessFilterIndexPlanRejection),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::rules::access::filter) enum AccessFilterIndexPlanRejection {
    NotIndexCandidate,
    PropertyNotIndexable,
    EmptyIndexAtoms,
    TooFewIndexBranches,
    BranchLimitDisabled,
    BranchLimitExceeded,
    BranchNotIndexable,
    LabelScopeMismatch,
}

impl AsRef<[AccessFilterIndexAtoms]> for AccessFilterIndexBranches {
    fn as_ref(&self) -> &[AccessFilterIndexAtoms] {
        self.0.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn atom_and_branch_wrappers_reject_empty_payloads() {
        assert_eq!(
            AccessFilterIndexAtoms::new(Vec::new()),
            Err(EmptyIndexAtoms)
        );
        assert_eq!(
            AccessFilterIndexBranches::new(Vec::new()),
            Err(TooFewIndexBranches)
        );

        let atom = AccessFilterIndexAtom::Range {
            property: ir::NonEmptyString::new("age").unwrap(),
            range: ir::IndexRange::All,
        };
        let atoms = AccessFilterIndexAtoms::new(vec![atom]).unwrap();
        assert_eq!(atoms.as_ref().len(), 1);
        assert_eq!(
            AccessFilterIndexBranches::new(vec![atoms.clone()]),
            Err(TooFewIndexBranches)
        );
        assert_eq!(
            AccessFilterIndexBranches::new(vec![atoms.clone(), atoms])
                .unwrap()
                .as_ref()
                .len(),
            2
        );
    }

    #[test]
    fn atom_and_branch_wrappers_preserve_cardinality_through_fallible_maps() {
        let atom = AccessFilterIndexAtom::Equality {
            property: ir::NonEmptyString::new("age").unwrap(),
            domain: AccessEqualityDomain::One(ir::IndexValue::Param(
                ir::NonEmptyString::new("age").unwrap(),
            )),
        };
        let atoms = AccessFilterIndexAtoms::new(vec![atom]).unwrap();
        let branches = AccessFilterIndexBranches::new(vec![atoms.clone(), atoms.clone()]).unwrap();

        let mapped_atoms = atoms
            .try_map_ref(|atom| match atom {
                AccessFilterIndexAtom::Equality { property, .. }
                | AccessFilterIndexAtom::Range { property, .. } => Ok::<_, ()>(property.clone()),
            })
            .unwrap();
        assert_eq!(mapped_atoms.as_ref().len(), 1);

        let mapped_branches = branches
            .try_map_ref(|branch| {
                branch.try_map_ref(|atom| match atom {
                    AccessFilterIndexAtom::Equality { property, .. }
                    | AccessFilterIndexAtom::Range { property, .. } => {
                        Ok::<_, ()>(property.clone())
                    }
                })
            })
            .unwrap();
        assert_eq!(mapped_branches.as_ref().len(), 2);
        assert_eq!(mapped_branches.as_ref()[0].as_ref().len(), 1);
    }
}
