//! Typed candidate indices into the compiled optimizer registry.

/// Dense position of a rule inside the compiled optimizer registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(in crate::optimizer::driver::schedule) struct RuleIndex {
    position: usize,
}

impl RuleIndex {
    pub(in crate::optimizer::driver::schedule) const fn from_registry_position(
        position: usize,
        registry_len: usize,
    ) -> Option<Self> {
        if position < registry_len {
            Some(Self { position })
        } else {
            None
        }
    }

    #[cfg(test)]
    pub(in crate::optimizer::driver::schedule) const fn from_test_registry_position(
        position: usize,
        registry_len: usize,
    ) -> Option<Self> {
        Self::from_registry_position(position, registry_len)
    }

    pub(in crate::optimizer::driver::schedule) fn from_enumerated_registry_position(
        position: usize,
        registry_len: usize,
    ) -> Self {
        Self::from_registry_position(position, registry_len)
            .expect("enumerated rule index must stay inside the compiled registry")
    }

    pub(super) const fn position(self) -> usize {
        self.position
    }
}
