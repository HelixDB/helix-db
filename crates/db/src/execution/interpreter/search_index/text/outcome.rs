use std::collections::BTreeSet;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(in crate::execution::interpreter) struct TextIndexMaintenanceOutcome {
    touched_indexes: BTreeSet<String>,
}

impl TextIndexMaintenanceOutcome {
    pub(super) fn is_empty(&self) -> bool {
        self.touched_indexes.is_empty()
    }

    pub(super) fn indexes(&self) -> impl Iterator<Item = &str> {
        self.touched_indexes.iter().map(String::as_str)
    }

    pub(super) fn record(&mut self, index_name: impl Into<String>) {
        self.touched_indexes.insert(index_name.into());
    }

    pub(in crate::execution::interpreter) fn merge_into(self, target: &mut Self) {
        target.touched_indexes.extend(self.touched_indexes);
    }
}
