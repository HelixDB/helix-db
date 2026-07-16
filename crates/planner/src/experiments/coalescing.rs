//! MultiGet coalescing benchmark input contracts.

use crate::{exec, properties};

/// Default coalescing keys used by the Criterion fixture.
pub fn coalescing_keys(count: properties::PositiveUsize) -> Vec<exec::KvKey> {
    (0..count.get())
        .rev()
        .map(|key| exec::ElementKeyspace::NodeProperty.point_key(key as u64))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coalescing_keys_are_reverse_node_property_point_keys() {
        let keys = coalescing_keys(properties::PositiveUsize::new(3).unwrap());

        assert_eq!(
            keys.iter().map(exec::KvKey::id).collect::<Vec<_>>(),
            vec![2, 1, 0]
        );
        assert!(keys
            .iter()
            .all(|key| key.keyspace() == exec::ElementKeyspace::NodeProperty));
    }
}
