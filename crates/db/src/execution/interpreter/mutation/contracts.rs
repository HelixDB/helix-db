//! Mutation interpreter helper contracts.
//!
//! These helpers keep storage-key decoding, label/property invariants, and edge
//! mutation identity separate from the high-level executable mutation dispatch.

use super::*;
use bytes::Bytes;

#[derive(Debug, Clone, Copy)]
pub(super) struct EdgeMutationTarget {
    pub(super) edge_id: u64,
    pub(super) from: u64,
    pub(super) to: u64,
}

impl EdgeMutationTarget {
    pub(super) const fn new(edge_id: u64, from: u64, to: u64) -> Self {
        Self { edge_id, from, to }
    }
}

pub(super) fn decode_stored_edges(value: Option<Bytes>) -> Result<values::adjacency::Edges> {
    match value {
        Some(value) => Ok(values::adjacency::decode_edges(&value)?),
        None => Ok(values::adjacency::Edges::new()),
    }
}

pub(super) fn label_of(properties: &[Property]) -> Option<&str> {
    properties
        .iter()
        .find(|property| property.name == "$label")
        .and_then(|property| property.value.as_str())
}

pub(super) fn reject_label_mutation(name: &ir::NonEmptyString) -> Result<()> {
    if name.as_ref() == "$label" {
        Err(HelixDbError::Query(
            "mutating `$label` directly is not supported by executable mutations".to_string(),
        ))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::super::test_support;
    use super::*;
    use bytes::Bytes;

    #[test]
    fn edge_mutation_target_preserves_endpoint_identity() {
        let target = EdgeMutationTarget::new(11, 22, 33);

        assert_eq!(target.edge_id, 11);
        assert_eq!(target.from, 22);
        assert_eq!(target.to, 33);
    }

    #[test]
    fn decode_stored_edges_handles_absent_encoded_and_invalid_payloads() {
        assert!(decode_stored_edges(None).unwrap().is_empty());

        let mut edges = values::adjacency::Edges::new();
        edges.add_out(7);
        edges.add_in(9);
        let encoded = values::adjacency::encode_edges(&edges);
        let decoded = decode_stored_edges(Some(encoded)).unwrap();
        assert!(decoded.contains_out(7));
        assert!(decoded.contains_in(9));

        assert!(decode_stored_edges(Some(Bytes::from_static(b"bad-edges"))).is_err());
    }

    #[test]
    fn label_of_reads_string_label_only() {
        assert_eq!(
            label_of(&[
                Property::i64("$label", 99),
                Property::string("name", "alice"),
            ]),
            None
        );
        assert_eq!(
            label_of(&[
                Property::string("name", "alice"),
                Property::string("$label", "User"),
            ]),
            Some("User")
        );
        assert_eq!(label_of(&[Property::string("name", "alice")]), None);
    }

    #[test]
    fn reject_label_mutation_blocks_direct_label_property_updates() {
        reject_label_mutation(&test_support::name("name")).unwrap();

        let err = reject_label_mutation(&test_support::name("$label"))
            .expect_err("direct label mutation should fail");

        assert!(err
            .to_string()
            .contains("mutating `$label` directly is not supported"));
    }
}
