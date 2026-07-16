//! Mutation interpreter helper contracts.
//!
//! These helpers keep storage-key decoding, label/property invariants, and edge
//! mutation identity separate from the high-level executable mutation dispatch.

use bytes::Bytes;
use slatedb::DbTransaction;

use super::*;
use crate::config;
use crate::encoding::property;

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

pub(super) async fn remove_edge_property_indexes(
    txn: &DbTransaction,
    edge: EdgeMutationTarget,
    label: Option<&str>,
    name: &str,
    value: &DbPropertyValue,
    indexes: &crate::config::RuntimeIndexCatalog,
    tenant_scope: crate::encoding::keys::tenant::DataScope,
) -> Result<()> {
    let Some(scoped) = secondary_index_property_key(label, name) else {
        return Ok(());
    };
    let value = crate::search::property_value_to_index_string(value);
    if indexes.contains_edge_equality_scoped(&scoped) {
        crate::search::remove_from_edge_equality_index_scoped(
            txn,
            edge.from,
            edge.to,
            edge.edge_id,
            &scoped,
            &value,
            tenant_scope,
        )
        .await?;
    }
    if indexes.contains_edge_range_scoped(&scoped) {
        crate::search::remove_from_edge_range_index_with_direction_scoped(
            txn,
            edge.from,
            edge.to,
            edge.edge_id,
            &scoped,
            &value,
            crate::encoding::indexes::range::RangeIndexDirection::Asc,
            tenant_scope,
        )
        .await?;
    }
    if indexes.contains_edge_range_desc_scoped(&scoped) {
        crate::search::remove_from_edge_range_index_with_direction_scoped(
            txn,
            edge.from,
            edge.to,
            edge.edge_id,
            &scoped,
            &value,
            crate::encoding::indexes::range::RangeIndexDirection::Desc,
            tenant_scope,
        )
        .await?;
    }
    Ok(())
}

pub(super) fn decode_stored_properties(value: Option<Bytes>) -> Result<Vec<Property>> {
    match value {
        Some(value) => Ok(property::decode_properties(&value)?),
        None => Ok(Vec::new()),
    }
}

pub(super) fn decode_stored_edges(value: Option<Bytes>) -> Result<values::edges::Edges> {
    match value {
        Some(value) => Ok(values::edges::decode_edges(&value)?),
        None => Ok(values::edges::Edges::new()),
    }
}

pub(super) fn label_of(properties: &[Property]) -> Option<&str> {
    properties
        .iter()
        .find(|property| property.name == "$label")
        .and_then(|property| property.value.as_str())
}

pub(super) fn upsert_property(
    properties: &mut Vec<Property>,
    property: Property,
) -> Option<Property> {
    if let Some(existing) = properties
        .iter_mut()
        .find(|existing| existing.name == property.name)
    {
        return Some(std::mem::replace(existing, property));
    }
    properties.push(property);
    None
}

pub(super) fn remove_property_by_name(
    properties: &mut Vec<Property>,
    name: &str,
) -> Option<Property> {
    properties
        .iter()
        .position(|property| property.name == name)
        .map(|position| properties.remove(position))
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

fn secondary_index_property_key(label: Option<&str>, property: &str) -> Option<String> {
    if property == "$label" {
        return Some(property.to_string());
    }
    label.map(|label| config::scoped_secondary_index_property(label, property))
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use helix_ast::value::PropertyValue as AstPropertyValue;
    use slatedb::IsolationLevel;

    use super::super::super::test_support;
    use super::*;

    fn property_names(properties: &[Property]) -> Vec<&str> {
        properties
            .iter()
            .map(|property| property.name.as_str())
            .collect()
    }

    #[test]
    fn edge_mutation_target_preserves_endpoint_identity() {
        let target = EdgeMutationTarget::new(11, 22, 33);

        assert_eq!(target.edge_id, 11);
        assert_eq!(target.from, 22);
        assert_eq!(target.to, 33);
    }

    #[test]
    fn decode_stored_properties_handles_absent_encoded_and_invalid_payloads() {
        assert_eq!(decode_stored_properties(None).unwrap(), Vec::new());

        let properties = vec![Property::string("$label", "User"), Property::i64("age", 42)];
        let encoded = property::encode_properties(&properties);
        assert_eq!(decode_stored_properties(Some(encoded)).unwrap(), properties);

        assert!(decode_stored_properties(Some(Bytes::from_static(b"not-rkyv"))).is_err());
    }

    #[test]
    fn decode_stored_edges_handles_absent_encoded_and_invalid_payloads() {
        assert!(decode_stored_edges(None).unwrap().is_empty());

        let mut edges = values::edges::Edges::new();
        edges.add_out(7);
        edges.add_in(9);
        let encoded = values::edges::encode_edges(&edges);
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
    fn upsert_property_replaces_existing_property_or_appends_new_property() {
        let mut properties = vec![
            Property::string("$label", "User"),
            Property::string("name", "alice"),
        ];

        let previous = upsert_property(&mut properties, Property::string("name", "ada"))
            .expect("existing property is replaced");
        assert_eq!(previous, Property::string("name", "alice"));
        assert_eq!(property_names(&properties), vec!["$label", "name"]);
        assert_eq!(properties[1], Property::string("name", "ada"));

        assert!(upsert_property(&mut properties, Property::i64("age", 42)).is_none());
        assert_eq!(property_names(&properties), vec!["$label", "name", "age"]);
    }

    #[test]
    fn remove_property_by_name_removes_first_match_and_preserves_order() {
        let mut properties = vec![
            Property::string("$label", "User"),
            Property::string("name", "alice"),
            Property::string("name", "duplicate"),
            Property::i64("age", 42),
        ];

        let removed = remove_property_by_name(&mut properties, "name")
            .expect("first matching property is removed");
        assert_eq!(removed, Property::string("name", "alice"));
        assert_eq!(property_names(&properties), vec!["$label", "name", "age"]);
        assert_eq!(properties[1], Property::string("name", "duplicate"));
        assert!(remove_property_by_name(&mut properties, "missing").is_none());
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

    #[test]
    fn secondary_index_property_key_preserves_label_scope_contract() {
        assert_eq!(
            secondary_index_property_key(None, "$label"),
            Some("$label".to_string())
        );
        assert_eq!(secondary_index_property_key(None, "email"), None);
        assert_eq!(
            secondary_index_property_key(Some("User"), "email"),
            Some(config::scoped_secondary_index_property("User", "email"))
        );
    }

    #[tokio::test]
    async fn remove_edge_property_indexes_clears_every_configured_index_direction() {
        let config = test_support::in_memory_config("mutation-remove-edge-property-indexes")
            .with_edge_equality_index("FOLLOWS", "weight")
            .with_edge_range_index("FOLLOWS", "weight")
            .with_edge_range_desc_index("FOLLOWS", "weight");
        let db = test_support::open_db_with_config(config).await;
        let from = test_support::add_user(&db, "alice").await;
        let to = test_support::add_user(&db, "bob").await;
        let edge_id = test_support::add_edge_with_properties(
            &db,
            from,
            to,
            "FOLLOWS",
            vec![("weight", AstPropertyValue::I64(3))],
        )
        .await;
        let txn = db
            .inner_db()
            .begin(IsolationLevel::Snapshot)
            .await
            .expect("snapshot transaction begins");
        let indexes = db.runtime_config_snapshot_loaded(
            crate::encoding::keys::tenant::DataScope::LegacyUnscoped,
        );
        let scoped = config::scoped_secondary_index_property("FOLLOWS", "weight");
        let value = crate::search::property_value_to_index_string(&DbPropertyValue::I64(3));

        assert!(
            crate::search::lookup_edges_out_by_equality(&txn, from, &scoped, &value)
                .await
                .unwrap()
                .contains(edge_id)
        );
        for direction in [
            crate::encoding::indexes::range::RangeIndexDirection::Asc,
            crate::encoding::indexes::range::RangeIndexDirection::Desc,
        ] {
            assert_eq!(
                crate::search::scan_edge_range_index_out_with_direction(
                    &txn,
                    from,
                    &scoped,
                    crate::search::RangeQuery::Between(&value, &value),
                    direction,
                )
                .await
                .unwrap(),
                vec![edge_id]
            );
        }

        remove_edge_property_indexes(
            &txn,
            EdgeMutationTarget::new(edge_id, from, to),
            Some("FOLLOWS"),
            "weight",
            &DbPropertyValue::I64(3),
            &indexes,
            crate::encoding::keys::tenant::DataScope::LegacyUnscoped,
        )
        .await
        .expect("all configured edge property indexes are removed");
        remove_edge_property_indexes(
            &txn,
            EdgeMutationTarget::new(edge_id, from, to),
            None,
            "weight",
            &DbPropertyValue::I64(3),
            &indexes,
            crate::encoding::keys::tenant::DataScope::LegacyUnscoped,
        )
        .await
        .expect("unlabeled property removal is a no-op");

        assert!(
            crate::search::lookup_edges_out_by_equality(&txn, from, &scoped, &value)
                .await
                .unwrap()
                .is_empty()
        );
        for direction in [
            crate::encoding::indexes::range::RangeIndexDirection::Asc,
            crate::encoding::indexes::range::RangeIndexDirection::Desc,
        ] {
            assert!(crate::search::scan_edge_range_index_out_with_direction(
                &txn,
                from,
                &scoped,
                crate::search::RangeQuery::Between(&value, &value),
                direction,
            )
            .await
            .unwrap()
            .is_empty());
        }
    }
}
