//! Projection of stored graph properties into current text-index documents.
//!
//! The projection retains both the deployed physical index name and a complete
//! v1-encoded logical partition identity. Active text maintenance uses the
//! former; hidden lifecycle maintenance uses both to detect partition routing
//! contradictions before persisting a coalesced build delta.

use crate::config::{TextElementType, TextIndexDefinition};
use crate::encoding::property::property_value::PropertyValue as DbPropertyValue;
use crate::encoding::property::Property;
use crate::error::Result;
use crate::search;

use super::super::properties;

#[derive(Debug, Clone, PartialEq)]
pub(super) struct TextIndexedDocument {
    pub(super) index_name: String,
    pub(super) input: search::text::TextDocumentInput,
    pub(super) logical_partition_identity: Vec<u8>,
}

impl TextIndexedDocument {
    pub(super) fn same_indexed_content(&self, other: &Self) -> bool {
        self.index_name == other.index_name
            && self.logical_partition_identity == other.logical_partition_identity
            && self.input.text == other.input.text
    }

    pub(super) fn map_index_name(self, map: impl FnOnce(&str) -> String) -> Self {
        Self {
            index_name: map(&self.index_name),
            input: self.input,
            logical_partition_identity: self.logical_partition_identity,
        }
    }

    /// Returns complete v1-derived bytes for hidden partition collision checks.
    #[cfg(test)]
    pub(super) fn logical_partition_identity(&self) -> &[u8] {
        &self.logical_partition_identity
    }
}

pub(super) fn text_definition_matches(
    definition: &TextIndexDefinition,
    element_type: TextElementType,
    old_properties: &[Property],
    new_properties: &[Property],
    changed_property: Option<&str>,
) -> bool {
    definition.element_type() == element_type
        && (properties::label_value(old_properties) == Some(definition.label())
            || properties::label_value(new_properties) == Some(definition.label()))
        && changed_property
            .map(|property| {
                property == "$label"
                    || property == definition.property()
                    || definition.tenant_property() == Some(property)
            })
            .unwrap_or(true)
}

pub(super) fn text_document_for_definition(
    definition: &TextIndexDefinition,
    element_type: TextElementType,
    entity_id: u64,
    properties: &[Property],
) -> Result<Option<TextIndexedDocument>> {
    debug_assert_eq!(definition.element_type(), element_type);
    if properties::label_value(properties) != Some(definition.label()) {
        return Ok(None);
    }

    let Some(value) = properties::property_value(properties, definition.property()) else {
        return Ok(None);
    };
    let Some(text) = search::text::normalize_indexed_text_value(value)? else {
        return Ok(None);
    };
    let tenant_value = text_tenant_value_for_definition(definition, properties)?;
    if definition.tenant_property().is_some() && tenant_value.is_none() {
        return Ok(None);
    }
    let index_name = search::text::resolve_physical_index_name(definition, tenant_value.as_ref())?;
    Ok(Some(TextIndexedDocument {
        index_name,
        input: search::text::TextDocumentInput::new(entity_id, text),
        logical_partition_identity: text_logical_partition_identity(tenant_value.as_ref()),
    }))
}

/// Frames unpartitioned or exact tenant values using the existing v1 property codec.
fn text_logical_partition_identity(tenant_value: Option<&DbPropertyValue>) -> Vec<u8> {
    let Some(tenant_value) = tenant_value else {
        return vec![0];
    };
    let encoded = crate::encoding::property::encode_properties(&[Property::new(
        "$text_tenant",
        tenant_value.clone(),
    )]);
    let mut identity = Vec::with_capacity(
        core::mem::size_of::<u8>()
            .checked_add(encoded.len())
            .expect("one property encoding fits addressable memory"),
    );
    identity.push(1);
    identity.extend_from_slice(&encoded);
    identity
}

fn text_tenant_value_for_definition(
    definition: &TextIndexDefinition,
    properties: &[Property],
) -> Result<Option<DbPropertyValue>> {
    let Some(tenant_property) = definition.tenant_property() else {
        return Ok(None);
    };
    Ok(properties::property_value(properties, tenant_property)
        .and_then(search::text::normalize_tenant_value)
        .cloned())
}

#[cfg(test)]
fn property(name: &str, value: DbPropertyValue) -> Property {
    Property::new(name, value)
}

#[cfg(test)]
fn text_definition() -> TextIndexDefinition {
    TextIndexDefinition::new_node("Doc", "body").expect("valid text index definition")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn definition_match_respects_element_label_property_and_tenant_property() {
        let definition = text_definition()
            .with_tenant_property("tenant_id")
            .expect("valid tenant property");
        let old_properties = vec![
            property("$label", DbPropertyValue::String("Doc".to_string())),
            property("tenant_id", DbPropertyValue::String("acme".to_string())),
            property("body", DbPropertyValue::String("before".to_string())),
        ];
        let new_properties = vec![
            property("$label", DbPropertyValue::String("Doc".to_string())),
            property("tenant_id", DbPropertyValue::String("acme".to_string())),
            property("body", DbPropertyValue::String("after".to_string())),
        ];

        assert!(text_definition_matches(
            &definition,
            TextElementType::Node,
            &old_properties,
            &new_properties,
            Some("body"),
        ));
        assert!(text_definition_matches(
            &definition,
            TextElementType::Node,
            &old_properties,
            &new_properties,
            Some("tenant_id"),
        ));
        assert!(text_definition_matches(
            &definition,
            TextElementType::Node,
            &old_properties,
            &new_properties,
            Some("$label"),
        ));
        assert!(!text_definition_matches(
            &definition,
            TextElementType::Edge,
            &old_properties,
            &new_properties,
            Some("body"),
        ));
        assert!(!text_definition_matches(
            &definition,
            TextElementType::Node,
            &old_properties,
            &new_properties,
            Some("title"),
        ));
    }

    #[test]
    fn document_for_definition_validates_label_text_and_tenant_partition() {
        let definition = text_definition()
            .with_tenant_property("tenant_id")
            .expect("valid tenant property");
        let properties = vec![
            property("$label", DbPropertyValue::String("Doc".to_string())),
            property("tenant_id", DbPropertyValue::String("acme".to_string())),
            property("body", DbPropertyValue::String("planner text".to_string())),
        ];

        let document =
            text_document_for_definition(&definition, TextElementType::Node, 42, &properties)
                .expect("document parses")
                .expect("document matches");

        assert_eq!(document.input.entity_id, 42);
        assert_eq!(document.input.text, "planner text");
        assert!(!document.logical_partition_identity().is_empty());
        assert_eq!(
            document.index_name,
            search::text_tenant_index_name(
                TextElementType::Node,
                "Doc",
                "body",
                "tenant_id",
                &DbPropertyValue::String("acme".to_string()),
            )
        );

        let other_tenant = vec![
            property("$label", DbPropertyValue::String("Doc".to_string())),
            property(
                "tenant_id",
                DbPropertyValue::String("different".to_string()),
            ),
            property("body", DbPropertyValue::String("planner text".to_string())),
        ];
        let other_document =
            text_document_for_definition(&definition, TextElementType::Node, 42, &other_tenant)
                .unwrap()
                .unwrap();
        assert_ne!(
            document.logical_partition_identity(),
            other_document.logical_partition_identity()
        );

        let missing_tenant = vec![
            property("$label", DbPropertyValue::String("Doc".to_string())),
            property("body", DbPropertyValue::String("planner text".to_string())),
        ];
        assert_eq!(
            text_document_for_definition(&definition, TextElementType::Node, 42, &missing_tenant,)
                .expect("missing tenant skips document"),
            None
        );

        let non_text_value = vec![
            property("$label", DbPropertyValue::String("Doc".to_string())),
            property("tenant_id", DbPropertyValue::String("acme".to_string())),
            property("body", DbPropertyValue::I64(7)),
        ];
        assert!(text_document_for_definition(
            &definition,
            TextElementType::Node,
            42,
            &non_text_value
        )
        .unwrap_err()
        .to_string()
        .contains("only support String and StringArray"));
    }
}
