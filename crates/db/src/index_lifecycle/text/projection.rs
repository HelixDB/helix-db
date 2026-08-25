//! Closed projection from authoritative graph properties to text-index source state.

use crate::encoding::v2::values::property::property_value::PropertyValue;
use crate::encoding::v2::values::property::Property;
use crate::index_lifecycle::{work, ValidatedTextIndexDefinition};

/// One label/property membership decision before indexed-value validation.
///
/// A candidate can still fail text or tenant validation. Keeping that state
/// distinct from definite absence lets mutation admission skip only entities
/// that cannot contribute to the index while preserving fail-closed behavior
/// for malformed indexed documents.
pub(super) enum TextSourceCandidate<'a> {
    /// The label does not match or the indexed property is absent.
    NotIndexed,
    /// The indexed property is present and still requires complete validation.
    Candidate(&'a Property),
}

/// One definition's complete source projection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum TextSourceProjection {
    /// The label does not match or the indexed property is absent.
    NotIndexed,
    /// Present indexed text with its validated partition.
    Indexed {
        partition: work::TextPartition,
        text: String,
    },
}

/// A present source document that cannot satisfy its index definition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(super) enum TextSourceProjectionError {
    #[error("the indexed property has an unsupported value")]
    UnsupportedTextValue,
    #[error("the indexed document is missing its tenant property")]
    MissingTenant,
    #[error("the indexed document has a null tenant property")]
    NullTenant,
    #[error("the indexed document tenant partition exceeds the storage limit")]
    OversizedTenant,
}

/// Projects one canonical graph row without representing invalid data as absence.
pub(super) fn project(
    definition: &ValidatedTextIndexDefinition,
    properties: &[Property],
) -> Result<TextSourceProjection, TextSourceProjectionError> {
    let TextSourceCandidate::Candidate(indexed_property) = source_candidate(definition, properties)
    else {
        return Ok(TextSourceProjection::NotIndexed);
    };
    let text = crate::search::text::normalize_indexed_text_value(&indexed_property.value)
        .map_err(|_| TextSourceProjectionError::UnsupportedTextValue)?;
    let partition = match definition.tenant_property() {
        None => work::TextPartition::Unpartitioned,
        Some(tenant_property) => {
            let Some(tenant_property) = properties
                .iter()
                .find(|property| property.name == tenant_property.as_str())
            else {
                return Err(TextSourceProjectionError::MissingTenant);
            };
            if matches!(tenant_property.value, PropertyValue::Null) {
                return Err(TextSourceProjectionError::NullTenant);
            }
            let encoded = crate::encoding::v2::values::property::encode_index_partition_value(
                &tenant_property.value,
            );
            work::TextPartition::try_tenant_value(encoded)
                .map_err(|_| TextSourceProjectionError::OversizedTenant)?
        }
    };
    Ok(TextSourceProjection::Indexed { partition, text })
}

/// Classifies only definite index absence without validating candidate data.
pub(super) fn source_candidate<'a>(
    definition: &ValidatedTextIndexDefinition,
    properties: &'a [Property],
) -> TextSourceCandidate<'a> {
    let label_matches = properties.iter().any(|property| {
        property.name == "$label" && property.value.as_str() == Some(definition.label().as_str())
    });
    if !label_matches {
        return TextSourceCandidate::NotIndexed;
    }
    let Some(indexed_property) = properties
        .iter()
        .find(|property| property.name == definition.property().as_str())
    else {
        return TextSourceCandidate::NotIndexed;
    };
    TextSourceCandidate::Candidate(indexed_property)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::TextAnalyzerKind;
    use crate::index_lifecycle::{IndexElementKind, ValidatedTextIndexDefinition};

    fn definition() -> ValidatedTextIndexDefinition {
        ValidatedTextIndexDefinition::try_new(
            IndexElementKind::Node,
            "Document",
            "body",
            Some("tenant"),
            TextAnalyzerKind::Standard,
            false,
        )
        .unwrap()
    }

    #[test]
    fn only_absent_membership_projects_to_not_indexed() {
        assert_eq!(
            project(
                &definition(),
                &[
                    Property::string("$label", "Another"),
                    Property::string("body", "x")
                ]
            )
            .unwrap(),
            TextSourceProjection::NotIndexed
        );
        assert_eq!(
            project(&definition(), &[Property::string("$label", "Document")]).unwrap(),
            TextSourceProjection::NotIndexed
        );
    }

    #[test]
    fn present_text_requires_a_non_null_bounded_tenant() {
        let source = [
            Property::string("$label", "Document"),
            Property::string("body", "x"),
        ];
        assert_eq!(
            project(&definition(), &source),
            Err(TextSourceProjectionError::MissingTenant)
        );
        let mut null = source.to_vec();
        null.push(Property::new("tenant", PropertyValue::Null));
        assert_eq!(
            project(&definition(), &null),
            Err(TextSourceProjectionError::NullTenant)
        );
    }
}
