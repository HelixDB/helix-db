//! Canonical conflict diagnostics for dynamic index definitions.
//!
//! The V2 lifecycle repository compares complete validated definitions inside
//! its enqueue transaction. This module names every incompatible field and
//! makes an empty conflict unrepresentable; it performs no storage work.

use std::collections::BTreeSet;
use std::fmt;

use crate::index_v2::{
    ValidatedDynamicIndexDefinition, ValidatedSecondaryIndexDefinition,
    ValidatedTextIndexDefinition, ValidatedVectorIndexDefinition,
};

/// Canonical field names that can make two dynamic index definitions conflict.
///
/// Declaration order is the stable diagnostic order used by
/// [`NonEmptyDefinitionDifferences::iter`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DefinitionDifference {
    /// Secondary, vector, or text family differs.
    IndexFamily,
    /// Node-versus-edge ownership differs.
    ElementType,
    /// Label scope differs.
    Label,
    /// Indexed property differs.
    Property,
    /// Equality-versus-range secondary behavior differs.
    SecondaryKind,
    /// Secondary uniqueness differs.
    Uniqueness,
    /// Secondary range ordering differs.
    Direction,
    /// Optional tenant partition property differs.
    TenantProperty,
    /// Vector dimension differs.
    VectorDimension,
    /// Vector metric differs.
    VectorMetric,
    /// HNSW maximum connections differs.
    VectorConnections,
    /// HNSW layer-zero maximum connections differs.
    VectorLayer0Connections,
    /// HNSW construction beam width differs.
    VectorConstructionBeamWidth,
    /// HNSW layer multiplier differs.
    VectorLayerMultiplier,
    /// SimHash threshold differs.
    VectorSimHashThreshold,
    /// Vector sampling ratio differs.
    VectorSamplingRatio,
    /// Adaptive vector traversal enablement differs.
    VectorAdaptiveEnabled,
    /// Adaptive vector traversal failure probability differs.
    VectorAdaptiveFailureProbability,
    /// Text analyzer differs.
    TextAnalyzer,
    /// Text position recording differs.
    TextPositionsEnabled,
}

impl DefinitionDifference {
    /// Returns the stable field label used in structured conflict diagnostics.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::IndexFamily => "index_family",
            Self::ElementType => "element_type",
            Self::Label => "label",
            Self::Property => "property",
            Self::SecondaryKind => "secondary_kind",
            Self::Uniqueness => "uniqueness",
            Self::Direction => "direction",
            Self::TenantProperty => "tenant_property",
            Self::VectorDimension => "vector_dimension",
            Self::VectorMetric => "vector_metric",
            Self::VectorConnections => "vector_connections",
            Self::VectorLayer0Connections => "vector_layer0_connections",
            Self::VectorConstructionBeamWidth => "vector_construction_beam_width",
            Self::VectorLayerMultiplier => "vector_layer_multiplier",
            Self::VectorSimHashThreshold => "vector_simhash_threshold",
            Self::VectorSamplingRatio => "vector_sampling_ratio",
            Self::VectorAdaptiveEnabled => "vector_adaptive_enabled",
            Self::VectorAdaptiveFailureProbability => "vector_adaptive_failure_probability",
            Self::TextAnalyzer => "text_analyzer",
            Self::TextPositionsEnabled => "text_positions_enabled",
        }
    }
}

/// Canonical, non-empty set of conflicting definition fields.
///
/// Fields are private and construction is available only through a comparison
/// that found at least one difference. Iteration is sorted and duplicate-free.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NonEmptyDefinitionDifferences(BTreeSet<DefinitionDifference>);

impl NonEmptyDefinitionDifferences {
    /// Compares two definitions and returns `None` only when they are identical.
    pub(crate) fn between(
        existing: &ValidatedDynamicIndexDefinition,
        requested: &ValidatedDynamicIndexDefinition,
    ) -> Option<Self> {
        let mut differences = BTreeSet::new();
        match (existing, requested) {
            (
                ValidatedDynamicIndexDefinition::Secondary(existing),
                ValidatedDynamicIndexDefinition::Secondary(requested),
            ) => compare_secondary(existing, requested, &mut differences),
            (
                ValidatedDynamicIndexDefinition::Vector(existing),
                ValidatedDynamicIndexDefinition::Vector(requested),
            ) => compare_vector(existing, requested, &mut differences),
            (
                ValidatedDynamicIndexDefinition::Text(existing),
                ValidatedDynamicIndexDefinition::Text(requested),
            ) => compare_text(existing, requested, &mut differences),
            _ => {
                differences.insert(DefinitionDifference::IndexFamily);
            }
        }
        (!differences.is_empty()).then_some(Self(differences))
    }

    /// Iterates conflicting fields in canonical diagnostic order.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = DefinitionDifference> + '_ {
        self.0.iter().copied()
    }

    /// Returns the number of distinct conflicting fields.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.0.len()
    }
}

impl fmt::Display for NonEmptyDefinitionDifferences {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut fields = self.iter();
        let Some(first) = fields.next() else {
            unreachable!("non-empty definition differences always contain a field")
        };
        formatter.write_str(first.as_str())?;
        for field in fields {
            formatter.write_str(", ")?;
            formatter.write_str(field.as_str())?;
        }
        Ok(())
    }
}

fn compare_secondary(
    existing: &ValidatedSecondaryIndexDefinition,
    requested: &ValidatedSecondaryIndexDefinition,
    differences: &mut BTreeSet<DefinitionDifference>,
) {
    if existing.element_kind() != requested.element_kind() {
        differences.insert(DefinitionDifference::ElementType);
    }
    if existing.label() != requested.label() {
        differences.insert(DefinitionDifference::Label);
    }
    if existing.property() != requested.property() {
        differences.insert(DefinitionDifference::Property);
    }
    if existing.identity_family() != requested.identity_family() {
        differences.insert(DefinitionDifference::SecondaryKind);
    }
    if existing.unique() != requested.unique() {
        differences.insert(DefinitionDifference::Uniqueness);
    }
    if existing.direction() != requested.direction() {
        differences.insert(DefinitionDifference::Direction);
    }
}

fn compare_vector(
    existing: &ValidatedVectorIndexDefinition,
    requested: &ValidatedVectorIndexDefinition,
    differences: &mut BTreeSet<DefinitionDifference>,
) {
    if existing.element_kind() != requested.element_kind() {
        differences.insert(DefinitionDifference::ElementType);
    }
    if existing.label() != requested.label() {
        differences.insert(DefinitionDifference::Label);
    }
    if existing.property() != requested.property() {
        differences.insert(DefinitionDifference::Property);
    }
    if existing.tenant_property() != requested.tenant_property() {
        differences.insert(DefinitionDifference::TenantProperty);
    }
    if existing.dimension() != requested.dimension() {
        differences.insert(DefinitionDifference::VectorDimension);
    }
    if existing.metric() != requested.metric() {
        differences.insert(DefinitionDifference::VectorMetric);
    }
    if existing.m() != requested.m() {
        differences.insert(DefinitionDifference::VectorConnections);
    }
    if existing.m0() != requested.m0() {
        differences.insert(DefinitionDifference::VectorLayer0Connections);
    }
    if existing.ef_construction() != requested.ef_construction() {
        differences.insert(DefinitionDifference::VectorConstructionBeamWidth);
    }
    if existing.ml().to_bits() != requested.ml().to_bits() {
        differences.insert(DefinitionDifference::VectorLayerMultiplier);
    }
    if existing.simhash_threshold() != requested.simhash_threshold() {
        differences.insert(DefinitionDifference::VectorSimHashThreshold);
    }
    if existing.sampling_ratio().to_bits() != requested.sampling_ratio().to_bits() {
        differences.insert(DefinitionDifference::VectorSamplingRatio);
    }
    if existing.adaptive_enabled() != requested.adaptive_enabled() {
        differences.insert(DefinitionDifference::VectorAdaptiveEnabled);
    }
    if existing.adaptive_failure_probability().to_bits()
        != requested.adaptive_failure_probability().to_bits()
    {
        differences.insert(DefinitionDifference::VectorAdaptiveFailureProbability);
    }
}

fn compare_text(
    existing: &ValidatedTextIndexDefinition,
    requested: &ValidatedTextIndexDefinition,
    differences: &mut BTreeSet<DefinitionDifference>,
) {
    if existing.element_kind() != requested.element_kind() {
        differences.insert(DefinitionDifference::ElementType);
    }
    if existing.label() != requested.label() {
        differences.insert(DefinitionDifference::Label);
    }
    if existing.property() != requested.property() {
        differences.insert(DefinitionDifference::Property);
    }
    if existing.tenant_property() != requested.tenant_property() {
        differences.insert(DefinitionDifference::TenantProperty);
    }
    if existing.analyzer() != requested.analyzer() {
        differences.insert(DefinitionDifference::TextAnalyzer);
    }
    if existing.positions_enabled() != requested.positions_enabled() {
        differences.insert(DefinitionDifference::TextPositionsEnabled);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        RangeIndexDirection, SecondaryIndexDefinition, TextAnalyzerKind, TextIndexDefinition,
        VectorIndexDefinition,
    };
    use crate::search::vector::VectorDistanceMetric;

    fn vector() -> ValidatedDynamicIndexDefinition {
        ValidatedDynamicIndexDefinition::try_from(
            VectorIndexDefinition::new_node(
                "Document",
                "embedding",
                3,
                VectorDistanceMetric::Cosine,
            )
            .unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn secondary_differences_are_canonical_and_non_empty() {
        let existing = ValidatedDynamicIndexDefinition::try_from(
            SecondaryIndexDefinition::node_unique_equality("User", "age").unwrap(),
        )
        .unwrap();
        let requested = ValidatedDynamicIndexDefinition::try_from(
            SecondaryIndexDefinition::node_range_with_direction(
                "User",
                "age",
                RangeIndexDirection::Desc,
            )
            .unwrap(),
        )
        .unwrap();
        let differences = NonEmptyDefinitionDifferences::between(&existing, &requested).unwrap();
        assert_eq!(differences.len(), 3);
        assert_eq!(
            differences.iter().collect::<Vec<_>>(),
            vec![
                DefinitionDifference::SecondaryKind,
                DefinitionDifference::Uniqueness,
                DefinitionDifference::Direction,
            ]
        );
        assert_eq!(
            differences.to_string(),
            "secondary_kind, uniqueness, direction"
        );
        assert!(NonEmptyDefinitionDifferences::between(&existing, &existing).is_none());
    }

    #[test]
    fn text_and_vector_compare_every_physical_setting() {
        let text = TextIndexDefinition::new_node("Document", "body").unwrap();
        let changed_text = TextIndexDefinition::new_node("Document", "body")
            .unwrap()
            .with_tenant_property("tenant")
            .unwrap()
            .with_analyzer(TextAnalyzerKind::StandardStemEn)
            .with_positions_enabled(true);
        let text_differences = NonEmptyDefinitionDifferences::between(
            &ValidatedDynamicIndexDefinition::try_from(text).unwrap(),
            &ValidatedDynamicIndexDefinition::try_from(changed_text).unwrap(),
        )
        .unwrap();
        assert_eq!(
            text_differences.iter().collect::<Vec<_>>(),
            vec![
                DefinitionDifference::TenantProperty,
                DefinitionDifference::TextAnalyzer,
                DefinitionDifference::TextPositionsEnabled,
            ]
        );

        let vector = VectorIndexDefinition::new_edge(
            "Reference",
            "embedding",
            3,
            VectorDistanceMetric::Cosine,
        )
        .unwrap();
        let changed_vector = VectorIndexDefinition::new_edge(
            "Reference",
            "embedding",
            3,
            VectorDistanceMetric::Cosine,
        )
        .unwrap()
        .with_tenant_property("tenant")
        .unwrap()
        .with_m(12)
        .unwrap()
        .with_m0(30)
        .unwrap()
        .with_ef_construction(150)
        .unwrap()
        .with_ml(0.25)
        .unwrap()
        .with_simhash_threshold(31)
        .unwrap()
        .with_sampling_ratio(0.5)
        .unwrap()
        .with_adaptive_enabled(false)
        .with_adaptive_failure_prob(0.2)
        .unwrap();
        let vector_differences = NonEmptyDefinitionDifferences::between(
            &ValidatedDynamicIndexDefinition::try_from(vector).unwrap(),
            &ValidatedDynamicIndexDefinition::try_from(changed_vector).unwrap(),
        )
        .unwrap();
        assert_eq!(vector_differences.len(), 9);
    }

    #[test]
    fn cross_family_comparison_has_an_explicit_difference() {
        let vector = vector();
        let text = ValidatedDynamicIndexDefinition::try_from(
            TextIndexDefinition::new_node("Document", "embedding").unwrap(),
        )
        .unwrap();
        let differences = NonEmptyDefinitionDifferences::between(&vector, &text).unwrap();
        assert_eq!(
            differences.iter().collect::<Vec<_>>(),
            vec![DefinitionDifference::IndexFamily]
        );
    }
}
