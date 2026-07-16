//! Runtime index-catalog conversion for executable DDL.

use helix_ast::index::RangeIndexDirection;
use helix_planner::{catalog, ir};

use crate::error::{HelixDbError, Result};
use crate::{config, index_v2};

pub(crate) fn dynamic_index_definition_from_create_spec(
    spec: &ir::IndexDdlCreateSpec,
) -> Result<crate::index_v2::ValidatedDynamicIndexDefinition> {
    match spec {
        ir::IndexDdlCreateSpec::NodeEquality { key, uniqueness } => {
            let definition = match uniqueness {
                catalog::IndexUniqueness::Unique => {
                    config::SecondaryIndexDefinition::node_unique_equality(
                        key.label.as_ref(),
                        key.property.as_ref(),
                    )
                }
                catalog::IndexUniqueness::NonUnique => {
                    config::SecondaryIndexDefinition::node_equality(
                        key.label.as_ref(),
                        key.property.as_ref(),
                    )
                }
            }?;
            Ok(definition.try_into()?)
        }
        ir::IndexDdlCreateSpec::NodeRange { key } => {
            Ok(config::SecondaryIndexDefinition::node_range_with_direction(
                key.label.as_ref(),
                key.property.as_ref(),
                config_range_direction(key.direction),
            )?
            .try_into()?)
        }
        ir::IndexDdlCreateSpec::EdgeEquality { key } => {
            Ok(config::SecondaryIndexDefinition::edge_equality(
                key.label.as_ref(),
                key.property.as_ref(),
            )?
            .try_into()?)
        }
        ir::IndexDdlCreateSpec::EdgeRange { key } => {
            Ok(config::SecondaryIndexDefinition::edge_range_with_direction(
                key.label.as_ref(),
                key.property.as_ref(),
                config_range_direction(key.direction),
            )?
            .try_into()?)
        }
        ir::IndexDdlCreateSpec::NodeVector {
            key,
            dimension,
            metric,
            scope,
        } => Ok(vector_definition(
            config::VectorElementType::Node,
            key,
            *dimension,
            *metric,
            scope,
        )?
        .try_into()?),
        ir::IndexDdlCreateSpec::EdgeVector {
            key,
            dimension,
            metric,
            scope,
        } => Ok(vector_definition(
            config::VectorElementType::Edge,
            key,
            *dimension,
            *metric,
            scope,
        )?
        .try_into()?),
        ir::IndexDdlCreateSpec::NodeText { key, scope } => {
            Ok(text_definition(config::TextElementType::Node, key, scope)?.try_into()?)
        }
        ir::IndexDdlCreateSpec::EdgeText { key, scope } => {
            Ok(text_definition(config::TextElementType::Edge, key, scope)?.try_into()?)
        }
    }
}

/// Derives the canonical V2 identity named by a DROP without inventing settings.
///
/// Vector tuning and text analyzer settings are intentionally absent from DROP
/// syntax. This contract therefore constructs only the logical row identity;
/// callers must point-load the canonical record before resolving the complete
/// validated definition.
pub(crate) fn dynamic_index_identity_from_drop_spec(
    spec: &ir::IndexDdlDropSpec,
) -> Result<index_v2::IndexIdentity> {
    let (family, element_kind, label, property) = match spec {
        ir::IndexDdlDropSpec::NodeEquality { key, .. } => (
            index_v2::IndexIdentityFamily::SecondaryEquality,
            index_v2::IndexElementKind::Node,
            key.label.as_ref(),
            key.property.as_ref(),
        ),
        ir::IndexDdlDropSpec::NodeRange { key } => (
            index_v2::IndexIdentityFamily::SecondaryRange,
            index_v2::IndexElementKind::Node,
            key.label.as_ref(),
            key.property.as_ref(),
        ),
        ir::IndexDdlDropSpec::EdgeEquality { key } => (
            index_v2::IndexIdentityFamily::SecondaryEquality,
            index_v2::IndexElementKind::Edge,
            key.label.as_ref(),
            key.property.as_ref(),
        ),
        ir::IndexDdlDropSpec::EdgeRange { key } => (
            index_v2::IndexIdentityFamily::SecondaryRange,
            index_v2::IndexElementKind::Edge,
            key.label.as_ref(),
            key.property.as_ref(),
        ),
        ir::IndexDdlDropSpec::NodeVector { key } => (
            index_v2::IndexIdentityFamily::Vector,
            index_v2::IndexElementKind::Node,
            key.label.as_ref(),
            key.property.as_ref(),
        ),
        ir::IndexDdlDropSpec::EdgeVector { key } => (
            index_v2::IndexIdentityFamily::Vector,
            index_v2::IndexElementKind::Edge,
            key.label.as_ref(),
            key.property.as_ref(),
        ),
        ir::IndexDdlDropSpec::NodeText { key } => (
            index_v2::IndexIdentityFamily::Text,
            index_v2::IndexElementKind::Node,
            key.label.as_ref(),
            key.property.as_ref(),
        ),
        ir::IndexDdlDropSpec::EdgeText { key } => (
            index_v2::IndexIdentityFamily::Text,
            index_v2::IndexElementKind::Edge,
            key.label.as_ref(),
            key.property.as_ref(),
        ),
    };
    Ok(index_v2::IndexIdentity::new(
        family,
        element_kind,
        index_v2::IndexComponent::try_new("label", label)?,
        index_v2::IndexComponent::try_new("property", property)?,
    ))
}

/// Resolves DROP against one point-loaded canonical definition.
///
/// The temporary catalog exists only to reuse the planner-shape validation for
/// secondary uniqueness/direction while returning the exact persisted vector
/// or text settings. The lifecycle transaction compares the returned complete
/// definition again, closing a concurrent drop/recreate race.
pub(crate) fn dynamic_index_definition_from_canonical_drop_spec(
    spec: &ir::IndexDdlDropSpec,
    canonical: &index_v2::ValidatedDynamicIndexDefinition,
) -> Result<index_v2::ValidatedDynamicIndexDefinition> {
    let identity = dynamic_index_identity_from_drop_spec(spec)?;
    if canonical.identity() != identity {
        return Err(HelixDbError::IndexNotFound(format!("{identity:?}")));
    }
    let mut indexes = config::RuntimeIndexCatalog::new();
    indexes.insert_dynamic_index(canonical);
    dynamic_index_definition_from_drop_spec(spec, &indexes)
}

/// Resolves a drop to the exact validated semantic definition already active.
///
/// Drop syntax does not repeat vector tuning or text analyzer settings. The
/// runtime catalog therefore supplies those fields without recreating a
/// persistence identity or consulting physical rows.
pub(crate) fn dynamic_index_definition_from_drop_spec(
    spec: &ir::IndexDdlDropSpec,
    indexes: &config::RuntimeIndexCatalog,
) -> Result<crate::index_v2::ValidatedDynamicIndexDefinition> {
    let snapshot = indexes.planner_snapshot();
    if let Some(definition) = secondary_definition_from_drop_spec(spec, &snapshot)? {
        return Ok(definition.try_into()?);
    }
    if let Some(definition) = vector_definition_from_drop_spec(spec, indexes)? {
        return Ok(definition.try_into()?);
    }
    if let Some(definition) = text_definition_from_drop_spec(spec, indexes)? {
        return Ok(definition.try_into()?);
    }
    unreachable!("every index drop spec belongs to exactly one family")
}

/// Reconstructs the exact active secondary definition named by a drop.
///
/// Text and vector drops need persisted semantic fields not carried by every
/// planner drop shape, so this boundary intentionally returns `None` for those
/// families. Secondary equality/range definitions are complete in the drop
/// shape plus active planner snapshot and can therefore be lifecycle-bound
/// without consulting or rewriting physical rows.
pub(crate) fn secondary_definition_from_drop_spec(
    spec: &ir::IndexDdlDropSpec,
    snapshot: &catalog::IndexCatalogSnapshot,
) -> Result<Option<config::SecondaryIndexDefinition>> {
    let definition = match spec {
        ir::IndexDdlDropSpec::NodeEquality { key, uniqueness } => {
            let Some(existing) = snapshot.node_eq.get(key) else {
                return Err(index_not_found("node equality", key));
            };
            if existing.uniqueness != *uniqueness {
                return Err(index_not_found("node equality", key));
            }
            match uniqueness {
                catalog::IndexUniqueness::Unique => {
                    config::SecondaryIndexDefinition::node_unique_equality(
                        key.label.as_ref(),
                        key.property.as_ref(),
                    )
                }
                catalog::IndexUniqueness::NonUnique => {
                    config::SecondaryIndexDefinition::node_equality(
                        key.label.as_ref(),
                        key.property.as_ref(),
                    )
                }
            }?
        }
        ir::IndexDdlDropSpec::NodeRange { key } => {
            if !snapshot.node_range.contains_key(key) {
                return Err(index_not_found("node range", key));
            }
            config::SecondaryIndexDefinition::node_range_with_direction(
                key.label.as_ref(),
                key.property.as_ref(),
                config_range_direction(key.direction),
            )?
        }
        ir::IndexDdlDropSpec::EdgeEquality { key } => {
            if !snapshot.edge_eq.contains_key(key) {
                return Err(index_not_found("edge equality", key));
            }
            config::SecondaryIndexDefinition::edge_equality(
                key.label.as_ref(),
                key.property.as_ref(),
            )?
        }
        ir::IndexDdlDropSpec::EdgeRange { key } => {
            if !snapshot.edge_range.contains_key(key) {
                return Err(index_not_found("edge range", key));
            }
            config::SecondaryIndexDefinition::edge_range_with_direction(
                key.label.as_ref(),
                key.property.as_ref(),
                config_range_direction(key.direction),
            )?
        }
        ir::IndexDdlDropSpec::NodeVector { .. }
        | ir::IndexDdlDropSpec::NodeText { .. }
        | ir::IndexDdlDropSpec::EdgeVector { .. }
        | ir::IndexDdlDropSpec::EdgeText { .. } => return Ok(None),
    };
    Ok(Some(definition))
}

pub(crate) fn vector_definition_from_drop_spec(
    spec: &ir::IndexDdlDropSpec,
    config: &config::IndexConfig,
) -> Result<Option<config::VectorIndexDefinition>> {
    let (element_type, key) = match spec {
        ir::IndexDdlDropSpec::NodeVector { key } => (config::VectorElementType::Node, key),
        ir::IndexDdlDropSpec::EdgeVector { key } => (config::VectorElementType::Edge, key),
        ir::IndexDdlDropSpec::NodeEquality { .. }
        | ir::IndexDdlDropSpec::NodeRange { .. }
        | ir::IndexDdlDropSpec::EdgeEquality { .. }
        | ir::IndexDdlDropSpec::EdgeRange { .. }
        | ir::IndexDdlDropSpec::NodeText { .. }
        | ir::IndexDdlDropSpec::EdgeText { .. } => return Ok(None),
    };
    config
        .vector_indexes()
        .find(|definition| {
            definition.element_type() == element_type
                && definition.label() == key.label.as_ref()
                && definition.property() == key.property.as_ref()
        })
        .cloned()
        .map(Some)
        .ok_or_else(|| index_not_found("vector", &search_key_from_scoped(element_type.into(), key)))
}

pub(crate) fn text_definition_from_drop_spec(
    spec: &ir::IndexDdlDropSpec,
    indexes: &config::IndexConfig,
) -> Result<Option<config::TextIndexDefinition>> {
    let (element_type, key) = match spec {
        ir::IndexDdlDropSpec::NodeText { key } => (config::TextElementType::Node, key),
        ir::IndexDdlDropSpec::EdgeText { key } => (config::TextElementType::Edge, key),
        ir::IndexDdlDropSpec::NodeEquality { .. }
        | ir::IndexDdlDropSpec::NodeRange { .. }
        | ir::IndexDdlDropSpec::EdgeEquality { .. }
        | ir::IndexDdlDropSpec::EdgeRange { .. }
        | ir::IndexDdlDropSpec::NodeVector { .. }
        | ir::IndexDdlDropSpec::EdgeVector { .. } => return Ok(None),
    };
    indexes
        .text_indexes()
        .find(|definition| {
            definition.element_type() == element_type
                && definition.label() == key.label.as_ref()
                && definition.property() == key.property.as_ref()
        })
        .cloned()
        .map(Some)
        .ok_or_else(|| {
            let element = match element_type {
                config::TextElementType::Node => catalog::ElementKind::Node,
                config::TextElementType::Edge => catalog::ElementKind::Edge,
            };
            index_not_found("text", &search_key_from_scoped(element, key))
        })
}

fn text_definition(
    element_type: config::TextElementType,
    key: &catalog::ScopedPropertyKey,
    scope: &catalog::SearchIndexScope,
) -> Result<config::TextIndexDefinition> {
    text_definition_from_key(element_type, key, scope)
}

fn text_definition_from_key(
    element_type: config::TextElementType,
    key: &catalog::ScopedPropertyKey,
    scope: &catalog::SearchIndexScope,
) -> Result<config::TextIndexDefinition> {
    let definition = match element_type {
        config::TextElementType::Node => {
            config::TextIndexDefinition::new_node(key.label.as_ref(), key.property.as_ref())
        }
        config::TextElementType::Edge => {
            config::TextIndexDefinition::new_edge(key.label.as_ref(), key.property.as_ref())
        }
    }?;
    match scope {
        catalog::SearchIndexScope::Unscoped => Ok(definition),
        catalog::SearchIndexScope::Tenant { property } => {
            Ok(definition.with_tenant_property(property.as_ref())?)
        }
    }
}

fn vector_definition(
    element_type: config::VectorElementType,
    key: &catalog::ScopedPropertyKey,
    dimension: ir::VectorIndexDimension,
    metric: ir::VectorIndexMetric,
    scope: &catalog::SearchIndexScope,
) -> Result<config::VectorIndexDefinition> {
    let definition = match element_type {
        config::VectorElementType::Node => config::VectorIndexDefinition::new_node(
            key.label.as_ref(),
            key.property.as_ref(),
            dimension.get(),
            db_vector_metric(metric),
        ),
        config::VectorElementType::Edge => config::VectorIndexDefinition::new_edge(
            key.label.as_ref(),
            key.property.as_ref(),
            dimension.get(),
            db_vector_metric(metric),
        ),
    }?;
    match scope {
        catalog::SearchIndexScope::Unscoped => Ok(definition),
        catalog::SearchIndexScope::Tenant { property } => {
            Ok(definition.with_tenant_property(property.as_ref())?)
        }
    }
}

fn db_vector_metric(metric: ir::VectorIndexMetric) -> crate::search::vector::VectorDistanceMetric {
    match metric {
        ir::VectorIndexMetric::Cosine => crate::search::vector::VectorDistanceMetric::Cosine,
        ir::VectorIndexMetric::Euclidean => crate::search::vector::VectorDistanceMetric::Euclidean,
        ir::VectorIndexMetric::Manhattan => crate::search::vector::VectorDistanceMetric::Manhattan,
    }
}

fn config_range_direction(direction: RangeIndexDirection) -> config::RangeIndexDirection {
    match direction {
        RangeIndexDirection::Asc => config::RangeIndexDirection::Asc,
        RangeIndexDirection::Desc => config::RangeIndexDirection::Desc,
    }
}

fn index_not_found(kind: &'static str, key: &impl std::fmt::Display) -> HelixDbError {
    HelixDbError::IndexNotFound(format!("{kind} index `{key}`"))
}

fn search_key_from_scoped(
    element: catalog::ElementKind,
    key: &catalog::ScopedPropertyKey,
) -> catalog::SearchIndexKey {
    catalog::SearchIndexKey::new(element, key.label.clone(), key.property.clone())
}

impl From<config::VectorElementType> for catalog::ElementKind {
    fn from(value: config::VectorElementType) -> Self {
        match value {
            config::VectorElementType::Node => Self::Node,
            config::VectorElementType::Edge => Self::Edge,
        }
    }
}
