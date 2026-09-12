//! Row-property lookup contracts.

use std::collections::{btree_map::Entry, BTreeMap};

use super::*;

/// Lazy stored-value resolver owned by one input row's evaluation.
///
/// Cache absence means an element has not been visited. [`CachedPropertyBlob::Missing`]
/// records a completed negative lookup, so repeated missing fields remain lazy without
/// repeating storage I/O. Resolved values are deliberately not cached because virtual
/// properties belong to the row or binding that requested them.
pub(in crate::execution::interpreter::stream) struct RowValueResolver<'ctx, 'db> {
    context: &'ctx ExecutionContext<'db>,
    property_blobs: BTreeMap<ElementRef, CachedPropertyBlob>,
    edge_endpoints: BTreeMap<u64, Option<(u64, u64)>>,
}

impl<'ctx, 'db> RowValueResolver<'ctx, 'db> {
    pub(in crate::execution::interpreter::stream) fn new(
        context: &'ctx ExecutionContext<'db>,
    ) -> Self {
        Self {
            context,
            property_blobs: BTreeMap::new(),
            edge_endpoints: BTreeMap::new(),
        }
    }

    pub(in crate::execution::interpreter::stream) async fn row_property(
        &mut self,
        row: &ExecutionRow,
        property: &ir::NonEmptyString,
    ) -> Result<Option<DbPropertyValue>> {
        if let Some(ElementRef::Edge(edge_id)) = row.current.as_ref() {
            let property_name = property.as_ref();
            match property_name {
                "$from" | "$to" => {
                    let Some((from, to)) = self.edge_endpoints(*edge_id).await? else {
                        return Ok(None);
                    };
                    let endpoint = if property_name == "$from" { from } else { to };
                    return Ok(Some(DbPropertyValue::I64(
                        endpoint.try_into().unwrap_or(i64::MAX),
                    )));
                }
                _ => {
                    if let Some((endpoint, path)) = edge_endpoint_property(property_name) {
                        let Some((from, to)) = self.edge_endpoints(*edge_id).await? else {
                            return Ok(None);
                        };
                        let endpoint_id = endpoint.node_id(from, to);
                        if path == "$id" {
                            return Ok(Some(DbPropertyValue::I64(
                                endpoint_id.try_into().unwrap_or(i64::MAX),
                            )));
                        }
                        let properties = self
                            .element_properties(&ElementRef::Node(endpoint_id))
                            .await?;
                        return Ok(property_value(properties, path));
                    }
                }
            }
        }
        if property.as_ref() == "$id" {
            return Ok(row
                .current
                .as_ref()
                .map(|element| DbPropertyValue::I64(element.id().try_into().unwrap_or(i64::MAX))));
        }
        if let Some(value) = row.virtual_properties.get(property) {
            return Ok(Some(value));
        }
        let Some(element) = row.current.as_ref() else {
            return Ok(None);
        };
        let properties = self.element_properties(element).await?;
        Ok(property_value(properties, property.as_ref()))
    }

    async fn element_properties(&mut self, element: &ElementRef) -> Result<&[Property]> {
        if let Entry::Vacant(entry) = self.property_blobs.entry(element.clone()) {
            let blob = self.context.load_property_blob(element).await?;
            entry.insert(blob);
        }
        Ok(self
            .property_blobs
            .get(element)
            .expect("visited element has a cached property blob")
            .properties())
    }

    async fn edge_endpoints(&mut self, edge_id: u64) -> Result<Option<(u64, u64)>> {
        if let Some(endpoints) = self.edge_endpoints.get(&edge_id) {
            return Ok(*endpoints);
        }
        let endpoints = self.context.get_edge_endpoints(edge_id).await?;
        self.edge_endpoints.insert(edge_id, endpoints);
        Ok(endpoints)
    }
}

impl<'db> ExecutionContext<'db> {
    pub(in crate::execution::interpreter) async fn row_property(
        &self,
        row: &ExecutionRow,
        property: &ir::NonEmptyString,
    ) -> Result<Option<DbPropertyValue>> {
        RowValueResolver::new(self)
            .row_property(row, property)
            .await
    }

    pub(in crate::execution::interpreter) async fn row_properties(
        &self,
        row: &ExecutionRow,
    ) -> Result<Vec<Property>> {
        let Some(element) = row.current.as_ref() else {
            return Ok(Vec::new());
        };
        Ok(self.load_property_blob(element).await?.into_properties())
    }

    async fn load_property_blob(&self, element: &ElementRef) -> Result<CachedPropertyBlob> {
        let Some(value) = self.property_bytes(element).await? else {
            return Ok(CachedPropertyBlob::Missing);
        };
        #[cfg(test)]
        self.record_property_decode();
        Ok(CachedPropertyBlob::Decoded(decode_properties(&value)?))
    }

    /// Inspect a fixed native field set without letting decoded ownership escape
    /// its admission guard. Missing rows retain the native empty-property contract.
    pub(in crate::execution::interpreter) async fn row_properties_match(
        &self,
        row: &ExecutionRow,
        names: &[&str],
        predicate: impl FnOnce(&[Property]) -> bool,
    ) -> Result<bool> {
        let Some(element) = row.current.as_ref() else {
            return Ok(predicate(&[]));
        };
        let Some(value) = self.property_bytes(element).await? else {
            return Ok(predicate(&[]));
        };
        #[cfg(test)]
        self.record_property_decode();
        let properties = crate::query_resources::properties::Decoded::new(
            &value,
            crate::encoding::v2::values::property::prepared::Selection::Names(names),
            self.row_memory.as_ref(),
        )?;
        Ok(predicate(&properties))
    }

    async fn property_bytes(&self, element: &ElementRef) -> Result<Option<bytes::Bytes>> {
        let kind = match element {
            ElementRef::Node(id) => {
                keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(*id))
            }
            ElementRef::Edge(id) => {
                keys::DataKeyKind::EdgePropertyById(keys::EdgePropertyByIdKey::new(*id))
            }
        };
        let key = keys::DataKey::Data {
            scope: self.tenant_scope,
            kind,
        }
        .to_bytes();
        #[cfg(test)]
        self.record_property_get();
        self.get_raw(&key).await
    }
}

enum CachedPropertyBlob {
    Missing,
    Decoded(Vec<Property>),
}

impl CachedPropertyBlob {
    fn properties(&self) -> &[Property] {
        match self {
            Self::Missing => &[],
            Self::Decoded(properties) => properties,
        }
    }

    fn into_properties(self) -> Vec<Property> {
        match self {
            Self::Missing => Vec::new(),
            Self::Decoded(properties) => properties,
        }
    }
}

#[derive(Clone, Copy)]
enum EdgeEndpoint {
    From,
    To,
}

impl EdgeEndpoint {
    fn node_id(self, from: u64, to: u64) -> u64 {
        match self {
            Self::From => from,
            Self::To => to,
        }
    }
}

fn edge_endpoint_property(path: &str) -> Option<(EdgeEndpoint, &str)> {
    path.strip_prefix("$from.")
        .map(|path| (EdgeEndpoint::From, path))
        .or_else(|| {
            path.strip_prefix("$to.")
                .map(|path| (EdgeEndpoint::To, path))
        })
}

fn property_value(properties: &[Property], path: &str) -> Option<DbPropertyValue> {
    properties
        .iter()
        .find(|item| item.name == path)
        .map(|item| item.value.clone())
        .or_else(|| nested_property_value(properties, path))
}

fn nested_property_value(properties: &[Property], path: &str) -> Option<DbPropertyValue> {
    if !path.contains('.') {
        return None;
    }

    let mut segments = path.split('.');
    let first = segments.next()?;
    if first.is_empty() {
        return None;
    }

    let mut value = properties
        .iter()
        .find(|property| property.name == first)
        .map(|property| property.value.clone())?;

    for segment in segments {
        if segment.is_empty() {
            return None;
        }
        let DbPropertyValue::Object(values) = value else {
            return None;
        };
        value = values.get(segment)?.clone();
    }

    Some(value)
}
