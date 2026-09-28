//! Row-property lookup contracts.

use std::collections::{btree_map::Entry, BTreeMap};

use super::*;

/// Lazy stored-value resolver owned by one row's evaluation or one record batch.
///
/// Cache absence means an element has not been visited. [`CachedPropertyBlob::Missing`]
/// records a completed negative lookup, so repeated missing fields remain lazy without
/// repeating storage I/O. Resolved values are deliberately not cached because virtual
/// properties belong to the row or binding that requested them. A batch operator
/// prefetches with [`Self::prefetch_rows`], so rows sharing an element read it once.
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
        self.element_property(
            row.current.as_ref(),
            Some(&row.virtual_properties),
            property,
        )
        .await
    }

    /// Read `property` of `element`, whose virtual properties shadow its record.
    pub(in crate::execution::interpreter::stream) async fn element_property(
        &mut self,
        element: Option<&ElementRef>,
        virtual_properties: Option<&RowVirtualProperties>,
        property: &ir::NonEmptyString,
    ) -> Result<Option<DbPropertyValue>> {
        match PropertySource::of(element, virtual_properties, property) {
            PropertySource::EdgeEndpoint {
                edge_id,
                endpoint,
                path,
            } => {
                let Some((from, to)) = self.edge_endpoints(edge_id).await? else {
                    return Ok(None);
                };
                let endpoint_id = endpoint.node_id(from, to);
                let Some(path) = path else {
                    return Ok(Some(DbPropertyValue::I64(
                        endpoint_id.try_into().unwrap_or(i64::MAX),
                    )));
                };
                let properties = self
                    .element_properties(&ElementRef::Node(endpoint_id))
                    .await?;
                Ok(property_value(properties, path))
            }
            PropertySource::Known(value) => Ok(value),
            PropertySource::Record(element) => {
                let properties = self.element_properties(element).await?;
                Ok(property_value(properties, property.as_ref()))
            }
        }
    }

    /// All stored properties of the row element, read through the cache.
    pub(in crate::execution::interpreter::stream) async fn row_properties(
        &mut self,
        row: &ExecutionRow,
    ) -> Result<Vec<Property>> {
        let Some(element) = row.current.as_ref() else {
            return Ok(Vec::new());
        };
        Ok(self.element_properties(element).await?.to_vec())
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

    /// Load the stored records of `elements` with one multi-get.
    ///
    /// Elements already visited by this resolver and repeated elements are
    /// read once, so a batch of rows costs one read per distinct element.
    pub(in crate::execution::interpreter::stream) async fn prefetch<'e>(
        &mut self,
        elements: impl IntoIterator<Item = &'e ElementRef>,
    ) -> Result<()> {
        let missing = elements
            .into_iter()
            .filter(|element| !self.property_blobs.contains_key(*element))
            .cloned()
            .collect::<std::collections::BTreeSet<_>>();
        if missing.is_empty() {
            return Ok(());
        }
        let keys = missing
            .iter()
            .map(|element| self.context.property_blob_key(element))
            .collect::<Vec<_>>();
        let values = self.context.multi_get_raw(&keys).await?;
        for (element, value) in missing.into_iter().zip(values) {
            #[cfg(test)]
            self.context.record_property_get();
            let blob = self.context.decode_property_blob(value)?;
            self.property_blobs.insert(element, blob);
        }
        Ok(())
    }

    /// Prefetch what reading any of `properties` from each row always loads:
    /// the row element's record and its edge endpoints, each with one
    /// multi-get. Records the per-row lookup would skip are never read.
    pub(in crate::execution::interpreter::stream) async fn prefetch_rows(
        &mut self,
        rows: &[ExecutionRow],
        properties: &[&ir::NonEmptyString],
    ) -> Result<()> {
        let mut edges = Vec::new();
        let mut records = Vec::new();
        for row in rows {
            for property in properties {
                match PropertySource::of(
                    row.current.as_ref(),
                    Some(&row.virtual_properties),
                    property,
                ) {
                    PropertySource::EdgeEndpoint { edge_id, .. } => edges.push(edge_id),
                    PropertySource::Record(element) => records.push(element),
                    PropertySource::Known(_) => {}
                }
            }
        }
        self.prefetch_edge_endpoints(&edges).await?;
        self.prefetch(records).await
    }

    /// Load the endpoints of `edges` not yet visited with one multi-get.
    pub(in crate::execution::interpreter::stream) async fn prefetch_edge_endpoints(
        &mut self,
        edges: &[u64],
    ) -> Result<()> {
        let missing = edges
            .iter()
            .copied()
            .filter(|edge_id| !self.edge_endpoints.contains_key(edge_id))
            .collect::<std::collections::BTreeSet<_>>();
        if missing.is_empty() {
            return Ok(());
        }
        let keys = missing
            .iter()
            .map(|edge_id| {
                self.context.storage_key(keys::DataKeyKind::EdgeEndpoints(
                    keys::EdgeEndpointsKey::new(*edge_id),
                ))
            })
            .collect::<Vec<_>>();
        let values = self.context.multi_get_raw(&keys).await?;
        for (edge_id, value) in missing.into_iter().zip(values) {
            #[cfg(test)]
            self.context.record_endpoint_get();
            let endpoints = value
                .map(|bytes| {
                    crate::encoding::v2::values::edge_endpoints::EdgeEndpointsValue::decode(&bytes)
                        .map(|endpoints| (endpoints.source(), endpoints.target()))
                })
                .transpose()?;
            self.edge_endpoints.insert(edge_id, endpoints);
        }
        Ok(())
    }

    pub(in crate::execution::interpreter::stream) async fn edge_endpoints(
        &mut self,
        edge_id: u64,
    ) -> Result<Option<(u64, u64)>> {
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

    async fn load_property_blob(&self, element: &ElementRef) -> Result<CachedPropertyBlob> {
        self.decode_property_blob(self.property_bytes(element).await?)
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
        let key = self.property_blob_key(element);
        #[cfg(test)]
        self.record_property_get();
        self.get_raw(&key).await
    }

    fn property_blob_key(&self, element: &ElementRef) -> bytes::Bytes {
        let kind = match element {
            ElementRef::Node(id) => {
                keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(*id))
            }
            ElementRef::Edge(id) => {
                keys::DataKeyKind::EdgePropertyById(keys::EdgePropertyByIdKey::new(*id))
            }
        };
        keys::DataKey::Data {
            scope: self.tenant_scope,
            kind,
        }
        .to_bytes()
    }

    fn decode_property_blob(&self, value: Option<bytes::Bytes>) -> Result<CachedPropertyBlob> {
        let Some(value) = value else {
            return Ok(CachedPropertyBlob::Missing);
        };
        #[cfg(test)]
        self.record_property_decode();
        Ok(CachedPropertyBlob::Decoded(decode_properties(&value)?))
    }
}

/// Where [`RowValueResolver::element_property`] finds one property.
///
/// Classifying before reading lets batch operators prefetch exactly the
/// storage the per-row lookup reads.
enum PropertySource<'r> {
    /// An endpoint of the element's edge: its id, or the endpoint node's
    /// property at `path`.
    EdgeEndpoint {
        edge_id: u64,
        endpoint: EdgeEndpoint,
        path: Option<&'r str>,
    },
    /// A value known without reading storage.
    Known(Option<DbPropertyValue>),
    /// A property of the element's stored record.
    Record(&'r ElementRef),
}

impl<'r> PropertySource<'r> {
    fn of(
        element: Option<&'r ElementRef>,
        virtual_properties: Option<&RowVirtualProperties>,
        property: &'r ir::NonEmptyString,
    ) -> Self {
        let name = property.as_ref();
        if let Some(ElementRef::Edge(edge_id)) = element {
            let endpoint = match name {
                "$from" => Some((EdgeEndpoint::From, None)),
                "$to" => Some((EdgeEndpoint::To, None)),
                _ => edge_endpoint_property(name)
                    .map(|(endpoint, path)| (endpoint, (path != "$id").then_some(path))),
            };
            if let Some((endpoint, path)) = endpoint {
                return Self::EdgeEndpoint {
                    edge_id: *edge_id,
                    endpoint,
                    path,
                };
            }
        }
        if name == "$id" {
            return Self::Known(
                element.map(|element| {
                    DbPropertyValue::I64(element.id().try_into().unwrap_or(i64::MAX))
                }),
            );
        }
        if let Some(value) = virtual_properties.and_then(|properties| properties.get(property)) {
            return Self::Known(Some(value));
        }
        element.map_or(Self::Known(None), Self::Record)
    }

    /// The stored record this lookup always reads.
    fn record(self) -> Option<&'r ElementRef> {
        match self {
            Self::Record(element) => Some(element),
            Self::EdgeEndpoint { .. } | Self::Known(_) => None,
        }
    }
}

/// The stored record that reading `property` of `element` always loads.
pub(in crate::execution::interpreter::stream) fn record_read<'r>(
    element: Option<&'r ElementRef>,
    virtual_properties: Option<&RowVirtualProperties>,
    property: &'r ir::NonEmptyString,
) -> Option<&'r ElementRef> {
    PropertySource::of(element, virtual_properties, property).record()
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
