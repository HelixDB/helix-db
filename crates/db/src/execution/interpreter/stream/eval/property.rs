//! Row-property lookup contracts.

use std::collections::BTreeMap;

use super::*;
use crate::encoding::v2::values::property::view;

/// Lazy stored-value resolver owned by one input row's evaluation, or by one
/// record batch of rows whose records it prefetched.
///
/// Cache absence means an element has not been visited. [`CachedPropertyBlob::Missing`]
/// records a completed negative lookup, so repeated missing fields remain lazy without
/// repeating storage I/O. The cache is keyed by element, never by row, so rows sharing
/// one resolver read each record once and resolve the same values they would alone.
/// Resolved values are deliberately not cached because virtual properties belong to
/// the row or binding that requested them.
///
/// A batch owner that prefetches decodes every prefetched record first, so a read or
/// decode error anywhere in the batch is returned before an earlier row's own error.
///
/// A batch operator prefetches with [`Self::prefetch_rows`], so rows sharing an element
/// read it once.
///
/// Records are validated once when loaded and then read in place: a lookup
/// deserializes only the value it returns. Aligned copies of unaligned records
/// return to the resolver's buffers when their record leaves the cache.
pub(in crate::execution::interpreter::stream) struct RowValueResolver<'ctx, 'db> {
    context: &'ctx ExecutionContext<'db>,
    property_blobs: BTreeMap<ElementRef, CachedPropertyBlob>,
    edge_endpoints: BTreeMap<u64, Option<(u64, u64)>>,
    /// A record its caller already read from this request's view, loaded in
    /// place of a storage read the first time its element is loaded.
    scanned: Option<(ElementRef, bytes::Bytes)>,
    buffers: view::Buffers,
}

impl<'ctx, 'db> RowValueResolver<'ctx, 'db> {
    pub(in crate::execution::interpreter::stream) fn new(
        context: &'ctx ExecutionContext<'db>,
    ) -> Self {
        Self {
            context,
            property_blobs: BTreeMap::new(),
            edge_endpoints: BTreeMap::new(),
            scanned: None,
            buffers: view::Buffers::default(),
        }
    }

    /// A resolver for a row whose element `record` the caller already read
    /// from this request's view, as a storage scan does.
    ///
    /// The record is validated only when evaluation first reads it, exactly
    /// where a storage read would have happened, so errors and their timing
    /// are unchanged. `buffers` lends aligned copies across rows; take them
    /// back with [`Self::into_buffers`].
    pub(in crate::execution::interpreter::stream) fn with_record(
        context: &'ctx ExecutionContext<'db>,
        element: ElementRef,
        record: bytes::Bytes,
        buffers: view::Buffers,
    ) -> Self {
        Self {
            scanned: Some((element, record)),
            buffers,
            ..Self::new(context)
        }
    }

    /// Release every cached record and return the aligned copies for reuse.
    pub(in crate::execution::interpreter::stream) fn into_buffers(mut self) -> view::Buffers {
        for blob in std::mem::take(&mut self.property_blobs).into_values() {
            let CachedPropertyBlob::Row(row) = blob else {
                continue;
            };
            row.recycle(&mut self.buffers);
        }
        self.buffers
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
                let Some(row) = self.element_row(&ElementRef::Node(endpoint_id)).await? else {
                    return Ok(None);
                };
                property_value(row, path)
            }
            PropertySource::Known(value) => Ok(value),
            PropertySource::Record(element) => {
                let Some(row) = self.element_row(element).await? else {
                    return Ok(None);
                };
                property_value(row, property.as_ref())
            }
        }
    }

    /// All stored properties of the row element, read through the cache.
    ///
    /// The `last_use` of an element releases its record from the cache; a
    /// later use reads the record again.
    pub(in crate::execution::interpreter::stream) async fn row_properties(
        &mut self,
        row: &ExecutionRow,
        last_use: bool,
    ) -> Result<Vec<Property>> {
        let Some(element) = row.current.as_ref() else {
            return Ok(Vec::new());
        };
        if !last_use {
            let Some(row) = self.element_row(element).await? else {
                return Ok(Vec::new());
            };
            return Ok(row.decode()?);
        }
        let blob = match self.property_blobs.remove(element) {
            Some(blob) => blob,
            None => self.load_blob(element).await?,
        };
        let CachedPropertyBlob::Row(row) = blob else {
            return Ok(Vec::new());
        };
        let properties = row.decode()?;
        row.recycle(&mut self.buffers);
        Ok(properties)
    }

    /// The element's validated record, loaded through the cache; `None` when
    /// the element has no record.
    async fn element_row(&mut self, element: &ElementRef) -> Result<Option<&view::Row>> {
        // The load borrows the whole resolver, so the cache is probed before
        // it rather than through a held entry.
        if !self.property_blobs.contains_key(element) {
            let blob = self.load_blob(element).await?;
            self.property_blobs.insert(element.clone(), blob);
        }
        Ok(self
            .property_blobs
            .get(element)
            .expect("visited element has a cached property blob")
            .row())
    }

    async fn load_blob(&mut self, element: &ElementRef) -> Result<CachedPropertyBlob> {
        let value = match self.scanned.take_if(|(scanned, _)| *scanned == *element) {
            Some((_, record)) => Some(record),
            None => self.context.property_bytes(element).await?,
        };
        self.decode_blob(value)
    }

    fn decode_blob(&mut self, value: Option<bytes::Bytes>) -> Result<CachedPropertyBlob> {
        let Some(value) = value else {
            return Ok(CachedPropertyBlob::Missing);
        };
        #[cfg(test)]
        self.context.record_property_decode();
        Ok(CachedPropertyBlob::Row(view::Row::new(
            value,
            &mut self.buffers,
        )?))
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
            let blob = self.decode_blob(value)?;
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
    Row(view::Row),
}

impl CachedPropertyBlob {
    fn row(&self) -> Option<&view::Row> {
        match self {
            Self::Missing => None,
            Self::Row(row) => Some(row),
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

/// The value at `path`: the first property named exactly `path`, otherwise
/// a dotted walk from the first property named by its first segment through
/// nested objects. Only the value read is deserialized.
fn property_value(row: &view::Row, path: &str) -> Result<Option<DbPropertyValue>> {
    match row.value(path)? {
        Some(value) => Ok(Some(value)),
        None => nested_property_value(row, path),
    }
}

fn nested_property_value(row: &view::Row, path: &str) -> Result<Option<DbPropertyValue>> {
    if !path.contains('.') {
        return Ok(None);
    }
    let mut segments = path.split('.');
    let Some(first) = segments.next().filter(|first| !first.is_empty()) else {
        return Ok(None);
    };
    let Some(mut value) = row.value(first)? else {
        return Ok(None);
    };
    for segment in segments {
        if segment.is_empty() {
            return Ok(None);
        }
        let DbPropertyValue::Object(mut values) = value else {
            return Ok(None);
        };
        let Some(next) = values.remove(segment) else {
            return Ok(None);
        };
        value = next;
    }
    Ok(Some(value))
}
