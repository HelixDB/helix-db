//! Row-property lookup contracts.

use super::*;

impl<'db> ExecutionContext<'db> {
    pub(in crate::execution::interpreter) async fn row_property(
        &self,
        row: &ExecutionRow,
        property: &ir::NonEmptyString,
    ) -> Result<Option<DbPropertyValue>> {
        if let Some(ElementRef::Edge(edge_id)) = row.current.as_ref() {
            let property_name = property.as_ref();
            match property_name {
                "$from" | "$to" => {
                    let Some((from, to)) = self.get_edge_endpoints(*edge_id).await? else {
                        return Ok(None);
                    };
                    let endpoint = if property_name == "$from" { from } else { to };
                    return Ok(Some(DbPropertyValue::I64(
                        endpoint.try_into().unwrap_or(i64::MAX),
                    )));
                }
                _ => {
                    if let Some((endpoint, path)) = edge_endpoint_property(property_name) {
                        let Some((from, to)) = self.get_edge_endpoints(*edge_id).await? else {
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
                        return Ok(property_value(&properties, path));
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
        let properties = self.row_properties(row).await?;
        Ok(property_value(&properties, property.as_ref()))
    }

    pub(in crate::execution::interpreter) async fn row_properties(
        &self,
        row: &ExecutionRow,
    ) -> Result<Vec<Property>> {
        let Some(element) = row.current.as_ref() else {
            return Ok(Vec::new());
        };
        self.element_properties(element).await
    }

    async fn element_properties(&self, element: &ElementRef) -> Result<Vec<Property>> {
        let kind = match element {
            ElementRef::Node(id) => {
                keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(*id))
            }
            ElementRef::Edge(id) => {
                keys::DataKeyKind::EdgePropertyById(keys::EdgePropertyByIdKey::new(*id))
            }
        };
        let key = keys::Key::Data {
            scope: self.tenant_scope,
            kind,
        }
        .to_bytes();
        self.get_raw(&key)
            .await?
            .map_or(Ok(Vec::new()), |value| Ok(decode_properties(&value)?))
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
