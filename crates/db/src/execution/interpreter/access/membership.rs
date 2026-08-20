//! Exact runtime equality-domain classification and execution.

use helix_planner::{catalog, exec, ir};

use super::super::{ElementRef, ExecutionContext, ExecutionRow};
use crate::encoding::property::property_value::PropertyValue;
use crate::error::Result;

enum RuntimeEqualityDomain {
    Indexed(Vec<PropertyValue>),
    Authoritative(PropertyValue),
}

impl<'db> ExecutionContext<'db> {
    pub(in crate::execution::interpreter) async fn dynamic_membership_ids(
        &self,
        kind: crate::index_lifecycle::IndexElementKind,
        key: &catalog::ScopedPropertyKey,
        plan: &ir::RuntimeEqualitySet,
    ) -> Result<roaring::RoaringTreemap> {
        match self.runtime_equality_domain(plan)? {
            RuntimeEqualityDomain::Indexed(values) => {
                self.lookup_managed_equality_union(kind, key, &values).await
            }
            RuntimeEqualityDomain::Authoritative(values) => {
                let (keyspace, element) = match kind {
                    crate::index_lifecycle::IndexElementKind::Node => (
                        exec::ElementKeyspace::NodeProperty,
                        ElementRef::Node as fn(u64) -> ElementRef,
                    ),
                    crate::index_lifecycle::IndexElementKind::Edge => (
                        exec::ElementKeyspace::EdgeEndpoints,
                        ElementRef::Edge as fn(u64) -> ElementRef,
                    ),
                };
                let ids = self.scan_element_ids(keyspace, None).await?;
                let mut matches = roaring::RoaringTreemap::new();
                for id in ids {
                    self.check_execution_deadline()?;
                    let row = ExecutionRow::current(element(id));
                    if self.scoped_membership_matches(&row, key, &values).await? {
                        matches.insert(id);
                    }
                }
                Ok(matches)
            }
        }
    }

    fn runtime_equality_domain(
        &self,
        plan: &ir::RuntimeEqualitySet,
    ) -> Result<RuntimeEqualityDomain> {
        let original = self.param_value(plan.param())?;
        let values = runtime_members(original.clone());
        let mut unique = Vec::new();
        for value in values {
            if !value.eq_value(&value) {
                continue;
            }
            if matches!(
                value,
                PropertyValue::Null | PropertyValue::Array(_) | PropertyValue::Object(_)
            ) {
                return Ok(RuntimeEqualityDomain::Authoritative(original));
            }
            if !unique
                .iter()
                .any(|existing: &PropertyValue| existing.eq_value(&value))
            {
                unique.push(value);
            }
        }
        if unique.len() > plan.max_values().get() {
            return Ok(RuntimeEqualityDomain::Authoritative(original));
        }
        Ok(RuntimeEqualityDomain::Indexed(unique))
    }

    async fn scoped_membership_matches(
        &self,
        row: &ExecutionRow,
        key: &catalog::ScopedPropertyKey,
        values: &PropertyValue,
    ) -> Result<bool> {
        let properties = self.row_properties(row).await?;
        if properties
            .iter()
            .find(|property| property.name == "$label")
            .and_then(|property| property.value.as_str())
            != Some(key.label.as_ref())
        {
            return Ok(false);
        }
        let value = properties
            .iter()
            .find(|property| property.name == key.property.as_ref())
            .map_or(&PropertyValue::Null, |property| &property.value);
        Ok(super::super::stream::property_value_is_in(value, values))
    }
}

fn runtime_members(value: PropertyValue) -> Vec<PropertyValue> {
    match value {
        PropertyValue::I64Array(values) => values.into_iter().map(PropertyValue::I64).collect(),
        PropertyValue::F64Array(values) => values.into_iter().map(PropertyValue::F64).collect(),
        PropertyValue::F32Array(values) => values
            .into_iter()
            .map(|value| PropertyValue::F32(f64::from(value)))
            .collect(),
        PropertyValue::StringArray(values) => {
            values.into_iter().map(PropertyValue::String).collect()
        }
        PropertyValue::Array(values) => values,
        value @ (PropertyValue::Null
        | PropertyValue::Bool(_)
        | PropertyValue::I64(_)
        | PropertyValue::DateTime(_)
        | PropertyValue::F64(_)
        | PropertyValue::F32(_)
        | PropertyValue::String(_)
        | PropertyValue::Bytes(_)
        | PropertyValue::Object(_)) => vec![value],
    }
}
