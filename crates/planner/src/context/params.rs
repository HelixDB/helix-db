use std::collections::BTreeMap;
use std::sync::Arc;

use helix_ast::query::QueryValue;
use helix_ast::value::PropertyValue;
use serde::{Deserialize, Serialize};

use crate::ir;

/// Runtime parameter bindings.
///
/// Parameter names are [`ir::NonEmptyString`] values, so empty runtime parameter
/// names cannot be inserted through this builder API.
///
/// # Examples
///
/// ```
/// use helix_planner::context::ParamBindings;
/// use helix_planner::ir::NonEmptyString;
///
/// let name = NonEmptyString::new("limit").unwrap();
/// let params = ParamBindings::default().with_value(name.clone(), 10);
///
/// assert_eq!(params.values[&name].as_i64(), Some(10));
/// ```
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ParamBindings {
    /// Property-compatible parameters.
    pub values: BTreeMap<ir::NonEmptyString, PropertyValue>,
    /// JSON-compatible query parameters retained for executor use.
    pub query_values: BTreeMap<ir::NonEmptyString, QueryValue>,
}

impl ParamBindings {
    /// Insert a property-compatible parameter.
    pub fn with_value(mut self, name: ir::NonEmptyString, value: impl Into<PropertyValue>) -> Self {
        self.values.insert(name, value.into());
        self
    }

    /// Insert a JSON-compatible query parameter.
    pub fn with_query_value(mut self, name: ir::NonEmptyString, value: QueryValue) -> Self {
        self.query_values.insert(name, value);
        self
    }
}

/// One request's bindings, shared rather than copied by everything that
/// plans the request.
///
/// The planner context, the optimizer configuration and every cardinality
/// expression the optimizer explores read the same bindings, and a request's
/// parameters can be megabytes (a bulk insert's rows). Clones share one
/// allocation, equality checks identity before contents, and serialization is
/// the bindings' own, so the planner context's wire format is unchanged.
///
/// ```
/// use helix_planner::context::{ParamBindings, SharedParamBindings};
/// use helix_planner::ir::NonEmptyString;
///
/// let name = NonEmptyString::new("tenant").unwrap();
/// let shared = SharedParamBindings::from(ParamBindings::default().with_value(name.clone(), "acme"));
/// let copy = shared.clone();
/// assert!(std::ptr::eq(&*shared, &*copy));
/// assert_eq!(copy.values[&name].as_str(), Some("acme"));
///
/// // With every clone gone, the bindings come back without a copy.
/// let address = shared.values[&name].as_str().unwrap().as_ptr();
/// drop(copy);
/// let owned = shared.into_inner();
/// assert_eq!(owned.values[&name].as_str().unwrap().as_ptr(), address);
/// ```
#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SharedParamBindings(Arc<ParamBindings>);

impl SharedParamBindings {
    /// The bindings, moved out when this is the last holder and copied
    /// otherwise.
    pub fn into_inner(self) -> ParamBindings {
        Arc::try_unwrap(self.0).unwrap_or_else(|shared| (*shared).clone())
    }

    /// Mutable bindings for building a context, copied first only if
    /// another holder shares them.
    ///
    /// ```
    /// use helix_planner::context::PlannerContext;
    /// use helix_planner::ir::NonEmptyString;
    ///
    /// let mut ctx = PlannerContext::default();
    /// ctx.params.make_mut().values.insert(NonEmptyString::new("n").unwrap(), 1.into());
    /// assert_eq!(ctx.params.values.len(), 1);
    /// ```
    pub fn make_mut(&mut self) -> &mut ParamBindings {
        Arc::make_mut(&mut self.0)
    }
}

impl From<ParamBindings> for SharedParamBindings {
    fn from(bindings: ParamBindings) -> Self {
        Self(Arc::new(bindings))
    }
}

impl std::ops::Deref for SharedParamBindings {
    type Target = ParamBindings;

    fn deref(&self) -> &ParamBindings {
        &self.0
    }
}

impl PartialEq for SharedParamBindings {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0) || self.0 == other.0
    }
}

impl std::fmt::Debug for SharedParamBindings {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builders_keep_property_and_query_values_separate() {
        let property_name = ir::NonEmptyString::new("limit").unwrap();
        let query_name = ir::NonEmptyString::new("payload").unwrap();
        let query_value = QueryValue::String("raw".to_string());

        let params = ParamBindings::default()
            .with_value(property_name.clone(), 10)
            .with_query_value(query_name.clone(), query_value.clone());

        assert_eq!(
            params
                .values
                .get(&property_name)
                .and_then(PropertyValue::as_i64),
            Some(10)
        );
        assert_eq!(params.query_values.get(&query_name), Some(&query_value));
        assert!(!params.values.contains_key(&query_name));
        assert!(!params.query_values.contains_key(&property_name));
    }

    /// Planning a count, whose cardinality expressions carry the request's
    /// bindings through every rewrite, shares the bindings and releases them:
    /// they come back from the context as the same allocation.
    #[test]
    fn planning_shares_request_bindings_and_releases_them() {
        use helix_ast::{batch, traversal};

        let name = ir::NonEmptyString::new("unused").unwrap();
        let ctx = crate::context::PlannerContext {
            params: ParamBindings::default()
                .with_query_value(name.clone(), QueryValue::String("x".repeat(1024)))
                .into(),
            ..crate::context::PlannerContext::default()
        };
        let address = |params: &ParamBindings| {
            let QueryValue::String(value) = &params.query_values[&name] else {
                panic!("string parameter");
            };
            value.as_ptr()
        };
        let before = address(&ctx.params);
        let query = batch::BatchQuery::Read(
            batch::read_batch()
                .var_as("users", traversal::g().n_with_label("User").count())
                .returning(["users"]),
        );
        let planned = crate::planning::plan_with_diagnostics(&query, &ctx).unwrap();
        drop(planned);
        assert_eq!(address(&ctx.params.into_inner()), before);
    }
}
