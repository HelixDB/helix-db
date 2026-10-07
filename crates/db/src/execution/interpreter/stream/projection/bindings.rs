//! Binding projection contracts.

use super::*;

impl<'db> ExecutionContext<'db> {
    #[cfg(test)]
    pub(in crate::execution::interpreter::stream::projection) async fn binding_projection(
        &self,
        row: &ExecutionRow,
        projection: &ir::BindingProjectionPlan,
    ) -> Result<Option<(String, DbPropertyValue)>> {
        let mut resolver = eval::RowValueResolver::new(self);
        self.binding_projection_with_resolver(row, projection, &mut resolver)
            .await
    }

    pub(in crate::execution::interpreter::stream::projection) async fn binding_projection_with_resolver(
        &self,
        row: &ExecutionRow,
        projection: &ir::BindingProjectionPlan,
        resolver: &mut eval::RowValueResolver<'_, 'db>,
    ) -> Result<Option<(String, DbPropertyValue)>> {
        match projection {
            ir::BindingProjectionPlan::Property {
                target,
                source,
                alias,
            } => {
                let Some((element, virtual_properties)) = self.binding_target(row, target) else {
                    return Ok(None);
                };
                Ok(resolver
                    .element_property(Some(element), virtual_properties, source)
                    .await?
                    .map(|value| (alias.as_ref().to_string(), value)))
            }
            ir::BindingProjectionPlan::Coalesce { refs, alias } => {
                for value_ref in refs.as_ref() {
                    let Some((element, virtual_properties)) =
                        self.binding_target(row, &value_ref.target)
                    else {
                        continue;
                    };
                    if let Some(value) = resolver
                        .element_property(Some(element), virtual_properties, &value_ref.source)
                        .await?
                        && !matches!(value, DbPropertyValue::Null)
                    {
                        return Ok(Some((alias.as_ref().to_string(), value)));
                    }
                }
                Ok(None)
            }
        }
    }

    pub(in crate::execution::interpreter::stream::projection) fn binding_target<'r>(
        &self,
        row: &'r ExecutionRow,
        target: &ir::BindingTargetPlan,
    ) -> Option<(&'r ElementRef, Option<&'r RowVirtualProperties>)> {
        match target {
            ir::BindingTargetPlan::Current => row
                .current
                .as_ref()
                .map(|element| (element, Some(&row.virtual_properties))),
            ir::BindingTargetPlan::Binding(name) => row
                .bindings
                .get(name)
                .map(|element| (element, row.binding_virtual_properties.get(name))),
        }
    }
}
