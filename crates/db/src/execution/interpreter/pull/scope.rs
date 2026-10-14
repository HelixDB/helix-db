//! A suspended child owns its context binding only while it is being polled.
//! Dropping a cancelled poll restores the parent binding before returning.
use super::*;

pub(super) struct Scope<'a, 'db> {
    pub context: &'a mut ExecutionContext<'db>,
    previous: Option<ExecutionValue>,
    suspended: &'a mut ExecutionValue,
    variable: ir::NonEmptyString,
}

impl<'a, 'db> Scope<'a, 'db> {
    pub fn new(context: &'a mut ExecutionContext<'db>, suspended: &'a mut ExecutionValue) -> Self {
        // Move the binding instead of cloning a potentially large repeat frontier.
        // Drop returns ownership even when a pending future is cancelled.
        let value = std::mem::replace(suspended, ExecutionValue::Stream(Vec::new()));
        let variable = ir::NonEmptyString::new("$context").expect("constant variable");
        let previous = context.variables.insert(variable.clone(), value);
        Self {
            context,
            previous,
            suspended,
            variable,
        }
    }
}

impl Drop for Scope<'_, '_> {
    fn drop(&mut self) {
        *self.suspended = self
            .context
            .variables
            .remove(&self.variable)
            .expect("active pure scope retains its context binding");
        match self.previous.take() {
            Some(value) => {
                self.context.variables.insert(self.variable.clone(), value);
            }
            None => {
                self.context.variables.remove(&self.variable);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cancelling_suspended_scope_restores_parent_binding() {
        let db = test_support::open_db("pull-cancel-scope").await;
        let mut context = ExecutionContext::new(&db, context::ParamBindings::default());
        let variable = ir::NonEmptyString::new("$context").unwrap();
        for previous in [None, Some(ExecutionValue::Count(7))] {
            if let Some(value) = previous.clone() {
                context.variables.insert(variable.clone(), value);
            }
            let mut suspended = ExecutionValue::Count(99);
            let mut future = Box::pin(async {
                let scope = Scope::new(&mut context, &mut suspended);
                assert_eq!(
                    scope.context.variable_value(&variable).unwrap(),
                    &ExecutionValue::Count(99)
                );
                futures::future::pending::<()>().await;
                drop(scope);
            });
            assert!(matches!(
                futures::poll!(&mut future),
                std::task::Poll::Pending
            ));
            drop(future);
            assert_eq!(suspended, ExecutionValue::Count(99));
            assert_eq!(context.variables.get(&variable), previous.as_ref());
        }
        db.close().await.unwrap();
    }
    #[tokio::test]
    async fn large_scope_reuses_frontier_allocation_and_restores_nested_bindings() {
        let db = test_support::open_db("pull-scope-ownership").await;
        let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
        let variable = test_support::name("$context");
        let rows = vec![ExecutionRow::current(ElementRef::Node(1)); 8192];
        let allocation = rows.as_ptr();
        let mut suspended = ExecutionValue::Stream(rows);
        for _ in 0..8192 {
            let outer = Scope::new(&mut ctx, &mut suspended);
            let ExecutionValue::Stream(rows) = outer.context.variable_value(&variable).unwrap()
            else {
                panic!("row context");
            };
            assert_eq!(
                rows.as_ptr(),
                allocation,
                "scope must move, never copy, the frontier"
            );
            let mut inner_value = ExecutionValue::Count(3);
            {
                let inner = Scope::new(outer.context, &mut inner_value);
                assert_eq!(
                    inner.context.variable_value(&variable).unwrap(),
                    &ExecutionValue::Count(3)
                );
            }
            assert_eq!(inner_value, ExecutionValue::Count(3));
        }
        assert!(ctx.variable_value(&variable).is_err());
        let ExecutionValue::Stream(rows) = suspended else {
            panic!("restored rows")
        };
        assert_eq!(rows.as_ptr(), allocation);
        db.close().await.unwrap();
    }
}
