//! Public index lifecycle execution boundary.
//!
//! Status/control operations point-read or mutate the retained operation in
//! the execution scope. Family CREATE/DROP crosses the DB-owned capability and
//! canonical-definition boundary before atomically enqueueing durable work.

use super::*;

impl<'db> ExecutionContext<'db> {
    /// Executes scope-bound lifecycle controls and durable family DDL.
    pub(super) async fn execute_index_ddl(
        &mut self,
        _input: ExecutionValue,
        plan: &ir::IndexDdlPlan,
    ) -> Result<ExecutionValue> {
        self.discard_pending_catalog_freshness();
        let operation_id = |operation_id: &ir::IndexOperationId| {
            let uuid = uuid::Uuid::parse_str(operation_id.as_str()).map_err(|error| {
                HelixDbError::InvariantViolation(format!(
                    "validated planner operation ID did not parse: {error}"
                ))
            })?;
            crate::index_lifecycle::IndexOperationId::new(uuid).map_err(HelixDbError::from)
        };
        match plan {
            ir::IndexDdlPlan::GetOperation { operation_id: id } => {
                let status = self
                    .db
                    .get_index_operation(self.tenant_scope, operation_id(id)?)
                    .await?;
                Ok(ExecutionValue::IndexOperationStatus(status))
            }
            ir::IndexDdlPlan::RetryOperation { operation_id: id } => {
                let status = self
                    .db
                    .retry_index_operation_with_control(
                        self.tenant_scope,
                        operation_id(id)?,
                        &self.execution_control,
                    )
                    .await?;
                Ok(ExecutionValue::IndexOperationStatus(status))
            }
            ir::IndexDdlPlan::AbortOperation { operation_id: id } => {
                let status = self
                    .db
                    .abort_index_operation_with_control(
                        self.tenant_scope,
                        operation_id(id)?,
                        &self.execution_control,
                    )
                    .await?;
                Ok(ExecutionValue::IndexOperationStatus(status))
            }
            ir::IndexDdlPlan::Create { spec, mode } => {
                let receipt = self
                    .db
                    .enqueue_index_create_with_control(
                        self.tenant_scope,
                        spec,
                        *mode,
                        &self.execution_control,
                    )
                    .await?;
                Ok(ExecutionValue::IndexDdlReceipt(receipt))
            }
            ir::IndexDdlPlan::Drop { spec } => {
                let receipt = self
                    .db
                    .enqueue_index_drop_with_control(
                        self.tenant_scope,
                        spec,
                        &self.execution_control,
                    )
                    .await?;
                Ok(ExecutionValue::IndexDdlReceipt(receipt))
            }
        }
    }
}
