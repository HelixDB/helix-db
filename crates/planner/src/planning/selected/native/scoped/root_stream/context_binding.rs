//! Scoped `$context` root-stream recognition.

use super::super::binding;
use super::ScopedRootStream;
use crate::error;
use crate::logical;
use crate::planning::selected::native::scope::NativeAstScope;

pub(super) fn context_root_stream(
    scope: NativeAstScope,
) -> Result<ScopedRootStream, error::PlannerError> {
    if scope.binds_context() {
        Ok(ScopedRootStream::Stream(Box::new(
            logical::RootStream::VariableSource(binding::context_variable_source()),
        )))
    } else {
        Ok(ScopedRootStream::NotRootStream)
    }
}
