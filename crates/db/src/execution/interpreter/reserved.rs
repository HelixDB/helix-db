use super::*;

mod fold;
mod path;
mod sack;

impl<'db> ExecutionContext<'db> {
    pub(super) async fn reserved(
        &mut self,
        input: ExecutionValue,
        op: &ir::ReservedOp,
    ) -> Result<ExecutionValue> {
        match op {
            ir::ReservedOp::Fold => self.fold(input),
            ir::ReservedOp::Unfold => self.unfold(input),
            ir::ReservedOp::Path => self.path(input),
            ir::ReservedOp::SimplePath => self.simple_path(input),
            ir::ReservedOp::WithSack(initial) => self.with_sack(input, initial),
            ir::ReservedOp::SackSet(property) => self.sack_set(input, property).await,
            ir::ReservedOp::SackAdd(property) => self.sack_add(input, property).await,
            ir::ReservedOp::SackGet => self.sack_get(input),
        }
    }
}

#[cfg(test)]
mod tests;
