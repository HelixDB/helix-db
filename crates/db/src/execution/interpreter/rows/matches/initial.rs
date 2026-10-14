//! Initial sources retain the operation that must validate their candidates.
//! Generic cursors keep their existence contract; only this private producer
//! can defer a redundant probe to single-node label-pattern completion.
use super::{ExecutionContext, Match, Result};
use crate::execution::interpreter::rows::scan::NodeCursor;
use helix_planner::{exec, relational as r};

#[derive(Clone, Copy)]
pub(super) enum Membership {
    Checked,
    FreshLabel(r::Slot),
}

pub(in crate::execution::interpreter::rows) struct InitialSource<'a> {
    pub(super) cursor: NodeCursor,
    pub(super) operation: Match<'a>,
    pub(super) plan: &'a r::MatchPlan,
    pub(super) membership: Membership,
}

#[cfg(test)]
impl<'a> InitialSource<'a> {
    pub(in crate::execution::interpreter::rows) fn checked(
        cursor: NodeCursor,
        operation: Match<'a>,
        plan: &'a r::MatchPlan,
    ) -> Self {
        Self {
            cursor,
            operation,
            plan,
            membership: Membership::Checked,
        }
    }
}

#[cfg(test)]
#[path = "tests/initial.rs"]
mod tests;

impl ExecutionContext<'_> {
    pub(in crate::execution::interpreter::rows) async fn initial_match_source<'a>(
        &self,
        operation: Match<'a>,
        plan: &'a r::MatchPlan,
    ) -> Result<Option<InitialSource<'a>>> {
        if !plan.incoming.is_empty() {
            return Ok(None);
        }
        let Some(r::MatchStep::Scan(slot)) = plan.steps.first() else {
            return Ok(None);
        };
        let Some(source) = plan.sources.iter().find(|source| source.slot == *slot) else {
            return Ok(None);
        };
        let [step] = source.access.steps() else {
            return Ok(None);
        };
        let Some(mut cursor) = self.node_cursor(&step.op).await? else {
            return Ok(None);
        };
        let membership = match (
            step.op.node_cursor_access(),
            operation.pattern.nodes.as_slice(),
        ) {
            (Some(exec::ExecNodeCursor::LabelScan { label }), [node])
                if plan.steps.len() == 1
                    && operation.pattern.relationships.is_empty()
                    && node.slot == *slot
                    && node.label.as_deref() == Some(label.as_ref()) =>
            {
                let NodeCursor::Indexed {
                    verify_existence, ..
                } = &mut cursor
                else {
                    unreachable!("selected label source has an indexed cursor");
                };
                assert!(
                    *verify_existence,
                    "generic label cursor requires membership validation"
                );
                *verify_existence = false;
                Membership::FreshLabel(*slot)
            }
            _ => Membership::Checked,
        };
        Ok(Some(InitialSource {
            cursor,
            operation,
            plan,
            membership,
        }))
    }
}
