//! Native executable-plan envelope validation.
//!
//! This module owns the plan metadata that is independent of traversal shape:
//! read/write kind, return variables, and planner trace. Keeping it separate
//! lets production planning emit selected executable IR without first building a
//! compatibility physical tree.

mod handoff_trace;
mod root_kind;
mod write_contract;

use helix_ast::batch::{BatchQuery, ReadBatch, WriteBatch};

use crate::{error, ir, trace};

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PlanEnvelope {
    pub(crate) kind: ir::PlanKind,
    pub(crate) returns: ir::ReturnPlan,
    pub(crate) trace: trace::PlanningTrace,
}

impl PlanEnvelope {
    pub(crate) fn from_query(query: &BatchQuery) -> Result<Self, error::PlannerError> {
        match query {
            BatchQuery::Read(batch) => Self::read(batch),
            BatchQuery::Write(batch) => Self::write(batch),
        }
    }

    pub(crate) fn read(batch: &ReadBatch) -> Result<Self, error::PlannerError> {
        Ok(Self {
            kind: ir::PlanKind::Read,
            returns: return_plan(&batch.returns)?,
            trace: handoff_trace::native_handoff_trace(&batch.entries),
        })
    }

    pub(crate) fn write(batch: &WriteBatch) -> Result<Self, error::PlannerError> {
        write_contract::validate_write_batch(batch)?;
        Ok(Self {
            kind: ir::PlanKind::Write,
            returns: return_plan(&batch.returns)?,
            trace: handoff_trace::native_handoff_trace(&batch.entries),
        })
    }
}

fn return_plan(returns: &[String]) -> Result<ir::ReturnPlan, error::PlannerError> {
    let returns = returns
        .iter()
        .map(|name| {
            ir::NonEmptyString::new(name.clone()).ok_or(error::PlannerError::InvalidEmptyName {
                field: ir::NameField::Return,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    match ir::AtLeast::<_, 1>::try_from_vec(returns) {
        Some(names) => ir::ReturnVariables::new(names)
            .map(ir::ReturnPlan::Variables)
            .map_err(|err| match err {
                ir::ReturnVariablesError::DuplicateName { name } => {
                    error::PlannerError::DuplicateReturnVariable { name }
                }
            }),
        None => Ok(ir::ReturnPlan::None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use helix_ast::batch::{BatchEntry, NamedQuery};
    use helix_ast::graph::NodeRef;
    use helix_ast::traversal::AstNode;

    #[test]
    fn plan_envelope_validates_returns_and_kind() {
        let read = PlanEnvelope::read(&ReadBatch {
            entries: vec![BatchEntry::Query(Box::new(NamedQuery {
                name: Some("users".to_owned()),
                root: AstNode::Nodes {
                    reference: NodeRef::All,
                },
                condition: None,
            }))],
            returns: vec!["users".to_owned()],
        })
        .unwrap();
        assert_eq!(read.kind, ir::PlanKind::Read);
        assert!(matches!(read.returns, ir::ReturnPlan::Variables(_)));
        assert_eq!(read.trace.events.len(), 1);
        assert_eq!(read.trace.events[0].path.as_ref(), "entry[0].root");
        assert_eq!(read.trace.events[0].pass, trace::TracePass::NativeHandoff);
        assert_eq!(
            read.trace.events[0].decision,
            trace::TraceDecision::NativeQueryRoot
        );
        assert_eq!(
            read.trace.events[0].reason,
            trace::TraceReason::NativeAstRoot(ir::NonEmptyString::from_static("nodes"))
        );

        let duplicate = PlanEnvelope::write(&WriteBatch {
            entries: Vec::new(),
            returns: vec!["users".to_owned(), "users".to_owned()],
        });
        assert!(matches!(
            duplicate,
            Err(error::PlannerError::DuplicateReturnVariable { .. })
        ));
    }
}
