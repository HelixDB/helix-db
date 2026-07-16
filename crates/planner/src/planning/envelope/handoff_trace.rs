//! Native AST handoff trace reconstruction.
//!
//! The executable planner no longer materializes compatibility physical plans
//! on the production path, so this module records the native batch/AST handoff
//! contract directly on the executable envelope.

use helix_ast::batch::BatchEntry;

use super::root_kind;
use crate::{ir, trace};

pub(super) fn native_handoff_trace(entries: &[BatchEntry]) -> trace::PlanningTrace {
    let mut trace = trace::PlanningTrace::default();
    push_native_handoff_entries("entry", entries, &mut trace);
    trace
}

fn push_native_handoff_entries(
    prefix: &str,
    entries: &[BatchEntry],
    trace: &mut trace::PlanningTrace,
) {
    entries.iter().enumerate().for_each(|(index, entry)| {
        let path = format!("{prefix}[{index}]");
        match entry {
            BatchEntry::Query(query) => {
                if let Some(event) = trace::TraceEvent::try_new(
                    trace::TracePass::NativeHandoff,
                    format!("{path}.root"),
                    trace::TraceDecision::NativeQueryRoot,
                    trace::TraceReason::NativeAstRoot(ir::NonEmptyString::from_static(
                        root_kind::ast_root_kind(&query.root),
                    )),
                ) {
                    trace.events.push(event);
                }
            }
            BatchEntry::ForEach { body, .. } => {
                if let Some(event) = trace::TraceEvent::try_new(
                    trace::TracePass::NativeHandoff,
                    path.as_str(),
                    trace::TraceDecision::NativeForEach,
                    trace::TraceReason::NativeForEachBody,
                ) {
                    trace.events.push(event);
                }
                push_native_handoff_entries(&format!("{path}.body"), body, trace);
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use helix_ast::batch::NamedQuery;
    use helix_ast::graph::NodeRef;
    use helix_ast::traversal::AstNode;

    #[test]
    fn native_handoff_trace_records_query_roots() {
        let trace = native_handoff_trace(&[BatchEntry::Query(Box::new(NamedQuery {
            name: Some("users".to_owned()),
            root: AstNode::Nodes {
                reference: NodeRef::All,
            },
            condition: None,
        }))]);

        assert_eq!(trace.events.len(), 1);
        assert_eq!(trace.events[0].path.as_ref(), "entry[0].root");
        assert_eq!(trace.events[0].pass, trace::TracePass::NativeHandoff);
        assert_eq!(
            trace.events[0].decision,
            trace::TraceDecision::NativeQueryRoot
        );
        assert_eq!(
            trace.events[0].reason,
            trace::TraceReason::NativeAstRoot(ir::NonEmptyString::from_static("nodes"))
        );
    }

    #[test]
    fn native_handoff_trace_records_nested_foreach_entries() {
        let trace = native_handoff_trace(&[BatchEntry::ForEach {
            param: "event".to_owned(),
            body: vec![BatchEntry::Query(Box::new(NamedQuery {
                name: Some("created".to_owned()),
                root: AstNode::AddN {
                    input: None,
                    label: "Event".to_owned(),
                    properties: Vec::new(),
                },
                condition: None,
            }))],
        }]);

        assert_eq!(trace.events.len(), 2);
        assert_eq!(trace.events[0].path.as_ref(), "entry[0]");
        assert_eq!(
            trace.events[0].decision,
            trace::TraceDecision::NativeForEach
        );
        assert_eq!(
            trace.events[0].reason,
            trace::TraceReason::NativeForEachBody
        );
        assert_eq!(trace.events[1].path.as_ref(), "entry[0].body[0].root");
        assert_eq!(
            trace.events[1].reason,
            trace::TraceReason::NativeAstRoot(ir::NonEmptyString::from_static("add_node_source"))
        );
    }
}
