//! Downstream search pipeline recognition.

use helix_ast::traversal::AstNode;

use super::contract::{NativePipelineOp, NativePipelineOpMatch};
use crate::{context, error, ir, logical, planning};

pub(super) fn pipeline_op_from_ast<'a>(
    ctx: &context::PlannerContext,
    root: &'a AstNode,
) -> Result<NativePipelineOpMatch<'a>, error::PlannerError> {
    Ok(match root {
        AstNode::TextSearchNodesWithin {
            input,
            label,
            property,
            tenant_value,
            query_text,
            k,
            fuzzy_distance,
        } => {
            let search = planning::search::node_text_search(
                &ctx.indexes,
                label,
                property,
                tenant_value.as_ref(),
                query_text,
                k,
                *fuzzy_distance,
            )?;
            let ir::NodeAccessPlan::TextSearch {
                key,
                index,
                query_text,
                k,
                fuzzy_distance,
            } = search.plan
            else {
                unreachable!("node text-search builder returned another access family")
            };
            NativePipelineOpMatch::Op(NativePipelineOp::new(
                input,
                logical::StreamPipelineOp::TextSearch {
                    plan: Box::new(ir::RestrictedTextSearchPlan::Nodes {
                        key,
                        index,
                        query_text,
                        k,
                        fuzzy_distance,
                    }),
                },
            ))
        }
        AstNode::TextSearchEdgesWithin {
            input,
            label,
            property,
            tenant_value,
            query_text,
            k,
            fuzzy_distance,
        } => {
            let search = planning::search::edge_text_search(
                &ctx.indexes,
                label,
                property,
                tenant_value.as_ref(),
                query_text,
                k,
                *fuzzy_distance,
            )?;
            let ir::EdgeAccessPlan::TextSearch {
                key,
                index,
                query_text,
                k,
                fuzzy_distance,
            } = search.plan
            else {
                unreachable!("edge text-search builder returned another access family")
            };
            NativePipelineOpMatch::Op(NativePipelineOp::new(
                input,
                logical::StreamPipelineOp::TextSearch {
                    plan: Box::new(ir::RestrictedTextSearchPlan::Edges {
                        key,
                        index,
                        query_text,
                        k,
                        fuzzy_distance,
                    }),
                },
            ))
        }
        AstNode::VectorSearchNodesWithin {
            input,
            label,
            property,
            tenant_value,
            query_vector,
            k,
        } => {
            let search = planning::search::node_vector_search(
                &ctx.indexes,
                label,
                property,
                tenant_value.as_ref(),
                query_vector,
                k,
            )?;
            let ir::NodeAccessPlan::VectorSearch {
                key,
                index,
                query_vector,
                k,
            } = search.plan
            else {
                unreachable!("node vector-search builder returned another access family")
            };
            NativePipelineOpMatch::Op(NativePipelineOp::new(
                input,
                logical::StreamPipelineOp::VectorSearch {
                    plan: Box::new(ir::RestrictedVectorSearchPlan::Nodes {
                        key,
                        index,
                        query_vector,
                        k,
                    }),
                },
            ))
        }
        AstNode::VectorSearchEdgesWithin {
            input,
            label,
            property,
            tenant_value,
            query_vector,
            k,
        } => {
            let search = planning::search::edge_vector_search(
                &ctx.indexes,
                label,
                property,
                tenant_value.as_ref(),
                query_vector,
                k,
            )?;
            let ir::EdgeAccessPlan::VectorSearch {
                key,
                index,
                query_vector,
                k,
            } = search.plan
            else {
                unreachable!("edge vector-search builder returned another access family")
            };
            NativePipelineOpMatch::Op(NativePipelineOp::new(
                input,
                logical::StreamPipelineOp::VectorSearch {
                    plan: Box::new(ir::RestrictedVectorSearchPlan::Edges {
                        key,
                        index,
                        query_vector,
                        k,
                    }),
                },
            ))
        }
        _ => NativePipelineOpMatch::NotThisFamily,
    })
}
