//! Validated native source stream construction.

use super::super::ast::NativeSourceAst;
use crate::planning::selected::native::{access, parameter_specialization, stream};
use crate::{analysis, context, error, ir, planning};

enum SourcePredicatePlan {
    Empty,
    Unfiltered,
    Label {
        label: ir::NonEmptyString,
        residual: Option<ir::PredicatePlan>,
    },
    Residual(ir::PredicatePlan),
}

impl SourcePredicatePlan {
    fn new(
        ctx: &context::PlannerContext,
        predicate: &helix_ast::expr::Predicate,
    ) -> Result<Self, error::PlannerError> {
        ir::PredicatePlan::validate(predicate)?;
        let specialized = parameter_specialization::predicate(ctx, predicate)?;
        // Pruning borrows the specialized predicate; when it keeps the whole
        // tree, that tree moves into the plan rather than being copied.
        let (pruned, label) = match analysis::prune_borrowed(&specialized)? {
            analysis::PrunedPredicate::Impossible => return Ok(Self::Empty),
            analysis::PrunedPredicate::Tautology => return Ok(Self::Unfiltered),
            analysis::PrunedPredicate::Feasible { predicate, label } => match predicate {
                std::borrow::Cow::Borrowed(kept) if std::ptr::eq(kept, &specialized) => {
                    (None, label)
                }
                predicate => (Some(predicate.into_owned()), label),
            },
        };
        let predicate = pruned.unwrap_or(specialized);
        Ok(match label {
            analysis::FeasibleLabelScope::Scoped(label) => Self::Label {
                residual: residual_after_label_scope(predicate, &label),
                label,
            },
            analysis::FeasibleLabelScope::Unscoped => Self::Residual(
                ir::PredicatePlan::new(predicate)
                    .expect("predicate pruning preserves validated predicate names"),
            ),
        })
    }
}

fn residual_after_label_scope(
    predicate: helix_ast::expr::Predicate,
    label: &ir::NonEmptyString,
) -> Option<ir::PredicatePlan> {
    if analysis::predicate_is_tautological_for_label(&predicate, label) {
        return None;
    }
    let residual = match predicate {
        helix_ast::expr::Predicate::And { predicates } => {
            let mut residuals = predicates
                .into_iter()
                .filter(|predicate| {
                    !analysis::predicate_is_tautological_for_label(predicate, label)
                })
                .collect::<Vec<_>>();
            match residuals.len() {
                0 => None,
                1 => residuals.pop(),
                _ => Some(helix_ast::expr::Predicate::and(residuals)),
            }
        }
        predicate => Some(predicate),
    };
    residual.map(|predicate| {
        ir::PredicatePlan::new(predicate)
            .expect("removing a label conjunct preserves validated predicate names")
    })
}

impl<'a> NativeSourceAst<'a> {
    /// Lower this source AST shape into a native access stream.
    pub(super) fn into_stream(
        self,
        ctx: &context::PlannerContext,
    ) -> Result<stream::NativeAccessStream, error::PlannerError> {
        Ok(match self {
            Self::Nodes(reference) => {
                stream::NativeAccessStream::new(access::NativeAccessPath::nodes(reference)?)
            }
            Self::Edges(reference) => {
                stream::NativeAccessStream::new(access::NativeAccessPath::edges(reference)?)
            }
            Self::NodesWhere(predicate) => node_predicate_stream(ctx, predicate)?,
            Self::EdgesWhere(predicate) => edge_predicate_stream(ctx, predicate)?,
            Self::NodeVectorSearch {
                label,
                property,
                tenant_value,
                query_vector,
                k,
            } => stream::NativeAccessStream::new(access::NativeAccessPath::node_plan(
                planning::search::node_vector_search(
                    &ctx.indexes,
                    label,
                    property,
                    tenant_value,
                    query_vector,
                    k,
                )?
                .plan,
            )),
            Self::NodeTextSearch {
                label,
                property,
                tenant_value,
                query_text,
                k,
            } => stream::NativeAccessStream::new(access::NativeAccessPath::node_plan(
                planning::search::node_text_search(
                    &ctx.indexes,
                    label,
                    property,
                    tenant_value,
                    query_text,
                    k,
                )?
                .plan,
            )),
            Self::EdgeVectorSearch {
                label,
                property,
                tenant_value,
                query_vector,
                k,
            } => stream::NativeAccessStream::new(access::NativeAccessPath::edge_plan(
                planning::search::edge_vector_search(
                    &ctx.indexes,
                    label,
                    property,
                    tenant_value,
                    query_vector,
                    k,
                )?
                .plan,
            )),
            Self::EdgeTextSearch {
                label,
                property,
                tenant_value,
                query_text,
                k,
            } => stream::NativeAccessStream::new(access::NativeAccessPath::edge_plan(
                planning::search::edge_text_search(
                    &ctx.indexes,
                    label,
                    property,
                    tenant_value,
                    query_text,
                    k,
                )?
                .plan,
            )),
        })
    }
}

fn node_predicate_stream(
    ctx: &context::PlannerContext,
    predicate: &helix_ast::expr::Predicate,
) -> Result<stream::NativeAccessStream, error::PlannerError> {
    Ok(match SourcePredicatePlan::new(ctx, predicate)? {
        SourcePredicatePlan::Empty => stream::NativeAccessStream::new(
            access::NativeAccessPath::node_plan(ir::NodeAccessPlan::Empty),
        ),
        SourcePredicatePlan::Unfiltered => {
            stream::NativeAccessStream::new(access::NativeAccessPath::all_nodes())
        }
        SourcePredicatePlan::Label { label, residual } => {
            let stream = stream::NativeAccessStream::new(access::NativeAccessPath::node_plan(
                ir::NodeAccessPlan::LabelScan { label },
            ));
            match residual {
                Some(predicate) => stream.filter_plan(predicate),
                None => stream,
            }
        }
        SourcePredicatePlan::Residual(predicate) => {
            stream::NativeAccessStream::new(access::NativeAccessPath::all_nodes())
                .filter_plan(predicate)
        }
    })
}

fn edge_predicate_stream(
    ctx: &context::PlannerContext,
    predicate: &helix_ast::expr::Predicate,
) -> Result<stream::NativeAccessStream, error::PlannerError> {
    Ok(match SourcePredicatePlan::new(ctx, predicate)? {
        SourcePredicatePlan::Empty => stream::NativeAccessStream::new(
            access::NativeAccessPath::edge_plan(ir::EdgeAccessPlan::Empty),
        ),
        SourcePredicatePlan::Unfiltered => {
            stream::NativeAccessStream::new(access::NativeAccessPath::all_edges())
        }
        SourcePredicatePlan::Label { label, residual } => {
            let stream = stream::NativeAccessStream::new(access::NativeAccessPath::edge_plan(
                ir::EdgeAccessPlan::LabelScan { label },
            ));
            match residual {
                Some(predicate) => stream.filter_plan(predicate),
                None => stream,
            }
        }
        SourcePredicatePlan::Residual(predicate) => {
            stream::NativeAccessStream::new(access::NativeAccessPath::all_edges())
                .filter_plan(predicate)
        }
    })
}
