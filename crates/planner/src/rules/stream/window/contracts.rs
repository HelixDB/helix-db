//! Static stream-window contract ADTs.

use crate::{ir, logical, optimizer};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct StaticStreamWindow {
    start: usize,
    end: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StaticStreamWindowMatch {
    NotWindow,
    Window(StaticStreamWindow),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StaticStreamWindowComposition {
    Invalid,
    Window(StaticStreamWindow),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AdvancedOffset {
    Overflow,
    Advanced(usize),
}

#[derive(Debug, Clone, PartialEq)]
pub(super) enum StaticStreamWindowOutput {
    Invalid,
    Identity,
    Op(logical::PureLogicalOp),
}

#[derive(Debug, Clone, PartialEq)]
pub(super) enum StreamWindowComposition {
    NotApplicable,
    Rewritten(ir::AtLeast<logical::PureLogicalOp, 1>),
}

impl StreamWindowComposition {
    pub(super) fn into_rule_result(self) -> optimizer::RuleResult {
        match self {
            Self::NotApplicable => optimizer::RuleResult::NotApplicable,
            Self::Rewritten(composed) => optimizer::RuleResult::Applied(
                optimizer::RuleEffect::Logical(ir::AtLeast::<_, 1>::from_one(
                    logical::LogicalExpr::PurePipeline(logical::PurePipeline::new(composed)),
                )),
            ),
        }
    }
}

impl StaticStreamWindow {
    pub(super) fn from_op(op: &logical::PureLogicalOp) -> StaticStreamWindowMatch {
        match op {
            logical::PureLogicalOp::Limit {
                count: ir::StreamBoundPlan::Literal(count),
            } => StaticStreamWindowMatch::Window(Self {
                start: 0,
                end: Some(*count),
            }),
            logical::PureLogicalOp::Skip {
                count: ir::StreamBoundPlan::Literal(count),
            } => StaticStreamWindowMatch::Window(Self {
                start: *count,
                end: None,
            }),
            logical::PureLogicalOp::Range {
                range: ir::StreamRangePlan::Literal(range),
            } => StaticStreamWindowMatch::Window(Self {
                start: range.start(),
                end: Some(range.end()),
            }),
            logical::PureLogicalOp::Limit {
                count: ir::StreamBoundPlan::Expr(_),
            }
            | logical::PureLogicalOp::Skip {
                count: ir::StreamBoundPlan::Expr(_),
            }
            | logical::PureLogicalOp::Range {
                range: ir::StreamRangePlan::Dynamic(_),
            }
            | logical::PureLogicalOp::NoOp
            | logical::PureLogicalOp::Empty
            | logical::PureLogicalOp::Source { .. }
            | logical::PureLogicalOp::Filter { .. }
            | logical::PureLogicalOp::Order { .. }
            | logical::PureLogicalOp::Distinct
            | logical::PureLogicalOp::Expand { .. }
            | logical::PureLogicalOp::Project
            | logical::PureLogicalOp::Aggregate
            | logical::PureLogicalOp::Variable
            | logical::PureLogicalOp::Reserved => StaticStreamWindowMatch::NotWindow,
        }
    }

    pub(super) fn compose(self, rhs: Self) -> StaticStreamWindowComposition {
        let AdvancedOffset::Advanced(start) = self.advance(rhs.start) else {
            return StaticStreamWindowComposition::Invalid;
        };
        let end = match rhs.end {
            Some(end) => {
                let AdvancedOffset::Advanced(end) = self.advance(end) else {
                    return StaticStreamWindowComposition::Invalid;
                };
                Some(end)
            }
            None => self.end,
        };
        StaticStreamWindowComposition::Window(Self { start, end })
    }

    fn advance(self, offset: usize) -> AdvancedOffset {
        let Some(advanced) = self.start.checked_add(offset) else {
            return AdvancedOffset::Overflow;
        };
        AdvancedOffset::Advanced(self.end.map_or(advanced, |end| advanced.min(end)))
    }

    pub(super) fn into_op(self) -> StaticStreamWindowOutput {
        match (self.start, self.end) {
            (0, None) => StaticStreamWindowOutput::Identity,
            (0, Some(end)) => StaticStreamWindowOutput::Op(logical::PureLogicalOp::Limit {
                count: ir::StreamBoundPlan::Literal(end),
            }),
            (start, None) => StaticStreamWindowOutput::Op(logical::PureLogicalOp::Skip {
                count: ir::StreamBoundPlan::Literal(start),
            }),
            (start, Some(end)) => ir::StreamLiteralRange::new(start, end).map_or(
                StaticStreamWindowOutput::Invalid,
                |range| {
                    StaticStreamWindowOutput::Op(logical::PureLogicalOp::Range {
                        range: ir::StreamRangePlan::Literal(range),
                    })
                },
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limit(count: usize) -> logical::PureLogicalOp {
        logical::PureLogicalOp::Limit {
            count: ir::StreamBoundPlan::Literal(count),
        }
    }

    #[test]
    fn static_window_matching_distinguishes_static_and_dynamic_windows() {
        assert_eq!(
            StaticStreamWindow::from_op(&limit(3)),
            StaticStreamWindowMatch::Window(StaticStreamWindow {
                start: 0,
                end: Some(3),
            })
        );
        assert_eq!(
            StaticStreamWindow::from_op(&logical::PureLogicalOp::Limit {
                count: ir::StreamBoundPlan::Expr(
                    ir::StreamBoundExprPlan::new(helix_ast::expr::Expr::param("limit")).unwrap()
                )
            }),
            StaticStreamWindowMatch::NotWindow
        );
    }
}
