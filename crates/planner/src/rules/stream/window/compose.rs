//! Static stream-window composition algorithm.

use super::contracts;
use crate::{ir, logical};

pub(super) fn compose_static_stream_windows(
    ops: &[logical::PureLogicalOp],
) -> contracts::StreamWindowComposition {
    let mut composed = Vec::with_capacity(ops.len());
    let mut pending = None::<contracts::StaticStreamWindow>;

    for op in ops {
        match contracts::StaticStreamWindow::from_op(op) {
            contracts::StaticStreamWindowMatch::Window(window) => {
                pending = match pending {
                    Some(pending) => match pending.compose(window) {
                        contracts::StaticStreamWindowComposition::Window(window) => Some(window),
                        contracts::StaticStreamWindowComposition::Invalid => {
                            return contracts::StreamWindowComposition::NotApplicable;
                        }
                    },
                    None => Some(window),
                };
            }
            contracts::StaticStreamWindowMatch::NotWindow => {
                if !flush_static_stream_window(&mut pending, &mut composed) {
                    return contracts::StreamWindowComposition::NotApplicable;
                }
                composed.push(op.clone());
            }
        }
    }
    if !flush_static_stream_window(&mut pending, &mut composed) {
        return contracts::StreamWindowComposition::NotApplicable;
    }

    if composed.as_slice() == ops {
        contracts::StreamWindowComposition::NotApplicable
    } else {
        ir::AtLeast::<_, 1>::try_from_vec(composed).map_or(
            contracts::StreamWindowComposition::NotApplicable,
            contracts::StreamWindowComposition::Rewritten,
        )
    }
}

fn flush_static_stream_window(
    pending: &mut Option<contracts::StaticStreamWindow>,
    composed: &mut Vec<logical::PureLogicalOp>,
) -> bool {
    if let Some(window) = pending.take() {
        match window.into_op() {
            contracts::StaticStreamWindowOutput::Invalid => return false,
            contracts::StaticStreamWindowOutput::Identity => {}
            contracts::StaticStreamWindowOutput::Op(op) => composed.push(op),
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limit(count: usize) -> logical::PureLogicalOp {
        logical::PureLogicalOp::Limit {
            count: ir::StreamBoundPlan::Literal(count),
        }
    }

    fn skip(count: usize) -> logical::PureLogicalOp {
        logical::PureLogicalOp::Skip {
            count: ir::StreamBoundPlan::Literal(count),
        }
    }

    #[test]
    fn stream_window_composition_reports_rewrites_and_noops_explicitly() {
        let rewritten = compose_static_stream_windows(&[skip(2), limit(5)]);
        assert!(matches!(
            rewritten,
            contracts::StreamWindowComposition::Rewritten(ops)
                if matches!(
                    ops.as_ref(),
                    [logical::PureLogicalOp::Range {
                        range: ir::StreamRangePlan::Literal(range)
                    }] if range.start() == 2 && range.end() == 7
                )
        ));

        assert_eq!(
            compose_static_stream_windows(&[limit(3)]),
            contracts::StreamWindowComposition::NotApplicable
        );
    }
}
