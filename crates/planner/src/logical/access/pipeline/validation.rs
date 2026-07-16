//! Canonical stream-pipeline validation and effect folding.

use crate::properties;

use super::op::StreamPipelineOp;

pub(in crate::logical) fn validate_stream_pipeline_ops(ops: &[StreamPipelineOp]) -> Option<()> {
    let mut previous_was_window = false;
    for op in ops {
        let is_window = matches!(op, StreamPipelineOp::Window { .. });
        if matches!(op, StreamPipelineOp::Window { window } if window.is_identity())
            || (previous_was_window && is_window)
        {
            return None;
        }
        previous_was_window = is_window;
    }
    Some(())
}

pub(in crate::logical) fn pipeline_ops_effect(ops: &[StreamPipelineOp]) -> properties::EffectKind {
    ops.iter().fold(properties::EffectKind::Pure, |effect, op| {
        combine_effect(effect, op.effect())
    })
}

pub(in crate::logical) fn combine_effect(
    left: properties::EffectKind,
    right: properties::EffectKind,
) -> properties::EffectKind {
    if matches!(left, properties::EffectKind::Barrier)
        || matches!(right, properties::EffectKind::Barrier)
    {
        properties::EffectKind::Barrier
    } else {
        properties::EffectKind::Pure
    }
}

#[cfg(test)]
mod tests {
    use crate::ir;
    use crate::logical::access::AccessWindowRange;
    use crate::logical::variables::StreamVariableWriteOp;

    use super::*;

    fn window(start: usize, end: Option<usize>) -> StreamPipelineOp {
        StreamPipelineOp::Window {
            window: AccessWindowRange::new(start, end).unwrap(),
        }
    }

    fn limit() -> StreamPipelineOp {
        StreamPipelineOp::Limit {
            count: ir::StreamBoundPlan::Literal(1),
        }
    }

    #[test]
    fn validation_rejects_identity_and_adjacent_windows() {
        assert!(validate_stream_pipeline_ops(&[limit()]).is_some());
        assert!(validate_stream_pipeline_ops(&[window(0, None)]).is_none());
        assert!(validate_stream_pipeline_ops(&[window(1, None), window(2, None)]).is_none());
        assert!(
            validate_stream_pipeline_ops(&[window(1, None), limit(), window(2, None)]).is_some()
        );
    }

    #[test]
    fn pipeline_effect_is_barrier_when_any_operator_writes_state() {
        let variable = ir::NonEmptyString::new("rows").unwrap();
        let ops = [
            limit(),
            StreamPipelineOp::VariableWrite {
                op: StreamVariableWriteOp::Store(variable),
            },
        ];

        assert_eq!(pipeline_ops_effect(&ops), properties::EffectKind::Barrier);
        assert_eq!(
            combine_effect(properties::EffectKind::Pure, properties::EffectKind::Pure),
            properties::EffectKind::Pure
        );
        assert_eq!(
            combine_effect(
                properties::EffectKind::Barrier,
                properties::EffectKind::Pure
            ),
            properties::EffectKind::Barrier
        );
        assert_eq!(
            combine_effect(
                properties::EffectKind::Pure,
                properties::EffectKind::Barrier
            ),
            properties::EffectKind::Barrier
        );
    }
}
