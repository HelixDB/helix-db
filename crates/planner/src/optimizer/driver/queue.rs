//! Exploration queue contract.

use std::collections::VecDeque;

use crate::{logical, memo, optimizer};

/// Logical expression scheduled for rule exploration.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct ExplorationTask {
    pub(super) group: memo::MemoGroupId,
    pub(super) source_expr: memo::MemoExprId,
    pub(super) expr: logical::LogicalExpr,
}

/// FIFO work queue for deterministic Cascades exploration.
pub(super) type ExplorationQueue = VecDeque<ExplorationTask>;

/// Append memoized expressions to the deterministic exploration queue.
pub(super) fn push_memoized(
    queue: &mut ExplorationQueue,
    queued: Vec<optimizer::memoize::QueuedMemoExpr>,
) {
    queue.extend(queued.into_iter().map(|queued| ExplorationTask {
        group: queued.group,
        source_expr: queued.expr,
        expr: queued.logical,
    }));
}

#[cfg(test)]
mod tests {
    use crate::{logical, memo, optimizer, properties};

    #[test]
    fn push_memoized_preserves_fifo_order_and_source_ids() {
        let source = logical::LogicalExpr::Pure(logical::PureLogicalOp::Source {
            element: properties::ElementKind::Node,
        });
        let queued = vec![
            optimizer::memoize::QueuedMemoExpr {
                group: memo::MemoGroupId::new(1).unwrap(),
                expr: memo::MemoExprId::new(2).unwrap(),
                logical: source.clone(),
            },
            optimizer::memoize::QueuedMemoExpr {
                group: memo::MemoGroupId::new(3).unwrap(),
                expr: memo::MemoExprId::new(4).unwrap(),
                logical: source.clone(),
            },
        ];
        let mut queue = super::ExplorationQueue::default();

        super::push_memoized(&mut queue, queued);

        assert_eq!(
            queue.pop_front(),
            Some(super::ExplorationTask {
                group: memo::MemoGroupId::new(1).unwrap(),
                source_expr: memo::MemoExprId::new(2).unwrap(),
                expr: source.clone(),
            })
        );
        assert_eq!(
            queue.pop_front(),
            Some(super::ExplorationTask {
                group: memo::MemoGroupId::new(3).unwrap(),
                source_expr: memo::MemoExprId::new(4).unwrap(),
                expr: source,
            })
        );
        assert!(queue.is_empty());
    }
}
