use super::contracts::MemoizedExpr;
use super::*;
use crate::{ir, logical, memo, properties};

fn variable_root_pipeline(variable: &str, count: usize) -> logical::LogicalExpr {
    logical::LogicalExpr::RootPipeline(
        logical::RootPipeline::new(
            logical::RootStream::VariableSource(logical::VariableSource::new(
                ir::NonEmptyString::new(variable).expect("test variable is non-empty"),
            )),
            ir::AtLeast::<_, 1>::from_one(logical::StreamPipelineOp::Limit {
                count: ir::StreamBoundPlan::Literal(count),
            }),
        )
        .expect("test pipeline is canonical"),
    )
}

fn insert_root(
    memoizer: &mut MemoExpressionMemoizer,
    memo: &mut memo::Memo,
    expr: logical::LogicalExpr,
) -> MemoizedExpr {
    memoizer
        .insert_root(memo, expr)
        .expect("test memo allocation should fit")
}

#[test]
fn parent_local_leaf_inputs_do_not_create_child_groups() {
    let mut memo = memo::Memo::default();
    let mut memoizer = MemoExpressionMemoizer::default();

    let first = insert_root(&mut memoizer, &mut memo, variable_root_pipeline("seed", 1));
    let first_queued = memoizer.drain_queued();
    let second = insert_root(&mut memoizer, &mut memo, variable_root_pipeline("seed", 2));
    let second_queued = memoizer.drain_queued();

    assert_eq!(memo.group_count(), 2);
    assert_eq!(first_queued.len(), 1);
    assert_eq!(second_queued.len(), 1);
    assert_eq!(first_queued[0].group, first.group);
    assert_eq!(second_queued[0].group, second.group);
    assert!(memo
        .expression(first_queued[0].expr)
        .unwrap()
        .children
        .is_empty());
    assert!(memo
        .expression(second_queued[0].expr)
        .unwrap()
        .children
        .is_empty());
}

#[test]
fn parent_local_leaf_roots_insert_independently_when_later_selected_as_roots() {
    let mut memo = memo::Memo::default();
    let mut memoizer = MemoExpressionMemoizer::default();
    let parent = insert_root(&mut memoizer, &mut memo, variable_root_pipeline("seed", 1));
    let queued = memoizer.drain_queued();
    assert_eq!(queued[0].group, parent.group);
    let child_root = logical::LogicalExpr::VariableSource(logical::VariableSource::new(
        ir::NonEmptyString::new("seed").unwrap(),
    ));

    let promoted = insert_root(&mut memoizer, &mut memo, child_root);
    let promoted_queued = memoizer.drain_queued();

    assert_ne!(promoted.group, parent.group);
    assert_eq!(promoted_queued.len(), 1);
    assert_eq!(promoted_queued[0].group, promoted.group);
    assert_eq!(memo.group_count(), 2);
}

#[test]
fn composed_child_roots_are_queued_for_same_memo_exploration() {
    let mut memo = memo::Memo::default();
    let mut memoizer = MemoExpressionMemoizer::default();
    let inner = logical::RootPipeline::new(
        logical::RootStream::VariableSource(logical::VariableSource::new(
            ir::NonEmptyString::new("seed").unwrap(),
        )),
        ir::AtLeast::<_, 1>::from_one(logical::StreamPipelineOp::Distinct),
    )
    .unwrap();
    let outer = logical::LogicalExpr::RootPipeline(
        logical::RootPipeline::new(
            logical::RootStream::Pipeline(Box::new(inner)),
            ir::AtLeast::<_, 1>::from_one(logical::StreamPipelineOp::Limit {
                count: ir::StreamBoundPlan::Literal(1),
            }),
        )
        .unwrap(),
    );

    let inserted = insert_root(&mut memoizer, &mut memo, outer);
    let queued = memoizer.drain_queued();

    assert_eq!(memo.group_count(), 2);
    assert_eq!(queued.len(), 2);
    assert_ne!(queued[0].group, inserted.group);
    assert_eq!(queued[1].group, inserted.group);
}

#[test]
fn leaf_roots_have_empty_child_groups() {
    let mut memo = memo::Memo::default();
    let mut memoizer = MemoExpressionMemoizer::default();

    let inserted = insert_root(
        &mut memoizer,
        &mut memo,
        logical::LogicalExpr::Pure(logical::PureLogicalOp::Source {
            element: properties::ElementKind::Node,
        }),
    );
    let queued = memoizer.drain_queued();

    assert!(memo.expression(queued[0].expr).unwrap().children.is_empty());
    assert_eq!(queued[0].group, inserted.group);
}
