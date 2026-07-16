use super::*;

#[tokio::test]
async fn variable_dispatch_handles_source_storage_selection_and_filters() {
    let db = test_support::open_db("stream-sets-variable-dispatch").await;
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    let saved = name("saved");

    assert_eq!(
        ctx.variable(
            stream(&[1, 2]),
            &exec::ExecVariableOp::Stream(ir::StreamVariableOp::As(saved.clone())),
        )
        .unwrap(),
        stream(&[1, 2])
    );
    let stored = name("stored");
    assert_eq!(
        ctx.variable(
            stream(&[8]),
            &exec::ExecVariableOp::Stream(ir::StreamVariableOp::Store(stored.clone())),
        )
        .unwrap(),
        stream(&[8])
    );
    assert_eq!(
        ctx.variable(
            stream(&[9]),
            &exec::ExecVariableOp::Stream(ir::StreamVariableOp::Select(stored)),
        )
        .unwrap(),
        stream(&[8])
    );
    assert_eq!(
        ctx.variable(
            stream(&[9]),
            &exec::ExecVariableOp::SourceInject {
                variable: saved.clone(),
            },
        )
        .unwrap(),
        stream(&[1, 2])
    );
    assert_eq!(
        ctx.variable(
            stream(&[9]),
            &exec::ExecVariableOp::Stream(ir::StreamVariableOp::Select(saved.clone())),
        )
        .unwrap(),
        stream(&[1, 2])
    );
    assert_eq!(
        row_ids(expect_stream(
            ctx.variable(
                stream(&[3]),
                &exec::ExecVariableOp::Stream(ir::StreamVariableOp::Inject(saved.clone())),
            )
            .unwrap(),
            "injected",
        )),
        vec![3, 1, 2]
    );
    assert_eq!(
        row_ids(expect_stream(
            ctx.variable(
                stream(&[1, 2, 3]),
                &exec::ExecVariableOp::Stream(ir::StreamVariableOp::Within(saved.clone())),
            )
            .unwrap(),
            "within",
        )),
        vec![1, 2]
    );
    assert_eq!(
        row_ids(expect_stream(
            ctx.variable(
                stream(&[1, 2, 3]),
                &exec::ExecVariableOp::Stream(ir::StreamVariableOp::Without(saved)),
            )
            .unwrap(),
            "without",
        )),
        vec![3]
    );
}

#[tokio::test]
async fn variable_dispatch_binds_and_rejects_missing_variables() {
    let db = test_support::open_db("stream-sets-variable-bind").await;
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    let binding = name("current");

    let bound = expect_stream(
        ctx.variable(
            stream(&[4]),
            &exec::ExecVariableOp::Stream(ir::StreamVariableOp::Bind(binding.clone())),
        )
        .unwrap(),
        "bound",
    );
    assert_eq!(bound[0].bindings.get(&binding), Some(&ElementRef::Node(4)));
    assert!(error_message(ctx.variable(
        stream(&[1]),
        &exec::ExecVariableOp::Stream(ir::StreamVariableOp::Select(name("missing"))),
    ))
    .contains("variable `missing` is not bound"));
}

#[test]
fn bind_and_filter_helpers_handle_rows_without_current_elements() {
    let binding = name("current");
    let bound = set_variables::bind_rows(
        vec![
            ExecutionRow::empty(),
            ExecutionRow::current(ElementRef::Node(1)),
        ],
        &binding,
    );
    assert!(!bound[0].bindings.contains_key(&binding));
    assert_eq!(bound[1].bindings.get(&binding), Some(&ElementRef::Node(1)));

    let allowed = BTreeSet::from([ElementRef::Node(1)]);
    assert_eq!(
        row_ids(set_variables::filter_within_rows(
            vec![
                ExecutionRow::empty(),
                ExecutionRow::current(ElementRef::Node(1)),
                ExecutionRow::current(ElementRef::Node(2)),
            ],
            &allowed,
        )),
        vec![1]
    );

    let without = set_variables::filter_without_rows(
        vec![
            ExecutionRow::empty(),
            ExecutionRow::current(ElementRef::Node(1)),
            ExecutionRow::current(ElementRef::Node(2)),
        ],
        &allowed,
    );
    assert_eq!(without.len(), 2);
    assert!(without[0].current.is_none());
    assert_eq!(row_ids(vec![without[1].clone()]), vec![2]);
}

#[test]
fn bind_rows_snapshot_current_virtual_properties() {
    let binding = name("current");
    let properties = RowVirtualProperties::from_one(
        name("score"),
        crate::encoding::property::property_value::PropertyValue::I64(7),
    );
    let bound = set_variables::bind_rows(
        vec![ExecutionRow::current_with_virtual_properties(
            ElementRef::Node(1),
            properties.clone(),
        )],
        &binding,
    );

    assert_eq!(
        bound[0].binding_virtual_properties.get(&binding),
        Some(&properties)
    );
}
