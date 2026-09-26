use super::*;

#[tokio::test]
async fn distinct_dispatch_handles_stream_scalar_and_folded_inputs() {
    let db = test_support::open_db("stream-sets-distinct-dispatch").await;
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());

    assert_eq!(
        row_ids(expect_stream(
            ctx.distinct(stream(&[1, 2, 1])).unwrap(),
            "distinct result",
        )),
        vec![1, 2]
    );
    assert_eq!(ctx.distinct(scalars(&[7, 7, 8])).unwrap(), scalars(&[7, 8]));
    assert!(error_message(
        ctx.distinct(ExecutionValue::FoldedStream(FoldedStream::new(rows(&[
            1, 2,
        ]))))
    )
    .contains("distinct expected stream input, got folded stream"));
}

#[tokio::test]
async fn distinct_rejects_index_lifecycle_values() {
    let db = test_support::open_db("stream-sets-distinct-index-lifecycle").await;
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
    let lifecycle = ExecutionValue::IndexDdlReceipt(
        crate::index_lifecycle::IndexDdlReceipt::ExistingOperation {
            operation_id: crate::index_lifecycle::IndexOperationId::from_bytes([7; 16]).unwrap(),
        },
    );

    assert!(error_message(ctx.distinct(lifecycle))
        .contains("distinct cannot consume an index lifecycle value"));
}

#[test]
fn distinct_rows_are_order_preserving() {
    assert_eq!(
        row_ids(set_distinct::distinct_rows(rows(&[1, 2, 1, 3, 2]))),
        vec![1, 2, 3]
    );
}

#[test]
fn distinct_rows_compare_empty_row_state() {
    assert_eq!(
        set_distinct::distinct_rows(vec![ExecutionRow::empty(), ExecutionRow::empty()]).len(),
        1
    );

    let visible_empty = ExecutionRow::empty().mark_path_visible();
    let distinct = set_distinct::distinct_rows(vec![
        ExecutionRow::empty(),
        visible_empty.clone(),
        visible_empty,
    ]);
    assert_eq!(distinct.len(), 2);
    assert!(!distinct[0].path_visible);
    assert!(distinct[1].path_visible);
}

#[test]
fn distinct_rows_use_current_element_and_preserve_first_row_payload() {
    let hidden_path_a = {
        let mut row = ExecutionRow::current(ElementRef::Node(1));
        row.set_current(ElementRef::Node(2));
        row
    };
    let hidden_path_b = ExecutionRow::current(ElementRef::Node(2));
    assert_eq!(
        set_distinct::distinct_rows(vec![hidden_path_a, hidden_path_b]).len(),
        1
    );
    assert_eq!(
        set_distinct::distinct_rows(vec![visible_path_row(1, 2), visible_path_row(3, 2)]).len(),
        1
    );

    let binding = name("source");
    let mut bound_a = ExecutionRow::current(ElementRef::Node(2));
    bound_a
        .bindings
        .insert(binding.clone(), ElementRef::Node(1));
    let mut bound_b = ExecutionRow::current(ElementRef::Node(2));
    bound_b
        .bindings
        .insert(binding.clone(), ElementRef::Node(3));
    let deduped = set_distinct::distinct_rows(vec![bound_a, bound_b]);
    assert_eq!(deduped.len(), 1);
    assert_eq!(
        deduped[0].bindings.get(&binding),
        Some(&ElementRef::Node(1))
    );
}

/// DISTINCT must use the engine's numeric identity (`PropertyValue::total_order`, the
/// same identity `WHERE`, `ORDER BY`, GROUP BY and secondary indexes use), not the
/// `Debug` rendering of the storage variant. The SDKs store integral JSON numbers as
/// I64 and fractional ones as F64, so mixed-variant columns arise without any tuning.
#[tokio::test]
async fn distinct_scalars_unify_numerically_equal_values_across_storage_variants() {
    use crate::encoding::property::property_value::PropertyValue;

    let db = test_support::open_db("stream-sets-distinct-numeric-identity").await;
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());

    let values = ExecutionValue::Scalars(vec![
        ExecutionScalar::Value(PropertyValue::I64(42)),
        ExecutionScalar::Value(PropertyValue::F64(42.0)),
        ExecutionScalar::Value(PropertyValue::F32(42.0)),
        ExecutionScalar::Value(PropertyValue::I64(7)),
    ]);
    assert_eq!(
        ctx.distinct(values).unwrap(),
        ExecutionValue::Scalars(vec![
            ExecutionScalar::Value(PropertyValue::I64(42)),
            ExecutionScalar::Value(PropertyValue::I64(7)),
        ]),
        "DISTINCT over scalars must collapse numerically equal I64/F64/F32 values"
    );
}

/// Same contract for `value_map(...).dedup()`, which dedups `ExecutionScalar::Object`.
#[tokio::test]
async fn distinct_objects_unify_numerically_equal_property_values() {
    use std::collections::BTreeMap;

    use crate::encoding::property::property_value::PropertyValue;

    let db = test_support::open_db("stream-sets-distinct-numeric-identity-objects").await;
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());

    let object = |value: PropertyValue| {
        ExecutionScalar::Object(BTreeMap::from([("score".to_string(), value)]))
    };
    let values = ExecutionValue::Scalars(vec![
        object(PropertyValue::I64(42)),
        object(PropertyValue::F64(42.0)),
        object(PropertyValue::F32(42.0)),
        object(PropertyValue::I64(7)),
    ]);
    assert_eq!(
        ctx.distinct(values).unwrap(),
        ExecutionValue::Scalars(vec![
            object(PropertyValue::I64(42)),
            object(PropertyValue::I64(7)),
        ]),
        "DISTINCT over value_map objects must collapse numerically equal I64/F64/F32 values"
    );
}

/// Numeric identity does not leak across types: the string `"42"` is not the number
/// `42`, and `true` is not `1`. This mirrors `WHERE`, where `score = "42"` matches no
/// I64 row.
#[tokio::test]
async fn distinct_keeps_numeric_string_and_bool_forms_apart() {
    use crate::encoding::property::property_value::PropertyValue;

    let db = test_support::open_db("stream-sets-distinct-type-boundary").await;
    let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());

    let values = vec![
        ExecutionScalar::Value(PropertyValue::I64(42)),
        ExecutionScalar::Value(PropertyValue::String("42".to_string())),
        ExecutionScalar::Value(PropertyValue::Bool(true)),
        ExecutionScalar::Value(PropertyValue::I64(1)),
    ];
    assert_eq!(
        ctx.distinct(ExecutionValue::Scalars(values.clone()))
            .unwrap(),
        ExecutionValue::Scalars(values),
        "DISTINCT must keep a number, its string form and a bool as separate values"
    );
}
