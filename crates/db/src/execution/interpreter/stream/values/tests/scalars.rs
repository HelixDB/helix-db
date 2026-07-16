use super::*;

#[test]
fn scalar_items_normalize_terminal_values() {
    let values = vec![
        ExecutionScalar::Value(DbPropertyValue::I64(1)),
        ExecutionScalar::NodeId(1),
    ];

    assert_eq!(
        value_scalars::scalar_items(ExecutionValue::Count(3)),
        vec![ExecutionScalar::Value(DbPropertyValue::I64(3))]
    );
    assert_eq!(
        value_scalars::scalar_items(ExecutionValue::Bool(false)),
        vec![ExecutionScalar::Value(DbPropertyValue::Bool(false))]
    );
    assert_eq!(
        value_scalars::scalar_items(ExecutionValue::Scalars(values.clone())),
        values
    );
    assert_eq!(
        value_scalars::scalar_items(ExecutionValue::Count(usize::MAX)),
        vec![ExecutionScalar::Value(DbPropertyValue::I64(i64::MAX))]
    );
}

#[test]
#[should_panic(expected = "scalar_items is only called for scalar execution values")]
fn scalar_items_panics_for_stream_values() {
    let _ = value_scalars::scalar_items(ExecutionValue::Stream(Vec::new()));
}

#[test]
fn scalar_windows_and_distinct_are_deterministic() {
    let values = vec![
        ExecutionScalar::Value(DbPropertyValue::I64(1)),
        ExecutionScalar::Value(DbPropertyValue::I64(2)),
        ExecutionScalar::Value(DbPropertyValue::I64(1)),
        ExecutionScalar::Value(DbPropertyValue::Bool(true)),
        ExecutionScalar::NodeId(1),
        ExecutionScalar::EdgeId(1),
    ];

    assert_eq!(
        value_scalars::limit_scalars(values.clone(), 2),
        values[..2].to_vec()
    );
    assert_eq!(
        value_scalars::skip_scalars(values.clone(), 2),
        values[2..].to_vec()
    );
    assert_eq!(
        value_scalars::slice_scalars(values.clone(), 1, 3),
        values[1..3].to_vec()
    );
    assert_eq!(
        value_scalars::slice_scalars(values.clone(), 4, 2),
        Vec::new()
    );
    assert_eq!(
        value_scalars::distinct_scalars(values),
        vec![
            ExecutionScalar::Value(DbPropertyValue::I64(1)),
            ExecutionScalar::Value(DbPropertyValue::I64(2)),
            ExecutionScalar::Value(DbPropertyValue::Bool(true)),
            ExecutionScalar::NodeId(1),
            ExecutionScalar::EdgeId(1),
        ]
    );
}
