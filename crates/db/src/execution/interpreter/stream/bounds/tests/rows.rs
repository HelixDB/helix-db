use super::*;

#[test]
fn row_bound_helpers_preserve_order_and_saturate_empty_windows() {
    assert_eq!(
        row_ids(ExecutionValue::Stream(row_bounds::limit_rows(
            rows(&[1, 2, 3]),
            2,
        ))),
        vec![1, 2]
    );
    assert_eq!(
        row_ids(ExecutionValue::Stream(row_bounds::limit_rows(
            rows(&[1, 2, 3]),
            8,
        ))),
        vec![1, 2, 3]
    );
    assert_eq!(
        row_ids(ExecutionValue::Stream(row_bounds::skip_rows(
            rows(&[1, 2, 3]),
            1,
        ))),
        vec![2, 3]
    );
    assert_eq!(
        row_ids(ExecutionValue::Stream(row_bounds::skip_rows(
            rows(&[1, 2, 3]),
            8,
        ))),
        Vec::<u64>::new()
    );
    assert_eq!(
        row_ids(ExecutionValue::Stream(row_bounds::slice_rows(
            rows(&[1, 2, 3, 4]),
            1,
            3,
        ))),
        vec![2, 3]
    );
    assert_eq!(
        row_ids(ExecutionValue::Stream(row_bounds::slice_rows(
            rows(&[1, 2, 3]),
            3,
            1,
        ))),
        Vec::<u64>::new()
    );
}
