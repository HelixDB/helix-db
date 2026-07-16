use super::{AtLeast, ElementIds};

fn element_ids(values: Vec<u64>) -> ElementIds {
    ElementIds::new(AtLeast::<_, 1>::try_from_vec(values).expect("test IDs are non-empty"))
        .expect("test IDs are unique")
}

#[test]
fn element_ids_slice_preserves_order_and_uniqueness_contract() {
    let ids = element_ids(vec![10, 20, 30, 40]);

    assert_eq!(ids.slice(1..4).unwrap().as_ref(), &[20, 30, 40]);
    assert_eq!(ids.slice(0..4).unwrap().as_ref(), &[10, 20, 30, 40]);
    assert_eq!(ids.slice(2..3).unwrap().as_ref(), &[30]);
}

#[test]
fn element_ids_slice_rejects_empty_or_invalid_ranges() {
    let ids = element_ids(vec![10, 20, 30, 40]);
    let inverted_start = ids.as_ref().len() - 1;
    let inverted_end = inverted_start - 1;

    assert!(ids.slice(2..2).is_none());
    assert!(ids.slice(inverted_start..inverted_end).is_none());
    assert!(ids.slice(4..5).is_none());
}
