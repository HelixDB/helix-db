use super::*;
use crate::{allocation_testing, encoding::v2::values::property::Property};

#[test]
fn encoding_admits_exact_output_and_releases_request_ownership_separately_from_storage() {
    for size in [0, 1, 7, 8, 31, 32, 4095, 4096, 65536] {
        let properties = vec![Property::bytes("native", vec![7; size])];
        let prepared = property::write::Prepared::new(&properties).unwrap();
        let budget = Budget::new(1024 * 1024);
        let encoded = Encoded::new(&prepared, &budget).unwrap();
        assert_eq!(encoded.bytes, property::encode_properties(&properties));
        let remaining = budget.available();
        assert!(remaining < 1024 * 1024 - size);
        let cloned = encoded.clone();
        let storage = cloned.bytes.clone();
        assert_eq!(storage.as_ptr(), encoded.bytes.as_ptr());
        drop(encoded);
        assert_eq!(budget.available(), remaining);
        drop(cloned);
        assert_eq!(budget.available(), 1024 * 1024);
        assert_eq!(property::decode_properties(&storage).unwrap(), properties);
        for limit in [0, prepared.scratch_bytes(), budget.peak() - 1] {
            let denied = Budget::new(limit);
            assert!(matches!(
                Encoded::new(&prepared, &denied),
                Err(HelixDbError::QueryMemoryLimitExceeded)
            ));
            assert_eq!(denied.available(), limit);
        }
    }
    let budget = Budget::new(0);
    let prepared = property::write::Prepared::new(&[]).unwrap();
    let (encoded, allocation) = allocation_testing::observe(|| Encoded::new(&prepared, &budget));
    assert!(encoded.unwrap().bytes.is_empty());
    assert_eq!(allocation.allocations, 0);
}

#[test]
fn exact_encoding_allocates_no_output_until_the_entire_buffer_is_admitted() {
    let properties = vec![Property::bytes("native", vec![7; 65536])];
    let prepared = property::write::Prepared::new(&properties).unwrap();
    let budget = Budget::new(prepared.scratch_bytes());
    let (result, allocation) = allocation_testing::observe(|| Encoded::new(&prepared, &budget));
    assert!(matches!(
        result,
        Err(HelixDbError::QueryMemoryLimitExceeded)
    ));
    assert_eq!(
        allocation.bytes,
        prepared.scratch_bytes(),
        "failed output admission cannot allocate a payload buffer"
    );
    assert_eq!(budget.available(), prepared.scratch_bytes());
    let budget = Budget::new(128 * 1024);
    let (result, allocation) = allocation_testing::observe(|| Encoded::new(&prepared, &budget));
    let encoded = result.unwrap();
    assert!(allocation.bytes <= budget.peak());
    assert!(
        budget.peak() < encoded.bytes.len() + 1024,
        "encoding never allocates growth or trimming buffers"
    );
}
