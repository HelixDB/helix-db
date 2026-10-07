use super::*;
use crate::{allocation_testing, HelixDbError};

#[test]
fn property_builder_admits_capacity_and_payload_before_clone_and_transfers_ownership() {
    let budget = Budget::new(16 * 1024);
    let mut builder = Builder::new(2, Some(&budget)).unwrap();
    let input = property::Property::bytes("clone", vec![7; 4096]);
    let before = budget.available();
    let (result, allocations) = allocation_testing::observe(|| builder.push_cloned(&input));
    result.unwrap();
    assert_eq!(before - budget.available(), allocations.bytes);
    let mut spare = String::with_capacity(1024);
    spare.push('x');
    let owned = property::Property::string("owned", spare);
    let pointer = owned.value.as_str().unwrap().as_ptr();
    let before = budget.available();
    let (result, allocations) = allocation_testing::observe(|| builder.push_owned(owned));
    result.unwrap();
    assert_eq!(allocations.allocations, 0);
    assert!(before - budget.available() >= 1024);
    let before = budget.available();
    let decoded = builder.finish();
    assert!(decoded.budget().is_some());
    assert_eq!(decoded[0], input);
    assert_eq!(decoded[1].value.as_str().unwrap().as_ptr(), pointer);
    assert_eq!(budget.available(), before);
    drop(decoded);
    assert_eq!(budget.available(), 16 * 1024);
    let mut native = Builder::new(2, None).unwrap();
    native.push_cloned(&input).unwrap();
    native.push_owned(input.clone()).unwrap();
    let native = native.finish();
    assert!(native.budget().is_none());
    assert_eq!(&*native, &[input.clone(), input]);
}

#[test]
fn property_builder_failure_does_not_allocate_or_exceed_prepared_capacity() {
    let budget = Budget::new(size_of::<property::Property>());
    let input = property::Property::bytes("value", vec![7; 8192]);
    let mut builder = Builder::new(1, Some(&budget)).unwrap();
    let (result, allocations) = allocation_testing::observe(|| builder.push_cloned(&input));
    assert!(matches!(
        result,
        Err(HelixDbError::QueryMemoryLimitExceeded)
    ));
    assert_eq!(allocations.allocations, 0);
    let (result, allocations) = allocation_testing::observe(|| builder.push_owned(input));
    assert!(matches!(
        result,
        Err(HelixDbError::QueryMemoryLimitExceeded)
    ));
    assert_eq!(allocations.allocations, 0);
    assert!(builder.finish().is_empty());
    assert_eq!(budget.available(), size_of::<property::Property>());
    let (result, allocations) =
        allocation_testing::observe(|| Builder::new(usize::MAX, Some(&budget)));
    assert!(matches!(
        result,
        Err(HelixDbError::QueryMemoryLimitExceeded)
    ));
    assert_eq!(allocations.allocations, 0);
    for cloned in [false, true] {
        let mut builder = Builder::new(0, None).unwrap();
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let input = property::Property::i64("value", 1);
            if cloned {
                builder.push_cloned(&input)
            } else {
                builder.push_owned(input)
            }
        }))
        .is_err());
    }
}

#[test]
fn shared_decoded_rows_charge_the_arc_and_release_only_after_the_last_owner() {
    let input = vec![property::Property::string("value", "x".repeat(4096))];
    let encoded = property::encode_properties(&input);
    let budget = Budget::new(64 * 1024);
    let decoded = Decoded::new(&encoded, prepared::Selection::All, Some(&budget)).unwrap();
    let before = budget.available();
    let (shared, allocation) = allocation_testing::observe(|| decoded.share().unwrap());
    assert_eq!(allocation.allocations, 1);
    assert_eq!(before - budget.available(), allocation.bytes);
    let retained = budget.available();
    let (copy, allocation) = allocation_testing::observe(|| shared.clone());
    assert_eq!(allocation.allocations, 0);
    drop(shared);
    assert_eq!(budget.available(), retained);
    assert_eq!(&**copy, input.as_slice());
    drop(copy);
    assert_eq!(budget.available(), 64 * 1024);
    let budget = Budget::new(0);
    let empty = Decoded::new(&[], prepared::Selection::All, Some(&budget)).unwrap();
    let (result, allocation) = allocation_testing::observe(|| empty.share());
    assert!(matches!(
        result,
        Err(HelixDbError::QueryMemoryLimitExceeded)
    ));
    assert_eq!(allocation.allocations, 0);
    assert_eq!(budget.available(), 0);
    let native = Decoded::native(input.clone()).share().unwrap();
    assert_eq!(&**native, input.as_slice());
    assert_eq!(
        *native,
        Decoded::new(&encoded, prepared::Selection::All, None).unwrap()
    );
    assert_eq!(format!("{native:?}"), format!("{input:?}"));
}

#[test]
fn raw_property_batches_admit_all_reads_before_any_decode_and_keep_iterators_owned() {
    let encoded =
        property::encode_properties(&[property::Property::string("value", "x".repeat(4096))]);
    let budget = Budget::new(64 * 1024);
    let request = ReadRequest::new(3, Some(&budget)).unwrap();
    let mut reads = request
        .attach(vec![Some(encoded.clone()), None, Some(encoded.clone())])
        .unwrap();
    assert_eq!(reads.len(), 3);
    let available = budget.available();
    assert!(available < 64 * 1024 - 2 * encoded.len());
    let read = reads.next().unwrap().unwrap();
    assert_eq!(read.bytes(), encoded);
    assert_eq!(budget.available(), available);
    assert!(reads.next().unwrap().is_none());
    let (bytes, decoded) = read.decode().unwrap();
    assert_eq!(decoded[0].name, "value");
    drop(decoded);
    assert_eq!(budget.available(), available);
    drop(reads);
    assert!(budget.available() > available && budget.available() < 64 * 1024);
    let copy = bytes.clone();
    drop(bytes);
    assert!(budget.available() < 64 * 1024);
    drop(copy);
    assert_eq!(budget.available(), 64 * 1024);
    let (result, allocation) =
        allocation_testing::observe(|| ReadRequest::new(usize::MAX, Some(&budget)));
    assert!(matches!(
        result,
        Err(HelixDbError::QueryMemoryLimitExceeded)
    ));
    assert_eq!(allocation.allocations, 0);
    let budget = Budget::new(encoded.len() + 1024);
    let request = ReadRequest::new(2, Some(&budget)).unwrap();
    assert!(matches!(
        request.attach(vec![Some(encoded.clone()), Some(encoded.clone())]),
        Err(HelixDbError::QueryMemoryLimitExceeded)
    ));
    assert_eq!(budget.available(), encoded.len() + 1024);
    let values = vec![None, Some(encoded.clone())];
    let (mut reads, allocation) =
        allocation_testing::observe(|| ReadRequest::new(2, None).unwrap().attach(values).unwrap());
    assert_eq!(
        allocation.allocations, 0,
        "native batches reuse result handles"
    );
    assert_eq!(reads.len(), 2);
    assert!(reads.next().unwrap().is_none());
    assert_eq!(reads.next().unwrap().unwrap().decode().unwrap().0, encoded);
    assert!(reads.next().is_none());
    let budget = Budget::new(1024);
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        ReadRequest::new(1, Some(&budget)).unwrap().attach(vec![])
    }))
    .is_err());
    assert_eq!(budget.available(), 1024);
    let read = Read::new(bytes::Bytes::from_static(b"bad"), Some(&budget)).unwrap();
    assert!(matches!(read.decode(), Err(HelixDbError::Encoding(_))));
    assert_eq!(budget.available(), 1024);
}

#[test]
fn drained_fields_retain_admission_through_success_failure_and_unwinding() {
    let encoded = property::encode_properties(&[
        property::Property::string("first", "x".repeat(4096)),
        property::Property::string("last", "y".repeat(4096)),
    ]);
    let budget = Budget::new(64 * 1024);
    for path in 0..3 {
        let decoded = Decoded::new(&encoded, prepared::Selection::All, Some(&budget)).unwrap();
        let admitted = budget.available();
        assert!(admitted < 64 * 1024 - 8192);
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            decoded.with_owned(|mut properties| {
                drop(properties.next().unwrap());
                assert_eq!(
                    budget.available(),
                    admitted,
                    "the vector and remaining fields remain owned"
                );
                match path {
                    0 => Ok(()),
                    1 => Err("conversion failed"),
                    _ => panic!("conversion panicked"),
                }
            })
        }));
        match path {
            0 => assert_eq!(outcome.unwrap(), Ok(())),
            1 => assert_eq!(outcome.unwrap(), Err("conversion failed")),
            _ => assert!(outcome.is_err()),
        }
        assert_eq!(budget.available(), 64 * 1024);
    }
}

#[test]
fn admission_precedes_alignment_and_decoding_and_follows_the_decoded_owner() {
    let input = vec![
        property::Property::string("wanted", "value".repeat(4096)),
        property::Property::i64("small", 17),
    ];
    let encoded = property::encode_properties(&input);
    for selection in [
        prepared::Selection::All,
        prepared::Selection::Names(&["small"]),
    ] {
        let archive = prepared::Archive::new(&encoded);
        let bytes = archive.prepare(selection).unwrap().owned_bytes();
        for limit in [
            0,
            encoded.len() - 1,
            encoded.len() + bytes - 1,
            encoded.len() + bytes,
        ] {
            let budget = Budget::new(limit);
            let (result, allocations) =
                allocation_testing::observe(|| Decoded::new(&encoded, selection, Some(&budget)));
            if limit < encoded.len() {
                assert!(matches!(
                    result,
                    Err(HelixDbError::QueryMemoryLimitExceeded)
                ));
                assert_eq!(allocations.allocations, 0);
            } else if limit < encoded.len() + bytes {
                assert!(matches!(
                    result,
                    Err(HelixDbError::QueryMemoryLimitExceeded)
                ));
                assert_eq!(
                    allocations.allocations, 1,
                    "only the admitted aligned copy exists"
                );
                assert_eq!(allocations.bytes, encoded.len());
            } else {
                let decoded = result.unwrap();
                assert_eq!(
                    budget.available(),
                    limit - bytes,
                    "alignment charge ends after decode"
                );
                let expected = input
                    .iter()
                    .filter(|p| selection.contains(&p.name))
                    .cloned()
                    .collect::<Vec<_>>();
                assert_eq!(&*decoded, expected.as_slice());
                let future = async move {
                    std::future::pending::<()>().await;
                    decoded
                };
                drop(future);
            }
            assert_eq!(budget.available(), limit);
        }
        assert_eq!(
            &*Decoded::new(&encoded, selection, None).unwrap(),
            input
                .iter()
                .filter(|p| selection.contains(&p.name))
                .cloned()
                .collect::<Vec<_>>()
                .as_slice()
        );
    }
    let budget = Budget::new(0);
    assert!(Decoded::new(&[], prepared::Selection::All, Some(&budget))
        .unwrap()
        .is_empty());
    assert_eq!(budget.available(), 0);
    let budget = Budget::new(128);
    assert!(matches!(
        Decoded::new(&[1, 2, 3], prepared::Selection::All, Some(&budget)),
        Err(HelixDbError::Encoding(_))
    ));
    assert_eq!(budget.available(), 128);
}
