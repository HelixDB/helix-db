use super::*;
use crate::{allocation_testing, HelixDbError};

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
