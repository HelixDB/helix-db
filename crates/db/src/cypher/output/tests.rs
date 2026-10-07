use super::*;
use serde_json::json;

fn fixture() -> Response {
    Response {
        columns: vec!["escaped\"alias\\\n".into(), "nested".into()],
        rows: vec![vec![
            json!({"$type":"integer","value":"9223372036854775807"}),
            json!({"$type":"map","value":{"$type":"literal","values":[null,true,"large".repeat(16 * 1024)]}}),
        ]],
        diagnostics: Default::default(),
        resources: Default::default(),
    }
}

#[test]
fn encoding_uses_one_exact_buffer_and_shared_slices_retain_its_admission() {
    let response = fixture();
    let expected = serde_json::to_vec(&response).unwrap();
    let budget = Budget::new(expected.len() * 4);
    let original = budget.available();
    let memory = budget.reserve(expected.len()).unwrap();
    let (result, observed) = crate::allocation_testing::observe(|| {
        Json::prepare(response, memory, expected.len(), &budget, || Ok(()))
    });
    let prepared = result.unwrap();
    assert_eq!(prepared.body(), expected);
    assert_eq!(observed.allocations, 1);
    assert_eq!(observed.bytes, expected.len());
    let resources = ResourceUsage {
        peak_memory_bytes: budget.peak(),
        ..Default::default()
    };
    let mut prepared = Json::finish(prepared, resources);
    assert_eq!(
        Json::resources(&mut prepared).peak_memory_bytes,
        budget.peak()
    );
    assert!(format!("{prepared:?}").contains("body_bytes"));
    let pointer = prepared.body().as_ptr();
    let before = budget.available();
    let bytes = prepared.into_bytes();
    assert_eq!(bytes.as_ptr(), pointer);
    assert_eq!(budget.available(), before);
    let clone = bytes.clone();
    let slice = bytes.slice(1..1 + 2);
    drop((bytes, clone));
    assert_eq!(budget.available(), before);
    drop(slice);
    assert_eq!(budget.available(), original);
}

#[test]
fn moving_json_to_an_embedded_caller_never_copies_and_typed_output_stays_admitted() {
    let response = fixture();
    let expected = serde_json::to_vec(&response).unwrap();
    let budget = Budget::new(expected.len() * 4);
    let original = budget.available();
    let encoded = Json::prepare(
        response,
        budget.reserve(expected.len()).unwrap(),
        expected.len(),
        &budget,
        || Ok(()),
    )
    .unwrap();
    let pointer = encoded.body().as_ptr();
    let (bytes, observed) = crate::allocation_testing::observe(|| encoded.into_vec());
    assert_eq!(bytes.as_ptr(), pointer);
    assert_eq!(bytes, expected);
    assert_eq!(observed.allocations, 0);
    assert_eq!(budget.available(), original);
    let typed = Typed::prepare(
        fixture(),
        budget.reserve(100).unwrap(),
        expected.len(),
        &budget,
        || Ok(()),
    )
    .unwrap();
    assert_eq!(budget.available(), original - 100);
    let mut typed = Typed::finish(
        typed,
        ResourceUsage {
            peak_memory_bytes: 17,
            ..Default::default()
        },
    );
    assert_eq!(Typed::resources(&mut typed).peak_memory_bytes, 17);
    assert_eq!(serde_json::to_vec(&typed).unwrap(), expected);
    assert_eq!(budget.available(), original);
}

#[test]
fn failed_encoding_admission_precedes_payload_allocation_and_releases_input() {
    let response = fixture();
    let expected = serde_json::to_vec(&response).unwrap();
    let budget = Budget::new(expected.len());
    let memory = budget.reserve(1).unwrap();
    let (result, observed) = crate::allocation_testing::observe(|| {
        Json::prepare(response, memory, expected.len(), &budget, || Ok(()))
    });
    assert!(matches!(result, Err(Error::Query(error)) if error.detail == "MemoryLimit"));
    assert!(observed.bytes < expected.len());
    assert_eq!(budget.available(), expected.len());
}

#[test]
fn cancellation_and_serializer_size_failures_release_all_owned_memory() {
    for checkpoint in [0, 1, 2, 8] {
        let response = fixture();
        let length = serde_json::to_vec(&response).unwrap().len();
        let budget = Budget::new(length * 4);
        let mut checks = 0;
        let result = Json::prepare(
            response,
            budget.reserve(length).unwrap(),
            length,
            &budget,
            || {
                checks += 1;
                if checks > checkpoint {
                    Err(Error::Storage(crate::HelixDbError::QueryDeadlineExceeded))
                } else {
                    Ok(())
                }
            },
        );
        assert!(matches!(
            result,
            Err(Error::Storage(crate::HelixDbError::QueryDeadlineExceeded))
        ));
        assert_eq!(budget.available(), length * 4);
    }
    let budget = Budget::new(1024 * 1024);
    let result = Typed::prepare(fixture(), budget.reserve(1).unwrap(), 0, &budget, || {
        Err(Error::Storage(crate::HelixDbError::QueryDeadlineExceeded))
    });
    assert!(matches!(
        result,
        Err(Error::Storage(crate::HelixDbError::QueryDeadlineExceeded))
    ));
    assert_eq!(budget.available(), 1024 * 1024);
    let result = Json::prepare(fixture(), budget.reserve(1).unwrap(), 0, &budget, || Ok(()));
    assert!(matches!(result, Err(Error::Json(_))));
    assert_eq!(budget.available(), 1024 * 1024);
    let mut check = || Ok(());
    let mut writer = Writer {
        bytes: Vec::with_capacity(1),
        limit: 1,
        check: &mut check,
        failure: None,
    };
    std::io::Write::flush(&mut writer).unwrap();
    let response = fixture();
    let length = serde_json::to_vec(&response).unwrap().len();
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| Json::prepare(
            response,
            budget.reserve(1).unwrap(),
            length + 1,
            &budget,
            || Ok(())
        )))
        .is_err()
    );
    assert_eq!(budget.available(), 1024 * 1024);
}
