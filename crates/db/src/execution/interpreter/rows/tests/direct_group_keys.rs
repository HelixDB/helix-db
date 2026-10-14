use super::*;
use crate::allocation_testing;
use std::collections::BTreeMap;

#[test]
fn grouped_key_transfer_preserves_first_values_and_allocates_nothing() {
    let items = r::ProjectionProgram::new(vec![
        r::Projection {
            slot: r::Slot(2),
            expression: r::Expression::Slot(r::Slot(0)),
        },
        r::Projection {
            slot: r::Slot(1),
            expression: r::Expression::Aggregate {
                function: r::Aggregate::Count,
                argument: None,
                distinct: false,
            },
        },
        r::Projection {
            slot: r::Slot(0),
            expression: r::Expression::Slot(r::Slot(2)),
        },
    ])
    .unwrap();
    let values = vec![
        r::Value::String("key".repeat(32 * 1024)),
        r::Value::List(vec![r::Value::Null, r::Value::Integer(1)]),
        r::Value::Map(BTreeMap::from([("key".into(), r::Value::Integer(1))])),
        r::Value::Null,
    ];
    let budget = memory::Budget::new(1024 * 1024);
    let mut groups = GroupBuffer::new(&budget).unwrap();
    for value in &values {
        let key = r::GroupingKey::row(vec![value.clone(), r::Value::Integer(1)]).unwrap();
        let reservation = budget.reserve(key.value().allocated_bytes()).unwrap();
        let index = groups
            .insert(
                key,
                reservation,
                None,
                3,
                &ProjectionInputs::Discard,
                &[(r::Aggregate::Count, None, false)],
            )
            .unwrap();
        let duplicate = r::GroupingKey::row(vec![value.clone(), r::Value::Float(1.0)]).unwrap();
        assert_eq!(groups.index(&duplicate), Some(index));
    }
    let mut checkpoints = 0;
    let (result, allocations) = allocation_testing::observe(|| {
        groups.into_drain(&items, || {
            checkpoints += 1;
            Ok(())
        })
    });
    assert_eq!(allocations.allocations, 0);
    assert_eq!(checkpoints, values.len());
    let drain = result.unwrap();
    for (group, expected) in drain.groups.zip(values) {
        assert!(matches!(group.base[0], r::Value::Integer(1)));
        assert_eq!(group.base[1], r::Value::Null);
        assert_eq!(group.base[2], expected);
    }
    drop(drain.memory);
    assert_eq!(budget.available(), 1024 * 1024);
}

#[test]
fn cancelling_group_key_transfer_releases_all_remaining_owners() {
    let items = r::ProjectionProgram::new(vec![r::Projection {
        slot: r::Slot(0),
        expression: r::Expression::Slot(r::Slot(0)),
    }])
    .unwrap();
    for cancel_after in 0..=3 {
        let budget = memory::Budget::new(128 * 1024);
        let mut groups = GroupBuffer::new(&budget).unwrap();
        for index in 0..3 {
            let key = r::GroupingKey::row(vec![r::Value::String(format!(
                "{index}{}",
                "k".repeat(4096)
            ))])
            .unwrap();
            let reservation = budget.reserve(key.value().allocated_bytes()).unwrap();
            groups
                .insert(key, reservation, None, 1, &ProjectionInputs::Discard, &[])
                .unwrap();
        }
        let mut checkpoints = 0;
        let result = groups.into_drain(&items, || {
            if checkpoints == cancel_after {
                return Err(Error::Storage(crate::HelixDbError::QueryDeadlineExceeded));
            }
            checkpoints += 1;
            Ok(())
        });
        assert_eq!(result.is_ok(), cancel_after == 3);
        drop(result);
        assert_eq!(budget.available(), 128 * 1024);
    }
}
