use super::super::{memory, requirements::Requirements};
use crate::allocation_testing;
use helix_planner::relational as r;

#[test]
fn requirement_growth_is_admitted_and_duplicate_probes_allocate_nothing() {
    let limit = 1024 * 1024;
    let budget = memory::Budget::new(limit);
    let mut requirements = Requirements::new(&budget).unwrap();
    let names = (0..64).map(|i| format!("field_{i}")).collect::<Vec<_>>();
    let (result, allocations) = allocation_testing::observe(|| {
        for slot in 0..17 {
            requirements.insert(r::Slot(slot), r::PropertyRequirement::Metadata)?;
        }
        for name in &names {
            requirements.insert(r::Slot(0), r::PropertyRequirement::Key(name))?;
        }
        Ok::<_, crate::cypher::Error>(())
    });
    result.unwrap();
    let used = limit - budget.available();
    assert!(allocations.bytes <= used, "{allocations:?} exceeds {used}");
    let (result, allocations) = allocation_testing::observe(|| {
        requirements.insert(r::Slot(0), r::PropertyRequirement::Metadata)?;
        for name in &names {
            requirements.insert(r::Slot(0), r::PropertyRequirement::Key(name))?;
        }
        Ok::<_, crate::cypher::Error>(())
    });
    result.unwrap();
    assert_eq!(allocations.allocations, 0);
    assert_eq!(limit - budget.available(), used);
    let (result, allocations) = allocation_testing::observe(|| {
        requirements.insert(r::Slot(0), r::PropertyRequirement::All)?;
        requirements.insert(r::Slot(0), r::PropertyRequirement::Metadata)?;
        requirements.insert(r::Slot(0), r::PropertyRequirement::Key("ignored"))?;
        requirements.insert(r::Slot(0), r::PropertyRequirement::All)
    });
    result.unwrap();
    assert_eq!(allocations.allocations, 0);
    assert!(limit - budget.available() < used);
    assert_eq!(requirements.values()[&r::Slot(0)], r::PropertyDemand::All);
    drop(requirements);
    assert_eq!(budget.available(), limit);
}

#[test]
fn requirements_reject_growth_before_copying_names_and_preserve_previous_state() {
    let budget = memory::Budget::new(0);
    let mut empty = Requirements::new(&budget).unwrap();
    assert!(empty
        .insert(r::Slot(0), r::PropertyRequirement::Metadata)
        .is_err());
    assert!(empty.values().is_empty());
    assert_eq!(budget.available(), 0);

    let limit = 64 * 1024;
    let budget = memory::Budget::new(limit);
    let mut requirements = Requirements::new(&budget).unwrap();
    requirements
        .insert(r::Slot(0), r::PropertyRequirement::Key("kept"))
        .unwrap();
    let available = budget.available();
    let large = "x".repeat(128 * 1024);
    for slot in [r::Slot(0), r::Slot(1)] {
        let (result, allocations) = allocation_testing::observe(|| {
            requirements.insert(slot, r::PropertyRequirement::Key(&large))
        });
        assert!(
            matches!(result, Err(crate::cypher::Error::Query(error)) if error.detail == "MemoryLimit")
        );
        // The structured error owns short diagnostic strings, never the key.
        assert!(allocations.bytes < 1024, "{allocations:?}");
        assert_eq!(budget.available(), available);
        assert_eq!(requirements.values().len(), 1);
        assert!(requirements.values()[&r::Slot(0)].contains("kept"));
    }
    requirements
        .insert(r::Slot(1), r::PropertyRequirement::All)
        .unwrap();
    requirements
        .insert(r::Slot(2), r::PropertyRequirement::Metadata)
        .unwrap();
    requirements
        .insert(r::Slot(2), r::PropertyRequirement::All)
        .unwrap();
    drop(requirements);
    assert_eq!(budget.available(), limit);
}

#[test]
fn borrowed_graph_requirement_visits_do_not_copy_large_property_names() {
    let expression = r::Expression::Property(
        Box::new(r::Expression::Slot(r::Slot(0))),
        "key".repeat(64 * 1024),
    );
    let r::Expression::Property(_, key) = &expression else {
        unreachable!()
    };
    let (result, allocations) = allocation_testing::observe(|| {
        expression.try_graph_requirements(|slot, requirement| {
            assert_eq!(slot, r::Slot(0));
            let r::PropertyRequirement::Key(name) = requirement else {
                panic!("expected key")
            };
            assert_eq!(name.as_ptr(), key.as_ptr());
            Ok::<_, ()>(())
        })
    });
    result.unwrap();
    assert_eq!(allocations.allocations, 0);
}
