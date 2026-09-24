//! Independent allocator observations guard the standard-library layout
//! assumptions in production admission. Observation is local to one test thread;
//! construction fixtures never free owners created outside that observation.
use helix_planner::relational as r;
use std::{alloc, cell::Cell, collections::BTreeMap};

#[derive(Clone, Copy, Default)]
struct Observation {
    active: bool,
    live: usize,
    peak: usize,
    invalid: bool,
}
thread_local! {
    static OBSERVATION: Cell<Observation> = const { Cell::new(Observation { active:false,live:0,peak:0,invalid:false }) };
}
struct ObservedAllocator;
#[global_allocator]
static ALLOCATOR: ObservedAllocator = ObservedAllocator;

fn changed(old: usize, new: usize) {
    let _ = OBSERVATION.try_with(|cell| {
        let mut value = cell.get();
        if !value.active {
            return;
        }
        match value
            .live
            .checked_sub(old)
            .and_then(|live| live.checked_add(new))
        {
            Some(live) => {
                value.live = live;
                value.peak = value.peak.max(live);
            }
            None => value.invalid = true,
        }
        cell.set(value);
    });
}
// SAFETY: Each operation delegates to System with the caller's unchanged valid
// layout/pointer. Observation uses only allocation-free thread-local arithmetic.
unsafe impl alloc::GlobalAlloc for ObservedAllocator {
    unsafe fn alloc(&self, layout: alloc::Layout) -> *mut u8 {
        // SAFETY: GlobalAlloc's caller supplies a valid allocation layout.
        let pointer = unsafe { alloc::System.alloc(layout) };
        if !pointer.is_null() {
            changed(0, layout.size());
        }
        pointer
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: alloc::Layout) {
        changed(layout.size(), 0);
        // SAFETY: Pointer and layout are passed unchanged to their allocator.
        unsafe { alloc::System.dealloc(pointer, layout) };
    }
    unsafe fn alloc_zeroed(&self, layout: alloc::Layout) -> *mut u8 {
        // SAFETY: GlobalAlloc's caller supplies a valid allocation layout.
        let pointer = unsafe { alloc::System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            changed(0, layout.size());
        }
        pointer
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: alloc::Layout, size: usize) -> *mut u8 {
        // SAFETY: Forward the caller's original allocation and valid new size.
        let next = unsafe { alloc::System.realloc(pointer, layout, size) };
        if !next.is_null() {
            changed(layout.size(), size);
        }
        next
    }
}

fn observe<T>(build: impl FnOnce() -> T) -> (T, usize) {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            OBSERVATION.with(|cell| cell.set(Observation::default()));
        }
    }
    OBSERVATION.with(|cell| {
        assert!(!cell.get().active);
        cell.set(Observation {
            active: true,
            ..Default::default()
        });
    });
    let reset = Reset;
    let result = build();
    let observed = OBSERVATION.with(Cell::get);
    drop(reset);
    assert!(
        !observed.invalid,
        "an observed closure freed an unobserved allocation"
    );
    (result, observed.live)
}

#[test]
fn value_bounds_cover_sparse_dense_pruned_cloned_and_nested_maps() {
    for count in [0, 1, 5, 6, 11, 12, 63, 64, 127, 511, 4096] {
        for reverse in [false, true] {
            for retain_every in [1, 7, usize::MAX] {
                let (value, live) = observe(|| {
                    let mut map = BTreeMap::new();
                    for index in 0..count {
                        let index = if reverse { count - 1 - index } else { index };
                        map.insert(
                            format!("key{index:05}"),
                            r::Value::String("x".repeat(index % 31)),
                        );
                    }
                    let mut ordinal = 0_usize;
                    map.retain(|_, _| {
                        let keep = ordinal.is_multiple_of(retain_every);
                        ordinal += 1;
                        keep
                    });
                    r::Value::Map(map)
                });
                assert!(
                    value.allocated_bytes() >= live + size_of::<r::Value>(),
                    "count={count} reverse={reverse} retain={retain_every} bound={} live={live}",
                    value.allocated_bytes()
                );
                let (copied, live) = observe(|| value.clone());
                assert_eq!(copied, value);
                assert!(copied.allocated_bytes() >= live + size_of::<r::Value>());
            }
        }
    }
    let (empty, live) = observe(|| {
        let mut map = BTreeMap::from([("key".to_owned(), r::Value::Integer(1))]);
        map.remove("key");
        r::Value::Map(map)
    });
    assert!(empty.allocated_bytes() >= live + size_of::<r::Value>());
    for depth in [1, 4, 16, 32] {
        let (value, live) = observe(|| {
            let mut value = r::Value::String("leaf".repeat(7));
            for _ in 0..depth {
                value = r::Value::Map(BTreeMap::from([("child".to_owned(), value)]));
            }
            r::Value::List(vec![value])
        });
        assert!(value.allocated_bytes() >= live + size_of::<r::Value>());
        let (copy, live) = observe(|| value.clone());
        assert!(copy.allocated_bytes() >= live + size_of::<r::Value>());
    }
}

#[test]
fn tree_bounds_cover_graph_property_result_layout_and_wide_values() {
    for count in [0, 1, 5, 11, 12, 64, 1024] {
        let (map, live) = observe(|| {
            (0..count)
                .map(|index| {
                    let value = if index % 2 == 0 {
                        Ok(r::Value::Integer(index as i64))
                    } else {
                        Err(r::QueryError::runtime(
                            "UnsupportedFeature",
                            "StoredValue",
                            "unsupported stored property",
                        ))
                    };
                    (format!("key{index}"), value)
                })
                .collect::<BTreeMap<_, _>>()
        });
        let payload = map.iter().fold(0_usize, |bytes, (key, value)| {
            bytes
                + key.capacity()
                + match value {
                    Ok(value) => value.allocated_bytes() - size_of::<r::Value>(),
                    Err(error) => {
                        error.category.capacity()
                            + error.detail.capacity()
                            + error.message.capacity()
                    }
                }
        });
        assert!(r::allocation::btree_bytes::<String, r::Result<r::Value>>(count) + payload >= live);
        let (_, live) = observe(|| {
            (0..count)
                .map(|key| (key, [0_u8; 128]))
                .collect::<BTreeMap<_, _>>()
        });
        assert!(r::allocation::btree_bytes::<usize, [u8; 128]>(count) >= live);
    }
}

#[test]
fn hash_bounds_cover_sparse_growth_wide_values_and_duplicate_buckets() {
    use std::collections::HashMap;
    for count in [1, 3, 4, 7, 8, 14, 15, 28, 29, 64, 1024] {
        let (_, live) = observe(|| {
            let mut map = HashMap::new();
            for key in 0..count {
                map.insert(key as u64, [0_u128; 16]);
                let bound = r::allocation::hash_table_bytes::<u64, [u128; 16]>(key + 1);
                assert!(bound >= OBSERVATION.with(Cell::get).peak);
            }
            map
        });
        assert!(r::allocation::hash_table_bytes::<u64, [u128; 16]>(count) >= live);
    }
    for distinct in [1, 3, 7, 14, 28, 128, 1024] {
        let (_, live) = observe(|| {
            let mut map = HashMap::<r::GroupingKey, Vec<u64>>::new();
            for id in 0..4 * distinct + 17 {
                let key = r::GroupingKey::new(r::Value::Integer((id % distinct) as i64)).unwrap();
                map.entry(key).or_default().push(id as u64);
                let bound = r::allocation::hash_table_bytes::<r::GroupingKey, Vec<u64>>(map.len())
                    + (id + 1) * 4 * size_of::<u64>();
                assert!(bound >= OBSERVATION.with(Cell::get).peak);
            }
            map
        });
        assert!(
            r::allocation::hash_table_bytes::<r::GroupingKey, Vec<u64>>(distinct)
                + (4 * distinct + 17) * 4 * size_of::<u64>()
                >= live
        );
    }
    assert_eq!(
        r::allocation::hash_table_bytes::<(), ()>(usize::MAX),
        usize::MAX
    );
}

#[test]
fn retained_and_growth_hash_bounds_cover_each_allocator_lifetime() {
    use std::collections::HashMap;
    let (_, live) = observe(|| {
        let mut map = HashMap::<r::GroupingKey, Vec<u64>>::new();
        for id in 0..8193 {
            let key = r::GroupingKey::new(r::Value::Integer(id)).unwrap();
            let old = if map.len() == map.capacity() {
                r::allocation::hash_table_retained_bytes::<r::GroupingKey, Vec<u64>>(map.len())
            } else {
                0
            };
            let retained =
                r::allocation::hash_table_retained_bytes::<r::GroupingKey, Vec<u64>>(map.len() + 1);
            // Observe this insertion's maximum live allocation, including both
            // tables during a resize, while keeping all prior owners tracked.
            OBSERVATION.with(|cell| {
                let mut state = cell.get();
                state.peak = state.live;
                cell.set(state);
            });
            map.insert(key, Vec::new());
            let actual = OBSERVATION.with(Cell::get);
            assert!(
                retained + old >= actual.peak,
                "insertion {id}: peak {}, bound {}",
                actual.peak,
                retained + old
            );
            assert!(
                retained >= actual.live,
                "insertion {id}: retained {}, bound {retained}",
                actual.live
            );
        }
        map
    });
    assert!(r::allocation::hash_table_retained_bytes::<r::GroupingKey, Vec<u64>>(8193) >= live);
    for count in [0, 1, 3, 7, 14, 28, 112, 3584, 3585] {
        let (_, live) = observe(|| {
            let mut map = HashMap::<u64, [u128; 16]>::with_capacity(count);
            for id in 0..count {
                map.insert(id as u64, [0; 16]);
            }
            map
        });
        assert!(r::allocation::hash_table_retained_bytes::<u64, [u128; 16]>(count) >= live);
    }
    assert_eq!(
        r::allocation::hash_table_retained_bytes::<(), ()>(usize::MAX),
        usize::MAX
    );
}

#[test]
fn shape_validation_has_no_heap_frontier_for_scalar_wide_or_deep_values() {
    let mut deep = r::Value::Integer(7);
    for _ in 0..47 {
        deep = r::Value::Map(BTreeMap::from([("child".into(), deep)]));
    }
    for value in [
        r::Value::Integer(1),
        r::Value::List(vec![r::Value::Integer(1); 199_999]),
        deep,
    ] {
        let (peak, live) = observe(|| {
            value.validate_shape().unwrap();
            value.validate_depth().unwrap();
            OBSERVATION.with(Cell::get).peak
        });
        assert_eq!(
            (peak, live),
            (0, 0),
            "shape validation must not allocate a work queue"
        );
    }
    for value in [
        r::Value::List(vec![r::Value::Null; 200_000]),
        (0..48).fold(r::Value::Null, |value, _| r::Value::List(vec![value])),
    ] {
        let error = value.validate_shape().unwrap_err();
        assert_eq!(error.detail, "ValueDepth");
    }
}

#[test]
fn runtime_depth_rejection_allocates_only_the_error_before_borrowed_value_cloning() {
    let value = (0..96).fold(r::Value::String("x".repeat(1024 * 1024)), |value, _| {
        r::Value::List(vec![value])
    });
    let parameters = BTreeMap::from([("x".into(), value.clone())]);
    let row = [value.clone()];
    let evaluation = r::Evaluation {
        row: &row,
        parameters: &parameters,
        graph: &NoGraph,
        group: None,
        max_collection_items: usize::MAX,
        max_value_bytes: usize::MAX,
    };
    for expression in [
        r::Expression::Literal(value),
        r::Expression::Slot(r::Slot(0)),
        r::Expression::Parameter("x".into()),
    ] {
        let ((error, peak), live) = observe(|| {
            let error = evaluation.eval(&expression).unwrap_err();
            (error, OBSERVATION.with(Cell::get).peak)
        });
        assert_eq!(error.detail, "ValueDepth");
        assert_eq!(error.phase, r::ErrorPhase::Runtime);
        let error_bytes =
            error.category.capacity() + error.detail.capacity() + error.message.capacity();
        assert_eq!((peak, live), (error_bytes, error_bytes));
    }
}

struct NoGraph;
impl r::GraphValues for NoGraph {
    fn properties(&self, _: r::Entity) -> r::Result<&r::GraphProperties> {
        panic!("scalar value requested graph data")
    }
    fn label(&self, _: r::Entity) -> r::Result<Option<&str>> {
        panic!("scalar value requested graph data")
    }
}

#[test]
fn case_conversion_admission_covers_live_buffers_and_rejection() {
    for fragment in ["", "aBc", "Σ", "ΟΣ", "AΣ'", "İ", "K", "ΐ", "ﬃ", "猫😀"] {
        let source = fragment.repeat(4096);
        let parameters = BTreeMap::from([("text".into(), r::Value::String(source.clone()))]);
        for function in [r::Function::ToLower, r::Function::ToUpper] {
            let expected = if function == r::Function::ToLower {
                source.to_lowercase()
            } else {
                source.to_uppercase()
            };
            let expression =
                r::Expression::Function(function, vec![r::Expression::Parameter("text".into())]);
            let mut succeeded = false;
            for allowance in [
                source.len(),
                source.len() * 3 / 2,
                source.len() * 2,
                source.len() * 3,
                source.len() * 4,
                source.len() * 8,
            ] {
                let budget = allowance + 256;
                let evaluation = r::Evaluation {
                    row: &[],
                    parameters: &parameters,
                    graph: &NoGraph,
                    group: None,
                    max_collection_items: 10,
                    max_value_bytes: budget,
                };
                let ((result, peak), _) = observe(|| {
                    let result = evaluation.eval(&expression);
                    (result, OBSERVATION.with(Cell::get).peak)
                });
                assert!(
                    peak <= budget,
                    "{fragment:?} {function:?}: live allocation peak {peak} exceeds {budget}"
                );
                match result {
                    Ok(result) => {
                        assert_eq!(result, r::Value::String(expected.clone()));
                        succeeded = true;
                        if source.is_ascii() {
                            assert!(
                                peak <= source.len() + 2 * size_of::<r::Value>(),
                                "ASCII conversion copied its argument: {peak}"
                            );
                        }
                    }
                    Err(error) => {
                        assert!(
                            !source.is_ascii(),
                            "owned ASCII conversion needs no new buffer"
                        );
                        assert_eq!(
                            (&*error.category, &*error.detail),
                            ("ResourceLimit", "MemoryLimit")
                        );
                        assert!(
                            peak < source.len() + 256,
                            "rejected Unicode output was allocated: {peak}"
                        );
                    }
                }
            }
            assert!(
                succeeded,
                "{fragment:?} {function:?}: larger budget must succeed"
            );
        }
        assert_eq!(parameters["text"], r::Value::String(source));
    }
}

#[test]
fn case_conversion_matches_all_unicode_scalars_and_contextual_mapping() {
    let parameters = BTreeMap::new();
    let evaluation = r::Evaluation {
        row: &[],
        parameters: &parameters,
        graph: &NoGraph,
        group: None,
        max_collection_items: 10,
        max_value_bytes: 4 * 1024 * 1024,
    };
    let check = |source: &str| {
        for function in [r::Function::ToLower, r::Function::ToUpper] {
            let expected = if function == r::Function::ToLower {
                source.to_lowercase()
            } else {
                source.to_uppercase()
            };
            // Guard the standard-library capacity assumption independently of
            // the evaluator's implementation and memory-limit decisions.
            let mapped = source
                .chars()
                .map(|c| {
                    if function == r::Function::ToLower {
                        c.to_lowercase().map(char::len_utf8).sum::<usize>()
                    } else {
                        c.to_uppercase().map(char::len_utf8).sum::<usize>()
                    }
                })
                .sum::<usize>();
            assert_eq!(expected.len(), mapped);
            let bound = if mapped > source.len() {
                mapped.saturating_mul(2).max(8)
            } else {
                source.len()
            };
            assert!(
                expected.capacity() <= bound,
                "case conversion grew beyond the admitted capacity"
            );
            let expression = r::Expression::Function(
                function,
                vec![r::Expression::Literal(r::Value::String(source.into()))],
            );
            assert_eq!(
                evaluation.eval(&expression).unwrap(),
                r::Value::String(expected),
                "{source:?} {function:?}"
            );
        }
    };
    let mut bytes = [0_u8; 4];
    for c in (0..=0x10ffff).filter_map(char::from_u32) {
        check(c.encode_utf8(&mut bytes));
    }
    let fragments = [
        "",
        "A",
        "Σ",
        "ΟΣ",
        "ΣΣ",
        "Ο\u{301}Σ",
        "Σ\u{301}Ο",
        "\u{200d}",
        "'",
        "İ",
        "K",
        "ΐ",
        "ﬃ",
        "猫😀",
        "aBcDeFgHiJkLmNoPqRsTuVwXyZ",
    ];
    for before in fragments {
        for middle in fragments {
            for after in fragments {
                check(&format!("{before}{middle}{after}"));
            }
        }
    }
    for source in fragments {
        for count in [1, 2, 3, 7, 8, 15, 16, 31, 32, 255, 4096] {
            check(&source.repeat(count));
        }
    }
}

#[test]
fn property_keys_admit_one_output_buffer_and_do_not_evaluate_values() {
    struct Graph(r::GraphProperties);
    impl r::GraphValues for Graph {
        fn properties(&self, _: r::Entity) -> r::Result<&r::GraphProperties> {
            Ok(&self.0)
        }
        fn label(&self, _: r::Entity) -> r::Result<Option<&str>> {
            panic!("keys requested a label")
        }
    }
    let parameters = BTreeMap::new();
    for count in [0, 1, 3, 4, 12, 257] {
        let graph = Graph(
            (0..count)
                .map(|i| {
                    (
                        format!("key{i:04}😀"),
                        Err(r::QueryError::unsupported("StoredValue")),
                    )
                })
                .collect(),
        );
        let budget = 2 * size_of::<r::Value>()
            + count * size_of::<r::Value>()
            + graph.0.keys().map(String::len).sum::<usize>();
        for entity in [r::Entity::Node(1), r::Entity::Relationship(1)] {
            let expression = r::Expression::Function(
                r::Function::Keys,
                vec![r::Expression::Literal(r::Value::Entity(entity))],
            );
            for (max_value_bytes, fits) in [(budget, true), (budget - 1, false)] {
                let evaluation = r::Evaluation {
                    row: &[],
                    parameters: &parameters,
                    graph: &graph,
                    group: None,
                    max_collection_items: 1024,
                    max_value_bytes,
                };
                let ((result, peak), _) = observe(|| {
                    let result = evaluation.eval(&expression);
                    (result, OBSERVATION.with(Cell::get).peak)
                });
                if fits {
                    assert!(
                        peak <= max_value_bytes,
                        "{count} keys: live allocation peak {peak} exceeds {max_value_bytes}"
                    );
                    assert_eq!(
                        result.unwrap(),
                        r::Value::List(graph.0.keys().cloned().map(r::Value::String).collect())
                    );
                } else {
                    let error = result.unwrap_err();
                    assert_eq!(
                        (&*error.category, &*error.detail),
                        ("ResourceLimit", "MemoryLimit")
                    );
                    let error_bytes = error.category.capacity()
                        + error.detail.capacity()
                        + error.message.capacity();
                    assert_eq!(
                        peak,
                        size_of::<r::Value>() + error_bytes,
                        "rejection allocated output keys"
                    );
                }
            }
        }
    }
}

#[test]
fn map_keys_admit_output_slots_while_the_owned_map_is_live() {
    let parameters = BTreeMap::new();
    for count in [0, 1, 3, 4, 12, 257] {
        let map = r::Value::Map(
            (0..count)
                .map(|i| {
                    (
                        format!("key{i:04}😀"),
                        r::Value::String("payload".repeat(128)),
                    )
                })
                .collect(),
        );
        // Match the exact key capacities of the evaluated argument clone.
        let map = map.clone();
        let budget = map.allocated_bytes() + (count + 1) * size_of::<r::Value>();
        let expected = match &map {
            r::Value::Map(values) => {
                r::Value::List(values.keys().cloned().map(r::Value::String).collect())
            }
            _ => unreachable!(),
        };
        let expression =
            r::Expression::Function(r::Function::Keys, vec![r::Expression::Literal(map)]);
        for (max_value_bytes, fits) in [(budget, true), (budget - 1, false)] {
            let evaluation = r::Evaluation {
                row: &[],
                parameters: &parameters,
                graph: &NoGraph,
                group: None,
                max_collection_items: 1024,
                max_value_bytes,
            };
            let ((result, peak), _) = observe(|| {
                let result = evaluation.eval(&expression);
                (result, OBSERVATION.with(Cell::get).peak)
            });
            assert!(
                peak <= max_value_bytes,
                "{count} map keys exceeded live allocation allowance"
            );
            if fits {
                assert_eq!(result.unwrap(), expected);
            } else {
                let error = result.unwrap_err();
                assert_eq!(
                    (&*error.category, &*error.detail),
                    ("ResourceLimit", "MemoryLimit")
                );
            }
        }
    }
}
