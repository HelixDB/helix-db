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
        memory: r::EvaluationMemory::new(usize::MAX),
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
                    memory: r::EvaluationMemory::new(budget),
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
        memory: r::EvaluationMemory::new(4 * 1024 * 1024),
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
                    memory: r::EvaluationMemory::new(max_value_bytes),
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
                memory: r::EvaluationMemory::new(max_value_bytes),
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

#[test]
fn concatenation_moves_owned_payloads_and_preserves_parameters() {
    let payload = "λ猫".repeat(16_384);
    let text = r::Value::String(payload.clone());
    let nested = r::Value::Map(BTreeMap::from([(
        "p".into(),
        r::Value::List(vec![text.clone()]),
    )]));
    let list = r::Value::List(vec![nested.clone()]);
    for (left, right, expected) in [
        (
            list.clone(),
            list.clone(),
            r::Value::List(vec![nested.clone(), nested.clone()]),
        ),
        (
            list.clone(),
            text.clone(),
            r::Value::List(vec![nested.clone(), text.clone()]),
        ),
        (
            text.clone(),
            list.clone(),
            r::Value::List(vec![text.clone(), nested.clone()]),
        ),
        (
            text.clone(),
            r::Value::String("!".into()),
            r::Value::String(format!("{payload}!")),
        ),
        (
            text.clone(),
            r::Value::Integer(i64::MIN),
            r::Value::String(format!("{payload}{}", i64::MIN)),
        ),
        (
            r::Value::Integer(i64::MIN),
            text.clone(),
            r::Value::String(format!("{}{payload}", i64::MIN)),
        ),
        (
            text.clone(),
            r::Value::Float(1.5),
            r::Value::String(format!("{payload}1.5")),
        ),
        (
            r::Value::Float(1.5),
            text.clone(),
            r::Value::String(format!("1.5{payload}")),
        ),
    ] {
        let input_bytes = left.allocated_bytes() + right.allocated_bytes();
        let parameters = BTreeMap::from([
            ("left".into(), left.clone()),
            ("right".into(), right.clone()),
        ]);
        let evaluation = r::Evaluation {
            row: &[],
            parameters: &parameters,
            graph: &NoGraph,
            group: None,
            max_collection_items: 100,
            memory: r::EvaluationMemory::new(4 * input_bytes),
        };
        let expression = r::Expression::Binary(
            r::Binary::Add,
            Box::new(r::Expression::Parameter("left".into())),
            Box::new(r::Expression::Parameter("right".into())),
        );
        let ((result, peak), _) = observe(|| {
            let result = evaluation.eval(&expression);
            (result, OBSERVATION.with(Cell::get).peak)
        });
        assert_eq!(result.unwrap(), expected);
        assert!(
            peak <= input_bytes + 4096,
            "concatenation duplicated payloads: {peak} vs {input_bytes}"
        );
        let (_, live_inputs) = observe(|| (left.clone(), right.clone()));
        let limited = r::Evaluation {
            memory: r::EvaluationMemory::new(input_bytes),
            ..evaluation
        };
        let ((error, peak), _) = observe(|| {
            let error = limited.eval(&expression).unwrap_err();
            (error, OBSERVATION.with(Cell::get).peak)
        });
        assert_eq!(error.detail, "MemoryLimit");
        let error_bytes =
            error.category.capacity() + error.detail.capacity() + error.message.capacity();
        assert!(
            peak <= live_inputs + error_bytes,
            "rejected concatenation allocated output before admission"
        );
        assert_eq!(parameters["left"], left);
        assert_eq!(parameters["right"], right);
    }
}

#[test]
fn concatenation_reuses_empty_operands_and_retained_capacity() {
    let parameters = BTreeMap::new();
    for (empty, value) in [
        (
            r::Value::String(String::new()),
            r::Value::String("x".repeat(8192)),
        ),
        (
            r::Value::List(vec![]),
            r::Value::List(vec![r::Value::String("x".repeat(8192))]),
        ),
    ] {
        for (left, right) in [
            (empty.clone(), value.clone()),
            (value.clone(), empty.clone()),
        ] {
            let budget = left.allocated_bytes() + right.allocated_bytes();
            let evaluation = r::Evaluation {
                row: &[],
                parameters: &parameters,
                graph: &NoGraph,
                group: None,
                max_collection_items: 100,
                memory: r::EvaluationMemory::new(budget),
            };
            let expression = r::Expression::Binary(
                r::Binary::Add,
                Box::new(r::Expression::Literal(left)),
                Box::new(r::Expression::Literal(right)),
            );
            let ((result, peak), _) = observe(|| {
                let result = evaluation.eval(&expression);
                (result, OBSERVATION.with(Cell::get).peak)
            });
            assert_eq!(result.unwrap(), value);
            assert!(peak <= budget);
        }
    }
    let list = r::Expression::Slice {
        value: Box::new(r::Expression::Literal(r::Value::List(
            (0..16).map(r::Value::Integer).collect(),
        ))),
        start: None,
        end: Some(Box::new(r::Expression::Literal(r::Value::Integer(1)))),
    };
    let expression = r::Expression::Binary(
        r::Binary::Add,
        Box::new(list),
        Box::new(r::Expression::Literal(r::Value::List(vec![
            r::Value::Integer(2),
        ]))),
    );
    let budget = 19 * size_of::<r::Value>();
    let evaluation = r::Evaluation {
        row: &[],
        parameters: &parameters,
        graph: &NoGraph,
        group: None,
        max_collection_items: 100,
        memory: r::EvaluationMemory::new(budget),
    };
    let ((result, peak), _) = observe(|| {
        let result = evaluation.eval(&expression);
        (result, OBSERVATION.with(Cell::get).peak)
    });
    assert_eq!(
        result.unwrap(),
        r::Value::List(vec![r::Value::Integer(0), r::Value::Integer(2)])
    );
    assert!(peak <= budget);
    let count = 4096;
    let left = r::Expression::Function(
        r::Function::ToLower,
        vec![r::Expression::Literal(r::Value::String("K".repeat(count)))],
    );
    let expression = r::Expression::Binary(
        r::Binary::Add,
        Box::new(left),
        Box::new(r::Expression::Literal(r::Value::String("x".repeat(count)))),
    );
    let evaluation = r::Evaluation {
        memory: r::EvaluationMemory::new(6 * count + 256),
        ..evaluation
    };
    let ((result, peak), _) = observe(|| {
        let result = evaluation.eval(&expression);
        (result, OBSERVATION.with(Cell::get).peak)
    });
    assert_eq!(
        result.unwrap(),
        r::Value::String("k".repeat(count) + &"x".repeat(count))
    );
    assert!(peak <= evaluation.memory.available());
}

#[test]
fn primitive_binary_evaluation_needs_only_live_operands() {
    let parameters = BTreeMap::new();
    let large = r::Value::String("λ猫".repeat(8192));
    for (op, left, right, expected) in [
        (
            r::Binary::Equal,
            large.clone(),
            large.clone(),
            r::Value::Boolean(true),
        ),
        (
            r::Binary::Contains,
            large.clone(),
            r::Value::String("λ猫".into()),
            r::Value::Boolean(true),
        ),
        (
            r::Binary::StartsWith,
            large.clone(),
            r::Value::String("λ".into()),
            r::Value::Boolean(true),
        ),
        (
            r::Binary::EndsWith,
            large.clone(),
            r::Value::String("猫".into()),
            r::Value::Boolean(true),
        ),
        (
            r::Binary::In,
            large.clone(),
            r::Value::List(vec![large.clone()]),
            r::Value::Boolean(true),
        ),
        (
            r::Binary::Add,
            r::Value::Integer(7),
            r::Value::Integer(5),
            r::Value::Integer(12),
        ),
    ] {
        let budget = left.allocated_bytes() + right.allocated_bytes();
        let evaluation = r::Evaluation {
            row: &[],
            parameters: &parameters,
            graph: &NoGraph,
            group: None,
            max_collection_items: 100,
            memory: r::EvaluationMemory::new(budget),
        };
        let expression = r::Expression::Binary(
            op,
            Box::new(r::Expression::Literal(left)),
            Box::new(r::Expression::Literal(right)),
        );
        let ((result, peak), _) = observe(|| {
            let result = evaluation.eval(&expression);
            (result, OBSERVATION.with(Cell::get).peak)
        });
        assert_eq!(result.unwrap(), expected, "{op:?}");
        assert!(peak <= budget);
    }
}

#[test]
fn addition_preserves_nulls_errors_and_operand_order() {
    let parameters = BTreeMap::new();
    let evaluation = r::Evaluation {
        row: &[],
        parameters: &parameters,
        graph: &NoGraph,
        group: None,
        max_collection_items: 100,
        memory: r::EvaluationMemory::new(16 * 1024),
    };
    for value in [
        r::Value::List(vec![r::Value::Integer(1)]),
        r::Value::String("x".into()),
        r::Value::Integer(7),
    ] {
        for (left, right) in [(r::Value::Null, value.clone()), (value, r::Value::Null)] {
            let expression = r::Expression::Binary(
                r::Binary::Add,
                Box::new(r::Expression::Literal(left)),
                Box::new(r::Expression::Literal(right)),
            );
            assert_eq!(evaluation.eval(&expression).unwrap(), r::Value::Null);
        }
    }
    for (left, right) in [
        (r::Value::Boolean(true), r::Value::String("x".into())),
        (r::Value::String("x".into()), r::Value::Boolean(true)),
    ] {
        let expression = r::Expression::Binary(
            r::Binary::Add,
            Box::new(r::Expression::Literal(left)),
            Box::new(r::Expression::Literal(right)),
        );
        assert_eq!(
            evaluation.eval(&expression).unwrap_err().category,
            "TypeError"
        );
    }
    let overflow = r::Expression::Binary(
        r::Binary::Add,
        Box::new(r::Expression::Literal(r::Value::Integer(i64::MAX))),
        Box::new(r::Expression::Literal(r::Value::Integer(1))),
    );
    assert_eq!(
        evaluation.eval(&overflow).unwrap_err().detail,
        "NumberOutOfRange"
    );
    for (left, right, detail) in [
        (
            overflow.clone(),
            r::Expression::Parameter("missing".into()),
            "NumberOutOfRange",
        ),
        (
            r::Expression::Parameter("missing".into()),
            overflow,
            "MissingParameter",
        ),
        (
            r::Expression::Literal(r::Value::Null),
            r::Expression::Parameter("missing".into()),
            "MissingParameter",
        ),
    ] {
        let expression = r::Expression::Binary(r::Binary::Add, Box::new(left), Box::new(right));
        assert_eq!(evaluation.eval(&expression).unwrap_err().detail, detail);
    }
    for (op, left) in [(r::Binary::And, false), (r::Binary::Or, true)] {
        let expression = r::Expression::Binary(
            op,
            Box::new(r::Expression::Literal(r::Value::Boolean(left))),
            Box::new(r::Expression::Parameter("missing".into())),
        );
        assert_eq!(
            evaluation.eval(&expression).unwrap(),
            r::Value::Boolean(left)
        );
    }
}

#[test]
fn numeric_text_uses_exact_buffers_and_rejects_before_formatting_output() {
    let parameters = BTreeMap::new();
    let check = |value: r::Value| {
        let expected = match &value {
            r::Value::Integer(value) => value.to_string(),
            r::Value::Boolean(value) => value.to_string(),
            r::Value::Float(value) => {
                let mut text = value.to_string();
                if value.is_finite() && !text.contains(['.', 'e', 'E']) {
                    text.push_str(".0");
                }
                text
            }
            _ => unreachable!(),
        };
        let literal = r::Expression::Literal(value.clone());
        let empty = r::Expression::Literal(r::Value::String(String::new()));
        let mut expressions = vec![(
            r::Expression::Function(r::Function::ToString, vec![literal.clone()]),
            size_of::<r::Value>(),
            size_of::<r::Value>(),
        )];
        if matches!(value, r::Value::Integer(_) | r::Value::Float(_)) {
            expressions.push((
                r::Expression::Binary(
                    r::Binary::Add,
                    Box::new(literal.clone()),
                    Box::new(empty.clone()),
                ),
                2 * size_of::<r::Value>(),
                0,
            ));
            expressions.push((
                r::Expression::Binary(r::Binary::Add, Box::new(empty), Box::new(literal)),
                2 * size_of::<r::Value>(),
                0,
            ));
        }
        for (expression, input_bytes, argument_heap) in expressions {
            let exact = input_bytes + size_of::<r::Value>() + expected.len();
            for budget in [exact, exact - 1] {
                let evaluation = r::Evaluation {
                    row: &[],
                    parameters: &parameters,
                    graph: &NoGraph,
                    group: None,
                    max_collection_items: 10,
                    memory: r::EvaluationMemory::new(budget),
                };
                let ((result, peak), _) = observe(|| {
                    let result = evaluation.eval(&expression);
                    (result, OBSERVATION.with(Cell::get).peak)
                });
                if budget == exact {
                    let r::Value::String(text) = result.unwrap() else {
                        panic!("expected scalar text")
                    };
                    assert_eq!(text, expected, "{expression:?}");
                    assert_eq!(text.capacity(), expected.len());
                    assert!(peak <= budget, "{peak} > {budget}");
                } else {
                    let error = result.unwrap_err();
                    assert_eq!(error.detail, "MemoryLimit");
                    let error_bytes = error.category.capacity()
                        + error.detail.capacity()
                        + error.message.capacity();
                    assert_eq!(
                        peak,
                        argument_heap + error_bytes,
                        "rejection allocated formatted text for {expression:?}"
                    );
                }
            }
        }
    };
    for value in [i64::MIN, -1, 0, 1, i64::MAX] {
        check(r::Value::Integer(value));
    }
    for value in [true, false] {
        check(r::Value::Boolean(value));
    }
    for value in [
        f64::MIN,
        f64::MAX,
        0.0,
        -0.0,
        1.0,
        -1.0,
        0.1,
        1.0 / 3.0,
        f64::MIN_POSITIVE,
        f64::from_bits(1),
        f64::from_bits((1_u64 << 52) - 1),
        f64::NAN,
        f64::INFINITY,
        f64::NEG_INFINITY,
    ] {
        check(r::Value::Float(value));
    }
    let mut bits = 0x6a09e667f3bcc909_u64;
    for _ in 0..2048 {
        bits ^= bits << 13;
        bits ^= bits >> 7;
        bits ^= bits << 17;
        check(r::Value::Float(f64::from_bits(bits)));
    }
    for value in [
        r::Value::List(vec![]),
        r::Value::Map(BTreeMap::new()),
        r::Value::Entity(r::Entity::Node(1)),
        r::Value::Entity(r::Entity::Relationship(1)),
        r::Value::Path(r::Path::new(vec![1], vec![]).unwrap()),
    ] {
        let evaluation = r::Evaluation {
            row: &[],
            parameters: &parameters,
            graph: &NoGraph,
            group: None,
            max_collection_items: 100,
            memory: r::EvaluationMemory::new(16 * 1024),
        };
        let expression =
            r::Expression::Function(r::Function::ToString, vec![r::Expression::Literal(value)]);
        let error = evaluation.eval(&expression).unwrap_err();
        assert_eq!(error.category, "TypeError");
        assert_eq!(error.detail, "InvalidArgumentType");
        assert_eq!(error.phase, r::ErrorPhase::Runtime);
        assert_eq!(error.message, "value cannot be converted to a string");
    }
}

#[test]
fn impossible_expression_sequence_capacity_is_a_resource_error() {
    let parameters = BTreeMap::new();
    let expression = r::Expression::Literal(r::Value::Integer(1));
    let evaluation = r::Evaluation {
        row: &[],
        parameters: &parameters,
        graph: &NoGraph,
        group: None,
        max_collection_items: usize::MAX,
        memory: r::EvaluationMemory::new(usize::MAX),
    };
    let attempt = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        evaluation.eval_sequence(std::iter::repeat_n(&expression, usize::MAX))
    }));
    assert!(
        attempt.is_ok(),
        "an impossible capacity must be rejected before Vec allocation"
    );
    assert_eq!(attempt.unwrap().unwrap_err().detail, "MemoryLimit");
}

#[test]
fn scalar_ranges_admit_exact_outputs_at_integer_boundaries() {
    let parameters = BTreeMap::new();
    // An incrementing model independently checks the closed-form allocation
    // bound for every small endpoint/step combination in both directions.
    let small_ranges = (-8_i64..=8).flat_map(|start| {
        (-8_i64..=8).flat_map(move |end| {
            [-4_i64, -3, -2, -1, 1, 2, 3, 4].map(move |step| {
                let expected = std::iter::successors(Some(start), |n| n.checked_add(step))
                    .take_while(|&n| if step > 0 { n <= end } else { n >= end })
                    .collect();
                (vec![start, end, step], expected)
            })
        })
    });
    for (arguments, expected) in [
        (vec![0, -1], vec![]),
        (vec![0, 0], vec![0]),
        (vec![0, 3], vec![0, 1, 2, 3]),
        (vec![3, 0, -1], vec![3, 2, 1, 0]),
        (vec![0, 3, -1], vec![]),
        (vec![i64::MAX, i64::MAX], vec![i64::MAX]),
        (vec![i64::MIN, i64::MIN, -1], vec![i64::MIN]),
        (vec![0, i64::MIN, i64::MIN], vec![0, i64::MIN]),
        (vec![i64::MAX, i64::MIN, i64::MIN], vec![i64::MAX, -1]),
        (
            vec![i64::MIN, i64::MAX, i64::MAX],
            vec![i64::MIN, -1, i64::MAX - 1],
        ),
    ]
    .into_iter()
    .chain(small_ranges)
    {
        let input_bytes = arguments.len() * size_of::<r::Value>();
        let budget = input_bytes + (expected.len() + 1) * size_of::<r::Value>();
        let expression = r::Expression::Function(
            r::Function::Range,
            arguments
                .iter()
                .map(|&i| r::Expression::Literal(r::Value::Integer(i)))
                .collect(),
        );
        for (max_value_bytes, fits) in [(budget, true), (budget - 1, false)] {
            let evaluation = r::Evaluation {
                row: &[],
                parameters: &parameters,
                graph: &NoGraph,
                group: None,
                max_collection_items: 128,
                memory: r::EvaluationMemory::new(max_value_bytes),
            };
            let ((result, peak), _) = observe(|| {
                let result = evaluation.eval(&expression);
                (result, OBSERVATION.with(Cell::get).peak)
            });
            if fits {
                assert_eq!(
                    result.unwrap(),
                    r::Value::List(expected.iter().copied().map(r::Value::Integer).collect()),
                    "{arguments:?}"
                );
                assert!(peak <= budget, "{arguments:?}: {peak} > {budget}");
            } else {
                let error = result.unwrap_err();
                assert_eq!(error.detail, "MemoryLimit");
                let error_bytes =
                    error.category.capacity() + error.detail.capacity() + error.message.capacity();
                assert!(
                    peak <= input_bytes + error_bytes,
                    "rejected range allocated an output: {arguments:?}"
                );
            }
        }
    }
}

#[test]
fn scalar_range_limits_reject_huge_collections_without_changing_streaming() {
    let parameters = BTreeMap::new();
    for (arguments, items, memory, detail) in [
        (
            vec![i64::MIN, i64::MAX],
            usize::MAX,
            usize::MAX,
            "CollectionLimit",
        ),
        (
            vec![i64::MIN, i64::MAX - 1],
            usize::MAX,
            usize::MAX,
            if usize::BITS == 64 {
                "MemoryLimit"
            } else {
                "CollectionLimit"
            },
        ),
        (
            vec![
                0,
                i64::try_from(isize::MAX as usize / size_of::<r::Value>()).unwrap(),
            ],
            usize::MAX,
            usize::MAX,
            "MemoryLimit",
        ),
        (
            vec![0, 1000],
            3,
            6 * size_of::<r::Value>(),
            "CollectionLimit",
        ),
        (vec![10, 0, -1], 3, usize::MAX, "CollectionLimit"),
        (vec![0, 1000], 3, 0, "MemoryLimit"),
    ] {
        let input_bytes = arguments.len() * size_of::<r::Value>();
        let expression = r::Expression::Function(
            r::Function::Range,
            arguments
                .into_iter()
                .map(|i| r::Expression::Literal(r::Value::Integer(i)))
                .collect(),
        );
        let evaluation = r::Evaluation {
            row: &[],
            parameters: &parameters,
            graph: &NoGraph,
            group: None,
            max_collection_items: items,
            memory: r::EvaluationMemory::new(memory),
        };
        let ((error, peak), _) = observe(|| {
            let error = evaluation.eval(&expression).unwrap_err();
            (error, OBSERVATION.with(Cell::get).peak)
        });
        assert_eq!(error.detail, detail);
        let error_bytes =
            error.category.capacity() + error.detail.capacity() + error.message.capacity();
        assert!(
            peak <= input_bytes + error_bytes,
            "range rejection materialized values"
        );
    }
    let expression = r::Expression::Function(
        r::Function::Range,
        vec![
            r::Expression::Literal(r::Value::Integer(i64::MIN)),
            r::Expression::Literal(r::Value::Integer(i64::MAX)),
        ],
    );
    let evaluation = r::Evaluation {
        row: &[],
        parameters: &parameters,
        graph: &NoGraph,
        group: None,
        max_collection_items: 3,
        memory: r::EvaluationMemory::new(6 * size_of::<r::Value>()),
    };
    let ((range, peak), _) = observe(|| {
        let range = evaluation.unwind(&expression).unwrap();
        (range, OBSERVATION.with(Cell::get).peak)
    });
    assert!(peak <= 2 * size_of::<r::Value>());
    assert_eq!(
        range.take(3).collect::<Vec<_>>(),
        [i64::MIN, i64::MIN + 1, i64::MIN + 2].map(r::Value::Integer)
    );
}

#[test]
fn decimal_integer_conversion_allocates_only_its_owned_argument() {
    let parameters = BTreeMap::new();
    for (text, expected) in [
        (
            "0".repeat(100_000) + "9007199254740993.0",
            r::Value::Integer(9_007_199_254_740_993),
        ),
        (
            "9223372036854775807.".to_owned() + &"0".repeat(100_000),
            r::Value::Integer(i64::MAX),
        ),
        (
            "-9223372036854775808.".to_owned() + &"0".repeat(100_000) + "1",
            r::Value::Null,
        ),
        ("1e".to_owned() + &"9".repeat(100_000), r::Value::Null),
        (
            "1e-".to_owned() + &"9".repeat(100_000),
            r::Value::Integer(0),
        ),
        ("0".repeat(100_000) + "!", r::Value::Null),
    ] {
        // Match the exact capacity of the evaluator's cloned argument.
        let text = text.clone();
        let bytes = text.len();
        let expression = r::Expression::Function(
            r::Function::ToInteger,
            vec![r::Expression::Literal(r::Value::String(text))],
        );
        let evaluation = r::Evaluation {
            row: &[],
            parameters: &parameters,
            graph: &NoGraph,
            group: None,
            max_collection_items: 100,
            memory: r::EvaluationMemory::new(2 * size_of::<r::Value>() + bytes),
        };
        let ((result, peak), _) = observe(|| {
            let result = evaluation.eval(&expression);
            (result, OBSERVATION.with(Cell::get).peak)
        });
        assert_eq!(result.unwrap(), expected);
        assert_eq!(
            peak,
            size_of::<r::Value>() + bytes,
            "conversion allocated beyond the argument slots and string"
        );
    }
}

#[test]
fn collection_rejection_does_not_clone_borrowed_payloads() {
    use r::{Expression as E, Value as V};
    let wide = V::List((0..128).map(|_| V::String("x".repeat(1024))).collect());
    let nested = V::Map(BTreeMap::from([("nested".into(), wide.clone())]));
    let row = vec![wide.clone(), nested.clone()];
    let parameters = BTreeMap::from([
        ("wide".into(), wide.clone()),
        ("nested".into(), nested.clone()),
    ]);
    let evaluation = r::Evaluation {
        row: &row,
        parameters: &parameters,
        graph: &NoGraph,
        group: None,
        max_collection_items: 1,
        memory: r::EvaluationMemory::new(usize::MAX),
    };
    for expression in [
        E::Literal(wide.clone()),
        E::Literal(nested.clone()),
        E::Slot(r::Slot(0)),
        E::Slot(r::Slot(1)),
        E::Parameter("wide".into()),
        E::Parameter("nested".into()),
    ] {
        let ((error, peak), _) = observe(|| {
            let error = evaluation.eval(&expression).unwrap_err();
            (error, OBSERVATION.with(Cell::get).peak)
        });
        assert_eq!(error.detail, "CollectionLimit");
        let error_bytes =
            error.category.capacity() + error.detail.capacity() + error.message.capacity();
        assert_eq!(
            peak, error_bytes,
            "rejection cloned a borrowed value before checking its cardinality"
        );
    }
    assert_eq!(row[0], wide);
    assert_eq!(row[1], nested);
    assert_eq!(parameters["wide"], wide);
    assert_eq!(parameters["nested"], nested);
}

#[test]
fn graph_collection_rejection_does_not_allocate_output_buffers() {
    use r::{Expression as E, Function as F, Value as V};
    struct Graph(r::GraphProperties);
    impl r::GraphValues for Graph {
        fn properties(&self, _: r::Entity) -> r::Result<&r::GraphProperties> {
            Ok(&self.0)
        }
        fn label(&self, _: r::Entity) -> r::Result<Option<&str>> {
            Ok(Some("N"))
        }
    }
    let parameters = BTreeMap::new();
    let graph = Graph(BTreeMap::from([
        (
            "large".into(),
            Ok(V::List(
                (0..128).map(|_| V::String("x".repeat(1024))).collect(),
            )),
        ),
        (
            "unsupported".into(),
            Err(r::QueryError::unsupported("StoredValue")),
        ),
    ]));
    let evaluation = r::Evaluation {
        row: &[],
        parameters: &parameters,
        graph: &graph,
        group: None,
        max_collection_items: 1,
        memory: r::EvaluationMemory::new(usize::MAX),
    };
    let node = E::Literal(V::Entity(r::Entity::Node(1)));
    for (expression, argument_bytes) in [
        (E::Property(Box::new(node.clone()), "large".into()), 0),
        (
            E::Index(
                Box::new(node.clone()),
                Box::new(E::Literal(V::String("large".into()))),
            ),
            5,
        ),
        (E::Function(F::Keys, vec![node.clone()]), size_of::<V>()),
        (E::Function(F::Properties, vec![node]), size_of::<V>()),
    ] {
        let ((error, peak), _) = observe(|| {
            let error = evaluation.eval(&expression).unwrap_err();
            (error, OBSERVATION.with(Cell::get).peak)
        });
        assert_eq!(error.detail, "CollectionLimit");
        let error_bytes =
            error.category.capacity() + error.detail.capacity() + error.message.capacity();
        assert_eq!(
            peak,
            argument_bytes + error_bytes,
            "graph output was copied before collection admission"
        );
    }
}

#[test]
fn scalar_memory_peaks_cover_transient_allocations() {
    use r::{Binary as B, Expression as E, Function as F, Value as V};
    use std::sync::atomic::{AtomicUsize, Ordering};
    let parameters = BTreeMap::new();
    let integer = |n| E::Literal(V::Integer(n));
    let range = |from, to| E::Function(F::Range, vec![integer(from), integer(to)]);
    let size = |value| E::Function(F::Size, vec![value]);
    let input = "straße猫".repeat(1024);
    let string = || E::Literal(V::String(input.clone()));
    let expressions = [
        size(range(1, 10000)),
        size(E::Function(F::Reverse, vec![range(1, 10000)])),
        size(E::Function(F::ToUpper, vec![string()])),
        size(E::Binary(B::Add, Box::new(string()), Box::new(string()))),
        size(E::Binary(
            B::Add,
            Box::new(range(1, 1000)),
            Box::new(range(1001, 2000)),
        )),
        size(E::Function(
            F::Keys,
            vec![E::Map(
                (0..1000).map(|i| (format!("key{i}"), integer(i))).collect(),
            )],
        )),
        E::Index(Box::new(range(1, 10000)), Box::new(integer(9999))),
        E::Function(F::Substring, vec![string(), integer(100), integer(200)]),
    ];
    let evaluation = r::Evaluation {
        row: &[],
        parameters: &parameters,
        graph: &NoGraph,
        group: None,
        max_collection_items: 20000,
        memory: r::EvaluationMemory::new(16 * 1024 * 1024),
    };
    for expression in expressions {
        let expected = evaluation.eval(&expression).unwrap();
        let reported = AtomicUsize::new(0);
        let observed = r::Evaluation {
            memory: r::EvaluationMemory::observed(16 * 1024 * 1024, 4096, &reported).unwrap(),
            ..evaluation
        };
        let ((value, actual), _) = observe(|| {
            let value = observed.eval(&expression).unwrap();
            (value, OBSERVATION.with(Cell::get).peak)
        });
        assert_eq!(value, expected);
        let peak = reported.load(Ordering::Relaxed);
        assert!(
            peak >= 4096 + actual,
            "reported {peak} misses {actual} transient bytes for {expression:?}"
        );
        assert!(peak <= 16 * 1024 * 1024);
        assert_eq!(
            r::Evaluation {
                memory: r::EvaluationMemory::new(peak - 4096),
                ..evaluation
            }
            .eval(&expression)
            .unwrap(),
            expected
        );
    }
    struct Graph(r::GraphProperties);
    impl r::GraphValues for Graph {
        fn properties(&self, _: r::Entity) -> r::Result<&r::GraphProperties> {
            Ok(&self.0)
        }
        fn label(&self, _: r::Entity) -> r::Result<Option<&str>> {
            Ok(Some("N"))
        }
    }
    let graph = Graph(BTreeMap::from([
        ("text".into(), Ok(V::String(input))),
        (
            "unused".into(),
            Err(r::QueryError::unsupported("DormantProperty")),
        ),
    ]));
    let expression = size(E::Function(
        F::ToUpper,
        vec![E::Property(
            Box::new(E::Literal(V::Entity(r::Entity::Node(1)))),
            "text".into(),
        )],
    ));
    let reported = AtomicUsize::new(0);
    let observed = r::Evaluation {
        graph: &graph,
        memory: r::EvaluationMemory::observed(16 * 1024 * 1024, 4096, &reported).unwrap(),
        ..evaluation
    };
    let ((value, actual), _) = observe(|| {
        let value = observed.eval(&expression).unwrap();
        (value, OBSERVATION.with(Cell::get).peak)
    });
    assert_eq!(value, V::Integer(8 * 1024));
    assert!(reported.load(Ordering::Relaxed) >= 4096 + actual);
}

#[test]
fn scalar_sequence_peak_includes_earlier_owned_outputs() {
    use r::{Expression as E, Function as F, Value as V};
    use std::sync::atomic::{AtomicUsize, Ordering};
    let parameters = BTreeMap::new();
    let reported = AtomicUsize::new(0);
    let evaluation = r::Evaluation {
        row: &[],
        parameters: &parameters,
        graph: &NoGraph,
        group: None,
        max_collection_items: 20000,
        memory: r::EvaluationMemory::observed(16 * 1024 * 1024, 4096, &reported).unwrap(),
    };
    let expressions = [
        E::Literal(V::String("x".repeat(10000))),
        E::Function(
            F::Range,
            vec![E::Literal(V::Integer(1)), E::Literal(V::Integer(10000))],
        ),
        E::Function(F::Size, vec![E::Literal(V::String("y".repeat(20000)))]),
    ];
    let ((values, actual), _) = observe(|| {
        let values = evaluation.eval_sequence(expressions.iter()).unwrap();
        (values, OBSERVATION.with(Cell::get).peak)
    });
    assert_eq!(values[2], V::Integer(20000));
    assert!(reported.load(Ordering::Relaxed) >= 4096 + actual);
}

#[test]
fn scalar_distinct_count_admits_input_and_key_copy_together() {
    use r::{Expression as E, Value as V};
    let parameters = BTreeMap::new();
    for bytes in [1024, 4096, 16384] {
        let rows = vec![vec![V::String("x".repeat(bytes))]];
        let expression = E::Aggregate {
            function: r::Aggregate::Count,
            argument: Some(Box::new(E::Slot(r::Slot(0)))),
            distinct: true,
        };
        let budget = bytes + 2048;
        let evaluation = r::Evaluation {
            row: &[],
            parameters: &parameters,
            graph: &NoGraph,
            group: Some(&rows),
            max_collection_items: 10,
            memory: r::EvaluationMemory::new(budget),
        };
        let ((result, actual), _) = observe(|| {
            let result = evaluation.eval(&expression);
            (result, OBSERVATION.with(Cell::get).peak)
        });
        assert!(
            actual <= budget,
            "input={bytes}, allocation peak={actual}, budget={budget}"
        );
        if bytes >= 4096 {
            let error = result.unwrap_err();
            assert_eq!(error.category, "ResourceLimit");
            assert_eq!(error.detail, "MemoryLimit");
        } else {
            assert_eq!(result.unwrap(), V::Integer(1));
        }
        assert_eq!(
            r::Evaluation {
                memory: r::EvaluationMemory::new(4 * bytes + 4096),
                ..evaluation
            }
            .eval(&expression)
            .unwrap(),
            V::Integer(1)
        );
        assert_eq!(rows[0][0], V::String("x".repeat(bytes)));
    }
}

#[test]
fn scalar_aggregate_peak_covers_input_and_retained_state() {
    use r::{Aggregate as A, Expression as E, Value as V};
    use std::sync::atomic::{AtomicUsize, Ordering};
    let parameters = BTreeMap::new();
    let rows = (0..64)
        .map(|i| vec![V::String(format!("{i:02}{}", "x".repeat(4096)))])
        .collect::<Vec<_>>();
    for count in [1, 2, 3, 4, 8, 64] {
        for (function, distinct) in [
            (A::Collect, false),
            (A::Collect, true),
            (A::Count, true),
            (A::Min, false),
            (A::Max, false),
        ] {
            let reported = AtomicUsize::new(0);
            let evaluation = r::Evaluation {
                row: &[],
                parameters: &parameters,
                graph: &NoGraph,
                group: Some(&rows[..count]),
                max_collection_items: 20000,
                memory: r::EvaluationMemory::observed(16 * 1024 * 1024, 4096, &reported).unwrap(),
            };
            let expression = E::Aggregate {
                function,
                argument: Some(Box::new(E::Slot(r::Slot(0)))),
                distinct,
            };
            let ((value, actual), _) = observe(|| {
                let value = evaluation.eval(&expression).unwrap();
                (value, OBSERVATION.with(Cell::get).peak)
            });
            assert!(
                reported.load(Ordering::Relaxed) >= 4096 + actual,
                "aggregate {function:?}, distinct={distinct}: reported {}, actual {}",
                reported.load(Ordering::Relaxed),
                4096 + actual
            );
            assert!(match value {
                V::List(values) => values.len() == count,
                V::Integer(n) => n == count as i64,
                V::String(_) => true,
                _ => false,
            });
        }
    }
}
