//! Arena parsing must accept exactly what owned parsing accepts, report the
//! same errors, and convert back to the same owned values.

use std::num::NonZeroUsize;

use crate::arena::{self, Bump, IntoOwned, Pool, PoolConfig};
use crate::query::{ArenaQueryRequest, QueryRequest};
use crate::testing;

/// Unoptimized builds need several times the stack an optimized build does
/// for the deepest requests, so these tests run on a larger stack.
fn on_large_stack(test: impl FnOnce() + Send + 'static) {
    std::thread::Builder::new()
        .stack_size(64 << 20)
        .spawn(test)
        .expect("test thread spawns")
        .join()
        .expect("test passes");
}

/// The outcome of one parse: the owned request, or the error message.
type Outcome = Result<QueryRequest, String>;

fn owned(json: &[u8]) -> Outcome {
    QueryRequest::from_json_slice(json).map_err(|error| error.to_string())
}

fn arena(json: &[u8]) -> Outcome {
    let bump = Bump::new();
    ArenaQueryRequest::from_json_slice(&bump, json)
        .map(IntoOwned::into_owned)
        .map_err(|error| error.to_string())
}

/// Arena and owned parsing agree exactly: the same request or the same
/// error message.
fn assert_equivalent(name: &str, json: &[u8]) {
    assert_eq!(arena(json), owned(json), "{name}");
}

#[test]
fn every_variant_and_benchmark_shape_parses_to_the_owned_request() {
    on_large_stack(|| {
        for shape in testing::every_variant().into_iter().chain(testing::all()) {
            assert!(owned(&shape.json).is_ok(), "{} is valid", shape.name);
            assert_equivalent(&shape.name, &shape.json);
        }
    });
}

/// Single edits of a valid request: each object key removed or renamed,
/// each value replaced by every other JSON kind, and each array shortened
/// or lengthened. Together they reach every missing-field, default,
/// unknown-variant, unknown-field, invalid-type and invalid-length path of
/// every visitor in the corpus.
fn mutations(value: &serde_json::Value) -> Vec<serde_json::Value> {
    use serde_json::Value;

    let replacements = [
        Value::Null,
        Value::Bool(true),
        Value::from(7),
        Value::from(-1),
        Value::from(1.5),
        Value::from("s"),
        Value::Array(Vec::new()),
        Value::Object(serde_json::Map::new()),
    ];
    let here = replacements
        .iter()
        .filter(|replacement| *replacement != value)
        .cloned()
        .collect::<Vec<_>>();
    let nested = match value {
        Value::Object(object) => object
            .iter()
            .flat_map(|(key, child)| {
                let without = {
                    let mut edited = object.clone();
                    edited.remove(key);
                    Value::Object(edited)
                };
                let renamed = {
                    let mut edited = object.clone();
                    let moved = edited.remove(key).expect("key is present");
                    edited.insert(format!("{key}_unknown"), moved);
                    Value::Object(edited)
                };
                let children = mutations(child).into_iter().map(|mutated| {
                    let mut edited = object.clone();
                    edited.insert(key.clone(), mutated);
                    Value::Object(edited)
                });
                [without, renamed]
                    .into_iter()
                    .chain(children)
                    .collect::<Vec<_>>()
            })
            .collect(),
        Value::Array(array) => {
            let shorter = array
                .split_last()
                .map(|(_, rest)| Value::Array(rest.to_vec()));
            let longer = array.first().map(|first| {
                let mut edited = array.clone();
                edited.push(first.clone());
                Value::Array(edited)
            });
            shorter
                .into_iter()
                .chain(longer)
                .chain(array.iter().enumerate().flat_map(|(index, child)| {
                    mutations(child).into_iter().map(move |mutated| {
                        let mut edited = array.clone();
                        edited[index] = mutated;
                        Value::Array(edited)
                    })
                }))
                .collect()
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => Vec::new(),
    };
    here.into_iter().chain(nested).collect()
}

#[test]
fn single_edits_of_every_variant_get_the_same_verdict_and_error() {
    on_large_stack(|| {
        let mut accepted = 0_usize;
        let mut rejected = 0_usize;
        for shape in testing::every_variant() {
            let value = serde_json::from_slice::<serde_json::Value>(&shape.json)
                .expect("corpus requests are JSON");
            for mutated in mutations(&value) {
                let json = serde_json::to_vec(&mutated).expect("values serialize");
                assert_equivalent(&shape.name, &json);
                match owned(&json) {
                    Ok(_) => accepted += 1,
                    Err(_) => rejected += 1,
                }
            }
        }
        // Both verdicts are exercised in volume.
        assert!(
            accepted > 1_000 && rejected > 10_000,
            "{accepted} accepted, {rejected} rejected"
        );
    });
}

#[test]
fn mutations_nested_in_read_batches_are_rejected_alike() {
    on_large_stack(|| {
        let read_only = testing::ast_nodes()
            .into_iter()
            .filter(|node| !node.is_read_only())
            .collect::<Vec<_>>();
        assert!(read_only.len() >= 12, "every mutation family is sampled");
        for mutation in read_only {
            // Built as a write request, then retagged as read, which the
            // read-batch check must reject at every nesting position.
            let nested = |root: crate::traversal::AstNode| {
                let request = QueryRequest::write(crate::batch::WriteBatch {
                    entries: vec![crate::batch::BatchEntry::ForEach {
                        param: "items".to_owned(),
                        body: vec![crate::batch::BatchEntry::Query(Box::new(
                            crate::batch::NamedQuery {
                                name: None,
                                root,
                                condition: None,
                            },
                        ))],
                    }],
                    returns: Vec::new(),
                });
                String::from_utf8(request.to_json_bytes().unwrap())
                    .unwrap()
                    .replacen(r#""request_type":"write""#, r#""request_type":"read""#, 1)
                    .replacen(r#""query":{"write":"#, r#""query":{"read":"#, 1)
            };
            let branch = || crate::traversal::SubTraversal {
                root: Box::new(mutation.clone()),
            };
            let nodes = || {
                Box::new(crate::traversal::AstNode::Nodes {
                    reference: crate::graph::NodeRef::All,
                })
            };
            for root in [
                mutation.clone(),
                crate::traversal::AstNode::Count {
                    input: Box::new(mutation.clone()),
                },
                crate::traversal::AstNode::Union {
                    input: nodes(),
                    traversals: vec![crate::traversal::SubTraversal::new(), branch()],
                },
                crate::traversal::AstNode::Choose {
                    input: nodes(),
                    condition: crate::expr::Predicate::eq("a", 1_i64),
                    then_traversal: crate::traversal::SubTraversal::new(),
                    else_traversal: Some(branch()),
                },
                crate::traversal::AstNode::Repeat {
                    input: nodes(),
                    config: crate::traversal::RepeatConfig::new(branch()),
                },
                crate::traversal::AstNode::Optional {
                    input: nodes(),
                    traversal: branch(),
                },
            ] {
                let json = nested(root);
                let owned = owned(json.as_bytes());
                assert!(
                    owned
                        .as_ref()
                        .is_err_and(|error| error.contains("persistent mutation")),
                    "{owned:?}"
                );
                assert_equivalent("nested mutation", json.as_bytes());
            }
        }
    });
}

#[test]
fn hand_written_edge_cases_get_the_same_verdict_and_error() {
    let read = |entries: &str, extra: &str| {
        format!(r#"{{"request_type":"read","query":{{"read":{{"entries":[{entries}]}}}}{extra}}}"#)
    };
    let query = |root: &str| format!(r#"{{"query":{{"root":{root}}}}}"#);
    let cases = [
        // Duplicate struct field and duplicate parameter name.
        read(&query(r#"{"nodes":{"reference":"all","reference":"all"}}"#), ""),
        read("", r#","parameters":{"a":1,"a":2}"#),
        // Empty and mismatched parameter names, and typed mismatches.
        read("", r#","parameters":{"":1}"#),
        read("", r#","parameters":{"a":1},"parameter_types":{"b":"i64"}"#),
        read("", r#","parameters":{"a":"x"},"parameter_types":{"a":"i64"}"#),
        read("", r#","parameters":{"a":1e39},"parameter_types":{"a":"f32"}"#),
        // Validation error and trailing garbage: validation reports first.
        format!("{} trailing", read("", r#","search_consistency":"eventual","request_type":"write""#)),
        format!("{} trailing", read("", "")),
        // Eventual search consistency on a write.
        r#"{"request_type":"write","query":{"write":{"entries":[]}},"search_consistency":"eventual"}"#
            .to_owned(),
        // Request type disagreeing with the batch.
        r#"{"request_type":"write","query":{"read":{"entries":[]}}}"#.to_owned(),
        // Unit variants as strings and as single-key maps.
        read(&query(r#""context""#), ""),
        read(&query(r#"{"context":null}"#), ""),
        read(&query(r#"{"context":{}}"#), ""),
        // Structs and struct variants in sequence form, short and long.
        read(&query(r#"{"nodes":["all"]}"#), ""),
        read(&query(r#"{"nodes":[]}"#), ""),
        r#"{"request_type":"read","query":{"read":[[],["x"]]}}"#.to_owned(),
        r#"{"request_type":"read","query":{"read":[[]]}}"#.to_owned(),
        // Variant index form.
        read(&query(r#"{"0":null}"#), ""),
        // Numbers at their edges.
        read(&query(r#"{"nodes":{"reference":{"ids":[18446744073709551615]}}}"#), ""),
        read(&query(r#"{"nodes":{"reference":{"ids":[18446744073709551616]}}}"#), ""),
        read(&query(r#"{"nodes":{"reference":{"ids":[-1]}}}"#), ""),
        read(&query(r#"{"limit":{"input":"context","count":{"literal":-1}}}"#), ""),
        read(&query(r#"{"limit":{"input":"context","count":{"literal":1.5}}}"#), ""),
        read(&query(r#"{"has":{"input":"context","property":"p","value":{"f32":16777217}}}"#), ""),
        read(&query(r#"{"has":{"input":"context","property":"p","value":{"f32":1e39}}}"#), ""),
        read(&query(r#"{"has":{"input":"context","property":"p","value":{"f32":1.1}}}"#), ""),
        read(&query(r#"{"has":{"input":"context","property":"p","value":{"i64":9223372036854775808}}}"#), ""),
        read(&query(r#"{"get_index_operation":{"operation_id":"x"}}"#), ""),
        r#"{"request_type":"write","query":{"write":{"entries":[{"query":{"root":{"create_index":{"if_not_exists":true,"spec":{"node_vector":{"label":"D","property":"v","dimension":0,"metric":"cosine"}}}}}}]}}}"#
            .to_owned(),
        // Escapes, unicode and a repeated object key in a property value.
        read(&query(r#"{"has":{"input":"context","property":"é\n🦀","value":{"object":{"k":{"null":null},"k":{"i64":2}}}}}"#), ""),
        // Not JSON, or not a request.
        "".to_owned(),
        "[]".to_owned(),
        "{".to_owned(),
        r#"{"request_type":"read"}"#.to_owned(),
    ];
    on_large_stack(move || {
        for (index, case) in cases.iter().enumerate() {
            assert_equivalent(&format!("case {index}: {case}"), case.as_bytes());
        }
        // Past the depth limit, every entry point rejects before parsing.
        let deep = testing::deep_chain(testing::MAX_DEEP_CHAIN_STEPS + 1).json;
        assert_equivalent("deepest chain + 1", &deep);
        assert!(owned(&deep).is_err());
    });
}

#[test]
fn invalid_utf8_is_rejected_by_every_entry_point() {
    let body = |bytes: &[u8]| {
        [
            br#"{"request_type":"read","query":{"read":{"entries":[]}},"query_name":""#.as_slice(),
            bytes,
            br#""}"#.as_slice(),
        ]
        .concat()
    };
    for json in [body(b"\xff"), body(b"ok\xc3")] {
        assert!(owned(&json).is_err());
        assert_equivalent("invalid UTF-8", &json);
    }
}

#[test]
fn an_allocation_limit_fails_the_parse_instead_of_panicking() {
    let json = testing::wide_batch(200).json;
    let bump = Bump::new();
    bump.set_allocation_limit(Some(4096));
    let error = ArenaQueryRequest::from_json_slice(&bump, &json).unwrap_err();
    assert!(
        error.to_string().contains(arena::ALLOCATION_LIMIT_EXCEEDED),
        "{error}"
    );
    // The same limit with room to spare parses.
    let bump = Bump::new();
    bump.set_allocation_limit(Some(1 << 20));
    assert!(ArenaQueryRequest::from_json_slice(&bump, &json).is_ok());
}

#[test]
fn the_pool_keeps_small_arenas_and_frees_oversized_ones() {
    let pool = Pool::new(PoolConfig {
        initial_chunk_bytes: 1024,
        retain_bytes: 64 * 1024,
        max_idle: 2,
        allocation_limit: NonZeroUsize::new(8 << 20),
    });
    let small = testing::fixtures().remove(0).json;
    let large = testing::wide_batch(1_000).json;
    {
        let bump = pool.checkout();
        assert!(ArenaQueryRequest::from_json_slice(&bump, &small).is_ok());
    }
    assert_eq!(pool.idle(), 1, "a small arena is kept");
    {
        let bump = pool.checkout();
        assert!(ArenaQueryRequest::from_json_slice(&bump, &large).is_ok());
        assert!(bump.allocated_bytes() > 64 * 1024);
    }
    assert_eq!(pool.idle(), 0, "an arena past retain_bytes is freed");
    let held = [pool.checkout(), pool.checkout(), pool.checkout()];
    drop(held);
    assert_eq!(pool.idle(), 2, "at most max_idle arenas are kept");
    // Checked-out arenas carry the configured limit.
    let bump = pool.checkout();
    assert!(bump.try_alloc_slice_fill_copy(16 << 20, 0_u8).is_err());
}

/// Covers a first chunk larger than the limit, and one that fits only before
/// bumpalo rounds its size up.
#[test]
fn a_pooled_arena_stays_under_a_limit_smaller_than_its_first_chunk() {
    const LIMIT: usize = 64 * 1024;
    for initial_chunk_bytes in [1 << 20, LIMIT] {
        let pool = Pool::new(PoolConfig {
            initial_chunk_bytes,
            retain_bytes: 1 << 20,
            max_idle: 2,
            allocation_limit: NonZeroUsize::new(LIMIT),
        });
        for _ in 0..2 {
            let bump = pool.checkout();
            assert!(
                bump.allocated_bytes() <= LIMIT,
                "{initial_chunk_bytes}: {}",
                bump.allocated_bytes()
            );
            assert!(bump.try_alloc_slice_fill_copy(LIMIT / 2, 0_u8).is_ok());
            assert!(bump.try_alloc_slice_fill_copy(LIMIT, 0_u8).is_err());
            assert!(
                bump.allocated_bytes() <= LIMIT,
                "{initial_chunk_bytes}: {}",
                bump.allocated_bytes()
            );
        }
        assert_eq!(pool.idle(), 1, "the limited arena is reused");
    }
}

#[test]
fn mirrors_are_send_sync_and_copy() {
    fn assert_thread_safe_copy<T: Send + Sync + Copy>() {}
    assert_thread_safe_copy::<arena::AstNode<'static>>();
    assert_thread_safe_copy::<arena::BatchQuery<'static>>();
    assert_thread_safe_copy::<arena::PropertyValue<'static>>();
    assert_thread_safe_copy::<arena::QueryValue<'static>>();
    fn assert_send<T: Send>() {}
    assert_send::<ArenaQueryRequest<'static>>();
    assert_send::<arena::PooledBump<'static>>();
}

/// A request can own its arena and hold the parsed tree across awaits in a
/// `Send + 'static` future, as the uniffi runtime and tokio require.
#[test]
fn a_parsed_tree_lives_across_awaits_in_a_send_static_future() {
    static POOL: Pool = Pool::new(PoolConfig {
        initial_chunk_bytes: 4096,
        retain_bytes: 1 << 20,
        max_idle: 4,
        allocation_limit: None,
    });
    fn spawn<F: std::future::Future + Send + 'static>(future: F) -> F {
        future
    }
    let future = spawn(async {
        let body = testing::fixtures().remove(0).json;
        let bump = POOL.checkout();
        let request = ArenaQueryRequest::from_json_slice(&bump, &body).expect("fixture parses");
        drop(body);
        std::future::ready(()).await;
        request.request_type()
    });
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    let mut future = std::pin::pin!(future);
    assert!(matches!(
        future.as_mut().poll(&mut context),
        std::task::Poll::Ready(crate::query::QueryRequestType::Read)
    ));
}
