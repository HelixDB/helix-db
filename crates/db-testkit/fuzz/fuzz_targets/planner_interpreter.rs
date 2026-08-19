//! Fuzzes public planner-to-interpreter execution on an empty graph model.

#![no_main]

use std::sync::LazyLock;

use db::{HelixDB, HelixDbSource};
use helix_ast::batch;
use helix_ast::expr::Predicate;
use helix_ast::graph::{EdgeRef, NodeRef};
use helix_ast::query::QueryRequest;
use helix_ast::traversal;
use libfuzzer_sys::fuzz_target;

static RUNTIME: LazyLock<tokio::runtime::Runtime> =
    LazyLock::new(|| tokio::runtime::Runtime::new().expect("planner fuzz runtime must initialize"));

static DATABASE: LazyLock<HelixDB> = LazyLock::new(|| {
    RUNTIME
        .block_on(HelixDB::open(HelixDbSource::InMemory {
            database: "planner-interpreter-fuzz".to_string(),
        }))
        .expect("planner fuzz database must open")
});

fuzz_target!(|data: &[u8]| {
    let Some(selector) = data.first() else {
        return;
    };
    let first_bound = usize::from(data.get(1).copied().unwrap_or_default() % 4);
    let take = usize::from(data.get(2).copied().unwrap_or(1) % 4);
    let traversal = match selector % 12 {
        0 => traversal::g().n(NodeRef::all()).count(),
        1 => traversal::g().n(NodeRef::id(u64::from(*selector))).count(),
        2 => traversal::g().n_with_label("FuzzNode").count(),
        3 => traversal::g()
            .n(NodeRef::id(u64::from(*selector)))
            .out(None::<&str>)
            .count(),
        4 => traversal::g()
            .n(NodeRef::all())
            .range(first_bound, first_bound.saturating_add(take))
            .count(),
        5 => traversal::g()
            .n(NodeRef::all())
            .where_(Predicate::eq("status", "active"))
            .count(),
        6 => traversal::g().n(NodeRef::all()).dedup().count(),
        7 => traversal::g()
            .n(NodeRef::all())
            .order_by("rank", traversal::Order::Desc)
            .skip(first_bound)
            .limit(take)
            .count(),
        8 => traversal::g()
            .n(NodeRef::all())
            .union(vec![
                traversal::sub().limit(first_bound),
                traversal::sub().skip(take),
            ])
            .count(),
        9 => traversal::g().e(EdgeRef::all()).count(),
        10 => traversal::g()
            .e_with_label_where("FuzzEdge", Predicate::eq("status", "active"))
            .count(),
        _ => traversal::g()
            .n(NodeRef::all())
            .where_(Predicate::and(vec![
                Predicate::eq("status", "active"),
                Predicate::gte("rank", i64::from(*selector)),
            ]))
            .dedup()
            .skip(first_bound)
            .limit(take)
            .count(),
    };
    let request = QueryRequest::read(
        batch::read_batch()
            .var_as("result", traversal)
            .returning(["result"]),
    );
    let result = RUNTIME
        .block_on(DATABASE.query(request))
        .expect("valid empty-graph query must execute");
    assert_eq!(result["result"], 0);
});
