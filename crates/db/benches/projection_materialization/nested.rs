//! Planned Values over nested fields, with and without a large unused sibling.

use std::{collections::BTreeMap, sync::OnceLock};

use db::{HelixDB, HelixDbSource, ProcessLocalDatabaseToken};
use helix_ast::prelude::*;
use helix_planner::{context::ParamBindings, exec::ExecutablePlan, planning};

struct Fixture {
    runtime: tokio::runtime::Runtime,
    db: HelixDB,
    plan: ExecutablePlan,
}

fn fixture(large_sibling: bool) -> &'static Fixture {
    static FIXTURES: [OnceLock<Fixture>; 2] = [const { OnceLock::new() }; 2];
    FIXTURES[usize::from(large_sibling)].get_or_init(|| {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("benchmark runtime starts");
        let (db, plan) = runtime.block_on(async {
            let db = HelixDB::open(HelixDbSource::InMemoryToken {
                token: ProcessLocalDatabaseToken::new("nested-values-bench")
                    .expect("benchmark token is valid"),
            })
            .await
            .expect("benchmark database opens");
            let count = if std::env::args().any(|argument| argument == "--bench") {
                16
            } else {
                4
            };
            let mut create = write_batch();
            for id in 0..count {
                // The small object is the property-path production contract's
                // metadata, repeated across a stream of nodes.
                let mut metadata = BTreeMap::from([
                    ("score".to_string(), PropertyValue::I64(9)),
                    (
                        "deep".to_string(),
                        PropertyValue::object([("exact", PropertyValue::from("source"))]),
                    ),
                ]);
                if large_sibling {
                    metadata.insert("body".to_string(), PropertyValue::from("s".repeat(65_536)));
                }
                create = create.var_as(
                    &format!("node_{id}"),
                    g().add_n(
                        "Person",
                        vec![
                            ("name", PropertyInput::from("alice")),
                            ("plain", PropertyInput::from("root")),
                            ("scalar", PropertyInput::from(7_i64)),
                            (
                                "metadata",
                                PropertyInput::from(PropertyValue::Object(metadata)),
                            ),
                        ],
                    ),
                );
            }
            let plan =
                planning::plan_write_batch(&create, &db.planner_context(ParamBindings::default()))
                    .expect("benchmark graph plans");
            db.execute(&plan, ParamBindings::default())
                .await
                .expect("benchmark graph is created");
            let query = read_batch()
                .var_as(
                    "rows",
                    g().n(NodeRef::all())
                        .values(vec!["metadata.score", "metadata.deep.exact"]),
                )
                .returning(["rows"]);
            let plan =
                planning::plan_read_batch(&query, &db.planner_context(ParamBindings::default()))
                    .expect("nested Values plans");
            db.execute(&plan, ParamBindings::default())
                .await
                .expect("nested Values warms");
            (db, plan)
        });
        Fixture { runtime, db, plan }
    })
}

#[divan::bench(args = [false, true], threads = 1)]
fn values(bencher: divan::Bencher<'_, '_>, large_sibling: bool) {
    let fixture = fixture(large_sibling);
    bencher.bench_local(|| {
        divan::black_box(
            fixture
                .runtime
                .block_on(fixture.db.execute(&fixture.plan, ParamBindings::default()))
                .expect("nested Values executes"),
        )
    });
}
