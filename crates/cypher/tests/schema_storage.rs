//! Construction budgets exclude parsing and measure allocation requests, not RSS.
use helix_planner::relational as r;
#[path = "../../planner/src/analysis/tests/allocations.rs"]
mod allocations;

#[test]
fn growing_and_shadowed_scopes_stay_within_compilation_allocation_budgets() {
    for growing in [false, true] {
        for width in [64, 256, 1024, 4095] {
            let source = if growing {
                (0..width)
                    .map(|slot| format!("UNWIND [1] AS v{slot} "))
                    .collect::<String>()
                    + "RETURN v0"
            } else {
                "WITH 1 AS v0 ".to_owned()
                    + &(1..width)
                        .map(|slot| format!("WITH v{} AS v{slot} ", slot - 1))
                        .collect::<String>()
                    + &format!("RETURN v{}", width - 1)
            };
            let syntax = helix_cypher::parse(&source).unwrap();
            let (query, count) = allocations::observe(|| helix_cypher::resolve(&syntax));
            let query = query.unwrap();
            // These ceilings allow ordinary container growth but reject a map
            // per expanding scope. Sparse alias IDs must remain economical too.
            let bytes_per_binding = if growing { 2048 } else { 4096 };
            assert!(
                count.bytes <= bytes_per_binding * (width + 1),
                "growing={growing}, width={width}, count={count:?}"
            );
            assert!(
                count.allocations <= 32 * (width + 1),
                "growing={growing}, width={width}, count={count:?}"
            );
            assert_eq!(query.layout().width(), if growing { width + 1 } else { 2 });
            assert_eq!(query.contracts().len(), width + 1);
            let last = query.contracts().last().unwrap();
            assert_eq!(last.input().len(), if growing { width } else { 1 });
            assert_eq!(last.output().len(), 1);
            assert!(last.output().get(query.returns()[0].1).is_some());
            assert!(last.output().get(r::Slot(u32::MAX)).is_none());
            assert_eq!(last.output().iter().count(), 1);
        }
    }
}
