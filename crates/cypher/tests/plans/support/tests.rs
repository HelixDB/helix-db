use super::*;

#[test]
fn normalization_keeps_user_data_and_only_removes_elapsed_metrics() {
    let user = json!({"optimization_micros": 99, "metrics": {"optimization_micros": 77}});
    let native = json!({"metrics": {"optimization_micros": 123, "rule_fires": 7}, "literal": user});
    let mut row = json!({
        "planner": {"optimization_micros": 456, "rule_fires": 9},
        "bindings": user,
        "operators": [
            {"graph": null, "logical": user},
            {"graph": {"sources": [{"access": native}]}, "logical": user},
        ]
    });
    normalize_cypher(&mut row);
    assert!(row["planner"].get("optimization_micros").is_none());
    assert_eq!(row["planner"]["rule_fires"], 9);
    assert_eq!(row["bindings"], user);
    assert_eq!(row["operators"][0]["logical"], user);
    assert_eq!(row["operators"][1]["logical"], user);
    let access = &row["operators"][1]["graph"]["sources"][0]["access"];
    assert!(access["metrics"].get("optimization_micros").is_none());
    assert_eq!(access["metrics"]["rule_fires"], 7);
    assert_eq!(access["literal"], user);
}

#[test]
fn captures_replay_exact_inputs_and_sign_every_plan_field() {
    let input = Input {
        query: Query::Cypher("RETURN {optimization_micros: 42} AS value".into()),
        context: context::PlannerContext::default(),
    };
    let capture = Capture::plan(BTreeMap::from([("literal".into(), input)]));
    let manifest = capture.manifest();
    let mut replay = capture.replay();
    assert_eq!(replay.manifest(), manifest);
    replay.cases.get_mut("literal").unwrap().outcome["unrecognized_future_contract"] = json!(true);
    assert_ne!(replay.manifest(), manifest, "new fields require review too");
    let mut replay = replay.replay();
    assert_eq!(replay.manifest(), manifest);
    replay
        .cases
        .get_mut("literal")
        .unwrap()
        .input
        .context
        .optimizer_limits
        .optimization_micros = helix_planner::properties::PositiveUsize::at_least_one(123);
    assert_ne!(
        replay.manifest(),
        manifest,
        "configured time budget is an input, not elapsed time"
    );
}

#[test]
fn matrix_preserves_all_families_and_reports_exhausted_search() {
    let capture = Capture::plan(inputs());
    assert_eq!(capture.cases.len(), 810);
    let mut exhausted = 0;
    for (name, case) in &capture.cases {
        assert!(
            case.outcome.get("error").is_none(),
            "{name}: {}",
            case.outcome
        );
        if name.contains("exhausted=true") {
            assert_eq!(
                case.outcome["plan"]["planner"]["guardrail_hit"], true,
                "{name}"
            );
            exhausted += 1;
        }
        if name.contains("cold_") {
            assert_eq!(
                case.input.context.storage.object_get_latency,
                case.input.context.storage.authoritative_verify_per_id
            );
        }
    }
    assert_eq!(exhausted, 15);
    assert_eq!(
        capture
            .cases
            .keys()
            .filter(|key| key.starts_with("native/"))
            .count(),
        108
    );
}

#[test]
fn sdk_fixture_denominators_cannot_silently_shrink() {
    let missing = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/plans/this-directory-must-not-exist");
    assert!(std::panic::catch_unwind(|| sdk_inputs(&missing, 248)).is_err());
}

#[test]
fn sdk_catalog_scenarios_keep_successes_and_missing_index_errors() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/plans/sdk-fixture");
    assert!(std::panic::catch_unwind(|| sdk_inputs(&root, 3)).is_err());
    let capture = Capture::plan(sdk_inputs(&root, 2));
    assert_eq!(capture.cases.len(), 12);
    for (name, case) in &capture.cases {
        if name.ends_with("search=true") {
            assert!(
                case.outcome.get("plan").is_some(),
                "{name}: {}",
                case.outcome
            );
        } else {
            assert!(
                case.outcome["error"]
                    .as_str()
                    .unwrap()
                    .contains("MissingSearchIndex"),
                "{name}"
            );
        }
    }
}

#[test]
fn derived_execution_exposes_nested_demand_and_mutation_boundaries() {
    let mut matrix = inputs();
    let cases = [
        "native/BranchHeavyQueries/scale=1/storage=default",
        "native/ForEachBodyRootReuse/scale=1/storage=default",
        "native/OrderedRangeWindowPushdown/scale=1/storage=default",
    ];
    let mut capture = Capture::plan(
        cases
            .into_iter()
            .map(|name| (name.to_string(), matrix.remove(name).unwrap()))
            .collect(),
    );
    let branch = &capture.cases[cases[0]].outcome["execution"]["dag"];
    let children = &branch["steps"][1]["children"];
    for child in ["union/0", "union/1"] {
        assert_eq!(
            children[child]["regions"],
            json!([{"terminal": 3, "steps": [1, 2, 3]}])
        );
        assert_eq!(children[child]["steps"][0]["absorbed"], true);
        assert_eq!(children[child]["steps"][2]["absorbed"], false);
    }
    let foreach = &capture.cases[cases[1]].outcome["execution"]["dag"]["steps"][0];
    assert_eq!(foreach["pull_capability"], "Boundary");
    assert_eq!(foreach["absorbed"], false);
    assert_eq!(
        foreach["children"]["foreach"]["steps"][0]["pull_capability"],
        "Prepared"
    );
    let window = &capture.cases[cases[2]].outcome["execution"]["dag"];
    assert_eq!(window["regions"], json!([{"terminal": 2, "steps": [1, 2]}]));
    assert_eq!(window["steps"][0]["absorbed"], true);
    assert_eq!(
        capture.cases[cases[0]].outcome["execution"]["returns"],
        json!([{"name": "result", "shape": "list"}])
    );
    assert_eq!(
        capture.cases[cases[1]].outcome["execution"]["returns"],
        json!([])
    );
    let manifest = capture.manifest();
    capture.cases.get_mut(cases[0]).unwrap().outcome["execution"]["returns"][0]["shape"] =
        json!("object");
    assert_ne!(
        capture.manifest(),
        manifest,
        "return shape changes require review"
    );
}

#[test]
fn control_flow_capture_cannot_omit_a_nested_program() {
    use helix_ast::{batch, expr, traversal};
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/plans/control-fixture");
    let mut cases = sdk_inputs(&root, 2);
    cases.insert(
        "foreach".into(),
        inputs()
            .remove("native/ForEachBodyRootReuse/scale=1/storage=default")
            .unwrap(),
    );
    let query = batch::BatchQuery::Read(
        batch::read_batch()
            .var_as(
                "result",
                traversal::g().n_with_label("User").choose(
                    expr::Predicate::eq("active", true),
                    traversal::sub().limit(1),
                    None,
                ),
            )
            .returning(["result"]),
    );
    cases.insert(
        "choose".into(),
        Input {
            query: Query::Native(Box::new(query)),
            context: context::PlannerContext::default(),
        },
    );
    let capture = Capture::plan(cases);
    assert_eq!(capture.cases.len(), 14);
    let mut kinds = std::collections::BTreeSet::new();
    for (name, case) in &capture.cases {
        assert!(
            case.outcome.get("error").is_none(),
            "{name}: {}",
            case.outcome
        );
        let mut pending = vec![&case.outcome["execution"]["dag"]];
        while let Some(dag) = pending.pop() {
            assert!(!dag["order"]["stages"].as_array().unwrap().is_empty());
            for step in dag["steps"].as_array().unwrap() {
                for (name, child) in step["children"].as_object().unwrap() {
                    kinds.insert(name.split('/').next().unwrap().to_string());
                    pending.push(child);
                }
            }
        }
    }
    assert_eq!(
        kinds,
        ["union", "then", "else", "coalesce", "optional", "repeat", "foreach"]
            .map(str::to_string)
            .into()
    );
    let choose = &capture.cases["choose"].outcome["execution"]["dag"];
    let branches: Vec<_> = choose["steps"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|step| !step["children"].as_object().unwrap().is_empty())
        .collect();
    assert_eq!(branches.len(), 1);
    assert_eq!(
        branches[0]["children"]
            .as_object()
            .unwrap()
            .keys()
            .collect::<Vec<_>>(),
        vec!["then"]
    );
}

#[test]
fn compact_execution_cells_are_signed() {
    let key = "cypher/composed_limits/rows=100/indexed=false/storage=default";
    let mut capture = Capture::plan(BTreeMap::from([(
        key.to_string(),
        inputs().remove(key).unwrap(),
    )]));
    let case = &capture.cases[key];
    assert_eq!(
        case.outcome["plan"]["bindings"].as_array().unwrap().len(),
        3
    );
    assert_eq!(case.outcome["execution"]["width"], 2);
    assert_eq!(case.outcome["execution"]["layout"], "Compact");
    let manifest = capture.manifest();
    capture.cases.get_mut(key).unwrap().outcome["execution"]["width"] = json!(3);
    assert_ne!(
        capture.manifest(),
        manifest,
        "extra retained cells require review"
    );
    assert_eq!(capture.replay().manifest(), manifest);
}
