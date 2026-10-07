//! Corpus access and environment reporting shared by the benchmark binaries.

#![allow(dead_code)] // Each benchmark binary uses a different subset.

pub mod scoped_bump;

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::OnceLock;

use helix_ast::batch::BatchQuery;
use helix_ast::query::QueryRequest;
use helix_ast::testing::{self, Shape};
use helix_planner::context::PlannerContext;
use helix_planner::{exec, experiments, planning};

/// Every request shape, built once per benchmark process.
pub fn shapes() -> &'static [Shape] {
    static SHAPES: OnceLock<Vec<Shape>> = OnceLock::new();
    SHAPES.get_or_init(testing::all)
}

/// The shape named `name`.
///
/// # Panics
///
/// Panics when no shape has that name.
pub fn shape(name: &str) -> &'static Shape {
    shapes()
        .iter()
        .find(|shape| shape.name == name)
        .unwrap_or_else(|| panic!("no shape named {name}"))
}

/// Names of every shape, in corpus order.
pub fn shape_names() -> impl Iterator<Item = &'static str> {
    shapes().iter().map(|shape| shape.name.as_str())
}

/// Names of the shapes the planner accepts with an empty catalog.
pub fn plannable_shape_names() -> impl Iterator<Item = &'static str> {
    static NAMES: OnceLock<Vec<&'static str>> = OnceLock::new();
    NAMES
        .get_or_init(|| {
            shapes()
                .iter()
                .filter(|shape| {
                    let request =
                        QueryRequest::from_json_slice(&shape.json).expect("corpus shapes parse");
                    let (batch, parameters) = request.into_query();
                    let context = helix_ast_bench::planner_context(
                        helix_ast_bench::param_bindings(parameters),
                    );
                    helix_planner::planning::plan_with_diagnostics(&batch, &context).is_ok()
                })
                .map(|shape| shape.name.as_str())
                .collect()
        })
        .iter()
        .copied()
}

/// Shapes the multi-threaded runs use: small reads, a wide projection, a
/// wide batch and a bulk write.
pub const THROUGHPUT_SHAPES: [&str; 4] = [
    "dynamic-read",
    "ordered-range-wide-projection",
    "wide_batch/1000",
    "bulk_write_untyped/1000x768",
];

/// Print which SIMD path simd-json detected on this CPU, so every result
/// records what actually ran.
pub fn print_environment() {
    eprintln!(
        "simd-json: {:?} (runtime); allocator: mimalloc",
        simd_json::Deserializer::algorithm()
    );
}

/// One planning workload.
pub enum PlanInput {
    /// A corpus request against an empty catalog, planned as
    /// `query_service` plans it.
    Request {
        batch: BatchQuery,
        context: PlannerContext,
    },
    /// One of the planner's own scalability fixtures, with its catalog.
    Fixture(experiments::PlanningScalabilityCase),
}

/// What planning a [`PlanInput`] returns.
#[derive(Clone)]
#[expect(
    clippy::large_enum_variant,
    reason = "boxing would add an allocation to every plan the benchmarks time"
)]
pub enum PlanOutput {
    Request(planning::PlanningOutput),
    Fixture(exec::ExecutablePlan),
}

impl PlanInput {
    /// Plan this workload.
    ///
    /// # Panics
    ///
    /// Panics when planning fails; every input plans.
    pub fn plan(&self) -> PlanOutput {
        match self {
            Self::Request { batch, context } => PlanOutput::Request(
                planning::plan_with_diagnostics(batch, context).expect("plannable shapes plan"),
            ),
            Self::Fixture(case) => PlanOutput::Fixture(case.plan().expect("fixtures plan")),
        }
    }
}

/// Every plannable corpus shape, then every planner scalability fixture
/// (named `fixture/<shape>/<scale>`), built once per benchmark process.
pub fn plan_inputs() -> &'static [(String, PlanInput)] {
    static INPUTS: OnceLock<Vec<(String, PlanInput)>> = OnceLock::new();
    INPUTS.get_or_init(|| {
        plannable_shape_names()
            .map(str::to_owned)
            .chain(fixtures().map(|(name, _)| name))
            .map(|name| {
                let input = build_plan_input(&name);
                (name, input)
            })
            .collect()
    })
}

/// The planner scalability fixtures, by benchmark name.
fn fixtures() -> impl Iterator<Item = (String, experiments::PlanScalabilityFixture)> {
    experiments::default_planning_scalability_fixtures()
        .into_iter()
        .map(|fixture| {
            (
                format!("fixture/{:?}/{}", fixture.shape(), fixture.scale().get()),
                fixture,
            )
        })
}

/// Build a fresh copy of the workload named `name`.
fn build_plan_input(name: &str) -> PlanInput {
    match fixtures().find(|(candidate, _)| candidate == name) {
        Some((_, fixture)) => PlanInput::Fixture(fixture.case()),
        None => {
            let request =
                QueryRequest::from_json_slice(&shape(name).json).expect("corpus shapes parse");
            let (batch, parameters) = request.into_query();
            let context =
                helix_ast_bench::planner_context(helix_ast_bench::param_bindings(parameters));
            PlanInput::Request { batch, context }
        }
    }
}

/// Run `work` on this thread's own copy of the workload named `name`.
///
/// Each request plans against its own context, so threads must not share
/// one: shared reference-counted planner values would make every core write
/// the same counters. A thread builds its copy on first use.
///
/// # Panics
///
/// Panics when no workload has that name.
pub fn with_thread_plan_input<T>(name: &str, work: impl FnOnce(&PlanInput) -> T) -> T {
    thread_local! {
        static INPUTS: RefCell<BTreeMap<String, Rc<PlanInput>>> = const { RefCell::new(BTreeMap::new()) };
    }
    let input = INPUTS.with_borrow_mut(|inputs| {
        Rc::clone(
            inputs
                .entry(name.to_owned())
                .or_insert_with(|| Rc::new(build_plan_input(name))),
        )
    });
    work(&input)
}

/// Names of every [`plan_inputs`] entry, in order.
pub fn plan_input_names() -> impl Iterator<Item = &'static str> {
    plan_inputs().iter().map(|(name, _)| name.as_str())
}

/// The planning workload named `name`.
///
/// # Panics
///
/// Panics when no workload has that name.
pub fn plan_input(name: &str) -> &'static PlanInput {
    plan_inputs()
        .iter()
        .find_map(|(candidate, input)| (candidate == name).then_some(input))
        .unwrap_or_else(|| panic!("no planning workload named {name}"))
}
