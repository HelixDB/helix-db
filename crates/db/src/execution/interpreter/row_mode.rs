//! Row-mode runtime guardrails.
//!
//! Row bindings can fan out quickly once a traversal enters row mode. This
//! module owns the optional process-level cap so execution code can enforce it
//! consistently without spreading environment parsing through traversal ops.

use std::num::NonZeroUsize;

use super::*;

const ROW_MODE_MAX_ROWS_ENV: &str = "HELIX_ROW_MODE_MAX_ROWS";

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(in crate::execution::interpreter) enum RowModeMaxRowsSetting {
    #[default]
    Unread,
    Disabled,
    Enabled(RowModeMaxRows),
}

impl RowModeMaxRowsSetting {
    fn resolve(&mut self) -> Result<Option<RowModeMaxRows>> {
        match *self {
            Self::Unread => {
                let resolved = RowModeMaxRows::from_env()?;
                *self = match resolved {
                    Some(cap) => Self::Enabled(cap),
                    None => Self::Disabled,
                };
                Ok(resolved)
            }
            Self::Disabled => Ok(None),
            Self::Enabled(cap) => Ok(Some(cap)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::execution::interpreter) struct RowModeMaxRows(NonZeroUsize);

impl RowModeMaxRows {
    fn get(self) -> usize {
        self.0.get()
    }

    fn from_env() -> Result<Option<Self>> {
        match std::env::var(ROW_MODE_MAX_ROWS_ENV) {
            Ok(raw) => parse_row_mode_max_rows(&raw).map(Some),
            Err(std::env::VarError::NotPresent) => Ok(None),
            Err(std::env::VarError::NotUnicode(_)) => Err(invalid_row_mode_cap(
                "value is not valid unicode".to_string(),
            )),
        }
    }
}

impl<'db> ExecutionContext<'db> {
    pub(in crate::execution::interpreter) fn enforce_row_mode_cap(
        &mut self,
        op_name: &'static str,
        value: &ExecutionValue,
    ) -> Result<()> {
        let ExecutionValue::Stream(rows) = value else {
            return Ok(());
        };
        if !rows_are_in_row_mode(rows) {
            return Ok(());
        }
        let Some(cap) = self.row_mode_max_rows.resolve()? else {
            return Ok(());
        };
        if rows.len() <= cap.get() {
            return Ok(());
        }
        Err(HelixDbError::Query(format!(
            "{op_name} produced {} row-mode rows, exceeding {ROW_MODE_MAX_ROWS_ENV}={}",
            rows.len(),
            cap.get()
        )))
    }
}

pub(in crate::execution::interpreter) fn op_name(op: &exec::ExecOp) -> &'static str {
    match op {
        exec::ExecOp::Access { .. } => "source()",
        exec::ExecOp::KvRead(_) => "kv_read()",
        exec::ExecOp::Expand { plan } => expand_op_name(plan),
        exec::ExecOp::Filter { .. } => "filter()",
        exec::ExecOp::Limit { .. } => "limit()",
        exec::ExecOp::Skip { .. } => "skip()",
        exec::ExecOp::Range { .. } => "range()",
        exec::ExecOp::Distinct => "dedup()",
        exec::ExecOp::Order { .. } => "order_by()",
        exec::ExecOp::Project { projection } => projection_op_name(projection),
        exec::ExecOp::Aggregate { .. } => "aggregate()",
        exec::ExecOp::Variable { op } => variable_op_name(op),
        exec::ExecOp::Branch { plan } => branch_op_name(plan),
        exec::ExecOp::Repeat { .. } => "repeat()",
        exec::ExecOp::ShortestPath { .. } => "shortest_path()",
        exec::ExecOp::Mutation { .. } => "mutation()",
        exec::ExecOp::IndexDdl { .. } => "index_ddl()",
        exec::ExecOp::Merge { .. } => "merge()",
        exec::ExecOp::Reserved { op } => reserved_op_name(op),
        exec::ExecOp::ForEach { .. } => "for_each()",
        exec::ExecOp::Barrier { .. } => "barrier()",
        exec::ExecOp::Noop => "noop()",
    }
}

fn expand_op_name(plan: &ir::ExpandPlan) -> &'static str {
    match (plan.direction, plan.output) {
        (ir::ExpandDirection::Out, ir::ExpandOutput::Nodes) => "out()",
        (ir::ExpandDirection::In, ir::ExpandOutput::Nodes) => "in_()",
        (ir::ExpandDirection::Both, ir::ExpandOutput::Nodes) => "both()",
        (ir::ExpandDirection::Out, ir::ExpandOutput::Edges) => "out_e()",
        (ir::ExpandDirection::In, ir::ExpandOutput::Edges) => "in_e()",
        (ir::ExpandDirection::Both, ir::ExpandOutput::Edges) => "both_e()",
    }
}

fn projection_op_name(plan: &ir::ProjectionPlan) -> &'static str {
    match plan {
        ir::ProjectionPlan::Count => "count()",
        ir::ProjectionPlan::Exists => "exists()",
        ir::ProjectionPlan::Id => "id()",
        ir::ProjectionPlan::Label => "label()",
        ir::ProjectionPlan::Values(_) => "values()",
        ir::ProjectionPlan::ValueMap(_) => "value_map()",
        ir::ProjectionPlan::Project(_) => "project()",
        ir::ProjectionPlan::ProjectBindings { .. } => "project_bindings()",
        ir::ProjectionPlan::EdgeProperties => "edge_properties()",
    }
}

fn variable_op_name(op: &exec::ExecVariableOp) -> &'static str {
    match op {
        exec::ExecVariableOp::SourceInject { .. } => "source_inject()",
        exec::ExecVariableOp::Stream(op) => match op {
            ir::StreamVariableOp::As(_) => "as()",
            ir::StreamVariableOp::Store(_) => "store()",
            ir::StreamVariableOp::Select(_) => "select()",
            ir::StreamVariableOp::Bind(_) => "bind()",
            ir::StreamVariableOp::Inject(_) => "inject()",
            ir::StreamVariableOp::Within(_) => "within()",
            ir::StreamVariableOp::Without(_) => "without()",
        },
    }
}

fn branch_op_name(plan: &exec::ExecBranchPlan) -> &'static str {
    match plan {
        exec::ExecBranchPlan::Union(_) => "union()",
        exec::ExecBranchPlan::Choose { .. } => "choose()",
        exec::ExecBranchPlan::ChooseElse { .. } => "choose()",
        exec::ExecBranchPlan::Coalesce(_) => "coalesce()",
        exec::ExecBranchPlan::Optional(_) => "optional()",
    }
}

fn reserved_op_name(op: &ir::ReservedOp) -> &'static str {
    match op {
        ir::ReservedOp::Fold => "fold()",
        ir::ReservedOp::Unfold => "unfold()",
        ir::ReservedOp::Path => "path()",
        ir::ReservedOp::SimplePath => "simple_path()",
        ir::ReservedOp::WithSack(_) => "with_sack()",
        ir::ReservedOp::SackSet(_) => "sack_assign()",
        ir::ReservedOp::SackAdd(_) => "sack_add()",
        ir::ReservedOp::SackGet => "sack()",
    }
}

fn rows_are_in_row_mode(rows: &[ExecutionRow]) -> bool {
    rows.iter().any(|row| !row.bindings.is_empty())
}

fn parse_row_mode_max_rows(raw: &str) -> Result<RowModeMaxRows> {
    let trimmed = raw.trim();
    let value = trimmed
        .parse::<usize>()
        .map_err(|_| invalid_row_mode_cap(format!("got `{raw}`")))?;
    NonZeroUsize::new(value)
        .map(RowModeMaxRows)
        .ok_or_else(|| invalid_row_mode_cap(format!("got `{raw}`")))
}

fn invalid_row_mode_cap(reason: String) -> HelixDbError {
    HelixDbError::Query(format!(
        "{ROW_MODE_MAX_ROWS_ENV} must be a positive integer; {reason}"
    ))
}

#[cfg(test)]
mod tests {
    use helix_ast::expr::Predicate;
    use helix_planner::context;

    use super::test_support;
    use super::*;

    fn row(id: u64, bound: bool) -> ExecutionRow {
        let mut row = ExecutionRow::current(ElementRef::Node(id));
        if bound {
            row.bindings
                .insert(test_support::name("source"), ElementRef::Node(id));
        }
        row
    }

    fn expand(direction: ir::ExpandDirection, output: ir::ExpandOutput) -> ir::ExpandPlan {
        ir::ExpandPlan {
            direction,
            output,
            label: ir::ExpandLabelPlan::Any,
        }
    }

    #[test]
    fn parse_row_mode_max_rows_accepts_positive_integers() {
        assert_eq!(parse_row_mode_max_rows("1").unwrap().get(), 1);
        assert_eq!(parse_row_mode_max_rows(" 42 ").unwrap().get(), 42);
    }

    #[test]
    fn parse_row_mode_max_rows_rejects_zero_and_invalid_values() {
        for raw in ["0", "-1", "abc", ""] {
            let err = parse_row_mode_max_rows(raw).unwrap_err();
            assert!(
                err.to_string()
                    .contains("HELIX_ROW_MODE_MAX_ROWS must be a positive integer"),
                "{err}"
            );
        }
    }

    #[test]
    fn op_name_reports_user_facing_row_mode_operations() {
        assert_eq!(
            op_name(&exec::ExecOp::Variable {
                op: exec::ExecVariableOp::Stream(ir::StreamVariableOp::Bind(test_support::name(
                    "row"
                )))
            }),
            "bind()"
        );
        assert_eq!(
            op_name(&exec::ExecOp::Expand {
                plan: expand(ir::ExpandDirection::Out, ir::ExpandOutput::Nodes)
            }),
            "out()"
        );
        assert_eq!(
            op_name(&exec::ExecOp::Expand {
                plan: expand(ir::ExpandDirection::In, ir::ExpandOutput::Edges)
            }),
            "in_e()"
        );

        let branch = Box::new(test_support::subplan(
            vec![test_support::step(1, Vec::new(), exec::ExecOp::Noop)],
            1,
        ));
        assert_eq!(
            op_name(&exec::ExecOp::Branch {
                plan: exec::ExecBranchPlan::Optional(branch)
            }),
            "optional()"
        );
        assert_eq!(
            op_name(&exec::ExecOp::Merge {
                mode: exec::ExecMergeMode::Union
            }),
            "merge()"
        );

        assert_eq!(
            op_name(&exec::ExecOp::Filter {
                predicate: ir::PredicatePlan::new(Predicate::eq("active", true)).unwrap(),
            }),
            "filter()"
        );
        assert_eq!(
            expand_op_name(&expand(ir::ExpandDirection::Both, ir::ExpandOutput::Nodes,)),
            "both()"
        );
        assert_eq!(projection_op_name(&ir::ProjectionPlan::Exists), "exists()");
        assert_eq!(
            projection_op_name(&ir::ProjectionPlan::Project(
                ir::ProjectionItems::new(ir::AtLeast::from_one_and_rest(
                    ir::ProjectionItem::Property {
                        source: test_support::name("name"),
                        alias: test_support::name("display"),
                    },
                    Vec::new(),
                ))
                .unwrap(),
            )),
            "project()"
        );

        for (op, expected) in [
            (ir::StreamVariableOp::As(test_support::name("v")), "as()"),
            (
                ir::StreamVariableOp::Select(test_support::name("v")),
                "select()",
            ),
            (
                ir::StreamVariableOp::Inject(test_support::name("v")),
                "inject()",
            ),
            (
                ir::StreamVariableOp::Within(test_support::name("v")),
                "within()",
            ),
            (
                ir::StreamVariableOp::Without(test_support::name("v")),
                "without()",
            ),
        ] {
            assert_eq!(
                variable_op_name(&exec::ExecVariableOp::Stream(op)),
                expected
            );
        }
    }

    #[tokio::test]
    async fn cap_is_enforced_only_after_rows_enter_row_mode() {
        let db = test_support::open_db("row-mode-cap-only-after-bindings").await;
        let mut ctx = ExecutionContext::new(&db, context::ParamBindings::default());
        ctx.row_mode_max_rows = RowModeMaxRowsSetting::Enabled(RowModeMaxRows(
            NonZeroUsize::new(1).expect("test cap is positive"),
        ));

        ctx.enforce_row_mode_cap(
            "n()",
            &ExecutionValue::Stream(vec![row(1, false), row(2, false)]),
        )
        .expect("unbound rows are not row mode");
        ctx.enforce_row_mode_cap("bind()", &ExecutionValue::Stream(vec![row(1, true)]))
            .expect("rows at the cap are accepted");

        let err = ctx
            .enforce_row_mode_cap(
                "bind()",
                &ExecutionValue::Stream(vec![row(1, true), row(2, true)]),
            )
            .expect_err("bound rows above the cap are rejected");
        assert!(
            err.to_string()
                .contains("bind() produced 2 row-mode rows, exceeding HELIX_ROW_MODE_MAX_ROWS=1"),
            "{err}"
        );

        let mut disabled = RowModeMaxRowsSetting::Disabled;
        assert_eq!(disabled.resolve().unwrap(), None);
    }
}
