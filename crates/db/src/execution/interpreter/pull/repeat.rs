//! Repeat preserves whole-frontier iteration and until semantics. Emitted rows
//! can satisfy downstream demand before the next frontier is started.
use super::*;

enum Phase {
    Initialize,
    Before,
    Body,
    After,
    Stop,
    Done,
}

pub(super) struct Repeat<'a> {
    plan: &'a exec::ExecRepeatPlan,
    input: Option<Box<Cursor<'a>>>,
    frontier: Vec<ExecutionRow>,
    pending: std::vec::IntoIter<ExecutionRow>,
    phase: Phase,
    depth: usize,
    maximum: usize,
    frame_context: ExecutionValue,
    after_index: usize,
}

impl<'a> Repeat<'a> {
    pub(super) fn new(plan: &'a exec::ExecRepeatPlan, input: Box<Cursor<'a>>) -> Self {
        let maximum = match &plan.stop {
            ir::RepeatStopPlan::MaxDepthOnly | ir::RepeatStopPlan::Until { .. } => {
                plan.max_depth.get()
            }
            ir::RepeatStopPlan::Times { count }
            | ir::RepeatStopPlan::TimesOrUntil { count, .. } => {
                plan.max_depth.get().min(count.get())
            }
        };
        Self {
            plan,
            input: Some(input),
            frontier: Vec::new(),
            pending: Vec::new().into_iter(),
            phase: Phase::Initialize,
            depth: 0,
            maximum,
            frame_context: ExecutionValue::Stream(Vec::new()),
            after_index: 0,
        }
    }

    pub(super) async fn next(
        &mut self,
        ctx: &mut ExecutionContext<'_>,
    ) -> Result<Option<ExecutionValue>> {
        loop {
            ctx.check_execution_deadline()?;
            if let Some(row) = self.pending.next() {
                return Ok(Some(ExecutionValue::Stream(vec![row])));
            }
            match self.phase {
                Phase::Initialize => {
                    if matches!(
                        self.plan.emit,
                        ir::RepeatEmitPlan::Before | ir::RepeatEmitPlan::All
                    ) {
                        let item = self
                            .input
                            .as_mut()
                            .expect("repeat opens once")
                            .next(ctx)
                            .await?;
                        if let Some(item) = item {
                            let rows = ctx.stream_rows(item, "repeat")?;
                            self.frontier.extend(rows.iter().cloned());
                            return Ok(Some(ExecutionValue::Stream(rows)));
                        }
                        self.input = None;
                        self.phase = Phase::Body;
                    } else {
                        let value = self
                            .input
                            .take()
                            .expect("repeat opens once")
                            .drain(ctx)
                            .await?;
                        self.frontier = ctx.stream_rows(value, "repeat")?;
                        self.phase = Phase::Before;
                    }
                }
                Phase::Before => {
                    if matches!(
                        self.plan.emit,
                        ir::RepeatEmitPlan::Before | ir::RepeatEmitPlan::All
                    ) {
                        self.pending = self.frontier.clone().into_iter();
                    }
                    self.phase = Phase::Body;
                }
                Phase::Body => {
                    self.frame_context = ExecutionValue::Stream(self.frontier.clone());
                    self.after_index = 0;
                    let mut body = Cursor::scoped_subplan(
                        ctx,
                        &self.plan.body,
                        ExecutionValue::Stream(self.frontier.clone()),
                    )?;
                    let value = body.drain(ctx).await?;
                    self.frontier = ctx.stream_rows(value, "repeat")?;
                    self.depth += 1;
                    self.phase = Phase::After;
                }
                Phase::After => {
                    if let ir::RepeatEmitPlan::AfterIf { predicate } = &self.plan.emit {
                        while self.after_index < self.frontier.len() {
                            let row = &self.frontier[self.after_index];
                            self.after_index += 1;
                            let accepted = {
                                let scope = scope::Scope::new(ctx, &mut self.frame_context);
                                scope.context.eval_predicate_plan(row, predicate).await
                            };
                            if accepted? {
                                return Ok(Some(ExecutionValue::Stream(vec![row.clone()])));
                            }
                        }
                    } else if matches!(
                        self.plan.emit,
                        ir::RepeatEmitPlan::After | ir::RepeatEmitPlan::All
                    ) {
                        self.pending = self.frontier.clone().into_iter();
                    }
                    self.phase = Phase::Stop;
                }
                Phase::Stop => {
                    let stop = {
                        let scope = scope::Scope::new(ctx, &mut self.frame_context);
                        scope
                            .context
                            .repeat_should_stop(&self.frontier, &self.plan.stop)
                            .await?
                    };
                    if stop || self.frontier.is_empty() || self.depth >= self.maximum {
                        if matches!(self.plan.emit, ir::RepeatEmitPlan::None) {
                            self.pending = std::mem::take(&mut self.frontier).into_iter();
                        }
                        self.phase = Phase::Done;
                    } else {
                        self.phase = Phase::Before;
                    }
                }
                Phase::Done => return Ok(None),
            }
        }
    }
}
