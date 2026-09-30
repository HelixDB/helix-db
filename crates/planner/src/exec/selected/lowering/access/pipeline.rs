use super::*;

impl ExecutableDagBuilder<'_> {
    pub(in crate::exec::selected::lowering) fn push_selected_stream_pipeline_op(
        &mut self,
        op: &logical::StreamPipelineOp,
        input_id: ExecStepId,
        delivered: properties::DeliveredProperties,
        output: ir::BatchOutputPlan,
        condition: ExecCondition,
    ) -> Result<ExecStepId, ExecPlanError> {
        let rows = selected_rows_for_delivered(&delivered, self.profile);
        let draft = match op {
            logical::StreamPipelineOp::Filter { predicate } => StepDraft {
                dependencies: vec![input_id],
                output,
                condition,
                op: ExecOp::Filter {
                    predicate: predicate.clone(),
                },
                schedule: ExecSchedule::Pipeline,
                delivered: filtered_delivered_properties(delivered),
                cost: self.profile.stored_predicate_filter(rows),
            },
            logical::StreamPipelineOp::IndexMembership { plan } => StepDraft {
                dependencies: vec![input_id],
                output,
                condition,
                op: ExecOp::IndexMembership {
                    plan: Box::new(crate::exec::ExecNodeIndexMembershipPlan::from(
                        plan.as_ref(),
                    )),
                },
                schedule: ExecSchedule::Pipeline,
                delivered: filtered_delivered_properties(delivered),
                // Lowering has no statistics; selection already priced the set.
                cost: self.profile.index_membership_filter(
                    match plan.set() {
                        ir::NodeMembershipSet::Index { .. } => self
                            .profile
                            .bitmap_equality_lookup(self.profile.default_equality_index_rows),
                        ir::NodeMembershipSet::Labels(labels) => self.profile.parallel(
                            &labels
                                .iter()
                                .map(|_| {
                                    self.profile.bitmap_equality_lookup(
                                        self.profile.default_unknown_scan_rows,
                                    )
                                })
                                .collect::<Vec<_>>(),
                            self.profile.max_parallel_kv_reads,
                        ),
                    },
                    match plan.set() {
                        ir::NodeMembershipSet::Index {
                            outside_label: ir::NodeMembershipOutsideLabel::Evaluate,
                            ..
                        } => Some(crate::cost::MembershipLabelDomain {
                            read: self
                                .profile
                                .bitmap_equality_lookup(self.profile.default_unknown_scan_rows),
                            label_rows: self.profile.default_unknown_scan_rows,
                            predicate: plan.predicate().as_ref(),
                        }),
                        ir::NodeMembershipSet::Index {
                            outside_label: ir::NodeMembershipOutsideLabel::Reject,
                            ..
                        }
                        | ir::NodeMembershipSet::Labels(_) => None,
                    },
                    plan.residual().map(AsRef::as_ref),
                    rows,
                    self.profile.default_equality_index_rows,
                ),
            },
            logical::StreamPipelineOp::Window { window } => selected_access_window_step_draft(
                *window,
                input_id,
                delivered,
                rows,
                output,
                condition,
                self.profile,
            )?,
            logical::StreamPipelineOp::Limit { count } => {
                let literal_count = stream_bound_literal(count);
                StepDraft {
                    dependencies: vec![input_id],
                    output,
                    condition,
                    op: ExecOp::Limit {
                        count: count.clone(),
                    },
                    schedule: ExecSchedule::Pipeline,
                    delivered: limit_delivered_properties(delivered, literal_count),
                    cost: self.profile.stream_operator(estimated_rows_bounded_by(
                        rows,
                        literal_count.map(|count| count as u64),
                    )),
                }
            }
            logical::StreamPipelineOp::Skip { count } => StepDraft {
                dependencies: vec![input_id],
                output,
                condition,
                op: ExecOp::Skip {
                    count: count.clone(),
                },
                schedule: ExecSchedule::Pipeline,
                delivered: skip_delivered_properties(delivered, stream_bound_literal(count)),
                cost: self.profile.stream_operator(rows),
            },
            logical::StreamPipelineOp::Range { range } => {
                let literal_bounds = stream_range_literal_bounds(range);
                StepDraft {
                    dependencies: vec![input_id],
                    output,
                    condition,
                    op: ExecOp::Range {
                        range: range.clone(),
                    },
                    schedule: ExecSchedule::Pipeline,
                    delivered: range_delivered_properties(delivered, literal_bounds),
                    cost: self.profile.stream_operator(estimated_rows_bounded_by(
                        rows,
                        literal_bounds.map(|(start, end)| end.saturating_sub(start) as u64),
                    )),
                }
            }
            logical::StreamPipelineOp::Order { ordering } => StepDraft {
                dependencies: vec![input_id],
                output,
                condition,
                op: ExecOp::Order {
                    plan: ir::OrderPlan::ExplicitSort(ordering.clone()),
                },
                schedule: ExecSchedule::Barrier,
                delivered: ordered_delivered_properties(
                    materialized_delivered_properties(delivered),
                    properties::DeliveredOrdering::ByKeys(ordering.clone()),
                ),
                cost: self.profile.explicit_sort(rows),
            },
            logical::StreamPipelineOp::Expand { plan } => StepDraft {
                dependencies: vec![input_id],
                output,
                condition,
                op: ExecOp::Expand { plan: plan.clone() },
                schedule: ExecSchedule::Pipeline,
                delivered: expand_delivered_properties(plan),
                cost: self.profile.stream_operator(rows),
            },
            logical::StreamPipelineOp::VectorSearch { plan } => {
                let k = match plan.as_ref() {
                    ir::RestrictedVectorSearchPlan::Nodes { k, .. }
                    | ir::RestrictedVectorSearchPlan::Edges { k, .. } => match k {
                        ir::SearchLimitPlan::Literal(k) => Some(k.get()),
                        ir::SearchLimitPlan::Expr(_) => None,
                    },
                };
                StepDraft {
                    dependencies: vec![input_id],
                    output,
                    condition,
                    op: ExecOp::VectorSearch { plan: plan.clone() },
                    schedule: ExecSchedule::Barrier,
                    delivered: materialized_delivered_properties(limit_delivered_properties(
                        delivered, k,
                    )),
                    cost: self.profile.explicit_sort(rows),
                }
            }
            logical::StreamPipelineOp::TextSearch { plan } => {
                let k = match plan.as_ref() {
                    ir::RestrictedTextSearchPlan::Nodes { k, .. }
                    | ir::RestrictedTextSearchPlan::Edges { k, .. } => match k {
                        ir::SearchLimitPlan::Literal(k) => Some(k.get()),
                        ir::SearchLimitPlan::Expr(_) => None,
                    },
                };
                StepDraft {
                    dependencies: vec![input_id],
                    output,
                    condition,
                    op: ExecOp::TextSearch { plan: plan.clone() },
                    schedule: ExecSchedule::Barrier,
                    delivered: materialized_delivered_properties(limit_delivered_properties(
                        delivered, k,
                    )),
                    cost: self.profile.explicit_sort(rows),
                }
            }
            logical::StreamPipelineOp::Variable { op } => StepDraft {
                dependencies: vec![input_id],
                output,
                condition,
                op: ExecOp::Variable {
                    op: ExecVariableOp::Stream(op.to_stream_op()),
                },
                schedule: ExecSchedule::Pipeline,
                delivered: selected_stream_variable_delivered_properties(delivered, op),
                cost: self.profile.stream_operator(rows),
            },
            logical::StreamPipelineOp::VariableWrite { op } => StepDraft {
                dependencies: vec![input_id],
                output,
                condition,
                op: ExecOp::Variable {
                    op: ExecVariableOp::Stream(op.to_stream_op()),
                },
                schedule: ExecSchedule::Barrier,
                delivered: selected_stream_variable_write_delivered_properties(delivered, op),
                cost: self.profile.stream_operator(rows),
            },
            logical::StreamPipelineOp::Distinct => StepDraft {
                dependencies: vec![input_id],
                output,
                condition,
                op: ExecOp::Distinct,
                schedule: ExecSchedule::Barrier,
                delivered: materialized_delivered_properties(filtered_delivered_properties(
                    delivered,
                )),
                cost: self.profile.explicit_sort(rows),
            },
        };
        self.push_step(draft)
    }
}
