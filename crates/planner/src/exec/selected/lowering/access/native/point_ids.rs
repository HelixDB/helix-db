//! Selected point-ID access allocation.

use std::borrow::Cow;

use super::super::*;
use crate::exec;

impl ExecutableDagBuilder<'_> {
    pub(super) fn push_selected_point_ids(
        &mut self,
        keyspace: exec::ElementKeyspace,
        ids: &ir::ElementIds,
        read_limit: exec::ExecAccessReadLimit,
        dependencies: Vec<ExecStepId>,
        output: ir::BatchOutputPlan,
        condition: ExecCondition,
    ) -> Result<ExecStepId, ExecPlanError> {
        let ids = point_ids_with_read_limit(ids, read_limit);
        let keys = ids.as_at_least().map_ref(|id| keyspace.point_key(*id));
        let count =
            properties::PositiveUsize::new(keys.len()).ok_or(ExecPlanError::EmptyMultiGet)?;
        let delivered = exec::element_point_delivered_properties(keyspace.element(), keys.len());

        if let [key] = keys.as_ref() {
            return self.push_step(StepDraft {
                dependencies,
                output,
                condition,
                op: ExecOp::KvRead(exec::KvReadPlan::Get { key: key.clone() }),
                schedule: ExecSchedule::Pipeline,
                delivered,
                cost: self.profile.point_gets(count),
            });
        }

        let batches = exec::coalesce_non_empty_multi_get_batches(
            keys,
            properties::KeyLocality::Close,
            self.profile,
        )?;
        if let [batch] = batches.as_ref() {
            let batch_count =
                properties::PositiveUsize::new(batch.len()).ok_or(ExecPlanError::EmptyMultiGet)?;
            return self.push_step(StepDraft {
                dependencies,
                output,
                condition,
                op: ExecOp::KvRead(exec::KvReadPlan::MultiGet(batch.clone())),
                schedule: ExecSchedule::Pipeline,
                delivered,
                cost: self
                    .profile
                    .multi_get(batch_count, properties::KeyLocality::Close),
            });
        }

        let mut read_ids = Vec::with_capacity(batches.len());
        for batch in batches {
            let batch_count =
                properties::PositiveUsize::new(batch.len()).ok_or(ExecPlanError::EmptyMultiGet)?;
            let batch_delivered =
                exec::element_point_delivered_properties(keyspace.element(), batch.len());
            let read_id = self.push_step(StepDraft {
                dependencies: dependencies.clone(),
                output: ir::BatchOutputPlan::Discard,
                condition: condition.clone(),
                op: ExecOp::KvRead(exec::KvReadPlan::MultiGet(batch)),
                schedule: ExecSchedule::Pipeline,
                delivered: batch_delivered,
                cost: self
                    .profile
                    .multi_get(batch_count, properties::KeyLocality::Close),
            })?;
            read_ids.push(read_id);
        }

        self.push_native_merge(
            read_ids,
            exec::ExecMergeMode::Concat,
            output,
            condition,
            delivered,
            true,
        )
    }
}

fn point_ids_with_read_limit(
    ids: &ir::ElementIds,
    read_limit: exec::ExecAccessReadLimit,
) -> Cow<'_, ir::ElementIds> {
    let exec::ExecAccessReadLimit::Bounded(limit) = read_limit else {
        return Cow::Borrowed(ids);
    };
    if limit.get() >= ids.as_ref().len() {
        return Cow::Borrowed(ids);
    }
    Cow::Owned(
        ids.slice(0..limit.get())
            .expect("positive read limit preserves a non-empty point-id prefix"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn element_ids(values: Vec<u64>) -> ir::ElementIds {
        ir::ElementIds::new(ir::AtLeast::<_, 1>::try_from_vec(values).unwrap()).unwrap()
    }

    #[test]
    fn single_point_id_allocates_typed_get() {
        let profile = cost::StorageCostProfile::default();
        let mut lowering = ExecutableDagBuilder::new(&profile);
        let ids = element_ids(vec![42]);

        let root = lowering
            .push_selected_point_ids(
                exec::ElementKeyspace::NodeProperty,
                &ids,
                exec::ExecAccessReadLimit::Unbounded,
                Vec::new(),
                ir::BatchOutputPlan::Discard,
                ExecCondition::Always,
            )
            .unwrap();

        assert_eq!(root.get(), 1);
        assert_eq!(lowering.steps.len(), 1);
        assert!(matches!(
            &lowering.steps[0].op,
            ExecOp::KvRead(exec::KvReadPlan::Get { key })
                if key.keyspace() == exec::ElementKeyspace::NodeProperty && key.id() == 42
        ));
        assert_eq!(
            lowering.steps[0].delivered.cardinality,
            properties::CardinalityBounds::exact(1)
        );
    }

    #[test]
    fn split_point_id_batches_allocate_order_preserving_concat_merge() {
        let profile = cost::StorageCostProfile {
            close_key_multi_get_batch: properties::PositiveUsize::at_least_one(2),
            ..cost::StorageCostProfile::default()
        };
        let mut lowering = ExecutableDagBuilder::new(&profile);
        let ids = element_ids(vec![3, 1, 2]);

        let root = lowering
            .push_selected_point_ids(
                exec::ElementKeyspace::EdgeEndpoints,
                &ids,
                exec::ExecAccessReadLimit::Unbounded,
                Vec::new(),
                ir::BatchOutputPlan::Discard,
                ExecCondition::Always,
            )
            .unwrap();

        assert_eq!(root.get(), 3);
        assert_eq!(lowering.steps.len(), 3);
        assert!(matches!(
            lowering.steps[0].op,
            ExecOp::KvRead(exec::KvReadPlan::MultiGet(_))
        ));
        assert!(matches!(
            lowering.steps[1].op,
            ExecOp::KvRead(exec::KvReadPlan::MultiGet(_))
        ));
        assert!(matches!(
            lowering.steps[2].op,
            ExecOp::Merge {
                mode: exec::ExecMergeMode::Concat
            }
        ));
        assert!(matches!(
            lowering.steps[2].schedule,
            ExecSchedule::Parallel {
                preserve_order: true,
                ..
            }
        ));
        assert_eq!(
            lowering.steps[2].dependencies,
            vec![lowering.steps[0].id, lowering.steps[1].id]
        );
    }

    #[test]
    fn point_id_read_limit_slices_ids_before_batching() {
        let profile = cost::StorageCostProfile::default();
        let mut lowering = ExecutableDagBuilder::new(&profile);
        let ids = element_ids(vec![7, 9, 11]);

        let root = lowering
            .push_selected_point_ids(
                exec::ElementKeyspace::NodeProperty,
                &ids,
                exec::ExecAccessReadLimit::bounded(properties::PositiveUsize::new(2).unwrap()),
                Vec::new(),
                ir::BatchOutputPlan::Discard,
                ExecCondition::Always,
            )
            .unwrap();

        assert_eq!(root.get(), 1);
        assert_eq!(lowering.steps.len(), 1);
        assert!(matches!(
            &lowering.steps[0].op,
            ExecOp::KvRead(exec::KvReadPlan::MultiGet(batch))
                if batch.len() == 2
        ));
        assert_eq!(
            lowering.steps[0].delivered.cardinality,
            properties::CardinalityBounds::exact(2)
        );
    }
}
