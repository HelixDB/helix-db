//! Exact serialized resource admission for Active text mutations.
//!
//! Request-owned text publication is allowed to reserve an intent only after
//! every graph, BUILD/statistics, destination, and upload component has been
//! measured and their epoch aggregate has been admitted.
//! [`ActiveTextMutationMeasurements`] records those exact values and validates
//! all independent ceilings in a stable order. The admitted capability is
//! runtime-only and never changes a database key, value, or text split format.

use crate::config::ActiveTextMutationLimits;
use crate::error::{ActiveTextMutationResource, HelixDbError, Result};

/// Admitted exact sizes for one Active component or complete request aggregate.
///
/// Private fields prevent downstream publication code from replacing a checked
/// measurement with an unvalidated count.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ActiveTextMutationMeasurements {
    entities: u64,
    input_bytes: u64,
    output_operations: u64,
    output_bytes: u64,
    split_bytes: u64,
    retained_split_bytes: u64,
    manifest_page_bytes: u64,
}

/// Exact resource usage presented to the epoch admission boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ActiveTextMutationUsage {
    pub(super) entities: u64,
    pub(super) input_bytes: u64,
    pub(super) output_operations: u64,
    pub(super) output_bytes: u64,
    pub(super) split_bytes: u64,
    pub(super) retained_split_bytes: u64,
    pub(super) manifest_page_bytes: u64,
}

impl ActiveTextMutationMeasurements {
    /// Admits all exact counts or returns the first stable exceeded resource.
    #[cfg(any(test, feature = "production-coverage"))]
    pub(super) fn try_admit(
        limits: ActiveTextMutationLimits,
        input_bytes: u64,
        output_operations: u64,
        output_bytes: u64,
        split_bytes: u64,
        manifest_page_bytes: u64,
    ) -> Result<Self> {
        Self::try_admit_epoch(
            limits,
            ActiveTextMutationUsage {
                entities: 0,
                input_bytes,
                output_operations,
                output_bytes,
                split_bytes,
                retained_split_bytes: 0,
                manifest_page_bytes,
            },
        )
    }

    /// Returns the number of distinct graph entities in the admitted epoch.
    #[cfg(any(test, feature = "production-coverage"))]
    pub(super) const fn entities(self) -> u64 {
        self.entities
    }

    /// Admits the complete epoch, including retained entity and payload totals.
    pub(super) fn try_admit_epoch(
        limits: ActiveTextMutationLimits,
        usage: ActiveTextMutationUsage,
    ) -> Result<Self> {
        let ActiveTextMutationUsage {
            entities,
            input_bytes,
            output_operations,
            output_bytes,
            split_bytes,
            retained_split_bytes,
            manifest_page_bytes,
        } = usage;
        let measurements = Self {
            entities,
            input_bytes,
            output_operations,
            output_bytes,
            split_bytes,
            retained_split_bytes,
            manifest_page_bytes,
        };
        let exceeded = [
            (
                ActiveTextMutationResource::Entities,
                entities,
                u64::try_from(limits.max_entities().get()).unwrap_or(u64::MAX),
            ),
            (
                ActiveTextMutationResource::InputBytes,
                input_bytes,
                limits.max_input_bytes().get(),
            ),
            (
                ActiveTextMutationResource::OutputOperations,
                output_operations,
                limits.max_output_operations().get(),
            ),
            (
                ActiveTextMutationResource::OutputBytes,
                output_bytes,
                limits.max_output_bytes().get(),
            ),
            (
                ActiveTextMutationResource::SplitBytes,
                split_bytes,
                limits.max_split_bytes().get(),
            ),
            (
                ActiveTextMutationResource::RetainedSplitBytes,
                retained_split_bytes,
                limits.max_input_bytes().get(),
            ),
            (
                ActiveTextMutationResource::ManifestPageBytes,
                manifest_page_bytes,
                limits.max_manifest_page_bytes().get(),
            ),
        ]
        .into_iter()
        .find(|(_, observed, limit)| observed > limit);
        let Some((resource, observed, limit)) = exceeded else {
            return Ok(measurements);
        };
        Err(HelixDbError::ActiveTextMutationLimitExceeded {
            resource,
            observed,
            limit,
        })
    }

    /// Returns exact serialized database input bytes.
    pub(super) const fn input_bytes(self) -> u64 {
        self.input_bytes
    }

    /// Returns exact request-owned database write count.
    pub(super) const fn output_operations(self) -> u64 {
        self.output_operations
    }

    /// Returns exact serialized request-owned database output bytes.
    pub(super) const fn output_bytes(self) -> u64 {
        self.output_bytes
    }

    /// Returns the immutable split payload size.
    pub(super) const fn split_bytes(self) -> u64 {
        self.split_bytes
    }

    /// Returns aggregate immutable payload bytes retained for publication.
    pub(super) const fn retained_split_bytes(self) -> u64 {
        self.retained_split_bytes
    }

    /// Returns the encoded V2 manifest-page value size.
    pub(super) const fn manifest_page_bytes(self) -> u64 {
        self.manifest_page_bytes
    }
}

#[cfg(any(test, feature = "production-coverage"))]
#[path = "../../../tests/production_support/index_lifecycle_active_text_preflight.rs"]
pub(crate) mod production_contracts;

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU64, NonZeroUsize};

    use super::*;
    use crate::config::{
        SearchIndexBackfillLimits, SearchIndexBatchLimits, TextBackfillCompactionLimits,
        TextBuildArtifactLimits,
    };

    #[test]
    fn production_active_text_preflight_matrix_runs_in_workspace_tests() {
        production_contracts::run();
    }

    /// Constructs distinct ceilings so every rejection identifies one resource.
    fn limits() -> ActiveTextMutationLimits {
        SearchIndexBackfillLimits::try_new(
            SearchIndexBatchLimits::try_new(
                NonZeroUsize::MIN,
                NonZeroU64::new(10).unwrap(),
                NonZeroU64::new(20).unwrap(),
                NonZeroU64::new(60).unwrap(),
                NonZeroU64::MIN,
            )
            .unwrap(),
            NonZeroUsize::MIN,
            TextBuildArtifactLimits::new(NonZeroUsize::MIN, NonZeroU64::MIN),
            TextBackfillCompactionLimits::new(
                NonZeroUsize::MIN,
                NonZeroU64::new(10).unwrap(),
                NonZeroU64::new(40).unwrap(),
                NonZeroU64::new(40).unwrap(),
                NonZeroU64::new(50).unwrap(),
            ),
        )
        .unwrap()
        .active_text_mutation()
    }

    #[test]
    fn exact_limits_are_admitted_and_retained() {
        let admitted =
            ActiveTextMutationMeasurements::try_admit(limits(), 10, 20, 60, 40, 50).unwrap();
        assert_eq!(admitted.input_bytes(), 10);
        assert_eq!(admitted.output_operations(), 20);
        assert_eq!(admitted.output_bytes(), 60);
        assert_eq!(admitted.split_bytes(), 40);
        assert_eq!(admitted.manifest_page_bytes(), 50);

        let epoch = ActiveTextMutationMeasurements::try_admit_epoch(
            limits(),
            ActiveTextMutationUsage {
                entities: 1,
                input_bytes: 10,
                output_operations: 20,
                output_bytes: 60,
                split_bytes: 40,
                retained_split_bytes: 10,
                manifest_page_bytes: 50,
            },
        )
        .unwrap();
        assert_eq!(epoch.entities(), 1);
        assert_eq!(epoch.retained_split_bytes(), 10);
    }

    #[test]
    fn epoch_entity_and_retained_payload_limits_are_independent() {
        for (entities, retained, expected_resource, expected_limit) in [
            (2, 10, ActiveTextMutationResource::Entities, 1),
            (1, 11, ActiveTextMutationResource::RetainedSplitBytes, 10),
        ] {
            assert!(matches!(
                ActiveTextMutationMeasurements::try_admit_epoch(
                    limits(),
                    ActiveTextMutationUsage {
                        entities,
                        input_bytes: 10,
                        output_operations: 20,
                        output_bytes: 60,
                        split_bytes: 40,
                        retained_split_bytes: retained,
                        manifest_page_bytes: 50,
                    },
                ),
                Err(HelixDbError::ActiveTextMutationLimitExceeded {
                    resource,
                    observed,
                    limit,
                }) if resource == expected_resource
                    && observed == expected_limit + 1
                    && limit == expected_limit
            ));
        }
    }

    #[test]
    fn every_resource_rejects_before_a_capability_exists() {
        let cases = [
            (
                [11, 20, 60, 40, 50],
                ActiveTextMutationResource::InputBytes,
                10,
            ),
            (
                [10, 21, 60, 40, 50],
                ActiveTextMutationResource::OutputOperations,
                20,
            ),
            (
                [10, 20, 61, 40, 50],
                ActiveTextMutationResource::OutputBytes,
                60,
            ),
            (
                [10, 20, 60, 41, 50],
                ActiveTextMutationResource::SplitBytes,
                40,
            ),
            (
                [10, 20, 60, 40, 51],
                ActiveTextMutationResource::ManifestPageBytes,
                50,
            ),
        ];
        for (values, expected_resource, expected_limit) in cases {
            let [input, operations, output, split, manifest] = values;
            let error = ActiveTextMutationMeasurements::try_admit(
                limits(),
                input,
                operations,
                output,
                split,
                manifest,
            )
            .unwrap_err();
            assert!(matches!(
                error,
                HelixDbError::ActiveTextMutationLimitExceeded {
                    resource,
                    observed,
                    limit,
                } if resource == expected_resource
                    && observed == expected_limit + 1
                    && limit == expected_limit
            ));
        }
    }

    #[test]
    fn rejection_order_is_stable_when_every_resource_is_oversized() {
        assert!(matches!(
            ActiveTextMutationMeasurements::try_admit(
                limits(),
                u64::MAX,
                u64::MAX,
                u64::MAX,
                u64::MAX,
                u64::MAX,
            ),
            Err(HelixDbError::ActiveTextMutationLimitExceeded {
                resource: ActiveTextMutationResource::InputBytes,
                observed: u64::MAX,
                limit: 10,
            })
        ));
    }
}
