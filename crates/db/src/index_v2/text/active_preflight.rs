//! Exact serialized resource admission for Active text mutations.
//!
//! Request-owned text publication is allowed to reserve an intent only after
//! every graph/attachment component and independent upload outbox has been
//! measured and their request aggregate has been admitted.
//! [`ActiveTextMutationMeasurements`] records those exact values and validates
//! all five independent ceilings in a stable order. The admitted capability is
//! runtime-only and never changes a database key, value, or text split format.

use crate::config::ActiveTextMutationLimits;
use crate::error::{ActiveTextMutationResource, HelixDbError, Result};

/// Admitted exact sizes for one Active component or complete request aggregate.
///
/// Private fields prevent downstream publication code from replacing a checked
/// measurement with an unvalidated count.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ActiveTextMutationMeasurements {
    input_bytes: u64,
    output_operations: u64,
    output_bytes: u64,
    split_bytes: u64,
    manifest_page_bytes: u64,
}

impl ActiveTextMutationMeasurements {
    /// Admits all exact counts or returns the first stable exceeded resource.
    pub(super) fn try_admit(
        limits: ActiveTextMutationLimits,
        input_bytes: u64,
        output_operations: u64,
        output_bytes: u64,
        split_bytes: u64,
        manifest_page_bytes: u64,
    ) -> Result<Self> {
        let measurements = Self {
            input_bytes,
            output_operations,
            output_bytes,
            split_bytes,
            manifest_page_bytes,
        };
        let exceeded = [
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

    /// Returns the encoded V2 manifest-page value size.
    pub(super) const fn manifest_page_bytes(self) -> u64 {
        self.manifest_page_bytes
    }
}

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU64, NonZeroUsize};

    use super::*;
    use crate::config::{
        SearchIndexBackfillLimits, SearchIndexBatchLimits, TextBackfillCompactionLimits,
        TextBuildArtifactLimits,
    };

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
