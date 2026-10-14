//! Production contracts for Active text mutation resource admission.
//!
//! The harness is a feature-gated child of the owning module, preserving the
//! private capability boundary while exercising the exact production limits.
//! It proves equality-at-limit admission, stable first-failure ordering, and
//! every typed resource rejection without constructing any persisted row.

use std::num::{NonZeroU64, NonZeroUsize};

use super::*;
use crate::config::{
    SearchIndexBackfillLimits, SearchIndexBatchLimits, TextBackfillCompactionLimits,
    TextBuildArtifactLimits,
};

/// Distinct ceilings, each above the smallest document's per-document share.
const INPUT: u64 = 40_000;
const OPERATIONS: u64 = 20;
const OUTPUT: u64 = 3_000;
const SPLIT: u64 = 20_000;
const PAGE: u64 = 500;

/// Constructs distinct ceilings so every rejection identifies one resource.
fn limits() -> ActiveTextMutationLimits {
    SearchIndexBackfillLimits::try_new(
        SearchIndexBatchLimits::try_new(
            NonZeroUsize::MIN,
            NonZeroU64::new(INPUT).expect("input limit is non-zero"),
            NonZeroU64::new(OPERATIONS).expect("operation limit is non-zero"),
            NonZeroU64::new(OUTPUT).expect("output limit is non-zero"),
            NonZeroU64::MIN,
        )
        .expect("batch limits validate"),
        NonZeroUsize::MIN,
        TextBuildArtifactLimits::new(NonZeroUsize::MIN, NonZeroU64::MIN),
        TextBackfillCompactionLimits::new(
            NonZeroUsize::MIN,
            NonZeroU64::new(INPUT).expect("compaction input limit is non-zero"),
            NonZeroU64::new(SPLIT).expect("temporary limit is non-zero"),
            NonZeroU64::new(SPLIT).expect("split limit is non-zero"),
            NonZeroU64::new(PAGE).expect("manifest limit is non-zero"),
        ),
    )
    .expect("backfill limits validate")
    .active_text_mutation()
}

/// Runs exact admission and every ordered resource rejection.
pub(crate) fn run() {
    let admitted =
        ActiveTextMutationMeasurements::try_admit(limits(), INPUT, OPERATIONS, OUTPUT, SPLIT, PAGE)
            .expect("values equal to every ceiling are admitted");
    assert_eq!(admitted.input_bytes(), INPUT);
    assert_eq!(admitted.output_operations(), OPERATIONS);
    assert_eq!(admitted.output_bytes(), OUTPUT);
    assert_eq!(admitted.split_bytes(), SPLIT);
    assert_eq!(admitted.manifest_page_bytes(), PAGE);

    let epoch = ActiveTextMutationMeasurements::try_admit_epoch(
        limits(),
        ActiveTextMutationUsage {
            entities: 1,
            input_bytes: INPUT,
            output_operations: OPERATIONS,
            output_bytes: OUTPUT,
            split_bytes: SPLIT,
            retained_split_bytes: INPUT,
            manifest_page_bytes: PAGE,
        },
    )
    .expect("epoch values equal to every ceiling are admitted");
    assert_eq!(epoch.entities(), 1);
    assert_eq!(epoch.retained_split_bytes(), INPUT);
    for (entities, retained, expected_resource, expected_limit) in [
        (2, INPUT, ActiveTextMutationResource::Entities, 1),
        (
            1,
            INPUT + 1,
            ActiveTextMutationResource::RetainedSplitBytes,
            INPUT,
        ),
    ] {
        assert!(matches!(
            ActiveTextMutationMeasurements::try_admit_epoch(
                limits(),
                ActiveTextMutationUsage {
                    entities,
                    input_bytes: INPUT,
                    output_operations: OPERATIONS,
                    output_bytes: OUTPUT,
                    split_bytes: SPLIT,
                    retained_split_bytes: retained,
                    manifest_page_bytes: PAGE,
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

    for (values, expected_resource, expected_limit) in [
        (
            [INPUT + 1, OPERATIONS, OUTPUT, SPLIT, PAGE],
            ActiveTextMutationResource::InputBytes,
            INPUT,
        ),
        (
            [INPUT, OPERATIONS + 1, OUTPUT, SPLIT, PAGE],
            ActiveTextMutationResource::OutputOperations,
            OPERATIONS,
        ),
        (
            [INPUT, OPERATIONS, OUTPUT + 1, SPLIT, PAGE],
            ActiveTextMutationResource::OutputBytes,
            OUTPUT,
        ),
        (
            [INPUT, OPERATIONS, OUTPUT, SPLIT + 1, PAGE],
            ActiveTextMutationResource::SplitBytes,
            SPLIT,
        ),
        (
            [INPUT, OPERATIONS, OUTPUT, SPLIT, PAGE + 1],
            ActiveTextMutationResource::ManifestPageBytes,
            PAGE,
        ),
    ] {
        let [input, operations, output, split, manifest] = values;
        assert!(matches!(
            ActiveTextMutationMeasurements::try_admit(
                limits(),
                input,
                operations,
                output,
                split,
                manifest,
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
            limit: INPUT,
        })
    ));
}
