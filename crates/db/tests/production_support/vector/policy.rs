//! Production contracts for pure layer-zero vector policy decisions.
//!
//! This feature-gated child module exercises the production policy ADTs through
//! deployed configuration projections. It performs no I/O and consumes no
//! randomness, so each assertion isolates filtering, pre-sampling, sampling,
//! and adaptive-threshold behavior as deterministic contract boundaries.

use super::*;
use crate::search::vector::ResultCount;

/// Constructs a threshold validated against the active 64-bit SimHash width.
fn threshold(value: usize) -> CollisionThreshold {
    CollisionThreshold::try_new(value, NonZeroUsize::new(64).unwrap()).unwrap()
}

/// Constructs a validated closed-unit-interval sampling ratio.
fn ratio(value: f32) -> UnitInterval {
    UnitInterval::try_new(value).unwrap()
}

/// Constructs a validated non-zero adaptive failure probability.
fn failure(value: f32) -> FailureProbability {
    FailureProbability::try_new(value).unwrap()
}

/// Builds a representative active frontier decision epoch.
fn context() -> SimHashContext {
    SimHashContext {
        topk_ready: true,
        ef: 64,
        search_frontier_len: 64,
        candidate_frontier_len: 64,
        current: DistanceScore::try_new(0.2).unwrap(),
        delta: DistanceScore::try_new(0.4).unwrap(),
        adaptive_bypass: AdaptiveBypassObservation::idle(),
    }
}

/// Constructs the deployed adaptive-bypass policy for the representative beam.
fn adaptive_bypass_policy(mode: SimHashMode) -> AdaptiveBypassPolicy {
    AdaptiveBypassPolicy::from_deployed(
        mode,
        SearchBeamWidth::try_new(64, ResultCount::try_new(10).unwrap()).unwrap(),
        NonZeroUsize::new(24).unwrap(),
        NonZeroUsize::new(4).unwrap(),
        ratio(0.12),
        NonZeroUsize::new(3).unwrap(),
    )
}

/// Verifies the complete metric, mode, and adaptive-flag compatibility table.
fn run_compatibility_contracts() {
    for metric in [
        ActiveMetricKind::Cosine,
        ActiveMetricKind::Euclidean,
        ActiveMetricKind::Manhattan,
    ] {
        for mode in [SimHashMode::Off, SimHashMode::Always, SimHashMode::Adaptive] {
            for adaptive_enabled in [false, true] {
                let decision = Layer0Policy::from_deployed(
                    metric,
                    mode,
                    threshold(43),
                    ratio(0.4),
                    None,
                    adaptive_enabled,
                    failure(0.1),
                )
                .decide(context());
                let filtering_expected =
                    metric == ActiveMetricKind::Cosine && !matches!(mode, SimHashMode::Off);
                assert_eq!(decision.fetch_missing, filtering_expected);
                assert_eq!(decision.filter_cached, filtering_expected);
                assert_eq!(decision.threshold.is_some(), filtering_expected);
                assert_eq!(
                    decision.sampling.probability() == 1.0,
                    matches!(mode, SimHashMode::Off)
                );
            }
        }
    }
}

/// Verifies bypass, fixed threshold, and pre/post sampling activation behavior.
fn run_fixed_and_bypass_contracts() {
    let fixed = Layer0Policy::from_deployed(
        ActiveMetricKind::Cosine,
        SimHashMode::Always,
        threshold(37),
        ratio(0.4),
        Some(ratio(0.2)),
        true,
        failure(0.1),
    );
    let active = fixed.decide(context());
    assert_eq!(active.threshold, Some(threshold(37)));
    assert_eq!(active.pre_sampling.probability(), 0.25);
    assert_eq!(active.sampling.probability(), 0.4);
    assert_eq!(active.base_sampling_probability, 0.4);

    let bypassed = Layer0Policy::from_deployed(
        ActiveMetricKind::Cosine,
        SimHashMode::Adaptive,
        threshold(37),
        ratio(0.4),
        Some(ratio(0.2)),
        true,
        failure(0.1),
    )
    .with_adaptive_bypass(adaptive_bypass_policy(SimHashMode::Adaptive))
    .decide(SimHashContext {
        adaptive_bypass: AdaptiveBypassObservation {
            simhash_filter_reads: 192,
            ..AdaptiveBypassObservation::idle()
        },
        ..context()
    });
    assert!(bypassed.bypassed);
    assert!(!bypassed.fetch_missing);
    assert!(!bypassed.filter_cached);
    assert_eq!(bypassed.threshold, None);

    let mut small = context();
    small.candidate_frontier_len = 4;
    let small = fixed.decide(small);
    assert_eq!(small.pre_sampling, SamplingDecision::Exhaustive);
    assert_eq!(small.sampling, SamplingDecision::Exhaustive);

    let defer_all = Layer0Policy::from_deployed(
        ActiveMetricKind::Cosine,
        SimHashMode::Always,
        threshold(43),
        ratio(0.0),
        Some(ratio(0.0)),
        true,
        failure(0.1),
    )
    .decide(context());
    assert_eq!(defer_all.pre_sampling.probability(), 0.0);
    assert_eq!(defer_all.sampling.probability(), 0.0);
}

/// Verifies adaptive cold-start, quality, threshold, and candidate weighting bounds.
fn run_adaptive_contracts() {
    let policy = Layer0Policy::from_deployed(
        ActiveMetricKind::Cosine,
        SimHashMode::Adaptive,
        threshold(43),
        ratio(0.3),
        None,
        true,
        failure(0.1),
    );
    let mut cold = context();
    cold.topk_ready = false;
    cold.search_frontier_len = 4;
    cold.candidate_frontier_len = 4;
    let cold = policy.decide(cold);
    assert_eq!(cold.threshold, Some(threshold(1)));
    assert_eq!(cold.sampling.probability(), 1.0);

    let mut near = context();
    near.current = DistanceScore::try_new(0.1).unwrap();
    near.delta = DistanceScore::try_new(0.2).unwrap();
    let mut far = context();
    far.current = DistanceScore::try_new(0.8).unwrap();
    far.delta = DistanceScore::try_new(0.9).unwrap();
    let near = policy.decide(near);
    let far = policy.decide(far);
    assert!(near.threshold.unwrap().get() >= far.threshold.unwrap().get());
    assert!(near.sampling.probability() >= far.sampling.probability());
    assert!(
        near.sampling.candidate_probability(58, near.threshold)
            >= near.sampling.candidate_probability(32, near.threshold)
    );
    assert_eq!(
        SamplingDecision::Fixed(ratio(0.4)).candidate_probability(64, None),
        0.4
    );
    assert_eq!(
        SamplingDecision::Adaptive(ratio(0.0)).candidate_probability(64, None),
        0.0
    );
    assert_eq!(
        SamplingDecision::Adaptive(ratio(1.0)).candidate_probability(64, None),
        1.0
    );

    let zero_threshold = Layer0Policy::from_deployed(
        ActiveMetricKind::Cosine,
        SimHashMode::Adaptive,
        threshold(0),
        ratio(0.5),
        None,
        true,
        failure(0.1),
    )
    .decide(context());
    assert_eq!(zero_threshold.threshold, Some(threshold(0)));

    assert_eq!(adaptive_sampling_ratio(1.0, context()), 1.0);
    let mut equal_scores = context();
    equal_scores.current = DistanceScore::try_new(0.0).unwrap();
    equal_scores.delta = DistanceScore::try_new(0.0).unwrap();
    assert_eq!(adaptive_sampling_ratio(0.3, equal_scores), 0.3);
}

/// Exercises every active production filtering and sampling policy branch.
pub(crate) fn run() {
    run_compatibility_contracts();
    run_fixed_and_bypass_contracts();
    run_adaptive_contracts();
}
