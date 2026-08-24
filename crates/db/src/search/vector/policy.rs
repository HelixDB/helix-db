//! Pure layer-zero SimHash filtering and sampling policy.
//!
//! Persisted configuration remains byte-compatible. Callers project its raw
//! fields into [`Layer0Policy`] once, then provide storage-free frontier state
//! to [`Layer0Policy::decide`]. The returned decision is the sole authority for
//! hash fetching, cached-hash filtering, thresholding, and post-filter sampling.

use std::num::NonZeroUsize;

use super::{
    CollisionThreshold, DistanceScore, FailureProbability, SearchBeamWidth, SimHashMode,
    UnitInterval,
};
use crate::encoding::v2::values::indexes::vector::ActiveMetricKind;

/// Metric-qualified filtering behavior for one query.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum SimHashFilteringPolicy {
    /// No hash is fetched or filtered for policy purposes.
    Disabled,
    /// Use the configured threshold exactly on every decision epoch.
    Fixed { threshold: CollisionThreshold },
    /// Derive a threshold from the current cosine frontier quality.
    Adaptive {
        /// Maximum permitted strictness from the deployed configuration.
        configured: CollisionThreshold,
        /// Failure probability used by the LSM-Vec concentration bound.
        failure: FailureProbability,
    },
}

/// Post-filter frontier sampling behavior for one query.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum FrontierSamplingPolicy {
    /// Visit every candidate that passes filtering.
    Exhaustive,
    /// Apply one fixed Bernoulli probability.
    Fixed { ratio: UnitInterval },
    /// Increase the base probability for promising frontier positions.
    Adaptive { base: UnitInterval },
}

/// Complete pure layer-zero policy after compatibility projection.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct Layer0Policy {
    filtering: SimHashFilteringPolicy,
    sampling: FrontierSamplingPolicy,
    pre_sampling_override: Option<UnitInterval>,
    adaptive_bypass: AdaptiveBypassPolicy,
}

impl Layer0Policy {
    /// Maps deployed settings into closed filtering and sampling policies.
    ///
    /// Only cosine has an approved angular SimHash contract. Fixed mode uses
    /// the configured threshold without an adaptive formula; adaptive behavior
    /// also requires the persisted adaptive flag.
    pub(crate) fn from_deployed(
        metric: ActiveMetricKind,
        mode: SimHashMode,
        configured_threshold: CollisionThreshold,
        sampling_ratio: UnitInterval,
        pre_sampling_override: Option<UnitInterval>,
        adaptive_enabled: bool,
        failure: FailureProbability,
    ) -> Self {
        let filtering = match (metric, mode, adaptive_enabled) {
            (_, SimHashMode::Off, _) => SimHashFilteringPolicy::Disabled,
            (ActiveMetricKind::Cosine, SimHashMode::Always, _) => SimHashFilteringPolicy::Fixed {
                threshold: configured_threshold,
            },
            (ActiveMetricKind::Cosine, SimHashMode::Adaptive, true) => {
                SimHashFilteringPolicy::Adaptive {
                    configured: configured_threshold,
                    failure,
                }
            }
            (ActiveMetricKind::Cosine, SimHashMode::Adaptive, false) => {
                SimHashFilteringPolicy::Fixed {
                    threshold: configured_threshold,
                }
            }
            (
                ActiveMetricKind::Euclidean | ActiveMetricKind::Manhattan,
                SimHashMode::Always | SimHashMode::Adaptive,
                _,
            ) => SimHashFilteringPolicy::Disabled,
        };
        let sampling = match mode {
            SimHashMode::Off => FrontierSamplingPolicy::Exhaustive,
            SimHashMode::Always => FrontierSamplingPolicy::Fixed {
                ratio: sampling_ratio,
            },
            SimHashMode::Adaptive if adaptive_enabled => FrontierSamplingPolicy::Adaptive {
                base: sampling_ratio,
            },
            SimHashMode::Adaptive => FrontierSamplingPolicy::Fixed {
                ratio: sampling_ratio,
            },
        };
        Self {
            filtering,
            sampling,
            pre_sampling_override,
            adaptive_bypass: AdaptiveBypassPolicy::Disabled,
        }
    }

    /// Installs the query's validated adaptive-bypass policy.
    pub(crate) const fn with_adaptive_bypass(
        mut self,
        adaptive_bypass: AdaptiveBypassPolicy,
    ) -> Self {
        self.adaptive_bypass = adaptive_bypass;
        self
    }

    /// Produces one total operational decision without storage or randomness.
    pub(crate) fn decide(self, context: SimHashContext) -> SimHashDecision {
        let adaptive_bypass = self
            .adaptive_bypass
            .decide(context.candidate_frontier_len, context.adaptive_bypass);
        let base_sampling = match self.sampling {
            FrontierSamplingPolicy::Exhaustive => SamplingDecision::Exhaustive,
            FrontierSamplingPolicy::Fixed { ratio } => SamplingDecision::Fixed(ratio),
            FrontierSamplingPolicy::Adaptive { base } => SamplingDecision::Adaptive(
                UnitInterval::try_new(adaptive_sampling_ratio(base.get(), context))
                    .expect("adaptive sampling remains in the closed unit interval"),
            ),
        };
        let sampling = activate_sampling(base_sampling, context.candidate_frontier_len, context.ef);
        let base_sampling_probability = base_sampling.probability();
        let pre_sampling = pre_sampling_decision(
            self.pre_sampling_override
                .map_or(base_sampling.probability(), UnitInterval::get),
            context.candidate_frontier_len,
            context.ef,
        );
        if adaptive_bypass.bypassed {
            return SimHashDecision::bypassed(
                pre_sampling,
                sampling,
                base_sampling_probability,
                adaptive_bypass,
            );
        }
        match self.filtering {
            SimHashFilteringPolicy::Disabled => SimHashDecision::disabled(
                pre_sampling,
                sampling,
                base_sampling_probability,
                adaptive_bypass,
            ),
            SimHashFilteringPolicy::Fixed { threshold } => SimHashDecision::filtering(
                threshold,
                pre_sampling,
                sampling,
                base_sampling_probability,
                adaptive_bypass,
            ),
            SimHashFilteringPolicy::Adaptive {
                configured,
                failure,
            } => {
                let threshold = adaptive_threshold(context, configured, failure);
                SimHashDecision::filtering(
                    threshold,
                    pre_sampling,
                    sampling,
                    base_sampling_probability,
                    adaptive_bypass,
                )
            }
        }
    }
}

/// Closed query policy for adaptive SimHash bypass windows.
///
/// Search supplies validated tuning and storage-free observations. This ADT
/// owns the budget/quality trigger calculation and every window transition, so
/// the traversal loop cannot manufacture a bypass boolean independently.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum AdaptiveBypassPolicy {
    /// Fixed/off query modes never bypass filtering adaptively.
    Disabled,
    /// Adaptive mode uses bounded bypass and cooldown windows.
    Windowed {
        min_frontier: NonZeroUsize,
        window_expansions: NonZeroUsize,
        min_filter_rate: UnitInterval,
        read_budget: NonZeroUsize,
    },
}

impl AdaptiveBypassPolicy {
    /// Projects validated query settings into the complete bypass policy.
    pub(crate) fn from_deployed(
        mode: SimHashMode,
        ef: SearchBeamWidth,
        min_frontier: NonZeroUsize,
        window_expansions: NonZeroUsize,
        min_filter_rate: UnitInterval,
        read_budget_multiplier: NonZeroUsize,
    ) -> Self {
        let SimHashMode::Adaptive = mode else {
            return Self::Disabled;
        };
        let read_budget = ef
            .get()
            .saturating_mul(read_budget_multiplier.get())
            .max(min_frontier.get());
        Self::Windowed {
            min_frontier,
            window_expansions,
            min_filter_rate,
            read_budget: NonZeroUsize::new(read_budget)
                .expect("validated search beam and multiplier produce a non-zero budget"),
        }
    }

    fn decide(
        self,
        candidate_frontier_len: usize,
        observation: AdaptiveBypassObservation,
    ) -> AdaptiveBypassDecision {
        let Self::Windowed {
            min_frontier,
            window_expansions,
            min_filter_rate,
            read_budget,
        } = self
        else {
            return AdaptiveBypassDecision::inactive();
        };

        match observation.state {
            AdaptiveBypassState::Bypassing { remaining } => {
                let next_state = match NonZeroUsize::new(remaining.get() - 1) {
                    Some(remaining) => AdaptiveBypassState::Bypassing { remaining },
                    None => AdaptiveBypassState::CoolingDown {
                        remaining: window_expansions,
                    },
                };
                return AdaptiveBypassDecision {
                    bypassed: true,
                    next_state,
                    trigger: AdaptiveBypassTrigger::None,
                };
            }
            AdaptiveBypassState::CoolingDown { remaining } if remaining.get() > 1 => {
                return AdaptiveBypassDecision {
                    bypassed: false,
                    next_state: AdaptiveBypassState::CoolingDown {
                        remaining: NonZeroUsize::new(remaining.get() - 1)
                            .expect("cooldown greater than one remains non-zero"),
                    },
                    trigger: AdaptiveBypassTrigger::None,
                };
            }
            AdaptiveBypassState::Ready | AdaptiveBypassState::CoolingDown { .. } => {}
        }

        let budget_exhausted = observation.simhash_filter_reads >= read_budget.get();
        let filter_rate = if observation.window_examined == 0 {
            1.0
        } else {
            observation.window_filtered as f32 / observation.window_examined as f32
        };
        let low_yield = observation.window_expansions >= window_expansions.get()
            && filter_rate < min_filter_rate.get();
        let trigger = AdaptiveBypassTrigger::from_causes(budget_exhausted, low_yield);
        if candidate_frontier_len < min_frontier.get()
            || matches!(trigger, AdaptiveBypassTrigger::None)
        {
            return AdaptiveBypassDecision::inactive();
        }

        let next_state = match NonZeroUsize::new(window_expansions.get() - 1) {
            Some(remaining) => AdaptiveBypassState::Bypassing { remaining },
            None => AdaptiveBypassState::CoolingDown {
                remaining: window_expansions,
            },
        };
        AdaptiveBypassDecision {
            bypassed: true,
            next_state,
            trigger,
        }
    }
}

/// Valid adaptive-bypass query state; zero-length windows are unrepresentable.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum AdaptiveBypassState {
    /// A new trigger may start a bypass window.
    #[default]
    Ready,
    /// The current query must bypass this many subsequent expansion epochs.
    Bypassing { remaining: NonZeroUsize },
    /// New triggers are suppressed until the non-zero cooldown expires.
    CoolingDown { remaining: NonZeroUsize },
}

/// Storage-free observations consumed by the pure adaptive-bypass policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AdaptiveBypassObservation {
    /// Query-local state returned by the preceding policy decision.
    pub(crate) state: AdaptiveBypassState,
    /// Cumulative SimHash filter reads performed by this query.
    pub(crate) simhash_filter_reads: usize,
    /// Candidates with hashes observed in the rolling yield window.
    pub(crate) window_examined: usize,
    /// Candidates rejected in the rolling yield window.
    pub(crate) window_filtered: usize,
    /// Expansion epochs represented by the rolling yield window.
    pub(crate) window_expansions: usize,
}

impl AdaptiveBypassObservation {
    /// Returns an idle observation for fixed policy tests and cold-start search.
    #[cfg(any(test, feature = "production-coverage"))]
    pub(crate) const fn idle() -> Self {
        Self {
            state: AdaptiveBypassState::Ready,
            simhash_filter_reads: 0,
            window_examined: 0,
            window_filtered: 0,
            window_expansions: 0,
        }
    }
}

/// Cause recorded only when a new adaptive bypass window starts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AdaptiveBypassTrigger {
    /// No new window started during this decision.
    None,
    /// The query exhausted its bounded SimHash read budget.
    ReadBudget,
    /// The rolling window's filter yield fell below the configured minimum.
    LowYield,
    /// Budget exhaustion and low yield were both observed.
    ReadBudgetAndLowYield,
}

impl AdaptiveBypassTrigger {
    const fn from_causes(read_budget: bool, low_yield: bool) -> Self {
        match (read_budget, low_yield) {
            (false, false) => Self::None,
            (true, false) => Self::ReadBudget,
            (false, true) => Self::LowYield,
            (true, true) => Self::ReadBudgetAndLowYield,
        }
    }

    /// Returns whether budget exhaustion caused this new window.
    pub(crate) const fn includes_read_budget(self) -> bool {
        matches!(self, Self::ReadBudget | Self::ReadBudgetAndLowYield)
    }

    /// Returns whether low filter yield caused this new window.
    pub(crate) const fn includes_low_yield(self) -> bool {
        matches!(self, Self::LowYield | Self::ReadBudgetAndLowYield)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct AdaptiveBypassDecision {
    bypassed: bool,
    next_state: AdaptiveBypassState,
    trigger: AdaptiveBypassTrigger,
}

impl AdaptiveBypassDecision {
    const fn inactive() -> Self {
        Self {
            bypassed: false,
            next_state: AdaptiveBypassState::Ready,
            trigger: AdaptiveBypassTrigger::None,
        }
    }
}

/// Validated, storage-free inputs for one decision epoch.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct SimHashContext {
    pub(crate) topk_ready: bool,
    pub(crate) ef: usize,
    pub(crate) search_frontier_len: usize,
    pub(crate) candidate_frontier_len: usize,
    pub(crate) current: DistanceScore,
    pub(crate) delta: DistanceScore,
    pub(crate) adaptive_bypass: AdaptiveBypassObservation,
}

/// Sampling action emitted by the pure policy.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum SamplingDecision {
    Exhaustive,
    Fixed(UnitInterval),
    Adaptive(UnitInterval),
}

impl SamplingDecision {
    /// Returns the exact probability consumed by the query-owned RNG.
    pub(crate) const fn probability(self) -> f32 {
        match self {
            Self::Exhaustive => 1.0,
            Self::Fixed(ratio) | Self::Adaptive(ratio) => ratio.get(),
        }
    }

    /// Returns the candidate probability without consuming query randomness.
    pub(crate) fn candidate_probability(
        self,
        similarity_bits: u32,
        threshold: Option<CollisionThreshold>,
    ) -> f32 {
        let base = self.probability();
        let Self::Adaptive(_) = self else {
            return base;
        };
        if base <= 0.0 || base >= 1.0 {
            return base;
        }
        let similarity_ratio = similarity_bits.min(64) as f32 / 64.0;
        let threshold_ratio = threshold.map_or(0.0, |value| value.get() as f32 / 64.0);
        (base + (1.0 - base) * (similarity_ratio - threshold_ratio).max(0.0)).clamp(base, 1.0)
    }
}

/// Atomic operational decision used for both cached and missing hashes.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct SimHashDecision {
    pub(crate) fetch_missing: bool,
    pub(crate) filter_cached: bool,
    pub(crate) threshold: Option<CollisionThreshold>,
    pub(crate) pre_sampling: SamplingDecision,
    pub(crate) sampling: SamplingDecision,
    /// Pre-activation probability retained for stable diagnostics.
    pub(crate) base_sampling_probability: f32,
    pub(crate) bypassed: bool,
    /// State the next decision epoch must feed back to this policy.
    pub(crate) next_bypass_state: AdaptiveBypassState,
    /// Cause of a newly started window, or `None` while continuing/idle.
    pub(crate) bypass_trigger: AdaptiveBypassTrigger,
}

impl SimHashDecision {
    /// Returns the closed no-filtering, no-sampling decision.
    ///
    /// Search uses this only for the separately represented strict exhaustive
    /// query state, where running the adaptive formulas cannot affect output.
    pub(crate) const fn exhaustive() -> Self {
        Self {
            fetch_missing: false,
            filter_cached: false,
            threshold: None,
            pre_sampling: SamplingDecision::Exhaustive,
            sampling: SamplingDecision::Exhaustive,
            base_sampling_probability: 1.0,
            bypassed: false,
            next_bypass_state: AdaptiveBypassState::Ready,
            bypass_trigger: AdaptiveBypassTrigger::None,
        }
    }

    fn disabled(
        pre_sampling: SamplingDecision,
        sampling: SamplingDecision,
        base_sampling_probability: f32,
        adaptive_bypass: AdaptiveBypassDecision,
    ) -> Self {
        Self {
            fetch_missing: false,
            filter_cached: false,
            threshold: None,
            pre_sampling,
            sampling,
            base_sampling_probability,
            bypassed: false,
            next_bypass_state: adaptive_bypass.next_state,
            bypass_trigger: adaptive_bypass.trigger,
        }
    }

    fn bypassed(
        pre_sampling: SamplingDecision,
        sampling: SamplingDecision,
        base_sampling_probability: f32,
        adaptive_bypass: AdaptiveBypassDecision,
    ) -> Self {
        Self {
            bypassed: true,
            ..Self::disabled(
                pre_sampling,
                sampling,
                base_sampling_probability,
                adaptive_bypass,
            )
        }
    }

    fn filtering(
        threshold: CollisionThreshold,
        pre_sampling: SamplingDecision,
        sampling: SamplingDecision,
        base_sampling_probability: f32,
        adaptive_bypass: AdaptiveBypassDecision,
    ) -> Self {
        Self {
            fetch_missing: true,
            filter_cached: true,
            threshold: Some(threshold),
            pre_sampling,
            sampling,
            base_sampling_probability,
            bypassed: false,
            next_bypass_state: adaptive_bypass.next_state,
            bypass_trigger: adaptive_bypass.trigger,
        }
    }
}

fn activate_sampling(
    sampling: SamplingDecision,
    candidate_frontier_len: usize,
    ef: usize,
) -> SamplingDecision {
    let ratio = sampling.probability();
    if ratio <= 0.0 || ratio >= 1.0 || candidate_frontier_len > (ef / 4).max(8) {
        sampling
    } else {
        SamplingDecision::Exhaustive
    }
}

fn pre_sampling_decision(
    base_ratio: f32,
    candidate_frontier_len: usize,
    ef: usize,
) -> SamplingDecision {
    if base_ratio >= 1.0 || candidate_frontier_len <= (ef / 4).max(8) {
        return SamplingDecision::Exhaustive;
    }
    let mut ratio = (base_ratio * 0.65).clamp(0.25, 0.9);
    if base_ratio <= 0.0 {
        ratio = 0.0;
    } else if candidate_frontier_len > ef.saturating_mul(2).max(32) {
        ratio = (ratio * 0.8).max(0.20);
    }
    SamplingDecision::Fixed(
        UnitInterval::try_new(ratio).expect("pre-sampling remains in the closed unit interval"),
    )
}

fn adaptive_sampling_ratio(base: f32, context: SimHashContext) -> f32 {
    if base >= 1.0 {
        return base;
    }
    if context.search_frontier_len < (context.ef / 3).max(8) {
        return 1.0;
    }
    let current = context.current.get();
    let delta = context.delta.get();
    if delta <= 1e-6 {
        return base;
    }
    let relative_quality = (1.0 - (current / delta).clamp(0.0, 1.0)).clamp(0.0, 1.0);
    (base + (1.0 - base) * relative_quality)
        .clamp(base, 1.0)
        .min(0.90f32.max(base))
}

fn adaptive_threshold(
    context: SimHashContext,
    configured: CollisionThreshold,
    failure: FailureProbability,
) -> CollisionThreshold {
    let value = if configured.get() == 0 {
        0
    } else if !context.topk_ready {
        1
    } else {
        // The only active adaptive policy is cosine, whose current score is
        // already the normalized half-distance in the closed unit interval.
        let normalized = context.delta.get().clamp(0.0, 1.0);
        let cosine_similarity = (1.0 - 2.0 * normalized).clamp(-1.0, 1.0);
        let collision = 1.0 - cosine_similarity.acos() / std::f32::consts::PI;
        let bits = 64.0;
        let margin = ((bits * (1.0 / failure.get()).ln()) / 2.0).sqrt();
        ((bits * collision - margin).floor().clamp(1.0, bits) as usize).min(configured.get())
    };
    CollisionThreshold::try_new(value, NonZeroUsize::new(64).unwrap())
        .expect("adaptive threshold is clamped to the SimHash bit width")
}

#[cfg(feature = "production-coverage")]
#[path = "../../../tests/production_support/vector/policy.rs"]
pub(crate) mod production_contracts;

#[cfg(test)]
mod tests {
    use super::*;

    fn threshold(value: usize) -> CollisionThreshold {
        CollisionThreshold::try_new(value, NonZeroUsize::new(64).unwrap()).unwrap()
    }

    fn ratio(value: f32) -> UnitInterval {
        UnitInterval::try_new(value).unwrap()
    }

    fn failure(value: f32) -> FailureProbability {
        FailureProbability::try_new(value).unwrap()
    }

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

    fn adaptive_bypass_policy(mode: SimHashMode) -> AdaptiveBypassPolicy {
        AdaptiveBypassPolicy::from_deployed(
            mode,
            SearchBeamWidth::try_new(64, super::super::ResultCount::try_new(10).unwrap()).unwrap(),
            NonZeroUsize::new(24).unwrap(),
            NonZeroUsize::new(4).unwrap(),
            ratio(0.12),
            NonZeroUsize::new(3).unwrap(),
        )
    }

    #[test]
    fn compatibility_table_separates_metric_filtering_from_sampling() {
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

    #[test]
    fn fixed_mode_uses_configured_threshold_exactly() {
        let decision = Layer0Policy::from_deployed(
            ActiveMetricKind::Cosine,
            SimHashMode::Always,
            threshold(37),
            ratio(0.5),
            None,
            true,
            failure(0.1),
        )
        .decide(context());
        assert_eq!(decision.threshold, Some(threshold(37)));
        assert_eq!(decision.sampling.probability(), 0.5);
    }

    #[test]
    fn bypass_disables_cached_and_missing_filtering_together() {
        let decision = Layer0Policy::from_deployed(
            ActiveMetricKind::Cosine,
            SimHashMode::Adaptive,
            threshold(43),
            ratio(0.5),
            None,
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
        assert!(decision.bypassed);
        assert!(!decision.fetch_missing);
        assert!(!decision.filter_cached);
        assert_eq!(decision.threshold, None);
        assert_eq!(decision.bypass_trigger, AdaptiveBypassTrigger::ReadBudget);
        assert_eq!(
            decision.next_bypass_state,
            AdaptiveBypassState::Bypassing {
                remaining: NonZeroUsize::new(3).unwrap()
            }
        );
    }

    #[test]
    fn adaptive_bypass_policy_owns_triggers_windows_and_cooldown() {
        let policy = Layer0Policy::from_deployed(
            ActiveMetricKind::Cosine,
            SimHashMode::Adaptive,
            threshold(43),
            ratio(0.5),
            None,
            true,
            failure(0.1),
        )
        .with_adaptive_bypass(adaptive_bypass_policy(SimHashMode::Adaptive));
        let decide = |observation| {
            policy.decide(SimHashContext {
                adaptive_bypass: observation,
                ..context()
            })
        };

        let low_yield = decide(AdaptiveBypassObservation {
            window_examined: 20,
            window_filtered: 0,
            window_expansions: 4,
            ..AdaptiveBypassObservation::idle()
        });
        assert!(low_yield.bypassed);
        assert_eq!(low_yield.bypass_trigger, AdaptiveBypassTrigger::LowYield);

        let both = decide(AdaptiveBypassObservation {
            simhash_filter_reads: 192,
            window_examined: 20,
            window_filtered: 0,
            window_expansions: 4,
            ..AdaptiveBypassObservation::idle()
        });
        assert_eq!(
            both.bypass_trigger,
            AdaptiveBypassTrigger::ReadBudgetAndLowYield
        );

        let mut state = both.next_bypass_state;
        for expected_remaining in [2, 1] {
            let continuing = decide(AdaptiveBypassObservation {
                state,
                ..AdaptiveBypassObservation::idle()
            });
            assert!(continuing.bypassed);
            assert_eq!(continuing.bypass_trigger, AdaptiveBypassTrigger::None);
            assert_eq!(
                continuing.next_bypass_state,
                AdaptiveBypassState::Bypassing {
                    remaining: NonZeroUsize::new(expected_remaining).unwrap()
                }
            );
            state = continuing.next_bypass_state;
        }
        let final_bypass = decide(AdaptiveBypassObservation {
            state,
            ..AdaptiveBypassObservation::idle()
        });
        assert!(final_bypass.bypassed);
        assert_eq!(
            final_bypass.next_bypass_state,
            AdaptiveBypassState::CoolingDown {
                remaining: NonZeroUsize::new(4).unwrap()
            }
        );

        state = final_bypass.next_bypass_state;
        for expected_remaining in [3, 2, 1] {
            let cooling = decide(AdaptiveBypassObservation {
                state,
                ..AdaptiveBypassObservation::idle()
            });
            assert!(!cooling.bypassed);
            assert_eq!(
                cooling.next_bypass_state,
                AdaptiveBypassState::CoolingDown {
                    remaining: NonZeroUsize::new(expected_remaining).unwrap()
                }
            );
            state = cooling.next_bypass_state;
        }
        let ready = decide(AdaptiveBypassObservation {
            state,
            ..AdaptiveBypassObservation::idle()
        });
        assert!(!ready.bypassed);
        assert_eq!(ready.next_bypass_state, AdaptiveBypassState::Ready);

        let retriggered = decide(AdaptiveBypassObservation {
            state: AdaptiveBypassState::CoolingDown {
                remaining: NonZeroUsize::MIN,
            },
            simhash_filter_reads: 192,
            ..AdaptiveBypassObservation::idle()
        });
        assert!(retriggered.bypassed);
        assert_eq!(
            retriggered.bypass_trigger,
            AdaptiveBypassTrigger::ReadBudget
        );

        let too_small = policy.decide(SimHashContext {
            candidate_frontier_len: 23,
            adaptive_bypass: AdaptiveBypassObservation {
                simhash_filter_reads: 192,
                ..AdaptiveBypassObservation::idle()
            },
            ..context()
        });
        assert!(!too_small.bypassed);
        assert_eq!(too_small.bypass_trigger, AdaptiveBypassTrigger::None);

        let one_epoch = Layer0Policy::from_deployed(
            ActiveMetricKind::Cosine,
            SimHashMode::Adaptive,
            threshold(43),
            ratio(0.5),
            None,
            true,
            failure(0.1),
        )
        .with_adaptive_bypass(AdaptiveBypassPolicy::from_deployed(
            SimHashMode::Adaptive,
            SearchBeamWidth::try_new(64, super::super::ResultCount::try_new(10).unwrap()).unwrap(),
            NonZeroUsize::new(24).unwrap(),
            NonZeroUsize::MIN,
            ratio(0.12),
            NonZeroUsize::new(3).unwrap(),
        ))
        .decide(SimHashContext {
            adaptive_bypass: AdaptiveBypassObservation {
                simhash_filter_reads: 192,
                ..AdaptiveBypassObservation::idle()
            },
            ..context()
        });
        assert_eq!(
            one_epoch.next_bypass_state,
            AdaptiveBypassState::CoolingDown {
                remaining: NonZeroUsize::MIN
            }
        );
    }

    #[test]
    fn adaptive_policy_is_total_at_cold_start_and_quality_boundaries() {
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

        let active = policy.decide(context());
        assert!((0.3..=0.90).contains(&active.sampling.probability()));
        assert!(active.threshold.unwrap().get() <= 64);
    }

    #[test]
    fn adaptive_threshold_and_sampling_are_monotonic_with_quality() {
        let policy = Layer0Policy::from_deployed(
            ActiveMetricKind::Cosine,
            SimHashMode::Adaptive,
            threshold(43),
            ratio(0.3),
            None,
            true,
            failure(0.1),
        );
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
    }

    #[test]
    fn adaptive_failure_probability_controls_threshold_strictness() {
        let decision = |failure_probability| {
            Layer0Policy::from_deployed(
                ActiveMetricKind::Cosine,
                SimHashMode::Adaptive,
                threshold(43),
                ratio(0.5),
                None,
                true,
                failure(failure_probability),
            )
            .decide(context())
            .threshold
            .unwrap()
            .get()
        };
        assert!(decision(0.4) >= decision(0.01));
    }

    #[test]
    fn adaptive_policy_consumes_zero_and_low_configured_thresholds() {
        let decide = |configured| {
            Layer0Policy::from_deployed(
                ActiveMetricKind::Cosine,
                SimHashMode::Adaptive,
                threshold(configured),
                ratio(0.5),
                None,
                true,
                failure(0.1),
            )
            .decide(context())
            .threshold
            .unwrap()
            .get()
        };
        assert_eq!(decide(0), 0);
        assert!(decide(20) <= 20);
        assert!(decide(43) <= 43);

        let mut cold = context();
        cold.topk_ready = false;
        assert_eq!(
            Layer0Policy::from_deployed(
                ActiveMetricKind::Cosine,
                SimHashMode::Adaptive,
                threshold(43),
                ratio(0.5),
                None,
                true,
                failure(0.1),
            )
            .decide(cold)
            .threshold,
            Some(threshold(1))
        );
    }

    #[test]
    fn decision_owns_pre_and_post_sampling_activation() {
        let policy = Layer0Policy::from_deployed(
            ActiveMetricKind::Cosine,
            SimHashMode::Always,
            threshold(43),
            ratio(0.4),
            Some(ratio(0.2)),
            true,
            failure(0.1),
        );
        let active = policy.decide(context());
        assert_eq!(active.pre_sampling.probability(), 0.25);
        assert_eq!(active.sampling.probability(), 0.4);

        let mut small = context();
        small.candidate_frontier_len = 4;
        let small = policy.decide(small);
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
}
