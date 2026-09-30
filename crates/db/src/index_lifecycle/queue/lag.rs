//! Exact-operation publication lag.
//!
//! Lag is the time from a locally observed durable enqueue commit to the
//! durable acknowledgement of that same operation ID. Operations this process
//! learned were durable without first observing their commit return (loaded
//! at startup, reconciled after an uncertain commit, or acknowledged, or
//! their acknowledgement attempted, by publication before the producer saw
//! its commit return, even if it then did) have no measurable lag; they are
//! counted as censored instead of being given a guessed value.

use std::collections::BTreeMap;

use serde::Serialize;

/// Values below this many microseconds keep exact buckets.
const LINEAR_MICROS: u64 = 16;
/// Each power of two above the linear range splits into `2^SUB_BUCKET_BITS`
/// buckets, so a bucket spans at most 12.5% of its lower bound.
const SUB_BUCKET_BITS: u32 = 3;

/// Cumulative log-linear histogram of publication lag in microseconds.
///
/// Buckets are keyed by their inclusive lower bound. Consumers compute a
/// window's distribution by subtracting two snapshots bucket by bucket.
///
/// ```
/// let mut lag = db::PublicationLagHistogram::default();
/// for micros in [3, 100, 101, 5_000] {
///     lag.record(micros);
/// }
/// assert_eq!(lag.count(), 4);
/// assert_eq!(lag.max_micros(), 5_000);
/// // 100 and 101 share the [96, 104) bucket.
/// assert_eq!(lag.buckets().collect::<Vec<_>>(), [(3, 1), (96, 2), (4_608, 1)]);
/// assert_eq!(lag.quantile_lower_bound(0.5), Some(96));
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct PublicationLagHistogram {
    buckets: BTreeMap<u64, u64>,
    count: u64,
    sum_micros: u64,
    max_micros: u64,
}

impl PublicationLagHistogram {
    /// Records one observed lag.
    pub fn record(&mut self, micros: u64) {
        *self.buckets.entry(bucket_floor(micros)).or_default() += 1;
        self.count += 1;
        self.sum_micros = self.sum_micros.saturating_add(micros);
        self.max_micros = self.max_micros.max(micros);
    }

    /// Returns `(lower bound in microseconds, observations)` in ascending order.
    pub fn buckets(&self) -> impl Iterator<Item = (u64, u64)> + '_ {
        self.buckets.iter().map(|(floor, count)| (*floor, *count))
    }

    /// Returns the number of recorded observations.
    pub const fn count(&self) -> u64 {
        self.count
    }

    /// Returns the saturating sum of recorded lag.
    pub const fn sum_micros(&self) -> u64 {
        self.sum_micros
    }

    /// Returns the largest recorded lag.
    pub const fn max_micros(&self) -> u64 {
        self.max_micros
    }

    /// Returns the lower bound of the bucket holding quantile `q` in `[0, 1]`,
    /// or `None` when nothing was recorded.
    pub fn quantile_lower_bound(&self, q: f64) -> Option<u64> {
        assert!((0.0..=1.0).contains(&q), "quantile must lie in [0, 1]");
        // Nearest-rank: the smallest value with at least `q * count` at or
        // below it. The float rank is clamped back into `[1, count]`.
        let rank = ((q * self.count as f64).ceil() as u64).clamp(1, self.count.max(1));
        let mut seen = 0_u64;
        self.buckets.iter().find_map(|(floor, count)| {
            seen += count;
            (seen >= rank).then_some(*floor)
        })
    }
}

/// Lower bound of the bucket holding `micros`.
fn bucket_floor(micros: u64) -> u64 {
    if micros < LINEAR_MICROS {
        return micros;
    }
    let shift = micros.ilog2() - SUB_BUCKET_BITS;
    (micros >> shift) << shift
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buckets_are_exact_below_sixteen_and_within_an_eighth_above() {
        for micros in 0..LINEAR_MICROS {
            assert_eq!(bucket_floor(micros), micros);
        }
        for micros in [16, 17, 31, 32, 1_000, 999_999, u64::MAX / 3, u64::MAX] {
            let floor = bucket_floor(micros);
            assert!(floor <= micros);
            assert!(micros - floor <= floor / 8, "{micros} -> {floor}");
            assert_eq!(bucket_floor(floor), floor, "floors are fixed points");
        }
        assert_eq!(bucket_floor(u64::MAX), 0xF000_0000_0000_0000);
    }

    #[test]
    fn quantiles_use_nearest_rank_and_empty_has_none() {
        let mut lag = PublicationLagHistogram::default();
        assert_eq!(lag.quantile_lower_bound(0.99), None);
        for micros in 1..=100 {
            lag.record(micros);
        }
        assert_eq!(lag.quantile_lower_bound(0.0), Some(1));
        assert_eq!(lag.quantile_lower_bound(0.5), Some(48));
        assert_eq!(lag.quantile_lower_bound(0.99), Some(96));
        assert_eq!(lag.quantile_lower_bound(1.0), Some(96));
        assert_eq!(lag.sum_micros(), 5_050);
        lag.record(u64::MAX);
        assert_eq!(lag.sum_micros(), u64::MAX, "the sum saturates");
    }

    #[test]
    #[should_panic(expected = "quantile must lie in [0, 1]")]
    fn out_of_range_quantiles_are_rejected() {
        PublicationLagHistogram::default().quantile_lower_bound(1.5);
    }
}
