//! Bounded tally of the planner insights of recently executed queries.
//!
//! Each recorded execution adds the missing-index and unbounded-scan insights
//! of its selected plan ([`PlannerInsight`]). An unbounded scan that a missing
//! index of the same plan would bound (same element and label, with the
//! index's property among the scan's predicate properties) is counted under
//! that missing index only, so creating the index answers both. Deep
//! traversals describe query shape rather than an access path and are not
//! tallied.
//!
//! The tally holds at most [`MAX_TALLIED_INSIGHTS`] distinct insights. It
//! forgets an insight [`INSIGHT_WINDOW`] after it was last seen, and a new
//! insight arriving while it is full evicts the least recently seen one.
//! Insights carry label and property names only, never values, and the
//! names any client sends are bounded: each is cut to
//! [`MAX_TALLIED_NAME_BYTES`], and a scan keeps its first
//! [`MAX_TALLIED_PROPERTIES`] predicate properties. Recording takes one
//! short lock, and only for executions with tallied insights.
//!
//! # Examples
//!
//! ```
//! use db::query_service::insight_tally::{InsightTally, TalliedInsight};
//! use helix_planner::catalog::{ElementKind, IndexCatalogSnapshot, ScopedPropertyKey};
//! use helix_planner::diagnostics::{
//!     MissingIndexInsight, PlannerDiagnostics, PlannerInsight, SecondaryIndexKind,
//! };
//! use helix_planner::ir::NonEmptyString;
//!
//! let name = |value: &str| NonEmptyString::new(value).unwrap();
//! let diagnostics = PlannerDiagnostics {
//!     insights: vec![PlannerInsight::MissingIndex(MissingIndexInsight {
//!         element: ElementKind::Node,
//!         label: name("User"),
//!         property: name("email"),
//!         index_kind: SecondaryIndexKind::Equality,
//!         occurrences: 1,
//!     })],
//!     ..PlannerDiagnostics::default()
//! };
//! let tally = InsightTally::default();
//! tally.record(&diagnostics);
//! tally.record(&diagnostics);
//!
//! let snapshot = tally.snapshot(&IndexCatalogSnapshot::default());
//! assert_eq!(snapshot.analyzed_queries, 2);
//! assert_eq!(snapshot.insights[0].queries, 2);
//! assert!(matches!(
//!     &snapshot.insights[0].insight,
//!     TalliedInsight::MissingIndex { property, .. } if property.as_ref() == "email"
//! ));
//!
//! // Once the recommended index is active, the insight is answered.
//! let indexed = IndexCatalogSnapshot::default()
//!     .with_node_eq(ScopedPropertyKey::try_new("User", "email").unwrap());
//! assert!(tally.snapshot(&indexed).insights.is_empty());
//! ```

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use helix_ast::index::RangeIndexDirection;
use helix_planner::catalog::{
    ElementKind, IndexCatalogSnapshot, ScopedPropertyDirectionKey, ScopedPropertyKey,
};
use helix_planner::diagnostics::{
    PlannerDiagnostics, PlannerInsight, PredicatePropertySet, SecondaryIndexKind,
    UnboundedScanInsight,
};
use helix_planner::ir::NonEmptyString;

/// Most distinct insights the tally holds at once.
pub const MAX_TALLIED_INSIGHTS: usize = 128;

/// How long after it was last seen the tally forgets an insight.
pub const INSIGHT_WINDOW: Duration = Duration::from_secs(24 * 60 * 60);

/// Longest label or property name the tally keeps, in bytes; longer names
/// are cut at a character boundary.
pub const MAX_TALLIED_NAME_BYTES: usize = 128;

/// Most predicate properties a tallied unbounded scan keeps, in name order.
pub const MAX_TALLIED_PROPERTIES: usize = 8;

/// One distinct tallied insight.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum TalliedInsight {
    /// Queries filtered `label.property` without the secondary index the
    /// planner recommends for it.
    MissingIndex {
        /// Node or edge index family.
        element: ElementKind,
        /// Label the index is scoped to.
        label: NonEmptyString,
        /// Property the index covers.
        property: NonEmptyString,
        /// Equality or range index family.
        index_kind: SecondaryIndexKind,
    },
    /// Queries read every element of a label, or every element, and no
    /// missing index of theirs would bound the read.
    UnboundedScan {
        /// Node or edge scan.
        element: ElementKind,
        /// Scanned label, when the scan is label-scoped.
        label: Option<NonEmptyString>,
        /// Properties the scanned elements were filtered on.
        predicate_properties: PredicatePropertySet,
    },
}

impl TalliedInsight {
    /// Whether an active index in `catalog` answers this insight: the
    /// missing index exists, in either range direction for a range index.
    /// Unbounded scans have no index that answers them.
    pub fn is_answered_by(&self, catalog: &IndexCatalogSnapshot) -> bool {
        match self {
            Self::MissingIndex {
                element,
                label,
                property,
                index_kind,
            } => {
                let key = ScopedPropertyKey::new(label.clone(), property.clone());
                let ranged = |direction| {
                    ScopedPropertyDirectionKey::new(label.clone(), property.clone(), direction)
                };
                match (element, index_kind) {
                    (ElementKind::Node, SecondaryIndexKind::Equality) => {
                        catalog.node_eq.contains_key(&key)
                    }
                    (ElementKind::Edge, SecondaryIndexKind::Equality) => {
                        catalog.edge_eq.contains_key(&key)
                    }
                    (ElementKind::Node, SecondaryIndexKind::Range) => {
                        [RangeIndexDirection::Asc, RangeIndexDirection::Desc]
                            .into_iter()
                            .any(|direction| catalog.node_range.contains_key(&ranged(direction)))
                    }
                    (ElementKind::Edge, SecondaryIndexKind::Range) => {
                        [RangeIndexDirection::Asc, RangeIndexDirection::Desc]
                            .into_iter()
                            .any(|direction| catalog.edge_range.contains_key(&ranged(direction)))
                    }
                }
            }
            Self::UnboundedScan { .. } => false,
        }
    }
}

/// How often one tallied insight was raised.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InsightCount {
    /// The insight.
    pub insight: TalliedInsight,
    /// Executions that raised it since it last entered the tally.
    pub queries: u64,
    /// Time since an execution last raised it.
    pub last_seen_ago: Duration,
}

/// Point-in-time view of an [`InsightTally`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct InsightSnapshot {
    /// Executions recorded, with or without insights.
    pub analyzed_queries: u64,
    /// Unanswered insights last seen within [`INSIGHT_WINDOW`], most queries
    /// first, then most recently seen.
    pub insights: Vec<InsightCount>,
    /// Distinct insights evicted because the tally was full.
    pub evicted_insights: u64,
}

/// Bounded, recent tally of planner insights; see the [module docs](self).
#[derive(Debug, Default)]
pub struct InsightTally {
    analyzed_queries: AtomicU64,
    state: Mutex<TallyState>,
}

#[derive(Debug, Default)]
struct TallyState {
    seen: BTreeMap<TalliedInsight, Sightings>,
    evicted: u64,
}

#[derive(Debug, Clone, Copy)]
struct Sightings {
    queries: u64,
    last: Instant,
}

impl InsightTally {
    /// Adds one execution and the insights of its selected plan.
    ///
    /// Executions without tallied insights touch only an atomic counter.
    pub fn record(&self, diagnostics: &PlannerDiagnostics) {
        self.record_at(diagnostics, Instant::now());
    }

    fn record_at(&self, diagnostics: &PlannerDiagnostics, now: Instant) {
        self.analyzed_queries.fetch_add(1, Ordering::Relaxed);
        let tallied = diagnostics
            .insights
            .iter()
            .filter_map(|insight| match insight {
                PlannerInsight::MissingIndex(missing) => Some(TalliedInsight::MissingIndex {
                    element: missing.element,
                    label: bounded_name(&missing.label),
                    property: bounded_name(&missing.property),
                    index_kind: missing.index_kind,
                }),
                PlannerInsight::UnboundedScan(scan)
                    if !bounded_by_missing_index(scan, &diagnostics.insights) =>
                {
                    Some(TalliedInsight::UnboundedScan {
                        element: scan.element,
                        label: scan.label.as_ref().map(bounded_name),
                        predicate_properties: PredicatePropertySet::new(
                            scan.predicate_properties
                                .iter()
                                .take(MAX_TALLIED_PROPERTIES)
                                .map(bounded_name),
                        ),
                    })
                }
                PlannerInsight::UnboundedScan(_) | PlannerInsight::DeepTraversal(_) => None,
            })
            .collect::<Vec<_>>();
        if tallied.is_empty() {
            return;
        }
        let mut state = self
            .state
            .lock()
            .expect("insight tally lock is not poisoned");
        tallied
            .into_iter()
            .for_each(|insight| state.sighted(insight, now));
    }

    /// Returns the insights last seen within [`INSIGHT_WINDOW`] that no
    /// active index in `catalog` answers yet.
    pub fn snapshot(&self, catalog: &IndexCatalogSnapshot) -> InsightSnapshot {
        self.snapshot_at(catalog, Instant::now())
    }

    fn snapshot_at(&self, catalog: &IndexCatalogSnapshot, now: Instant) -> InsightSnapshot {
        // Copy under the lock; filter and sort after releasing it.
        let (seen, evicted_insights) = {
            let state = self
                .state
                .lock()
                .expect("insight tally lock is not poisoned");
            let seen = state
                .seen
                .iter()
                .map(|(insight, sightings)| (insight.clone(), *sightings))
                .collect::<Vec<_>>();
            (seen, state.evicted)
        };
        let mut insights = seen
            .into_iter()
            .map(|(insight, sightings)| InsightCount {
                insight,
                queries: sightings.queries,
                last_seen_ago: now.saturating_duration_since(sightings.last),
            })
            .filter(|count| count.last_seen_ago <= INSIGHT_WINDOW)
            .filter(|count| !count.insight.is_answered_by(catalog))
            .collect::<Vec<_>>();
        // Stable, so equal counts keep the tally's key order.
        insights.sort_by(|left, right| {
            right
                .queries
                .cmp(&left.queries)
                .then(left.last_seen_ago.cmp(&right.last_seen_ago))
        });
        InsightSnapshot {
            analyzed_queries: self.analyzed_queries.load(Ordering::Relaxed),
            insights,
            evicted_insights,
        }
    }
}

impl TallyState {
    /// Counts one sighting, restarting an insight the window had expired and
    /// making room for a new one when full.
    fn sighted(&mut self, insight: TalliedInsight, now: Instant) {
        let expired =
            |sightings: &Sightings| now.saturating_duration_since(sightings.last) > INSIGHT_WINDOW;
        let Some(sightings) = self.seen.get_mut(&insight) else {
            if self.seen.len() >= MAX_TALLIED_INSIGHTS {
                self.seen.retain(|_, sightings| !expired(sightings));
            }
            if self.seen.len() >= MAX_TALLIED_INSIGHTS {
                let oldest = self
                    .seen
                    .iter()
                    .min_by_key(|(_, sightings)| sightings.last)
                    .map(|(insight, _)| insight.clone())
                    .expect("a full tally has an oldest insight");
                self.seen.remove(&oldest);
                self.evicted = self.evicted.saturating_add(1);
            }
            self.seen.insert(
                insight,
                Sightings {
                    queries: 1,
                    last: now,
                },
            );
            return;
        };
        *sightings = Sightings {
            queries: if expired(sightings) {
                1
            } else {
                sightings.queries.saturating_add(1)
            },
            last: now,
        };
    }
}

/// `name` cut to at most [`MAX_TALLIED_NAME_BYTES`] at a character boundary.
fn bounded_name(name: &NonEmptyString) -> NonEmptyString {
    let name = name.as_ref();
    let end = (1..=MAX_TALLIED_NAME_BYTES.min(name.len()))
        .rev()
        .find(|&end| name.is_char_boundary(end))
        .expect("a non-empty name has a first character within the limit");
    NonEmptyString::new(&name[..end]).expect("a cut name keeps its first character")
}

/// Whether a missing index of the same plan would bound `scan`.
fn bounded_by_missing_index(scan: &UnboundedScanInsight, insights: &[PlannerInsight]) -> bool {
    insights.iter().any(|insight| match insight {
        PlannerInsight::MissingIndex(missing) => {
            missing.element == scan.element
                && scan.label.as_ref() == Some(&missing.label)
                && scan
                    .predicate_properties
                    .iter()
                    .any(|property| *property == missing.property)
        }
        PlannerInsight::UnboundedScan(_) | PlannerInsight::DeepTraversal(_) => false,
    })
}

#[cfg(test)]
mod tests {
    use helix_planner::diagnostics::{
        DeepTraversalInsight, MissingIndexInsight, PlannerDiagnostics, PlannerInsight,
        UnboundedScanInsight,
    };

    use super::*;

    fn name(value: &str) -> NonEmptyString {
        NonEmptyString::new(value).expect("test names are non-empty")
    }

    fn missing(
        element: ElementKind,
        label: &str,
        property: &str,
        kind: SecondaryIndexKind,
    ) -> PlannerInsight {
        PlannerInsight::MissingIndex(MissingIndexInsight {
            element,
            label: name(label),
            property: name(property),
            index_kind: kind,
            occurrences: 3,
        })
    }

    fn scan(label: Option<&str>, properties: &[&str]) -> PlannerInsight {
        PlannerInsight::UnboundedScan(UnboundedScanInsight {
            element: ElementKind::Node,
            label: label.map(name),
            predicate_properties: PredicatePropertySet::new(properties.iter().copied().map(name)),
            occurrences: 1,
        })
    }

    fn diagnostics(insights: Vec<PlannerInsight>) -> PlannerDiagnostics {
        PlannerDiagnostics {
            insights,
            ..PlannerDiagnostics::default()
        }
    }

    fn tallied_missing(label: &str, property: &str, kind: SecondaryIndexKind) -> TalliedInsight {
        TalliedInsight::MissingIndex {
            element: ElementKind::Node,
            label: name(label),
            property: name(property),
            index_kind: kind,
        }
    }

    #[test]
    fn executions_without_insights_only_count_analyzed_queries() {
        let tally = InsightTally::default();
        tally.record(&PlannerDiagnostics::default());
        tally.record(&diagnostics(vec![PlannerInsight::DeepTraversal(
            DeepTraversalInsight {
                expansion_count: 4,
                repeat_count: 1,
                maximum_depth: 4,
            },
        )]));

        let snapshot = tally.snapshot(&IndexCatalogSnapshot::default());
        assert_eq!(snapshot.analyzed_queries, 2);
        assert!(
            snapshot.insights.is_empty(),
            "deep traversals are not tallied"
        );
        assert_eq!(snapshot.evicted_insights, 0);
    }

    #[test]
    fn a_scan_a_missing_index_would_bound_counts_only_under_that_index() {
        let tally = InsightTally::default();
        tally.record(&diagnostics(vec![
            scan(Some("User"), &["email", "name"]),
            missing(
                ElementKind::Node,
                "User",
                "email",
                SecondaryIndexKind::Equality,
            ),
        ]));

        let snapshot = tally.snapshot(&IndexCatalogSnapshot::default());
        assert_eq!(
            snapshot
                .insights
                .iter()
                .map(|count| &count.insight)
                .collect::<Vec<_>>(),
            [&tallied_missing(
                "User",
                "email",
                SecondaryIndexKind::Equality
            )]
        );
    }

    #[test]
    fn scans_no_missing_index_bounds_are_tallied_on_their_own() {
        let tally = InsightTally::default();
        tally.record(&diagnostics(vec![
            // Another label's missing index does not bound this scan.
            scan(Some("User"), &["bio"]),
            missing(
                ElementKind::Node,
                "Post",
                "bio",
                SecondaryIndexKind::Equality,
            ),
            // Nor does one whose property the scan never filtered on.
            scan(Some("Post"), &[]),
            // An unlabeled scan has no label for an index.
            scan(None, &["bio"]),
            // An edge index never bounds a node scan.
            missing(
                ElementKind::Edge,
                "User",
                "bio",
                SecondaryIndexKind::Equality,
            ),
        ]));

        let insights = tally
            .snapshot(&IndexCatalogSnapshot::default())
            .insights
            .into_iter()
            .map(|count| count.insight)
            .collect::<Vec<_>>();
        assert_eq!(insights.len(), 5, "{insights:?}");
        for (label, properties) in [
            (Some("User"), vec!["bio"]),
            (Some("Post"), vec![]),
            (None, vec!["bio"]),
        ] {
            let expected = TalliedInsight::UnboundedScan {
                element: ElementKind::Node,
                label: label.map(name),
                predicate_properties: PredicatePropertySet::new(properties.into_iter().map(name)),
            };
            assert!(insights.contains(&expected), "{expected:?} in {insights:?}");
        }
        let indexed = IndexCatalogSnapshot::default()
            .with_node_eq(ScopedPropertyKey::try_new("User", "bio").unwrap());
        assert!(
            !TalliedInsight::UnboundedScan {
                element: ElementKind::Node,
                label: Some(name("User")),
                predicate_properties: PredicatePropertySet::new([name("bio")]),
            }
            .is_answered_by(&indexed),
            "no index answers an unbounded scan"
        );
    }

    #[test]
    fn insights_rank_by_queries_then_recency() {
        let tally = InsightTally::default();
        let start = Instant::now();
        let email = missing(
            ElementKind::Node,
            "User",
            "email",
            SecondaryIndexKind::Equality,
        );
        let age = missing(ElementKind::Node, "User", "age", SecondaryIndexKind::Range);
        let name_index = missing(
            ElementKind::Node,
            "User",
            "name",
            SecondaryIndexKind::Equality,
        );
        tally.record_at(&diagnostics(vec![email.clone()]), start);
        tally.record_at(&diagnostics(vec![email]), start);
        tally.record_at(&diagnostics(vec![name_index]), start);
        tally.record_at(&diagnostics(vec![age]), start + Duration::from_secs(5));

        let snapshot = tally.snapshot_at(
            &IndexCatalogSnapshot::default(),
            start + Duration::from_secs(10),
        );
        let ranked = snapshot
            .insights
            .iter()
            .map(|count| (count.insight.clone(), count.queries, count.last_seen_ago))
            .collect::<Vec<_>>();
        assert_eq!(
            ranked,
            [
                (
                    tallied_missing("User", "email", SecondaryIndexKind::Equality),
                    2,
                    Duration::from_secs(10)
                ),
                (
                    tallied_missing("User", "age", SecondaryIndexKind::Range),
                    1,
                    Duration::from_secs(5)
                ),
                (
                    tallied_missing("User", "name", SecondaryIndexKind::Equality),
                    1,
                    Duration::from_secs(10)
                ),
            ]
        );
        assert_eq!(snapshot.analyzed_queries, 4);
    }

    #[test]
    fn the_window_forgets_insights_and_restarts_their_count() {
        let tally = InsightTally::default();
        let start = Instant::now();
        let email = diagnostics(vec![missing(
            ElementKind::Node,
            "User",
            "email",
            SecondaryIndexKind::Equality,
        )]);
        tally.record_at(&email, start);
        tally.record_at(&email, start);

        let catalog = IndexCatalogSnapshot::default();
        assert_eq!(
            tally.snapshot_at(&catalog, start + INSIGHT_WINDOW).insights[0].queries,
            2,
            "an insight seen exactly one window ago is still reported"
        );
        let later = start + INSIGHT_WINDOW + Duration::from_secs(1);
        assert!(tally.snapshot_at(&catalog, later).insights.is_empty());

        tally.record_at(&email, later);
        let restarted = tally.snapshot_at(&catalog, later);
        assert_eq!(restarted.insights[0].queries, 1);
        assert_eq!(restarted.insights[0].last_seen_ago, Duration::ZERO);
        assert_eq!(restarted.evicted_insights, 0);
    }

    #[test]
    fn a_full_tally_drops_expired_insights_before_evicting_the_least_recent() {
        let tally = InsightTally::default();
        let start = Instant::now();
        let property = |index: usize| format!("p{index}");
        let insight = |index: usize| {
            diagnostics(vec![missing(
                ElementKind::Node,
                "User",
                &property(index),
                SecondaryIndexKind::Equality,
            )])
        };
        // p0 expires; the rest stay within the window.
        tally.record_at(&insight(0), start);
        (1..MAX_TALLIED_INSIGHTS).for_each(|index| {
            tally.record_at(
                &insight(index),
                start + INSIGHT_WINDOW + Duration::from_secs(index as u64),
            );
        });
        let full = start + INSIGHT_WINDOW + Duration::from_secs(1_000);

        tally.record_at(&insight(MAX_TALLIED_INSIGHTS), full);
        let snapshot = tally.snapshot_at(&IndexCatalogSnapshot::default(), full);
        assert_eq!(snapshot.insights.len(), MAX_TALLIED_INSIGHTS);
        assert_eq!(snapshot.evicted_insights, 0, "an expired insight made room");

        tally.record_at(&insight(MAX_TALLIED_INSIGHTS + 1), full);
        let snapshot = tally.snapshot_at(&IndexCatalogSnapshot::default(), full);
        assert_eq!(snapshot.insights.len(), MAX_TALLIED_INSIGHTS);
        assert_eq!(snapshot.evicted_insights, 1);
        let tallied = |index: usize| {
            snapshot.insights.iter().any(|count| {
                count.insight
                    == tallied_missing("User", &property(index), SecondaryIndexKind::Equality)
            })
        };
        assert!(!tallied(1), "p1 was the least recently seen");
        assert!(tallied(2));
        assert!(tallied(MAX_TALLIED_INSIGHTS + 1));
    }

    #[test]
    fn active_indexes_answer_their_missing_index_in_any_range_direction() {
        let tally = InsightTally::default();
        tally.record(&diagnostics(vec![
            missing(
                ElementKind::Node,
                "User",
                "email",
                SecondaryIndexKind::Equality,
            ),
            missing(ElementKind::Node, "User", "age", SecondaryIndexKind::Range),
            missing(
                ElementKind::Edge,
                "Follows",
                "since",
                SecondaryIndexKind::Range,
            ),
            missing(
                ElementKind::Edge,
                "Follows",
                "kind",
                SecondaryIndexKind::Equality,
            ),
        ]));
        let remaining = |catalog: IndexCatalogSnapshot| tally.snapshot(&catalog).insights;
        let key =
            |label: &str, property: &str| ScopedPropertyKey::try_new(label, property).unwrap();
        let ranged = |label: &str, property: &str, direction| {
            ScopedPropertyDirectionKey::try_new(label, property, direction).unwrap()
        };

        assert_eq!(remaining(IndexCatalogSnapshot::default()).len(), 4);
        let answered = IndexCatalogSnapshot::default()
            .with_node_eq(key("User", "email"))
            .with_node_range(ranged("User", "age", RangeIndexDirection::Desc))
            .with_edge_range(ranged("Follows", "since", RangeIndexDirection::Asc))
            .with_edge_eq(key("Follows", "kind"));
        assert!(remaining(answered).is_empty());

        // An index of another family, element, or label answers nothing.
        let unrelated = IndexCatalogSnapshot::default()
            .with_node_range(ranged("User", "email", RangeIndexDirection::Asc))
            .with_edge_eq(key("User", "email"))
            .with_node_eq(key("Admin", "email"))
            .with_node_eq(key("User", "age"))
            .with_node_eq(key("Follows", "kind"));
        assert_eq!(remaining(unrelated).len(), 4);
    }

    #[test]
    fn client_supplied_names_and_property_sets_are_bounded() {
        let tally = InsightTally::default();
        // Two-byte characters, so the byte limit falls between characters.
        let long = "é".repeat(MAX_TALLIED_NAME_BYTES);
        let wide = (0..MAX_TALLIED_PROPERTIES + 3)
            .map(|index| format!("p{index:02}"))
            .collect::<Vec<_>>();
        tally.record(&diagnostics(vec![
            missing(
                ElementKind::Node,
                &long,
                "email",
                SecondaryIndexKind::Equality,
            ),
            scan(
                Some("Post"),
                &wide.iter().map(String::as_str).collect::<Vec<_>>(),
            ),
        ]));

        let insights = tally.snapshot(&IndexCatalogSnapshot::default()).insights;
        let cut = "é".repeat(MAX_TALLIED_NAME_BYTES / 2);
        assert!(insights.iter().any(|count| count.insight
            == TalliedInsight::MissingIndex {
                element: ElementKind::Node,
                label: name(&cut),
                property: name("email"),
                index_kind: SecondaryIndexKind::Equality,
            }));
        assert!(insights.iter().any(|count| count.insight
            == TalliedInsight::UnboundedScan {
                element: ElementKind::Node,
                label: Some(name("Post")),
                predicate_properties: PredicatePropertySet::new(
                    wide.iter()
                        .take(MAX_TALLIED_PROPERTIES)
                        .map(|property| name(property)),
                ),
            }));
        assert_eq!(bounded_name(&name("short")), name("short"));
    }

    #[test]
    fn concurrent_records_lose_no_counts() {
        let tally = InsightTally::default();
        let email = diagnostics(vec![missing(
            ElementKind::Node,
            "User",
            "email",
            SecondaryIndexKind::Equality,
        )]);
        std::thread::scope(|scope| {
            for _ in 0..8 {
                scope.spawn(|| (0..250).for_each(|_| tally.record(&email)));
            }
        });
        let snapshot = tally.snapshot(&IndexCatalogSnapshot::default());
        assert_eq!(snapshot.analyzed_queries, 2_000);
        assert_eq!(snapshot.insights[0].queries, 2_000);
    }
}
