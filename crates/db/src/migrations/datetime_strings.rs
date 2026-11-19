//! Background conversion of RFC3339 strings into stored datetimes.
//!
//! Before typed `date_time` parameters kept their type, every value written
//! through one was stored as its raw RFC3339 string. Those values compare and
//! index in the string domain, apart from datetimes written as literals or by
//! `now()`, so range and equality filters of one kind miss the other. This job
//! converts them once, storage scope by storage scope (the unscoped namespace,
//! then every tenant in key order):
//!
//! 1. **Detect** scans node rows, then edge rows, and keeps one verdict per
//!    `(element, label, property)`. A property converts only when every
//!    non-null value seen is a datetime or an RFC3339 string with at most
//!    millisecond precision, and at least one is a string. Any other value
//!    leaves the whole property as stored and reports it, so a string that
//!    only looks like a timestamp is never reinterpreted.
//! 2. **Convert** rescans the rows and rewrites each convertible string through
//!    the ordinary write path, guarded on that exact string still being stored.
//!    A concurrent write wins, every secondary index generation (active or
//!    building) is maintained exactly as for a user write, and a replayed batch
//!    is a no-op because its guards no longer match.
//!
//! Progress is one JSON row in the unscoped metadata namespace, committed after
//! every batch, so the job resumes after a restart. Older binaries never read
//! that row. A row whose conversion would collide in a unique index keeps its
//! string and is counted, as is every skipped property.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;

use bytes::Bytes;
use helix_ast::batch::write_batch;
use helix_ast::expr::Predicate;
use helix_ast::graph::{EdgeRef, NodeRef};
use helix_ast::query::QueryRequest;
use helix_ast::traversal::g;
use helix_ast::value::{DateTime, PropertyInput};
use serde::{Deserialize, Serialize};

use super::{decode_json, encode_json, label_of, scan_bounds_for_prefix, scoped_metadata_key};
use crate::config::MigrationTuning;
use crate::encoding::keys::scope::{DataScope, TenantId, TENANT_KEY_PREFIX};
use crate::encoding::property::property_value::PropertyValue;
use crate::encoding::property::{decode_properties, Property};
use crate::encoding::v1::keys::{Key, KeyPrefix};
use crate::error::{HelixDbError, Result};
use crate::{HelixDB, HelixStorage};

/// Unscoped metadata row holding the job.
const JOB_KEY: &[u8] = b"datetime_string_migration:v1";

/// Distinct `(element, label, property)` verdicts one scope may track. A scope
/// with more distinct property names than this is skipped and reported rather
/// than holding an unbounded verdict map in every progress row.
pub(crate) const MAX_TRACKED_PROPERTIES: usize = if cfg!(test) { 32 } else { 10_000 };

/// Skipped properties the report lists; later ones are only counted.
const MAX_REPORTED_SKIPS: usize = 1_000;

/// Physical id suffix of node and by-id edge property keys.
const ID_LEN: usize = core::mem::size_of::<u64>();

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Element {
    Node,
    Edge,
}

impl Element {
    fn logical_prefix(self) -> &'static [u8] {
        match self {
            Self::Node => KeyPrefix::NodeProperty.as_slice(),
            Self::Edge => KeyPrefix::EdgePropertyById.as_slice(),
        }
    }
}

/// Storage scope position, ordered as the job visits scopes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ScopeCursor {
    Unscoped,
    /// Tenant id as 32 lowercase hex digits (JSON numbers lose `u128`s).
    Tenant(#[serde(with = "tenant_hex")] u128),
}

impl ScopeCursor {
    fn data_scope(self) -> DataScope {
        match self {
            Self::Unscoped => DataScope::LegacyUnscoped,
            Self::Tenant(tenant) => DataScope::Tenant(TenantId::from_u128(tenant)),
        }
    }
}

mod tenant_hex {
    use serde::{Deserialize, Deserializer, Serializer};

    pub(super) fn serialize<S: Serializer>(
        tenant: &u128,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&format!("{tenant:032x}"))
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<u128, D::Error> {
        let text = String::deserialize(deserializer)?;
        u128::from_str_radix(&text, 16).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub(crate) struct PropertyKey {
    pub(crate) element: Element,
    pub(crate) label: String,
    pub(crate) name: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SkipReason {
    /// A value that is neither a string, a datetime, nor null.
    MixedTypes,
    /// A string that is not RFC3339.
    NotRfc3339,
    /// An RFC3339 string finer than milliseconds; converting it would lose digits.
    SubMillisecond,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Verdict {
    /// Only datetimes so far: nothing to convert yet.
    Datetimes,
    /// Datetimes and at least one convertible string.
    Convertible,
    /// Left as stored.
    Skipped(SkipReason),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Tracked {
    verdict: Verdict,
    /// Convertible RFC3339 strings seen, reported when the property is skipped.
    rfc3339_strings: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SkippedProperty {
    pub(crate) scope: ScopeCursor,
    pub(crate) property: PropertyKey,
    pub(crate) reason: SkipReason,
    pub(crate) rfc3339_strings: u64,
}

/// Cumulative outcome across every finished scope and the current one.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Report {
    /// String values rewritten as datetimes.
    pub(crate) converted_values: u64,
    /// Rows left as strings because the datetime collides in a unique index.
    pub(crate) unique_conflicts: u64,
    /// Properties with RFC3339 strings left as stored, first ones listed.
    pub(crate) skipped_properties: Vec<SkippedProperty>,
    /// Skipped properties beyond the listed ones.
    pub(crate) unlisted_skipped_properties: u64,
    /// Scopes skipped for exceeding [`MAX_TRACKED_PROPERTIES`].
    pub(crate) skipped_scopes: Vec<ScopeCursor>,
    /// Rows whose properties failed to decode, left as stored.
    pub(crate) undecodable_rows: u64,
}

impl Report {
    fn skip(&mut self, skipped: SkippedProperty) {
        if self.skipped_properties.len() < MAX_REPORTED_SKIPS {
            self.skipped_properties.push(skipped);
        } else {
            self.unlisted_skipped_properties = self.unlisted_skipped_properties.saturating_add(1);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum Phase {
    Detect {
        element: Element,
        resume_after: Option<Vec<u8>>,
        properties: Vec<(PropertyKey, Tracked)>,
    },
    Convert {
        element: Element,
        resume_after: Option<Vec<u8>>,
        candidates: Vec<PropertyKey>,
    },
}

impl Phase {
    const fn start() -> Self {
        Self::Detect {
            element: Element::Node,
            resume_after: None,
            properties: Vec::new(),
        }
    }
}

/// Durable job state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum Job {
    Running {
        scope: ScopeCursor,
        phase: Phase,
        report: Report,
    },
    Completed {
        report: Report,
    },
}

impl Job {
    fn start() -> Self {
        Self::Running {
            scope: ScopeCursor::Unscoped,
            phase: Phase::start(),
            report: Report::default(),
        }
    }

    /// The report so far, complete once the job is.
    #[cfg(test)]
    pub(crate) fn report(&self) -> &Report {
        let (Self::Running { report, .. } | Self::Completed { report }) = self;
        report
    }

    pub(crate) fn is_completed(&self) -> bool {
        matches!(self, Self::Completed { .. })
    }
}

/// How one stored value bears on its property's verdict.
fn classify(value: &PropertyValue) -> Option<std::result::Result<(), SkipReason>> {
    match value {
        PropertyValue::Null => None,
        PropertyValue::DateTime(_) => Some(Ok(())),
        PropertyValue::String(text) => Some(match parse_millis(text) {
            Some(Ok(_)) => Ok(()),
            Some(Err(reason)) => Err(reason),
            None => Err(SkipReason::NotRfc3339),
        }),
        PropertyValue::Bool(_)
        | PropertyValue::I64(_)
        | PropertyValue::F64(_)
        | PropertyValue::F32(_)
        | PropertyValue::Bytes(_)
        | PropertyValue::I64Array(_)
        | PropertyValue::F64Array(_)
        | PropertyValue::F32Array(_)
        | PropertyValue::StringArray(_)
        | PropertyValue::Array(_)
        | PropertyValue::Object(_)
        | PropertyValue::DateTimeArray(_) => Some(Err(SkipReason::MixedTypes)),
    }
}

/// UTC epoch millis of an RFC3339 string; `Some(Err)` when the string has
/// sub-millisecond digits, `None` when it is not RFC3339.
fn parse_millis(text: &str) -> Option<std::result::Result<i64, SkipReason>> {
    let parsed = chrono::DateTime::parse_from_rfc3339(text).ok()?;
    Some(if parsed.timestamp_subsec_nanos() % 1_000_000 == 0 {
        Ok(parsed.timestamp_millis())
    } else {
        Err(SkipReason::SubMillisecond)
    })
}

fn observe(tracked: &mut BTreeMap<PropertyKey, Tracked>, key: PropertyKey, value: &PropertyValue) {
    let Some(outcome) = classify(value) else {
        return;
    };
    let is_string = matches!(value, PropertyValue::String(_));
    let entry = tracked.entry(key).or_insert(Tracked {
        verdict: Verdict::Datetimes,
        rfc3339_strings: 0,
    });
    if is_string && outcome.is_ok() {
        entry.rfc3339_strings = entry.rfc3339_strings.saturating_add(1);
    }
    entry.verdict = match (entry.verdict, outcome) {
        (skipped @ Verdict::Skipped(_), _) => skipped,
        (_, Err(reason)) => Verdict::Skipped(reason),
        (Verdict::Datetimes, Ok(())) if is_string => Verdict::Convertible,
        (verdict, Ok(())) => verdict,
    };
}

/// Rows of one element after `resume_after`, at most `limit` of them.
async fn read_rows(
    store: &slatedb::Db,
    scope: DataScope,
    element: Element,
    resume_after: Option<&[u8]>,
    tuning: MigrationTuning,
) -> Result<Vec<(Bytes, Bytes)>> {
    let prefix = Key::data_prefix(scope, Bytes::from_static(element.logical_prefix()));
    let resume_after = resume_after
        .map(|key| super::MigrationResumeKey::try_from(key.to_vec()))
        .transpose()
        .map_err(|err| HelixDbError::InvariantViolation(err.to_string()))?;
    let mut iter = store
        .scan(scan_bounds_for_prefix(&prefix, resume_after.as_ref()))
        .await?;
    let mut rows = Vec::with_capacity(tuning.batch_rows().get());
    let mut bytes = 0_usize;
    // At least one row per batch, however large, so the job always advances.
    while rows.len() < tuning.batch_rows().get()
        && (rows.is_empty() || bytes < tuning.batch_bytes().get())
    {
        let Some(kv) = iter.next().await? else {
            break;
        };
        bytes = bytes.saturating_add(kv.key.len() + kv.value.len());
        rows.push((kv.key, kv.value));
    }
    Ok(rows)
}

/// Id and properties of a node or by-id edge row; legacy pair rows are `None`.
fn decode_row(
    scope: DataScope,
    element: Element,
    key: &[u8],
    value: &[u8],
) -> Result<Option<(u64, Vec<Property>)>> {
    let logical = scope.strip_key(key).ok_or_else(|| {
        HelixDbError::InvariantViolation("scanned row outside its storage scope".to_string())
    })?;
    // `[prefix:1][id:8]`; a by-pair edge row is `[prefix:1][from:8][to:8]`.
    let Ok(id) = <[u8; ID_LEN]>::try_from(&logical[1..]) else {
        return match element {
            Element::Edge => Ok(None),
            Element::Node => Err(HelixDbError::InvariantViolation(
                "node property key has no eight-byte id".to_string(),
            )),
        };
    };
    Ok(Some((u64::from_be_bytes(id), decode_properties(value)?)))
}

/// The first tenant scope after `after`, by physical key order.
async fn next_tenant(store: &slatedb::Db, after: ScopeCursor) -> Result<Option<ScopeCursor>> {
    let mut start = vec![TENANT_KEY_PREFIX];
    match after {
        ScopeCursor::Unscoped => {}
        ScopeCursor::Tenant(tenant) => {
            let Some(next) = tenant.checked_add(1) else {
                return Ok(None);
            };
            start.extend_from_slice(&next.to_be_bytes());
        }
    }
    let end = Bytes::from_static(&[TENANT_KEY_PREFIX + 1]);
    let mut iter = store.scan(Bytes::from(start)..end).await?;
    let Some(kv) = iter.next().await? else {
        return Ok(None);
    };
    Ok(DataScope::strip_tenant_envelope(&kv.key)
        .map(|(tenant, _)| ScopeCursor::Tenant(tenant.as_u128())))
}

/// The job state after the current scope finishes.
async fn finish_scope(store: &slatedb::Db, scope: ScopeCursor, report: Report) -> Result<Job> {
    Ok(match next_tenant(store, scope).await? {
        Some(scope) => Job::Running {
            scope,
            phase: Phase::start(),
            report,
        },
        None => Job::Completed { report },
    })
}

async fn detect(
    store: &slatedb::Db,
    scope: ScopeCursor,
    element: Element,
    resume_after: Option<Vec<u8>>,
    properties: Vec<(PropertyKey, Tracked)>,
    mut report: Report,
    tuning: MigrationTuning,
) -> Result<Job> {
    let data_scope = scope.data_scope();
    let rows = read_rows(store, data_scope, element, resume_after.as_deref(), tuning).await?;
    let mut tracked = properties.into_iter().collect::<BTreeMap<_, _>>();
    for (key, value) in &rows {
        // A row that cannot decode is reported and left alone rather than
        // retried forever, which would stall every later scope.
        let row = match decode_row(data_scope, element, key, value) {
            Ok(Some((_, row))) => row,
            Ok(None) => continue,
            Err(error) => {
                tracing::warn!(scope = ?scope, %error, "datetime string migration skips an undecodable row");
                report.undecodable_rows = report.undecodable_rows.saturating_add(1);
                continue;
            }
        };
        let Some(label) = label_of(&row) else {
            continue;
        };
        for property in row
            .iter()
            .filter(|property| !property.name.starts_with('$'))
        {
            observe(
                &mut tracked,
                PropertyKey {
                    element,
                    label: label.to_owned(),
                    name: property.name.clone(),
                },
                &property.value,
            );
        }
    }
    if tracked.len() > MAX_TRACKED_PROPERTIES {
        tracing::warn!(
            scope = ?scope,
            tracked = tracked.len(),
            limit = MAX_TRACKED_PROPERTIES,
            "datetime string migration skips a scope with too many distinct properties"
        );
        report.skipped_scopes.push(scope);
        return finish_scope(store, scope, report).await;
    }
    if let Some((last, _)) = rows.last() {
        return Ok(Job::Running {
            scope,
            phase: Phase::Detect {
                element,
                resume_after: Some(last.to_vec()),
                properties: tracked.into_iter().collect(),
            },
            report,
        });
    }
    if element == Element::Node {
        return Ok(Job::Running {
            scope,
            phase: Phase::Detect {
                element: Element::Edge,
                resume_after: None,
                properties: tracked.into_iter().collect(),
            },
            report,
        });
    }
    let mut candidates = Vec::new();
    for (property, entry) in tracked {
        match entry.verdict {
            Verdict::Convertible => candidates.push(property),
            Verdict::Skipped(reason) if entry.rfc3339_strings > 0 => {
                tracing::warn!(
                    scope = ?scope,
                    element = ?property.element,
                    label = %property.label,
                    property = %property.name,
                    reason = ?reason,
                    rfc3339_strings = entry.rfc3339_strings,
                    "datetime string migration leaves a property with RFC3339 strings as stored"
                );
                report.skip(SkippedProperty {
                    scope,
                    property,
                    reason,
                    rfc3339_strings: entry.rfc3339_strings,
                });
            }
            Verdict::Datetimes | Verdict::Skipped(_) => {}
        }
    }
    tracing::info!(
        scope = ?scope,
        candidates = candidates.len(),
        "datetime string migration detected convertible properties"
    );
    if candidates.is_empty() {
        return finish_scope(store, scope, report).await;
    }
    Ok(Job::Running {
        scope,
        phase: Phase::Convert {
            element: Element::Node,
            resume_after: None,
            candidates,
        },
        report,
    })
}

/// One row's conversions: `(property, stored string, datetime millis)`.
struct RowConversion {
    id: u64,
    values: Vec<(String, String, i64)>,
}

fn conversion_request(element: Element, rows: &[RowConversion]) -> QueryRequest {
    let names = (0..rows.len())
        .map(|index| format!("r{index}"))
        .collect::<Vec<_>>();
    let batch = rows
        .iter()
        .zip(&names)
        .fold(write_batch(), |batch, (row, name)| {
            let guard = Predicate::and(
                row.values
                    .iter()
                    .map(|(property, stored, _)| Predicate::eq(property.as_str(), stored.as_str()))
                    .collect(),
            );
            let mut sets = row.values.iter().map(|(property, _, millis)| {
                (
                    property.clone(),
                    PropertyInput::from(DateTime::from_millis(*millis)),
                )
            });
            let (property, value) = sets
                .next()
                .expect("a conversion row has at least one value");
            match element {
                Element::Node => batch.var_as(
                    name.as_str(),
                    sets.fold(
                        g().n(NodeRef::id(row.id))
                            .where_(guard)
                            .set_property(property, value),
                        |traversal, (property, value)| traversal.set_property(property, value),
                    )
                    .count(),
                ),
                Element::Edge => batch.var_as(
                    name.as_str(),
                    sets.fold(
                        g().e(EdgeRef::id(row.id))
                            .where_(guard)
                            .set_property(property, value),
                        |traversal, (property, value)| traversal.set_property(property, value),
                    )
                    .count(),
                ),
            }
        });
    QueryRequest::write(batch.returning(names))
}

async fn execute(
    db: &HelixDB,
    scope: DataScope,
    request: QueryRequest,
) -> Result<serde_json::Value> {
    crate::query_service::execute_query_on_scoped_observed::<serde_json::Value>(
        db,
        request,
        // Conversions and progress rows share one ordered WAL, so a progress
        // row never becomes durable without the conversions before it, and a
        // lost tail only replays batches whose guards no longer match.
        crate::query_service::QueryMode::ExecuteWithoutDurableWait,
        scope,
        Default::default(),
        None,
        crate::execution_control::ExecutionControl::unlimited(),
    )
    .await
    .map(crate::query_service::QueryResponse::into_value)
    .map_err(HelixDbError::from)
}

/// Values rewritten by a conversion response: each row's string count when
/// its guarded traversal matched (counted 1, not 0).
fn converted_values(response: &serde_json::Value, rows: &[RowConversion]) -> u64 {
    rows.iter()
        .enumerate()
        .filter(|(index, _)| {
            response[format!("r{index}")]
                .as_u64()
                .is_some_and(|matched| matched > 0)
        })
        .map(|(_, row)| row.values.len() as u64)
        .sum()
}

#[allow(
    clippy::too_many_arguments,
    reason = "one phase step owns every piece of its durable state"
)]
async fn convert(
    db: &HelixDB,
    store: &slatedb::Db,
    scope: ScopeCursor,
    element: Element,
    resume_after: Option<Vec<u8>>,
    candidates: Vec<PropertyKey>,
    mut report: Report,
    tuning: MigrationTuning,
) -> Result<Job> {
    let data_scope = scope.data_scope();
    let rows = read_rows(store, data_scope, element, resume_after.as_deref(), tuning).await?;
    let Some((last, _)) = rows.last() else {
        if element == Element::Node {
            return Ok(Job::Running {
                scope,
                phase: Phase::Convert {
                    element: Element::Edge,
                    resume_after: None,
                    candidates,
                },
                report,
            });
        }
        return finish_scope(store, scope, report).await;
    };
    let last = last.to_vec();
    let lookup = candidates.iter().collect::<BTreeSet<_>>();
    let mut conversions = Vec::new();
    for (key, value) in &rows {
        // Detect already reported undecodable rows.
        let Ok(Some((id, row))) = decode_row(data_scope, element, key, value) else {
            continue;
        };
        let Some(label) = label_of(&row) else {
            continue;
        };
        let values = row
            .iter()
            .filter_map(|property| {
                let PropertyValue::String(stored) = &property.value else {
                    return None;
                };
                let key = PropertyKey {
                    element,
                    label: label.to_owned(),
                    name: property.name.clone(),
                };
                if !lookup.contains(&key) {
                    return None;
                }
                let Some(Ok(millis)) = parse_millis(stored) else {
                    return None;
                };
                Some((property.name.clone(), stored.clone(), millis))
            })
            .collect::<Vec<_>>();
        if !values.is_empty() {
            conversions.push(RowConversion { id, values });
        }
    }
    if !conversions.is_empty() {
        match execute(db, data_scope, conversion_request(element, &conversions)).await {
            Ok(response) => {
                report.converted_values = report
                    .converted_values
                    .saturating_add(converted_values(&response, &conversions));
            }
            // One collision fails the whole batch: retry row by row so only the
            // colliding rows keep their strings.
            Err(HelixDbError::UniqueConstraintViolation { .. }) => {
                for row in conversions {
                    let single = std::slice::from_ref(&row);
                    match execute(db, data_scope, conversion_request(element, single)).await {
                        Ok(response) => {
                            report.converted_values = report
                                .converted_values
                                .saturating_add(converted_values(&response, single));
                        }
                        Err(HelixDbError::UniqueConstraintViolation { .. }) => {
                            report.unique_conflicts = report.unique_conflicts.saturating_add(1);
                        }
                        Err(error) => return Err(error),
                    }
                }
            }
            Err(error) => return Err(error),
        }
    }
    Ok(Job::Running {
        scope,
        phase: Phase::Convert {
            element,
            resume_after: Some(last),
            candidates,
        },
        report,
    })
}

fn job_key() -> Bytes {
    scoped_metadata_key(DataScope::LegacyUnscoped, JOB_KEY)
}

/// The stored job, or `None` before its first batch.
pub(crate) async fn load(store: &slatedb::Db) -> Result<Option<Job>> {
    store
        .get(job_key())
        .await?
        .map(|value| decode_json::<Job>(&value))
        .transpose()
}

/// Advances the job by one batch and commits its progress.
///
/// Returns `false` once the job has completed, or on a reader handle.
pub(crate) async fn process_once(db: &HelixDB, tuning: MigrationTuning) -> Result<bool> {
    let HelixStorage::Writer(writer) = db.storage() else {
        return Ok(false);
    };
    let store = writer.db();
    let started = Instant::now();
    let Job::Running {
        scope,
        phase,
        report,
    } = load(store).await?.unwrap_or_else(Job::start)
    else {
        return Ok(false);
    };
    let next = match phase {
        Phase::Detect {
            element,
            resume_after,
            properties,
        } => {
            detect(
                store,
                scope,
                element,
                resume_after,
                properties,
                report,
                tuning,
            )
            .await?
        }
        Phase::Convert {
            element,
            resume_after,
            candidates,
        } => {
            convert(
                db,
                store,
                scope,
                element,
                resume_after,
                candidates,
                report,
                tuning,
            )
            .await?
        }
    };
    // Batches do not wait for the WAL upload (see `execute`); completion does,
    // so a finished job stays finished.
    let durability = if next.is_completed() {
        crate::transaction::CommitDurability::Durable
    } else {
        crate::transaction::CommitDurability::Applied
    };
    let transaction = store.begin(slatedb::IsolationLevel::Snapshot).await?;
    crate::transaction::Mutation::put(&transaction, job_key(), encode_json(&next)?)?;
    transaction
        .commit_with_options(&durability.write_options())
        .await?;
    match &next {
        Job::Completed { report } => tracing::info!(
            converted_values = report.converted_values,
            unique_conflicts = report.unique_conflicts,
            skipped_properties =
                report.skipped_properties.len() as u64 + report.unlisted_skipped_properties,
            skipped_scopes = report.skipped_scopes.len(),
            "datetime string migration completed"
        ),
        Job::Running { scope, report, .. } => tracing::debug!(
            scope = ?scope,
            converted_values = report.converted_values,
            duration_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
            "datetime string migration batch committed"
        ),
    }
    Ok(true)
}

#[cfg(test)]
#[path = "datetime_strings_tests.rs"]
mod tests;
