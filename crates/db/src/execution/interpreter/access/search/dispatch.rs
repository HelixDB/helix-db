//! Vector and text search access dispatch contracts.

use std::sync::Arc;

use helix_planner::ir;

use super::limits::SearchReadLimit;
use super::tenant::{validate_text_search_tenant, validate_vector_search_tenant};
use super::*;
use crate::config::{TextElementType, VectorElementType};
use crate::encoding::v2::values::indexes::vector::{ActiveScoreSemantic, VectorEntityKind};
use crate::search::text::{RestrictedTextCandidates, TextSearchScope};
use crate::search::vector::distance::{Cosine, Euclidean, Manhattan};
use crate::search::vector::RestrictedVectorCandidates;
use crate::search::vector::{TypedVectorSearchResult, VectorDistanceMetric};

pub(in crate::execution::interpreter::access) struct RestrictedVectorSearchRead<'a> {
    limit: SearchReadLimit<'a>,
    candidates: &'a RestrictedVectorCandidates,
}

impl<'a> RestrictedVectorSearchRead<'a> {
    pub(in crate::execution::interpreter::access) const fn new(
        limit: SearchReadLimit<'a>,
        candidates: &'a RestrictedVectorCandidates,
    ) -> Self {
        Self { limit, candidates }
    }
}

enum VectorSearchScope<'a> {
    Unrestricted(SearchReadLimit<'a>),
    Restricted(RestrictedVectorSearchRead<'a>),
}

pub(in crate::execution::interpreter::access) struct RestrictedTextSearchRead<'a> {
    limit: SearchReadLimit<'a>,
    candidates: Arc<RestrictedTextCandidates>,
}

impl<'a> RestrictedTextSearchRead<'a> {
    pub(in crate::execution::interpreter::access) const fn new(
        limit: SearchReadLimit<'a>,
        candidates: Arc<RestrictedTextCandidates>,
    ) -> Self {
        Self { limit, candidates }
    }
}

struct TextSearchAccess<'a> {
    element_type: TextElementType,
    label: &'a ir::NonEmptyString,
    property: &'a ir::NonEmptyString,
    index: &'a ir::SearchIndexPlan,
    query_text: &'a ir::TextQueryInputPlan,
}

impl<'db> ExecutionContext<'db> {
    pub(in crate::execution::interpreter) async fn vector_search_results(
        &self,
        element_type: VectorElementType,
        label: &ir::NonEmptyString,
        property: &ir::NonEmptyString,
        index: &ir::SearchIndexPlan,
        query_vector: &ir::VectorQueryInputPlan,
        limit: SearchReadLimit<'_>,
    ) -> Result<Vec<TypedVectorSearchResult>> {
        Box::pin(self.vector_search_results_with_candidates(
            element_type,
            label,
            property,
            index,
            query_vector,
            VectorSearchScope::Unrestricted(limit),
        ))
        .await
    }

    pub(in crate::execution::interpreter::access) async fn restricted_vector_search_results(
        &self,
        element_type: VectorElementType,
        label: &ir::NonEmptyString,
        property: &ir::NonEmptyString,
        index: &ir::SearchIndexPlan,
        query_vector: &ir::VectorQueryInputPlan,
        read: RestrictedVectorSearchRead<'_>,
    ) -> Result<Vec<TypedVectorSearchResult>> {
        Box::pin(self.vector_search_results_with_candidates(
            element_type,
            label,
            property,
            index,
            query_vector,
            VectorSearchScope::Restricted(read),
        ))
        .await
    }

    /// Callers box this future: the queued-state overlay makes it large, and
    /// boxing keeps it out of every frame of recursive callers such as count
    /// cursors.
    async fn vector_search_results_with_candidates(
        &self,
        element_type: VectorElementType,
        label: &ir::NonEmptyString,
        property: &ir::NonEmptyString,
        index: &ir::SearchIndexPlan,
        query_vector: &ir::VectorQueryInputPlan,
        scope: VectorSearchScope<'_>,
    ) -> Result<Vec<TypedVectorSearchResult>> {
        if let Some(reason) = self
            .db
            .index_lifecycle_unavailable_reason(crate::error::IndexFamily::Vector)
        {
            return Err(HelixDbError::IndexLifecycleUnavailable {
                family: crate::error::IndexFamily::Vector,
                reason,
            });
        }
        let definition = self.vector_definition(element_type, label, property)?;
        let tenant_value = self.search_tenant_value(&index.tenant).await?;
        validate_vector_search_tenant(&definition, &index.tenant, tenant_value.as_ref())?;
        let query = self.search_query_vector(query_vector).await?;
        // Restricted search has its own reject-based result-count ceiling,
        // computed against the traversal's exact candidate count rather than
        // the raw request. Applying the unrestricted path's blanket
        // default-ceiling clamp ahead of that would silently truncate a request
        // the restricted path is meant to reject outright. It is checked here,
        // before pending overlays shrink the physical candidate set, so pending
        // work never lifts it. See `effective_restricted_vector_search_limit`.
        let (k, candidates) = match scope {
            VectorSearchScope::Unrestricted(limit) => {
                (self.effective_search_limit(limit).await?, None)
            }
            VectorSearchScope::Restricted(read) => {
                let k = self
                    .effective_restricted_vector_search_limit(read.limit)
                    .await?;
                read.candidates.validate_result_count(k)?;
                (k, Some(read.candidates))
            }
        };

        let pending = self
            .pending_selection(
                &crate::index_lifecycle::ValidatedVectorIndexDefinition::try_from_runtime(
                    &definition,
                )?
                .identity(),
                crate::encoding::v2::values::indexes::operation_queue::QueueFamily::Vector,
            )
            .await?;
        let raw_results = match definition.metric() {
            VectorDistanceMetric::Cosine => {
                self.overlaid_vector_results::<Cosine>(
                    &definition,
                    tenant_value.as_ref(),
                    &query,
                    k,
                    candidates,
                    pending,
                )
                .await
            }
            VectorDistanceMetric::Euclidean => {
                self.overlaid_vector_results::<Euclidean>(
                    &definition,
                    tenant_value.as_ref(),
                    &query,
                    k,
                    candidates,
                    pending,
                )
                .await
            }
            VectorDistanceMetric::Manhattan => {
                self.overlaid_vector_results::<Manhattan>(
                    &definition,
                    tenant_value.as_ref(),
                    &query,
                    k,
                    candidates,
                    pending,
                )
                .await
            }
        }?;
        let entity_kind = match element_type {
            VectorElementType::Node => VectorEntityKind::Node,
            VectorElementType::Edge => VectorEntityKind::Edge,
        };
        let score_semantic = match definition.metric() {
            VectorDistanceMetric::Cosine => ActiveScoreSemantic::CosineHalfF32V1,
            VectorDistanceMetric::Euclidean => ActiveScoreSemantic::SquaredEuclideanF32V1,
            VectorDistanceMetric::Manhattan => ActiveScoreSemantic::ManhattanF32V1,
        };
        Ok(raw_results
            .into_iter()
            .map(|result| {
                TypedVectorSearchResult::from_physical(entity_kind, score_semantic, result)
            })
            .collect())
    }

    /// Merges physical ANN results with exact scores of selected pending
    /// vectors in the searched partition.
    ///
    /// Physical rows of selected pending entities are superseded. A
    /// traversal-restricted search removes them from its allowed set, so it
    /// runs once at `k` within the restricted result cap, or not at all when
    /// no candidate remains. An unrestricted search suppresses them, so they
    /// still route the ANN traversal, and widens past them with
    /// [`settle_physical`]; a search past its suppression limit fails or
    /// reruns with a smaller selection, as
    /// [`PendingSelection::yield_to_suppression_limit`] decides.
    /// Pending vectors pass the same traversal restriction, tenant partition,
    /// and metric as physical rows, and both sources merge by
    /// `(distance, entity id)`.
    ///
    /// [`PendingSelection::yield_to_suppression_limit`]: super::pending::PendingSelection::yield_to_suppression_limit
    #[allow(
        clippy::too_many_arguments,
        reason = "one overlay binds the definition, tenant, query, limit, restriction, and selection"
    )]
    async fn overlaid_vector_results<D: crate::search::vector::Distance>(
        &self,
        definition: &crate::config::VectorIndexDefinition,
        tenant_value: Option<&crate::encoding::property::property_value::PropertyValue>,
        query: &[f32],
        k: usize,
        candidates: Option<&RestrictedVectorCandidates>,
        pending: Option<super::pending::PendingSelection>,
    ) -> Result<Vec<crate::search::vector::SearchResult>> {
        let generation = self
            .managed_vector_generation::<D>(definition, tenant_value)
            .await?;
        let Some(mut pending) = pending else {
            return match candidates {
                Some(candidates) => {
                    self.search_vector_index_restricted::<D>(
                        query,
                        k,
                        generation.as_ref(),
                        candidates,
                    )
                    .await
                }
                None => {
                    self.search_vector_index::<D>(query, k, generation.as_ref())
                        .await
                }
            };
        };
        loop {
            let mut pending_scored =
                score_pending_vectors::<D>(definition, tenant_value, query, candidates, &pending)?;
            let physical = match generation.as_ref() {
                super::generation::VectorSearchAuthority::AbsentManagedPartition => None,
                super::generation::VectorSearchAuthority::Managed(handle) => Some((
                    handle,
                    candidates.map(|candidates| candidates.without(&pending.superseded)),
                )),
            };
            let mut results = match physical {
                // An absent partition has no physical rows, and no physical row
                // can rank once every candidate is superseded, so neither runs
                // a physical search.
                None | Some((_, Some(RestrictedVectorCandidates::Empty))) => Vec::new(),
                Some((handle, unsuperseded)) => {
                    let physical_candidates = unsuperseded.as_ref();
                    let settlement = settle_physical(
                        k,
                        &pending.superseded,
                        pending.local(),
                        |result: &crate::search::vector::SearchResult| result.entity_id(),
                        |farthest| {
                            pending_scored
                                .iter()
                                .filter(|pending| pending.score() < farthest.score())
                                .count()
                        },
                        move |request| async move {
                            let authority =
                                super::generation::VectorSearchAuthority::Managed(handle);
                            match physical_candidates {
                                Some(candidates) => {
                                    self.search_vector_index_restricted::<D>(
                                        query, request, authority, candidates,
                                    )
                                    .await
                                }
                                None => {
                                    self.search_vector_index::<D>(query, request, authority)
                                        .await
                                }
                            }
                        },
                    )
                    .await?;
                    match settlement {
                        Settlement::Settled(kept) => kept,
                        Settlement::Exceeded { skipped } => {
                            pending.yield_to_suppression_limit(skipped)?;
                            continue;
                        }
                    }
                }
            };
            results.append(&mut pending_scored);
            results.sort_by(|left, right| {
                left.score()
                    .cmp(&right.score())
                    .then_with(|| left.entity_id().cmp(&right.entity_id()))
            });
            results.truncate(k);
            return Ok(results);
        }
    }

    pub(in crate::execution::interpreter) async fn text_search_hits(
        &self,
        element_type: TextElementType,
        label: &ir::NonEmptyString,
        property: &ir::NonEmptyString,
        index: &ir::SearchIndexPlan,
        query_text: &ir::TextQueryInputPlan,
        limit: SearchReadLimit<'_>,
    ) -> Result<Vec<crate::search::text::TextSearchHit>> {
        let access = TextSearchAccess {
            element_type,
            label,
            property,
            index,
            query_text,
        };
        Box::pin(self.text_search_hits_with_scope(&access, limit, TextSearchScope::Unrestricted))
            .await
    }

    pub(in crate::execution::interpreter::access) async fn restricted_text_search_hits(
        &self,
        element_type: TextElementType,
        label: &ir::NonEmptyString,
        property: &ir::NonEmptyString,
        index: &ir::SearchIndexPlan,
        query_text: &ir::TextQueryInputPlan,
        read: RestrictedTextSearchRead<'_>,
    ) -> Result<Vec<crate::search::text::TextSearchHit>> {
        let access = TextSearchAccess {
            element_type,
            label,
            property,
            index,
            query_text,
        };
        Box::pin(self.text_search_hits_with_scope(
            &access,
            read.limit,
            TextSearchScope::restricted(read.candidates),
        ))
        .await
    }

    /// Callers box this future for the same reason as
    /// [`Self::vector_search_results_with_candidates`].
    async fn text_search_hits_with_scope(
        &self,
        access: &TextSearchAccess<'_>,
        limit: SearchReadLimit<'_>,
        scope: TextSearchScope,
    ) -> Result<Vec<crate::search::text::TextSearchHit>> {
        if scope.is_empty_restricted() {
            return Ok(Vec::new());
        }
        let definition =
            self.text_definition(access.element_type, access.label, access.property)?;
        let tenant_value = self.search_tenant_value(&access.index.tenant).await?;
        validate_text_search_tenant(&definition, &access.index.tenant, tenant_value.as_ref())?;
        let query = self.search_query_text(access.query_text).await?;
        let k = self.effective_search_limit(limit).await?;

        let generation = self
            .managed_text_generation(&definition, tenant_value.as_ref())
            .await?;
        let pending = self
            .pending_selection(
                &crate::index_lifecycle::ValidatedTextIndexDefinition::try_from_runtime(
                    &definition,
                )?
                .identity(),
                crate::encoding::v2::values::indexes::operation_queue::QueueFamily::Text,
            )
            .await?;
        let Some(mut pending) = pending else {
            let Some(manifest) = self.load_text_manifest_root(generation.as_ref()).await? else {
                return Ok(Vec::new());
            };
            return self
                .search_text_manifest_with_scope(&manifest, &query, k, scope)
                .await;
        };
        // One logical search records one use of its splits, however often it
        // widens or reruns with a smaller selection.
        let mut demand = crate::search::text::SplitDemand::Record;
        loop {
            match self
                .overlaid_text_hits(
                    &definition,
                    generation.as_ref(),
                    &pending,
                    &query,
                    k,
                    &scope,
                    &mut demand,
                )
                .await?
            {
                Settlement::Settled(hits) => return Ok(hits),
                Settlement::Exceeded { skipped } => pending.yield_to_suppression_limit(skipped)?,
            }
        }
    }

    /// Merges persisted split hits with pending documents under one
    /// consistent set of BM25 statistics.
    ///
    /// Selected pending entities supersede their physical documents: their
    /// persisted contributions leave the corpus statistics, and their latest
    /// documents in the searched partition are indexed in memory with the
    /// production analyzer and scored against the same statistics. A
    /// traversal-restricted search removes superseded entities from its
    /// candidate set and skips its physical search when none remain. An
    /// unrestricted search suppresses their split hits and widens past them
    /// with [`settle_physical`], reporting a search past its suppression
    /// limit to the caller, which fails or reruns it with a smaller
    /// selection. Only the first physical search that `demand` allows
    /// records a use of its splits.
    #[allow(
        clippy::too_many_arguments,
        reason = "one overlaid attempt binds its definition, generation, selection, query, and demand"
    )]
    async fn overlaid_text_hits(
        &self,
        definition: &crate::config::TextIndexDefinition,
        generation: super::generation::TextSearchAuthority<
            &super::generation::ResolvedTextGenerationHandle,
        >,
        pending: &super::pending::PendingSelection,
        query: &str,
        k: usize,
        scope: &TextSearchScope,
        demand: &mut crate::search::text::SplitDemand,
    ) -> Result<Settlement<crate::search::text::TextSearchHit>> {
        let super::generation::TextSearchAuthority::Managed(handle) = generation else {
            return Ok(Settlement::Settled(Vec::new()));
        };
        let partition = handle.partition();
        let authority = handle.physical();
        let overlay = pending
            .entities
            .iter()
            .map(|pending| {
                let text = match &pending.latest {
                    Some((pending_partition, super::pending::PendingValue::Text(text)))
                        if pending_partition == partition =>
                    {
                        Some(&**text)
                    }
                    Some(_) | None => None,
                };
                (pending.entity, text)
            })
            .collect::<Vec<_>>();
        let statistics = if let Some(active) = self.active_write_tx() {
            crate::index_lifecycle::text::statistics::load_overlaid_query_statistics(
                &active.txn,
                authority.scope(),
                authority.index_id(),
                authority.generation(),
                partition,
                definition.analyzer(),
                query,
                &overlay,
            )
            .await?
        } else if let Some(view) = self.request_read_view() {
            crate::index_lifecycle::text::statistics::load_overlaid_query_statistics(
                view,
                authority.scope(),
                authority.index_id(),
                authority.generation(),
                partition,
                definition.analyzer(),
                query,
                &overlay,
            )
            .await?
        } else {
            return Err(HelixDbError::InvariantViolation(
                "text overlay escaped its request view".to_string(),
            ));
        };
        let crate::index_lifecycle::text::statistics::LoadedTextQueryStatistics::Ready(statistics) =
            statistics
        else {
            return Ok(Settlement::Settled(Vec::new()));
        };
        let documents = overlay
            .iter()
            .filter_map(|(entity, text)| {
                let text = (*text)?;
                let id = entity.id.get();
                scope
                    .candidates()
                    .is_none_or(|candidates| candidates.contains(id))
                    .then(|| (id, std::sync::Arc::<str>::from(text)))
            })
            .collect::<Vec<_>>();
        let pending_hits = {
            let definition = definition.clone();
            let query = query.to_string();
            let statistics = statistics.clone();
            let scope = scope.clone();
            tokio::task::spawn_blocking(move || {
                crate::search::text::search_pending_documents(
                    &definition,
                    &documents,
                    &query,
                    k,
                    &statistics,
                    &scope,
                )
            })
            .await
            .map_err(|error| {
                HelixDbError::InvariantViolation(format!(
                    "pending text search task failed: {error}"
                ))
            })??
        };
        let physical_scope = match scope.candidates() {
            Some(candidates) => {
                TextSearchScope::restricted(Arc::new(candidates.without(&pending.superseded)))
            }
            None => TextSearchScope::Unrestricted,
        };
        // No physical document can rank once every candidate is superseded,
        // so the search reads neither the manifest nor its splits.
        let manifest = if physical_scope.is_empty_restricted() {
            None
        } else {
            self.load_text_manifest_root(super::generation::TextSearchAuthority::Managed(handle))
                .await?
        };
        let settlement = match manifest {
            None => Settlement::Settled(Vec::new()),
            Some(manifest) => {
                let (manifest, statistics, physical_scope) =
                    (&manifest, &statistics, &physical_scope);
                settle_physical(
                    k,
                    &pending.superseded,
                    pending.local(),
                    |hit: &crate::search::text::TextSearchHit| hit.entity_id,
                    |lowest| {
                        pending_hits
                            .iter()
                            .filter(|hit| hit.score > lowest.score)
                            .count()
                    },
                    move |request| {
                        let scope = physical_scope.clone();
                        let demand =
                            std::mem::replace(demand, crate::search::text::SplitDemand::Skip);
                        async move {
                            self.search_text_manifest_with_statistics(
                                manifest,
                                query,
                                request,
                                scope,
                                Some(statistics),
                                demand,
                            )
                            .await
                        }
                    },
                )
                .await?
            }
        };
        let mut hits = match settlement {
            Settlement::Settled(hits) => hits,
            exceeded @ Settlement::Exceeded { .. } => return Ok(exceeded),
        };
        hits.extend(pending_hits);
        hits.sort_by(|left, right| {
            right
                .score
                .total_cmp(&left.score)
                .then_with(|| left.entity_id.cmp(&right.entity_id))
        });
        hits.truncate(k);
        Ok(Settlement::Settled(hits))
    }
}

/// Results of one overlaid search attempt.
#[derive(Debug)]
enum Settlement<T> {
    /// Results settled within the suppression limit.
    Settled(Vec<T>),
    /// Settling needs more than [`MAX_SUPPRESSED_SEARCH_RESULTS`] physical
    /// results superseded by committed work skipped; the last attempt skipped
    /// `skipped` of those.
    ///
    /// [`MAX_SUPPRESSED_SEARCH_RESULTS`]: super::limits::MAX_SUPPRESSED_SEARCH_RESULTS
    Exceeded { skipped: usize },
}

/// Widens a physical search past superseded results until `k` are settled.
///
/// `search(request)` returns up to `request` physical results in rank order.
/// It is a closure returning a future rather than an async closure, whose
/// borrow-generic future would stop the calling request future from being
/// provably `Send`.
/// Settled results are the kept physical results, whose entity is not in
/// `superseded`, plus the pending results `pending_ahead` counts as ranking
/// strictly ahead of the last physical result examined; an exhausted source
/// settles whatever it returned. `local`, a subset of `superseded`, holds the
/// entities the searching write transaction changed itself. Each attempt
/// grows the request by the superseded results it skipped, up to `k` plus
/// every local entity plus at most [`MAX_SUPPRESSED_SEARCH_RESULTS`] others:
/// the widening grows with the transaction's own changes, which it bounds
/// itself, but not with the committed backlog. A request at that bound keeps
/// at least `k` results whenever at most that many committed entities are
/// superseded, so only a larger committed selection can exceed it, and the
/// committed results the last attempt skipped then exceed the limit.
///
/// [`MAX_SUPPRESSED_SEARCH_RESULTS`]: super::limits::MAX_SUPPRESSED_SEARCH_RESULTS
async fn settle_physical<T, Search>(
    k: usize,
    superseded: &roaring::RoaringTreemap,
    local: Option<&roaring::RoaringTreemap>,
    entity_id: impl Fn(&T) -> u64,
    pending_ahead: impl Fn(&T) -> usize,
    mut search: impl FnMut(usize) -> Search,
) -> Result<Settlement<T>>
where
    Search: std::future::Future<Output = Result<Vec<T>>>,
{
    debug_assert!(
        local.is_none_or(|local| local.is_subset(superseded)),
        "local changes supersede their physical results"
    );
    let local_count = local.map_or(0, roaring::RoaringTreemap::len);
    let bound = k
        .saturating_add(usize::try_from(local_count).unwrap_or(usize::MAX))
        .saturating_add(
            usize::try_from(superseded.len() - local_count)
                .unwrap_or(usize::MAX)
                .min(super::limits::MAX_SUPPRESSED_SEARCH_RESULTS),
        );
    let mut request = k;
    loop {
        let found = search(request).await?;
        let returned = found.len();
        let pending_settled = found.last().map_or(0, &pending_ahead);
        let (kept, skipped): (Vec<_>, Vec<_>) = found
            .into_iter()
            .partition(|result| !superseded.contains(entity_id(result)));
        if returned < request || kept.len() + pending_settled >= k {
            return Ok(Settlement::Settled(kept));
        }
        if request >= bound {
            return Ok(Settlement::Exceeded {
                skipped: skipped
                    .iter()
                    .filter(|result| local.is_none_or(|local| !local.contains(entity_id(result))))
                    .count(),
            });
        }
        request = request.saturating_add(skipped.len()).min(bound);
    }
}

/// Scores selected pending vectors in the searched partition that pass the
/// traversal restriction.
fn score_pending_vectors<D: crate::search::vector::Distance>(
    definition: &crate::config::VectorIndexDefinition,
    tenant_value: Option<&crate::encoding::property::property_value::PropertyValue>,
    query: &[f32],
    candidates: Option<&RestrictedVectorCandidates>,
    pending: &super::pending::PendingSelection,
) -> Result<Vec<crate::search::vector::SearchResult>> {
    let partition = match (definition.tenant_property(), tenant_value) {
        (None, _) => Some(crate::index_lifecycle::work::TextPartition::Unpartitioned),
        (Some(_), Some(value)) => crate::search::text::normalize_tenant_value(value)
            .map(|value| {
                crate::index_lifecycle::work::TextPartition::try_tenant_value(
                    crate::encoding::v2::values::property::encode_index_partition_value(value),
                )
                .map_err(|error| HelixDbError::IndexCatalogCorruption(error.to_string()))
            })
            .transpose()?,
        (Some(_), None) => None,
    };
    let Some(partition) = partition else {
        return Ok(Vec::new());
    };
    let dimension = crate::search::vector::VectorDimension::try_new(definition.dimension())
        .map_err(|error| HelixDbError::InvariantViolation(error.to_string()))?;
    crate::search::vector::score_exact_in_memory::<D>(
        query,
        dimension,
        pending.entities.iter().filter_map(|pending| {
            let Some((pending_partition, super::pending::PendingValue::Vector(vector))) =
                &pending.latest
            else {
                return None;
            };
            let id = pending.entity.id.get();
            (*pending_partition == partition
                && candidates.is_none_or(|candidates| candidates.contains(id)))
            .then_some((id, &vector[..]))
        }),
    )
}

#[cfg(test)]
mod tests {
    use roaring::RoaringTreemap;

    use super::*;

    /// Settles `k` results over the physical results `1..=physical`, in rank
    /// order, with the `local` subset of `superseded` changed by the searching
    /// transaction, and returns each request made.
    async fn settle(
        k: usize,
        physical: u64,
        superseded: &RoaringTreemap,
        local: Option<&RoaringTreemap>,
        pending_ahead: usize,
    ) -> (Settlement<u64>, Vec<usize>) {
        let mut requests = Vec::new();
        let settlement = settle_physical(
            k,
            superseded,
            local,
            |id: &u64| *id,
            |_| pending_ahead,
            |request| {
                requests.push(request);
                let found = (1..=physical).take(request).collect::<Vec<_>>();
                async move { Ok(found) }
            },
        )
        .await
        .unwrap();
        (settlement, requests)
    }

    #[tokio::test]
    async fn settling_grows_the_request_by_the_superseded_results_skipped() {
        let (settlement, requests) =
            settle(2, 10, &RoaringTreemap::from_iter([1, 2, 3]), None, 0).await;
        assert!(
            matches!(&settlement, Settlement::Settled(kept) if *kept == [4, 5]),
            "{settlement:?}"
        );
        assert_eq!(requests, [2, 4, 5], "capped at k plus the superseded count");
    }

    #[tokio::test]
    async fn pending_results_ahead_of_the_last_physical_result_settle_it() {
        let superseded = (1..=900).collect::<RoaringTreemap>();
        let (settlement, requests) = settle(1, 1_000, &superseded, None, 1).await;
        assert!(
            matches!(&settlement, Settlement::Settled(kept) if kept.is_empty()),
            "{settlement:?}"
        );
        assert_eq!(requests, [1]);
    }

    #[tokio::test]
    async fn an_exhausted_source_settles_what_it_returned() {
        let (settlement, requests) = settle(5, 3, &RoaringTreemap::from_iter([1]), None, 0).await;
        assert!(
            matches!(&settlement, Settlement::Settled(kept) if *kept == [2, 3]),
            "{settlement:?}"
        );
        assert_eq!(requests, [5]);
    }

    #[tokio::test]
    async fn settling_exceeds_only_past_the_suppression_limit() {
        let limit = super::super::limits::MAX_SUPPRESSED_SEARCH_RESULTS;
        let bound = limit + 1;
        // Exactly the limit of superseded results ahead of the answer settles.
        let superseded = (1..=limit as u64).collect::<RoaringTreemap>();
        let (settlement, requests) = settle(1, 1_000, &superseded, None, 0).await;
        assert!(
            matches!(&settlement, Settlement::Settled(kept) if *kept == [bound as u64]),
            "{settlement:?}"
        );
        assert_eq!(requests.last(), Some(&bound));
        // One more is reported with the results skipped, never widened past.
        let superseded = (1..=bound as u64).collect::<RoaringTreemap>();
        let (settlement, requests) = settle(1, 1_000, &superseded, None, 0).await;
        assert!(
            matches!(settlement, Settlement::Exceeded { skipped } if skipped == bound),
            "{settlement:?}"
        );
        assert!(requests.iter().all(|request| *request <= bound));
    }

    #[tokio::test]
    async fn local_changes_widen_the_search_without_counting_toward_the_limit() {
        let limit = super::super::limits::MAX_SUPPRESSED_SEARCH_RESULTS as u64;
        // Every superseded result is the transaction's own: the search widens
        // past all of them, far beyond the limit.
        let local = (1..=limit + 100).collect::<RoaringTreemap>();
        let (settlement, requests) = settle(1, 2_000, &local, Some(&local), 0).await;
        assert!(
            matches!(&settlement, Settlement::Settled(kept) if *kept == [limit + 101]),
            "{settlement:?}"
        );
        assert_eq!(requests.last(), Some(&(limit as usize + 101)));

        // Local changes ahead of exactly the limit of committed ones settle.
        let local = (1..=100).collect::<RoaringTreemap>();
        let superseded = (1..=limit + 100).collect::<RoaringTreemap>();
        let (settlement, _) = settle(1, 2_000, &superseded, Some(&local), 0).await;
        assert!(
            matches!(&settlement, Settlement::Settled(kept) if *kept == [limit + 101]),
            "{settlement:?}"
        );

        // One more committed one exceeds, and only committed results count.
        let superseded = (1..=limit + 101).collect::<RoaringTreemap>();
        let (settlement, requests) = settle(1, 2_000, &superseded, Some(&local), 0).await;
        assert!(
            matches!(settlement, Settlement::Exceeded { skipped } if skipped == limit as usize + 1),
            "{settlement:?}"
        );
        assert!(requests
            .iter()
            .all(|request| *request <= limit as usize + 101));

        // A strong read selection has no local changes and counts them all.
        let (settlement, _) = settle(1, 2_000, &superseded, Some(&RoaringTreemap::new()), 0).await;
        assert!(
            matches!(settlement, Settlement::Exceeded { skipped } if skipped == limit as usize + 1),
            "{settlement:?}"
        );
    }
}
