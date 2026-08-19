//! Vector and text search access dispatch contracts.

use std::sync::Arc;

use helix_planner::ir;

use super::limits::SearchReadLimit;
use super::tenant::{validate_text_search_tenant, validate_vector_search_tenant};
use super::*;
use crate::config::{TextElementType, VectorElementType};
use crate::encoding::v1::values::vector_generation::{ActiveScoreSemantic, VectorEntityKind};
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
        self.vector_search_results_with_candidates(
            element_type,
            label,
            property,
            index,
            query_vector,
            VectorSearchScope::Unrestricted(limit),
        )
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
        self.vector_search_results_with_candidates(
            element_type,
            label,
            property,
            index,
            query_vector,
            VectorSearchScope::Restricted(read),
        )
        .await
    }

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
        let (limit, candidates) = match scope {
            VectorSearchScope::Unrestricted(limit) => (limit, None),
            VectorSearchScope::Restricted(read) => (read.limit, Some(read.candidates)),
        };
        let k = self.effective_search_limit(limit).await?;

        let raw_results = match definition.metric() {
            VectorDistanceMetric::Cosine => {
                let generation = self
                    .managed_vector_generation::<Cosine>(&definition, tenant_value.as_ref())
                    .await?;
                match candidates {
                    Some(candidates) => {
                        self.search_vector_index_restricted::<Cosine>(
                            &query,
                            k,
                            generation.as_ref(),
                            candidates,
                        )
                        .await
                    }
                    None => {
                        self.search_vector_index::<Cosine>(&query, k, generation.as_ref())
                            .await
                    }
                }
            }
            VectorDistanceMetric::Euclidean => {
                let generation = self
                    .managed_vector_generation::<Euclidean>(&definition, tenant_value.as_ref())
                    .await?;
                match candidates {
                    Some(candidates) => {
                        self.search_vector_index_restricted::<Euclidean>(
                            &query,
                            k,
                            generation.as_ref(),
                            candidates,
                        )
                        .await
                    }
                    None => {
                        self.search_vector_index::<Euclidean>(&query, k, generation.as_ref())
                            .await
                    }
                }
            }
            VectorDistanceMetric::Manhattan => {
                let generation = self
                    .managed_vector_generation::<Manhattan>(&definition, tenant_value.as_ref())
                    .await?;
                match candidates {
                    Some(candidates) => {
                        self.search_vector_index_restricted::<Manhattan>(
                            &query,
                            k,
                            generation.as_ref(),
                            candidates,
                        )
                        .await
                    }
                    None => {
                        self.search_vector_index::<Manhattan>(&query, k, generation.as_ref())
                            .await
                    }
                }
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
        self.text_search_hits_with_scope(&access, limit, TextSearchScope::Unrestricted)
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
        self.text_search_hits_with_scope(
            &access,
            read.limit,
            TextSearchScope::restricted(read.candidates),
        )
        .await
    }

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
        let Some(manifest) = self.load_text_manifest_root(generation.as_ref()).await? else {
            return Ok(Vec::new());
        };
        self.search_text_manifest_with_scope(&manifest, &query, k, scope)
            .await
    }
}
