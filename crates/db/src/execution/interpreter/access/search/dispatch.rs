//! Vector and text search access dispatch contracts.

use helix_planner::ir;

use super::limits::SearchReadLimit;
use super::tenant::{validate_text_search_tenant, validate_vector_search_tenant};
use super::*;
use crate::config::{TextElementType, VectorElementType};
use crate::encoding::v1::values::vector_generation::{ActiveScoreSemantic, VectorEntityKind};
use crate::search::vector::distance::{Cosine, Euclidean, Manhattan};
use crate::search::vector::{TypedVectorSearchResult, VectorDistanceMetric};

impl<'db> ExecutionContext<'db> {
    pub(in crate::execution::interpreter::access) async fn vector_search_results(
        &self,
        element_type: VectorElementType,
        label: &ir::NonEmptyString,
        property: &ir::NonEmptyString,
        index: &ir::SearchIndexPlan,
        query_vector: &ir::VectorQueryInputPlan,
        limit: SearchReadLimit<'_>,
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
        let k = self.effective_search_limit(limit).await?;

        let raw_results = match definition.metric() {
            VectorDistanceMetric::Cosine => {
                let generation = self
                    .managed_vector_generation::<Cosine>(&definition, tenant_value.as_ref())
                    .await?;
                self.search_vector_index::<Cosine>(&query, k, generation.as_ref())
                    .await
            }
            VectorDistanceMetric::Euclidean => {
                let generation = self
                    .managed_vector_generation::<Euclidean>(&definition, tenant_value.as_ref())
                    .await?;
                self.search_vector_index::<Euclidean>(&query, k, generation.as_ref())
                    .await
            }
            VectorDistanceMetric::Manhattan => {
                let generation = self
                    .managed_vector_generation::<Manhattan>(&definition, tenant_value.as_ref())
                    .await?;
                self.search_vector_index::<Manhattan>(&query, k, generation.as_ref())
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

    pub(in crate::execution::interpreter::access) async fn text_search_ids(
        &self,
        element_type: TextElementType,
        label: &ir::NonEmptyString,
        property: &ir::NonEmptyString,
        index: &ir::SearchIndexPlan,
        query_text: &ir::TextQueryInputPlan,
        limit: SearchReadLimit<'_>,
    ) -> Result<Vec<u64>> {
        if let Some(reason) = self
            .db
            .index_lifecycle_unavailable_reason(crate::error::IndexFamily::Text)
        {
            return Err(HelixDbError::IndexLifecycleUnavailable {
                family: crate::error::IndexFamily::Text,
                reason,
            });
        }
        let definition = self.text_definition(element_type, label, property)?;
        let tenant_value = self.search_tenant_value(&index.tenant).await?;
        validate_text_search_tenant(&definition, &index.tenant, tenant_value.as_ref())?;
        let query = self.search_query_text(query_text).await?;
        let k = self.effective_search_limit(limit).await?;

        let generation = self
            .managed_text_generation(&definition, tenant_value.as_ref())
            .await?;
        let Some(manifest) = self.load_text_manifest_root(generation.as_ref()).await? else {
            return Ok(Vec::new());
        };
        self.search_text_manifest(&manifest, &query, k).await
    }
}
