//! Traversal-scoped vector ranking while preserving upstream rows.

use std::collections::BTreeMap;

use helix_planner::{catalog, exec, ir};

use super::super::{ElementRef, ExecutionContext, ExecutionValue};
use super::search::{RestrictedVectorSearchRead, SearchReadLimit};
use crate::config::VectorElementType;
use crate::encoding::property::property_value::PropertyValue as DbPropertyValue;
use crate::error::{HelixDbError, Result};
use crate::search::vector::{DistanceOutputVersion, RestrictedVectorCandidates, VectorEntityId};

fn unique_restricted_rows(
    rows: Vec<super::super::ExecutionRow>,
    element_type: VectorElementType,
) -> Result<BTreeMap<u64, super::super::ExecutionRow>> {
    let mut rows_by_id = BTreeMap::new();
    for row in rows {
        let Some(current) = &row.current else {
            return Err(HelixDbError::Query(
                "vector_search expected rows with a current graph element".to_string(),
            ));
        };
        let id = match (element_type, current) {
            (VectorElementType::Node, ElementRef::Node(id))
            | (VectorElementType::Edge, ElementRef::Edge(id)) => *id,
            (VectorElementType::Node, ElementRef::Edge(_))
            | (VectorElementType::Edge, ElementRef::Node(_)) => {
                return Err(HelixDbError::Query(
                    "vector_search index kind does not match the input stream".to_string(),
                ));
            }
        };
        rows_by_id.entry(id).or_insert(row);
    }
    Ok(rows_by_id)
}

fn materialize_restricted_results(
    mut rows_by_id: BTreeMap<u64, super::super::ExecutionRow>,
    results: Vec<crate::search::vector::TypedVectorSearchResult>,
) -> Result<Vec<super::super::ExecutionRow>> {
    let distance_name =
        ir::NonEmptyString::new("$distance").expect("distance virtual property is non-empty");
    let mut ranked = Vec::with_capacity(results.len());
    for result in results {
        let id = match result.entity_id() {
            VectorEntityId::Node(id) | VectorEntityId::Edge(id) => id,
        };
        let Some(mut row) = rows_by_id.remove(&id) else {
            return Err(HelixDbError::InvariantViolation(
                "restricted vector search returned an ID outside its exact bitmap".to_string(),
            ));
        };
        let distance = result.materialize_distance(DistanceOutputVersion::CurrentScore);
        row.virtual_properties.insert(
            distance_name.clone(),
            DbPropertyValue::F64(distance.value() as f64),
        );
        ranked.push(row);
    }
    Ok(ranked)
}

impl<'db> ExecutionContext<'db> {
    pub(in crate::execution::interpreter) async fn restricted_vector_search(
        &self,
        input: ExecutionValue,
        plan: &ir::RestrictedVectorSearchPlan,
    ) -> Result<ExecutionValue> {
        let ExecutionValue::Stream(rows) = input else {
            return Err(HelixDbError::Query(
                "vector_search expected an element stream input".to_string(),
            ));
        };
        if rows.is_empty() {
            return Ok(ExecutionValue::Stream(Vec::new()));
        }

        let (element_type, label, property, index, query_vector, k) = match plan {
            ir::RestrictedVectorSearchPlan::Nodes {
                key,
                index,
                query_vector,
                k,
            } => (
                VectorElementType::Node,
                &key.label,
                &key.property,
                index,
                query_vector,
                k,
            ),
            ir::RestrictedVectorSearchPlan::Edges {
                key,
                index,
                query_vector,
                k,
            } => (
                VectorElementType::Edge,
                &key.label,
                &key.property,
                index,
                query_vector,
                k,
            ),
        };

        let rows_by_id = unique_restricted_rows(rows, element_type)?;
        let candidates = RestrictedVectorCandidates::from_ids(rows_by_id.keys().copied())?;
        let results = self
            .restricted_vector_search_results(
                element_type,
                label,
                property,
                index,
                query_vector,
                RestrictedVectorSearchRead::new(SearchReadLimit::new(k, None), &candidates),
            )
            .await?;
        Ok(ExecutionValue::Stream(materialize_restricted_results(
            rows_by_id, results,
        )?))
    }

    /// Ranks an index-served node ID set directly, with the same result as
    /// running its access and passing every row to
    /// [`Self::restricted_vector_search`]: those rows carry only their node,
    /// so here the `k` result rows are the only rows built. The set keeps its
    /// memory reservation until the search ends.
    pub(in crate::execution::interpreter) async fn id_set_vector_search(
        &self,
        search: IdSetVectorSearch<'_>,
    ) -> Result<ExecutionValue> {
        let ids = match search.set {
            NodeIdSet::Bitmap(bitmap) => {
                self.node_bitmap(bitmap, super::PARALLEL_INDEX_READS)
                    .await?
            }
            NodeIdSet::Label(label) => {
                self.lookup_equality_index_set(
                    "$label",
                    &DbPropertyValue::String(label.to_string()),
                )
                .await?
            }
            NodeIdSet::Secondary(set) => self.node_secondary_bitmap(set).await?,
        };
        #[cfg(test)]
        self.pull_work
            .fused_vector_searches
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let (ids, _admitted) = ids.into_parts();
        if ids.is_empty() {
            return Ok(ExecutionValue::Stream(Vec::new()));
        }
        let candidates = RestrictedVectorCandidates::from_bitmap(ids)?;
        let results = self
            .restricted_vector_search_results(
                VectorElementType::Node,
                &search.key.label,
                &search.key.property,
                search.index,
                search.query_vector,
                RestrictedVectorSearchRead::new(SearchReadLimit::new(search.k, None), &candidates),
            )
            .await?;
        let rows_by_id = results
            .iter()
            .filter_map(|result| {
                let (VectorEntityId::Node(id) | VectorEntityId::Edge(id)) = result.entity_id();
                candidates.contains(id).then(|| {
                    (
                        id,
                        super::super::ExecutionRow::current(ElementRef::Node(id)),
                    )
                })
            })
            .collect();
        Ok(ExecutionValue::Stream(materialize_restricted_results(
            rows_by_id, results,
        )?))
    }
}

/// A node vector search over an access that reads an index-served node ID
/// set, which the search can rank without the access's rows.
///
/// Only node searches over node ID sets are representable: edge searches and
/// every other access kind run as separate steps.
#[derive(Debug, Clone, Copy)]
pub(in crate::execution::interpreter) struct IdSetVectorSearch<'a> {
    set: NodeIdSet<'a>,
    key: &'a catalog::NodeSearchIndexKey,
    index: &'a ir::SearchIndexPlan,
    query_vector: &'a ir::VectorQueryInputPlan,
    k: &'a ir::SearchLimitPlan,
}

/// The index-served node ID sets whose access rows carry nothing but their
/// node, and which verify their own members, so the set alone is the input.
#[derive(Debug, Clone, Copy)]
enum NodeIdSet<'a> {
    Bitmap(&'a exec::ExecNodeBitmapExpr),
    Label(&'a ir::NonEmptyString),
    Secondary(&'a exec::ExecNodeSecondarySetPlan),
}

impl<'a> IdSetVectorSearch<'a> {
    /// Pairs `access` with `search`, or `None` unless `access` reads an
    /// index-served node ID set and `search` ranks nodes.
    pub(in crate::execution::interpreter) fn new(
        access: &'a exec::ExecAccessPlan,
        search: &'a ir::RestrictedVectorSearchPlan,
    ) -> Option<Self> {
        let ir::RestrictedVectorSearchPlan::Nodes {
            key,
            index,
            query_vector,
            k,
        } = search
        else {
            return None;
        };
        let exec::ExecAccessPlan::Node(access) = access else {
            return None;
        };
        let set = match access {
            exec::ExecNodeAccessPlan::Bitmap { bitmap } => NodeIdSet::Bitmap(bitmap),
            exec::ExecNodeAccessPlan::LabelScan { label } => NodeIdSet::Label(label),
            exec::ExecNodeAccessPlan::SecondarySet { set } => NodeIdSet::Secondary(set),
            // Not index ID sets, or read with per-ID checks or in an order
            // that ranking the set alone would skip.
            exec::ExecNodeAccessPlan::Empty
            | exec::ExecNodeAccessPlan::FromParam { .. }
            | exec::ExecNodeAccessPlan::FromVar { .. }
            | exec::ExecNodeAccessPlan::AllScan
            | exec::ExecNodeAccessPlan::Unique { .. }
            | exec::ExecNodeAccessPlan::AuthoritativeScan { .. }
            | exec::ExecNodeAccessPlan::DynamicEquality { .. }
            | exec::ExecNodeAccessPlan::DynamicMembership { .. }
            | exec::ExecNodeAccessPlan::RangeIndex { .. }
            | exec::ExecNodeAccessPlan::VectorSearch { .. }
            | exec::ExecNodeAccessPlan::TextSearch { .. } => return None,
        };
        Some(Self {
            set,
            key,
            index,
            query_vector,
            k,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encoding::v2::values::indexes::vector::{ActiveScoreSemantic, VectorEntityKind};
    use crate::execution::interpreter::ExecutionRow;
    use crate::search::vector::{DistanceScore, SearchResult, TypedVectorSearchResult};

    fn result(entity_id: u64, score: f32) -> TypedVectorSearchResult {
        TypedVectorSearchResult::from_physical(
            VectorEntityKind::Node,
            ActiveScoreSemantic::ManhattanF32V1,
            SearchResult::new(entity_id, DistanceScore::try_new(score).unwrap()),
        )
    }

    #[test]
    fn row_materialization_keeps_first_complete_row_and_replaces_only_distance() {
        let marker = ir::NonEmptyString::new("marker").unwrap();
        let distance = ir::NonEmptyString::new("$distance").unwrap();
        let binding = ir::NonEmptyString::new("bound").unwrap();
        let mut first = ExecutionRow::current(ElementRef::Node(1))
            .mark_path_visible()
            .mark_sack_visible();
        first
            .virtual_properties
            .insert(marker.clone(), DbPropertyValue::String("first".to_string()));
        first
            .virtual_properties
            .insert(distance.clone(), DbPropertyValue::F64(99.0));
        first.bindings.insert(binding.clone(), ElementRef::Node(77));
        first.set_sack(DbPropertyValue::I64(12));
        let first_before = first.clone();

        let mut duplicate = ExecutionRow::current(ElementRef::Node(1));
        duplicate.virtual_properties.insert(
            marker.clone(),
            DbPropertyValue::String("second".to_string()),
        );
        let second = ExecutionRow::current(ElementRef::Node(2));
        let second_before = second.clone();

        let rows = unique_restricted_rows(vec![first, duplicate, second], VectorElementType::Node)
            .unwrap();
        let ranked =
            materialize_restricted_results(rows, vec![result(2, 0.1), result(1, 0.2)]).unwrap();

        assert_eq!(ranked[0].current, second_before.current);
        assert_eq!(ranked[0].path, second_before.path);
        assert_eq!(ranked[1].bindings, first_before.bindings);
        assert_eq!(ranked[1].path, first_before.path);
        assert_eq!(ranked[1].path_visible, first_before.path_visible);
        assert_eq!(ranked[1].sack, first_before.sack);
        assert_eq!(
            ranked[1].virtual_properties.get(&marker),
            Some(DbPropertyValue::String("first".to_string()))
        );
        assert_eq!(
            ranked[1].virtual_properties.get(&distance),
            Some(DbPropertyValue::F64(0.2_f32 as f64))
        );
    }

    #[test]
    fn row_contract_rejects_mixed_element_kinds_and_out_of_bitmap_results() {
        let error = unique_restricted_rows(
            vec![ExecutionRow::current(ElementRef::Edge(1))],
            VectorElementType::Node,
        )
        .expect_err("node vector ranking must reject an edge row");
        assert!(error.to_string().contains("index kind"));

        let rows = unique_restricted_rows(
            vec![ExecutionRow::current(ElementRef::Node(1))],
            VectorElementType::Node,
        )
        .unwrap();
        let error = materialize_restricted_results(rows, vec![result(2, 0.1)])
            .expect_err("a result outside the exact candidate bitmap must fail closed");
        assert!(error.to_string().contains("outside its exact bitmap"));
    }
}
