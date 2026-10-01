//! Traversal-scoped BM25 ranking while preserving upstream rows.

use std::collections::BTreeMap;
use std::sync::Arc;

use helix_planner::ir;

use super::super::{ElementRef, ExecutionContext, ExecutionRow, ExecutionValue};
use super::search::{RestrictedTextSearchRead, SearchReadLimit, TextSearchAccess};
use crate::config::TextElementType;
use crate::encoding::property::property_value::PropertyValue as DbPropertyValue;
use crate::error::{HelixDbError, Result};
use crate::search::text::{
    RestrictedTextCandidates, RestrictedTextCandidatesBuilder, TextSearchHit,
};

#[derive(Debug)]
struct RestrictedTextRows {
    rows_by_id: BTreeMap<u64, ExecutionRow>,
    candidates: RestrictedTextCandidates,
}

fn unique_restricted_rows(
    rows: Vec<ExecutionRow>,
    element_type: TextElementType,
    mut candidates: RestrictedTextCandidatesBuilder,
) -> Result<RestrictedTextRows> {
    let mut rows_by_id = BTreeMap::new();
    for row in rows {
        let Some(current) = &row.current else {
            return Err(HelixDbError::Query(
                "text_search expected rows with a current graph element".to_string(),
            ));
        };
        let id = match (element_type, current) {
            (TextElementType::Node, ElementRef::Node(id))
            | (TextElementType::Edge, ElementRef::Edge(id)) => *id,
            (TextElementType::Node, ElementRef::Edge(_))
            | (TextElementType::Edge, ElementRef::Node(_)) => {
                return Err(HelixDbError::Query(
                    "text_search index kind does not match the input stream".to_string(),
                ));
            }
        };
        if candidates.try_insert(id)? {
            let previous = rows_by_id.insert(id, row);
            debug_assert!(
                previous.is_none(),
                "the candidate bitmap and retained traversal rows must agree"
            );
        }
    }
    Ok(RestrictedTextRows {
        rows_by_id,
        candidates: candidates.finish(),
    })
}

fn materialize_restricted_results(
    mut rows_by_id: BTreeMap<u64, ExecutionRow>,
    results: Vec<TextSearchHit>,
) -> Result<Vec<ExecutionRow>> {
    let score_name = ir::NonEmptyString::new("$score").expect("score property is non-empty");
    let mut ranked = Vec::with_capacity(results.len());
    for result in results {
        let Some(mut row) = rows_by_id.remove(&result.entity_id) else {
            return Err(HelixDbError::InvariantViolation(
                "restricted text search returned an ID outside its exact bitmap".to_string(),
            ));
        };
        row.virtual_properties.insert(
            score_name.clone(),
            DbPropertyValue::F64(f64::from(result.score)),
        );
        ranked.push(row);
    }
    Ok(ranked)
}

impl<'db> ExecutionContext<'db> {
    pub(in crate::execution::interpreter) async fn restricted_text_search(
        &self,
        input: ExecutionValue,
        plan: &ir::RestrictedTextSearchPlan,
    ) -> Result<ExecutionValue> {
        let ExecutionValue::Stream(rows) = input else {
            return Err(HelixDbError::Query(
                "text_search expected an element stream input".to_string(),
            ));
        };
        if rows.is_empty() {
            return Ok(ExecutionValue::Stream(Vec::new()));
        }

        let (element_type, label, property, index, query_text, k, fuzzy_distance) = match plan {
            ir::RestrictedTextSearchPlan::Nodes {
                key,
                index,
                query_text,
                k,
                fuzzy_distance,
            } => (
                TextElementType::Node,
                &key.label,
                &key.property,
                index,
                query_text,
                k,
                *fuzzy_distance,
            ),
            ir::RestrictedTextSearchPlan::Edges {
                key,
                index,
                query_text,
                k,
                fuzzy_distance,
            } => (
                TextElementType::Edge,
                &key.label,
                &key.property,
                index,
                query_text,
                k,
                *fuzzy_distance,
            ),
        };

        let RestrictedTextRows {
            rows_by_id,
            candidates,
        } = unique_restricted_rows(rows, element_type, RestrictedTextCandidatesBuilder::new())?;
        let candidates = Arc::new(candidates);
        let results = self
            .restricted_text_search_hits(
                TextSearchAccess::new(
                    element_type,
                    label,
                    property,
                    index,
                    query_text,
                    fuzzy_distance,
                ),
                RestrictedTextSearchRead::new(SearchReadLimit::new(k, None), candidates),
            )
            .await?;
        Ok(ExecutionValue::Stream(materialize_restricted_results(
            rows_by_id, results,
        )?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hit(entity_id: u64, score: f32) -> TextSearchHit {
        TextSearchHit { entity_id, score }
    }

    #[test]
    fn row_materialization_deduplicates_and_preserves_complete_first_row() {
        let marker = ir::NonEmptyString::new("marker").unwrap();
        let score = ir::NonEmptyString::new("$score").unwrap();
        let binding = ir::NonEmptyString::new("bound").unwrap();
        let mut first = ExecutionRow::current(ElementRef::Node(1))
            .mark_path_visible()
            .mark_sack_visible();
        first
            .virtual_properties
            .insert(marker.clone(), DbPropertyValue::String("first".to_string()));
        first
            .virtual_properties
            .insert(score.clone(), DbPropertyValue::F64(99.0));
        first.bindings.insert(binding, ElementRef::Node(77));
        first.set_sack(DbPropertyValue::I64(12));
        let first_before = first.clone();

        let mut duplicate = ExecutionRow::current(ElementRef::Node(1));
        duplicate.virtual_properties.insert(
            marker.clone(),
            DbPropertyValue::String("second".to_string()),
        );
        let second = ExecutionRow::current(ElementRef::Node(2));

        let rows = unique_restricted_rows(
            vec![first, duplicate, second],
            TextElementType::Node,
            RestrictedTextCandidatesBuilder::new(),
        )
        .unwrap();
        assert!(rows.candidates.contains(1));
        assert!(rows.candidates.contains(2));
        let ranked =
            materialize_restricted_results(rows.rows_by_id, vec![hit(2, 4.0), hit(1, 3.0)])
                .unwrap();

        assert_eq!(ranked.len(), 2);
        assert_eq!(ranked[1].bindings, first_before.bindings);
        assert_eq!(ranked[1].path, first_before.path);
        assert_eq!(ranked[1].path_visible, first_before.path_visible);
        assert_eq!(ranked[1].sack, first_before.sack);
        assert_eq!(
            ranked[1].virtual_properties.get(&marker),
            Some(DbPropertyValue::String("first".to_string()))
        );
        assert_eq!(
            ranked[1].virtual_properties.get(&score),
            Some(DbPropertyValue::F64(3.0))
        );
    }

    #[test]
    fn row_contract_rejects_wrong_kind_and_out_of_bitmap_results() {
        let error = unique_restricted_rows(
            vec![ExecutionRow::current(ElementRef::Edge(1))],
            TextElementType::Node,
            RestrictedTextCandidatesBuilder::new(),
        )
        .expect_err("node FTS must reject edge rows");
        assert!(error.to_string().contains("index kind"));

        let rows = unique_restricted_rows(
            vec![ExecutionRow::current(ElementRef::Node(1))],
            TextElementType::Node,
            RestrictedTextCandidatesBuilder::new(),
        )
        .unwrap();
        let error = materialize_restricted_results(rows.rows_by_id, vec![hit(2, 1.0)])
            .expect_err("results outside the candidate bitmap must fail closed");
        assert!(error.to_string().contains("outside its exact bitmap"));
    }

    #[test]
    fn row_collection_rejects_unique_overflow_while_deduplicating() {
        let rows = vec![
            ExecutionRow::current(ElementRef::Node(1)),
            ExecutionRow::current(ElementRef::Node(1)),
            ExecutionRow::current(ElementRef::Node(2)),
            ExecutionRow::current(ElementRef::Node(3)),
        ];
        let error = unique_restricted_rows(
            rows,
            TextElementType::Node,
            RestrictedTextCandidatesBuilder::with_limit(std::num::NonZeroU64::new(2).unwrap()),
        )
        .expect_err("the third unique row must fail during collection");

        assert!(error.to_string().contains("at most 2"));
    }
}
