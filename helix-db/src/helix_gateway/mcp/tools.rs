use crate::{
    helix_engine::{
        storage_core::HelixGraphStorage,
        traversal_core::{
            ops::{
                g::G,
                in_::{in_::InAdapter, in_e::InEdgesAdapter},
                out::{out::OutAdapter, out_e::OutEdgesAdapter},
                source::{
                    e_from_type::EFromTypeAdapter, n_from_type::NFromTypeAdapter,
                    v_from_type::VFromTypeAdapter,
                },
                util::{order::OrderByAdapter, range::RangeAdapter},
            },
            traversal_iter::RoTraversalIterator,
            traversal_value::TraversalValue,
        },
        types::GraphError,
    },
    protocol::value::Value,
};
use bumpalo::Bump;
use heed3::RoTxn;
use serde::Deserialize;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EdgeType {
    Node,
    Vec,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "tool_name", content = "args")]
pub enum ToolArgs {
    OutStep {
        edge_label: String,
        edge_type: EdgeType,
        filter: Option<FilterTraversal>,
    },
    OutEStep {
        edge_label: String,
        filter: Option<FilterTraversal>,
    },
    InStep {
        edge_label: String,
        edge_type: EdgeType,
        filter: Option<FilterTraversal>,
    },
    InEStep {
        edge_label: String,
        filter: Option<FilterTraversal>,
    },
    NFromType {
        node_type: String,
    },
    EFromType {
        edge_type: String,
    },
    VFromType {
        vector_type: String,
        filter: Option<FilterTraversal>,
    },
    FilterItems {
        #[serde(default)]
        filter: FilterTraversal,
    },
    OrderBy {
        properties: String,
        order: Order,
    },
    SearchKeyword {
        query: String,
        limit: usize,
        label: String,
    },
    SearchVecText {
        query: String,
        label: String,
        k: usize,
    },
    SearchVec {
        vector: Vec<f64>,
        k: usize,
        min_score: Option<f64>,
        cutoff: Option<usize>,
    },
}

#[derive(Debug, Clone, Copy, Deserialize)]
pub enum Order {
    #[serde(rename = "asc")]
    Asc,
    #[serde(rename = "desc")]
    Desc,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct FilterProperties {
    pub key: String,
    pub value: Value,
    pub operator: Option<Operator>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct FilterTraversal {
    pub properties: Option<Vec<Vec<FilterProperties>>>,
    pub filter_traversals: Option<Vec<ToolArgs>>,
}

#[derive(Debug, Clone, Copy, Deserialize)]
pub enum Operator {
    #[serde(rename = "==")]
    Eq,
    #[serde(rename = "!=")]
    Neq,
    #[serde(rename = ">")]
    Gt,
    #[serde(rename = "<")]
    Lt,
    #[serde(rename = ">=")]
    Gte,
    #[serde(rename = "<=")]
    Lte,
}

impl Operator {
    #[inline]
    pub fn execute(&self, lhs: &Value, rhs: &Value) -> bool {
        match self {
            Operator::Eq => lhs == rhs,
            Operator::Neq => lhs != rhs,
            Operator::Gt => lhs > rhs,
            Operator::Lt => lhs < rhs,
            Operator::Gte => lhs >= rhs,
            Operator::Lte => lhs <= rhs,
        }
    }
}

type DynIter<'arena, 'txn> =
    Box<dyn Iterator<Item = Result<TraversalValue<'arena>, GraphError>> + 'txn>;

pub struct TraversalStream<'db, 'arena, 'txn>
where
    'db: 'arena,
    'arena: 'txn,
{
    iter: RoTraversalIterator<'db, 'arena, 'txn, DynIter<'arena, 'txn>>,
}

impl<'db, 'arena, 'txn> TraversalStream<'db, 'arena, 'txn>
where
    'db: 'arena,
    'arena: 'txn,
{
    pub fn new(
        storage: &'db HelixGraphStorage,
        txn: &'txn RoTxn<'db>,
        arena: &'arena Bump,
    ) -> Self {
        Self::from_ro_iterator(G::new(storage, txn, arena))
    }

    pub fn from_iter(
        storage: &'db HelixGraphStorage,
        txn: &'txn RoTxn<'db>,
        arena: &'arena Bump,
        items: impl Iterator<Item = TraversalValue<'arena>> + 'txn,
    ) -> Self {
        Self::from_ro_iterator(G::from_iter(storage, txn, items, arena))
    }

    pub fn from_ro_iterator<I>(iter: RoTraversalIterator<'db, 'arena, 'txn, I>) -> Self
    where
        I: Iterator<Item = Result<TraversalValue<'arena>, GraphError>> + 'txn,
    {
        let RoTraversalIterator {
            storage,
            arena,
            txn,
            inner,
        } = iter;

        let boxed: DynIter<'arena, 'txn> = Box::new(inner);

        Self {
            iter: RoTraversalIterator {
                storage,
                arena,
                txn,
                inner: boxed,
            },
        }
    }

    pub fn map<I, F>(self, f: F) -> TraversalStream<'db, 'arena, 'txn>
    where
        I: Iterator<Item = Result<TraversalValue<'arena>, GraphError>> + 'txn,
        F: FnOnce(
            RoTraversalIterator<'db, 'arena, 'txn, DynIter<'arena, 'txn>>,
        ) -> RoTraversalIterator<'db, 'arena, 'txn, I>,
    {
        TraversalStream::from_ro_iterator(f(self.iter))
    }

    pub fn into_ro(self) -> RoTraversalIterator<'db, 'arena, 'txn, DynIter<'arena, 'txn>> {
        self.iter
    }

    pub fn into_inner_iter(self) -> DynIter<'arena, 'txn> {
        self.iter.inner
    }

    pub fn collect(self) -> Result<Vec<TraversalValue<'arena>>, GraphError> {
        let mut values = Vec::new();
        for item in self.into_inner_iter() {
            values.push(item?);
        }
        Ok(values)
    }

    pub fn nth(self, index: usize) -> Result<Option<TraversalValue<'arena>>, GraphError> {
        let mut iter = self.into_inner_iter();
        for _ in 0..index {
            if let Some(res) = iter.next() {
                res?;
            } else {
                return Ok(None);
            }
        }

        match iter.next() {
            Some(res) => res.map(Some),
            None => Ok(None),
        }
    }
}

pub fn execute_query_chain<'db, 'arena, 'txn>(
    steps: &[ToolArgs],
    storage: &'db HelixGraphStorage,
    txn: &'txn RoTxn<'db>,
    arena: &'arena Bump,
) -> Result<TraversalStream<'db, 'arena, 'txn>, GraphError>
where
    'db: 'arena,
    'arena: 'txn,
{
    let initial = TraversalStream::new(storage, txn, arena);
    execute_query_chain_with_stream(initial, steps, storage, txn, arena)
}

pub fn execute_query_chain_from_seed<'db, 'arena, 'txn>(
    steps: &[ToolArgs],
    storage: &'db HelixGraphStorage,
    txn: &'txn RoTxn<'db>,
    arena: &'arena Bump,
    seed: impl Iterator<Item = TraversalValue<'arena>> + 'txn,
) -> Result<TraversalStream<'db, 'arena, 'txn>, GraphError>
where
    'db: 'arena,
    'arena: 'txn,
{
    let initial = TraversalStream::from_iter(storage, txn, arena, seed);
    execute_query_chain_with_stream(initial, steps, storage, txn, arena)
}

pub fn execute_query_chain_with_stream<'db, 'arena, 'txn>(
    initial: TraversalStream<'db, 'arena, 'txn>,
    steps: &[ToolArgs],
    storage: &'db HelixGraphStorage,
    txn: &'txn RoTxn<'db>,
    arena: &'arena Bump,
) -> Result<TraversalStream<'db, 'arena, 'txn>, GraphError>
where
    'db: 'arena,
    'arena: 'txn,
{
    steps.iter().try_fold(initial, |stream, step| {
        apply_step(stream, step, storage, txn, arena)
    })
}

fn apply_step<'db, 'arena, 'txn>(
    stream: TraversalStream<'db, 'arena, 'txn>,
    step: &ToolArgs,
    storage: &'db HelixGraphStorage,
    txn: &'txn RoTxn<'db>,
    arena: &'arena Bump,
) -> Result<TraversalStream<'db, 'arena, 'txn>, GraphError>
where
    'db: 'arena,
    'arena: 'txn,
{
    match step {
        ToolArgs::NFromType { node_type } => {
            let label = arena.alloc_str(node_type);
            Ok(TraversalStream::from_ro_iterator(
                G::new(storage, txn, arena).n_from_type(label),
            ))
        }
        ToolArgs::EFromType { edge_type } => {
            let label = arena.alloc_str(edge_type);
            Ok(TraversalStream::from_ro_iterator(
                G::new(storage, txn, arena).e_from_type(label),
            ))
        }
        ToolArgs::VFromType {
            vector_type,
            filter,
        } => {
            let label = arena.alloc_str(vector_type);
            let transformed = TraversalStream::from_ro_iterator(
                G::new(storage, txn, arena).v_from_type(label, true),
            );
            if let Some(filter) = filter.clone() {
                apply_filter(transformed, filter)
            } else {
                Ok(transformed)
            }
        }
        ToolArgs::OutStep {
            edge_label,
            edge_type,
            filter,
        } => {
            let label = arena.alloc_str(edge_label);
            let edge_kind = *edge_type;
            let transformed = match edge_kind {
                EdgeType::Node => stream.map(|iter| iter.out_node(label)),
                EdgeType::Vec => stream.map(|iter| iter.out_vec(label, true)),
            };

            if let Some(filter) = filter.clone() {
                apply_filter(transformed, filter)
            } else {
                Ok(transformed)
            }
        }
        ToolArgs::OutEStep { edge_label, filter } => {
            let label = arena.alloc_str(edge_label);
            let transformed = stream.map(|iter| iter.out_e(label));

            if let Some(filter) = filter.clone() {
                apply_filter(transformed, filter)
            } else {
                Ok(transformed)
            }
        }
        ToolArgs::InStep {
            edge_label,
            edge_type,
            filter,
        } => {
            let label = arena.alloc_str(edge_label);
            let edge_kind = *edge_type;
            let transformed = match edge_kind {
                EdgeType::Node => stream.map(|iter| iter.in_node(label)),
                EdgeType::Vec => stream.map(|iter| iter.in_vec(label, true)),
            };

            if let Some(filter) = filter.clone() {
                apply_filter(transformed, filter)
            } else {
                Ok(transformed)
            }
        }
        ToolArgs::InEStep { edge_label, filter } => {
            let label = arena.alloc_str(edge_label);
            let transformed = stream.map(|iter| iter.in_e(label));

            if let Some(filter) = filter.clone() {
                apply_filter(transformed, filter)
            } else {
                Ok(transformed)
            }
        }
        ToolArgs::FilterItems { filter } => apply_filter(stream, filter.clone()),
        ToolArgs::OrderBy { properties, order } => {
            let props = arena.alloc_str(properties);
            let values = stream.collect()?;
            let iter = TraversalStream::from_iter(storage, txn, arena, values.into_iter());
            let ordered_stream = match order {
                Order::Asc => iter
                    .map(|iter| iter.order_by_asc(|val| val.get_property(props).unwrap().clone())),
                Order::Desc => iter
                    .map(|iter| iter.order_by_desc(|val| val.get_property(props).unwrap().clone())),
            };

            Ok(ordered_stream)
        }
        ToolArgs::SearchKeyword { .. } => {
            // SearchKeyword requires special BM25 indexing and connection state
            // It should be called via the dedicated search_keyword MCP handler
            // not through the generic query chain execution
            Err(GraphError::New(
                "SearchKeyword is not supported in generic query chains. Use the search_keyword endpoint directly.".to_string()
            ))
        }
        ToolArgs::SearchVecText { query, label, k } => {
            // SearchVecText requires embedding model initialization
            // It should be called via the dedicated search_vec_text MCP handler
            // not through the generic query chain execution
            Err(GraphError::New(format!(
                "SearchVecText (query: {}, label: {}, k: {}) is not supported in generic query chains. Use the search_vec_text endpoint directly.",
                query, label, k
            )))
        }
        ToolArgs::SearchVec {
            vector,
            k,
            min_score,
            cutoff,
        } => {
            use crate::helix_engine::traversal_core::ops::vectors::brute_force_search::BruteForceSearchVAdapter;

            let query_vec = arena.alloc_slice_copy(vector);
            let mut results = match cutoff {
                Some(cutoff_val) => stream.map(|iter| {
                    iter.range(0, *cutoff_val)
                        .brute_force_search_v(query_vec, *k)
                }),
                None => stream.map(|iter| iter.brute_force_search_v(query_vec, *k)),
            };

            // Apply min_score filter if specified
            if let Some(min_score_val) = min_score {
                let min_score_copy = *min_score_val;
                results = results.map(|iter| {
                    let RoTraversalIterator {
                        storage,
                        arena,
                        txn,
                        inner,
                    } = iter;
                    let filtered: DynIter<'arena, 'txn> = Box::new(inner.filter(move |item_res| {
                        match item_res {
                            Ok(TraversalValue::Vector(v)) => v.get_distance() > min_score_copy,
                            _ => true, // Keep non-vector items
                        }
                    }));
                    RoTraversalIterator {
                        storage,
                        arena,
                        txn,
                        inner: filtered,
                    }
                });
            }

            Ok(results)
        }
    }
}

fn apply_filter<'db, 'arena, 'txn>(
    stream: TraversalStream<'db, 'arena, 'txn>,
    filter: FilterTraversal,
) -> Result<TraversalStream<'db, 'arena, 'txn>, GraphError>
where
    'db: 'arena,
    'arena: 'txn,
{
    let filter_arc = Arc::new(filter);

    Ok(stream.map(|iter| {
        let RoTraversalIterator {
            storage,
            arena,
            txn,
            inner,
        } = iter;

        let filter_clone = Arc::clone(&filter_arc);
        let filtered: DynIter<'arena, 'txn> =
            Box::new(inner.filter_map(move |item_res| match item_res {
                Ok(item) => match matches_filter(&item, &filter_clone, storage, txn, arena) {
                    Ok(true) => Some(Ok(item)),
                    Ok(false) => None,
                    Err(err) => Some(Err(err)),
                },
                Err(err) => Some(Err(err)),
            }));

        RoTraversalIterator {
            storage,
            arena,
            txn,
            inner: filtered,
        }
    }))
}

fn matches_filter<'db, 'arena, 'txn>(
    item: &TraversalValue<'arena>,
    filter: &FilterTraversal,
    storage: &'db HelixGraphStorage,
    txn: &'txn RoTxn<'db>,
    arena: &'arena Bump,
) -> Result<bool, GraphError>
where
    'db: 'arena,
    'arena: 'txn,
{
    if !matches_properties(item, filter.properties.as_ref()) {
        return Ok(false);
    }

    match &filter.filter_traversals {
        Some(traversals) => {
            for tool in traversals {
                if !evaluate_sub_traversal(item, tool, storage, txn, arena)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        None => Ok(true),
    }
}

fn matches_properties(
    item: &TraversalValue<'_>,
    groups: Option<&Vec<Vec<FilterProperties>>>,
) -> bool {
    match groups {
        Some(groups) => groups.iter().any(|filters| {
            filters.iter().all(|filter| {
                item.get_property(&filter.key)
                    .map(|value| value.compare(&filter.value, filter.operator))
                    .unwrap_or(false)
            })
        }),
        None => true,
    }
}

fn evaluate_sub_traversal<'db, 'arena, 'txn>(
    item: &TraversalValue<'arena>,
    step: &ToolArgs,
    storage: &'db HelixGraphStorage,
    txn: &'txn RoTxn<'db>,
    arena: &'arena Bump,
) -> Result<bool, GraphError>
where
    'db: 'arena,
    'arena: 'txn,
{
    let seed = std::iter::once(item.clone());
    let stream =
        execute_query_chain_from_seed(std::slice::from_ref(step), storage, txn, arena, seed)?;
    let mut iter = stream.into_inner_iter();
    match iter.next() {
        Some(Ok(_)) => Ok(true),
        Some(Err(err)) => Err(err),
        None => Ok(false),
    }
}

pub trait FilterValues {
    fn compare(&self, value: &Value, operator: Option<Operator>) -> bool;
}

#[cfg(test)]
mod tests {
    use super::*;

    // ============================================================================
    // Operator::execute() Tests
    // ============================================================================

    #[test]
    fn test_operator_eq() {
        let op = Operator::Eq;

        // Integers
        assert!(op.execute(&Value::I32(5), &Value::I32(5)));
        assert!(!op.execute(&Value::I32(5), &Value::I32(6)));

        // Strings
        assert!(op.execute(
            &Value::String("hello".to_string()),
            &Value::String("hello".to_string())
        ));
        assert!(!op.execute(
            &Value::String("hello".to_string()),
            &Value::String("world".to_string())
        ));

        // Booleans
        assert!(op.execute(&Value::Boolean(true), &Value::Boolean(true)));
        assert!(!op.execute(&Value::Boolean(true), &Value::Boolean(false)));
    }

    #[test]
    fn test_operator_neq() {
        let op = Operator::Neq;

        assert!(op.execute(&Value::I32(5), &Value::I32(6)));
        assert!(!op.execute(&Value::I32(5), &Value::I32(5)));

        assert!(op.execute(
            &Value::String("hello".to_string()),
            &Value::String("world".to_string())
        ));
        assert!(!op.execute(
            &Value::String("hello".to_string()),
            &Value::String("hello".to_string())
        ));
    }

    #[test]
    fn test_operator_lt() {
        let op = Operator::Lt;

        // Integers
        assert!(op.execute(&Value::I32(3), &Value::I32(5)));
        assert!(!op.execute(&Value::I32(5), &Value::I32(3)));
        assert!(!op.execute(&Value::I32(5), &Value::I32(5))); // Equal is not less than

        // Floats
        assert!(op.execute(&Value::F64(1.5), &Value::F64(2.5)));
        assert!(!op.execute(&Value::F64(2.5), &Value::F64(1.5)));
    }

    #[test]
    fn test_operator_gt() {
        let op = Operator::Gt;

        // Integers
        assert!(op.execute(&Value::I32(5), &Value::I32(3)));
        assert!(!op.execute(&Value::I32(3), &Value::I32(5)));
        assert!(!op.execute(&Value::I32(5), &Value::I32(5))); // Equal is not greater than

        // Floats
        assert!(op.execute(&Value::F64(2.5), &Value::F64(1.5)));
        assert!(!op.execute(&Value::F64(1.5), &Value::F64(2.5)));
    }

    #[test]
    fn test_operator_lte() {
        let op = Operator::Lte;

        // Less than
        assert!(op.execute(&Value::I32(3), &Value::I32(5)));
        // Equal
        assert!(op.execute(&Value::I32(5), &Value::I32(5)));
        // Greater than
        assert!(!op.execute(&Value::I32(5), &Value::I32(3)));
    }

    #[test]
    fn test_operator_gte() {
        let op = Operator::Gte;

        // Greater than
        assert!(op.execute(&Value::I32(5), &Value::I32(3)));
        // Equal
        assert!(op.execute(&Value::I32(5), &Value::I32(5)));
        // Less than
        assert!(!op.execute(&Value::I32(3), &Value::I32(5)));
    }

    #[test]
    fn test_operator_cross_type_numeric() {
        // Value's PartialOrd and PartialEq handle cross-type numeric comparisons
        let op_eq = Operator::Eq;
        let op_gt = Operator::Gt;

        // I32 vs F64 - equality works for matching values
        assert!(op_eq.execute(&Value::I32(42), &Value::F64(42.0)));

        // Different integer sizes work correctly
        assert!(op_eq.execute(&Value::I32(100), &Value::I64(100)));
        assert!(op_gt.execute(&Value::U64(1000), &Value::I32(500)));
        assert!(op_gt.execute(&Value::I64(1000), &Value::I32(500)));
    }

    #[test]
    fn test_operator_string_comparison() {
        let op_lt = Operator::Lt;
        let op_gt = Operator::Gt;

        // Lexicographic ordering
        assert!(op_lt.execute(
            &Value::String("apple".to_string()),
            &Value::String("banana".to_string())
        ));
        assert!(op_gt.execute(
            &Value::String("zebra".to_string()),
            &Value::String("apple".to_string())
        ));
    }

    #[test]
    fn test_operator_empty_value() {
        let op_eq = Operator::Eq;

        // Empty equals empty
        assert!(op_eq.execute(&Value::Empty, &Value::Empty));

        // Empty doesn't equal non-empty
        assert!(!op_eq.execute(&Value::Empty, &Value::I32(0)));
        assert!(!op_eq.execute(&Value::I32(0), &Value::Empty));
    }

    #[test]
    fn test_operator_boolean_values() {
        let op_eq = Operator::Eq;
        let op_neq = Operator::Neq;

        assert!(op_eq.execute(&Value::Boolean(true), &Value::Boolean(true)));
        assert!(op_eq.execute(&Value::Boolean(false), &Value::Boolean(false)));
        assert!(op_neq.execute(&Value::Boolean(true), &Value::Boolean(false)));
    }
}
