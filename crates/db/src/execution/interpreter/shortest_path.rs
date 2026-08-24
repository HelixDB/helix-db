//! Shortest-path execution.
//!
//! The planner validates the static payload. Runtime execution resolves each
//! endpoint to exactly one existing node, then runs an unweighted breadth-first
//! search over adjacency data or label-neighbor indexes.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use helix_ast::graph::NodeRef;
use helix_ast::traversal::ShortestPathDirection;

use super::*;
use crate::encoding::keys;
use crate::encoding::v2::values;

impl<'db> ExecutionContext<'db> {
    pub(in crate::execution::interpreter) async fn execute_shortest_path(
        &mut self,
        plan: &ir::ShortestPathPlan,
    ) -> Result<ExecutionValue> {
        let Some(source) = self
            .resolve_shortest_path_endpoint(&plan.source, "shortest_path.source")
            .await?
        else {
            return Ok(ExecutionValue::Scalars(Vec::new()));
        };
        let Some(target) = self
            .resolve_shortest_path_endpoint(&plan.target, "shortest_path.target")
            .await?
        else {
            return Ok(ExecutionValue::Scalars(Vec::new()));
        };
        if source == target {
            return Ok(ExecutionValue::Scalars(vec![ExecutionScalar::NodeId(
                source,
            )]));
        }

        let mut queue = VecDeque::from([source]);
        let mut visited = BTreeSet::from([source]);
        let mut predecessor = BTreeMap::<u64, u64>::new();
        let mut found = false;

        for _depth in 0..plan.max_depth.get() {
            self.check_execution_deadline()?;
            let level_len = queue.len();
            if level_len == 0 {
                break;
            }
            for _ in 0..level_len {
                self.check_execution_deadline()?;
                let Some(current) = queue.pop_front() else {
                    break;
                };
                for neighbor in self.shortest_path_neighbors(current, plan).await? {
                    self.check_execution_deadline()?;
                    if !visited.insert(neighbor) {
                        continue;
                    }
                    predecessor.insert(neighbor, current);
                    if neighbor == target {
                        found = true;
                        break;
                    }
                    queue.push_back(neighbor);
                }
                if found {
                    break;
                }
            }
            if found {
                break;
            }
        }

        if !found {
            return Ok(ExecutionValue::Scalars(Vec::new()));
        }

        let mut path = vec![target];
        let mut current = target;
        while current != source {
            self.check_execution_deadline()?;
            let Some(previous) = predecessor.get(&current).copied() else {
                return Err(HelixDbError::InvariantViolation(
                    "shortest path predecessor chain is incomplete".to_string(),
                ));
            };
            current = previous;
            path.push(current);
        }
        path.reverse();
        Ok(ExecutionValue::Scalars(
            path.into_iter().map(ExecutionScalar::NodeId).collect(),
        ))
    }

    async fn resolve_shortest_path_endpoint(
        &self,
        reference: &NodeRef,
        op: &'static str,
    ) -> Result<Option<u64>> {
        let ids = match reference {
            NodeRef::All => {
                return Err(HelixDbError::Query(format!(
                    "{op} must resolve to exactly one node, got all nodes"
                )));
            }
            NodeRef::Ids(ids) => ids.clone(),
            NodeRef::Var(variable) => {
                let variable = ir::NonEmptyString::new(variable.clone()).ok_or_else(|| {
                    HelixDbError::Query(format!("{op} variable name must not be empty"))
                })?;
                self.variable_nodes(&variable)?
            }
            NodeRef::Param(param) => {
                let param = ir::NonEmptyString::new(param.clone()).ok_or_else(|| {
                    HelixDbError::Query(format!("{op} parameter name must not be empty"))
                })?;
                self.param_ids(&param)?
            }
        };

        match ids.as_slice() {
            [] => Ok(None),
            [id] => Ok(self.node_exists(*id).await?.then_some(*id)),
            _ => Err(HelixDbError::Query(format!(
                "{op} must resolve to exactly one node, got {} nodes",
                ids.len()
            ))),
        }
    }

    async fn node_exists(&self, node_id: u64) -> Result<bool> {
        let key = keys::DataKey::Data {
            scope: self.tenant_scope,
            kind: keys::DataKeyKind::NodeProperty(keys::NodePropertyKey::new(node_id)),
        }
        .to_bytes();
        self.get_raw(&key).await.map(|value| value.is_some())
    }

    async fn shortest_path_neighbors(
        &self,
        node_id: u64,
        plan: &ir::ShortestPathPlan,
    ) -> Result<Vec<u64>> {
        match plan.label.as_ref() {
            Some(label) => {
                self.shortest_path_labeled_neighbors(node_id, plan.direction, label)
                    .await
            }
            None => {
                self.shortest_path_any_neighbors(node_id, plan.direction)
                    .await
            }
        }
    }

    async fn shortest_path_any_neighbors(
        &self,
        node_id: u64,
        direction: ShortestPathDirection,
    ) -> Result<Vec<u64>> {
        let key = keys::DataKey::Data {
            scope: self.tenant_scope,
            kind: keys::DataKeyKind::Adjacency(keys::AdjacencyKey::new(node_id)),
        }
        .to_bytes();
        let Some(value) = self.get_raw(&key).await? else {
            return Ok(Vec::new());
        };
        let edges = values::adjacency::decode_edges(&value)?;
        let mut out = BTreeSet::new();
        match direction {
            ShortestPathDirection::Out | ShortestPathDirection::Both => {
                out.extend(edges.iter_out())
            }
            ShortestPathDirection::In => {}
        }
        match direction {
            ShortestPathDirection::In | ShortestPathDirection::Both => out.extend(edges.iter_in()),
            ShortestPathDirection::Out => {}
        }
        Ok(out.into_iter().collect())
    }

    async fn shortest_path_labeled_neighbors(
        &self,
        node_id: u64,
        direction: ShortestPathDirection,
        label: &ir::NonEmptyString,
    ) -> Result<Vec<u64>> {
        let mut out = BTreeSet::new();
        match direction {
            ShortestPathDirection::Out | ShortestPathDirection::Both => {
                out.extend(
                    self.lookup_out_neighbors_by_label(node_id, label.as_ref())
                        .await?,
                );
            }
            ShortestPathDirection::In => {}
        }
        match direction {
            ShortestPathDirection::In | ShortestPathDirection::Both => {
                out.extend(
                    self.lookup_in_neighbors_by_label(node_id, label.as_ref())
                        .await?,
                );
            }
            ShortestPathDirection::Out => {}
        }
        Ok(out.into_iter().collect())
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use helix_planner::context::ParamBindings;

    use super::super::test_support;
    use super::*;
    use crate::encoding::{indexes, keys};

    fn plan(source: NodeRef, target: NodeRef) -> ir::ShortestPathPlan {
        ir::ShortestPathPlan {
            source,
            target,
            label: None,
            direction: ShortestPathDirection::Out,
            max_depth: NonZeroUsize::new(2).unwrap(),
        }
    }

    #[tokio::test]
    async fn shortest_path_returns_empty_for_absent_source_and_target_nodes() {
        let db = test_support::open_db("shortest-path-missing-endpoints").await;
        let mut context = ExecutionContext::new(&db, ParamBindings::default());

        assert_eq!(
            context
                .execute_shortest_path(&plan(NodeRef::id(99), NodeRef::id(100)))
                .await
                .unwrap(),
            ExecutionValue::Scalars(Vec::new())
        );

        let source = test_support::add_node_with_properties(&db, "Node", Vec::new()).await;
        let mut context = ExecutionContext::new(&db, ParamBindings::default());
        assert_eq!(
            context
                .execute_shortest_path(&plan(NodeRef::id(source), NodeRef::id(100)))
                .await
                .unwrap(),
            ExecutionValue::Scalars(Vec::new())
        );
    }

    #[tokio::test]
    async fn shortest_path_endpoint_resolution_rejects_invalid_runtime_shapes() {
        let db = test_support::open_db("shortest-path-endpoint-contracts").await;
        let context = ExecutionContext::new(&db, ParamBindings::default());

        assert!(matches!(
            context
                .resolve_shortest_path_endpoint(&NodeRef::All, "source")
                .await,
            Err(HelixDbError::Query(message)) if message.contains("all nodes")
        ));
        assert!(matches!(
            context
                .resolve_shortest_path_endpoint(&NodeRef::var(""), "source")
                .await,
            Err(HelixDbError::Query(message)) if message.contains("variable name")
        ));
        assert!(matches!(
            context
                .resolve_shortest_path_endpoint(&NodeRef::param(""), "target")
                .await,
            Err(HelixDbError::Query(message)) if message.contains("parameter name")
        ));
        assert!(matches!(
            context
                .resolve_shortest_path_endpoint(&NodeRef::ids([1, 2]), "target")
                .await,
            Err(HelixDbError::Query(message)) if message.contains("got 2 nodes")
        ));
        assert_eq!(
            context
                .resolve_shortest_path_endpoint(&NodeRef::ids([]), "target")
                .await
                .unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn shortest_path_neighbor_reads_handle_missing_and_incoming_labeled_rows() {
        let db = test_support::open_db("shortest-path-empty-neighbors").await;
        let context = ExecutionContext::new(&db, ParamBindings::default());

        assert!(context
            .shortest_path_any_neighbors(99, ShortestPathDirection::Out)
            .await
            .unwrap()
            .is_empty());
        assert!(context
            .shortest_path_labeled_neighbors(
                99,
                ShortestPathDirection::In,
                &ir::NonEmptyString::new("LINK").unwrap(),
            )
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn shortest_path_propagates_corrupt_labeled_neighbor_rows() {
        let db = test_support::open_db("shortest-path-corrupt-labeled-neighbors").await;
        let inner = db.inner_db();
        let label = ir::NonEmptyString::new("LINK").unwrap();

        for (index_direction, path_direction) in [
            (indexes::EdgeDirection::Out, ShortestPathDirection::Out),
            (indexes::EdgeDirection::In, ShortestPathDirection::In),
        ] {
            inner
                .put(
                    keys::DataKey::Data {
                        scope: keys::scope::DataScope::LegacyUnscoped,
                        kind: keys::DataKeyKind::PropertyIndex(
                            indexes::PropertyIndexKey::EdgeLabelNeighbor(
                                indexes::label::EdgeLabelNeighborKey::new(
                                    index_direction,
                                    7,
                                    indexes::hash_property_value(label.as_ref()),
                                ),
                            ),
                        ),
                    }
                    .to_bytes(),
                    bytes::Bytes::from_static(b"corrupt labeled-neighbor bitmap"),
                )
                .await
                .expect("corrupt labeled-neighbor fixture persists");

            assert!(matches!(
                ExecutionContext::new(&db, ParamBindings::default())
                    .shortest_path_labeled_neighbors(7, path_direction, &label)
                    .await,
                Err(HelixDbError::Encoding(_))
            ));
        }

        db.close().await.expect("test database closes");
    }
}
