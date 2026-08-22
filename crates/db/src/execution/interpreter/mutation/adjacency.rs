//! Mutation-maintained adjacency index contracts.

#[cfg(test)]
use slatedb::DbTransaction;

#[cfg(test)]
use super::contracts::decode_stored_edges;
use super::*;

impl<'db> ExecutionContext<'db> {
    #[cfg(test)]
    pub(super) async fn add_adjacency(
        &self,
        txn: &DbTransaction,
        node: u64,
        neighbor: u64,
        direction: ir::ExpandDirection,
    ) -> Result<()> {
        let key = self.storage_key(keys::DataKeyKind::Adjacency(keys::AdjacencyKey::new(node)));
        let mut edges = decode_stored_edges(txn.get(&key).await?)?;
        match direction {
            ir::ExpandDirection::Out => edges.add_out(neighbor),
            ir::ExpandDirection::In => edges.add_in(neighbor),
            ir::ExpandDirection::Both => {
                edges.add_out(neighbor);
                edges.add_in(neighbor);
            }
        }
        txn.put(&key, values::adjacency::encode_edges(&edges))?;
        Ok(())
    }

    #[cfg(test)]
    pub(super) async fn remove_adjacency(
        &self,
        txn: &DbTransaction,
        node: u64,
        neighbor: u64,
        direction: ir::ExpandDirection,
    ) -> Result<()> {
        let key = self.storage_key(keys::DataKeyKind::Adjacency(keys::AdjacencyKey::new(node)));
        let mut edges = decode_stored_edges(txn.get(&key).await?)?;
        let removed = match direction {
            ir::ExpandDirection::Out => edges.remove_out(neighbor),
            ir::ExpandDirection::In => edges.remove_in(neighbor),
            ir::ExpandDirection::Both => {
                let out = edges.remove_out(neighbor);
                let in_ = edges.remove_in(neighbor);
                out || in_
            }
        };
        debug_assert!(
            removed,
            "edge adjacency index was missing node {node} -> neighbor {neighbor}"
        );
        if edges.is_empty() {
            txn.delete(&key)?;
        } else {
            txn.put(&key, values::adjacency::encode_edges(&edges))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use helix_planner::context;
    use slatedb::IsolationLevel;

    use super::super::super::test_support;
    use super::*;

    #[tokio::test]
    async fn bidirectional_adjacency_add_and_partial_remove_preserve_remaining_edges() {
        let db = test_support::open_db("mutation-bidirectional-adjacency").await;
        let context = ExecutionContext::new(&db, context::ParamBindings::default());
        let txn = db
            .inner_db()
            .begin(IsolationLevel::Snapshot)
            .await
            .expect("snapshot transaction begins");

        context
            .add_adjacency(&txn, 1, 2, ir::ExpandDirection::Both)
            .await
            .expect("bidirectional adjacency is added");
        context
            .add_adjacency(&txn, 1, 3, ir::ExpandDirection::Out)
            .await
            .expect("second outgoing adjacency is added");

        let key = context.storage_key(keys::DataKeyKind::Adjacency(keys::AdjacencyKey::new(1)));
        let edges = decode_stored_edges(txn.get(&key).await.unwrap()).unwrap();
        assert!(edges.contains_out(2));
        assert!(edges.contains_in(2));
        assert!(edges.contains_out(3));

        context
            .remove_adjacency(&txn, 1, 2, ir::ExpandDirection::Both)
            .await
            .expect("bidirectional adjacency is removed");

        let edges = decode_stored_edges(txn.get(&key).await.unwrap()).unwrap();
        assert!(!edges.contains_out(2));
        assert!(!edges.contains_in(2));
        assert!(edges.contains_out(3));
    }
}
