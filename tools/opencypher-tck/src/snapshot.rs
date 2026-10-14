//! Observable side effects measured through the native frontend, independently
//! of Cypher execution, counters, matching, and result serialization.
use crate::corpus::Result;
use helix_ast::{batch, graph, query, traversal};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Default, Debug, PartialEq)]
pub struct Snapshot {
    nodes: BTreeSet<u64>,
    relationships: BTreeSet<u64>,
    labels: BTreeSet<String>,
    properties: BTreeSet<(bool, u64, String, String)>,
}

pub async fn take(db: &db::HelixDB) -> Result<Snapshot> {
    let request = query::QueryRequest::read(
        batch::read_batch()
            .var_as(
                "nodes",
                traversal::g()
                    .n(graph::NodeRef::all())
                    .value_map(None::<Vec<String>>),
            )
            .var_as(
                "relationships",
                traversal::g()
                    .e(graph::EdgeRef::all())
                    .value_map(None::<Vec<String>>),
            )
            .returning(["nodes", "relationships"]),
    );
    let result = db.query(request).await?;
    let mut snapshot = Snapshot::default();
    for (name, is_node) in [("nodes", true), ("relationships", false)] {
        for row in result[name].as_array().ok_or("snapshot is not an array")? {
            let row = row.as_object().ok_or("snapshot row is not an object")?;
            let id = row
                .get("$id")
                .and_then(serde_json::Value::as_u64)
                .ok_or("snapshot entity lacks lossless ID")?;
            if is_node {
                snapshot.nodes.insert(id);
            } else {
                snapshot.relationships.insert(id);
            }
            for (key, value) in row {
                if key == "$label" && is_node {
                    snapshot
                        .labels
                        .insert(value.as_str().ok_or("invalid stored label")?.into());
                }
                if !key.starts_with('$') && !value.is_null() {
                    snapshot.properties.insert((
                        is_node,
                        id,
                        key.clone(),
                        serde_json::to_string(value)?,
                    ));
                }
            }
        }
    }
    Ok(snapshot)
}

impl Snapshot {
    pub fn changes(&self, after: &Self) -> BTreeMap<String, usize> {
        BTreeMap::from([
            ("+nodes".into(), after.nodes.difference(&self.nodes).count()),
            ("-nodes".into(), self.nodes.difference(&after.nodes).count()),
            (
                "+relationships".into(),
                after.relationships.difference(&self.relationships).count(),
            ),
            (
                "-relationships".into(),
                self.relationships.difference(&after.relationships).count(),
            ),
            (
                "+labels".into(),
                after.labels.difference(&self.labels).count(),
            ),
            (
                "-labels".into(),
                self.labels.difference(&after.labels).count(),
            ),
            (
                "+properties".into(),
                after.properties.difference(&self.properties).count(),
            ),
            (
                "-properties".into(),
                self.properties.difference(&after.properties).count(),
            ),
        ])
    }
}

#[cfg(test)]
#[path = "tests/snapshot.rs"]
mod tests;
