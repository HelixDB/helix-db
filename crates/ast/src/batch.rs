use helix_ast_arena_derive::ArenaMirror;
use serde::{Deserialize, Deserializer, Serialize};

use crate::arena::ArenaDeserialize;
use crate::traversal::{ArenaAstNode, AstNode, MutationMode, ReadOnly, Traversal, TraversalState};
/// Condition for conditional batch entries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ArenaMirror)]
#[serde(rename_all = "snake_case")]
pub enum BatchCondition {
    /// Variable is not empty.
    VarNotEmpty(String),
    /// Variable is empty.
    VarEmpty(String),
    /// Variable has at least this size.
    VarMinSize(String, usize),
    /// Previous query result was not empty.
    PrevNotEmpty,
}

/// A named batch query.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ArenaMirror)]
pub struct NamedQuery {
    /// Variable name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Traversal root.
    pub root: AstNode,
    /// Optional condition.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub condition: Option<BatchCondition>,
}

/// Batch entry.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ArenaMirror)]
#[serde(rename_all = "snake_case")]
pub enum BatchEntry {
    /// Single query.
    Query(Box<NamedQuery>),
    /// Execute body once per object in a parameter array.
    ForEach {
        /// Top-level parameter.
        param: String,
        /// Body entries.
        body: Vec<BatchEntry>,
    },
}

/// Read-only query batch.
#[derive(Debug, Clone, PartialEq, Default, Serialize, ArenaMirror)]
#[arena(manual_deserialize)]
pub struct ReadBatch {
    /// Batch entries in execution order.
    entries: Vec<BatchEntry>,
    /// Variables to return.
    #[serde(default)]
    returns: Vec<String>,
}

/// A read batch contained a persistent mutation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadBatchError {
    entry_path: String,
}

impl ReadBatchError {
    fn mutation(entry_path: String) -> Self {
        Self { entry_path }
    }
}

impl std::fmt::Display for ReadBatchError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "read batch entry '{}' contains a persistent mutation",
            self.entry_path
        )
    }
}

impl std::error::Error for ReadBatchError {}

#[derive(Deserialize, ArenaMirror)]
struct RawReadBatch {
    entries: Vec<BatchEntry>,
    #[serde(default)]
    returns: Vec<String>,
}

impl ReadBatch {
    /// Create an empty read batch.
    pub fn new() -> Self {
        Self::default()
    }

    /// Construct a read batch from raw wire-compatible parts.
    ///
    /// Every query root, including nested `for_each` bodies and branch
    /// traversals, is checked before the batch becomes representable.
    pub fn try_from_parts(
        entries: Vec<BatchEntry>,
        returns: Vec<String>,
    ) -> Result<Self, ReadBatchError> {
        validate_read_entries(&entries, "entries")?;
        Ok(Self { entries, returns })
    }

    /// Construct an unchecked batch for downstream raw-AST contract tests.
    ///
    /// This escape hatch is unavailable in normal builds.
    #[cfg(feature = "test-utils")]
    #[doc(hidden)]
    pub fn from_parts_unchecked_for_tests(entries: Vec<BatchEntry>, returns: Vec<String>) -> Self {
        Self { entries, returns }
    }

    /// Batch entries in execution order.
    pub fn entries(&self) -> &[BatchEntry] {
        &self.entries
    }

    /// Variables returned by this batch.
    pub fn returns(&self) -> &[String] {
        &self.returns
    }

    /// Add a named read-only traversal.
    pub fn var_as<S: TraversalState>(
        mut self,
        name: &str,
        traversal: Traversal<S, ReadOnly>,
    ) -> Self {
        self.entries.push(BatchEntry::Query(Box::new(NamedQuery {
            name: Some(name.to_string()),
            root: traversal.into_ast(),
            condition: None,
        })));
        self
    }

    /// Add a conditional named read-only traversal.
    pub fn var_as_if<S: TraversalState>(
        mut self,
        name: &str,
        condition: BatchCondition,
        traversal: Traversal<S, ReadOnly>,
    ) -> Self {
        self.entries.push(BatchEntry::Query(Box::new(NamedQuery {
            name: Some(name.to_string()),
            root: traversal.into_ast(),
            condition: Some(condition),
        })));
        self
    }

    /// Add a for-each body.
    pub fn for_each_param(mut self, param: &str, body: ReadBatch) -> Self {
        self.entries.push(BatchEntry::ForEach {
            param: param.to_string(),
            body: body.entries,
        });
        self
    }

    /// Set returned variables.
    pub fn returning<I, S>(mut self, vars: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.returns = vars.into_iter().map(Into::into).collect();
        self
    }
}

impl<'de> Deserialize<'de> for ReadBatch {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = RawReadBatch::deserialize(deserializer)?;
        Self::try_from_parts(raw.entries, raw.returns).map_err(serde::de::Error::custom)
    }
}

fn validate_read_entries(entries: &[BatchEntry], path: &str) -> Result<(), ReadBatchError> {
    entries
        .iter()
        .enumerate()
        .try_for_each(|(index, entry)| match entry {
            BatchEntry::Query(query) if query.root.is_read_only() => Ok(()),
            BatchEntry::Query(_) => Err(ReadBatchError::mutation(format!("{path}[{index}]"))),
            BatchEntry::ForEach { body, .. } => {
                validate_read_entries(body, &format!("{path}[{index}].for_each"))
            }
        })
}

impl<'a> ArenaReadBatch<'a> {
    /// [`ReadBatch::try_from_parts`] for arena parts: every query root,
    /// including nested `for_each` bodies and branch traversals, is checked
    /// before the batch becomes representable.
    fn try_from_parts(
        entries: &'a [ArenaBatchEntry<'a>],
        returns: &'a [&'a str],
    ) -> Result<Self, ReadBatchError> {
        validate_arena_read_entries(entries, "entries")?;
        Ok(Self { entries, returns })
    }

    /// Batch entries in execution order.
    pub fn entries(&self) -> &'a [ArenaBatchEntry<'a>] {
        self.entries
    }

    /// Variables returned by this batch.
    pub fn returns(&self) -> &'a [&'a str] {
        self.returns
    }
}

/// [`validate_read_entries`] over arena entries, with the same paths.
fn validate_arena_read_entries(
    entries: &[ArenaBatchEntry<'_>],
    path: &str,
) -> Result<(), ReadBatchError> {
    entries
        .iter()
        .enumerate()
        .try_for_each(|(index, entry)| match entry {
            ArenaBatchEntry::Query(query) if query.root.is_read_only() => Ok(()),
            ArenaBatchEntry::Query(_) => Err(ReadBatchError::mutation(format!("{path}[{index}]"))),
            ArenaBatchEntry::ForEach { body, .. } => {
                validate_arena_read_entries(body, &format!("{path}[{index}].for_each"))
            }
        })
}

/// The arena counterpart of `ReadBatch`'s `Deserialize`: the raw batch, then
/// the read-only check.
impl<'a> ArenaDeserialize<'a> for ArenaReadBatch<'a> {
    fn deserialize_in<'de, D: Deserializer<'de>>(
        bump: &'a crate::arena::Bump,
        deserializer: D,
    ) -> Result<Self, D::Error> {
        let raw = ArenaRawReadBatch::deserialize_in(bump, deserializer)?;
        Self::try_from_parts(raw.entries, raw.returns).map_err(serde::de::Error::custom)
    }
}

/// Write-capable query batch.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize, ArenaMirror)]
pub struct WriteBatch {
    /// Batch entries in execution order.
    pub entries: Vec<BatchEntry>,
    /// Variables to return.
    #[serde(default)]
    pub returns: Vec<String>,
}

impl WriteBatch {
    /// Create an empty write batch.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a named traversal.
    pub fn var_as<S: TraversalState, M: MutationMode>(
        mut self,
        name: &str,
        traversal: Traversal<S, M>,
    ) -> Self {
        self.entries.push(BatchEntry::Query(Box::new(NamedQuery {
            name: Some(name.to_string()),
            root: traversal.into_ast(),
            condition: None,
        })));
        self
    }

    /// Add a conditional named traversal.
    pub fn var_as_if<S: TraversalState, M: MutationMode>(
        mut self,
        name: &str,
        condition: BatchCondition,
        traversal: Traversal<S, M>,
    ) -> Self {
        self.entries.push(BatchEntry::Query(Box::new(NamedQuery {
            name: Some(name.to_string()),
            root: traversal.into_ast(),
            condition: Some(condition),
        })));
        self
    }

    /// Add a for-each body.
    pub fn for_each_param(mut self, param: &str, body: WriteBatch) -> Self {
        self.entries.push(BatchEntry::ForEach {
            param: param.to_string(),
            body: body.entries,
        });
        self
    }

    /// Set returned variables.
    pub fn returning<I, S>(mut self, vars: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.returns = vars.into_iter().map(Into::into).collect();
        self
    }
}

/// Batch query payload.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ArenaMirror)]
#[serde(rename_all = "snake_case")]
pub enum BatchQuery {
    /// Read-only batch.
    Read(ReadBatch),
    /// Write-capable batch.
    Write(WriteBatch),
}
/// Create a read batch.
pub fn read_batch() -> ReadBatch {
    ReadBatch::new()
}

/// Create a write batch.
pub fn write_batch() -> WriteBatch {
    WriteBatch::new()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::Predicate;
    use crate::graph::{EdgeRef, NodeRef};
    use crate::index::IndexSpec;
    use crate::traversal::{EmitBehavior, RepeatConfig, SubTraversal};
    use crate::value::PropertyInput;

    fn query(root: AstNode) -> BatchEntry {
        BatchEntry::Query(Box::new(NamedQuery {
            name: Some("result".to_owned()),
            root,
            condition: None,
        }))
    }

    fn nodes() -> AstNode {
        AstNode::Nodes {
            reference: NodeRef::All,
        }
    }

    fn branch(root: AstNode) -> SubTraversal {
        SubTraversal {
            root: Box::new(root),
        }
    }

    fn mutation_families() -> Vec<(&'static str, AstNode)> {
        vec![
            (
                "create_index",
                AstNode::CreateIndex {
                    spec: IndexSpec::node_equality("User", "email"),
                    if_not_exists: true,
                },
            ),
            (
                "drop_index",
                AstNode::DropIndex {
                    spec: IndexSpec::node_equality("User", "email"),
                },
            ),
            (
                "retry_index_operation",
                AstNode::RetryIndexOperation {
                    operation_id: "00000000-0000-0000-0000-000000000001".to_owned(),
                },
            ),
            (
                "abort_index_operation",
                AstNode::AbortIndexOperation {
                    operation_id: "00000000-0000-0000-0000-000000000001".to_owned(),
                },
            ),
            (
                "add_node",
                AstNode::AddN {
                    input: None,
                    label: "User".to_owned(),
                    properties: Vec::new(),
                },
            ),
            (
                "add_edge",
                AstNode::AddE {
                    input: Box::new(nodes()),
                    label: "KNOWS".to_owned(),
                    to: NodeRef::id(2),
                    properties: Vec::new(),
                },
            ),
            (
                "set_property",
                AstNode::SetProperty {
                    input: Box::new(nodes()),
                    name: "active".to_owned(),
                    value: PropertyInput::from(true),
                },
            ),
            (
                "remove_property",
                AstNode::RemoveProperty {
                    input: Box::new(nodes()),
                    name: "active".to_owned(),
                },
            ),
            (
                "drop_node",
                AstNode::Drop {
                    input: Box::new(nodes()),
                },
            ),
            (
                "drop_edge",
                AstNode::DropEdge {
                    input: Box::new(nodes()),
                    to: NodeRef::id(2),
                },
            ),
            (
                "drop_labeled_edge",
                AstNode::DropEdgeLabeled {
                    input: Box::new(nodes()),
                    to: NodeRef::id(2),
                    label: "KNOWS".to_owned(),
                },
            ),
            (
                "drop_edge_by_id",
                AstNode::DropEdgeById {
                    input: None,
                    edges: EdgeRef::id(1),
                },
            ),
        ]
    }

    fn nested_positions(mutation: &AstNode) -> Vec<(&'static str, AstNode)> {
        let condition = Predicate::eq("active", true);
        vec![
            (
                "unary_input",
                AstNode::Count {
                    input: Box::new(mutation.clone()),
                },
            ),
            (
                "repeat_body",
                AstNode::Repeat {
                    input: Box::new(nodes()),
                    config: RepeatConfig {
                        traversal: branch(mutation.clone()),
                        times: Some(1),
                        until: None,
                        emit: EmitBehavior::None,
                        emit_predicate: None,
                        max_depth: 1,
                    },
                },
            ),
            (
                "union_branch",
                AstNode::Union {
                    input: Box::new(nodes()),
                    traversals: vec![branch(AstNode::Context), branch(mutation.clone())],
                },
            ),
            (
                "coalesce_branch",
                AstNode::Coalesce {
                    input: Box::new(nodes()),
                    traversals: vec![branch(AstNode::Context), branch(mutation.clone())],
                },
            ),
            (
                "choose_then",
                AstNode::Choose {
                    input: Box::new(nodes()),
                    condition: condition.clone(),
                    then_traversal: branch(mutation.clone()),
                    else_traversal: Some(branch(AstNode::Context)),
                },
            ),
            (
                "choose_else",
                AstNode::Choose {
                    input: Box::new(nodes()),
                    condition,
                    then_traversal: branch(AstNode::Context),
                    else_traversal: Some(branch(mutation.clone())),
                },
            ),
            (
                "optional_branch",
                AstNode::Optional {
                    input: Box::new(nodes()),
                    traversal: branch(mutation.clone()),
                },
            ),
        ]
    }

    #[test]
    fn read_batch_rejects_every_mutation_family_at_root_and_nested_positions() {
        for (mutation_name, mutation) in mutation_families() {
            assert!(
                ReadBatch::try_from_parts(vec![query(mutation.clone())], Vec::new()).is_err(),
                "{mutation_name} must be rejected at the root"
            );

            for (position, nested) in nested_positions(&mutation) {
                assert!(
                    ReadBatch::try_from_parts(vec![query(nested)], Vec::new()).is_err(),
                    "{mutation_name} must be rejected at {position}"
                );
            }

            assert!(
                ReadBatch::try_from_parts(
                    vec![BatchEntry::ForEach {
                        param: "items".to_owned(),
                        body: vec![query(mutation)],
                    }],
                    Vec::new(),
                )
                .is_err(),
                "{mutation_name} must be rejected in a for_each body"
            );
        }
    }

    #[test]
    fn read_batch_accepts_recursive_read_only_positions() {
        let safe_branch = || {
            branch(AstNode::Count {
                input: Box::new(AstNode::Context),
            })
        };
        let entries = vec![
            query(AstNode::Repeat {
                input: Box::new(nodes()),
                config: RepeatConfig::new(safe_branch()).times(1),
            }),
            query(AstNode::Union {
                input: Box::new(nodes()),
                traversals: vec![safe_branch()],
            }),
            query(AstNode::Coalesce {
                input: Box::new(nodes()),
                traversals: vec![safe_branch()],
            }),
            query(AstNode::Choose {
                input: Box::new(nodes()),
                condition: Predicate::eq("active", true),
                then_traversal: safe_branch(),
                else_traversal: Some(safe_branch()),
            }),
            query(AstNode::Optional {
                input: Box::new(nodes()),
                traversal: safe_branch(),
            }),
            BatchEntry::ForEach {
                param: "items".to_owned(),
                body: vec![query(nodes())],
            },
        ];

        let batch = ReadBatch::try_from_parts(entries, vec!["result".to_owned()])
            .expect("read-only recursive positions should be accepted");
        assert_eq!(batch.entries().len(), 6);
        assert_eq!(batch.returns(), ["result"]);
    }
}
