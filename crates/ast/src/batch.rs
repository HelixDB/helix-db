use serde::{Deserialize, Serialize};

use crate::traversal::{AstNode, MutationMode, ReadOnly, Traversal, TraversalState};
/// Condition for conditional batch entries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct ReadBatch {
    /// Batch entries in execution order.
    pub entries: Vec<BatchEntry>,
    /// Variables to return.
    #[serde(default)]
    pub returns: Vec<String>,
}

impl ReadBatch {
    /// Create an empty read batch.
    pub fn new() -> Self {
        Self::default()
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

/// Write-capable query batch.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
