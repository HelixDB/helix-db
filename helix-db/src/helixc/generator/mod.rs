use core::fmt;
use std::fmt::Display;

use crate::{
    helix_engine::graph_core::config::Config,
    helixc::generator::{
        migrations::GeneratedMigration,
        queries::Query,
        schemas::{EdgeSchema, NodeSchema, VectorSchema},
        utils::write_headers,
    },
};

pub mod bool_op;
pub mod migrations;
pub mod object_remapping_generation;
pub mod queries;
pub mod return_values;
pub mod schemas;
pub mod source_steps;
pub mod statements;
pub mod traversal_steps;
pub mod tsdisplay;
pub mod utils;

#[cfg(test)]
mod generator_tests;

pub struct Source {
    pub nodes: Vec<NodeSchema>,
    pub edges: Vec<EdgeSchema>,
    pub vectors: Vec<VectorSchema>,
    pub queries: Vec<Query>,
    pub config: Config,
    pub src: String,
    pub migrations: Vec<GeneratedMigration>,
}
impl Default for Source {
    fn default() -> Self {
        Self {
            nodes: vec![],
            edges: vec![],
            vectors: vec![],
            queries: vec![],
            config: Config::default(),
            src: "".to_string(),
            migrations: vec![],
        }
    }
}
impl Display for Source {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "{}", write_headers())?;
        writeln!(f, "{}", self.config)?;
        write!(
            f,
            "{}",
            self.nodes
                .iter()
                .map(|n| format!("{n}"))
                .collect::<Vec<_>>()
                .join("\n")
        )?;
        writeln!(f)?;
        write!(
            f,
            "{}",
            self.edges
                .iter()
                .map(|e| format!("{e}"))
                .collect::<Vec<_>>()
                .join("\n")
        )?;
        writeln!(f)?;
        write!(
            f,
            "{}",
            self.vectors
                .iter()
                .map(|v| format!("{v}"))
                .collect::<Vec<_>>()
                .join("\n")
        )?;
        writeln!(f)?;
        write!(
            f,
            "{}",
            self.queries
                .iter()
                .map(|q| format!("{q}"))
                .collect::<Vec<_>>()
                .join("\n")
        )?;
        writeln!(f)?;
        writeln!(
            f,
            "{}",
            self.migrations
                .iter()
                .map(|m| format!("{m}"))
                .collect::<Vec<_>>()
                .join("\n")
        )?;
        Ok(())
    }
}
