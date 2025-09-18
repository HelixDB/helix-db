// Copyright 2025 HelixDB Inc.
// SPDX-License-Identifier: AGPL-3.0

//! This is the generator for HelixQL. It transforms the AST into Rust code.
//! The generator methods are broken up into separate files, grouped by general functionality.
//! File names should be self-explanatory as to what is included in the file.

use crate::{
    helix_engine::traversal_core::config::Config,
    helixc::generator::{
        migrations::GeneratedMigration,
        queries::Query,
        schemas::{EdgeSchema, NodeSchema, VectorSchema},
        utils::write_headers,
    },
};
use core::fmt;
use std::io::Write;
use std::{fmt::Display, fs::File, io::Result, path::Path};

pub mod bool_ops;
pub mod migrations;
pub mod object_remappings;
pub mod queries;
pub mod return_values;
pub mod schemas;
pub mod source_steps;
pub mod statements;
pub mod traversal_steps;
pub mod tsdisplay;
pub mod utils;

/// Source is analyzed source
/// Path is directory to place the generated files
pub fn generate(source: Source, path: &Path) -> Result<()> {
    let mut file = File::create(path.join("queries.rs"))?;
    write!(file, "{source}")?;
    Ok(())
}

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
