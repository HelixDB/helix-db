// Copyright 2025 HelixDB Inc.
// SPDX-License-Identifier: AGPL-3.0

//! HelixQL compiler, analyzer, and transpiler.

pub mod analyzer;
pub mod generator;
pub mod parser;

#[cfg(test)]
mod parity_snapshots;
