// Copyright 2025 HelixDB Inc.
// SPDX-License-Identifier: AGPL-3.0

//! Python code generator for HelixQL
//! Generates Python client code, models, and query functions from HelixQL schemas and queries

use crate::helixc::generator::{
    queries::Query,
    schemas::{EdgeSchema, NodeSchema, VectorSchema},
    utils::{GeneratedType, RustType},
    Source,
};
use std::fmt::Write;

pub struct PythonGenerator<'a> {
    source: &'a Source,
}

impl<'a> PythonGenerator<'a> {
    pub fn new(source: &'a Source) -> Self {
        Self { source }
    }

    /// Generate complete Python package with models and queries
    pub fn generate_package(&self) -> PythonPackage {
        PythonPackage {
            models: self.generate_models(),
            queries: self.generate_queries(),
            client: self.generate_client(),
            init: self.generate_init(),
        }
    }

    /// Generate Pydantic models from schemas
    fn generate_models(&self) -> String {
        let mut output = String::new();

        // Write imports
        writeln!(&mut output, "\"\"\"Generated Pydantic models from HelixQL schemas\"\"\"").unwrap();
        writeln!(&mut output, "from typing import List, Optional, Dict, Any").unwrap();
        writeln!(&mut output, "from pydantic import BaseModel, Field").unwrap();
        writeln!(&mut output, "from datetime import datetime").unwrap();
        writeln!(&mut output).unwrap();

        // Generate node models
        for node in &self.source.nodes {
            writeln!(&mut output, "{}", self.generate_node_model(node)).unwrap();
        }

        // Generate edge models
        for edge in &self.source.edges {
            writeln!(&mut output, "{}", self.generate_edge_model(edge)).unwrap();
        }

        // Generate vector models
        for vector in &self.source.vectors {
            writeln!(&mut output, "{}", self.generate_vector_model(vector)).unwrap();
        }

        output
    }

    /// Generate query functions
    fn generate_queries(&self) -> String {
        let mut output = String::new();

        writeln!(&mut output, "\"\"\"Generated query functions from HelixQL\"\"\"").unwrap();
        writeln!(&mut output, "from typing import List, Optional, Dict, Any, Union").unwrap();
        writeln!(&mut output, "from .client import HelixDBClient").unwrap();

        // Import models if they're used in return types
        if self.has_model_returns() {
            writeln!(&mut output, "from .models import *").unwrap();
        }
        writeln!(&mut output).unwrap();

        // Generate query functions
        for query in &self.source.queries {
            writeln!(&mut output, "{}", self.generate_query_function(query)).unwrap();
        }

        output
    }

    /// Generate client wrapper
    fn generate_client(&self) -> String {
        r#""""HelixDB client wrapper"""
from helix import Client
from typing import Optional
import os


class HelixDBClient:
    """Wrapper for HelixDB client with project-specific configuration"""

    def __init__(
        self,
        local: bool = True,
        port: int = 6969,
        api_endpoint: Optional[str] = None,
        api_key: Optional[str] = None,
        verbose: bool = False,
        max_workers: int = 1
    ):
        """Initialize HelixDB client

        Args:
            local: Whether to connect to local instance
            port: Port for local connection (default: 6969)
            api_endpoint: Remote API endpoint URL
            api_key: API key for remote connection
            verbose: Enable verbose logging
            max_workers: Number of concurrent workers
        """
        self._client = Client(
            local=local,
            port=port,
            api_endpoint=api_endpoint or os.getenv("HELIX_API_ENDPOINT"),
            api_key=api_key or os.getenv("HELIX_API_KEY"),
            verbose=verbose,
            max_workers=max_workers
        )

    @property
    def client(self) -> Client:
        """Get the underlying helix-py client"""
        return self._client

    def query(self, name: str, payload: Optional[Dict] = None) -> Any:
        """Execute a query

        Args:
            name: Query name
            payload: Query parameters

        Returns:
            Query results
        """
        return self._client.query(name, payload)
"#.to_string()
    }

    /// Generate __init__.py
    fn generate_init(&self) -> String {
        let mut output = String::new();
        writeln!(&mut output, "\"\"\"Generated HelixDB Python client package\"\"\"").unwrap();
        writeln!(&mut output, "from .client import HelixDBClient").unwrap();
        writeln!(&mut output, "from .models import *").unwrap();
        writeln!(&mut output, "from .queries import *").unwrap();
        writeln!(&mut output).unwrap();
        writeln!(&mut output, "__version__ = \"0.1.0\"").unwrap();
        writeln!(&mut output, "__all__ = [\"HelixDBClient\"]").unwrap();
        output
    }

    /// Generate a Pydantic model for a node schema
    fn generate_node_model(&self, node: &NodeSchema) -> String {
        let mut output = String::new();
        let name = &node.name;

        writeln!(&mut output, "class {}(BaseModel):", name).unwrap();
        writeln!(&mut output, "    \"\"\"Generated model for {} node\"\"\"", name).unwrap();

        if node.properties.is_empty() {
            writeln!(&mut output, "    pass").unwrap();
        } else {
            for property in &node.properties {
                let field_name = &property.name;
                let field_type = self.map_generated_type_to_python(&property.field_type);
                writeln!(&mut output, "    {}: {}", field_name, field_type).unwrap();
            }
        }

        writeln!(&mut output).unwrap();
        output
    }

    /// Generate a Pydantic model for an edge schema
    fn generate_edge_model(&self, edge: &EdgeSchema) -> String {
        let mut output = String::new();
        let name = &edge.name;

        writeln!(&mut output, "class {}(BaseModel):", name).unwrap();
        writeln!(&mut output, "    \"\"\"Generated model for {} edge\"\"\"", name).unwrap();
        writeln!(&mut output, "    from_node: str").unwrap();
        writeln!(&mut output, "    to_node: str").unwrap();

        for property in &edge.properties {
            let field_name = &property.name;
            let field_type = self.map_generated_type_to_python(&property.field_type);
            writeln!(&mut output, "    {}: {}", field_name, field_type).unwrap();
        }

        writeln!(&mut output).unwrap();
        output
    }

    /// Generate a Pydantic model for a vector schema
    fn generate_vector_model(&self, vector: &VectorSchema) -> String {
        let mut output = String::new();
        let name = &vector.name;

        writeln!(&mut output, "class {}(BaseModel):", name).unwrap();
        writeln!(&mut output, "    \"\"\"Generated model for {} vector\"\"\"", name).unwrap();

        // Vectors have embedding data and properties
        writeln!(&mut output, "    embedding: List[float]").unwrap();

        for property in &vector.properties {
            let field_name = &property.name;
            let field_type = self.map_generated_type_to_python(&property.field_type);
            writeln!(&mut output, "    {}: {}", field_name, field_type).unwrap();
        }

        writeln!(&mut output).unwrap();
        output
    }

    /// Generate a query function
    fn generate_query_function(&self, query: &Query) -> String {
        let mut output = String::new();
        let name = &query.name;

        // Generate function signature
        let params = self.generate_function_params(query);
        let return_type = self.generate_return_type(query);

        writeln!(&mut output, "def {}(client: HelixDBClient{}) -> {}:",
            name, params, return_type).unwrap();

        // Generate docstring
        writeln!(&mut output, "    \"\"\"Generated query: {}\"\"\"", name).unwrap();

        // Generate query execution
        if query.parameters.is_empty() {
            writeln!(&mut output, "    return client.query(\"{}\")", name).unwrap();
        } else {
            writeln!(&mut output, "    payload = {{").unwrap();
            for param in &query.parameters {
                writeln!(&mut output, "        \"{}\": {},", param.name, param.name).unwrap();
            }
            writeln!(&mut output, "    }}").unwrap();
            writeln!(&mut output, "    return client.query(\"{}\", payload)", name).unwrap();
        }

        writeln!(&mut output).unwrap();
        output
    }

    /// Generate function parameters
    fn generate_function_params(&self, query: &Query) -> String {
        if query.parameters.is_empty() {
            return String::new();
        }

        let mut params = String::new();
        for param in &query.parameters {
            let param_type = self.map_generated_type_to_python(&param.field_type);
            write!(&mut params, ", {}: {}", param.name, param_type).unwrap();
        }
        params
    }

    /// Generate return type annotation
    fn generate_return_type(&self, _query: &Query) -> String {
        // For now, return Any - can be improved with better type inference
        "Any".to_string()
    }

    /// Map GeneratedType to Python type
    fn map_generated_type_to_python(&self, gen_type: &GeneratedType) -> String {
        match gen_type {
            GeneratedType::RustType(rust_type) => self.map_rust_type_to_python(rust_type),
            GeneratedType::Vec(inner) => {
                format!("List[{}]", self.map_generated_type_to_python(inner))
            },
            GeneratedType::Object(name) => name.to_string(),
            GeneratedType::Variable(name) => name.to_string(),
        }
    }

    /// Map RustType enum to Python types
    fn map_rust_type_to_python(&self, rust_type: &RustType) -> String {
        match rust_type {
            RustType::String => "str".to_string(),
            RustType::I8 | RustType::I16 | RustType::I32 | RustType::I64 => "int".to_string(),
            RustType::U8 | RustType::U16 | RustType::U32 | RustType::U64 | RustType::U128 => "int".to_string(),
            RustType::F32 | RustType::F64 => "float".to_string(),
            RustType::Bool => "bool".to_string(),
            RustType::Uuid => "str".to_string(),
            RustType::Date => "datetime".to_string(),
        }
    }

    /// Check if any queries return model types
    fn has_model_returns(&self) -> bool {
        // For now, assume true if we have any models
        !self.source.nodes.is_empty() || !self.source.edges.is_empty() || !self.source.vectors.is_empty()
    }
}

/// Represents a complete Python package
pub struct PythonPackage {
    pub models: String,
    pub queries: String,
    pub client: String,
    pub init: String,
}