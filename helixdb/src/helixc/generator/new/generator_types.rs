use core::fmt;
use std::{collections::HashMap, fmt::Display};

use crate::protocol::value::Value;

use super::{traversal_steps::Traversal, types::GenRef};

pub struct Source {
    pub nodes: Vec<NodeSchema>,
    pub edges: Vec<EdgeSchema>,
    pub vectors: Vec<VectorSchema>,
    pub queries: Vec<Query>,
}
impl Default for Source {
    fn default() -> Self {
        Self {
            nodes: vec![],
            edges: vec![],
            vectors: vec![],
            queries: vec![],
        }
    }
}
impl Display for Source {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}\n", write_headers())?;
        write!(
            f,
            "{}",
            self.nodes
                .iter()
                .map(|n| format!("{}", n))
                .collect::<Vec<_>>()
                .join("\n")
        )?;
        write!(f, "\n")?;
        write!(
            f,
            "{}",
            self.edges
                .iter()
                .map(|e| format!("{}", e))
                .collect::<Vec<_>>()
                .join("\n")
        )?;
        write!(f, "\n")?;
        write!(
            f,
            "{}",
            self.vectors
                .iter()
                .map(|v| format!("{}", v))
                .collect::<Vec<_>>()
                .join("\n")
        )?;
        write!(f, "\n")?;
        write!(
            f,
            "{}",
            self.queries
                .iter()
                .map(|q| format!("{}", q))
                .collect::<Vec<_>>()
                .join("\n")
        )
    }
}

pub struct NodeSchema {
    pub name: String,
    pub properties: Vec<(String, GeneratedType)>,
}
impl Display for NodeSchema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "pub struct {} {{\n", self.name)?;
        for (name, ty) in &self.properties {
            write!(f, "    pub {}: {},\n", name, ty)?;
        }
        write!(f, "}}\n")
    }
}

pub struct EdgeSchema {
    pub name: String,
    pub from: String,
    pub to: String,
    pub properties: Vec<(String, GeneratedType)>,
}
impl Display for EdgeSchema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "pub struct {} {{\n", self.name)?;
        write!(f, "    pub from: {},\n", self.from)?;
        write!(f, "    pub to: {},\n", self.to)?;
        for (name, ty) in &self.properties {
            write!(f, "    pub {}: {},\n", name, ty)?;
        }
        write!(f, "}}\n")
    }
}

pub struct VectorSchema {
    pub name: String,
    pub properties: Vec<(String, GeneratedType)>,
}
impl Display for VectorSchema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "pub struct {} {{\n", self.name)?;
        for (name, ty) in &self.properties {
            write!(f, "    pub {}: {},\n", name, ty)?;
        }
        write!(f, "}}\n")
    }
}

pub struct Query {
    pub name: String,
    pub statements: Vec<Statement>,
    pub parameters: Vec<Parameter>, // iterate through and print each one
    pub sub_parameters: Vec<(String, Vec<Parameter>)>,
}
impl Display for Query {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // prints sub parameter structs (e.g. (docs: {doc: String, id: String}))
        for (name, parameters) in &self.sub_parameters {
            write!(f, "pub struct {} {{\n", name)?;
            for parameter in parameters {
                write!(f, "    pub {}: {},\n", parameter.name, parameter.field_type)?;
            }
            write!(f, "}}\n")?;
        }
        // prints top level parameters (e.g. (docs: {doc: String, id: String}))
        write!(
            f,
            "{}",
            self.parameters
                .iter()
                .map(|p| format!("{}", p))
                .collect::<Vec<_>>()
                .join("\n")
        )?;
        write!(f, "#[handler]\n")?; // Handler macro

        // prints the function signature
        write!(f, "pub struct (input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {{\n")?;

        // prints each statement
        for statement in &self.statements {
            write!(f, "    {}\n", statement)?;
        }
        // closes the handler function
        write!(f, "}}\n")
    }
}
impl Default for Query {
    fn default() -> Self {
        Self {
            name: "".to_string(),
            statements: vec![],
            parameters: vec![],
            sub_parameters: vec![],
        }
    }
}

pub struct Parameter {
    pub name: String,
    pub field_type: GeneratedType,
}
impl Display for Parameter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "pub {}: {}", self.name, self.field_type)
    }
}

pub enum Statement {
    Assignment(Assignment),
    Drop(Drop),
    Traversal(Traversal),
    ForEach(ForEach),
}
impl Display for Statement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Statement::Assignment(assignment) => write!(f, "{}", assignment),
            Statement::Drop(drop) => write!(f, "{}", drop),
            Statement::Traversal(traversal) => write!(f, "{}", traversal),
            Statement::ForEach(foreach) => write!(f, "{}", foreach),
        }
    }
}
pub struct Assignment {
    pub variable: String,
    pub value: Traversal,
}
impl Display for Assignment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "let {} = {};", self.variable, self.value)
    }
}

pub struct ForEach {
    // TODO: IMPLEMENT
}
impl Display for ForEach {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self)
    }
}

pub struct Drop {
    pub expression: Traversal,
}
impl Display for Drop {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.drop()", self.expression)
    }
}

/// Boolean expression is used for a traversal or set of traversals wrapped in AND/OR
/// that resolve to a boolean value
pub enum BoExp {
    And(Vec<Traversal>),
    Or(Vec<Traversal>),
    // Not(Traversal),
    // Eq(Traversal, Traversal),
    Expr(Traversal),
}
impl Display for BoExp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BoExp::And(traversals) => {
                let tr = traversals
                    .iter()
                    .map(|s| format!("{}", s))
                    .collect::<Vec<_>>();
                write!(f, "{}", tr.join(" && "))
            }
            BoExp::Or(traversals) => {
                let tr = traversals
                    .iter()
                    .map(|s| format!("{}", s))
                    .collect::<Vec<_>>();
                write!(f, "{}", tr.join(" || "))
            }
            BoExp::Expr(traversal) => write!(f, "{}", traversal),
        }
    }
}

pub enum GeneratedValue {
    // needed?
    Literal(GenRef<String>),
}

impl Display for GeneratedValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GeneratedValue::Literal(value) => write!(f, "{}", value),
        }
    }
}

pub enum GeneratedType {
    RustType(RustType),
    Vec(Box<GeneratedType>),
    Object(String),
    Variable(String),
}

impl Display for GeneratedType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GeneratedType::RustType(t) => write!(f, "{}", t),
            GeneratedType::Vec(t) => write!(f, "Vec<{}>", t),
            GeneratedType::Variable(v) => write!(f, "{}", v),
            GeneratedType::Object(o) => write!(f, "{}", o),
        }
    }
}

pub enum RustType {
    String,
    I8,
    I16,
    I32,
    I64,
    U8,
    U16,
    U32,
    U64,
    U128,
    F32,
    F64,
    Bool,
    Uuid,
    Date,
}
impl Display for RustType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RustType::String => write!(f, "String"),
            RustType::I8 => write!(f, "i8"),
            RustType::I16 => write!(f, "i16"),
            RustType::I32 => write!(f, "i32"),
            RustType::I64 => write!(f, "i64"),
            RustType::U8 => write!(f, "u8"),
            RustType::U16 => write!(f, "u16"),
            RustType::U32 => write!(f, "u32"),
            RustType::U64 => write!(f, "u64"),
            RustType::U128 => write!(f, "u128"),
            RustType::F32 => write!(f, "f32"),
            RustType::F64 => write!(f, "f64"),
            RustType::Bool => write!(f, "bool"),
            RustType::Uuid => unimplemented!(),
            RustType::Date => unimplemented!(),
        }
    }
}

pub enum Separator<T> {
    Comma(T),
    Semicolon(T),
    Period(T),
}
impl<T: Display> Display for Separator<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Separator::Comma(t) => write!(f, "{}", t),
            Separator::Semicolon(t) => write!(f, "{}", t),
            Separator::Period(t) => write!(f, "{}", t),
        }
    }
}

fn write_headers() -> String {
    r#"
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use get_routes::handler;
use helixdb::{field_remapping, traversal_remapping};
use helixdb::helix_engine::graph_core::ops::util::map::MapAdapter;
use helixdb::helix_engine::vector_core::vector::HVector;
use helixdb::{
    helix_engine::graph_core::ops::{
        g::G,
        in_::{in_::InAdapter, in_e::InEdgesAdapter, to_n::ToNAdapter},
        out::{from_n::FromNAdapter, out::OutAdapter, out_e::OutEdgesAdapter},
        source::{
            add_e::{AddEAdapter, EdgeType},
            add_n::AddNAdapter,
            e::EAdapter,
            e_from_id::EFromId,
            e_from_types::EFromTypes,
            n::NAdapter,
            n_from_id::NFromId,
            n_from_types::NFromTypesAdapter,
        },
        tr_val::{Traversable, TraversalVal},
        util::{
            dedup::DedupAdapter, drop::DropAdapter, filter_mut::FilterMut,
            filter_ref::FilterRefAdapter, range::RangeAdapter, update::Update,
        },
        vectors::{insert::InsertVAdapter, search::SearchVAdapter},
    },
    helix_engine::types::GraphError,
    helix_gateway::router::router::HandlerInput,
    node_matches, props,
    protocol::count::Count,
    protocol::remapping::ResponseRemapping,
    protocol::response::Response,
    protocol::traversal_value::TraversalValue,
    protocol::{
        filterable::Filterable, remapping::Remapping, return_values::ReturnValue, value::Value,
    },
};
use sonic_rs::{Deserialize, Serialize};
    "#
    .to_string()
}
