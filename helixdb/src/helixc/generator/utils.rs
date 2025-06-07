use std::{fmt::{self, Debug, Display}};

use crate::helixc::parser::helix_parser::IdType;

#[derive(Clone)]
pub enum GenRef<T>
where
    T: Display,
{
    Literal(T),
    Mut(T),
    Ref(T),
    RefLT(String, T),
    DeRef(T),
    MutRef(T),
    MutRefLT(String, T),
    MutDeRef(T),
    RefLiteral(T),
    Unknown,
    Std(T),
    Id(String),
}

impl<T> Display for GenRef<T>
where
    T: Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GenRef::Literal(t) => write!(f, "\"{}\"", t),
            GenRef::Std(t) => write!(f, "{}", t),
            GenRef::Mut(t) => write!(f, "mut {}", t),
            GenRef::Ref(t) => write!(f, "&{}", t),
            GenRef::RefLT(lifetime_name, t) => write!(f, "&'{} {}", lifetime_name, t),
            GenRef::DeRef(t) => write!(f, "*{}", t),
            GenRef::MutRef(t) => write!(f, "& mut {}", t),
            GenRef::MutRefLT(lifetime_name, t) => write!(f, "&'{} mut {}", lifetime_name, t),
            GenRef::MutDeRef(t) => write!(f, "mut *{}", t),
            GenRef::RefLiteral(t) => write!(f, "ref {}", t),
            GenRef::Unknown => write!(f, ""),
            GenRef::Id(id) => write!(f, "data.{}", id),
        }
    }
}

impl<T> GenRef<T>
where
    T: Display,
{
    pub fn inner(&self) -> &T {
        match self {
            GenRef::Literal(t) => t,
            GenRef::Mut(t) => t,
            GenRef::Ref(t) => t,
            GenRef::RefLT(_, t) => t,
            GenRef::DeRef(t) => t,
            GenRef::MutRef(t) => t,
            GenRef::MutRefLT(_, t) => t,
            GenRef::MutDeRef(t) => t,
            GenRef::RefLiteral(t) => t,
            GenRef::Unknown => panic!("Cannot get inner of unknown"),
            GenRef::Std(t) => t,
            GenRef::Id(_t) => panic!("Cannot get inner of unknown"),
        }
    }
}
impl<T> Debug for GenRef<T>
where
    T: Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GenRef::Literal(t) => write!(f, "Literal({})", t),
            GenRef::Std(t) => write!(f, "Std({})", t),
            GenRef::Mut(t) => write!(f, "Mut({})", t),
            GenRef::Ref(t) => write!(f, "Ref({})", t),
            GenRef::RefLT(lifetime_name, t) => write!(f, "RefLT({}, {})", lifetime_name, t),
            GenRef::DeRef(t) => write!(f, "DeRef({})", t),
            GenRef::MutRef(t) => write!(f, "MutRef({})", t),
            GenRef::MutRefLT(lifetime_name, t) => write!(f, "MutRefLT({}, {})", lifetime_name, t),
            GenRef::MutDeRef(t) => write!(f, "MutDeRef({})", t),
            GenRef::RefLiteral(t) => write!(f, "RefLiteral({})", t),
            GenRef::Unknown => write!(f, "Unknown"),
            GenRef::Id(id) => write!(f, "String({})", id),
        }
    }
}
impl From<GenRef<String>> for String {
    fn from(value: GenRef<String>) -> Self {
        match value {
            GenRef::Literal(s) => format!("\"{}\"", s),
            GenRef::Std(s) => format!("\"{}\"", s),
            GenRef::Ref(s) => format!("\"{}\"", s),
            _ => {
                println!("Cannot convert to string: {:?}", value);
                panic!("Cannot convert to string")
            }
        }
    }
}
impl From<IdType> for GenRef<String> {
    fn from(value: IdType) -> Self {
        match value {
            IdType::Literal { value: s, loc: _ } => GenRef::Literal(s),
            IdType::Identifier { value: s, loc: _ } => GenRef::Id(s),
            _ => panic!("Cannot convert to string: {:?}", value),
        }
    }
}

#[derive(Clone)]
pub enum Order {
    Asc,
    Desc,
}

impl Display for Order {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Order::Asc => write!(f, "Asc"),
            Order::Desc => write!(f, "Desc"),
        }
    }
}

pub fn write_properties(properties: &Option<Vec<(String, GeneratedValue)>>) -> String {
    match properties {
        Some(properties) => format!(
            "Some(props! {{ {} }})",
            properties
                .iter()
                .map(|(name, value)| format!("\"{}\" => {}", name, value))
                .collect::<Vec<String>>()
                .join(", ")
        ),
        None => "None".to_string(),
    }
}

pub fn write_secondary_indices(secondary_indices: &Option<Vec<String>>) -> String {
    match secondary_indices {
        Some(indices) => format!(
            "Some(&[{}])",
            indices
                .iter()
                .map(|idx| format!("\"{}\"", idx))
                .collect::<Vec<String>>()
                .join(", ")
        ),
        None => "None".to_string(),
    }
}

#[derive(Clone)]
pub enum GeneratedValue {
    // needed?
    Literal(GenRef<String>),
    Identifier(GenRef<String>),
    Primitive(GenRef<String>),
    Unknown,
}

impl Display for GeneratedValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GeneratedValue::Literal(value) => write!(f, "{}", value),
            GeneratedValue::Primitive(value) => write!(f, "{}", value),
            GeneratedValue::Identifier(value) => write!(f, "{}", value),
            GeneratedValue::Unknown => write!(f, ""),
        }
    }
}
impl Debug for GeneratedValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GeneratedValue::Literal(value) => write!(f, "Literal({})", value),
            GeneratedValue::Primitive(value) => write!(f, "Primitive({})", value),
            GeneratedValue::Identifier(value) => write!(f, "Identifier({})", value),
            GeneratedValue::Unknown => write!(f, "Unknown"),
        }
    }
}

#[derive(Clone)]
pub enum GeneratedType {
    RustType(RustType),
    Vec(Box<GeneratedType>),
    Object(GenRef<String>),
    Variable(GenRef<String>),
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

#[derive(Clone)]
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
            RustType::Uuid => write!(f, "ID"), // TODO: Change this for actual UUID
            RustType::Date => write!(f, "DateTime<Utc>"),
        }
    }
}
impl RustType {
    pub fn to_ts(&self) -> String {
        let s = match self {
            RustType::String =>  "string",
            RustType::I8 =>  "number",
            RustType::I16 =>  "number",
            RustType::I32 =>  "number",
            RustType::I64 =>  "number",
            RustType::U8 =>  "number",
            RustType::U16 =>  "number",
            RustType::U32 =>  "number",
            RustType::U64 =>  "number",
            RustType::U128 =>  "number",
            RustType::F32 =>  "number",
            RustType::F64 =>  "number",
            RustType::Bool =>  "boolean",
            RustType::Uuid =>  "string", // do thee
            RustType::Date =>  "Date",   // do thee
        };
        s.to_string()
    }
}

#[derive(Clone)]
pub enum Separator<T> {
    Comma(T),
    Semicolon(T),
    Period(T),
    Newline(T),
    Empty(T),
}
impl<T: Display> Display for Separator<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Separator::Comma(t) => write!(f, ",\n{}", t),
            Separator::Semicolon(t) => write!(f, ";\n{}", t),
            Separator::Period(t) => write!(f, "\n.{}", t),
            Separator::Newline(t) => write!(f, "\n{}", t),
            Separator::Empty(t) => write!(f, "{}", t),
        }
    }
}
impl<T: Display> Separator<T> {
    pub fn inner(&self) -> &T {
        match self {
            Separator::Comma(t) => t,
            Separator::Semicolon(t) => t,
            Separator::Period(t) => t,
            Separator::Newline(t) => t,
            Separator::Empty(t) => t,
        }
    }
}
pub fn write_headers() -> String {
    r#"

use heed3::RoTxn;
use get_routes::handler;
use helixdb::{field_remapping, identifier_remapping, traversal_remapping, exclude_field};
use helixdb::helix_engine::vector_core::vector::HVector;
use helixdb::{
    helix_engine::graph_core::ops::{
        g::G,
        in_::{in_::InAdapter, in_e::InEdgesAdapter, to_n::ToNAdapter},
        out::{from_n::FromNAdapter, out::OutAdapter, out_e::OutEdgesAdapter},
        source::{
            add_e::{AddEAdapter, EdgeType},
            add_n::AddNAdapter,
            e_from_id::EFromIdAdapter,
            e_from_type::EFromTypeAdapter,
            n_from_id::NFromIdAdapter,
            n_from_type::NFromTypeAdapter,
            n_from_index::NFromIndexAdapter,
        },
        tr_val::{Traversable, TraversalVal},
        util::{
            dedup::DedupAdapter, filter_mut::FilterMut,
            filter_ref::FilterRefAdapter, range::RangeAdapter, update::UpdateAdapter,
            map::MapAdapter, paths::ShortestPathAdapter, props::PropsAdapter, drop::Drop,
        },
        vectors::{insert::InsertVAdapter, search::SearchVAdapter},
        bm25::search_bm25::SearchBM25Adapter,
    },
    helix_engine::types::GraphError,
    helix_gateway::router::router::HandlerInput,
    node_matches, props,
    protocol::count::Count,
    protocol::remapping::ResponseRemapping,
    protocol::response::Response,
    protocol::traversal_value::TraversalValue,
    protocol::{
        filterable::Filterable, remapping::Remapping, return_values::ReturnValue, value::Value, id::ID,
    },
};
use sonic_rs::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;
use std::cell::RefCell;
use chrono::{DateTime, Utc};
    "#
    .to_string()
}
