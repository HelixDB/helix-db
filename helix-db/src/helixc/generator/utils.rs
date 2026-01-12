use crate::helixc::{generator::traversal_steps::Traversal, parser::types::IdType};
use std::fmt::{self, Debug, Display};

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
            GenRef::Literal(t) => write!(f, "\"{t}\""),
            GenRef::Std(t) => write!(f, "{t}"),
            GenRef::Mut(t) => write!(f, "mut {t}"),
            GenRef::Ref(t) => write!(f, "&{t}"),
            GenRef::RefLT(lifetime_name, t) => write!(f, "&'{lifetime_name} {t}"),
            GenRef::DeRef(t) => write!(f, "*{t}"),
            GenRef::MutRef(t) => write!(f, "& mut {t}"),
            GenRef::MutRefLT(lifetime_name, t) => write!(f, "&'{lifetime_name} mut {t}"),
            GenRef::MutDeRef(t) => write!(f, "mut *{t}"),
            GenRef::RefLiteral(t) => write!(f, "ref {t}"),
            GenRef::Unknown => write!(f, ""),
            GenRef::Id(id) => write!(f, "data.{id}"),
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
            GenRef::Unknown => {
                // This should have been caught during analysis
                // For now, panic with a descriptive message
                panic!(
                    "Code generation error: Unknown reference type encountered. This indicates a bug in the analyzer."
                )
            }
            GenRef::Std(t) => t,
            GenRef::Id(_) => {
                // Id doesn't have an inner T, it's just a String identifier
                panic!(
                    "Code generation error: Cannot get inner value of Id type. Use the identifier directly."
                )
            }
        }
    }
}
impl<T> Debug for GenRef<T>
where
    T: Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GenRef::Literal(t) => write!(f, "Literal({t})"),
            GenRef::Std(t) => write!(f, "Std({t})"),
            GenRef::Mut(t) => write!(f, "Mut({t})"),
            GenRef::Ref(t) => write!(f, "Ref({t})"),
            GenRef::RefLT(lifetime_name, t) => write!(f, "RefLT({lifetime_name}, {t})"),
            GenRef::DeRef(t) => write!(f, "DeRef({t})"),
            GenRef::MutRef(t) => write!(f, "MutRef({t})"),
            GenRef::MutRefLT(lifetime_name, t) => write!(f, "MutRefLT({lifetime_name}, {t})"),
            GenRef::MutDeRef(t) => write!(f, "MutDeRef({t})"),
            GenRef::RefLiteral(t) => write!(f, "RefLiteral({t})"),
            GenRef::Unknown => write!(f, "Unknown"),
            GenRef::Id(id) => write!(f, "String({id})"),
        }
    }
}
impl From<GenRef<String>> for String {
    fn from(value: GenRef<String>) -> Self {
        match value {
            GenRef::Literal(s) => format!("\"{s}\""),
            GenRef::Std(s) => format!("\"{s}\""),
            GenRef::Ref(s) => format!("\"{s}\""),
            GenRef::Id(s) => s, // Identifiers don't need quotes
            GenRef::Unknown => {
                // Generate a compile error in the output code
                "compile_error!(\"Unknown value in code generation\")".to_string()
            }
            _ => {
                // For other ref types, try to use the inner value
                "compile_error!(\"Unsupported GenRef variant in code generation\")".to_string()
            }
        }
    }
}
impl From<IdType> for GenRef<String> {
    fn from(value: IdType) -> Self {
        match value {
            IdType::Literal { value: s, .. } => GenRef::Literal(s),
            IdType::Identifier { value: s, .. } => GenRef::Id(s),
            _ => GenRef::Unknown,
        }
    }
}

#[derive(Clone, Debug)]
pub enum VecData {
    Standard(GeneratedValue),
    // Embed {
    //     data: GeneratedValue,
    //     model_name: Option<String>,
    // },
    Hoisted(String),
    Unknown,
}

impl Display for VecData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            VecData::Standard(v) => write!(f, "{v}"),
            // VecData::Embed { data, model_name } => match model_name {
            //     Some(model) => write!(f, "&embed!(db, {data}, {model})"),
            //     None => write!(f, "&embed!(db, {data})"),
            // },
            VecData::Hoisted(ident) => write!(f, "&{ident}"),
            VecData::Unknown => {
                // Generate a compile error in the output code
                write!(f, "compile_error!(\"Unknown VecData in code generation\")")
            }
        }
    }
}

pub struct EmbedData {
    pub data: GeneratedValue,
    pub model_name: Option<String>,
}

impl EmbedData {
    pub fn name_from_index(idx: usize) -> String {
        format!("__internal_embed_data_{idx}")
    }
}

impl Display for EmbedData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let EmbedData { data, model_name } = self;
        match model_name {
            Some(model) => write!(f, "embed_async!(db, {data}, {model})"),
            None => write!(f, "embed_async!(db, {data})"),
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
        Some(properties) => {
            let prop_count = properties.len();
            let props_str = properties
                .iter()
                .map(|(name, value)| format!("(\"{name}\", Value::from({value}))"))
                .collect::<Vec<String>>()
                .join(", ");
            format!(
                "Some(ImmutablePropertiesMap::new({}, vec![{}].into_iter(), &arena))",
                prop_count, props_str
            )
        }
        None => "None".to_string(),
    }
}

pub fn write_properties_slice(properties: &Option<Vec<(String, GeneratedValue)>>) -> String {
    match properties {
        Some(properties) => {
            format!(
                "&[{}]",
                properties
                    .iter()
                    .map(|(name, value)| format!("(\"{name}\", Value::from({value}))"))
                    .collect::<Vec<String>>()
                    .join(", ")
            )
        }
        None => unreachable!(),
    }
}

pub fn write_secondary_indices(secondary_indices: &Option<Vec<String>>) -> String {
    match secondary_indices {
        Some(indices) => format!(
            "Some(&[{}])",
            indices
                .iter()
                .map(|idx| format!("\"{idx}\""))
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
    Parameter(GenRef<String>),
    Array(GenRef<String>),
    Aggregate(GenRef<String>),
    Traversal(Box<Traversal>),
    Unknown,
}
impl GeneratedValue {
    pub fn inner(&self) -> &GenRef<String> {
        match self {
            GeneratedValue::Literal(value) => value,
            GeneratedValue::Primitive(value) => value,
            GeneratedValue::Identifier(value) => value,
            GeneratedValue::Parameter(value) => value,
            GeneratedValue::Array(value) => value,
            GeneratedValue::Aggregate(value) => value,
            GeneratedValue::Traversal(_) => {
                // This should not be called for traversals
                // The caller should handle traversals specially
                panic!(
                    "Code generation error: Cannot get inner value of Traversal. Traversals should be handled specially."
                )
            }
            GeneratedValue::Unknown => {
                // This indicates a bug in the analyzer
                panic!(
                    "Code generation error: Unknown GeneratedValue encountered. This indicates incomplete type inference in the analyzer."
                )
            }
        }
    }
}

impl Display for GeneratedValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GeneratedValue::Literal(value) => write!(f, "{value}"),
            GeneratedValue::Primitive(value) => write!(f, "{value}"),
            GeneratedValue::Identifier(value) => write!(f, "{value}"),
            GeneratedValue::Parameter(value) => write!(f, "{value}"),
            GeneratedValue::Array(value) => write!(f, "&[{value}]"),
            GeneratedValue::Aggregate(value) => write!(f, "{value}"),
            GeneratedValue::Traversal(value) => write!(f, "{value}"),
            GeneratedValue::Unknown => write!(f, ""),
        }
    }
}
impl Debug for GeneratedValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GeneratedValue::Literal(value) => write!(f, "GV: Literal({value})"),
            GeneratedValue::Primitive(value) => write!(f, "GV: Primitive({value})"),
            GeneratedValue::Identifier(value) => write!(f, "GV: Identifier({value})"),
            GeneratedValue::Parameter(value) => write!(f, "GV: Parameter({value})"),
            GeneratedValue::Array(value) => write!(f, "GV: Array({value:?})"),
            GeneratedValue::Aggregate(value) => write!(f, "GV: Aggregate({value:?})"),
            GeneratedValue::Traversal(value) => write!(f, "GV: Traversal({value})"),
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
            GeneratedType::RustType(t) => write!(f, "{t}"),
            GeneratedType::Vec(t) => write!(f, "Vec<{t}>"),
            GeneratedType::Variable(v) => write!(f, "{v}"),
            GeneratedType::Object(o) => write!(f, "{o}"),
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
            RustType::Uuid => write!(f, "ID"),
            RustType::Date => write!(f, "DateTime<Utc>"),
        }
    }
}
impl RustType {
    pub fn to_ts(&self) -> String {
        let s = match self {
            RustType::String => "string",
            RustType::I8 => "number",
            RustType::I16 => "number",
            RustType::I32 => "number",
            RustType::I64 => "number",
            RustType::U8 => "number",
            RustType::U16 => "number",
            RustType::U32 => "number",
            RustType::U64 => "number",
            RustType::U128 => "number",
            RustType::F32 => "number",
            RustType::F64 => "number",
            RustType::Bool => "boolean",
            RustType::Uuid => "string", // do thee
            RustType::Date => "Date",   // do thee
        };
        s.to_string()
    }
}

#[derive(Clone, Debug)]
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
            Separator::Comma(t) => write!(f, ",\n{t}"),
            Separator::Semicolon(t) => writeln!(f, "{t};"),
            Separator::Period(t) => write!(f, "\n.{t}"),
            Separator::Newline(t) => write!(f, "\n{t}"),
            Separator::Empty(t) => write!(f, "{t}"),
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
// DEFAULT CODE
// use helix_db::helix_engine::traversal_core::config::Config;

// pub fn config() -> Option<Config> {
//     None
// }



use bumpalo::Bump;
use heed3::RoTxn;
use helix_macros::{handler, tool_call, mcp_handler, migration};
use helix_db::{
    helix_engine::{
        reranker::{
            RerankAdapter,
            fusion::{RRFReranker, MMRReranker, DistanceMethod},
        },
        traversal_core::{
            config::{Config, GraphConfig, VectorConfig},
            ops::{
                bm25::search_bm25::SearchBM25Adapter,
                g::G,
                in_::{in_::InAdapter, in_e::InEdgesAdapter, to_n::ToNAdapter, to_v::ToVAdapter},
                out::{
                    from_n::FromNAdapter, from_v::FromVAdapter, out::OutAdapter, out_e::OutEdgesAdapter,
                },
                source::{
                    add_e::AddEAdapter,
                    add_n::AddNAdapter,
                    e_from_id::EFromIdAdapter,
                    e_from_type::EFromTypeAdapter,
                    n_from_id::NFromIdAdapter,
                    n_from_index::NFromIndexAdapter,
                    n_from_type::NFromTypeAdapter,
                    v_from_id::VFromIdAdapter,
                    v_from_type::VFromTypeAdapter
                },
                util::{
                    dedup::DedupAdapter, drop::Drop, exist::Exist, filter_mut::FilterMut,
                    filter_ref::FilterRefAdapter, map::MapAdapter, paths::{PathAlgorithm, ShortestPathAdapter},
                    range::RangeAdapter, update::UpdateAdapter, order::OrderByAdapter,
                    aggregate::AggregateAdapter, group_by::GroupByAdapter, count::CountAdapter,
                    upsert::UpsertAdapter,
                },
                vectors::{
                    brute_force_search::BruteForceSearchVAdapter, insert::InsertVAdapter,
                    search::SearchVAdapter,
                },
            },
            traversal_value::TraversalValue,
        },
        types::GraphError,
        vector_core::HVector,
    },
    helix_gateway::{
        embedding_providers::{EmbeddingModel, get_embedding_model},
        router::router::{HandlerInput, IoContFn},
        mcp::mcp::{MCPHandlerSubmission, MCPToolInput, MCPHandler}
    },
    node_matches, props, embed, embed_async,
    field_addition_from_old_field, field_type_cast, field_addition_from_value,
    protocol::{
        response::Response,
        value::{casting::{cast, CastType}, Value},
        format::Format,
    },
    utils::{
        id::{ID, uuid_str},
        items::{Edge, Node},
        properties::ImmutablePropertiesMap,
    },
};
use sonic_rs::{Deserialize, Serialize, json};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;
use chrono::{DateTime, Utc};

// Re-export scalar types for generated code
type I8 = i8;
type I16 = i16;
type I32 = i32;
type I64 = i64;
type U8 = u8;
type U16 = u16;
type U32 = u32;
type U64 = u64;
type U128 = u128;
type F32 = f32;
type F64 = f64;
    "#
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    // ============================================================================
    // GenRef Tests
    // ============================================================================

    #[test]
    fn test_genref_literal_display() {
        let genref = GenRef::Literal("test".to_string());
        assert_eq!(format!("{}", genref), "\"test\"");
    }

    #[test]
    fn test_genref_std_display() {
        let genref = GenRef::Std("variable".to_string());
        assert_eq!(format!("{}", genref), "variable");
    }

    #[test]
    fn test_genref_mut_display() {
        let genref = GenRef::Mut("x".to_string());
        assert_eq!(format!("{}", genref), "mut x");
    }

    #[test]
    fn test_genref_ref_display() {
        let genref = GenRef::Ref("data".to_string());
        assert_eq!(format!("{}", genref), "&data");
    }

    #[test]
    fn test_genref_ref_with_lifetime() {
        let genref = GenRef::RefLT("a".to_string(), "value".to_string());
        assert_eq!(format!("{}", genref), "&'a value");
    }

    #[test]
    fn test_genref_deref_display() {
        let genref = GenRef::DeRef("ptr".to_string());
        assert_eq!(format!("{}", genref), "*ptr");
    }

    #[test]
    fn test_genref_mut_ref_display() {
        let genref = GenRef::MutRef("item".to_string());
        assert_eq!(format!("{}", genref), "& mut item");
    }

    #[test]
    fn test_genref_id_display() {
        let genref = GenRef::<String>::Id("user_id".to_string());
        assert_eq!(format!("{}", genref), "data.user_id");
    }

    // ============================================================================
    // RustType Tests
    // ============================================================================

    #[test]
    fn test_rust_type_string_display() {
        assert_eq!(format!("{}", RustType::String), "String");
    }

    #[test]
    fn test_rust_type_numeric_display() {
        assert_eq!(format!("{}", RustType::I32), "i32");
        assert_eq!(format!("{}", RustType::U64), "u64");
        assert_eq!(format!("{}", RustType::F64), "f64");
    }

    #[test]
    fn test_rust_type_bool_display() {
        assert_eq!(format!("{}", RustType::Bool), "bool");
    }

    #[test]
    fn test_rust_type_uuid_display() {
        assert_eq!(format!("{}", RustType::Uuid), "ID");
    }

    #[test]
    fn test_rust_type_to_typescript_primitives() {
        assert_eq!(RustType::String.to_ts(), "string");
        assert_eq!(RustType::Bool.to_ts(), "boolean");
        assert_eq!(RustType::I32.to_ts(), "number");
        assert_eq!(RustType::F64.to_ts(), "number");
    }

    #[test]
    fn test_rust_type_to_typescript_special() {
        assert_eq!(RustType::Uuid.to_ts(), "string");
        assert_eq!(RustType::Date.to_ts(), "Date");
    }

    // ============================================================================
    // GeneratedType Tests
    // ============================================================================

    #[test]
    fn test_generated_type_rust_type() {
        let gen_type = GeneratedType::RustType(RustType::String);
        assert_eq!(format!("{}", gen_type), "String");
    }

    #[test]
    fn test_generated_type_vec() {
        let gen_type = GeneratedType::Vec(Box::new(GeneratedType::RustType(RustType::I32)));
        assert_eq!(format!("{}", gen_type), "Vec<i32>");
    }

    #[test]
    fn test_generated_type_nested_vec() {
        let gen_type = GeneratedType::Vec(Box::new(GeneratedType::Vec(Box::new(
            GeneratedType::RustType(RustType::String),
        ))));
        assert_eq!(format!("{}", gen_type), "Vec<Vec<String>>");
    }

    #[test]
    fn test_generated_type_variable() {
        let gen_type = GeneratedType::Variable(GenRef::Std("T".to_string()));
        assert_eq!(format!("{}", gen_type), "T");
    }

    // ============================================================================
    // GeneratedValue Tests
    // ============================================================================

    #[test]
    fn test_generated_value_literal() {
        let value = GeneratedValue::Literal(GenRef::Literal("hello".to_string()));
        assert_eq!(format!("{}", value), "\"hello\"");
    }

    #[test]
    fn test_generated_value_identifier() {
        let value = GeneratedValue::Identifier(GenRef::Std("var_name".to_string()));
        assert_eq!(format!("{}", value), "var_name");
    }

    #[test]
    fn test_generated_value_parameter() {
        let value = GeneratedValue::Parameter(GenRef::Std("param".to_string()));
        assert_eq!(format!("{}", value), "param");
    }

    #[test]
    fn test_generated_value_array() {
        let value = GeneratedValue::Array(GenRef::Std("1, 2, 3".to_string()));
        assert_eq!(format!("{}", value), "&[1, 2, 3]");
    }

    // ============================================================================
    // Order Tests
    // ============================================================================

    #[test]
    fn test_order_asc_display() {
        assert_eq!(format!("{}", Order::Asc), "Asc");
    }

    #[test]
    fn test_order_desc_display() {
        assert_eq!(format!("{}", Order::Desc), "Desc");
    }

    // ============================================================================
    // VecData Tests
    // ============================================================================

    #[test]
    fn test_vecdata_standard_display() {
        let vec_data = VecData::Standard(GeneratedValue::Identifier(GenRef::Std(
            "embedding".to_string(),
        )));
        assert_eq!(format!("{}", vec_data), "embedding");
    }

    #[test]
    fn test_vecdata_hoisted_display() {
        let vec_data = VecData::Hoisted("vec_var".to_string());
        assert_eq!(format!("{}", vec_data), "&vec_var");
    }

    // ============================================================================
    // Helper Function Tests
    // ============================================================================

    #[test]
    fn test_write_properties_some() {
        let props = Some(vec![
            (
                "name".to_string(),
                GeneratedValue::Literal(GenRef::Literal("Alice".to_string())),
            ),
            (
                "age".to_string(),
                GeneratedValue::Primitive(GenRef::Std("30".to_string())),
            ),
        ]);
        let output = write_properties(&props);
        assert!(output.contains("Some(ImmutablePropertiesMap::new("));
        assert!(output.contains("(\"name\", Value::from(\"Alice\"))"));
        assert!(output.contains("(\"age\", Value::from(30))"));
    }

    #[test]
    fn test_write_properties_none() {
        let output = write_properties(&None);
        assert_eq!(output, "None");
    }

    #[test]
    fn test_write_secondary_indices_some() {
        let indices = Some(vec!["email".to_string(), "username".to_string()]);
        let output = write_secondary_indices(&indices);
        assert!(output.contains("Some(&["));
        assert!(output.contains("\"email\""));
        assert!(output.contains("\"username\""));
    }

    #[test]
    fn test_write_secondary_indices_none() {
        let output = write_secondary_indices(&None);
        assert_eq!(output, "None");
    }

    // ============================================================================
    // Separator Tests
    // ============================================================================

    #[test]
    fn test_separator_comma() {
        let sep = Separator::Comma("item".to_string());
        assert_eq!(format!("{}", sep), ",\nitem");
    }

    #[test]
    fn test_separator_semicolon() {
        let sep = Separator::Semicolon("stmt".to_string());
        assert_eq!(format!("{}", sep), "stmt;\n");
    }

    #[test]
    fn test_separator_period() {
        let sep = Separator::Period("method".to_string());
        assert_eq!(format!("{}", sep), "\n.method");
    }

    #[test]
    fn test_separator_newline() {
        let sep = Separator::Newline("line".to_string());
        assert_eq!(format!("{}", sep), "\nline");
    }

    #[test]
    fn test_separator_empty() {
        let sep = Separator::Empty("content".to_string());
        assert_eq!(format!("{}", sep), "content");
    }

    #[test]
    fn test_separator_inner() {
        let sep = Separator::Comma("value".to_string());
        assert_eq!(sep.inner(), "value");
    }
}
