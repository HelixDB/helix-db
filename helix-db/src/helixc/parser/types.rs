use super::location::Loc;
use crate::{helixc::parser::{errors::ParserError, HelixParser}, protocol::value::Value};
use chrono::{DateTime, NaiveDate, Utc};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fmt::{Debug, Display},
};

pub struct Content {
    /// Source code of the content
    pub content: String,
    /// Parsed source code
    pub source: Source,
    /// Files in the content
    pub files: Vec<HxFile>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct HxFile {
    pub name: String,
    pub content: String,
}

impl Default for HelixParser {
    fn default() -> Self {
        Self {
            source: Source {
                source: String::new(),
                schema: HashMap::new(),
                migrations: Vec::new(),
                queries: Vec::new(),
            },
        }
    }
}

// AST Structures
#[derive(Debug, Clone, Default)]
pub struct Source {
    pub source: String,
    pub schema: HashMap<usize, Schema>,
    pub migrations: Vec<Migration>,
    pub queries: Vec<Query>,
}

impl Source {
    pub fn get_latest_schema(&self) -> Result<&Schema, ParserError> {
        let latest_schema = self
            .schema
            .iter()
            .max_by(|a, b| a.1.version.1.cmp(&b.1.version.1))
            .map(|(_, schema)| schema);
        assert!(latest_schema.is_some());
        latest_schema.ok_or_else(|| ParserError::from("No latest schema found"))
    }

    /// Gets the schemas in order of version, from oldest to newest.
    pub fn get_schemas_in_order(&self) -> Vec<&Schema> {
        self.schema
            .iter()
            .sorted_by(|a, b| a.1.version.1.cmp(&b.1.version.1))
            .map(|(_, schema)| schema)
            .collect()
    }
}

#[derive(Debug, Clone)]
pub struct Schema {
    pub loc: Loc,
    pub version: (Loc, usize),
    pub node_schemas: Vec<NodeSchema>,
    pub edge_schemas: Vec<EdgeSchema>,
    pub vector_schemas: Vec<VectorSchema>,
}

#[derive(Debug, Clone)]
pub struct NodeSchema {
    pub name: (Loc, String),
    pub fields: Vec<Field>,
    pub loc: Loc,
}

#[derive(Debug, Clone)]
pub struct VectorSchema {
    pub name: String,
    pub fields: Vec<Field>,
    pub loc: Loc,
}

#[derive(Debug, Clone)]
pub struct EdgeSchema {
    pub name: (Loc, String),
    pub from: (Loc, String),
    pub to: (Loc, String),
    pub properties: Option<Vec<Field>>,
    pub loc: Loc,
}

#[derive(Debug, Clone)]
pub struct Migration {
    pub from_version: (Loc, usize),
    pub to_version: (Loc, usize),
    pub body: Vec<MigrationItemMapping>,
    pub loc: Loc,
}

#[derive(Debug, Clone)]
pub enum MigrationItem {
    Node(String),
    Edge(String),
    Vector(String),
}

impl MigrationItem {
    pub fn inner(&self) -> &str {
        match self {
            Self::Node(s) |
            Self::Edge(s) |
            Self::Vector(s) => s,
        }
    }
}

impl PartialEq<Self> for MigrationItem {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Node(a), Self::Node(b)) |
            (Self::Edge(a), Self::Edge(b)) |
            (Self::Vector(a), Self::Vector(b)) => a == b,
            _ => false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct MigrationItemMapping {
    pub from_item: (Loc, MigrationItem),
    pub to_item: (Loc, MigrationItem),
    pub remappings: Vec<MigrationPropertyMapping>,
    pub loc: Loc,
}

#[derive(Debug, Clone)]
pub struct MigrationPropertyMapping {
    pub property_name: (Loc, String),
    pub property_value: FieldValue,
    pub default: Option<DefaultValue>,
    pub cast: Option<ValueCast>,
    pub loc: Loc,
}

#[derive(Debug, Clone)]
pub struct ValueCast {
    pub loc: Loc,
    pub cast_to: FieldType,
}

#[derive(Debug, Clone)]
pub struct Field {
    pub prefix: FieldPrefix,
    pub defaults: Option<DefaultValue>,
    pub name: String,
    pub field_type: FieldType,
    pub loc: Loc,
}
impl Field {
    pub fn is_indexed(&self) -> bool {
        self.prefix.is_indexed()
    }
}

#[derive(Debug, Clone)]
pub enum DefaultValue {
    Now,
    String(String),
    F32(f32),
    F64(f64),
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    U128(u128),
    Boolean(bool),
    Empty,
}

#[derive(Debug, Clone)]
pub enum FieldPrefix {
    Index,
    Optional,
    Empty,
}
impl FieldPrefix {
    pub fn is_indexed(&self) -> bool {
        matches!(self, Self::Index)
    }
}

#[derive(Debug, Clone)]
pub enum FieldType {
    String,
    F32,
    F64,
    I8,
    I16,
    I32,
    I64,
    U8,
    U16,
    U32,
    U64,
    U128,
    Boolean,
    Uuid,
    Date,
    Array(Box<FieldType>),
    Identifier(String),
    Object(HashMap<String, FieldType>),
    // Closure(String, HashMap<String, FieldType>),
}

impl PartialEq for FieldType {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::String, Self::String) => true,
            (Self::F32 | Self::F64, Self::F32 | Self::F64) => true,
            (
                Self::I8
                | Self::I16
                | Self::I32
                | Self::I64
                | Self::U8
                | Self::U16
                | Self::U32
                | Self::U64
                | Self::U128,
                Self::I8
                | Self::I16
                | Self::I32
                | Self::I64
                | Self::U8
                | Self::U16
                | Self::U32
                | Self::U64
                | Self::U128,
            ) => true,

            (Self::Boolean, Self::Boolean) => true,
            (Self::Uuid, Self::Uuid) => true,
            (Self::Date, Self::Date) => true,
            (Self::Array(a), Self::Array(b)) => a == b,
            (Self::Identifier(a), Self::Identifier(b)) => a == b,
            (Self::Object(a), Self::Object(b)) => a == b,
            // (FieldType::Closure(a, b), FieldType::Closure(c, d)) => a == c && b == d,
            _ => false,
        }
    }
}

impl Display for FieldType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::String => write!(f, "String"),
            Self::F32 => write!(f, "F32"),
            Self::F64 => write!(f, "F64"),
            Self::I8 => write!(f, "I8"),
            Self::I16 => write!(f, "I16"),
            Self::I32 => write!(f, "I32"),
            Self::I64 => write!(f, "I64"),
            Self::U8 => write!(f, "U8"),
            Self::U16 => write!(f, "U16"),
            Self::U32 => write!(f, "U32"),
            Self::U64 => write!(f, "U64"),
            Self::U128 => write!(f, "U128"),
            Self::Boolean => write!(f, "Boolean"),
            Self::Uuid => write!(f, "ID"),
            Self::Date => write!(f, "Date"),
            Self::Array(t) => write!(f, "Array({t})"),
            Self::Identifier(s) => write!(f, "{s}"),
            Self::Object(m) => {
                write!(f, "{{")?;
                for (k, v) in m {
                    write!(f, "{k}: {v}")?;
                }
                write!(f, "}}")
            } // FieldType::Closure(a, b) => write!(f, "Closure({})", a),
        }
    }
}

impl PartialEq<Value> for FieldType {
    fn eq(&self, other: &Value) -> bool {
        match (self, other) {
            (Self::String, Value::String(_)) => true,
            (Self::F32 | Self::F64, Value::F32(_) | Value::F64(_)) => true,
            (
                Self::I8
                | Self::I16
                | Self::I32
                | Self::I64
                | Self::U8
                | Self::U16
                | Self::U32
                | Self::U64
                | Self::U128,
                Value::I8(_)
                | Value::I16(_)
                | Value::I32(_)
                | Value::I64(_)
                | Value::U8(_)
                | Value::U16(_)
                | Value::U32(_)
                | Value::U64(_)
                | Value::U128(_),
            ) => true,
            (Self::Boolean, Value::Boolean(_)) => true,
            (Self::Array(inner_type), Value::Array(values)) => {
                values.iter().all(|v| inner_type.as_ref().eq(v))
            }
            (Self::Object(fields), Value::Object(values)) => {
                fields.len() == values.len()
                    && fields.iter().all(|(k, field_type)| match values.get(k) {
                        Some(value) => field_type.eq(value),
                        None => false,
                    })
            }
            (Self::Date, value) => match value {
                Value::String(date) => {
                    date.parse::<NaiveDate>().is_ok() || date.parse::<DateTime<Utc>>().is_ok()
                }
                Value::I64(timestamp) => DateTime::from_timestamp(*timestamp, 0).is_some(),
                Value::U64(timestamp) => DateTime::from_timestamp(*timestamp as i64, 0).is_some(),
                _ => false,
            },
            _ => false,
        }
    }
}

impl PartialEq<DefaultValue> for FieldType {
    fn eq(&self, other: &DefaultValue) -> bool {
        match (self, other) {
            (Self::String, DefaultValue::String(_)) => true,
            (Self::F32 | Self::F64, DefaultValue::F32(_) | DefaultValue::F64(_)) => true,
            (
                Self::I8
                | Self::I16
                | Self::I32
                | Self::I64
                | Self::U8
                | Self::U16
                | Self::U32
                | Self::U64
                | Self::U128,
                DefaultValue::I8(_)
                | DefaultValue::I16(_)
                | DefaultValue::I32(_)
                | DefaultValue::I64(_)
                | DefaultValue::U8(_)
                | DefaultValue::U16(_)
                | DefaultValue::U32(_)
                | DefaultValue::U64(_)
                | DefaultValue::U128(_),
            ) => true,
            (Self::Boolean, DefaultValue::Boolean(_)) => true,
            (Self::Date, DefaultValue::String(date)) => {
                date.parse::<NaiveDate>().is_ok() || date.parse::<DateTime<Utc>>().is_ok()
            }
            (Self::Date, DefaultValue::I64(timestamp)) => {
                DateTime::from_timestamp(*timestamp, 0).is_some()
            }
            (Self::Date, DefaultValue::U64(timestamp)) => {
                DateTime::from_timestamp(*timestamp as i64, 0).is_some()
            }
            (Self::Date, DefaultValue::Now) => true,
            _ => false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Query {
    pub original_query: String,
    pub built_in_macro: Option<BuiltInMacro>,
    pub name: String,
    pub parameters: Vec<Parameter>,
    pub statements: Vec<Statement>,
    pub return_values: Vec<ReturnType>,
    pub loc: Loc,
}

#[derive(Debug, Clone)]
pub struct Parameter {
    pub name: (Loc, String),
    pub param_type: (Loc, FieldType),
    pub is_optional: bool,
    pub loc: Loc,
}

#[derive(Debug, Clone)]
pub struct Statement {
    pub loc: Loc,
    pub statement: StatementType,
}

#[derive(Debug, Clone)]
pub enum StatementType {
    Assignment(Assignment),
    Expression(Expression),
    Drop(Expression),
    ForLoop(ForLoop),
}

#[derive(Debug, Clone)]
pub struct Assignment {
    pub variable: String,
    pub value: Expression,
    pub loc: Loc,
}

#[derive(Debug, Clone)]
pub struct ForLoop {
    pub variable: ForLoopVars,
    pub in_variable: (Loc, String),
    pub statements: Vec<Statement>,
    pub loc: Loc,
}

#[derive(Debug, Clone)]
pub enum ForLoopVars {
    Identifier {
        name: String,
        loc: Loc,
    },
    ObjectAccess {
        name: String,
        field: String,
        loc: Loc,
    },
    ObjectDestructuring {
        fields: Vec<(Loc, String)>,
        loc: Loc,
    },
}

#[derive(Debug, Clone)]
pub struct Expression {
    pub loc: Loc,
    pub expr: ExpressionType,
}

#[derive(Debug, Clone)]
pub struct ExistsExpression {
    pub loc: Loc,
    pub expr: Box<Expression>,
}

/// Mathematical function types
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum MathFunction {
    // Arithmetic (binary operations)
    Add,
    Sub,
    Mul,
    Div,
    Pow,
    Mod,

    // Unary math functions
    Abs,
    Sqrt,
    Ln,
    Log10,
    Log,    // Binary: LOG(x, base)
    Exp,
    Ceil,
    Floor,
    Round,

    // Trigonometry
    Sin,
    Cos,
    Tan,
    Asin,
    Acos,
    Atan,
    Atan2,  // Binary: ATAN2(y, x)

    // Constants (nullary)
    Pi,
    E,

    // Aggregates (unary, operates on collections)
    Min,
    Max,
    Sum,
    Avg,
    Count,
}

impl MathFunction {
    /// Returns the expected number of arguments for this function
    pub fn arity(&self) -> usize {
        match self {
            Self::Pi | Self::E => 0,
            Self::Abs | Self::Sqrt | Self::Ln |
            Self::Log10 | Self::Exp | Self::Ceil |
            Self::Floor | Self::Round | Self::Sin |
            Self::Cos | Self::Tan | Self::Asin |
            Self::Acos | Self::Atan | Self::Min |
            Self::Max | Self::Sum | Self::Avg |
            Self::Count => 1,
            Self::Add | Self::Sub | Self::Mul |
            Self::Div | Self::Pow | Self::Mod |
            Self::Atan2 | Self::Log => 2,
        }
    }

    /// Returns the function name as a string
    pub fn name(&self) -> &'static str {
        match self {
            Self::Add => "ADD",
            Self::Sub => "SUB",
            Self::Mul => "MUL",
            Self::Div => "DIV",
            Self::Pow => "POW",
            Self::Mod => "MOD",
            Self::Abs => "ABS",
            Self::Sqrt => "SQRT",
            Self::Ln => "LN",
            Self::Log10 => "LOG10",
            Self::Log => "LOG",
            Self::Exp => "EXP",
            Self::Ceil => "CEIL",
            Self::Floor => "FLOOR",
            Self::Round => "ROUND",
            Self::Sin => "SIN",
            Self::Cos => "COS",
            Self::Tan => "TAN",
            Self::Asin => "ASIN",
            Self::Acos => "ACOS",
            Self::Atan => "ATAN",
            Self::Atan2 => "ATAN2",
            Self::Pi => "PI",
            Self::E => "E",
            Self::Min => "MIN",
            Self::Max => "MAX",
            Self::Sum => "SUM",
            Self::Avg => "AVG",
            Self::Count => "COUNT",
        }
    }
}

/// Function call AST node
#[derive(Debug, Clone)]
pub struct MathFunctionCall {
    pub function: MathFunction,
    pub args: Vec<Expression>,
    pub loc: Loc,
}

#[derive(Clone)]
pub enum ExpressionType {
    Traversal(Box<Traversal>),
    Identifier(String),
    StringLiteral(String),
    IntegerLiteral(i32),
    FloatLiteral(f64),
    BooleanLiteral(bool),
    ArrayLiteral(Vec<Expression>),
    Exists(ExistsExpression),
    AddVector(AddVector),
    AddNode(AddNode),
    AddEdge(AddEdge),
    Not(Box<Expression>),
    And(Vec<Expression>),
    Or(Vec<Expression>),
    SearchVector(SearchVector),
    BM25Search(BM25Search),
    MathFunctionCall(MathFunctionCall),
    Empty,
}

#[derive(Debug, Clone)]
pub enum ReturnType {
    Array(Vec<ReturnType>),
    Object(HashMap<String, ReturnType>),
    Expression(Expression),
    Empty,
}

impl Debug for ExpressionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Traversal(traversal) => write!(f, "Traversal({traversal:?})"),
            Self::Identifier(s) => write!(f, "{s}"),
            Self::StringLiteral(s) => write!(f, "{s}"),
            Self::IntegerLiteral(i) => write!(f, "{i}"),
            Self::FloatLiteral(fl) => write!(f, "{fl}"),
            Self::BooleanLiteral(b) => write!(f, "{b}"),
            Self::ArrayLiteral(a) => write!(f, "Array({a:?})"),
            Self::Exists(e) => write!(f, "Exists({e:?})"),
            Self::AddVector(av) => write!(f, "AddVector({av:?})"),
            Self::AddNode(an) => write!(f, "AddNode({an:?})"),
            Self::AddEdge(ae) => write!(f, "AddEdge({ae:?})"),
            Self::Not(expr) => write!(f, "Not({expr:?})"),
            Self::And(exprs) => write!(f, "And({exprs:?})"),
            Self::Or(exprs) => write!(f, "Or({exprs:?})"),
            Self::SearchVector(sv) => write!(f, "SearchVector({sv:?})"),
            Self::BM25Search(bm25) => write!(f, "BM25Search({bm25:?})"),
            Self::MathFunctionCall(mfc) => write!(f, "MathFunctionCall({mfc:?})"),
            Self::Empty => write!(f, "Empty"),
        }
    }
}
impl Display for ExpressionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Traversal(traversal) => write!(f, "Traversal({traversal:?})"),
            Self::Identifier(s) => write!(f, "{s}"),
            Self::StringLiteral(s) => write!(f, "{s}"),
            Self::IntegerLiteral(i) => write!(f, "{i}"),
            Self::FloatLiteral(fl) => write!(f, "{fl}"),
            Self::BooleanLiteral(b) => write!(f, "{b}"),
            Self::ArrayLiteral(a) => write!(f, "Array({a:?})"),
            Self::Exists(e) => write!(f, "Exists({e:?})"),
            Self::AddVector(av) => write!(f, "AddVector({av:?})"),
            Self::AddNode(an) => write!(f, "AddNode({an:?})"),
            Self::AddEdge(ae) => write!(f, "AddEdge({ae:?})"),
            Self::Not(expr) => write!(f, "Not({expr:?})"),
            Self::And(exprs) => write!(f, "And({exprs:?})"),
            Self::Or(exprs) => write!(f, "Or({exprs:?})"),
            Self::SearchVector(sv) => write!(f, "SearchVector({sv:?})"),
            Self::BM25Search(bm25) => write!(f, "BM25Search({bm25:?})"),
            Self::MathFunctionCall(mfc) => write!(f, "{}({:?})", mfc.function.name(), mfc.args),
            Self::Empty => write!(f, "Empty"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Traversal {
    pub start: StartNode,
    pub steps: Vec<Step>,
    pub loc: Loc,
}

#[derive(Debug, Clone)]
pub struct BatchAddVector {
    pub vector_type: Option<String>,
    pub vec_identifier: Option<String>,
    pub fields: Option<HashMap<String, ValueType>>,
    pub loc: Loc,
}

#[derive(Debug, Clone)]
pub enum StartNode {
    Node {
        node_type: String,
        ids: Option<Vec<IdType>>,
    },
    Edge {
        edge_type: String,
        ids: Option<Vec<IdType>>,
    },
    Vector {
        vector_type: String,
        ids: Option<Vec<IdType>>,
    },
    SearchVector(SearchVector),
    Identifier(String),
    Anonymous,
}

#[derive(Debug, Clone)]
pub struct Step {
    pub loc: Loc,
    pub step: StepType,
}

#[derive(Debug, Clone)]
pub enum OrderByType {
    Asc,
    Desc,
}

#[derive(Debug, Clone)]
pub struct OrderBy {
    pub loc: Loc,
    pub order_by_type: OrderByType,
    pub expression: Box<Expression>,
}

#[derive(Debug, Clone)]
pub struct Aggregate {
    pub loc: Loc,
    pub properties: Vec<String>
}

#[derive(Debug, Clone)]
pub struct GroupBy {
    pub loc: Loc,
    pub properties: Vec<String>
}

#[derive(Debug, Clone)]
pub struct RerankRRF {
    pub loc: Loc,
    pub k: Option<Expression>,
}

#[derive(Debug, Clone)]
pub struct RerankMMR {
    pub loc: Loc,
    pub lambda: Expression,
    pub distance: Option<MMRDistance>,
}

#[derive(Debug, Clone)]
pub enum MMRDistance {
    Cosine,
    Euclidean,
    DotProduct,
    Identifier(String),
}

#[derive(Debug, Clone)]
pub enum StepType {
    Node(GraphStep),
    Edge(GraphStep),
    Where(Box<Expression>),
    BooleanOperation(BooleanOp),
    Count,
    Update(Update),
    Object(Object),
    Exclude(Exclude),
    Closure(Closure),
    Range((Expression, Expression)),
    OrderBy(OrderBy),
    Aggregate(Aggregate),
    GroupBy(GroupBy),
    AddEdge(AddEdge),
    First,
    RerankRRF(RerankRRF),
    RerankMMR(RerankMMR),
}
impl PartialEq<Self> for StepType {
    fn eq(&self, other: &Self) -> bool {
        matches!(
            (self, other),
            (&Self::Node(_), &Self::Node(_))
                | (&Self::Edge(_), &Self::Edge(_))
                | (&Self::Where(_), &Self::Where(_))
                | (
                    &Self::BooleanOperation(_),
                    &Self::BooleanOperation(_)
                )
                | (&Self::Count, &Self::Count)
                | (&Self::Update(_), &Self::Update(_))
                | (&Self::Object(_), &Self::Object(_))
                | (&Self::Exclude(_), &Self::Exclude(_))
                | (&Self::Closure(_), &Self::Closure(_))
                | (&Self::Range(_), &Self::Range(_))
                | (&Self::OrderBy(_), &Self::OrderBy(_))
                | (&Self::AddEdge(_), &Self::AddEdge(_))
                | (&Self::Aggregate(_), &Self::Aggregate(_))
                | (&Self::GroupBy(_), &Self::GroupBy(_))
                | (&Self::RerankRRF(_), &Self::RerankRRF(_))
                | (&Self::RerankMMR(_), &Self::RerankMMR(_))
        )
    }
}
#[derive(Debug, Clone)]
pub struct FieldAddition {
    pub key: String,
    pub value: FieldValue,
    pub loc: Loc,
}

#[derive(Debug, Clone)]
pub struct FieldValue {
    pub loc: Loc,
    pub value: FieldValueType,
}

#[derive(Debug, Clone)]
pub enum FieldValueType {
    Traversal(Box<Traversal>),
    Expression(Expression),
    Fields(Vec<FieldAddition>),
    Literal(Value),
    Identifier(String),
    Empty,
}

#[derive(Debug, Clone)]
pub struct GraphStep {
    pub loc: Loc,
    pub step: GraphStepType,
}

#[derive(Debug, Clone)]
pub enum GraphStepType {
    Out(String),
    In(String),

    FromN,
    ToN,
    FromV,
    ToV,

    OutE(String),
    InE(String),

    ShortestPath(ShortestPath),
    ShortestPathDijkstras(ShortestPathDijkstras),
    ShortestPathBFS(ShortestPathBFS),
    ShortestPathAStar(ShortestPathAStar),
    SearchVector(SearchVector),
}
impl GraphStep {
    pub fn get_item_type(&self) -> Option<String> {
        match &self.step {
            GraphStepType::Out(s) => Some(s.clone()),
            GraphStepType::In(s) => Some(s.clone()),
            GraphStepType::OutE(s) => Some(s.clone()),
            GraphStepType::InE(s) => Some(s.clone()),
            GraphStepType::SearchVector(s) => Some(s.vector_type.clone().unwrap()),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ShortestPath {
    pub loc: Loc,
    pub from: Option<IdType>,
    pub to: Option<IdType>,
    pub type_arg: Option<String>,
}

/// Weight calculation expression for shortest path
#[derive(Debug, Clone)]
pub enum WeightExpression {
    /// Simple property access: _::{weight}
    Property(String),
    /// Mathematical expression (can include function calls)
    Expression(Box<Expression>),
    /// Default weight (constant 1.0)
    Default,
}

#[derive(Debug, Clone)]
pub struct ShortestPathDijkstras {
    pub loc: Loc,
    pub from: Option<IdType>,
    pub to: Option<IdType>,
    pub type_arg: Option<String>,
    pub inner_traversal: Option<Traversal>,
    // New field for better weight expression handling
    pub weight_expr: Option<WeightExpression>,
}

#[derive(Debug, Clone)]
pub struct ShortestPathBFS {
    pub loc: Loc,
    pub from: Option<IdType>,
    pub to: Option<IdType>,
    pub type_arg: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ShortestPathAStar {
    pub loc: Loc,
    pub from: Option<IdType>,
    pub to: Option<IdType>,
    pub type_arg: Option<String>,
    pub inner_traversal: Option<Traversal>,
    pub weight_expr: Option<WeightExpression>,
    pub heuristic_property: String,
}

// PathAlgorithm enum removed - now using distinct function names

#[derive(Debug, Clone)]
pub struct BooleanOp {
    pub loc: Loc,
    pub op: BooleanOpType,
}

#[derive(Debug, Clone)]
pub enum BooleanOpType {
    And(Vec<Expression>),
    Or(Vec<Expression>),
    GreaterThan(Box<Expression>),
    GreaterThanOrEqual(Box<Expression>),
    LessThan(Box<Expression>),
    LessThanOrEqual(Box<Expression>),
    Equal(Box<Expression>),
    NotEqual(Box<Expression>),
    Contains(Box<Expression>),
    IsIn(Box<Expression>),
}

#[derive(Debug, Clone)]
pub enum VectorData {
    Vector(Vec<f64>),
    Identifier(String),
    Embed(Embed),
}

#[derive(Debug, Clone)]
pub struct Embed {
    pub loc: Loc,
    pub value: EvaluatesToString,
}

#[derive(Debug, Clone)]
pub enum EvaluatesToString {
    Identifier(String),
    StringLiteral(String),
}

#[derive(Debug, Clone)]
pub struct SearchVector {
    pub loc: Loc,
    pub vector_type: Option<String>,
    pub data: Option<VectorData>,
    pub k: Option<EvaluatesToNumber>,
    pub pre_filter: Option<Box<Expression>>,
}

#[derive(Debug, Clone)]
pub struct BM25Search {
    pub loc: Loc,
    pub type_arg: Option<String>,
    pub data: Option<ValueType>,
    pub k: Option<EvaluatesToNumber>,
}

#[derive(Debug, Clone)]
pub struct EvaluatesToNumber {
    pub loc: Loc,
    pub value: EvaluatesToNumberType,
}

#[derive(Debug, Clone)]
pub enum EvaluatesToNumberType {
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    U128(u128),
    F32(f32),
    F64(f64),
    Identifier(String),
}

#[derive(Debug, Clone)]
pub struct AddVector {
    pub loc: Loc,
    pub vector_type: Option<String>,
    pub data: Option<VectorData>,
    pub fields: Option<HashMap<String, ValueType>>,
}

#[derive(Debug, Clone)]
pub struct AddNode {
    pub loc: Loc,
    pub node_type: Option<String>,
    pub fields: Option<HashMap<String, ValueType>>,
}

#[derive(Debug, Clone)]
pub struct AddEdge {
    pub loc: Loc,
    pub edge_type: Option<String>,
    pub fields: Option<HashMap<String, ValueType>>,
    pub connection: EdgeConnection,
    pub from_identifier: bool,
}

#[derive(Debug, Clone)]
pub struct EdgeConnection {
    pub loc: Loc,
    pub from_id: Option<IdType>,
    pub to_id: Option<IdType>,
}

#[derive(Debug, Clone)]
pub enum IdType {
    Literal {
        value: String,
        loc: Loc,
    },
    Identifier {
        value: String,
        loc: Loc,
    },
    ByIndex {
        index: Box<IdType>,
        value: Box<ValueType>,
        loc: Loc,
    },
}
impl Display for IdType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Literal { value, loc: _ } => write!(f, "{value}"),
            Self::Identifier { value, loc: _ } => write!(f, "{value}"),
            Self::ByIndex {
                index,
                value: _,
                loc: _,
            } => write!(f, "{index}"),
        }
    }
}

#[derive(Debug, Clone)]
pub enum ValueType {
    Literal {
        value: Value,
        loc: Loc,
    },
    Identifier {
        value: String,
        loc: Loc,
    },
    Object {
        fields: HashMap<String, ValueType>,
        loc: Loc,
    },
}
impl ValueType {
    pub fn new(value: Value, loc: Loc) -> Self {
        Self::Literal { value, loc }
    }
    pub fn to_string(&self) -> String {
        match self {
            Self::Literal { value, loc: _ } => value.inner_stringify(),
            Self::Identifier { value, loc: _ } => value.clone(),
            Self::Object { fields, loc: _ } => {
                fields.keys().cloned().collect::<Vec<String>>().join(", ")
            }
        }
    }
}

impl From<Value> for ValueType {
    fn from(value: Value) -> Self {
        match value {
            Value::String(s) => Self::Literal {
                value: Value::String(s),
                loc: Loc::empty(),
            },
            Value::I32(i) => Self::Literal {
                value: Value::I32(i),
                loc: Loc::empty(),
            },
            Value::F64(f) => Self::Literal {
                value: Value::F64(f),
                loc: Loc::empty(),
            },
            Value::Boolean(b) => Self::Literal {
                value: Value::Boolean(b),
                loc: Loc::empty(),
            },
            Value::Array(arr) => Self::Literal {
                value: Value::Array(arr),
                loc: Loc::empty(),
            },
            Value::Empty => Self::Literal {
                value: Value::Empty,
                loc: Loc::empty(),
            },
            _ => unreachable!(),
        }
    }
}

impl From<IdType> for String {
    fn from(id_type: IdType) -> Self {
        match id_type {
            IdType::Literal { mut value, loc: _ } => {
                value.retain(|c| c != '"');
                value
            }
            IdType::Identifier { value, loc: _ } => value,
            IdType::ByIndex {
                index,
                value: _,
                loc: _,
            } => Self::from(*index),
        }
    }
}

impl From<String> for IdType {
    fn from(mut s: String) -> Self {
        s.retain(|c| c != '"');
        Self::Literal {
            value: s,
            loc: Loc::empty(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Update {
    pub fields: Vec<FieldAddition>,
    pub loc: Loc,
}

#[derive(Debug, Clone)]
pub struct Object {
    pub loc: Loc,
    pub fields: Vec<FieldAddition>,
    pub should_spread: bool,
}

#[derive(Debug, Clone)]
pub struct Exclude {
    pub fields: Vec<(Loc, String)>,
    pub loc: Loc,
}

#[derive(Debug, Clone)]
pub struct Closure {
    pub identifier: String,
    pub object: Object,
    pub loc: Loc,
}

#[derive(Debug, Clone)]
pub enum BuiltInMacro {
    MCP,
    Model(String),
}
