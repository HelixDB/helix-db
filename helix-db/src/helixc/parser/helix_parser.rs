use super::{
    location::{HasLoc, Loc},
    parser_methods::ParserError,
};
use crate::{helix_engine::types::GraphError, protocol::value::Value};
use chrono::{DateTime, NaiveDate, Utc};
use pest::{
    Parser as PestParser,
    iterators::{Pair, Pairs},
};
use pest_derive::Parser;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    fmt::{Debug, Display},
    io::Write,
};

#[derive(Parser)]
#[grammar = "grammar.pest"]
pub struct HelixParser {
    source: Source,
}

pub struct Content {
    pub content: String,
    pub source: Source,
    pub files: Vec<HxFile>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct HxFile {
    pub name: String,
    pub content: String,
}

impl Default for HelixParser {
    fn default() -> Self {
        HelixParser {
            source: Source {
                source: String::new(),
                node_schemas: Vec::new(),
                edge_schemas: Vec::new(),
                vector_schemas: Vec::new(),
                queries: Vec::new(),
            },
        }
    }
}

// AST Structures
#[derive(Debug, Clone, Default)]
pub struct Source {
    pub source: String,
    pub node_schemas: Vec<NodeSchema>,
    pub edge_schemas: Vec<EdgeSchema>,
    pub vector_schemas: Vec<VectorSchema>,
    pub queries: Vec<Query>,
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
        matches!(self, FieldPrefix::Index)
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
            (FieldType::String, FieldType::String) => true,
            (FieldType::F32 | FieldType::F64, FieldType::F32 | FieldType::F64) => true,
            (
                FieldType::I8
                | FieldType::I16
                | FieldType::I32
                | FieldType::I64
                | FieldType::U8
                | FieldType::U16
                | FieldType::U32
                | FieldType::U64
                | FieldType::U128,
                FieldType::I8
                | FieldType::I16
                | FieldType::I32
                | FieldType::I64
                | FieldType::U8
                | FieldType::U16
                | FieldType::U32
                | FieldType::U64
                | FieldType::U128,
            ) => true,

            (FieldType::Boolean, FieldType::Boolean) => true,
            (FieldType::Uuid, FieldType::Uuid) => true,
            (FieldType::Date, FieldType::Date) => true,
            (FieldType::Array(a), FieldType::Array(b)) => a == b,
            (FieldType::Identifier(a), FieldType::Identifier(b)) => a == b,
            (FieldType::Object(a), FieldType::Object(b)) => a == b,
            // (FieldType::Closure(a, b), FieldType::Closure(c, d)) => a == c && b == d,
            _ => false,
        }
    }
}

impl Display for FieldType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FieldType::String => write!(f, "String"),
            FieldType::F32 => write!(f, "F32"),
            FieldType::F64 => write!(f, "F64"),
            FieldType::I8 => write!(f, "I8"),
            FieldType::I16 => write!(f, "I16"),
            FieldType::I32 => write!(f, "I32"),
            FieldType::I64 => write!(f, "I64"),
            FieldType::U8 => write!(f, "U8"),
            FieldType::U16 => write!(f, "U16"),
            FieldType::U32 => write!(f, "U32"),
            FieldType::U64 => write!(f, "U64"),
            FieldType::U128 => write!(f, "U128"),
            FieldType::Boolean => write!(f, "Boolean"),
            FieldType::Uuid => write!(f, "ID"),
            FieldType::Date => write!(f, "Date"),
            FieldType::Array(t) => write!(f, "Array({t})"),
            FieldType::Identifier(s) => write!(f, "{s}"),
            FieldType::Object(m) => {
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
            (FieldType::String, Value::String(_)) => true,
            (FieldType::F32 | FieldType::F64, Value::F32(_) | Value::F64(_)) => true,
            (
                FieldType::I8
                | FieldType::I16
                | FieldType::I32
                | FieldType::I64
                | FieldType::U8
                | FieldType::U16
                | FieldType::U32
                | FieldType::U64
                | FieldType::U128,
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
            (FieldType::Boolean, Value::Boolean(_)) => true,
            (FieldType::Array(inner_type), Value::Array(values)) => {
                values.iter().all(|v| inner_type.as_ref().eq(v))
            }
            (FieldType::Object(fields), Value::Object(values)) => {
                fields.len() == values.len()
                    && fields.iter().all(|(k, field_type)| match values.get(k) {
                        Some(value) => field_type.eq(value),
                        None => false,
                    })
            }
            (FieldType::Date, value) => match value {
                Value::String(date) => {
                    println!("date: {}, {:?}", date, date.parse::<NaiveDate>());
                    date.parse::<NaiveDate>().is_ok() || date.parse::<DateTime<Utc>>().is_ok()
                }
                Value::I64(timestamp) => DateTime::from_timestamp(*timestamp, 0).is_some(),
                Value::U64(timestamp) => DateTime::from_timestamp(*timestamp as i64, 0).is_some(),
                _ => false,
            },
            l => {
                println!("l: {l:?}");
                false
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct Query {
    pub original_query: String,
    pub is_mcp: bool,
    pub name: String,
    pub parameters: Vec<Parameter>,
    pub statements: Vec<Statement>,
    pub return_values: Vec<Expression>,
    pub loc: Loc,
}

#[derive(Debug, Clone)]
pub struct Parameter {
    pub name: (Loc, String),
    pub param_type: (Loc, FieldType),
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

#[derive(Clone)]
pub enum ExpressionType {
    Traversal(Box<Traversal>),
    Identifier(String),
    StringLiteral(String),
    IntegerLiteral(i32),
    FloatLiteral(f64),
    BooleanLiteral(bool),
    Exists(Box<Expression>),
    BatchAddVector(BatchAddVector),
    AddVector(AddVector),
    AddNode(AddNode),
    AddEdge(AddEdge),
    And(Vec<Expression>),
    Or(Vec<Expression>),
    SearchVector(SearchVector),
    BM25Search(BM25Search),
    Empty,
}
impl Debug for ExpressionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExpressionType::Traversal(traversal) => write!(f, "Traversal({traversal:?})"),
            ExpressionType::Identifier(s) => write!(f, "{s}"),
            ExpressionType::StringLiteral(s) => write!(f, "{s}"),
            ExpressionType::IntegerLiteral(i) => write!(f, "{i}"),
            ExpressionType::FloatLiteral(fl) => write!(f, "{fl}"),
            ExpressionType::BooleanLiteral(b) => write!(f, "{b}"),
            ExpressionType::Exists(e) => write!(f, "Exists({e:?})"),
            ExpressionType::BatchAddVector(bav) => write!(f, "BatchAddVector({bav:?})"),
            ExpressionType::AddVector(av) => write!(f, "AddVector({av:?})"),
            ExpressionType::AddNode(an) => write!(f, "AddNode({an:?})"),
            ExpressionType::AddEdge(ae) => write!(f, "AddEdge({ae:?})"),
            ExpressionType::And(and) => write!(f, "And({and:?})"),
            ExpressionType::Or(or) => write!(f, "Or({or:?})"),
            ExpressionType::SearchVector(sv) => write!(f, "SearchVector({sv:?})"),
            ExpressionType::BM25Search(bm25) => write!(f, "BM25Search({bm25:?})"),
            ExpressionType::Empty => write!(f, "Empty"),
        }
    }
}
impl Display for ExpressionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExpressionType::Traversal(traversal) => write!(f, "Traversal({traversal:?})"),
            ExpressionType::Identifier(s) => write!(f, "{s}"),
            ExpressionType::StringLiteral(s) => write!(f, "{s}"),
            ExpressionType::IntegerLiteral(i) => write!(f, "{i}"),
            ExpressionType::FloatLiteral(fl) => write!(f, "{fl}"),
            ExpressionType::BooleanLiteral(b) => write!(f, "{b}"),
            ExpressionType::Exists(e) => write!(f, "Exists({e:?})"),
            ExpressionType::BatchAddVector(bav) => write!(f, "BatchAddVector({bav:?})"),
            ExpressionType::AddVector(av) => write!(f, "AddVector({av:?})"),
            ExpressionType::AddNode(an) => write!(f, "AddNode({an:?})"),
            ExpressionType::AddEdge(ae) => write!(f, "AddEdge({ae:?})"),
            ExpressionType::And(and) => write!(f, "And({and:?})"),
            ExpressionType::Or(or) => write!(f, "Or({or:?})"),
            ExpressionType::SearchVector(sv) => write!(f, "SearchVector({sv:?})"),
            ExpressionType::BM25Search(bm25) => write!(f, "BM25Search({bm25:?})"),
            ExpressionType::Empty => write!(f, "Empty"),
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
    Identifier(String),
    Anonymous,
}

#[derive(Debug, Clone)]
pub struct Step {
    pub loc: Loc,
    pub step: StepType,
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
    OrderByAsc(Box<Expression>),
    OrderByDesc(Box<Expression>),
    AddEdge(AddEdge),
}
impl PartialEq<StepType> for StepType {
    fn eq(&self, other: &StepType) -> bool {
        matches!(
            (self, other),
            (&StepType::Node(_), &StepType::Node(_))
                | (&StepType::Edge(_), &StepType::Edge(_))
                | (&StepType::Where(_), &StepType::Where(_))
                | (
                    &StepType::BooleanOperation(_),
                    &StepType::BooleanOperation(_)
                )
                | (&StepType::Count, &StepType::Count)
                | (&StepType::Update(_), &StepType::Update(_))
                | (&StepType::Object(_), &StepType::Object(_))
                | (&StepType::Exclude(_), &StepType::Exclude(_))
                | (&StepType::Closure(_), &StepType::Closure(_))
                | (&StepType::Range(_), &StepType::Range(_))
                | (&StepType::OrderByAsc(_), &StepType::OrderByAsc(_))
                | (&StepType::OrderByDesc(_), &StepType::OrderByDesc(_))
                | (&StepType::AddEdge(_), &StepType::AddEdge(_))
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
            IdType::Literal { value, loc: _ } => write!(f, "{value}"),
            IdType::Identifier { value, loc: _ } => write!(f, "{value}"),
            IdType::ByIndex {
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
    pub fn new(value: Value, loc: Loc) -> ValueType {
        ValueType::Literal { value, loc }
    }
    pub fn to_string(&self) -> String {
        match self {
            ValueType::Literal { value, loc: _ } => value.to_string(),
            ValueType::Identifier { value, loc: _ } => value.clone(),
            ValueType::Object { fields, loc: _ } => {
                fields.keys().cloned().collect::<Vec<String>>().join(", ")
            }
        }
    }
}

impl From<Value> for ValueType {
    fn from(value: Value) -> ValueType {
        match value {
            Value::String(s) => ValueType::Literal {
                value: Value::String(s),
                loc: Loc::empty(),
            },
            Value::I32(i) => ValueType::Literal {
                value: Value::I32(i),
                loc: Loc::empty(),
            },
            Value::F64(f) => ValueType::Literal {
                value: Value::F64(f),
                loc: Loc::empty(),
            },
            Value::Boolean(b) => ValueType::Literal {
                value: Value::Boolean(b),
                loc: Loc::empty(),
            },
            Value::Array(arr) => ValueType::Literal {
                value: Value::Array(arr),
                loc: Loc::empty(),
            },
            Value::Empty => ValueType::Literal {
                value: Value::Empty,
                loc: Loc::empty(),
            },
            _ => unreachable!(),
        }
    }
}

impl From<IdType> for String {
    fn from(id_type: IdType) -> String {
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
            } => String::from(*index),
        }
    }
}

impl From<String> for IdType {
    fn from(mut s: String) -> IdType {
        s.retain(|c| c != '"');
        IdType::Literal {
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
    // TODO: Change this to be a vec of structs where the enums holds the name and value
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

impl HelixParser {
    pub fn parse_source(input: &Content) -> Result<Source, ParserError> {
        let mut source = Source {
            source: String::new(),
            node_schemas: Vec::new(),
            edge_schemas: Vec::new(),
            vector_schemas: Vec::new(),
            queries: Vec::new(),
        };

        input.files.iter().try_for_each(|file| {
            source.source.push_str(&file.content);
            source.source.push('\n');
            let pair = match HelixParser::parse(Rule::source, &file.content) {
                Ok(mut pairs) => pairs
                    .next()
                    .ok_or_else(|| ParserError::from("Empty input"))?,
                Err(e) => {
                    return Err(ParserError::from(e));
                }
            };
            let mut parser = HelixParser {
                source: Source::default(),
            };

            let pairs = pair.into_inner();
            let mut remaining = HashSet::new();
            for pair in pairs {
                match pair.as_rule() {
                    Rule::node_def => {
                        let node_schema = parser.parse_node_def(pair, file.name.clone())?;
                        parser.source.node_schemas.push(node_schema);
                    }
                    Rule::edge_def => {
                        let edge_schema = parser.parse_edge_def(pair, file.name.clone())?;
                        parser.source.edge_schemas.push(edge_schema);
                    }
                    Rule::vector_def => {
                        let vector_schema = parser.parse_vector_def(pair, file.name.clone())?;
                        parser.source.vector_schemas.push(vector_schema);
                    }
                    Rule::query_def => {
                        // parser.source.queries.push(parser.parse_query_def(pairs.next().unwrap())?),
                        remaining.insert(pair);
                    }
                    Rule::EOI => (),
                    _ => return Err(ParserError::from("Unexpected rule encountered")),
                }
            }

            for pair in remaining {
                // println!("{:?}", parser.source);
                parser
                    .source
                    .queries
                    .push(parser.parse_query_def(pair, file.name.clone())?);
            }

            // parse all schemas first then parse queries using self
            source.node_schemas.extend(parser.source.node_schemas);
            source.edge_schemas.extend(parser.source.edge_schemas);
            source.vector_schemas.extend(parser.source.vector_schemas);
            source.queries.extend(parser.source.queries);
            Ok(())
        })?;

        Ok(source)
    }

    fn parse_node_def(
        &self,
        pair: Pair<Rule>,
        filepath: String,
    ) -> Result<NodeSchema, ParserError> {
        let mut pairs = pair.clone().into_inner();
        let name = pairs.next().unwrap().as_str().to_string();
        let fields = self.parse_node_body(pairs.next().unwrap())?;
        Ok(NodeSchema {
            name: (pair.loc(), name),
            fields,
            loc: pair.loc_with_filepath(filepath),
        })
    }

    fn parse_vector_def(
        &self,
        pair: Pair<Rule>,
        filepath: String,
    ) -> Result<VectorSchema, ParserError> {
        let mut pairs = pair.clone().into_inner();
        let name = pairs.next().unwrap().as_str().to_string();
        let fields = self.parse_node_body(pairs.next().unwrap())?;
        Ok(VectorSchema {
            name,
            fields,
            loc: pair.loc_with_filepath(filepath),
        })
    }

    fn parse_node_body(&self, pair: Pair<Rule>) -> Result<Vec<Field>, ParserError> {
        let field_defs = pair
            .into_inner()
            .find(|p| p.as_rule() == Rule::field_defs)
            .expect("Expected field_defs in properties");

        // Now parse each individual field_def
        field_defs
            .into_inner()
            .map(|p| self.parse_field_def(p))
            .collect::<Result<Vec<_>, _>>()
    }

    fn parse_field_type(
        &self,
        field: Pair<Rule>,
        _schema: Option<&Source>,
    ) -> Result<FieldType, ParserError> {
        match field.as_rule() {
            Rule::named_type => {
                let type_str = field.as_str();
                match type_str {
                    "String" => Ok(FieldType::String),
                    "Boolean" => Ok(FieldType::Boolean),
                    "F32" => Ok(FieldType::F32),
                    "F64" => Ok(FieldType::F64),
                    "I8" => Ok(FieldType::I8),
                    "I16" => Ok(FieldType::I16),
                    "I32" => Ok(FieldType::I32),
                    "I64" => Ok(FieldType::I64),
                    "U8" => Ok(FieldType::U8),
                    "U16" => Ok(FieldType::U16),
                    "U32" => Ok(FieldType::U32),
                    "U64" => Ok(FieldType::U64),
                    "U128" => Ok(FieldType::U128),
                    _ => unreachable!(),
                }
            }
            Rule::array => {
                Ok(FieldType::Array(Box::new(
                    self.parse_field_type(
                        // unwraps the array type because grammar type is
                        // { array { param_type { array | object | named_type } } }
                        field
                            .into_inner()
                            .next()
                            .unwrap()
                            .into_inner()
                            .next()
                            .unwrap(),
                        _schema,
                    )?,
                )))
            }
            Rule::object => {
                let mut fields = HashMap::new();
                for field in field.into_inner().next().unwrap().into_inner() {
                    let (field_name, field_type) = {
                        let mut field_pair = field.clone().into_inner();
                        (
                            field_pair.next().unwrap().as_str().to_string(),
                            field_pair.next().unwrap().into_inner().next().unwrap(),
                        )
                    };
                    let field_type = self.parse_field_type(field_type, Some(&self.source))?;
                    fields.insert(field_name, field_type);
                }
                Ok(FieldType::Object(fields))
            }
            Rule::identifier => Ok(FieldType::Identifier(field.as_str().to_string())),
            Rule::ID_TYPE => Ok(FieldType::Uuid),
            Rule::date_type => Ok(FieldType::Date),
            _ => {
                unreachable!()
            }
        }
    }

    fn parse_field_def(&self, pair: Pair<Rule>) -> Result<Field, ParserError> {
        let mut pairs = pair.clone().into_inner();
        // structure is index? ~ identifier ~ ":" ~ param_type
        let prefix: FieldPrefix = match pairs.clone().next().unwrap().as_rule() {
            Rule::index => {
                pairs.next().unwrap();
                FieldPrefix::Index
            }
            // Rule::optional => {
            //     pairs.next().unwrap();
            //     FieldPrefix::Optional
            // }
            _ => FieldPrefix::Empty,
        };
        let name = pairs.next().unwrap().as_str().to_string();

        let field_type = self.parse_field_type(
            pairs.next().unwrap().into_inner().next().unwrap(),
            Some(&self.source),
        )?;

        let defaults = match pairs.next() {
            Some(pair) => {
                if pair.as_rule() == Rule::default {
                    let default_value = match pair.into_inner().next() {
                        Some(pair) => match pair.as_rule() {
                            Rule::string_literal => DefaultValue::String(pair.as_str().to_string()),
                            Rule::float => {
                                match field_type {
                                    FieldType::F32 => {
                                        DefaultValue::F32(pair.as_str().parse::<f32>().unwrap())
                                    }
                                    FieldType::F64 => {
                                        DefaultValue::F64(pair.as_str().parse::<f64>().unwrap())
                                    }
                                    _ => unreachable!(), // throw error
                                }
                            }
                            Rule::integer => {
                                match field_type {
                                    FieldType::I8 => {
                                        DefaultValue::I8(pair.as_str().parse::<i8>().unwrap())
                                    }
                                    FieldType::I16 => {
                                        DefaultValue::I16(pair.as_str().parse::<i16>().unwrap())
                                    }
                                    FieldType::I32 => {
                                        DefaultValue::I32(pair.as_str().parse::<i32>().unwrap())
                                    }
                                    FieldType::I64 => {
                                        DefaultValue::I64(pair.as_str().parse::<i64>().unwrap())
                                    }
                                    FieldType::U8 => {
                                        DefaultValue::U8(pair.as_str().parse::<u8>().unwrap())
                                    }
                                    FieldType::U16 => {
                                        DefaultValue::U16(pair.as_str().parse::<u16>().unwrap())
                                    }
                                    FieldType::U32 => {
                                        DefaultValue::U32(pair.as_str().parse::<u32>().unwrap())
                                    }
                                    FieldType::U64 => {
                                        DefaultValue::U64(pair.as_str().parse::<u64>().unwrap())
                                    }
                                    FieldType::U128 => {
                                        DefaultValue::U128(pair.as_str().parse::<u128>().unwrap())
                                    }
                                    _ => unreachable!(), // throw error
                                }
                            }
                            Rule::now => DefaultValue::Now,
                            Rule::boolean => {
                                DefaultValue::Boolean(pair.as_str().parse::<bool>().unwrap())
                            }
                            _ => unreachable!(), // throw error
                        },
                        None => DefaultValue::Empty,
                    };
                    Some(default_value)
                } else {
                    None
                }
            }
            None => None,
        };

        Ok(Field {
            prefix,
            defaults,
            name,
            field_type,
            loc: pair.loc(),
        })
    }

    fn parse_edge_def(
        &self,
        pair: Pair<Rule>,
        filepath: String,
    ) -> Result<EdgeSchema, ParserError> {
        let mut pairs = pair.clone().into_inner();
        let name = pairs.next().unwrap().as_str().to_string();
        let body = pairs.next().unwrap();
        let mut body_pairs = body.into_inner();

        let from = {
            let pair = body_pairs.next().unwrap();
            (pair.loc(), pair.as_str().to_string())
        };
        let to = {
            let pair = body_pairs.next().unwrap();
            (pair.loc(), pair.as_str().to_string())
        };
        let properties = match body_pairs.next() {
            Some(pair) => Some(self.parse_properties(pair)?),
            None => None,
        };

        Ok(EdgeSchema {
            name: (pair.loc(), name),
            from,
            to,
            properties,
            loc: pair.loc_with_filepath(filepath),
        })
    }
    fn parse_properties(&self, pair: Pair<Rule>) -> Result<Vec<Field>, ParserError> {
        pair.into_inner()
            .find(|p| p.as_rule() == Rule::field_defs)
            .map_or(Ok(Vec::new()), |field_defs| {
                field_defs
                    .into_inner()
                    .map(|p| self.parse_field_def(p))
                    .collect::<Result<Vec<_>, _>>()
            })
    }

    fn parse_query_def(&self, pair: Pair<Rule>, filepath: String) -> Result<Query, ParserError> {
        let original_query = pair.clone().as_str().to_string();
        let mut pairs = pair.clone().into_inner();
        let is_mcp = match pairs.peek() {
            Some(pair) if pair.as_rule() == Rule::mcp_macro => {
                pairs.next();
                true
            }
            _ => false,
        };
        let name = pairs.next().unwrap().as_str().to_string();
        let parameters = self.parse_parameters(pairs.next().unwrap())?;
        let nect = pairs.next().unwrap();
        let statements = self.parse_query_body(nect)?;
        let return_values = self.parse_return_statement(pairs.next().unwrap())?;

        Ok(Query {
            is_mcp,
            name,
            parameters,
            statements,
            return_values,
            original_query,
            loc: pair.loc_with_filepath(filepath),
        })
    }

    fn parse_parameters(&self, pair: Pair<Rule>) -> Result<Vec<Parameter>, ParserError> {
        let mut seen = HashSet::new();
        pair.clone()
            .into_inner()
            .map(|p: Pair<'_, Rule>| -> Result<Parameter, ParserError> {
                let mut inner = p.into_inner();
                let name = {
                    let pair = inner.next().unwrap();
                    (pair.loc(), pair.as_str().to_string())
                };

                // gets param type
                let param_pair = inner
                    .clone()
                    .next()
                    .unwrap()
                    .clone()
                    .into_inner()
                    .next()
                    .unwrap();
                let param_type_location = param_pair.loc();
                let param_type = self.parse_field_type(
                    // unwraps the param type to get the rule (array, object, named_type, etc)
                    param_pair,
                    Some(&self.source),
                )?;

                if seen.insert(name.1.clone()) {
                    Ok(Parameter {
                        name,
                        param_type: (param_type_location, param_type),
                        loc: pair.loc(),
                    })
                } else {
                    Err(ParserError::from(format!(
                        r#"Duplicate parameter name: {}
                            Please use unique parameter names.

                            Error happened at line {} column {} here: {}
                        "#,
                        name.1,
                        pair.line_col().0,
                        pair.line_col().1,
                        pair.as_str(),
                    )))
                }
            })
            .collect::<Result<Vec<_>, _>>()
    }

    fn parse_query_body(&self, pair: Pair<Rule>) -> Result<Vec<Statement>, ParserError> {
        pair.into_inner()
            .map(|p| match p.as_rule() {
                Rule::get_stmt => Ok(Statement {
                    loc: p.loc(),
                    statement: StatementType::Assignment(self.parse_get_statement(p)?),
                }),
                Rule::creation_stmt => Ok(Statement {
                    loc: p.loc(),
                    statement: StatementType::Expression(self.parse_expression(p)?),
                }),

                Rule::drop => {
                    let inner = p.into_inner().next().unwrap();
                    Ok(Statement {
                        loc: inner.loc(),
                        statement: StatementType::Drop(self.parse_expression(inner)?),
                    })
                }

                Rule::for_loop => Ok(Statement {
                    loc: p.loc(),
                    statement: StatementType::ForLoop(self.parse_for_loop(p)?),
                }),
                _ => Err(ParserError::from(format!(
                    "Unexpected statement type in query body: {:?}",
                    p.as_rule()
                ))),
            })
            .collect()
    }

    fn parse_bm25_search(&self, pair: Pair<Rule>) -> Result<BM25Search, ParserError> {
        let mut pairs = pair.clone().into_inner();
        let vector_type = pairs.next().unwrap().as_str().to_string();
        let query = match pairs.next() {
            Some(pair) => match pair.as_rule() {
                Rule::identifier => ValueType::Identifier {
                    value: pair.as_str().to_string(),
                    loc: pair.loc(),
                },
                Rule::string_literal => ValueType::Literal {
                    value: Value::String(pair.as_str().to_string()),
                    loc: pair.loc(),
                },
                _ => {
                    return Err(ParserError::from(format!(
                        "Unexpected rule in BM25Search: {:?}",
                        pair.as_rule()
                    )));
                }
            },
            None => {
                return Err(ParserError::from(format!(
                    "Unexpected rule in BM25Search: {:?}",
                    pair.as_rule()
                )));
            }
        };
        let k = pairs.next().unwrap().as_str().to_string();
        Ok(BM25Search {
            loc: pair.loc(),
            type_arg: Some(vector_type),
            data: Some(query),
            k: Some(EvaluatesToNumber {
                loc: pair.loc(),
                value: EvaluatesToNumberType::U32(k.parse::<u32>().unwrap()),
            }),
        })
    }

    fn parse_for_loop(&self, pair: Pair<Rule>) -> Result<ForLoop, ParserError> {
        let mut pairs = pair.clone().into_inner();
        // parse the arguments
        let argument = pairs.next().unwrap().clone().into_inner().next().unwrap();
        let argument_loc = argument.loc();
        let variable = match argument.as_rule() {
            Rule::object_destructuring => {
                let fields = argument
                    .into_inner()
                    .map(|p| (p.loc(), p.as_str().to_string()))
                    .collect();
                ForLoopVars::ObjectDestructuring {
                    fields,
                    loc: argument_loc,
                }
            }
            Rule::object_access => {
                let mut inner = argument.clone().into_inner();
                let object_name = inner.next().unwrap().as_str().to_string();
                let field_name = inner.next().unwrap().as_str().to_string();
                ForLoopVars::ObjectAccess {
                    name: object_name,
                    field: field_name,
                    loc: argument_loc,
                }
            }
            Rule::identifier => ForLoopVars::Identifier {
                name: argument.as_str().to_string(),
                loc: argument_loc,
            },
            _ => {
                return Err(ParserError::from(format!(
                    "Unexpected rule in ForLoop: {:?}",
                    argument.as_rule()
                )));
            }
        };

        // parse the in
        let in_ = pairs.next().unwrap().clone();
        let in_variable = match in_.as_rule() {
            Rule::identifier => (in_.loc(), in_.as_str().to_string()),
            _ => {
                return Err(ParserError::from(format!(
                    "Unexpected rule in ForLoop: {:?}",
                    in_.as_rule()
                )));
            }
        };
        // parse the body
        let statements = self.parse_query_body(pairs.next().unwrap())?;

        Ok(ForLoop {
            variable,
            in_variable,
            statements,
            loc: pair.loc(),
        })
    }

    fn parse_batch_add_vector(&self, pair: Pair<Rule>) -> Result<BatchAddVector, ParserError> {
        let mut vector_type = None;
        let mut vec_identifier = None;
        let mut fields = None;

        for p in pair.clone().into_inner() {
            match p.as_rule() {
                Rule::identifier_upper => {
                    vector_type = Some(p.as_str().to_string());
                }
                Rule::identifier => {
                    vec_identifier = Some(p.as_str().to_string());
                }
                Rule::create_field => {
                    fields = Some(self.parse_property_assignments(p)?);
                }
                _ => {
                    return Err(ParserError::from(format!(
                        "Unexpected rule in AddV: {:?} => {:?}",
                        p.as_rule(),
                        p,
                    )));
                }
            }
        }

        Ok(BatchAddVector {
            vector_type,
            vec_identifier,
            fields,
            loc: pair.loc(),
        })
    }

    fn parse_add_vector(&self, pair: Pair<Rule>) -> Result<AddVector, ParserError> {
        let mut vector_type = None;
        let mut data = None;
        let mut fields = None;

        for p in pair.clone().into_inner() {
            match p.as_rule() {
                Rule::identifier_upper => {
                    vector_type = Some(p.as_str().to_string());
                }
                Rule::vector_data => match p.clone().into_inner().next() {
                    Some(vector_data) => match vector_data.as_rule() {
                        Rule::identifier => {
                            data = Some(VectorData::Identifier(p.as_str().to_string()));
                        }
                        Rule::vec_literal => {
                            data = Some(VectorData::Vector(self.parse_vec_literal(p)?));
                        }
                        Rule::embed_method => {
                            data = Some(VectorData::Embed(Embed {
                                loc: vector_data.loc(),
                                value: match vector_data.clone().into_inner().next() {
                                    Some(inner) => match inner.as_rule() {
                                        Rule::identifier => EvaluatesToString::Identifier(
                                            inner.as_str().to_string(),
                                        ),
                                        Rule::string_literal => EvaluatesToString::StringLiteral(
                                            inner.as_str().to_string(),
                                        ),
                                        _ => {
                                            return Err(ParserError::from(format!(
                                                "Unexpected rule in AddV: {:?} => {:?}",
                                                inner.as_rule(),
                                                inner,
                                            )));
                                        }
                                    },
                                    None => {
                                        return Err(ParserError::from(format!(
                                            "Unexpected rule in AddV: {:?} => {:?}",
                                            p.as_rule(),
                                            p,
                                        )));
                                    }
                                },
                            }));
                        }
                        _ => {
                            return Err(ParserError::from(format!(
                                "Unexpected rule in AddV: {:?} => {:?}",
                                vector_data.as_rule(),
                                vector_data,
                            )));
                        }
                    },
                    None => {
                        return Err(ParserError::from(format!(
                            "Unexpected rule in AddV: {:?} => {:?}",
                            p.as_rule(),
                            p,
                        )));
                    }
                },
                Rule::create_field => {
                    fields = Some(self.parse_property_assignments(p)?);
                }
                _ => {
                    return Err(ParserError::from(format!(
                        "Unexpected rule in AddV: {:?} => {:?}",
                        p.as_rule(),
                        p,
                    )));
                }
            }
        }

        Ok(AddVector {
            vector_type,
            data,
            fields,
            loc: pair.loc(),
        })
    }

    fn parse_search_vector(&self, pair: Pair<Rule>) -> Result<SearchVector, ParserError> {
        let mut vector_type = None;
        let mut data = None;
        let mut k = None;
        let mut pre_filter = None;
        for p in pair.clone().into_inner() {
            match p.as_rule() {
                Rule::identifier_upper => {
                    vector_type = Some(p.as_str().to_string());
                }
                Rule::vector_data => match p.clone().into_inner().next() {
                    Some(vector_data) => match vector_data.as_rule() {
                        Rule::identifier => {
                            data = Some(VectorData::Identifier(p.as_str().to_string()));
                        }
                        Rule::vec_literal => {
                            data = Some(VectorData::Vector(self.parse_vec_literal(p)?));
                        }
                        Rule::embed_method => {
                            data = Some(VectorData::Embed(Embed {
                                loc: vector_data.loc(),
                                value: match vector_data.clone().into_inner().next() {
                                    Some(inner) => match inner.as_rule() {
                                        Rule::identifier => EvaluatesToString::Identifier(
                                            inner.as_str().to_string(),
                                        ),
                                        Rule::string_literal => EvaluatesToString::StringLiteral(
                                            inner.as_str().to_string(),
                                        ),
                                        _ => {
                                            return Err(ParserError::from(format!(
                                                "Unexpected rule in SearchV: {:?} => {:?}",
                                                inner.as_rule(),
                                                inner,
                                            )));
                                        }
                                    },
                                    None => {
                                        return Err(ParserError::from(format!(
                                            "Unexpected rule in SearchV: {:?} => {:?}",
                                            p.as_rule(),
                                            p,
                                        )));
                                    }
                                },
                            }));
                        }
                        _ => {
                            return Err(ParserError::from(format!(
                                "Unexpected rule in SearchV: {:?} => {:?}",
                                vector_data.as_rule(),
                                vector_data,
                            )));
                        }
                    },
                    None => {
                        return Err(ParserError::from(format!(
                            "Unexpected rule in SearchV: {:?} => {:?}",
                            p.as_rule(),
                            p,
                        )));
                    }
                },
                Rule::integer => {
                    k = Some(EvaluatesToNumber {
                        loc: p.loc(),
                        value: EvaluatesToNumberType::I32(
                            p.as_str()
                                .to_string()
                                .parse::<i32>()
                                .map_err(|_| ParserError::from("Invalid integer value"))?,
                        ),
                    });
                }
                Rule::identifier => {
                    k = Some(EvaluatesToNumber {
                        loc: p.loc(),
                        value: EvaluatesToNumberType::Identifier(p.as_str().to_string()),
                    });
                }
                Rule::pre_filter => {
                    pre_filter = Some(Box::new(self.parse_expression(p)?));
                }
                _ => {
                    return Err(ParserError::from(format!(
                        "Unexpected rule in SearchV: {:?} => {:?}",
                        p.as_rule(),
                        p,
                    )));
                }
            }
        }

        Ok(SearchVector {
            loc: pair.loc(),
            vector_type,
            data,
            k,
            pre_filter,
        })
    }

    fn parse_vec_literal(&self, pair: Pair<Rule>) -> Result<Vec<f64>, ParserError> {
        let pairs = pair.into_inner();
        let mut vec = Vec::new();
        for p in pairs {
            vec.push(
                p.as_str()
                    .parse::<f64>()
                    .map_err(|_| ParserError::from("Invalid float value"))?,
            );
        }
        Ok(vec)
    }

    fn parse_add_node(&self, pair: Pair<Rule>) -> Result<AddNode, ParserError> {
        let mut node_type = None;
        let mut fields = None;

        for p in pair.clone().into_inner() {
            match p.as_rule() {
                Rule::identifier_upper => {
                    node_type = Some(p.as_str().to_string());
                }
                Rule::create_field => {
                    fields = Some(self.parse_property_assignments(p)?);
                }
                _ => {
                    return Err(ParserError::from(format!(
                        "Unexpected rule in AddV: {:?} => {:?}",
                        p.as_rule(),
                        p,
                    )));
                }
            }
        }

        Ok(AddNode {
            node_type,
            fields,
            loc: pair.loc(),
        })
    }

    fn parse_property_assignments(
        &self,
        pair: Pair<Rule>,
    ) -> Result<HashMap<String, ValueType>, ParserError> {
        pair.into_inner()
            .map(|p| {
                let mut pairs = p.into_inner();
                let prop_key = pairs
                    .next()
                    .ok_or_else(|| ParserError::from("Missing property key"))?
                    .as_str()
                    .to_string();

                let prop_val = match pairs.next() {
                    Some(p) => {
                        let value_pair = p
                            .into_inner()
                            .next()
                            .ok_or_else(|| ParserError::from("Empty property value"))?;

                        match value_pair.as_rule() {
                            Rule::string_literal => Ok(ValueType::new(
                                Value::from(value_pair.as_str().to_string()),
                                value_pair.loc(),
                            )),
                            Rule::integer => value_pair
                                .as_str()
                                .parse()
                                .map(|i| ValueType::new(Value::I32(i), value_pair.loc()))
                                .map_err(|_| ParserError::from("Invalid integer value")),
                            Rule::float => value_pair
                                .as_str()
                                .parse()
                                .map(|f| ValueType::new(Value::F64(f), value_pair.loc()))
                                .map_err(|_| ParserError::from("Invalid float value")),
                            Rule::boolean => Ok(ValueType::new(
                                Value::Boolean(value_pair.as_str() == "true"),
                                value_pair.loc(),
                            )),
                            Rule::identifier => Ok(ValueType::Identifier {
                                value: value_pair.as_str().to_string(),
                                loc: value_pair.loc(),
                            }),
                            _ => Err(ParserError::from("Invalid property value type")),
                        }?
                    }
                    None => ValueType::new(Value::Empty, Loc::empty()),
                };

                Ok((prop_key, prop_val))
            })
            .collect()
    }

    fn parse_add_edge(
        &self,
        pair: Pair<Rule>,
        from_identifier: bool,
    ) -> Result<AddEdge, ParserError> {
        let mut edge_type = None;
        let mut fields = None;
        let mut connection = None;

        for p in pair.clone().into_inner() {
            match p.as_rule() {
                Rule::identifier_upper => {
                    edge_type = Some(p.as_str().to_string());
                }
                Rule::create_field => {
                    fields = Some(self.parse_property_assignments(p)?);
                }
                Rule::to_from => {
                    connection = Some(self.parse_to_from(p)?);
                }
                _ => {
                    return Err(ParserError::from(format!(
                        "Unexpected rule in AddE: {:?}",
                        p.as_rule()
                    )));
                }
            }
        }
        if edge_type.is_none() {
            return Err(ParserError::from("Missing edge type"));
        }
        if connection.is_none() {
            return Err(ParserError::from("Missing edge connection"));
        }
        Ok(AddEdge {
            edge_type,
            fields,
            connection: connection.ok_or_else(|| ParserError::from("Missing edge connection"))?,
            from_identifier,
            loc: pair.loc(),
        })
    }

    fn parse_id_args(&self, pair: Pair<Rule>) -> Result<Option<IdType>, ParserError> {
        let p = pair
            .into_inner()
            .next()
            .ok_or_else(|| ParserError::from("Missing ID"))?;
        match p.as_rule() {
            Rule::identifier => Ok(Some(IdType::Identifier {
                value: p.as_str().to_string(),
                loc: p.loc(),
            })),
            Rule::string_literal | Rule::inner_string => Ok(Some(IdType::Literal {
                value: p.as_str().to_string(),
                loc: p.loc(),
            })),
            _ => Err(ParserError::from(format!(
                "Unexpected rule in parse_id_args: {:?}",
                p.as_rule()
            ))),
        }
    }

    fn parse_to_from(&self, pair: Pair<Rule>) -> Result<EdgeConnection, ParserError> {
        let pairs = pair.clone().into_inner();
        let mut from_id = None;
        let mut to_id = None;
        // println!("pairs: {:?}", pairs);
        for p in pairs {
            match p.as_rule() {
                Rule::from => {
                    from_id = self.parse_id_args(p.into_inner().next().unwrap())?;
                }
                Rule::to => {
                    to_id = self.parse_id_args(p.into_inner().next().unwrap())?;
                }
                _ => unreachable!(),
            }
        }
        Ok(EdgeConnection {
            from_id,
            to_id,
            loc: pair.loc(),
        })
    }

    fn parse_get_statement(&self, pair: Pair<Rule>) -> Result<Assignment, ParserError> {
        let mut pairs = pair.clone().into_inner();
        let variable = pairs.next().unwrap().as_str().to_string();
        let value = self.parse_expression(pairs.next().unwrap())?;

        Ok(Assignment {
            variable,
            value,
            loc: pair.loc(),
        })
    }

    fn parse_return_statement(&self, pair: Pair<Rule>) -> Result<Vec<Expression>, ParserError> {
        // println!("pair: {:?}", pair.clone().into_inner());
        pair.into_inner()
            .map(|p| self.parse_expression(p))
            .collect()
    }

    fn parse_expression_vec(&self, pairs: Pairs<Rule>) -> Result<Vec<Expression>, ParserError> {
        let mut expressions = Vec::new();
        for p in pairs {
            match p.as_rule() {
                Rule::anonymous_traversal => {
                    expressions.push(Expression {
                        loc: p.loc(),
                        expr: ExpressionType::Traversal(Box::new(self.parse_anon_traversal(p)?)),
                    });
                }
                Rule::traversal => {
                    expressions.push(Expression {
                        loc: p.loc(),
                        expr: ExpressionType::Traversal(Box::new(self.parse_traversal(p)?)),
                    });
                }
                Rule::id_traversal => {
                    expressions.push(Expression {
                        loc: p.loc(),
                        expr: ExpressionType::Traversal(Box::new(self.parse_traversal(p)?)),
                    });
                }
                Rule::evaluates_to_bool => {
                    expressions.push(self.parse_boolean_expression(p)?);
                }
                _ => unreachable!(),
            }
        }
        Ok(expressions)
    }

    fn parse_boolean_expression(&self, pair: Pair<Rule>) -> Result<Expression, ParserError> {
        let expression = pair.into_inner().next().unwrap();
        match expression.as_rule() {
            Rule::and => Ok(Expression {
                loc: expression.loc(),
                expr: ExpressionType::And(self.parse_expression_vec(expression.into_inner())?),
            }),
            Rule::or => Ok(Expression {
                loc: expression.loc(),
                expr: ExpressionType::Or(self.parse_expression_vec(expression.into_inner())?),
            }),
            Rule::boolean => Ok(Expression {
                loc: expression.loc(),
                expr: ExpressionType::BooleanLiteral(expression.as_str() == "true"),
            }),
            Rule::exists => Ok(Expression {
                loc: expression.loc(),
                expr: ExpressionType::Exists(Box::new(Expression {
                    loc: expression.loc(),
                    expr: ExpressionType::Traversal(Box::new(
                        self.parse_anon_traversal(expression.into_inner().next().unwrap())?,
                    )),
                })),
            }),

            _ => unreachable!(),
        }
    }

    fn parse_expression(&self, p: Pair<Rule>) -> Result<Expression, ParserError> {
        let pair = p
            .into_inner()
            .next()
            .ok_or_else(|| ParserError::from("Empty expression"))?;

        match pair.as_rule() {
            Rule::traversal => Ok(Expression {
                loc: pair.loc(),
                expr: ExpressionType::Traversal(Box::new(self.parse_traversal(pair)?)),
            }),
            Rule::id_traversal => Ok(Expression {
                loc: pair.loc(),
                expr: ExpressionType::Traversal(Box::new(self.parse_traversal(pair)?)),
            }),
            Rule::anonymous_traversal => Ok(Expression {
                loc: pair.loc(),
                expr: ExpressionType::Traversal(Box::new(self.parse_anon_traversal(pair)?)),
            }),
            Rule::identifier => Ok(Expression {
                loc: pair.loc(),
                expr: ExpressionType::Identifier(pair.as_str().to_string()),
            }),
            Rule::string_literal => Ok(Expression {
                loc: pair.loc(),
                expr: ExpressionType::StringLiteral(self.parse_string_literal(pair)?),
            }),
            Rule::exists => {
                let traversal = pair
                    .clone()
                    .into_inner()
                    .next()
                    .ok_or_else(|| ParserError::from("Missing exists traversal"))?;
                Ok(Expression {
                    loc: pair.loc(),
                    expr: ExpressionType::Exists(Box::new(Expression {
                        loc: pair.loc(),
                        expr: ExpressionType::Traversal(Box::new(match traversal.as_rule() {
                            Rule::traversal => self.parse_traversal(traversal)?,
                            Rule::id_traversal => self.parse_traversal(traversal)?,
                            _ => unreachable!(),
                        })),
                    })),
                })
            }
            Rule::integer => pair
                .as_str()
                .parse()
                .map(|i| Expression {
                    loc: pair.loc(),
                    expr: ExpressionType::IntegerLiteral(i),
                })
                .map_err(|_| ParserError::from("Invalid integer literal")),
            Rule::float => pair
                .as_str()
                .parse()
                .map(|f| Expression {
                    loc: pair.loc(),
                    expr: ExpressionType::FloatLiteral(f),
                })
                .map_err(|_| ParserError::from("Invalid float literal")),
            Rule::boolean => Ok(Expression {
                loc: pair.loc(),
                expr: ExpressionType::BooleanLiteral(pair.as_str() == "true"),
            }),
            Rule::evaluates_to_bool => Ok(self.parse_boolean_expression(pair)?),
            Rule::AddN => Ok(Expression {
                loc: pair.loc(),
                expr: ExpressionType::AddNode(self.parse_add_node(pair)?),
            }),
            Rule::AddV => Ok(Expression {
                loc: pair.loc(),
                expr: ExpressionType::AddVector(self.parse_add_vector(pair)?),
            }),
            Rule::BatchAddV => Ok(Expression {
                loc: pair.loc(),
                expr: ExpressionType::BatchAddVector(self.parse_batch_add_vector(pair)?),
            }),
            Rule::AddE => Ok(Expression {
                loc: pair.loc(),
                expr: ExpressionType::AddEdge(self.parse_add_edge(pair, false)?),
            }),
            Rule::search_vector => Ok(Expression {
                loc: pair.loc(),
                expr: ExpressionType::SearchVector(self.parse_search_vector(pair)?),
            }),
            Rule::none => Ok(Expression {
                loc: pair.loc(),
                expr: ExpressionType::Empty,
            }),
            Rule::bm25_search => Ok(Expression {
                loc: pair.loc(),
                expr: ExpressionType::BM25Search(self.parse_bm25_search(pair)?),
            }),
            _ => Err(ParserError::from(format!(
                "Unexpected expression type: {:?}",
                pair.as_rule()
            ))),
        }
    }

    fn parse_string_literal(&self, pair: Pair<Rule>) -> Result<String, ParserError> {
        let inner = pair
            .into_inner()
            .next()
            .ok_or_else(|| ParserError::from("Empty string literal"))?;

        let mut literal = inner.as_str().to_string();
        literal.retain(|c| c != '"');
        Ok(literal)
    }

    fn parse_traversal(&self, pair: Pair<Rule>) -> Result<Traversal, ParserError> {
        let mut pairs = pair.clone().into_inner();
        let start = self.parse_start_node(pairs.next().unwrap())?;
        let steps = pairs
            .map(|p| self.parse_step(p))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Traversal {
            start,
            steps,
            loc: pair.loc(),
        })
    }

    fn parse_anon_traversal(&self, pair: Pair<Rule>) -> Result<Traversal, ParserError> {
        let pairs = pair.clone().into_inner();
        let start = StartNode::Anonymous;
        let steps = pairs
            .map(|p| self.parse_step(p))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Traversal {
            start,
            steps,
            loc: pair.loc(),
        })
    }

    fn parse_start_node(&self, pair: Pair<Rule>) -> Result<StartNode, ParserError> {
        match pair.as_rule() {
            Rule::start_node => {
                let pairs = pair.into_inner();
                let mut node_type = String::new();
                let mut ids = None;
                for p in pairs {
                    match p.as_rule() {
                        Rule::type_args => {
                            node_type = p.into_inner().next().unwrap().as_str().to_string();
                            // WATCH
                        }
                        Rule::id_args => {
                            ids = Some(
                                p.into_inner()
                                    .map(|id| {
                                        let id = id.into_inner().next().unwrap();
                                        match id.as_rule() {
                                            Rule::identifier => IdType::Identifier {
                                                value: id.as_str().to_string(),
                                                loc: id.loc(),
                                            },
                                            Rule::string_literal => IdType::Literal {
                                                value: id.as_str().to_string(),
                                                loc: id.loc(),
                                            },
                                            _ => {
                                                panic!("Should be identifier or string literal")
                                            }
                                        }
                                    })
                                    .collect::<Vec<_>>(),
                            );
                        }
                        Rule::by_index => {
                            ids = Some({
                                let mut pairs: Pairs<'_, Rule> = p.clone().into_inner();
                                let index = match pairs.next().unwrap().clone().into_inner().next()
                                {
                                    Some(id) => match id.as_rule() {
                                        Rule::identifier => IdType::Identifier {
                                            value: id.as_str().to_string(),
                                            loc: id.loc(),
                                        },
                                        Rule::string_literal => IdType::Literal {
                                            value: id.as_str().to_string(),
                                            loc: id.loc(),
                                        },
                                        other => {
                                            panic!(
                                                "Should be identifier or string literal: {other:?}"
                                            )
                                        }
                                    },
                                    None => return Err(ParserError::from("Missing index")),
                                };
                                let value = match pairs.next().unwrap().into_inner().next() {
                                    Some(val) => match val.as_rule() {
                                        Rule::identifier => ValueType::Identifier {
                                            value: val.as_str().to_string(),
                                            loc: val.loc(),
                                        },
                                        Rule::string_literal => ValueType::Literal {
                                            value: Value::from(val.as_str()),
                                            loc: val.loc(),
                                        },
                                        Rule::integer => ValueType::Literal {
                                            value: Value::from(
                                                val.as_str().parse::<i64>().unwrap(),
                                            ),
                                            loc: val.loc(),
                                        },
                                        Rule::float => ValueType::Literal {
                                            value: Value::from(
                                                val.as_str().parse::<f64>().unwrap(),
                                            ),
                                            loc: val.loc(),
                                        },
                                        Rule::boolean => ValueType::Literal {
                                            value: Value::from(
                                                val.as_str().parse::<bool>().unwrap(),
                                            ),
                                            loc: val.loc(),
                                        },
                                        _ => {
                                            panic!("Should be identifier or string literal")
                                        }
                                    },
                                    _ => unreachable!(),
                                };
                                vec![IdType::ByIndex {
                                    index: Box::new(index),
                                    value: Box::new(value),
                                    loc: p.loc(),
                                }]
                            })
                        }
                        _ => unreachable!(),
                    }
                }
                Ok(StartNode::Node { node_type, ids })
            }
            Rule::start_edge => {
                let pairs = pair.into_inner();
                let mut edge_type = String::new();
                let mut ids = None;
                for p in pairs {
                    match p.as_rule() {
                        Rule::type_args => {
                            edge_type = p.into_inner().next().unwrap().as_str().to_string();
                        }
                        Rule::id_args => {
                            ids = Some(
                                p.into_inner()
                                    .map(|id| {
                                        let id = id.into_inner().next().unwrap();
                                        match id.as_rule() {
                                            Rule::identifier => IdType::Identifier {
                                                value: id.as_str().to_string(),
                                                loc: id.loc(),
                                            },
                                            Rule::string_literal => IdType::Literal {
                                                value: id.as_str().to_string(),
                                                loc: id.loc(),
                                            },
                                            other => {
                                                println!("{other:?}");
                                                panic!("Should be identifier or string literal")
                                            }
                                        }
                                    })
                                    .collect::<Vec<_>>(),
                            );
                        }
                        _ => unreachable!(),
                    }
                }
                Ok(StartNode::Edge { edge_type, ids })
            }
            Rule::identifier => Ok(StartNode::Identifier(pair.as_str().to_string())),
            _ => Ok(StartNode::Anonymous),
        }
    }

    fn parse_step(&self, pair: Pair<Rule>) -> Result<Step, ParserError> {
        let inner = pair.clone().into_inner().next().unwrap();
        match inner.as_rule() {
            Rule::graph_step => Ok(Step {
                loc: inner.loc(),
                step: StepType::Node(self.parse_graph_step(inner)),
            }),
            Rule::object_step => Ok(Step {
                loc: inner.loc(),
                step: StepType::Object(self.parse_object_step(inner)?),
            }),
            Rule::closure_step => Ok(Step {
                loc: inner.loc(),
                step: StepType::Closure(self.parse_closure(inner)?),
            }),
            Rule::where_step => Ok(Step {
                loc: inner.loc(),
                step: StepType::Where(Box::new(self.parse_expression(inner)?)),
            }),
            Rule::range_step => Ok(Step {
                loc: inner.loc(),
                step: StepType::Range(self.parse_range(pair)?),
            }),

            Rule::bool_operations => Ok(Step {
                loc: inner.loc(),
                step: StepType::BooleanOperation(self.parse_bool_operation(inner)?),
            }),
            Rule::count => Ok(Step {
                loc: inner.loc(),
                step: StepType::Count,
            }),
            Rule::ID => Ok(Step {
                loc: inner.loc(),
                step: StepType::Object(Object {
                    fields: vec![FieldAddition {
                        key: "id".to_string(),
                        value: FieldValue {
                            loc: pair.loc(),
                            value: FieldValueType::Identifier("id".to_string()),
                        },
                        loc: pair.loc(),
                    }],
                    should_spread: false,
                    loc: pair.loc(),
                }),
            }),
            Rule::update => Ok(Step {
                loc: inner.loc(),
                step: StepType::Update(self.parse_update(inner)?),
            }),
            Rule::exclude_field => Ok(Step {
                loc: inner.loc(),
                step: StepType::Exclude(self.parse_exclude(inner)?),
            }),
            Rule::AddE => Ok(Step {
                loc: inner.loc(),
                step: StepType::AddEdge(self.parse_add_edge(inner, true)?),
            }),
            Rule::order_by_asc => Ok(Step {
                loc: inner.loc(),
                step: StepType::OrderByAsc(Box::new(self.parse_expression(inner)?)),
            }),
            Rule::order_by_desc => Ok(Step {
                loc: inner.loc(),
                step: StepType::OrderByDesc(Box::new(self.parse_expression(inner)?)),
            }),
            _ => Err(ParserError::from(format!(
                "Unexpected step type: {:?}",
                inner.as_rule()
            ))),
        }
    }

    fn parse_range(&self, pair: Pair<Rule>) -> Result<(Expression, Expression), ParserError> {
        let mut inner = pair.into_inner().next().unwrap().into_inner();
        // println!("inner: {:?}", inner);
        let start = self.parse_expression(inner.next().unwrap())?;
        let end = self.parse_expression(inner.next().unwrap())?;

        Ok((start, end))
    }

    fn parse_graph_step(&self, pair: Pair<Rule>) -> GraphStep {
        let types = |pair: &Pair<Rule>| {
            pair.clone()
                .into_inner()
                .next()
                .map(|p| p.as_str().to_string())
                .ok_or_else(|| ParserError::from("Expected type".to_string()))
                .unwrap()
        }; // TODO: change to error
        let pair = pair.into_inner().next().unwrap(); // TODO: change to error
        match pair.as_rule() {
            // s if s.starts_with("OutE") => GraphStep {
            //     loc: pair.loc(),
            //     step: GraphStepType::OutE(types),
            // },
            // s if s.starts_with("InE") => GraphStep {
            //     loc: pair.loc(),
            //     step: GraphStepType::InE(types),
            // },
            // s if s.starts_with("FromN") => GraphStep {
            //     loc: pair.loc(),
            //     step: GraphStepType::FromN,
            // },
            // s if s.starts_with("ToN") => GraphStep {
            //     loc: pair.loc(),
            //     step: GraphStepType::ToN,
            // },
            // s if s.starts_with("Out") => GraphStep {
            //     loc: pair.loc(),
            //     step: GraphStepType::Out(types),
            // },
            // s if s.starts_with("In") => GraphStep {
            //     loc: pair.loc(),
            //     step: GraphStepType::In(types),
            // },
            // _ => {
            //     println!("rule_str: {:?}", rule_str);
            //     unreachable!()
            // }
            Rule::out_e => {
                let types = types(&pair);
                GraphStep {
                    loc: pair.loc(),
                    step: GraphStepType::OutE(types),
                }
            }
            Rule::in_e => {
                let types = types(&pair);
                GraphStep {
                    loc: pair.loc(),
                    step: GraphStepType::InE(types),
                }
            }
            Rule::from_n => GraphStep {
                loc: pair.loc(),
                step: GraphStepType::FromN,
            },
            Rule::to_n => GraphStep {
                loc: pair.loc(),
                step: GraphStepType::ToN,
            },
            Rule::from_v => GraphStep {
                loc: pair.loc(),
                step: GraphStepType::FromV,
            },
            Rule::to_v => GraphStep {
                loc: pair.loc(),
                step: GraphStepType::ToV,
            },
            Rule::out => {
                let types = types(&pair);
                GraphStep {
                    loc: pair.loc(),
                    step: GraphStepType::Out(types),
                }
            }
            Rule::in_nodes => {
                let types = types(&pair);
                GraphStep {
                    loc: pair.loc(),
                    step: GraphStepType::In(types),
                }
            }
            Rule::shortest_path => {
                let (type_arg, from, to) = pair.clone().into_inner().fold(
                    (None, None, None),
                    |(type_arg, from, to), p| match p.as_rule() {
                        Rule::type_args => (
                            Some(p.into_inner().next().unwrap().as_str().to_string()),
                            from,
                            to,
                        ),
                        Rule::to_from => match p.into_inner().next() {
                            Some(p) => match p.as_rule() {
                                Rule::to => (
                                    type_arg,
                                    from,
                                    Some(p.into_inner().next().unwrap().as_str().to_string()),
                                ),
                                Rule::from => (
                                    type_arg,
                                    Some(p.into_inner().next().unwrap().as_str().to_string()),
                                    to,
                                ),
                                _ => unreachable!(),
                            },
                            None => (type_arg, from, to),
                        },
                        _ => (type_arg, from, to),
                    },
                );

                // TODO: add error handling and check about IdType as might not always be data.
                // possibly use stack to keep track of variables and use them via precedence and then check on type
                // e.g. if valid variable and is param then use data. otherwise use plain identifier
                GraphStep {
                    loc: pair.loc(),
                    step: GraphStepType::ShortestPath(ShortestPath {
                        loc: pair.loc(),
                        from: from.map(|id| IdType::Identifier {
                            value: id,
                            loc: pair.loc(),
                        }),
                        to: to.map(|id| IdType::Identifier {
                            value: id,
                            loc: pair.loc(),
                        }),
                        type_arg,
                    }),
                }
            }
            Rule::search_vector => GraphStep {
                loc: pair.loc(),
                step: GraphStepType::SearchVector(self.parse_search_vector(pair).unwrap()),
            },
            _ => {
                println!("rule_str: {:?}", pair.as_str());
                unreachable!()
            }
        }
    }

    fn parse_bool_operation(&self, pair: Pair<Rule>) -> Result<BooleanOp, ParserError> {
        let inner = pair.clone().into_inner().next().unwrap();
        let expr = match inner.as_rule() {
            Rule::GT => BooleanOp {
                loc: pair.loc(),
                op: BooleanOpType::GreaterThan(Box::new(
                    self.parse_expression(inner.into_inner().next().unwrap())?,
                )),
            },
            Rule::GTE => BooleanOp {
                loc: pair.loc(),
                op: BooleanOpType::GreaterThanOrEqual(Box::new(
                    self.parse_expression(inner.into_inner().next().unwrap())?,
                )),
            },
            Rule::LT => BooleanOp {
                loc: pair.loc(),
                op: BooleanOpType::LessThan(Box::new(
                    self.parse_expression(inner.into_inner().next().unwrap())?,
                )),
            },
            Rule::LTE => BooleanOp {
                loc: pair.loc(),
                op: BooleanOpType::LessThanOrEqual(Box::new(
                    self.parse_expression(inner.into_inner().next().unwrap())?,
                )),
            },
            Rule::EQ => BooleanOp {
                loc: pair.loc(),
                op: BooleanOpType::Equal(Box::new(
                    self.parse_expression(inner.into_inner().next().unwrap())?,
                )),
            },
            Rule::NEQ => BooleanOp {
                loc: pair.loc(),
                op: BooleanOpType::NotEqual(Box::new(
                    self.parse_expression(inner.into_inner().next().unwrap())?,
                )),
            },
            _ => return Err(ParserError::from("Invalid boolean operation")),
        };
        Ok(expr)
    }

    fn parse_field_additions(&self, pair: Pair<Rule>) -> Result<Vec<FieldAddition>, ParserError> {
        pair.into_inner()
            .map(|p| self.parse_new_field_pair(p))
            .collect()
    }

    fn parse_new_field_pair(&self, pair: Pair<Rule>) -> Result<FieldAddition, ParserError> {
        let mut pairs = pair.clone().into_inner();
        let key = pairs.next().unwrap().as_str().to_string();
        let value_pair = pairs.next().unwrap();

        let value: FieldValue = match value_pair.as_rule() {
            Rule::evaluates_to_anything => FieldValue {
                loc: value_pair.loc(),
                value: FieldValueType::Expression(self.parse_expression(value_pair)?),
            },
            Rule::anonymous_traversal => FieldValue {
                loc: value_pair.loc(),
                value: FieldValueType::Traversal(Box::new(self.parse_traversal(value_pair)?)),
            },
            Rule::object_step => FieldValue {
                loc: value_pair.loc(),
                value: FieldValueType::Fields(self.parse_field_additions(value_pair)?),
            },
            Rule::string_literal => FieldValue {
                loc: value_pair.loc(),
                value: FieldValueType::Literal(Value::String(
                    self.parse_string_literal(value_pair)?,
                )),
            },
            Rule::integer => FieldValue {
                loc: value_pair.loc(),
                value: FieldValueType::Literal(Value::I32(
                    value_pair
                        .as_str()
                        .parse()
                        .map_err(|_| ParserError::from("Invalid integer literal"))?,
                )),
            },
            Rule::float => FieldValue {
                loc: value_pair.loc(),
                value: FieldValueType::Literal(Value::F64(
                    value_pair
                        .as_str()
                        .parse()
                        .map_err(|_| ParserError::from("Invalid float literal"))?,
                )),
            },
            Rule::boolean => FieldValue {
                loc: value_pair.loc(),
                value: FieldValueType::Literal(Value::Boolean(value_pair.as_str() == "true")),
            },
            Rule::none => FieldValue {
                loc: value_pair.loc(),
                value: FieldValueType::Empty,
            },
            Rule::mapping_field => FieldValue {
                loc: value_pair.loc(),
                value: FieldValueType::Fields(self.parse_field_additions(value_pair)?),
            },
            _ => {
                return Err(ParserError::from(format!(
                    "Unexpected field pair type: {:?} \n {:?} \n\n {:?}",
                    value_pair.as_rule(),
                    value_pair,
                    pair
                )));
            }
        };

        Ok(FieldAddition {
            loc: pair.loc(),
            key,
            value,
        })
    }

    fn parse_new_field_value(&self, pair: Pair<Rule>) -> Result<FieldValue, ParserError> {
        let value_pair = pair.into_inner().next().unwrap();
        let value: FieldValue = match value_pair.as_rule() {
            Rule::evaluates_to_anything => FieldValue {
                loc: value_pair.loc(),
                value: FieldValueType::Expression(self.parse_expression(value_pair)?),
            },
            Rule::anonymous_traversal => FieldValue {
                loc: value_pair.loc(),
                value: FieldValueType::Traversal(Box::new(self.parse_traversal(value_pair)?)),
            },
            Rule::object_step => FieldValue {
                loc: value_pair.loc(),
                value: FieldValueType::Fields(self.parse_field_additions(value_pair)?),
            },
            Rule::string_literal => FieldValue {
                loc: value_pair.loc(),
                value: FieldValueType::Literal(Value::String(
                    self.parse_string_literal(value_pair)?,
                )),
            },
            Rule::integer => FieldValue {
                loc: value_pair.loc(),
                value: FieldValueType::Literal(Value::I32(
                    value_pair
                        .as_str()
                        .parse()
                        .map_err(|_| ParserError::from("Invalid integer literal"))?,
                )),
            },
            Rule::float => FieldValue {
                loc: value_pair.loc(),
                value: FieldValueType::Literal(Value::F64(
                    value_pair
                        .as_str()
                        .parse()
                        .map_err(|_| ParserError::from("Invalid float literal"))?,
                )),
            },
            Rule::boolean => FieldValue {
                loc: value_pair.loc(),
                value: FieldValueType::Literal(Value::Boolean(value_pair.as_str() == "true")),
            },
            Rule::none => FieldValue {
                loc: value_pair.loc(),
                value: FieldValueType::Empty,
            },
            Rule::mapping_field => FieldValue {
                loc: value_pair.loc(),
                value: FieldValueType::Fields(self.parse_field_additions(value_pair)?),
            },
            _ => {
                return Err(ParserError::from(format!(
                    "Unexpected field value type: {:?} \n {:?}",
                    value_pair.as_rule(),
                    value_pair,
                )));
            }
        };

        Ok(value)
    }

    fn parse_update(&self, pair: Pair<Rule>) -> Result<Update, ParserError> {
        let fields = self.parse_field_additions(pair.clone())?;
        Ok(Update {
            fields,
            loc: pair.loc(),
        })
    }

    fn parse_object_step(&self, pair: Pair<Rule>) -> Result<Object, ParserError> {
        let mut fields = Vec::new();
        let mut should_spread = false;
        for p in pair.clone().into_inner() {
            if p.as_rule() == Rule::spread_object {
                println!("spread_object");
                should_spread = true;
                continue;
            }
            let mut pairs = p.clone().into_inner();
            let prop_key = pairs.next().unwrap().as_str().to_string();
            let field_addition = match pairs.next() {
                Some(p) => match p.as_rule() {
                    Rule::evaluates_to_anything => FieldValue {
                        loc: p.loc(),
                        value: FieldValueType::Expression(self.parse_expression(p)?),
                    },
                    Rule::anonymous_traversal => FieldValue {
                        loc: p.loc(),
                        value: FieldValueType::Traversal(Box::new(self.parse_anon_traversal(p)?)),
                    },
                    Rule::mapping_field => FieldValue {
                        loc: p.loc(),
                        value: FieldValueType::Fields(self.parse_field_additions(p)?),
                    },
                    Rule::object_step => FieldValue {
                        loc: p.clone().loc(),
                        value: FieldValueType::Fields(self.parse_object_step(p.clone())?.fields),
                    },
                    _ => self.parse_new_field_value(p)?,
                },
                None if !prop_key.is_empty() => FieldValue {
                    loc: p.loc(),
                    value: FieldValueType::Identifier(prop_key.clone()),
                },
                None => FieldValue {
                    loc: p.loc(),
                    value: FieldValueType::Empty,
                },
            };
            fields.push(FieldAddition {
                loc: p.loc(),
                key: prop_key,
                value: field_addition,
            });
        }
        Ok(Object {
            loc: pair.loc(),
            fields,
            should_spread,
        })
    }

    fn parse_closure(&self, pair: Pair<Rule>) -> Result<Closure, ParserError> {
        let mut pairs = pair.clone().into_inner();
        let identifier = pairs.next().unwrap().as_str().to_string();
        let object = self.parse_object_step(pairs.next().unwrap())?;
        Ok(Closure {
            loc: pair.loc(),
            identifier,
            object,
        })
    }

    fn parse_exclude(&self, pair: Pair<Rule>) -> Result<Exclude, ParserError> {
        let mut fields = Vec::new();
        for p in pair.clone().into_inner() {
            fields.push((p.loc(), p.as_str().to_string()));
        }
        Ok(Exclude {
            loc: pair.loc(),
            fields,
        })
    }
}
