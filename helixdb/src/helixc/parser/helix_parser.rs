use super::{
    location::{HasLoc, Loc},
    parser_methods::ParserError,
};
use crate::protocol::value::Value;
use pest::{
    iterators::{Pair, Pairs},
    Parser as PestParser,
};
use pest_derive::Parser;
use std::{
    collections::{HashMap, HashSet},
    fmt::{Debug, Display},
    io::Write,
    path::Path,
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
#[derive(Debug, Clone)]
pub struct Source {
    pub source: String,
    pub node_schemas: Vec<NodeSchema>,
    pub edge_schemas: Vec<EdgeSchema>,
    pub vector_schemas: Vec<VectorSchema>,
    pub queries: Vec<Query>,
}

impl Default for Source {
    fn default() -> Self {
        Source {
            source: String::new(),
            node_schemas: Vec::new(),
            edge_schemas: Vec::new(),
            vector_schemas: Vec::new(),
            queries: Vec::new(),
        }
    }
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

#[derive(Debug, Clone)]
pub enum DefaultValue {
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
            (FieldType::F32, FieldType::F32) => true,
            (FieldType::F64, FieldType::F64) => true,
            (FieldType::I8, FieldType::I8) => true,
            (FieldType::I16, FieldType::I16) => true,
            (FieldType::I32, FieldType::I32) => true,
            (FieldType::I64, FieldType::I64) => true,
            (FieldType::U8, FieldType::U8) => true,
            (FieldType::U16, FieldType::U16) => true,
            (FieldType::U32, FieldType::U32) => true,
            (FieldType::U64, FieldType::U64) => true,
            (FieldType::U128, FieldType::U128) => true,
            (FieldType::Boolean, FieldType::Boolean) => true,
            (FieldType::Uuid, FieldType::Uuid) => true,
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
            FieldType::Date => todo!(),
            FieldType::Array(t) => write!(f, "Array({})", t),
            FieldType::Identifier(s) => write!(f, "{}", s),
            FieldType::Object(m) => {
                write!(f, "{{")?;
                for (k, v) in m {
                    write!(f, "{}: {}", k, v)?;
                }
                write!(f, "}}")
            } // FieldType::Closure(a, b) => write!(f, "Closure({})", a),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Query {
    pub original_query: String,
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
    AddVector(AddVector),
    AddNode(AddNode),
    AddEdge(AddEdge),
    Drop(Expression),
    SearchVector(SearchVector),
    BatchAddVector(BatchAddVector),
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
    Empty,
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
    AddEdge(AddEdge),
    SearchVector(String),
}
impl PartialEq<StepType> for StepType {
    fn eq(&self, other: &StepType) -> bool {
        match (self, other) {
            (&StepType::Node(_), &StepType::Node(_)) => true,
            (&StepType::Edge(_), &StepType::Edge(_)) => true,
            (&StepType::Where(_), &StepType::Where(_)) => true,
            (&StepType::BooleanOperation(_), &StepType::BooleanOperation(_)) => true,
            (&StepType::Count, &StepType::Count) => true,
            (&StepType::Update(_), &StepType::Update(_)) => true,
            (&StepType::Object(_), &StepType::Object(_)) => true,
            (&StepType::Exclude(_), &StepType::Exclude(_)) => true,
            (&StepType::Closure(_), &StepType::Closure(_)) => true,
            (&StepType::Range(_), &StepType::Range(_)) => true,
            (&StepType::AddEdge(_), &StepType::AddEdge(_)) => true,
            (&StepType::SearchVector(_), &StepType::SearchVector(_)) => true,
            _ => false,
        }
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

    OutE(String),
    InE(String),

    ShortestPath(ShortestPath),
}
impl GraphStep {
    pub fn get_item_type(&self) -> Option<String> {
        match &self.step {
            GraphStepType::Out(s) => Some(s.clone()),
            GraphStepType::In(s) => Some(s.clone()),
            GraphStepType::OutE(s) => Some(s.clone()),
            GraphStepType::InE(s) => Some(s.clone()),
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
        value: Box<IdType>,
        loc: Loc,
    },
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
            IdType::Literal { mut value, loc } => {
                value.retain(|c| c != '"');
                value
            }
            IdType::Identifier { value, loc } => value,
            IdType::ByIndex { index, value, loc } => {
                format!("{{ {} : {} }}", String::from(*index), String::from(*value))
            }
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
            source.source.push_str("\n");
            let pair = match HelixParser::parse(Rule::source, &file.content) {
                Ok(mut pairs) => {
                    let pair = pairs
                        .next()
                        .ok_or_else(|| ParserError::from("Empty input"))?;
                    pair
                }
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
        schema: Option<&Source>,
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
                return Ok(FieldType::Array(Box::new(
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
                        schema,
                    )?,
                )));
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
        println!("defaults: {:?}", defaults);

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
        let name = pairs.next().unwrap().as_str().to_string();
        let parameters = self.parse_parameters(pairs.next().unwrap())?;
        let nect = pairs.next().unwrap();
        let statements = self.parse_query_body(nect)?;
        let return_values = self.parse_return_statement(pairs.next().unwrap())?;

        Ok(Query {
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
                Rule::AddN => Ok(Statement {
                    loc: p.loc(),
                    statement: StatementType::AddNode(self.parse_add_node(p)?),
                }),
                Rule::AddV => Ok(Statement {
                    loc: p.loc(),
                    statement: StatementType::AddVector(self.parse_add_vector(p)?),
                }),
                Rule::AddE => Ok(Statement {
                    loc: p.loc(),
                    statement: StatementType::AddEdge(self.parse_add_edge(p, false)?),
                }),
                Rule::drop => Ok(Statement {
                    loc: p.loc(),
                    statement: StatementType::Drop(self.parse_expression(p)?),
                }),
                Rule::BatchAddV => Ok(Statement {
                    loc: p.loc(),
                    statement: StatementType::BatchAddVector(self.parse_batch_add_vector(p)?),
                }),
                Rule::search_vector => Ok(Statement {
                    loc: p.loc(),
                    statement: StatementType::SearchVector(self.parse_search_vector(p)?),
                }),
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

    fn parse_for_loop(&self, pair: Pair<Rule>) -> Result<ForLoop, ParserError> {
        let mut pairs = pair.clone().into_inner();
        // parse the arguments
        let argument = pairs.next().unwrap().clone().into_inner().next().unwrap();
        let argument_loc = argument.loc();
        let variable = match argument.as_rule() {
            Rule::object_destructuring => {
                let fields = argument
                    .into_inner()
                    .into_iter()
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
                    )))
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
                Rule::vector_data => match p.clone().into_inner().next().unwrap().as_rule() {
                    Rule::identifier => {
                        data = Some(VectorData::Identifier(p.as_str().to_string()));
                    }
                    Rule::vec_literal => {
                        data = Some(VectorData::Vector(self.parse_vec_literal(p)?));
                    }
                    _ => unreachable!(),
                },
                Rule::create_field => {
                    fields = Some(self.parse_property_assignments(p)?);
                }
                _ => {
                    return Err(ParserError::from(format!(
                        "Unexpected rule in AddV: {:?} => {:?}",
                        p.as_rule(),
                        p,
                    )))
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
                Rule::vector_data => match p.clone().into_inner().next().unwrap().as_rule() {
                    Rule::identifier => {
                        data = Some(VectorData::Identifier(p.as_str().to_string()));
                    }
                    Rule::vec_literal => {
                        data = Some(VectorData::Vector(self.parse_vec_literal(p)?));
                    }
                    _ => unreachable!(),
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
                    )))
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
                    )))
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
                            Rule::string_literal => Ok(ValueType::from(Value::from(
                                value_pair.as_str().to_string(),
                            ))),
                            Rule::integer => value_pair
                                .as_str()
                                .parse()
                                .map(|i| ValueType::from(Value::I32(i)))
                                .map_err(|_| ParserError::from("Invalid integer value")),
                            Rule::float => value_pair
                                .as_str()
                                .parse()
                                .map(|f| ValueType::from(Value::F64(f)))
                                .map_err(|_| ParserError::from("Invalid float value")),
                            Rule::boolean => Ok(ValueType::from(Value::Boolean(
                                value_pair.as_str() == "true",
                            ))),
                            Rule::identifier => Ok(ValueType::Identifier {
                                value: value_pair.as_str().to_string(),
                                loc: value_pair.loc(),
                            }),
                            _ => Err(ParserError::from("Invalid property value type")),
                        }?
                    }
                    None => ValueType::from(Value::Empty),
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
                    )))
                }
            }
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
            from_id: from_id,
            to_id: to_id,
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
                                            other => {
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
                                    println!("pairs: {:?}", pairs);
                                    let index = match pairs.next().unwrap().clone().into_inner().next() {
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
                                                    "Should be identifier or string literal: {:?}",
                                                    other
                                                )
                                            }
                                        },
                                        None => return Err(ParserError::from("Missing index")),
                                    };
                                    println!("index: {:?}", index); 
                                    println!("pairs: {:?}", pairs);
                                    let value = match pairs.next().unwrap().into_inner().next() {
                                        Some(val) => match val.as_rule() {
                                            Rule::identifier => IdType::Identifier {
                                                value: val.as_str().to_string(),
                                                loc: val.loc(),
                                            },
                                            Rule::string_literal => IdType::Literal {
                                                value: val.as_str().to_string(),
                                                loc: val.loc(),
                                            },
                                            other => {
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
                                                println!("{:?}", other);
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
                            value: FieldValueType::Empty,
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
            _ => Err(ParserError::from(format!(
                "Unexpected step type: {:?}",
                inner.as_rule()
            ))),
        }
    }

    fn parse_range(&self, pair: Pair<Rule>) -> Result<(Expression, Expression), ParserError> {
        let mut inner = pair.into_inner().next().unwrap().into_inner();
        // println!("inner: {:?}", inner);
        let start = match self.parse_expression(inner.next().unwrap()) {
            Ok(val) => val,
            Err(e) => return Err(e),
        };
        let end = match self.parse_expression(inner.next().unwrap()) {
            Ok(val) => val,
            Err(e) => return Err(e),
        };

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
                let mut inner = pair.clone().into_inner().next().unwrap().into_inner();

                let (type_arg, from, to) =
                    inner.fold((None, None, None), |(type_arg, from, to), p| {
                        match p.as_rule() {
                            Rule::type_args => (
                                Some(
                                    p.into_inner()
                                        .next()
                                        .unwrap()
                                        .into_inner()
                                        .next()
                                        .unwrap()
                                        .as_str()
                                        .to_string(),
                                ),
                                from,
                                to,
                            ),
                            Rule::from => (
                                type_arg,
                                Some(
                                    p.into_inner()
                                        .next()
                                        .unwrap()
                                        .into_inner()
                                        .next()
                                        .unwrap()
                                        .as_str()
                                        .to_string(),
                                ),
                                to,
                            ),
                            Rule::to => (
                                type_arg,
                                from,
                                Some(
                                    p.into_inner()
                                        .next()
                                        .unwrap()
                                        .into_inner()
                                        .next()
                                        .unwrap()
                                        .as_str()
                                        .to_string(),
                                ),
                            ),
                            _ => (type_arg, from, to),
                        }
                    });

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
            Rule::string_literal => {
                println!("string_literal: {:?}", value_pair);
                FieldValue {
                    loc: value_pair.loc(),
                    value: FieldValueType::Literal(Value::String(
                        self.parse_string_literal(value_pair)?,
                    )),
                }
            }
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
                )))
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
                )))
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
                        value: FieldValueType::Traversal(Box::new(self.parse_traversal(p)?)),
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
                None if prop_key.len() > 0 => FieldValue {
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

// Tests module
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_node_schema() {
        let input = r#"
        N::User {
            Name: String,
            Age: I32
        }
        "#;

        let input = write_to_temp_file(vec![input]);
        let result = HelixParser::parse_source(&input).unwrap();
        assert_eq!(result.node_schemas.len(), 1);
        let schema = &result.node_schemas[0];
        assert_eq!(schema.name.1, "User");
        assert_eq!(schema.fields.len(), 2);
    }

    #[test]
    fn test_parse_edge_schema() {
        let input = r#"
        
        E::Follows {
            From: User,
            To: User,
            Properties: {
                Since: F64
            }
        }
        "#;

        let input = write_to_temp_file(vec![input]);
        let result = HelixParser::parse_source(&input).unwrap();
        assert_eq!(result.edge_schemas.len(), 1);
        let schema = &result.edge_schemas[0];
        assert_eq!(schema.name.1, "Follows");
        assert_eq!(schema.from.1, "User");
        assert_eq!(schema.to.1, "User");
        assert!(schema.properties.is_some());
        let properties = schema.properties.as_ref().unwrap();
        assert_eq!(properties.len(), 1);
        assert_eq!(properties[0].name, "Since");
        matches!(properties[0].field_type, FieldType::F64);
    }

    #[test]
    fn test_parse_edge_schema_no_props() {
        let input = r#"
        
        E::Follows {
            From: User,
            To: User,
            Properties: {
            }
        }
        "#;
        let input = Content {
            content: input.to_string(),
            files: vec![],
            source: Source::default(),
        };
        let result = HelixParser::parse_source(&input).unwrap();
        assert_eq!(result.edge_schemas.len(), 1);
        let schema = &result.edge_schemas[0];
        assert_eq!(schema.name.1, "Follows");
        assert_eq!(schema.from.1, "User");
        assert_eq!(schema.to.1, "User");
        assert!(schema.properties.is_some());
        let properties = schema.properties.as_ref().unwrap();
        assert_eq!(properties.len(), 0);
    }

    #[test]
    fn test_parse_query() {
        let input = r#"
        QUERY FindUser(userName : String) => 
            user <- N<User>
            RETURN user
        "#;

        let input = write_to_temp_file(vec![input]);
        let result = HelixParser::parse_source(&input).unwrap();
        assert_eq!(result.queries.len(), 1);
        let query = &result.queries[0];
        assert_eq!(query.name, "FindUser");
        assert_eq!(query.parameters.len(), 1);
        assert_eq!(query.parameters[0].name.1, "userName");
        assert_eq!(query.statements.len(), 1);
        assert_eq!(query.return_values.len(), 1);
    }

    #[test]
    fn test_query_with_parameters() {
        let input = r#"
        QUERY fetchUsers(name: String, age: I32) =>
            user <- N<USER>("123")
            nameField <- user::{Name}
            ageField <- user::{Age}
            RETURN nameField, ageField
        "#;
        let input = write_to_temp_file(vec![input]);
        let result = HelixParser::parse_source(&input).unwrap();
        assert_eq!(result.queries.len(), 1);
        let query = &result.queries[0];
        assert_eq!(query.name, "fetchUsers");
        assert_eq!(query.parameters.len(), 2);
        assert_eq!(query.parameters[0].name.1, "name");
        assert!(matches!(
            query.parameters[0].param_type.1,
            FieldType::String
        ));
        assert_eq!(query.parameters[1].name.1, "age");
        assert!(matches!(query.parameters[1].param_type.1, FieldType::I32));
        assert_eq!(query.statements.len(), 3);
        assert_eq!(query.return_values.len(), 2);
    }

    #[test]
    fn test_node_definition() {
        let input = r#"
        N::USER {
            ID: String,
            Name: String,
            Age: I32
        }
        "#;
        let input = write_to_temp_file(vec![input]);
        let result = HelixParser::parse_source(&input).unwrap();
        assert_eq!(result.node_schemas.len(), 1);
        let schema = &result.node_schemas[0];
        assert_eq!(schema.name.1, "USER");
        assert_eq!(schema.fields.len(), 3);
    }

    #[test]
    fn test_edge_with_properties() {
        let input = r#"
        E::FRIENDSHIP {
            From: USER,
            To: USER,
            Properties: {
                Since: String,
                Strength: I32
            }
        }
        "#;
        let input = write_to_temp_file(vec![input]);
        let result = HelixParser::parse_source(&input).unwrap();
        assert_eq!(result.edge_schemas.len(), 1);
        let schema = &result.edge_schemas[0];
        assert_eq!(schema.name.1, "FRIENDSHIP");
        assert_eq!(schema.from.1, "USER");
        assert_eq!(schema.to.1, "USER");
        let props = schema.properties.as_ref().unwrap();
        assert_eq!(props.len(), 2);
    }

    #[test]
    fn test_multiple_schemas() {
        let input = r#"
        N::USER {
            ID: String,
            Name: String,
            Email: String
        }
        N::POST {
            ID: String,
            Content: String
        }
        E::LIKES {
            From: USER,
            To: POST,
            Properties: {
                Timestamp: String
            }
        }
        "#;
        let input = write_to_temp_file(vec![input]);
        let result = HelixParser::parse_source(&input).unwrap();
        assert_eq!(result.node_schemas.len(), 2);
        assert_eq!(result.edge_schemas.len(), 1);
    }

    /// THESE FAIL
    ///
    ///
    ///

    #[test]
    fn test_logical_operations() {
        let input = r#"
    QUERY logicalOps(id : String) =>
        user <- N<USER>(id)
        condition <- user::{name}::EQ("Alice")
        condition2 <- user::{age}::GT(20)
        RETURN condition
    "#;
        let input = write_to_temp_file(vec![input]);
        let result = HelixParser::parse_source(&input).unwrap();
        let query = &result.queries[0];
        assert_eq!(query.name, "logicalOps");
        assert_eq!(query.statements.len(), 3);
    }

    #[test]
    fn test_anonymous_traversal() {
        let input = r#"
    QUERY anonymousTraversal() =>
        result <- N::OutE<FRIENDSHIP>::InN::{Age}
        RETURN result
    "#;
        let input = write_to_temp_file(vec![input]);
        let result = HelixParser::parse_source(&input).unwrap();
        let query = &result.queries[0];
        assert_eq!(query.name, "anonymousTraversal");
        assert_eq!(query.statements.len(), 1);
    }

    #[test]
    fn test_edge_traversal() {
        let input = r#"
    QUERY getEdgeInfo() =>
        edge <- E<FRIENDSHIP>("999")
        fromUser <- edge::OutE
        toUser <- edge::OutN
        RETURN fromUser, toUser

    "#;
        let input = write_to_temp_file(vec![input]);
        let result = HelixParser::parse_source(&input).unwrap();
        let query = &result.queries[0];
        assert_eq!(query.statements.len(), 3);
        assert_eq!(query.return_values.len(), 2);
    }

    #[test]
    fn test_exists_query() {
        let input = r#"
        QUERY userExists(id : String) =>
            user <- N<User>(id)
            result <- EXISTS(user::OutE::InN<User>)
            RETURN result
        "#;
        let input = write_to_temp_file(vec![input]);
        let result = HelixParser::parse_source(&input).unwrap();
        assert_eq!(result.queries.len(), 1);
        let query = &result.queries[0];
        assert_eq!(query.name, "userExists");
        assert_eq!(query.parameters.len(), 1);
        assert_eq!(query.statements.len(), 2);
    }

    #[test]
    fn test_multiple_return_values() {
        let input = r#"
    N::USER {
        Name: String,
        Age: Int
    }

    QUERY returnMultipleValues() =>
        user <- N<USER>("999")
        name <- user::{Name}
        age <- user::{Age}
        RETURN name, age
    "#;
        let input = write_to_temp_file(vec![input]);
        let result = HelixParser::parse_source(&input).unwrap();
        let query = &result.queries[0];
        assert_eq!(query.statements.len(), 3);
        assert_eq!(query.return_values.len(), 2);
    }

    #[test]
    fn test_add_fields() {
        let input = r#"
    QUERY enrichUserData() =>
        user <- N<USER>("123")
        enriched <- user::{Name: "name", Follows: _::Out<Follows>::{Age}}
        RETURN enriched
    "#;
        let input = write_to_temp_file(vec![input]);
        let result = HelixParser::parse_source(&input).unwrap();
        let query = &result.queries[0];
        assert_eq!(query.statements.len(), 2);
    }

    #[test]
    fn test_query_with_count() {
        let input = r#"
    QUERY analyzeNetwork() =>
        user <- N<USER>("999")
        friends <- user::Out<FRIENDSHIP>::InN::WHERE(_::Out::COUNT::GT(0))
        friendCount <- activeFriends::COUNT
        RETURN friendCount
    "#;
        let input = write_to_temp_file(vec![input]);
        let result = HelixParser::parse_source(&input).unwrap();
        let query = &result.queries[0];
        assert_eq!(query.statements.len(), 3);
    }

    #[test]
    fn test_add_node_query() {
        let input = r#"
    QUERY analyzeNetwork() =>
        user <- AddN<User>({Name: "Alice"})
        RETURN user
    "#;
        let input = write_to_temp_file(vec![input]);
        let result = match HelixParser::parse_source(&input) {
            Ok(result) => result,
            Err(e) => {
                println!("{:?}", e);
                panic!();
            }
        };
        let query = &result.queries[0];
        // println!("{:?}", query);
        assert_eq!(query.statements.len(), 1);
    }

    #[test]
    fn test_add_edge_query() {
        let input = r#"
    QUERY analyzeNetwork() =>
        edge <- AddE<Rating>({Rating: 5})::To("123")::From("456")
        edge <- AddE<Rating>({Rating: 5, Date: "2025-01-01"})::To("123")::From("456")
        RETURN edge
    "#;
        let input = write_to_temp_file(vec![input]);
        let result = match HelixParser::parse_source(&input) {
            Ok(result) => result,
            Err(e) => {
                println!("{:?}", e);
                panic!();
            }
        };
        let query = &result.queries[0];
        // println!("{:?}", query);
        assert_eq!(query.statements.len(), 2);
    }

    #[test]
    fn test_adding_with_identifiers() {
        let input = r#"
    QUERY addUsers() =>
        user1 <- AddN<User>({Name: "Alice", Age: 30})
        user2 <- AddN<User>({Name: "Bob", Age: 25})
        AddE<Follows>({Since: "1.0"})::From(user1)::To(user2)
        RETURN user1, user2
    "#;
        let input = write_to_temp_file(vec![input]);
        let result = match HelixParser::parse_source(&input) {
            Ok(result) => result,
            Err(e) => {
                println!("{:?}", e);
                panic!();
            }
        };
        // println!("{:?}", result);
        let query = &result.queries[0];
        // println!("{:?}", query);
        assert_eq!(query.statements.len(), 3);
    }

    #[test]
    fn test_where_with_props() {
        let input = r#"
    QUERY getFollows() =>
        user <- N<User>::WHERE(_::{Age}::GT(2))
        user <- N<User>::WHERE(_::GT(2))
        RETURN user, follows
        "#;
        let input = write_to_temp_file(vec![input]);
        let result = match HelixParser::parse_source(&input) {
            Ok(result) => result,
            Err(_e) => {
                panic!();
            }
        };
        let query = &result.queries[0];
        assert_eq!(query.statements.len(), 2);
    }

    #[test]
    fn test_drop_operation() {
        let input = r#"
        QUERY deleteUser(id: String) =>
            user <- N<USER>(id)
            DROP user
            DROP user::OutE
            DROP N::OutE
            RETURN user
        "#;
        let input = write_to_temp_file(vec![input]);
        let result = HelixParser::parse_source(&input).unwrap();
        let query = &result.queries[0];
        assert_eq!(query.name, "deleteUser");
        assert_eq!(query.parameters.len(), 1);
        assert_eq!(query.statements.len(), 4);
    }

    #[test]
    fn test_update_operation() {
        let input = r#"
        QUERY updateUser(id: String) =>
            user <- N<USER>(id)
            x <- user::UPDATE({Name: "NewName"})
            l <- user::UPDATE({Name: "NewName", Age: 30})
            RETURN user
        "#;
        let input = write_to_temp_file(vec![input]);
        let result = HelixParser::parse_source(&input).unwrap();
        let query = &result.queries[0];
        assert_eq!(query.name, "updateUser");
        assert_eq!(query.parameters.len(), 1);
        assert_eq!(query.statements.len(), 3);
    }

    #[test]
    fn test_complex_traversal_combinations() {
        let input = r#"
        QUERY complexTraversal() =>
            result1 <- N<User>::OutE<Follows>::InN<User>::{name}
            result2 <- N::WHERE(AND(
                _::{age}::GT(20),
                OR(_::{name}::EQ("Alice"), _::{name}::EQ("Bob"))
            ))
            result3 <- N<User>::{
                friends: _::Out<Follows>::InN::{name},
                avgFriendAge: _::Out<Follows>::InN::{age}::GT(25)
            }
            RETURN result1, result2, result3
        "#;
        let input = write_to_temp_file(vec![input]);
        let result = HelixParser::parse_source(&input).unwrap();
        let query = &result.queries[0];
        assert_eq!(query.name, "complexTraversal");
        assert_eq!(query.statements.len(), 3);
        assert_eq!(query.return_values.len(), 3);
    }

    #[test]
    fn test_nested_property_operations() {
        let input = r#"
        QUERY nestedProps() =>
            user <- N<User>("123")
            // Test nested property operations
            result <- user::{
                basic: {
                    name: _::{name},
                    age: _::{age}
                },
                social: {
                    friends: _::Out<Follows>::COUNT,
                    groups: _::Out<BelongsTo>::InN<Group>::{name}
                }
            }
            RETURN result
        "#;
        let input = write_to_temp_file(vec![input]);
        let result = HelixParser::parse_source(&input).unwrap();
        let query = &result.queries[0];
        assert_eq!(query.statements.len(), 2);
    }

    #[test]
    fn test_complex_edge_operations() {
        let input = r#"
        QUERY edgeOperations() =>
            edge1 <- AddE<Follows>({since: "2024-01-01", weight: 0.8})::From("user1")::To("user2")
            edge2 <- E<Follows>::WHERE(_::{weight}::GT(0.5))
            edge3 <- edge2::UPDATE({weight: 1.0, updated: "2024-03-01"})
            RETURN edge1, edge2, edge3
        "#;
        let input = write_to_temp_file(vec![input]);
        let result = HelixParser::parse_source(&input).unwrap();
        let query = &result.queries[0];
        assert_eq!(query.statements.len(), 3);
        assert_eq!(query.return_values.len(), 3);
    }

    #[test]
    fn test_mixed_type_operations() {
        let input = r#"
        QUERY mixedTypes() =>
            v1 <- AddN<User>({
                name: "Alice",
                age: 25,
                active: true,
                score: 4.5
            })
            result <- N<User>::WHERE(OR(
                _::{age}::GT(20),
                _::{score}::LT(5.0)
            ))
            RETURN v1, result
        "#;
        let input = write_to_temp_file(vec![input]);
        let result = HelixParser::parse_source(&input).unwrap();
        let query = &result.queries[0];
        assert_eq!(query.statements.len(), 2);
        assert_eq!(query.return_values.len(), 2);
    }

    #[test]
    fn test_error_cases() {
        // Test missing return statement
        let missing_return = r#"
        QUERY noReturn() =>
            result <- N<User>()
        "#;
        let input = write_to_temp_file(vec![missing_return]);
        let result = HelixParser::parse_source(&input);
        assert!(result.is_err());

        // Test invalid property access
        let invalid_props = r#"
        QUERY invalidProps() =>
            result <- N<User>::{}
            RETURN result
        "#;
        let input = write_to_temp_file(vec![invalid_props]);
        let result = HelixParser::parse_source(&input);
        assert!(result.is_err());
    }

    #[test]
    fn test_complex_schema_definitions() {
        let input = r#"
        N::ComplexUser {
            ID: String,
            Name: String,
            Age: I32,
            Score: F64,
            Active: Boolean
        }
        E::ComplexRelation {
            From: ComplexUser,
            To: ComplexUser,
            Properties: {
                StartDate: String,
                EndDate: String,
                Weight: F64,
                Valid: Boolean,
                Count: I32
            }
        }
        "#;
        let input = write_to_temp_file(vec![input]);
        let result = HelixParser::parse_source(&input).unwrap();
        assert_eq!(result.node_schemas.len(), 1);
        assert_eq!(result.edge_schemas.len(), 1);

        let node = &result.node_schemas[0];
        assert_eq!(node.fields.len(), 5);

        let edge = &result.edge_schemas[0];
        let props = edge.properties.as_ref().unwrap();
        assert_eq!(props.len(), 5);
    }

    #[test]
    fn test_query_chaining() {
        let input = r#"
        QUERY chainedOperations() =>
            result <- N<User>("123")
                ::OutE<Follows>
                ::InN<User>
                ::{name}
                ::EQ("Alice")
            filtered <- N<User>::WHERE(
                _::Out<Follows>
                    ::InN<User>
                    ::{age}
                    ::GT(25)
            )
            updated <- filtered
                ::UPDATE({status: "active"})
            has_updated <- updated::{status}
                ::EQ("active")
            RETURN result, filtered, updated, has_updated
        "#;
        let input = write_to_temp_file(vec![input]);
        let result = HelixParser::parse_source(&input).unwrap();
        let query = &result.queries[0];
        assert_eq!(query.statements.len(), 4);
        assert_eq!(query.return_values.len(), 4);
    }

    #[test]
    fn test_property_assignments() {
        let input = r#"
        QUERY testProperties(age: I32) =>
            user <- AddN<User>({
                name: "Alice",
                age: age
            })
            RETURN user
        "#;
        let input = write_to_temp_file(vec![input]);
        let result = HelixParser::parse_source(&input).unwrap();
        let query = &result.queries[0];
        assert_eq!(query.parameters.len(), 1);
    }

    #[test]
    fn test_map_operation() {
        let input = r#"
        QUERY mapOperation() =>
            user <- N<User>("123")
            mapped <- user::{name: "name", age: "age"}
            RETURN mapped
        "#;
        let input = write_to_temp_file(vec![input]);
        let result = HelixParser::parse_source(&input).unwrap();
        let query = &result.queries[0];
        assert_eq!(query.statements.len(), 2);
        assert_eq!(query.return_values.len(), 1);
    }

    #[test]
    fn test_map_in_return() {
        let input = r#"
        QUERY mapInReturn() =>
            user <- N<User>("123")
            RETURN user::{
                name, 
                age
            }
        "#;
        let input = write_to_temp_file(vec![input]);
        let result = HelixParser::parse_source(&input).unwrap();
        let query = &result.queries[0];
        assert_eq!(query.statements.len(), 1);
        assert_eq!(query.return_values.len(), 1);
    }

    #[test]
    fn test_complex_object_operations() {
        let input = r#"
        QUERY complexObjects() =>
            user <- N<User>("123")
            result <- user::{
                basic: {
                    name,
                    age
                },
                friends: _::Out<Follows>::InN::{
                    name,
                    mutualFriends: _::Out<Follows>::COUNT
                }
            }
            RETURN result
        "#;
        let input = write_to_temp_file(vec![input]);
        let result = HelixParser::parse_source(&input).unwrap();
        let query = &result.queries[0];
        assert_eq!(query.statements.len(), 2);
        assert_eq!(query.return_values.len(), 1);
    }

    #[test]
    fn test_exclude_fields() {
        let input = r#"
        QUERY excludeFields() =>
            user <- N<User>("123")
            filtered <- user::!{password, secretKey}
            RETURN filtered
        "#;
        let input = write_to_temp_file(vec![input]);
        let result = HelixParser::parse_source(&input).unwrap();
        let query = &result.queries[0];
        assert_eq!(query.statements.len(), 2);
        assert_eq!(query.return_values.len(), 1);
    }

    #[test]
    fn test_spread_operator() {
        let input = r#"
        QUERY spreadFields() =>
            user <- N<User>("123")
            result <- user::{
                newField: "value",
                ..
            }
            RETURN result
        "#;
        let input = write_to_temp_file(vec![input]);
        let result = HelixParser::parse_source(&input).unwrap();
        let query = &result.queries[0];
        assert_eq!(query.statements.len(), 2);
        assert_eq!(query.return_values.len(), 1);
    }

    #[test]
    fn test_complex_update_operations() {
        let input = r#"
        QUERY updateUser() =>
            user <- N<User>("123")
            updated <- user::UPDATE({
                name: "New Name",
                age: 30,
                lastUpdated: "2024-03-01",
                friendCount: _::Out<Follows>::COUNT
            })
            RETURN updated
        "#;
        let input = write_to_temp_file(vec![input]);
        let result = HelixParser::parse_source(&input).unwrap();
        let query = &result.queries[0];
        assert_eq!(query.statements.len(), 2);
        assert_eq!(query.return_values.len(), 1);
    }

    #[test]
    fn test_nested_traversals() {
        let input = r#"
        QUERY nestedTraversals() =>
            start <- N<User>("123")
            result <- start::Out<Follows>::InN<User>::Out<Likes>::InN<Post>::{title}
            filtered <- result::WHERE(_::{likes}::GT(10))
            RETURN filtered
        "#;
        let input = write_to_temp_file(vec![input]);
        let result = HelixParser::parse_source(&input).unwrap();
        let query = &result.queries[0];
        assert_eq!(query.statements.len(), 3);
        assert_eq!(query.return_values.len(), 1);
    }

    #[test]
    fn test_combined_operations() {
        let input = r#"
        QUERY combinedOps() =>
            // Test combination of different operations
            user <- N<User>("123")
            friends <- user::Out<Follows>::InN<User>
            active <- friends::WHERE(_::{active}::EQ(true))
            result <- active::{
                name,
                posts: _::Out<Created>::InN<Post>::!{deleted}::{
                    title: title,
                    likes: _::In<Likes>::COUNT
                }
            }
            RETURN result
        "#;
        let input = write_to_temp_file(vec![input]);
        let result = HelixParser::parse_source(&input).unwrap();
        let query = &result.queries[0];
        assert_eq!(query.statements.len(), 4);
        assert_eq!(query.return_values.len(), 1);
    }

    #[test]
    fn test_closure() {
        let input = r#"
        QUERY multipleLayers() =>
            result <- N<User>::|user|{
                posts: _::Out<Created>::{
                    user_id: user::ID
                }
            }
            RETURN result
        "#;
        let input = write_to_temp_file(vec![input]);
        let result = HelixParser::parse_source(&input).unwrap();
        // println!("\n\nresult: {:?}\n\n", result);
        let query = &result.queries[0];
        assert_eq!(query.statements.len(), 1);
        assert_eq!(query.return_values.len(), 1);
    }

    #[test]
    fn test_complex_return_traversal() {
        let input = r#"
        QUERY returnTraversal() =>
            RETURN N<User>::|user|{
                posts: _::Out<Created>::{
                    user_id: user::ID
                }
            }::!{createdAt, lastUpdated}::{username: name, ..}
        "#;
        let input = write_to_temp_file(vec![input]);
        let result = HelixParser::parse_source(&input).unwrap();
        let query = &result.queries[0];
        assert_eq!(query.return_values.len(), 1);
    }

    #[test]
    fn test_array_as_param_type() {
        let input = r#"
        QUERY trWithArrayParam(ids: [String], names:[String], ages: [I32], createdAt: String) => 
            AddN<User>({Name: "test"})
            RETURN "SUCCESS"
        "#;
        let input = write_to_temp_file(vec![input]);
        let result = HelixParser::parse_source(&input).unwrap();
        let query = &result.queries[0];
        assert_eq!(query.return_values.len(), 1);

        assert!(query
            .parameters
            .iter()
            .any(|param| match param.param_type.1 {
                FieldType::String => true,
                _ => false,
            }));
        assert!(query
            .parameters
            .iter()
            .any(|param| match param.param_type.1 {
                FieldType::Array(ref field) => match &**field {
                    FieldType::String =>
                        if param.name.1 == "names" || param.name.1 == "ids" {
                            true
                        } else {
                            false
                        },
                    _ => false,
                },
                _ => false,
            }));
        assert!(query
            .parameters
            .iter()
            .any(|param| match param.param_type.1 {
                FieldType::Array(ref field) => match &**field {
                    FieldType::I32 =>
                        if param.name.1 == "ages" {
                            true
                        } else {
                            false
                        },
                    _ => false,
                },
                _ => false,
            }))
    }

    #[test]
    fn test_schema_obj_as_param_type() {
        let input = r#"
        N::User {
            Name: String
        }

        QUERY trWithArrayParam(user: User) => 
            AddN<User>({Name: "test"})
            RETURN "SUCCESS"
        "#;
        let input = write_to_temp_file(vec![input]);
        let result = HelixParser::parse_source(&input).unwrap();
        let query = &result.queries[0];
        assert_eq!(query.return_values.len(), 1);

        // println!("{:?}", query.parameters);
        let mut param_type = "";
        assert!(
            query
                .parameters
                .iter()
                .any(|param| match param.param_type.1 {
                    FieldType::Identifier(ref id) => match id.as_str() {
                        "User" => true,
                        _ => {
                            param_type = id;
                            false
                        }
                    },
                    _ => false,
                }),
            "Param of type {} was not found",
            param_type
        );
    }

    #[test]
    fn test_add_vector() {
        let input = r#"
        V::User 

        QUERY addVector(vector: [F64]) =>
            RETURN AddV<User>(vector)
        "#;
        let input = write_to_temp_file(vec![input]);
        let result = HelixParser::parse_source(&input).unwrap();
        let query = &result.queries[0];
        assert_eq!(query.return_values.len(), 1);
    }

    #[test]
    fn test_bulk_insert() {
        let input = r#"
        QUERY bulkInsert(vectors: [[F64]]) =>
            BatchAddV<User>(vectors)
            RETURN "SUCCESS"
        "#;
        let input = write_to_temp_file(vec![input]);
        let result = HelixParser::parse_source(&input).unwrap();
        let query = &result.queries[0];
        assert_eq!(query.return_values.len(), 1);
    }

    #[test]
    fn test_search_vector() {
        let input = r#"
        V::User

        QUERY searchVector(vector: [F64], k: I32) =>
            RETURN SearchV<User>(vector, k)
        "#;
        let input = write_to_temp_file(vec![input]);
        let result = HelixParser::parse_source(&input).unwrap();
        let query = &result.queries[0];
        assert_eq!(query.return_values.len(), 1);
    }
}

pub fn write_to_temp_file(content: Vec<&str>) -> Content {
    let mut files = Vec::new();
    for c in content {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(c.as_bytes()).unwrap();
        let path = file.path().to_string_lossy().into_owned();
        files.push(HxFile {
            name: path,
            content: c.to_string(),
        });
    }
    Content {
        content: String::new(),
        files: files,
        source: Source::default(),
    }
}
