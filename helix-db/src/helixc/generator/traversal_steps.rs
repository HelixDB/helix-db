use crate::helixc::{
    generator::{
        statements::Statement,
        utils::{VecData, write_properties},
    },
    parser::types::{MatchType, Optional, SchemaMatchType},
};

use super::{
    bool_ops::{BoExp, BoolOp},
    object_remappings::Remapping,
    source_steps::SourceStep,
    utils::{GenRef, GeneratedValue, Order, Separator},
};
use core::fmt;
use std::fmt::{Debug, Display};

#[derive(Clone)]
pub enum TraversalType {
    FromVar(GenRef<String>),
    Ref,
    Mut,
    Nested(GenRef<String>), // Should contain `.clone()` if necessary (probably is)
    NestedFrom(GenRef<String>),
    Empty,
    Update(Option<Vec<(String, GeneratedValue)>>),
}
impl Debug for TraversalType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TraversalType::FromVar(_) => write!(f, "FromVar"),
            TraversalType::Ref => write!(f, "Ref"),
            TraversalType::Nested(_) => write!(f, "Nested"),
            _ => write!(f, "other"),
        }
    }
}
// impl Display for TraversalType {
//     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
//         match self {
//             TraversalType::FromVar => write!(f, ""),
//             TraversalType::Ref => write!(f, "G::new(Arc::clone(&db), &txn)"),

//             TraversalType::Mut => write!(f, "G::new_mut(Arc::clone(&db), &mut txn)"),
//             TraversalType::Nested(nested) => {
//                 assert!(nested.inner().len() > 0, "Empty nested traversal name");
//                 write!(f, "G::new_from(Arc::clone(&db), &txn, {})", nested)
//             }
//             TraversalType::Update => write!(f, ""),
//             // TraversalType::FromVar(var) => write!(f, "G::new_from(Arc::clone(&db), &txn, {})", var),
//             TraversalType::Empty => panic!("Should not be empty"),
//         }
//     }
// }
#[derive(Clone, Debug)]
pub enum ShouldCollect {
    ToVec,
    ToVal,
    No,
    Try,
}
impl Display for ShouldCollect {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ShouldCollect::ToVec => write!(f, ".collect_to::<Vec<_>>()"),
            ShouldCollect::ToVal => write!(f, ".collect_to_obj()"),
            ShouldCollect::Try => write!(f, "?"),
            ShouldCollect::No => write!(f, ""),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Traversal {
    pub traversal_type: TraversalType,
    pub source_step: Separator<SourceStep>,
    pub steps: Vec<Separator<Step>>,
    pub should_collect: ShouldCollect,
}

impl Display for Traversal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.traversal_type {
            TraversalType::FromVar(var) => {
                write!(f, "G::new_from(Arc::clone(&db), &txn, {var}.clone())")?;
                write!(f, "{}", self.source_step)?;
                for step in &self.steps {
                    write!(f, "\n{step}")?;
                }
            }
            TraversalType::Ref => {
                write!(f, "G::new(Arc::clone(&db), &txn)")?;
                write!(f, "{}", self.source_step)?;
                for step in &self.steps {
                    write!(f, "\n{step}")?;
                }
            }

            TraversalType::Mut => {
                write!(f, "G::new_mut(Arc::clone(&db), &mut txn)")?;
                write!(f, "{}", self.source_step)?;
                for step in &self.steps {
                    write!(f, "\n{step}")?;
                }
            }
            TraversalType::Nested(nested) => {
                assert!(!nested.inner().is_empty(), "Empty nested traversal name");
                write!(f, "{nested}")?; // this should be var name default val
                for step in &self.steps {
                    write!(f, "\n{step}")?;
                }
            }
            TraversalType::NestedFrom(nested) => {
                assert!(!nested.inner().is_empty(), "Empty nested traversal name");
                write!(
                    f,
                    "G::new_from(Arc::clone(&db), &txn, vec![{nested}.clone()])"
                )?;
                for step in &self.steps {
                    write!(f, "\n{step}")?;
                }
            }
            TraversalType::Empty => panic!("Should not be empty"),
            TraversalType::Update(properties) => {
                write!(f, "{{")?;
                write!(f, "let update_tr = G::new(Arc::clone(&db), &txn)")?;
                write!(f, "{}", self.source_step)?;
                for step in &self.steps {
                    write!(f, "\n{step}")?;
                }
                write!(f, "\n    .collect_to::<Vec<_>>();")?;
                write!(f, "G::new_mut_from(Arc::clone(&db), &mut txn, update_tr)",)?;
                write!(f, "\n    .update({})", write_properties(properties))?;
                write!(f, "\n    .collect_to_obj()")?;
                write!(f, "}}")?;
            }
        }
        write!(f, "{}", self.should_collect)
    }
}
impl Default for Traversal {
    fn default() -> Self {
        Self {
            traversal_type: TraversalType::Ref,
            source_step: Separator::Empty(SourceStep::Empty),
            steps: vec![],
            should_collect: ShouldCollect::ToVec,
        }
    }
}
#[derive(Clone)]
pub enum Step {
    // graph steps
    Out(Out),
    In(In),
    OutE(OutE),
    InE(InE),
    FromN,
    ToN,
    FromV,
    ToV,

    // utils
    Count,

    Where(Where),
    Range(Range),
    OrderBy(OrderBy),
    Dedup,

    // bool ops
    BoolOp(BoolOp),

    // property
    PropertyFetch(GenRef<String>),

    // object
    Remapping(Remapping),

    // closure
    // Closure(ClosureRemapping),

    // shortest path
    ShortestPath(ShortestPath),

    // search vector
    SearchVector(SearchVectorStep),

    GroupBy(GroupBy),

    AggregateBy(AggregateBy),

    Match(GeneratedMatch),
}
impl Display for Step {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Step::Count => write!(f, "count_to_val()"),
            Step::Dedup => write!(f, "dedup()"),
            Step::FromN => write!(f, "from_n()"),
            Step::FromV => write!(f, "from_v()"),
            Step::ToN => write!(f, "to_n()"),
            Step::ToV => write!(f, "to_v()"),
            Step::PropertyFetch(property) => write!(f, "check_property({property})"),

            Step::Out(out) => write!(f, "{out}"),
            Step::In(in_) => write!(f, "{in_}"),
            Step::OutE(out_e) => write!(f, "{out_e}"),
            Step::InE(in_e) => write!(f, "{in_e}"),
            Step::Where(where_) => write!(f, "{where_}"),
            Step::Range(range) => write!(f, "{range}"),
            Step::OrderBy(order_by) => write!(f, "{order_by}"),
            Step::BoolOp(bool_op) => write!(f, "{bool_op}"),
            Step::Remapping(remapping) => write!(f, "{remapping}"),
            Step::ShortestPath(shortest_path) => write!(f, "{shortest_path}"),
            Step::SearchVector(search_vector) => write!(f, "{search_vector}"),
            Step::GroupBy(group_by) => write!(f, "{group_by}"),
            Step::AggregateBy(aggregate_by) => write!(f, "{aggregate_by}"),
            Step::Match(match_) => write!(f, "{match_}"),
        }
    }
}
impl Debug for Step {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Step::Count => write!(f, "Count"),
            Step::Dedup => write!(f, "Dedup"),
            Step::FromN => write!(f, "FromN"),
            Step::ToN => write!(f, "ToN"),
            Step::PropertyFetch(property) => write!(f, "check_property({property})"),
            Step::FromV => write!(f, "FromV"),
            Step::ToV => write!(f, "ToV"),
            Step::Out(_) => write!(f, "Out"),
            Step::In(_) => write!(f, "In"),
            Step::OutE(_) => write!(f, "OutE"),
            Step::InE(_) => write!(f, "InE"),
            Step::Where(_) => write!(f, "Where"),
            Step::Range(_) => write!(f, "Range"),
            Step::OrderBy(_) => write!(f, "OrderBy"),
            Step::BoolOp(_) => write!(f, "Bool"),
            Step::Remapping(_) => write!(f, "Remapping"),
            Step::ShortestPath(_) => write!(f, "ShortestPath"),
            Step::SearchVector(_) => write!(f, "SearchVector"),
            Step::GroupBy(_) => write!(f, "GroupBy"),
            Step::AggregateBy(_) => write!(f, "AggregateBy"),
            Step::Match(_) => write!(f, "Match"),
        }
    }
}

#[derive(Clone)]
pub struct Out {
    pub label: GenRef<String>,
    pub edge_type: GenRef<String>,
}
impl Display for Out {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "out({},{})", self.label, self.edge_type)
    }
}

#[derive(Clone)]
pub struct In {
    pub label: GenRef<String>,
    pub edge_type: GenRef<String>,
}
impl Display for In {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "in_({},{})", self.label, self.edge_type)
    }
}

#[derive(Clone)]
pub struct OutE {
    pub label: GenRef<String>,
}
impl Display for OutE {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "out_e({})", self.label)
    }
}

#[derive(Clone)]
pub struct InE {
    pub label: GenRef<String>,
}
impl Display for InE {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "in_e({})", self.label)
    }
}

#[derive(Clone)]
pub enum Where {
    Ref(WhereRef),
}
impl Display for Where {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Where::Ref(wr) = self;
        write!(f, "{wr}")
    }
}

#[derive(Clone)]
pub struct WhereRef {
    pub expr: BoExp,
}
impl Display for WhereRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "filter_ref(|val, txn|{{
                if let Ok(val) = val {{ 
                    Ok({})
                }} else {{
                    Ok(false)
                }}
            }})",
            self.expr
        )
    }
}

#[derive(Clone)]
pub struct Range {
    pub start: GeneratedValue,
    pub end: GeneratedValue,
}
impl Display for Range {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "range({}, {})", self.start, self.end)
    }
}

#[derive(Clone)]
pub struct OrderBy {
    pub property: GenRef<String>,
    pub order: Order,
}
impl Display for OrderBy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.order {
            Order::Asc => write!(f, "order_by_asc({})", self.property),
            Order::Desc => write!(f, "order_by_desc({})", self.property),
        }
    }
}

#[derive(Clone)]
pub struct GroupBy {
    pub should_count: bool,
    pub properties: Vec<GenRef<String>>,
}
impl Display for GroupBy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "group_by(&[{}], {})",
            self.properties
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<_>>()
                .join(","),
            self.should_count
        )
    }
}

#[derive(Clone)]
pub struct AggregateBy {
    pub should_count: bool,
    pub properties: Vec<GenRef<String>>,
}
impl Display for AggregateBy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "aggregate_by(&[{}], {})",
            self.properties
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<_>>()
                .join(","),
            self.should_count
        )
    }
}

#[derive(Clone)]
pub struct ShortestPath {
    pub label: Option<GenRef<String>>,
    pub from: Option<GenRef<String>>,
    pub to: Option<GenRef<String>>,
}
impl Display for ShortestPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "shortest_path({}, {}, {})",
            self.label
                .clone()
                .map_or("None".to_string(), |label| format!("Some({label})")),
            self.from
                .clone()
                .map_or("None".to_string(), |from| format!("Some(&{from})")),
            self.to
                .clone()
                .map_or("None".to_string(), |to| format!("Some(&{to})"))
        )
    }
}

#[derive(Clone)]
pub struct SearchVectorStep {
    pub vec: VecData,
    pub k: GeneratedValue,
}
impl Display for SearchVectorStep {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "brute_force_search_v({}, {})", self.vec, self.k)
    }
}

#[derive(Clone, Debug)]
pub struct GeneratedMatch {
    pub variable: Option<GeneratedMatchVariable>,
    pub statements: Vec<GeneratedMatchStatement>,
    pub default: GeneratedMatchDefault,
}
impl Display for GeneratedMatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "match_(|val, txn| {{ match {} {{",
            self.variable
                .as_ref()
                .map_or("".to_string(), |v| format!("{v}"))
        )?;
        for statement in &self.statements {
            println!("GeneratedMatchStatement: {:?}", statement);
            writeln!(f, "    {statement}")?;
        }
        writeln!(f, "    _ => {},", self.default)?;
        writeln!(f, "\t}}")?;
        writeln!(f, "}})")
    }
}

#[derive(Clone, Debug)]
pub enum GeneratedMatchDefault {
    Empty,
    Optional,
    TraversalValue,
}

impl Display for GeneratedMatchDefault {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GeneratedMatchDefault::Empty => write!(f, "{{}}"),
            GeneratedMatchDefault::Optional => write!(f, "None"),
            GeneratedMatchDefault::TraversalValue => write!(f, "TraversalValue::Empty"),
        }
    }
}

#[derive(Clone, Debug)]
pub enum GeneratedMatchVariable {
    Identifier(String),
    Traversal(Box<Traversal>),
    Anonymous,
}

impl Display for GeneratedMatchVariable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GeneratedMatchVariable::Identifier(identifier) => write!(f, "{identifier}"),
            GeneratedMatchVariable::Traversal(traversal) => write!(f, "{traversal}"),
            GeneratedMatchVariable::Anonymous => write!(f, "_"),
        }
    }
}

#[derive(Clone, Debug)]
pub enum GeneratedMatchType {
    Optional(GeneratedOptional),
    Identifier(String),
    Boolean(bool),
    SchemaType(GeneratedSchemaMatchType),
}

impl Display for GeneratedMatchType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GeneratedMatchType::Optional(optional) => write!(f, "{optional}"),
            GeneratedMatchType::Identifier(identifier) => write!(f, "{identifier}"),
            GeneratedMatchType::Boolean(boolean) => {
                if *boolean {
                    write!(f, "TraversalValue::Value(Value::Boolean(b)) if b")
                } else {
                    write!(f, "TraversalValue::Value(Value::Boolean(b)) if !b")
                }
            }
            GeneratedMatchType::SchemaType(schema_type) => write!(f, "{schema_type}"),
        }
    }
}

impl From<MatchType> for GeneratedMatchType {
    fn from(match_type: MatchType) -> Self {
        match match_type {
            MatchType::Optional(optional) => GeneratedMatchType::Optional(optional.into()),
            MatchType::Identifier(identifier) => GeneratedMatchType::Identifier(identifier.clone()),
            MatchType::Boolean(boolean) => GeneratedMatchType::Boolean(boolean),
            MatchType::SchemaType(schema_type) => {
                GeneratedMatchType::SchemaType(schema_type.into())
            }
        }
    }
}

#[derive(Clone, Debug)]
pub enum GeneratedOptional {
    Some(String),
    None,
}

impl From<Optional> for GeneratedOptional {
    fn from(optional: Optional) -> Self {
        match optional {
            Optional::Some(some) => GeneratedOptional::Some(some),
            Optional::None => GeneratedOptional::None,
        }
    }
}

#[derive(Clone, Debug)]
pub enum GeneratedSchemaMatchType {
    Node(String),
    Edge(String),
    Vector(String),
}

impl Display for GeneratedOptional {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GeneratedOptional::Some(identifier) => write!(f, "Some({identifier})"),
            GeneratedOptional::None => write!(f, "None"),
        }
    }
}

impl From<SchemaMatchType> for GeneratedSchemaMatchType {
    fn from(schema_type: SchemaMatchType) -> Self {
        match schema_type {
            SchemaMatchType::Node(node) => GeneratedSchemaMatchType::Node(node.clone()),
            SchemaMatchType::Edge(edge) => GeneratedSchemaMatchType::Edge(edge.clone()),
            SchemaMatchType::Vector(vector) => GeneratedSchemaMatchType::Vector(vector.clone()),
        }
    }
}

impl Display for GeneratedSchemaMatchType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GeneratedSchemaMatchType::Node(identifier) => write!(
                f,
                "TraversalValue::Node(node) if node.label() == \"{identifier}\""
            ),
            GeneratedSchemaMatchType::Edge(identifier) => write!(
                f,
                "TraversalValue::Edge(edge) if edge.label() == \"{identifier}\""
            ),
            GeneratedSchemaMatchType::Vector(identifier) => write!(
                f,
                "TraversalValue::Vector(vector) if vector.label() == \"{identifier}\""
            ),
        }
    }
}

#[derive(Clone, Debug)]
pub struct GeneratedMatchStatement {
    pub match_type: GeneratedMatchType,
    pub match_value: GeneratedMatchValueType,
}
impl Display for GeneratedMatchStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} => {{ {} }}", self.match_type, self.match_value)
    }
}

#[derive(Clone, Debug)]
pub enum GeneratedMatchValueType {
    Expression(Statement),
    Statements(Vec<Statement>),
    Anonymous,
    None,
}

impl Display for GeneratedMatchValueType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GeneratedMatchValueType::Expression(expression) => write!(f, "{expression}"),
            GeneratedMatchValueType::Statements(statements) => {
                for statement in statements {
                    writeln!(f, "    {statement};")?;
                }
                Ok(())
            }
            GeneratedMatchValueType::Anonymous => write!(f, "Some(val)"),
            GeneratedMatchValueType::None => write!(f, "None"),
        }
    }
}
