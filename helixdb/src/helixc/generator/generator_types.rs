use core::fmt;
use std::{
    collections::HashMap,
    fmt::Display,
    io::{self, Write},
};

use crate::{helixc::parser::helix_parser::FieldPrefix, protocol::value::Value};

use super::{
    traversal_steps::{ShouldCollect, Traversal},
    tsdisplay::ToTypeScript,
    utils::{write_headers, write_properties, GenRef, GeneratedType, GeneratedValue},
};

pub struct Source {
    pub nodes: Vec<NodeSchema>,
    pub edges: Vec<EdgeSchema>,
    pub vectors: Vec<VectorSchema>,
    pub queries: Vec<Query>,
    pub src: String,
}
impl Default for Source {
    fn default() -> Self {
        Self {
            nodes: vec![],
            edges: vec![],
            vectors: vec![],
            queries: vec![],
            src: "".to_string(),
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

#[derive(Clone)]
pub struct NodeSchema {
    pub name: String,
    pub properties: Vec<SchemaProperty>,
}
impl Display for NodeSchema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "pub struct {} {{\n", self.name)?;
        for property in &self.properties {
            write!(f, "    pub {}: {},\n", property.name, property.field_type)?;
        }
        write!(f, "}}\n")
    }
}
impl ToTypeScript for NodeSchema {
    fn to_typescript(&self) -> String {
        let mut result = format!("interface {} {{\n", self.name);
        result.push_str("  id: string;\n");

        for property in &self.properties {
            result.push_str(&format!(
                "  {}: {};\n",
                property.name,
                match &property.field_type {
                    GeneratedType::RustType(t) => t.to_ts(),
                    _ => unreachable!(),
                }
            ));
        }

        result.push_str("}\n");
        result
    }
}

#[derive(Clone)]
pub struct EdgeSchema {
    pub name: String,
    pub from: String,
    pub to: String,
    pub properties: Vec<SchemaProperty>,
}
impl Display for EdgeSchema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "pub struct {} {{\n", self.name)?;
        write!(f, "    pub from: {},\n", self.from)?;
        write!(f, "    pub to: {},\n", self.to)?;
        for property in &self.properties {
            write!(f, "    pub {}: {},\n", property.name, property.field_type)?;
        }
        write!(f, "}}\n")
    }
}
impl ToTypeScript for VectorSchema {
    fn to_typescript(&self) -> String {
        let mut result = format!("interface {} {{\n", self.name);
        result.push_str("  id: string;\n");
        result.push_str("  data: Array<number>;\n");

        for property in &self.properties {
            result.push_str(&format!(
                "  {}: {};\n",
                property.name,
                match &property.field_type {
                    GeneratedType::RustType(t) => t.to_ts(),
                    _ => unreachable!(),
                }
            ));
        }

        result.push_str("}\n");
        result
    }
}
#[derive(Clone)]
pub struct VectorSchema {
    pub name: String,
    pub properties: Vec<SchemaProperty>,
}
impl Display for VectorSchema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "pub struct {} {{\n", self.name)?;
        for property in &self.properties {
            write!(f, "    pub {}: {},\n", property.name, property.field_type)?;
        }
        write!(f, "}}\n")
    }
}
impl ToTypeScript for EdgeSchema {
    fn to_typescript(&self) -> String {
        let properties_str = self
            .properties
            .iter()
            .map(|p| {
                format!(
                    "    {}: {}",
                    p.name,
                    match &p.field_type {
                        GeneratedType::RustType(t) => t.to_ts(),
                        _ => unreachable!(),
                    }
                )
            })
            .collect::<Vec<_>>()
            .join(";");

        format!(
            "interface {} {{\n  id: string;\n  from: {};\n  to: {};\n  properties: {{\n\t{}\n}};\n}}\n",
            self.name, self.from, self.to, properties_str
        )
    }
}

#[derive(Clone)]
pub struct SchemaProperty {
    pub name: String,
    pub field_type: GeneratedType,
    pub default_value: Option<GeneratedValue>,
    // pub is_optional: bool,
    pub is_index: FieldPrefix,
}

pub struct Query {
    pub name: String,
    pub statements: Vec<Statement>,
    pub parameters: Vec<Parameter>, // iterate through and print each one
    pub sub_parameters: Vec<(String, Vec<Parameter>)>,
    pub return_values: Vec<ReturnValue>,
    pub is_mut: bool,
}
impl Display for Query {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // prints sub parameter structs (e.g. (docs: {doc: String, id: String}))
        for (name, parameters) in &self.sub_parameters {
            writeln!(f, "#[derive(Serialize, Deserialize)]")?;
            write!(f, "pub struct {} {{\n", name)?;
            for parameter in parameters {
                write!(f, "    pub {}: {},\n", parameter.name, parameter.field_type)?;
            }
            write!(f, "}}\n")?;
        }
        // prints top level parameters (e.g. (docs: {doc: String, id: String}))
        if !self.parameters.is_empty() {
            writeln!(f, "#[derive(Serialize, Deserialize)]")?;
            writeln!(f, "pub struct {}Input {{\n", self.name)?;
            write!(
                f,
                "{}",
                self.parameters
                    .iter()
                    .map(|p| format!("{}", p))
                    .collect::<Vec<_>>()
                    .join(",\n")
            )?;
            write!(f, "\n}}\n")?;
        }

        write!(f, "#[handler]\n")?; // Handler macro

        // prints the function signature
        write!(f, "pub fn {} (input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {{\n", self.name)?;

        // prints basic query items
        if !self.parameters.is_empty() {
            write!(
                f,
                "let data: {}Input = match sonic_rs::from_slice(&input.request.body) {{\n",
                self.name
            )?;
            writeln!(f, "    Ok(data) => data,")?;
            writeln!(f, "    Err(err) => return Err(GraphError::from(err)),")?;
            writeln!(f, "}};\n")?;
        }
        writeln!(
            f,
            "let mut remapping_vals: RefCell<HashMap<u128, ResponseRemapping>> = RefCell::new(HashMap::new());"
        )?;

        writeln!(f, "let db = Arc::clone(&input.graph.storage);")?;
        // if mut then get write txn
        // if not then get read txn
        if self.is_mut {
            writeln!(f, "let mut txn = db.graph_env.write_txn().unwrap();")?;
        } else {
            writeln!(f, "let txn = db.graph_env.read_txn().unwrap();")?;
        }

        // prints each statement
        for statement in &self.statements {
            write!(f, "    {};\n", statement)?;
        }

        writeln!(
            f,
            "let mut return_vals: HashMap<String, ReturnValue> = HashMap::new();"
        )?;
        if !self.return_values.is_empty() {
            for return_value in &self.return_values {
                write!(f, "    {}\n", return_value)?;
            }
        }

        // commit the transaction
        // if self.is_mut {
            writeln!(f, "    txn.commit().unwrap();")?;
        // }/
        // closes the handler function
        write!(
            f,
            "    response.body = sonic_rs::to_vec(&return_vals).unwrap();\n"
        )?;
        write!(f, "    Ok(())\n")?;
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
            return_values: vec![],
            is_mut: false,
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

#[derive(Clone)]
pub enum Statement {
    Assignment(Assignment),
    Drop(Drop),
    Traversal(Traversal),
    ForEach(ForEach),
    Literal(GenRef<String>),
    Identifier(GenRef<String>),
    BoExp(BoExp),
    Empty,
}
impl Display for Statement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Statement::Assignment(assignment) => write!(f, "{}", assignment),
            Statement::Drop(drop) => write!(f, "{}", drop),
            Statement::Traversal(traversal) => write!(f, "{}", traversal),
            Statement::ForEach(foreach) => write!(f, "{}", foreach),
            Statement::Literal(literal) => write!(f, "{}", literal),
            Statement::Identifier(identifier) => write!(f, "{}", identifier),
            Statement::BoExp(bo) => write!(f, "{}", bo),
            Statement::Empty => write!(f, ""),
        }
    }
}

#[derive(Clone)]
pub enum IdentifierType {
    Primitive,
    Traversal,
    Empty,
}

#[derive(Clone)]
pub struct Assignment {
    pub variable: GenRef<String>,
    pub value: Box<Statement>,
}
impl Display for Assignment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "let {} = {}", self.variable, *self.value)
    }
}

#[derive(Clone)]
pub struct ForEach {
    pub for_variables: ForVariable,
    pub in_variable: ForLoopInVariable,
    pub statements: Vec<Statement>,
}
impl Display for ForEach {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.for_variables {
            ForVariable::ObjectDestructure(variables) => {
                write!(
                    f,
                    "for data in data.{}",
                    // self.in_variable,
                    // variables
                    //     .iter()
                    //     .map(|v| format!("{}", v))
                    //     .collect::<Vec<_>>()
                    //     .join(", "),
                    self.in_variable
                )?;
            }
            ForVariable::Identifier(variable) => {
                write!(f, "for data in {}", self.in_variable)?;
            }
            ForVariable::Empty => {
                assert!(false, "For variable is empty");
            }
        }
        write!(f, " {{\n")?;
        for statement in &self.statements {
            write!(f, "    {};\n", statement)?;
        }
        write!(f, "}}\n")
    }
}

#[derive(Clone)]
pub enum ForVariable {
    ObjectDestructure(Vec<GenRef<String>>),
    Identifier(GenRef<String>),
    Empty,
}
#[derive(Debug, Clone)]
pub enum ForLoopInVariable {
    Identifier(GenRef<String>),
    Parameter(GenRef<String>),
    Empty,
}
impl Display for ForLoopInVariable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ForLoopInVariable::Identifier(identifier) => write!(f, "{}", identifier),
            ForLoopInVariable::Parameter(parameter) => write!(f, "{}", parameter),
            ForLoopInVariable::Empty => {
                assert!(false, "For loop in variable is empty");
                write!(f, "_")
            }
        }
    }
}
#[derive(Clone)]
pub struct Drop {
    pub expression: Traversal,
}
impl Display for Drop {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Drop::<Vec<_>>::drop_traversal(
                {}.collect::<Vec<_>>(),
                Arc::clone(&db),
                &mut txn,
            )?;",
            self.expression
        )
    }
}

/// Boolean expression is used for a traversal or set of traversals wrapped in AND/OR
/// that resolve to a boolean value
#[derive(Clone)]
pub enum BoExp {
    And(Vec<BoExp>),
    Or(Vec<BoExp>),
    Exists(Traversal),
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
            BoExp::Exists(traversal) => write!(f, "{}", traversal),
            BoExp::Expr(traversal) => write!(f, "{}", traversal),
        }
    }
}

pub struct ReturnValue {
    pub value: ReturnValueExpr,
    pub return_type: ReturnType,
}
impl Display for ReturnValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.return_type {
            ReturnType::Literal(name) => {
                write!(
                    f,
                    "    return_vals.insert({}.to_string(), ReturnValue::from(Value::from({})));\n",
                    name, self.value
                )
            }
            ReturnType::NamedLiteral(name) => {
                write!(
                    f,
                    "    return_vals.insert(\"{}\".to_string(), ReturnValue::from(Value::from({})));\n",
                    name, self.value
                )
            }
            ReturnType::NamedExpr(name) => {
                write!(f, "    return_vals.insert({}.to_string(), ReturnValue::from_traversal_value_array_with_mixin({}.clone(), remapping_vals.borrow_mut()));\n", String::from(name.clone()), self.value)
            }
            ReturnType::UnnamedExpr => {
                write!(f, "// need to implement unnamed return value\n todo!()")
            }
        }
    }
}

impl ReturnValue {
    pub fn new_literal(name: GenRef<String>, value: GenRef<String>) -> Self {
        Self {
            value: ReturnValueExpr::Value(value.clone()),
            return_type: ReturnType::Literal(name),
        }
    }
    pub fn new_named_literal(name: GenRef<String>, value: GenRef<String>) -> Self {
        Self {
            value: ReturnValueExpr::Value(value.clone()),
            return_type: ReturnType::NamedLiteral(name),
        }
    }
    pub fn new_named(name: GenRef<String>, value: ReturnValueExpr) -> Self {
        Self {
            value,
            return_type: ReturnType::NamedExpr(name),
        }
    }
    pub fn new_unnamed(value: ReturnValueExpr) -> Self {
        Self {
            value,
            return_type: ReturnType::UnnamedExpr,
        }
    }
}

#[derive(Clone)]
pub enum ReturnType {
    Literal(GenRef<String>),
    NamedLiteral(GenRef<String>),
    NamedExpr(GenRef<String>),
    UnnamedExpr,
}
#[derive(Clone)]
pub enum ReturnValueExpr {
    Traversal(Traversal),
    Identifier(GenRef<String>),
    Value(GenRef<String>),
}
impl Display for ReturnValueExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReturnValueExpr::Traversal(traversal) => write!(f, "{}", traversal),
            ReturnValueExpr::Identifier(identifier) => write!(f, "{}", identifier),
            ReturnValueExpr::Value(value) => write!(f, "{}", value),
        }
    }
}
