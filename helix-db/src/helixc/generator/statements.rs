use core::fmt;
use std::fmt::Display;

use crate::helixc::generator::{bool_ops::BoExp, traversal_steps::Traversal, utils::GenRef};



#[derive(Clone, Debug)]
pub enum Statement {
    Assignment(Assignment),
    Drop(Drop),
    Traversal(Traversal),
    ForEach(ForEach),
    Literal(GenRef<String>),
    Identifier(GenRef<String>),
    BoExp(BoExp),
    Array(Vec<Statement>),
    Empty,
}
impl Display for Statement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Statement::Assignment(assignment) => write!(f, "{assignment}"),
            Statement::Drop(drop) => write!(f, "{drop}"),
            Statement::Traversal(traversal) => write!(f, "{traversal}"),
            Statement::ForEach(foreach) => write!(f, "{foreach}"),
            Statement::Literal(literal) => write!(f, "{literal}"),
            Statement::Identifier(identifier) => write!(f, "{identifier}"),
            Statement::BoExp(bo) => write!(f, "{bo}"),
            Statement::Array(array) => write!(f, "[{}]", array.iter().map(|s| s.to_string()).collect::<Vec<_>>().join(", ")),
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

#[derive(Clone, Debug)]
pub struct Assignment {
    pub variable: GenRef<String>,
    pub value: Box<Statement>,
}
impl Display for Assignment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "let {} = {}", self.variable, *self.value)
    }
}

#[derive(Clone, Debug)]
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
                    "for {}Data {{ {} }} in {}",
                    self.in_variable.inner(),
                    variables
                        .iter()
                        .map(|v| format!("{v}"))
                        .collect::<Vec<_>>()
                        .join(", "),
                    self.in_variable
                )?;
            }
            ForVariable::Identifier(identifier) => {
                write!(f, "for {} in {}", identifier, self.in_variable)?;
            }
            ForVariable::Empty => {
                panic!("For variable is empty");
            }
        }
        writeln!(f, " {{")?;
        for statement in &self.statements {
            writeln!(f, "    {statement};")?;
        }
        writeln!(f, "}}")
    }
}

#[derive(Clone, Debug)]
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
impl ForLoopInVariable {
    pub fn inner(&self) -> String {
        match self {
            ForLoopInVariable::Identifier(identifier) => identifier.to_string(),
            ForLoopInVariable::Parameter(parameter) => parameter.to_string(),
            ForLoopInVariable::Empty => "".to_string(),
        }
    }
}
impl Display for ForLoopInVariable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ForLoopInVariable::Identifier(identifier) => write!(f, "{identifier}"),
            ForLoopInVariable::Parameter(parameter) => write!(f, "&data.{parameter}"),
            ForLoopInVariable::Empty => {
                panic!("For loop in variable is empty");
            }
        }
    }
}
#[derive(Clone, Debug)]
pub struct Drop {
    pub expression: Traversal,
}
impl Display for Drop {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Drop::<Vec<_>>::drop_traversal(
                {},
                Arc::clone(&db),
                &mut txn,
            )?;",
            self.expression
        )
    }
}

