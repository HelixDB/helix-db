use core::fmt;
use std::fmt::Display;

use crate::helixc::generator::{bool_ops::BoExp, traversal_steps::Traversal, utils::GenRef};

#[derive(Clone)]
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
            Statement::Array(array) => write!(
                f,
                "[{}]",
                array
                    .iter()
                    .map(|s| s.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
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
                // Use struct_name if available (for parameter-based loops), otherwise fall back to inner()Data
                let struct_name = self
                    .in_variable
                    .struct_name()
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| format!("{}Data", self.in_variable.inner()));
                write!(
                    f,
                    "for {} {{ {} }} in {}",
                    struct_name,
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
                debug_assert!(false, "ForVariable::Empty should be caught by analyzer");
                write!(f, "/* ERROR: empty for variable */")?;
            }
        }
        writeln!(f, " {{")?;
        for statement in &self.statements {
            writeln!(f, "    {statement};")?;
        }
        writeln!(f, "}}")
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
    Identifier(GenRef<String>, Option<String>), // (identifier_name, optional_struct_name)
    Parameter(GenRef<String>, String),          // (param_name, struct_name)
    Empty,
}
impl ForLoopInVariable {
    pub fn inner(&self) -> String {
        match self {
            ForLoopInVariable::Identifier(identifier, _) => identifier.to_string(),
            ForLoopInVariable::Parameter(parameter, _) => parameter.to_string(),
            ForLoopInVariable::Empty => "".to_string(),
        }
    }

    pub fn struct_name(&self) -> Option<&str> {
        match self {
            ForLoopInVariable::Parameter(_, struct_name) => Some(struct_name.as_str()),
            ForLoopInVariable::Identifier(_, Some(struct_name)) => Some(struct_name.as_str()),
            _ => None,
        }
    }
}
impl Display for ForLoopInVariable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ForLoopInVariable::Identifier(identifier, _) => write!(f, "{identifier}"),
            ForLoopInVariable::Parameter(parameter, _) => write!(f, "&data.{parameter}"),
            ForLoopInVariable::Empty => {
                debug_assert!(
                    false,
                    "ForLoopInVariable::Empty should be caught by analyzer"
                );
                write!(f, "/* ERROR: empty for loop in variable */")
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
            "Drop::drop_traversal(
                {}.collect::<Vec<_>>().into_iter(),
                &db,
                &mut txn,
            )?;",
            self.expression
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ============================================================================
    // Statement Tests
    // ============================================================================

    #[test]
    fn test_statement_literal() {
        let stmt = Statement::Literal(GenRef::Literal("test".to_string()));
        assert_eq!(format!("{}", stmt), "\"test\"");
    }

    #[test]
    fn test_statement_identifier() {
        let stmt = Statement::Identifier(GenRef::Std("variable".to_string()));
        assert_eq!(format!("{}", stmt), "variable");
    }

    #[test]
    fn test_statement_empty() {
        let stmt = Statement::Empty;
        assert_eq!(format!("{}", stmt), "");
    }

    #[test]
    fn test_statement_array() {
        let stmt = Statement::Array(vec![
            Statement::Literal(GenRef::Literal("a".to_string())),
            Statement::Literal(GenRef::Literal("b".to_string())),
        ]);
        assert_eq!(format!("{}", stmt), "[\"a\", \"b\"]");
    }

    #[test]
    fn test_statement_empty_array() {
        let stmt = Statement::Array(vec![]);
        assert_eq!(format!("{}", stmt), "[]");
    }

    // ============================================================================
    // Assignment Tests
    // ============================================================================

    #[test]
    fn test_assignment_simple() {
        let assignment = Assignment {
            variable: GenRef::Std("x".to_string()),
            value: Box::new(Statement::Literal(GenRef::Literal("value".to_string()))),
        };
        assert_eq!(format!("{}", assignment), "let x = \"value\"");
    }

    #[test]
    fn test_assignment_statement() {
        let assignment = Statement::Assignment(Assignment {
            variable: GenRef::Std("result".to_string()),
            value: Box::new(Statement::Identifier(GenRef::Std(
                "computation".to_string(),
            ))),
        });
        let output = format!("{}", assignment);
        assert!(output.contains("let result = computation"));
    }

    // ============================================================================
    // ForLoopInVariable Tests
    // ============================================================================

    #[test]
    fn test_for_loop_in_variable_identifier() {
        let var = ForLoopInVariable::Identifier(GenRef::Std("items".to_string()), None);
        assert_eq!(format!("{}", var), "items");
        assert_eq!(var.inner(), "items");
    }

    #[test]
    fn test_for_loop_in_variable_identifier_with_struct_name() {
        let var = ForLoopInVariable::Identifier(
            GenRef::Std("subchapters".to_string()),
            Some("loaddocs_ragSubchaptersData".to_string()),
        );
        assert_eq!(format!("{}", var), "subchapters");
        assert_eq!(var.inner(), "subchapters");
        assert_eq!(var.struct_name(), Some("loaddocs_ragSubchaptersData"));
    }

    #[test]
    fn test_for_loop_in_variable_parameter() {
        let var = ForLoopInVariable::Parameter(
            GenRef::Std("param_name".to_string()),
            "TestStruct".to_string(),
        );
        assert_eq!(format!("{}", var), "&data.param_name");
        assert_eq!(var.inner(), "param_name");
    }

    #[test]
    fn test_for_loop_in_variable_empty_inner() {
        let var = ForLoopInVariable::Empty;
        assert_eq!(var.inner(), "");
    }
}
