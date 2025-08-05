use core::fmt;
use std::fmt::Display;

use crate::helixc::generator::traversal_steps::Traversal;

use super::utils::GeneratedValue;

#[derive(Clone)]
pub enum BoolOp {
    Gt(Gt),
    Gte(Gte),
    Lt(Lt),
    Lte(Lte),
    Eq(Eq),
    Neq(Neq),
    Contains(Contains),
}
impl Display for BoolOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            BoolOp::Gt(gt) => format!("{gt}"),
            BoolOp::Gte(gte) => format!("{gte}"),
            BoolOp::Lt(lt) => format!("{lt}"),
            BoolOp::Lte(lte) => format!("{lte}"),
            BoolOp::Eq(eq) => format!("{eq}"),
            BoolOp::Neq(neq) => format!("{neq}"),
            BoolOp::Contains(_) => unimplemented!(),
        };
        write!(f, "map_value_or(false, |v| *v{s})?")
    }
}
#[derive(Clone)]
pub struct Gt {
    pub value: GeneratedValue,
}
impl Display for Gt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, " > {}", self.value)
    }
}

#[derive(Clone)]
pub struct Gte {
    pub value: GeneratedValue,
}
impl Display for Gte {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, " >= {}", self.value)
    }
}

#[derive(Clone)]
pub struct Lt {
    pub value: GeneratedValue,
}
impl Display for Lt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, " < {}", self.value)
    }
}

#[derive(Clone)]
pub struct Lte {
    pub value: GeneratedValue,
}
impl Display for Lte {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, " <= {}", self.value)
    }
}

#[derive(Clone)]
pub struct Eq {
    pub value: GeneratedValue,
}
impl Display for Eq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, " == {}", self.value)
    }
}

#[derive(Clone)]
pub struct Neq {
    pub value: GeneratedValue,
}
impl Display for Neq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, " != {}", self.value)
    }
}

#[derive(Clone)]
pub struct Contains {
    pub value: GeneratedValue,
}
impl Display for Contains {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, ".contains({})", self.value)
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
                    .map(|s| format!("{s}"))
                    .collect::<Vec<_>>();
                write!(f, "{}", tr.join(" && "))
            }
            BoExp::Or(traversals) => {
                let tr = traversals
                    .iter()
                    .map(|s| format!("{s}"))
                    .collect::<Vec<_>>();
                write!(f, "{}", tr.join(" || "))
            }
            BoExp::Exists(traversal) => write!(f, "Exist::exists(&mut {traversal})"),
            BoExp::Expr(traversal) => write!(f, "{traversal}"),
        }
    }
}
