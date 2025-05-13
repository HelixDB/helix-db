use core::fmt;
use std::fmt::Display;

use super::generator_types::GeneratedValue;

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
        write!(f, ".map_or(false, |v| *v{}))", self)
    }
}
pub struct Gt {
    pub value: GeneratedValue,
}
impl Display for Gt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, " > {}", self.value)
    }
}

pub struct Gte {
    pub value: GeneratedValue,
}
impl Display for Gte {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, " >= {}", self.value)
    }
}

pub struct Lt {
    pub value: GeneratedValue,
}
impl Display for Lt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, " < {}", self.value)
    }
}

pub struct Lte {
    pub value: GeneratedValue,
}
impl Display for Lte {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, " <= {}", self.value)
    }
}

pub struct Eq {
    pub value: GeneratedValue,
}
impl Display for Eq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, " == {}", self.value)
    }
}
pub struct Neq {
    pub value: GeneratedValue,
}
impl Display for Neq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, " != {}", self.value)
    }
}

pub struct Contains {
    pub value: GeneratedValue,
}
impl Display for Contains {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, ".contains({})", self.value)
    }
}
