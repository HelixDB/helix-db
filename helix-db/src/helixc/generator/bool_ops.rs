use core::fmt;
use std::fmt::Display;

use crate::helixc::generator::traversal_steps::{Step, Traversal, TraversalType};

use super::utils::{GenRef, GeneratedValue, Separator};

#[derive(Clone, Debug)]
pub enum BoolOp {
    Gt(Gt),
    Gte(Gte),
    Lt(Lt),
    Lte(Lte),
    Eq(Eq),
    Neq(Neq),
    Contains(Contains),
    IsIn(IsIn),
}
impl Display for BoolOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Self::Gt(gt) => format!("{gt}"),
            Self::Gte(gte) => format!("{gte}"),
            Self::Lt(lt) => format!("{lt}"),
            Self::Lte(lte) => format!("{lte}"),
            Self::Eq(eq) => format!("{eq}"),
            Self::Neq(neq) => format!("{neq}"),
            Self::Contains(contains) => format!("v{contains}"),
            Self::IsIn(is_in) => format!("v{is_in}"),
        };
        write!(f, "map_value_or(false, |v| {s})?")
    }
}
#[derive(Clone, Debug)]
pub struct Gt {
    pub left: GeneratedValue,
    pub right: GeneratedValue,
}
impl Display for Gt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} > {}", self.left, self.right)
    }
}

#[derive(Clone, Debug)]
pub struct Gte {
    pub left: GeneratedValue,
    pub right: GeneratedValue,
}
impl Display for Gte {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} >= {}", self.left, self.right)
    }
}

#[derive(Clone, Debug)]
pub struct Lt {
    pub left: GeneratedValue,
    pub right: GeneratedValue,
}
impl Display for Lt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} < {}", self.left, self.right)
    }
}

#[derive(Clone, Debug)]
pub struct Lte {
    pub left: GeneratedValue,
    pub right: GeneratedValue,
}
impl Display for Lte {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} <= {}", self.left, self.right)
    }
}

#[derive(Clone, Debug)]
pub struct Eq {
    pub left: GeneratedValue,
    pub right: GeneratedValue,
}
impl Display for Eq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} == {}", self.left, self.right)
    }
}

#[derive(Clone, Debug)]
pub struct Neq {
    pub left: GeneratedValue,
    pub right: GeneratedValue,
}
impl Display for Neq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} != {}", self.left, self.right)
    }
}

#[derive(Clone, Debug)]
pub struct Contains {
    pub value: GeneratedValue,
}
impl Display for Contains {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, ".contains({})", self.value)
    }
}

#[derive(Clone, Debug)]
pub struct IsIn {
    pub value: GeneratedValue,
}
impl Display for IsIn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, ".is_in({})", self.value)
    }
}

/// Boolean expression is used for a traversal or set of traversals wrapped in AND/OR
/// that resolve to a boolean value
#[derive(Clone, Debug)]
pub enum BoExp {
    Not(Box<BoExp>),
    And(Vec<BoExp>),
    Or(Vec<BoExp>),
    Exists(Traversal),
    Expr(Traversal),
    Empty,
}

impl BoExp {
    pub fn negate(&self) -> Self {
        match self {
            Self::Not(expr) => *expr.clone(),
            _ => Self::Not(Box::new(self.clone())),
        }
    }

    pub fn is_not(&self) -> bool {
        matches!(self, Self::Not(_))
    }
}
impl Display for BoExp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Not(expr) => write!(f, "!({expr})"),
            Self::And(exprs) => {
                let displayed_exprs = exprs.iter().map(|s| format!("{s}")).collect::<Vec<_>>();
                write!(f, "({})", displayed_exprs.join(" && "))
            }
            Self::Or(exprs) => {
                let displayed_exprs = exprs.iter().map(|s| format!("{s}")).collect::<Vec<_>>();
                write!(f, "({})", displayed_exprs.join(" || "))
            }
            Self::Exists(traversal) => {
                // Optimize Exists expressions in filter context to use std::iter::once for single values
                let is_val_traversal = match &traversal.traversal_type {
                    TraversalType::FromIter(var) | TraversalType::FromSingle(var) => match var {
                        GenRef::Std(s) | GenRef::Literal(s) => s == "val",
                        _ => false,
                    },
                    _ => false,
                };

                if is_val_traversal {
                    // Create a modified traversal that uses FromSingle instead of FromIter
                    // This will generate: G::from_iter(&db, &txn, std::iter::once(val.clone()), &arena)
                    let mut optimized = traversal.clone();
                    if let TraversalType::FromIter(var) = &traversal.traversal_type {
                        optimized.traversal_type = TraversalType::FromSingle(var.clone());
                    }
                    write!(f, "Exist::exists(&mut {optimized})")
                } else {
                    write!(f, "Exist::exists(&mut {traversal})")
                }
            }
            Self::Expr(traversal) => {
                // Optimize simple property checks in filters to avoid unnecessary cloning and traversal creation
                // Check if this is a FromVar("val") or FromSingle("val") traversal with just property fetch + bool op
                let is_val_traversal = match &traversal.traversal_type {
                    TraversalType::FromIter(var) | TraversalType::FromSingle(var) => match var {
                        GenRef::Std(s) | GenRef::Literal(s) => s == "val",
                        _ => false,
                    },
                    _ => false,
                };

                if is_val_traversal {
                    // Look for PropertyFetch followed by BoolOp pattern (in any Separator type)
                    let mut prop_info: Option<&GenRef<String>> = None;
                    let mut bool_op_info: Option<&BoolOp> = None;
                    let mut other_steps = 0;

                    for step in traversal.steps.iter() {
                        match step {
                            Separator::Period(Step::PropertyFetch(prop))
                            | Separator::Newline(Step::PropertyFetch(prop))
                            | Separator::Empty(Step::PropertyFetch(prop))
                            | Separator::Comma(Step::PropertyFetch(prop))
                            | Separator::Semicolon(Step::PropertyFetch(prop)) => {
                                if prop_info.is_none() {
                                    prop_info = Some(prop);
                                }
                            }
                            Separator::Period(Step::BoolOp(op))
                            | Separator::Newline(Step::BoolOp(op))
                            | Separator::Empty(Step::BoolOp(op))
                            | Separator::Comma(Step::BoolOp(op))
                            | Separator::Semicolon(Step::BoolOp(op)) => {
                                if bool_op_info.is_none() {
                                    bool_op_info = Some(op);
                                }
                            }
                            _ => {
                                other_steps += 1;
                            }
                        }
                    }

                    // If we found exactly one PropertyFetch and one BoolOp, and no other steps, optimize
                    if let (Some(prop), Some(bool_op)) = (prop_info, bool_op_info)
                        && other_steps == 0
                    {
                        // Generate optimized code: val.get_property("prop").map_or(false, |v| ...)
                        let bool_expr = match bool_op {
                            BoolOp::Gt(gt) => format!("{gt}"),
                            BoolOp::Gte(gte) => format!("{gte}"),
                            BoolOp::Lt(lt) => format!("{lt}"),
                            BoolOp::Lte(lte) => format!("{lte}"),
                            BoolOp::Eq(eq) => format!("{eq}"),
                            BoolOp::Neq(neq) => format!("{neq}"),
                            BoolOp::Contains(contains) => format!("v{contains}"),
                            BoolOp::IsIn(is_in) => format!("v{is_in}"),
                        };
                        return write!(
                            f,
                            "val\n                    .get_property({})\n                    .map_or(false, |v| {})",
                            prop, bool_expr
                        );
                    }
                }
                // Fall back to full traversal for complex expressions
                write!(f, "{traversal}")
            }
            Self::Empty => write!(f, ""),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::helixc::generator::utils::GenRef;

    // ============================================================================
    // Comparison Operator Tests
    // ============================================================================

    #[test]
    fn test_gt_display() {
        let gt = Gt {
            left: GeneratedValue::Primitive(GenRef::Std("*v".to_string())),
            right: GeneratedValue::Primitive(GenRef::Std("10".to_string())),
        };
        assert_eq!(format!("{}", gt), "*v > 10");
    }

    #[test]
    fn test_gte_display() {
        let gte = Gte {
            left: GeneratedValue::Primitive(GenRef::Std("*v".to_string())),
            right: GeneratedValue::Primitive(GenRef::Std("5".to_string())),
        };
        assert_eq!(format!("{}", gte), "*v >= 5");
    }

    #[test]
    fn test_lt_display() {
        let lt = Lt {
            left: GeneratedValue::Primitive(GenRef::Std("*v".to_string())),
            right: GeneratedValue::Primitive(GenRef::Std("100".to_string())),
        };
        assert_eq!(format!("{}", lt), "*v < 100");
    }

    #[test]
    fn test_lte_display() {
        let lte = Lte {
            left: GeneratedValue::Primitive(GenRef::Std("*v".to_string())),
            right: GeneratedValue::Primitive(GenRef::Std("50".to_string())),
        };
        assert_eq!(format!("{}", lte), "*v <= 50");
    }

    #[test]
    fn test_eq_display() {
        let eq = Eq {
            left: GeneratedValue::Primitive(GenRef::Std("*v".to_string())),
            right: GeneratedValue::Literal(GenRef::Literal("test".to_string())),
        };
        assert_eq!(format!("{}", eq), "*v == \"test\"");
    }

    #[test]
    fn test_neq_display() {
        let neq = Neq {
            left: GeneratedValue::Primitive(GenRef::Std("*v".to_string())),
            right: GeneratedValue::Primitive(GenRef::Std("null".to_string())),
        };
        assert_eq!(format!("{}", neq), "*v != null");
    }

    #[test]
    fn test_contains_display() {
        let contains = Contains {
            value: GeneratedValue::Literal(GenRef::Literal("substring".to_string())),
        };
        assert_eq!(format!("{}", contains), ".contains(\"substring\")");
    }

    #[test]
    fn test_is_in_display() {
        let is_in = IsIn {
            value: GeneratedValue::Array(GenRef::Std("1, 2, 3".to_string())),
        };
        assert_eq!(format!("{}", is_in), ".is_in(&[1, 2, 3])");
    }

    // ============================================================================
    // BoolOp Tests
    // ============================================================================

    #[test]
    fn test_boolop_gt_wrapped() {
        let bool_op = BoolOp::Gt(Gt {
            left: GeneratedValue::Primitive(GenRef::Std("*v".to_string())),
            right: GeneratedValue::Primitive(GenRef::Std("20".to_string())),
        });
        let output = format!("{}", bool_op);
        assert!(output.contains("map_value_or(false, |v| *v > 20)"));
    }

    #[test]
    fn test_boolop_eq_wrapped() {
        let bool_op = BoolOp::Eq(Eq {
            left: GeneratedValue::Primitive(GenRef::Std("*v".to_string())),
            right: GeneratedValue::Literal(GenRef::Literal("value".to_string())),
        });
        let output = format!("{}", bool_op);
        assert!(output.contains("map_value_or(false, |v| *v == \"value\")"));
    }

    #[test]
    fn test_boolop_contains_wrapped() {
        let bool_op = BoolOp::Contains(Contains {
            value: GeneratedValue::Literal(GenRef::Literal("text".to_string())),
        });
        let output = format!("{}", bool_op);
        assert!(output.contains("map_value_or(false, |v| v.contains(\"text\"))"));
    }

    // ============================================================================
    // BoExp Tests
    // ============================================================================

    #[test]
    fn test_boexp_empty() {
        let boexp = BoExp::Empty;
        assert_eq!(format!("{}", boexp), "");
    }

    #[test]
    fn test_boexp_negate() {
        let boexp = BoExp::Empty;
        let negated = boexp.negate();
        assert!(negated.is_not());
    }

    #[test]
    fn test_boexp_double_negate() {
        let boexp = BoExp::Empty;
        let negated = boexp.negate();
        let double_negated = negated.negate();
        assert!(!double_negated.is_not());
    }

    #[test]
    fn test_boexp_is_not() {
        let not_expr = BoExp::Not(Box::new(BoExp::Empty));
        assert!(not_expr.is_not());

        let normal_expr = BoExp::Empty;
        assert!(!normal_expr.is_not());
    }
}
