use std::fmt::{self, Display};

use crate::helixc::{
    generator::utils::GenRef,
    parser::types::{Expression, ExpressionType, MathFunction},
};

/// Generated mathematical expression
#[derive(Debug, Clone)]
pub enum MathExpr {
    FunctionCall(MathFunctionCallGen),
    NumericLiteral(NumericLiteral),
    PropertyAccess(PropertyAccess),
    Identifier(String),
}

/// Context for property access in weight calculations
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PropertyContext {
    Edge,       // _::{property}
    SourceNode, // _::From::{property}
    TargetNode, // _::To::{property}
    Current,    // Default context (from traversal value)
}

#[derive(Debug, Clone)]
pub struct MathFunctionCallGen {
    pub function: MathFunction,
    pub args: Vec<MathExpr>,
}

#[derive(Debug, Clone)]
pub struct NumericLiteral {
    pub value: f64,
}

#[derive(Debug, Clone)]
pub struct PropertyAccess {
    pub context: PropertyContext,
    pub property: GenRef<String>,
}

impl Display for MathExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FunctionCall(call) => write!(f, "{}", call),
            Self::NumericLiteral(n) => write!(f, "{}", n),
            Self::PropertyAccess(prop) => write!(f, "{}", prop),
            Self::Identifier(id) => write!(f, "{}", id),
        }
    }
}

impl Display for NumericLiteral {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Handle special formatting for cleaner output
        if self.value.fract() == 0.0 && self.value.abs() < i64::MAX as f64 {
            write!(f, "{}_f64", self.value as i64)
        } else {
            write!(f, "{}_f64", self.value)
        }
    }
}

impl Display for PropertyAccess {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.context {
            PropertyContext::Edge => {
                write!(f, "(edge.get_property({}).ok_or(GraphError::Default)?.as_f64())", self.property)
            }
            PropertyContext::SourceNode => {
                write!(f, "(src_node.get_property({}).ok_or(GraphError::Default)?.as_f64())", self.property)
            }
            PropertyContext::TargetNode => {
                write!(f, "(dst_node.get_property({}).ok_or(GraphError::Default)?.as_f64())", self.property)
            }
            PropertyContext::Current => {
                write!(f, "(v.get_property({}).ok_or(GraphError::Default)?.as_f64())", self.property)
            }
        }
    }
}

impl Display for MathFunctionCallGen {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.function {
            // Binary operators
            MathFunction::Add => {
                if self.args.len() != 2 {
                    return Err(fmt::Error);
                }
                write!(f, "({} + {})", self.args[0], self.args[1])
            }
            MathFunction::Sub => {
                if self.args.len() != 2 {
                    return Err(fmt::Error);
                }
                write!(f, "({} - {})", self.args[0], self.args[1])
            }
            MathFunction::Mul => {
                if self.args.len() != 2 {
                    return Err(fmt::Error);
                }
                write!(f, "({} * {})", self.args[0], self.args[1])
            }
            MathFunction::Div => {
                if self.args.len() != 2 {
                    return Err(fmt::Error);
                }
                write!(f, "({} / {})", self.args[0], self.args[1])
            }
            MathFunction::Pow => {
                if self.args.len() != 2 {
                    return Err(fmt::Error);
                }
                write!(f, "({}).powf({})", self.args[0], self.args[1])
            }
            MathFunction::Mod => {
                if self.args.len() != 2 {
                    return Err(fmt::Error);
                }
                write!(f, "({}) % ({})", self.args[0], self.args[1])
            }

            // Unary functions
            MathFunction::Abs => {
                if self.args.len() != 1 {
                    return Err(fmt::Error);
                }
                write!(f, "({}).abs()", self.args[0])
            }
            MathFunction::Sqrt => {
                if self.args.len() != 1 {
                    return Err(fmt::Error);
                }
                write!(f, "({}).sqrt()", self.args[0])
            }
            MathFunction::Ln => {
                if self.args.len() != 1 {
                    return Err(fmt::Error);
                }
                write!(f, "({}).ln()", self.args[0])
            }
            MathFunction::Log10 => {
                if self.args.len() != 1 {
                    return Err(fmt::Error);
                }
                write!(f, "({}).log10()", self.args[0])
            }
            MathFunction::Log => {
                if self.args.len() != 2 {
                    return Err(fmt::Error);
                }
                write!(f, "({}).log({})", self.args[0], self.args[1])
            }
            MathFunction::Exp => {
                if self.args.len() != 1 {
                    return Err(fmt::Error);
                }
                write!(f, "({}).exp()", self.args[0])
            }
            MathFunction::Ceil => {
                if self.args.len() != 1 {
                    return Err(fmt::Error);
                }
                write!(f, "({}).ceil()", self.args[0])
            }
            MathFunction::Floor => {
                if self.args.len() != 1 {
                    return Err(fmt::Error);
                }
                write!(f, "({}).floor()", self.args[0])
            }
            MathFunction::Round => {
                if self.args.len() != 1 {
                    return Err(fmt::Error);
                }
                write!(f, "({}).round()", self.args[0])
            }

            // Trigonometry
            MathFunction::Sin => {
                if self.args.len() != 1 {
                    return Err(fmt::Error);
                }
                write!(f, "({}).sin()", self.args[0])
            }
            MathFunction::Cos => {
                if self.args.len() != 1 {
                    return Err(fmt::Error);
                }
                write!(f, "({}).cos()", self.args[0])
            }
            MathFunction::Tan => {
                if self.args.len() != 1 {
                    return Err(fmt::Error);
                }
                write!(f, "({}).tan()", self.args[0])
            }
            MathFunction::Asin => {
                if self.args.len() != 1 {
                    return Err(fmt::Error);
                }
                write!(f, "({}).asin()", self.args[0])
            }
            MathFunction::Acos => {
                if self.args.len() != 1 {
                    return Err(fmt::Error);
                }
                write!(f, "({}).acos()", self.args[0])
            }
            MathFunction::Atan => {
                if self.args.len() != 1 {
                    return Err(fmt::Error);
                }
                write!(f, "({}).atan()", self.args[0])
            }
            MathFunction::Atan2 => {
                if self.args.len() != 2 {
                    return Err(fmt::Error);
                }
                write!(f, "({}).atan2({})", self.args[0], self.args[1])
            }

            // Constants (nullary)
            MathFunction::Pi => write!(f, "std::f64::consts::PI"),
            MathFunction::E => write!(f, "std::f64::consts::E"),

            // Aggregates (special handling needed)
            MathFunction::Min
            | MathFunction::Max
            | MathFunction::Sum
            | MathFunction::Avg
            | MathFunction::Count => {
                // For now, these will need special implementation in the context they're used
                write!(
                    f,
                    "/* Aggregate function {} not yet implemented */",
                    self.function.name()
                )
            }
        }
    }
}

/// Expression context for code generation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpressionContext {
    WeightCalculation, // Has access to edge, src_node, dst_node
    Filter,            // Has access to current traversal value
    General,           // Standard expression context
}

/// Convert AST expression to generated math expression
pub fn generate_math_expr(
    expr: &Expression,
    context: ExpressionContext,
) -> Result<MathExpr, String> {
    match &expr.expr {
        ExpressionType::MathFunctionCall(call) => {
            let args = call
                .args
                .iter()
                .map(|arg| generate_math_expr(arg, context))
                .collect::<Result<Vec<_>, _>>()?;

            Ok(MathExpr::FunctionCall(MathFunctionCallGen {
                function: call.function.clone(),
                args,
            }))
        }
        ExpressionType::IntegerLiteral(i) => Ok(MathExpr::NumericLiteral(NumericLiteral {
            value: *i as f64,
        })),
        ExpressionType::FloatLiteral(f) => {
            Ok(MathExpr::NumericLiteral(NumericLiteral { value: *f }))
        }
        ExpressionType::Identifier(id) => Ok(MathExpr::Identifier(id.clone())),
        ExpressionType::Traversal(traversal) => {
            // Parse property access from traversal
            // This is where we'd handle _::{prop}, _::From::{prop}, _::To::{prop}
            parse_property_access_from_traversal(traversal, context)
        }
        _ => Err(format!(
            "Unsupported expression type in math expression: {:?}",
            expr.expr
        )),
    }
}

/// Parse property access from traversal to determine context
fn parse_property_access_from_traversal(
    traversal: &crate::helixc::parser::types::Traversal,
    context: ExpressionContext,
) -> Result<MathExpr, String> {
    use crate::helixc::parser::types::{GraphStepType, StartNode, StepType};

    // Check if this is an anonymous traversal (_::...)
    if !matches!(traversal.start, StartNode::Anonymous) {
        return Err("Expected anonymous traversal starting with _::".to_string());
    }

    // Determine property context based on traversal steps
    let (prop_context, property_step_idx) = if traversal.steps.len() == 1 {
        // Simple case: _::{property}
        (PropertyContext::Edge, 0)
    } else if traversal.steps.len() == 2 {
        // Check if first step is FromN or ToN
        match &traversal.steps[0].step {
            StepType::Node(graph_step) => {
                match &graph_step.step {
                    GraphStepType::FromN => (PropertyContext::SourceNode, 1),
                    GraphStepType::ToN => (PropertyContext::TargetNode, 1),
                    _ => return Err(format!("Unexpected node step type in property access: {:?}", graph_step.step)),
                }
            }
            _ => return Err(format!("Expected FromN or ToN step, got: {:?}", traversal.steps[0].step)),
        }
    } else {
        return Err(format!("Invalid traversal length for property access: {}", traversal.steps.len()));
    };

    // Extract property name from the Object step
    if let StepType::Object(obj) = &traversal.steps[property_step_idx].step
        && obj.fields.len() == 1 && !obj.should_spread {
        let property_name = obj.fields[0].key.clone();

        // Override context if specified by ExpressionContext
        let final_context = match context {
            ExpressionContext::WeightCalculation => prop_context,
            ExpressionContext::Filter | ExpressionContext::General => PropertyContext::Current,
        };

        return Ok(MathExpr::PropertyAccess(PropertyAccess {
            context: final_context,
            property: GenRef::Literal(property_name),
        }));
    }

    Err("Failed to extract property name from traversal".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_numeric_literal_integer() {
        let lit = NumericLiteral { value: 5.0 };
        assert_eq!(lit.to_string(), "5_f64");
    }

    #[test]
    fn test_numeric_literal_float() {
        let lit = NumericLiteral { value: 3.14 };
        assert_eq!(lit.to_string(), "3.14_f64");
    }

    #[test]
    fn test_add_function() {
        let add = MathFunctionCallGen {
            function: MathFunction::Add,
            args: vec![
                MathExpr::NumericLiteral(NumericLiteral { value: 5.0 }),
                MathExpr::NumericLiteral(NumericLiteral { value: 3.0 }),
            ],
        };
        assert_eq!(add.to_string(), "(5_f64 + 3_f64)");
    }

    #[test]
    fn test_pow_function() {
        let pow = MathFunctionCallGen {
            function: MathFunction::Pow,
            args: vec![
                MathExpr::NumericLiteral(NumericLiteral { value: 0.95 }),
                MathExpr::NumericLiteral(NumericLiteral { value: 30.0 }),
            ],
        };
        assert_eq!(pow.to_string(), "(0.95_f64).powf(30_f64)");
    }

    #[test]
    fn test_nested_functions() {
        let nested = MathFunctionCallGen {
            function: MathFunction::Pow,
            args: vec![
                MathExpr::NumericLiteral(NumericLiteral { value: 0.95 }),
                MathExpr::FunctionCall(MathFunctionCallGen {
                    function: MathFunction::Div,
                    args: vec![
                        MathExpr::NumericLiteral(NumericLiteral { value: 10.0 }),
                        MathExpr::NumericLiteral(NumericLiteral { value: 30.0 }),
                    ],
                }),
            ],
        };
        assert_eq!(nested.to_string(), "(0.95_f64).powf((10_f64 / 30_f64))");
    }

    #[test]
    fn test_sqrt_function() {
        let sqrt = MathFunctionCallGen {
            function: MathFunction::Sqrt,
            args: vec![MathExpr::NumericLiteral(NumericLiteral { value: 16.0 })],
        };
        assert_eq!(sqrt.to_string(), "(16_f64).sqrt()");
    }

    #[test]
    fn test_trig_functions() {
        let sin = MathFunctionCallGen {
            function: MathFunction::Sin,
            args: vec![MathExpr::NumericLiteral(NumericLiteral { value: 1.57 })],
        };
        assert_eq!(sin.to_string(), "(1.57_f64).sin()");
    }

    #[test]
    fn test_constants() {
        let pi = MathFunctionCallGen {
            function: MathFunction::Pi,
            args: vec![],
        };
        assert_eq!(pi.to_string(), "std::f64::consts::PI");

        let e = MathFunctionCallGen {
            function: MathFunction::E,
            args: vec![],
        };
        assert_eq!(e.to_string(), "std::f64::consts::E");
    }

    #[test]
    fn test_property_access_contexts() {
        // Test Edge context
        let edge_prop = PropertyAccess {
            context: PropertyContext::Edge,
            property: GenRef::Literal("distance".to_string()),
        };
        assert_eq!(
            edge_prop.to_string(),
            "(edge.get_property(\"distance\").ok_or(GraphError::Default)?.as_f64())"
        );

        // Test SourceNode context
        let src_prop = PropertyAccess {
            context: PropertyContext::SourceNode,
            property: GenRef::Literal("traffic_factor".to_string()),
        };
        assert_eq!(
            src_prop.to_string(),
            "(src_node.get_property(\"traffic_factor\").ok_or(GraphError::Default)?.as_f64())"
        );

        // Test TargetNode context
        let dst_prop = PropertyAccess {
            context: PropertyContext::TargetNode,
            property: GenRef::Literal("popularity".to_string()),
        };
        assert_eq!(
            dst_prop.to_string(),
            "(dst_node.get_property(\"popularity\").ok_or(GraphError::Default)?.as_f64())"
        );
    }

    #[test]
    fn test_complex_weight_expression() {
        // Test: MUL(_::{distance}, POW(0.95, DIV(_::{days}, 30)))
        // Should generate: ((edge.get_property("distance").ok_or(GraphError::Default)?.as_f64()) * (0.95_f64).powf(((edge.get_property("days").ok_or(GraphError::Default)?.as_f64()) / 30_f64)))
        let expr = MathFunctionCallGen {
            function: MathFunction::Mul,
            args: vec![
                MathExpr::PropertyAccess(PropertyAccess {
                    context: PropertyContext::Edge,
                    property: GenRef::Literal("distance".to_string()),
                }),
                MathExpr::FunctionCall(MathFunctionCallGen {
                    function: MathFunction::Pow,
                    args: vec![
                        MathExpr::NumericLiteral(NumericLiteral { value: 0.95 }),
                        MathExpr::FunctionCall(MathFunctionCallGen {
                            function: MathFunction::Div,
                            args: vec![
                                MathExpr::PropertyAccess(PropertyAccess {
                                    context: PropertyContext::Edge,
                                    property: GenRef::Literal("days".to_string()),
                                }),
                                MathExpr::NumericLiteral(NumericLiteral { value: 30.0 }),
                            ],
                        }),
                    ],
                }),
            ],
        };

        assert_eq!(
            expr.to_string(),
            "((edge.get_property(\"distance\").ok_or(GraphError::Default)?.as_f64()) * (0.95_f64).powf(((edge.get_property(\"days\").ok_or(GraphError::Default)?.as_f64()) / 30_f64)))"
        );
    }

    #[test]
    fn test_multi_context_expression() {
        // Test: MUL(_::{distance}, _::From::{traffic_factor})
        // Should generate: ((edge.get_property("distance").ok_or(GraphError::Default)?.as_f64()) * (src_node.get_property("traffic_factor").ok_or(GraphError::Default)?.as_f64()))
        let expr = MathFunctionCallGen {
            function: MathFunction::Mul,
            args: vec![
                MathExpr::PropertyAccess(PropertyAccess {
                    context: PropertyContext::Edge,
                    property: GenRef::Literal("distance".to_string()),
                }),
                MathExpr::PropertyAccess(PropertyAccess {
                    context: PropertyContext::SourceNode,
                    property: GenRef::Literal("traffic_factor".to_string()),
                }),
            ],
        };

        assert_eq!(
            expr.to_string(),
            "((edge.get_property(\"distance\").ok_or(GraphError::Default)?.as_f64()) * (src_node.get_property(\"traffic_factor\").ok_or(GraphError::Default)?.as_f64()))"
        );
    }
}
