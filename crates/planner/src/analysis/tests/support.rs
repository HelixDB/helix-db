use helix_ast::expr::Expr;
use helix_ast::value::PropertyValue;

pub(super) fn constant(value: impl Into<PropertyValue>) -> Expr {
    Expr::Constant(value.into())
}
