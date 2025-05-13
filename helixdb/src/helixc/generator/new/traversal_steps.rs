use super::{
    bool_op::BoolOp,
    generator_types::{BoExp, GeneratedValue, Separator},
    object_remapping_generation::{ClosureFieldRemapping, ExcludeField, FieldRemapping},
    source_steps::SourceStep,
    types::GenRef,
};
use core::fmt;
use std::fmt::Display;

pub enum TraversalType {
    Ref,
    Mut,
    Nested(GenRef<String>), // Should contain `.clone()` if necessary (probably is)
    Empty,
}
impl Display for TraversalType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TraversalType::Ref => write!(f, "G::new(Arc::clone(&db), txn)"),
            TraversalType::Mut => write!(f, "MG::new_mut(Arc::clone(&db), txn)"),
            TraversalType::Nested(nested) => {
                assert!(nested.inner().len() > 0, "Empty nested traversal name");
                write!(f, "G::new_from(Arc::clone(&db), txn, {})", nested)
            }
            TraversalType::Empty => panic!("Should not be empty"),
        }
    }
}

pub struct Traversal {

    pub traversal_type: TraversalType,
    pub source_step: SourceStep,
    pub steps: Vec<Separator<Step>>,
}
impl Display for Traversal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.traversal_type)?;
        write!(f, "{}", self.source_step)?;
        for step in &self.steps {
            write!(f, "{}", step)?;
        }
        Ok(())
    }
}
impl Default for Traversal {
    fn default() -> Self {
        Self {
            traversal_type: TraversalType::Ref,
            source_step: SourceStep::Empty,
            steps: vec![],
        }
    }
}
pub enum Step {
    // graph steps
    Out(Out),
    In(In),
    OutE(OutE),
    InE(InE),
    FromN,
    ToN,

    // utils
    Count,
    Where(Where),
    Range(Range),
    OrderBy(OrderBy),
    Dedup,

    // bool ops
    BoolOp(BoolOp),

    // property
    Property(GenRef<String>),

    // object
    ClosureFieldRemapping(ClosureFieldRemapping),
    FieldRemapping(FieldRemapping),
    ExcludeField(ExcludeField),

}
impl Display for Step {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Step::Count => write!(f, "count()"),
            Step::Dedup => write!(f, "dedup()"),
            Step::FromN => write!(f, "from_n()"),
            Step::ToN => write!(f, "to_n()"),
            Step::Property(property) => write!(f, "check_property({})", property),

            Step::Out(out) => write!(f, "{}", out),
            Step::In(in_) => write!(f, "{}", in_),
            Step::OutE(out_e) => write!(f, "{}", out_e),
            Step::InE(in_e) => write!(f, "{}", in_e),
            Step::Where(where_) => write!(f, "{}", where_),
            Step::Range(range) => write!(f, "{}", range),
            Step::OrderBy(order_by) => write!(f, "{}", order_by),
            Step::BoolOp(bool_op) => write!(f, "{}", bool_op),
            Step::ClosureFieldRemapping(closure_field_remapping) => {
                write!(f, "{}", closure_field_remapping)
            }
            Step::FieldRemapping(field_remapping) => write!(f, "{}", field_remapping),
            Step::ExcludeField(exclude_field) => write!(f, "{}", exclude_field),
        }
    }
}
pub struct Out {
    pub label: GenRef<String>,
}
impl Display for Out {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "out({})", self.label)
    }
}

pub struct In {
    pub label: GenRef<String>,
}
impl Display for In {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "in({})", self.label)
    }
}

pub struct OutE {
    pub label: GenRef<String>,
}
impl Display for OutE {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "out_e({})", self.label)
    }
}

pub struct InE {
    pub label: GenRef<String>,
}
impl Display for InE {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "in_e({})", self.label)
    }
}

pub enum Where {
    Ref(WhereRef),
    Mut(WhereMut),
}
impl Display for Where {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self)
    }
}

pub struct WhereRef {
    pub expr: BoExp,
}
impl Display for WhereRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "filter_ref(||val, txn|{{
                if let Ok(val) = val {{ 
                    {}
                }} else {{
                    Ok(false)
                }}
            }})",
            self.expr
        )
    }
}

pub struct WhereMut {
    pub expr: BoExp,
}
impl Display for WhereMut {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NOT IMPLEMENTED")
    }
}

pub struct Range {
    pub start: GenRef<String>,
    pub end: GenRef<String>,
}
impl Display for Range {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "range({}, {})", self.start, self.end)
    }
}

pub struct OrderBy {
    pub property: String,
    pub order: Order,
}
impl Display for OrderBy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "order_by({}, HelixOrder::{})",
            self.property, self.order
        )
    }
}
// TODO: probably move to protocol
pub enum Order {
    Asc,
    Desc,
}

impl Display for Order {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Order::Asc => write!(f, "Asc"),
            Order::Desc => write!(f, "Desc"),
        }
    }
}
