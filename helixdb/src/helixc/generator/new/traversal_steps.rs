use crate::helixc::generator::new::utils::write_properties;

use super::{
    bool_op::BoolOp,
    generator_types::BoExp,
    object_remapping_generation::{ClosureFieldRemapping, ExcludeField, FieldRemapping, Remapping},
    source_steps::SourceStep,
    utils::{GenRef, GeneratedValue, Order, Separator},
};
use core::fmt;
use std::{clone, fmt::Display};

#[derive(Clone)]
pub enum TraversalType {
    FromVar,
    Ref,
    Mut,
    Nested(GenRef<String>), // Should contain `.clone()` if necessary (probably is)
    // FromVar(GenRef<String>),
    Update,
    Empty,
}
impl Display for TraversalType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TraversalType::FromVar => write!(f, ""),
            TraversalType::Ref => write!(f, "G::new(Arc::clone(&db), &txn)"),

            TraversalType::Mut => write!(f, "G::new_mut(Arc::clone(&db), &mut txn)"),
            TraversalType::Nested(nested) => {
                assert!(nested.inner().len() > 0, "Empty nested traversal name");
                write!(f, "G::new_from(Arc::clone(&db), &txn, {})", nested)
            }
            TraversalType::Update => write!(f, ""),
            // TraversalType::FromVar(var) => write!(f, "G::new_from(Arc::clone(&db), &txn, {})", var),
            TraversalType::Empty => panic!("Should not be empty"),
        }
    }
}

#[derive(Clone)]
pub struct Traversal {
    pub traversal_type: TraversalType,
    pub source_step: Separator<SourceStep>,
    pub steps: Vec<Separator<Step>>,
}

impl Display for Traversal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.traversal_type)?;
        write!(f, "{}", self.source_step)?;
        for step in &self.steps {
            write!(f, "\n{}", step)?;
        }
        if let TraversalType::FromVar = self.traversal_type {
            write!(f, "")
        } else {
            write!(f, "\n    .collect_to::<Vec<_>>()")
        }
    }
}
impl Default for Traversal {
    fn default() -> Self {
        Self {
            traversal_type: TraversalType::Ref,
            source_step: Separator::Empty(SourceStep::Empty),
            steps: vec![],
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

    // update
    Update(Update),
}
impl Display for Step {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Step::Count => write!(f, "count()"),
            Step::Dedup => write!(f, "dedup()"),
            Step::FromN => write!(f, "from_n()"),
            Step::ToN => write!(f, "to_n()"),
            Step::PropertyFetch(property) => write!(f, "check_property({})", property),

            Step::Out(out) => write!(f, "{}", out),
            Step::In(in_) => write!(f, "{}", in_),
            Step::OutE(out_e) => write!(f, "{}", out_e),
            Step::InE(in_e) => write!(f, "{}", in_e),
            Step::Where(where_) => write!(f, "{}", where_),
            Step::Range(range) => write!(f, "{}", range),
            Step::OrderBy(order_by) => write!(f, "{}", order_by),
            Step::BoolOp(bool_op) => write!(f, "{}", bool_op),
            Step::Remapping(remapping) => write!(f, "{}", remapping),

            Step::Update(update) => write!(f, "{}", update),
        }
    }
}

#[derive(Clone)]
pub struct Out {
    pub label: GenRef<String>,
}
impl Display for Out {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "out({})", self.label)
    }
}

#[derive(Clone)]
pub struct In {
    pub label: GenRef<String>,
}
impl Display for In {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "in({})", self.label)
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
    Mut(WhereMut),
}
impl Display for Where {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self)
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

#[derive(Clone)]
pub struct WhereMut {
    pub expr: BoExp,
}
impl Display for WhereMut {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NOT IMPLEMENTED")
    }
}

#[derive(Clone)]
pub struct Range {
    pub start: GenRef<String>,
    pub end: GenRef<String>,
}
impl Display for Range {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "range({}, {})", self.start, self.end)
    }
}

#[derive(Clone)]
pub struct OrderBy {
    pub property: String,
    pub order: Order,
}
impl Display for OrderBy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "order_by({}, HelixOrder::{})", self.property, self.order)
    }
}

#[derive(Clone)]
pub struct Update {
    pub fields: Vec<(String, GeneratedValue)>,
}

impl Display for Update {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "G::new_mut_from(Arc::clone(&db), &mut txn,)")?;
        write!(f, ".update({})", write_properties(&self.fields))
    }
}
