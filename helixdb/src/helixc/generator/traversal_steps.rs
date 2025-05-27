use crate::helixc::generator::utils::write_properties;

use super::{
    bool_op::BoolOp,
    generator_types::BoExp,
    object_remapping_generation::{ClosureFieldRemapping, ExcludeField, FieldRemapping, Remapping},
    source_steps::SourceStep,
    utils::{GenRef, GeneratedValue, Order, Separator},
};
use core::fmt;
use std::{
    clone,
    fmt::{Debug, Display},
};

#[derive(Clone)]
pub enum TraversalType {
    FromVar(GenRef<String>),
    Ref,
    Mut,
    Nested(GenRef<String>), // Should contain `.clone()` if necessary (probably is)
    NestedFrom(GenRef<String>),
    Empty,
    Update(Option<Vec<(String, GeneratedValue)>>),
}
impl Debug for TraversalType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TraversalType::FromVar(_) => write!(f, "FromVar"),
            TraversalType::Ref => write!(f, "Ref"),
            TraversalType::Nested(_) => write!(f, "Nested"),
            _ => write!(f, "other"),
        }
    }
}
// impl Display for TraversalType {
//     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
//         match self {
//             TraversalType::FromVar => write!(f, ""),
//             TraversalType::Ref => write!(f, "G::new(Arc::clone(&db), &txn)"),

//             TraversalType::Mut => write!(f, "G::new_mut(Arc::clone(&db), &mut txn)"),
//             TraversalType::Nested(nested) => {
//                 assert!(nested.inner().len() > 0, "Empty nested traversal name");
//                 write!(f, "G::new_from(Arc::clone(&db), &txn, {})", nested)
//             }
//             TraversalType::Update => write!(f, ""),
//             // TraversalType::FromVar(var) => write!(f, "G::new_from(Arc::clone(&db), &txn, {})", var),
//             TraversalType::Empty => panic!("Should not be empty"),
//         }
//     }
// }
#[derive(Clone)]
pub enum ShouldCollect {
    ToVec,
    ToVal,
    No,
}
impl Display for ShouldCollect {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ShouldCollect::ToVec => write!(f, ".collect_to::<Vec<_>>()"),
            ShouldCollect::ToVal => write!(f, ".collect_to::<_>()"),
            ShouldCollect::No => write!(f, ""),
        }
    }
}

#[derive(Clone)]
pub struct Traversal {
    pub traversal_type: TraversalType,
    pub source_step: Separator<SourceStep>,
    pub steps: Vec<Separator<Step>>,
    pub should_collect: ShouldCollect,
}

impl Display for Traversal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.traversal_type {
            TraversalType::FromVar(var) => {
                write!(f, "G::new_from(Arc::clone(&db), &txn, {}.clone())", var)?;
                write!(f, "{}", self.source_step)?;
                for step in &self.steps {
                    write!(f, "\n{}", step)?;
                }
            }
            TraversalType::Ref => {
                write!(f, "G::new(Arc::clone(&db), &txn)")?;
                write!(f, "{}", self.source_step)?;
                for step in &self.steps {
                    write!(f, "\n{}", step)?;
                }
            }

            TraversalType::Mut => {
                write!(f, "G::new_mut(Arc::clone(&db), &mut txn)")?;
                write!(f, "{}", self.source_step)?;
                for step in &self.steps {
                    write!(f, "\n{}", step)?;
                }
            }
            TraversalType::Nested(nested) => {
                assert!(nested.inner().len() > 0, "Empty nested traversal name");
                write!(f, "{}", nested)?; // this should be var name default val
                for step in &self.steps {
                    write!(f, "\n{}", step)?;
                }
            }
            TraversalType::NestedFrom(nested) => {
                assert!(nested.inner().len() > 0, "Empty nested traversal name");
                write!(
                    f,
                    "G::new_from(Arc::clone(&db), &txn, vec![{}.clone()])",
                    nested
                )?;
                for step in &self.steps {
                    write!(f, "\n{}", step)?;
                }
            }
            TraversalType::Empty => panic!("Should not be empty"),
            TraversalType::Update(properties) => {
                write!(f, "{{")?;
                write!(f, "let update_tr = G::new(Arc::clone(&db), &txn)")?;
                write!(f, "{}", self.source_step)?;
                for step in &self.steps {
                    write!(f, "\n{}", step)?;
                }
                write!(f, "\n    .collect_to::<Vec<_>>();")?;
                write!(
                    f,
                    "G::new_mut_from(Arc::clone(&db), &mut txn, update_tr)", // TODO: make
                                                                             // this less
                                                                             // scrappy
                )?;
                write!(f, "\n    .update({})", write_properties(&properties))?;
                write!(f, "\n    .collect_to::<Vec<_>>()")?;
                write!(f, "}}")?;
            }
        }
        write!(f, "{}", self.should_collect)
    }
}
impl Default for Traversal {
    fn default() -> Self {
        Self {
            traversal_type: TraversalType::Ref,
            source_step: Separator::Empty(SourceStep::Empty),
            steps: vec![],
            should_collect: ShouldCollect::ToVec,
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

    // closure
    // Closure(ClosureRemapping),

    // shortest path
    ShortestPath(ShortestPath),
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
            Step::ShortestPath(shortest_path) => write!(f, "{}", shortest_path),
        }
    }
}
impl Debug for Step {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Step::Count => write!(f, "Count"),
            Step::Dedup => write!(f, "Dedup"),
            Step::FromN => write!(f, "FromN"),
            Step::ToN => write!(f, "ToN"),
            Step::PropertyFetch(property) => write!(f, "check_property({})", property),

            Step::Out(out) => write!(f, "Out"),
            Step::In(in_) => write!(f, "In"),
            Step::OutE(out_e) => write!(f, "OutE"),
            Step::InE(in_e) => write!(f, "InE"),
            Step::Where(where_) => write!(f, "Where"),
            Step::Range(range) => write!(f, "Range"),
            Step::OrderBy(order_by) => write!(f, "OrderBy"),
            Step::BoolOp(bool_op) => write!(f, "Bool"),
            Step::Remapping(remapping) => write!(f, "Remapping"),
            Step::ShortestPath(shortest_path) => write!(f, "ShortestPath"),
        }
    }
}

#[derive(Clone)]
pub struct Out {
    pub label: GenRef<String>,
    pub edge_type: GenRef<String>,
}
impl Display for Out {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "out({},{})", self.label, self.edge_type)
    }
}

#[derive(Clone)]
pub struct In {
    pub label: GenRef<String>,
    pub edge_type: GenRef<String>,
}
impl Display for In {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "in_({},{})", self.label, self.edge_type)
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
    Exists(WhereExists),
    Ref(WhereRef),
    Mut(WhereMut),
}
impl Display for Where {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Where::Exists(ex) => write!(f, "{}", ex),
            Where::Ref(wr) => write!(f, "{}", wr),
            Where::Mut(wm) => write!(f, "{}", wm),
        }
    }
}

#[derive(Clone)]
pub struct WhereExists {
    pub tr: Traversal,
}
impl Display for WhereExists {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "filter_ref(|val, txn|{{
                if let Ok(val) = val {{ 
                    Ok({}.count().gt(&0))
                }} else {{
                    Ok(false)
                }}
            }})",
            self.tr
        )
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
            "filter_ref(|val, txn|{{
                if let Ok(val) = val {{ 
                    Ok({})
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
        unimplemented!();
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
pub struct ShortestPath {
    pub label: Option<GenRef<String>>,
    pub from: Option<GenRef<String>>,
    pub to: Option<GenRef<String>>,
}
impl Display for ShortestPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "shortest_path({}, {}, {})",
            self.label
                .clone()
                .map_or("None".to_string(), |label| format!("Some({})", label)),
            self.from
                .clone()
                .map_or("None".to_string(), |from| format!("Some(&{})", from)),
            self.to
                .clone()
                .map_or("None".to_string(), |to| format!("Some(&{})", to))
        )
    }
}
