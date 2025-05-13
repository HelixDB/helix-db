use core::fmt;
use std::fmt::Display;

use crate::helixc::generator::new::new_generator::{write_properties, write_secondary_indices};

use super::{
    generator_types::{BoExp, GeneratedValue},
    types::GenRef,
};

pub enum SourceStep {
    Variable(GenRef<String>),
    AddN(AddN),
    AddE(AddE),
    AddV(AddV),
    SearchV(SearchV),
    NFromID(NFromID),
    NFromType(NFromType),
    EFromID(EFromID),
    EFromType(EFromType),
    Empty,
}

pub struct AddN {
    pub label: GenRef<String>,
    pub properties: Vec<(String, GeneratedValue)>,
    pub secondary_indices: Option<Vec<String>>,
}
impl Display for AddN {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let properties = write_properties(&self.properties);
        let secondary_indices = write_secondary_indices(&self.secondary_indices);
        write!(
            f,
            "add_n({}, {}, {})",
            self.label, properties, secondary_indices
        )
    }
}

pub struct AddE {
    pub label: GenRef<String>,
    pub properties: Vec<(String, GeneratedValue)>,
    pub from: GenRef<String>,
    pub to: GenRef<String>,
    pub secondary_indices: Option<Vec<String>>,
}
impl Display for AddE {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let properties = write_properties(&self.properties);
        let secondary_indices = write_secondary_indices(&self.secondary_indices);
        write!(
            f,
            "add_e({}, {}, {}, {}, {})",
            self.label, properties, self.from, self.to, secondary_indices
        )
    }
}
pub struct AddV {
    pub vec: GeneratedValue,
    pub label: String,
    pub properties: Vec<(String, GeneratedValue)>,
}
impl Display for AddV {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let properties = write_properties(&self.properties);
        write!(f, "add_v({}, {}, {})", self.vec, self.label, properties)
    }
}

/// where F: Fn(&HVector) -> bool;
pub struct SearchV {
    pub vec: GeneratedValue,
    pub properties: Vec<(String, GeneratedValue)>,
    pub f: Vec<BoExp>,
}
impl Display for SearchV {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let properties = write_properties(&self.properties);
        let f_str = self.f.iter().map(|f| format!("{}", f)).collect::<Vec<_>>().join(", ");
        write!(f, "search_v({}, {}, {})", self.vec, properties, f_str)
    }
}

pub struct NFromID {
    pub id: GenRef<String>,
    pub label: GenRef<String>, // possible not needed, do we do runtime label checking?
}
impl Display for NFromID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // TODO: possibly add label for runtime label checking?
        write!(f, "n_from_id({})", self.id)
    }
}

pub struct NFromType {
    pub label: GenRef<String>,
}
impl Display for NFromType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "n_from_type({})", self.label)
    }
}

pub struct EFromID {
    pub id: GenRef<String>,
    pub label: GenRef<String>, // possible not needed, do we do runtime label checking?
}
impl Display for EFromID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "e_from_id({})", self.id)
    }
}

pub struct EFromType {
    pub label: GenRef<String>,
}
impl Display for EFromType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "e_from_type({})", self.label)
    }
}

impl Display for SourceStep {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SourceStep::Variable(v) => write!(f, "{}", v),
            SourceStep::AddN(add_n) => write!(f, "{}", add_n),
            SourceStep::AddE(add_e) => write!(f, "{}", add_e),
            SourceStep::AddV(add_v) => write!(f, "{}", add_v),
            SourceStep::SearchV(search_v) => write!(f, "{}", search_v),
            SourceStep::NFromID(n_from_id) => write!(f, "{}", n_from_id),
            SourceStep::NFromType(n_from_type) => write!(f, "{}", n_from_type),
            SourceStep::EFromID(e_from_id) => write!(f, "{}", e_from_id),
            SourceStep::EFromType(e_from_type) => write!(f, "{}", e_from_type),
            SourceStep::Empty => panic!("Should not be empty"),
        }
    }
}
