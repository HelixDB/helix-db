use core::fmt;
use std::fmt::Display;

use super::{traversal_steps::Traversal, types::GenRef};

/// This is for creating a new field where the result is a traversal
pub struct TraversalRemapping {
    pub variable_name: String,
    pub new_field: String,
    pub new_value: Traversal,
}
impl Display for TraversalRemapping {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "|{}, txn| {{ traversal_remapping!({}, {} => {}) }}",
            self.variable_name, self.variable_name, self.new_field, self.new_value
        )
    }
}

/// This is used for renaming fields
pub struct FieldRemapping {
    pub variable_name: String,
    pub new_name: String,
    pub old_name: String,
    pub should_spread: bool,
}
impl Display for FieldRemapping {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "field_remapping!({}, {} => {})",
            self.variable_name, self.old_name, self.new_name
        )
    }
}

/// This is used for excluding fields
pub struct ExcludeField {
    pub fields_to_exclude: Vec<String>,
}
impl Display for ExcludeField {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "exclude_fields!({})", self.fields_to_exclude.join(", "))
    }
}

pub struct ClosureFieldRemapping {
    pub variable_name: String,
    pub remapping: Remapping,
}
impl Display for ClosureFieldRemapping {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // assert that closure mappings are not excluded fields?
        write!(f, "{}", self.remapping)
    }
}

/// This is used for creating a new field where the result is either another value or another object
pub struct ObjectRemapping {
    pub variable_name: String,
    pub field_name: String,
    pub remapping: Remapping,
}
impl Display for ObjectRemapping {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // CHECK: do we just let it cascade to terminal value or do we need to handle here?
        write!(f, "{}", self.remapping)
    }
}

pub struct ValueRemapping {
    pub variable_name: String,
    pub field_name: String,
    pub value: GenRef<String>,
}
impl Display for ValueRemapping {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "value_remapping!({}, {}: {})",
            self.variable_name, self.field_name, self.value
        )
    }
}

// pub enum RemappingValue {
//     Remapping(Remapping),
//     String(GenRef<String>),
// }
// impl Display for RemappingValue {
//     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
//         write!(f, "{}", self)
//     }
// }

// split obj and tr

pub struct Remapping {
    pub is_inner: bool,
    pub variable_name: String,
    pub remappings: Vec<RemappingType>,
}
impl Display for Remapping {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.is_inner {
            true => write!(
                f,
                "map_traversal(|{}, txn| {{ {} }})",
                self.variable_name,
                self.remappings
                    .iter()
                    .map(|remapping| format!("{}", remapping))
                    .collect::<Vec<String>>()
                    .join("?;")
            ),
            false => write!(
                f,
                "map_traversal(|{}, txn| {{ {} }}",
                self.variable_name,
                self.remappings
                    .iter()
                    .map(|remapping| format!("{}", remapping))
                    .collect::<Vec<String>>()
                    .join("?;")
            ),
        }
    }
}

// if there is only one field then it is a property access
// if more than one field then iterate over the fields
// for each field, if the field value is an identifier then it is is a field remapping
// if the field value is a traversal then it is a TraversalRemapping
// if the field value is another object or closure then recurse (sub mapping would go where traversal would go)

pub enum RemappingType {
    ObjectRemapping(ObjectRemapping),
    FieldRemapping(FieldRemapping),
    ClosureFieldRemapping(ClosureFieldRemapping),
    ExcludeField(ExcludeField),
    TraversalRemapping(TraversalRemapping),
}
impl Display for RemappingType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self)
    }
}
