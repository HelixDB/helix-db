use core::fmt;
use std::fmt::Display;

use super::{traversal_steps::Traversal, utils::GenRef};

/// This is for creating a new field where the result is a traversal
#[derive(Clone)]
pub struct TraversalRemapping {
    pub variable_name: String,
    pub new_field: String,
    pub new_value: Traversal,
}
impl Display for TraversalRemapping {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "traversal_remapping!(remapping_vals, {}.clone(), \"{}\" => {})",
            self.variable_name, self.new_field, self.new_value
        )
    }
}

/// This is used for renaming fields
#[derive(Clone)]
pub struct FieldRemapping {
    pub variable_name: String,
    pub new_name: String,
    pub field_name: String,
}
impl Display for FieldRemapping {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "field_remapping!(remapping_vals, {}.clone(), \"{}\" => \"{}\")",
            self.variable_name, self.field_name, self.new_name
        )
    }
}

/// This is used for excluding fields
#[derive(Clone)]
pub struct ExcludeField {
    pub fields_to_exclude: Vec<GenRef<String>>,
}
impl Display for ExcludeField {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "exclude_fields!(remapping_vals, {})",
            self.fields_to_exclude
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

#[derive(Clone)]
pub struct ClosureFieldRemapping {
    pub variable_name: String,
    pub parent_variable_name: String,
    pub remapping: Remapping,
}
impl Display for ClosureFieldRemapping {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // assert that closure mappings are not excluded fields?
        write!(
            f,
            "let {} = {};",
            self.variable_name, self.parent_variable_name
        )?;
        write!(f, "{}", self.remapping)
    }
}

/// This is used for creating a new field where the result is either another value or another object
#[derive(Clone)]
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

#[derive(Clone)]
pub struct ValueRemapping {
    pub variable_name: String,
    pub field_name: String,
    pub value: GenRef<String>,
}
impl Display for ValueRemapping {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "value_remapping!(remapping_vals, {}.clone(), \"{}\" => {})",
            self.variable_name, self.field_name, self.value
        )
    }
}

#[derive(Clone)]
pub struct IdentifierRemapping {
    pub variable_name: String,
    pub field_name: String,
    pub identifier_value: String,
}
impl Display for IdentifierRemapping {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "identifier_remapping!(remapping_vals, {}.clone(), \"{}\" => \"{}\")",
            self.variable_name, self.field_name, self.identifier_value
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

#[derive(Clone)]
pub struct Remapping {
    pub is_inner: bool,
    pub should_spread: bool,
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
                "map_traversal(|{}, txn| {{ {}?;\n Ok({}) }})",
                self.variable_name,
                self.remappings
                    .iter()
                    .map(|remapping| format!("{}", remapping))
                    .collect::<Vec<String>>()
                    .join("?;\n"),
                self.variable_name
            ),
        }
    }
}

// #[derive(Clone)]
// pub struct ClosureRemapping {
//     pub is_inner: bool,
//     pub should_spread: bool,
//     pub variable_name: String,
//     pub remappings: Vec<RemappingType>,
// }
// impl Display for ClosureRemapping {
//     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
//         write!(f, "{}", self.remapping)
//     }
// }
// if there is only one field then it is a property access
// if more than one field then iterate over the fields
// for each field, if the field value is an identifier then it is is a field remapping
// if the field value is a traversal then it is a TraversalRemapping
// if the field value is another object or closure then recurse (sub mapping would go where traversal would go)

#[derive(Clone)]
pub enum RemappingType {
    ObjectRemapping(ObjectRemapping),
    FieldRemapping(FieldRemapping),
    ClosureFieldRemapping(ClosureFieldRemapping),
    ExcludeField(ExcludeField),
    TraversalRemapping(TraversalRemapping),
    ValueRemapping(ValueRemapping),
    IdentifierRemapping(IdentifierRemapping),
    Spread,
    Empty,
}
impl Display for RemappingType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RemappingType::ObjectRemapping(r) => write!(f, "{}", r),
            RemappingType::FieldRemapping(r) => write!(f, "{}", r),
            RemappingType::ClosureFieldRemapping(r) => write!(f, "{}", r),
            RemappingType::ExcludeField(r) => write!(f, "{}", r),
            RemappingType::TraversalRemapping(r) => write!(f, "{}", r),
            RemappingType::ValueRemapping(r) => write!(f, "{}", r),
            RemappingType::IdentifierRemapping(r) => write!(f, "{}", r),
            RemappingType::Spread => write!(f, ""),
            RemappingType::Empty => write!(f, ""),
        }
    }
}
