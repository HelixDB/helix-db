use std::collections::BTreeSet;
use std::num::NonZeroUsize;

use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::{AtLeast, NonEmptyString};

/// Planned operation kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlanKind {
    /// Read transaction.
    Read,
    /// Write transaction.
    Write,
}

/// Variables returned by a planned batch.
///
/// Empty return lists are represented explicitly instead of using vector
/// cardinality as a hidden mode.
///
/// ```
/// use helix_planner::ir::{AtLeast, NonEmptyString, ReturnPlan, ReturnVariables};
///
/// let users = NonEmptyString::new("users").unwrap();
/// let variables = ReturnVariables::new(AtLeast::<_, 1>::from_one_and_rest(users, Vec::new())).unwrap();
/// let returns = ReturnPlan::Variables(variables);
///
/// assert!(matches!(ReturnPlan::None, ReturnPlan::None));
/// assert!(matches!(returns, ReturnPlan::Variables(_)));
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReturnPlan {
    /// Return no variables.
    None,
    /// Return one or more named variables.
    Variables(ReturnVariables),
}

/// Invalid return-variable payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReturnVariablesError {
    /// More than one return references the same variable.
    DuplicateName {
        /// Duplicate return variable.
        name: NonEmptyString,
    },
}

impl std::fmt::Display for ReturnVariablesError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DuplicateName { name } => write!(f, "duplicate return variable `{name}`"),
        }
    }
}

/// Non-empty return variables with unique names.
///
/// ```
/// use helix_planner::ir::{AtLeast, NonEmptyString, ReturnVariables, ReturnVariablesError};
///
/// let users = NonEmptyString::new("users").unwrap();
/// assert!(ReturnVariables::new(AtLeast::<_, 1>::from_one_and_rest(users.clone(), Vec::new())).is_ok());
///
/// let duplicate = AtLeast::<_, 1>::from_one_and_rest(users.clone(), vec![users]);
/// assert!(matches!(
///     ReturnVariables::new(duplicate),
///     Err(ReturnVariablesError::DuplicateName { .. })
/// ));
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReturnVariables {
    names: AtLeast<NonEmptyString, 1>,
}

impl ReturnVariables {
    /// Build a return-variable list, returning an error for duplicate names.
    pub fn new(names: AtLeast<NonEmptyString, 1>) -> Result<Self, ReturnVariablesError> {
        let mut seen = BTreeSet::new();
        for name in &names {
            if !seen.insert(name.clone()) {
                return Err(ReturnVariablesError::DuplicateName { name: name.clone() });
            }
        }
        Ok(Self { names })
    }
}

impl AsRef<[NonEmptyString]> for ReturnVariables {
    fn as_ref(&self) -> &[NonEmptyString] {
        self.names.as_ref()
    }
}

impl Serialize for ReturnVariables {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.names.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ReturnVariables {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let names = AtLeast::<NonEmptyString, 1>::deserialize(deserializer)?;
        Self::new(names).map_err(|err| match err {
            ReturnVariablesError::DuplicateName { name } => {
                D::Error::custom(format!("duplicate return variable `{name}`"))
            }
        })
    }
}

/// Output binding behavior for a batch run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BatchOutputPlan {
    /// Do not bind the run output to a variable.
    Discard,
    /// Bind the run output to a variable.
    Bind(NonEmptyString),
}

/// Condition mode for a batch run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunConditionPlan<C> {
    /// Always execute the run.
    Always,
    /// Execute the run only when this condition is true.
    If(C),
}

/// Initial batch condition that depends only on named variables.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BatchVariableConditionPlan {
    /// Variable is not empty.
    VarNotEmpty(NonEmptyString),
    /// Variable is empty.
    VarEmpty(NonEmptyString),
    /// Variable has at least this size.
    VarMinSize(NonEmptyString, NonZeroUsize),
}

/// Follow-up batch condition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BatchConditionPlan {
    /// Variable is not empty.
    VarNotEmpty(NonEmptyString),
    /// Variable is empty.
    VarEmpty(NonEmptyString),
    /// Variable has at least this size.
    VarMinSize(NonEmptyString, NonZeroUsize),
    /// Previous query result was not empty.
    PrevNotEmpty,
}

impl From<BatchVariableConditionPlan> for BatchConditionPlan {
    fn from(condition: BatchVariableConditionPlan) -> Self {
        match condition {
            BatchVariableConditionPlan::VarNotEmpty(variable) => Self::VarNotEmpty(variable),
            BatchVariableConditionPlan::VarEmpty(variable) => Self::VarEmpty(variable),
            BatchVariableConditionPlan::VarMinSize(variable, size) => {
                Self::VarMinSize(variable, size)
            }
        }
    }
}
