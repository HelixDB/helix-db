use helix_ast::expr::Expr;
use helix_ast::value::{PropertyInput, PropertyValue};
use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::catalog;

use super::{
    SearchQueryExprPlan, SearchQueryExprPlanError, SearchQueryInputExpected,
    SearchQueryInputPlanError,
};
use crate::ir::AtLeast;

/// Invalid literal vector-search query payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchVectorError {
    /// Literal vector had no dimensions.
    Empty,
    /// Literal vector contained a non-finite component.
    NonFiniteComponent,
}

/// Finite `f32` component inside a vector-search query.
///
/// # Examples
///
/// ```
/// use helix_planner::ir::SearchVectorComponent;
///
/// assert!(SearchVectorComponent::new(0.25).is_some());
/// assert!(SearchVectorComponent::new(f32::INFINITY).is_none());
/// ```
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SearchVectorComponent {
    value: f32,
}

impl SearchVectorComponent {
    /// Build a vector component, returning `None` for non-finite values.
    pub fn new(value: f32) -> Option<Self> {
        value.is_finite().then_some(Self { value })
    }

    /// Return the finite component value.
    ///
    /// ```
    /// use helix_planner::ir::SearchVectorComponent;
    ///
    /// assert_eq!(SearchVectorComponent::new(0.25).unwrap().get(), 0.25);
    /// ```
    pub fn get(self) -> f32 {
        self.value
    }
}

/// Non-empty literal vector-search query with finite components.
///
/// ```
/// use helix_planner::ir::{SearchVector, SearchVectorComponent, SearchVectorError};
///
/// let vector = SearchVector::new(vec![0.1, 0.2]).unwrap();
/// assert_eq!(serde_json::to_string(&vector).unwrap(), "[0.1,0.2]");
/// assert_eq!(vector.as_ref()[0], SearchVectorComponent::new(0.1).unwrap());
/// assert_eq!(SearchVector::new(Vec::new()), Err(SearchVectorError::Empty));
/// assert_eq!(
///     SearchVector::new(vec![f32::INFINITY]),
///     Err(SearchVectorError::NonFiniteComponent)
/// );
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct SearchVector {
    values: AtLeast<SearchVectorComponent, 1>,
}

impl SearchVector {
    /// Build a literal vector-search query, rejecting empty vectors and non-finite components.
    pub fn new(values: Vec<f32>) -> Result<Self, SearchVectorError> {
        let values = values
            .into_iter()
            .map(|value| {
                SearchVectorComponent::new(value).ok_or(SearchVectorError::NonFiniteComponent)
            })
            .collect::<Result<Vec<_>, _>>()?;
        AtLeast::<_, 1>::try_from_vec(values)
            .map(|values| Self { values })
            .ok_or(SearchVectorError::Empty)
    }
}

impl AsRef<[SearchVectorComponent]> for SearchVector {
    fn as_ref(&self) -> &[SearchVectorComponent] {
        self.values.as_ref()
    }
}

impl Serialize for SearchVector {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.values
            .iter()
            .map(|value| value.value)
            .collect::<Vec<_>>()
            .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for SearchVector {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let values = Vec::<f32>::deserialize(deserializer)?;
        Self::new(values).map_err(|_| D::Error::custom("expected non-empty finite f32 array"))
    }
}

/// Vector-search query input with literal payloads restricted to valid vectors.
///
/// Runtime expression inputs remain valid here because the planner cannot know
/// their value types before execution. Static constant expressions are
/// normalized through the literal arm so they cannot bypass this contract.
///
/// ```
/// use helix_ast::expr::Expr;
/// use helix_ast::value::{PropertyInput, PropertyValue};
/// use helix_planner::catalog::SearchIndexKind;
/// use helix_planner::ir::{
///     SearchQueryInputExpected, SearchQueryInputPlanError, VectorQueryInputPlan,
/// };
///
/// assert!(matches!(
///     VectorQueryInputPlan::new(PropertyInput::from(PropertyValue::F32Array(vec![0.1_f32]))),
///     Ok(VectorQueryInputPlan::Vector(values)) if serde_json::to_string(&values).unwrap() == "[0.1]"
/// ));
/// assert!(matches!(
///     VectorQueryInputPlan::new(PropertyInput::from(Expr::val(vec![0.1_f32]))),
///     Ok(VectorQueryInputPlan::Vector(values)) if serde_json::to_string(&values).unwrap() == "[0.1]"
/// ));
/// assert!(matches!(
///     VectorQueryInputPlan::new(PropertyInput::from("not a vector")),
///     Err(SearchQueryInputPlanError::InvalidLiteral {
///         kind: SearchIndexKind::Vector,
///         expected: SearchQueryInputExpected::NonEmptyFiniteF32Array,
///     })
/// ));
/// ```
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum VectorQueryInputPlan {
    /// Literal query vector.
    Vector(SearchVector),
    /// Runtime query-vector expression.
    Expr(SearchQueryExprPlan),
}

impl VectorQueryInputPlan {
    /// Build a vector-query input plan after validating embedded expressions.
    pub fn new(input: PropertyInput) -> Result<Self, SearchQueryInputPlanError> {
        match input {
            PropertyInput::Value(PropertyValue::F32Array(values))
            | PropertyInput::Expr(Expr::Constant(PropertyValue::F32Array(values))) => {
                SearchVector::new(values).map(Self::Vector).map_err(|_| {
                    SearchQueryInputPlanError::InvalidLiteral {
                        kind: catalog::SearchIndexKind::Vector,
                        expected: SearchQueryInputExpected::NonEmptyFiniteF32Array,
                    }
                })
            }
            PropertyInput::Value(_) => Err(SearchQueryInputPlanError::InvalidLiteral {
                kind: catalog::SearchIndexKind::Vector,
                expected: SearchQueryInputExpected::NonEmptyFiniteF32Array,
            }),
            PropertyInput::Expr(expr) => {
                SearchQueryExprPlan::new(expr)
                    .map(Self::Expr)
                    .map_err(|err| match err {
                        SearchQueryExprPlanError::StaticLiteral => {
                            SearchQueryInputPlanError::InvalidLiteral {
                                kind: catalog::SearchIndexKind::Vector,
                                expected: SearchQueryInputExpected::NonEmptyFiniteF32Array,
                            }
                        }
                        SearchQueryExprPlanError::Expression(err) => {
                            SearchQueryInputPlanError::Expression(err)
                        }
                    })
            }
        }
    }
}

impl<'de> Deserialize<'de> for VectorQueryInputPlan {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(rename_all = "snake_case")]
        enum Raw {
            Vector(SearchVector),
            Expr(Expr),
        }

        match Raw::deserialize(deserializer)? {
            Raw::Vector(vector) => Ok(Self::Vector(vector)),
            Raw::Expr(expr) => Self::new(PropertyInput::Expr(expr)).map_err(|err| match err {
                SearchQueryInputPlanError::InvalidLiteral { kind, expected } => {
                    D::Error::custom(format!("{kind} search query input must be {expected}"))
                }
                SearchQueryInputPlanError::Expression(err) => D::Error::custom(err),
            }),
        }
    }
}
