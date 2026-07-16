//! Original logical-position validation for sorted multi-get keys.

use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize};

use crate::exec::ExecPlanError;
use crate::ir;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub(super) struct OriginalPositions(ir::AtLeast<usize, 1>);

impl OriginalPositions {
    pub(super) fn from_unique(positions: ir::AtLeast<usize, 1>) -> Result<Self, ExecPlanError> {
        let mut sorted = positions.as_ref().to_vec();
        sorted.sort_unstable();
        if let Some(pair) = sorted.windows(2).find(|pair| pair[0] == pair[1]) {
            return Err(ExecPlanError::DuplicateMultiGetOriginalPosition { position: pair[0] });
        }
        Ok(Self(positions))
    }

    pub(super) fn from_unique_unchecked(positions: ir::AtLeast<usize, 1>) -> Self {
        Self(positions)
    }

    pub(super) fn len(&self) -> usize {
        self.0.len()
    }

    pub(super) fn as_ref(&self) -> &[usize] {
        self.0.as_ref()
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = &usize> {
        self.0.iter()
    }
}

impl<'de> Deserialize<'de> for OriginalPositions {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::from_unique(ir::AtLeast::<usize, 1>::deserialize(deserializer)?)
            .map_err(|err| D::Error::custom(err.to_string()))
    }
}
