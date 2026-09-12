//! Owned selected properties retain their memory admission through inspection.
use super::{Budget, Reservation, Result};
use crate::encoding::v2::values::property::{self, prepared};

#[cfg(test)]
mod tests;

pub(crate) struct Decoded {
    properties: Vec<property::Property>,
    _memory: Option<Reservation>,
}
impl Decoded {
    /// Move fields while retaining their original admission through the entire
    /// callback. The caller admits transferred output before the callback ends;
    /// errors and unwinding drop every remaining field and the original guard.
    pub(crate) fn with_owned<T>(
        mut self,
        convert: impl FnOnce(std::vec::Drain<'_, property::Property>) -> T,
    ) -> T {
        convert(self.properties.drain(..))
    }

    pub(crate) fn new(
        data: &[u8],
        selection: prepared::Selection<'_>,
        budget: Option<&Budget>,
    ) -> Result<Self> {
        let Some(budget) = budget else {
            // Keep the legacy native decoder and its stored-value behavior.
            // Retaining only selected fields does not allocate a second vector.
            let mut properties = property::decode_properties(data)?;
            properties.retain(|property| selection.contains(&property.name));
            return Ok(Self {
                properties,
                _memory: None,
            });
        };
        let _alignment = budget.reserve(data.len())?;
        let archive = prepared::Archive::new(data);
        let prepared = archive.prepare(selection)?;
        let memory = budget.reserve(prepared.owned_bytes())?;
        let properties = prepared.decode()?;
        Ok(Self {
            properties,
            _memory: Some(memory),
        })
    }
}
impl std::ops::Deref for Decoded {
    type Target = [property::Property];
    fn deref(&self) -> &Self::Target {
        &self.properties
    }
}
