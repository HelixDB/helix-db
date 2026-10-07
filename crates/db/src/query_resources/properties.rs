//! Owned selected properties retain their memory admission through inspection.
use super::{Budget, Reservation, Result};
use crate::encoding::v2::values::property::{self, prepared};
mod read;
mod write;
pub(crate) use read::{Read, ReadRequest};
pub(crate) use write::Encoded;
mod builder;
pub(crate) use builder::Builder;

#[cfg(test)]
mod tests;

pub(crate) struct Decoded {
    properties: Vec<property::Property>,
    _memory: Option<Reservation>,
}
impl Decoded {
    /// Encoding inherits the decoded owner's budget; callers cannot substitute
    /// a different request or turn admission off when transferring a row.
    pub(crate) fn budget(&self) -> Option<&Budget> {
        self._memory.as_ref().map(|memory| &memory.budget)
    }
    /// Retain an already-owned write input. Its producer admits construction;
    /// this guard takes over before the producer releases its reservation.
    pub(crate) fn owned(properties: Vec<property::Property>, budget: &Budget) -> Result<Self> {
        let prepared = property::write::Prepared::new(&properties)?;
        let memory = budget.reserve(prepared.retained_bytes(properties.capacity()))?;
        Ok(Self {
            properties,
            _memory: Some(memory),
        })
    }

    /// Admit a complete clone and optional insertion before allocating it.
    /// The callback only replaces/removes entries or fills the admitted spare
    /// slot. Incoming property payloads remain charged by their producer.
    pub(crate) fn rewritten(
        properties: &[property::Property],
        insert: bool,
        extra_payload: usize,
        budget: &Budget,
        edit: impl FnOnce(&mut Vec<property::Property>),
    ) -> Result<Self> {
        let prepared = property::write::Prepared::new(properties)?;
        let memory = budget.reserve(
            prepared
                .clone_bytes()
                .saturating_add(usize::from(insert) * size_of::<property::Property>())
                .saturating_add(extra_payload),
        )?;
        let mut output = Vec::with_capacity(properties.len() + usize::from(insert));
        output.extend_from_slice(properties);
        edit(&mut output);
        Ok(Self {
            properties: output,
            _memory: Some(memory),
        })
    }
    /// Native callers without query admission retain their existing owned-input
    /// behavior. Admitted writes use `owned` or `rewritten` instead.
    pub(crate) fn native(properties: Vec<property::Property>) -> Self {
        Self {
            properties,
            _memory: None,
        }
    }

    /// Share a decoded snapshot without cloning its fields. The Arc allocation
    /// and decoded payload remain charged together until the last clone drops.
    pub(crate) fn share(mut self) -> Result<std::sync::Arc<Self>> {
        self._memory
            .as_mut()
            .map(|memory| {
                // ArcInner contains two atomic counters followed by this owner.
                // Both have usize alignment; the independent allocator test checks
                // the complete allocation rather than relying only on this bound.
                memory.resize(
                    memory
                        .bytes
                        .saturating_add(size_of::<Self>() + 2 * size_of::<usize>()),
                )
            })
            .transpose()?;
        Ok(std::sync::Arc::new(self))
    }

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
impl std::fmt::Debug for Decoded {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.properties.fmt(formatter)
    }
}
impl PartialEq for Decoded {
    fn eq(&self, other: &Self) -> bool {
        self.properties == other.properties
    }
}
impl std::ops::Deref for Decoded {
    type Target = [property::Property];
    fn deref(&self) -> &Self::Target {
        &self.properties
    }
}
