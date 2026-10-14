//! Fixed-capacity property output: every retained or cloned payload is admitted
//! before insertion. Removed values never need an output allocation.
use super::{property, Budget, Decoded, Reservation, Result};

pub(crate) struct Builder {
    properties: Vec<property::Property>,
    memory: Option<Reservation>,
}
impl Builder {
    pub(crate) fn new(capacity: usize, budget: Option<&Budget>) -> Result<Self> {
        let memory = budget
            .map(|budget| budget.reserve(capacity.saturating_mul(size_of::<property::Property>())))
            .transpose()?;
        Ok(Self {
            properties: Vec::with_capacity(capacity),
            memory,
        })
    }

    pub(crate) fn push_cloned(&mut self, property: &property::Property) -> Result<()> {
        assert!(
            self.properties.len() < self.properties.capacity(),
            "prepared property output cannot grow"
        );
        self.memory
            .as_mut()
            .map(|memory| -> Result<()> {
                let prepared = property::write::Prepared::new(std::slice::from_ref(property))?;
                memory.resize(
                    memory.bytes.saturating_add(
                        prepared
                            .clone_bytes()
                            .saturating_sub(size_of::<property::Property>()),
                    ),
                )?;
                Ok(())
            })
            .transpose()?;
        self.properties.push(property.clone());
        Ok(())
    }

    pub(crate) fn push_owned(&mut self, property: property::Property) -> Result<()> {
        assert!(
            self.properties.len() < self.properties.capacity(),
            "prepared property output cannot grow"
        );
        self.memory
            .as_mut()
            .map(|memory| -> Result<()> {
                let prepared = property::write::Prepared::new(std::slice::from_ref(&property))?;
                memory.resize(
                    memory.bytes.saturating_add(
                        prepared
                            .retained_bytes(1)
                            .saturating_sub(size_of::<property::Property>()),
                    ),
                )?;
                Ok(())
            })
            .transpose()?;
        self.properties.push(property);
        Ok(())
    }

    pub(crate) fn finish(self) -> Decoded {
        Decoded {
            properties: self.properties,
            _memory: self.memory,
        }
    }
}
