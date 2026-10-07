//! Raw property observations are admitted as a batch before decoding any row.
use super::{Budget, Decoded, Reservation, Result};
use bytes::Bytes;
use std::sync::Arc;

/// A raw observation retains its storage bytes through clones of the eventual
/// snapshot. The budget is private so decoding cannot silently change ledgers.
pub(crate) struct Read {
    encoded: Bytes,
    budget: Option<Budget>,
}
impl Read {
    /// Endpoint metadata shares the observation batch but uses its own typed
    /// decoder instead of the property decoder.
    pub(crate) fn bytes(&self) -> &[u8] {
        &self.encoded
    }

    pub(crate) fn new(encoded: Bytes, budget: Option<&Budget>) -> Result<Self> {
        Ok(Self {
            encoded: match budget {
                Some(budget) => budget.retain_read(encoded)?,
                None => encoded,
            },
            budget: budget.cloned(),
        })
    }

    pub(crate) fn decode(self) -> Result<(Bytes, Arc<Decoded>)> {
        let properties = Decoded::new(
            &self.encoded,
            super::prepared::Selection::All,
            self.budget.as_ref(),
        )?
        .share()?;
        Ok((self.encoded, properties))
    }
}

/// The result-handle allocation is admitted before a backend multi-get starts.
/// Attaching its result admits every present raw row before exposing an iterator.
pub(crate) struct ReadRequest<'a> {
    count: usize,
    budget: Option<&'a Budget>,
    memory: Option<Reservation>,
}
impl<'a> ReadRequest<'a> {
    pub(crate) fn new(count: usize, budget: Option<&'a Budget>) -> Result<Self> {
        let memory = budget
            .map(|budget| {
                budget.reserve(
                    count
                        .saturating_mul(2 * size_of::<Option<Bytes>>() + size_of::<Option<Read>>()),
                )
            })
            .transpose()?;
        Ok(Self {
            count,
            budget,
            memory,
        })
    }

    pub(crate) fn attach(self, values: Vec<Option<Bytes>>) -> Result<Reads> {
        assert_eq!(values.len(), self.count, "one result per observed key");
        let Some(budget) = self.budget else {
            return Ok(Reads {
                values: Values::Native(values.into_iter()),
                _memory: self.memory,
            });
        };
        let mut reads = Vec::with_capacity(self.count);
        for value in values {
            reads.push(
                value
                    .map(|encoded| Read::new(encoded, Some(budget)))
                    .transpose()?,
            );
        }
        Ok(Reads {
            values: Values::Admitted(reads.into_iter()),
            _memory: self.memory,
        })
    }
}

/// Unconsumed raw values and handle storage retain ownership across suspension,
/// early exit, decoding errors and cancellation.
pub(crate) struct Reads {
    values: Values,
    _memory: Option<Reservation>,
}
enum Values {
    Native(std::vec::IntoIter<Option<Bytes>>),
    Admitted(std::vec::IntoIter<Option<Read>>),
}
impl Iterator for Reads {
    type Item = Option<Read>;
    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.values {
            Values::Native(values) => values.next().map(|value| {
                value.map(|encoded| Read {
                    encoded,
                    budget: None,
                })
            }),
            Values::Admitted(values) => values.next(),
        }
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        match &self.values {
            Values::Native(values) => values.size_hint(),
            Values::Admitted(values) => values.size_hint(),
        }
    }
}
impl ExactSizeIterator for Reads {}
