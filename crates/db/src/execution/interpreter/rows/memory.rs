//! Row-buffer ownership layered over the common request resource budget.
use super::Result;
pub(in crate::execution::interpreter) use crate::query_resources::{Budget, Reservation};

/// A relation and its admission reservation are moved together. Consumers may
/// mutate rows after admitting additional payload, and call `refresh` to release
/// a conservative bound once construction has finished.
pub(super) struct Rows {
    pub data: Vec<super::r::Row>,
    pub(super) reservation: Reservation,
}
impl Rows {
    /// Borrow batches without copying rows. The parent relation retains its
    /// reservation for the entire lifetime of every returned view.
    pub fn batches(&self, count: usize) -> impl Iterator<Item = Batch<'_>> {
        self.data
            .chunks(count)
            .map(|rows| Batch(BatchStorage::Borrowed(rows)))
    }

    /// Transfer a complete construction reservation to its final row owner.
    /// Both payload and vector capacity must already fit the incoming bound;
    /// this constructor can release admission but cannot acquire more of it.
    pub fn from_admitted(data: Vec<super::r::Row>, mut reservation: Reservation) -> Self {
        reservation.shrink_to(
            super::rows_bytes(&data).saturating_add(
                data.capacity()
                    .saturating_sub(data.len())
                    .saturating_mul(size_of::<super::r::Row>()),
            ),
        );
        Self { data, reservation }
    }
    pub fn new(data: Vec<super::r::Row>, budget: &Budget) -> Result<Self> {
        let mut rows = Self {
            data,
            reservation: budget.reserve(0)?,
        };
        rows.refresh()?;
        Ok(rows)
    }
    /// Discard consumed row payloads while preserving candidate indices. A
    /// continuation must never read these positions again. Visiting only newly
    /// consumed rows avoids repeatedly recounting wide remaining parent batches.
    pub fn release_rows(&mut self, range: std::ops::Range<usize>) {
        let bytes = self.data[range]
            .iter_mut()
            .map(std::mem::take)
            .fold(0_usize, |bytes, row| {
                bytes
                    .checked_add(super::row_bytes(&row) - size_of::<super::r::Row>())
                    .expect("admitted row payload fits its reservation")
            });
        self.reservation.release(bytes);
    }
    pub fn refresh(&mut self) -> Result<()> {
        self.admit_payload(0)
    }
    /// Admit additional nested payload without reallocating the relation. The
    /// caller must keep all construction within this bound until `refresh`.
    pub fn admit_payload(&mut self, additional: usize) -> Result<()> {
        self.reservation
            .resize(
                super::rows_bytes(&self.data)
                    .saturating_add(additional)
                    .saturating_add(
                        self.data
                            .capacity()
                            .saturating_sub(self.data.len())
                            .saturating_mul(size_of::<super::r::Row>()),
                    ),
            )
            .map_err(Into::into)
    }
}
impl std::ops::Deref for Rows {
    type Target = Vec<super::r::Row>;
    fn deref(&self) -> &Self::Target {
        &self.data
    }
}
impl std::ops::DerefMut for Rows {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

/// A batch can only be built from an admitted owner or a view borrowed from
/// that owner. A bare Vec or slice cannot manufacture an admitted batch.
pub(super) struct Batch<'a>(BatchStorage<'a>);
enum BatchStorage<'a> {
    Owned(Rows),
    Borrowed(&'a [super::r::Row]),
}
impl From<Rows> for Batch<'_> {
    fn from(rows: Rows) -> Self {
        Self(BatchStorage::Owned(rows))
    }
}
impl AsRef<[super::r::Row]> for Batch<'_> {
    fn as_ref(&self) -> &[super::r::Row] {
        match &self.0 {
            BatchStorage::Owned(rows) => &rows.data,
            BatchStorage::Borrowed(rows) => rows,
        }
    }
}

pub(super) struct IntoRows {
    iter: std::vec::IntoIter<super::r::Row>,
    _reservation: Reservation,
}
impl IntoRows {
    /// Borrow the remaining admitted input for property hydration before moving
    /// individual rows into an expansion cursor.
    pub fn as_slice(&self) -> &[super::r::Row] {
        self.iter.as_slice()
    }
}
impl Iterator for IntoRows {
    type Item = super::r::Row;
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }
}
impl IntoIterator for Rows {
    type Item = super::r::Row;
    type IntoIter = IntoRows;
    fn into_iter(self) -> IntoRows {
        IntoRows {
            iter: self.data.into_iter(),
            _reservation: self.reservation,
        }
    }
}
impl<'a> IntoIterator for &'a Rows {
    type Item = &'a super::r::Row;
    type IntoIter = std::slice::Iter<'a, super::r::Row>;
    fn into_iter(self) -> Self::IntoIter {
        self.data.iter()
    }
}
impl<'a> IntoIterator for &'a mut Rows {
    type Item = &'a mut super::r::Row;
    type IntoIter = std::slice::IterMut<'a, super::r::Row>;
    fn into_iter(self) -> Self::IntoIter {
        self.data.iter_mut()
    }
}
