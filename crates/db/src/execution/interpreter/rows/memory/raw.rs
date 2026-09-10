//! Raw read reservations follow the shared byte owner through clones and slices.
use super::{Budget, Reservation, Result};
use bytes::Bytes;

struct ReadOwner {
    bytes: Bytes,
    _reservation: Reservation,
}

impl AsRef<[u8]> for ReadOwner {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl Budget {
    /// Retain a storage-returned value without copying its contents. Admission
    /// occurs before decoding or retaining it in an interpreter read result.
    /// The storage engine's shared caches and internal I/O buffers are separate
    /// from these request-owned references.
    pub(in crate::execution::interpreter) fn retain_read(&self, bytes: Bytes) -> Result<Bytes> {
        // Besides the owner/refcount, reserve room for byte handles in a growing
        // read-result vector. Sharing or slicing the admitted Bytes keeps the
        // entire original read charged until its final reference is released.
        let reservation = self.reserve(
            bytes
                .len()
                .saturating_add(size_of::<ReadOwner>())
                .saturating_add(size_of::<std::sync::atomic::AtomicUsize>())
                .saturating_add(2 * size_of::<Bytes>()),
        )?;
        Ok(Bytes::from_owner(ReadOwner {
            bytes,
            _reservation: reservation,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_ownership_charges_once_and_survives_clones_and_slices() {
        let budget = Budget::new(1024);
        let bytes = Bytes::from(vec![7_u8; 256]);
        let pointer = bytes.as_ptr();
        let admitted = budget.retain_read(bytes).unwrap();
        assert_eq!(
            admitted.as_ptr(),
            pointer,
            "read admission must not copy data"
        );
        let available = budget.available();
        assert!(available < 1024 - 256);
        let cloned = admitted.clone();
        let sliced = admitted.slice(1..2);
        assert_eq!(budget.available(), available);
        drop(admitted);
        drop(cloned);
        assert_eq!(budget.available(), available);
        assert_eq!(sliced.as_ref(), &[7]);
        drop(sliced);
        assert_eq!(budget.available(), 1024);

        let retained = budget.retain_read(Bytes::from(vec![0; 512])).unwrap();
        let available = budget.available();
        assert!(budget.retain_read(Bytes::from(vec![0; 512])).is_err());
        assert_eq!(
            budget.available(),
            available,
            "failed admission releases nothing it did not own"
        );
        drop(retained);
        assert_eq!(budget.available(), 1024);
        let empty = budget.retain_read(Bytes::new()).unwrap();
        assert!(empty.is_empty());
        assert!(budget.available() < 1024);
        drop(empty);
        assert_eq!(budget.available(), 1024);
    }
}
