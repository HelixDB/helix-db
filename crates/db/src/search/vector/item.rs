use core::fmt;
use std::borrow::Cow;

use crate::search::vector::{distance::Distance, unaligned_vector::UnalignedVector};

/// An item node which corresponds to the vector inputed
/// by the user and the distance header.
pub struct Item<'a, D: Distance> {
    /// The header of this item.
    pub header: D::Header,
    /// The vector of this item.
    pub vector: Cow<'a, UnalignedVector<D::VectorCodec>>,
}

impl<D: Distance> fmt::Debug for Item<'_, D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Item")
            .field("header", &self.header)
            .field("vector", &self.vector)
            .finish()
    }
}

impl<D: Distance> Clone for Item<'_, D> {
    fn clone(&self) -> Self {
        Self {
            header: self.header,
            vector: self.vector.clone(),
        }
    }
}

impl<D: Distance> Item<'_, D> {
    /// Converts the item into an owned version of itself by cloning
    /// the internal vector. Doing so will make it mutable.
    pub fn into_owned(self) -> Item<'static, D> {
        Item {
            header: self.header,
            vector: Cow::Owned(self.vector.into_owned()),
        }
    }

    /// Builds a new item from a `Vec<f32>`.
    pub fn new(vec: Vec<f32>) -> Self {
        let vector = UnalignedVector::from_vec(vec);
        let header = D::new_header(&vector);
        Self { header, vector }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::search::vector::distance::{Cosine, Distance};

    #[test]
    fn item_new_clone_debug_and_into_owned_preserve_vector_and_header() {
        let item = Item::<Cosine>::new(vec![3.0, 4.0]);
        assert_eq!(item.vector.to_vec(), vec![3.0, 4.0]);
        assert_eq!(Cosine::norm(&item), 5.0);

        let cloned = item.clone();
        assert_eq!(cloned.vector.to_vec(), vec![3.0, 4.0]);
        assert_eq!(Cosine::distance(&item, &cloned), 0.0);
        assert!(format!("{:?}", cloned).contains("Item"));

        let owned = cloned.into_owned();
        assert_eq!(owned.vector.to_vec(), vec![3.0, 4.0]);
        assert_eq!(Cosine::norm(&owned), 5.0);
    }
}
