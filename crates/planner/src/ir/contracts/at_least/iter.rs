//! Borrowing and consuming collection traits for `AtLeast`.

use super::AtLeast;

impl<T, const MIN: usize> AsRef<[T]> for AtLeast<T, MIN> {
    fn as_ref(&self) -> &[T] {
        &self.items
    }
}

impl<T, const MIN: usize> std::ops::Deref for AtLeast<T, MIN> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        &self.items
    }
}

impl<T, const MIN: usize> IntoIterator for AtLeast<T, MIN> {
    type Item = T;
    type IntoIter = std::vec::IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        self.items.into_iter()
    }
}

impl<'a, T, const MIN: usize> IntoIterator for &'a AtLeast<T, MIN> {
    type Item = &'a T;
    type IntoIter = std::slice::Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.items.iter()
    }
}
