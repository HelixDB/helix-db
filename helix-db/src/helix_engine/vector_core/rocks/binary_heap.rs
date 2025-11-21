use core::mem::{ManuallyDrop, swap};
use core::ptr;
use core::slice;
use std::iter::FusedIterator;
pub struct BinaryHeap<'arena, T> {
    pub arena: &'arena bumpalo::Bump,
    data: bumpalo::collections::Vec<'arena, T>,
}

impl<'arena, T: Ord> BinaryHeap<'arena, T> {
    pub fn new(arena: &'arena bumpalo::Bump) -> BinaryHeap<'arena, T> {
        BinaryHeap {
            arena,
            data: bumpalo::collections::Vec::with_capacity_in(0, arena),
        }
    }

    pub fn with_capacity(arena: &'arena bumpalo::Bump, capacity: usize) -> BinaryHeap<'arena, T> {
        BinaryHeap {
            arena,
            data: bumpalo::collections::Vec::with_capacity_in(capacity, arena),
        }
    }

    #[inline]
    pub fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I) {
        let guard = RebuildOnDrop {
            rebuild_from: self.len(),
            heap: self,
        };
        guard.heap.data.extend(iter);
    }

    pub fn pop(&mut self) -> Option<T> {
        self.data.pop().map(|mut item| {
            if !self.is_empty() {
                swap(&mut item, &mut self.data[0]);
                // SAFETY: !self.is_empty() means that self.len() > 0
                unsafe { self.sift_down_to_bottom(0) };
            }
            item
        })
    }

    #[must_use]
    pub fn peek(&self) -> Option<&T> {
        self.data.first()
    }

    pub fn from(
        arena: &'arena bumpalo::Bump,
        data: bumpalo::collections::Vec<'arena, T>,
    ) -> BinaryHeap<'arena, T> {
        BinaryHeap { arena, data }
    }

    pub fn push(&mut self, item: T) {
        let old_len = self.len();
        self.data.push(item);
        // SAFETY: Since we pushed a new item it means that
        //  old_len = self.len() - 1 < self.len()
        unsafe { self.sift_up(0, old_len) };
    }

    // The implementations of sift_up and sift_down use unsafe blocks in
    // order to move an element out of the vector (leaving behind a
    // hole), shift along the others and move the removed element back into the
    // vector at the final location of the hole.
    // The `Hole` type is used to represent this, and make sure
    // the hole is filled back at the end of its scope, even on panic.
    // Using a hole reduces the constant factor compared to using swaps,
    // which involves twice as many moves.

    /// # Safety
    ///
    /// The caller must guarantee that `pos < self.len()`.
    ///
    /// Returns the new position of the element.
    unsafe fn sift_up(&mut self, start: usize, pos: usize) -> usize {
        // Take out the value at `pos` and create a hole.
        // SAFETY: The caller guarantees that pos < self.len()
        let mut hole = unsafe { Hole::new(&mut self.data, pos) };

        while hole.pos() > start {
            let parent = (hole.pos() - 1) / 2;

            // SAFETY: hole.pos() > start >= 0, which means hole.pos() > 0
            //  and so hole.pos() - 1 can't underflow.
            //  This guarantees that parent < hole.pos() so
            //  it's a valid index and also != hole.pos().
            if hole.element() <= unsafe { hole.get(parent) } {
                break;
            }

            // SAFETY: Same as above
            unsafe { hole.move_to(parent) };
        }

        hole.pos()
    }

    /// Take an element at `pos` and move it down the heap,
    /// while its children are larger.
    ///
    /// Returns the new position of the element.
    ///
    /// # Safety
    ///
    /// The caller must guarantee that `pos < end <= self.len()`.
    unsafe fn sift_down_range(&mut self, pos: usize, end: usize) -> usize {
        // SAFETY: The caller guarantees that pos < end <= self.len().
        let mut hole = unsafe { Hole::new(&mut self.data, pos) };
        let mut child = 2 * hole.pos() + 1;

        // Loop invariant: child == 2 * hole.pos() + 1.
        while child <= end.saturating_sub(2) {
            // compare with the greater of the two children
            // SAFETY: child < end - 1 < self.len() and
            //  child + 1 < end <= self.len(), so they're valid indexes.
            //  child == 2 * hole.pos() + 1 != hole.pos() and
            //  child + 1 == 2 * hole.pos() + 2 != hole.pos().
            // FIXME: 2 * hole.pos() + 1 or 2 * hole.pos() + 2 could overflow
            //  if T is a ZST
            child += unsafe { hole.get(child) <= hole.get(child + 1) } as usize;

            // if we are already in order, stop.
            // SAFETY: child is now either the old child or the old child+1
            //  We already proven that both are < self.len() and != hole.pos()
            if hole.element() >= unsafe { hole.get(child) } {
                return hole.pos();
            }

            // SAFETY: same as above.
            unsafe { hole.move_to(child) };
            child = 2 * hole.pos() + 1;
        }

        // SAFETY: && short circuit, which means that in the
        //  second condition it's already true that child == end - 1 < self.len().
        if child == end - 1 && hole.element() < unsafe { hole.get(child) } {
            // SAFETY: child is already proven to be a valid index and
            //  child == 2 * hole.pos() + 1 != hole.pos().
            unsafe { hole.move_to(child) };
        }

        hole.pos()
    }

    /// # Safety
    ///
    /// The caller must guarantee that `pos < self.len()`.
    unsafe fn sift_down(&mut self, pos: usize) -> usize {
        let len = self.len();
        // SAFETY: pos < len is guaranteed by the caller and
        //  obviously len = self.len() <= self.len().
        unsafe { self.sift_down_range(pos, len) }
    }

    /// Take an element at `pos` and move it all the way down the heap,
    /// then sift it up to its position.
    ///
    /// Note: This is faster when the element is known to be large / should
    /// be closer to the bottom.
    ///
    /// # Safety
    ///
    /// The caller must guarantee that `pos < self.len()`.
    unsafe fn sift_down_to_bottom(&mut self, mut pos: usize) {
        let end = self.len();
        let start = pos;

        // SAFETY: The caller guarantees that pos < self.len().
        let mut hole = unsafe { Hole::new(&mut self.data, pos) };
        let mut child = 2 * hole.pos() + 1;

        // Loop invariant: child == 2 * hole.pos() + 1.
        while child <= end.saturating_sub(2) {
            // SAFETY: child < end - 1 < self.len() and
            //  child + 1 < end <= self.len(), so they're valid indexes.
            //  child == 2 * hole.pos() + 1 != hole.pos() and
            //  child + 1 == 2 * hole.pos() + 2 != hole.pos().
            // FIXME: 2 * hole.pos() + 1 or 2 * hole.pos() + 2 could overflow
            //  if T is a ZST
            child += unsafe { hole.get(child) <= hole.get(child + 1) } as usize;

            // SAFETY: Same as above
            unsafe { hole.move_to(child) };
            child = 2 * hole.pos() + 1;
        }

        if child == end - 1 {
            // SAFETY: child == end - 1 < self.len(), so it's a valid index
            //  and child == 2 * hole.pos() + 1 != hole.pos().
            unsafe { hole.move_to(child) };
        }
        pos = hole.pos();
        drop(hole);

        // SAFETY: pos is the position in the hole and was already proven
        //  to be a valid index.
        unsafe { self.sift_up(start, pos) };
    }

    /// Rebuild assuming data[0..start] is still a proper heap.
    fn rebuild_tail(&mut self, start: usize) {
        if start == self.len() {
            return;
        }

        let tail_len = self.len() - start;

        #[inline(always)]
        fn log2_fast(x: usize) -> usize {
            (usize::BITS - x.leading_zeros() - 1) as usize
        }

        // `rebuild` takes O(self.len()) operations
        // and about 2 * self.len() comparisons in the worst case
        // while repeating `sift_up` takes O(tail_len * log(start)) operations
        // and about 1 * tail_len * log_2(start) comparisons in the worst case,
        // assuming start >= tail_len. For larger heaps, the crossover point
        // no longer follows this reasoning and was determined empirically.
        let better_to_rebuild = if start < tail_len {
            true
        } else if self.len() <= 2048 {
            2 * self.len() < tail_len * log2_fast(start)
        } else {
            2 * self.len() < tail_len * 11
        };

        if better_to_rebuild {
            self.rebuild();
        } else {
            for i in start..self.len() {
                // SAFETY: The index `i` is always less than self.len().
                unsafe { self.sift_up(0, i) };
            }
        }
    }

    fn rebuild(&mut self) {
        let mut n = self.len() / 2;
        while n > 0 {
            n -= 1;
            // SAFETY: n starts from self.len() / 2 and goes down to 0.
            //  The only case when !(n < self.len()) is if
            //  self.len() == 0, but it's ruled out by the loop condition.
            unsafe { self.sift_down(n) };
        }
    }

    /// Moves all the elements of `other` into `self`, leaving `other` empty.
    ///
    /// # Examples
    ///
    /// Basic usage:
    ///
    /// ```
    /// use std::collections::BinaryHeap;
    ///
    /// let mut a = BinaryHeap::from([-10, 1, 2, 3, 3]);
    /// let mut b = BinaryHeap::from([-20, 5, 43]);
    ///
    /// a.append(&mut b);
    ///
    /// assert_eq!(a.into_sorted_vec(), [-20, -10, 1, 2, 3, 3, 5, 43]);
    /// assert!(b.is_empty());
    /// ```
    pub fn append(&mut self, other: &mut Self) {
        if self.len() < other.len() {
            swap(self, other);
        }

        let start = self.data.len();

        self.data.append(&mut other.data);

        self.rebuild_tail(start);
    }

    /// Returns the length of the binary heap.
    ///
    /// # Examples
    ///
    /// Basic usage:
    ///
    /// ```
    /// use std::collections::BinaryHeap;
    /// let heap = BinaryHeap::from([1, 3]);
    ///
    /// assert_eq!(heap.len(), 2);
    /// ```
    #[must_use]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Checks if the binary heap is empty.
    ///
    /// # Examples
    ///
    /// Basic usage:
    ///
    /// ```
    /// use std::collections::BinaryHeap;
    /// let mut heap = BinaryHeap::new();
    ///
    /// assert!(heap.is_empty());
    ///
    /// heap.push(3);
    /// heap.push(5);
    /// heap.push(1);
    ///
    /// assert!(!heap.is_empty());
    /// ```
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Clears the binary heap, returning an iterator over the removed elements
    /// in arbitrary order. If the iterator is dropped before being fully
    /// consumed, it drops the remaining elements in arbitrary order.
    ///
    /// The returned iterator keeps a mutable borrow on the heap to optimize
    /// its implementation.
    ///
    /// # Examples
    ///
    /// Basic usage:
    ///
    /// ```
    /// use std::collections::BinaryHeap;
    /// let mut heap = BinaryHeap::from([1, 3]);
    ///
    /// assert!(!heap.is_empty());
    ///
    /// for x in heap.drain() {
    ///     println!("{x}");
    /// }
    ///
    /// assert!(heap.is_empty());
    /// ```
    #[inline]
    pub fn drain(&'arena mut self) -> Drain<'arena, 'arena, T> {
        Drain {
            iter: self.data.drain(..),
        }
    }

    pub fn reserve(&mut self, additional: usize) {
        self.data.reserve(additional);
    }

    pub fn iter(&self) -> Iter<'_, T> {
        Iter {
            iter: self.data.iter(),
        }
    }
}

/// Hole represents a hole in a slice i.e., an index without valid value
/// (because it was moved from or duplicated).
/// In drop, `Hole` will restore the slice by filling the hole
/// position with the value that was originally removed.
struct Hole<'a, T: 'a> {
    data: &'a mut [T],
    elt: ManuallyDrop<T>,
    pos: usize,
}

impl<'a, T> Hole<'a, T> {
    /// Creates a new `Hole` at index `pos`.
    ///
    /// Unsafe because pos must be within the data slice.
    #[inline]
    unsafe fn new(data: &'a mut [T], pos: usize) -> Self {
        debug_assert!(pos < data.len());
        // SAFE: pos should be inside the slice
        let elt = unsafe { ptr::read(data.get_unchecked(pos)) };
        Hole {
            data,
            elt: ManuallyDrop::new(elt),
            pos,
        }
    }

    #[inline]
    fn pos(&self) -> usize {
        self.pos
    }

    /// Returns a reference to the element removed.
    #[inline]
    fn element(&self) -> &T {
        &self.elt
    }

    /// Returns a reference to the element at `index`.
    ///
    /// Unsafe because index must be within the data slice and not equal to pos.
    #[inline]
    unsafe fn get(&self, index: usize) -> &T {
        debug_assert!(index != self.pos);
        debug_assert!(index < self.data.len());
        unsafe { self.data.get_unchecked(index) }
    }

    /// Move hole to new location
    ///
    /// Unsafe because index must be within the data slice and not equal to pos.
    #[inline]
    unsafe fn move_to(&mut self, index: usize) {
        debug_assert!(index != self.pos);
        debug_assert!(index < self.data.len());
        unsafe {
            let ptr = self.data.as_mut_ptr();
            let index_ptr: *const _ = ptr.add(index);
            let hole_ptr = ptr.add(self.pos);
            ptr::copy_nonoverlapping(index_ptr, hole_ptr, 1);
        }
        self.pos = index;
    }
}

impl<T> Drop for Hole<'_, T> {
    #[inline]
    fn drop(&mut self) {
        // fill the hole again
        unsafe {
            let pos = self.pos;
            ptr::copy_nonoverlapping(&*self.elt, self.data.get_unchecked_mut(pos), 1);
        }
    }
}

#[derive(Debug)]
pub struct Drain<'a, 'arena, T: 'a> {
    iter: bumpalo::collections::vec::Drain<'a, 'arena, T>,
}

impl<'arena, T> Iterator for Drain<'_, 'arena, T> {
    type Item = T;

    #[inline]
    fn next(&mut self) -> Option<T> {
        self.iter.next()
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }
}

impl<'arena, T> DoubleEndedIterator for Drain<'_, 'arena, T> {
    #[inline]
    fn next_back(&mut self) -> Option<T> {
        self.iter.next_back()
    }
}

impl<'arena, T> FusedIterator for Drain<'_, 'arena, T> {}

pub struct Iter<'a, T: 'a> {
    iter: slice::Iter<'a, T>,
}

impl<'a, T> Iterator for Iter<'a, T> {
    type Item = &'a T;

    #[inline]
    fn next(&mut self) -> Option<&'a T> {
        self.iter.next()
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }

    #[inline]
    fn last(self) -> Option<&'a T> {
        self.iter.last()
    }
}

impl<'a, T> DoubleEndedIterator for Iter<'a, T> {
    #[inline]
    fn next_back(&mut self) -> Option<&'a T> {
        self.iter.next_back()
    }
}
impl<T> FusedIterator for Iter<'_, T> {}

struct RebuildOnDrop<'a, 'arena, T: Ord> {
    heap: &'a mut BinaryHeap<'arena, T>,
    rebuild_from: usize,
}

impl<'arena, T: Ord> Drop for RebuildOnDrop<'_, 'arena, T> {
    fn drop(&mut self) {
        self.heap.rebuild_tail(self.rebuild_from);
    }
}

/// An owning iterator over the elements of a `BinaryHeap`.
///
/// This `struct` is created by [`BinaryHeap::into_iter()`]
/// (provided by the [`IntoIterator`] trait). See its documentation for more.
///
/// [`into_iter`]: BinaryHeap::into_iter
pub struct IntoIter<'arena, T> {
    iter: bumpalo::collections::vec::IntoIter<'arena, T>,
}

impl<'arena, T> Iterator for IntoIter<'arena, T> {
    type Item = T;

    #[inline]
    fn next(&mut self) -> Option<T> {
        self.iter.next()
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }
}

impl<'arena, T> DoubleEndedIterator for IntoIter<'arena, T> {
    #[inline]
    fn next_back(&mut self) -> Option<T> {
        self.iter.next_back()
    }
}

impl<T> FusedIterator for IntoIter<'_, T> {}

impl<'arena, T> IntoIterator for BinaryHeap<'arena, T> {
    type Item = T;
    type IntoIter = IntoIter<'arena, T>;

    /// Creates a consuming iterator, that is, one that moves each value out of
    /// the binary heap in arbitrary order. The binary heap cannot be used
    /// after calling this.
    ///
    /// # Examples
    ///
    /// Basic usage:
    ///
    /// ```
    /// use std::collections::BinaryHeap;
    /// let heap = BinaryHeap::from([1, 2, 3, 4]);
    ///
    /// // Print 1, 2, 3, 4 in arbitrary order
    /// for x in heap.into_iter() {
    ///     // x has type i32, not &i32
    ///     println!("{x}");
    /// }
    /// ```
    fn into_iter(self) -> IntoIter<'arena, T> {
        IntoIter {
            iter: self.data.into_iter(),
        }
    }
}
