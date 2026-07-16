//! Cardinality-preserving `AtLeast` operations.

use super::AtLeast;

impl<T, const MIN: usize> AtLeast<T, MIN> {
    /// Transform every item while preserving the statically-known cardinality.
    ///
    /// ```
    /// use helix_planner::ir::AtLeast;
    ///
    /// let values = AtLeast::<_, 2>::from_pair("scan", "filter");
    /// let lengths = values.map(str::len);
    ///
    /// assert_eq!(lengths.as_ref(), &[4, 6]);
    /// ```
    pub fn map<U, F>(self, f: F) -> AtLeast<U, MIN>
    where
        F: FnMut(T) -> U,
    {
        AtLeast {
            items: self.items.into_iter().map(f).collect(),
        }
    }

    /// Sort the collection without changing its cardinality.
    pub fn sort_by_key<K, F>(&mut self, f: F)
    where
        K: Ord,
        F: FnMut(&T) -> K,
    {
        self.items.sort_by_key(f);
    }

    /// Transform every item while preserving the statically-known cardinality.
    ///
    /// ```
    /// use helix_planner::ir::AtLeast;
    ///
    /// let values = AtLeast::<_, 2>::from_pair("1", "2");
    /// let parsed = values.try_map(|value| value.parse::<u8>()).unwrap();
    ///
    /// assert_eq!(parsed.as_ref(), &[1, 2]);
    /// ```
    pub fn try_map<U, E, F>(self, f: F) -> Result<AtLeast<U, MIN>, E>
    where
        F: FnMut(T) -> Result<U, E>,
    {
        self.items
            .into_iter()
            .map(f)
            .collect::<Result<Vec<_>, _>>()
            .map(|items| AtLeast { items })
    }

    /// Fallibly transform borrowed items while preserving cardinality.
    ///
    /// ```
    /// use helix_planner::ir::AtLeast;
    ///
    /// let values = AtLeast::<_, 1>::from_one("42".to_owned());
    /// let parsed = values.try_map_ref(|value| value.parse::<u8>()).unwrap();
    ///
    /// assert_eq!(parsed.as_ref(), &[42]);
    /// ```
    pub fn try_map_ref<'a, U, E, F>(&'a self, f: F) -> Result<AtLeast<U, MIN>, E>
    where
        F: FnMut(&'a T) -> Result<U, E>,
    {
        self.items
            .iter()
            .map(f)
            .collect::<Result<Vec<_>, _>>()
            .map(|items| AtLeast { items })
    }

    /// Transform borrowed items while preserving cardinality.
    ///
    /// ```
    /// use helix_planner::ir::AtLeast;
    ///
    /// let values = AtLeast::<_, 2>::from_pair("scan", "filter");
    /// let lengths = values.map_ref(|value| value.len());
    ///
    /// assert_eq!(lengths.as_ref(), &[4, 6]);
    /// ```
    pub fn map_ref<'a, U, F>(&'a self, f: F) -> AtLeast<U, MIN>
    where
        F: FnMut(&'a T) -> U,
    {
        AtLeast {
            items: self.items.iter().map(f).collect(),
        }
    }

    /// Transform borrowed items with their stable position while preserving
    /// cardinality.
    ///
    /// ```
    /// use helix_planner::ir::AtLeast;
    ///
    /// let values = AtLeast::<_, 2>::from_pair("scan", "filter");
    /// let indexed = values.enumerate_map_ref(|index, value| (index, value.len()));
    ///
    /// assert_eq!(indexed.as_ref(), &[(0, 4), (1, 6)]);
    /// ```
    pub fn enumerate_map_ref<'a, U, F>(&'a self, mut f: F) -> AtLeast<U, MIN>
    where
        F: FnMut(usize, &'a T) -> U,
    {
        AtLeast {
            items: self
                .items
                .iter()
                .enumerate()
                .map(|(index, item)| f(index, item))
                .collect(),
        }
    }
}

impl<T> AtLeast<T, 1> {
    /// Sort and deduplicate a non-empty collection while preserving
    /// non-emptiness.
    ///
    /// ```
    /// use helix_planner::ir::AtLeast;
    ///
    /// let values = AtLeast::<_, 1>::from_one_and_rest(3, vec![1, 3, 2]);
    /// let canonical = values.sorted_dedup();
    ///
    /// assert_eq!(canonical.as_ref(), &[1, 2, 3]);
    /// ```
    pub fn sorted_dedup(mut self) -> Self
    where
        T: Ord,
    {
        self.items.sort();
        self.items.dedup();
        self
    }

    /// Consume a non-empty collection into its first item and remaining suffix.
    ///
    /// ```
    /// use helix_planner::ir::AtLeast;
    ///
    /// let values = AtLeast::<_, 1>::from_one_and_rest("scan", vec!["filter", "project"]);
    /// let (first, rest) = values.into_first_and_rest();
    ///
    /// assert_eq!(first, "scan");
    /// assert_eq!(rest, vec!["filter", "project"]);
    /// ```
    pub fn into_first_and_rest(mut self) -> (T, Vec<T>) {
        let rest = self.items.split_off(1);
        // `AtLeast` stores a Vec so serde can keep the external representation
        // as a plain list; constructors and deserialization enforce the
        // non-empty invariant before this consuming accessor can exist.
        let first = self
            .items
            .pop()
            .expect("AtLeast<_, 1> always contains a first item");
        (first, rest)
    }

    /// Split a non-empty collection into its final item and preceding prefix.
    ///
    /// ```
    /// use helix_planner::ir::AtLeast;
    ///
    /// let values = AtLeast::<_, 1>::from_one_and_rest("scan", vec!["filter", "project"]);
    /// let (last, prefix) = values.split_last();
    ///
    /// assert_eq!(last, &"project");
    /// assert_eq!(prefix, &["scan", "filter"]);
    /// ```
    pub fn split_last(&self) -> (&T, &[T]) {
        let last_index = self.items.len() - 1;
        (&self.items[last_index], &self.items[..last_index])
    }
}
