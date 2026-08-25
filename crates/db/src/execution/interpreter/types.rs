//! Runtime row, element, scalar, and virtual-property representations.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use helix_planner::ir;

use crate::encoding::property::property_value::PropertyValue as DbPropertyValue;

/// Element reference carried by an execution row.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum ElementRef {
    /// Node ID.
    Node(u64),
    /// Edge ID.
    Edge(u64),
}

impl ElementRef {
    pub(super) const fn id(&self) -> u64 {
        match self {
            Self::Node(id) | Self::Edge(id) => *id,
        }
    }
}

/// Traversal history carried by an execution row.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct RowPath {
    elements: Vec<ElementRef>,
}

impl RowPath {
    /// Build an empty path for rows that do not yet point at an element.
    pub fn empty() -> Self {
        Self {
            elements: Vec::new(),
        }
    }

    /// Build a path whose first element is the row's current element.
    pub fn from_current(current: ElementRef) -> Self {
        Self {
            elements: vec![current],
        }
    }

    /// Elements in traversal order.
    pub fn elements(&self) -> &[ElementRef] {
        &self.elements
    }

    fn push(&mut self, element: ElementRef) {
        self.elements.push(element);
    }

    fn is_simple(&self) -> bool {
        let mut seen = BTreeSet::new();
        self.elements.iter().all(|element| seen.insert(element))
    }
}

/// One row in an executable stream.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ExecutionRow {
    /// Current traverser element.
    pub current: Option<ElementRef>,
    /// Virtual properties attached to the current row by runtime-only operators.
    pub virtual_properties: RowVirtualProperties,
    /// Row-local bindings captured by `bind`.
    pub bindings: BTreeMap<ir::NonEmptyString, ElementRef>,
    /// Virtual properties snapshotted for row-local bindings.
    pub binding_virtual_properties: BTreeMap<ir::NonEmptyString, RowVirtualProperties>,
    /// Traversal history.
    pub path: RowPath,
    /// Whether the public response should expose `path`.
    pub path_visible: bool,
    /// Row-local sack state.
    pub sack: RowSack,
}

impl ExecutionRow {
    pub(crate) fn empty() -> Self {
        Self {
            current: None,
            virtual_properties: RowVirtualProperties::empty(),
            bindings: BTreeMap::new(),
            binding_virtual_properties: BTreeMap::new(),
            path: RowPath::empty(),
            path_visible: false,
            sack: RowSack::empty(),
        }
    }

    pub(super) fn current(current: ElementRef) -> Self {
        Self {
            current: Some(current.clone()),
            virtual_properties: RowVirtualProperties::empty(),
            bindings: BTreeMap::new(),
            binding_virtual_properties: BTreeMap::new(),
            path: RowPath::from_current(current),
            path_visible: false,
            sack: RowSack::empty(),
        }
    }

    pub(super) fn set_current(&mut self, current: ElementRef) {
        self.current = Some(current.clone());
        self.virtual_properties = RowVirtualProperties::empty();
        self.path.push(current);
    }

    pub(super) fn current_with_virtual_properties(
        current: ElementRef,
        virtual_properties: RowVirtualProperties,
    ) -> Self {
        Self {
            current: Some(current.clone()),
            virtual_properties,
            bindings: BTreeMap::new(),
            binding_virtual_properties: BTreeMap::new(),
            path: RowPath::from_current(current),
            path_visible: false,
            sack: RowSack::empty(),
        }
    }

    pub(super) fn mark_path_visible(mut self) -> Self {
        self.path_visible = true;
        self
    }

    pub(super) fn has_simple_path(&self) -> bool {
        self.path.is_simple()
    }

    pub(super) fn set_sack(&mut self, value: DbPropertyValue) {
        self.sack.set(value);
    }

    pub(super) fn clear_sack(&mut self) {
        self.sack.clear();
    }

    pub(super) fn mark_sack_visible(mut self) -> Self {
        self.sack.mark_visible();
        self
    }
}

/// Virtual row properties that are not stored on the graph element.
#[derive(Debug, Clone, PartialEq)]
pub struct RowVirtualProperties {
    values: BTreeMap<ir::NonEmptyString, DbPropertyValue>,
}

impl RowVirtualProperties {
    /// Build an empty virtual-property set.
    pub fn empty() -> Self {
        Self {
            values: BTreeMap::new(),
        }
    }

    /// Build a one-property virtual set.
    pub fn from_one(name: ir::NonEmptyString, value: DbPropertyValue) -> Self {
        Self {
            values: BTreeMap::from([(name, value)]),
        }
    }

    /// Whether there are no virtual properties.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Clone a virtual value for projection.
    pub fn get(&self, name: &ir::NonEmptyString) -> Option<DbPropertyValue> {
        self.values.get(name).cloned()
    }

    /// Sets one runtime-only property while preserving existing row annotations.
    pub(super) fn insert(&mut self, name: ir::NonEmptyString, value: DbPropertyValue) {
        self.values.insert(name, value);
    }
}

impl Eq for RowVirtualProperties {}

impl PartialOrd for RowVirtualProperties {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RowVirtualProperties {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        virtual_property_key(&self.values).cmp(&virtual_property_key(&other.values))
    }
}

fn virtual_property_key(
    values: &BTreeMap<ir::NonEmptyString, DbPropertyValue>,
) -> Vec<(&str, String)> {
    values
        .iter()
        .map(|(name, value)| (name.as_ref(), format!("{value:?}")))
        .collect()
}

/// Per-row sack value carried by reserved sack operations.
#[derive(Debug, Clone)]
pub struct RowSack {
    value: Option<DbPropertyValue>,
    visible: bool,
}

impl RowSack {
    /// Build an unset sack.
    pub fn empty() -> Self {
        Self {
            value: None,
            visible: false,
        }
    }

    /// Current sack value, if one has been assigned.
    pub fn value(&self) -> Option<&DbPropertyValue> {
        self.value.as_ref()
    }

    /// Whether the public response should expose the sack value.
    pub fn is_visible(&self) -> bool {
        self.visible
    }

    fn set(&mut self, value: DbPropertyValue) {
        self.value = Some(value);
    }

    fn clear(&mut self) {
        self.value = None;
    }

    fn mark_visible(&mut self) {
        self.visible = true;
    }
}

impl PartialEq for RowSack {
    fn eq(&self, other: &Self) -> bool {
        self.visible == other.visible
            && sack_value_key(self.value()) == sack_value_key(other.value())
    }
}

impl Eq for RowSack {}

impl PartialOrd for RowSack {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RowSack {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (sack_value_key(self.value()), self.visible)
            .cmp(&(sack_value_key(other.value()), other.visible))
    }
}

fn sack_value_key(value: Option<&DbPropertyValue>) -> Option<String> {
    value.map(|value| format!("{value:?}"))
}

/// Materialized stream captured by a `fold` barrier.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FoldedStream {
    rows: Vec<ExecutionRow>,
}

impl FoldedStream {
    /// Capture stream rows behind an explicit folded-stream contract.
    pub fn new(rows: Vec<ExecutionRow>) -> Self {
        Self { rows }
    }

    /// Rows contained in the folded stream.
    pub fn rows(&self) -> &[ExecutionRow] {
        &self.rows
    }

    /// Consume the folded stream and return its rows.
    pub fn into_rows(self) -> Vec<ExecutionRow> {
        self.rows
    }

    /// Number of stream items visible to batch conditions.
    pub fn len(&self) -> usize {
        usize::from(!self.rows.is_empty())
    }

    /// Whether the folded stream contains no rows.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

/// Scalar output values produced by terminal projections.
#[derive(Debug, Clone, PartialEq)]
pub enum ExecutionScalar {
    /// Node ID.
    NodeId(u64),
    /// Edge ID.
    EdgeId(u64),
    /// String scalar.
    String(String),
    /// Stored property value.
    Value(DbPropertyValue),
    /// Object-shaped projection row.
    Object(BTreeMap<String, DbPropertyValue>),
}

/// Runtime value bound to batch variables or returned from execution.
#[derive(Debug, Clone, PartialEq)]
pub enum ExecutionValue {
    /// Stream of element rows.
    Stream(Vec<ExecutionRow>),
    /// Stream rows materialized behind a `fold` barrier.
    FoldedStream(FoldedStream),
    /// Count terminal.
    Count(usize),
    /// Exists terminal.
    Bool(bool),
    /// Scalar terminal rows.
    Scalars(Vec<ExecutionScalar>),
    /// One bindable CREATE/DROP receipt.
    IndexDdlReceipt(crate::index_lifecycle::IndexDdlReceipt),
    /// One bindable lifecycle operation status.
    IndexOperationStatus(crate::index_lifecycle::IndexOperationStatus),
}

/// Normalized value for one declared query return.
///
/// Empty collection and object results are distinct variants so response
/// serialization cannot lose the planner-inferred shape.
#[derive(Debug, Clone, PartialEq)]
pub enum ReturnedValue {
    /// Preserve the existing serialization for a present value.
    Present(ExecutionValue),
    /// Serialize an empty collection as `[]`.
    EmptyList,
    /// Serialize an absent at-most-one value as `null`.
    EmptyObject,
}

/// Runtime ownership for a value retained by interpreter state.
///
/// Linear pipelines stay uniquely owned. A value becomes shared only when two
/// live interpreter locations must retain it, such as a bound step output or a
/// parallel task snapshot.
#[derive(Debug, Clone)]
pub(super) enum ExecutionValueSlot {
    Unique(ExecutionValue),
    Shared(Arc<ExecutionValue>),
}

impl ExecutionValueSlot {
    pub(super) fn value(&self) -> &ExecutionValue {
        match self {
            Self::Unique(value) => value,
            Self::Shared(value) => value,
        }
    }

    pub(super) fn into_value(self) -> ExecutionValue {
        match self {
            Self::Unique(value) => value,
            Self::Shared(value) => {
                Arc::try_unwrap(value).unwrap_or_else(|shared| shared.as_ref().clone())
            }
        }
    }

    pub(super) fn fork(self) -> (Self, Self) {
        let shared = match self {
            Self::Unique(value) => Arc::new(value),
            Self::Shared(value) => value,
        };
        (Self::Shared(Arc::clone(&shared)), Self::Shared(shared))
    }
}

impl From<ExecutionValue> for ExecutionValueSlot {
    fn from(value: ExecutionValue) -> Self {
        Self::Unique(value)
    }
}

/// Ownership of one ordered interpreter value table.
///
/// Serial execution keeps the table inline and allocation-free while empty.
/// Parallel execution transitions the whole immutable table to shared ownership
/// exactly once, without rebuilding keys or values for each task.
#[derive(Debug)]
enum ExecutionValueTable<K: Ord> {
    Unique(BTreeMap<K, ExecutionValueSlot>),
    Shared(Arc<BTreeMap<K, ExecutionValueSlot>>),
}

/// Ordered interpreter values with explicit table and value ownership.
#[derive(Debug)]
pub(super) struct ExecutionValueStore<K: Ord> {
    values: ExecutionValueTable<K>,
}

impl<K: Ord> Default for ExecutionValueStore<K> {
    fn default() -> Self {
        Self {
            values: ExecutionValueTable::Unique(BTreeMap::new()),
        }
    }
}

impl<K: Ord> ExecutionValueStore<K> {
    pub(super) fn get(&self, key: &K) -> Option<&ExecutionValue> {
        self.table().get(key).map(ExecutionValueSlot::value)
    }

    #[cfg(test)]
    pub(super) fn contains_key(&self, key: &K) -> bool {
        self.table().contains_key(key)
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.table().len()
    }

    #[cfg(test)]
    pub(super) fn is_empty(&self) -> bool {
        self.table().is_empty()
    }

    /// Shares one immutable value table with an isolated parallel context.
    pub(super) fn shallow_snapshot(&mut self) -> Self {
        match &self.values {
            ExecutionValueTable::Shared(values) => Self {
                values: ExecutionValueTable::Shared(Arc::clone(values)),
            },
            ExecutionValueTable::Unique(_) => {
                let ExecutionValueTable::Unique(values) = std::mem::replace(
                    &mut self.values,
                    ExecutionValueTable::Unique(BTreeMap::new()),
                ) else {
                    unreachable!("the table variant was matched before replacement");
                };
                let values = Arc::new(values);
                self.values = ExecutionValueTable::Shared(Arc::clone(&values));
                Self {
                    values: ExecutionValueTable::Shared(values),
                }
            }
        }
    }

    fn table(&self) -> &BTreeMap<K, ExecutionValueSlot> {
        match &self.values {
            ExecutionValueTable::Unique(values) => values,
            ExecutionValueTable::Shared(values) => values,
        }
    }
}

impl<K: Ord + Clone> ExecutionValueStore<K> {
    pub(super) fn insert(&mut self, key: K, value: ExecutionValue) -> Option<ExecutionValue> {
        self.insert_slot(key, value.into())
            .map(ExecutionValueSlot::into_value)
    }

    pub(super) fn insert_slot(
        &mut self,
        key: K,
        value: ExecutionValueSlot,
    ) -> Option<ExecutionValueSlot> {
        self.table_mut().insert(key, value)
    }

    pub(super) fn remove(&mut self, key: &K) -> Option<ExecutionValue> {
        self.table_mut()
            .remove(key)
            .map(ExecutionValueSlot::into_value)
    }

    pub(super) fn take_slot(&mut self, key: &K) -> Option<ExecutionValueSlot> {
        self.table_mut().remove(key)
    }

    pub(super) fn into_values(self) -> BTreeMap<K, ExecutionValue> {
        let values = match self.values {
            ExecutionValueTable::Unique(values) => values,
            ExecutionValueTable::Shared(values) => {
                Arc::try_unwrap(values).unwrap_or_else(|shared| shared.as_ref().clone())
            }
        };
        values
            .into_iter()
            .map(|(key, value)| (key, value.into_value()))
            .collect()
    }

    /// Forks one retained value while keeping the other shared owner in place.
    pub(super) fn fork_slot(&mut self, key: &K) -> Option<ExecutionValueSlot> {
        let values = self.table_mut();
        let value = values.remove(key)?;
        let (retained, snapshot) = value.fork();
        values.insert(key.clone(), retained);
        Some(snapshot)
    }

    fn table_mut(&mut self) -> &mut BTreeMap<K, ExecutionValueSlot> {
        match &mut self.values {
            ExecutionValueTable::Unique(values) => values,
            ExecutionValueTable::Shared(values) => Arc::make_mut(values),
        }
    }
}

impl ExecutionValue {
    /// Number of result items represented by this value.
    pub fn len(&self) -> usize {
        match self {
            Self::Stream(rows) => rows.len(),
            Self::FoldedStream(folded) => folded.len(),
            Self::Count(count) => *count,
            Self::Bool(value) => usize::from(*value),
            Self::Scalars(values) => values.len(),
            Self::IndexDdlReceipt(_) | Self::IndexOperationStatus(_) => 1,
        }
    }

    /// Whether this value represents no results.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Final result of executing an executable plan.
#[derive(Debug, Clone, PartialEq)]
pub struct ExecutionResult {
    /// Root step result.
    pub last: Option<ExecutionValue>,
    /// Values bound by batch outputs and variable operations.
    pub variables: BTreeMap<ir::NonEmptyString, ExecutionValue>,
    /// Requested return values, keyed by the planner return list.
    pub returns: BTreeMap<ir::NonEmptyString, ReturnedValue>,
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    #[test]
    fn execution_value_len_and_empty_are_shape_aware() {
        assert_eq!(ExecutionValue::Stream(Vec::new()).len(), 0);
        assert!(ExecutionValue::Stream(Vec::new()).is_empty());
        assert_eq!(
            ExecutionValue::Stream(vec![ExecutionRow::current(ElementRef::Node(7))]).len(),
            1
        );
        let folded = FoldedStream::new(vec![
            ExecutionRow::current(ElementRef::Node(7)),
            ExecutionRow::current(ElementRef::Node(8)),
        ]);
        assert_eq!(ExecutionValue::FoldedStream(folded).len(), 1);
        assert!(ExecutionValue::FoldedStream(FoldedStream::new(Vec::new())).is_empty());
        assert_eq!(ExecutionValue::Count(3).len(), 3);
        assert_eq!(ExecutionValue::Bool(true).len(), 1);
        assert_eq!(ExecutionValue::Bool(false).len(), 0);
        assert_eq!(
            ExecutionValue::Scalars(vec![ExecutionScalar::NodeId(1), ExecutionScalar::EdgeId(2)])
                .len(),
            2
        );
        assert_eq!(
            ExecutionValue::IndexDdlReceipt(
                crate::index_lifecycle::IndexDdlReceipt::ExistingOperation {
                    operation_id: crate::index_lifecycle::IndexOperationId::from_bytes([7; 16])
                        .unwrap(),
                }
            )
            .len(),
            1
        );
    }

    #[test]
    fn execution_value_slots_move_linear_values_and_clone_only_live_fanout() {
        let rows = vec![ExecutionRow::current(ElementRef::Node(7))];
        let original_rows = rows.as_ptr();
        let unique = ExecutionValueSlot::from(ExecutionValue::Stream(rows));
        let (first, final_owner) = unique.fork();

        let first = first.into_value();
        let ExecutionValue::Stream(first_rows) = first else {
            panic!("forked value should remain a stream");
        };
        assert_ne!(first_rows.as_ptr(), original_rows);
        drop(first_rows);

        let final_owner = final_owner.into_value();
        let ExecutionValue::Stream(final_rows) = final_owner else {
            panic!("final value should remain a stream");
        };
        assert_eq!(final_rows.as_ptr(), original_rows);
    }

    #[test]
    fn element_refs_order_by_kind_then_id_for_deterministic_sets() {
        let refs = BTreeMap::from([(ElementRef::Edge(3), "edge"), (ElementRef::Node(1), "node")]);

        assert_eq!(
            refs.keys().cloned().collect::<Vec<_>>(),
            vec![ElementRef::Node(1), ElementRef::Edge(3)]
        );
    }

    #[test]
    fn execution_rows_track_path_on_current_transitions() {
        let mut row = ExecutionRow::current(ElementRef::Node(1));
        row.set_current(ElementRef::Edge(7));
        row.set_current(ElementRef::Node(2));

        assert_eq!(
            row.path.elements(),
            &[
                ElementRef::Node(1),
                ElementRef::Edge(7),
                ElementRef::Node(2)
            ]
        );
        assert!(row.has_simple_path());
    }

    #[test]
    fn execution_rows_detect_repeated_path_elements() {
        let mut row = ExecutionRow::current(ElementRef::Node(1));
        row.set_current(ElementRef::Node(2));
        row.set_current(ElementRef::Node(1));

        assert!(!row.has_simple_path());
    }

    #[test]
    fn execution_rows_clear_sack_state() {
        let mut row = ExecutionRow::current(ElementRef::Node(1));
        row.set_sack(DbPropertyValue::I64(7));

        row.clear_sack();

        assert_eq!(row.sack.value(), None);
    }

    #[test]
    fn folded_stream_reports_direct_emptiness() {
        assert!(FoldedStream::new(Vec::new()).is_empty());
        assert!(!FoldedStream::new(vec![ExecutionRow::empty()]).is_empty());
    }

    #[test]
    fn row_metadata_partial_order_matches_its_total_order() {
        let empty_virtual = RowVirtualProperties::empty();
        let populated_virtual = RowVirtualProperties::from_one(
            ir::NonEmptyString::new("score".to_string()).unwrap(),
            DbPropertyValue::I64(7),
        );
        assert_eq!(
            empty_virtual.partial_cmp(&populated_virtual),
            Some(empty_virtual.cmp(&populated_virtual))
        );

        let empty_sack = RowSack::empty();
        let mut populated_sack = RowSack::empty();
        populated_sack.set(DbPropertyValue::I64(7));
        populated_sack.mark_visible();
        assert_eq!(
            empty_sack.partial_cmp(&populated_sack),
            Some(empty_sack.cmp(&populated_sack))
        );
    }
}
