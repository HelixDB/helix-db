//! Runtime row, element, scalar, and virtual-property representations.

use std::collections::{BTreeMap, BTreeSet};

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
    IndexDdlReceipt(crate::index_v2::IndexDdlReceipt),
    /// One bindable lifecycle operation status.
    IndexOperationStatus(crate::index_v2::IndexOperationStatus),
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
    pub returns: BTreeMap<ir::NonEmptyString, ExecutionValue>,
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
            ExecutionValue::IndexDdlReceipt(crate::index_v2::IndexDdlReceipt::ExistingOperation {
                operation_id: crate::index_v2::IndexOperationId::from_bytes([7; 16]).unwrap(),
            })
            .len(),
            1
        );
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
}
