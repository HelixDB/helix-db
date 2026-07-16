//! Selected root construction error contracts.
//!
//! These errors are raised before a value crosses into selected executable IR,
//! keeping physically impossible selected roots out of the executable-lowering
//! boundary.

/// A selected root wrapper could not be built because its physical
/// implementation does not satisfy the selected root contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelectedRootConstructionError {
    /// The physical expression family does not match the selected root family.
    IncompatiblePhysicalShape,
    /// A selected root pipeline's physical pipeline is shorter than its logical suffix.
    RootPipelineLogicalSuffixTooLong,
    /// A selected root pipeline's physical suffix does not implement its logical suffix.
    RootPipelinePhysicalSuffixMismatch,
    /// A selected root terminal's physical terminal does not implement its logical terminal.
    RootTerminalPhysicalSuffixMismatch,
    /// A recursive selected root-stream input was paired with a parent-local prefix.
    RecursiveRootStreamInputNonLocalizedPrefix,
}

/// A selected ordinary alternative wrapper could not be built because its
/// logical/physical implementation pair does not satisfy the selected
/// executable alternative contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelectedAlternativeConstructionError {
    /// The logical/physical pair is not an executable selected alternative.
    UnsupportedLogicalPhysicalPair,
    /// A caller-provided family proof did not match the logical/physical pair.
    ClassifiedFamilyMismatch,
}
