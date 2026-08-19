//! Executable selected-root construction error translation.

use super::planner::unsupported;
use super::reason::Reason;
use crate::{error, exec};

pub(in crate::planning::selected) fn unsupported_root_construction(
    error: exec::SelectedRootConstructionError,
) -> error::PlannerError {
    unsupported(match error {
        exec::SelectedRootConstructionError::IncompatiblePhysicalShape => {
            Reason::SelectedRootPhysicalMismatch
        }
        exec::SelectedRootConstructionError::RootPipelineLogicalSuffixTooLong => {
            Reason::SelectedRootPipelineLogicalSuffixTooLong
        }
        exec::SelectedRootConstructionError::RootPipelinePhysicalSuffixMismatch => {
            Reason::SelectedRootPipelinePhysicalSuffixMismatch
        }
        exec::SelectedRootConstructionError::RootTerminalPhysicalSuffixMismatch => {
            Reason::SelectedRootTerminalPhysicalSuffixMismatch
        }
        exec::SelectedRootConstructionError::RecursiveRootStreamInputNonLocalizedPrefix => {
            Reason::SelectedRootStreamInputNonLocalizedPrefix
        }
        exec::SelectedRootConstructionError::CountInputMismatch => {
            Reason::SelectedCountInputMismatch
        }
    })
}

pub(in crate::planning::selected) fn unsupported_alternative_construction(
    error: exec::SelectedAlternativeConstructionError,
) -> error::PlannerError {
    unsupported(match error {
        exec::SelectedAlternativeConstructionError::UnsupportedLogicalPhysicalPair => {
            Reason::SelectedAlternativeUnsupported
        }
        exec::SelectedAlternativeConstructionError::ClassifiedFamilyMismatch => {
            Reason::SelectedAlternativeFamilyMismatch
        }
    })
}
