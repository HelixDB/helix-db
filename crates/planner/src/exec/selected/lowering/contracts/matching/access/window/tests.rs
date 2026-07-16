use super::contracts::{SelectedAccessWindowPipelineMatch, SelectedAccessWindowPipelineMismatch};
use super::suffix::{
    selected_access_window_pipeline_match, selected_access_window_pipeline_matches,
};
use crate::{logical, physical};

#[test]
fn access_window_matching_preserves_identity_range_and_skip_shapes() {
    let identity = logical::AccessWindowRange::new(0, None).expect("identity is valid");
    assert_eq!(
        selected_access_window_pipeline_match(identity, &[]),
        SelectedAccessWindowPipelineMatch::Matched
    );
    assert!(selected_access_window_pipeline_matches(identity, &[]));

    let bounded = logical::AccessWindowRange::new(1, Some(4)).expect("bounded range is valid");
    assert_eq!(
        selected_access_window_pipeline_match(
            bounded,
            &[physical::PhysicalPipelineOp::Stream(
                physical::PhysicalStreamOp::Range,
            )],
        ),
        SelectedAccessWindowPipelineMatch::Matched
    );
    assert_eq!(
        selected_access_window_pipeline_match(
            bounded,
            &[physical::PhysicalPipelineOp::Stream(
                physical::PhysicalStreamOp::Skip,
            )],
        ),
        SelectedAccessWindowPipelineMatch::NotMatched(
            SelectedAccessWindowPipelineMismatch::BoundedWindowNeedsRange
        )
    );

    let open = logical::AccessWindowRange::new(2, None).expect("open skip is valid");
    assert_eq!(
        selected_access_window_pipeline_match(
            open,
            &[physical::PhysicalPipelineOp::Stream(
                physical::PhysicalStreamOp::Skip,
            )],
        ),
        SelectedAccessWindowPipelineMatch::Matched
    );
}
