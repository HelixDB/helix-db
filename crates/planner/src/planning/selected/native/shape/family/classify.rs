//! Three-way native access-stream shape classification.

use helix_ast::traversal::AstNode;

use super::{source, wrapper, NativeAccessStreamShape};

pub(in crate::planning::selected::native::shape) fn access_stream_shape_from_ast(
    root: &AstNode,
) -> NativeAccessStreamShape<'_> {
    match source::source_from_ast(root) {
        source::NativeAccessStreamSourceMatch::Source(source) => {
            NativeAccessStreamShape::Source(source)
        }
        source::NativeAccessStreamSourceMatch::NotSource => match wrapper::wrapper_from_ast(root) {
            wrapper::NativeAccessStreamWrapperMatch::Wrapper(wrapper) => {
                NativeAccessStreamShape::Wrapper(wrapper)
            }
            wrapper::NativeAccessStreamWrapperMatch::NotWrapper => {
                NativeAccessStreamShape::NotAccessStream
            }
        },
    }
}
