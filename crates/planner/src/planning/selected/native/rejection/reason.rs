//! Stable native selected-planning unsupported-shape inventory.

/// Unsupported native selected-planning boundary reason.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(in crate::planning::selected::native) enum NativeUnsupportedReason {
    /// A materialized batch root use did not match optimizer output.
    BatchRootUseMismatch,
    /// A native AST query root cannot be selected by Cascades.
    QueryRootUnsupported,
    /// A logical expression cannot be consumed as a root stream.
    RootStreamUnsupportedExpression,
    /// A recognized stream wrapper cannot consume its AST input as a root stream.
    RootStreamInputUnsupported,
    /// A scoped recursive child cannot be selected by Cascades.
    ScopedChildUnsupported,
    /// Access-stream accumulation produced a non-canonical pipeline.
    AccessStreamNonCanonicalPipeline,
    /// Access-rooted pipeline composition produced a non-canonical pipeline.
    AccessPipelineNonCanonical,
    /// Root-stream pipeline composition produced a non-canonical pipeline.
    RootPipelineNonCanonical,
}

impl NativeUnsupportedReason {
    #[cfg(test)]
    pub(super) const ALL: &'static [Self] = &[
        Self::BatchRootUseMismatch,
        Self::QueryRootUnsupported,
        Self::RootStreamUnsupportedExpression,
        Self::RootStreamInputUnsupported,
        Self::ScopedChildUnsupported,
        Self::AccessStreamNonCanonicalPipeline,
        Self::AccessPipelineNonCanonical,
        Self::RootPipelineNonCanonical,
    ];

    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::BatchRootUseMismatch => "selected batch root use did not match optimized roots",
            Self::QueryRootUnsupported => {
                "native AST query root does not expose a supported Cascades contract"
            }
            Self::RootStreamUnsupportedExpression => {
                "native AST root did not produce a root stream"
            }
            Self::RootStreamInputUnsupported => {
                "native AST stream wrapper input did not produce a supported root stream"
            }
            Self::ScopedChildUnsupported => {
                "scoped native AST child root does not expose a supported Cascades contract"
            }
            Self::AccessStreamNonCanonicalPipeline => {
                "native AST access stream produced a non-canonical pipeline"
            }
            Self::AccessPipelineNonCanonical => {
                "native AST access pipeline produced a non-canonical pipeline"
            }
            Self::RootPipelineNonCanonical => {
                "native AST root pipeline produced a non-canonical pipeline"
            }
        }
    }
}
