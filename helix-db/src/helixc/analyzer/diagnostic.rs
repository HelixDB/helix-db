use crate::helixc::{
    analyzer::{error_codes::ErrorCode, fix::Fix, pretty},
    parser::location::Loc,
};

/// A single diagnostic to be surfaced to the editor.
#[derive(Debug, Clone)]
#[allow(unused)]
pub struct Diagnostic {
    pub location: Loc,
    pub error_code: ErrorCode,
    pub message: String,
    pub hint: Option<String>,
    pub filepath: Option<String>,
    pub severity: DiagnosticSeverity,
    pub fix: Option<Fix>,
}

#[derive(Debug, Clone)]
pub enum DiagnosticSeverity {
    Error,
    Warning,
    Info,
    Hint,
    Empty,
}

impl Diagnostic {
    pub fn new(
        location: Loc,
        message: impl Into<String>,
        severity: DiagnosticSeverity,
        error_code: ErrorCode,
        hint: Option<String>,
        fix: Option<Fix>,
    ) -> Self {
        let filepath = location.filepath.clone();
        Self {
            location,
            message: message.into(),
            error_code,
            hint,
            fix,
            filepath,
            severity,
        }
    }

    pub fn render(&self, src: &str, filepath: &str) -> String {
        pretty::render(self, src, filepath)
    }
}

#[derive(Debug, Clone)]
pub enum Something {}
