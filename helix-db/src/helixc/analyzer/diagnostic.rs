use crate::helixc::{
    analyzer::{ariadne_render, error_codes::ErrorCode, fix::Fix},
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
        ariadne_render::render(self, src, filepath)
    }

    /// Get report header: "unknown edge type (in query `X`)"
    /// Combines the error description with the context extracted from the message.
    pub fn report_message(&self) -> String {
        let desc = self.error_code.description();
        // Extract context from message if present: "(in QUERY named `X`)"
        if let Some(idx) = self.message.rfind(" (in ") {
            let context = &self.message[idx + 2..self.message.len() - 1];
            // Normalize "QUERY named" to "query"
            let context = context.replace("QUERY named ", "query ");
            format!("{} ({})", desc, context)
        } else {
            desc.to_string()
        }
    }

    /// Get label message: "unknown edge type `User_Has_Access_T`" (without context suffix)
    pub fn label_message(&self) -> String {
        // Strip the "(in QUERY named `X`)" suffix if present
        if let Some(idx) = self.message.rfind(" (in ") {
            self.message[..idx].to_string()
        } else {
            self.message.clone()
        }
    }

    /// Get a string representation of the severity level.
    pub fn severity_str(&self) -> &'static str {
        match self.severity {
            DiagnosticSeverity::Error => "error",
            DiagnosticSeverity::Warning => "warning",
            DiagnosticSeverity::Info => "info",
            DiagnosticSeverity::Hint => "hint",
            DiagnosticSeverity::Empty => "note",
        }
    }
}

#[derive(Debug, Clone)]
pub enum Something {}
