// Copyright 2025 HelixDB Inc.
// SPDX-License-Identifier: AGPL-3.0

//! Ariadne-based diagnostic rendering for rustc-style output.

use super::diagnostic::{Diagnostic, DiagnosticSeverity};
use ariadne::{Color, Label, Report, ReportKind, Source};
use std::io::Cursor;

/// Render a single diagnostic using ariadne.
///
/// * `diag` - The diagnostic to render.
/// * `src` - The entire source text.
/// * `filepath` - Label for the file (e.g. `"query.hx"`).
pub fn render(diag: &Diagnostic, src: &str, filepath: &str) -> String {
    let kind = match diag.severity {
        DiagnosticSeverity::Error => ReportKind::Error,
        DiagnosticSeverity::Warning => ReportKind::Warning,
        DiagnosticSeverity::Info | DiagnosticSeverity::Hint => ReportKind::Advice,
        DiagnosticSeverity::Empty => ReportKind::Custom("note", Color::White),
    };

    let color = match diag.severity {
        DiagnosticSeverity::Error => Color::Red,
        DiagnosticSeverity::Warning => Color::Yellow,
        _ => Color::Blue,
    };

    let byte_range = diag.location.byte_range();

    // Validate byte range to prevent panics
    let src_len = src.len();
    if byte_range.is_empty() {
        // Empty range - return a simple text error
        return format!(
            "[{}] {}: {}",
            diag.error_code,
            diag.severity_str(),
            diag.message
        );
    }
    if byte_range.start > src_len || byte_range.end > src_len {
        // Range exceeds source length - return a simple text error
        return format!(
            "[{}] {}: {} (at byte {}..{})",
            diag.error_code,
            diag.severity_str(),
            diag.message,
            byte_range.start,
            byte_range.end
        );
    }
    if byte_range.start > byte_range.end {
        // Inverted range - return a simple text error
        return format!(
            "[{}] {}: {}",
            diag.error_code,
            diag.severity_str(),
            diag.message
        );
    }

    // Build the primary label - uses message without context suffix
    let label = Label::new((filepath, byte_range.clone()))
        .with_message(diag.label_message())
        .with_color(color);

    // Build the report - header uses description + context
    let mut report = Report::build(kind, (filepath, byte_range))
        .with_code(format!("{}", diag.error_code))
        .with_message(diag.report_message())
        .with_label(label);

    // Add hint as help text if present
    if let Some(hint) = &diag.hint {
        report = report.with_help(hint);
    }

    // Handle fix suggestions
    if let Some(fix) = &diag.fix
        && let Some(to_add) = &fix.to_add
        && let Some(span) = &fix.span
    {
        let fix_range = span.byte_range();
        // Only add if range is valid and within source bounds
        if fix_range.start < fix_range.end && fix_range.start <= src_len && fix_range.end <= src_len
        {
            let fix_label = Label::new((filepath, fix_range))
                .with_message(format!("suggestion: {}", to_add))
                .with_color(Color::Green);
            report = report.with_label(fix_label);
        }
    }

    // Render to string
    let mut output = Cursor::new(Vec::new());
    let source = Source::from(src);

    if let Err(e) = report.finish().write((filepath, source), &mut output) {
        // If rendering fails, return a simple text error
        return format!(
            "[Render Error: {}] {}: {}",
            e, diag.error_code, diag.message
        );
    }

    String::from_utf8(output.into_inner())
        .unwrap_or_else(|_| format!("[{}] {}", diag.error_code, diag.message))
}
