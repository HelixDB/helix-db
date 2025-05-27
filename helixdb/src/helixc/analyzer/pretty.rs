//! Lightweight pretty-printer for `Diagnostic`.
//
//! Produces "rustc-style" output, e.g.
//! ```text
//! error: unknown node type `Post`
//!   ┌─ schema.hx:12:7
//!   │
//! 12 │     To: Post,
//!    │       ^^^^ declare `N::Post` above
//! ```

use super::analyzer::Diagnostic;
use super::analyzer::DiagnosticSeverity;
use colored::*;

/// Render a single diagnostic plus a code snippet.
///
/// * `src` – the entire text parsed by `HelixParser::parse_source`.
/// * `filepath` – label in the left gutter (e.g. `"query.hx"`).
pub fn render(diag: &Diagnostic, src: &str, filepath: &str) -> String {
    // 1-based → 0-based index

    let line_idx = diag.location.start.line.saturating_sub(1);
    let code_line = src.lines().nth(line_idx).unwrap_or("");
    let caret_pad = " ".repeat(diag.location.start.column.saturating_sub(2));
    // Calculate the span length for the error underline
    let span_length = diag
        .location
        .end
        .column
        .saturating_sub(diag.location.start.column)
        .max(1);
    let caret_underline = "^".repeat(span_length).red().bold();

    let mut out = String::new();

    // Color the error/warning label based on severity
    let severity_str = match diag.severity {
        DiagnosticSeverity::Error => "error".red().bold(),
        DiagnosticSeverity::Warning => "warning".yellow().bold(),
        DiagnosticSeverity::Info => "info".blue().bold(),
        DiagnosticSeverity::Hint => "hint".green().bold(),
        DiagnosticSeverity::Empty => "note".normal(),
    };

    out.push_str(&format!("{}: {}\n", severity_str, diag.message));

    // Color the location line in red for errors, yellow for warnings
    let location_color = match diag.severity {
        DiagnosticSeverity::Error => format!(
            "{:>2} {} {}",
            "",
            "┌─".red(),
            format!(
                "{filepath}:{line}:{col}",
                filepath = filepath,
                line = diag.location.start.line,
                col = diag.location.start.column
            )
            .bold(),
        ),
        DiagnosticSeverity::Warning => format!(
            "{:>2} {} {}",
            "",
            "┌─".yellow(),
            format!(
                "{filepath}:{line}:{col}",
                filepath = filepath,
                line = diag.location.start.line,
                col = diag.location.start.column
            )
            .bold(),
        ),
        _ => format!(
            "{:>2} {} {}",
            "",
            "┌─".normal(),
            format!(
                "{filepath}:{line}:{col}",
                filepath = filepath,
                line = diag.location.start.line,
                col = diag.location.start.column
            )
            .bold(),
        ),
    };

    out.push_str(&format!("{}\n", location_color));

    // Color the vertical bars and line numbers based on severity
    let (line_num_color, vertical_bar) = match diag.severity {
        DiagnosticSeverity::Error => (format!("{:>2}", diag.location.start.line).red().bold(), "│".red()),
        DiagnosticSeverity::Warning => (
            format!("{:>2}", diag.location.start.line).yellow().bold(),
            "│".yellow(),
        ),
        _ => (
            format!("{:>2}", diag.location.start.line).normal().bold(),
            "│".normal(),
        ),
    };

    out.push_str(&format!("{:>2} {} \n", "", vertical_bar));
    out.push_str(&format!(
        "{} {} {}\n",
        line_num_color, vertical_bar, code_line
    ));
    out.push_str(&format!(
        "{:>2} {} {}{}\n",
        "", vertical_bar, caret_pad, caret_underline
    ));

    if let Some(ref hint) = diag.hint {
        out.push_str(&format!(
            "{} {}: {}\n",
            "--->".blue().bold(),
            "help".blue().bold(),
            hint.blue().bold()
        ));
    }

    if let Some(ref fix) = diag.fix {
        let bar = &vertical_bar.clone().blue().bold();
        let location = &format!("{:>2}", diag.location.start.line).blue().bold();

        let (start_chunk, end_chunk) = match &fix.span {
            Some(span) => {
                if let Some(to_remove) = &fix.to_remove {
                    let start = to_remove.start.column - span.start.column;
                    let end = to_remove.end.column - span.start.column;
                    let start_chunk = span.span.split_at(start).0.to_string();
                    let end_chunk = span.span.split_at(end).1.to_string();
                    (
                        start_chunk.trim_start_matches("\t").to_string(),
                        end_chunk
                            .trim_end_matches(|c: char| c.is_whitespace() || c == '\n' || c == '\r')
                            .to_string(),
                    )
                } else {
                    ("".to_string(), "".to_string())
                }
            }
            None => ("".to_string(), "".to_string()),
        };
        out.push_str(&format!("{:>2} {}\n", "", bar));
        if let Some(to_remove) = &fix.to_remove {
            out.push_str(&format!(
                "{} {} {}\n",
                location,
                "-".bright_red().bold(),
                format!(
                    "{}{}{}",
                    start_chunk,
                    format!("{}", to_remove.span.trim_end_matches('\n'))
                        .red()
                        .bold(),
                    end_chunk
                )
            ));
        }
        if let Some(to_add) = &fix.to_add {
            out.push_str(&format!(
                "{} {} {}\n",
                location,
                "+".green().bold(),
                format!(
                    "{}{}{}",
                    start_chunk,
                    format!("{}", to_add.trim_end_matches('\n'))
                        .green()
                        .bold(),
                    end_chunk
                )
            ));
        }
        out.push_str(&format!("{:>2} {}\n", "", bar));
    }

    out
}
