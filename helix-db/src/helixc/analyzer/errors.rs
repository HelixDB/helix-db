use crate::helixc::{
    analyzer::{
        Ctx,
        diagnostic::{Diagnostic, DiagnosticSeverity},
        error_codes::ErrorCode,
        fix::Fix,
    },
    parser::{location::Loc, types::Query},
};

pub(crate) fn push_schema_err(
    ctx: &mut Ctx,
    loc: Loc,
    error_code: ErrorCode,
    msg: String,
    hint: Option<String>,
) {
    ctx.diagnostics.push(Diagnostic::new(
        loc,
        msg,
        DiagnosticSeverity::Error,
        error_code,
        hint,
        None,
    ));
}

pub(crate) fn push_schema_warn(
    ctx: &mut Ctx,
    loc: Loc,
    error_code: ErrorCode,
    msg: String,
    hint: Option<String>,
) {
    ctx.diagnostics.push(Diagnostic::new(
        loc,
        msg,
        DiagnosticSeverity::Warning,
        error_code,
        hint,
        None,
    ));
}

pub(crate) fn push_query_err(
    ctx: &mut Ctx,
    q: &Query,
    loc: Loc,
    error_code: ErrorCode,
    msg: String,
    hint: impl Into<String>,
) {
    ctx.diagnostics.push(Diagnostic::new(
        Loc::new(q.loc.filepath.clone(), loc.start, loc.end, loc.span),
        format!("{} (in QUERY named `{}`)", msg, q.name),
        DiagnosticSeverity::Error,
        error_code,
        Some(hint.into()),
        None,
    ));
}

pub(crate) fn push_query_err_with_fix(
    ctx: &mut Ctx,
    q: &Query,
    loc: Loc,
    error_code: ErrorCode,
    msg: String,
    hint: impl Into<String>,
    fix: Fix,
) {
    ctx.diagnostics.push(Diagnostic::new(
        Loc::new(q.loc.filepath.clone(), loc.start, loc.end, loc.span),
        format!("{} (in QUERY named `{}`)", msg, q.name),
        DiagnosticSeverity::Error,
        error_code,
        Some(hint.into()),
        Some(fix),
    ));
}

pub(crate) fn push_query_warn(
    ctx: &mut Ctx,
    q: &Query,
    loc: Loc,
    error_code: ErrorCode,
    msg: String,
    hint: impl Into<String>,
    fix: Option<Fix>,
) {
    ctx.diagnostics.push(Diagnostic::new(
        Loc::new(q.loc.filepath.clone(), loc.start, loc.end, loc.span),
        format!("{} (in QUERY named `{}`)", msg, q.name),
        DiagnosticSeverity::Warning,
        error_code,
        Some(hint.into()),
        fix,
    ));
}
