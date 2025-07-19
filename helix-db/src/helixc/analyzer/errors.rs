use crate::helixc::{
    analyzer::{
        analyzer::Ctx,
        diagnostic::{Diagnostic, DiagnosticSeverity},
        fix::Fix,
    },
    parser::{helix_parser::Query, location::Loc},
};

pub(crate) fn push_schema_err(ctx: &mut Ctx, loc: Loc, msg: String, hint: Option<String>) {
    ctx.diagnostics.push(Diagnostic::new(
        loc,
        msg,
        DiagnosticSeverity::Error,
        hint,
        None,
    ));
}
pub(crate) fn push_query_err(
    ctx: &mut Ctx,
    q: &Query,
    loc: Loc,
    msg: String,
    hint: impl Into<String>,
) {
    ctx.diagnostics.push(Diagnostic::new(
        Loc::new(q.loc.filepath.clone(), loc.start, loc.end, loc.span),
        format!("{} (in QUERY named `{}`)", msg, q.name),
        DiagnosticSeverity::Error,
        Some(hint.into()),
        None,
    ));
}

pub(crate) fn push_query_err_with_fix(
    ctx: &mut Ctx,
    q: &Query,
    loc: Loc,
    msg: String,
    hint: impl Into<String>,
    fix: Fix,
) {
    ctx.diagnostics.push(Diagnostic::new(
        Loc::new(q.loc.filepath.clone(), loc.start, loc.end, loc.span),
        format!("{} (in QUERY named `{}`)", msg, q.name),
        DiagnosticSeverity::Error,
        Some(hint.into()),
        Some(fix),
    ));
}

pub(crate) fn push_query_warn(
    ctx: &mut Ctx,
    q: &Query,
    loc: Loc,
    msg: String,
    hint: impl Into<String>,
    fix: Option<Fix>,
) {
    ctx.diagnostics.push(Diagnostic::new(
        Loc::new(q.loc.filepath.clone(), loc.start, loc.end, loc.span),
        format!("{} (in QUERY named `{}`)", msg, q.name),
        DiagnosticSeverity::Warning,
        Some(hint.into()),
        fix,
    ));
}

#[macro_export]
macro_rules! generate_error {
    () => {
        
    };
}