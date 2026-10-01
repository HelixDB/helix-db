//! Terminal output shared by every command.
//!
//! Stream contract:
//! - **stdout** carries only a command's result — tables, detail views, query
//!   JSON, one-time tokens, or the `--json` payload — so it is always safe to
//!   pipe.
//! - **stderr** carries all human chrome (sessions, steps, spinners, warnings,
//!   errors), drawn in the same cliclack style as the interactive prompts.
//!
//! [`OutputMode::Json`] suppresses chrome entirely, never prompts, and reports
//! errors as a JSON object on stderr (see [`report_error`]).

pub mod json;
pub mod table;
mod theme;

pub use theme::HelixTheme;

use crate::errors::CliError;
use cliclack::Theme as _;
use console::style;
use indicatif::{ProgressBar, ProgressStyle};
use serde::Serialize;
use std::io::{IsTerminal, Write as _};
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::time::{Duration, Instant};

const SPINNER_TICK: Duration = Duration::from_millis(80);

// ============================================================================
// Output mode
// ============================================================================

/// How much human chrome to print.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum Verbosity {
    /// Nothing, not even the final result. Set internally when one command
    /// wraps another (e.g. `helix chef` driving `init::run` behind its own
    /// spinner).
    Silent = 0,
    /// Errors and the final result only (`--quiet`).
    Quiet = 1,
    /// Sessions, steps, and spinners (default).
    Normal = 2,
    /// Every sub-step with timing (`--verbose`).
    Verbose = 3,
}

/// Output mode for the whole process, selected by the global `--json`,
/// `--quiet`, and `--verbose` flags.
///
/// JSON has no verbosity: it always prints exactly the result payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputMode {
    Human(Verbosity),
    Json,
}

/// Encoded [`OutputMode`]: `0..=3` are human verbosities, [`JSON_MODE`] is JSON.
static MODE: AtomicU8 = AtomicU8::new(Verbosity::Normal as u8);
const JSON_MODE: u8 = 4;

/// Serializes tests that read or write the process-wide output mode.
#[cfg(test)]
pub(crate) static MODE_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

impl OutputMode {
    /// The process-wide mode.
    pub fn current() -> Self {
        match MODE.load(Ordering::Relaxed) {
            0 => Self::Human(Verbosity::Silent),
            1 => Self::Human(Verbosity::Quiet),
            3 => Self::Human(Verbosity::Verbose),
            JSON_MODE => Self::Json,
            _ => Self::Human(Verbosity::Normal),
        }
    }

    /// Replace the process-wide mode. JSON mode also disables ANSI colours on
    /// both streams so no escape code can leak into machine output.
    pub fn set(self) {
        let encoded = match self {
            Self::Human(verbosity) => verbosity as u8,
            Self::Json => {
                console::set_colors_enabled(false);
                console::set_colors_enabled_stderr(false);
                JSON_MODE
            }
        };
        MODE.store(encoded, Ordering::Relaxed);
    }

    /// Map the global flags onto a mode. clap rejects `--json` together with
    /// `--quiet`/`--verbose`, and `--json` wins if both ever reach here.
    ///
    /// ```
    /// use helix_cli::output::{OutputMode, Verbosity};
    ///
    /// assert_eq!(OutputMode::from_flags(true, false, false), OutputMode::Json);
    /// assert_eq!(
    ///     OutputMode::from_flags(false, true, false),
    ///     OutputMode::Human(Verbosity::Quiet)
    /// );
    /// assert_eq!(
    ///     OutputMode::from_flags(false, false, false),
    ///     OutputMode::Human(Verbosity::Normal)
    /// );
    /// ```
    pub fn from_flags(json: bool, quiet: bool, verbose: bool) -> Self {
        match (json, quiet, verbose) {
            (true, _, _) => Self::Json,
            (false, true, _) => Self::Human(Verbosity::Quiet),
            (false, false, true) => Self::Human(Verbosity::Verbose),
            (false, false, false) => Self::Human(Verbosity::Normal),
        }
    }

    pub fn is_json(self) -> bool {
        self == Self::Json
    }
}

impl Verbosity {
    /// Chrome verbosity for the current mode. JSON prints no chrome, so it
    /// reads as [`Verbosity::Silent`].
    pub fn current() -> Self {
        match OutputMode::current() {
            OutputMode::Human(verbosity) => verbosity,
            OutputMode::Json => Self::Silent,
        }
    }

    /// Whether the final result line should print.
    pub fn show_quiet(self) -> bool {
        self >= Self::Quiet
    }

    /// Whether normal chrome should print.
    pub fn show_normal(self) -> bool {
        self >= Self::Normal
    }

    /// Whether verbose sub-steps should print.
    pub fn show_verbose(self) -> bool {
        self >= Self::Verbose
    }
}

// ============================================================================
// Results (stdout)
// ============================================================================

/// Print a command's result: compact JSON on stdout in JSON mode, otherwise
/// the human rendering. Nothing prints under [`Verbosity::Silent`].
///
/// This is the only path a command's result should take, so `--json` works
/// uniformly across commands.
pub fn emit<T: Serialize + ?Sized>(
    value: &T,
    human: impl FnOnce(&T) -> eyre::Result<()>,
) -> eyre::Result<()> {
    match OutputMode::current() {
        OutputMode::Json => {
            let mut stdout = std::io::stdout().lock();
            serde_json::to_writer(&mut stdout, value)?;
            writeln!(stdout)?;
            Ok(())
        }
        OutputMode::Human(Verbosity::Silent) => Ok(()),
        OutputMode::Human(_) => human(value),
    }
}

// ============================================================================
// Chrome (stderr)
// ============================================================================

/// Whether an [`intro`] is open, so log lines draw the connecting side bar.
static SESSION_OPEN: AtomicBool = AtomicBool::new(false);

fn log_line(symbol: &str, text: &str) {
    eprint!(
        "{}",
        HelixTheme.format_log_with_spacing(text, symbol, SESSION_OPEN.load(Ordering::Relaxed))
    );
}

/// Open a session (`┌  title`). Later chrome connects to it with a side bar
/// until [`outro`] closes it.
pub fn intro(title: &str) {
    if Verbosity::current().show_normal() {
        eprint!("{}", HelixTheme.format_intro(title));
        SESSION_OPEN.store(true, Ordering::Relaxed);
    }
}

/// Close the session with a success message (`└  message`). Without an open
/// session (e.g. under `--quiet`) it prints a standalone success line.
pub fn outro(message: &str) {
    if !Verbosity::current().show_quiet() {
        return;
    }
    if SESSION_OPEN.swap(false, Ordering::Relaxed) {
        eprint!("{}", HelixTheme.format_outro(message));
    } else {
        log_line(&HelixTheme.active_symbol(), message);
    }
}

/// Close the session with a failure message.
pub fn outro_cancel(message: &str) {
    if !Verbosity::current().show_quiet() {
        return;
    }
    if SESSION_OPEN.swap(false, Ordering::Relaxed) {
        eprint!("{}", HelixTheme.format_outro_cancel(message));
    } else {
        log_line(&HelixTheme.error_symbol(), message);
    }
}

/// A completed step (`◇  message`).
pub fn step(message: &str) {
    if Verbosity::current().show_normal() {
        log_line(&HelixTheme.submit_symbol(), message);
    }
}

/// A success confirmation (`◆  message`). Shown under `--quiet` because it is
/// the final result of an action command.
pub fn success(message: &str) {
    if Verbosity::current().show_quiet() {
        log_line(&HelixTheme.active_symbol(), message);
    }
}

/// An informational line (`●  message`).
pub fn info(message: &str) {
    if Verbosity::current().show_normal() {
        log_line(&HelixTheme.info_symbol(), message);
    }
}

/// A warning (`▲  message`).
pub fn warning(message: &str) {
    if Verbosity::current().show_normal() {
        log_line(&HelixTheme.warning_symbol(), message);
    }
}

/// A dim aside (`├  message`), e.g. which resource was picked automatically.
pub fn remark(message: &str) {
    if Verbosity::current().show_normal() {
        log_line(
            &HelixTheme.remark_symbol(),
            &style(message).dim().for_stderr().to_string(),
        );
    }
}

/// A verbose-only dim aside.
pub fn verbose(message: &str) {
    if Verbosity::current().show_verbose() {
        remark(message);
    }
}

/// A boxed note, e.g. connection details after `helix start`.
pub fn note(title: &str, body: &str) {
    if Verbosity::current().show_normal() {
        eprint!("{}", HelixTheme.format_note(title, body));
    }
}

/// A numbered "Next steps" note.
pub fn next_steps<S: AsRef<str>>(steps: &[S]) {
    let body = steps
        .iter()
        .enumerate()
        .map(|(index, step)| format!("{}. {}", index + 1, step.as_ref()))
        .collect::<Vec<_>>()
        .join("\n");
    note("Next steps", &body);
}

/// Print an error to stderr without ending the process: the cliclack-styled
/// block in human mode, or `{"error": {...}}` in JSON mode.
pub fn print_error(error: &CliError) {
    match OutputMode::current() {
        OutputMode::Json => {
            /// Field order is part of the contract, so serialize a struct
            /// rather than a `serde_json::Map` (which sorts keys).
            #[derive(Serialize)]
            struct Envelope<'a> {
                error: &'a CliError,
            }
            let json = serde_json::to_string(&Envelope { error })
                .expect("CliError holds only strings and always serializes");
            eprintln!("{json}");
        }
        OutputMode::Human(_) => eprint!("{}", error.render()),
    }
}

/// Report the error that failed the command, closing any open session.
pub fn report_error(report: &eyre::Report) {
    print_error(&CliError::from_report(report));
    if !OutputMode::current().is_json() && SESSION_OPEN.swap(false, Ordering::Relaxed) {
        eprintln!("{}", style("└").dim().for_stderr());
    }
}

/// Format a duration for display (e.g. "1.2s", "150ms").
pub fn format_duration(duration: Duration) -> String {
    let millis = duration.as_millis();
    if millis < 1000 {
        format!("{millis}ms")
    } else {
        format!("{:.1}s", duration.as_secs_f64())
    }
}

// ============================================================================
// Operation
// ============================================================================

/// A top-level action drawn as a session: `┌  Starting dev` … `└  Started dev`.
///
/// ```
/// use helix_cli::output::Operation;
///
/// let op = Operation::new("Building", "dev");
/// let mut step = op.step("Compiling");
/// step.start();
/// step.done();
/// op.success();
/// ```
pub struct Operation {
    verb: String,
    target: String,
    started: Instant,
}

impl Operation {
    pub fn new(verb: &str, target: &str) -> Self {
        intro(&format!("{verb} {target}"));
        Self {
            verb: verb.to_owned(),
            target: target.to_owned(),
            started: Instant::now(),
        }
    }

    pub fn step(&self, description: &str) -> Step {
        Step::with_messages(description, description)
    }

    /// Close the session with the past-tense verb, plus timing when verbose.
    pub fn success(self) {
        let mut message = format!("{} {}", past_tense(&self.verb), self.target);
        if Verbosity::current().show_verbose() {
            message.push_str(&format!(
                " {}",
                style(format!("({})", format_duration(self.started.elapsed())))
                    .dim()
                    .for_stderr()
            ));
        }
        outro(&message);
    }

    /// Close the session as failed. The error itself is reported separately.
    pub fn failure(self) {
        outro_cancel(&format!("{} {} failed", self.verb, self.target));
    }
}

// ============================================================================
// Step
// ============================================================================

/// One unit of work inside an operation: a live spinner on an interactive
/// terminal, plain start/finish lines elsewhere, nothing when quiet.
///
/// ```
/// use helix_cli::output::Step;
///
/// let mut step = Step::with_messages("Pulling image", "Image pulled");
/// step.start();
/// step.set_message("Pulling image (layer 2/3)");
/// step.done_with_info("3 layers");
/// ```
pub struct Step {
    progress: String,
    completion: String,
    run: Option<Run>,
}

struct Run {
    started: Instant,
    surface: Surface,
}

enum Surface {
    /// Animated spinner on an interactive stderr.
    Live(ProgressBar),
    /// Plain lines: verbose mode, or stderr is not a terminal (CI logs).
    /// indicatif hides its output entirely off a TTY, so this keeps
    /// completion lines visible there.
    Plain,
    /// Quiet, silent, or JSON.
    Hidden,
}

impl Step {
    /// A step with separate progress and completion messages, e.g.
    /// `("Pulling image", "Image pulled")`.
    pub fn with_messages(progress: &str, completion: &str) -> Self {
        Self {
            progress: progress.to_owned(),
            completion: completion.to_owned(),
            run: None,
        }
    }

    pub fn start(&mut self) {
        let surface = match Verbosity::current() {
            Verbosity::Silent | Verbosity::Quiet => Surface::Hidden,
            Verbosity::Normal if std::io::stderr().is_terminal() => {
                let bar = ProgressBar::new_spinner();
                bar.set_style(
                    ProgressStyle::with_template("{spinner:.208}  {msg}")
                        .expect("valid spinner template")
                        .tick_strings(&["◒", "◐", "◓", "◑", "◇"]),
                );
                bar.set_message(self.progress.clone());
                bar.enable_steady_tick(SPINNER_TICK);
                Surface::Live(bar)
            }
            Verbosity::Normal | Verbosity::Verbose => {
                log_line(
                    &style("◒").color256(208).for_stderr().to_string(),
                    &format!("{}…", self.progress),
                );
                Surface::Plain
            }
        };
        self.run = Some(Run {
            started: Instant::now(),
            surface,
        });
    }

    /// Print a line above the live spinner without breaking the animation.
    pub fn println(&self, message: &str) {
        match self.run.as_ref().map(|run| &run.surface) {
            Some(Surface::Live(bar)) => bar.println(message),
            Some(Surface::Hidden) => {}
            Some(Surface::Plain) | None => {
                if Verbosity::current().show_normal() {
                    eprintln!("{message}");
                }
            }
        }
    }

    /// Replace the live spinner's message in place. No-op without a spinner.
    pub fn set_message(&self, message: &str) {
        let Some(Run {
            surface: Surface::Live(bar),
            ..
        }) = self.run.as_ref()
        else {
            return;
        };
        bar.set_message(message.to_owned());
    }

    /// Override the completion message, e.g. once cost or duration is known.
    pub fn set_completion(&mut self, message: &str) {
        self.completion = message.to_owned();
    }

    pub fn done(mut self) {
        self.finish(true, None);
    }

    /// Finish with a dim parenthesised detail: `◇  Image pulled (3 layers)`.
    pub fn done_with_info(mut self, info: &str) {
        self.finish(true, Some(info));
    }

    pub fn fail(mut self) {
        self.finish(false, None);
    }

    fn finish(&mut self, success: bool, info: Option<&str>) {
        let started = match self.run.take() {
            Some(Run {
                surface: Surface::Hidden,
                ..
            }) => return,
            Some(Run {
                surface: Surface::Live(bar),
                started,
            }) => {
                bar.finish_and_clear();
                Some(started)
            }
            Some(Run {
                surface: Surface::Plain,
                started,
            }) => Some(started),
            None if Verbosity::current().show_normal() => None,
            None => return,
        };

        let timing = started
            .filter(|_| Verbosity::current().show_verbose())
            .map(|started| format_duration(started.elapsed()));
        let message = std::iter::once(self.completion.clone())
            .chain(
                info.map(str::to_owned)
                    .into_iter()
                    .chain(timing)
                    .map(|detail| style(format!("({detail})")).dim().for_stderr().to_string()),
            )
            .collect::<Vec<_>>()
            .join(" ");
        let symbol = if success {
            HelixTheme.submit_symbol()
        } else {
            HelixTheme.error_symbol()
        };
        log_line(&symbol, &message);
    }
}

/// Past tense of the CLI's operation verbs.
fn past_tense(verb: &str) -> String {
    let lower = verb.to_lowercase();
    match lower.as_str() {
        "adding" => "Added",
        "bootstrapping" => "Bootstrapped",
        "building" => "Built",
        "creating" => "Created",
        "deleting" => "Deleted",
        "initializing" => "Initialized",
        "linking" => "Linked",
        "pruning" => "Pruned",
        "pulling" => "Pulled",
        "restarting" => "Restarted",
        "running" => "Ran",
        "starting" => "Started",
        "stopping" => "Stopped",
        "updating" => "Updated",
        _ => {
            let stem = lower.trim_end_matches("ing");
            let mut chars = stem.chars();
            return chars.next().map_or_else(String::new, |first| {
                format!("{}{}ed", first.to_uppercase(), chars.as_str())
            });
        }
    }
    .to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn past_tense_covers_known_and_regular_verbs() {
        assert_eq!(past_tense("Building"), "Built");
        assert_eq!(past_tense("Restarting"), "Restarted");
        assert_eq!(past_tense("Running"), "Ran");
        assert_eq!(past_tense("Linking"), "Linked");
        assert_eq!(past_tense("Checking"), "Checked");
    }

    #[test]
    fn durations_switch_units_at_one_second() {
        assert_eq!(format_duration(Duration::from_millis(50)), "50ms");
        assert_eq!(format_duration(Duration::from_millis(999)), "999ms");
        assert_eq!(format_duration(Duration::from_millis(1500)), "1.5s");
    }

    #[test]
    fn verbosity_levels_are_ordered() {
        assert!(Verbosity::Silent < Verbosity::Quiet);
        assert!(Verbosity::Quiet < Verbosity::Normal);
        assert!(Verbosity::Normal < Verbosity::Verbose);
        assert!(!Verbosity::Silent.show_quiet());
        assert!(Verbosity::Quiet.show_quiet());
        assert!(!Verbosity::Quiet.show_normal());
        assert!(Verbosity::Normal.show_normal());
        assert!(!Verbosity::Normal.show_verbose());
        assert!(Verbosity::Verbose.show_verbose());
    }

    #[test]
    fn mode_round_trips_through_the_global() {
        let _lock = MODE_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for mode in [
            OutputMode::Human(Verbosity::Silent),
            OutputMode::Human(Verbosity::Quiet),
            OutputMode::Human(Verbosity::Normal),
            OutputMode::Human(Verbosity::Verbose),
            OutputMode::Json,
        ] {
            mode.set();
            assert_eq!(OutputMode::current(), mode);
        }
        assert_eq!(Verbosity::current(), Verbosity::Silent);
        OutputMode::Human(Verbosity::Normal).set();
    }

    #[test]
    fn json_flag_wins_over_verbosity_flags() {
        assert_eq!(OutputMode::from_flags(true, true, true), OutputMode::Json);
        assert_eq!(
            OutputMode::from_flags(false, true, true),
            OutputMode::Human(Verbosity::Quiet)
        );
        assert_eq!(
            OutputMode::from_flags(false, false, true),
            OutputMode::Human(Verbosity::Verbose)
        );
    }

    #[test]
    fn emit_prints_json_or_human_and_nothing_when_silent() {
        let _lock = MODE_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let rendered = std::cell::Cell::new(0);
        for (mode, expected) in [
            (OutputMode::Json, 0),
            (OutputMode::Human(Verbosity::Silent), 0),
            (OutputMode::Human(Verbosity::Quiet), 1),
            (OutputMode::Human(Verbosity::Normal), 1),
        ] {
            mode.set();
            rendered.set(0);
            emit(&serde_json::json!({"ok": true}), |_| {
                rendered.set(rendered.get() + 1);
                Ok(())
            })
            .unwrap();
            assert_eq!(rendered.get(), expected, "{mode:?}");
        }
        OutputMode::Human(Verbosity::Normal).set();
    }

    #[test]
    fn chrome_and_steps_run_in_every_mode() {
        let _lock = MODE_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for mode in [
            OutputMode::Json,
            OutputMode::Human(Verbosity::Silent),
            OutputMode::Human(Verbosity::Quiet),
            OutputMode::Human(Verbosity::Normal),
            OutputMode::Human(Verbosity::Verbose),
        ] {
            mode.set();
            let operation = Operation::new("Checking", "test");
            let mut step = operation.step("Checking files");
            step.start();
            step.println("progress");
            step.set_message("still checking");
            step.set_completion("Files checked");
            step.done_with_info("2 files");
            let mut failed = Step::with_messages("Starting", "Started");
            failed.start();
            failed.fail();
            Step::with_messages("Never started", "Done").done();
            super::step("step");
            success("done");
            info("info");
            warning("warning");
            remark("remark");
            verbose("verbose");
            note("Title", "body");
            next_steps(&["one", "two"]);
            operation.success();
            Operation::new("Checking", "test").failure();
            outro("standalone");
            outro_cancel("standalone");
            report_error(&eyre::eyre!("boom"));
        }
        OutputMode::Human(Verbosity::Normal).set();
    }
}
