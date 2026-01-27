//! Clean, modern CLI output system with verbosity control.
//!
//! Provides a structured approach to CLI output with three verbosity levels:
//! - **Quiet**: Only errors and final results
//! - **Normal**: Key milestones with spinners for long operations
//! - **Verbose**: All sub-steps with timing information

use color_eyre::owo_colors::OwoColorize;
use indicatif::{ProgressBar, ProgressStyle};
use std::io::IsTerminal;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::{Duration, Instant};

// ============================================================================
// Verbosity Control
// ============================================================================

/// Global verbosity level (atomic for thread safety)
static VERBOSITY: AtomicU8 = AtomicU8::new(1); // Default: Normal

/// Output verbosity levels
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Verbosity {
    /// Only errors and final result
    Quiet = 0,
    /// Key milestones with spinners (default)
    Normal = 1,
    /// All sub-steps with timing
    Verbose = 2,
}

impl Verbosity {
    /// Get the current global verbosity level
    pub fn current() -> Self {
        match VERBOSITY.load(Ordering::Relaxed) {
            0 => Verbosity::Quiet,
            2 => Verbosity::Verbose,
            _ => Verbosity::Normal,
        }
    }

    /// Set the global verbosity level
    pub fn set(level: Verbosity) {
        VERBOSITY.store(level as u8, Ordering::Relaxed);
    }

    /// Set verbosity from CLI flags
    pub fn from_flags(quiet: bool, verbose: bool) -> Self {
        if quiet {
            Verbosity::Quiet
        } else if verbose {
            Verbosity::Verbose
        } else {
            Verbosity::Normal
        }
    }

    /// Check if we should show normal output
    pub fn show_normal(&self) -> bool {
        *self >= Verbosity::Normal
    }

    /// Check if we should show verbose output
    pub fn show_verbose(&self) -> bool {
        *self >= Verbosity::Verbose
    }
}

impl PartialOrd for Verbosity {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some((*self as u8).cmp(&(*other as u8)))
    }
}

// ============================================================================
// Symbols
// ============================================================================

/// Unicode symbols for terminal output
pub mod symbols {
    /// Success checkmark: ✓
    pub const SUCCESS: &str = "✓";
    /// Failure X: ✗
    pub const FAILURE: &str = "✗";
    /// Warning triangle: ⚠
    pub const WARNING: &str = "⚠";
    /// Info arrow: →
    pub const INFO: &str = "→";
}

// ============================================================================
// Duration Formatting
// ============================================================================

/// Format a duration for display (e.g., "1.2s", "150ms")
pub fn format_duration(duration: Duration) -> String {
    let millis = duration.as_millis();
    if millis < 1000 {
        format!("{}ms", millis)
    } else {
        format!("{:.1}s", duration.as_secs_f64())
    }
}

// ============================================================================
// Operation - Top Level
// ============================================================================

/// A top-level operation (e.g., "Building 'dev'")
///
/// # Example
/// ```ignore
/// let op = Operation::new("Building", "dev");
/// // ... do work with op.step() calls ...
/// op.success();
/// ```
pub struct Operation {
    verb: String,
    target: String,
    start_time: Instant,
}

impl Operation {
    /// Create a new operation with verb and target
    ///
    /// Prints the operation header in normal/verbose mode:
    /// `Building 'dev'`
    pub fn new(verb: &str, target: &str) -> Self {
        let op = Self {
            verb: verb.to_string(),
            target: target.to_string(),
            start_time: Instant::now(),
        };

        if Verbosity::current().show_normal() {
            println!("{} '{}'", verb, target);
        }

        op
    }

    /// Create a step within this operation
    #[allow(dead_code)]
    pub fn step(&self, description: &str) -> Step {
        Step::new(description)
    }

    /// Mark the operation as successful
    ///
    /// In quiet mode: `Built 'dev'`
    /// In normal mode: `Built 'dev' successfully` (bold, with newline before)
    /// In verbose mode: `Built 'dev' successfully (2.3s)` (bold, with newline before)
    pub fn success(self) {
        let verb_past = past_tense(&self.verb);
        let duration = self.start_time.elapsed();

        match Verbosity::current() {
            Verbosity::Quiet => {
                println!("{} '{}'", verb_past, self.target);
            }
            Verbosity::Normal => {
                println!();
                println!(
                    "{}",
                    format!("{} '{}' successfully", verb_past, self.target).bold()
                );
            }
            Verbosity::Verbose => {
                println!();
                println!(
                    "{} {}",
                    format!("{} '{}' successfully", verb_past, self.target).bold(),
                    format!("({})", format_duration(duration)).dimmed()
                );
            }
        }
    }

    /// Print success details with divider and bullet points
    pub fn print_details(items: &[(&str, &str)]) {
        println!("{}", "────────────────────────────────".dimmed());
        for (label, value) in items {
            println!("  {} {}: {}", "•".dimmed(), label, value);
        }
    }

    /// Mark the operation as failed (does not print the error itself)
    pub fn failure(self) {
        let duration = self.start_time.elapsed();

        match Verbosity::current() {
            Verbosity::Quiet => {
                println!(
                    "{} {} '{}' failed",
                    symbols::FAILURE.red().bold(),
                    self.verb,
                    self.target
                );
            }
            _ => {
                println!(
                    "{} {} '{}' failed {}",
                    symbols::FAILURE.red().bold(),
                    self.verb,
                    self.target,
                    format!("({})", format_duration(duration)).dimmed()
                );
            }
        }
    }
}

// ============================================================================
// Step - Individual Steps
// ============================================================================

/// An individual step within an operation
///
/// # Example
/// ```ignore
/// let mut step = op.step("Repository synced");
/// step.start();
/// // ... do work ...
/// step.done();
/// ```
pub struct Step {
    /// Message shown during spinner (e.g., "Building Docker image")
    progress_message: String,
    /// Message shown on completion (e.g., "Docker image built")
    completion_message: String,
    spinner: Option<LiveSpinner>,
    start_time: Option<Instant>,
}

impl Step {
    /// Create a new step with the same message for progress and completion
    #[allow(dead_code)]
    fn new(description: &str) -> Self {
        Self {
            progress_message: description.to_string(),
            completion_message: description.to_string(),
            spinner: None,
            start_time: None,
        }
    }

    /// Create a step with separate progress and completion messages
    ///
    /// Example: `Step::with_messages("Building Docker image", "Docker image built")`
    pub fn with_messages(progress: &str, completion: &str) -> Self {
        Self {
            progress_message: progress.to_string(),
            completion_message: completion.to_string(),
            spinner: None,
            start_time: None,
        }
    }

    /// Start the step (shows spinner in normal mode, text in verbose mode)
    pub fn start(&mut self) {
        self.start_time = Some(Instant::now());

        match Verbosity::current() {
            Verbosity::Quiet => {}
            Verbosity::Normal => {
                self.spinner = Some(LiveSpinner::new(&self.progress_message));
            }
            Verbosity::Verbose => {
                println!("  {} {}...", symbols::INFO.blue(), self.progress_message);
            }
        }
    }

    /// Mark the step as done
    ///
    /// Normal: `  ✓ Repository synced`
    /// Verbose: `  ✓ Repository synced (150ms)`
    pub fn done(mut self) {
        self.finish_with_status(true, None);
    }

    /// Mark the step as done with additional info
    ///
    /// Normal: `  ✓ Queries compiled (5 queries)`
    /// Verbose: `  ✓ Queries compiled (5 queries) (150ms)`
    pub fn done_with_info(mut self, info: &str) {
        self.finish_with_status(true, Some(info));
    }

    /// Mark the step as failed
    pub fn fail(mut self) {
        self.finish_with_status(false, None);
    }

    /// Internal: finish step with status
    fn finish_with_status(&mut self, success: bool, info: Option<&str>) {
        // Stop spinner if running
        if let Some(spinner) = self.spinner.take() {
            spinner.finish();
        }

        let verbosity = Verbosity::current();
        if verbosity == Verbosity::Quiet {
            return;
        }

        let symbol = if success {
            symbols::SUCCESS.green().to_string()
        } else {
            symbols::FAILURE.red().to_string()
        };

        let duration_str = if verbosity == Verbosity::Verbose {
            self.start_time
                .map(|t| {
                    format!(
                        " {}",
                        format!("({})", format_duration(t.elapsed())).dimmed()
                    )
                })
                .unwrap_or_default()
        } else {
            String::new()
        };

        let info_str = info
            .map(|i| format!(" {}", format!("({})", i).dimmed()))
            .unwrap_or_default();

        println!(
            "  {} {}{}{}",
            symbol, self.completion_message, info_str, duration_str
        );
    }

    /// Print a verbose-only sub-step (e.g., parsing, analyzing)
    pub fn verbose_substep(message: &str) {
        if Verbosity::current().show_verbose() {
            println!("    {} {}", symbols::INFO.dimmed(), message.dimmed());
        }
    }
}

// ============================================================================
// LiveSpinner - Animated Spinner
// ============================================================================

/// Animated spinner for long-running operations
pub struct LiveSpinner {
    progress_bar: ProgressBar,
}

impl LiveSpinner {
    /// Create and start a new spinner
    pub fn new(message: &str) -> Self {
        let pb = if std::io::stdout().is_terminal() {
            let pb = ProgressBar::new_spinner();
            pb.set_style(
                ProgressStyle::default_spinner()
                    .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"])
                    .template("  {spinner:.blue} {msg}")
                    .expect("valid template"),
            );
            pb.set_message(message.to_string());
            pb.enable_steady_tick(Duration::from_millis(80));
            pb
        } else {
            // Non-TTY: just print the message
            println!("  {} {}...", symbols::INFO.blue(), message);
            ProgressBar::hidden()
        };

        Self { progress_bar: pb }
    }

    /// Update the spinner message
    #[allow(dead_code)]
    pub fn update(&self, message: &str) {
        if std::io::stdout().is_terminal() {
            self.progress_bar.set_message(message.to_string());
        }
    }

    /// Finish and clear the spinner (doesn't print anything)
    pub fn finish(self) {
        self.progress_bar.finish_and_clear();
    }
}

// ============================================================================
// Standalone Output Functions
// ============================================================================

/// Print a success message (used for simple confirmations outside operations)
pub fn success(message: &str) {
    println!("{} {}", symbols::SUCCESS.green().bold(), message);
}

/// Print a warning message
pub fn warning(message: &str) {
    if Verbosity::current().show_normal() {
        println!("{} {}", symbols::WARNING.yellow().bold(), message);
    }
}

/// Print an info message (normal and verbose only)
pub fn info(message: &str) {
    if Verbosity::current().show_normal() {
        println!("{} {}", symbols::INFO.blue(), message);
    }
}

/// Print a verbose-only message
#[allow(dead_code)]
pub fn verbose(message: &str) {
    if Verbosity::current().show_verbose() {
        println!("  {}", message.dimmed());
    }
}

// ============================================================================
// Helpers
// ============================================================================

/// Convert a verb to past tense for CLI verbs
fn past_tense(verb: &str) -> String {
    let lower = verb.to_lowercase();
    match lower.as_str() {
        "adding" => "Added",
        "backing up" => "Backed up",
        "building" | "build" => "Built",
        "checking" => "Checked",
        "compiling" => "Compiled",
        "deleting" => "Deleted",
        "deploying" => "Deployed",
        "initializing" => "Initialized",
        "pruning" => "Pruned",
        "pulling" => "Pulled",
        "starting" => "Started",
        "stopping" => "Stopped",
        "updating" => "Updated",
        _ => return format!("{}ed", lower.trim_end_matches("ing")),
    }
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_past_tense() {
        assert_eq!(past_tense("Building"), "Built");
        assert_eq!(past_tense("Deploying"), "Deployed");
        assert_eq!(past_tense("Starting"), "Started");
        assert_eq!(past_tense("Backing up"), "Backed up");
    }

    #[test]
    fn test_format_duration() {
        assert_eq!(format_duration(Duration::from_millis(50)), "50ms");
        assert_eq!(format_duration(Duration::from_millis(500)), "500ms");
        assert_eq!(format_duration(Duration::from_millis(1500)), "1.5s");
        assert_eq!(format_duration(Duration::from_secs(5)), "5.0s");
    }

    #[test]
    fn test_verbosity_ordering() {
        assert!(Verbosity::Quiet < Verbosity::Normal);
        assert!(Verbosity::Normal < Verbosity::Verbose);
        assert!(Verbosity::Quiet.show_normal() == false);
        assert!(Verbosity::Normal.show_normal() == true);
        assert!(Verbosity::Normal.show_verbose() == false);
        assert!(Verbosity::Verbose.show_verbose() == true);
    }
}
