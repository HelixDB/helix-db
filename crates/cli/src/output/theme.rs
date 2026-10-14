//! The Helix cliclack theme: the default clack look with the Helix orange
//! accent, styled for stderr (where all prompts and chrome render) so colour
//! follows stderr's terminal and `NO_COLOR` rather than stdout's.

use cliclack::{Theme, ThemeState};
use console::{style, Style};

/// Helix brand orange in the 256-colour palette.
const ORANGE: u8 = 208;

/// Installed once in `main` with [`cliclack::set_theme`]; the output module
/// also calls its trait methods directly for non-prompt chrome.
pub struct HelixTheme;

impl Theme for HelixTheme {
    fn bar_color(&self, state: &ThemeState) -> Style {
        let style = Style::new().for_stderr();
        match state {
            ThemeState::Active => style.color256(ORANGE),
            ThemeState::Cancel => style.red(),
            ThemeState::Submit => style.bright().black(),
            ThemeState::Error(_) => style.yellow(),
        }
    }

    fn state_symbol_color(&self, state: &ThemeState) -> Style {
        match state {
            ThemeState::Submit => Style::new().for_stderr().green(),
            _ => self.bar_color(state),
        }
    }

    fn info_symbol(&self) -> String {
        style("●").for_stderr().blue().to_string()
    }

    fn warning_symbol(&self) -> String {
        style("▲").for_stderr().yellow().to_string()
    }

    fn error_symbol(&self) -> String {
        style("■").for_stderr().red().to_string()
    }

    fn active_symbol(&self) -> String {
        style("◆").for_stderr().green().to_string()
    }

    fn submit_symbol(&self) -> String {
        style("◇").for_stderr().green().to_string()
    }
}
