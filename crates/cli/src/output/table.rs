//! Borderless, column-aligned tables and key/value views for stdout.
//!
//! Widths are measured with [`console::measure_text_width`], so cells may
//! contain ANSI styling without breaking alignment.

use console::{measure_text_width, pad_str, style, Alignment};

/// A header row plus data rows, rendered with a two-space gutter.
///
/// ```
/// use helix_cli::output::table::Table;
///
/// let mut table = Table::new(["NAME", "ID"]);
/// table.row(["Acme", "ws-1"]);
/// table.row(["Beta Corp", "ws-2"]);
/// let rendered = console::strip_ansi_codes(&table.render()).to_string();
/// assert_eq!(rendered, "NAME       ID\nAcme       ws-1\nBeta Corp  ws-2\n");
/// ```
pub struct Table {
    headers: Vec<String>,
    rows: Vec<Vec<String>>,
}

impl Table {
    pub fn new<const N: usize>(headers: [&str; N]) -> Self {
        Self {
            headers: headers.into_iter().map(str::to_owned).collect(),
            rows: Vec::new(),
        }
    }

    /// Append a row. It must have one cell per header.
    pub fn row<S: Into<String>>(&mut self, cells: impl IntoIterator<Item = S>) {
        let cells: Vec<String> = cells.into_iter().map(Into::into).collect();
        assert_eq!(
            cells.len(),
            self.headers.len(),
            "table row must have one cell per header"
        );
        self.rows.push(cells);
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn render(&self) -> String {
        let widths: Vec<usize> = (0..self.headers.len())
            .map(|column| {
                std::iter::once(&self.headers)
                    .chain(&self.rows)
                    .map(|row| measure_text_width(&row[column]))
                    .max()
                    .unwrap_or(0)
            })
            .collect();
        let header: Vec<String> = self
            .headers
            .iter()
            .map(|cell| style(cell).bold().dim().to_string())
            .collect();
        std::iter::once(&header)
            .chain(&self.rows)
            .map(|row| {
                let line = row
                    .iter()
                    .zip(&widths)
                    .map(|(cell, width)| pad_str(cell, *width, Alignment::Left, None))
                    .collect::<Vec<_>>()
                    .join("  ");
                format!("{}\n", line.trim_end())
            })
            .collect()
    }

    pub fn print(&self) {
        print!("{}", self.render());
    }
}

/// Aligned `key  value` lines with dim keys, e.g. a `get` command's detail
/// view. Pairs with an empty value are skipped.
///
/// ```
/// use helix_cli::output::table::key_values;
///
/// let rendered = key_values(&[("Name", "Acme".into()), ("Region", String::new()), ("ID", "ws-1".into())]);
/// assert_eq!(console::strip_ansi_codes(&rendered), "Name  Acme\nID    ws-1\n");
/// ```
pub fn key_values(pairs: &[(&str, String)]) -> String {
    let pairs: Vec<_> = pairs
        .iter()
        .filter(|(_, value)| !value.is_empty())
        .collect();
    let width = pairs
        .iter()
        .map(|(key, _)| measure_text_width(key))
        .max()
        .unwrap_or(0);
    pairs
        .iter()
        .map(|(key, value)| {
            format!(
                "{}  {value}\n",
                style(pad_str(key, width, Alignment::Left, None)).dim()
            )
        })
        .collect()
}

/// A resource or container state coloured by health: green when up, red
/// when failed or unreachable, dim otherwise (pending, not created, …).
pub fn state(state: &str) -> String {
    let lower = state.to_lowercase();
    let healthy =
        lower.starts_with("up") || matches!(lower.as_str(), "running" | "active" | "ready");
    let failed = lower.starts_with("exited")
        || matches!(lower.as_str(), "unreachable" | "failed" | "unknown");
    let styled = match (healthy, failed) {
        (true, _) => style(state).green(),
        (false, true) => style(state).red(),
        (false, false) => style(state).dim(),
    };
    styled.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn columns_align_to_the_widest_cell_including_styled_cells() {
        let mut table = Table::new(["A", "LONG HEADER", "C"]);
        table.row([
            style("wide cell").green().force_styling(true).to_string(),
            "x".into(),
            "".into(),
        ]);
        table.row(["y", "z", "last"]);
        let rendered = console::strip_ansi_codes(&table.render()).to_string();
        assert_eq!(
            rendered,
            "A          LONG HEADER  C\nwide cell  x\ny          z            last\n"
        );
    }

    #[test]
    fn empty_tables_render_only_the_header() {
        let table = Table::new(["NAME"]);
        assert!(table.is_empty());
        assert_eq!(console::strip_ansi_codes(&table.render()), "NAME\n");
    }

    #[test]
    #[should_panic(expected = "one cell per header")]
    fn rows_must_match_the_header_width() {
        Table::new(["A", "B"]).row(["only one"]);
    }

    #[test]
    fn state_colouring_never_changes_the_text() {
        for state in [
            "Up 3 minutes",
            "active",
            "exited (1)",
            "not created",
            "unreachable",
        ] {
            assert_eq!(console::strip_ansi_codes(&super::state(state)), state);
        }
    }

    #[test]
    fn key_values_are_empty_without_values() {
        assert_eq!(key_values(&[("Name", String::new())]), "");
    }
}
