//! TUI mode for interactive log viewing.

use super::log_source::LogSource;
use chrono::{DateTime, Datelike, Duration, Timelike, Utc};
use crossterm::{
    event::{self, Event, KeyCode, KeyModifiers},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use eyre::Result;
use flume;
use ratatui::{
    Frame, Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Clear, List, ListItem, Paragraph},
};
use std::io;
use std::time::Duration as StdDuration;
use tokio::task::JoinHandle;

/// Orange highlight color matching Helix branding
const HELIX_ORANGE: Color = Color::Rgb(253, 169, 66);

/// Log viewing mode
#[derive(Clone, PartialEq)]
pub enum LogMode {
    Live,
    TimeRange,
    SelectingPreset,
    CustomRangeInput,
}

/// Field being edited in custom date picker
#[derive(Clone, Copy, PartialEq)]
pub enum PickerField {
    Year,
    Month,
    Day,
    Hour,
    Minute,
}

impl PickerField {
    fn next(self) -> Self {
        match self {
            PickerField::Year => PickerField::Month,
            PickerField::Month => PickerField::Day,
            PickerField::Day => PickerField::Hour,
            PickerField::Hour => PickerField::Minute,
            PickerField::Minute => PickerField::Year,
        }
    }

    fn prev(self) -> Self {
        match self {
            PickerField::Year => PickerField::Minute,
            PickerField::Month => PickerField::Year,
            PickerField::Day => PickerField::Month,
            PickerField::Hour => PickerField::Day,
            PickerField::Minute => PickerField::Hour,
        }
    }
}

/// Which datetime is being edited
#[derive(Clone, Copy, PartialEq)]
pub enum PickerTarget {
    Start,
    End,
}

/// Time range presets
const PRESETS: &[(&str, i64)] = &[
    ("Last 15 minutes", 15),
    ("Last 30 minutes", 30),
    ("Last hour", 60),
    ("Custom range...", 0),
];

fn days_in_month(year: i32, month: u32) -> i32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0) {
                29
            } else {
                28
            }
        }
        _ => 28,
    }
}

/// Application state
pub struct App {
    mode: LogMode,
    logs: Vec<String>,
    scroll_offset: usize,
    instance_name: String,
    log_source: LogSource,
    range_start: Option<DateTime<Utc>>,
    range_end: Option<DateTime<Utc>>,
    selected_preset: usize,
    // Custom picker state
    picker_field: PickerField,
    picker_target: PickerTarget,
    picker_start: DateTime<Utc>,
    picker_end: DateTime<Utc>,
    // Status message
    status_message: Option<String>,
    // Pending 'z' key for zt/zb commands
    pending_z: bool,
}

impl App {
    pub fn new(log_source: LogSource, instance_name: String) -> Self {
        let now = Utc::now();
        Self {
            mode: LogMode::Live,
            logs: Vec::with_capacity(5000),
            scroll_offset: 0,
            instance_name,
            log_source,
            range_start: None,
            range_end: None,
            selected_preset: 0,
            picker_field: PickerField::Year,
            picker_target: PickerTarget::Start,
            picker_start: now - Duration::hours(1),
            picker_end: now,
            status_message: None,
            pending_z: false,
        }
    }

    fn visible_log_lines(&self, height: usize) -> usize {
        height.saturating_sub(6) // Account for header and footer
    }

    fn max_scroll(&self, height: usize) -> usize {
        let visible = self.visible_log_lines(height);
        self.logs.len().saturating_sub(visible)
    }

    fn scroll_up(&mut self) {
        self.scroll_offset = self.scroll_offset.saturating_sub(1);
    }

    fn scroll_down(&mut self, height: usize) {
        let max = self.max_scroll(height);
        if self.scroll_offset < max {
            self.scroll_offset += 1;
        }
    }

    fn scroll_to_bottom(&mut self, height: usize) {
        self.scroll_offset = self.max_scroll(height);
    }

    fn adjust_picker_value(&mut self, delta: i32) {
        let dt = match self.picker_target {
            PickerTarget::Start => &mut self.picker_start,
            PickerTarget::End => &mut self.picker_end,
        };

        *dt = match self.picker_field {
            PickerField::Year => {
                let new_year = (dt.year() + delta).clamp(2020, 2030);
                dt.with_year(new_year).unwrap_or(*dt)
            }
            PickerField::Month => {
                let current = dt.month() as i32;
                let new_month = ((current - 1 + delta).rem_euclid(12) + 1) as u32;
                dt.with_month(new_month).unwrap_or(*dt)
            }
            PickerField::Day => {
                let current = dt.day() as i32;
                let max_days = days_in_month(dt.year(), dt.month());
                let new_day = ((current - 1 + delta).rem_euclid(max_days) + 1) as u32;
                dt.with_day(new_day).unwrap_or(*dt)
            }
            PickerField::Hour => {
                let current = dt.hour() as i32;
                let new_hour = (current + delta).rem_euclid(24) as u32;
                dt.with_hour(new_hour).unwrap_or(*dt)
            }
            PickerField::Minute => {
                let current = dt.minute() as i32;
                let new_minute = (current + delta).rem_euclid(60) as u32;
                dt.with_minute(new_minute).unwrap_or(*dt)
            }
        };
    }
}

/// Run the TUI application.
pub async fn run(log_source: LogSource, instance_name: String) -> Result<()> {
    // Setup terminal
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    // Create app state
    let mut app = App::new(log_source, instance_name);

    // Start in live mode - fetch initial logs
    app.status_message = Some("Loading logs...".to_string());

    let result = run_app(&mut terminal, &mut app).await;

    // Restore terminal
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    result
}

/// Spawn the SSE streaming task for live mode.
/// Returns a channel receiver and the task handle.
fn spawn_live_stream(log_source: LogSource) -> (flume::Receiver<String>, JoinHandle<Result<()>>) {
    let (tx, rx) = flume::unbounded::<String>();

    let handle = tokio::spawn(async move {
        log_source
            .stream_live(|line| {
                // Expand escaped newlines and send each line
                let expanded = line.replace("\\n", "\n");
                for l in expanded.lines() {
                    let _ = tx.send(l.to_string());
                }
            })
            .await
    });

    (rx, handle)
}

async fn run_app(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    app: &mut App,
) -> Result<()> {
    // Clear terminal for clean initial display
    terminal.clear()?;

    // Get initial size for log fetching
    let initial_size = terminal.size()?;

    // Initial log fetch for backfill (scrolls to bottom for newest logs)
    fetch_initial_logs(app, initial_size.height as usize).await?;

    // Start SSE streaming for live mode
    let (mut log_rx, mut stream_handle): (flume::Receiver<String>, JoinHandle<Result<()>>) =
        spawn_live_stream(app.log_source.clone());

    loop {
        let size = terminal.size()?;
        terminal.draw(|f| ui(f, app))?;

        // Use tokio::select! to handle both keyboard events and incoming logs
        tokio::select! {
            // Check for keyboard events (with short timeout for responsiveness)
            _ = tokio::time::sleep(StdDuration::from_millis(50)) => {
                if event::poll(StdDuration::ZERO)?
                    && let Event::Key(key) = event::read()?
                {
                    // Handle Ctrl+C globally
                    if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
                        stream_handle.abort();
                        return Ok(());
                    }

                    match app.mode {
                        LogMode::Live | LogMode::TimeRange => {
                            // Handle z-prefix commands (zt, zb)
                            if app.pending_z {
                                app.pending_z = false;
                                match key.code {
                                    KeyCode::Char('t') => {
                                        // zt - scroll to top (oldest logs)
                                        app.scroll_offset = 0;
                                    }
                                    KeyCode::Char('b') => {
                                        // zb - scroll to bottom (newest logs)
                                        app.scroll_to_bottom(size.height as usize);
                                    }
                                    _ => {} // Any other key cancels the z prefix
                                }
                            } else {
                                match key.code {
                                    KeyCode::Esc | KeyCode::Char('q') => {
                                        stream_handle.abort();
                                        return Ok(());
                                    }
                                    KeyCode::Char('z') => {
                                        app.pending_z = true;
                                    }
                                    KeyCode::Char('l') => {
                                        if app.mode != LogMode::Live {
                                            // Switch to live mode - restart streaming
                                            stream_handle.abort();
                                            app.mode = LogMode::Live;
                                            app.logs.clear();
                                            app.scroll_offset = 0;
                                            app.status_message =
                                                Some("Switching to live mode...".to_string());
                                            fetch_initial_logs(app, size.height as usize).await?;
                                            // Restart SSE stream
                                            let (rx, handle) = spawn_live_stream(app.log_source.clone());
                                            log_rx = rx;
                                            stream_handle = handle;
                                        }
                                    }
                                    KeyCode::Char('r') => {
                                        // Entering preset selection - stop streaming
                                        stream_handle.abort();
                                        app.mode = LogMode::SelectingPreset;
                                        app.selected_preset = 0;
                                    }
                                    KeyCode::Up => {
                                        app.scroll_up();
                                    }
                                    KeyCode::Down => {
                                        app.scroll_down(size.height as usize);
                                    }
                                    KeyCode::Char('k') | KeyCode::PageUp => {
                                        // k and PageUp - page up
                                        let jump = app.visible_log_lines(size.height as usize);
                                        app.scroll_offset = app.scroll_offset.saturating_sub(jump);
                                    }
                                    KeyCode::Char('j') | KeyCode::PageDown => {
                                        // j and PageDown - page down
                                        let jump = app.visible_log_lines(size.height as usize);
                                        let max = app.max_scroll(size.height as usize);
                                        app.scroll_offset = (app.scroll_offset + jump).min(max);
                                    }
                                    KeyCode::Home => {
                                        app.scroll_offset = 0;
                                    }
                                    KeyCode::End => {
                                        app.scroll_to_bottom(size.height as usize);
                                    }
                                    _ => {}
                                }
                            }
                        }
                        LogMode::SelectingPreset => {
                            match key.code {
                                KeyCode::Esc => {
                                    // Return to live mode - restart streaming
                                    app.mode = LogMode::Live;
                                    let (rx, handle) = spawn_live_stream(app.log_source.clone());
                                    log_rx = rx;
                                    stream_handle = handle;
                                }
                                KeyCode::Up | KeyCode::Char('k') => {
                                    if app.selected_preset > 0 {
                                        app.selected_preset -= 1;
                                    }
                                }
                                KeyCode::Down | KeyCode::Char('j') => {
                                    if app.selected_preset < PRESETS.len() - 1 {
                                        app.selected_preset += 1;
                                    }
                                }
                                KeyCode::Enter => {
                                    if app.selected_preset == PRESETS.len() - 1 {
                                        // Custom range
                                        app.mode = LogMode::CustomRangeInput;
                                        app.picker_field = PickerField::Year;
                                        app.picker_target = PickerTarget::Start;
                                        let now = Utc::now();
                                        app.picker_start = now - Duration::hours(1);
                                        app.picker_end = now;
                                    } else {
                                        // Preset selected - stay in time range mode (no streaming)
                                        let minutes = PRESETS[app.selected_preset].1;
                                        let now = Utc::now();
                                        app.range_start = Some(now - Duration::minutes(minutes));
                                        app.range_end = Some(now);
                                        app.mode = LogMode::TimeRange;
                                        app.logs.clear();
                                        app.scroll_offset = 0;
                                        app.status_message = Some("Fetching logs...".to_string());
                                        fetch_range_logs(app).await?;
                                    }
                                }
                                _ => {}
                            }
                        }
                        LogMode::CustomRangeInput => {
                            match key.code {
                                KeyCode::Esc => {
                                    app.mode = LogMode::SelectingPreset;
                                }
                                KeyCode::Left | KeyCode::Char('h') => {
                                    app.picker_field = app.picker_field.prev();
                                }
                                KeyCode::Right | KeyCode::Char('l') => {
                                    app.picker_field = app.picker_field.next();
                                }
                                KeyCode::Up | KeyCode::Char('k') => {
                                    app.adjust_picker_value(1);
                                }
                                KeyCode::Down | KeyCode::Char('j') => {
                                    app.adjust_picker_value(-1);
                                }
                                KeyCode::Tab => {
                                    app.picker_target = match app.picker_target {
                                        PickerTarget::Start => PickerTarget::End,
                                        PickerTarget::End => PickerTarget::Start,
                                    };
                                }
                                KeyCode::Enter => {
                                    // Validate and fetch
                                    if app.picker_start >= app.picker_end {
                                        app.status_message =
                                            Some("Start time must be before end time".to_string());
                                    } else {
                                        let duration =
                                            app.picker_end.signed_duration_since(app.picker_start);
                                        if duration > Duration::hours(1) {
                                            app.status_message =
                                                Some("Time range cannot exceed 1 hour".to_string());
                                        } else {
                                            app.range_start = Some(app.picker_start);
                                            app.range_end = Some(app.picker_end);
                                            app.mode = LogMode::TimeRange;
                                            app.logs.clear();
                                            app.scroll_offset = 0;
                                            app.status_message = Some("Fetching logs...".to_string());
                                            fetch_range_logs(app).await?;
                                        }
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
            // Check for incoming log messages from SSE stream (only in live mode)
            Ok(line) = log_rx.recv_async(), if app.mode == LogMode::Live => {
                let was_at_bottom = app.scroll_offset >= app.max_scroll(size.height as usize);
                app.logs.push(line);
                // Clear status message once we start receiving logs
                if app.status_message.as_deref() == Some("Switching to live mode...") {
                    app.status_message = None;
                }
                // Auto-scroll if user was at bottom
                if was_at_bottom {
                    app.scroll_to_bottom(size.height as usize);
                }
            }
        }
    }
}

/// Expand escaped newlines in log lines.
/// Splits lines containing literal `\n` into multiple lines for proper display.
fn expand_log_lines(logs: Vec<String>) -> Vec<String> {
    logs.into_iter()
        .flat_map(|line| {
            // Replace escaped \n with actual newlines, then split
            let expanded = line.replace("\\n", "\n");
            expanded.lines().map(String::from).collect::<Vec<_>>()
        })
        .collect()
}

async fn fetch_initial_logs(app: &mut App, height: usize) -> Result<()> {
    // For initial load, fetch last 15 minutes of logs
    let now = Utc::now();
    let start = now - Duration::minutes(15);

    match app.log_source.query_range(start, now).await {
        Ok(logs) => {
            app.logs = expand_log_lines(logs);
            app.status_message = None;
            // Scroll to bottom (newest logs) in live mode
            app.scroll_to_bottom(height);
        }
        Err(e) => {
            app.status_message = Some(format!("Error: {}", e));
        }
    }

    Ok(())
}

async fn fetch_range_logs(app: &mut App) -> Result<()> {
    if let (Some(start), Some(end)) = (app.range_start, app.range_end) {
        match app.log_source.query_range(start, end).await {
            Ok(logs) => {
                app.logs = expand_log_lines(logs);
                app.status_message = None;
            }
            Err(e) => {
                app.status_message = Some(format!("Error: {}", e));
            }
        }
    }
    Ok(())
}

fn ui(f: &mut Frame, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // Header
            Constraint::Min(5),    // Logs
            Constraint::Length(3), // Footer/Controls
        ])
        .split(f.area());

    // Header
    let mode_text = match app.mode {
        LogMode::Live => "LIVE",
        LogMode::TimeRange => "TIME RANGE",
        LogMode::SelectingPreset => "SELECT TIME RANGE",
        LogMode::CustomRangeInput => "CUSTOM RANGE",
    };

    let header_text = format!("Instance: {}  |  Mode: {}", app.instance_name, mode_text);
    let header = Paragraph::new(header_text)
        .style(Style::default().fg(HELIX_ORANGE))
        .block(Block::default().borders(Borders::ALL).title("Helix Logs"));
    f.render_widget(header, chunks[0]);

    // Main content area
    match app.mode {
        LogMode::SelectingPreset => {
            render_preset_menu(f, app, chunks[1]);
        }
        LogMode::CustomRangeInput => {
            render_custom_picker(f, app, chunks[1]);
        }
        _ => {
            render_logs(f, app, chunks[1]);
        }
    }

    // Footer
    render_footer(f, app, chunks[2]);
}

/// Strip ANSI escape codes from a string
fn strip_ansi_codes(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();

    while let Some(c) = chars.next() {
        if c == '\x1b' {
            // Skip escape sequence
            if chars.peek() == Some(&'[') {
                chars.next(); // consume '['
                // Skip until we hit a letter (end of escape sequence)
                while let Some(&next) = chars.peek() {
                    chars.next();
                    if next.is_ascii_alphabetic() {
                        break;
                    }
                }
            }
        } else if c.is_control() && c != '\t' {
            // Skip other control characters except tab
            continue;
        } else {
            result.push(c);
        }
    }

    result
}

fn render_logs(f: &mut Frame, app: &App, area: Rect) {
    let visible_height = area.height.saturating_sub(2) as usize;
    let start = app.scroll_offset;
    let end = (start + visible_height).min(app.logs.len());

    let items: Vec<ListItem> = app.logs[start..end]
        .iter()
        .map(|line| {
            // Strip ANSI codes for clean display
            let clean_line = strip_ansi_codes(line);
            let style = if clean_line.contains("ERROR") || clean_line.contains("error") {
                Style::default().fg(Color::Red)
            } else if clean_line.contains("WARN") || clean_line.contains("warn") {
                Style::default().fg(Color::Yellow)
            } else if clean_line.contains("INFO") || clean_line.contains("info") {
                Style::default().fg(Color::Green)
            } else if clean_line.contains("DEBUG") || clean_line.contains("debug") {
                Style::default().fg(Color::Cyan)
            } else {
                Style::default().fg(Color::Gray)
            };
            ListItem::new(Line::from(clean_line)).style(style)
        })
        .collect();

    let logs_block =
        Block::default()
            .borders(Borders::ALL)
            .title(format!("Logs ({}/{})", end, app.logs.len()));

    let list = List::new(items).block(logs_block);
    f.render_widget(list, area);
}

fn render_preset_menu(f: &mut Frame, app: &App, area: Rect) {
    // Clear the area first to prevent leftover content
    f.render_widget(Clear, area);

    let items: Vec<ListItem> = PRESETS
        .iter()
        .enumerate()
        .map(|(i, (label, _))| {
            let style = if i == app.selected_preset {
                Style::default()
                    .fg(Color::Black)
                    .bg(HELIX_ORANGE)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default()
            };
            ListItem::new(Line::from(*label)).style(style)
        })
        .collect();

    let block = Block::default()
        .borders(Borders::ALL)
        .title("Select Time Range");

    let list = List::new(items).block(block);
    f.render_widget(list, area);
}

fn render_custom_picker(f: &mut Frame, app: &App, area: Rect) {
    // Clear the area first to prevent leftover content
    f.render_widget(Clear, area);

    let inner = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Length(3),
            Constraint::Min(1),
        ])
        .split(area);

    // Start datetime
    let start_line = format_picker_line(
        "Start:",
        &app.picker_start,
        app.picker_target == PickerTarget::Start,
        app.picker_field,
    );
    let start_para = Paragraph::new(start_line).block(Block::default().borders(Borders::ALL));
    f.render_widget(start_para, inner[0]);

    // End datetime
    let end_line = format_picker_line(
        "End:  ",
        &app.picker_end,
        app.picker_target == PickerTarget::End,
        app.picker_field,
    );
    let end_para = Paragraph::new(end_line).block(Block::default().borders(Borders::ALL));
    f.render_widget(end_para, inner[1]);

    // Instructions
    let instructions = Paragraph::new(
        "Use arrow keys to adjust values, Tab to switch Start/End, Enter to confirm",
    )
    .style(Style::default().fg(Color::DarkGray));
    f.render_widget(instructions, inner[2]);
}

fn format_picker_line(
    label: &str,
    dt: &DateTime<Utc>,
    is_active: bool,
    active_field: PickerField,
) -> Line<'static> {
    let base_style = if is_active {
        Style::default().fg(HELIX_ORANGE)
    } else {
        Style::default().fg(Color::DarkGray)
    };

    let highlight_style = Style::default()
        .fg(Color::Black)
        .bg(HELIX_ORANGE)
        .add_modifier(Modifier::BOLD);

    let year_style = if is_active && active_field == PickerField::Year {
        highlight_style
    } else {
        base_style
    };
    let month_style = if is_active && active_field == PickerField::Month {
        highlight_style
    } else {
        base_style
    };
    let day_style = if is_active && active_field == PickerField::Day {
        highlight_style
    } else {
        base_style
    };
    let hour_style = if is_active && active_field == PickerField::Hour {
        highlight_style
    } else {
        base_style
    };
    let minute_style = if is_active && active_field == PickerField::Minute {
        highlight_style
    } else {
        base_style
    };

    Line::from(vec![
        Span::raw(format!("{} ", label)),
        Span::styled(format!("{:04}", dt.year()), year_style),
        Span::styled("-", base_style),
        Span::styled(format!("{:02}", dt.month()), month_style),
        Span::styled("-", base_style),
        Span::styled(format!("{:02}", dt.day()), day_style),
        Span::styled(" ", base_style),
        Span::styled(format!("{:02}", dt.hour()), hour_style),
        Span::styled(":", base_style),
        Span::styled(format!("{:02}", dt.minute()), minute_style),
        Span::styled(" UTC", base_style),
    ])
}

fn render_footer(f: &mut Frame, app: &App, area: Rect) {
    let content = match app.mode {
        LogMode::Live | LogMode::TimeRange => {
            if let Some(ref msg) = app.status_message {
                msg.clone()
            } else {
                "j/k: page | zt: top | zb: bottom | l: live | r: time range | esc: exit".to_string()
            }
        }
        LogMode::SelectingPreset => "up/down: select | enter: confirm | esc: back".to_string(),
        LogMode::CustomRangeInput => {
            if let Some(ref msg) = app.status_message {
                msg.clone()
            } else {
                "arrows: adjust | tab: switch | enter: confirm | esc: back".to_string()
            }
        }
    };

    let footer = Paragraph::new(content)
        .style(Style::default().fg(Color::DarkGray))
        .block(Block::default().borders(Borders::ALL));
    f.render_widget(footer, area);
}
