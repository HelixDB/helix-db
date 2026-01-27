use eyre::{Result, eyre};
use std::net::TcpListener;

pub const DEFAULT_PORT: u16 = 6969;
const MAX_PORT_ATTEMPTS: u16 = 100;

/// Check if a port is available by attempting to bind
pub fn is_port_available(port: u16) -> bool {
    TcpListener::bind(("127.0.0.1", port)).is_ok()
}

/// Find the next available port starting from `starting_port`
pub fn find_available_port(starting_port: u16) -> Result<u16> {
    for offset in 0..MAX_PORT_ATTEMPTS {
        let port = starting_port.saturating_add(offset);
        if is_port_available(port) {
            return Ok(port);
        }
    }
    Err(eyre!(
        "Could not find available port in range {}-{}",
        starting_port,
        starting_port + MAX_PORT_ATTEMPTS - 1
    ))
}

/// Check port and return actual port to use (may differ from requested)
/// Returns (actual_port, port_was_changed)
pub fn ensure_port_available(requested_port: u16) -> Result<(u16, bool)> {
    if is_port_available(requested_port) {
        return Ok((requested_port, false));
    }
    let available = find_available_port(requested_port + 1)?;
    Ok((available, true))
}
