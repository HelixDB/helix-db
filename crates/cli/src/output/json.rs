//! Syntax-highlighted pretty JSON for human output (query results, API
//! responses). Layout and escaping are delegated to serde_json's
//! [`PrettyFormatter`]; this only wraps tokens in ANSI colours.

use serde::Serialize as _;
use serde_json::ser::{CompactFormatter, Formatter, PrettyFormatter};
use serde_json::Value;
use std::io;

const KEY: &[u8] = b"\x1b[34m";
const STRING: &[u8] = b"\x1b[32m";
const NUMBER: &[u8] = b"\x1b[33m";
const LITERAL: &[u8] = b"\x1b[35m";
const RESET: &[u8] = b"\x1b[0m";

/// Pretty-print `value`, highlighted when `color` is set. With colour off the
/// output is byte-identical to [`serde_json::to_string_pretty`].
///
/// ```
/// use helix_cli::output::json::pretty;
///
/// let value = serde_json::json!({"name": "Acme", "count": 2, "ok": true, "none": null});
/// assert_eq!(pretty(&value, false), serde_json::to_string_pretty(&value).unwrap());
/// assert_eq!(console::strip_ansi_codes(&pretty(&value, true)), pretty(&value, false));
/// ```
pub fn pretty(value: &Value, color: bool) -> String {
    let mut buffer = Vec::new();
    let result = if color {
        let formatter = Highlight {
            pretty: PrettyFormatter::new(),
            in_key: false,
        };
        value.serialize(&mut serde_json::Serializer::with_formatter(
            &mut buffer,
            formatter,
        ))
    } else {
        value.serialize(&mut serde_json::Serializer::pretty(&mut buffer))
    };
    // Serializing a `Value` into memory cannot fail, and the output is UTF-8.
    result.expect("serialize JSON value");
    String::from_utf8(buffer).expect("serde_json writes UTF-8")
}

struct Highlight<'a> {
    pretty: PrettyFormatter<'a>,
    in_key: bool,
}

impl Formatter for Highlight<'_> {
    fn write_null<W: ?Sized + io::Write>(&mut self, writer: &mut W) -> io::Result<()> {
        writer.write_all(LITERAL)?;
        writer.write_all(b"null")?;
        writer.write_all(RESET)
    }

    fn write_bool<W: ?Sized + io::Write>(&mut self, writer: &mut W, value: bool) -> io::Result<()> {
        writer.write_all(LITERAL)?;
        CompactFormatter.write_bool(writer, value)?;
        writer.write_all(RESET)
    }

    fn write_i64<W: ?Sized + io::Write>(&mut self, writer: &mut W, value: i64) -> io::Result<()> {
        writer.write_all(NUMBER)?;
        CompactFormatter.write_i64(writer, value)?;
        writer.write_all(RESET)
    }

    fn write_u64<W: ?Sized + io::Write>(&mut self, writer: &mut W, value: u64) -> io::Result<()> {
        writer.write_all(NUMBER)?;
        CompactFormatter.write_u64(writer, value)?;
        writer.write_all(RESET)
    }

    fn write_f64<W: ?Sized + io::Write>(&mut self, writer: &mut W, value: f64) -> io::Result<()> {
        writer.write_all(NUMBER)?;
        CompactFormatter.write_f64(writer, value)?;
        writer.write_all(RESET)
    }

    fn begin_string<W: ?Sized + io::Write>(&mut self, writer: &mut W) -> io::Result<()> {
        writer.write_all(if self.in_key { KEY } else { STRING })?;
        writer.write_all(b"\"")
    }

    fn end_string<W: ?Sized + io::Write>(&mut self, writer: &mut W) -> io::Result<()> {
        writer.write_all(b"\"")?;
        writer.write_all(RESET)
    }

    fn begin_array<W: ?Sized + io::Write>(&mut self, writer: &mut W) -> io::Result<()> {
        self.pretty.begin_array(writer)
    }

    fn end_array<W: ?Sized + io::Write>(&mut self, writer: &mut W) -> io::Result<()> {
        self.pretty.end_array(writer)
    }

    fn begin_array_value<W: ?Sized + io::Write>(
        &mut self,
        writer: &mut W,
        first: bool,
    ) -> io::Result<()> {
        self.pretty.begin_array_value(writer, first)
    }

    fn end_array_value<W: ?Sized + io::Write>(&mut self, writer: &mut W) -> io::Result<()> {
        self.pretty.end_array_value(writer)
    }

    fn begin_object<W: ?Sized + io::Write>(&mut self, writer: &mut W) -> io::Result<()> {
        self.pretty.begin_object(writer)
    }

    fn end_object<W: ?Sized + io::Write>(&mut self, writer: &mut W) -> io::Result<()> {
        self.pretty.end_object(writer)
    }

    fn begin_object_key<W: ?Sized + io::Write>(
        &mut self,
        writer: &mut W,
        first: bool,
    ) -> io::Result<()> {
        self.in_key = true;
        self.pretty.begin_object_key(writer, first)
    }

    fn end_object_key<W: ?Sized + io::Write>(&mut self, writer: &mut W) -> io::Result<()> {
        self.in_key = false;
        self.pretty.end_object_key(writer)
    }

    fn begin_object_value<W: ?Sized + io::Write>(&mut self, writer: &mut W) -> io::Result<()> {
        self.pretty.begin_object_value(writer)
    }

    fn end_object_value<W: ?Sized + io::Write>(&mut self, writer: &mut W) -> io::Result<()> {
        self.pretty.end_object_value(writer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn highlighting_preserves_layout_for_every_value_kind() {
        let value = json!({
            "string": "with \"escapes\" and \n newline",
            "int": -3,
            "uint": 18446744073709551615u64,
            "float": 1.5,
            "bool": false,
            "null": null,
            "empty_array": [],
            "empty_object": {},
            "nested": [{"key": [1, "two", {"three": 3}]}],
        });
        let plain = pretty(&value, false);
        assert_eq!(plain, serde_json::to_string_pretty(&value).unwrap());
        let highlighted = pretty(&value, true);
        assert_ne!(highlighted, plain);
        assert_eq!(console::strip_ansi_codes(&highlighted), plain);
    }

    #[test]
    fn keys_and_string_values_get_distinct_colours() {
        let highlighted = pretty(&json!({"k": "v"}), true);
        assert!(highlighted.contains("\x1b[34m\"k\"\x1b[0m"));
        assert!(highlighted.contains("\x1b[32m\"v\"\x1b[0m"));
    }

    #[test]
    fn scalars_render_without_containers() {
        assert_eq!(pretty(&json!("x"), false), "\"x\"");
        assert_eq!(
            console::strip_ansi_codes(&pretty(&json!(null), true)),
            "null"
        );
    }
}
