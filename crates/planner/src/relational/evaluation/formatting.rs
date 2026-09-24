//! Prepare primitive text without allocating, then write into admitted capacity.
//! The byte count preserves the existing Cypher finite-float decimal suffix.
use super::{QueryError, Result, Value};
use std::fmt::{self, Write as _};

// Formatting preparation allocates nothing; callers admit bytes before writing.
pub(super) struct ScalarText<'a> {
    display: &'a dyn fmt::Display,
    bytes: usize,
    decimal_suffix: bool,
}

impl<'a> ScalarText<'a> {
    pub(super) fn new(value: &'a Value) -> Result<Self> {
        let display: &dyn fmt::Display = match value {
            Value::Integer(value) => value,
            Value::Float(value) => value,
            Value::Boolean(value) => value,
            _ => {
                return Err(QueryError::runtime(
                    "TypeError",
                    "InvalidArgumentType",
                    "value cannot be converted to a string",
                ))
            }
        };
        let mut size = FormatSize {
            bytes: 0,
            decimal: false,
        };
        write!(&mut size, "{display}").expect("primitive formatting into a counter cannot fail");
        let decimal_suffix = matches!(value, Value::Float(f) if f.is_finite() && !size.decimal);
        Ok(Self {
            display,
            bytes: size.bytes.saturating_add(usize::from(decimal_suffix) * 2),
            decimal_suffix,
        })
    }

    pub(super) fn bytes(&self) -> usize {
        self.bytes
    }

    pub(super) fn write_to(&self, output: &mut String) {
        assert!(
            output.capacity() - output.len() >= self.bytes,
            "scalar output capacity was admitted"
        );
        let before = output.len();
        write!(output, "{}", self.display).expect("formatting into an admitted String cannot fail");
        if self.decimal_suffix {
            output.push_str(".0");
        }
        assert_eq!(
            output.len() - before,
            self.bytes,
            "stable scalar formatting"
        );
    }
}

struct FormatSize {
    bytes: usize,
    decimal: bool,
}
impl fmt::Write for FormatSize {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        self.bytes = self.bytes.saturating_add(text.len());
        self.decimal |= text.contains(['.', 'e', 'E']);
        Ok(())
    }
}
