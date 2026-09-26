//! Bounded numeric averaging, without overflowing an intermediate sum.
use super::{evaluation, Result, Value};

#[derive(Debug, Clone, Copy)]
pub(super) struct Average {
    total: Total,
    count: i64,
}

#[derive(Debug, Clone, Copy)]
enum Total {
    // Native i128 alignment would enlarge every accumulator, including count.
    // This private in-memory byte array preserves the ordinary alignment;
    // standard native-endian conversion retains the exact signed total.
    Integer([u8; size_of::<i128>()]),
    Finite(ScaledSum),
    NonFinite(f64),
}

#[derive(Debug, Clone, Copy, Default)]
struct ScaledSum {
    scale: f64,
    sum: f64,
    correction: f64,
}

impl ScaledSum {
    fn push_integer(self, value: i128) -> Self {
        let high = value as f64;
        // Preserve the integer residue when promoting to floating state.
        // The bounded integer total keeps the reverse conversion in range.
        let low = (value - high as i128) as f64;
        self.push(high).push(low)
    }

    fn push(mut self, value: f64) -> Self {
        debug_assert!(value.is_finite());
        if value == 0.0 {
            return self;
        }
        // A power-of-two scale avoids introducing a rounding division for
        // normal inputs. Keep subnormal inputs relative to the minimum normal.
        let exponent = value.to_bits() & (0x7ff_u64 << 52);
        let scale = f64::from_bits(exponent.max(1_u64 << 52));
        if scale > self.scale {
            let factor = self.scale / scale;
            self.sum *= factor;
            self.correction *= factor;
            self.scale = scale;
        }
        let term = value / self.scale;
        let sum = self.sum + term;
        self.correction += if self.sum.abs() >= term.abs() {
            (self.sum - sum) + term
        } else {
            (term - sum) + self.sum
        };
        self.sum = sum;
        self
    }

    fn mean(self, count: i64) -> f64 {
        debug_assert!(count > 0);
        // Every finite-input mean fits the finite input range. Rounding at
        // the largest exponent must not turn that representable result into
        // infinity. Extremely disparate magnitudes retain floating rounding.
        let count = count as f64;
        let quotient = self.sum / count;
        // Retain compensation through division instead of rounding it away
        // in sum + correction first. The fused residual corrects the quotient.
        let residual = (-quotient).mul_add(count, self.sum) + self.correction;
        ((quotient + residual / count) * self.scale).clamp(-f64::MAX, f64::MAX)
    }
}

impl Average {
    pub(super) fn new() -> Self {
        Self {
            total: Total::Integer(0_i128.to_ne_bytes()),
            count: 0,
        }
    }

    /// Return a checked transition. The owner can admit it before replacing
    /// its current state; invalid input leaves the previous average intact.
    pub(super) fn next(self, value: &Value) -> Result<Self> {
        let number = match value {
            Value::Integer(value) => *value as f64,
            Value::Float(value) => *value,
            _ => return Err(evaluation::type_error("numeric aggregate requires numbers")),
        };
        let count = self.count.checked_add(1).ok_or_else(evaluation::overflow)?;
        let total = match (self.total, value) {
            (Total::Integer(bytes), Value::Integer(value)) => {
                let sum = i128::from_ne_bytes(bytes);
                // At most i64::MAX signed-64 inputs have magnitude below 2^126.
                Total::Integer(
                    sum.checked_add(i128::from(*value))
                        .expect("bounded integer average sum fits i128")
                        .to_ne_bytes(),
                )
            }
            (Total::NonFinite(sum), _) => Total::NonFinite(sum + number),
            (_, _) if !number.is_finite() => Total::NonFinite(number),
            (Total::Integer(bytes), _) => Total::Finite(
                ScaledSum::default()
                    .push_integer(i128::from_ne_bytes(bytes))
                    .push(number),
            ),
            (Total::Finite(sum), Value::Integer(value)) => {
                Total::Finite(sum.push_integer(i128::from(*value)))
            }
            (Total::Finite(sum), _) => Total::Finite(sum.push(number)),
        };
        Ok(Self { total, count })
    }

    pub(super) fn finish(self) -> Value {
        if self.count == 0 {
            return Value::Null;
        }
        Value::Float(match self.total {
            // Rounding the full integer total first can change even a mean of
            // identical inputs. Keep its low residue through final division.
            Total::Integer(bytes) => ScaledSum::default()
                .push_integer(i128::from_ne_bytes(bytes))
                .mean(self.count),
            Total::Finite(sum) => sum.mean(self.count),
            Total::NonFinite(value) => value,
        })
    }
}

#[cfg(test)]
#[path = "tests/average.rs"]
mod tests;
