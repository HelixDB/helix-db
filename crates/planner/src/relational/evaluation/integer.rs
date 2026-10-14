//! Integer conversion bounds are checked before truncation. Decimal text keeps
//! its exact digits; a floating-point value retains its existing binary precision.

pub(super) fn from_float(value: f64) -> Option<i64> {
    (value.is_finite() && value >= i64::MIN as f64 && value < 9_223_372_036_854_775_808.0)
        .then_some(value as i64)
}

/// Convert signed decimal text exactly, truncating toward zero. Invalid or
/// out-of-range text returns None, including a fractional excess at either
/// boundary. Parsing borrows the input and never builds an expanded decimal.
pub(super) fn from_decimal(text: &str) -> Option<i64> {
    let text = text.trim();
    let (negative, unsigned) = match text.strip_prefix('-') {
        Some(digits) => (true, digits),
        None => (false, text.strip_prefix('+').unwrap_or(text)),
    };
    let (mantissa, exponent) = match unsigned.split_once(['e', 'E']) {
        None => (unsigned, 0_i128),
        Some((mantissa, exponent)) => {
            let (negative, digits) = match exponent.strip_prefix('-') {
                Some(digits) => (true, digits),
                None => (false, exponent.strip_prefix('+').unwrap_or(exponent)),
            };
            if digits.is_empty() || !digits.bytes().all(|c| c.is_ascii_digit()) {
                return None;
            }
            // Exponents beyond this bound cannot be cancelled by the length
            // of any addressable input. Saturation preserves their direction.
            let magnitude = digits.bytes().fold(0_i128, |n, c| {
                n.saturating_mul(10).saturating_add(i128::from(c - b'0'))
            });
            (mantissa, if negative { -magnitude } else { magnitude })
        }
    };
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let digits = whole.bytes().chain(fraction.bytes());
    if whole.is_empty() && fraction.is_empty() || !digits.clone().all(|c| c.is_ascii_digit()) {
        return None;
    }
    let Some(leading_zeroes) = digits.clone().position(|c| c != b'0') else {
        return Some(0);
    };
    let integer_digits = (whole.len() as i128)
        .saturating_add(exponent)
        .saturating_sub(leading_zeroes as i128);
    if integer_digits <= 0 {
        return Some(0);
    }
    if integer_digits > 19 {
        return None;
    }
    let mut significant = digits.skip(leading_zeroes);
    let mut magnitude = 0_u64;
    for _ in 0..integer_digits {
        let digit = significant.next().unwrap_or(b'0') - b'0';
        magnitude = magnitude.checked_mul(10)?.checked_add(u64::from(digit))?;
    }
    let limit = i64::MAX as u64 + u64::from(negative);
    if magnitude > limit || magnitude == limit && significant.any(|c| c != b'0') {
        return None;
    }
    if negative {
        i64::try_from(-i128::from(magnitude)).ok()
    } else {
        i64::try_from(magnitude).ok()
    }
}
