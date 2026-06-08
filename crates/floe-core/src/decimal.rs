use std::io::Write as _;

use anyhow::{Context, Result, bail, ensure};

pub fn parse_decimal_text_to_i128(value: &str, scale: i8) -> Result<i128> {
    let scale = u32::try_from(scale).context("Decimal128 scale cannot be negative")?;
    let value = value.trim();
    ensure!(!value.is_empty(), "decimal value cannot be empty");

    let (negative, digits) = value
        .strip_prefix('-')
        .map(|rest| (true, rest))
        .unwrap_or_else(|| {
            value
                .strip_prefix('+')
                .map(|rest| (false, rest))
                .unwrap_or((false, value))
        });

    let mut parsed = 0_i128;
    let mut saw_digit = false;
    let mut saw_decimal = false;
    let mut fraction_len = 0_usize;
    let scale_usize = usize::try_from(scale).context("Decimal128 scale exceeds usize")?;

    for byte in digits.bytes() {
        match byte {
            b'0'..=b'9' => {
                saw_digit = true;
                if saw_decimal {
                    fraction_len = fraction_len.saturating_add(1);
                    ensure!(
                        fraction_len <= scale_usize,
                        "decimal value '{value}' has more fractional digits than scale {scale}"
                    );
                }
                parsed = parsed
                    .checked_mul(10)
                    .and_then(|acc| acc.checked_add(i128::from(byte - b'0')))
                    .with_context(|| format!("decimal value '{value}' exceeds i128 range"))?;
            }
            b'.' if !saw_decimal => {
                saw_decimal = true;
            }
            _ => bail!("invalid decimal value '{value}'"),
        }
    }

    ensure!(saw_digit, "decimal value '{value}' has no digits");
    for _ in 0..scale_usize.saturating_sub(fraction_len) {
        parsed = parsed
            .checked_mul(10)
            .with_context(|| format!("decimal value '{value}' exceeds i128 range"))?;
    }

    if negative {
        parsed
            .checked_neg()
            .with_context(|| format!("decimal value '{value}' exceeds i128 range"))
    } else {
        Ok(parsed)
    }
}

pub fn format_decimal128(value: i128, scale: i8) -> Result<String> {
    let mut out = Vec::new();
    append_decimal128_text(&mut out, value, scale)?;
    String::from_utf8(out).context("Decimal128 text should be ASCII")
}

pub fn append_decimal128_text(out: &mut Vec<u8>, value: i128, scale: i8) -> Result<()> {
    if scale <= 0 {
        write!(out, "{value}")?;
        return Ok(());
    }
    let scale = scale as u32;
    let factor = 10_u128
        .checked_pow(scale)
        .with_context(|| format!("Decimal128 scale {scale} is too large"))?;
    if value < 0 {
        out.push(b'-');
    }
    let magnitude = value.unsigned_abs();
    let whole = magnitude / factor;
    let fraction = magnitude % factor;
    write!(out, "{whole}.{fraction:0width$}", width = scale as usize)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_decimal_text_with_scale() {
        assert_eq!(parse_decimal_text_to_i128("123.45", 2).unwrap(), 12_345);
        assert_eq!(parse_decimal_text_to_i128("123", 2).unwrap(), 12_300);
        assert_eq!(parse_decimal_text_to_i128("-0.07", 2).unwrap(), -7);
        assert_eq!(parse_decimal_text_to_i128("+42.1", 3).unwrap(), 42_100);
        assert_eq!(parse_decimal_text_to_i128(" .5 ", 2).unwrap(), 50);
    }

    #[test]
    fn rejects_invalid_decimal_text() {
        assert!(parse_decimal_text_to_i128("1.234", 2).is_err());
        assert!(parse_decimal_text_to_i128("1.2.3", 2).is_err());
        assert!(parse_decimal_text_to_i128("", 2).is_err());
        assert!(parse_decimal_text_to_i128(".", 2).is_err());
        assert!(parse_decimal_text_to_i128("+.", 2).is_err());
        assert!(parse_decimal_text_to_i128("abc", 2).is_err());
        assert!(parse_decimal_text_to_i128("1.0", -1).is_err());
    }

    #[test]
    fn formats_decimal128_with_scale() -> Result<()> {
        assert_eq!(format_decimal128(12_345, 2)?, "123.45");
        assert_eq!(format_decimal128(-7, 2)?, "-0.07");
        assert_eq!(format_decimal128(42_100, 3)?, "42.100");
        assert_eq!(format_decimal128(42, 0)?, "42");
        Ok(())
    }
}
