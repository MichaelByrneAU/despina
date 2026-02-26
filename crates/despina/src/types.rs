//! Storage type codes used by `.mat` matrix tables.
//!
//! In the source file's table catalogue (MVR header record), each table has a
//! one-character ASCII token in the `DEC=` list. This module models those
//! tokens as a strongly typed enum:
//!
//! - `'0'`..`'9'` map to fixed-point storage with that many decimal places.
//! - `'S'` maps to IEEE 754 binary32 storage.
//! - `'D'` maps to IEEE 754 binary64 storage.
//!
//! Parsing of those single-character tokens is implemented by
//! [`TypeCode::from_ascii`].

use std::fmt;

/// Storage type code for a matrix table.
///
/// Each table in a `.mat` file declares exactly one type code in the header's
/// MVR record. That code determines how row payload bytes for the table are
/// interpreted.
///
/// `Fixed(p)` corresponds to decimal codes `0` through `9`, where `p` is the
/// number of decimal places. `Float32` corresponds to `S`. `Float64`
/// corresponds to `D`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TypeCode {
    /// Fixed-point numeric with `p` decimal places.
    ///
    /// The `.mat` format represents these codes as ASCII digits (`'0'`..`'9'`).
    /// Values outside `0..=9` are not valid `.mat` type codes and are rejected
    /// by writer/build APIs as
    /// [`ErrorKind::InvalidTypeCode`](crate::ErrorKind::InvalidTypeCode).
    Fixed(u8),
    /// IEEE 754 binary32 (`S`).
    Float32,
    /// IEEE 754 binary64 (`D`).
    Float64,
}

impl fmt::Display for TypeCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Float32 => f.write_str("S"),
            Self::Float64 => f.write_str("D"),
            Self::Fixed(p) if *p <= 9 => {
                write!(f, "{}", char::from(b'0' + p))
            }
            Self::Fixed(p) => write!(f, "Fixed({})", p),
        }
    }
}

impl TypeCode {
    /// Returns whether this type code is representable in a `.mat` MVR token.
    pub(crate) const fn is_valid_mat_code(self) -> bool {
        matches!(self, Self::Float32 | Self::Float64 | Self::Fixed(0..=9))
    }

    /// Appends the ASCII token for this type code to `buf`.
    ///
    /// `Fixed(p)` appends the digit `p`, `Float32` appends `S`, and `Float64`
    /// appends `D`. Returns `false` if this code has no valid on-disk token.
    pub(crate) fn write_ascii(self, buf: &mut Vec<u8>) -> bool {
        match self {
            Self::Float32 => {
                buf.push(b'S');
                true
            }
            Self::Float64 => {
                buf.push(b'D');
                true
            }
            Self::Fixed(p) if p <= 9 => {
                buf.push(b'0' + p);
                true
            }
            Self::Fixed(_) => false,
        }
    }

    /// Parse a table type code from its ASCII token.
    ///
    /// Returns `None` unless `token` is one ASCII digit (`'0'`..`'9'`) or
    /// exactly `S`/`D`.
    pub fn from_ascii(token: &str) -> Option<Self> {
        match token {
            "S" => Some(Self::Float32),
            "D" => Some(Self::Float64),
            _ => {
                if token.len() != 1 {
                    return None;
                }
                let byte = token.as_bytes()[0];
                if byte.is_ascii_digit() {
                    Some(Self::Fixed(byte - b'0'))
                } else {
                    None
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_ascii_accepts_all_valid_tokens() {
        for p in 0..=9 {
            let token = (b'0' + p) as char;
            assert_eq!(
                TypeCode::from_ascii(&token.to_string()),
                Some(TypeCode::Fixed(p))
            );
        }
        assert_eq!(TypeCode::from_ascii("S"), Some(TypeCode::Float32));
        assert_eq!(TypeCode::from_ascii("D"), Some(TypeCode::Float64));
    }

    #[test]
    fn from_ascii_rejects_invalid_tokens() {
        for token in ["", " ", "Z", "s", "d", "10", "-1", "FF"] {
            assert_eq!(TypeCode::from_ascii(token), None, "token={token:?}");
        }
    }

    #[test]
    fn write_ascii_accepts_valid_codes() {
        let mut buf = Vec::new();

        for p in 0..=9 {
            assert!(TypeCode::Fixed(p).write_ascii(&mut buf));
        }
        assert!(TypeCode::Float32.write_ascii(&mut buf));
        assert!(TypeCode::Float64.write_ascii(&mut buf));

        assert_eq!(buf, b"0123456789SD");
    }

    #[test]
    fn write_ascii_rejects_invalid_fixed_codes() {
        let mut buf = Vec::new();
        assert!(!TypeCode::Fixed(10).write_ascii(&mut buf));
        assert!(buf.is_empty());
        assert!(!TypeCode::Fixed(255).is_valid_mat_code());
    }
}
