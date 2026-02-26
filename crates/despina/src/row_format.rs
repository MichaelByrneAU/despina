//! Shared row-payload header constants and helpers.
//!
//! Every `.mat` row payload begins with a three-byte header:
//!
//! 1. preamble byte 0 (`0x80`)
//! 2. preamble byte 1 (`0x80`)
//! 3. descriptor byte

/// Fixed two-byte row preamble (`0x80 0x80`).
pub(crate) const ROW_PREAMBLE: [u8; 2] = [0x80, 0x80];

/// Total length of the mandatory row prefix (`preamble + descriptor`).
pub(crate) const ROW_PREFIX_LEN: usize = 3;

/// Canonical all-zero row descriptor.
pub(crate) const DESCRIPTOR_ZERO: u8 = 0x00;

/// Float32 row descriptor (`S` table family).
pub(crate) const DESCRIPTOR_FLOAT32: u8 = 0xF8;

/// Float64 row descriptor.
pub(crate) const DESCRIPTOR_FLOAT64: u8 = 0xFF;

/// Numeric descriptor bit for magnitude plane `B0` (least significant byte).
pub(crate) const NUMERIC_B0_FLAG: u8 = 0x80;
/// Numeric descriptor bit for magnitude plane `B1`.
pub(crate) const NUMERIC_B1_FLAG: u8 = 0x40;
/// Numeric descriptor bit for magnitude plane `B2`.
pub(crate) const NUMERIC_B2_FLAG: u8 = 0x20;
/// Numeric descriptor bit for magnitude plane `B3`.
pub(crate) const NUMERIC_B3_FLAG: u8 = 0x10;
/// Numeric descriptor bit for sign/scale plane `SS`.
pub(crate) const NUMERIC_SS_FLAG: u8 = 0x08;
/// Numeric descriptor mask for reserved low bits (must be zero).
pub(crate) const NUMERIC_RESERVED_LOW_MASK: u8 = 0x07;

/// Appends the canonical two-byte preamble and descriptor byte.
#[inline]
pub(crate) fn append_row_prefix(destination: &mut Vec<u8>, descriptor: u8) {
    destination.extend_from_slice(&ROW_PREAMBLE);
    destination.push(descriptor);
}

/// Returns true when `payload` starts with the canonical two-byte preamble.
#[inline]
pub(crate) fn has_row_preamble(payload: &[u8]) -> bool {
    payload.len() >= ROW_PREAMBLE.len()
        && payload[0] == ROW_PREAMBLE[0]
        && payload[1] == ROW_PREAMBLE[1]
}

/// Returns true when `payload` is exactly the canonical zero-row marker.
#[inline]
pub(crate) fn is_canonical_zero_row(payload: &[u8]) -> bool {
    payload.len() == ROW_PREFIX_LEN
        && payload[0] == ROW_PREAMBLE[0]
        && payload[1] == ROW_PREAMBLE[1]
        && payload[2] == DESCRIPTOR_ZERO
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_row_prefix_writes_expected_bytes() {
        let mut destination = vec![0xAA];
        append_row_prefix(&mut destination, DESCRIPTOR_FLOAT32);
        assert_eq!(destination, vec![0xAA, 0x80, 0x80, 0xF8]);
    }

    #[test]
    fn canonical_zero_row_match_is_exact() {
        assert!(is_canonical_zero_row(&[0x80, 0x80, 0x00]));
        assert!(!is_canonical_zero_row(&[0x80, 0x80, 0x00, 0x00]));
        assert!(!is_canonical_zero_row(&[0x80, 0x80, 0x01]));
    }

    #[test]
    fn preamble_check_requires_two_leading_0x80_bytes() {
        assert!(has_row_preamble(&[0x80, 0x80]));
        assert!(has_row_preamble(&[0x80, 0x80, 0xFF]));
        assert!(!has_row_preamble(&[0x80]));
        assert!(!has_row_preamble(&[0x80, 0x00]));
    }
}
