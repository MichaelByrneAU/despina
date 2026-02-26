//! Row payload encoding for `.mat` matrix files.
//!
//! The encode pipeline for a single row:
//!
//! 1. Detect all-zero row and emit the minimal payload.
//! 2. Dispatch on type code:
//!    - `Fixed(p)`: try numeric encoding (quantise, decompose, compress),
//!      falling back to float64 on `u32` overflow.
//!    - `Float32`: try float32 encoding (cast to `f32`, decompose, compress),
//!      falling back to float64 on `f32` overflow.
//!    - `Float64`: encode float64 (decompose, compress).
//! 3. Each encoding path then:
//!    - Fills [`PlaneScratch`](crate::plane::PlaneScratch) planes from values.
//!    - Writes row header bytes (preamble + descriptor).
//!    - Compresses each present plane via
//!      [`encode_plane`](crate::plane::encode_plane) and appends to the
//!      payload.
//!
//! The encoder always tries the most compact encoding first: numeric for
//! fixed-point tables, float32 for type-S tables. If any value in the row
//! overflows the native representation (magnitude > `u32::MAX` for numeric,
//! finite `f64` that is not representable as a finite `f32` for float32), the
//! encoder falls back to float64.
//!
//! Numeric tables use half-even (banker's) rounding in scaled space.

use crate::plane::{self, POW10, PlaneScratch};
use crate::row_format::{
    DESCRIPTOR_FLOAT32, DESCRIPTOR_FLOAT64, DESCRIPTOR_ZERO, NUMERIC_B0_FLAG, NUMERIC_B1_FLAG,
    NUMERIC_B2_FLAG, NUMERIC_B3_FLAG, NUMERIC_SS_FLAG, append_row_prefix,
};
use crate::types::TypeCode;

/// Quantises one numeric-table value to a 32-bit unsigned magnitude with
/// half-even rounding.
///
/// Returns `None` for non-finite values, scaled values that are not finite
/// after rounding, or magnitudes that overflow `u32`.
#[inline]
fn quantise_half_even_u32(value: f64, decimal_places: u8) -> Option<(u32, bool)> {
    if !value.is_finite() {
        return None;
    }

    let scale = POW10[decimal_places as usize];
    let scaled = (value * scale).round_ties_even();
    if !scaled.is_finite() {
        return None;
    }

    let magnitude = scaled.abs();
    if magnitude > u32::MAX as f64 {
        return None;
    }

    let mag_u32 = magnitude as u32;
    let negative = scaled.is_sign_negative() && mag_u32 != 0;
    Some((mag_u32, negative))
}

/// Encode a row of `f64` values into a compressed row payload.
///
/// Takes the table's [`TypeCode`], the cell values for one row, reusable
/// scratch buffers, and a payload buffer to **append** into. The caller is
/// responsible for clearing or positioning the buffer before calling this
/// function.
///
/// This function is infallible: it operates on known-good `f64` values and
/// always produces valid output. If the native encoding overflows (magnitude >
/// `u32::MAX` for numeric tables, or `f64` value outside `f32` range for
/// float32 tables), the encoder automatically falls back to float64 encoding.
pub(crate) fn encode_row_payload(
    type_code: TypeCode,
    values: &[f64],
    scratch: &mut PlaneScratch,
    payload: &mut Vec<u8>,
) {
    let start_offset = payload.len();

    // All-zero row: emit the minimal 3-byte payload.
    // IEEE 754: `-0.0 == 0.0` is true, so negative zeros are caught here.
    if values.iter().all(|&v| v == 0.0) {
        append_row_prefix(payload, DESCRIPTOR_ZERO);
        return;
    }

    match type_code {
        TypeCode::Fixed(decimal_places) => {
            if !encode_numeric_row(decimal_places, values, scratch, payload) {
                payload.truncate(start_offset);
                encode_float64_row(values, scratch, payload);
            }
        }
        TypeCode::Float32 => {
            if !encode_float32_row(values, scratch, payload) {
                payload.truncate(start_offset);
                encode_float64_row(values, scratch, payload);
            }
        }
        TypeCode::Float64 => {
            encode_float64_row(values, scratch, payload);
        }
    }
}

/// Encode a numeric bitfield row body (type codes `0`..`9`).
///
/// Returns `true` on success, `false` if any value is non-finite or any
/// magnitude overflows `u32`. On `false`, the payload is in an unspecified
/// state and the caller should clear it and fall back to float64.
fn encode_numeric_row(
    decimal_places: u8,
    values: &[f64],
    scratch: &mut PlaneScratch,
    payload: &mut Vec<u8>,
) -> bool {
    let zone_count = values.len();
    let [b0, b1, b2, b3, ss, _, _, _] = scratch.all_planes_mut();
    let b0 = &mut b0[..zone_count];
    let b1 = &mut b1[..zone_count];
    let b2 = &mut b2[..zone_count];
    let b3 = &mut b3[..zone_count];
    let ss = &mut ss[..zone_count];
    let mut any_b0 = 0u8;
    let mut any_b1 = 0u8;
    let mut any_b2 = 0u8;
    let mut any_b3 = 0u8;
    let mut any_ss = 0u8;

    // Quantise and decompose each value into plane bytes. Track non-zero
    // presence inline to avoid extra descriptor-scan passes.
    for j in 0..zone_count {
        let Some((mag_u32, negative)) = quantise_half_even_u32(values[j], decimal_places) else {
            return false;
        };
        let b0_byte = mag_u32 as u8;
        let b1_byte = (mag_u32 >> 8) as u8;
        let b2_byte = (mag_u32 >> 16) as u8;
        let b3_byte = (mag_u32 >> 24) as u8;
        b0[j] = b0_byte;
        b1[j] = b1_byte;
        b2[j] = b2_byte;
        b3[j] = b3_byte;
        any_b0 |= b0_byte;
        any_b1 |= b1_byte;
        any_b2 |= b2_byte;
        any_b3 |= b3_byte;

        // SS byte: if magnitude is zero, emit 0x00 to maximise plane
        // compressibility. Otherwise encode decimal_places in the upper nibble
        // and the sign bit in bit 0.
        let ss_byte = if mag_u32 == 0 {
            0x00
        } else {
            (decimal_places << 4) | (negative as u8)
        };
        ss[j] = ss_byte;
        any_ss |= ss_byte;
    }

    let has_b0 = any_b0 != 0;
    let has_b1 = any_b1 != 0;
    let has_b2 = any_b2 != 0;
    let has_b3 = any_b3 != 0;
    let has_ss = any_ss != 0;

    // Build descriptor from presence flags.
    let descriptor = (if has_b0 { NUMERIC_B0_FLAG } else { 0 })
        | (if has_b1 { NUMERIC_B1_FLAG } else { 0 })
        | (if has_b2 { NUMERIC_B2_FLAG } else { 0 })
        | (if has_b3 { NUMERIC_B3_FLAG } else { 0 })
        | (if has_ss { NUMERIC_SS_FLAG } else { 0 });

    // Emit preamble + descriptor.
    append_row_prefix(payload, descriptor);

    // Compress each present plane in order.
    if has_b0 {
        plane::encode_plane(b0, payload);
    }
    if has_b1 {
        plane::encode_plane(b1, payload);
    }
    if has_b2 {
        plane::encode_plane(b2, payload);
    }
    if has_b3 {
        plane::encode_plane(b3, payload);
    }
    if has_ss {
        plane::encode_plane(ss, payload);
    }

    true
}

/// Encode a float32 row body.
///
/// Returns `true` on success, `false` if any finite `f64` value overflows the
/// `f32` range (i.e. the `f64` is finite but the `f32` cast produces infinity).
/// On `false`, the caller falls back to float64.
fn encode_float32_row(values: &[f64], scratch: &mut PlaneScratch, payload: &mut Vec<u8>) -> bool {
    let zone_count = values.len();
    let [b0, b1, b2, b3, marker, _, _, _] = scratch.all_planes_mut();
    let b0 = &mut b0[..zone_count];
    let b1 = &mut b1[..zone_count];
    let b2 = &mut b2[..zone_count];
    let b3 = &mut b3[..zone_count];
    let marker = &mut marker[..zone_count];

    for j in 0..zone_count {
        let value = values[j];
        let f32_val = value as f32;

        // A finite f64 that becomes infinite as f32 has overflowed.
        if value.is_finite() && !f32_val.is_finite() {
            return false;
        }

        let bytes = f32_val.to_le_bytes();
        b0[j] = bytes[0];
        b1[j] = bytes[1];
        b2[j] = bytes[2];
        b3[j] = bytes[3];
        marker[j] = 0xFF;
    }

    // Emit row prefix.
    append_row_prefix(payload, DESCRIPTOR_FLOAT32);

    // Compress all five planes (B0..B3 + marker).
    plane::encode_plane(b0, payload);
    plane::encode_plane(b1, payload);
    plane::encode_plane(b2, payload);
    plane::encode_plane(b3, payload);
    plane::encode_plane(marker, payload);

    true
}

/// Encode a float64 row body.
///
/// This always succeeds. Any `f64` value (including NaN, infinity, and negative
/// zero) is representable exactly.
fn encode_float64_row(values: &[f64], scratch: &mut PlaneScratch, payload: &mut Vec<u8>) {
    let zone_count = values.len();
    let [b0, b1, b2, b3, b4, b5, b6, b7] = scratch.all_planes_mut();
    let b0 = &mut b0[..zone_count];
    let b1 = &mut b1[..zone_count];
    let b2 = &mut b2[..zone_count];
    let b3 = &mut b3[..zone_count];
    let b4 = &mut b4[..zone_count];
    let b5 = &mut b5[..zone_count];
    let b6 = &mut b6[..zone_count];
    let b7 = &mut b7[..zone_count];

    for j in 0..zone_count {
        let bytes = values[j].to_le_bytes();
        b0[j] = bytes[0];
        b1[j] = bytes[1];
        b2[j] = bytes[2];
        b3[j] = bytes[3];
        b4[j] = bytes[4];
        b5[j] = bytes[5];
        b6[j] = bytes[6];
        b7[j] = bytes[7];
    }

    // Emit row prefix.
    append_row_prefix(payload, DESCRIPTOR_FLOAT64);

    // Compress all eight planes.
    plane::encode_plane(b0, payload);
    plane::encode_plane(b1, payload);
    plane::encode_plane(b2, payload);
    plane::encode_plane(b3, payload);
    plane::encode_plane(b4, payload);
    plane::encode_plane(b5, payload);
    plane::encode_plane(b6, payload);
    plane::encode_plane(b7, payload);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decode::decode_row_payload;

    #[test]
    fn quantise_half_even_midpoint_cases() {
        assert_eq!(quantise_half_even_u32(0.005, 2), Some((0, false)));
        assert_eq!(quantise_half_even_u32(0.015, 2), Some((2, false)));
        assert_eq!(quantise_half_even_u32(1.005, 2), Some((100, false)));
        assert_eq!(quantise_half_even_u32(-0.015, 2), Some((2, true)));
    }

    #[test]
    fn quantise_half_even_non_midpoint_cases() {
        assert_eq!(quantise_half_even_u32(2.3, 0), Some((2, false)));
        assert_eq!(quantise_half_even_u32(2.7, 0), Some((3, false)));
        assert_eq!(quantise_half_even_u32(-2.3, 0), Some((2, true)));
        assert_eq!(quantise_half_even_u32(-2.7, 0), Some((3, true)));
    }

    #[test]
    fn quantise_half_even_zero_has_no_sign() {
        assert_eq!(quantise_half_even_u32(-0.0, 2), Some((0, false)));
        assert_eq!(quantise_half_even_u32(-0.004, 2), Some((0, false)));
    }

    #[test]
    fn quantise_half_even_large_values() {
        assert_eq!(
            quantise_half_even_u32(4_294_967_295.0, 0),
            Some((u32::MAX, false))
        );
        assert_eq!(
            quantise_half_even_u32(4_294_967_294.5, 0),
            Some((4_294_967_294, false))
        );
    }

    #[test]
    fn quantise_half_even_non_finite_rejected() {
        assert_eq!(quantise_half_even_u32(f64::NAN, 2), None);
        assert_eq!(quantise_half_even_u32(f64::INFINITY, 2), None);
        assert_eq!(quantise_half_even_u32(f64::NEG_INFINITY, 2), None);
    }

    #[test]
    fn quantise_half_even_overflow_rejected() {
        assert_eq!(quantise_half_even_u32(4_294_967_296.0, 0), None);
    }

    #[test]
    fn zero_row_all_positive_zeros() {
        let values = vec![0.0; 5];
        let mut scratch = PlaneScratch::new(5);
        let mut payload = Vec::new();
        encode_row_payload(TypeCode::Fixed(2), &values, &mut scratch, &mut payload);
        assert_eq!(payload, vec![0x80, 0x80, 0x00]);
    }

    #[test]
    fn zero_row_mixed_positive_negative_zeros() {
        let values = vec![0.0, -0.0, 0.0, -0.0];
        let mut scratch = PlaneScratch::new(4);
        let mut payload = Vec::new();
        encode_row_payload(TypeCode::Fixed(0), &values, &mut scratch, &mut payload);
        assert_eq!(payload, vec![0x80, 0x80, 0x00]);
    }

    #[test]
    fn zero_row_works_for_all_type_codes() {
        for type_code in [
            TypeCode::Fixed(0),
            TypeCode::Fixed(9),
            TypeCode::Float32,
            TypeCode::Float64,
        ] {
            let values = vec![0.0; 3];
            let mut scratch = PlaneScratch::new(3);
            let mut payload = Vec::new();
            encode_row_payload(type_code, &values, &mut scratch, &mut payload);
            assert_eq!(payload, vec![0x80, 0x80, 0x00]);
        }
    }

    #[test]
    fn numeric_simple_integers_b0_only() {
        let values = vec![10.0, 20.0, 30.0];
        let mut scratch = PlaneScratch::new(3);
        let mut payload = Vec::new();
        encode_row_payload(TypeCode::Fixed(0), &values, &mut scratch, &mut payload);

        assert_eq!(payload[0], 0x80);
        assert_eq!(payload[1], 0x80);
        assert_eq!(payload[2], 0x80);
    }

    #[test]
    fn numeric_two_byte_magnitudes() {
        let values = vec![513.0];
        let mut scratch = PlaneScratch::new(1);
        let mut payload = Vec::new();
        encode_row_payload(TypeCode::Fixed(0), &values, &mut scratch, &mut payload);

        assert_eq!(payload[2], 0xC0);
    }

    #[test]
    fn numeric_all_four_magnitude_planes() {
        let values = vec![0x12345678_u32 as f64];
        let mut scratch = PlaneScratch::new(1);
        let mut payload = Vec::new();
        encode_row_payload(TypeCode::Fixed(0), &values, &mut scratch, &mut payload);

        assert_eq!(payload[2], 0xF0);
    }

    #[test]
    fn numeric_with_decimal_places_and_sign() {
        let values = vec![1.25, 0.0, -2.34];
        let mut scratch = PlaneScratch::new(3);
        let mut payload = Vec::new();
        encode_row_payload(TypeCode::Fixed(2), &values, &mut scratch, &mut payload);

        assert_eq!(payload[0], 0x80);
        assert_eq!(payload[1], 0x80);
        assert_eq!(payload[2], 0x88);

        let mut decoded = vec![0.0; 3];
        decode_row_payload(TypeCode::Fixed(2), &payload, &mut scratch, &mut decoded).unwrap();
        assert!((decoded[0] - 1.25).abs() < 1e-15);
        assert_eq!(decoded[1], 0.0);
        assert!((decoded[2] - (-2.34)).abs() < 1e-15);
    }

    #[test]
    fn numeric_descriptor_minimal_planes_255() {
        let values = vec![255.0];
        let mut scratch = PlaneScratch::new(1);
        let mut payload = Vec::new();
        encode_row_payload(TypeCode::Fixed(0), &values, &mut scratch, &mut payload);
        assert_eq!(payload[2], 0x80);
    }

    #[test]
    fn numeric_descriptor_minimal_planes_256() {
        let values = vec![256.0];
        let mut scratch = PlaneScratch::new(1);
        let mut payload = Vec::new();
        encode_row_payload(TypeCode::Fixed(0), &values, &mut scratch, &mut payload);
        assert_eq!(payload[2], 0x40);

        let values = vec![257.0];
        payload.clear();
        encode_row_payload(TypeCode::Fixed(0), &values, &mut scratch, &mut payload);
        assert_eq!(payload[2], 0xC0);
    }

    #[test]
    fn numeric_descriptor_minimal_planes_65536() {
        let values = vec![65536.0];
        let mut scratch = PlaneScratch::new(1);
        let mut payload = Vec::new();
        encode_row_payload(TypeCode::Fixed(0), &values, &mut scratch, &mut payload);
        assert_eq!(payload[2], 0x20);

        let values = vec![65537.0];
        payload.clear();
        encode_row_payload(TypeCode::Fixed(0), &values, &mut scratch, &mut payload);
        assert_eq!(payload[2], 0xA0);
    }

    #[test]
    fn numeric_descriptor_minimal_planes_2_pow_24() {
        let values = vec![16_777_216.0];
        let mut scratch = PlaneScratch::new(1);
        let mut payload = Vec::new();
        encode_row_payload(TypeCode::Fixed(0), &values, &mut scratch, &mut payload);
        assert_eq!(payload[2], 0x10);

        let values = vec![0x01020304_u32 as f64];
        payload.clear();
        encode_row_payload(TypeCode::Fixed(0), &values, &mut scratch, &mut payload);
        assert_eq!(payload[2], 0xF0);
    }

    #[test]
    fn numeric_ss_present_with_decimal_places() {
        let values = vec![1.0];
        let mut scratch = PlaneScratch::new(1);
        let mut payload = Vec::new();
        encode_row_payload(TypeCode::Fixed(3), &values, &mut scratch, &mut payload);
        assert_eq!(payload[2] & 0x08, 0x08);
    }

    #[test]
    fn numeric_values_that_round_to_zero() {
        let values = vec![0.004, 0.001, -0.004];
        let mut scratch = PlaneScratch::new(3);
        let mut payload = Vec::new();
        encode_row_payload(TypeCode::Fixed(2), &values, &mut scratch, &mut payload);
        assert_eq!(payload, vec![0x80, 0x80, 0x00]);
    }

    #[test]
    fn numeric_sparse_row() {
        let mut values = vec![0.0; 100];
        values[5] = 1.23;
        values[50] = -45.0;
        let mut scratch = PlaneScratch::new(100);
        let mut payload = Vec::new();
        encode_row_payload(TypeCode::Fixed(2), &values, &mut scratch, &mut payload);

        let mut decoded = vec![0.0; 100];
        decode_row_payload(TypeCode::Fixed(2), &payload, &mut scratch, &mut decoded).unwrap();
        assert!((decoded[5] - 1.23).abs() < 1e-15);
        assert!((decoded[50] - (-45.0)).abs() < 1e-15);
        for (j, &v) in decoded.iter().enumerate() {
            if j != 5 && j != 50 {
                assert_eq!(v, 0.0, "zone {} should be zero, got {}", j, v);
            }
        }
    }

    #[test]
    fn numeric_overflow_magnitude_falls_back_to_float64() {
        let values = vec![4_294_967_296.0];
        let mut scratch = PlaneScratch::new(1);
        let mut payload = Vec::new();
        encode_row_payload(TypeCode::Fixed(0), &values, &mut scratch, &mut payload);
        assert_eq!(payload[2], 0xFF);
    }

    #[test]
    fn numeric_overflow_with_decimal_places_falls_back() {
        let values = vec![500_000.0];
        let mut scratch = PlaneScratch::new(1);
        let mut payload = Vec::new();
        encode_row_payload(TypeCode::Fixed(4), &values, &mut scratch, &mut payload);
        assert_eq!(payload[2], 0xFF);
    }

    #[test]
    fn numeric_nan_falls_back_to_float64() {
        let values = vec![1.0, f64::NAN, 3.0];
        let mut scratch = PlaneScratch::new(3);
        let mut payload = Vec::new();
        encode_row_payload(TypeCode::Fixed(2), &values, &mut scratch, &mut payload);
        assert_eq!(payload[2], 0xFF);
    }

    #[test]
    fn numeric_infinity_falls_back_to_float64() {
        let values = vec![f64::INFINITY, 1.0];
        let mut scratch = PlaneScratch::new(2);
        let mut payload = Vec::new();
        encode_row_payload(TypeCode::Fixed(0), &values, &mut scratch, &mut payload);
        assert_eq!(payload[2], 0xFF);
    }

    #[test]
    fn float32_overflow_falls_back_to_float64() {
        let values = vec![1.0, f64::MAX];
        let mut scratch = PlaneScratch::new(2);
        let mut payload = Vec::new();
        encode_row_payload(TypeCode::Float32, &values, &mut scratch, &mut payload);
        assert_eq!(payload[2], 0xFF);
    }

    #[test]
    fn float32_simple_values() {
        let values = vec![1.0, -2.5, 0.0, 3.125];
        let mut scratch = PlaneScratch::new(4);
        let mut payload = Vec::new();
        encode_row_payload(TypeCode::Float32, &values, &mut scratch, &mut payload);
        assert_eq!(payload[2], 0xF8);
    }

    #[test]
    fn float32_encode_decode_round_trip() {
        let original: Vec<f64> = vec![1.0, -2.5, 100.25, 0.0, -0.001];
        let mut scratch = PlaneScratch::new(original.len());
        let mut payload = Vec::new();
        encode_row_payload(TypeCode::Float32, &original, &mut scratch, &mut payload);

        let mut decoded = vec![0.0; original.len()];
        decode_row_payload(TypeCode::Float32, &payload, &mut scratch, &mut decoded).unwrap();

        for (j, (&orig, &dec)) in original.iter().zip(decoded.iter()).enumerate() {
            let expected = orig as f32 as f64;
            assert!(
                (dec - expected).abs() < 1e-7,
                "zone {}: expected {} (via f32: {}), got {}",
                j,
                orig,
                expected,
                dec,
            );
        }
    }

    #[test]
    fn float64_exact_value_preservation() {
        let values = vec![std::f64::consts::PI, -1e30, 1.0 / 3.0, 42.0];
        let mut scratch = PlaneScratch::new(values.len());
        let mut payload = Vec::new();
        encode_row_payload(TypeCode::Float64, &values, &mut scratch, &mut payload);
        assert_eq!(payload[2], 0xFF);

        let mut decoded = vec![0.0; values.len()];
        decode_row_payload(TypeCode::Float64, &payload, &mut scratch, &mut decoded).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn float64_negative_zero_preservation() {
        let values = vec![-0.0, 1.0];
        let mut scratch = PlaneScratch::new(2);
        let mut payload = Vec::new();
        encode_row_payload(TypeCode::Float64, &values, &mut scratch, &mut payload);

        let mut decoded = vec![0.0; 2];
        decode_row_payload(TypeCode::Float64, &payload, &mut scratch, &mut decoded).unwrap();
        assert!(decoded[0].is_sign_negative());
        assert_eq!(decoded[1], 1.0);
    }

    #[test]
    fn float64_nan_preservation() {
        let values = vec![f64::NAN, 1.0];
        let mut scratch = PlaneScratch::new(2);
        let mut payload = Vec::new();
        encode_row_payload(TypeCode::Float64, &values, &mut scratch, &mut payload);

        let mut decoded = vec![0.0; 2];
        decode_row_payload(TypeCode::Float64, &payload, &mut scratch, &mut decoded).unwrap();
        assert!(decoded[0].is_nan());
        assert_eq!(decoded[1], 1.0);
    }

    #[test]
    fn round_trip_numeric_all_type_codes() {
        for decimal_places in 0..=9u8 {
            let type_code = TypeCode::Fixed(decimal_places);
            let scale = POW10[decimal_places as usize];
            let values: Vec<f64> = (1..=10)
                .map(|i| {
                    let v = i as f64 * 1.23456789;
                    let rounded = (v * scale).round_ties_even() / scale;
                    if rounded == 0.0 { 0.0 } else { rounded }
                })
                .collect();

            let mut scratch = PlaneScratch::new(values.len());
            let mut payload = Vec::new();
            encode_row_payload(type_code, &values, &mut scratch, &mut payload);

            let mut decoded = vec![0.0; values.len()];
            decode_row_payload(type_code, &payload, &mut scratch, &mut decoded).unwrap();

            for (j, (&orig, &dec)) in values.iter().zip(decoded.iter()).enumerate() {
                assert!(
                    (dec - orig).abs() < 1e-10,
                    "type_code {:?}, zone {}: expected {}, got {}",
                    type_code,
                    j,
                    orig,
                    dec,
                );
            }
        }
    }

    #[test]
    fn round_trip_float32_varied_values() {
        let values: Vec<f64> = (0..50)
            .map(|i| {
                let v = (i as f64 - 25.0) * 3.7;
                v as f32 as f64
            })
            .collect();

        let mut scratch = PlaneScratch::new(50);
        let mut payload = Vec::new();
        encode_row_payload(TypeCode::Float32, &values, &mut scratch, &mut payload);

        let mut decoded = vec![0.0; 50];
        decode_row_payload(TypeCode::Float32, &payload, &mut scratch, &mut decoded).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn round_trip_float64_large_matrix() {
        let values: Vec<f64> = (0..500).map(|i| (i as f64) * 0.001 - 0.25).collect();

        let mut scratch = PlaneScratch::new(500);
        let mut payload = Vec::new();
        encode_row_payload(TypeCode::Float64, &values, &mut scratch, &mut payload);

        let mut decoded = vec![0.0; 500];
        decode_row_payload(TypeCode::Float64, &payload, &mut scratch, &mut decoded).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn round_trip_single_zone() {
        let values = vec![42.0];
        let mut scratch = PlaneScratch::new(1);
        let mut payload = Vec::new();
        encode_row_payload(TypeCode::Fixed(0), &values, &mut scratch, &mut payload);

        let mut decoded = vec![0.0];
        decode_row_payload(TypeCode::Fixed(0), &payload, &mut scratch, &mut decoded).unwrap();
        assert_eq!(decoded, vec![42.0]);
    }

    #[test]
    fn preamble_always_0x80_0x80() {
        let test_cases: Vec<(TypeCode, Vec<f64>)> = vec![
            (TypeCode::Fixed(0), vec![0.0; 3]),
            (TypeCode::Fixed(2), vec![1.25]),
            (TypeCode::Float32, vec![1.0]),
            (TypeCode::Float64, vec![1.0]),
            (TypeCode::Fixed(0), vec![f64::NAN, 1.0]),
        ];

        for (type_code, values) in test_cases {
            let mut scratch = PlaneScratch::new(values.len());
            let mut payload = Vec::new();
            encode_row_payload(type_code, &values, &mut scratch, &mut payload);

            assert_eq!(payload[0], 0x80, "preamble[0] for {:?}", type_code);
            assert_eq!(payload[1], 0x80, "preamble[1] for {:?}", type_code);
        }
    }

    #[test]
    fn round_trip_fixed_point_with_sign() {
        let values = vec![1.25, 0.0, -2.34];
        let type_code = TypeCode::Fixed(2);

        let mut scratch = PlaneScratch::new(3);
        let mut payload = Vec::new();
        encode_row_payload(type_code, &values, &mut scratch, &mut payload);

        let mut decoded = vec![0.0; 3];
        decode_row_payload(type_code, &payload, &mut scratch, &mut decoded).unwrap();
        assert!((decoded[0] - 1.25).abs() < 1e-15);
        assert_eq!(decoded[1], 0.0);
        assert!((decoded[2] - (-2.34)).abs() < 1e-15);
    }
}
