//! Row payload decoding for `.mat` matrix files.
//!
//! The dispatch order is important. `0x00` is checked first, then `0xFF`, then
//! type-specific rules. This means a zero row in a float64 table is handled as
//! a zero row, not as a float64 decode error, and any table can contain
//! individual float64-encoded rows regardless of its declared type code.

use crate::error::{Error, ErrorKind};
use crate::plane::{self, B0, B1, B2, B3, B4, B5, B6, B7, POW10, PlaneScratch, SS};
use crate::row_format::{
    DESCRIPTOR_FLOAT32, DESCRIPTOR_FLOAT64, DESCRIPTOR_ZERO, NUMERIC_B0_FLAG, NUMERIC_B1_FLAG,
    NUMERIC_B2_FLAG, NUMERIC_B3_FLAG, NUMERIC_RESERVED_LOW_MASK, NUMERIC_SS_FLAG, ROW_PREFIX_LEN,
    has_row_preamble,
};
use crate::types::TypeCode;

/// Decode a row payload into destination-zone `f64` values.
///
/// Takes the raw payload bytes from a single row record (everything after the
/// five-byte record header), validates the row header prefix, dispatches on the
/// descriptor byte according to the table's `type_code`, decompresses the byte
/// planes, and writes the reconstructed cell values into `values`.
///
/// `values` must have length equal to the zone count. On success, every element
/// of `values` has been overwritten with the decoded cell value for that
/// destination zone.
///
/// # Errors
///
/// Returns an error if:
///
/// - The payload is shorter than the mandatory row prefix (preamble +
///   descriptor)
///   ([`ErrorKind::UnexpectedEof`]).
/// - The preamble bytes are invalid ([`ErrorKind::InvalidPreamble`]).
/// - The descriptor byte is not valid for the given `type_code`
///   ([`ErrorKind::InvalidDescriptor`]).
/// - A zero-row payload contains extra bytes after the preamble
///   ([`ErrorKind::TrailingBytes`]).
/// - Plane decompression fails (propagated from
///   [`plane::decode_plane`](crate::plane)).
/// - A float32 marker plane contains a byte other than `0xFF`
///   ([`ErrorKind::InvalidFloat32Marker`]).
pub(crate) fn decode_row_payload(
    type_code: TypeCode,
    payload: &[u8],
    scratch: &mut PlaneScratch,
    values: &mut [f64],
) -> crate::Result<()> {
    // Every payload starts with the preamble.
    if payload.len() < ROW_PREFIX_LEN {
        return Err(Error::new(ErrorKind::UnexpectedEof));
    }
    if !has_row_preamble(payload) {
        return Err(Error::new(ErrorKind::InvalidPreamble {
            got: [payload[0], payload[1]],
        }));
    }
    let descriptor = payload[ROW_PREFIX_LEN - 1];
    let data = &payload[ROW_PREFIX_LEN..];
    let zone_count = values.len();

    // Dispatch order:
    //   1. descriptor 0x00 -> zero row.
    //   2. descriptor 0xFF -> float64 (all 8 planes).
    //   3. type D with partial descriptor -> selective float64 planes.
    //   4. type S requires 0xF8 -> float32.
    //   5. everything else -> numeric bitfield.
    match descriptor {
        DESCRIPTOR_ZERO => {
            if !data.is_empty() {
                return Err(Error::new(ErrorKind::TrailingBytes));
            }
            values.fill(0.0);
        }
        DESCRIPTOR_FLOAT64 => {
            decode_float64_row(data, zone_count, scratch, values)?;
        }
        _ if matches!(type_code, TypeCode::Float64) => {
            decode_float64_selective_row(descriptor, data, zone_count, scratch, values)?;
        }
        DESCRIPTOR_FLOAT32 if matches!(type_code, TypeCode::Float32) => {
            decode_float32_row(data, zone_count, scratch, values)?;
        }
        _ if matches!(type_code, TypeCode::Float32) => {
            return Err(Error::new(ErrorKind::InvalidDescriptor {
                descriptor,
                type_code,
            }));
        }
        _ => {
            decode_numeric_row(descriptor, type_code, data, zone_count, scratch, values)?;
        }
    }

    Ok(())
}

/// Decode a numeric bitfield row body (type codes `0`..`9`).
///
/// Interprets numeric descriptor flags from [`row_format`](crate::row_format),
/// decodes the present planes in fixed order (`B0`, `B1`, `B2`, `B3`, `SS`),
/// then reconstructs each value from the 32-bit magnitude and optional
/// sign/scale byte.
fn decode_numeric_row(
    descriptor: u8,
    type_code: TypeCode,
    data: &[u8],
    zone_count: usize,
    scratch: &mut PlaneScratch,
    values: &mut [f64],
) -> crate::Result<()> {
    // The reserved low three bits must be clear.
    if descriptor & NUMERIC_RESERVED_LOW_MASK != 0 {
        return Err(Error::new(ErrorKind::InvalidDescriptor {
            descriptor,
            type_code,
        }));
    }

    let has_b0 = descriptor & NUMERIC_B0_FLAG != 0;
    let has_b1 = descriptor & NUMERIC_B1_FLAG != 0;
    let has_b2 = descriptor & NUMERIC_B2_FLAG != 0;
    let has_b3 = descriptor & NUMERIC_B3_FLAG != 0;
    let has_ss = descriptor & NUMERIC_SS_FLAG != 0;

    // Decode each present plane in order, zeroing absent planes so the
    // reconstruction loop below is branchless and autovectorisable.
    let mut offset = 0;

    if has_b0 {
        offset += plane::decode_plane(&data[offset..], &mut scratch.plane(B0)[..zone_count])?;
    } else {
        scratch.plane(B0)[..zone_count].fill(0);
    }
    if has_b1 {
        offset += plane::decode_plane(&data[offset..], &mut scratch.plane(B1)[..zone_count])?;
    } else {
        scratch.plane(B1)[..zone_count].fill(0);
    }
    if has_b2 {
        offset += plane::decode_plane(&data[offset..], &mut scratch.plane(B2)[..zone_count])?;
    } else {
        scratch.plane(B2)[..zone_count].fill(0);
    }
    if has_b3 {
        offset += plane::decode_plane(&data[offset..], &mut scratch.plane(B3)[..zone_count])?;
    } else {
        scratch.plane(B3)[..zone_count].fill(0);
    }
    if has_ss {
        offset += plane::decode_plane(&data[offset..], &mut scratch.plane(SS)[..zone_count])?;
    } else {
        scratch.plane(SS)[..zone_count].fill(0);
    }

    // All payload bytes should have been consumed.
    if offset != data.len() {
        return Err(Error::new(ErrorKind::TrailingBytes));
    }

    // Reconstruct f64 values column by column. Absent planes were zeroed above,
    // so no per-element branching is needed here.
    let b0 = scratch.plane_ref(B0);
    let b1 = scratch.plane_ref(B1);
    let b2 = scratch.plane_ref(B2);
    let b3 = scratch.plane_ref(B3);
    let ss = scratch.plane_ref(SS);

    if has_ss {
        for j in 0..zone_count {
            let magnitude = u32::from(b0[j])
                | (u32::from(b1[j]) << 8)
                | (u32::from(b2[j]) << 16)
                | (u32::from(b3[j]) << 24);
            let sign_scale = ss[j];
            let negative = sign_scale & 0x01 != 0;
            let decimal_places = (sign_scale >> 4) as usize;
            let divisor = POW10[decimal_places];
            let val = magnitude as f64 / divisor;
            values[j] = if negative { -val } else { val };
        }
    } else {
        for j in 0..zone_count {
            let magnitude = u32::from(b0[j])
                | (u32::from(b1[j]) << 8)
                | (u32::from(b2[j]) << 16)
                | (u32::from(b3[j]) << 24);
            values[j] = magnitude as f64;
        }
    }

    Ok(())
}

/// Decode a float32 row body.
///
/// Five planes are decompressed in order: B0, B1, B2, B3, and a marker plane.
/// Every byte in the marker plane must be `0xFF`. Per-column values are
/// reassembled from the four value planes as little-endian IEEE 754 binary32
/// bit patterns and widened to `f64`.
fn decode_float32_row(
    data: &[u8],
    zone_count: usize,
    scratch: &mut PlaneScratch,
    values: &mut [f64],
) -> crate::Result<()> {
    let mut offset = 0;

    // Decode the four value planes.
    offset += plane::decode_plane(&data[offset..], &mut scratch.plane(B0)[..zone_count])?;
    offset += plane::decode_plane(&data[offset..], &mut scratch.plane(B1)[..zone_count])?;
    offset += plane::decode_plane(&data[offset..], &mut scratch.plane(B2)[..zone_count])?;
    offset += plane::decode_plane(&data[offset..], &mut scratch.plane(B3)[..zone_count])?;

    // Decode the marker plane into the SS slot (reusing it as temporary space).
    offset += plane::decode_plane(&data[offset..], &mut scratch.plane(SS)[..zone_count])?;

    if offset != data.len() {
        return Err(Error::new(ErrorKind::TrailingBytes));
    }

    // Validate that every marker byte is 0xFF.
    let marker = scratch.plane_ref(SS);
    if let Some(marker_index) = marker[..zone_count].iter().position(|&byte| byte != 0xFF) {
        return Err(Error::new(ErrorKind::InvalidFloat32Marker {
            marker_index,
            got: marker[marker_index],
        }));
    }

    let b0 = scratch.plane_ref(B0);
    let b1 = scratch.plane_ref(B1);
    let b2 = scratch.plane_ref(B2);
    let b3 = scratch.plane_ref(B3);

    for j in 0..zone_count {
        let bits = u32::from(b0[j])
            | (u32::from(b1[j]) << 8)
            | (u32::from(b2[j]) << 16)
            | (u32::from(b3[j]) << 24);
        values[j] = f32::from_bits(bits) as f64;
    }

    Ok(())
}

/// Decode a float64 row body.
///
/// Eight planes are decompressed in order: B0 through B7. Per-column values are
/// reassembled as little-endian IEEE 754 binary64 bit patterns. This path
/// handles both type-D tables (which may use `0xFF` for all planes or a partial
/// descriptor to omit all-zero planes) and the overflow escape hatch for
/// numeric or float32 tables that occasionally need full double precision.
fn decode_float64_row(
    data: &[u8],
    zone_count: usize,
    scratch: &mut PlaneScratch,
    values: &mut [f64],
) -> crate::Result<()> {
    let mut offset = 0;

    offset += plane::decode_plane(&data[offset..], &mut scratch.plane(B0)[..zone_count])?;
    offset += plane::decode_plane(&data[offset..], &mut scratch.plane(B1)[..zone_count])?;
    offset += plane::decode_plane(&data[offset..], &mut scratch.plane(B2)[..zone_count])?;
    offset += plane::decode_plane(&data[offset..], &mut scratch.plane(B3)[..zone_count])?;
    offset += plane::decode_plane(&data[offset..], &mut scratch.plane(B4)[..zone_count])?;
    offset += plane::decode_plane(&data[offset..], &mut scratch.plane(B5)[..zone_count])?;
    offset += plane::decode_plane(&data[offset..], &mut scratch.plane(B6)[..zone_count])?;
    offset += plane::decode_plane(&data[offset..], &mut scratch.plane(B7)[..zone_count])?;

    if offset != data.len() {
        return Err(Error::new(ErrorKind::TrailingBytes));
    }

    let b0 = scratch.plane_ref(B0);
    let b1 = scratch.plane_ref(B1);
    let b2 = scratch.plane_ref(B2);
    let b3 = scratch.plane_ref(B3);
    let b4 = scratch.plane_ref(B4);
    let b5 = scratch.plane_ref(B5);
    let b6 = scratch.plane_ref(B6);
    let b7 = scratch.plane_ref(B7);

    for j in 0..zone_count {
        let bits = u64::from(b0[j])
            | (u64::from(b1[j]) << 8)
            | (u64::from(b2[j]) << 16)
            | (u64::from(b3[j]) << 24)
            | (u64::from(b4[j]) << 32)
            | (u64::from(b5[j]) << 40)
            | (u64::from(b6[j]) << 48)
            | (u64::from(b7[j]) << 56);
        values[j] = f64::from_bits(bits);
    }

    Ok(())
}

/// Plane slot indices in descriptor-bit order (bit 7 → B0, … , bit 0 → B7).
const FLOAT64_PLANE_SLOTS: [usize; 8] = [B0, B1, B2, B3, B4, B5, B6, B7];

/// Decode a float64 row body with selective plane presence.
///
/// The descriptor byte is an 8-bit field where each bit indicates whether
/// the corresponding byte plane is present in the compressed stream:
///
/// - bit 7 → B0 (least-significant byte of IEEE 754 binary64)
/// - bit 6 → B1
/// - …
/// - bit 0 → B7 (most-significant byte)
///
/// Absent planes are zero-filled. This is a compression optimisation: when
/// all values in a row share the same zero byte in a given position, the
/// encoder can omit that plane entirely.
fn decode_float64_selective_row(
    descriptor: u8,
    data: &[u8],
    zone_count: usize,
    scratch: &mut PlaneScratch,
    values: &mut [f64],
) -> crate::Result<()> {
    let mut offset = 0;

    for (bit_position, &slot) in FLOAT64_PLANE_SLOTS.iter().enumerate() {
        let flag = 0x80u8 >> bit_position;
        if descriptor & flag != 0 {
            offset += plane::decode_plane(&data[offset..], &mut scratch.plane(slot)[..zone_count])?;
        } else {
            scratch.plane(slot)[..zone_count].fill(0);
        }
    }

    if offset != data.len() {
        return Err(Error::new(ErrorKind::TrailingBytes));
    }

    let b0 = scratch.plane_ref(B0);
    let b1 = scratch.plane_ref(B1);
    let b2 = scratch.plane_ref(B2);
    let b3 = scratch.plane_ref(B3);
    let b4 = scratch.plane_ref(B4);
    let b5 = scratch.plane_ref(B5);
    let b6 = scratch.plane_ref(B6);
    let b7 = scratch.plane_ref(B7);

    for j in 0..zone_count {
        let bits = u64::from(b0[j])
            | (u64::from(b1[j]) << 8)
            | (u64::from(b2[j]) << 16)
            | (u64::from(b3[j]) << 24)
            | (u64::from(b4[j]) << 32)
            | (u64::from(b5[j]) << 40)
            | (u64::from(b6[j]) << 48)
            | (u64::from(b7[j]) << 56);
        values[j] = f64::from_bits(bits);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plane::encode_plane;

    /// Build a complete row payload from a descriptor and raw post-preamble
    /// data.
    fn make_payload(descriptor: u8, data: &[u8]) -> Vec<u8> {
        let mut payload = vec![0x80, 0x80, descriptor];
        payload.extend_from_slice(data);
        payload
    }

    /// Encode a single byte plane using the plane codec and append to `out`.
    fn push_encoded_plane(plane_bytes: &[u8], out: &mut Vec<u8>) {
        encode_plane(plane_bytes, out);
    }

    #[test]
    fn zero_row_fills_values_with_zero() {
        let payload = make_payload(0x00, &[]);
        let mut scratch = PlaneScratch::new(5);
        let mut values = vec![999.0; 5];
        decode_row_payload(TypeCode::Fixed(2), &payload, &mut scratch, &mut values).unwrap();
        assert_eq!(values, vec![0.0; 5]);
    }

    #[test]
    fn zero_row_works_for_all_type_codes() {
        for type_code in [
            TypeCode::Fixed(0),
            TypeCode::Fixed(9),
            TypeCode::Float32,
            TypeCode::Float64,
        ] {
            let payload = make_payload(0x00, &[]);
            let mut scratch = PlaneScratch::new(3);
            let mut values = vec![1.0; 3];
            decode_row_payload(type_code, &payload, &mut scratch, &mut values).unwrap();
            assert_eq!(values, vec![0.0; 3]);
        }
    }

    #[test]
    fn zero_row_rejects_trailing_bytes() {
        let payload = make_payload(0x00, &[0x01]);
        let mut scratch = PlaneScratch::new(3);
        let mut values = vec![0.0; 3];
        let err = decode_row_payload(TypeCode::Fixed(2), &payload, &mut scratch, &mut values)
            .unwrap_err();
        assert!(matches!(err.kind(), ErrorKind::TrailingBytes));
    }

    #[test]
    fn invalid_preamble_first_byte() {
        let payload = [0x00, 0x80, 0x00];
        let mut scratch = PlaneScratch::new(1);
        let mut values = vec![0.0];
        let err = decode_row_payload(TypeCode::Fixed(0), &payload, &mut scratch, &mut values)
            .unwrap_err();
        assert!(matches!(
            err.kind(),
            ErrorKind::InvalidPreamble { got: [0x00, 0x80] }
        ));
    }

    #[test]
    fn invalid_preamble_second_byte() {
        let payload = [0x80, 0x00, 0x00];
        let mut scratch = PlaneScratch::new(1);
        let mut values = vec![0.0];
        let err = decode_row_payload(TypeCode::Fixed(0), &payload, &mut scratch, &mut values)
            .unwrap_err();
        assert!(matches!(
            err.kind(),
            ErrorKind::InvalidPreamble { got: [0x80, 0x00] }
        ));
    }

    #[test]
    fn truncated_preamble_rejected() {
        let payload = [0x80, 0x80];
        let mut scratch = PlaneScratch::new(1);
        let mut values = vec![0.0];
        let err = decode_row_payload(TypeCode::Fixed(0), &payload, &mut scratch, &mut values)
            .unwrap_err();
        assert!(matches!(err.kind(), ErrorKind::UnexpectedEof));
    }

    #[test]
    fn empty_payload_rejected() {
        let payload = [];
        let mut scratch = PlaneScratch::new(1);
        let mut values = vec![0.0];
        let err = decode_row_payload(TypeCode::Fixed(0), &payload, &mut scratch, &mut values)
            .unwrap_err();
        assert!(matches!(err.kind(), ErrorKind::UnexpectedEof));
    }

    #[test]
    fn type_d_selective_b0_and_b4() {
        // Type D tables accept selective plane descriptors. Descriptor 0x88
        // means B0 and B4 present, all other planes zero-filled.
        let mut data = Vec::new();
        push_encoded_plane(&[0x00], &mut data); // B0
        push_encoded_plane(&[0x00], &mut data); // B4
        let payload = make_payload(0x88, &data);
        let mut scratch = PlaneScratch::new(1);
        let mut values = vec![999.0];
        decode_row_payload(TypeCode::Float64, &payload, &mut scratch, &mut values).unwrap();
        // B0=0x00, B4=0x00, everything else zero → all-zero f64.
        assert_eq!(values[0], 0.0);
    }

    #[test]
    fn type_d_selective_reconstructs_value() {
        // Encode f64 value 1.0 = 0x3FF0_0000_0000_0000 in LE:
        // B0=0x00 B1=0x00 B2=0x00 B3=0x00 B4=0x00 B5=0x00 B6=0xF0 B7=0x3F
        // Descriptor with only B6 and B7 present: bit 1 (B6) + bit 0 (B7)
        // = 0x03.
        let mut data = Vec::new();
        push_encoded_plane(&[0xF0], &mut data); // B6
        push_encoded_plane(&[0x3F], &mut data); // B7
        let payload = make_payload(0x03, &data);
        let mut scratch = PlaneScratch::new(1);
        let mut values = vec![0.0];
        decode_row_payload(TypeCode::Float64, &payload, &mut scratch, &mut values).unwrap();
        assert_eq!(values[0], 1.0);
    }

    #[test]
    fn type_d_selective_trailing_bytes_rejected() {
        let mut data = Vec::new();
        push_encoded_plane(&[0xF0], &mut data); // B6
        push_encoded_plane(&[0x3F], &mut data); // B7
        data.push(0xAA); // trailing garbage
        let payload = make_payload(0x03, &data);
        let mut scratch = PlaneScratch::new(1);
        let mut values = vec![0.0];
        let err =
            decode_row_payload(TypeCode::Float64, &payload, &mut scratch, &mut values).unwrap_err();
        assert!(matches!(err.kind(), ErrorKind::TrailingBytes));
    }

    #[test]
    fn type_s_rejects_non_0xf8_descriptor() {
        // Type S tables must use 0x00, 0xFF, or 0xF8.
        let payload = make_payload(0x88, &[]);
        let mut scratch = PlaneScratch::new(1);
        let mut values = vec![0.0];
        let err =
            decode_row_payload(TypeCode::Float32, &payload, &mut scratch, &mut values).unwrap_err();
        assert!(matches!(
            err.kind(),
            ErrorKind::InvalidDescriptor {
                descriptor: 0x88,
                type_code: TypeCode::Float32
            }
        ));
    }

    #[test]
    fn numeric_descriptor_rejects_nonzero_low_bits() {
        // Descriptor 0x81 has bit 0 set, which is reserved.
        let payload = make_payload(0x81, &[]);
        let mut scratch = PlaneScratch::new(1);
        let mut values = vec![0.0];
        let err = decode_row_payload(TypeCode::Fixed(2), &payload, &mut scratch, &mut values)
            .unwrap_err();
        assert!(matches!(
            err.kind(),
            ErrorKind::InvalidDescriptor {
                descriptor: 0x81,
                ..
            }
        ));
    }

    #[test]
    fn numeric_b0_only() {
        // Descriptor 0x80: only B0 present. Three zones with values 10, 20, 30.
        let mut data = Vec::new();
        push_encoded_plane(&[10, 20, 30], &mut data);
        let payload = make_payload(0x80, &data);

        let mut scratch = PlaneScratch::new(3);
        let mut values = vec![0.0; 3];
        decode_row_payload(TypeCode::Fixed(0), &payload, &mut scratch, &mut values).unwrap();
        assert_eq!(values, vec![10.0, 20.0, 30.0]);
    }

    #[test]
    fn numeric_b0_b1_two_byte_magnitude() {
        // Descriptor 0xC0: B0 and B1 present.
        let mut data = Vec::new();
        push_encoded_plane(&[0x01, 0xFF], &mut data);
        push_encoded_plane(&[0x02, 0x00], &mut data);
        let payload = make_payload(0xC0, &data);

        let mut scratch = PlaneScratch::new(2);
        let mut values = vec![0.0; 2];
        decode_row_payload(TypeCode::Fixed(0), &payload, &mut scratch, &mut values).unwrap();
        assert_eq!(values, vec![513.0, 255.0]);
    }

    #[test]
    fn numeric_all_four_magnitude_planes() {
        // Descriptor 0xF0: B0, B1, B2, B3.
        let mut data = Vec::new();
        push_encoded_plane(&[0x78], &mut data);
        push_encoded_plane(&[0x56], &mut data);
        push_encoded_plane(&[0x34], &mut data);
        push_encoded_plane(&[0x12], &mut data);
        let payload = make_payload(0xF0, &data);

        let mut scratch = PlaneScratch::new(1);
        let mut values = vec![0.0];
        decode_row_payload(TypeCode::Fixed(0), &payload, &mut scratch, &mut values).unwrap();
        assert_eq!(values, vec![0x12345678_u32 as f64]);
    }

    #[test]
    fn numeric_b0_and_ss_with_sign_and_scale() {
        // Descriptor 0x88: B0 + SS.
        let mut data = Vec::new();
        push_encoded_plane(&[125, 0, 234], &mut data);
        push_encoded_plane(&[0x20, 0x00, 0x21], &mut data);
        let payload = make_payload(0x88, &data);

        let mut scratch = PlaneScratch::new(3);
        let mut values = vec![0.0; 3];
        decode_row_payload(TypeCode::Fixed(2), &payload, &mut scratch, &mut values).unwrap();
        assert!((values[0] - 1.25).abs() < 1e-15);
        assert_eq!(values[1], 0.0);
        assert!((values[2] - (-2.34)).abs() < 1e-15);
    }

    #[test]
    fn numeric_ss_only() {
        // Descriptor 0x08: SS only.
        let mut data = Vec::new();
        push_encoded_plane(&[0x21, 0x31], &mut data);
        let payload = make_payload(0x08, &data);

        let mut scratch = PlaneScratch::new(2);
        let mut values = vec![999.0; 2];
        decode_row_payload(TypeCode::Fixed(3), &payload, &mut scratch, &mut values).unwrap();
        assert_eq!(values[0], 0.0);
        assert_eq!(values[1], 0.0);
    }

    #[test]
    fn numeric_b0_b1_b2_b3_and_ss() {
        // Descriptor 0xF8: B0..B3 + SS on numeric tables.
        let mut data = Vec::new();
        push_encoded_plane(&[0xE8], &mut data); // B0
        push_encoded_plane(&[0x03], &mut data); // B1
        push_encoded_plane(&[0x00], &mut data); // B2
        push_encoded_plane(&[0x00], &mut data); // B3
        push_encoded_plane(&[0x20], &mut data); // SS
        let payload = make_payload(0xF8, &data);

        let mut scratch = PlaneScratch::new(1);
        let mut values = vec![0.0];
        decode_row_payload(TypeCode::Fixed(2), &payload, &mut scratch, &mut values).unwrap();
        assert!((values[0] - 10.0).abs() < 1e-15);
    }

    #[test]
    fn numeric_trailing_bytes_rejected() {
        // Valid B0 plane for 2 zones, but with an extra byte at the end.
        let mut data = Vec::new();
        push_encoded_plane(&[10, 20], &mut data);
        data.push(0xFF); // trailing garbage
        let payload = make_payload(0x80, &data);

        let mut scratch = PlaneScratch::new(2);
        let mut values = vec![0.0; 2];
        let err = decode_row_payload(TypeCode::Fixed(0), &payload, &mut scratch, &mut values)
            .unwrap_err();
        assert!(matches!(err.kind(), ErrorKind::TrailingBytes));
    }

    #[test]
    fn float32_positive_value() {
        let mut data = Vec::new();
        push_encoded_plane(&[0x00], &mut data); // B0
        push_encoded_plane(&[0x00], &mut data); // B1
        push_encoded_plane(&[0x80], &mut data); // B2
        push_encoded_plane(&[0x3F], &mut data); // B3
        push_encoded_plane(&[0xFF], &mut data); // marker
        let payload = make_payload(0xF8, &data);

        let mut scratch = PlaneScratch::new(1);
        let mut values = vec![0.0];
        decode_row_payload(TypeCode::Float32, &payload, &mut scratch, &mut values).unwrap();
        assert!((values[0] - 1.0).abs() < 1e-7);
    }

    #[test]
    fn float32_negative_value() {
        let bytes = (-2.5_f32).to_le_bytes();
        let mut data = Vec::new();
        push_encoded_plane(&[bytes[0]], &mut data);
        push_encoded_plane(&[bytes[1]], &mut data);
        push_encoded_plane(&[bytes[2]], &mut data);
        push_encoded_plane(&[bytes[3]], &mut data);
        push_encoded_plane(&[0xFF], &mut data);
        let payload = make_payload(0xF8, &data);

        let mut scratch = PlaneScratch::new(1);
        let mut values = vec![0.0];
        decode_row_payload(TypeCode::Float32, &payload, &mut scratch, &mut values).unwrap();
        assert!((values[0] - (-2.5)).abs() < 1e-7);
    }

    #[test]
    fn float32_multiple_zones() {
        let test_values: [f32; 4] = [0.0, 1.5, -100.25, 3.125];
        let mut b0 = Vec::new();
        let mut b1 = Vec::new();
        let mut b2 = Vec::new();
        let mut b3 = Vec::new();
        for &v in &test_values {
            let bytes = v.to_le_bytes();
            b0.push(bytes[0]);
            b1.push(bytes[1]);
            b2.push(bytes[2]);
            b3.push(bytes[3]);
        }

        let mut data = Vec::new();
        push_encoded_plane(&b0, &mut data);
        push_encoded_plane(&b1, &mut data);
        push_encoded_plane(&b2, &mut data);
        push_encoded_plane(&b3, &mut data);
        push_encoded_plane(&[0xFF; 4], &mut data);
        let payload = make_payload(0xF8, &data);

        let mut scratch = PlaneScratch::new(4);
        let mut values = vec![0.0; 4];
        decode_row_payload(TypeCode::Float32, &payload, &mut scratch, &mut values).unwrap();

        for (j, &expected) in test_values.iter().enumerate() {
            assert!(
                (values[j] - expected as f64).abs() < 1e-6,
                "zone {}: expected {}, got {}",
                j,
                expected,
                values[j]
            );
        }
    }

    #[test]
    fn float32_bad_marker_rejected() {
        let mut data = Vec::new();
        push_encoded_plane(&[0x00, 0x00], &mut data);
        push_encoded_plane(&[0x00, 0x00], &mut data);
        push_encoded_plane(&[0x80, 0x80], &mut data);
        push_encoded_plane(&[0x3F, 0x3F], &mut data);
        // Marker plane with a bad byte at position 1.
        push_encoded_plane(&[0xFF, 0xFE], &mut data);
        let payload = make_payload(0xF8, &data);

        let mut scratch = PlaneScratch::new(2);
        let mut values = vec![0.0; 2];
        let err =
            decode_row_payload(TypeCode::Float32, &payload, &mut scratch, &mut values).unwrap_err();
        assert!(matches!(
            err.kind(),
            ErrorKind::InvalidFloat32Marker {
                marker_index: 1,
                got: 0xFE
            }
        ));
    }

    #[test]
    fn float32_trailing_bytes_rejected() {
        let mut data = Vec::new();
        push_encoded_plane(&[0x00], &mut data);
        push_encoded_plane(&[0x00], &mut data);
        push_encoded_plane(&[0x80], &mut data);
        push_encoded_plane(&[0x3F], &mut data);
        push_encoded_plane(&[0xFF], &mut data);
        data.push(0xAA);
        let payload = make_payload(0xF8, &data);

        let mut scratch = PlaneScratch::new(1);
        let mut values = vec![0.0];
        let err =
            decode_row_payload(TypeCode::Float32, &payload, &mut scratch, &mut values).unwrap_err();
        assert!(matches!(err.kind(), ErrorKind::TrailingBytes));
    }

    #[test]
    fn float64_positive_value() {
        let bytes = 1.0_f64.to_le_bytes();
        let mut data = Vec::new();
        for &b in &bytes {
            push_encoded_plane(&[b], &mut data);
        }
        let payload = make_payload(0xFF, &data);

        let mut scratch = PlaneScratch::new(1);
        let mut values = vec![0.0];
        decode_row_payload(TypeCode::Float64, &payload, &mut scratch, &mut values).unwrap();
        assert_eq!(values[0], 1.0);
    }

    #[test]
    fn float64_negative_value() {
        let bytes = (-123.456_f64).to_le_bytes();
        let mut data = Vec::new();
        for &b in &bytes {
            push_encoded_plane(&[b], &mut data);
        }
        let payload = make_payload(0xFF, &data);

        let mut scratch = PlaneScratch::new(1);
        let mut values = vec![0.0];
        decode_row_payload(TypeCode::Float64, &payload, &mut scratch, &mut values).unwrap();
        assert!((values[0] - (-123.456)).abs() < 1e-10);
    }

    #[test]
    fn float64_multiple_zones() {
        let test_values: [f64; 3] = [0.0, std::f64::consts::PI, -1e30];
        let mut planes = vec![Vec::new(); 8];
        for &v in &test_values {
            let bytes = v.to_le_bytes();
            for (p, &b) in planes.iter_mut().zip(bytes.iter()) {
                p.push(b);
            }
        }

        let mut data = Vec::new();
        for plane_bytes in &planes {
            push_encoded_plane(plane_bytes, &mut data);
        }
        let payload = make_payload(0xFF, &data);

        let mut scratch = PlaneScratch::new(3);
        let mut values = vec![0.0; 3];
        decode_row_payload(TypeCode::Float64, &payload, &mut scratch, &mut values).unwrap();

        for (j, &expected) in test_values.iter().enumerate() {
            assert_eq!(
                values[j], expected,
                "zone {}: expected {}, got {}",
                j, expected, values[j]
            );
        }
    }

    #[test]
    fn float64_on_numeric_table() {
        // Any table can use descriptor 0xFF (the overflow escape hatch).
        let bytes = 42.0_f64.to_le_bytes();
        let mut data = Vec::new();
        for &b in &bytes {
            push_encoded_plane(&[b], &mut data);
        }
        let payload = make_payload(0xFF, &data);

        let mut scratch = PlaneScratch::new(1);
        let mut values = vec![0.0];
        decode_row_payload(TypeCode::Fixed(2), &payload, &mut scratch, &mut values).unwrap();
        assert_eq!(values[0], 42.0);
    }

    #[test]
    fn float64_on_float32_table() {
        // Type S tables also accept descriptor 0xFF.
        let bytes = 99.9_f64.to_le_bytes();
        let mut data = Vec::new();
        for &b in &bytes {
            push_encoded_plane(&[b], &mut data);
        }
        let payload = make_payload(0xFF, &data);

        let mut scratch = PlaneScratch::new(1);
        let mut values = vec![0.0];
        decode_row_payload(TypeCode::Float32, &payload, &mut scratch, &mut values).unwrap();
        assert!((values[0] - 99.9).abs() < 1e-10);
    }

    #[test]
    fn float64_trailing_bytes_rejected() {
        let bytes = 1.0_f64.to_le_bytes();
        let mut data = Vec::new();
        for &b in &bytes {
            push_encoded_plane(&[b], &mut data);
        }
        data.push(0xAA);
        let payload = make_payload(0xFF, &data);

        let mut scratch = PlaneScratch::new(1);
        let mut values = vec![0.0];
        let err =
            decode_row_payload(TypeCode::Float64, &payload, &mut scratch, &mut values).unwrap_err();
        assert!(matches!(err.kind(), ErrorKind::TrailingBytes));
    }

    #[test]
    fn numeric_round_trip_with_sparse_data() {
        let zone_count = 100;
        let mut b0_raw = vec![0u8; zone_count];
        let mut ss_raw = vec![0u8; zone_count];

        b0_raw[5] = 123;
        ss_raw[5] = 0x20;

        b0_raw[50] = 45;
        ss_raw[50] = 0x01;

        let mut data = Vec::new();
        push_encoded_plane(&b0_raw, &mut data);
        push_encoded_plane(&ss_raw, &mut data);
        let payload = make_payload(0x88, &data);

        let mut scratch = PlaneScratch::new(zone_count);
        let mut values = vec![0.0; zone_count];
        decode_row_payload(TypeCode::Fixed(2), &payload, &mut scratch, &mut values).unwrap();

        assert!((values[5] - 1.23).abs() < 1e-15);
        assert!((values[50] - (-45.0)).abs() < 1e-15);

        for (j, &v) in values.iter().enumerate() {
            if j != 5 && j != 50 {
                assert_eq!(v, 0.0, "zone {} should be zero, got {}", j, v);
            }
        }
    }

    #[test]
    fn float64_round_trip_through_planes() {
        let zone_count = 50;
        let original: Vec<f64> = (0..zone_count).map(|i| (i as f64) * 1.1 - 25.0).collect();

        let mut planes = vec![vec![0u8; zone_count]; 8];
        for (j, &v) in original.iter().enumerate() {
            let bytes = v.to_le_bytes();
            for (p, &b) in planes.iter_mut().zip(bytes.iter()) {
                p[j] = b;
            }
        }

        let mut data = Vec::new();
        for plane_bytes in &planes {
            push_encoded_plane(plane_bytes, &mut data);
        }
        let payload = make_payload(0xFF, &data);

        let mut scratch = PlaneScratch::new(zone_count);
        let mut values = vec![0.0; zone_count];
        decode_row_payload(TypeCode::Float64, &payload, &mut scratch, &mut values).unwrap();

        assert_eq!(values, original);
    }
}
