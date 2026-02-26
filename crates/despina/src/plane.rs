//! Byte-plane codec and shared plane buffers.
//!
//! Each command starts with a two-byte header:
//! - byte 0: low 8 bits of run count
//! - byte 1: high 7 bits of run count plus mode bit (bit 7: `1` constant,
//!   `0` literal)
//!
//! The run count range is `1..=32_767`. A constant run carries one fill byte
//! after the header. A literal run carries `count` literal bytes.
//!
//! `decode_plane` expands commands into exactly `destination.len()` bytes and
//! returns how many source bytes were consumed. `encode_plane` emits a greedy
//! stream: constant runs for repeats of length at least 3, literal runs
//! otherwise.

use crate::error::{Error, ErrorKind};

/// Number of plane slots reserved in [`PlaneScratch`].
pub(crate) const MAX_PLANE_COUNT: usize = 8;

// SS and B4 intentionally alias slot 4. Numeric rows use SS, float64 rows use
// B4, and float32 rows reuse SS as temporary marker storage.
pub(crate) const B0: usize = 0;
pub(crate) const B1: usize = 1;
pub(crate) const B2: usize = 2;
pub(crate) const B3: usize = 3;
pub(crate) const SS: usize = 4;
pub(crate) const B4: usize = 4;
pub(crate) const B5: usize = 5;
pub(crate) const B6: usize = 6;
pub(crate) const B7: usize = 7;

/// Powers of 10 indexed by decimal-place count (`0..=15`).
pub(crate) const POW10: [f64; 16] = [
    1.0,
    10.0,
    100.0,
    1_000.0,
    10_000.0,
    100_000.0,
    1_000_000.0,
    10_000_000.0,
    100_000_000.0,
    1_000_000_000.0,
    10_000_000_000.0,
    100_000_000_000.0,
    1_000_000_000_000.0,
    10_000_000_000_000.0,
    100_000_000_000_000.0,
    1_000_000_000_000_000.0,
];

/// Reusable contiguous buffers for up to eight byte planes.
pub(crate) struct PlaneScratch {
    buf: Vec<u8>,
    zone_count: usize,
}

impl PlaneScratch {
    /// Create scratch storage sized for `zone_count` bytes per plane.
    pub(crate) fn new(zone_count: usize) -> Self {
        Self {
            buf: vec![0u8; zone_count * MAX_PLANE_COUNT],
            zone_count,
        }
    }

    /// Return a mutable slice for the plane at `index`, spanning exactly
    /// `zone_count` bytes.
    #[inline]
    pub(crate) fn plane(&mut self, index: usize) -> &mut [u8] {
        let start = index * self.zone_count;
        &mut self.buf[start..start + self.zone_count]
    }

    /// Return an immutable slice for the plane at `index`.
    #[inline]
    pub(crate) fn plane_ref(&self, index: usize) -> &[u8] {
        let start = index * self.zone_count;
        &self.buf[start..start + self.zone_count]
    }

    /// Return mutable slices for all plane slots.
    #[inline]
    pub(crate) fn all_planes_mut(&mut self) -> [&mut [u8]; MAX_PLANE_COUNT] {
        let zc = self.zone_count;
        let (p0, rest) = self.buf.split_at_mut(zc);
        let (p1, rest) = rest.split_at_mut(zc);
        let (p2, rest) = rest.split_at_mut(zc);
        let (p3, rest) = rest.split_at_mut(zc);
        let (p4, rest) = rest.split_at_mut(zc);
        let (p5, rest) = rest.split_at_mut(zc);
        let (p6, p7) = rest.split_at_mut(zc);
        [p0, p1, p2, p3, p4, p5, p6, p7]
    }
}

/// Maximum run count representable in a command header.
const MAX_RUN_COUNT: usize = 32_767;

/// Minimum repeat length emitted as a constant run.
const CONSTANT_RUN_THRESHOLD: usize = 3;

#[inline]
fn constant_run_len(source: &[u8], start: usize, limit: usize) -> usize {
    let value = source[start];
    let mut end = start + 1;
    while end < limit && source[end] == value {
        end += 1;
    }
    end - start
}

/// Decode a plane command stream from `source` into `destination`.
///
/// Writes exactly `destination.len()` bytes and returns the number of consumed
/// bytes from `source`.
///
/// # Errors
///
/// Returns an error if:
///
/// - A command has a zero run count ([`ErrorKind::ZeroRunCount`]).
/// - The decoded output would exceed `destination.len()` bytes
///   ([`ErrorKind::PlaneSize`]).
/// - The source is exhausted before the destination is filled
///   ([`ErrorKind::UnexpectedEof`]).
/// - After consuming all commands, the total decoded size is not exactly
///   `destination.len()` ([`ErrorKind::PlaneSize`]).
pub(crate) fn decode_plane(source: &[u8], destination: &mut [u8]) -> crate::Result<usize> {
    let zone_count = destination.len();
    let mut source_index = 0;
    let mut destination_index = 0;

    while destination_index < zone_count {
        // Every command starts with a two-byte header.
        if source_index + 2 > source.len() {
            return Err(Error::new(ErrorKind::UnexpectedEof));
        }
        let count_lo = source[source_index] as usize;
        let count_hi_mode = source[source_index + 1];
        source_index += 2;

        let count = count_lo | (((count_hi_mode & 0x7F) as usize) << 8);
        if count == 0 {
            return Err(Error::new(ErrorKind::ZeroRunCount));
        }
        if destination_index + count > zone_count {
            return Err(Error::new(ErrorKind::PlaneSize {
                expected: zone_count as u16,
                got: destination_index + count,
            }));
        }

        let is_constant = count_hi_mode & 0x80 != 0;
        if is_constant {
            // Constant run: one fill byte repeated `count` times.
            if source_index >= source.len() {
                return Err(Error::new(ErrorKind::UnexpectedEof));
            }
            let fill = source[source_index];
            source_index += 1;
            destination[destination_index..destination_index + count].fill(fill);
        } else {
            // Literal run: `count` verbatim bytes.
            if source_index + count > source.len() {
                return Err(Error::new(ErrorKind::UnexpectedEof));
            }
            destination[destination_index..destination_index + count]
                .copy_from_slice(&source[source_index..source_index + count]);
            source_index += count;
        }
        destination_index += count;
    }

    debug_assert_eq!(destination_index, zone_count);
    Ok(source_index)
}

/// Encode one plane command stream, appending output to `destination`.
///
/// Uses a greedy scan: constant runs for repeats of length at least 3, literal
/// runs otherwise.
pub(crate) fn encode_plane(source: &[u8], destination: &mut Vec<u8>) {
    let source_size = source.len();
    if source_size == 0 {
        return;
    }

    let literal_chunks = source_size.div_ceil(MAX_RUN_COUNT);
    let reserve = source_size.saturating_add(literal_chunks.saturating_mul(2));
    destination.reserve(reserve);

    let mut index = 0;

    while index < source_size {
        let run_start = index;
        let run_limit = source_size.min(index.saturating_add(MAX_RUN_COUNT));
        let run_count = constant_run_len(source, run_start, run_limit);

        if run_count >= CONSTANT_RUN_THRESHOLD {
            emit_constant_run(destination, run_count, source[run_start]);
            index += run_count;
        } else {
            let literal_start = run_start;
            index = run_start + run_count;

            while index < source_size {
                let peek_start = index;
                let peek_count = constant_run_len(source, peek_start, source_size);

                if peek_count >= CONSTANT_RUN_THRESHOLD {
                    break;
                }

                let literal_len = index - literal_start;
                let remaining = MAX_RUN_COUNT - literal_len;
                let step = peek_count.min(remaining);
                index += step;
                if step == remaining {
                    break;
                }
            }

            let literal_count = index - literal_start;
            emit_literal_run(
                destination,
                &source[literal_start..literal_start + literal_count],
            );
        }
    }
}

/// Append a constant-run command (2-byte header + 1 fill byte).
#[inline]
fn emit_constant_run(destination: &mut Vec<u8>, count: usize, fill: u8) {
    debug_assert!((1..=MAX_RUN_COUNT).contains(&count));
    destination.push((count & 0xFF) as u8);
    destination.push(0x80 | ((count >> 8) & 0x7F) as u8);
    destination.push(fill);
}

/// Append a literal-run command (2-byte header + `data.len()` literal bytes).
#[inline]
fn emit_literal_run(destination: &mut Vec<u8>, data: &[u8]) {
    let count = data.len();
    debug_assert!((1..=MAX_RUN_COUNT).contains(&count));
    destination.push((count & 0xFF) as u8);
    destination.push(((count >> 8) & 0x7F) as u8);
    destination.extend_from_slice(data);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_round_trip(original: &[u8]) {
        let mut encoded = Vec::new();
        encode_plane(original, &mut encoded);

        let mut decoded = vec![0u8; original.len()];
        let consumed = decode_plane(&encoded, &mut decoded).unwrap();
        assert_eq!(consumed, encoded.len());
        assert_eq!(decoded, original);
    }

    #[test]
    fn scratch_planes_are_independent() {
        let mut scratch = PlaneScratch::new(4);
        scratch.plane(B0).copy_from_slice(&[1, 2, 3, 4]);
        scratch.plane(B1).copy_from_slice(&[5, 6, 7, 8]);
        assert_eq!(scratch.plane_ref(B0), &[1, 2, 3, 4]);
        assert_eq!(scratch.plane_ref(B1), &[5, 6, 7, 8]);
    }

    #[test]
    fn scratch_covers_all_float64_planes() {
        let mut scratch = PlaneScratch::new(2);
        for i in 0..MAX_PLANE_COUNT {
            scratch.plane(i).fill(i as u8);
        }
        for i in 0..MAX_PLANE_COUNT {
            assert_eq!(scratch.plane_ref(i), &[i as u8, i as u8]);
        }
    }

    #[test]
    fn decode_constant_run() {
        let source = [0x05, 0x80, 0xAB];
        let mut destination = [0u8; 5];
        let consumed = decode_plane(&source, &mut destination).unwrap();
        assert_eq!(consumed, 3);
        assert_eq!(destination, [0xAB; 5]);
    }

    #[test]
    fn decode_literal_run() {
        let source = [0x04, 0x00, 0x0A, 0x0B, 0x0C, 0x0D];
        let mut destination = [0u8; 4];
        let consumed = decode_plane(&source, &mut destination).unwrap();
        assert_eq!(consumed, 6);
        assert_eq!(destination, [0x0A, 0x0B, 0x0C, 0x0D]);
    }

    #[test]
    fn decode_mixed_commands() {
        let source = [0x03, 0x00, 0x01, 0x02, 0x03, 0x02, 0x80, 0xFF];
        let mut destination = [0u8; 5];
        let consumed = decode_plane(&source, &mut destination).unwrap();
        assert_eq!(consumed, 8);
        assert_eq!(destination, [0x01, 0x02, 0x03, 0xFF, 0xFF]);
    }

    #[test]
    fn decode_large_constant_run() {
        let source = [0x2C, 0x81, 0x7F];
        let mut destination = [0u8; 300];
        let consumed = decode_plane(&source, &mut destination).unwrap();
        assert_eq!(consumed, 3);
        assert_eq!(destination, [0x7F; 300]);
    }

    #[test]
    fn decode_single_byte_constant() {
        let source = [0x01, 0x80, 0x42];
        let mut destination = [0u8; 1];
        let consumed = decode_plane(&source, &mut destination).unwrap();
        assert_eq!(consumed, 3);
        assert_eq!(destination, [0x42]);
    }

    #[test]
    fn decode_single_byte_literal() {
        let source = [0x01, 0x00, 0x42];
        let mut destination = [0u8; 1];
        let consumed = decode_plane(&source, &mut destination).unwrap();
        assert_eq!(consumed, 3);
        assert_eq!(destination, [0x42]);
    }

    #[test]
    fn decode_consumes_exact_source_bytes() {
        let source = [0x02, 0x80, 0xAA, 0xFF, 0xFF, 0xFF];
        let mut destination = [0u8; 2];
        let consumed = decode_plane(&source, &mut destination).unwrap();
        assert_eq!(consumed, 3);
        assert_eq!(destination, [0xAA, 0xAA]);
    }

    #[test]
    fn decode_zero_run_length_rejected() {
        let source = [0x00, 0x00];
        let mut destination = [0u8; 1];
        let err = decode_plane(&source, &mut destination).unwrap_err();
        assert!(matches!(err.kind(), ErrorKind::ZeroRunCount));
    }

    #[test]
    fn decode_overflow_rejected() {
        let source = [0x05, 0x80, 0xAA];
        let mut destination = [0u8; 3];
        let err = decode_plane(&source, &mut destination).unwrap_err();
        assert!(matches!(err.kind(), ErrorKind::PlaneSize { .. }));
    }

    #[test]
    fn decode_truncated_header_rejected() {
        let source = [0x03];
        let mut destination = [0u8; 3];
        let err = decode_plane(&source, &mut destination).unwrap_err();
        assert!(matches!(err.kind(), ErrorKind::UnexpectedEof));
    }

    #[test]
    fn decode_truncated_constant_fill_rejected() {
        let source = [0x03, 0x80];
        let mut destination = [0u8; 3];
        let err = decode_plane(&source, &mut destination).unwrap_err();
        assert!(matches!(err.kind(), ErrorKind::UnexpectedEof));
    }

    #[test]
    fn decode_truncated_literal_data_rejected() {
        let source = [0x05, 0x00, 0xAA, 0xBB];
        let mut destination = [0u8; 5];
        let err = decode_plane(&source, &mut destination).unwrap_err();
        assert!(matches!(err.kind(), ErrorKind::UnexpectedEof));
    }

    #[test]
    fn decode_underflow_rejected() {
        let source = [0x02, 0x80, 0x00];
        let mut destination = [0u8; 5];
        let err = decode_plane(&source, &mut destination).unwrap_err();
        assert!(matches!(err.kind(), ErrorKind::UnexpectedEof));
    }

    #[test]
    fn decode_empty_destination() {
        let source = [];
        let mut destination = [0u8; 0];
        let consumed = decode_plane(&source, &mut destination).unwrap();
        assert_eq!(consumed, 0);
    }

    #[test]
    fn encode_empty_source() {
        let mut destination = Vec::new();
        encode_plane(&[], &mut destination);
        assert!(destination.is_empty());
    }

    #[test]
    fn encode_all_same_bytes() {
        let source = [0xAB; 10];
        let mut destination = Vec::new();
        encode_plane(&source, &mut destination);
        assert_eq!(destination, [0x0A, 0x80, 0xAB]);
    }

    #[test]
    fn encode_all_distinct_bytes() {
        let source = [0x01, 0x02, 0x03, 0x04, 0x05];
        let mut destination = Vec::new();
        encode_plane(&source, &mut destination);
        assert_eq!(destination, [0x05, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05]);
    }

    #[test]
    fn encode_constant_then_literal() {
        let source = [0xAA, 0xAA, 0xAA, 0x01, 0x02];
        let mut destination = Vec::new();
        encode_plane(&source, &mut destination);
        assert_eq!(destination, [0x03, 0x80, 0xAA, 0x02, 0x00, 0x01, 0x02]);
    }

    #[test]
    fn encode_literal_then_constant() {
        let source = [0x01, 0x02, 0xBB, 0xBB, 0xBB, 0xBB];
        let mut destination = Vec::new();
        encode_plane(&source, &mut destination);
        assert_eq!(destination, [0x02, 0x00, 0x01, 0x02, 0x04, 0x80, 0xBB]);
    }

    #[test]
    fn encode_pair_not_promoted_to_constant() {
        let source = [0xAA, 0xAA];
        let mut destination = Vec::new();
        encode_plane(&source, &mut destination);
        assert_eq!(destination, [0x02, 0x00, 0xAA, 0xAA]);
    }

    #[test]
    fn encode_single_byte() {
        let source = [0x42];
        let mut destination = Vec::new();
        encode_plane(&source, &mut destination);
        assert_eq!(destination, [0x01, 0x00, 0x42]);
    }

    #[test]
    fn encode_short_constant_absorbed_into_literal() {
        let source = [0x0A, 0x0B, 0x0B, 0x0C];
        let mut destination = Vec::new();
        encode_plane(&source, &mut destination);
        assert_eq!(destination, [0x04, 0x00, 0x0A, 0x0B, 0x0B, 0x0C]);
    }

    #[test]
    fn encode_alternating_long_runs() {
        let mut source = Vec::new();
        source.extend([0xAA; 100]);
        source.extend([0xBB; 200]);
        source.extend([0xCC; 50]);

        let mut destination = Vec::new();
        encode_plane(&source, &mut destination);
        assert_eq!(destination.len(), 9);
        assert_eq!(&destination[0..3], [100, 0x80, 0xAA]);
        assert_eq!(&destination[3..6], [200, 0x80, 0xBB]);
        assert_eq!(&destination[6..9], [50, 0x80, 0xCC]);
    }

    #[test]
    fn round_trip_all_zeros() {
        let original = [0u8; 500];
        assert_round_trip(&original);
    }

    #[test]
    fn round_trip_ascending_bytes() {
        let original: Vec<u8> = (0..=255).collect();
        assert_round_trip(&original);
    }

    #[test]
    fn round_trip_sparse_with_clusters() {
        let mut original = vec![0u8; 1000];
        original[10] = 0x05;
        original[11] = 0x0A;
        original[500] = 0xFF;
        original[501] = 0xFF;
        original[502] = 0xFF;
        original[503] = 0xFE;
        original[999] = 0x01;
        assert_round_trip(&original);
    }

    #[test]
    fn round_trip_worst_case_no_runs() {
        let original: Vec<u8> = (0..200)
            .map(|i| if i % 2 == 0 { 0xAA } else { 0x55 })
            .collect();
        assert_round_trip(&original);
    }

    #[test]
    fn round_trip_max_run_count_boundary() {
        let original = vec![0x42u8; MAX_RUN_COUNT];
        let mut encoded = Vec::new();
        encode_plane(&original, &mut encoded);
        assert_eq!(encoded.len(), 3);

        let mut decoded = vec![0u8; MAX_RUN_COUNT];
        let consumed = decode_plane(&encoded, &mut decoded).unwrap();
        assert_eq!(consumed, 3);
        assert_eq!(decoded, original);
    }

    #[test]
    fn round_trip_exceeds_max_run_count() {
        let original = vec![0x42u8; MAX_RUN_COUNT + 1];
        let mut encoded = Vec::new();
        encode_plane(&original, &mut encoded);
        assert_eq!(encoded.len(), 6);

        let mut decoded = vec![0u8; MAX_RUN_COUNT + 1];
        let consumed = decode_plane(&encoded, &mut decoded).unwrap();
        assert_eq!(consumed, 6);
        assert_eq!(decoded, original);
    }

    #[test]
    fn round_trip_large_constant_run() {
        let original = vec![0x7Fu8; 300];
        let mut encoded = Vec::new();
        encode_plane(&original, &mut encoded);
        assert_eq!(encoded, [0x2C, 0x81, 0x7F]);

        let mut decoded = vec![0u8; 300];
        let consumed = decode_plane(&encoded, &mut decoded).unwrap();
        assert_eq!(consumed, 3);
        assert_eq!(decoded, original);
    }

    #[test]
    fn round_trip_short_literal_run() {
        let original = [0xAA, 0xBB, 0xCC];
        let mut encoded = Vec::new();
        encode_plane(&original, &mut encoded);
        assert_eq!(encoded, [0x03, 0x00, 0xAA, 0xBB, 0xCC]);

        let mut decoded = [0u8; 3];
        let consumed = decode_plane(&encoded, &mut decoded).unwrap();
        assert_eq!(consumed, 5);
        assert_eq!(decoded, original);
    }
}
