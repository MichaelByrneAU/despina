//! Reusable row buffer for streaming matrix reads.
//!
//! [`RowBuf`] is a caller-owned destination for
//! [`Reader::read_row`](crate::Reader::read_row). The reader overwrites it on
//! each successful call, so one `RowBuf` can be reused for an entire file with
//! a stable allocation after first growth.
//!
//! The buffer stores one decoded row from one table:
//! - `values`: one cell per destination zone, in destination order.
//! - `row_index`: 1-based origin row index.
//! - `table_index`: 1-based table index.
//!
//! Before the first successful read, indices are zero and `values` is empty
//! (unless created with [`RowBuf::with_zone_count`]).
//!
//! Decoded values are always exposed as [`f64`]:
//! - Type `D` rows are read as `f64` bit patterns.
//! - Type `S` rows are read as `f32` then widened to `f64`.
//! - Fixed-point rows are reconstructed from integer magnitude plus decimal
//!   scale. These values are mathematically defined by the encoded
//!   integer/scale pair, but fractional decimals are not generally exact in
//!   binary64.
//!
//! # Example
//!
//! ```
//! use despina::{MatrixBuilder, ReaderBuilder, RowBuf, TypeCode};
//!
//! fn row_sums(bytes: &[u8]) -> despina::Result<Vec<f64>> {
//!     let mut reader = ReaderBuilder::new().from_bytes(bytes)?;
//!     let mut row = RowBuf::new();
//!     let mut sums = Vec::new();
//!
//!     while reader.read_row(&mut row)? {
//!         sums.push(row.values().iter().sum());
//!     }
//!     Ok(sums)
//! }
//!
//! let mut matrix = MatrixBuilder::new(2)
//!     .table("DIST_AM", TypeCode::Float32)
//!     .build()?;
//! matrix.set_by_name("DIST_AM", 1, 1, 1.0);
//! matrix.set_by_name("DIST_AM", 1, 2, 2.0);
//! matrix.set_by_name("DIST_AM", 2, 1, 3.0);
//! matrix.set_by_name("DIST_AM", 2, 2, 4.0);
//! let mut bytes = Vec::new();
//! matrix.write_to_writer(&mut bytes)?;
//!
//! let sums = row_sums(&bytes)?;
//! assert_eq!(sums, vec![3.0, 7.0]);
//! # Ok::<(), despina::Error>(())
//! ```

/// A reusable buffer for one decoded row from a `.mat` matrix file.
///
/// Construct once and pass to [`Reader::read_row`](crate::Reader::read_row)
/// repeatedly. The reader updates row metadata and overwrites value slots in
/// place.
///
/// # Example
///
/// ```
/// use despina::{MatrixBuilder, ReaderBuilder, RowBuf, TypeCode};
///
/// let mut matrix = MatrixBuilder::new(2)
///     .table("DIST_AM", TypeCode::Float32)
///     .build()?;
/// matrix.set_by_name("DIST_AM", 2, 1, 1.0);
/// matrix.set_by_name("DIST_AM", 2, 2, 2.0);
/// let mut bytes = Vec::new();
/// matrix.write_to_writer(&mut bytes)?;
///
/// let mut reader = ReaderBuilder::new().from_bytes(&bytes)?;
/// let mut row = RowBuf::new();
/// let mut totals = Vec::new();
///
/// while reader.read_row(&mut row)? {
///     if row.is_zero_row() {
///         continue; // skip sparse zero rows
///     }
///     totals.push(row.values().iter().sum::<f64>());
/// }
/// assert_eq!(totals, vec![3.0]);
/// # Ok::<(), despina::Error>(())
/// ```
#[derive(Debug, Clone)]
pub struct RowBuf {
    row_index: u16,
    table_index: u8,
    is_zero_row: bool,
    values: Vec<f64>,
}

impl RowBuf {
    /// Create an empty row buffer.
    ///
    /// To pre-allocate for a known zone count, use [`RowBuf::with_zone_count`].
    #[must_use]
    pub fn new() -> Self {
        Self {
            row_index: 0,
            table_index: 0,
            is_zero_row: false,
            values: Vec::new(),
        }
    }

    /// Create a row buffer pre-allocated for `zone_count` destination zones.
    ///
    /// This is equivalent to [`RowBuf::new`] followed by an internal resize to
    /// `zone_count` entries. Use this constructor when you already have the
    /// zone count from a previously parsed [`Header`](crate::Header) and want
    /// to avoid the one-time reallocation that would otherwise occur on the
    /// first [`Reader::read_row`](crate::Reader::read_row) call.
    ///
    /// The values are initialised to zero and indices are zero.
    #[must_use]
    pub fn with_zone_count(zone_count: u16) -> Self {
        Self {
            row_index: 0,
            table_index: 0,
            is_zero_row: false,
            values: vec![0.0; usize::from(zone_count)],
        }
    }

    /// Returns the 1-based origin row index of the current row.
    ///
    /// Returns 0 before the first successful
    /// [`Reader::read_row`](crate::Reader::read_row) call.
    #[inline]
    pub fn row_index(&self) -> u16 {
        self.row_index
    }

    /// Returns the 1-based table index of the current row.
    ///
    /// Returns 0 before the first successful
    /// [`Reader::read_row`](crate::Reader::read_row) call.
    #[inline]
    pub fn table_index(&self) -> u8 {
        self.table_index
    }

    /// Returns the decoded cell values, one per destination zone.
    ///
    /// After a successful [`Reader::read_row`](crate::Reader::read_row) call,
    /// this slice has exactly [`Header::zone_count`](crate::Header::zone_count)
    /// entries. Each element is the OD cell value for the origin zone given by
    /// [`row_index`](RowBuf::row_index) and the destination zone at the
    /// corresponding position (destination 1 at index 0, destination 2 at index
    /// 1, and so on).
    ///
    /// All values are exposed as [`f64`] regardless of on-disk type code.
    ///
    /// The slice is empty before the first successful read.
    #[inline]
    pub fn values(&self) -> &[f64] {
        &self.values
    }

    /// Returns whether the current row was encoded as an all-zero shortcut.
    ///
    /// In the `.mat` binary format, rows where every destination cell is zero
    /// are encoded with the special descriptor byte `0x00`, which allows the
    /// reader to skip all decompression work and simply fill the values with
    /// zeros. This is common in sparse origin-destination matrices where many
    /// origin zones have no trips to any destination.
    ///
    /// This flag is purely informational. The values in the buffer are already
    /// set to zero when this returns `true`. Callers who want to skip
    /// processing of zero rows can check this flag as a fast path without
    /// scanning the values slice.
    ///
    /// Returns `false` before the first successful
    /// [`Reader::read_row`](crate::Reader::read_row) call.
    #[inline]
    pub fn is_zero_row(&self) -> bool {
        self.is_zero_row
    }

    /// Prepares the buffer for the reader to fill with decoded values.
    ///
    /// This resizes the internal values storage to `zone_count` if needed, and
    /// sets the indices and zero-row flag to the values from the row record
    /// that is about to be decoded. After this call, the reader writes decoded
    /// values directly into the slice returned by
    /// [`values_mut`](RowBuf::values_mut).
    pub(crate) fn prepare(&mut self, row_index: u16, table_index: u8, zone_count: usize) {
        self.row_index = row_index;
        self.table_index = table_index;
        self.is_zero_row = false;
        self.values.resize(zone_count, 0.0);
    }

    /// Marks the current row as an all-zero shortcut row and fills all values
    /// with zero.
    pub(crate) fn set_zero_row(&mut self) {
        self.is_zero_row = true;
        self.values.fill(0.0);
    }

    /// Returns a mutable slice of the values buffer for the reader to fill.
    ///
    /// The slice length is the zone count established by the most recent
    /// [`prepare`](RowBuf::prepare) call.
    #[inline]
    pub(crate) fn values_mut(&mut self) -> &mut [f64] {
        &mut self.values
    }
}

impl Default for RowBuf {
    fn default() -> Self {
        Self::new()
    }
}

/// Compares row index, table index, and values. The `is_zero_row` flag is
/// excluded because it reflects how the row was encoded on disk, not its
/// semantic content. `Eq` is intentionally not implemented because the values
/// are `f64`, which does not satisfy the reflexivity requirement for `NaN`.
impl PartialEq for RowBuf {
    fn eq(&self, other: &Self) -> bool {
        self.row_index == other.row_index
            && self.table_index == other.table_index
            && self.values == other.values
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_is_empty() {
        let row = RowBuf::new();
        assert_eq!(row.row_index(), 0);
        assert_eq!(row.table_index(), 0);
        assert!(row.values().is_empty());
        assert!(!row.is_zero_row());
    }

    #[test]
    fn default_matches_new() {
        let row = RowBuf::default();
        assert_eq!(row.row_index(), 0);
        assert_eq!(row.table_index(), 0);
        assert!(row.values().is_empty());
        assert!(!row.is_zero_row());

        let fresh = RowBuf::new();
        assert_eq!(row, fresh);
    }

    #[test]
    fn with_zone_count_preallocates() {
        let row = RowBuf::with_zone_count(100);
        assert_eq!(row.values().len(), usize::from(100u16));
        assert!(row.values().iter().all(|&v| v == 0.0));
        assert_eq!(row.row_index(), 0);
        assert_eq!(row.table_index(), 0);
        assert!(!row.is_zero_row());
    }

    #[test]
    fn prepare_sets_indices_and_resizes() {
        let mut row = RowBuf::new();
        assert!(row.values().is_empty());

        row.prepare(5, 2, 50);
        assert_eq!(row.row_index(), 5);
        assert_eq!(row.table_index(), 2);
        assert_eq!(row.values().len(), 50);
        assert!(!row.is_zero_row());
    }

    #[test]
    fn prepare_reuses_allocation() {
        let mut row = RowBuf::new();
        row.prepare(1, 1, 100);
        let ptr_first = row.values().as_ptr();

        // Second prepare with the same zone count should reuse the allocation.
        row.prepare(2, 1, 100);
        let ptr_second = row.values().as_ptr();
        assert_eq!(ptr_first, ptr_second);
    }

    #[test]
    fn set_zero_row_fills_zeros_and_sets_flag() {
        let mut row = RowBuf::new();
        row.prepare(3, 1, 10);

        // Simulate the reader having written some values.
        row.values_mut()[0] = 42.0;
        row.values_mut()[5] = 7.5;

        row.set_zero_row();
        assert!(row.is_zero_row());
        assert!(row.values().iter().all(|&v| v == 0.0));
    }

    #[test]
    fn prepare_clears_zero_row_flag() {
        let mut row = RowBuf::new();
        row.prepare(1, 1, 10);
        row.set_zero_row();
        assert!(row.is_zero_row());

        // Next prepare should reset the flag.
        row.prepare(2, 1, 10);
        assert!(!row.is_zero_row());
    }

    #[test]
    fn values_mut_writes_through() {
        let mut row = RowBuf::new();
        row.prepare(1, 1, 5);

        let values = row.values_mut();
        values[0] = 1.0;
        values[1] = 2.0;
        values[4] = 5.0;

        assert_eq!(row.values(), &[1.0, 2.0, 0.0, 0.0, 5.0]);
    }

    #[test]
    fn equality_ignores_zero_row_flag() {
        let mut a = RowBuf::new();
        a.prepare(1, 1, 3);
        a.set_zero_row();

        let mut b = RowBuf::new();
        b.prepare(1, 1, 3);
        // b has the same indices and values (all zero) but is_zero_row is false.

        assert_eq!(a, b);
    }

    #[test]
    fn equality_considers_indices() {
        let mut a = RowBuf::new();
        a.prepare(1, 1, 3);

        let mut b = RowBuf::new();
        b.prepare(1, 2, 3);

        assert_ne!(a, b);
    }

    #[test]
    fn clone_has_independent_value_storage() {
        let mut row = RowBuf::new();
        row.prepare(7, 3, 4);
        row.values_mut()[0] = 99.0;

        let mut cloned = row.clone();
        row.values_mut()[0] = 11.0;
        cloned.values_mut()[1] = 22.0;

        assert_ne!(row.values().as_ptr(), cloned.values().as_ptr());
        assert_eq!(row.row_index(), cloned.row_index());
        assert_eq!(row.table_index(), cloned.table_index());
        assert_eq!(row.values(), &[11.0, 0.0, 0.0, 0.0]);
        assert_eq!(cloned.values(), &[99.0, 22.0, 0.0, 0.0]);

        // Equality follows row metadata + value content.
        assert_ne!(row, cloned);
    }
}
