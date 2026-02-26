//! Streaming writer for `.mat` matrix files.
//!
//! [`WriterBuilder`] constructs a [`Writer`] from a file path or arbitrary
//! [`Write`] sink. Construction validates dimensions and table definitions,
//! then writes the five-record header immediately so row writes can stream
//! directly.
//!
//! `.mat` row data is stored as a sequence of **row records**. Each record
//! contains one full destination vector (`zone_count` values) for one
//! `(origin, table)` pair. Records must be written in strict canonical order:
//! all tables for origin 1, then all tables for origin 2, and so on.
//!
//! In this documentation, a **stack** means the full matrix payload as
//! `(table, origin, destination)` values. For `T` tables and `N` zones, that is
//! `T × N × N` values flattened in table-major order, then origin, then
//! destination.
//!
//! Choosing a write path:
//! - [`Writer::write_stack`] is the most ergonomic path when the full matrix is
//!   already available in `(table, origin, destination)` layout. It validates
//!   shape once, then writes rows in a tight loop.
//! - [`Writer::write_origin`] and [`Writer::write_origins`] are designed for
//!   origin-block workflows. They avoid materialising a full stack and enforce
//!   origin-boundary ordering checks for you.
//! - [`Writer::write_next_row`] is the low-level path for row-at-a-time
//!   pipelines and transforms. It offers maximum control, but puts loop control
//!   and completion discipline on the caller.
//!
//! All three paths use the same row encoder and can be mixed in one writer as
//! long as calls remain in canonical order.
//!
//! Choose finalisation by intent:
//!
//! - [`Writer::finish`] is the normal path: it verifies completeness, flushes,
//!   and returns the sink.
//! - [`Writer::into_inner`] flushes and returns the sink without a completeness
//!   check (escape hatch for recovery flows).
//!
//! ```
//! use despina::{ReaderBuilder, RowBuf, TableDef, TypeCode, WriterBuilder};
//!
//! let tables = [TableDef::new("DIST_AM", TypeCode::Float32)];
//! let mut writer = WriterBuilder::new().open_writer(Vec::new(), 3, &tables)?;
//!
//! // One table -> stack layout is (origin, destination) rows.
//! writer.write_stack(&[
//!     1.0, 2.0, 3.0, // origin 1
//!     1.0, 2.0, 3.0, // origin 2
//!     1.0, 2.0, 3.0, // origin 3
//! ])?;
//! let bytes = writer.finish()?;
//!
//! let mut reader = ReaderBuilder::new().from_bytes(&bytes)?;
//! let mut row = RowBuf::new();
//! let mut row_count = 0;
//! while reader.read_row(&mut row)? {
//!     row_count += 1;
//!     assert_eq!(row.values(), &[1.0, 2.0, 3.0]);
//! }
//! assert_eq!(row_count, 3);
//! # Ok::<(), despina::Error>(())
//! ```
//!
//! Operational notes:
//!
//! - If upstream already has full table-major data, start with `write_stack`.
//! - If upstream produces one origin at a time, prefer `write_origin` or
//!   `write_origins`.
//! - Use `write_next_row` when data naturally arrives row-by-row.
//! - `write_next_row` requires `values.len() == zone_count` for every call.
//! - The writer wraps sinks in an internal [`std::io::BufWriter`], so adding
//!   another `BufWriter` layer is usually unnecessary.
//! - [`Reader`](crate::Reader) is the streaming counterpart for reading, and
//!   [`Matrix::write_to`](crate::Matrix::write_to) offers round-trip
//!   convenience from the in-memory API.

use std::fmt;
#[cfg(not(target_arch = "wasm32"))]
use std::fs::File;
use std::io::{self, BufWriter, Write};
#[cfg(not(target_arch = "wasm32"))]
use std::path::Path;

use crate::MAX_ZONE_COUNT;
use crate::encode;
use crate::error::{Error, ErrorKind, IntoInnerError, Result};
use crate::header::Header;
use crate::matrix::TableDef;
use crate::plane::PlaneScratch;
use crate::table_defs::{table_infos_from_defs, validate_table_defs};

const DEFAULT_WRITER_BANNER: &str = concat!("MAT PGM=DESPINA VER=", env!("CARGO_PKG_VERSION"));
const DEFAULT_WRITER_RUN_ID: &str = "DESPINA";

/// Configures and constructs a [`Writer`].
///
/// `WriterBuilder` captures optional header metadata and opens writers for a
/// concrete sink and matrix shape. Both banner and run identifier have sensible
/// defaults, so callers that do not need custom metadata can go straight to
/// [`open_writer`](WriterBuilder::open_writer) or
/// [`open_path`](WriterBuilder::open_path).
///
/// # Example
///
/// ```
/// use despina::{ReaderBuilder, TableDef, TypeCode, WriterBuilder};
///
/// let tables = [TableDef::new("DIST_AM", TypeCode::Fixed(2))];
/// let mut plan = WriterBuilder::new();
/// plan
///     .banner("MAT PGM=MYAPP VER=1")
///     .run_id("Morning Peak");
///
/// let mut writer = plan.open_writer(Vec::new(), 1, &tables)?;
/// writer.write_stack(&[12.34])?;
/// let bytes = writer.finish()?;
///
/// let reader = ReaderBuilder::new().from_bytes(&bytes)?;
/// assert_eq!(reader.header().banner(), "MAT PGM=MYAPP VER=1");
/// assert_eq!(reader.header().run_id(), "Morning Peak");
/// # Ok::<(), despina::Error>(())
/// ```
#[derive(Debug, Clone)]
#[must_use]
pub struct WriterBuilder {
    banner: String,
    run_id: String,
}

impl Default for WriterBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl WriterBuilder {
    /// The default banner is `MAT PGM=DESPINA VER=<crate version>` and the
    /// default run identifier is `DESPINA`.
    pub fn new() -> Self {
        Self {
            banner: DEFAULT_WRITER_BANNER.to_owned(),
            run_id: DEFAULT_WRITER_RUN_ID.to_owned(),
        }
    }

    /// Sets the banner text for the first header record.
    ///
    /// The banner is free-form text identifying the generating program and
    /// version. It has no structural significance and is purely informational.
    ///
    /// Default: `MAT PGM=DESPINA VER=<crate version>`.
    pub fn banner(&mut self, text: impl Into<String>) -> &mut Self {
        self.banner = text.into();
        self
    }

    /// Sets the run identifier for the second header record.
    ///
    /// The run identifier is stored with an `ID=` prefix in the binary file.
    /// It is purely informational metadata.
    ///
    /// Default: `DESPINA`.
    pub fn run_id(&mut self, id: impl Into<String>) -> &mut Self {
        self.run_id = id.into();
        self
    }

    /// Opens a writer from any [`Write`] implementation.
    ///
    /// The writer wraps `writer` in an internal
    /// [`BufWriter`](std::io::BufWriter) with a fixed capacity. In most cases,
    /// callers should pass the raw sink directly instead of layering another
    /// buffer.
    ///
    /// The five-record header is written immediately. After this method
    /// returns, the writer is positioned at the start of row data and ready for
    /// [`Writer::write_next_row`] calls.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - `zone_count` is outside the supported range
    ///   `1..=[crate::MAX_ZONE_COUNT]` ([`ErrorKind::InvalidZoneCount`])
    /// - `tables` is empty or has more than [`crate::MAX_TABLE_COUNT`] entries
    ///   ([`ErrorKind::InvalidTableCount`])
    /// - Any table type code is invalid for `.mat`
    ///   ([`ErrorKind::InvalidTypeCode`])
    /// - Any table name is empty or non-ASCII ([`ErrorKind::InvalidTableName`])
    /// - An I/O error occurs while writing the header
    pub fn open_writer<W: Write>(
        &self,
        writer: W,
        zone_count: u16,
        tables: &[TableDef],
    ) -> Result<Writer<W>> {
        Writer::new(writer, zone_count, tables, &self.banner, &self.run_id)
    }

    /// Opens a writer from a file path.
    ///
    /// Creates (or truncates) the file at `path`, then writes the header.
    /// The returned writer uses an internal [`BufWriter`](std::io::BufWriter),
    /// so external buffering is usually unnecessary.
    ///
    /// This is a convenience wrapper around
    /// [`open_writer`](WriterBuilder::open_writer).
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be created or the header arguments
    /// are invalid.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn open_path<P: AsRef<Path>>(
        &self,
        path: P,
        zone_count: u16,
        tables: &[TableDef],
    ) -> Result<Writer<File>> {
        let file = File::create(path)?;
        self.open_writer(file, zone_count, tables)
    }
}

/// A streaming writer for `.mat` matrix files.
///
/// The writer encodes rows one at a time, reusing internal buffers to avoid
/// per-row allocations after the first row. Rows are written in strict
/// row-major, table-major order. The writer tracks position internally.
///
/// The public write methods represent three control levels:
/// - stack-level (`write_stack`)
/// - origin-block-level (`write_origin`, `write_origins`)
/// - row-level (`write_next_row`)
///
/// Choose the highest-level method that matches upstream data layout. Higher
/// level methods reduce caller-side loops and centralise ordering checks.
///
/// Data-shape quick reference:
/// - row: `(zone_count,)`
/// - origin block: `(table_count, zone_count)`
/// - origins batch: `(origin_count, table_count, zone_count)`
/// - full stack: `(table_count, zone_count, zone_count)`
///
/// The writer uses an internal [`BufWriter`](std::io::BufWriter) with a 64 KiB
/// capacity. Dropping the writer attempts to flush buffered data, but drop
/// cannot report I/O errors. To surface flush failures and verify completeness,
/// call [`finish`](Writer::finish) explicitly.
pub struct Writer<W: Write> {
    sink: Option<BufWriter<W>>,
    header: Header,
    payload: Vec<u8>,
    scratch: PlaneScratch,
    state: WriteState,
}

impl<W: Write> Writer<W> {
    /// Opens a writer from any [`Write`] implementation.
    ///
    /// The writer wraps `writer` in an internal
    /// [`BufWriter`](std::io::BufWriter), so wrapping with another buffer is
    /// usually unnecessary.
    ///
    /// Equivalent to
    /// `WriterBuilder::new().open_writer(writer, zone_count, tables)`.
    pub fn open_writer(writer: W, zone_count: u16, tables: &[TableDef]) -> Result<Self> {
        WriterBuilder::new().open_writer(writer, zone_count, tables)
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl Writer<File> {
    /// Opens a writer from a file path.
    ///
    /// The returned writer uses an internal [`BufWriter`](std::io::BufWriter),
    /// so wrapping the file in another buffered writer is usually unnecessary.
    ///
    /// Equivalent to
    /// `WriterBuilder::new().open_path(path, zone_count, tables)`.
    pub fn open_path<P: AsRef<Path>>(
        path: P,
        zone_count: u16,
        tables: &[TableDef],
    ) -> Result<Self> {
        WriterBuilder::new().open_path(path, zone_count, tables)
    }
}

/// Default internal write buffer size (64 KiB).
const DEFAULT_WRITE_BUFFER_SIZE: usize = 64 * 1024;

const ROW_RECORD_HEADER_SIZE: usize = 5;

impl<W: Write> Writer<W> {
    fn new(
        mut sink: W,
        zone_count: u16,
        tables: &[TableDef],
        banner: &str,
        run_id: &str,
    ) -> Result<Self> {
        if zone_count == 0 || zone_count > MAX_ZONE_COUNT {
            return Err(Error::new(ErrorKind::InvalidZoneCount));
        }
        validate_table_defs(tables)?;
        let table_infos = table_infos_from_defs(tables);

        let table_count = table_infos.len() as u8;
        let header = Header::new(
            banner.to_owned(),
            run_id.to_owned(),
            zone_count,
            table_infos,
        );

        header.write_to(&mut sink)?;

        let row_count = u32::from(zone_count) * u32::from(table_count);
        let state = WriteState::new(table_count, row_count);
        let scratch = PlaneScratch::new(usize::from(zone_count));

        Ok(Self {
            sink: Some(BufWriter::with_capacity(DEFAULT_WRITE_BUFFER_SIZE, sink)),
            header,
            payload: Vec::with_capacity(ROW_RECORD_HEADER_SIZE + usize::from(zone_count) * 8),
            scratch,
            state,
        })
    }

    fn write_current_row(&mut self, values: &[f64]) -> Result<()> {
        if self.state.done() {
            return Err(Error::new(ErrorKind::WriterFinished));
        }

        let row_index = self.state.next_row;
        let table_index = self.state.next_table;

        let zone_count = self.header.zone_count();
        if values.len() != usize::from(zone_count) {
            return Err(Error::new(ErrorKind::ZoneCountMismatch {
                expected: zone_count,
                got: values.len(),
            }));
        }

        let type_code = self.header.tables()[usize::from(table_index - 1)].type_code();
        debug_assert!(table_index >= 1, "table_index must be 1-based");

        // Reserve record-header bytes. The encoder appends payload after them.
        self.payload.clear();
        self.payload.resize(ROW_RECORD_HEADER_SIZE, 0);
        encode::encode_row_payload(type_code, values, &mut self.scratch, &mut self.payload);

        // `chunk_size` is u16 and includes its own 2-byte field.
        let payload_size = self.payload.len() - ROW_RECORD_HEADER_SIZE;
        let payload_plus_chunk = payload_size + 2;
        if payload_plus_chunk > u16::MAX as usize {
            return Err(Error::new(ErrorKind::PayloadTooLarge(payload_size)));
        }
        let chunk_size = payload_plus_chunk as u16;
        self.payload[0..2].copy_from_slice(&row_index.to_le_bytes());
        self.payload[2] = table_index;
        self.payload[3..5].copy_from_slice(&chunk_size.to_le_bytes());

        let sink = self
            .sink
            .as_mut()
            .expect("sink is always Some until consumed by into_inner or finish");
        sink.write_all(&self.payload)?;

        self.state.advance();

        Ok(())
    }

    /// Writes one canonical next row and advances the writer cursor.
    ///
    /// This is the low-level row API. Prefer
    /// [`write_stack`](Writer::write_stack),
    /// [`write_origin`](Writer::write_origin), or
    /// [`write_origins`](Writer::write_origins) when callers can provide data
    /// in block form.
    ///
    /// `values` is one destination vector for the writer's current expected
    /// `(origin, table)` coordinate.
    ///
    /// Use this when your producer already emits one canonical row at a time.
    /// The tradeoff is that shape checks happen per call and caller-side loop
    /// overhead is higher than bulk methods.
    pub fn write_next_row(&mut self, values: &[f64]) -> Result<()> {
        self.write_current_row(values)
    }

    /// Writes one full origin block in canonical table order.
    ///
    /// `origin_block` must contain exactly `table_count × zone_count` values in
    /// contiguous table-major row layout. Conceptually this is one 2D block
    /// shaped `(table_count, zone_count)` for the next expected origin.
    ///
    /// This is the most direct API when upstream computes all tables for one
    /// origin together. It avoids manual per-table loops and rejects calls made
    /// mid-origin so canonical ordering stays intact.
    pub fn write_origin(&mut self, origin_block: &[f64]) -> Result<()> {
        let table_count = usize::from(self.header.table_count());
        let zone_count = usize::from(self.header.zone_count());
        let expected = table_count * zone_count;
        if origin_block.len() != expected {
            return Err(Error::new(ErrorKind::ShapeMismatch {
                context: "write_origin",
                expected,
                got: origin_block.len(),
            }));
        }

        if self.state.next_table != 1 {
            return Err(Error::new(ErrorKind::WriterPositionMismatch {
                expected_origin: self.state.next_row,
                expected_table: self.state.next_table,
                got_origin: self.state.next_row,
                got_table: 1,
            }));
        }

        for table_offset in 0..table_count {
            let start = table_offset * zone_count;
            let end = start + zone_count;
            self.write_current_row(&origin_block[start..end])?;
        }
        Ok(())
    }

    /// Writes multiple full origin blocks in canonical order.
    ///
    /// `origins` must contain exactly
    /// `origin_count × table_count × zone_count` values laid out as
    /// `(origin, table, destination)`. This is the natural API when upstream
    /// code produces one or more complete origin slices at a time.
    ///
    /// Compared with repeated [`write_origin`](Writer::write_origin) calls,
    /// this reduces caller-side loop overhead while preserving explicit origin
    /// counts and shape validation.
    pub fn write_origins(&mut self, origins: &[f64], origin_count: u16) -> Result<usize> {
        let table_count = usize::from(self.header.table_count());
        let zone_count = usize::from(self.header.zone_count());
        let origin_count = usize::from(origin_count);
        let values_per_origin = table_count * zone_count;
        let expected = origin_count * values_per_origin;
        if origins.len() != expected {
            return Err(Error::new(ErrorKind::ShapeMismatch {
                context: "write_origins",
                expected,
                got: origins.len(),
            }));
        }

        if self.state.next_table != 1 {
            return Err(Error::new(ErrorKind::WriterPositionMismatch {
                expected_origin: self.state.next_row,
                expected_table: self.state.next_table,
                got_origin: self.state.next_row,
                got_table: 1,
            }));
        }

        let required_rows = u32::try_from(origin_count).expect("usize to u32")
            * u32::from(self.header.table_count());
        let rows_remaining = self.state.row_count.saturating_sub(self.state.rows_written);
        if required_rows > rows_remaining {
            return Err(Error::new(ErrorKind::WriterFinished));
        }

        for origin_chunk in origins.chunks_exact(values_per_origin) {
            self.write_origin(origin_chunk)?;
        }
        Ok(origin_count)
    }

    /// Writes remaining rows using a full stack shaped `(table, origin, destination)`.
    ///
    /// The writer cursor may already be part-way through the stream. This
    /// method writes only the remaining row records and returns that count.
    /// This is usually the most ergonomic and highest-throughput path when the
    /// full matrix values are already available in memory.
    ///
    /// Tradeoff: callers must provide complete table-major stack layout even if
    /// only a suffix is written from the current cursor position.
    pub fn write_stack(&mut self, stack: &[f64]) -> Result<usize> {
        let table_count = usize::from(self.header.table_count());
        let zone_count = usize::from(self.header.zone_count());
        let total_cells = table_count * zone_count * zone_count;
        if stack.len() != total_cells {
            return Err(Error::new(ErrorKind::ShapeMismatch {
                context: "write_stack",
                expected: total_cells,
                got: stack.len(),
            }));
        }

        let total_rows = table_count * zone_count;
        let next_origin = usize::from(self.state.next_row.saturating_sub(1));
        let next_table = usize::from(self.state.next_table.saturating_sub(1));
        let start_offset = next_origin * table_count + next_table;
        if start_offset > total_rows {
            return Err(Error::new(ErrorKind::WriterFinished));
        }
        let to_write = total_rows - start_offset;
        if to_write == 0 {
            return Ok(0);
        }

        for offset in start_offset..total_rows {
            let origin = offset / table_count;
            let table = offset % table_count;
            let start = (table * zone_count + origin) * zone_count;
            let end = start + zone_count;
            self.write_current_row(&stack[start..end])?;
        }
        Ok(to_write)
    }

    /// Returns the parsed header metadata.
    #[inline]
    pub fn header(&self) -> &Header {
        &self.header
    }

    /// Flushes the internal write buffer and the underlying sink.
    ///
    /// This does **not** check whether all expected rows were written. Use
    /// [`finish`](Writer::finish) to enforce completeness.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the flush fails.
    pub fn flush(&mut self) -> io::Result<()> {
        let sink = self
            .sink
            .as_mut()
            .expect("sink is always Some until consumed by into_inner or finish");
        sink.flush()
    }

    /// Verifies that all rows have been written, flushes, and returns the
    /// underlying sink.
    ///
    /// This is the **preferred** way to finalise a writer. It ensures the
    /// matrix is complete before handing back control of the sink. In practice
    /// this should be your default terminal call, since it reports both
    /// completeness violations and flush failures.
    ///
    /// # Errors
    ///
    /// Returns [`ErrorKind::IncompleteMatrix`] if fewer than
    /// `zone_count × table_count` rows were written. Returns an I/O error
    /// (wrapped in [`Error`]) if the flush fails.
    ///
    /// # Example
    ///
    /// ```
    /// use despina::{ErrorKind, TableDef, TypeCode, WriterBuilder};
    ///
    /// let tables = [TableDef::new("DIST_AM", TypeCode::Float32)];
    /// let mut writer = WriterBuilder::new().open_writer(Vec::new(), 2, &tables)?;
    /// writer.write_next_row(&[1.0, 2.0])?;
    ///
    /// let err = match writer.finish() {
    ///     Ok(_) => panic!("expected incomplete-matrix error"),
    ///     Err(err) => err,
    /// };
    /// assert!(matches!(
    ///     err.kind(),
    ///     ErrorKind::IncompleteMatrix {
    ///         expected: 2,
    ///         written: 1
    ///     }
    /// ));
    /// # Ok::<(), despina::Error>(())
    /// ```
    pub fn finish(self) -> Result<W> {
        if self.state.rows_written != self.state.row_count {
            return Err(Error::new(ErrorKind::IncompleteMatrix {
                expected: self.state.row_count,
                written: self.state.rows_written,
            }));
        }
        self.into_inner().map_err(|e| Error::from(e.into_error()))
    }

    /// Flushes the writer and returns the underlying sink **without** checking
    /// completeness.
    ///
    /// Use [`finish`](Writer::finish) for the normal case. This method is an
    /// escape hatch for callers who intentionally need to reclaim the sink
    /// before all rows have been written (e.g. error recovery).
    ///
    /// If the flush fails, the writer is returned inside an
    /// [`IntoInnerError`] so the caller can recover the sink or inspect the
    /// error.
    ///
    /// # Example
    ///
    /// ```
    /// use despina::{TableDef, TypeCode, WriterBuilder};
    ///
    /// let tables = [TableDef::new("DIST_AM", TypeCode::Float32)];
    /// let mut writer = WriterBuilder::new().open_writer(Vec::new(), 2, &tables)?;
    /// writer.write_next_row(&[1.0, 2.0])?;
    ///
    /// let bytes = writer
    ///     .into_inner()
    ///     .map_err(|err| despina::Error::from(err.into_error()))?;
    /// assert!(!bytes.is_empty());
    /// # Ok::<(), despina::Error>(())
    /// ```
    #[allow(clippy::result_large_err)]
    pub fn into_inner(mut self) -> std::result::Result<W, IntoInnerError<Writer<W>>> {
        let mut sink = self
            .sink
            .take()
            .expect("sink is always Some until consumed by into_inner or finish");
        if let Err(error) = sink.flush() {
            self.sink = Some(sink);
            return Err(IntoInnerError::new(self, error));
        }
        match sink.into_inner() {
            Ok(inner) => Ok(inner),
            Err(error) => {
                let (io_error, sink) = error.into_parts();
                self.sink = Some(sink);
                Err(IntoInnerError::new(self, io_error))
            }
        }
    }

    /// Returns a shared reference to the underlying writer.
    ///
    /// Bytes buffered inside this `Writer` may not yet be visible in the
    /// underlying sink until [`flush`](Writer::flush),
    /// [`finish`](Writer::finish), or [`into_inner`](Writer::into_inner) is
    /// called.
    #[inline]
    pub fn get_ref(&self) -> &W {
        self.sink
            .as_ref()
            .map(BufWriter::get_ref)
            .expect("sink is always Some until consumed by into_inner or finish")
    }
}

impl<W: Write> fmt::Debug for Writer<W> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Writer")
            .field("header", &self.header)
            .field("rows_written", &self.state.rows_written)
            .finish_non_exhaustive()
    }
}

/// Tracks write sequencing and progress.
#[derive(Debug, Clone, Copy)]
struct WriteState {
    next_row: u16,
    next_table: u8,
    table_count: u8,
    row_count: u32,
    rows_written: u32,
}

impl WriteState {
    fn new(table_count: u8, row_count: u32) -> Self {
        Self {
            next_row: 1,
            next_table: 1,
            table_count,
            row_count,
            rows_written: 0,
        }
    }

    /// Returns `true` when all expected rows have been written.
    fn done(&self) -> bool {
        self.rows_written >= self.row_count
    }

    /// Advances the state to the next (row, table) position.
    fn advance(&mut self) {
        self.rows_written += 1;
        if self.next_table < self.table_count {
            self.next_table += 1;
        } else {
            self.next_table = 1;
            self.next_row = self.next_row.saturating_add(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::io::{self, Write};
    use std::rc::Rc;

    use super::*;
    use crate::header;
    use crate::reader::ReaderBuilder;
    use crate::row::RowBuf;
    use crate::types::TypeCode;

    /// Helper: build a writer into a Vec<u8>, write all rows, return the bytes.
    fn write_matrix(zone_count: u16, tables: &[TableDef], rows: &[Vec<f64>]) -> Vec<u8> {
        let mut buf = Vec::new();
        let mut writer = WriterBuilder::new()
            .open_writer(&mut buf, zone_count, tables)
            .unwrap();
        for values in rows {
            writer.write_next_row(values).unwrap();
        }
        writer.finish().unwrap();
        buf
    }

    #[test]
    fn builder_rejects_zero_zone_count() {
        let tables = [TableDef::new("T", TypeCode::Fixed(0))];
        let result = WriterBuilder::new().open_writer(Vec::new(), 0, &tables);
        assert!(matches!(
            result.unwrap_err().kind(),
            ErrorKind::InvalidZoneCount
        ));
    }

    #[test]
    fn builder_rejects_zone_count_above_max() {
        let tables = [TableDef::new("T", TypeCode::Fixed(0))];
        let result = WriterBuilder::new().open_writer(Vec::new(), MAX_ZONE_COUNT + 1, &tables);
        assert!(matches!(
            result.unwrap_err().kind(),
            ErrorKind::InvalidZoneCount
        ));
    }

    #[test]
    fn builder_rejects_empty_tables() {
        let result = WriterBuilder::new().open_writer(Vec::new(), 10, &[]);
        assert!(matches!(
            result.unwrap_err().kind(),
            ErrorKind::InvalidTableCount(_)
        ));
    }

    #[test]
    fn builder_rejects_empty_table_name() {
        let tables = [TableDef::new("", TypeCode::Fixed(0))];
        let result = WriterBuilder::new().open_writer(Vec::new(), 10, &tables);
        assert!(matches!(
            result.unwrap_err().kind(),
            ErrorKind::InvalidTableName(_)
        ));
    }

    #[test]
    fn builder_rejects_non_ascii_table_name() {
        let tables = [TableDef::new("TRÏPS", TypeCode::Fixed(0))];
        let result = WriterBuilder::new().open_writer(Vec::new(), 10, &tables);
        assert!(matches!(
            result.unwrap_err().kind(),
            ErrorKind::InvalidTableName(_)
        ));
    }

    #[test]
    fn builder_rejects_invalid_type_code() {
        let tables = [TableDef::new("T", TypeCode::Fixed(10))];
        let result = WriterBuilder::new().open_writer(Vec::new(), 10, &tables);
        assert!(matches!(
            result.unwrap_err().kind(),
            ErrorKind::InvalidTypeCode { token } if token == "10"
        ));
    }

    #[test]
    fn builder_rejects_too_many_tables() {
        let tables: Vec<TableDef> = (0..256)
            .map(|index| TableDef::new(format!("T{}", index), TypeCode::Fixed(0)))
            .collect();
        let result = WriterBuilder::new().open_writer(Vec::new(), 10, &tables);
        assert!(matches!(
            result.unwrap_err().kind(),
            ErrorKind::InvalidTableCount(_)
        ));
    }

    #[test]
    fn header_round_trips() {
        let tables = [
            TableDef::new("TRIPS", TypeCode::Fixed(2)),
            TableDef::new("DIST", TypeCode::Float32),
            TableDef::new("TIME", TypeCode::Float64),
        ];
        let mut buf = Vec::new();
        {
            let mut writer = WriterBuilder::new()
                .banner("TEST BANNER")
                .run_id("Test Run")
                .open_writer(&mut buf, 50, &tables)
                .unwrap();
            let zero_row = vec![0.0; 50];
            for _ in 0..(50 * 3) {
                writer.write_next_row(&zero_row).unwrap();
            }
            writer.finish().unwrap();
        }

        let reader = ReaderBuilder::new().from_bytes(&buf).unwrap();
        let h = reader.header();
        assert_eq!(h.banner(), "TEST BANNER");
        assert_eq!(h.run_id(), "Test Run");
        assert_eq!(h.zone_count(), 50);
        assert_eq!(h.table_count(), 3);
        assert_eq!(h.tables()[0].name(), "TRIPS");
        assert_eq!(h.tables()[0].type_code(), TypeCode::Fixed(2));
        assert_eq!(h.tables()[1].name(), "DIST");
        assert_eq!(h.tables()[1].type_code(), TypeCode::Float32);
        assert_eq!(h.tables()[2].name(), "TIME");
        assert_eq!(h.tables()[2].type_code(), TypeCode::Float64);
    }

    #[test]
    fn header_only_parseable() {
        let tables = [TableDef::new("T1", TypeCode::Fixed(0))];
        let buf = write_matrix(2, &tables, &[vec![0.0; 2], vec![0.0; 2]]);

        let parsed = header::parse_header(&mut &buf[..]).unwrap();
        assert_eq!(parsed.zone_count(), 2);
        assert_eq!(parsed.table_count(), 1);
        assert_eq!(parsed.tables()[0].name(), "T1");

        let mut reader = ReaderBuilder::new().from_bytes(&buf).unwrap();
        let mut row = RowBuf::new();
        let mut count = 0u32;
        while reader.read_row(&mut row).unwrap() {
            count += 1;
        }
        assert_eq!(count, 2);
    }

    #[test]
    fn single_row_numeric_round_trip() {
        let tables = [TableDef::new("T", TypeCode::Fixed(2))];
        let values = vec![1.25, 0.0, -2.34];

        let mut full_buf = Vec::new();
        let mut writer = WriterBuilder::new()
            .open_writer(&mut full_buf, 3, &tables)
            .unwrap();
        let zero_row = [0.0; 3];
        writer.write_next_row(&values).unwrap();
        writer.write_next_row(&zero_row).unwrap();
        writer.write_next_row(&zero_row).unwrap();
        writer.finish().unwrap();

        let mut reader = ReaderBuilder::new().from_bytes(&full_buf).unwrap();
        let mut row = RowBuf::new();

        assert!(reader.read_row(&mut row).unwrap());
        assert_eq!(row.row_index(), 1);
        assert_eq!(row.table_index(), 1);
        assert!((row.values()[0] - 1.25).abs() < 1e-15);
        assert_eq!(row.values()[1], 0.0);
        assert!((row.values()[2] - (-2.34)).abs() < 1e-15);
    }

    #[test]
    fn multi_table_round_trip() {
        let tables = [
            TableDef::new("INT", TypeCode::Fixed(0)),
            TableDef::new("FLT", TypeCode::Float32),
            TableDef::new("DBL", TypeCode::Float64),
        ];
        let zone_count: u16 = 5;

        let mut full_buf = Vec::new();
        let mut writer = WriterBuilder::new()
            .open_writer(&mut full_buf, zone_count, &tables)
            .unwrap();

        for row_index in 1..=zone_count {
            for table_index in 1..=3u8 {
                let values: Vec<f64> = (0..zone_count)
                    .map(|j| (row_index as f64) * 10.0 + (j as f64) + (table_index as f64) * 0.1)
                    .collect();
                writer.write_next_row(&values).unwrap();
            }
        }
        writer.finish().unwrap();

        let mut reader = ReaderBuilder::new().from_bytes(&full_buf).unwrap();
        let mut row = RowBuf::new();

        for row_index in 1..=zone_count {
            for table_index in 1..=3u8 {
                assert!(reader.read_row(&mut row).unwrap());
                assert_eq!(row.row_index(), row_index);
                assert_eq!(row.table_index(), table_index);

                for (j, &got) in row.values().iter().enumerate() {
                    let raw = (row_index as f64) * 10.0 + (j as f64) + (table_index as f64) * 0.1;
                    let expected = match table_index {
                        1 => raw.round(),
                        2 => raw as f32 as f64,
                        _ => raw,
                    };
                    assert!(
                        (got - expected).abs() < 1e-6,
                        "row {}, table {}, zone {}: expected {}, got {}",
                        row_index,
                        table_index,
                        j,
                        expected,
                        got,
                    );
                }
            }
        }
        assert!(!reader.read_row(&mut row).unwrap());
    }

    #[test]
    fn all_zero_row_produces_minimal_payload() {
        let tables = [TableDef::new("T", TypeCode::Fixed(0))];
        let mut buf = Vec::new();
        let mut writer = WriterBuilder::new()
            .open_writer(&mut buf, 100, &tables)
            .unwrap();
        let zero_row = vec![0.0; 100];
        for _ in 0..100 {
            writer.write_next_row(&zero_row).unwrap();
        }
        writer.finish().unwrap();

        let mut reader = ReaderBuilder::new().from_bytes(&buf).unwrap();
        let mut row = RowBuf::new();
        while reader.read_row(&mut row).unwrap() {
            assert!(row.values().iter().all(|&v| v == 0.0));
        }
    }

    #[test]
    fn write_after_completion_rejected() {
        let tables = [TableDef::new("T", TypeCode::Fixed(0))];
        let mut buf = Vec::new();
        let mut writer = WriterBuilder::new()
            .open_writer(&mut buf, 2, &tables)
            .unwrap();

        writer.write_next_row(&[0.0; 2]).unwrap();
        writer.write_next_row(&[0.0; 2]).unwrap();

        let err = writer.write_next_row(&[0.0; 2]).unwrap_err();
        assert!(matches!(err.kind(), ErrorKind::WriterFinished));
    }

    #[test]
    fn finish_rejects_incomplete_matrix() {
        let tables = [TableDef::new("T", TypeCode::Fixed(0))];
        let mut buf = Vec::new();
        let mut writer = WriterBuilder::new()
            .open_writer(&mut buf, 3, &tables)
            .unwrap();

        writer.write_next_row(&[0.0; 3]).unwrap();
        let err = writer.finish().unwrap_err();
        assert!(matches!(
            err.kind(),
            ErrorKind::IncompleteMatrix {
                expected: 3,
                written: 1,
            }
        ));
    }

    #[test]
    fn wrong_values_length_rejected() {
        let tables = [TableDef::new("T", TypeCode::Fixed(0))];
        let mut buf = Vec::new();
        let mut writer = WriterBuilder::new()
            .open_writer(&mut buf, 5, &tables)
            .unwrap();

        let err = writer.write_next_row(&[0.0; 3]).unwrap_err();
        assert!(matches!(
            err.kind(),
            ErrorKind::ZoneCountMismatch {
                expected: 5,
                got: 3,
            }
        ));
    }

    #[test]
    fn into_inner_returns_complete_data() {
        let tables = [TableDef::new("T", TypeCode::Float64)];
        let mut buf = Vec::new();
        let mut writer = WriterBuilder::new()
            .open_writer(&mut buf, 1, &tables)
            .unwrap();
        writer.write_next_row(&[42.0]).unwrap();
        writer.into_inner().unwrap();

        let mut reader = ReaderBuilder::new().from_bytes(&buf).unwrap();
        let mut row = RowBuf::new();
        assert!(reader.read_row(&mut row).unwrap());
        assert_eq!(row.values()[0], 42.0);
        assert!(!reader.read_row(&mut row).unwrap());
    }

    #[derive(Debug, Default)]
    struct FlakyState {
        bytes: Vec<u8>,
        armed: bool,
        calls_after_arm: u8,
    }

    #[derive(Debug)]
    struct ArmedFlakySink {
        state: Rc<RefCell<FlakyState>>,
    }

    impl Write for ArmedFlakySink {
        fn write(&mut self, data: &[u8]) -> io::Result<usize> {
            let mut state = self.state.borrow_mut();
            if !state.armed {
                state.bytes.extend_from_slice(data);
                return Ok(data.len());
            }

            match state.calls_after_arm {
                0 => {
                    state.calls_after_arm = 1;
                    let written = (data.len() / 2).max(1);
                    state.bytes.extend_from_slice(&data[..written]);
                    Ok(written)
                }
                1 => {
                    state.calls_after_arm = 2;
                    Err(io::Error::other("injected write failure"))
                }
                _ => {
                    state.bytes.extend_from_slice(data);
                    Ok(data.len())
                }
            }
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn into_inner_retry_after_short_write_error_does_not_duplicate_bytes() {
        let tables = [TableDef::new("T", TypeCode::Float64)];
        let mut expected_writer = WriterBuilder::new()
            .open_writer(Vec::new(), 1, &tables)
            .unwrap();
        expected_writer.write_next_row(&[42.0]).unwrap();
        let expected = expected_writer.into_inner().unwrap();

        let state = Rc::new(RefCell::new(FlakyState::default()));
        let sink = ArmedFlakySink {
            state: state.clone(),
        };

        let mut writer = WriterBuilder::new().open_writer(sink, 1, &tables).unwrap();
        state.borrow_mut().armed = true;
        writer.write_next_row(&[42.0]).unwrap();
        let err = writer.into_inner().unwrap_err();
        let writer = err.into_inner();
        let _ = writer.into_inner().unwrap();
        assert_eq!(state.borrow().bytes, expected);
    }

    #[test]
    fn drop_after_into_inner_error_retries_remaining_bytes_only() {
        let tables = [TableDef::new("T", TypeCode::Float64)];
        let mut expected_writer = WriterBuilder::new()
            .open_writer(Vec::new(), 1, &tables)
            .unwrap();
        expected_writer.write_next_row(&[42.0]).unwrap();
        let expected = expected_writer.into_inner().unwrap();

        let state = Rc::new(RefCell::new(FlakyState::default()));
        let sink = ArmedFlakySink {
            state: state.clone(),
        };

        let mut writer = WriterBuilder::new().open_writer(sink, 1, &tables).unwrap();
        state.borrow_mut().armed = true;
        writer.write_next_row(&[42.0]).unwrap();
        let err = writer.into_inner().unwrap_err();
        drop(err.into_inner());
        assert_eq!(state.borrow().bytes, expected);
    }

    #[test]
    fn write_origin_writes_all_tables_for_expected_origin() {
        let tables = [
            TableDef::new("A", TypeCode::Float64),
            TableDef::new("B", TypeCode::Float64),
        ];
        let mut buf = Vec::new();
        let mut writer = WriterBuilder::new()
            .open_writer(&mut buf, 2, &tables)
            .unwrap();

        writer.write_origin(&[1.0, 2.0, 10.0, 20.0]).unwrap();
        writer.write_origin(&[3.0, 4.0, 30.0, 40.0]).unwrap();
        writer.finish().unwrap();

        let matrix = crate::Matrix::from_bytes(&buf).unwrap();
        assert_eq!(matrix.get_by_name("A", 1, 1), 1.0);
        assert_eq!(matrix.get_by_name("B", 1, 1), 10.0);
        assert_eq!(matrix.get_by_name("A", 2, 2), 4.0);
        assert_eq!(matrix.get_by_name("B", 2, 2), 40.0);
    }

    #[test]
    fn write_origins_writes_multiple_origins() {
        let tables = [
            TableDef::new("A", TypeCode::Float64),
            TableDef::new("B", TypeCode::Float64),
        ];
        let mut buf = Vec::new();
        let mut writer = WriterBuilder::new()
            .open_writer(&mut buf, 2, &tables)
            .unwrap();

        let origins = [
            1.0, 2.0, 10.0, 20.0, //
            3.0, 4.0, 30.0, 40.0,
        ];
        assert_eq!(writer.write_origins(&origins, 2).unwrap(), 2);
        writer.finish().unwrap();

        let matrix = crate::Matrix::from_bytes(&buf).unwrap();
        assert_eq!(matrix.get_by_name("A", 2, 1), 3.0);
        assert_eq!(matrix.get_by_name("B", 2, 2), 40.0);
    }

    #[test]
    fn write_stack_supports_partial_resume() {
        let tables = [
            TableDef::new("A", TypeCode::Float64),
            TableDef::new("B", TypeCode::Float64),
        ];
        let stack = [
            1.0, 2.0, 3.0, 4.0, //
            10.0, 20.0, 30.0, 40.0,
        ];

        let mut buf = Vec::new();
        let mut writer = WriterBuilder::new()
            .open_writer(&mut buf, 2, &tables)
            .unwrap();
        writer.write_next_row(&[1.0, 2.0]).unwrap();
        assert_eq!(writer.write_stack(&stack).unwrap(), 3);
        writer.finish().unwrap();

        let matrix = crate::Matrix::from_bytes(&buf).unwrap();
        assert_eq!(matrix.get_by_name("A", 1, 1), 1.0);
        assert_eq!(matrix.get_by_name("B", 1, 1), 10.0);
        assert_eq!(matrix.get_by_name("B", 2, 2), 40.0);
    }

    #[test]
    fn builder_chaining_works() {
        let tables = [TableDef::new("T", TypeCode::Fixed(0))];
        let mut buf = Vec::new();
        let mut writer = WriterBuilder::new()
            .banner("CUSTOM BANNER")
            .run_id("Custom Run")
            .open_writer(&mut buf, 1, &tables)
            .unwrap();
        writer.write_next_row(&[0.0]).unwrap();
        writer.finish().unwrap();

        let reader = ReaderBuilder::new().from_bytes(&buf).unwrap();
        assert_eq!(reader.header().banner(), "CUSTOM BANNER");
        assert_eq!(reader.header().run_id(), "Custom Run");
    }

    #[test]
    fn default_builder_has_sensible_defaults() {
        let tables = [TableDef::new("T", TypeCode::Fixed(0))];
        let mut buf = Vec::new();
        let mut writer = WriterBuilder::new()
            .open_writer(&mut buf, 1, &tables)
            .unwrap();
        writer.write_next_row(&[0.0]).unwrap();
        writer.finish().unwrap();

        let reader = ReaderBuilder::new().from_bytes(&buf).unwrap();
        assert_eq!(
            reader.header().banner(),
            concat!("MAT PGM=DESPINA VER=", env!("CARGO_PKG_VERSION"))
        );
        assert_eq!(reader.header().run_id(), "DESPINA");
    }

    #[test]
    fn all_type_codes_round_trip() {
        let type_codes = [
            TypeCode::Fixed(0),
            TypeCode::Fixed(1),
            TypeCode::Fixed(2),
            TypeCode::Fixed(5),
            TypeCode::Fixed(9),
            TypeCode::Float32,
            TypeCode::Float64,
        ];

        for &tc in &type_codes {
            let tables = [TableDef::new("T", tc)];
            let zone_count: u16 = 10;
            let values: Vec<f64> = (0..zone_count).map(|j| (j as f64) * 1.234 + 0.5).collect();

            let mut buf = Vec::new();
            let mut writer = WriterBuilder::new()
                .open_writer(&mut buf, zone_count, &tables)
                .unwrap();

            for _row_index in 1..=zone_count {
                writer.write_next_row(&values).unwrap();
            }
            writer.finish().unwrap();

            let mut reader = ReaderBuilder::new().from_bytes(&buf).unwrap();
            let mut row = RowBuf::new();

            assert!(reader.read_row(&mut row).unwrap());
            for (j, (&got, &orig)) in row.values().iter().zip(values.iter()).enumerate() {
                let tolerance = match tc {
                    TypeCode::Fixed(p) => {
                        if p == 0 {
                            0.5001
                        } else {
                            let scale = 10.0_f64.powi(-(p as i32));
                            scale + 1e-10
                        }
                    }
                    TypeCode::Float32 => 1e-5,
                    TypeCode::Float64 => 1e-15,
                };
                assert!(
                    (got - orig).abs() < tolerance,
                    "type {:?}, zone {}: expected {}, got {}, tol {}",
                    tc,
                    j,
                    orig,
                    got,
                    tolerance,
                );
            }
        }
    }

    #[test]
    fn write_state_advance_saturates_row_index_at_u16_max() {
        let mut state = WriteState {
            next_row: u16::MAX,
            next_table: 1,
            table_count: 1,
            row_count: 1,
            rows_written: 0,
        };
        state.advance();
        assert_eq!(state.next_row, u16::MAX);
        assert_eq!(state.next_table, 1);
        assert_eq!(state.rows_written, 1);
    }
}
