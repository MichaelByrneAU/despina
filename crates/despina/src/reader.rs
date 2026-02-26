//! Streaming reader for `.mat` matrix files.
//!
//! [`ReaderBuilder`] constructs a [`Reader`] from a file path, byte slice, or
//! arbitrary [`Read`] source. The reader parses the header up front, then
//! decodes one row at a time into a caller-supplied [`RowBuf`], making it
//! practical to stream very large matrices without loading everything into
//! memory.
//!
//! Choose a read method by access pattern:
//!
//! - [`Reader::read_table_row_by_name`] is the convenience API for single-table
//!   reads when the caller naturally has table names.
//! - [`Reader::read_table_row`] is the lower-overhead single-table API when the
//!   caller already has a 1-based table index.
//! - [`Reader::prepare_selection_by_name`] or [`Reader::prepare_selection`]
//!   plus [`Reader::read_selected_row`] streams multiple tables in one pass,
//!   avoiding repeated name/index validation in the hot path.
//! - [`Reader::read_row`] decodes every row and is best for full scans,
//!   round-tripping, and validation passes.
//!
//! For tight loops, prepare a [`PreparedSelection`] once and reuse it.
//! Prepared selections are deduplicated and rows are always returned in file
//! order (not selection input order).
//!
//! ```
//! use despina::{MatrixBuilder, ReaderBuilder, RowBuf, TypeCode};
//!
//! let mut matrix = MatrixBuilder::new(2)
//!     .table("DIST_AM", TypeCode::Float32)
//!     .table("TIME_AM", TypeCode::Float32)
//!     .build()?;
//! matrix.set_by_name("DIST_AM", 1, 1, 1.0);
//! matrix.set_by_name("DIST_AM", 1, 2, 2.0);
//! matrix.set_by_name("TIME_AM", 1, 1, 10.0);
//! matrix.set_by_name("TIME_AM", 1, 2, 20.0);
//! matrix.set_by_name("DIST_AM", 2, 1, 3.0);
//! matrix.set_by_name("DIST_AM", 2, 2, 4.0);
//! matrix.set_by_name("TIME_AM", 2, 1, 30.0);
//! matrix.set_by_name("TIME_AM", 2, 2, 40.0);
//! let mut bytes = Vec::new();
//! matrix.write_to_writer(&mut bytes)?;
//!
//! let mut reader = ReaderBuilder::new().from_bytes(&bytes)?;
//! let mut row = RowBuf::new();
//! let mut selected_row_count = 0;
//! let selection = reader.prepare_selection_by_name(&["DIST_AM", "TIME_AM"])?;
//!
//! while reader.read_selected_row(selection, &mut row)? {
//!     selected_row_count += 1;
//!     match row.table_index() {
//!         1 => {
//!             // DIST_AM row
//!         }
//!         2 => {
//!             // TIME_AM row
//!         }
//!         _ => unreachable!("only selected tables are returned"),
//!     }
//! }
//! assert_eq!(selected_row_count, 4);
//! # Ok::<(), despina::Error>(())
//! ```
//!
//! The reader is forward-only within a pass: once a read method returns
//! `Ok(false)`, row data is exhausted. To read again (or in a different table
//! order), call [`Reader::reset`] on readers whose source supports [`Seek`]
//! (for example, readers created by [`ReaderBuilder::from_path`] and
//! [`ReaderBuilder::from_bytes`]).
//!
//! Operational notes:
//!
//! - Table-name matching is case-sensitive and lookup is linear in table count.
//! - The reader wraps sources in an internal [`std::io::BufReader`], so adding
//!   another buffered layer is usually unnecessary.

use std::fmt;
#[cfg(not(target_arch = "wasm32"))]
use std::fs::File;
use std::io::{self, BufRead, BufReader, Cursor, Read, Seek, SeekFrom};
#[cfg(not(target_arch = "wasm32"))]
use std::path::Path;

use crate::decode;
use crate::error::{Error, ErrorKind, Result};
use crate::header::{self, Header};
use crate::plane::PlaneScratch;
use crate::row::RowBuf;
use crate::row_format::is_canonical_zero_row;
use crate::types::TypeCode;

/// Configures and constructs a [`Reader`].
#[derive(Debug, Clone, Copy, Default)]
pub struct ReaderBuilder;

impl ReaderBuilder {
    #[inline]
    pub fn new() -> Self {
        Self
    }

    /// Builds a reader from any [`Read`] implementation.
    ///
    /// The header is parsed immediately. `reader` is wrapped in an internal
    /// [`BufReader`](std::io::BufReader), so callers should pass the raw source
    /// directly in most cases.
    ///
    /// # Errors
    ///
    /// Returns an error if the header is malformed or truncated.
    pub fn from_reader<R: Read>(&self, reader: R) -> Result<Reader<R>> {
        Reader::new(reader)
    }

    /// Builds a reader from a file path.
    ///
    /// This is the preferred path for on-disk files. The header is parsed
    /// immediately. The reader manages buffering internally, so callers should
    /// pass the raw file handle directly.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be opened or the header is
    /// malformed.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn from_path<P: AsRef<Path>>(&self, path: P) -> Result<Reader<File>> {
        let file = File::open(path)?;
        Reader::new(file)
    }

    /// Builds a reader from a byte slice already in memory.
    ///
    /// The header is parsed immediately.
    ///
    /// # Errors
    ///
    /// Returns an error if the header is malformed or truncated.
    pub fn from_bytes<'a>(&self, bytes: &'a [u8]) -> Result<Reader<Cursor<&'a [u8]>>> {
        Reader::new(Cursor::new(bytes))
    }
}

/// A streaming reader for `.mat` matrix files.
///
/// The caller creates a [`RowBuf`] once and passes it by `&mut` reference to
/// one of the read methods on each iteration. The reader reuses both the row
/// buffer's allocation and its own internal scratch buffers, so the hot path
/// performs zero allocations after the first row.
///
/// Internally, the reader uses [`BufReader`](std::io::BufReader) for I/O and
/// tracks a logical byte offset for diagnostics.
///
/// # Example
///
/// ```
/// use despina::{MatrixBuilder, ReaderBuilder, RowBuf, TypeCode};
///
/// let mut matrix = MatrixBuilder::new(2)
///     .table("DIST_AM", TypeCode::Float32)
///     .build()?;
/// matrix.set_by_name("DIST_AM", 1, 1, 1.0);
/// matrix.set_by_name("DIST_AM", 1, 2, 2.0);
/// matrix.set_by_name("DIST_AM", 2, 1, 3.0);
/// matrix.set_by_name("DIST_AM", 2, 2, 4.0);
/// let mut bytes = Vec::new();
/// matrix.write_to_writer(&mut bytes)?;
///
/// let mut reader = ReaderBuilder::new().from_bytes(&bytes)?;
/// let mut row = RowBuf::new();
/// let mut row_sums = Vec::new();
///
/// while reader.read_table_row_by_name("DIST_AM", &mut row)? {
///     let sum: f64 = row.values().iter().sum();
///     row_sums.push((row.row_index(), sum));
/// }
/// assert_eq!(row_sums, vec![(1, 3.0), (2, 7.0)]);
/// # Ok::<(), despina::Error>(())
/// ```
pub struct Reader<R> {
    source: BufReader<R>,
    source_offset: u64,
    header: Header,
    table_type_codes: Box<[TypeCode]>,
    payload: Vec<u8>,
    scratch: PlaneScratch,
    state: ReadState,
    eof: bool,
    row_data_offset: u64,
}

/// Pre-validated table selection for repeated selected-row reads.
///
/// Build once with [`Reader::prepare_selection`] or
/// [`Reader::prepare_selection_by_name`], then reuse it in
/// [`Reader::read_selected_row`] to avoid re-validating names or
/// indices on every row read. Selections are deduplicated and preserve file
/// order semantics when reading.
#[derive(Debug, Clone, Copy)]
pub struct PreparedSelection(TableSelection);

impl<R: Read> Reader<R> {
    /// Builds a reader from any [`Read`] implementation.
    ///
    /// Equivalent to `ReaderBuilder::new().from_reader(reader)`.
    pub fn from_reader(reader: R) -> Result<Self> {
        ReaderBuilder::new().from_reader(reader)
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl Reader<File> {
    /// Builds a reader from a file path.
    ///
    /// Equivalent to `ReaderBuilder::new().from_path(path)`.
    pub fn from_path<P: AsRef<Path>>(path: P) -> Result<Self> {
        ReaderBuilder::new().from_path(path)
    }
}

impl<'a> Reader<Cursor<&'a [u8]>> {
    /// Builds a reader from a byte slice already in memory.
    ///
    /// Equivalent to `ReaderBuilder::new().from_bytes(bytes)`.
    pub fn from_bytes(bytes: &'a [u8]) -> Result<Self> {
        Reader::new(Cursor::new(bytes))
    }
}

impl<R> Reader<R> {
    /// Returns the parsed file header.
    #[inline]
    pub fn header(&self) -> &Header {
        &self.header
    }

    /// Returns a shared reference to the underlying I/O source.
    ///
    /// The reference is to the raw source inside the reader's internal
    /// buffering layer. Reading directly from this reference will desynchronise
    /// the reader's position tracking. This method is primarily useful for
    /// inspecting source metadata.
    #[inline]
    pub fn get_ref(&self) -> &R {
        self.source.get_ref()
    }

    /// Returns a mutable reference to the underlying I/O source.
    ///
    /// **Caution:** reading from or seeking the returned reference will
    /// desynchronise the reader's internal state. This is provided for the rare
    /// cases where callers need to manipulate the source between logical
    /// operations.
    #[inline]
    pub fn get_mut(&mut self) -> &mut R {
        self.source.get_mut()
    }

    /// Consumes the reader and returns the underlying I/O source.
    ///
    /// The reader buffers input and may read ahead from the source. The
    /// returned source can therefore be positioned ahead of
    /// [`position`](Reader::position), and any bytes that were prefetched into
    /// the reader's internal buffer are no longer accessible via that source.
    #[inline]
    pub fn into_inner(self) -> R {
        self.source.into_inner()
    }

    /// Returns the number of row records consumed so far.
    ///
    /// Includes rows that were skipped (consumed but not decoded) by the
    /// table-filtered methods. Compare against
    /// [`Header::row_count`](crate::Header::row_count) for progress
    /// reporting.
    ///
    /// # Example
    ///
    /// ```
    /// use despina::{MatrixBuilder, ReaderBuilder, RowBuf, TypeCode};
    ///
    /// let mut matrix = MatrixBuilder::new(1)
    ///     .table("DIST_AM", TypeCode::Float32)
    ///     .table("TIME_AM", TypeCode::Float32)
    ///     .build()?;
    /// matrix.set_by_name("DIST_AM", 1, 1, 1.0);
    /// matrix.set_by_name("TIME_AM", 1, 1, 2.0);
    /// let mut bytes = Vec::new();
    /// matrix.write_to_writer(&mut bytes)?;
    ///
    /// let mut reader = ReaderBuilder::new().from_bytes(&bytes)?;
    /// let mut row = RowBuf::new();
    /// assert_eq!(reader.rows_read(), 0);
    ///
    /// assert!(reader.read_table_row_by_name("TIME_AM", &mut row)?);
    /// assert_eq!(reader.rows_read(), 2); // one skipped row, one decoded row
    /// # Ok::<(), despina::Error>(())
    /// ```
    #[inline]
    pub fn rows_read(&self) -> u32 {
        self.state.rows_read
    }

    /// Returns the current byte offset from the start of the input.
    ///
    /// # Example
    ///
    /// ```
    /// use despina::{MatrixBuilder, ReaderBuilder, RowBuf, TypeCode};
    ///
    /// let mut matrix = MatrixBuilder::new(2)
    ///     .table("DIST_AM", TypeCode::Float32)
    ///     .build()?;
    /// matrix.set_by_name("DIST_AM", 1, 1, 1.0);
    /// matrix.set_by_name("DIST_AM", 1, 2, 2.0);
    /// matrix.set_by_name("DIST_AM", 2, 1, 3.0);
    /// matrix.set_by_name("DIST_AM", 2, 2, 4.0);
    /// let mut bytes = Vec::new();
    /// matrix.write_to_writer(&mut bytes)?;
    ///
    /// let mut reader = ReaderBuilder::new().from_bytes(&bytes)?;
    /// let mut row = RowBuf::new();
    /// let start = reader.position();
    ///
    /// assert!(reader.read_row(&mut row)?);
    /// assert!(reader.position() > start);
    /// # Ok::<(), despina::Error>(())
    /// ```
    #[inline]
    pub fn position(&self) -> u64 {
        self.source_offset
    }
}

impl<R: Read + Seek> Reader<R> {
    /// Resets the reader to the start of row data, allowing all rows to be read
    /// again from the beginning.
    ///
    /// This seeks the underlying source back to the first row record and resets
    /// all internal sequencing state. After a successful reset, the next call
    /// to any read method returns the first row in the file, just as it would
    /// on a freshly constructed reader.
    ///
    /// This method is only available when the underlying source supports
    /// seeking. Readers from [`ReaderBuilder::from_path`] (file-backed) and
    /// [`ReaderBuilder::from_bytes`] (byte slices) always support reset.
    /// Readers from [`ReaderBuilder::from_reader`] support reset only when the
    /// caller's source implements [`Seek`].
    ///
    /// # Errors
    ///
    /// Returns an error if the seek operation fails.
    ///
    /// # Example
    ///
    /// ```
    /// use despina::{MatrixBuilder, ReaderBuilder, RowBuf, TypeCode};
    ///
    /// let mut matrix = MatrixBuilder::new(2)
    ///     .table("DIST_AM", TypeCode::Float32)
    ///     .build()?;
    /// matrix.set_by_name("DIST_AM", 1, 1, 1.0);
    /// matrix.set_by_name("DIST_AM", 1, 2, 2.0);
    /// matrix.set_by_name("DIST_AM", 2, 1, 3.0);
    /// matrix.set_by_name("DIST_AM", 2, 2, 4.0);
    /// let mut bytes = Vec::new();
    /// matrix.write_to_writer(&mut bytes)?;
    ///
    /// let mut reader = ReaderBuilder::new().from_bytes(&bytes)?;
    /// let mut row = RowBuf::new();
    ///
    /// assert!(reader.read_row(&mut row)?);
    /// assert_eq!(row.values(), &[1.0, 2.0]);
    ///
    /// reader.reset()?;
    /// assert!(reader.read_row(&mut row)?);
    /// assert_eq!(row.values(), &[1.0, 2.0]);
    /// # Ok::<(), despina::Error>(())
    /// ```
    pub fn reset(&mut self) -> Result<()> {
        self.source
            .seek(SeekFrom::Start(self.row_data_offset))
            .map_err(|err| Error::new(ErrorKind::Io(err)))?;
        self.source_offset = self.row_data_offset;
        self.state.reset();
        self.eof = false;
        Ok(())
    }
}

impl<R: fmt::Debug> fmt::Debug for Reader<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Reader")
            .field("header", &self.header)
            .field("position", &self.source_offset)
            .field("rows_read", &self.state.rows_read)
            .finish_non_exhaustive()
    }
}

impl<R: Read> Reader<R> {
    fn new(source: R) -> Result<Self> {
        let mut source = BufReader::with_capacity(DEFAULT_READ_BUFFER, source);
        let mut source_offset = 0u64;
        let header = {
            let mut counting_reader = CountingReader {
                inner: &mut source,
                source_offset: &mut source_offset,
            };
            match header::parse_header(&mut counting_reader) {
                Ok(header) => header,
                Err(err) => return Err(Error::at(err.into_kind(), source_offset)),
            }
        };

        let row_data_offset = source_offset;
        let state = ReadState::new(&header);
        let scratch = PlaneScratch::new(usize::from(header.zone_count()));
        let table_type_codes: Box<[TypeCode]> = header
            .tables()
            .iter()
            .map(|table| table.type_code())
            .collect();

        Ok(Self {
            source,
            source_offset,
            header,
            table_type_codes,
            payload: Vec::new(),
            scratch,
            state,
            eof: false,
            row_data_offset,
        })
    }

    /// Reads the next row belonging to `table_index`, skipping other tables.
    ///
    /// Returns `Ok(true)` when a row was decoded, or `Ok(false)` at
    /// end-of-file. `table_index` is 1-based.
    ///
    /// Rows belonging to other tables are consumed but not decoded. To read
    /// multiple tables in one pass, use
    /// [`read_selected_row`](Reader::read_selected_row).
    /// For repeated calls in tight loops, this avoids name lookup overhead
    /// compared with [`read_table_row_by_name`](Reader::read_table_row_by_name).
    /// To re-read from the beginning, call [`reset`](Reader::reset).
    ///
    /// # Errors
    ///
    /// Returns an error if `table_index` is out of range or a row record is
    /// malformed.
    ///
    /// # Example
    ///
    /// ```
    /// use despina::{MatrixBuilder, ReaderBuilder, RowBuf, TypeCode};
    ///
    /// let mut matrix = MatrixBuilder::new(2)
    ///     .table("DIST_AM", TypeCode::Float32)
    ///     .table("TIME_AM", TypeCode::Float32)
    ///     .build()?;
    /// matrix.set_by_name("DIST_AM", 1, 1, 1.0);
    /// matrix.set_by_name("DIST_AM", 1, 2, 2.0);
    /// matrix.set_by_name("TIME_AM", 1, 1, 10.0);
    /// matrix.set_by_name("TIME_AM", 1, 2, 20.0);
    /// matrix.set_by_name("DIST_AM", 2, 1, 3.0);
    /// matrix.set_by_name("DIST_AM", 2, 2, 4.0);
    /// matrix.set_by_name("TIME_AM", 2, 1, 30.0);
    /// matrix.set_by_name("TIME_AM", 2, 2, 40.0);
    /// let mut bytes = Vec::new();
    /// matrix.write_to_writer(&mut bytes)?;
    ///
    /// let mut reader = ReaderBuilder::new().from_bytes(&bytes)?;
    /// let mut row = RowBuf::new();
    /// let mut table2_rows = Vec::new();
    ///
    /// while reader.read_table_row(2, &mut row)? {
    ///     table2_rows.push((row.row_index(), row.values().to_vec()));
    /// }
    /// assert_eq!(
    ///     table2_rows,
    ///     vec![(1, vec![10.0, 20.0]), (2, vec![30.0, 40.0])]
    /// );
    /// # Ok::<(), despina::Error>(())
    /// ```
    pub fn read_table_row(&mut self, table_index: u8, row: &mut RowBuf) -> Result<bool> {
        self.validate_requested_table(table_index)?;
        self.read_next_matching_row(row, |candidate| candidate == table_index)
    }

    /// Reads the next row belonging to the table named `name`.
    ///
    /// Resolves `name` to a 1-based table index via a case-sensitive linear
    /// scan of the header's table catalogue, then delegates to
    /// [`read_table_row`](Reader::read_table_row). If you will read many rows
    /// for the same table, resolve the table once and use
    /// [`read_table_row`](Reader::read_table_row) directly.
    ///
    /// # Errors
    ///
    /// Returns [`ErrorKind::TableNotFound`] if no table has the given name.
    pub fn read_table_row_by_name(&mut self, name: &str, row: &mut RowBuf) -> Result<bool> {
        let table_index = self.resolve_table_name(name)?;
        self.read_table_row(table_index, row)
    }

    /// Prepares a validated selection from 1-based table indices.
    ///
    /// For repeated selected-row reads, call this once and reuse the returned
    /// selection with [`read_selected_row`](Reader::read_selected_row).
    /// Duplicate indices are ignored.
    ///
    /// # Errors
    ///
    /// Returns an error if any table index is out of range.
    pub fn prepare_selection(&self, table_indices: &[u8]) -> Result<PreparedSelection> {
        let selection = self.build_selection_from_indices(table_indices)?;
        Ok(PreparedSelection(selection))
    }

    /// Prepares a validated selection from table names.
    ///
    /// Every name is resolved before any row read. For repeated reads, this
    /// avoids per-row name lookup and validation. Duplicate names are ignored.
    ///
    /// # Errors
    ///
    /// Returns [`ErrorKind::TableNotFound`] if any name is missing.
    pub fn prepare_selection_by_name(&self, names: &[&str]) -> Result<PreparedSelection> {
        let selection = self.build_selection_from_names(names)?;
        Ok(PreparedSelection(selection))
    }

    /// Reads the next row from a pre-validated table selection.
    ///
    /// Rows belonging to non-selected tables are consumed but not decoded.
    /// Returns `Ok(false)` at end-of-file, or immediately when `selection` is
    /// empty.
    ///
    /// Returned rows always follow on-disk row/table order. The order in which
    /// tables were supplied when preparing `selection` does not affect output
    /// order.
    ///
    /// # Errors
    ///
    /// Returns an error if a row record is malformed.
    ///
    /// # Example
    ///
    /// ```
    /// use despina::{MatrixBuilder, ReaderBuilder, RowBuf, TypeCode};
    ///
    /// let mut matrix = MatrixBuilder::new(1)
    ///     .table("DIST_AM", TypeCode::Float32)
    ///     .table("TIME_AM", TypeCode::Float32)
    ///     .table("COST_AM", TypeCode::Float32)
    ///     .build()?;
    /// matrix.set_by_name("DIST_AM", 1, 1, 1.0);
    /// matrix.set_by_name("TIME_AM", 1, 1, 2.0);
    /// matrix.set_by_name("COST_AM", 1, 1, 3.0);
    /// let mut bytes = Vec::new();
    /// matrix.write_to_writer(&mut bytes)?;
    ///
    /// let mut reader = ReaderBuilder::new().from_bytes(&bytes)?;
    /// let selection = reader.prepare_selection(&[1, 3])?;
    /// let mut row = RowBuf::new();
    /// let mut selected = Vec::new();
    ///
    /// while reader.read_selected_row(selection, &mut row)? {
    ///     selected.push((row.table_index(), row.values()[0]));
    /// }
    /// assert_eq!(selected, vec![(1, 1.0), (3, 3.0)]);
    /// # Ok::<(), despina::Error>(())
    /// ```
    pub fn read_selected_row(
        &mut self,
        selection: PreparedSelection,
        row: &mut RowBuf,
    ) -> Result<bool> {
        if selection.0.is_empty() {
            return Ok(false);
        }
        self.read_next_matching_row(row, |table_index| selection.0.contains(table_index))
    }

    /// Reads and decodes the next row record into `row`, regardless of table.
    ///
    /// This is the low-level primitive that decodes every row in sequence. It
    /// is used internally by [`Matrix::open`](crate::Matrix::open) and is
    /// useful for round-trip copying and validation. Most callers should prefer
    /// the table-filtered methods above.
    ///
    /// Returns `Ok(true)` when a row was decoded, or `Ok(false)` at
    /// end-of-file.
    ///
    /// # Errors
    ///
    /// Returns an error if a row record is malformed, truncated, or out of
    /// sequence.
    pub fn read_row(&mut self, row: &mut RowBuf) -> Result<bool> {
        self.read_next_matching_row(row, |_| true)
    }

    /// Parses the next row record header and reads its payload.
    fn read_record(&mut self) -> Result<Option<RowRecord>> {
        if self.eof {
            return Ok(None);
        }
        if self.state.done() {
            if self.peek_byte()?.is_some() {
                return Err(Error::at(ErrorKind::TrailingBytes, self.source_offset));
            }
            self.eof = true;
            return Ok(None);
        }

        let row_start = self.source_offset;

        // Record header:
        //   +0: row_index   (u16le).
        //   +2: table_index (u8).
        //   +3: chunk_size  (u16le, includes its own 2 bytes).
        let mut record_header = [0u8; 5];
        Self::read_source_exact_parts(
            &mut self.source,
            &mut self.source_offset,
            &mut record_header,
        )?;

        let row_index = u16::from_le_bytes([record_header[0], record_header[1]]);
        let table_index = record_header[2];
        let chunk_size = u16::from_le_bytes([record_header[3], record_header[4]]);

        if chunk_size < 2 {
            return Err(Error::at(
                ErrorKind::InvalidChunkSize(chunk_size),
                row_start + 3,
            ));
        }

        let payload_size = usize::from(chunk_size - 2);
        if self.payload.len() < payload_size {
            self.payload.resize(payload_size, 0);
        }
        if payload_size > 0 {
            let source = &mut self.source;
            let source_offset = &mut self.source_offset;
            let payload = &mut self.payload[..payload_size];
            Self::read_source_exact_parts(source, source_offset, payload)?;
        }

        self.state
            .validate_indices(row_index, table_index)
            .map_err(|kind| Error::at(kind, row_start))?;
        self.state
            .validate_order(row_index, table_index)
            .map_err(|kind| Error::at(kind, row_start))?;

        Ok(Some(RowRecord {
            row_index,
            table_index,
            payload_size,
            row_start,
        }))
    }

    /// Reads forward until a row matching `matches_table` is decoded.
    fn read_next_matching_row<F>(&mut self, row: &mut RowBuf, mut matches_table: F) -> Result<bool>
    where
        F: FnMut(u8) -> bool,
    {
        loop {
            let Some(record) = self.read_record()? else {
                return Ok(false);
            };
            if matches_table(record.table_index) {
                self.decode_record_into_row(record, row)?;
                self.state.advance();
                return Ok(true);
            }
            self.state.advance();
        }
    }

    /// Reads one byte without consuming it.
    fn peek_byte(&mut self) -> Result<Option<u8>> {
        loop {
            match self.source.fill_buf() {
                Ok(buffer) => return Ok(buffer.first().copied()),
                Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
                Err(err) => return Err(Error::at(ErrorKind::Io(err), self.source_offset)),
            }
        }
    }

    /// Reads exactly `destination.len()` bytes from the source.
    fn read_source_exact_parts(
        source: &mut BufReader<R>,
        source_offset: &mut u64,
        mut destination: &mut [u8],
    ) -> Result<()> {
        while !destination.is_empty() {
            match source.read(destination) {
                Ok(0) => return Err(Error::at(ErrorKind::UnexpectedEof, *source_offset)),
                Ok(read_count) => {
                    *source_offset += read_count as u64;
                    let (_, rest) = destination.split_at_mut(read_count);
                    destination = rest;
                }
                Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
                Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => {
                    return Err(Error::at(ErrorKind::UnexpectedEof, *source_offset));
                }
                Err(err) => return Err(Error::at(ErrorKind::Io(err), *source_offset)),
            }
        }
        Ok(())
    }

    /// Decodes a buffered row record's payload into `row`.
    fn decode_record_into_row(&mut self, record: RowRecord, row: &mut RowBuf) -> Result<()> {
        row.prepare(
            record.row_index,
            record.table_index,
            usize::from(self.state.zone_count),
        );

        let payload = &self.payload[..record.payload_size];
        if is_canonical_zero_row(payload) {
            row.set_zero_row();
            return Ok(());
        }

        let type_code = self.table_type_codes[usize::from(record.table_index - 1)];
        decode::decode_row_payload(type_code, payload, &mut self.scratch, row.values_mut())
            .map_err(|err| Error::at(err.into_kind(), record.row_start + 5))
    }

    /// Validates that `table_index` is in range for caller-supplied arguments.
    fn validate_requested_table(&self, table_index: u8) -> Result<()> {
        if table_index == 0 || table_index > self.state.table_count {
            return Err(Error::new(ErrorKind::TableIndexOutOfRange {
                table_index,
                table_count: self.state.table_count,
            }));
        }
        Ok(())
    }

    /// Resolves a table name to its 1-based index.
    fn resolve_table_name(&self, name: &str) -> Result<u8> {
        self.header
            .table_index_by_name(name)
            .ok_or_else(|| Error::new(ErrorKind::TableNotFound(name.to_owned())))
    }

    /// Builds a deduplicated table selection bitmap from caller-supplied
    /// 1-based indices.
    fn build_selection_from_indices(&self, table_indices: &[u8]) -> Result<TableSelection> {
        let mut selection = TableSelection::default();
        for &table_index in table_indices {
            self.validate_requested_table(table_index)?;
            selection.insert(table_index);
        }
        Ok(selection)
    }

    /// Builds a deduplicated table selection bitmap from caller-supplied table
    /// names.
    fn build_selection_from_names(&self, names: &[&str]) -> Result<TableSelection> {
        let mut selection = TableSelection::default();
        for &name in names {
            selection.insert(self.resolve_table_name(name)?);
        }
        Ok(selection)
    }
}

/// Compact membership bitmap for 1-based table indices in the inclusive range
/// `1` to [`crate::MAX_TABLE_COUNT`].
#[derive(Debug, Clone, Copy, Default)]
struct TableSelection([u64; 4]);

impl TableSelection {
    /// Marks a table as selected.
    #[inline]
    fn insert(&mut self, table_index: u8) {
        let bit = usize::from(table_index);
        self.0[bit >> 6] |= 1u64 << (bit & 63);
    }

    /// Returns true when `table_index` is selected.
    #[inline]
    fn contains(&self, table_index: u8) -> bool {
        let bit = usize::from(table_index);
        (self.0[bit >> 6] & (1u64 << (bit & 63))) != 0
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.0.iter().all(|&word| word == 0)
    }
}

/// Parsed metadata from one row record's five-byte header.
#[derive(Debug, Clone, Copy)]
struct RowRecord {
    row_index: u16,
    table_index: u8,
    payload_size: usize,
    row_start: u64,
}

/// Tracks expected row sequencing and read progress.
#[derive(Debug, Clone, Copy)]
struct ReadState {
    next_row: u16,
    next_table: u8,
    zone_count: u16,
    table_count: u8,
    rows_read: u32,
    row_count: u32,
}

impl ReadState {
    /// Creates initial state from header dimensions.
    fn new(header: &Header) -> Self {
        Self {
            next_row: 1,
            next_table: 1,
            zone_count: header.zone_count(),
            table_count: header.table_count(),
            rows_read: 0,
            row_count: header.row_count(),
        }
    }

    /// Returns true when all expected rows have been consumed.
    #[inline]
    fn done(&self) -> bool {
        self.rows_read >= self.row_count
    }

    /// Validates that a record's indices are within the header's declared bounds.
    fn validate_indices(
        &self,
        row_index: u16,
        table_index: u8,
    ) -> std::result::Result<(), ErrorKind> {
        if row_index == 0
            || row_index > self.zone_count
            || table_index == 0
            || table_index > self.table_count
        {
            return Err(ErrorKind::InvalidRowIndex {
                row_index,
                table_index,
                max_row: self.zone_count,
                max_table: self.table_count,
            });
        }
        Ok(())
    }

    /// Validates that a record matches the expected next position in sequence.
    fn validate_order(
        &self,
        row_index: u16,
        table_index: u8,
    ) -> std::result::Result<(), ErrorKind> {
        if row_index != self.next_row || table_index != self.next_table {
            return Err(ErrorKind::RowOrderViolation {
                expected_row: self.next_row,
                expected_table: self.next_table,
                got_row: row_index,
                got_table: table_index,
            });
        }
        Ok(())
    }

    /// Advances the sequence state to expect the next record.
    fn advance(&mut self) {
        self.rows_read += 1;
        if self.next_table < self.table_count {
            self.next_table += 1;
        } else {
            self.next_table = 1;
            self.next_row = self.next_row.saturating_add(1);
        }
    }

    /// Resets to initial state.
    fn reset(&mut self) {
        self.next_row = 1;
        self.next_table = 1;
        self.rows_read = 0;
    }
}

/// Default internal read buffer size (64 KiB).
const DEFAULT_READ_BUFFER: usize = 64 * 1024;

/// Read adapter used during header parsing to track consumed bytes.
struct CountingReader<'a, R> {
    inner: &'a mut BufReader<R>,
    source_offset: &'a mut u64,
}

impl<R: Read> Read for CountingReader<'_, R> {
    fn read(&mut self, destination: &mut [u8]) -> io::Result<usize> {
        loop {
            match self.inner.read(destination) {
                Ok(read_count) => {
                    *self.source_offset += read_count as u64;
                    return Ok(read_count);
                }
                Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
                Err(err) => return Err(err),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plane::encode_plane;

    /// Wraps a header payload with its 4-byte length prefix.
    fn make_record(payload: &[u8]) -> Vec<u8> {
        let total_size = 4 + payload.len() as u32;
        let mut record = total_size.to_le_bytes().to_vec();
        record.extend_from_slice(payload);
        record
    }

    /// Builds a complete five-record header for test fixtures.
    fn make_header(zones: u16, tables: &[(&str, &str)]) -> Vec<u8> {
        let mut data = Vec::new();

        let banner = b"MAT PGM=MATRIX VER=1";
        let run_id = b"ID=Reader Test";
        let par = format!("PAR Zones={} M={}", zones, tables.len()).into_bytes();

        let mut mvr = format!("MVR {}", tables.len()).into_bytes();
        mvr.push(0);
        for &(name, type_code) in tables {
            mvr.extend_from_slice(name.as_bytes());
            mvr.push(b'=');
            mvr.extend_from_slice(type_code.as_bytes());
            mvr.push(0);
        }

        data.extend(make_record(banner));
        data.extend(make_record(run_id));
        data.extend(make_record(&par));
        data.extend(make_record(&mvr));
        data.extend(make_record(b"ROW\0"));

        data
    }

    /// Builds one framed row record with the given payload bytes.
    fn make_row_record(row_index: u16, table_index: u8, payload: &[u8]) -> Vec<u8> {
        let chunk_size = (payload.len() + 2) as u16;
        let mut record = Vec::with_capacity(5 + payload.len());
        record.extend_from_slice(&row_index.to_le_bytes());
        record.push(table_index);
        record.extend_from_slice(&chunk_size.to_le_bytes());
        record.extend_from_slice(payload);
        record
    }

    /// Builds a numeric payload using descriptor `0x80` (B0 plane only).
    fn make_numeric_payload(values: &[u8]) -> Vec<u8> {
        let mut payload = vec![0x80, 0x80, 0x80];
        encode_plane(values, &mut payload);
        payload
    }

    #[test]
    fn reads_one_numeric_row_and_reaches_eof() {
        let mut bytes = make_header(1, &[("TRIPS", "0")]);
        let payload = make_numeric_payload(&[10]);
        bytes.extend(make_row_record(1, 1, &payload));

        let mut reader = ReaderBuilder::new().from_bytes(&bytes).unwrap();
        let header_len = reader.position();
        assert!(header_len > 0);

        let mut row = RowBuf::new();
        assert!(reader.read_row(&mut row).unwrap());
        assert_eq!(row.row_index(), 1);
        assert_eq!(row.table_index(), 1);
        assert_eq!(row.values(), &[10.0]);
        assert!(!row.is_zero_row());

        assert_eq!(reader.position() as usize, bytes.len());
        assert!(!reader.read_row(&mut row).unwrap());
        assert!(!reader.read_row(&mut row).unwrap());
    }

    #[test]
    fn zero_row_sets_zero_flag() {
        let mut bytes = make_header(4, &[("TRIPS", "2")]);
        bytes.extend(make_row_record(1, 1, &[0x80, 0x80, 0x00]));

        let mut reader = ReaderBuilder::new().from_bytes(&bytes).unwrap();
        let mut row = RowBuf::new();
        assert!(reader.read_row(&mut row).unwrap());
        assert_eq!(row.values(), &[0.0, 0.0, 0.0, 0.0]);
        assert!(row.is_zero_row());
    }

    #[test]
    fn trailing_bytes_after_last_row_are_rejected() {
        let mut bytes = make_header(1, &[("TRIPS", "0")]);
        let payload = make_numeric_payload(&[42]);
        bytes.extend(make_row_record(1, 1, &payload));
        bytes.push(0xAA);

        let mut reader = ReaderBuilder::new().from_bytes(&bytes).unwrap();
        let mut row = RowBuf::new();
        assert!(reader.read_row(&mut row).unwrap());
        let err = reader.read_row(&mut row).unwrap_err();
        assert!(matches!(err.kind(), ErrorKind::TrailingBytes));
    }

    #[test]
    fn out_of_order_rows_are_rejected() {
        let mut bytes = make_header(1, &[("A", "0"), ("B", "0")]);
        bytes.extend(make_row_record(1, 2, &[0x80, 0x80, 0x00]));

        let mut reader = ReaderBuilder::new().from_bytes(&bytes).unwrap();
        let mut row = RowBuf::new();
        let err = reader.read_row(&mut row).unwrap_err();
        assert!(matches!(
            err.kind(),
            ErrorKind::RowOrderViolation {
                expected_row: 1,
                expected_table: 1,
                got_row: 1,
                got_table: 2,
            }
        ));
    }

    #[test]
    fn invalid_row_indices_are_rejected() {
        let mut bytes = make_header(1, &[("A", "0")]);
        bytes.extend(make_row_record(2, 1, &[0x80, 0x80, 0x00]));

        let mut reader = ReaderBuilder::new().from_bytes(&bytes).unwrap();
        let mut row = RowBuf::new();
        let err = reader.read_row(&mut row).unwrap_err();
        assert!(matches!(
            err.kind(),
            ErrorKind::InvalidRowIndex {
                row_index: 2,
                table_index: 1,
                max_row: 1,
                max_table: 1,
            }
        ));
    }

    #[test]
    fn invalid_chunk_size_is_rejected() {
        let mut bytes = make_header(1, &[("A", "0")]);

        let row_index = 1u16;
        let table_index = 1u8;
        let invalid_chunk_size = 1u16;
        bytes.extend_from_slice(&row_index.to_le_bytes());
        bytes.push(table_index);
        bytes.extend_from_slice(&invalid_chunk_size.to_le_bytes());

        let mut reader = ReaderBuilder::new().from_bytes(&bytes).unwrap();
        let mut row = RowBuf::new();
        let err = reader.read_row(&mut row).unwrap_err();
        assert!(matches!(err.kind(), ErrorKind::InvalidChunkSize(1)));
    }

    #[test]
    fn truncated_payload_reports_unexpected_eof() {
        let mut bytes = make_header(1, &[("A", "0")]);
        let row_index = 1u16;
        let table_index = 1u8;
        let declared_chunk_size = 5u16;
        bytes.extend_from_slice(&row_index.to_le_bytes());
        bytes.push(table_index);
        bytes.extend_from_slice(&declared_chunk_size.to_le_bytes());
        bytes.extend_from_slice(&[0x80, 0x80]);

        let mut reader = ReaderBuilder::new().from_bytes(&bytes).unwrap();
        let mut row = RowBuf::new();
        let err = reader.read_row(&mut row).unwrap_err();
        assert!(matches!(err.kind(), ErrorKind::UnexpectedEof));
    }

    #[test]
    fn read_table_row_streams_only_requested_table() {
        let mut bytes = make_header(2, &[("A", "0"), ("B", "0")]);
        bytes.extend(make_row_record(1, 1, &make_numeric_payload(&[1, 2])));
        bytes.extend(make_row_record(1, 2, &make_numeric_payload(&[10, 20])));
        bytes.extend(make_row_record(2, 1, &make_numeric_payload(&[3, 4])));
        bytes.extend(make_row_record(2, 2, &make_numeric_payload(&[30, 40])));

        let mut reader = ReaderBuilder::new().from_bytes(&bytes).unwrap();
        let mut row = RowBuf::new();

        assert!(reader.read_table_row(2, &mut row).unwrap());
        assert_eq!(row.row_index(), 1);
        assert_eq!(row.table_index(), 2);
        assert_eq!(row.values(), &[10.0, 20.0]);

        assert!(reader.read_table_row(2, &mut row).unwrap());
        assert_eq!(row.row_index(), 2);
        assert_eq!(row.table_index(), 2);
        assert_eq!(row.values(), &[30.0, 40.0]);

        assert!(!reader.read_table_row(2, &mut row).unwrap());
    }

    #[test]
    fn read_selected_row_streams_requested_tables() {
        let mut bytes = make_header(1, &[("A", "0"), ("B", "0"), ("C", "0")]);
        bytes.extend(make_row_record(1, 1, &make_numeric_payload(&[1])));
        bytes.extend(make_row_record(1, 2, &make_numeric_payload(&[2])));
        bytes.extend(make_row_record(1, 3, &make_numeric_payload(&[3])));

        let mut reader = ReaderBuilder::new().from_bytes(&bytes).unwrap();
        let selection = reader.prepare_selection(&[1, 3]).unwrap();
        let mut row = RowBuf::new();

        assert!(reader.read_selected_row(selection, &mut row).unwrap());
        assert_eq!(row.table_index(), 1);
        assert_eq!(row.values(), &[1.0]);

        assert!(reader.read_selected_row(selection, &mut row).unwrap());
        assert_eq!(row.table_index(), 3);
        assert_eq!(row.values(), &[3.0]);

        assert!(!reader.read_selected_row(selection, &mut row).unwrap());
    }

    #[test]
    fn read_selected_row_empty_selection_returns_false() {
        let mut bytes = make_header(1, &[("A", "0")]);
        bytes.extend(make_row_record(1, 1, &make_numeric_payload(&[9])));

        let mut reader = ReaderBuilder::new().from_bytes(&bytes).unwrap();
        let selection = reader.prepare_selection(&[]).unwrap();
        let mut row = RowBuf::new();
        assert!(!reader.read_selected_row(selection, &mut row).unwrap());
        assert_eq!(reader.rows_read(), 0);
    }

    #[test]
    fn read_table_row_by_name_streams_named_table() {
        let mut bytes = make_header(2, &[("TRIPS", "0"), ("DIST", "0")]);
        bytes.extend(make_row_record(1, 1, &make_numeric_payload(&[1, 2])));
        bytes.extend(make_row_record(1, 2, &make_numeric_payload(&[10, 20])));
        bytes.extend(make_row_record(2, 1, &make_numeric_payload(&[3, 4])));
        bytes.extend(make_row_record(2, 2, &make_numeric_payload(&[30, 40])));

        let mut reader = ReaderBuilder::new().from_bytes(&bytes).unwrap();
        let mut row = RowBuf::new();

        assert!(reader.read_table_row_by_name("DIST", &mut row).unwrap());
        assert_eq!(row.row_index(), 1);
        assert_eq!(row.table_index(), 2);
        assert_eq!(row.values(), &[10.0, 20.0]);

        assert!(reader.read_table_row_by_name("DIST", &mut row).unwrap());
        assert_eq!(row.row_index(), 2);
        assert_eq!(row.values(), &[30.0, 40.0]);

        assert!(!reader.read_table_row_by_name("DIST", &mut row).unwrap());
    }

    #[test]
    fn read_table_row_by_name_rejects_unknown_name() {
        let mut bytes = make_header(1, &[("TRIPS", "0")]);
        bytes.extend(make_row_record(1, 1, &[0x80, 0x80, 0x00]));

        let mut reader = ReaderBuilder::new().from_bytes(&bytes).unwrap();
        let mut row = RowBuf::new();
        let err = reader.read_table_row_by_name("NOPE", &mut row).unwrap_err();
        assert!(matches!(err.kind(), ErrorKind::TableNotFound(name) if name == "NOPE"));
    }

    #[test]
    fn prepare_selection_by_name_streams_named_tables() {
        let mut bytes = make_header(1, &[("A", "0"), ("B", "0"), ("C", "0")]);
        bytes.extend(make_row_record(1, 1, &make_numeric_payload(&[1])));
        bytes.extend(make_row_record(1, 2, &make_numeric_payload(&[2])));
        bytes.extend(make_row_record(1, 3, &make_numeric_payload(&[3])));

        let mut reader = ReaderBuilder::new().from_bytes(&bytes).unwrap();
        let selection = reader.prepare_selection_by_name(&["A", "C"]).unwrap();
        let mut row = RowBuf::new();

        assert!(reader.read_selected_row(selection, &mut row).unwrap());
        assert_eq!(row.table_index(), 1);
        assert_eq!(row.values(), &[1.0]);

        assert!(reader.read_selected_row(selection, &mut row).unwrap());
        assert_eq!(row.table_index(), 3);
        assert_eq!(row.values(), &[3.0]);

        assert!(!reader.read_selected_row(selection, &mut row).unwrap());
    }

    #[test]
    fn prepare_selection_by_name_validates_names_past_255_entries() {
        let mut bytes = make_header(1, &[("A", "0")]);
        bytes.extend(make_row_record(1, 1, &make_numeric_payload(&[7])));

        let reader = ReaderBuilder::new().from_bytes(&bytes).unwrap();
        let mut names = vec!["A"; 255];
        names.push("NOPE");

        let err = reader.prepare_selection_by_name(&names).unwrap_err();
        assert!(matches!(err.kind(), ErrorKind::TableNotFound(name) if name == "NOPE"));
    }

    #[test]
    fn prepare_selection_by_name_uses_tables_named_after_255_entries() {
        let mut bytes = make_header(1, &[("A", "0"), ("B", "0")]);
        bytes.extend(make_row_record(1, 1, &make_numeric_payload(&[1])));
        bytes.extend(make_row_record(1, 2, &make_numeric_payload(&[2])));

        let mut reader = ReaderBuilder::new().from_bytes(&bytes).unwrap();
        let mut names = vec!["A"; 300];
        names.push("B");
        let selection = reader.prepare_selection_by_name(&names).unwrap();
        let mut row = RowBuf::new();

        assert!(reader.read_selected_row(selection, &mut row).unwrap());
        assert_eq!(row.table_index(), 1);
        assert_eq!(row.values(), &[1.0]);

        assert!(reader.read_selected_row(selection, &mut row).unwrap());
        assert_eq!(row.table_index(), 2);
        assert_eq!(row.values(), &[2.0]);

        assert!(!reader.read_selected_row(selection, &mut row).unwrap());
    }

    #[test]
    fn reset_allows_rereading_all_rows() {
        let mut bytes = make_header(1, &[("TRIPS", "0")]);
        let payload = make_numeric_payload(&[42]);
        bytes.extend(make_row_record(1, 1, &payload));

        let mut reader = ReaderBuilder::new().from_bytes(&bytes).unwrap();
        let mut row = RowBuf::new();

        assert!(reader.read_row(&mut row).unwrap());
        assert_eq!(row.values(), &[42.0]);
        assert!(!reader.read_row(&mut row).unwrap());

        reader.reset().unwrap();

        assert!(reader.read_row(&mut row).unwrap());
        assert_eq!(row.values(), &[42.0]);
        assert!(!reader.read_row(&mut row).unwrap());
    }

    #[test]
    fn reset_between_different_table_reads() {
        let mut bytes = make_header(2, &[("A", "0"), ("B", "0")]);
        bytes.extend(make_row_record(1, 1, &make_numeric_payload(&[1, 2])));
        bytes.extend(make_row_record(1, 2, &make_numeric_payload(&[10, 20])));
        bytes.extend(make_row_record(2, 1, &make_numeric_payload(&[3, 4])));
        bytes.extend(make_row_record(2, 2, &make_numeric_payload(&[30, 40])));

        let mut reader = ReaderBuilder::new().from_bytes(&bytes).unwrap();
        let mut row = RowBuf::new();

        let mut b_total = 0.0;
        while reader.read_table_row_by_name("B", &mut row).unwrap() {
            b_total += row.values().iter().sum::<f64>();
        }
        assert_eq!(b_total, 100.0);

        reader.reset().unwrap();
        let mut a_total = 0.0;
        while reader.read_table_row_by_name("A", &mut row).unwrap() {
            a_total += row.values().iter().sum::<f64>();
        }
        assert_eq!(a_total, 10.0);
    }

    #[test]
    fn reset_resets_rows_read_counter() {
        let mut bytes = make_header(1, &[("A", "0")]);
        bytes.extend(make_row_record(1, 1, &[0x80, 0x80, 0x00]));

        let mut reader = ReaderBuilder::new().from_bytes(&bytes).unwrap();
        let mut row = RowBuf::new();

        assert_eq!(reader.rows_read(), 0);
        assert!(reader.read_row(&mut row).unwrap());
        assert_eq!(reader.rows_read(), 1);

        reader.reset().unwrap();
        assert_eq!(reader.rows_read(), 0);
    }

    #[test]
    fn header_table_index_by_name_found() {
        let bytes = make_header(1, &[("TRIPS", "0"), ("DIST", "S"), ("TIME", "D")]);
        let reader = ReaderBuilder::new().from_bytes(&bytes).unwrap();
        let header = reader.header();

        assert_eq!(header.table_index_by_name("TRIPS"), Some(1));
        assert_eq!(header.table_index_by_name("DIST"), Some(2));
        assert_eq!(header.table_index_by_name("TIME"), Some(3));
    }

    #[test]
    fn header_table_index_by_name_not_found() {
        let bytes = make_header(1, &[("TRIPS", "0")]);
        let reader = ReaderBuilder::new().from_bytes(&bytes).unwrap();
        assert_eq!(reader.header().table_index_by_name("NOPE"), None);
    }

    #[test]
    fn header_table_index_by_name_is_case_sensitive() {
        let bytes = make_header(1, &[("TRIPS", "0")]);
        let reader = ReaderBuilder::new().from_bytes(&bytes).unwrap();
        assert_eq!(reader.header().table_index_by_name("trips"), None);
        assert_eq!(reader.header().table_index_by_name("TRIPS"), Some(1));
    }

    #[test]
    fn get_ref_returns_underlying_source() {
        let bytes = make_header(1, &[("A", "0")]);
        let reader = ReaderBuilder::new().from_bytes(&bytes).unwrap();
        let cursor: &Cursor<&[u8]> = reader.get_ref();
        assert!(cursor.position() > 0);
    }

    #[test]
    fn read_table_row_rejects_zero_table_index() {
        let mut bytes = make_header(1, &[("A", "0")]);
        bytes.extend(make_row_record(1, 1, &[0x80, 0x80, 0x00]));

        let mut reader = ReaderBuilder::new().from_bytes(&bytes).unwrap();
        let mut row = RowBuf::new();
        let err = reader.read_table_row(0, &mut row).unwrap_err();
        assert!(matches!(
            err.kind(),
            ErrorKind::TableIndexOutOfRange {
                table_index: 0,
                table_count: 1,
            }
        ));
    }

    #[test]
    fn read_table_row_rejects_excessive_table_index() {
        let mut bytes = make_header(1, &[("A", "0")]);
        bytes.extend(make_row_record(1, 1, &[0x80, 0x80, 0x00]));

        let mut reader = ReaderBuilder::new().from_bytes(&bytes).unwrap();
        let mut row = RowBuf::new();
        let err = reader.read_table_row(2, &mut row).unwrap_err();
        assert!(matches!(
            err.kind(),
            ErrorKind::TableIndexOutOfRange {
                table_index: 2,
                table_count: 1,
            }
        ));
    }

    #[test]
    fn reader_retries_on_interrupted_reads() {
        // Returns `Interrupted` a fixed number of times before delivering data.
        struct InterruptingReader {
            data: Vec<u8>,
            pos: usize,
            interrupts_remaining: usize,
        }

        impl Read for InterruptingReader {
            fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
                if self.interrupts_remaining > 0 {
                    self.interrupts_remaining -= 1;
                    return Err(io::Error::new(io::ErrorKind::Interrupted, "interrupted"));
                }
                let available = self.data.len() - self.pos;
                let count = available.min(buf.len());
                if count == 0 {
                    return Ok(0);
                }
                buf[..count].copy_from_slice(&self.data[self.pos..self.pos + count]);
                self.pos += count;
                Ok(count)
            }
        }

        let mut bytes = make_header(1, &[("A", "0")]);
        bytes.extend(make_row_record(1, 1, &make_numeric_payload(&[9])));

        let source = InterruptingReader {
            data: bytes,
            pos: 0,
            interrupts_remaining: 8,
        };
        let mut reader = ReaderBuilder::new().from_reader(source).unwrap();
        let mut row = RowBuf::new();
        assert!(reader.read_row(&mut row).unwrap());
        assert_eq!(row.values(), &[9.0]);
        assert!(!reader.read_row(&mut row).unwrap());
    }
}
