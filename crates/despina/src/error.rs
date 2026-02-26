//! Error types for reading and writing `.mat` matrix files.
//!
//! An [`std::io::Error`] is automatically converted into an [`Error`] via the
//! [`From`] implementation. As a special case, an I/O error with kind
//! [`std::io::ErrorKind::UnexpectedEof`] is promoted to
//! [`ErrorKind::UnexpectedEof`] rather than being wrapped in [`ErrorKind::Io`],
//! since premature end-of-file is a distinctive failure mode for truncated
//! `.mat` files.

use std::fmt;
use std::io;

use crate::types::TypeCode;

/// A type alias for `std::result::Result<T, Error>`.
pub type Result<T> = std::result::Result<T, Error>;

/// The error type for all operations in this crate.
///
/// Every error carries an [`ErrorKind`] describing what went wrong. Errors
/// produced while reading a `.mat` file also carry a byte offset indicating
/// where in the input the problem was detected. Use [`Error::offset`] to
/// retrieve it.
///
/// Internally, the error payload is heap-allocated so that `Result<T, Error>`
/// stays compact (pointer-sized) on the success path.
#[derive(Debug)]
pub struct Error(Box<ErrorInner>);

#[derive(Debug)]
struct ErrorInner {
    kind: ErrorKind,
    offset: Option<u64>,
}

impl Error {
    pub(crate) fn new(kind: ErrorKind) -> Self {
        Self(Box::new(ErrorInner { kind, offset: None }))
    }

    pub(crate) fn at(kind: ErrorKind, offset: u64) -> Self {
        Self(Box::new(ErrorInner {
            kind,
            offset: Some(offset),
        }))
    }

    /// Return the specific kind of this error.
    #[must_use]
    pub fn kind(&self) -> &ErrorKind {
        &self.0.kind
    }

    /// Consume the error and return the underlying [`ErrorKind`].
    #[must_use]
    pub fn into_kind(self) -> ErrorKind {
        self.0.kind
    }

    /// Byte offset in the input where the error was detected, if known.
    ///
    /// This is populated by the reader from its current stream position at the
    /// time the error occurs. It is not set for errors that originate from the
    /// writer or from contexts where the position is unavailable.
    #[must_use]
    pub fn offset(&self) -> Option<u64> {
        self.0.offset
    }
}

fn write_type_code_token(f: &mut fmt::Formatter<'_>, type_code: TypeCode) -> fmt::Result {
    match type_code {
        TypeCode::Float32 => f.write_str("'S'"),
        TypeCode::Float64 => f.write_str("'D'"),
        TypeCode::Fixed(p) if p <= 9 => write!(f, "'{}'", char::from(b'0' + p)),
        TypeCode::Fixed(p) => write!(f, "fixed({})", p),
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(offset) = self.0.offset {
            write!(f, "at byte offset {}: ", offset)?;
        }
        match &self.0.kind {
            ErrorKind::Io(err) => write!(f, "I/O error: {}", err),
            ErrorKind::InvalidHeaderLength {
                record_index,
                total_size,
            } => write!(
                f,
                "invalid header record length: record {} has {} bytes (minimum 4)",
                record_index, total_size
            ),
            ErrorKind::InvalidPar(msg) => write!(f, "invalid parameter record (PAR): {}", msg),
            ErrorKind::InvalidMvr(msg) => {
                write!(f, "invalid table catalogue record (MVR): {}", msg)
            }
            ErrorKind::InvalidTypeCode { token } => write!(
                f,
                "invalid type code: expected one of '0'..'9', 'S', 'D'; got \"{}\"",
                token
            ),
            ErrorKind::MissingRowMarker => write!(f, "missing ROW data-start marker in header"),
            ErrorKind::TableCountMismatch { par, mvr } => write!(
                f,
                "table count mismatch between PAR and MVR: PAR={}, MVR={}",
                par, mvr
            ),
            ErrorKind::InvalidRowIndex {
                row_index,
                table_index,
                max_row,
                max_table,
            } => write!(
                f,
                "row index out of range: row {}, table {} (expected row 1..={}, table 1..={})",
                row_index, table_index, max_row, max_table
            ),
            ErrorKind::RowOrderViolation {
                expected_row,
                expected_table,
                got_row,
                got_table,
            } => write!(
                f,
                "row order violation: expected row {}, table {}; got row {}, table {}",
                expected_row, expected_table, got_row, got_table
            ),
            ErrorKind::InvalidPreamble { got } => write!(
                f,
                "invalid row preamble: expected 0x80 0x80, got 0x{:02X} 0x{:02X}",
                got[0], got[1]
            ),
            ErrorKind::InvalidDescriptor {
                descriptor,
                type_code,
            } => {
                write!(f, "invalid descriptor byte for type code ")?;
                write_type_code_token(f, *type_code)?;
                write!(f, ": got 0x{:02X}", descriptor)
            }
            ErrorKind::PlaneSize { expected, got } => write!(
                f,
                "compressed row size mismatch: expected {} bytes after decode, got {}",
                expected, got
            ),
            ErrorKind::ZeroRunCount => {
                write!(f, "invalid compressed row data: run with zero count")
            }
            ErrorKind::InvalidFloat32Marker { marker_index, got } => write!(
                f,
                "invalid float32 marker byte: index {}, expected 0xFF, got 0x{:02X}",
                marker_index, got
            ),
            ErrorKind::InvalidChunkSize(size) => write!(
                f,
                "invalid row record chunk size: expected at least 2, got {}",
                size
            ),
            ErrorKind::TrailingBytes => {
                write!(f, "trailing bytes after the final expected row record")
            }
            ErrorKind::TableNotFound(name) => write!(f, "table not found in header: \"{}\"", name),
            ErrorKind::TableIndexOutOfRange {
                table_index,
                table_count,
            } => write!(
                f,
                "table index out of range: expected 1..={}, got {}",
                table_count, table_index
            ),
            ErrorKind::UnexpectedEof => write!(f, "unexpected end of file"),
            ErrorKind::InvalidZoneCount => write!(
                f,
                "invalid zone count: expected 1..={}",
                crate::MAX_ZONE_COUNT
            ),
            ErrorKind::InvalidTableCount(msg) => {
                write!(f, "invalid table configuration: {}", msg)
            }
            ErrorKind::ZoneCountMismatch { expected, got } => write!(
                f,
                "zone count mismatch: expected {} values, got {}",
                expected, got
            ),
            ErrorKind::ShapeMismatch {
                context,
                expected,
                got,
            } => write!(
                f,
                "shape mismatch in {}: expected {} values, got {}",
                context, expected, got
            ),
            ErrorKind::InvalidTableName(msg) => {
                write!(f, "invalid table name: {}", msg)
            }
            ErrorKind::PayloadTooLarge(size) => write!(
                f,
                "encoded row payload too large: maximum 65533 bytes, got {}",
                size
            ),
            ErrorKind::WriterFinished => {
                write!(f, "writer is complete: no further rows are accepted")
            }
            ErrorKind::WriterPositionMismatch {
                expected_origin,
                expected_table,
                got_origin,
                got_table,
            } => write!(
                f,
                "writer position mismatch: expected origin {}, table {}; got origin {}, table {}",
                expected_origin, expected_table, got_origin, got_table
            ),
            ErrorKind::IncompleteMatrix { expected, written } => write!(
                f,
                "incomplete matrix: expected {} rows, wrote {}",
                expected, written
            ),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.0.kind {
            ErrorKind::Io(err) => Some(err),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        if err.kind() == io::ErrorKind::UnexpectedEof {
            Error::new(ErrorKind::UnexpectedEof)
        } else {
            Error::new(ErrorKind::Io(err))
        }
    }
}

/// The specific kind of error that occurred.
///
/// This enum is marked `#[non_exhaustive]` so that new variants can be added in
/// future releases without breaking existing code. Match expressions should
/// include a wildcard arm.
#[non_exhaustive]
#[derive(Debug)]
pub enum ErrorKind {
    /// An I/O error from the underlying reader or writer.
    ///
    /// This wraps the original [`std::io::Error`] without losing information.
    /// Note that I/O errors with kind [`std::io::ErrorKind::UnexpectedEof`] are
    /// promoted to [`ErrorKind::UnexpectedEof`] instead of appearing here.
    Io(io::Error),

    /// A header record has an invalid length prefix.
    ///
    /// Every header record in a `.mat` file begins with a four-byte
    /// little-endian size that includes the size field itself, so the minimum
    /// valid value is 4. This error indicates the size field contained a
    /// smaller value, which means the header is corrupt or the file is not a
    /// valid `.mat` file.
    InvalidHeaderLength {
        /// Which header record failed (1-based index into the five mandatory
        /// header records).
        record_index: u8,
        /// The invalid size value that was read.
        total_size: u32,
    },

    /// The parameter record (PAR) could not be parsed.
    ///
    /// The PAR header record declares the matrix dimensions (`Zones`) and table
    /// count (`M`). This error fires when the record payload is missing these
    /// fields, contains unparseable values, or specifies zero for either
    /// dimension. The attached string describes the specific parsing failure.
    InvalidPar(String),

    /// The table catalogue record (MVR) is malformed.
    ///
    /// The MVR header record lists each table's name and storage type code.
    /// This error fires when the record has the wrong number of table entries,
    /// is missing the `=` separator between name and type code, or is otherwise
    /// structurally invalid. The
    /// attached string describes the specific parsing failure.
    InvalidMvr(String),

    /// A table storage type code is invalid.
    ///
    /// This is returned when an MVR record contains an unrecognised type-code
    /// token, or when writer/build APIs are given a [`TypeCode::Fixed`] value
    /// outside the supported range `0..=9`.
    InvalidTypeCode {
        /// The invalid token that was supplied or parsed.
        token: String,
    },

    /// The ROW data-start marker is missing or malformed.
    ///
    /// The fifth and final header record must contain exactly the bytes
    /// `ROW\0`. This marker separates the header from the row data that
    /// follows. If it is absent, the file is either corrupt or not a valid
    /// `.mat` file.
    MissingRowMarker,

    /// The parameter record and table catalogue disagree on the number of
    /// tables.
    ///
    /// The PAR header record declares a table count (`M=<T>`), and the MVR
    /// header record independently declares its own count (`MVR <T>`). These
    /// two values must agree. A mismatch indicates a corrupt or inconsistently
    /// written file.
    TableCountMismatch {
        /// Table count from the parameter record (PAR `M` value).
        par: u8,
        /// Table count from the table catalogue record (MVR header count).
        mvr: u8,
    },

    /// A row record's indices are outside the valid range.
    ///
    /// Each row record carries a 1-based row index (origin zone) and a 1-based
    /// table index (matrix layer). Both must fall within the dimensions
    /// declared in the header. This error typically indicates file corruption
    /// or a writer that emitted indices beyond the declared matrix size.
    InvalidRowIndex {
        /// The row index found in the record.
        row_index: u16,
        /// The table index found in the record.
        table_index: u8,
        /// Maximum valid row index (equal to the zone count).
        max_row: u16,
        /// Maximum valid table index (equal to the table count).
        max_table: u8,
    },

    /// Row records arrived out of the required sequential order.
    ///
    /// Row records in a `.mat` file must appear in strict row-major,
    /// table-major order: all tables for row 1, then all tables for row 2, and
    /// so on. This error fires when a record appears out of sequence, which
    /// typically indicates a corrupt file or a writer that did not emit rows in
    /// canonical order.
    RowOrderViolation {
        /// The row index that was expected next.
        expected_row: u16,
        /// The table index that was expected next.
        expected_table: u8,
        /// The row index that was actually found.
        got_row: u16,
        /// The table index that was actually found.
        got_table: u8,
    },

    /// The two-byte preamble at the start of a row payload is invalid.
    ///
    /// Every row payload begins with the bytes `0x80 0x80`. This fixed preamble
    /// acts as a sanity check on row framing. If the bytes differ, the reader
    /// has likely lost synchronisation with the row record boundaries, which
    /// points to file corruption or an incorrect chunk length in a preceding
    /// record.
    InvalidPreamble {
        /// The two bytes that were found instead of `0x80 0x80`.
        got: [u8; 2],
    },

    /// The descriptor byte is not valid for the table's type code.
    ///
    /// After the preamble, each row payload contains a descriptor byte that
    /// determines how the remaining data is decoded. The set of valid
    /// descriptors depends on the table's type code: fixed-point numeric tables
    /// (type codes `0`..`9`) use descriptors that encode decimal precision and
    /// sign information, while float32 (`S`) and float64 (`D`) tables each
    /// accept their own descriptor range. An invalid descriptor indicates
    /// corruption in the row data.
    InvalidDescriptor {
        /// The descriptor byte that was read.
        descriptor: u8,
        /// The type code of the table this row belongs to.
        type_code: TypeCode,
    },

    /// Decompressing row data produced the wrong number of bytes.
    ///
    /// Row payloads are compressed using a run-length command stream. After
    /// decompression, the output must be exactly `Zones` bytes (one per
    /// destination). A size mismatch means the command stream is corrupt or
    /// the zone count in the header does not match the actual data.
    PlaneSize {
        /// The expected number of bytes (equal to the zone count).
        expected: u16,
        /// The actual number of bytes produced by decompression.
        got: usize,
    },

    /// A run-length command in the compressed row data has a zero run count.
    ///
    /// The compression format encodes runs of repeated values with a count
    /// prefix. A count of zero is invalid and indicates corruption in the
    /// compressed row data.
    ZeroRunCount,

    /// A float32 marker byte is not the expected `0xFF` value.
    ///
    /// For float32 tables, a marker byte stream runs parallel to the value
    /// bytes and must consist entirely of `0xFF` entries. A non-`0xFF` byte at
    /// an index where `0xFF` was expected indicates corruption in the row
    /// data.
    InvalidFloat32Marker {
        /// The index within the marker stream where the bad value was found.
        marker_index: usize,
        /// The byte value that was found instead of `0xFF`.
        got: u8,
    },

    /// A row record's chunk size is below the minimum.
    ///
    /// Each row record carries a chunk size field that counts its own two
    /// bytes plus all payload bytes that follow. The minimum valid value is
    /// therefore 2 (an empty payload). A smaller value indicates a corrupt row
    /// record header.
    InvalidChunkSize(u16),

    /// Unexpected data remains after the last expected row record.
    ///
    /// A conforming `.mat` file contains exactly `Zones * Tables` row records
    /// after the header, with no trailing data. Extra bytes after the final
    /// record indicate that the file was not cleanly written or has been
    /// appended to.
    TrailingBytes,

    /// The requested table name was not found in the file header.
    ///
    /// Returned by the name-based read methods when the supplied name does not
    /// match any table declared in the header's table catalogue. The attached
    /// string is the name that was requested.
    TableNotFound(String),

    /// The caller-supplied table index is outside the valid range.
    ///
    /// This is returned by the table-filtered read methods when the caller
    /// passes a table index that is zero or greater than the number of tables
    /// declared in the file header. Unlike [`ErrorKind::InvalidRowIndex`],
    /// which signals a corrupt row record in the file, this variant indicates
    /// an invalid argument from the caller.
    TableIndexOutOfRange {
        /// The table index that was requested.
        table_index: u8,
        /// The number of tables in the file (maximum valid index).
        table_count: u8,
    },

    /// The file ended before a complete header or row record could be read.
    ///
    /// This typically means the file has been truncated. It is also produced
    /// when an [`std::io::Error`] with kind
    /// [`std::io::ErrorKind::UnexpectedEof`] is converted into an [`Error`].
    UnexpectedEof,

    /// A zone count is outside the supported range.
    ///
    /// Valid zone counts are `1..=[crate::MAX_ZONE_COUNT]`.
    InvalidZoneCount,

    /// The writer was given an invalid table configuration.
    ///
    /// This fires when the tables slice is empty or exceeds
    /// [`crate::MAX_TABLE_COUNT`]. The attached string describes the specific
    /// problem.
    InvalidTableCount(String),

    /// The values slice passed to
    /// [`Writer::write_next_row`](crate::Writer::write_next_row)
    /// has the wrong length.
    ///
    /// The values slice must have exactly `zone_count` elements, matching the
    /// dimension declared at writer construction time.
    ZoneCountMismatch {
        /// The expected number of elements (the zone count).
        expected: u16,
        /// The actual number of elements in the values slice.
        got: usize,
    },

    /// A bulk write call received a values slice with the wrong total length.
    ///
    /// This error is used by [`Matrix::from_parts`](crate::Matrix::from_parts)
    /// and shape-aware writer methods such as
    /// [`Writer::write_origin`](crate::Writer::write_origin),
    /// [`Writer::write_origins`](crate::Writer::write_origins),
    /// and [`Writer::write_stack`](crate::Writer::write_stack) when the input
    /// slice length does not match the expected element count.
    ShapeMismatch {
        /// Writer method name that rejected the input.
        context: &'static str,
        /// Expected total element count.
        expected: usize,
        /// Actual total element count.
        got: usize,
    },

    /// A table name is invalid for use in a `.mat` file.
    ///
    /// Table names must be non-empty and consist only of ASCII characters.
    /// This error is produced during writer construction if a table definition
    /// violates these constraints.
    InvalidTableName(String),

    /// The encoded row payload exceeds the maximum size representable in the
    /// `.mat` format.
    ///
    /// Each row record's chunk size is stored as a `u16`, limiting the payload
    /// to 65 533 bytes. This error indicates that the compressed row data for
    /// a particular origin and table exceeded this limit, which can happen with
    /// very large zone counts and incompressible data. The attached value is the
    /// payload size that was produced.
    PayloadTooLarge(usize),

    /// All rows have already been written to the writer.
    ///
    /// The writer tracks its expected row count (`zone_count × table_count`)
    /// and rejects further [`Writer::write_next_row`](crate::Writer::write_next_row)
    /// calls once all rows have been supplied. This prevents silent data
    /// corruption from writing past the end of the matrix.
    WriterFinished,

    /// A coordinate-aware write call targeted a position other than the
    /// current expected writer cursor.
    ///
    /// This currently occurs when origin-block APIs are called while the writer
    /// is not at a table boundary for the next origin. In that case,
    /// [`Writer::write_origin`](crate::Writer::write_origin) and
    /// [`Writer::write_origins`](crate::Writer::write_origins) reject the call
    /// to preserve canonical matrix-file order.
    WriterPositionMismatch {
        /// Expected 1-based origin index.
        expected_origin: u16,
        /// Expected 1-based table index.
        expected_table: u8,
        /// Supplied 1-based origin index.
        got_origin: u16,
        /// Supplied 1-based table index.
        got_table: u8,
    },

    /// [`Writer::finish`](crate::Writer::finish) was called before all rows
    /// were written.
    ///
    /// A conforming `.mat` file must contain exactly `zone_count × table_count`
    /// row records. This error indicates that the writer was finalised early.
    /// Use [`Writer::into_inner`](crate::Writer::into_inner) if you
    /// intentionally want to skip the completeness check.
    IncompleteMatrix {
        /// The number of rows that should have been written.
        expected: u32,
        /// The number of rows that were actually written.
        written: u32,
    },
}

/// An error returned by [`crate::Writer::into_inner`] when the flush fails.
///
/// The error wraps both the original [`io::Error`] and the writer value so
/// callers can recover and retry.
pub struct IntoInnerError<W> {
    writer: W,
    error: io::Error,
}

impl<W> IntoInnerError<W> {
    pub(crate) fn new(writer: W, error: io::Error) -> Self {
        Self { writer, error }
    }

    /// Returns a reference to the flush error.
    #[must_use]
    pub fn error(&self) -> &io::Error {
        &self.error
    }

    /// Consumes the error and returns the flush error.
    #[must_use]
    pub fn into_error(self) -> io::Error {
        self.error
    }

    /// Consumes the error and returns the writer.
    #[must_use]
    pub fn into_inner(self) -> W {
        self.writer
    }
}

impl<W> fmt::Debug for IntoInnerError<W> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IntoInnerError")
            .field("error", &self.error)
            .finish_non_exhaustive()
    }
}

impl<W> fmt::Display for IntoInnerError<W> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "flush failed during into_inner: {}", self.error)
    }
}

impl<W> std::error::Error for IntoInnerError<W> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}

const _: () = {
    assert!(std::mem::size_of::<Error>() == std::mem::size_of::<usize>());
};
