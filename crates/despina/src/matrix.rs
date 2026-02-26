//! High-level in-memory API for `.mat` matrix files.
//!
//! [`Matrix`] loads every row of a `.mat` file into a flat `Vec<f64>` and
//! provides random-access to individual cells, rows, and whole tables. For
//! streaming forward-only access without loading everything into memory, use
//! the low-level [`Reader`](crate::Reader) instead.
//!
//! # Opening a file
//!
//! The simplest way to open a `.mat` file is the [`open`] convenience function:
//!
//! ```
//! # use despina::{MatrixBuilder, TypeCode};
//! # fn temp_mat_path(stem: &str) -> std::path::PathBuf {
//! #     let unique = std::time::SystemTime::now()
//! #         .duration_since(std::time::UNIX_EPOCH)
//! #         .unwrap()
//! #         .as_nanos();
//! #     std::env::temp_dir().join(format!(
//! #         "despina-doc-{stem}-{}-{unique}.mat",
//! #         std::process::id()
//! #     ))
//! # }
//! # let mut source = MatrixBuilder::new(2)
//! #     .table("DIST_AM", TypeCode::Float32)
//! #     .build()?;
//! # source.set(1, 1, 2, 120.0);
//! # let path = temp_mat_path("skim");
//! # source.write_to(&path)?;
//! let mat = despina::open(&path)?;
//! assert_eq!(mat.zone_count(), 2);
//! assert_eq!(mat.table_count(), 1);
//!
//! let dist_am = mat.table("DIST_AM");
//! let flow = dist_am.get(1, 2); // origin 1, destination 2
//! assert_eq!(flow, 120.0);
//! # let _ = std::fs::remove_file(path);
//! # Ok::<(), despina::Error>(())
//! ```
//!
//! # Working with table views
//!
//! [`Table`] and [`TableMut`] are lightweight borrowed views into a single
//! matrix layer. They carry no allocation of their own and provide cell, row,
//! and aggregate accessors scoped to one table:
//!
//! ```
//! use despina::{MatrixBuilder, TypeCode};
//!
//! let mut mat = MatrixBuilder::new(3)
//!     .table("DIST_AM", TypeCode::Float32)
//!     .build()?;
//!
//! mat.set(1, 1, 2, 100.0);
//! mat.set(1, 2, 3, 200.0);
//!
//! let dist_am = mat.table("DIST_AM");
//! assert_eq!(dist_am.get(1, 2), 100.0);
//! assert_eq!(dist_am.total(), 300.0);
//! assert_eq!(dist_am.row(1), &[0.0, 100.0, 0.0]);
//! # Ok::<(), despina::Error>(())
//! ```
//!
//! # Building a matrix from scratch
//!
//! [`MatrixBuilder`] constructs a new empty matrix without reading from disk.
//! After building, populate cells via [`Matrix::set`] or [`Matrix::table_mut`]:
//!
//! ```
//! use despina::{MatrixBuilder, TypeCode};
//!
//! let mut mat = MatrixBuilder::new(100)
//!     .table("DIST_AM", TypeCode::Float32)
//!     .table("TIME_AM", TypeCode::Float64)
//!     .build()?;
//!
//! mat.set(1, 1, 1, 42.0);
//! mat.table_mut("TIME_AM").fill(1.0);
//! # Ok::<(), despina::Error>(())
//! ```
//!
//! # Memory layout
//!
//! All cell values are stored as [`f64`] in a single flat vector, regardless of
//! the on-disk type code. Data is arranged in table-major, then row-major
//! order: all cells of table 1 come first, then all cells of table 2, and so
//! on. Within each table, cells are stored row by row (origin 1 destinations
//! first, then origin 2, and so on).
//!
//! Total memory usage is `zone_count * zone_count * table_count * 8` bytes.
//!
//! # Indexing convention
//!
//! All origin, destination, and table indices throughout this module are
//! **1-based**, matching the on-disk format and the convention used universally
//! in transport planning. Passing 0 to any accessor will panic.

use std::io::Read;
#[cfg(not(target_arch = "wasm32"))]
use std::path::Path;

use crate::MAX_ZONE_COUNT;
use crate::error::{Error, ErrorKind, Result};
use crate::header::{Header, TableInfo};
use crate::reader::Reader;
#[cfg(not(target_arch = "wasm32"))]
use crate::reader::ReaderBuilder;
use crate::row::RowBuf;
use crate::table_defs::{table_infos_from_defs, validate_table_defs};
use crate::types::TypeCode;

const DEFAULT_MATRIX_BUILDER_BANNER: &str =
    concat!("MAT PGM=DESPINA VER=", env!("CARGO_PKG_VERSION"));
const DEFAULT_MATRIX_BUILDER_RUN_ID: &str = "DESPINA";

/// Panics if `table_index` is zero or exceeds `table_count`.
fn assert_table_index_in_bounds(table_index: u8, table_count: u8) {
    assert!(
        table_index >= 1 && table_index <= table_count,
        "table index {} is out of bounds for table count {} (tables are 1-based)",
        table_index,
        table_count,
    );
}

/// Panics if `origin` is zero or exceeds `zone_count`.
fn assert_origin_in_bounds(origin: u16, zone_count: u16) {
    assert!(
        origin >= 1 && origin <= zone_count,
        "origin {} is out of bounds for zone count {} (origins are 1-based)",
        origin,
        zone_count,
    );
}

/// Panics if `destination` is zero or exceeds `zone_count`.
fn assert_destination_in_bounds(destination: u16, zone_count: u16) {
    assert!(
        destination >= 1 && destination <= zone_count,
        "destination {} is out of bounds for zone count {} (destinations are 1-based)",
        destination,
        zone_count,
    );
}

/// Returns the element index into a single table's flat data for the cell at
/// 1-based `(origin, destination)`. Panics if either index is out of bounds.
fn checked_cell_index(origin: u16, destination: u16, zone_count: u16) -> usize {
    assert_origin_in_bounds(origin, zone_count);
    assert_destination_in_bounds(destination, zone_count);
    let zone_count = usize::from(zone_count);
    usize::from(origin - 1) * zone_count + usize::from(destination - 1)
}

/// Returns the element index into a single table's flat data for the cell at
/// 1-based `(origin, destination)`, or `None` if either index is out of bounds.
fn try_cell_index(origin: u16, destination: u16, zone_count: u16) -> Option<usize> {
    if origin < 1 || origin > zone_count || destination < 1 || destination > zone_count {
        return None;
    }
    let zone_count = usize::from(zone_count);
    Some(usize::from(origin - 1) * zone_count + usize::from(destination - 1))
}

/// Returns `zone_count * zone_count` if it fits in `usize`.
fn checked_cells_per_table(zone_count: usize) -> Result<usize> {
    zone_count.checked_mul(zone_count).ok_or_else(|| {
        Error::new(ErrorKind::InvalidPar(
            "matrix dimensions exceed addressable memory on this platform".into(),
        ))
    })
}

/// Returns `cells_per_table * table_count` if it fits in `usize`.
fn checked_total_cells(cells_per_table: usize, table_count: usize) -> Result<usize> {
    cells_per_table.checked_mul(table_count).ok_or_else(|| {
        Error::new(ErrorKind::InvalidPar(
            "matrix dimensions exceed addressable memory on this platform".into(),
        ))
    })
}

/// Resolves table names to validated, deduplicated 1-based indices.
///
/// Returns `InvalidTableCount` if `names` is empty, or `TableNotFound` for
/// any name not present in the header.
fn resolve_table_names(header: &Header, names: &[&str]) -> Result<Vec<u8>> {
    if names.is_empty() {
        return Err(Error::new(ErrorKind::InvalidTableCount(
            "at least one table name must be provided".into(),
        )));
    }
    let mut seen = [false; 256];
    let mut indices = Vec::with_capacity(names.len());
    for &name in names {
        let index = header
            .table_index_by_name(name)
            .ok_or_else(|| Error::new(ErrorKind::TableNotFound(name.to_owned())))?;
        if !seen[usize::from(index)] {
            seen[usize::from(index)] = true;
            indices.push(index);
        }
    }
    Ok(indices)
}

/// Builds a remap table: `remap[original_index]` yields the new 1-based
/// index for selected tables, or 0 for non-selected tables.
fn build_table_remap(header: &Header, selected_indices: &[u8]) -> [u8; 256] {
    let mut remap = [0u8; 256];
    let mut new_index: u8 = 0;
    // Walk tables in file order to assign sequential new indices.
    for table in header.tables() {
        if selected_indices.contains(&table.index()) {
            new_index += 1;
            remap[usize::from(table.index())] = new_index;
        }
    }
    remap
}

/// A fully-loaded in-memory matrix from a `.mat` file.
///
/// `Matrix` is the random-access API. All cells for all tables are stored in a
/// single `Vec<f64>`, allowing cheap repeated access and in-place mutation.
///
/// Core method families:
///
/// - Scalar access by index or name: [`get`](Matrix::get),
///   [`set`](Matrix::set), [`get_by_name`](Matrix::get_by_name),
///   [`set_by_name`](Matrix::set_by_name)
/// - Row and table slices: [`row`](Matrix::row),
///   [`row_mut`](Matrix::row_mut), [`table_data`](Matrix::table_data),
///   [`table_data_mut`](Matrix::table_data_mut)
/// - Table views: [`table`](Matrix::table), [`table_mut`](Matrix::table_mut),
///   [`table_by_index`](Matrix::table_by_index), [`tables`](Matrix::tables)
/// - Persistence: [`open`](Matrix::open), [`from_bytes`](Matrix::from_bytes),
///   [`write_to`](Matrix::write_to), [`write_to_writer`](Matrix::write_to_writer)
///
/// All public indices are **1-based** to match the `.mat` format.
///
/// See the module-level documentation for usage patterns and tradeoffs.
///
/// # Example
///
/// ```
/// # use despina::{MatrixBuilder, TypeCode};
/// # let mut source = MatrixBuilder::new(2)
/// #     .table("DIST_AM", TypeCode::Float32)
/// #     .table("TIME_AM", TypeCode::Float32)
/// #     .build()?;
/// # source.set(1, 1, 1, 1.0);
/// # source.set(2, 1, 1, 10.0);
/// # let mut bytes = Vec::new();
/// # source.write_to_writer(&mut bytes)?;
/// let mat = despina::Matrix::from_bytes(&bytes)?;
///
/// let totals: Vec<(String, f64)> = mat
///     .tables()
///     .map(|table| (table.name().to_owned(), table.total()))
///     .collect();
/// assert_eq!(
///     totals,
///     vec![("DIST_AM".to_owned(), 1.0), ("TIME_AM".to_owned(), 10.0)]
/// );
/// # Ok::<(), despina::Error>(())
/// ```
#[derive(Debug, Clone)]
pub struct Matrix {
    header: Header,
    data: Vec<f64>,
}

impl Matrix {
    /// Opens a `.mat` file from disk and loads all row data into memory.
    ///
    /// This is the most common way to load a matrix. The file is opened, the
    /// header is parsed, and every row of every table is decoded into the
    /// internal flat buffer. The file is closed before this method returns.
    ///
    /// For loading from a byte slice that is already in memory, use
    /// [`Matrix::from_bytes`] instead.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be opened, the header is malformed,
    /// or any row record is corrupt or truncated.
    ///
    /// # Example
    ///
    /// ```
    /// # use despina::{MatrixBuilder, TypeCode};
    /// # fn temp_mat_path(stem: &str) -> std::path::PathBuf {
    /// #     let unique = std::time::SystemTime::now()
    /// #         .duration_since(std::time::UNIX_EPOCH)
    /// #         .unwrap()
    /// #         .as_nanos();
    /// #     std::env::temp_dir().join(format!(
    /// #         "despina-doc-{stem}-{}-{unique}.mat",
    /// #         std::process::id()
    /// #     ))
    /// # }
    /// # let source = MatrixBuilder::new(2)
    /// #     .table("DIST_AM", TypeCode::Float32)
    /// #     .build()?;
    /// # let path = temp_mat_path("matrix-open");
    /// # source.write_to(&path)?;
    /// let mat = despina::Matrix::open(&path)?;
    /// assert_eq!(mat.zone_count(), 2);
    /// # let _ = std::fs::remove_file(path);
    /// # Ok::<(), despina::Error>(())
    /// ```
    #[cfg(not(target_arch = "wasm32"))]
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let mut reader = ReaderBuilder::new().from_path(path)?;
        Self::from_reader(&mut reader)
    }

    /// Loads a matrix from a byte slice already in memory.
    ///
    /// This is useful when the `.mat` data has been read into memory by other
    /// means (for example, from an archive or embedded resource). The byte
    /// slice must contain a complete, valid `.mat` file including the header.
    ///
    /// For loading directly from a file path, use [`Matrix::open`] instead.
    ///
    /// # Errors
    ///
    /// Returns an error if the header is malformed or any row record is corrupt
    /// or truncated.
    ///
    /// # Example
    ///
    /// ```
    /// # use despina::{MatrixBuilder, TypeCode};
    /// # let mut source = MatrixBuilder::new(2)
    /// #     .table("DIST_AM", TypeCode::Float32)
    /// #     .build()?;
    /// # source.set(1, 1, 2, 42.0);
    /// # let mut bytes = Vec::new();
    /// # source.write_to_writer(&mut bytes)?;
    /// let mat = despina::Matrix::from_bytes(&bytes)?;
    /// assert_eq!(mat.get(1, 1, 2), 42.0);
    /// # Ok::<(), despina::Error>(())
    /// ```
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let mut reader = Reader::from_bytes(bytes)?;
        Self::from_reader(&mut reader)
    }

    /// Opens a `.mat` file from disk and loads only the named tables into
    /// memory.
    ///
    /// The resulting `Matrix` contains only the selected tables, renumbered
    /// 1..N in their original file order. Table order in the returned matrix
    /// follows the file, not the order of `table_names`.
    ///
    /// # Errors
    ///
    /// Returns [`ErrorKind::TableNotFound`] if any name is missing from the
    /// header, [`ErrorKind::InvalidTableCount`] if `table_names` is empty,
    /// or any I/O or format error from the underlying reader.
    ///
    /// # Example
    ///
    /// ```
    /// use despina::{MatrixBuilder, TypeCode};
    ///
    /// # fn temp_mat_path(stem: &str) -> std::path::PathBuf {
    /// #     let unique = std::time::SystemTime::now()
    /// #         .duration_since(std::time::UNIX_EPOCH)
    /// #         .unwrap()
    /// #         .as_nanos();
    /// #     std::env::temp_dir().join(format!(
    /// #         "despina-doc-{stem}-{}-{unique}.mat",
    /// #         std::process::id()
    /// #     ))
    /// # }
    /// # let mut source = MatrixBuilder::new(2)
    /// #     .table("DIST_AM", TypeCode::Float32)
    /// #     .table("TIME_AM", TypeCode::Float64)
    /// #     .build()?;
    /// # source.set(1, 1, 2, 5.0);
    /// # source.set(2, 2, 1, 10.0);
    /// # let path = temp_mat_path("open-tables");
    /// # source.write_to(&path)?;
    /// let mat = despina::Matrix::open_tables(&path, &["TIME_AM"])?;
    /// assert_eq!(mat.table_count(), 1);
    /// assert_eq!(mat.table("TIME_AM").get(2, 1), 10.0);
    /// # let _ = std::fs::remove_file(path);
    /// # Ok::<(), despina::Error>(())
    /// ```
    #[cfg(not(target_arch = "wasm32"))]
    pub fn open_tables<P: AsRef<Path>>(path: P, table_names: &[&str]) -> Result<Self> {
        let mut reader = ReaderBuilder::new().from_path(path)?;
        let selected_indices = resolve_table_names(reader.header(), table_names)?;
        Self::from_reader_selected(&mut reader, &selected_indices)
    }

    /// Loads only the named tables from a byte slice already in memory.
    ///
    /// The resulting `Matrix` contains only the selected tables, renumbered
    /// 1..N in their original file order.
    ///
    /// # Errors
    ///
    /// Returns [`ErrorKind::TableNotFound`] if any name is missing,
    /// [`ErrorKind::InvalidTableCount`] if `table_names` is empty, or any
    /// format error from the underlying reader.
    ///
    /// # Example
    ///
    /// ```
    /// use despina::{MatrixBuilder, TypeCode};
    ///
    /// # let mut source = MatrixBuilder::new(2)
    /// #     .table("DIST_AM", TypeCode::Float32)
    /// #     .table("TIME_AM", TypeCode::Float64)
    /// #     .build()?;
    /// # source.set(1, 1, 2, 7.0);
    /// # let mut bytes = Vec::new();
    /// # source.write_to_writer(&mut bytes)?;
    /// let mat = despina::Matrix::from_bytes_tables(&bytes, &["DIST_AM"])?;
    /// assert_eq!(mat.table_count(), 1);
    /// assert_eq!(mat.get_by_name("DIST_AM", 1, 2), 7.0);
    /// # Ok::<(), despina::Error>(())
    /// ```
    pub fn from_bytes_tables(bytes: &[u8], table_names: &[&str]) -> Result<Self> {
        let mut reader = Reader::from_bytes(bytes)?;
        let selected_indices = resolve_table_names(reader.header(), table_names)?;
        Self::from_reader_selected(&mut reader, &selected_indices)
    }

    /// Shared implementation: reads all rows from a reader into the flat vec.
    fn from_reader<R: Read>(reader: &mut Reader<R>) -> Result<Self> {
        let header = reader.header().clone();
        let zone_count = usize::from(header.zone_count());
        let table_count = usize::from(header.table_count());
        let cells_per_table = checked_cells_per_table(zone_count)?;
        let total_cells = checked_total_cells(cells_per_table, table_count)?;
        let mut data = vec![0.0f64; total_cells];

        let mut row = RowBuf::with_zone_count(header.zone_count());
        while reader.read_row(&mut row)? {
            let table_offset = usize::from(row.table_index() - 1) * cells_per_table;
            let row_offset = usize::from(row.row_index() - 1) * zone_count;
            let start = table_offset + row_offset;
            data[start..start + zone_count].copy_from_slice(row.values());
        }

        Ok(Self { header, data })
    }

    /// Reads only the selected tables from a reader into memory, producing a
    /// `Matrix` with a renumbered header.
    fn from_reader_selected<R: Read>(
        reader: &mut Reader<R>,
        selected_indices: &[u8],
    ) -> Result<Self> {
        let new_header = reader.header().with_selected_tables(selected_indices);
        let zone_count = usize::from(new_header.zone_count());
        let selected_table_count = usize::from(new_header.table_count());
        let cells_per_table = checked_cells_per_table(zone_count)?;
        let total_cells = checked_total_cells(cells_per_table, selected_table_count)?;
        let mut data = vec![0.0f64; total_cells];

        // Build remap: original table index -> new 1-based index (0 = not selected).
        let table_remap = build_table_remap(reader.header(), selected_indices);

        let selection = reader.prepare_selection(selected_indices)?;
        let mut row = RowBuf::with_zone_count(new_header.zone_count());
        while reader.read_selected_row(selection, &mut row)? {
            let new_table_index = table_remap[usize::from(row.table_index())];
            debug_assert!(new_table_index >= 1);
            let table_offset = usize::from(new_table_index - 1) * cells_per_table;
            let row_offset = usize::from(row.row_index() - 1) * zone_count;
            let start = table_offset + row_offset;
            data[start..start + zone_count].copy_from_slice(row.values());
        }

        Ok(Self {
            header: new_header,
            data,
        })
    }

    /// Returns a reference to the parsed file header.
    ///
    /// The header contains the banner text, run identifier, zone count, table
    /// count, and per-table metadata. See [`Header`] for the full set of
    /// accessors.
    #[inline]
    pub fn header(&self) -> &Header {
        &self.header
    }

    /// Returns the zone count (the square dimension of every table).
    ///
    /// In transport planning, zones represent geographic areas. The zone count
    /// determines the size of each origin-destination matrix: every table has
    /// `zone_count * zone_count` cells. This value is always at least 1.
    #[inline]
    pub fn zone_count(&self) -> u16 {
        self.header.zone_count()
    }

    /// Returns the number of tables (matrix layers) in this file.
    ///
    /// Each table represents a separate quantity (for example, trips, distance,
    /// or travel time) stored in the same `.mat` file. Tables share the same
    /// zone system but may use different on-disk storage type codes.
    #[inline]
    pub fn table_count(&self) -> u8 {
        self.header.table_count()
    }

    /// Returns the cell value at `(origin, destination)` in the table at
    /// 1-based `table_index`.
    ///
    /// # Panics
    ///
    /// Panics if `table_index`, `origin`, or `destination` is out of bounds (0,
    /// or exceeding the table count / zone count respectively).
    ///
    /// # Example
    ///
    /// ```
    /// use despina::{MatrixBuilder, TypeCode};
    ///
    /// let mut mat = MatrixBuilder::new(5)
    ///     .table("DIST_AM", TypeCode::Float32)
    ///     .build()?;
    ///
    /// mat.set(1, 1, 2, 100.0);
    /// assert_eq!(mat.get(1, 1, 2), 100.0);
    /// assert_eq!(mat.get(1, 1, 1), 0.0);
    /// # Ok::<(), despina::Error>(())
    /// ```
    pub fn get(&self, table_index: u8, origin: u16, destination: u16) -> f64 {
        let offset = self.checked_cell_offset(table_index, origin, destination);
        self.data[offset]
    }

    /// Returns the cell value if all indices are in bounds, or `None`
    /// otherwise.
    ///
    /// This is the non-panicking equivalent of [`Matrix::get`].
    ///
    /// # Example
    ///
    /// ```
    /// use despina::{MatrixBuilder, TypeCode};
    ///
    /// let mut mat = MatrixBuilder::new(2)
    ///     .table("DIST_AM", TypeCode::Float32)
    ///     .build()?;
    ///
    /// mat.set(1, 1, 2, 7.5);
    /// assert_eq!(mat.checked_get(1, 1, 2), Some(7.5));
    /// assert_eq!(mat.checked_get(1, 0, 2), None);
    /// assert_eq!(mat.checked_get(2, 1, 2), None);
    /// # Ok::<(), despina::Error>(())
    /// ```
    pub fn checked_get(&self, table_index: u8, origin: u16, destination: u16) -> Option<f64> {
        self.try_cell_offset(table_index, origin, destination)
            .map(|offset| self.data[offset])
    }

    /// Sets the cell value at `(origin, destination)` in the table at 1-based
    /// `table_index`.
    ///
    /// # Panics
    ///
    /// Panics if any index is out of bounds.
    ///
    /// # Example
    ///
    /// ```
    /// use despina::{MatrixBuilder, TypeCode};
    ///
    /// let mut mat = MatrixBuilder::new(3)
    ///     .table("DIST_AM", TypeCode::Float32)
    ///     .build()?;
    ///
    /// mat.set(1, 2, 3, 99.5);
    /// assert_eq!(mat.get(1, 2, 3), 99.5);
    /// # Ok::<(), despina::Error>(())
    /// ```
    pub fn set(&mut self, table_index: u8, origin: u16, destination: u16, value: f64) {
        let offset = self.checked_cell_offset(table_index, origin, destination);
        self.data[offset] = value;
    }

    /// Sets the cell value if all indices are in bounds, returning
    /// `Some(())` on success or `None` if any index is out of bounds.
    ///
    /// This is the non-panicking equivalent of [`Matrix::set`].
    ///
    /// # Example
    ///
    /// ```
    /// use despina::{MatrixBuilder, TypeCode};
    ///
    /// let mut mat = MatrixBuilder::new(2)
    ///     .table("DIST_AM", TypeCode::Float32)
    ///     .build()?;
    ///
    /// assert_eq!(mat.checked_set(1, 1, 2, 7.5), Some(()));
    /// assert_eq!(mat.get(1, 1, 2), 7.5);
    /// assert_eq!(mat.checked_set(1, 0, 2, 1.0), None);
    /// assert_eq!(mat.checked_set(2, 1, 2, 1.0), None);
    /// # Ok::<(), despina::Error>(())
    /// ```
    pub fn checked_set(
        &mut self,
        table_index: u8,
        origin: u16,
        destination: u16,
        value: f64,
    ) -> Option<()> {
        let offset = self.try_cell_offset(table_index, origin, destination)?;
        self.data[offset] = value;
        Some(())
    }

    /// Returns the cell value at `(origin, destination)` in the named table.
    ///
    /// This is a convenience wrapper around [`Matrix::get`] that resolves the
    /// table name to its 1-based index first.
    ///
    /// # Panics
    ///
    /// Panics if no table has the given name, or if `origin`/`destination`
    /// is out of bounds.
    pub fn get_by_name(&self, name: &str, origin: u16, destination: u16) -> f64 {
        let table_index = self.resolve_name(name);
        self.get(table_index, origin, destination)
    }

    /// Sets the cell value at `(origin, destination)` in the named table.
    ///
    /// This is a convenience wrapper around [`Matrix::set`] that resolves the
    /// table name to its 1-based index first.
    ///
    /// # Panics
    ///
    /// Panics if no table has the given name, or if `origin`/`destination`
    /// is out of bounds.
    pub fn set_by_name(&mut self, name: &str, origin: u16, destination: u16, value: f64) {
        let table_index = self.resolve_name(name);
        self.set(table_index, origin, destination, value);
    }

    /// Returns the row slice for `origin` in the table at 1-based
    /// `table_index`. The returned slice has `zone_count` entries, one per
    /// destination zone.
    ///
    /// # Panics
    ///
    /// Panics if `table_index` or `origin` is out of bounds.
    pub fn row(&self, table_index: u8, origin: u16) -> &[f64] {
        let zone_count = usize::from(self.zone_count());
        let start = self.checked_row_offset(table_index, origin);
        &self.data[start..start + zone_count]
    }

    /// Returns a mutable row slice for `origin` in the table at 1-based
    /// `table_index`. The returned slice has `zone_count` entries, one per
    /// destination zone.
    ///
    /// # Panics
    ///
    /// Panics if `table_index` or `origin` is out of bounds.
    pub fn row_mut(&mut self, table_index: u8, origin: u16) -> &mut [f64] {
        let zone_count = usize::from(self.zone_count());
        let start = self.checked_row_offset(table_index, origin);
        &mut self.data[start..start + zone_count]
    }

    /// Returns the flat data slice for the table at 1-based `table_index`.
    /// The slice has `zone_count * zone_count` entries in row-major order.
    ///
    /// # Panics
    ///
    /// Panics if `table_index` is out of bounds (0 or > table_count).
    pub fn table_data(&self, table_index: u8) -> &[f64] {
        let zone_count = usize::from(self.zone_count());
        let cells_per_table = zone_count * zone_count;
        let start = self.checked_table_offset(table_index);
        &self.data[start..start + cells_per_table]
    }

    /// Returns a mutable flat data slice for the table at 1-based
    /// `table_index`. The slice has `zone_count * zone_count` entries in
    /// row-major order.
    ///
    /// # Panics
    ///
    /// Panics if `table_index` is out of bounds.
    pub fn table_data_mut(&mut self, table_index: u8) -> &mut [f64] {
        let zone_count = usize::from(self.zone_count());
        let cells_per_table = zone_count * zone_count;
        let start = self.checked_table_offset(table_index);
        &mut self.data[start..start + cells_per_table]
    }

    /// Returns a borrowed [`Table`] view for the table with the given name.
    ///
    /// This is the primary way to access table data. The returned view borrows
    /// from this matrix and provides cell, row, and aggregate accessors.
    ///
    /// For a non-panicking alternative, use [`Matrix::try_table`].
    ///
    /// # Panics
    ///
    /// Panics if no table has the given name.
    ///
    /// # Example
    ///
    /// ```
    /// use despina::{MatrixBuilder, TypeCode};
    ///
    /// let mat = MatrixBuilder::new(3)
    ///     .table("DIST_AM", TypeCode::Float32)
    ///     .build()?;
    ///
    /// let dist_am = mat.table("DIST_AM");
    /// assert_eq!(dist_am.zone_count(), 3);
    /// assert_eq!(dist_am.total(), 0.0);
    /// # Ok::<(), despina::Error>(())
    /// ```
    pub fn table(&self, name: &str) -> Table<'_> {
        match self.try_table(name) {
            Some(t) => t,
            None => panic!("no table named \"{}\" in this matrix", name),
        }
    }

    /// Returns a mutable [`TableMut`] view for the table with the given name.
    ///
    /// The returned view borrows mutably from this matrix, allowing cell values
    /// to be modified in place.
    ///
    /// For a non-panicking alternative, use [`Matrix::try_table_mut`].
    ///
    /// # Panics
    ///
    /// Panics if no table has the given name.
    ///
    /// # Example
    ///
    /// ```
    /// use despina::{MatrixBuilder, TypeCode};
    ///
    /// let mut mat = MatrixBuilder::new(2)
    ///     .table("DIST_AM", TypeCode::Float32)
    ///     .build()?;
    ///
    /// mat.table_mut("DIST_AM").fill(5.0);
    /// assert_eq!(mat.get(1, 1, 1), 5.0);
    /// # Ok::<(), despina::Error>(())
    /// ```
    pub fn table_mut(&mut self, name: &str) -> TableMut<'_> {
        match self.try_table_mut(name) {
            Some(t) => t,
            None => panic!("no table named \"{}\" in this matrix", name),
        }
    }

    /// Returns a borrowed [`Table`] view if a table with the given name exists,
    /// or `None` otherwise.
    ///
    /// This is the non-panicking equivalent of [`Matrix::table`].
    ///
    /// # Example
    ///
    /// ```
    /// use despina::{MatrixBuilder, TypeCode};
    ///
    /// let mat = MatrixBuilder::new(2)
    ///     .table("DIST_AM", TypeCode::Float32)
    ///     .build()?;
    ///
    /// assert!(mat.try_table("DIST_AM").is_some());
    /// assert!(mat.try_table("TIME_AM").is_none());
    /// # Ok::<(), despina::Error>(())
    /// ```
    pub fn try_table(&self, name: &str) -> Option<Table<'_>> {
        let index = self.header.table_index_by_name(name)?;
        Some(self.make_table(index))
    }

    /// Returns a mutable [`TableMut`] view if a table with the given name
    /// exists, or `None` otherwise.
    ///
    /// This is the non-panicking equivalent of [`Matrix::table_mut`].
    ///
    /// # Example
    ///
    /// ```
    /// use despina::{MatrixBuilder, TypeCode};
    ///
    /// let mut mat = MatrixBuilder::new(2)
    ///     .table("DIST_AM", TypeCode::Float32)
    ///     .build()?;
    ///
    /// if let Some(mut table) = mat.try_table_mut("DIST_AM") {
    ///     table.fill(2.0);
    /// }
    /// assert!(mat.try_table_mut("TIME_AM").is_none());
    /// assert_eq!(mat.get(1, 1, 1), 2.0);
    /// # Ok::<(), despina::Error>(())
    /// ```
    pub fn try_table_mut(&mut self, name: &str) -> Option<TableMut<'_>> {
        let index = self.header.table_index_by_name(name)?;
        Some(self.make_table_mut(index))
    }

    /// Returns a borrowed [`Table`] view for the 1-based table index.
    ///
    /// Use this when you already know the numeric index rather than the table
    /// name.
    ///
    /// For a non-panicking alternative, use [`Matrix::try_table_by_index`].
    ///
    /// # Panics
    ///
    /// Panics if `index` is out of bounds (0 or > table_count).
    ///
    /// # Example
    ///
    /// ```
    /// use despina::{MatrixBuilder, TypeCode};
    ///
    /// let mat = MatrixBuilder::new(2)
    ///     .table("DIST_AM", TypeCode::Float32)
    ///     .table("TIME_AM", TypeCode::Float64)
    ///     .build()?;
    ///
    /// assert_eq!(mat.table_by_index(1).name(), "DIST_AM");
    /// assert_eq!(mat.table_by_index(2).name(), "TIME_AM");
    /// # Ok::<(), despina::Error>(())
    /// ```
    pub fn table_by_index(&self, index: u8) -> Table<'_> {
        match self.try_table_by_index(index) {
            Some(t) => t,
            None => panic!(
                "table index {} is out of bounds for table count {} (tables are 1-based)",
                index,
                self.table_count()
            ),
        }
    }

    /// Returns a mutable [`TableMut`] view for the 1-based table index.
    ///
    /// Use this when you already know the numeric index rather than the table
    /// name.
    ///
    /// For a non-panicking alternative, use [`Matrix::try_table_by_index_mut`].
    ///
    /// # Panics
    ///
    /// Panics if `index` is out of bounds (0 or > table_count).
    ///
    /// # Example
    ///
    /// ```
    /// use despina::{MatrixBuilder, TypeCode};
    ///
    /// let mut mat = MatrixBuilder::new(2)
    ///     .table("DIST_AM", TypeCode::Float32)
    ///     .table("TIME_AM", TypeCode::Float64)
    ///     .build()?;
    ///
    /// mat.table_by_index_mut(2).fill(1.0);
    /// assert_eq!(mat.table("TIME_AM").total(), 4.0);
    /// # Ok::<(), despina::Error>(())
    /// ```
    pub fn table_by_index_mut(&mut self, index: u8) -> TableMut<'_> {
        let table_count = self.table_count();
        match self.try_table_by_index_mut(index) {
            Some(t) => t,
            None => panic!(
                "table index {} is out of bounds for table count {} (tables are 1-based)",
                index, table_count
            ),
        }
    }

    /// Returns a borrowed [`Table`] view for the 1-based table index, or
    /// `None` if the index is out of bounds.
    ///
    /// This is the non-panicking equivalent of [`Matrix::table_by_index`].
    ///
    /// # Example
    ///
    /// ```
    /// use despina::{MatrixBuilder, TypeCode};
    ///
    /// let mat = MatrixBuilder::new(2)
    ///     .table("DIST_AM", TypeCode::Float32)
    ///     .build()?;
    ///
    /// assert_eq!(
    ///     mat.try_table_by_index(1)
    ///         .map(|table| table.name().to_owned()),
    ///     Some("DIST_AM".to_owned())
    /// );
    /// assert!(mat.try_table_by_index(2).is_none());
    /// # Ok::<(), despina::Error>(())
    /// ```
    pub fn try_table_by_index(&self, index: u8) -> Option<Table<'_>> {
        if index < 1 || index > self.table_count() {
            return None;
        }
        Some(self.make_table(index))
    }

    /// Returns a mutable [`TableMut`] view for the 1-based table index, or
    /// `None` if the index is out of bounds.
    ///
    /// This is the non-panicking equivalent of
    /// [`Matrix::table_by_index_mut`].
    ///
    /// # Example
    ///
    /// ```
    /// use despina::{MatrixBuilder, TypeCode};
    ///
    /// let mut mat = MatrixBuilder::new(2)
    ///     .table("DIST_AM", TypeCode::Float32)
    ///     .build()?;
    ///
    /// if let Some(mut table) = mat.try_table_by_index_mut(1) {
    ///     table.fill(3.0);
    /// }
    /// assert!(mat.try_table_by_index_mut(2).is_none());
    /// assert_eq!(mat.get(1, 2, 2), 3.0);
    /// # Ok::<(), despina::Error>(())
    /// ```
    pub fn try_table_by_index_mut(&mut self, index: u8) -> Option<TableMut<'_>> {
        if index < 1 || index > self.table_count() {
            return None;
        }
        Some(self.make_table_mut(index))
    }

    /// Returns an iterator over all tables as borrowed [`Table`] views, in
    /// table-index order (table 1 first).
    ///
    /// # Example
    ///
    /// ```
    /// use despina::{MatrixBuilder, TypeCode};
    ///
    /// let mat = MatrixBuilder::new(2)
    ///     .table("DIST_AM", TypeCode::Float32)
    ///     .table("DIST_PM", TypeCode::Float64)
    ///     .build()?;
    ///
    /// let names: Vec<String> = mat.tables().map(|table| table.name().to_owned()).collect();
    /// assert_eq!(names, vec!["DIST_AM", "DIST_PM"]);
    /// # Ok::<(), despina::Error>(())
    /// ```
    pub fn tables(&self) -> impl Iterator<Item = Table<'_>> {
        let table_count = self.table_count();
        (1..=table_count).map(move |i| self.make_table(i))
    }

    /// Creates a [`MatrixBuilder`] for constructing a new matrix from scratch.
    ///
    /// This is a convenience shorthand for [`MatrixBuilder::new`]. For
    /// user-supplied dimensions that should not panic, use
    /// [`MatrixBuilder::try_new`].
    ///
    /// # Panics
    ///
    /// Panics if `zone_count` is 0 or exceeds 32,000.
    pub fn builder(zone_count: u16) -> MatrixBuilder {
        MatrixBuilder::new(zone_count)
    }

    /// Writes this matrix to a file at the given path.
    ///
    /// Creates (or truncates) the file and writes a conforming `.mat` file
    /// containing all tables and all rows. The header metadata (banner, run
    /// ID, table names and type codes) is preserved from when this matrix was
    /// originally loaded or built.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be created or an I/O error occurs
    /// during writing.
    ///
    /// # Example
    ///
    /// ```
    /// # use despina::{Matrix, MatrixBuilder, TypeCode};
    /// # fn temp_mat_path(stem: &str) -> std::path::PathBuf {
    /// #     let unique = std::time::SystemTime::now()
    /// #         .duration_since(std::time::UNIX_EPOCH)
    /// #         .unwrap()
    /// #         .as_nanos();
    /// #     std::env::temp_dir().join(format!(
    /// #         "despina-doc-{stem}-{}-{unique}.mat",
    /// #         std::process::id()
    /// #     ))
    /// # }
    ///
    /// let mut mat = MatrixBuilder::new(2)
    ///     .table("DIST_AM", TypeCode::Float32)
    ///     .build()?;
    /// mat.set(1, 1, 2, 9.0);
    ///
    /// let path = temp_mat_path("matrix-write-to");
    /// mat.write_to(&path)?;
    /// let round_trip = Matrix::open(&path)?;
    /// assert_eq!(round_trip.get(1, 1, 2), 9.0);
    /// let _ = std::fs::remove_file(path);
    /// # Ok::<(), despina::Error>(())
    /// ```
    #[cfg(not(target_arch = "wasm32"))]
    pub fn write_to<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let file = std::fs::File::create(path)?;
        self.write_to_writer(file)
    }

    /// Writes this matrix to an arbitrary writer.
    ///
    /// Writes a complete conforming `.mat` file to `writer`, including the
    /// five-record header and all row data. The header metadata (banner, run
    /// ID, table names and type codes) is preserved from when this matrix was
    /// originally loaded or built.
    ///
    /// # Errors
    ///
    /// Returns an error if an I/O error occurs during writing.
    ///
    /// # Example
    ///
    /// ```
    /// use despina::{Matrix, MatrixBuilder, TypeCode};
    ///
    /// let mut mat = MatrixBuilder::new(2)
    ///     .table("DIST_AM", TypeCode::Float32)
    ///     .table("TIME_AM", TypeCode::Float64)
    ///     .build()?;
    /// mat.set(1, 1, 2, 7.0);
    /// mat.set(2, 2, 1, 11.0);
    ///
    /// let mut bytes = Vec::new();
    /// mat.write_to_writer(&mut bytes)?;
    ///
    /// let round_trip = Matrix::from_bytes(&bytes)?;
    /// assert_eq!(round_trip.get_by_name("DIST_AM", 1, 2), 7.0);
    /// assert_eq!(round_trip.get_by_name("TIME_AM", 2, 1), 11.0);
    /// # Ok::<(), despina::Error>(())
    /// ```
    pub fn write_to_writer<W: std::io::Write>(&self, writer: W) -> Result<()> {
        let header = self.header();
        let table_infos = header.tables();
        let zone_count = usize::from(header.zone_count());
        let table_count = table_infos.len();
        let cells_per_table = checked_cells_per_table(zone_count)?;

        let mut tables = Vec::with_capacity(table_count);
        for info in table_infos {
            tables.push(TableDef::new(info.name(), info.type_code()));
        }

        let mut writer = crate::writer::WriterBuilder::new()
            .banner(header.banner())
            .run_id(header.run_id())
            .open_writer(writer, header.zone_count(), &tables)?;

        for row_offset in (0..cells_per_table).step_by(zone_count) {
            let mut start = row_offset;
            for _ in 0..table_count {
                writer.write_next_row(&self.data[start..start + zone_count])?;
                start += cells_per_table;
            }
        }

        writer.finish()?;
        Ok(())
    }

    /// Constructs a `Matrix` from a pre-built header and flat data buffer
    /// without validating the data length.
    pub(crate) fn from_parts_unchecked(header: Header, data: Vec<f64>) -> Self {
        Self { header, data }
    }

    /// Constructs a `Matrix` from a header and a flat data buffer, validating
    /// that the buffer length matches the header dimensions.
    ///
    /// The data must be laid out in table-major, row-major (C-order) layout:
    /// all cells of table 1 first, then table 2, and so on. Within each table,
    /// cells are stored row by row (origin 1 destinations first, then origin 2,
    /// etc.). This is the same layout used by [`data`](Matrix::data) and
    /// [`into_parts`](Matrix::into_parts).
    ///
    /// # Errors
    ///
    /// Returns [`ErrorKind::ShapeMismatch`] if
    /// `data.len() != table_count * zone_count * zone_count`.
    ///
    /// # Example
    ///
    /// ```
    /// use despina::{Matrix, MatrixBuilder, TypeCode};
    ///
    /// let header = MatrixBuilder::new(2)
    ///     .table("DIST", TypeCode::Float32)
    ///     .validate()?;
    ///
    /// let data = vec![1.0, 2.0, 3.0, 4.0]; // 1 table * 2 zones * 2 zones
    /// let matrix = Matrix::from_parts(header, data)?;
    /// assert_eq!(matrix.get(1, 1, 1), 1.0);
    /// assert_eq!(matrix.get(1, 2, 2), 4.0);
    /// # Ok::<(), despina::Error>(())
    /// ```
    pub fn from_parts(header: Header, data: Vec<f64>) -> Result<Self> {
        let zone_count = usize::from(header.zone_count());
        let table_count = usize::from(header.table_count());
        let cells_per_table = checked_cells_per_table(zone_count)?;
        let expected = checked_total_cells(cells_per_table, table_count)?;
        if data.len() != expected {
            return Err(Error::new(ErrorKind::ShapeMismatch {
                context: "from_parts",
                expected,
                got: data.len(),
            }));
        }
        Ok(Self { header, data })
    }

    /// Returns a reference to the full contiguous data buffer.
    ///
    /// The data is laid out in table-major, row-major (C-order) layout:
    /// `table_count * zone_count * zone_count` elements. This enables
    /// zero-copy 3D array views, e.g. via `ndarray::ArrayView3::from_shape`.
    ///
    /// # Example
    ///
    /// ```
    /// use despina::{MatrixBuilder, TypeCode};
    ///
    /// let mut mat = MatrixBuilder::new(2)
    ///     .table("DIST", TypeCode::Float32)
    ///     .build()?;
    /// mat.set(1, 1, 2, 42.0);
    ///
    /// let buf = mat.data();
    /// assert_eq!(buf.len(), 4); // 1 table * 2 zones * 2 zones
    /// assert_eq!(buf[1], 42.0); // origin 1, destination 2
    /// # Ok::<(), despina::Error>(())
    /// ```
    #[inline]
    pub fn data(&self) -> &[f64] {
        &self.data
    }

    /// Returns a mutable reference to the full contiguous data buffer.
    ///
    /// The layout is the same as described in [`data`](Matrix::data). This
    /// enables bulk writes from external computations via `copy_from_slice`.
    ///
    /// # Example
    ///
    /// ```
    /// use despina::{MatrixBuilder, TypeCode};
    ///
    /// let mut mat = MatrixBuilder::new(2)
    ///     .table("DIST", TypeCode::Float32)
    ///     .build()?;
    ///
    /// mat.data_mut().copy_from_slice(&[1.0, 2.0, 3.0, 4.0]);
    /// assert_eq!(mat.get(1, 1, 1), 1.0);
    /// assert_eq!(mat.get(1, 2, 2), 4.0);
    /// # Ok::<(), despina::Error>(())
    /// ```
    #[inline]
    pub fn data_mut(&mut self) -> &mut [f64] {
        &mut self.data
    }

    /// Consumes the matrix and returns the header and owned data buffer.
    ///
    /// This enables zero-copy ownership transfer to array libraries, e.g.
    /// `ndarray::Array3::from_shape_vec((T, Z, Z), data)`.
    ///
    /// The data layout is the same as described in [`data`](Matrix::data).
    /// Use [`from_parts`](Matrix::from_parts) to reconstruct a `Matrix` from
    /// a header and data buffer.
    ///
    /// # Example
    ///
    /// ```
    /// use despina::{MatrixBuilder, TypeCode};
    ///
    /// let mut mat = MatrixBuilder::new(2)
    ///     .table("DIST", TypeCode::Float32)
    ///     .build()?;
    /// mat.set(1, 1, 2, 42.0);
    ///
    /// let (header, data) = mat.into_parts();
    /// assert_eq!(header.zone_count(), 2);
    /// assert_eq!(data[1], 42.0);
    /// # Ok::<(), despina::Error>(())
    /// ```
    pub fn into_parts(self) -> (Header, Vec<f64>) {
        (self.header, self.data)
    }

    /// Returns the 1-based table index for `name`, panicking if not found.
    fn resolve_name(&self, name: &str) -> u8 {
        self.header
            .table_index_by_name(name)
            .unwrap_or_else(|| panic!("no table named \"{}\" in this matrix", name))
    }

    /// Returns the flat-data index of the first cell in `table_index`,
    /// panicking if out of bounds.
    fn checked_table_offset(&self, table_index: u8) -> usize {
        assert_table_index_in_bounds(table_index, self.table_count());
        let zone_count = usize::from(self.zone_count());
        usize::from(table_index - 1) * zone_count * zone_count
    }

    /// Returns the flat-data index of the first cell in `(table_index,
    /// origin)`, panicking if either is out of bounds.
    fn checked_row_offset(&self, table_index: u8, origin: u16) -> usize {
        assert_table_index_in_bounds(table_index, self.table_count());
        assert_origin_in_bounds(origin, self.zone_count());
        let zone_count = usize::from(self.zone_count());
        let table_offset = usize::from(table_index - 1) * zone_count * zone_count;
        let row_offset = usize::from(origin - 1) * zone_count;
        table_offset + row_offset
    }

    /// Returns the flat-data index for `(table_index, origin, destination)`,
    /// panicking if any index is out of bounds.
    fn checked_cell_offset(&self, table_index: u8, origin: u16, destination: u16) -> usize {
        let table_offset = self.checked_table_offset(table_index);
        table_offset + checked_cell_index(origin, destination, self.zone_count())
    }

    /// Returns the flat-data index for `(table_index, origin, destination)`, or
    /// `None` if any index is out of bounds.
    fn try_cell_offset(&self, table_index: u8, origin: u16, destination: u16) -> Option<usize> {
        let table_count = self.table_count();
        if table_index < 1 || table_index > table_count {
            return None;
        }
        let zone_count = self.zone_count();
        let cell_index = try_cell_index(origin, destination, zone_count)?;
        let zone_count = usize::from(zone_count);
        let table_offset = usize::from(table_index - 1) * zone_count * zone_count;
        Some(table_offset + cell_index)
    }

    /// Constructs a [`Table`] view for 1-based `index`. Caller must have
    /// already verified bounds.
    fn make_table(&self, index: u8) -> Table<'_> {
        let info = &self.header.tables()[usize::from(index - 1)];
        let zone_count = usize::from(self.zone_count());
        let cells_per_table = zone_count * zone_count;
        let start = usize::from(index - 1) * cells_per_table;
        Table {
            info,
            data: &self.data[start..start + cells_per_table],
            zone_count: self.zone_count(),
        }
    }

    /// Constructs a [`TableMut`] view for 1-based `index`. Caller must have
    /// already verified bounds.
    fn make_table_mut(&mut self, index: u8) -> TableMut<'_> {
        let zone_count = self.zone_count();
        let cells_per_table = usize::from(zone_count) * usize::from(zone_count);
        let start = usize::from(index - 1) * cells_per_table;
        // Disjoint field borrows: `info` from `self.header`, `data` from
        // `self.data`. The compiler can verify these do not overlap.
        let info = &self.header.tables()[usize::from(index - 1)];
        let data = &mut self.data[start..start + cells_per_table];
        TableMut {
            info,
            data,
            zone_count,
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
/// Convenience function equivalent to [`Matrix::open`].
///
/// Opens a `.mat` file from disk and loads all tables into memory. This is the
/// simplest entry point for reading matrix files.
///
/// # Errors
///
/// Returns an error if the file cannot be opened, the header is malformed, or
/// any row record is corrupt or truncated.
///
/// # Example
///
/// ```
/// # use despina::{MatrixBuilder, TypeCode};
/// # fn temp_mat_path(stem: &str) -> std::path::PathBuf {
/// #     let unique = std::time::SystemTime::now()
/// #         .duration_since(std::time::UNIX_EPOCH)
/// #         .unwrap()
/// #         .as_nanos();
/// #     std::env::temp_dir().join(format!(
/// #         "despina-doc-{stem}-{}-{unique}.mat",
/// #         std::process::id()
/// #     ))
/// # }
/// # let source = MatrixBuilder::new(2)
/// #     .table("DIST_AM", TypeCode::Float32)
/// #     .build()?;
/// # let path = temp_mat_path("open-fn");
/// # source.write_to(&path)?;
/// let mat = despina::open(&path)?;
/// assert_eq!(mat.zone_count(), 2);
/// # let _ = std::fs::remove_file(path);
/// # Ok::<(), despina::Error>(())
/// ```
pub fn open<P: AsRef<Path>>(path: P) -> Result<Matrix> {
    Matrix::open(path)
}

#[cfg(not(target_arch = "wasm32"))]
/// Convenience function equivalent to [`Matrix::open_tables`].
///
/// Opens a `.mat` file from disk and loads only the named tables into memory.
/// The resulting `Matrix` contains only the selected tables, renumbered 1..N
/// in their original file order.
///
/// # Errors
///
/// Returns [`ErrorKind::TableNotFound`] if any name is missing,
/// [`ErrorKind::InvalidTableCount`] if `table_names` is empty, or any I/O or
/// format error from the underlying reader.
///
/// # Example
///
/// ```
/// # use despina::{MatrixBuilder, TypeCode};
/// # fn temp_mat_path(stem: &str) -> std::path::PathBuf {
/// #     let unique = std::time::SystemTime::now()
/// #         .duration_since(std::time::UNIX_EPOCH)
/// #         .unwrap()
/// #         .as_nanos();
/// #     std::env::temp_dir().join(format!(
/// #         "despina-doc-{stem}-{}-{unique}.mat",
/// #         std::process::id()
/// #     ))
/// # }
/// # let mut source = MatrixBuilder::new(2)
/// #     .table("DIST_AM", TypeCode::Float32)
/// #     .table("TIME_AM", TypeCode::Float64)
/// #     .build()?;
/// # source.set(2, 1, 2, 3.0);
/// # let path = temp_mat_path("open-tables-fn");
/// # source.write_to(&path)?;
/// let mat = despina::open_tables(&path, &["TIME_AM"])?;
/// assert_eq!(mat.table_count(), 1);
/// assert_eq!(mat.table("TIME_AM").get(1, 2), 3.0);
/// # let _ = std::fs::remove_file(path);
/// # Ok::<(), despina::Error>(())
/// ```
pub fn open_tables<P: AsRef<Path>>(path: P, table_names: &[&str]) -> Result<Matrix> {
    Matrix::open_tables(path, table_names)
}

/// A borrowed, read-only view of a single table within a [`Matrix`].
///
/// `Table` is a lightweight, zero-allocation view that borrows from the parent
/// matrix. It is [`Copy`], so it can be freely duplicated without cloning any
/// data.
///
/// Obtained via [`Matrix::table`], [`Matrix::try_table`],
/// [`Matrix::table_by_index`], or [`Matrix::tables`]. The view provides
/// cell-level accessors ([`get`](Table::get),
/// [`checked_get`](Table::checked_get)), row slicing ([`row`](Table::row),
/// [`rows`](Table::rows)), and aggregate operations ([`total`](Table::total),
/// [`diagonal_total`](Table::diagonal_total)).
///
/// All index arguments are **1-based**.
///
/// # Example
///
/// ```
/// use despina::{MatrixBuilder, TypeCode};
///
/// let mut mat = MatrixBuilder::new(3)
///     .table("DIST_AM", TypeCode::Float32)
///     .build()?;
///
/// mat.set(1, 1, 1, 10.0);
/// mat.set(1, 2, 2, 20.0);
///
/// let dist_am = mat.table("DIST_AM");
/// assert_eq!(dist_am.get(1, 1), 10.0);
/// assert_eq!(dist_am.total(), 30.0);
/// assert_eq!(dist_am.diagonal_total(), 30.0);
/// # Ok::<(), despina::Error>(())
/// ```
#[derive(Debug, Clone, Copy)]
pub struct Table<'a> {
    info: &'a TableInfo,
    data: &'a [f64],
    zone_count: u16,
}

impl<'a> Table<'a> {
    /// Returns the [`TableInfo`] metadata for this table, including its name,
    /// type code, and 1-based index.
    #[inline]
    pub fn info(&self) -> &TableInfo {
        self.info
    }

    #[inline]
    pub fn name(&self) -> &str {
        self.info.name()
    }

    #[inline]
    pub fn type_code(&self) -> TypeCode {
        self.info.type_code()
    }

    #[inline]
    pub fn index(&self) -> u8 {
        self.info.index()
    }

    #[inline]
    pub fn zone_count(&self) -> u16 {
        self.zone_count
    }

    /// Returns the cell value at 1-based `(origin, destination)`.
    ///
    /// # Panics
    ///
    /// Panics if `origin` or `destination` is out of bounds.
    ///
    /// # Example
    ///
    /// ```
    /// use despina::{MatrixBuilder, TypeCode};
    ///
    /// let mut mat = MatrixBuilder::new(3)
    ///     .table("DIST_AM", TypeCode::Float32)
    ///     .build()?;
    ///
    /// mat.set(1, 2, 3, 42.0);
    /// assert_eq!(mat.table("DIST_AM").get(2, 3), 42.0);
    /// # Ok::<(), despina::Error>(())
    /// ```
    pub fn get(&self, origin: u16, destination: u16) -> f64 {
        let index = checked_cell_index(origin, destination, self.zone_count);
        self.data[index]
    }

    /// Returns the cell value if indices are in bounds, or `None` otherwise.
    ///
    /// This is the non-panicking equivalent of [`Table::get`].
    pub fn checked_get(&self, origin: u16, destination: u16) -> Option<f64> {
        try_cell_index(origin, destination, self.zone_count).map(|i| self.data[i])
    }

    /// Returns the row slice for 1-based `origin`. The returned slice has
    /// `zone_count` entries, one per destination zone.
    ///
    /// # Panics
    ///
    /// Panics if `origin` is out of bounds.
    ///
    /// # Example
    ///
    /// ```
    /// use despina::{MatrixBuilder, TypeCode};
    ///
    /// let mut mat = MatrixBuilder::new(3)
    ///     .table("DIST_AM", TypeCode::Float32)
    ///     .build()?;
    ///
    /// mat.set(1, 1, 2, 5.0);
    /// assert_eq!(mat.table("DIST_AM").row(1), &[0.0, 5.0, 0.0]);
    /// # Ok::<(), despina::Error>(())
    /// ```
    pub fn row(&self, origin: u16) -> &[f64] {
        assert_origin_in_bounds(origin, self.zone_count);
        let zone_count = usize::from(self.zone_count);
        let start = usize::from(origin - 1) * zone_count;
        &self.data[start..start + zone_count]
    }

    /// Returns the flat data slice for this table (`zone_count * zone_count`
    /// entries in row-major order).
    #[inline]
    pub fn as_slice(&self) -> &[f64] {
        self.data
    }

    /// Returns an iterator over rows. Each item is a `&[f64]` slice of length
    /// `zone_count`, representing one origin's destinations.
    pub fn rows(&self) -> impl Iterator<Item = &[f64]> {
        self.data.chunks_exact(usize::from(self.zone_count))
    }

    /// Returns the sum of all cell values in this table.
    ///
    /// This is useful for validating matrix data against known control totals.
    ///
    /// # Example
    ///
    /// ```
    /// use despina::{MatrixBuilder, TypeCode};
    ///
    /// let mut mat = MatrixBuilder::new(2)
    ///     .table("DIST_AM", TypeCode::Float32)
    ///     .build()?;
    ///
    /// mat.set(1, 1, 1, 10.0);
    /// mat.set(1, 2, 2, 20.0);
    /// assert_eq!(mat.table("DIST_AM").total(), 30.0);
    /// # Ok::<(), despina::Error>(())
    /// ```
    #[inline]
    pub fn total(&self) -> f64 {
        self.data.iter().sum()
    }

    /// Returns the sum of diagonal cells (where origin equals destination).
    ///
    /// The diagonal total (also called the trace) represents intra-zonal flows
    /// in transport planning: trips that start and end in the same zone.
    ///
    /// # Example
    ///
    /// ```
    /// use despina::{MatrixBuilder, TypeCode};
    ///
    /// let mut mat = MatrixBuilder::new(3)
    ///     .table("DIST_AM", TypeCode::Float32)
    ///     .build()?;
    ///
    /// mat.set(1, 1, 1, 1.0);
    /// mat.set(1, 2, 2, 2.0);
    /// mat.set(1, 3, 3, 3.0);
    /// mat.set(1, 1, 2, 99.0); // off-diagonal, not included
    /// assert_eq!(mat.table("DIST_AM").diagonal_total(), 6.0);
    /// # Ok::<(), despina::Error>(())
    /// ```
    #[inline]
    pub fn diagonal_total(&self) -> f64 {
        let zone_count = usize::from(self.zone_count);
        if zone_count == 0 {
            return 0.0;
        }
        self.data.iter().step_by(zone_count + 1).sum()
    }
}

/// A borrowed, mutable view of a single table within a [`Matrix`].
///
/// `TableMut` is the mutable counterpart to [`Table`]. It provides all the same
/// read accessors, plus mutation operations: [`set`](TableMut::set) for
/// individual cells, [`row_mut`](TableMut::row_mut) for mutable row slices, and
/// [`fill`](TableMut::fill) for bulk initialisation.
///
/// Obtained via [`Matrix::table_mut`], [`Matrix::try_table_mut`], or
/// [`Matrix::table_by_index_mut`]. The view borrows mutably from the parent
/// matrix and carries no allocation of its own. Unlike [`Table`], `TableMut` is
/// not `Copy` (mutable references cannot be duplicated).
///
/// All index arguments are **1-based**.
///
/// # Example
///
/// ```
/// use despina::{MatrixBuilder, TypeCode};
///
/// let mut mat = MatrixBuilder::new(2)
///     .table("DIST_AM", TypeCode::Float32)
///     .build()?;
///
/// {
///     let mut dist_am = mat.table_mut("DIST_AM");
///     dist_am.set(1, 1, 10.0);
///     dist_am.set(2, 2, 20.0);
///     dist_am.row_mut(1)[1] = 5.0;
/// }
///
/// assert_eq!(mat.get(1, 1, 1), 10.0);
/// assert_eq!(mat.get(1, 1, 2), 5.0);
/// # Ok::<(), despina::Error>(())
/// ```
#[derive(Debug)]
pub struct TableMut<'a> {
    info: &'a TableInfo,
    data: &'a mut [f64],
    zone_count: u16,
}

impl<'a> TableMut<'a> {
    /// Returns the [`TableInfo`] metadata for this table.
    #[inline]
    pub fn info(&self) -> &TableInfo {
        self.info
    }

    #[inline]
    pub fn name(&self) -> &str {
        self.info.name()
    }

    #[inline]
    pub fn type_code(&self) -> TypeCode {
        self.info.type_code()
    }

    #[inline]
    pub fn index(&self) -> u8 {
        self.info.index()
    }

    #[inline]
    pub fn zone_count(&self) -> u16 {
        self.zone_count
    }

    /// Returns the cell value at 1-based `(origin, destination)`.
    ///
    /// # Panics
    ///
    /// Panics if `origin` or `destination` is out of bounds.
    pub fn get(&self, origin: u16, destination: u16) -> f64 {
        let index = checked_cell_index(origin, destination, self.zone_count);
        self.data[index]
    }

    /// Returns the cell value if indices are in bounds, or `None` otherwise.
    ///
    /// This is the non-panicking equivalent of [`TableMut::get`].
    pub fn checked_get(&self, origin: u16, destination: u16) -> Option<f64> {
        try_cell_index(origin, destination, self.zone_count).map(|i| self.data[i])
    }

    /// Sets the cell value at 1-based `(origin, destination)`.
    ///
    /// # Panics
    ///
    /// Panics if `origin` or `destination` is out of bounds.
    ///
    /// # Example
    ///
    /// ```
    /// use despina::{MatrixBuilder, TypeCode};
    ///
    /// let mut mat = MatrixBuilder::new(2)
    ///     .table("DIST_AM", TypeCode::Float32)
    ///     .build()?;
    ///
    /// mat.table_mut("DIST_AM").set(1, 2, 99.0);
    /// assert_eq!(mat.get(1, 1, 2), 99.0);
    /// # Ok::<(), despina::Error>(())
    /// ```
    pub fn set(&mut self, origin: u16, destination: u16, value: f64) {
        let index = checked_cell_index(origin, destination, self.zone_count);
        self.data[index] = value;
    }

    /// Returns the row slice for 1-based `origin`. The returned slice has
    /// `zone_count` entries, one per destination zone.
    ///
    /// # Panics
    ///
    /// Panics if `origin` is out of bounds.
    pub fn row(&self, origin: u16) -> &[f64] {
        assert_origin_in_bounds(origin, self.zone_count);
        let zone_count = usize::from(self.zone_count);
        let start = usize::from(origin - 1) * zone_count;
        &self.data[start..start + zone_count]
    }

    /// Returns a mutable row slice for 1-based `origin`. The returned slice
    /// has `zone_count` entries, one per destination zone.
    ///
    /// # Panics
    ///
    /// Panics if `origin` is out of bounds.
    pub fn row_mut(&mut self, origin: u16) -> &mut [f64] {
        assert_origin_in_bounds(origin, self.zone_count);
        let zone_count = usize::from(self.zone_count);
        let start = usize::from(origin - 1) * zone_count;
        &mut self.data[start..start + zone_count]
    }

    /// Returns the flat data slice for this table (`zone_count * zone_count`
    /// entries in row-major order).
    #[inline]
    pub fn as_slice(&self) -> &[f64] {
        self.data
    }

    /// Returns a mutable flat data slice for this table.
    #[inline]
    pub fn as_mut_slice(&mut self) -> &mut [f64] {
        self.data
    }

    /// Fills all cells in this table with the given value.
    ///
    /// # Example
    ///
    /// ```
    /// use despina::{MatrixBuilder, TypeCode};
    ///
    /// let mut mat = MatrixBuilder::new(2)
    ///     .table("DIST_AM", TypeCode::Float32)
    ///     .build()?;
    ///
    /// mat.table_mut("DIST_AM").fill(7.0);
    /// assert_eq!(mat.get(1, 1, 1), 7.0);
    /// assert_eq!(mat.get(1, 2, 2), 7.0);
    /// # Ok::<(), despina::Error>(())
    /// ```
    pub fn fill(&mut self, value: f64) {
        self.data.fill(value);
    }

    /// Returns an iterator over rows. Each item is a `&[f64]` slice of length
    /// `zone_count`.
    pub fn rows(&self) -> impl Iterator<Item = &[f64]> {
        self.data.chunks_exact(usize::from(self.zone_count))
    }

    /// Returns the sum of all cell values in this table.
    #[inline]
    pub fn total(&self) -> f64 {
        self.data.iter().sum()
    }

    /// Returns the sum of diagonal cells (where origin equals destination).
    ///
    /// See [`Table::diagonal_total`] for details on what the diagonal total
    /// represents.
    #[inline]
    pub fn diagonal_total(&self) -> f64 {
        let zone_count = usize::from(self.zone_count);
        if zone_count == 0 {
            return 0.0;
        }
        self.data.iter().step_by(zone_count + 1).sum()
    }
}

/// Builder for constructing a [`Matrix`] from scratch.
///
/// Use [`Matrix::builder`], [`MatrixBuilder::new`], or the fallible
/// [`MatrixBuilder::try_new`] to create a builder, add tables with
/// [`table`](MatrixBuilder::table), then call [`build`](MatrixBuilder::build).
///
/// Built matrices are initialised with zero values.
///
/// [`banner`](MatrixBuilder::banner) and [`run_id`](MatrixBuilder::run_id) are
/// optional. The default banner is
/// `MAT PGM=DESPINA VER=<crate version>`, and the default run ID is `DESPINA`.
///
/// # Example
///
/// ```
/// use despina::{MatrixBuilder, TypeCode};
///
/// let mut mat = MatrixBuilder::new(100)
///     .table("DIST_AM", TypeCode::Float32)
///     .table("TIME_AM", TypeCode::Float64)
///     .build()?;
///
/// mat.set(1, 1, 1, 42.0);
/// assert_eq!(mat.get(1, 1, 1), 42.0);
/// # Ok::<(), despina::Error>(())
/// ```
pub struct MatrixBuilder {
    banner: String,
    run_id: String,
    zone_count: u16,
    tables: Vec<TableDef>,
}

impl MatrixBuilder {
    fn with_zone_count(zone_count: u16) -> Self {
        Self {
            banner: DEFAULT_MATRIX_BUILDER_BANNER.to_owned(),
            run_id: DEFAULT_MATRIX_BUILDER_RUN_ID.to_owned(),
            zone_count,
            tables: Vec::new(),
        }
    }

    /// Creates a new builder for a matrix with the given zone count.
    ///
    /// This is the panicking convenience constructor. For user-supplied values,
    /// prefer [`MatrixBuilder::try_new`].
    ///
    /// # Panics
    ///
    /// Panics if `zone_count` is 0 or exceeds 32,000 (the maximum supported
    /// by common `.mat` matrix files).
    pub fn new(zone_count: u16) -> Self {
        assert!(zone_count > 0, "zone count must be at least 1");
        assert!(
            zone_count <= MAX_ZONE_COUNT,
            "zone count {} exceeds the maximum of {} supported by the .mat format",
            zone_count,
            MAX_ZONE_COUNT,
        );
        Self::with_zone_count(zone_count)
    }

    /// Creates a new builder for a matrix with the given zone count.
    ///
    /// This is the non-panicking constructor for user-supplied dimensions.
    ///
    /// # Errors
    ///
    /// Returns [`ErrorKind::InvalidZoneCount`] if `zone_count` is 0 or exceeds
    /// 32,000.
    pub fn try_new(zone_count: u16) -> Result<Self> {
        if zone_count == 0 || zone_count > MAX_ZONE_COUNT {
            return Err(Error::new(ErrorKind::InvalidZoneCount));
        }
        Ok(Self::with_zone_count(zone_count))
    }

    /// Sets the banner text stored in the file header.
    ///
    /// The banner is free-form text, typically identifying the generating
    /// program and version. It defaults to
    /// `MAT PGM=DESPINA VER=<crate version>` if not set.
    pub fn banner(mut self, text: impl Into<String>) -> Self {
        self.banner = text.into();
        self
    }

    /// Sets the run identifier stored in the file header.
    ///
    /// The run ID is typically prefixed with `ID=` in on-disk files. It
    /// defaults to `DESPINA` if not set.
    pub fn run_id(mut self, id: impl Into<String>) -> Self {
        self.run_id = id.into();
        self
    }

    /// Adds a table with the given name and storage type code.
    ///
    /// Tables are numbered in the order they are added, starting from 1.
    pub fn table(mut self, name: impl Into<String>, type_code: TypeCode) -> Self {
        self.tables.push(TableDef::new(name, type_code));
        self
    }

    /// Validates the builder's configuration and returns the resulting header.
    ///
    /// This performs all the same validation as [`build`](Self::build) —
    /// table count, name, and type code checks — but does not allocate the
    /// data buffer. Use this to check configuration validity or to inspect
    /// the effective header metadata (banner, run ID, table definitions)
    /// before committing to a full build.
    ///
    /// # Errors
    ///
    /// Returns an error under the same conditions as `build`.
    pub fn validate(&self) -> Result<Header> {
        validate_table_defs(&self.tables)?;
        let table_infos = table_infos_from_defs(&self.tables);
        Ok(Header::new(
            self.banner.clone(),
            self.run_id.clone(),
            self.zone_count,
            table_infos,
        ))
    }

    /// Builds the matrix, consuming the builder.
    ///
    /// All cells in every table are initialised to zero. Populate them
    /// afterwards using [`Matrix::set`] or [`Matrix::table_mut`].
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - no tables have been added
    /// - there are more than [`crate::MAX_TABLE_COUNT`] tables
    /// - a table name is empty or non-ASCII
    /// - a table has an invalid type code ([`crate::ErrorKind::InvalidTypeCode`])
    pub fn build(self) -> Result<Matrix> {
        let header = self.validate()?;
        let zone_count = usize::from(self.zone_count);
        let cells_per_table = checked_cells_per_table(zone_count)?;
        let total_cells = checked_total_cells(cells_per_table, self.tables.len())?;
        let data = vec![0.0f64; total_cells];

        Ok(Matrix::from_parts_unchecked(header, data))
    }
}

/// A table definition for use with [`MatrixBuilder`].
///
/// Pairs a table name with its storage [`TypeCode`]. Most callers do not need
/// to construct this type directly. Instead, use
/// [`MatrixBuilder::table`] to add tables inline.
///
/// This type is only useful if you need to build up a list of table
/// definitions programmatically before passing them to a builder.
#[derive(Debug, Clone)]
pub struct TableDef {
    name: String,
    type_code: TypeCode,
}

impl TableDef {
    /// Creates a new table definition with the given name and type code.
    pub fn new(name: impl Into<String>, type_code: TypeCode) -> Self {
        Self {
            name: name.into(),
            type_code,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn type_code(&self) -> TypeCode {
        self.type_code
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_creates_empty_matrix() {
        let mat = MatrixBuilder::new(10)
            .banner("test banner")
            .run_id("test run")
            .table("TABLE1", TypeCode::Float32)
            .table("TABLE2", TypeCode::Float64)
            .build()
            .unwrap();

        assert_eq!(mat.zone_count(), 10);
        assert_eq!(mat.table_count(), 2);
        assert_eq!(mat.header().banner(), "test banner");
        assert_eq!(mat.header().run_id(), "test run");
        assert_eq!(mat.get(1, 1, 1), 0.0);
        assert_eq!(mat.get(2, 10, 10), 0.0);
    }

    #[test]
    fn builder_no_tables_errors() {
        let result = MatrixBuilder::new(10).build();
        assert!(matches!(
            result.unwrap_err().kind(),
            ErrorKind::InvalidTableCount(_)
        ));
    }

    #[test]
    fn builder_default_banner_uses_despina_version() {
        let mat = MatrixBuilder::new(1)
            .table("T", TypeCode::Fixed(0))
            .build()
            .unwrap();
        assert_eq!(
            mat.header().banner(),
            concat!("MAT PGM=DESPINA VER=", env!("CARGO_PKG_VERSION"))
        );
        assert_eq!(mat.header().run_id(), "DESPINA");
    }

    #[test]
    fn builder_try_new_rejects_invalid_zone_count() {
        let err = MatrixBuilder::try_new(0).err().unwrap();
        assert!(matches!(err.kind(), ErrorKind::InvalidZoneCount));
        let err = MatrixBuilder::try_new(32_001).err().unwrap();
        assert!(matches!(err.kind(), ErrorKind::InvalidZoneCount));
    }

    #[test]
    fn builder_try_new_accepts_max_zone_count() {
        let builder = MatrixBuilder::try_new(32_000).unwrap();
        let mat = builder.table("T", TypeCode::Fixed(0)).build().unwrap();
        assert_eq!(mat.zone_count(), 32_000);
    }

    #[test]
    fn builder_rejects_empty_table_name() {
        let result = MatrixBuilder::new(10).table("", TypeCode::Fixed(0)).build();
        assert!(matches!(
            result.unwrap_err().kind(),
            ErrorKind::InvalidTableName(_)
        ));
    }

    #[test]
    fn builder_rejects_non_ascii_table_name() {
        let result = MatrixBuilder::new(10)
            .table("TRÏPS", TypeCode::Fixed(0))
            .build();
        assert!(matches!(
            result.unwrap_err().kind(),
            ErrorKind::InvalidTableName(_)
        ));
    }

    #[test]
    fn builder_rejects_invalid_type_code() {
        let result = MatrixBuilder::new(10)
            .table("TRIPS", TypeCode::Fixed(10))
            .build();
        assert!(matches!(
            result.unwrap_err().kind(),
            ErrorKind::InvalidTypeCode { token } if token == "10"
        ));
    }

    #[test]
    fn builder_rejects_too_many_tables() {
        let mut builder = MatrixBuilder::new(10);
        for i in 0..256 {
            builder = builder.table(format!("T{}", i), TypeCode::Fixed(0));
        }
        let result = builder.build();
        assert!(matches!(
            result.unwrap_err().kind(),
            ErrorKind::InvalidTableCount(_)
        ));
    }

    #[test]
    #[should_panic(expected = "zone count must be at least 1")]
    fn builder_zero_zones_panics() {
        MatrixBuilder::new(0);
    }

    #[test]
    #[should_panic(expected = "zone count 32001 exceeds the maximum of 32000")]
    fn builder_excessive_zones_panics() {
        MatrixBuilder::new(32_001);
    }

    #[test]
    fn builder_max_zones_succeeds() {
        let mat = MatrixBuilder::new(32_000)
            .table("T", TypeCode::Fixed(0))
            .build()
            .unwrap();
        assert_eq!(mat.zone_count(), 32_000);
    }

    #[test]
    fn set_and_get() {
        let mut mat = MatrixBuilder::new(3)
            .table("T1", TypeCode::Fixed(2))
            .build()
            .unwrap();

        mat.set(1, 1, 1, 42.0);
        mat.set(1, 2, 3, 99.5);
        assert_eq!(mat.get(1, 1, 1), 42.0);
        assert_eq!(mat.get(1, 2, 3), 99.5);
        assert_eq!(mat.get(1, 1, 2), 0.0);
    }

    #[test]
    fn set_and_get_by_name() {
        let mut mat = MatrixBuilder::new(3)
            .table("TRIPS", TypeCode::Float32)
            .table("DIST", TypeCode::Float64)
            .build()
            .unwrap();

        mat.set_by_name("TRIPS", 1, 1, 10.0);
        mat.set_by_name("DIST", 2, 3, 55.5);
        assert_eq!(mat.get_by_name("TRIPS", 1, 1), 10.0);
        assert_eq!(mat.get_by_name("DIST", 2, 3), 55.5);
    }

    #[test]
    fn checked_get_returns_none_for_oob() {
        let mat = MatrixBuilder::new(3)
            .table("T1", TypeCode::Fixed(0))
            .build()
            .unwrap();

        assert!(mat.checked_get(1, 1, 1).is_some());
        assert!(mat.checked_get(0, 1, 1).is_none());
        assert!(mat.checked_get(2, 1, 1).is_none());
        assert!(mat.checked_get(1, 0, 1).is_none());
        assert!(mat.checked_get(1, 1, 4).is_none());
    }

    #[test]
    fn checked_set_writes_and_returns_none_for_oob() {
        let mut mat = MatrixBuilder::new(3)
            .table("T1", TypeCode::Fixed(0))
            .build()
            .unwrap();

        assert_eq!(mat.checked_set(1, 1, 2, 7.5), Some(()));
        assert_eq!(mat.get(1, 1, 2), 7.5);
        assert_eq!(mat.checked_set(0, 1, 1, 1.0), None);
        assert_eq!(mat.checked_set(2, 1, 1, 1.0), None);
        assert_eq!(mat.checked_set(1, 0, 1, 1.0), None);
        assert_eq!(mat.checked_set(1, 1, 4, 1.0), None);
    }

    #[test]
    #[should_panic(expected = "table index 0 is out of bounds")]
    fn get_panics_on_zero_table() {
        let mat = MatrixBuilder::new(2)
            .table("T", TypeCode::Fixed(0))
            .build()
            .unwrap();
        mat.get(0, 1, 1);
    }

    #[test]
    #[should_panic(expected = "origin 0 is out of bounds")]
    fn get_panics_on_zero_origin() {
        let mat = MatrixBuilder::new(2)
            .table("T", TypeCode::Fixed(0))
            .build()
            .unwrap();
        mat.get(1, 0, 1);
    }

    #[test]
    #[should_panic(expected = "destination 3 is out of bounds")]
    fn get_panics_on_oob_destination() {
        let mat = MatrixBuilder::new(2)
            .table("T", TypeCode::Fixed(0))
            .build()
            .unwrap();
        mat.get(1, 1, 3);
    }

    #[test]
    fn table_view_accessors() {
        let mut mat = MatrixBuilder::new(3)
            .table("TRIPS", TypeCode::Float32)
            .build()
            .unwrap();

        mat.set(1, 1, 1, 1.0);
        mat.set(1, 2, 2, 2.0);
        mat.set(1, 3, 3, 3.0);
        mat.set(1, 1, 2, 10.0);

        let t = mat.table("TRIPS");
        assert_eq!(t.name(), "TRIPS");
        assert_eq!(t.zone_count(), 3);
        assert_eq!(t.index(), 1);
        assert_eq!(t.get(1, 1), 1.0);
        assert_eq!(t.get(2, 2), 2.0);
        assert_eq!(t.row(1), &[1.0, 10.0, 0.0]);
        assert_eq!(t.total(), 16.0);
        assert_eq!(t.diagonal_total(), 6.0);
    }

    #[test]
    fn table_mut_view() {
        let mut mat = MatrixBuilder::new(2)
            .table("T1", TypeCode::Fixed(0))
            .build()
            .unwrap();

        {
            let mut t = mat.table_mut("T1");
            t.set(1, 1, 5.0);
            t.set(2, 2, 10.0);
            t.row_mut(1)[1] = 3.0;
        }

        assert_eq!(mat.get(1, 1, 1), 5.0);
        assert_eq!(mat.get(1, 1, 2), 3.0);
        assert_eq!(mat.get(1, 2, 2), 10.0);
    }

    #[test]
    fn table_mut_fill() {
        let mut mat = MatrixBuilder::new(2)
            .table("T1", TypeCode::Fixed(0))
            .build()
            .unwrap();

        mat.table_mut("T1").fill(7.0);
        assert_eq!(mat.get(1, 1, 1), 7.0);
        assert_eq!(mat.get(1, 1, 2), 7.0);
        assert_eq!(mat.get(1, 2, 1), 7.0);
        assert_eq!(mat.get(1, 2, 2), 7.0);
    }

    #[test]
    fn try_table_returns_none_for_missing() {
        let mat = MatrixBuilder::new(2)
            .table("T1", TypeCode::Fixed(0))
            .build()
            .unwrap();

        assert!(mat.try_table("T1").is_some());
        assert!(mat.try_table("NONEXISTENT").is_none());
    }

    #[test]
    #[should_panic(expected = "no table named")]
    fn table_panics_on_missing_name() {
        let mat = MatrixBuilder::new(2)
            .table("T1", TypeCode::Fixed(0))
            .build()
            .unwrap();
        mat.table("NOPE");
    }

    #[test]
    fn table_by_index() {
        let mat = MatrixBuilder::new(2)
            .table("A", TypeCode::Fixed(0))
            .table("B", TypeCode::Float32)
            .build()
            .unwrap();

        let a = mat.table_by_index(1);
        assert_eq!(a.name(), "A");
        let b = mat.table_by_index(2);
        assert_eq!(b.name(), "B");
    }

    #[test]
    fn try_table_by_index_returns_none_for_oob() {
        let mat = MatrixBuilder::new(2)
            .table("A", TypeCode::Fixed(0))
            .table("B", TypeCode::Float32)
            .build()
            .unwrap();
        assert!(mat.try_table_by_index(0).is_none());
        assert!(mat.try_table_by_index(3).is_none());
        assert_eq!(mat.try_table_by_index(2).unwrap().name(), "B");
    }

    #[test]
    fn try_table_by_index_mut_returns_none_for_oob() {
        let mut mat = MatrixBuilder::new(2)
            .table("A", TypeCode::Fixed(0))
            .build()
            .unwrap();
        assert!(mat.try_table_by_index_mut(0).is_none());
        assert!(mat.try_table_by_index_mut(2).is_none());
        mat.try_table_by_index_mut(1).unwrap().fill(3.0);
        assert_eq!(mat.get(1, 1, 1), 3.0);
    }

    #[test]
    fn tables_iterator() {
        let mat = MatrixBuilder::new(2)
            .table("A", TypeCode::Fixed(0))
            .table("B", TypeCode::Float32)
            .table("C", TypeCode::Float64)
            .build()
            .unwrap();

        let names: Vec<String> = mat.tables().map(|t| t.name().to_owned()).collect();
        assert_eq!(names, vec!["A", "B", "C"]);
    }

    #[test]
    fn table_rows_iterator() {
        let mut mat = MatrixBuilder::new(2)
            .table("T", TypeCode::Fixed(0))
            .build()
            .unwrap();

        mat.set(1, 1, 1, 1.0);
        mat.set(1, 1, 2, 2.0);
        mat.set(1, 2, 1, 3.0);
        mat.set(1, 2, 2, 4.0);

        let t = mat.table("T");
        let rows: Vec<&[f64]> = t.rows().collect();
        assert_eq!(rows, vec![&[1.0, 2.0][..], &[3.0, 4.0][..]]);
    }

    #[test]
    fn row_and_row_mut() {
        let mut mat = MatrixBuilder::new(3)
            .table("T1", TypeCode::Fixed(0))
            .build()
            .unwrap();

        mat.set(1, 2, 1, 10.0);
        mat.set(1, 2, 2, 20.0);
        mat.set(1, 2, 3, 30.0);

        assert_eq!(mat.row(1, 2), &[10.0, 20.0, 30.0]);

        mat.row_mut(1, 2)[0] = 99.0;
        assert_eq!(mat.get(1, 2, 1), 99.0);
    }

    #[test]
    fn table_data_and_table_data_mut() {
        let mut mat = MatrixBuilder::new(2)
            .table("T1", TypeCode::Fixed(0))
            .build()
            .unwrap();

        let data = mat.table_data(1);
        assert_eq!(data.len(), 4);
        assert!(data.iter().all(|&v| v == 0.0));

        mat.table_data_mut(1)[0] = 42.0;
        assert_eq!(mat.get(1, 1, 1), 42.0);
    }

    #[test]
    fn table_checked_get() {
        let mut mat = MatrixBuilder::new(2)
            .table("T", TypeCode::Fixed(0))
            .build()
            .unwrap();

        mat.set(1, 1, 2, 7.0);
        let t = mat.table("T");
        assert_eq!(t.checked_get(1, 2), Some(7.0));
        assert_eq!(t.checked_get(0, 1), None);
        assert_eq!(t.checked_get(1, 3), None);
    }

    /// Helper: builds a 3-table matrix, serialises it to bytes.
    fn three_table_bytes() -> Vec<u8> {
        let mut mat = MatrixBuilder::new(2)
            .table("DIST", TypeCode::Float32)
            .table("TIME", TypeCode::Float64)
            .table("COST", TypeCode::Fixed(2))
            .build()
            .unwrap();
        mat.set_by_name("DIST", 1, 2, 10.0);
        mat.set_by_name("TIME", 1, 2, 20.0);
        mat.set_by_name("COST", 1, 2, 30.0);
        mat.set_by_name("DIST", 2, 1, 11.0);
        mat.set_by_name("TIME", 2, 1, 22.0);
        mat.set_by_name("COST", 2, 1, 33.0);
        let mut bytes = Vec::new();
        mat.write_to_writer(&mut bytes).unwrap();
        bytes
    }

    #[test]
    fn from_bytes_tables_loads_subset() {
        let bytes = three_table_bytes();
        let mat = Matrix::from_bytes_tables(&bytes, &["DIST", "COST"]).unwrap();

        assert_eq!(mat.table_count(), 2);
        assert_eq!(mat.zone_count(), 2);
        assert_eq!(mat.table("DIST").get(1, 2), 10.0);
        assert_eq!(mat.table("DIST").get(2, 1), 11.0);
        assert_eq!(mat.table("COST").get(1, 2), 30.0);
        assert_eq!(mat.table("COST").get(2, 1), 33.0);
    }

    #[test]
    fn from_bytes_tables_renumbers_indices() {
        let bytes = three_table_bytes();
        // Select tables 2 and 3 from the original file.
        let mat = Matrix::from_bytes_tables(&bytes, &["TIME", "COST"]).unwrap();

        assert_eq!(mat.table_count(), 2);
        // Renumbered: TIME -> 1, COST -> 2.
        assert_eq!(mat.table("TIME").index(), 1);
        assert_eq!(mat.table("COST").index(), 2);
        assert_eq!(mat.get(1, 1, 2), 20.0); // TIME at new index 1
        assert_eq!(mat.get(2, 1, 2), 30.0); // COST at new index 2
    }

    #[test]
    fn from_bytes_tables_preserves_file_order() {
        let bytes = three_table_bytes();
        // Request tables in reverse order — result should still be file order.
        let mat = Matrix::from_bytes_tables(&bytes, &["COST", "DIST"]).unwrap();

        let names: Vec<&str> = mat.header().tables().iter().map(|t| t.name()).collect();
        assert_eq!(names, &["DIST", "COST"]);
    }

    #[test]
    fn from_bytes_tables_deduplicates_names() {
        let bytes = three_table_bytes();
        let mat = Matrix::from_bytes_tables(&bytes, &["DIST", "DIST", "DIST"]).unwrap();

        assert_eq!(mat.table_count(), 1);
        assert_eq!(mat.table("DIST").get(1, 2), 10.0);
    }

    #[test]
    fn from_bytes_tables_round_trip() {
        let bytes = three_table_bytes();
        let partial = Matrix::from_bytes_tables(&bytes, &["TIME", "COST"]).unwrap();

        // Write the partial matrix back out.
        let mut round_trip_bytes = Vec::new();
        partial.write_to_writer(&mut round_trip_bytes).unwrap();

        // Re-read the written file as a full matrix.
        let reloaded = Matrix::from_bytes(&round_trip_bytes).unwrap();
        assert_eq!(reloaded.table_count(), 2);
        assert_eq!(reloaded.table("TIME").get(1, 2), 20.0);
        assert_eq!(reloaded.table("TIME").get(2, 1), 22.0);
        assert_eq!(reloaded.table("COST").get(1, 2), 30.0);
        assert_eq!(reloaded.table("COST").get(2, 1), 33.0);
    }

    #[test]
    fn from_bytes_tables_unknown_name_errors() {
        let bytes = three_table_bytes();
        let err = Matrix::from_bytes_tables(&bytes, &["NONEXISTENT"]).unwrap_err();
        assert!(matches!(
            err.kind(),
            ErrorKind::TableNotFound(name) if name == "NONEXISTENT"
        ));
    }

    #[test]
    fn from_bytes_tables_empty_selection_errors() {
        let bytes = three_table_bytes();
        let err = Matrix::from_bytes_tables(&bytes, &[]).unwrap_err();
        assert!(matches!(err.kind(), ErrorKind::InvalidTableCount(_)));
    }

    #[test]
    fn from_bytes_tables_single_table() {
        let bytes = three_table_bytes();
        let mat = Matrix::from_bytes_tables(&bytes, &["TIME"]).unwrap();

        assert_eq!(mat.table_count(), 1);
        assert_eq!(mat.table("TIME").index(), 1);
        assert_eq!(mat.table("TIME").get(1, 2), 20.0);
        assert_eq!(mat.table("TIME").get(2, 1), 22.0);
        // All other cells should be zero.
        assert_eq!(mat.table("TIME").get(1, 1), 0.0);
    }

    #[test]
    fn from_bytes_tables_all_tables_equivalent_to_full_load() {
        let bytes = three_table_bytes();
        let full = Matrix::from_bytes(&bytes).unwrap();
        let selected = Matrix::from_bytes_tables(&bytes, &["DIST", "TIME", "COST"]).unwrap();

        assert_eq!(full.table_count(), selected.table_count());
        for table_index in 1..=full.table_count() {
            let ft = full.table_by_index(table_index);
            let st = selected.table_by_index(table_index);
            assert_eq!(ft.name(), st.name());
            assert_eq!(ft.as_slice(), st.as_slice());
        }
    }
}
