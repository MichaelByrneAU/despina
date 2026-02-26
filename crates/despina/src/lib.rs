//! Read and write `.mat` binary matrix files.
//!
//! `despina` provides two access patterns. [`Matrix`] is the in-memory API for
//! random access and in-place edits. [`Reader`] and [`Writer`] are streaming
//! APIs for single-pass processing with bounded memory use.
//!
//! Public indices are `1`-based, table-name matching is case-sensitive, and
//! decoded numeric values are exposed as [`f64`] regardless of on-disk type
//! code. Builder and writer paths enforce [`MAX_ZONE_COUNT`] and
//! [`MAX_TABLE_COUNT`] and return an error for unsupported matrix sizes.
//!
//! # Getting started
//!
//! Load a full matrix for random access:
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
//! # source.set(1, 1, 2, 12.0);
//! # let path = temp_mat_path("network");
//! # source.write_to(&path)?;
//! let mat = despina::open(&path)?;
//! let dist_am = mat.table("DIST_AM");
//! assert_eq!(mat.zone_count(), 2);
//! assert_eq!(dist_am.get(1, 2), 12.0);
//! # let _ = std::fs::remove_file(path);
//! # Ok::<(), despina::Error>(())
//! ```
//!
//! Stream rows for single-pass processing with low memory overhead:
//!
//! ```
//! use despina::{MatrixBuilder, ReaderBuilder, RowBuf, TypeCode};
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
//! let mut reader = ReaderBuilder::new().from_bytes(&bytes)?;
//! let mut row = RowBuf::new();
//! let selection = reader.prepare_selection_by_name(&["DIST_AM"])?;
//! let mut total = 0.0;
//!
//! while reader.read_selected_row(selection, &mut row)? {
//!     total += row.values().iter().sum::<f64>();
//! }
//! assert_eq!(total, 10.0);
//! # Ok::<(), despina::Error>(())
//! ```
//!
//! Write matrix data with the streaming writer:
//!
//! ```
//! use despina::{ReaderBuilder, RowBuf, TableDef, TypeCode, Writer};
//!
//! let tables = [TableDef::new("DIST_AM", TypeCode::Float32)];
//! let mut writer = Writer::open_writer(Vec::new(), 2, &tables)?;
//!
//! // Preferred bulk path for complete table data: (table, origin, destination).
//! writer.write_stack(&[
//!     1.0, 2.0, // table 1, origin 1
//!     3.0, 4.0, // table 1, origin 2
//! ])?;
//! let bytes = writer.finish()?;
//!
//! let mut reader = ReaderBuilder::new().from_bytes(&bytes)?;
//! let mut row = RowBuf::new();
//! assert!(reader.read_row(&mut row)?);
//! assert_eq!(row.values(), &[1.0, 2.0]);
//! assert!(reader.read_row(&mut row)?);
//! assert_eq!(row.values(), &[3.0, 4.0]);
//! assert!(!reader.read_row(&mut row)?);
//! # Ok::<(), despina::Error>(())
//! ```
//!
//! [`Writer`] exposes three write levels so callers can match upstream data
//! shape: [`Writer::write_stack`] (full in-memory stack),
//! [`Writer::write_origin`] / [`Writer::write_origins`] (origin blocks), and
//! [`Writer::write_next_row`] (row-at-a-time control). Prefer the highest-level
//! method that matches your data source to reduce caller-side loops and
//! ordering mistakes.
//!
//! [`RowBuf`] is designed for reuse across streaming reads to avoid per-row
//! allocation. [`TableDef`], [`TypeCode`], [`Header`], and [`TableInfo`] expose
//! the format metadata needed for controlled read and write workflows without
//! requiring callers to reason about binary records directly.
//!
//! # Errors
//!
//! Fallible operations return [`Result`]. Inspect failures via [`Error::kind`]
//! and match on [`ErrorKind`]. [`ErrorKind`] is non-exhaustive, so include a
//! wildcard arm for forward compatibility.
//!
mod error;
mod header;
mod reader;
mod row;
mod types;

mod decode;
mod encode;
mod matrix;
mod plane;
mod row_format;
mod table_defs;
mod writer;

/// Maximum zone count supported for writer APIs.
///
/// This limit matches practical bounds used by `.mat` matrix files.
pub const MAX_ZONE_COUNT: u16 = 32_000;

/// Maximum table count supported by `.mat` headers in this crate.
pub const MAX_TABLE_COUNT: u8 = u8::MAX;

pub use crate::error::{Error, ErrorKind, IntoInnerError, Result};
pub use crate::header::{Header, TableInfo};
pub use crate::matrix::{Matrix, MatrixBuilder, Table, TableDef, TableMut};
#[cfg(not(target_arch = "wasm32"))]
pub use crate::matrix::{open, open_tables};
pub use crate::reader::{PreparedSelection, Reader, ReaderBuilder};
pub use crate::row::RowBuf;
pub use crate::types::TypeCode;
pub use crate::writer::{Writer, WriterBuilder};
