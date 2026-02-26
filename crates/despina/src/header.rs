//! Header parsing and serialisation for `.mat` files.
//!
//! A `.mat` file begins with five length-prefixed records: banner, run ID,
//! PAR, MVR, and a `ROW\0` marker. This module parses those records into
//! [`Header`] and [`TableInfo`], validates their structural invariants, and
//! writes the same five-record form when serialising.
//!
//! Parsing rejects malformed record lengths, missing required tokens, invalid
//! type codes, and PAR/MVR table-count disagreement.

use crate::error::{Error, ErrorKind, Result};
use crate::types::TypeCode;

const HEADER_RECORD_BANNER: u8 = 1;
const HEADER_RECORD_RUN_ID: u8 = 2;
const HEADER_RECORD_PAR: u8 = 3;
const HEADER_RECORD_MVR: u8 = 4;
const HEADER_RECORD_ROW: u8 = 5;
const RUN_ID_PREFIX: &str = "ID=";
const ROW_MARKER: &[u8; 4] = b"ROW\0";

/// Parsed metadata from the five mandatory header records of a `.mat` file.
///
/// A `Header` carries the banner text, run identifier, zone count, and
/// per-table metadata.
#[derive(Debug, Clone)]
pub struct Header {
    banner: String,
    run_id: String,
    zone_count: u16,
    tables: Vec<TableInfo>,
}

impl Header {
    /// Creates a new `Header` from its constituent parts.
    ///
    /// This is used by [`MatrixBuilder`](crate::MatrixBuilder) to construct
    /// headers programmatically. Not exposed in the public API.
    pub(crate) fn new(
        banner: String,
        run_id: String,
        zone_count: u16,
        tables: Vec<TableInfo>,
    ) -> Self {
        Self {
            banner,
            run_id,
            zone_count,
            tables,
        }
    }

    /// Returns the `Zones` value from the PAR record.
    ///
    /// Every table is `zone_count × zone_count`. This value is always non-zero.
    #[inline]
    pub fn zone_count(&self) -> u16 {
        self.zone_count
    }

    /// Returns the number of tables declared by the header.
    ///
    /// This value is always non-zero.
    #[inline]
    pub fn table_count(&self) -> u8 {
        self.tables.len() as u8
    }

    /// Returns `zone_count × table_count`.
    ///
    /// This is the number of row records in the file body.
    ///
    /// # Example
    ///
    /// ```
    /// use despina::{MatrixBuilder, TypeCode};
    ///
    /// let mat = MatrixBuilder::new(3)
    ///     .table("DIST_AM", TypeCode::Float32)
    ///     .table("TIME_AM", TypeCode::Float64)
    ///     .build()?;
    ///
    /// assert_eq!(mat.header().row_count(), 6);
    /// # Ok::<(), despina::Error>(())
    /// ```
    #[inline]
    pub fn row_count(&self) -> u32 {
        u32::from(self.zone_count) * u32::from(self.table_count())
    }

    /// Returns the per-table metadata in header order.
    #[inline]
    pub fn tables(&self) -> &[TableInfo] {
        &self.tables
    }

    /// Returns the banner text from the first header record.
    #[inline]
    pub fn banner(&self) -> &str {
        &self.banner
    }

    /// Returns the run identifier from the second header record.
    ///
    /// If the raw record starts with `ID=`, that prefix is stripped on parse.
    #[inline]
    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    /// Returns a new `Header` containing only the tables whose original
    /// 1-based indices appear in `original_indices`, renumbered 1..N.
    ///
    /// Tables are kept in their original file order regardless of the order
    /// in `original_indices`. Banner and run ID are preserved.
    pub(crate) fn with_selected_tables(&self, original_indices: &[u8]) -> Self {
        let mut selected: Vec<&TableInfo> = self
            .tables
            .iter()
            .filter(|t| original_indices.contains(&t.index))
            .collect();
        selected.sort_by_key(|t| t.index);

        let tables = selected
            .into_iter()
            .enumerate()
            .map(|(position, info)| TableInfo {
                index: (position + 1) as u8,
                name: info.name.clone(),
                type_code: info.type_code,
            })
            .collect();

        Self {
            banner: self.banner.clone(),
            run_id: self.run_id.clone(),
            zone_count: self.zone_count,
            tables,
        }
    }

    /// Returns the 1-based index of the first table with `name`, or `None`.
    ///
    /// Matching is case-sensitive and uses a linear scan.
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
    /// let header = mat.header();
    /// assert_eq!(header.table_index_by_name("DIST_AM"), Some(1));
    /// assert_eq!(header.table_index_by_name("TIME_AM"), Some(2));
    /// assert_eq!(header.table_index_by_name("COST_AM"), None);
    /// # Ok::<(), despina::Error>(())
    /// ```
    pub fn table_index_by_name(&self, name: &str) -> Option<u8> {
        self.tables.iter().find(|t| t.name == name).map(|t| t.index)
    }
}

/// Metadata for a single table (matrix layer) in a `.mat` file.
///
/// Each entry stores the table's 1-based index, name, and [`TypeCode`].
#[derive(Debug, Clone)]
pub struct TableInfo {
    index: u8,
    name: String,
    type_code: TypeCode,
}

impl TableInfo {
    /// Creates a new `TableInfo` from its constituent parts.
    pub(crate) fn new(index: u8, name: String, type_code: TypeCode) -> Self {
        Self {
            index,
            name,
            type_code,
        }
    }

    /// Returns the 1-based table index.
    #[inline]
    pub fn index(&self) -> u8 {
        self.index
    }

    /// Returns the table name from the MVR record.
    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the table storage type code.
    #[inline]
    pub fn type_code(&self) -> TypeCode {
        self.type_code
    }
}

/// Writes a single length-prefixed header record.
fn write_record<W: std::io::Write>(sink: &mut W, payload: &[u8]) -> std::io::Result<()> {
    let total_size = (payload.len() as u32) + 4;
    sink.write_all(&total_size.to_le_bytes())?;
    sink.write_all(payload)?;
    Ok(())
}

impl Header {
    /// Serialises the five mandatory header records into `sink`.
    pub(crate) fn write_to<W: std::io::Write>(&self, sink: &mut W) -> Result<()> {
        let mut scratch = Vec::with_capacity(256);
        let table_count = self.table_count();

        // Record 1: banner.
        scratch.clear();
        scratch.extend_from_slice(self.banner.as_bytes());
        if !scratch.ends_with(&[0]) {
            scratch.push(0);
        }
        write_record(sink, &scratch)?;

        // Record 2: run ID ("ID=...").
        scratch.clear();
        scratch.extend_from_slice(RUN_ID_PREFIX.as_bytes());
        scratch.extend_from_slice(self.run_id.as_bytes());
        if !scratch.ends_with(&[0]) {
            scratch.push(0);
        }
        write_record(sink, &scratch)?;

        // Record 3: PAR.
        scratch.clear();
        use std::io::Write as _;
        write!(scratch, "PAR Zones={} M={}\0", self.zone_count, table_count)
            .expect("Vec write is infallible");
        write_record(sink, &scratch)?;

        // Record 4: MVR.
        scratch.clear();
        write!(scratch, "MVR {}\0", table_count).expect("Vec write is infallible");
        for table in &self.tables {
            scratch.extend_from_slice(table.name.as_bytes());
            scratch.push(b'=');
            if !table.type_code.write_ascii(&mut scratch) {
                let token = match table.type_code {
                    TypeCode::Fixed(p) => p.to_string(),
                    TypeCode::Float32 => "S".to_owned(),
                    TypeCode::Float64 => "D".to_owned(),
                };
                return Err(Error::new(ErrorKind::InvalidTypeCode { token }));
            }
            scratch.push(0);
        }
        write_record(sink, &scratch)?;

        // Record 5: ROW marker.
        write_record(sink, ROW_MARKER)?;

        Ok(())
    }
}

/// Thin wrapper around `Read::read_exact` that promotes `UnexpectedEof` to our
/// own `ErrorKind::UnexpectedEof` for uniform error handling.
fn read_exact<R: std::io::Read>(reader: &mut R, buf: &mut [u8]) -> Result<()> {
    reader.read_exact(buf).map_err(|e| {
        if e.kind() == std::io::ErrorKind::UnexpectedEof {
            Error::new(ErrorKind::UnexpectedEof)
        } else {
            Error::new(ErrorKind::Io(e))
        }
    })
}

/// Truncate `buf` at the first NUL byte (if any) and return the prefix.
fn truncate_nul(buf: &[u8]) -> &[u8] {
    match buf.iter().position(|&byte| byte == 0) {
        Some(offset) => &buf[..offset],
        None => buf,
    }
}

/// Truncate `buf` at the first NUL byte and convert to a `String`, reusing the
/// buffer when the contents are valid UTF-8.
fn vec_into_string_nul(mut buf: Vec<u8>) -> String {
    if let Some(offset) = buf.iter().position(|&byte| byte == 0) {
        buf.truncate(offset);
    }
    match String::from_utf8(buf) {
        Ok(s) => s,
        Err(e) => String::from_utf8_lossy(e.as_bytes()).into_owned(),
    }
}

/// Maximum allowed header record payload size (1 MiB).
///
/// Header records are small text (PAR, MVR, etc.) and should never approach
/// this limit under normal operation. The bound prevents a malformed
/// `total_size` from causing a multi-gigabyte allocation.
const MAX_HEADER_RECORD_SIZE: u32 = 1_048_576;

/// Read one length-prefixed header record, resizing `buf` to hold the payload.
///
/// Each header record starts with a `u32le` total size (including the size
/// field itself), so the payload is `total_size - 4` bytes. Returns
/// `InvalidHeaderLength` if the size is less than 4 or exceeds the maximum
/// allowed header record size.
fn read_record<R: std::io::Read>(
    reader: &mut R,
    record_index: u8,
    buf: &mut Vec<u8>,
) -> Result<()> {
    let mut size_bytes = [0u8; 4];
    read_exact(reader, &mut size_bytes)?;
    let total_size = u32::from_le_bytes(size_bytes);

    if !(4..=MAX_HEADER_RECORD_SIZE).contains(&total_size) {
        return Err(Error::new(ErrorKind::InvalidHeaderLength {
            record_index,
            total_size,
        }));
    }

    let payload_size = (total_size - 4) as usize;
    buf.resize(payload_size, 0);
    read_exact(reader, buf)?;
    Ok(())
}

/// Extract `Zones` and `M` from a PAR record payload.
///
/// The payload is ASCII text like `PAR Zones=100 M=3`, possibly NUL-terminated.
/// Both values must be present and non-zero.
fn parse_par(payload: &[u8]) -> Result<(u16, u8)> {
    let text = std::str::from_utf8(truncate_nul(payload))
        .map_err(|_| Error::new(ErrorKind::InvalidPar("non-UTF-8 payload".into())))?;

    let mut zone_count: Option<u16> = None;
    let mut table_count: Option<u8> = None;

    for token in text.split_ascii_whitespace() {
        if let Some(val) = token.strip_prefix("Zones=") {
            zone_count = Some(val.parse::<u16>().map_err(|_| {
                Error::new(ErrorKind::InvalidPar(format!(
                    "invalid Zones value: {}",
                    val
                )))
            })?);
        } else if let Some(val) = token.strip_prefix("M=") {
            table_count = Some(val.parse::<u8>().map_err(|_| {
                Error::new(ErrorKind::InvalidPar(format!("invalid M value: {}", val)))
            })?);
        }
    }

    let zone_count = zone_count
        .ok_or_else(|| Error::new(ErrorKind::InvalidPar("missing Zones= token".into())))?;
    let table_count =
        table_count.ok_or_else(|| Error::new(ErrorKind::InvalidPar("missing M= token".into())))?;

    if zone_count == 0 {
        return Err(Error::new(ErrorKind::InvalidPar(
            "Zones must be > 0".into(),
        )));
    }
    if table_count == 0 {
        return Err(Error::new(ErrorKind::InvalidPar("M must be > 0".into())));
    }

    Ok((zone_count, table_count))
}

/// Build the table catalogue from an MVR record payload.
///
/// The payload is NUL-delimited: `MVR 3\0T1=2\0T2=S\0T3=D\0`. The leading token
/// declares the expected count, and each subsequent token is split at its last
/// `=` into a name and type code. Returns `TableCountMismatch` if the MVR count
/// disagrees with `expected_count` from PAR.
fn parse_mvr(payload: &[u8], expected_count: u8) -> Result<Vec<TableInfo>> {
    let mut tokens = payload.split(|&b| b == 0).filter(|t| !t.is_empty());

    let first_raw = tokens
        .next()
        .ok_or_else(|| Error::new(ErrorKind::InvalidMvr("empty MVR payload".into())))?;

    // First token should be "MVR <T>".
    let first = std::str::from_utf8(first_raw)
        .map_err(|_| Error::new(ErrorKind::InvalidMvr("non-UTF-8 first token".into())))?;

    let mvr_count_str = first
        .strip_prefix("MVR ")
        .ok_or_else(|| Error::new(ErrorKind::InvalidMvr("missing MVR prefix".into())))?;

    let mvr_count = mvr_count_str.parse::<u8>().map_err(|_| {
        Error::new(ErrorKind::InvalidMvr(format!(
            "invalid MVR count: {}",
            mvr_count_str
        )))
    })?;

    if mvr_count != expected_count {
        return Err(Error::new(ErrorKind::TableCountMismatch {
            par: expected_count,
            mvr: mvr_count,
        }));
    }

    let mut tables = Vec::with_capacity(expected_count as usize);
    for token in tokens {
        if tables.len() == expected_count as usize {
            return Err(Error::new(ErrorKind::InvalidMvr(format!(
                "expected {} table tokens, got more",
                expected_count
            ))));
        }

        let s = std::str::from_utf8(token)
            .map_err(|_| Error::new(ErrorKind::InvalidMvr("non-UTF-8 table token".into())))?;

        // Split at the last '='.
        let eq_offset = s.rfind('=').ok_or_else(|| {
            Error::new(ErrorKind::InvalidMvr(format!(
                "table token missing '=': {}",
                s
            )))
        })?;

        let name = &s[..eq_offset];
        let type_str = &s[eq_offset + 1..];

        let type_code = TypeCode::from_ascii(type_str).ok_or_else(|| {
            Error::new(ErrorKind::InvalidTypeCode {
                token: type_str.to_owned(),
            })
        })?;

        tables.push(TableInfo {
            index: (tables.len() + 1) as u8,
            name: name.to_owned(),
            type_code,
        });
    }

    if tables.len() != expected_count as usize {
        return Err(Error::new(ErrorKind::InvalidMvr(format!(
            "expected {} table tokens, got {}",
            expected_count,
            tables.len()
        ))));
    }

    Ok(tables)
}

/// Parse all five mandatory header records from `reader` and return a [`Header`].
///
/// Reads records sequentially: banner, run ID, PAR, MVR, and the ROW data-start
/// marker. Cross-validates the PAR and MVR table counts before returning. After
/// this function succeeds, `reader` is positioned at the first byte of row data.
pub(crate) fn parse_header<R: std::io::Read>(reader: &mut R) -> Result<Header> {
    let mut buf = Vec::new();

    // Record 1: banner.
    read_record(reader, HEADER_RECORD_BANNER, &mut buf)?;
    let banner = vec_into_string_nul(std::mem::take(&mut buf));

    // Record 2: run ID ("ID=...").
    read_record(reader, HEADER_RECORD_RUN_ID, &mut buf)?;
    let mut run_id = vec_into_string_nul(std::mem::take(&mut buf));
    if run_id.starts_with(RUN_ID_PREFIX) {
        run_id.drain(..RUN_ID_PREFIX.len());
    }

    // Record 3: PAR.
    read_record(reader, HEADER_RECORD_PAR, &mut buf)?;
    let (zone_count, table_count) = parse_par(&buf)?;

    // Record 4: MVR.
    read_record(reader, HEADER_RECORD_MVR, &mut buf)?;
    let tables = parse_mvr(&buf, table_count)?;

    // Record 5: ROW marker.
    read_record(reader, HEADER_RECORD_ROW, &mut buf)?;
    if buf.as_slice() != ROW_MARKER {
        return Err(Error::new(ErrorKind::MissingRowMarker));
    }

    Ok(Header {
        banner,
        run_id,
        zone_count,
        tables,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn append_record(data: &mut Vec<u8>, payload: &[u8]) {
        let total_size = (payload.len() as u32) + 4;
        data.extend_from_slice(&total_size.to_le_bytes());
        data.extend_from_slice(payload);
    }

    fn split_records(data: &[u8], count: usize) -> Vec<&[u8]> {
        let mut records = Vec::with_capacity(count);
        let mut offset = 0usize;
        for _ in 0..count {
            assert!(offset + 4 <= data.len());
            let total_size =
                u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
            assert!(total_size >= 4);
            let end = offset + total_size;
            assert!(end <= data.len());
            records.push(&data[offset + 4..end]);
            offset = end;
        }
        records
    }

    fn make_header_bytes(banner: &[u8], id: &[u8], par: &[u8], mvr: &[u8], row: &[u8]) -> Vec<u8> {
        let mut data =
            Vec::with_capacity(banner.len() + id.len() + par.len() + mvr.len() + row.len() + 20);
        append_record(&mut data, banner);
        append_record(&mut data, id);
        append_record(&mut data, par);
        append_record(&mut data, mvr);
        append_record(&mut data, row);
        data
    }

    #[test]
    fn parse_valid_header() {
        let data = make_header_bytes(
            b"MAT PGM=MATRIX VER=1",
            b"ID=Test Run",
            b"PAR Zones=100 M=3",
            b"MVR 3\0T1=2\0T2=S\0T3=D\0",
            b"ROW\0",
        );
        let header = parse_header(&mut data.as_slice()).unwrap();
        assert_eq!(header.zone_count(), 100);
        assert_eq!(header.table_count(), 3);
        assert_eq!(header.row_count(), 300);
        assert_eq!(header.banner(), "MAT PGM=MATRIX VER=1");
        assert_eq!(header.run_id(), "Test Run");

        let tables = header.tables();
        assert_eq!(tables.len(), 3);
        assert_eq!(tables[0].index(), 1);
        assert_eq!(tables[0].name(), "T1");
        assert_eq!(tables[0].type_code(), TypeCode::Fixed(2));
        assert_eq!(tables[1].index(), 2);
        assert_eq!(tables[1].name(), "T2");
        assert_eq!(tables[1].type_code(), TypeCode::Float32);
        assert_eq!(tables[2].index(), 3);
        assert_eq!(tables[2].name(), "T3");
        assert_eq!(tables[2].type_code(), TypeCode::Float64);
    }

    #[test]
    fn invalid_header_length() {
        let data = [3u8, 0, 0, 0];
        let err = parse_header(&mut data.as_slice()).unwrap_err();
        assert!(matches!(
            err.kind(),
            ErrorKind::InvalidHeaderLength {
                record_index: 1,
                total_size: 3
            }
        ));
    }

    #[test]
    fn missing_zones() {
        let data = make_header_bytes(
            b"banner",
            b"ID=test",
            b"PAR M=3",
            b"MVR 3\0A=0\0B=0\0C=0\0",
            b"ROW\0",
        );
        let err = parse_header(&mut data.as_slice()).unwrap_err();
        assert!(matches!(
            err.kind(),
            ErrorKind::InvalidPar(msg) if msg == "missing Zones= token"
        ));
    }

    #[test]
    fn table_count_mismatch() {
        let data = make_header_bytes(
            b"banner",
            b"ID=test",
            b"PAR Zones=10 M=2",
            b"MVR 3\0A=0\0B=0\0C=0\0",
            b"ROW\0",
        );
        let err = parse_header(&mut data.as_slice()).unwrap_err();
        assert!(matches!(
            err.kind(),
            ErrorKind::TableCountMismatch { par: 2, mvr: 3 }
        ));
    }

    #[test]
    fn mvr_rejects_extra_table_tokens() {
        let data = make_header_bytes(
            b"banner",
            b"ID=test",
            b"PAR Zones=10 M=1",
            b"MVR 1\0A=0\0B=0\0",
            b"ROW\0",
        );
        let err = parse_header(&mut data.as_slice()).unwrap_err();
        assert!(matches!(
            err.kind(),
            ErrorKind::InvalidMvr(msg) if msg == "expected 1 table tokens, got more"
        ));
    }

    #[test]
    fn mvr_rejects_invalid_type_code_token() {
        let data = make_header_bytes(
            b"banner",
            b"ID=test",
            b"PAR Zones=10 M=1",
            b"MVR 1\0A=Z\0",
            b"ROW\0",
        );
        let err = parse_header(&mut data.as_slice()).unwrap_err();
        assert!(matches!(
            err.kind(),
            ErrorKind::InvalidTypeCode { token } if token == "Z"
        ));
    }

    #[test]
    fn missing_row_marker() {
        let data = make_header_bytes(
            b"banner",
            b"ID=test",
            b"PAR Zones=10 M=1",
            b"MVR 1\0A=0\0",
            b"NOTROW",
        );
        let err = parse_header(&mut data.as_slice()).unwrap_err();
        assert!(matches!(err.kind(), ErrorKind::MissingRowMarker));
    }

    #[test]
    fn unexpected_eof() {
        let data = [10u8, 0, 0, 0, 1, 2];
        let err = parse_header(&mut data.as_slice()).unwrap_err();
        assert!(matches!(err.kind(), ErrorKind::UnexpectedEof));
    }

    #[test]
    fn par_with_trailing_nul() {
        let data = make_header_bytes(
            b"banner",
            b"ID=test",
            b"PAR Zones=50 M=1\0",
            b"MVR 1\0TAB=S\0",
            b"ROW\0",
        );
        let header = parse_header(&mut data.as_slice()).unwrap();
        assert_eq!(header.zone_count(), 50);
        assert_eq!(header.table_count(), 1);
        assert_eq!(header.tables()[0].name(), "TAB");
        assert_eq!(header.tables()[0].type_code(), TypeCode::Float32);
    }

    #[test]
    fn zero_zone_count() {
        let data = make_header_bytes(
            b"banner",
            b"ID=test",
            b"PAR Zones=0 M=1",
            b"MVR 1\0A=0\0",
            b"ROW\0",
        );
        let err = parse_header(&mut data.as_slice()).unwrap_err();
        assert!(matches!(
            err.kind(),
            ErrorKind::InvalidPar(msg) if msg == "Zones must be > 0"
        ));
    }

    #[test]
    fn zero_table_count() {
        let data = make_header_bytes(
            b"banner",
            b"ID=test",
            b"PAR Zones=10 M=0",
            b"MVR 0\0",
            b"ROW\0",
        );
        let err = parse_header(&mut data.as_slice()).unwrap_err();
        assert!(matches!(
            err.kind(),
            ErrorKind::InvalidPar(msg) if msg == "M must be > 0"
        ));
    }

    #[test]
    fn mvr_split_at_last_equals() {
        let data = make_header_bytes(
            b"banner",
            b"ID=test",
            b"PAR Zones=5 M=1",
            b"MVR 1\0A=B=S\0",
            b"ROW\0",
        );
        let header = parse_header(&mut data.as_slice()).unwrap();
        assert_eq!(header.tables()[0].name(), "A=B");
        assert_eq!(header.tables()[0].type_code(), TypeCode::Float32);
    }

    #[test]
    fn write_nul_terminates_banner_and_run_id_records() {
        let header = Header::new(
            "Banner".to_owned(),
            "Run".to_owned(),
            2,
            vec![TableInfo::new(1, "T".to_owned(), TypeCode::Fixed(2))],
        );

        let mut out = Vec::new();
        header.write_to(&mut out).unwrap();
        let records = split_records(&out, 5);

        assert_eq!(records[0], b"Banner\0");
        assert_eq!(records[1], b"ID=Run\0");
    }
}
