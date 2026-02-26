//! WebAssembly codec bindings for despina.
//!
//! Exposes `decode`, `decodeTables`, and `encode` free functions. The JS
//! wrapper (`matrix.js`) owns the data as `Float64Array`s and provides the
//! user-facing `Matrix` class.

use wasm_bindgen::JsCast;
use wasm_bindgen::prelude::*;

use despina::{MatrixBuilder, TypeCode};

fn to_js_error(error: despina::Error) -> JsValue {
    let message = error.to_string();
    let js_error = js_sys::Error::new(&message);
    let kind = error_kind_name(error.kind());
    js_sys::Reflect::set(&js_error, &"kind".into(), &kind.into()).ok();
    if let Some(offset) = error.offset() {
        js_sys::Reflect::set(
            &js_error,
            &"offset".into(),
            &JsValue::from_f64(offset as f64),
        )
        .ok();
    }
    js_error.into()
}

fn error_kind_name(kind: &despina::ErrorKind) -> &'static str {
    match kind {
        despina::ErrorKind::Io(_) => "io",
        despina::ErrorKind::InvalidHeaderLength { .. } => "invalid_header_length",
        despina::ErrorKind::InvalidPar(_) => "invalid_par",
        despina::ErrorKind::InvalidMvr(_) => "invalid_mvr",
        despina::ErrorKind::InvalidTypeCode { .. } => "invalid_type_code",
        despina::ErrorKind::MissingRowMarker => "missing_row_marker",
        despina::ErrorKind::TableCountMismatch { .. } => "table_count_mismatch",
        despina::ErrorKind::InvalidRowIndex { .. } => "invalid_row_index",
        despina::ErrorKind::RowOrderViolation { .. } => "row_order_violation",
        despina::ErrorKind::InvalidPreamble { .. } => "invalid_preamble",
        despina::ErrorKind::InvalidDescriptor { .. } => "invalid_descriptor",
        despina::ErrorKind::PlaneSize { .. } => "plane_size",
        despina::ErrorKind::ZeroRunCount => "zero_run_count",
        despina::ErrorKind::InvalidFloat32Marker { .. } => "invalid_float32_marker",
        despina::ErrorKind::InvalidChunkSize(_) => "invalid_chunk_size",
        despina::ErrorKind::TrailingBytes => "trailing_bytes",
        despina::ErrorKind::TableNotFound(_) => "table_not_found",
        despina::ErrorKind::TableIndexOutOfRange { .. } => "table_index_out_of_range",
        despina::ErrorKind::UnexpectedEof => "unexpected_eof",
        despina::ErrorKind::InvalidZoneCount => "invalid_zone_count",
        despina::ErrorKind::InvalidTableCount(_) => "invalid_table_count",
        despina::ErrorKind::ZoneCountMismatch { .. } => "zone_count_mismatch",
        despina::ErrorKind::ShapeMismatch { .. } => "shape_mismatch",
        despina::ErrorKind::InvalidTableName(_) => "invalid_table_name",
        despina::ErrorKind::PayloadTooLarge(_) => "payload_too_large",
        despina::ErrorKind::WriterFinished => "writer_finished",
        despina::ErrorKind::WriterPositionMismatch { .. } => "writer_position_mismatch",
        despina::ErrorKind::IncompleteMatrix { .. } => "incomplete_matrix",
        _ => "unknown",
    }
}

fn parse_type_code(token: &str) -> Result<TypeCode, JsValue> {
    TypeCode::from_ascii(token).ok_or_else(|| {
        js_error(
            &format!("invalid type code \"{token}\", expected '0'..'9', 'S', or 'D'"),
            "invalid_type_code",
        )
    })
}

fn js_error(message: &str, kind: &str) -> JsValue {
    let error = js_sys::Error::new(message);
    js_sys::Reflect::set(&error, &"kind".into(), &kind.into()).ok();
    error.into()
}

fn build_from_params(
    zone_count: u16,
    table_names: &[String],
    type_codes: &[String],
    banner: Option<String>,
    run_id: Option<String>,
) -> Result<MatrixBuilder, JsValue> {
    if table_names.len() != type_codes.len() {
        return Err(js_error(
            "tableNames and typeCodes must have the same length",
            "length_mismatch",
        ));
    }

    let mut builder = MatrixBuilder::try_new(zone_count).map_err(to_js_error)?;
    if let Some(banner) = banner {
        builder = builder.banner(banner);
    }
    if let Some(run_id) = run_id {
        builder = builder.run_id(run_id);
    }
    for (name, code) in table_names.iter().zip(type_codes.iter()) {
        builder = builder.table(name.clone(), parse_type_code(code)?);
    }
    Ok(builder)
}

/// Result of schema validation. Contains the effective banner and run ID that
/// the builder would produce (including defaults when not specified).
#[wasm_bindgen(inspectable)]
pub struct SchemaResult {
    banner: String,
    run_id: String,
}

#[wasm_bindgen]
impl SchemaResult {
    /// Effective banner text.
    #[wasm_bindgen(getter)]
    pub fn banner(&self) -> String {
        self.banner.clone()
    }

    /// Effective run identifier.
    #[wasm_bindgen(getter, js_name = runId)]
    pub fn run_id(&self) -> String {
        self.run_id.clone()
    }
}

/// Validate a matrix schema without allocating any cell data.
///
/// Runs the same validation as `encode` (table count, name validity, type code
/// validity) but returns only the effective header metadata. Use this to
/// validate `Matrix.create()` without the cost of building and encoding a full
/// zero-filled matrix.
#[wasm_bindgen(js_name = validateSchema)]
pub fn validate_schema(
    zone_count: u16,
    table_names: Vec<String>,
    type_codes: Vec<String>,
    banner: Option<String>,
    run_id: Option<String>,
) -> Result<SchemaResult, JsValue> {
    let builder = build_from_params(zone_count, &table_names, &type_codes, banner, run_id)?;
    let header = builder.validate().map_err(to_js_error)?;
    Ok(SchemaResult {
        banner: header.banner().to_owned(),
        run_id: header.run_id().to_owned(),
    })
}

/// Result of decoding a `.mat` file. Call `tableData(name)` to extract each
/// table's data as a flat `Float64Array`, then let this object be collected.
#[wasm_bindgen(inspectable)]
pub struct DecodeResult {
    inner: despina::Matrix,
}

#[wasm_bindgen]
impl DecodeResult {
    /// Number of zones (square dimension).
    #[wasm_bindgen(getter, js_name = zoneCount)]
    pub fn zone_count(&self) -> u16 {
        self.inner.zone_count()
    }

    /// Number of tables.
    #[wasm_bindgen(getter, js_name = tableCount)]
    pub fn table_count(&self) -> u8 {
        self.inner.table_count()
    }

    /// Banner text from the file header.
    #[wasm_bindgen(getter)]
    pub fn banner(&self) -> String {
        self.inner.header().banner().to_owned()
    }

    /// Run identifier from the file header.
    #[wasm_bindgen(getter, js_name = runId)]
    pub fn run_id(&self) -> String {
        self.inner.header().run_id().to_owned()
    }

    /// Table names in header order.
    #[wasm_bindgen(getter, js_name = tableNames)]
    pub fn table_names(&self) -> Vec<String> {
        self.inner
            .header()
            .tables()
            .iter()
            .map(|t| t.name().to_owned())
            .collect()
    }

    /// Table type code tokens in header order.
    #[wasm_bindgen(getter, js_name = tableTypeCodes)]
    pub fn table_type_codes(&self) -> Vec<String> {
        self.inner
            .header()
            .tables()
            .iter()
            .map(|t| t.type_code().to_string())
            .collect()
    }

    /// Extract flat row-major `f64` data for a single table by name.
    #[wasm_bindgen(js_name = tableData)]
    pub fn table_data(&self, name: &str) -> Result<Vec<f64>, JsValue> {
        let index = self
            .inner
            .header()
            .table_index_by_name(name)
            .ok_or_else(|| {
                js_error(
                    &format!("table \"{name}\" not found in matrix header"),
                    "table_not_found",
                )
            })?;
        let table = self
            .inner
            .try_table_by_index(index)
            .ok_or_else(|| js_error("table index out of bounds", "table_index_out_of_range"))?;
        Ok(table.as_slice().to_vec())
    }
}

/// Decode a `.mat` file from raw bytes, loading all tables.
#[wasm_bindgen]
pub fn decode(bytes: &[u8]) -> Result<DecodeResult, JsValue> {
    let inner = despina::Matrix::from_bytes(bytes).map_err(to_js_error)?;
    Ok(DecodeResult { inner })
}

/// Decode a `.mat` file from raw bytes, loading only the named tables.
#[wasm_bindgen(js_name = decodeTables)]
pub fn decode_tables(bytes: &[u8], table_names: Vec<String>) -> Result<DecodeResult, JsValue> {
    let refs: Vec<&str> = table_names.iter().map(|s| s.as_str()).collect();
    let inner = despina::Matrix::from_bytes_tables(bytes, &refs).map_err(to_js_error)?;
    Ok(DecodeResult { inner })
}

/// Encode a matrix to `.mat` bytes.
///
/// `data` is a JS object mapping table names to `Float64Array` values, each of
/// length `zone_count * zone_count`.
#[wasm_bindgen]
pub fn encode(
    zone_count: u16,
    table_names: Vec<String>,
    type_codes: Vec<String>,
    data: &js_sys::Object,
    banner: Option<String>,
    run_id: Option<String>,
) -> Result<Vec<u8>, JsValue> {
    let builder = build_from_params(zone_count, &table_names, &type_codes, banner, run_id)?;
    let mut matrix = builder.build().map_err(to_js_error)?;

    let expected_size = usize::from(zone_count) * usize::from(zone_count);

    for (table_offset, name) in table_names.iter().enumerate() {
        let js_key = JsValue::from_str(name);
        let js_val = js_sys::Reflect::get(data, &js_key).map_err(|_| {
            js_error(
                &format!("could not read property \"{name}\" from data object"),
                "missing_table_data",
            )
        })?;

        if js_val.is_undefined() {
            return Err(js_error(
                &format!("data object is missing table \"{name}\""),
                "missing_table_data",
            ));
        }

        if !js_val.is_instance_of::<js_sys::Float64Array>() {
            return Err(js_error(
                &format!("table \"{name}\" data must be a Float64Array"),
                "type_error",
            ));
        }
        let typed_array = js_sys::Float64Array::unchecked_from_js(js_val);
        let length = typed_array.length() as usize;
        if length != expected_size {
            return Err(js_error(
                &format!("table \"{name}\" has {length} values, expected {expected_size}"),
                "shape_mismatch",
            ));
        }

        let table_index = u8::try_from(table_offset + 1)
            .map_err(|_| js_error("table index overflow", "table_index_out_of_range"))?;
        let dst = matrix.table_data_mut(table_index);
        typed_array.copy_to(dst);
    }

    let mut bytes = Vec::new();
    matrix.write_to_writer(&mut bytes).map_err(to_js_error)?;
    Ok(bytes)
}
