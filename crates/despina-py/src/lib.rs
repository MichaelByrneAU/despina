//! Thin Rust core bindings for the Python package.
//!
//! The public Python package in `python/despina/` wraps this module.
//! This module exposes `_MatrixCore` for parse/write and matrix mutation.

use std::path::PathBuf;

use despina::{ErrorKind, Header, Matrix, MatrixBuilder, TableDef as RustTableDef, TypeCode};
use numpy::ndarray::{Array2, Array3};
use numpy::{
    IntoPyArray, PyArray2, PyArray3, PyReadonlyArray2, PyReadonlyArray3, PyUntypedArrayMethods,
};
use pyo3::create_exception;
use pyo3::exceptions::{PyException, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBool, PyBytes, PyModule};

create_exception!(despina, DespinaError, PyException);
create_exception!(despina, DespinaIoError, DespinaError);
create_exception!(despina, DespinaParseError, DespinaError);
create_exception!(despina, DespinaValidationError, DespinaError);
create_exception!(despina, DespinaWriterError, DespinaError);

#[pyclass(name = "TableDef", module = "despina._despina", skip_from_py_object)]
#[derive(Debug, Clone)]
struct PyTableDef {
    name: String,
    type_code: TypeCode,
}

#[pymethods]
impl PyTableDef {
    #[new]
    #[pyo3(text_signature = "(name, type_code, /)")]
    fn new(name: String, type_code: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(Self {
            name,
            type_code: parse_type_code(type_code)?,
        })
    }

    #[getter]
    fn name(&self) -> &str {
        &self.name
    }

    #[getter]
    fn type_code(&self) -> String {
        self.type_code.to_string()
    }

    fn __repr__(&self) -> String {
        format!(
            "TableDef(name={:?}, type_code={:?})",
            self.name,
            self.type_code.to_string()
        )
    }
}

#[pyclass(
    name = "TableInfo",
    module = "despina._despina",
    frozen,
    skip_from_py_object
)]
#[derive(Debug, Clone)]
struct PyTableInfo {
    #[pyo3(get)]
    index: u8,
    #[pyo3(get)]
    name: String,
    #[pyo3(get)]
    type_code: String,
}

#[pymethods]
impl PyTableInfo {
    fn __repr__(&self) -> String {
        format!(
            "TableInfo(index={}, name={:?}, type_code={:?})",
            self.index, self.name, self.type_code
        )
    }
}

#[pyclass(name = "SchemaResult", module = "despina._despina", frozen)]
struct PySchemaResult {
    #[pyo3(get)]
    zone_count: u16,
    #[pyo3(get)]
    banner: String,
    #[pyo3(get)]
    run_id: String,
    table_infos: Vec<PyTableInfo>,
}

impl PySchemaResult {
    fn from_header(header: &Header) -> Self {
        Self {
            zone_count: header.zone_count(),
            banner: header.banner().to_owned(),
            run_id: header.run_id().to_owned(),
            table_infos: table_infos_to_py(header),
        }
    }
}

#[pymethods]
impl PySchemaResult {
    #[pyo3(text_signature = "($self, /)")]
    fn tables(&self) -> Vec<PyTableInfo> {
        self.table_infos.clone()
    }

    fn __repr__(&self) -> String {
        format!(
            "SchemaResult(zone_count={}, table_count={})",
            self.zone_count,
            self.table_infos.len()
        )
    }
}

#[pyclass(name = "_MatrixCore", module = "despina._despina", skip_from_py_object)]
#[derive(Debug)]
struct PyMatrixCore {
    inner: Matrix,
}

#[pymethods]
impl PyMatrixCore {
    #[staticmethod]
    #[pyo3(text_signature = "(path, /)")]
    fn open(py: Python<'_>, path: PathBuf) -> PyResult<Self> {
        let inner = py.detach(|| Matrix::open(path)).map_err(to_py_error)?;
        Ok(Self { inner })
    }

    #[staticmethod]
    #[pyo3(text_signature = "(bytes, /)")]
    fn from_bytes(py: Python<'_>, bytes: &[u8]) -> PyResult<Self> {
        let owned = bytes.to_vec();
        let inner = py
            .detach(|| Matrix::from_bytes(&owned))
            .map_err(to_py_error)?;
        Ok(Self { inner })
    }

    #[staticmethod]
    #[pyo3(text_signature = "(path, table_names, /)")]
    fn open_tables(py: Python<'_>, path: PathBuf, table_names: Vec<String>) -> PyResult<Self> {
        let inner = py
            .detach(|| {
                let names: Vec<&str> = table_names.iter().map(|s| s.as_str()).collect();
                Matrix::open_tables(path, &names)
            })
            .map_err(to_py_error)?;
        Ok(Self { inner })
    }

    #[staticmethod]
    #[pyo3(text_signature = "(bytes, table_names, /)")]
    fn from_bytes_tables(py: Python<'_>, bytes: &[u8], table_names: Vec<String>) -> PyResult<Self> {
        let owned = bytes.to_vec();
        let inner = py
            .detach(|| {
                let names: Vec<&str> = table_names.iter().map(|s| s.as_str()).collect();
                Matrix::from_bytes_tables(&owned, &names)
            })
            .map_err(to_py_error)?;
        Ok(Self { inner })
    }

    #[staticmethod]
    #[pyo3(
        signature = (zone_count, tables, banner=None, run_id=None),
        text_signature = "(zone_count, tables, banner=None, run_id=None)"
    )]
    fn create(
        zone_count: u16,
        tables: &Bound<'_, PyAny>,
        banner: Option<String>,
        run_id: Option<String>,
    ) -> PyResult<Self> {
        let table_defs = parse_table_defs(tables)?;
        let mut builder = MatrixBuilder::try_new(zone_count).map_err(to_py_error)?;

        if let Some(text) = banner.as_deref() {
            builder = builder.banner(text);
        }
        if let Some(id) = run_id.as_deref() {
            builder = builder.run_id(id);
        }
        for table in table_defs {
            builder = builder.table(table.name().to_owned(), table.type_code());
        }

        let inner = builder.build().map_err(to_py_error)?;
        Ok(Self { inner })
    }

    #[staticmethod]
    #[pyo3(
        signature = (zone_count, tables, banner=None, run_id=None),
        text_signature = "(zone_count, tables, banner=None, run_id=None)"
    )]
    fn validate_schema(
        zone_count: u16,
        tables: &Bound<'_, PyAny>,
        banner: Option<String>,
        run_id: Option<String>,
    ) -> PyResult<PySchemaResult> {
        let table_defs = parse_table_defs(tables)?;
        let mut builder = MatrixBuilder::try_new(zone_count).map_err(to_py_error)?;
        if let Some(text) = banner.as_deref() {
            builder = builder.banner(text);
        }
        if let Some(id) = run_id.as_deref() {
            builder = builder.run_id(id);
        }
        for table in table_defs {
            builder = builder.table(table.name().to_owned(), table.type_code());
        }
        let header = builder.validate().map_err(to_py_error)?;
        Ok(PySchemaResult::from_header(&header))
    }

    #[getter]
    fn zone_count(&self) -> u16 {
        self.inner.zone_count()
    }

    #[getter]
    fn table_count(&self) -> u8 {
        self.inner.table_count()
    }

    #[getter]
    fn banner(&self) -> &str {
        self.inner.header().banner()
    }

    #[getter]
    fn run_id(&self) -> &str {
        self.inner.header().run_id()
    }

    #[pyo3(text_signature = "($self, /)")]
    fn tables(&self) -> Vec<PyTableInfo> {
        table_infos_to_py(self.inner.header())
    }

    #[pyo3(text_signature = "($self, name, /)")]
    fn table_index_by_name(&self, name: &str) -> Option<u8> {
        self.inner.header().table_index_by_name(name)
    }

    #[pyo3(text_signature = "($self, table, origin, destination, /)")]
    fn checked_get(
        &self,
        table: &Bound<'_, PyAny>,
        origin: u16,
        destination: u16,
    ) -> PyResult<Option<f64>> {
        let table_index = resolve_table_index(&self.inner, table)?;
        Ok(self.inner.checked_get(table_index, origin, destination))
    }

    #[pyo3(text_signature = "($self, table, origin, destination, /)")]
    fn get(&self, table: &Bound<'_, PyAny>, origin: u16, destination: u16) -> PyResult<f64> {
        let table_index = resolve_table_index(&self.inner, table)?;
        self.inner
            .checked_get(table_index, origin, destination)
            .ok_or_else(|| {
                PyValueError::new_err(format!(
                    "origin and destination must be within 1..={} (got origin={}, destination={})",
                    self.inner.zone_count(),
                    origin,
                    destination
                ))
            })
    }

    #[pyo3(text_signature = "($self, table, origin, destination, value, /)")]
    fn set(
        &mut self,
        table: &Bound<'_, PyAny>,
        origin: u16,
        destination: u16,
        value: f64,
    ) -> PyResult<()> {
        let table_index = resolve_table_index(&self.inner, table)?;
        if self
            .inner
            .checked_get(table_index, origin, destination)
            .is_none()
        {
            return Err(PyValueError::new_err(format!(
                "origin and destination must be within 1..={} (got origin={}, destination={})",
                self.inner.zone_count(),
                origin,
                destination
            )));
        }
        self.inner.set(table_index, origin, destination, value);
        Ok(())
    }

    #[pyo3(text_signature = "($self, table, /)")]
    fn table_total(&self, table: &Bound<'_, PyAny>) -> PyResult<f64> {
        let table_index = resolve_table_index(&self.inner, table)?;
        Ok(self.inner.table_by_index(table_index).total())
    }

    #[pyo3(text_signature = "($self, table, /)")]
    fn table_diagonal_total(&self, table: &Bound<'_, PyAny>) -> PyResult<f64> {
        let table_index = resolve_table_index(&self.inner, table)?;
        Ok(self.inner.table_by_index(table_index).diagonal_total())
    }

    #[pyo3(text_signature = "($self, table, /)")]
    fn row(&self, table: &Bound<'_, PyAny>, origin: u16) -> PyResult<Vec<f64>> {
        let table_index = resolve_table_index(&self.inner, table)?;
        validate_zone_index(self.inner.zone_count(), "origin", origin)?;
        Ok(self.inner.row(table_index, origin).to_vec())
    }

    #[pyo3(text_signature = "($self, table, /)")]
    fn table_array<'py>(
        &self,
        py: Python<'py>,
        table: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyArray2<f64>>> {
        let table_index = resolve_table_index(&self.inner, table)?;
        let zone_count = usize::from(self.inner.zone_count());
        let src = self.inner.table_data(table_index);
        let array =
            Array2::from_shape_fn((zone_count, zone_count), |(r, c)| src[r * zone_count + c]);
        Ok(array.into_pyarray(py).unbind())
    }

    #[pyo3(text_signature = "($self, table, values, /)")]
    fn set_table_array(
        &mut self,
        table: &Bound<'_, PyAny>,
        values: PyReadonlyArray2<'_, f64>,
    ) -> PyResult<()> {
        let table_index = resolve_table_index(&self.inner, table)?;
        let zone_count = usize::from(self.inner.zone_count());
        let shape = values.shape();
        if shape != [zone_count, zone_count] {
            return Err(PyValueError::new_err(format!(
                "table array must have shape ({zone_count}, {zone_count}) (got {:?})",
                shape
            )));
        }

        let src = values.as_array();
        let dst = self.inner.table_data_mut(table_index);

        for (origin, row) in src.outer_iter().enumerate() {
            let start = origin * zone_count;
            let end = start + zone_count;
            let dst_row = &mut dst[start..end];

            if let Some(slice) = row.as_slice() {
                dst_row.copy_from_slice(slice);
            } else {
                for (destination, value) in row.iter().enumerate() {
                    dst_row[destination] = *value;
                }
            }
        }

        Ok(())
    }

    #[pyo3(text_signature = "($self, /)")]
    fn stack_array<'py>(&self, py: Python<'py>) -> PyResult<Py<PyArray3<f64>>> {
        let zone_count = usize::from(self.inner.zone_count());
        let table_count = usize::from(self.inner.table_count());
        let src = self.inner.data();
        let cells_per_table = zone_count * zone_count;
        let array = Array3::from_shape_fn((table_count, zone_count, zone_count), |(t, r, c)| {
            src[t * cells_per_table + r * zone_count + c]
        });
        Ok(array.into_pyarray(py).unbind())
    }

    #[pyo3(text_signature = "($self, values, /)")]
    fn set_stack_array(&mut self, values: PyReadonlyArray3<'_, f64>) -> PyResult<()> {
        let table_count = usize::from(self.inner.table_count());
        let zone_count = usize::from(self.inner.zone_count());
        let expected_shape = [table_count, zone_count, zone_count];
        let shape = values.shape();
        if shape != expected_shape {
            return Err(PyValueError::new_err(format!(
                "stack array must have shape ({table_count}, {zone_count}, {zone_count}) (got {:?})",
                shape
            )));
        }

        let src = values.as_array();
        let cells_per_table = zone_count * zone_count;

        if let Some(slice) = src.as_slice() {
            self.inner.data_mut().copy_from_slice(slice);
            return Ok(());
        }

        let dst = self.inner.data_mut();
        for table_offset in 0..table_count {
            for origin in 0..zone_count {
                for destination in 0..zone_count {
                    let index = table_offset * cells_per_table + origin * zone_count + destination;
                    dst[index] = src[[table_offset, origin, destination]];
                }
            }
        }

        Ok(())
    }

    #[pyo3(text_signature = "($self, path, /)")]
    fn write(&self, py: Python<'_>, path: PathBuf) -> PyResult<()> {
        let inner = &self.inner;
        py.detach(|| inner.write_to(path)).map_err(to_py_error)
    }

    #[pyo3(text_signature = "($self, /)")]
    fn to_bytes<'py>(&self, py: Python<'py>) -> PyResult<Py<PyBytes>> {
        let inner = &self.inner;
        let bytes = py
            .detach(|| {
                let mut buffer = Vec::new();
                inner.write_to_writer(&mut buffer).map(|()| buffer)
            })
            .map_err(to_py_error)?;
        Ok(PyBytes::new(py, &bytes).into())
    }

    fn __bytes__<'py>(&self, py: Python<'py>) -> PyResult<Py<PyBytes>> {
        self.to_bytes(py)
    }

    #[staticmethod]
    #[pyo3(
        signature = (path, zone_count, tables, stack, banner=None, run_id=None),
        text_signature = "(path, zone_count, tables, stack, banner=None, run_id=None)"
    )]
    fn write_from_stack(
        py: Python<'_>,
        path: PathBuf,
        zone_count: u16,
        tables: &Bound<'_, PyAny>,
        stack: PyReadonlyArray3<'_, f64>,
        banner: Option<String>,
        run_id: Option<String>,
    ) -> PyResult<()> {
        let matrix = build_matrix_from_stack(zone_count, tables, &stack, banner, run_id)?;
        py.detach(|| matrix.write_to(path)).map_err(to_py_error)
    }

    #[staticmethod]
    #[pyo3(
        signature = (zone_count, tables, stack, banner=None, run_id=None),
        text_signature = "(zone_count, tables, stack, banner=None, run_id=None)"
    )]
    fn to_bytes_from_stack<'py>(
        py: Python<'py>,
        zone_count: u16,
        tables: &Bound<'_, PyAny>,
        stack: PyReadonlyArray3<'_, f64>,
        banner: Option<String>,
        run_id: Option<String>,
    ) -> PyResult<Py<PyBytes>> {
        let matrix = build_matrix_from_stack(zone_count, tables, &stack, banner, run_id)?;
        let bytes = py
            .detach(|| {
                let mut buffer = Vec::new();
                matrix.write_to_writer(&mut buffer).map(|()| buffer)
            })
            .map_err(to_py_error)?;
        Ok(PyBytes::new(py, &bytes).into())
    }

    fn __repr__(&self) -> String {
        format!(
            "_MatrixCore(zone_count={}, table_count={})",
            self.inner.zone_count(),
            self.inner.table_count()
        )
    }
}

fn build_matrix_from_stack(
    zone_count: u16,
    tables: &Bound<'_, PyAny>,
    stack: &PyReadonlyArray3<'_, f64>,
    banner: Option<String>,
    run_id: Option<String>,
) -> PyResult<Matrix> {
    let table_defs = parse_table_defs(tables)?;
    let table_count = table_defs.len();
    let zc = usize::from(zone_count);
    let expected_shape = [table_count, zc, zc];
    let shape = stack.shape();
    if shape != expected_shape {
        return Err(PyValueError::new_err(format!(
            "stack array must have shape ({table_count}, {zc}, {zc}) (got {shape:?})"
        )));
    }

    let mut builder = MatrixBuilder::try_new(zone_count).map_err(to_py_error)?;
    if let Some(text) = banner.as_deref() {
        builder = builder.banner(text);
    }
    if let Some(id) = run_id.as_deref() {
        builder = builder.run_id(id);
    }
    for table in table_defs {
        builder = builder.table(table.name().to_owned(), table.type_code());
    }

    let mut matrix = builder.build().map_err(to_py_error)?;

    let src = stack.as_array();
    if let Some(slice) = src.as_slice() {
        matrix.data_mut().copy_from_slice(slice);
    } else {
        let cells_per_table = zc * zc;
        let dst = matrix.data_mut();
        for table_offset in 0..table_count {
            for origin in 0..zc {
                for destination in 0..zc {
                    let index = table_offset * cells_per_table + origin * zc + destination;
                    dst[index] = src[[table_offset, origin, destination]];
                }
            }
        }
    }

    Ok(matrix)
}

fn parse_table_defs(tables: &Bound<'_, PyAny>) -> PyResult<Vec<RustTableDef>> {
    let mut out = Vec::new();

    for (offset, item_result) in tables.try_iter()?.enumerate() {
        let item = item_result?;
        if let Ok(table_def) = item.extract::<PyRef<'_, PyTableDef>>() {
            out.push(RustTableDef::new(
                table_def.name.clone(),
                table_def.type_code,
            ));
            continue;
        }

        let (name, type_code_any): (String, Py<PyAny>) = item.extract().map_err(|_| {
            PyTypeError::new_err(format!(
                "tables[{offset}] must be TableDef or a (name, type_code) tuple"
            ))
        })?;

        let type_code = parse_type_code(type_code_any.bind(item.py()))?;
        out.push(RustTableDef::new(name, type_code));
    }

    Ok(out)
}

fn parse_type_code(value: &Bound<'_, PyAny>) -> PyResult<TypeCode> {
    if value.is_instance_of::<PyBool>() {
        return Err(PyTypeError::new_err(
            "type code must be int 0..9 or string '0'..'9', 'S', or 'D'",
        ));
    }

    if let Ok(token) = value.extract::<String>() {
        if let Some(type_code) = TypeCode::from_ascii(&token) {
            return Ok(type_code);
        }
        return Err(PyValueError::new_err(format!(
            "invalid type code {token:?}; expected '0'..'9', 'S', or 'D'"
        )));
    }

    if let Ok(decimal_places) = value.extract::<u8>() {
        if decimal_places <= 9 {
            return Ok(TypeCode::Fixed(decimal_places));
        }
        return Err(PyValueError::new_err(format!(
            "invalid fixed-point precision {decimal_places}; expected 0..9"
        )));
    }

    Err(PyTypeError::new_err(
        "type code must be int 0..9 or string '0'..'9', 'S', or 'D'",
    ))
}

fn table_infos_to_py(header: &Header) -> Vec<PyTableInfo> {
    header
        .tables()
        .iter()
        .map(|table| PyTableInfo {
            index: table.index(),
            name: table.name().to_owned(),
            type_code: table.type_code().to_string(),
        })
        .collect()
}

fn resolve_table_index(matrix: &Matrix, table: &Bound<'_, PyAny>) -> PyResult<u8> {
    resolve_table_index_by_header(matrix.header(), table)
}

fn resolve_table_index_by_header(header: &Header, table: &Bound<'_, PyAny>) -> PyResult<u8> {
    if let Ok(name) = table.extract::<String>() {
        return header.table_index_by_name(&name).ok_or_else(|| {
            PyValueError::new_err(format!("table {name:?} not found in matrix header"))
        });
    }

    let table_index = table.extract::<u8>().map_err(|_| {
        PyTypeError::new_err("table must be a table name (str) or 1-based index (int)")
    })?;
    validate_table_index(header.table_count(), table_index)?;
    Ok(table_index)
}

fn validate_table_index(table_count: u8, table_index: u8) -> PyResult<()> {
    if table_index == 0 || table_index > table_count {
        return Err(PyValueError::new_err(format!(
            "table index must be within 1..={} (got {})",
            table_count, table_index
        )));
    }
    Ok(())
}

fn validate_zone_index(zone_count: u16, label: &str, zone_index: u16) -> PyResult<()> {
    if zone_index == 0 || zone_index > zone_count {
        return Err(PyValueError::new_err(format!(
            "{label} must be within 1..={} (got {})",
            zone_count, zone_index
        )));
    }
    Ok(())
}

fn to_py_error(error: despina::Error) -> PyErr {
    Python::attach(|py| {
        let kind_name = py_error_kind_name(error.kind());
        let offset = error.offset();
        let message = error.to_string();
        let py_error = match py_error_category(error.kind()) {
            PyErrorCategory::Io => DespinaIoError::new_err(message),
            PyErrorCategory::Parse => DespinaParseError::new_err(message),
            PyErrorCategory::Validation => DespinaValidationError::new_err(message),
            PyErrorCategory::Writer => DespinaWriterError::new_err(message),
        };

        let value = py_error.value(py);
        let _ = value.setattr("kind", kind_name);
        let _ = value.setattr("offset", offset);
        py_error
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PyErrorCategory {
    Io,
    Parse,
    Validation,
    Writer,
}

fn py_error_category(kind: &ErrorKind) -> PyErrorCategory {
    match kind {
        ErrorKind::Io(_) => PyErrorCategory::Io,
        ErrorKind::InvalidHeaderLength { .. }
        | ErrorKind::InvalidPar(_)
        | ErrorKind::InvalidMvr(_)
        | ErrorKind::MissingRowMarker
        | ErrorKind::TableCountMismatch { .. }
        | ErrorKind::InvalidRowIndex { .. }
        | ErrorKind::RowOrderViolation { .. }
        | ErrorKind::InvalidPreamble { .. }
        | ErrorKind::InvalidDescriptor { .. }
        | ErrorKind::PlaneSize { .. }
        | ErrorKind::ZeroRunCount
        | ErrorKind::InvalidFloat32Marker { .. }
        | ErrorKind::InvalidChunkSize(_)
        | ErrorKind::TrailingBytes
        | ErrorKind::UnexpectedEof => PyErrorCategory::Parse,
        ErrorKind::PayloadTooLarge(_)
        | ErrorKind::WriterFinished
        | ErrorKind::WriterPositionMismatch { .. }
        | ErrorKind::IncompleteMatrix { .. } => PyErrorCategory::Writer,
        ErrorKind::InvalidTypeCode { .. }
        | ErrorKind::TableNotFound(_)
        | ErrorKind::TableIndexOutOfRange { .. }
        | ErrorKind::InvalidZoneCount
        | ErrorKind::InvalidTableCount(_)
        | ErrorKind::ZoneCountMismatch { .. }
        | ErrorKind::ShapeMismatch { .. }
        | ErrorKind::InvalidTableName(_) => PyErrorCategory::Validation,
        _ => PyErrorCategory::Validation,
    }
}

fn py_error_kind_name(kind: &ErrorKind) -> &'static str {
    match kind {
        ErrorKind::Io(_) => "io",
        ErrorKind::InvalidHeaderLength { .. } => "invalid_header_length",
        ErrorKind::InvalidPar(_) => "invalid_par",
        ErrorKind::InvalidMvr(_) => "invalid_mvr",
        ErrorKind::InvalidTypeCode { .. } => "invalid_type_code",
        ErrorKind::MissingRowMarker => "missing_row_marker",
        ErrorKind::TableCountMismatch { .. } => "table_count_mismatch",
        ErrorKind::InvalidRowIndex { .. } => "invalid_row_index",
        ErrorKind::RowOrderViolation { .. } => "row_order_violation",
        ErrorKind::InvalidPreamble { .. } => "invalid_preamble",
        ErrorKind::InvalidDescriptor { .. } => "invalid_descriptor",
        ErrorKind::PlaneSize { .. } => "plane_size",
        ErrorKind::ZeroRunCount => "zero_run_count",
        ErrorKind::InvalidFloat32Marker { .. } => "invalid_float32_marker",
        ErrorKind::InvalidChunkSize(_) => "invalid_chunk_size",
        ErrorKind::TrailingBytes => "trailing_bytes",
        ErrorKind::TableNotFound(_) => "table_not_found",
        ErrorKind::TableIndexOutOfRange { .. } => "table_index_out_of_range",
        ErrorKind::UnexpectedEof => "unexpected_eof",
        ErrorKind::InvalidZoneCount => "invalid_zone_count",
        ErrorKind::InvalidTableCount(_) => "invalid_table_count",
        ErrorKind::ZoneCountMismatch { .. } => "zone_count_mismatch",
        ErrorKind::ShapeMismatch { .. } => "shape_mismatch",
        ErrorKind::InvalidTableName(_) => "invalid_table_name",
        ErrorKind::PayloadTooLarge(_) => "payload_too_large",
        ErrorKind::WriterFinished => "writer_finished",
        ErrorKind::WriterPositionMismatch { .. } => "writer_position_mismatch",
        ErrorKind::IncompleteMatrix { .. } => "incomplete_matrix",
        _ => "unknown",
    }
}

#[pymodule]
#[pyo3(name = "_despina")]
fn despina_py(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add(
        "__doc__",
        "Low-level core bindings for despina.\n\nImport the public `despina` package for normal use.",
    )?;

    let exception_type = module.py().get_type::<DespinaError>();
    exception_type.setattr(
        "__doc__",
        "Base exception for despina parse, validation, and write failures.",
    )?;
    module.add("DespinaError", exception_type)?;
    module.add("DespinaIoError", module.py().get_type::<DespinaIoError>())?;
    module.add(
        "DespinaParseError",
        module.py().get_type::<DespinaParseError>(),
    )?;
    module.add(
        "DespinaValidationError",
        module.py().get_type::<DespinaValidationError>(),
    )?;
    module.add(
        "DespinaWriterError",
        module.py().get_type::<DespinaWriterError>(),
    )?;

    module.add_class::<PyTableDef>()?;
    module.add_class::<PyTableInfo>()?;
    module.add_class::<PySchemaResult>()?;
    module.add_class::<PyMatrixCore>()?;

    Ok(())
}
