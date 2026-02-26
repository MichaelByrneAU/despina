//! Validates table definitions and converts them into header table metadata.

use crate::MAX_TABLE_COUNT;
use crate::error::{Error, ErrorKind, Result};
use crate::header::TableInfo;
use crate::matrix::TableDef;
use crate::types::TypeCode;

/// Validates table definitions.
///
/// Rules:
/// - Table count is within `1..=[crate::MAX_TABLE_COUNT]`.
/// - Each table name is non-empty and ASCII-only.
/// - Each table type code is representable in `.mat`.
///
/// Duplicate table names are allowed.
///
/// # Errors
///
/// Returns:
///
/// - [`ErrorKind::InvalidTableCount`] when the list is empty or too long.
/// - [`ErrorKind::InvalidTableName`] for empty or non-ASCII names.
/// - [`ErrorKind::InvalidTypeCode`] for unsupported type codes.
pub(crate) fn validate_table_defs(table_defs: &[TableDef]) -> Result<()> {
    if table_defs.is_empty() {
        return Err(Error::new(ErrorKind::InvalidTableCount(
            "at least one table is required".into(),
        )));
    }
    if table_defs.len() > usize::from(MAX_TABLE_COUNT) {
        return Err(Error::new(ErrorKind::InvalidTableCount(format!(
            "at most {} tables allowed, got {}",
            MAX_TABLE_COUNT,
            table_defs.len()
        ))));
    }

    for table_def in table_defs {
        let name = table_def.name();
        if name.is_empty() {
            return Err(Error::new(ErrorKind::InvalidTableName(
                "table name must not be empty".into(),
            )));
        }
        if !name.is_ascii() {
            return Err(Error::new(ErrorKind::InvalidTableName(format!(
                "table name {:?} contains non-ASCII characters",
                name
            ))));
        }

        let type_code = table_def.type_code();
        if !type_code.is_valid_mat_code() {
            return Err(Error::new(ErrorKind::InvalidTypeCode {
                token: invalid_type_code_token(type_code),
            }));
        }
    }

    Ok(())
}

/// Converts validated table definitions into header table metadata.
///
/// Preserves input order and assigns contiguous 1-based table indices.
pub(crate) fn table_infos_from_defs(table_defs: &[TableDef]) -> Vec<TableInfo> {
    debug_assert!(table_defs.len() <= usize::from(MAX_TABLE_COUNT));
    let mut table_infos = Vec::with_capacity(table_defs.len());
    for (table_offset, table_def) in table_defs.iter().enumerate() {
        table_infos.push(TableInfo::new(
            (table_offset + 1) as u8,
            table_def.name().to_owned(),
            table_def.type_code(),
        ));
    }
    table_infos
}

fn invalid_type_code_token(type_code: TypeCode) -> String {
    match type_code {
        TypeCode::Fixed(decimal_places) => decimal_places.to_string(),
        TypeCode::Float32 => "S".to_owned(),
        TypeCode::Float64 => "D".to_owned(),
    }
}
