use std::collections::HashMap;

use despina::TypeCode;

pub fn parse_type_code(token: &str) -> color_eyre::Result<TypeCode> {
    TypeCode::from_ascii(token)
        .or_else(|| TypeCode::from_ascii(&token.to_ascii_uppercase()))
        .ok_or_else(|| {
            color_eyre::eyre::eyre!("invalid type code `{token}` (expected 0-9, S, or D)")
        })
}

pub struct TypeCodeSpecs {
    pub default: TypeCode,
    pub per_table: HashMap<String, TypeCode>,
}

pub fn parse_type_code_specs(specs: &[String]) -> color_eyre::Result<TypeCodeSpecs> {
    let mut default: Option<TypeCode> = None;
    let mut per_table: HashMap<String, TypeCode> = HashMap::new();

    for spec in specs {
        if let Some((name, code)) = spec.rsplit_once(':') {
            if name.is_empty() {
                color_eyre::eyre::bail!(
                    "invalid type code spec `{spec}` (table name must not be empty)"
                );
            }
            let type_code = parse_type_code(code)?;
            per_table.insert(name.to_owned(), type_code);
        } else {
            let type_code = parse_type_code(spec)?;
            if default.is_some() {
                color_eyre::eyre::bail!(
                    "multiple bare type codes provided; use TABLE:CODE for per-table overrides"
                );
            }
            default = Some(type_code);
        }
    }

    Ok(TypeCodeSpecs {
        default: default.unwrap_or(TypeCode::Float64),
        per_table,
    })
}

pub fn parse_rename_specs(specs: &[String]) -> color_eyre::Result<HashMap<String, String>> {
    let mut renames: HashMap<String, String> = HashMap::new();

    for spec in specs {
        let (original, new_name) = spec.split_once(':').ok_or_else(|| {
            color_eyre::eyre::eyre!("invalid rename spec `{spec}` (expected ORIGINAL:NEW format)")
        })?;
        if original.is_empty() || new_name.is_empty() {
            color_eyre::eyre::bail!("invalid rename spec `{spec}` (neither name may be empty)");
        }
        renames.insert(original.to_owned(), new_name.to_owned());
    }

    Ok(renames)
}

pub fn parse_separator(input: &str) -> color_eyre::Result<u8> {
    match input {
        "tab" | "\\t" | "\t" => Ok(b'\t'),
        s if s.len() == 1 => Ok(s.as_bytes()[0]),
        _ => color_eyre::eyre::bail!(
            "invalid separator `{input}` (expected a single character or `tab`)"
        ),
    }
}
