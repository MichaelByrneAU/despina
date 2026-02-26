use std::collections::HashMap;

use color_eyre::eyre::{Result, bail};
use despina::MatrixBuilder;

use crate::cli::ImportArgs;
use crate::parse::TypeCodeSpecs;

pub fn build_matrix(
    cells: &HashMap<(u16, u16), Vec<f64>>,
    max_zone: u16,
    table_names: &[String],
    type_specs: &TypeCodeSpecs,
    import: &ImportArgs,
) -> Result<()> {
    let zone_count = import.zone_count.unwrap_or(max_zone);
    if zone_count < max_zone {
        bail!(
            "specified zone count ({zone_count}) is less than the maximum zone ID ({max_zone}) \
             found in the input"
        );
    }

    let expected_pair_count = u64::from(zone_count) * u64::from(zone_count);
    let present_pair_count = cells.len() as u64;
    if present_pair_count < expected_pair_count {
        let missing_count = expected_pair_count - present_pair_count;
        let message = format!(
            "{missing_count} of {expected_pair_count} OD pairs missing from input \
             (missing pairs default to 0.0)"
        );
        if import.strict {
            bail!("{message}");
        } else {
            eprintln!("warning: {message}");
        }
    }

    for name in type_specs.per_table.keys() {
        if !table_names.contains(name) {
            bail!(
                "type code override for table `{name}` but no such table exists \
                 (available: {})",
                table_names.join(", ")
            );
        }
    }

    let mut builder = MatrixBuilder::new(zone_count);
    for name in table_names {
        let type_code = type_specs
            .per_table
            .get(name)
            .copied()
            .unwrap_or(type_specs.default);
        builder = builder.table(name, type_code);
    }
    if let Some(ref banner) = import.banner {
        builder = builder.banner(banner);
    }
    if let Some(ref run_id) = import.run_id {
        builder = builder.run_id(run_id);
    }
    let mut matrix = builder.build()?;

    for ((origin, destination), values) in cells {
        for (table_slot, &value) in values.iter().enumerate() {
            let table_index = (table_slot + 1) as u8;
            matrix.set(table_index, *origin, *destination, value);
        }
    }

    matrix.write_to(&import.output)?;
    Ok(())
}
