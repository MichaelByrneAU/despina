use std::fs::File;
use std::sync::Arc;

use arrow::array::{Float64Array, UInt32Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use color_eyre::eyre::Result;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

use crate::cli::{ParquetCompression, ToParquetArgs};
use crate::wide;

pub fn run(args: ToParquetArgs) -> Result<()> {
    let export = &args.export;
    let matrix = despina::open(&export.file)?;
    let schema = wide::resolve_tables(&matrix, &export.tables)?;
    let rows = wide::wide_rows(&matrix, &schema, export.include_zero_rows);

    let mut fields = vec![
        Field::new(&export.origin_col, DataType::UInt32, false),
        Field::new(&export.destination_col, DataType::UInt32, false),
    ];
    for name in &schema.names {
        fields.push(Field::new(name, DataType::Float64, false));
    }
    let arrow_schema = Arc::new(Schema::new(fields));

    let zone_base = export.zone_base;
    let row_count = rows.len();

    let mut origins = Vec::with_capacity(row_count);
    let mut destinations = Vec::with_capacity(row_count);
    let mut table_columns: Vec<Vec<f64>> = vec![Vec::with_capacity(row_count); schema.names.len()];

    for row in &rows {
        origins.push(u32::from(row.origin) - 1 + u32::from(zone_base));
        destinations.push(u32::from(row.destination) - 1 + u32::from(zone_base));
        for (col_index, &value) in row.values.iter().enumerate() {
            table_columns[col_index].push(value);
        }
    }

    let mut columns: Vec<Arc<dyn arrow::array::Array>> = vec![
        Arc::new(UInt32Array::from(origins)),
        Arc::new(UInt32Array::from(destinations)),
    ];
    for col in table_columns {
        columns.push(Arc::new(Float64Array::from(col)));
    }

    let batch = RecordBatch::try_new(arrow_schema.clone(), columns)?;

    let compression = match args.compression {
        ParquetCompression::Snappy => Compression::SNAPPY,
        ParquetCompression::Zstd => Compression::ZSTD(Default::default()),
        ParquetCompression::None => Compression::UNCOMPRESSED,
    };

    let file = File::create(&export.output)?;
    let props = WriterProperties::builder()
        .set_compression(compression)
        .build();
    let mut writer = ArrowWriter::try_new(file, arrow_schema, Some(props))?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(())
}
