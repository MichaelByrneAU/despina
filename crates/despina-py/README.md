# despina

Read, analyse, and write `.mat` origin-destination matrix files from Python.

`.mat` files store square origin-destination matrices used in transport
modelling. Each file contains a fixed-size header with zone count, banner text,
and run identifier, followed by one or more named numeric tables.

## Installation

```sh
uv add despina
```

Optional extras for tabular ingestion and export:

```sh
uv add "despina[dataframe]"   # Pandas support.
uv add "despina[polars]"      # Polars support.
uv add "despina[parquet]"     # Pandas + pyarrow for Parquet I/O.
```

## Requirements

- Python >= 3.9
- NumPy >= 1.23

## Quick start

### Read a matrix

```python
import despina

matrix = despina.read("skims.mat")

print(matrix.zone_count)       # Number of zones.
print(matrix.table_names)      # E.g. ('DIST_AM', 'TIME_AM').
```

### Access table data

```python
# Subscript returns the stored NumPy array.
dist = matrix["DIST_AM"]            # (zone_count, zone_count) float64 array.
dist[0, 1] = 7.5                    # Modifies the matrix directly.
dist *= 1.02                        # In-place scale.

# Replace a whole table (validates shape).
matrix["DIST_AM"] = new_array

# Iterate table names, convert to dict.
for name in matrix:
    print(name, matrix[name].sum())

arrays = dict(matrix)               # {"DIST_AM": ndarray, ...}
```

### Create a matrix from scratch

```python
import despina

matrix = despina.create(100, [("DIST_AM", "D"), ("TIME_AM", "S")])
matrix["DIST_AM"][0, 1] = 12.5
matrix.write("output.mat")
```

### Combine matrices

```python
am = despina.read("am_peak.mat")
pm = despina.read("pm_peak.mat")

result = despina.Matrix.like(am)
for name in am:
    result[name] = am[name] + pm[name]
result.write("combined.mat")
```

Table definitions accept `TableSpec` values, `(name, type_code)` tuples, or
the `despina.table()` convenience constructor. Type codes are `"D"` (float64),
`"S"` (float32), or `"0"` through `"9"` (fixed-decimal precision).

### Serialise to bytes

```python
payload = matrix.to_bytes()
restored = despina.from_bytes(payload)
```

## Tabular ingestion

Build matrices from wide-format OD data (Origin, Destination, value columns):

```python
matrix = despina.from_csv("skims_wide.csv")
matrix = despina.from_parquet("skims_wide.parquet")
matrix = despina.from_pandas(frame)
matrix = despina.from_polars(frame)
```

All tabular constructors accept options for column naming (`origin_col`,
`destination_col`), table renaming (`rename_tables`), type-code assignment
(`table_type_codes`), zone base (`zone_base`), and OD validation policy
(`on_duplicate_od`, `on_missing_od`).

## Export

```python
matrix.to_csv("output.csv")
matrix.to_parquet("output.parquet")

frame = matrix.to_pandas()
frame = matrix.to_polars()
```

Export methods support table selection (`tables`), column renaming
(`rename_columns`), zero-row filtering (`include_zero_rows`), and OD sorting
(`sort_od`).

## Part of the despina workspace

This is the Python package. The workspace also includes:

- [`despina`](https://crates.io/crates/despina): Rust library
- [`despina-cli`](https://crates.io/crates/despina-cli): command-line tool
- [npm package](https://www.npmjs.com/package/despina): `npm install despina`
- [GitHub repository](https://github.com/MichaelByrneAU/despina)

## Licence

MIT OR Apache-2.0
