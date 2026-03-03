# despina

[![crates.io](https://img.shields.io/crates/v/despina)](https://crates.io/crates/despina)
[![docs.rs](https://img.shields.io/docsrs/despina)](https://docs.rs/despina)
[![PyPI](https://img.shields.io/pypi/v/despina)](https://pypi.org/project/despina)
[![Read the Docs](https://img.shields.io/readthedocs/despina)](https://despina.readthedocs.io)
[![npm](https://img.shields.io/npm/v/despina)](https://www.npmjs.com/package/despina)

Read and write `.mat` binary matrix files.

## What are `.mat` files?

`.mat` files store square origin–destination matrices used in transport
modelling software. Each file contains a fixed-size header with zone count,
banner text, and run identifier, followed by one or more named numeric tables
encoded with type-specific compression.

## Components

### Rust library

Core library for reading and writing `.mat` files. Two-level API: `Matrix` for
in-memory random access, `Reader`/`Writer` for streaming I/O.

```sh
cargo add despina
```

```rust
let mat = despina::open("skims.mat")?;
let dist = mat.table("DIST_AM");
println!("zones: {}, distance 1→2: {}", mat.zone_count(), dist.get(1, 2));
```

### Command-line tool

Inspect, validate, and convert `.mat` files from the terminal.

```sh
cargo install despina-cli
```

```sh
despina info skims.mat
despina stats skims.mat
despina validate skims.mat
despina to-csv skims.mat -o skims.csv
```

### Python package

Read, analyse, and write `.mat` files from Python with NumPy arrays, pandas,
and polars integration.

```sh
uv add despina
```

```python
import despina

matrix = despina.read("skims.mat")
dist = matrix["DIST_AM"]         # (zone_count, zone_count) float64 array.

matrix = despina.from_csv("skims.csv")
matrix.to_parquet("skims.parquet")
```

### WebAssembly / npm package

Read and write `.mat` files from JavaScript and TypeScript in the browser or
Node.js.

```sh
npm install despina
```

```javascript
import init, { Matrix } from "despina";

await init();
const matrix = Matrix.fromBytes(bytes);
const dist = matrix.tableData(1); // Float64Array.
```

### DuckDB extension

Query `.mat` files directly from SQL.

```sql
INSTALL despina FROM 'https://despina.michaelbyrne.au';
LOAD despina;

SELECT Origin, Destination, DIST_AM
FROM read_mat('skims.mat', tables := ['DIST_AM'])
WHERE DIST_AM > 0;
```

> Requires the `-unsigned` flag: `duckdb -unsigned`

## Development

The workspace requires Rust 1.85+ and [just](https://github.com/casey/just).
Individual crates have additional tooling requirements:
[uv](https://docs.astral.sh/uv/) for Python bindings,
[wasm-pack](https://rustwasm.github.io/wasm-pack/) for the WASM package, and
the [DuckDB CLI](https://duckdb.org/) with `cargo-duckdb-ext` for the DuckDB
extension. The `xtask/` crate provides cross-platform build automation used by
several justfile recipes.

```sh
just ci                           # Full local CI: check, fmt, clippy, test.
just test                         # Run Rust tests.
just cli::run -- info skims.mat   # Run the CLI.
just py::build && just py::test   # Build and test Python bindings.
just wasm::build                  # Build the WASM package.
just duckdb::install              # Build and install the DuckDB extension.
just duckdb::shell                # Start a DuckDB shell with the extension.
```

Run `just --list` in any crate directory for all available recipes.

## Licence

MIT OR Apache-2.0
