# despina

Read and write `.mat` binary matrix files.

`.mat` files store square origin–destination matrices used in transport
modelling. Each file contains a fixed-size header with zone count, banner text,
and run identifier, followed by one or more named numeric tables.

## Overview

- Parses and writes the binary `.mat` matrix format.
- Two-level API: `Matrix` for in-memory random access, `Reader`/`Writer` for
  streaming single-pass I/O with bounded memory.
- All decoded values are presented as `f64` regardless of on-disk type code.
- No runtime dependencies. No proc-macro crates.
- Edition 2024, MSRV 1.85.

## Installation

```sh
cargo add despina
```

## Usage

### Load a matrix for random access

```rust
let mat = despina::open("skims.mat")?;
let dist = mat.table("DIST_AM");

println!("zones: {}", mat.zone_count());
println!("distance 1→2: {}", dist.get(1, 2));
```

### Stream rows with bounded memory

```rust
use despina::{ReaderBuilder, RowBuf};

let mut reader = ReaderBuilder::new().from_path("skims.mat")?;
let mut row = RowBuf::new();
let selection = reader.prepare_selection_by_name(&["DIST_AM"])?;
let mut total = 0.0;

while reader.read_selected_row(selection, &mut row)? {
    total += row.values().iter().sum::<f64>();
}
println!("total distance: {total}");
```

### Write a matrix

```rust
use despina::{TableDef, TypeCode, Writer};

let tables = [TableDef::new("DIST_AM", TypeCode::Float32)];
let mut writer = Writer::open_writer(Vec::new(), 2, &tables)?;

writer.write_stack(&[
    1.0, 2.0, // Origin 1.
    3.0, 4.0, // Origin 2.
])?;
let bytes = writer.finish()?;
```

`Writer` exposes three write levels to match upstream data shape:
`write_stack` (full matrix), `write_origin` / `write_origins` (origin blocks),
and `write_next_row` (row-at-a-time control).

## Key types

| Type | Role |
|------|------|
| `Matrix` | In-memory matrix with random access to cells and tables |
| `Table` | View over one named table in a `Matrix` |
| `Reader` | Streaming row-by-row reader over `Read` sources |
| `Writer` | Streaming writer over `Write` sinks |
| `ReaderBuilder` | Configures and opens a `Reader` |
| `WriterBuilder` | Configures and opens a `Writer` |
| `Header` | Parsed file header with zone count and table catalogue |
| `TableInfo` | Name, index, and type code for one table |
| `RowBuf` | Reusable buffer for streaming row reads |
| `TypeCode` | On-disk encoding: `Fixed(0..9)`, `Float32`, `Float64` |
| `MatrixBuilder` | Programmatic construction of in-memory matrices |
| `TableDef` | Table name and type code pair for writer APIs |

## Part of the despina workspace

This crate is the core library. The workspace also includes:

- [`despina-cli`](https://crates.io/crates/despina-cli) — command-line tool
- [npm package](https://www.npmjs.com/package/despina) — `npm install despina`
- [Python package](https://pypi.org/project/despina/) — `uv add despina`
- [GitHub repository](https://github.com/MichaelByrneAU/despina)

## Licence

MIT OR Apache-2.0
