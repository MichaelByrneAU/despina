# despina

Read and write `.mat` binary matrix files from JavaScript and TypeScript.

`.mat` files store square origin-destination matrices used in transport
modelling. Each file contains a fixed-size header with zone count, banner text,
and run identifier, followed by one or more named numeric tables.

## Installation

```sh
npm install despina
```

## Quick start

### Parse an existing matrix

```javascript
import { init, Matrix } from "despina";

await init();

const response = await fetch("skims.mat");
const bytes = new Uint8Array(await response.arrayBuffer());

const matrix = Matrix.fromBytes(bytes);
console.log(`zones: ${matrix.zoneCount}, tables: ${matrix.tableCount}`);

const dist = matrix.table("DIST"); // Float64Array, row-major
```

### Load a subset of tables

```javascript
const matrix = Matrix.fromBytes(bytes, { tables: ["DIST", "TIME"] });
```

### Create a matrix from scratch

```javascript
const matrix = Matrix.create(
  3,
  [
    { name: "DIST", typeCode: "2" },
    { name: "TIME", typeCode: "S" },
  ],
  { banner: "My model v1.0", runId: "RUN001" },
);

matrix.set("DIST", 1, 2, 4.5);
matrix.set("TIME", 1, 2, 12.0);

const bytes = matrix.toBytes(); // Uint8Array, valid .mat file
```

### Inspect table metadata

```javascript
for (const meta of matrix.tables) {
  console.log(`${meta.index}: ${meta.name} (${meta.typeCode})`);
}
```

## API

### Construction

| Method | Description |
|--------|-------------|
| `Matrix.fromBytes(bytes, options?)` | Parse a `.mat` file from a `Uint8Array` |
| `Matrix.create(zoneCount, tableDefs, options?)` | Create a new empty matrix |

### Serialisation

| Method | Description |
|--------|-------------|
| `matrix.toBytes()` | Serialise to a `Uint8Array` |

### Header properties

| Property | Type | Description |
|----------|------|-------------|
| `matrix.zoneCount` | `number` | Number of zones (square dimension) |
| `matrix.tableCount` | `number` | Number of tables |
| `matrix.banner` | `string` | Banner text from the file header |
| `matrix.runId` | `string` | Run identifier from the file header |

### Table metadata

| Property | Type | Description |
|----------|------|-------------|
| `matrix.tables` | `TableMeta[]` | Frozen array of table metadata |
| `matrix.tableNames` | `string[]` | Frozen array of table names |
| `meta.index` | `number` | 0-based table position |
| `meta.name` | `string` | Table name |
| `meta.typeCode` | `string` | Storage type code token |

### Table access

All methods accepting a table take `string | number` (name or 0-based index).

| Method | Description |
|--------|-------------|
| `matrix.table(table)` | `Float64Array` (live view, mutations apply) |
| `matrix.setTable(table, values)` | Replace all values with shape validation |
| `matrix.get(table, origin, dest)` | Get a cell value (1-based O/D) |
| `matrix.set(table, origin, dest, value)` | Set a cell value (1-based O/D) |
| `matrix.row(table, origin)` | `Float64Array` subarray view (1-based origin) |

### Aggregates

| Method | Description |
|--------|-------------|
| `matrix.tableTotal(table)` | Sum of all cells in a table |
| `matrix.tableDiagonalTotal(table)` | Sum of diagonal cells |

### Container protocol

| Method | Description |
|--------|-------------|
| `matrix.has(name)` | Check whether a table name exists |
| `matrix.keys()` | Iterate table names in header order |
| `[Symbol.iterator]()` | Iterate table names in header order |

## Error handling

All codec errors are thrown as `DespinaError`, which extends `Error` with:

- `.kind` — machine-readable discriminant (e.g. `"unexpected_eof"`)
- `.offset` — byte position of the failure, if applicable

```javascript
import { DespinaError } from "despina";

try {
  const matrix = Matrix.fromBytes(badBytes);
} catch (error) {
  if (error instanceof DespinaError) {
    console.log(error.kind);   // "unexpected_eof"
    console.log(error.offset); // 1024
  }
}
```

## Type codes

| Token | Storage |
|-------|---------|
| `"0"` - `"9"` | Fixed-point with 0-9 decimal places |
| `"S"` | IEEE 754 binary32 (float) |
| `"D"` | IEEE 754 binary64 (double) |

## Part of the despina workspace

This is the WebAssembly/npm package. The workspace also includes:

- [`despina`](https://crates.io/crates/despina) - Rust library
- [`despina-cli`](https://crates.io/crates/despina-cli) - command-line tool
- [Python package](https://pypi.org/project/despina/) - `uv add despina`
- [GitHub repository](https://github.com/MichaelByrneAU/despina)

## Licence

MIT OR Apache-2.0
