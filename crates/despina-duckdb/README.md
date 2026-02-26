# despina-duckdb

DuckDB extension for reading `.mat` binary matrix files.

Registers a `read_mat()` table function that presents matrix data in wide
format with `Origin`, `Destination`, and one column per matrix table.

## Installation

### From GitHub releases

Download the pre-built extension for your platform from the
[latest release](https://github.com/MichaelByrneAU/despina/releases/latest),
decompress it, and copy it into DuckDB's extension directory:

```sh
# Example for macOS ARM (osx_arm64)
gunzip despina-duckdb-v0.4.0-osx_arm64.duckdb_extension.gz
mkdir -p ~/.duckdb/extensions/v1.4.4/osx_arm64
cp despina-duckdb-v0.4.0-osx_arm64.duckdb_extension \
   ~/.duckdb/extensions/v1.4.4/osx_arm64/despina.duckdb_extension
```

Replace `v1.4.4` with your installed DuckDB version and choose the correct
platform (`linux_amd64`, `osx_arm64`, or `windows_amd64`).

> **Unsigned extension:** This extension is not signed by DuckDB. You must
> allow unsigned extensions before loading it:
>
> ```sql
> SET allow_unsigned_extensions = true;
> LOAD despina;
> ```
>
> Or start DuckDB with the `-unsigned` flag: `duckdb -unsigned`

### Build from source

Requires [cargo-duckdb-ext](https://crates.io/crates/cargo-duckdb-ext) and
DuckDB. The extension is pinned to DuckDB v1.4.4.

```sh
just duckdb-build          # release build (default)
just duckdb-build debug    # debug build
just duckdb-install        # install to local DuckDB
```

## Usage

Start a DuckDB shell with the extension loaded:

```sh
just duckdb-shell
```

Or load manually in any DuckDB session:

```sql
LOAD despina;
```

### Basic query

```sql
SELECT * FROM read_mat('skims.mat') LIMIT 10;
```

### Select specific tables

```sql
SELECT * FROM read_mat('skims.mat', tables := ['DIST_AM', 'TIME_AM']);
```

### Include zero-valued rows

By default, rows where all table values are zero are excluded. To include them:

```sql
SELECT * FROM read_mat('skims.mat', include_zeros := true);
```

### Aggregate example

```sql
SELECT Origin, SUM(DIST_AM) AS total_dist
FROM read_mat('skims.mat')
GROUP BY Origin
ORDER BY Origin;
```

## Output columns

| Column | Type | Description |
|--------|------|-------------|
| `Origin` | `BIGINT` | 1-based origin zone |
| `Destination` | `BIGINT` | 1-based destination zone |
| *(table names)* | `DOUBLE` | One column per matrix table |

## Part of the despina workspace

This is the DuckDB extension. The workspace also includes:

- [`despina`](https://crates.io/crates/despina) — Rust library
- [`despina-cli`](https://crates.io/crates/despina-cli) — command-line tool
- [npm package](https://www.npmjs.com/package/despina) — `npm install despina`
- [Python package](https://pypi.org/project/despina/) — `uv add despina`
- [GitHub repository](https://github.com/MichaelByrneAU/despina)

## Licence

MIT OR Apache-2.0
