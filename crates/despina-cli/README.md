# despina-cli

Command-line tool for inspecting, validating, and converting `.mat` binary
matrix files.

## Installation

```sh
cargo install despina-cli
```

The installed binary is named `despina`.

## Commands

```
despina <command> [options]

COMMANDS:
  info <file> [--format text|json]        Print header metadata and table catalogue.
  validate <file> [--quiet]               Parse and validate the full file.
  stats <file> [--format text|json]       Print per-table totals and diagonal totals.
  to-csv <file> -o <out> [opts]           Convert a .mat file to wide-format CSV.
  from-csv <file> -o <out> [opts]         Convert a wide-format CSV to a .mat file.
  to-parquet <file> -o <out> [opts]       Convert a .mat file to wide-format Parquet.
  from-parquet <file> -o <out> [opts]     Convert a wide-format Parquet to a .mat file.
```

Use `despina <command> --help` for command-specific options.

## Examples

Print header metadata:

```sh
despina info skims.mat
```

Print header metadata as JSON:

```sh
despina info skims.mat --format json
```

Validate a file (silent on success, non-zero exit on failure):

```sh
despina validate skims.mat --quiet
```

Print per-table totals:

```sh
despina stats skims.mat
```

Export to CSV:

```sh
despina to-csv skims.mat -o skims.csv
```

Export a subset of tables, including zero rows:

```sh
despina to-csv skims.mat -o skims.csv --table Time --table Distance --include-zero-rows
```

Import from CSV:

```sh
despina from-csv skims.csv -o skims.mat
```

Export to Parquet:

```sh
despina to-parquet skims.mat -o skims.parquet
```

Import from Parquet:

```sh
despina from-parquet skims.parquet -o skims.mat
```

## Related

- [`despina`](https://crates.io/crates/despina) — Rust library
- [npm package](https://www.npmjs.com/package/despina) — `npm install despina`
- [Python package](https://pypi.org/project/despina/) — `uv add despina`
- [GitHub repository](https://github.com/MichaelByrneAU/despina)

## Licence

MIT OR Apache-2.0
