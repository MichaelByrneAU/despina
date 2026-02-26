mod lib 'crates/despina'
mod cli 'crates/despina-cli'
mod duckdb 'crates/despina-duckdb'
mod py 'crates/despina-py'
mod wasm 'crates/despina-wasm'

default: help

help:
    @just --list --unsorted

# Compile the workspace.
check:
    cargo check --workspace

# Run all tests.
test:
    cargo test --workspace

# Format source files.
fmt *args:
    cargo fmt --all {{ args }}

# Run Clippy.
clippy:
    cargo clippy --workspace --all-targets -- -D warnings

# Run the full local CI check.
ci: check (fmt "--check") clippy test
