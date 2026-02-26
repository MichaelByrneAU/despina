from __future__ import annotations

from pathlib import Path

import despina
import pytest

_BENCH_DIR = Path(__file__).resolve().parents[3] / "fixtures" / "bench"

_COMPACT_PATH = _BENCH_DIR / "compact_uniform_32_tables_32_zones.mat"
_MEDIUM_PATH = _BENCH_DIR / "medium_mixed_modes_32_tables_2048_zones.mat"


def _load_fixture_bytes(path: Path) -> bytes:
    if not path.exists():
        pytest.skip(f"benchmark fixture not found at {path}")
    return path.read_bytes()


_compact_bytes: bytes | None = None
_medium_bytes: bytes | None = None


def _get_compact_bytes() -> bytes:
    global _compact_bytes
    if _compact_bytes is None:
        _compact_bytes = _load_fixture_bytes(_COMPACT_PATH)
    return _compact_bytes


def _get_medium_bytes() -> bytes:
    global _medium_bytes
    if _medium_bytes is None:
        _medium_bytes = _load_fixture_bytes(_MEDIUM_PATH)
    return _medium_bytes


def test_bench_from_bytes_compact(benchmark):
    data = _get_compact_bytes()
    benchmark(despina.from_bytes, data)


def test_bench_from_bytes_medium(benchmark):
    data = _get_medium_bytes()
    benchmark(despina.from_bytes, data)


def test_bench_to_bytes_compact(benchmark):
    data = _get_compact_bytes()
    matrix = despina.from_bytes(data)
    benchmark(matrix.to_bytes)


def test_bench_getitem(benchmark):
    data = _get_compact_bytes()
    matrix = despina.from_bytes(data)

    benchmark(matrix.__getitem__, "T1")
