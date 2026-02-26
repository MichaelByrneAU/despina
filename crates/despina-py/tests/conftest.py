"""Shared fixtures for despina test suite."""

from __future__ import annotations

import random
from pathlib import Path

import numpy as np
import pytest

import despina

_PROJECT_ROOT = Path(__file__).resolve().parents[3]


def pytest_configure(config):
    storage = f"file://{_PROJECT_ROOT / 'target' / '.benchmarks'}"
    config.option.benchmark_storage = storage


@pytest.fixture(scope="session")
def fixtures_dir() -> Path:
    path = Path(__file__).resolve().parents[3] / "fixtures"
    if not path.is_dir():
        pytest.skip(f"fixtures directory not found at {path}")
    return path


@pytest.fixture(scope="session")
def golden_dir(fixtures_dir: Path) -> Path:
    return fixtures_dir / "golden"


@pytest.fixture(scope="session")
def bench_dir(fixtures_dir: Path) -> Path:
    return fixtures_dir / "bench"


@pytest.fixture()
def small_matrix() -> despina.Matrix:
    matrix = despina.create(
        3,
        [
            despina.TableSpec.float64("TRIPS"),
            despina.TableSpec.fixed("DIST", 2),
        ],
    )
    matrix["TRIPS"][0, 1] = 10.0
    matrix["TRIPS"][1, 2] = 20.0
    matrix["DIST"][0, 2] = 5.5
    matrix["DIST"][2, 0] = 3.25
    return matrix


@pytest.fixture()
def zeros_matrix() -> despina.Matrix:
    return despina.create(4, [("TABLE1", "0")])


@pytest.fixture()
def single_table_matrix() -> despina.Matrix:
    matrix = despina.create(2, [("DATA", "D")])
    matrix["DATA"][0, 0] = 1.0
    matrix["DATA"][0, 1] = 2.0
    matrix["DATA"][1, 0] = 3.0
    matrix["DATA"][1, 1] = 4.0
    return matrix


@pytest.fixture()
def multi_type_matrix() -> despina.Matrix:
    type_codes = list(range(10)) + ["S", "D"]
    tables = [despina.table(f"T{code}", code) for code in type_codes]
    matrix = despina.create(5, tables)
    for table_index in range(12):
        matrix[table_index][:] = 1.0
    return matrix


@pytest.fixture()
def large_matrix() -> despina.Matrix:
    rng = random.Random(42)
    tables = [
        ("TRIPS", "D"),
        ("DIST", "2"),
        ("TIME", "S"),
        ("COST", "0"),
    ]
    matrix = despina.create(128, tables)
    for table_index in range(4):
        data = np.array(
            [rng.random() * 100 for _ in range(128 * 128)],
            dtype=np.float64,
        ).reshape(128, 128)
        matrix[table_index] = data
    return matrix
