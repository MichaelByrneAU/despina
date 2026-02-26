from __future__ import annotations

import csv
import warnings
from pathlib import Path
from typing import Mapping, Sequence

import numpy as np

from ._core import DespinaValidationError, _raise_validation
from .capabilities import has_pandas_support, has_polars_support
from .schema import TypeCode
from .warnings import DespinaWarning


def _validate_zone_base(zone_base: int) -> int:
    if isinstance(zone_base, bool) or not isinstance(zone_base, int):
        _raise_validation("zone_base must be integer 0 or 1")
    if zone_base not in {0, 1}:
        _raise_validation("zone_base must be 0 or 1")
    return zone_base


def _validate_zone_count(zone_count: int | None) -> int | None:
    if zone_count is None:
        return None
    if isinstance(zone_count, bool) or not isinstance(zone_count, int):
        _raise_validation("zone_count must be an integer greater than 0")
    if zone_count <= 0:
        _raise_validation("zone_count must be greater than 0")
    return zone_count


def _coerce_zone_vector(values: object, *, label: str) -> np.ndarray:
    array = np.asarray(values)
    if array.ndim != 1:
        _raise_validation(f"{label} must be 1D (got ndim={array.ndim})")
    if array.size == 0:
        return np.array([], dtype=np.int64)

    if np.issubdtype(array.dtype, np.bool_):
        _raise_validation(f"{label} must contain integer zones, not booleans")

    if np.issubdtype(array.dtype, np.integer):
        return array.astype(np.int64, copy=False)

    if np.issubdtype(array.dtype, np.floating):
        if not np.all(np.isfinite(array)):
            _raise_validation(f"{label} must contain finite integers")
        if not np.all(np.equal(array, np.floor(array))):
            _raise_validation(f"{label} must contain integer zone values")
        return array.astype(np.int64)

    # Object/string-like data: attempt numeric conversion.
    try:
        as_float = np.asarray(array, dtype=np.float64)
    except Exception as error:  # pragma: no cover - defensive
        raise DespinaValidationError(
            f"{label} must contain integer zone values"
        ) from error

    if not np.all(np.isfinite(as_float)):
        _raise_validation(f"{label} must contain finite integers")
    if not np.all(np.equal(as_float, np.floor(as_float))):
        _raise_validation(f"{label} must contain integer zone values")
    return as_float.astype(np.int64)


def _coerce_value_vector(values: object, *, label: str) -> np.ndarray:
    array = np.asarray(values)
    if array.ndim != 1:
        _raise_validation(f"{label} must be 1D (got ndim={array.ndim})")
    try:
        return np.asarray(array, dtype=np.float64)
    except Exception as error:  # pragma: no cover - defensive
        raise DespinaValidationError(f"{label} must be numeric") from error


def _resolve_table_columns(
    *,
    column_names: Sequence[str],
    origin_col: str,
    destination_col: str,
    table_columns: Sequence[str] | None,
    drop_extra_columns: bool,
) -> list[str]:
    value_candidates = [
        name for name in column_names if name not in {origin_col, destination_col}
    ]

    if table_columns is None:
        selected = list(value_candidates)
        if not selected:
            _raise_validation("at least one table value column is required")
        return selected

    selected = []
    seen: set[str] = set()
    for item in table_columns:
        if not isinstance(item, str):
            _raise_validation("table_columns must contain strings")
        if item in seen:
            _raise_validation(f"duplicate table column {item!r}")
        seen.add(item)
        selected.append(item)

    missing = [name for name in selected if name not in column_names]
    if missing:
        _raise_validation(
            "table_columns entries not present in input: "
            + ", ".join(repr(item) for item in missing)
        )

    extras = [name for name in value_candidates if name not in set(selected)]
    if extras and not drop_extra_columns:
        _raise_validation(
            "input contains extra non-OD columns not in table_columns: "
            + ", ".join(repr(item) for item in extras[:8])
            + (", ..." if len(extras) > 8 else "")
        )

    if not selected:
        _raise_validation("at least one table value column is required")
    return selected


def _resolve_table_names(
    *,
    source_columns: Sequence[str],
    rename_tables: Mapping[str, str] | None,
) -> list[str]:
    rename = dict(rename_tables or {})

    final_names: list[str] = []
    seen: set[str] = set()
    for source_name in source_columns:
        final_name = rename.get(source_name, source_name)
        if not isinstance(final_name, str):
            _raise_validation(f"rename target for {source_name!r} must be a string")
        if not final_name.strip():
            _raise_validation(f"rename target for {source_name!r} must not be empty")
        if final_name in seen:
            _raise_validation(f"duplicate table name after renaming: {final_name!r}")
        seen.add(final_name)
        final_names.append(final_name)

    unknown = [name for name in rename.keys() if name not in set(source_columns)]
    if unknown:
        _raise_validation(
            "rename_tables contains unknown source columns: "
            + ", ".join(repr(item) for item in unknown[:8])
            + (", ..." if len(unknown) > 8 else "")
        )
    return final_names


def _resolve_table_defs(
    *,
    source_columns: Sequence[str],
    final_names: Sequence[str],
    table_type_codes: Mapping[str, TypeCode | str | int] | None,
) -> list[tuple[str, str]]:
    type_map = dict(table_type_codes or {})

    known_keys = set(source_columns) | set(final_names)
    unknown = [name for name in type_map.keys() if name not in known_keys]
    if unknown:
        _raise_validation(
            "table_type_codes contains unknown table keys: "
            + ", ".join(repr(item) for item in unknown[:8])
            + (", ..." if len(unknown) > 8 else "")
        )

    defs: list[tuple[str, str]] = []
    for source_name, final_name in zip(source_columns, final_names):
        raw = type_map.get(final_name, type_map.get(source_name, "D"))
        token = TypeCode.parse(raw).token
        defs.append((final_name, token))
    return defs


def _validate_on_duplicate_od(value: str) -> str:
    if value != "error":
        _raise_validation("on_duplicate_od currently supports only 'error'")
    return value


def _validate_on_missing_od(value: str) -> str:
    if value not in {"warn", "ignore", "error"}:
        _raise_validation("on_missing_od must be 'warn', 'ignore', or 'error'")
    return value


def _sample_missing_pairs(
    observed_pairs: set[tuple[int, int]],
    *,
    zone_count: int,
    zone_base: int,
    limit: int,
) -> list[tuple[int, int]]:
    if limit <= 0:
        return []
    sample: list[tuple[int, int]] = []
    for origin in range(zone_count):
        for destination in range(zone_count):
            if (origin, destination) not in observed_pairs:
                sample.append((origin + zone_base, destination + zone_base))
                if len(sample) >= limit:
                    return sample
    return sample


def _normalise_column_names(column_names: Sequence[object]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for item in column_names:
        name = str(item)
        if name in seen:
            _raise_validation(f"duplicate column name {name!r}")
        seen.add(name)
        out.append(name)
    return out


def _build_matrix_from_columns(
    *,
    columns: Mapping[str, object],
    origin_col: str,
    destination_col: str,
    table_columns: Sequence[str] | None,
    rename_tables: Mapping[str, str] | None,
    table_type_codes: Mapping[str, TypeCode | str | int] | None,
    zone_base: int,
    zone_count: int | None,
    on_duplicate_od: str,
    on_missing_od: str,
    missing_warning_limit: int,
    include_zero_rows: bool,
    sort_od: bool,
    strict_zone_range: bool,
    drop_extra_columns: bool,
    banner: str | None,
    run_id: str | None,
):
    from .matrix import Matrix

    if not isinstance(origin_col, str) or not origin_col:
        _raise_validation("origin_col must be a non-empty string")
    if not isinstance(destination_col, str) or not destination_col:
        _raise_validation("destination_col must be a non-empty string")
    if origin_col == destination_col:
        _raise_validation("origin_col and destination_col must differ")

    zone_base = _validate_zone_base(zone_base)
    zone_count = _validate_zone_count(zone_count)
    _validate_on_duplicate_od(on_duplicate_od)
    on_missing_od = _validate_on_missing_od(on_missing_od)

    column_names = _normalise_column_names(list(columns.keys()))
    if origin_col not in column_names:
        _raise_validation(f"missing required origin column {origin_col!r}")
    if destination_col not in column_names:
        _raise_validation(f"missing required destination column {destination_col!r}")

    source_table_columns = _resolve_table_columns(
        column_names=column_names,
        origin_col=origin_col,
        destination_col=destination_col,
        table_columns=table_columns,
        drop_extra_columns=drop_extra_columns,
    )
    final_table_names = _resolve_table_names(
        source_columns=source_table_columns,
        rename_tables=rename_tables,
    )
    table_defs = _resolve_table_defs(
        source_columns=source_table_columns,
        final_names=final_table_names,
        table_type_codes=table_type_codes,
    )

    origin_raw = _coerce_zone_vector(columns[origin_col], label=origin_col)
    destination_raw = _coerce_zone_vector(
        columns[destination_col], label=destination_col
    )
    if origin_raw.shape[0] != destination_raw.shape[0]:
        _raise_validation(
            f"{origin_col!r} and {destination_col!r} length mismatch "
            f"({origin_raw.shape[0]} vs {destination_raw.shape[0]})"
        )

    table_values = [
        _coerce_value_vector(columns[name], label=name) for name in source_table_columns
    ]
    row_count = origin_raw.shape[0]
    for name, values in zip(source_table_columns, table_values):
        if values.shape[0] != row_count:
            _raise_validation(
                f"column {name!r} length mismatch with OD columns "
                f"({values.shape[0]} vs {row_count})"
            )

    if zone_base == 1:
        origin_idx = origin_raw - 1
        destination_idx = destination_raw - 1
    else:
        origin_idx = origin_raw.copy()
        destination_idx = destination_raw.copy()

    if np.any(origin_idx < 0) or np.any(destination_idx < 0):
        _raise_validation(
            f"zone ids must resolve to non-negative indices with zone_base={zone_base}"
        )

    if not include_zero_rows:
        nonzero_mask = np.zeros(row_count, dtype=bool)
        for values in table_values:
            nonzero_mask |= values != 0.0
        origin_idx = origin_idx[nonzero_mask]
        destination_idx = destination_idx[nonzero_mask]
        table_values = [values[nonzero_mask] for values in table_values]

    if sort_od and origin_idx.size > 0:
        order = np.lexsort((destination_idx, origin_idx))
        origin_idx = origin_idx[order]
        destination_idx = destination_idx[order]
        table_values = [values[order] for values in table_values]

    if zone_count is None:
        if origin_idx.size == 0:
            _raise_validation(
                "zone_count cannot be inferred from empty input; provide zone_count explicitly"
            )
        zone_count_value = int(
            max(int(origin_idx.max()), int(destination_idx.max())) + 1
        )
    else:
        zone_count_value = zone_count
        if origin_idx.size > 0:
            max_observed = int(max(int(origin_idx.max()), int(destination_idx.max())))
            if max_observed >= zone_count_value:
                if strict_zone_range:
                    _raise_validation(
                        "zone index exceeds zone_count bounds: "
                        f"max observed index {max_observed}, zone_count={zone_count_value}"
                    )
                zone_count_value = max_observed + 1

    if zone_count_value <= 0:
        _raise_validation("zone_count must be greater than 0")

    pairs = np.column_stack((origin_idx, destination_idx))
    if pairs.size == 0:
        unique_pairs = np.empty((0, 2), dtype=np.int64)
        duplicate_pairs = np.empty((0, 2), dtype=np.int64)
    else:
        unique_pairs, counts = np.unique(pairs, axis=0, return_counts=True)
        duplicate_pairs = unique_pairs[counts > 1]

    if duplicate_pairs.shape[0] > 0:
        preview = [
            (int(item[0] + zone_base), int(item[1] + zone_base))
            for item in duplicate_pairs[:8]
        ]
        suffix = ", ..." if duplicate_pairs.shape[0] > 8 else ""
        _raise_validation(
            f"duplicate OD pairs are not allowed; duplicates include {preview}{suffix}"
        )

    expected_pairs = zone_count_value * zone_count_value
    missing_pairs = expected_pairs - int(unique_pairs.shape[0])
    if missing_pairs > 0:
        if on_missing_od == "error":
            _raise_validation(
                f"input is missing {missing_pairs} OD pairs for zone_count={zone_count_value}"
            )
        elif on_missing_od == "warn":
            observed_set = {
                (int(item[0]), int(item[1])) for item in unique_pairs.tolist()
            }
            sample = _sample_missing_pairs(
                observed_set,
                zone_count=zone_count_value,
                zone_base=zone_base,
                limit=max(int(missing_warning_limit), 0),
            )
            sample_text = ", ".join(f"({o}, {d})" for o, d in sample)
            if sample_text:
                sample_text = f"; first missing pairs: {sample_text}"
            warnings.warn(
                (
                    f"input is missing {missing_pairs} OD pairs for zone_count={zone_count_value}"
                    f"{sample_text}; values defaulted to 0.0. "
                    "Set on_missing_od='ignore' to silence or 'error' to fail."
                ),
                category=DespinaWarning,
                stacklevel=2,
            )

    matrix = Matrix.create(
        zone_count_value,
        table_defs,
        banner=banner,
        run_id=run_id,
    )
    for table_index, values in enumerate(table_values):
        table_array = np.zeros((zone_count_value, zone_count_value), dtype=np.float64)
        table_array[origin_idx, destination_idx] = values
        matrix[table_index] = table_array
    return matrix


def matrix_from_csv(
    path: str | Path,
    *,
    origin_col: str = "Origin",
    destination_col: str = "Destination",
    table_columns: Sequence[str] | None = None,
    rename_tables: Mapping[str, str] | None = None,
    table_type_codes: Mapping[str, TypeCode | str | int] | None = None,
    zone_base: int = 1,
    zone_count: int | None = None,
    on_duplicate_od: str = "error",
    on_missing_od: str = "warn",
    missing_warning_limit: int = 10,
    include_zero_rows: bool = True,
    sort_od: bool = False,
    strict_zone_range: bool = True,
    drop_extra_columns: bool = True,
    encoding: str = "utf-8",
    sep: str = ",",
    quotechar: str = '"',
    csv_kwargs: Mapping[str, object] | None = None,
    banner: str | None = None,
    run_id: str | None = None,
):
    csv_options = dict(csv_kwargs or {})
    csv_options.pop("delimiter", None)
    csv_options.pop("quotechar", None)
    csv_options.pop("fieldnames", None)

    with Path(path).open(newline="", encoding=encoding) as handle:
        reader = csv.DictReader(
            handle, delimiter=sep, quotechar=quotechar, **csv_options
        )
        if reader.fieldnames is None:
            _raise_validation("CSV input does not contain a header row")

        columns: dict[str, list[object]] = {name: [] for name in reader.fieldnames}
        for row in reader:
            for name in reader.fieldnames:
                columns[name].append(row.get(name))

    return _build_matrix_from_columns(
        columns=columns,
        origin_col=origin_col,
        destination_col=destination_col,
        table_columns=table_columns,
        rename_tables=rename_tables,
        table_type_codes=table_type_codes,
        zone_base=zone_base,
        zone_count=zone_count,
        on_duplicate_od=on_duplicate_od,
        on_missing_od=on_missing_od,
        missing_warning_limit=missing_warning_limit,
        include_zero_rows=include_zero_rows,
        sort_od=sort_od,
        strict_zone_range=strict_zone_range,
        drop_extra_columns=drop_extra_columns,
        banner=banner,
        run_id=run_id,
    )


def matrix_from_pandas(
    frame,
    *,
    origin_col: str = "Origin",
    destination_col: str = "Destination",
    table_columns: Sequence[str] | None = None,
    rename_tables: Mapping[str, str] | None = None,
    table_type_codes: Mapping[str, TypeCode | str | int] | None = None,
    zone_base: int = 1,
    zone_count: int | None = None,
    on_duplicate_od: str = "error",
    on_missing_od: str = "warn",
    missing_warning_limit: int = 10,
    include_zero_rows: bool = True,
    sort_od: bool = False,
    strict_zone_range: bool = True,
    drop_extra_columns: bool = True,
    banner: str | None = None,
    run_id: str | None = None,
):
    if not has_pandas_support():
        raise ImportError(
            "pandas is required for from_pandas(); install with `uv add pandas` "
            "or `uv add despina[dataframe]`"
        )

    column_names = _normalise_column_names(list(frame.columns))
    columns = {name: frame[name].to_numpy(copy=False) for name in column_names}
    return _build_matrix_from_columns(
        columns=columns,
        origin_col=origin_col,
        destination_col=destination_col,
        table_columns=table_columns,
        rename_tables=rename_tables,
        table_type_codes=table_type_codes,
        zone_base=zone_base,
        zone_count=zone_count,
        on_duplicate_od=on_duplicate_od,
        on_missing_od=on_missing_od,
        missing_warning_limit=missing_warning_limit,
        include_zero_rows=include_zero_rows,
        sort_od=sort_od,
        strict_zone_range=strict_zone_range,
        drop_extra_columns=drop_extra_columns,
        banner=banner,
        run_id=run_id,
    )


def matrix_from_polars(
    frame,
    *,
    origin_col: str = "Origin",
    destination_col: str = "Destination",
    table_columns: Sequence[str] | None = None,
    rename_tables: Mapping[str, str] | None = None,
    table_type_codes: Mapping[str, TypeCode | str | int] | None = None,
    zone_base: int = 1,
    zone_count: int | None = None,
    on_duplicate_od: str = "error",
    on_missing_od: str = "warn",
    missing_warning_limit: int = 10,
    include_zero_rows: bool = True,
    sort_od: bool = False,
    strict_zone_range: bool = True,
    drop_extra_columns: bool = True,
    banner: str | None = None,
    run_id: str | None = None,
):
    if not has_polars_support():
        raise ImportError(
            "polars is required for from_polars(); install with `uv add polars`"
        )

    column_names = _normalise_column_names(list(frame.columns))
    columns = {name: frame.get_column(name).to_numpy() for name in column_names}
    return _build_matrix_from_columns(
        columns=columns,
        origin_col=origin_col,
        destination_col=destination_col,
        table_columns=table_columns,
        rename_tables=rename_tables,
        table_type_codes=table_type_codes,
        zone_base=zone_base,
        zone_count=zone_count,
        on_duplicate_od=on_duplicate_od,
        on_missing_od=on_missing_od,
        missing_warning_limit=missing_warning_limit,
        include_zero_rows=include_zero_rows,
        sort_od=sort_od,
        strict_zone_range=strict_zone_range,
        drop_extra_columns=drop_extra_columns,
        banner=banner,
        run_id=run_id,
    )


def matrix_from_parquet(
    path: str | Path,
    *,
    origin_col: str = "Origin",
    destination_col: str = "Destination",
    table_columns: Sequence[str] | None = None,
    rename_tables: Mapping[str, str] | None = None,
    table_type_codes: Mapping[str, TypeCode | str | int] | None = None,
    zone_base: int = 1,
    zone_count: int | None = None,
    on_duplicate_od: str = "error",
    on_missing_od: str = "warn",
    missing_warning_limit: int = 10,
    include_zero_rows: bool = True,
    sort_od: bool = False,
    strict_zone_range: bool = True,
    drop_extra_columns: bool = True,
    columns: Sequence[str] | None = None,
    parquet_engine: str = "auto",
    parquet_kwargs: Mapping[str, object] | None = None,
    banner: str | None = None,
    run_id: str | None = None,
):
    options = dict(parquet_kwargs or {})
    source = Path(path)

    if parquet_engine not in {"auto", "pyarrow", "fastparquet", "polars"}:
        _raise_validation(
            "parquet_engine must be 'auto', 'pyarrow', 'fastparquet', or 'polars'"
        )

    if parquet_engine == "polars":
        if not has_polars_support():
            raise ImportError(
                "polars is required for parquet_engine='polars'; "
                "install with `uv add polars`"
            )
        import polars as pl

        frame = pl.read_parquet(source, columns=columns, **options)
        return matrix_from_polars(
            frame,
            origin_col=origin_col,
            destination_col=destination_col,
            table_columns=table_columns,
            rename_tables=rename_tables,
            table_type_codes=table_type_codes,
            zone_base=zone_base,
            zone_count=zone_count,
            on_duplicate_od=on_duplicate_od,
            on_missing_od=on_missing_od,
            missing_warning_limit=missing_warning_limit,
            include_zero_rows=include_zero_rows,
            sort_od=sort_od,
            strict_zone_range=strict_zone_range,
            drop_extra_columns=drop_extra_columns,
            banner=banner,
            run_id=run_id,
        )

    # auto / pandas engines
    if not has_pandas_support():
        if parquet_engine == "auto" and has_polars_support():
            import polars as pl

            frame = pl.read_parquet(source, columns=columns, **options)
            return matrix_from_polars(
                frame,
                origin_col=origin_col,
                destination_col=destination_col,
                table_columns=table_columns,
                rename_tables=rename_tables,
                table_type_codes=table_type_codes,
                zone_base=zone_base,
                zone_count=zone_count,
                on_duplicate_od=on_duplicate_od,
                on_missing_od=on_missing_od,
                missing_warning_limit=missing_warning_limit,
                include_zero_rows=include_zero_rows,
                sort_od=sort_od,
                strict_zone_range=strict_zone_range,
                drop_extra_columns=drop_extra_columns,
                banner=banner,
                run_id=run_id,
            )

        raise ImportError(
            "pandas is required for parquet import with engine "
            f"{parquet_engine!r}; install with `uv add pandas pyarrow` "
            "or use parquet_engine='polars'"
        )

    import pandas as pd

    engine = None if parquet_engine == "auto" else parquet_engine
    frame = pd.read_parquet(source, columns=columns, engine=engine, **options)
    return matrix_from_pandas(
        frame,
        origin_col=origin_col,
        destination_col=destination_col,
        table_columns=table_columns,
        rename_tables=rename_tables,
        table_type_codes=table_type_codes,
        zone_base=zone_base,
        zone_count=zone_count,
        on_duplicate_od=on_duplicate_od,
        on_missing_od=on_missing_od,
        missing_warning_limit=missing_warning_limit,
        include_zero_rows=include_zero_rows,
        sort_od=sort_od,
        strict_zone_range=strict_zone_range,
        drop_extra_columns=drop_extra_columns,
        banner=banner,
        run_id=run_id,
    )
