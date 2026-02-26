from __future__ import annotations

from importlib.util import find_spec

from ._core import DespinaValidationError


def has_pandas_support() -> bool:
    """Return ``True`` when pandas is importable."""

    return find_spec("pandas") is not None


def has_polars_support() -> bool:
    """Return ``True`` when polars is importable."""

    return find_spec("polars") is not None


def has_dataframe_support() -> bool:
    """Return ``True`` when at least one dataframe backend is importable.

    This currently means pandas or polars.
    """

    return has_pandas_support() or has_polars_support()


def has_parquet_support(*, engine: str = "auto") -> bool:
    """Return ``True`` when parquet export/import is available for ``engine``.

    Supported values:

    - ``"auto"``: any supported parquet path.
    - ``"pyarrow"`` / ``"fastparquet"``: pandas parquet engines.
    - ``"polars"``: polars parquet support.

    ``engine="auto"`` returns ``True`` when any supported parquet backend is
    available in the current environment.
    """

    if engine not in {"auto", "pyarrow", "fastparquet", "polars"}:
        raise DespinaValidationError(
            "unsupported parquet engine "
            f"{engine!r}; expected 'auto', 'pyarrow', 'fastparquet', or 'polars'"
        )

    if engine == "polars":
        return has_polars_support()

    if engine in {"pyarrow", "fastparquet"}:
        return has_pandas_support() and find_spec(engine) is not None

    # auto
    pandas_parquet = has_pandas_support() and (
        find_spec("pyarrow") is not None or find_spec("fastparquet") is not None
    )
    return pandas_parquet or has_polars_support()
