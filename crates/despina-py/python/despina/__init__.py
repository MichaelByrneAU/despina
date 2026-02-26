from importlib.metadata import version as _metadata_version

from ._core import (
    DespinaError,
    DespinaIoError,
    DespinaParseError,
    DespinaValidationError,
    DespinaWriterError,
)
from .capabilities import (
    has_dataframe_support,
    has_pandas_support,
    has_parquet_support,
    has_polars_support,
)
from .io import (
    create,
    from_bytes,
    from_csv,
    from_pandas,
    from_parquet,
    from_polars,
    like,
    read,
)
from .matrix import Matrix, TableMeta
from .schema import TableSpec, TypeCode, table
from .warnings import DespinaWarning

__version__: str = _metadata_version("despina")

__all__ = [
    "__version__",
    "DespinaError",
    "DespinaIoError",
    "DespinaParseError",
    "DespinaValidationError",
    "DespinaWriterError",
    "DespinaWarning",
    "Matrix",
    "TableMeta",
    "TableSpec",
    "TypeCode",
    "table",
    "create",
    "like",
    "read",
    "from_bytes",
    "from_csv",
    "from_parquet",
    "from_pandas",
    "from_polars",
    "has_pandas_support",
    "has_polars_support",
    "has_dataframe_support",
    "has_parquet_support",
]
