Error Handling and Diagnostics
==============================

``despina`` enforces strict validation on shapes, selectors, and indices.
Every error raises a typed exception with stable, inspectable fields that
support both programmatic handling and diagnostic logging.

Core Exception Types
--------------------

The package raises four primary public exception classes:

- :class:`~despina.DespinaIoError` for path and I/O failures.
- :class:`~despina.DespinaParseError` for malformed or unsupported file content.
- :class:`~despina.DespinaValidationError` for bad API arguments and shape or index violations.
- :class:`~despina.DespinaWriterError` for invalid write order or writer lifecycle.

Each exception carries two diagnostic fields. ``kind`` is a stable snake_case
string that identifies the error category and is suitable for conditional
handling in code. ``offset`` reports the byte position in the source file where
the problem was detected, or ``None`` when the error is not position-specific.

.. code-block:: python

   import despina

   try:
       matrix = despina.read("skims.mat")
       matrix["DISTA_AM"]
   except despina.DespinaValidationError as error:
       print(error.kind)    # E.g. "table_not_found".
       print(error.offset)  # Byte position, or None.
       print(str(error))    # Full human-readable message.

Prefer logging ``kind`` and relevant input context rather than matching full
error message text, as message wording may change between versions while
``kind`` values remain stable.

Missing Table Names
-------------------

When a table name is not found, the error message includes a close-match
suggestion and a preview of the available names.

.. code-block:: python

   matrix = despina.read("skims.mat")
   matrix["DISTA_AM"]
   # Raises: table 'DISTA_AM' not found. Did you mean 'DIST_AM'?

For optional tables, prefer defensive membership checks over exception control
flow. The ``in`` operator tests whether a table name exists without raising.
:attr:`~despina.matrix.Matrix.table_names` exposes the full list of available
names for pre-flight checks.

.. code-block:: python

   # Check membership before selection when the name comes from config.
   if "TOLL_AM" in matrix:
       toll = matrix["TOLL_AM"]

   if "DIST_AM" in matrix:
       dist = matrix["DIST_AM"]

   # Inspect available names before dynamic selection.
   print(matrix.table_names)

Duplicate selectors that resolve to the same table are rejected regardless of
whether the duplication is by name or by index.

.. code-block:: python

   matrix.to_csv_long("out.csv", tables=["DIST_AM", 0], zero_policy="exclude")
   # Raises: duplicate table selection

Indexing Errors
---------------

Cell access uses standard NumPy 0-based indexing on the arrays returned by
``matrix["table_name"]``. Out-of-range indices raise the usual NumPy
``IndexError``.

Boolean values are rejected as table key arguments. Since ``True == 1`` and
``False == 0`` in Python, silently accepting booleans would mask programming
errors where a condition result is passed as a table selector.

Shape Errors
------------

Shape contracts are validated before every write. The expected shapes are:

- Subscript assignment (``matrix["T"] = arr``) expects ``(zone_count, zone_count)``.

Error messages include both the expected and actual shape, making shape
mismatches straightforward to diagnose.

Zone Reindex Errors
-------------------

:meth:`~despina.matrix.Matrix.reindex_zones` accepts only plain integer zone
identifiers. Boolean values, fractional values, duplicates, and out-of-range
values all raise :class:`~despina.DespinaValidationError`.

.. code-block:: python

   matrix = despina.read("skims.mat")
   reduced = matrix.reindex_zones([1, 5, 7], basis="zone")

Optional Dependency Errors
--------------------------

Several methods require optional packages that are not part of the base
installation. When a required package is absent, a descriptive ``ImportError``
names the missing dependency, provides the install command, and offers
``despina.has_dataframe_support()`` as a way to check availability
programmatically before calling the method.

.. code-block:: text

   ImportError: pandas is required for to_long_dataframe(); install with
   `uv add despina[dataframe]` or `uv add pandas`; check
   availability via `despina.has_dataframe_support()`

:meth:`~despina.matrix.Matrix.to_long_dataframe` requires pandas.
:meth:`~despina.matrix.Matrix.to_csv_long` and
:meth:`~despina.matrix.Matrix.to_parquet_long` do not require a dataframe
library.

Quick Debug Checklist
---------------------

When tracing a data or API failure, check in this order:

1. Confirm :attr:`~despina.matrix.Matrix.zone_count` and
   :attr:`~despina.matrix.Matrix.table_names` before any transform logic.
2. Confirm whether each call expects 1-based zone identifiers or 0-based
   position indices.
3. Confirm array shape immediately before every write-back call.
