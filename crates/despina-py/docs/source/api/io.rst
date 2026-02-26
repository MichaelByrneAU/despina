I/O Functions
=============

The root-level eager entrypoints construct or load :class:`despina.Matrix`
instances directly, or build matrices from canonical wide OD tabular inputs.
Import controls such as ``rename_tables``, ``zone_base``, ``zone_count``, and
``strict_zone_range`` are explicit parameters that keep ingestion assumptions
visible at the call site.

The import functions share the same validation model:

1. Duplicate OD pairs are rejected.
2. Missing OD pairs are configurable (warn/ignore/error).
3. Table names, type codes, and OD bounds are checked before matrix creation.
4. Canonical OD columns default to ``Origin`` and ``Destination``.

Quick Example
-------------

.. code-block:: python

   import despina

   matrix = despina.from_csv("skims_wide.csv")
   matrix["DIST_AM"] = matrix["DIST_AM"] * 1.02
   matrix.write("skims_updated.mat")

Module Reference
----------------

.. automodule:: despina.io
   :members:
