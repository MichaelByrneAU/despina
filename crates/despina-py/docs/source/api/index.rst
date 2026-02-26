API Reference
=============

The reference is organised by usage layer: top-level I/O and construction
functions, in-memory matrix operations, and schema definition helpers.
Read :doc:`../quickstart` first if you are new to the package, then use this
reference for precise method signatures and parameter behaviour.

Three conventions apply throughout the API:

1. Zone-based helpers are 1-based, matching common OD matrix conventions.
2. Position-based helpers and NumPy arrays are 0-based, matching NumPy indexing.
3. Wide tabular interchange defaults to ``Origin``/``Destination`` plus one
   column per table.

.. toctree::
   :maxdepth: 2

   io
   schema
   matrix
