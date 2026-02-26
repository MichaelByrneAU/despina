despina Python API
==================

``despina`` reads, analyses, transforms, and writes ``.mat`` origin-destination
matrices. The :class:`despina.Matrix <despina.matrix.Matrix>` API gives random
access to tables and cells with a straightforward NumPy-oriented interface.

What The Package Provides
-------------------------

The library covers the full round trip for matrix data: loading existing files,
inspecting schema and metadata, applying vectorised table edits, and writing
results back to ``.mat``. Tabular interchange with CSV,
`Parquet <https://parquet.apache.org/>`_, `pandas <https://pandas.pydata.org/>`_,
and `polars <https://pola.rs/>`_ uses a canonical wide OD form, with strict
validation around OD completeness, table naming, and type-code choices.
The canonical wide schema uses this default column pattern:

.. code-block:: text

   Origin,Destination,DIST_AM,TIME_AM,...

The first two columns are always ``Origin`` and ``Destination``. Each remaining
column maps to one matrix table. ``DIST_AM`` and ``TIME_AM`` are illustrative
table names. Only the OD columns are required.

Usage
-----

Import from the root package and work with
:class:`despina.Matrix <despina.matrix.Matrix>`.

.. code-block:: python

   import despina

   # Load the full matrix into memory for random table/cell access.
   matrix = despina.read("skims.mat")

   # Quick inspection: zone dimension and available table names.
   print(matrix.zone_count, matrix.table_names)

   # Returns the stored NumPy array. In-place edits apply immediately.
   dist = matrix["DIST_AM"]
   dist *= 1.02

   # Export canonical wide OD data for tabular workflows.
   matrix.to_parquet("skims_wide.parquet")

   # Rebuild a matrix from the wide file and write a .mat output.
   rebuilt = despina.from_parquet("skims_wide.parquet")
   rebuilt.write("skims_rebuilt.mat")

Documentation Map
-----------------

Start with :doc:`quickstart`, then read :doc:`tabular_workflows` and
:doc:`numpy_workflows` for more detailed coverage. :doc:`inspection` and
:doc:`error_handling` cover ingestion and validation patterns. :doc:`api/index`
contains the full public Python API reference.

.. toctree::
   :hidden:
   :maxdepth: 2

   Home <self>
   quickstart
   tabular_workflows
   inspection
   numpy_workflows
   error_handling
   type_codes
   api/index
