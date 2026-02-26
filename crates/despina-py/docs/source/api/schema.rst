Schema Helpers
==============

Schema objects validate table names and type codes at construction time,
causing schema errors to surface before matrix creation. They are useful when
schema originates from configuration, user input, or generated metadata.

The three core helpers are:

1. :class:`despina.schema.TypeCode` for validated storage tokens.
2. :class:`despina.schema.TableSpec` for validated ``(name, type_code)`` items.
3. :func:`despina.schema.table` as a concise checked constructor.

Quick Example
-------------

.. code-block:: python

   import despina

   tables = [
       despina.TableSpec.float64("DIST_AM"),
       despina.TableSpec.float32("TIME_AM"),
   ]

   matrix = despina.create(120, tables)

Module Reference
----------------

.. automodule:: despina.schema
   :members:
