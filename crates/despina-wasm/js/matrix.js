/**
 * High-level Matrix class for the despina WASM package.
 *
 * Mirrors `crates/despina-py/python/despina/matrix.py`: the JS side owns all
 * data as `Float64Array`s and provides the user-facing API, while Rust handles
 * only the binary codec (decode/encode).
 */

import { decode, decodeTables, encode, validateSchema } from "./despina.js";
import { DespinaError, rethrow } from "./errors.js";

/**
 * Resolve a table key (name or 0-based index) to a table name.
 *
 * @param {string | number} table
 * @param {ReadonlyArray<string>} tableNames
 * @param {ReadonlyMap<string, number>} indexByName
 * @param {number} tableCount
 * @returns {string}
 */
function resolveTableName(table, tableNames, indexByName, tableCount) {
  if (typeof table === "string") {
    if (!indexByName.has(table)) {
      throw new DespinaError(`table "${table}" not found`, "table_not_found");
    }
    return table;
  }
  if (typeof table === "number") {
    if (!Number.isInteger(table) || table < 0 || table >= tableCount) {
      throw new DespinaError(
        `table index must be within 0..${tableCount - 1} (got ${table})`,
        "table_index_out_of_range",
      );
    }
    return tableNames[table];
  }
  throw new DespinaError(
    "table must be a table name (string) or 0-based index (number)",
    "validation_error",
  );
}

/**
 * @typedef {Object} TableMeta
 * @property {number} index   0-based table position.
 * @property {string} name    Table name.
 * @property {string} typeCode Storage type code token.
 */

/**
 * @typedef {Object} FromBytesOptions
 * @property {string[]} [tables] Subset of table names to load.
 */

/**
 * @typedef {Object} CreateTableDef
 * @property {string} name     Table name.
 * @property {string} typeCode Type code token ('0'..'9', 'S', or 'D').
 */

/**
 * @typedef {Object} CreateOptions
 * @property {string} [banner] Banner text.
 * @property {string} [runId]  Run identifier.
 */

/**
 * Loaded `.mat` matrix with random table and cell access.
 *
 * Each table is stored as a flat `Float64Array` of length
 * `zoneCount * zoneCount`. Indexing by table name or 0-based index
 * returns the stored array. In-place edits apply immediately.
 */
export class Matrix {
  /**
   * @param {number} zoneCount
   * @param {number} tableCount
   * @param {string} banner
   * @param {string} runId
   * @param {ReadonlyArray<Readonly<TableMeta>>} tableMetas
   * @param {ReadonlyArray<string>} tableNames
   * @param {ReadonlyMap<string, number>} tableIndexByName
   * @param {Map<string, Float64Array>} tables
   */
  constructor(
    zoneCount,
    tableCount,
    banner,
    runId,
    tableMetas,
    tableNames,
    tableIndexByName,
    tables,
  ) {
    /** @private */ this._zoneCount = zoneCount;
    /** @private */ this._tableCount = tableCount;
    /** @private */ this._banner = banner;
    /** @private */ this._runId = runId;
    /** @private */ this._tableMetas = tableMetas;
    /** @private */ this._tableNames = tableNames;
    /** @private */ this._tableIndexByName = tableIndexByName;
    /** @private */ this._tables = tables;
  }

  /**
   * Parse a `.mat` file from raw bytes.
   *
   * @param {Uint8Array} bytes  Raw `.mat` file content.
   * @param {FromBytesOptions} [options]
   * @returns {Matrix}
   */
  static fromBytes(bytes, options) {
    try {
      const result =
        options?.tables != null
          ? decodeTables(bytes, options.tables)
          : decode(bytes);

      const zoneCount = result.zoneCount;
      const tableCount = result.tableCount;
      const banner = result.banner;
      const runId = result.runId;
      const names = result.tableNames;
      const typeCodes = result.tableTypeCodes;

      const tableMetas = [];
      const tableNames = [];
      const tableIndexByName = new Map();
      const tables = new Map();

      for (let i = 0; i < names.length; i++) {
        const name = names[i];
        tableMetas.push(
          Object.freeze({ index: i, name, typeCode: typeCodes[i] }),
        );
        tableNames.push(name);
        tableIndexByName.set(name, i);
        tables.set(name, result.tableData(name));
      }

      result.free();

      return new Matrix(
        zoneCount,
        tableCount,
        banner,
        runId,
        Object.freeze(tableMetas),
        Object.freeze(tableNames),
        tableIndexByName,
        tables,
      );
    } catch (error) {
      rethrow(error);
    }
  }

  /**
   * Create a new empty matrix.
   *
   * Table definitions are validated eagerly via the Rust codec. Cell data
   * is zero-initialised on the JS side.
   *
   * @param {number} zoneCount  Number of zones (1..32000).
   * @param {CreateTableDef[]} tableDefs  Table definitions.
   * @param {CreateOptions} [options]
   * @returns {Matrix}
   */
  static create(zoneCount, tableDefs, options) {
    if (
      typeof zoneCount !== "number" ||
      !Number.isInteger(zoneCount) ||
      zoneCount < 1 ||
      zoneCount > 32000
    ) {
      throw new DespinaError(
        "zoneCount must be an integer within 1..32000",
        "validation_error",
      );
    }
    if (!Array.isArray(tableDefs) || tableDefs.length === 0) {
      throw new DespinaError(
        "tableDefs must be a non-empty array",
        "validation_error",
      );
    }

    const cellCount = zoneCount * zoneCount;
    const tableNames = [];
    const typeCodes = [];
    const tableMetas = [];
    const tableIndexByName = new Map();
    const tables = new Map();

    for (let i = 0; i < tableDefs.length; i++) {
      const def = tableDefs[i];
      const name = def.name;
      const typeCode = def.typeCode;
      if (typeof name !== "string" || name.length === 0) {
        throw new DespinaError(
          `tableDefs[${i}].name must be a non-empty string`,
          "validation_error",
        );
      }
      if (typeof typeCode !== "string") {
        throw new DespinaError(
          `tableDefs[${i}].typeCode must be a string`,
          "validation_error",
        );
      }
      tableNames.push(name);
      typeCodes.push(typeCode);
      tableMetas.push(Object.freeze({ index: i, name, typeCode }));
      tableIndexByName.set(name, i);
      tables.set(name, new Float64Array(cellCount));
    }

    // Validate via the Rust codec (catches invalid zone counts, type codes,
    // table names, duplicate names, etc.) without allocating or encoding any
    // cell data.
    let banner;
    let runId;
    try {
      const result = validateSchema(
        zoneCount,
        tableNames,
        typeCodes,
        options?.banner ?? null,
        options?.runId ?? null,
      );
      banner = result.banner;
      runId = result.runId;
      result.free();
    } catch (error) {
      rethrow(error);
    }

    return new Matrix(
      zoneCount,
      tableDefs.length,
      banner,
      runId,
      Object.freeze(tableMetas),
      Object.freeze(tableNames),
      tableIndexByName,
      tables,
    );
  }

  /** Number of zones (square dimension). */
  get zoneCount() {
    return this._zoneCount;
  }

  /** Number of tables. */
  get tableCount() {
    return this._tableCount;
  }

  /** Banner text from the file header. */
  get banner() {
    return this._banner;
  }

  /** Run identifier from the file header. */
  get runId() {
    return this._runId;
  }

  /** Frozen array of table metadata objects in header order. */
  get tables() {
    return this._tableMetas;
  }

  /** Frozen array of table names in header order. */
  get tableNames() {
    return this._tableNames;
  }

  /**
   * Get the `Float64Array` backing a table.
   *
   * Returns a live view. Mutations apply immediately.
   *
   * @param {string | number} table  Table name or 0-based index.
   * @returns {Float64Array}
   */
  table(table) {
    const name = resolveTableName(
      table,
      this._tableNames,
      this._tableIndexByName,
      this._tableCount,
    );
    return this._tables.get(name);
  }

  /**
   * Replace all data for a table with shape validation.
   *
   * @param {string | number} table  Table name or 0-based index.
   * @param {Float64Array | ArrayLike<number>} values  Flat row-major data.
   */
  setTable(table, values) {
    const name = resolveTableName(
      table,
      this._tableNames,
      this._tableIndexByName,
      this._tableCount,
    );
    const expected = this._zoneCount * this._zoneCount;
    const array =
      values instanceof Float64Array ? values : new Float64Array(values);
    if (array.length !== expected) {
      throw new DespinaError(
        `expected ${expected} values, got ${array.length}`,
        "shape_mismatch",
      );
    }
    this._tables.set(name, array);
  }

  /**
   * Get a single cell value (1-based origin and destination).
   *
   * @param {string | number} table  Table name or 0-based index.
   * @param {number} origin       1-based origin zone.
   * @param {number} destination  1-based destination zone.
   * @returns {number}
   */
  get(table, origin, destination) {
    const data = this.table(table);
    const zc = this._zoneCount;
    if (origin < 1 || origin > zc || destination < 1 || destination > zc) {
      throw new DespinaError(
        `origin and destination must be within 1..${zc} (got origin=${origin}, destination=${destination})`,
        "index_out_of_bounds",
      );
    }
    return data[(origin - 1) * zc + (destination - 1)];
  }

  /**
   * Set a single cell value (1-based origin and destination).
   *
   * @param {string | number} table  Table name or 0-based index.
   * @param {number} origin       1-based origin zone.
   * @param {number} destination  1-based destination zone.
   * @param {number} value        Value to set.
   */
  set(table, origin, destination, value) {
    const data = this.table(table);
    const zc = this._zoneCount;
    if (origin < 1 || origin > zc || destination < 1 || destination > zc) {
      throw new DespinaError(
        `origin and destination must be within 1..${zc} (got origin=${origin}, destination=${destination})`,
        "index_out_of_bounds",
      );
    }
    data[(origin - 1) * zc + (destination - 1)] = value;
  }

  /**
   * Return a `Float64Array` subarray view for one row (1-based origin).
   *
   * The returned view shares the underlying buffer. Edits are visible
   * in the table immediately.
   *
   * @param {string | number} table  Table name or 0-based index.
   * @param {number} origin  1-based origin zone.
   * @returns {Float64Array}
   */
  row(table, origin) {
    const data = this.table(table);
    const zc = this._zoneCount;
    if (origin < 1 || origin > zc) {
      throw new DespinaError(
        `origin must be within 1..${zc} (got ${origin})`,
        "index_out_of_bounds",
      );
    }
    const start = (origin - 1) * zc;
    return data.subarray(start, start + zc);
  }

  /**
   * Sum of all cells in a table.
   *
   * @param {string | number} table  Table name or 0-based index.
   * @returns {number}
   */
  tableTotal(table) {
    const data = this.table(table);
    let total = 0;
    for (let i = 0; i < data.length; i++) {
      total += data[i];
    }
    return total;
  }

  /**
   * Sum of diagonal cells in a table.
   *
   * @param {string | number} table  Table name or 0-based index.
   * @returns {number}
   */
  tableDiagonalTotal(table) {
    const data = this.table(table);
    const zc = this._zoneCount;
    let total = 0;
    for (let i = 0; i < zc; i++) {
      total += data[i * zc + i];
    }
    return total;
  }

  /**
   * Serialise to `.mat` bytes.
   *
   * @returns {Uint8Array}
   */
  toBytes() {
    const tableNames = [];
    const typeCodes = [];
    const data = {};

    for (const meta of this._tableMetas) {
      tableNames.push(meta.name);
      typeCodes.push(meta.typeCode);
      data[meta.name] = this._tables.get(meta.name);
    }

    try {
      return encode(
        this._zoneCount,
        tableNames,
        typeCodes,
        data,
        this._banner,
        this._runId,
      );
    } catch (error) {
      rethrow(error);
    }
  }

  /**
   * Create a deep copy of this matrix.
   *
   * All `Float64Array` table data is copied. Mutations to the clone do
   * not affect the original.
   *
   * @returns {Matrix}
   */
  copy() {
    const tables = new Map();
    for (const name of this._tableNames) {
      tables.set(name, new Float64Array(this._tables.get(name)));
    }
    return new Matrix(
      this._zoneCount,
      this._tableCount,
      this._banner,
      this._runId,
      this._tableMetas,
      this._tableNames,
      new Map(this._tableIndexByName),
      tables,
    );
  }

  /**
   * Check whether a table name exists.
   *
   * @param {string} name
   * @returns {boolean}
   */
  has(name) {
    return this._tableIndexByName.has(name);
  }

  /**
   * Iterate table names in header order.
   *
   * @returns {IterableIterator<string>}
   */
  keys() {
    return this._tableNames[Symbol.iterator]();
  }

  /**
   * Iterate table `Float64Array` values in header order.
   *
   * @returns {IterableIterator<Float64Array>}
   */
  *values() {
    for (const name of this._tableNames) {
      yield this._tables.get(name);
    }
  }

  /**
   * Iterate `[name, Float64Array]` entries in header order.
   *
   * @returns {IterableIterator<[string, Float64Array]>}
   */
  *entries() {
    for (const name of this._tableNames) {
      yield [name, this._tables.get(name)];
    }
  }

  /**
   * Iterate table names in header order.
   *
   * @returns {IterableIterator<string>}
   */
  [Symbol.iterator]() {
    return this.keys();
  }
}
