/**
 * Lightweight table metadata.
 *
 * `index` is 0-based, matching JavaScript convention.
 */
export interface TableMeta {
  /** 0-based table position. */
  readonly index: number;
  /** Table name. */
  readonly name: string;
  /** Storage type code token (`'0'`..`'9'`, `'S'`, or `'D'`). */
  readonly typeCode: string;
}

/** Options for {@link Matrix.fromBytes}. */
export interface FromBytesOptions {
  /** Subset of table names to load. Omit to load all tables. */
  tables?: string[];
}

/** Table definition for {@link Matrix.create}. */
export interface CreateTableDef {
  /** Table name. */
  name: string;
  /** Type code token (`'0'`..`'9'`, `'S'`, or `'D'`). */
  typeCode: string;
}

/** Options for {@link Matrix.create}. */
export interface CreateOptions {
  /** Banner text. */
  banner?: string;
  /** Run identifier. */
  runId?: string;
}

/**
 * Loaded `.mat` matrix with random table and cell access.
 *
 * Each table is stored as a `Float64Array` of length `zoneCount * zoneCount`.
 * Table lookup accepts either a name (`string`) or a 0-based index (`number`).
 */
export declare class Matrix {
  private constructor(
    zoneCount: number,
    tableCount: number,
    banner: string,
    runId: string,
    tableMetas: readonly TableMeta[],
    tableNames: readonly string[],
    tableIndexByName: ReadonlyMap<string, number>,
    tables: Map<string, Float64Array>,
  );

  /**
   * Parse a `.mat` file from raw bytes.
   *
   * @param bytes  Raw `.mat` file content.
   * @param options  Optional: restrict which tables to load.
   * @throws {import('./errors.js').DespinaError} On parse failure.
   */
  static fromBytes(bytes: Uint8Array, options?: FromBytesOptions): Matrix;

  /**
   * Create a new empty matrix.
   *
   * @param zoneCount  Number of zones (1..32000).
   * @param tableDefs  Table definitions.
   * @param options    Optional banner and run ID.
   * @throws {import('./errors.js').DespinaError} On validation failure.
   */
  static create(
    zoneCount: number,
    tableDefs: CreateTableDef[],
    options?: CreateOptions,
  ): Matrix;

  /** Number of zones (square dimension). */
  readonly zoneCount: number;

  /** Number of tables. */
  readonly tableCount: number;

  /** Banner text from the file header. */
  readonly banner: string;

  /** Run identifier from the file header. */
  readonly runId: string;

  /** Frozen array of table metadata in header order. */
  readonly tables: readonly TableMeta[];

  /** Frozen array of table names in header order. */
  readonly tableNames: readonly string[];

  /**
   * Get the `Float64Array` backing a table (live view).
   *
   * @param table  Table name or 0-based index.
   */
  table(table: string | number): Float64Array;

  /**
   * Replace all data for a table.
   *
   * @param table   Table name or 0-based index.
   * @param values  Flat row-major data (`zoneCount * zoneCount` elements).
   */
  setTable(table: string | number, values: Float64Array | ArrayLike<number>): void;

  /**
   * Get a single cell value (1-based origin and destination).
   */
  get(table: string | number, origin: number, destination: number): number;

  /**
   * Set a single cell value (1-based origin and destination).
   */
  set(
    table: string | number,
    origin: number,
    destination: number,
    value: number,
  ): void;

  /**
   * Return a `Float64Array` subarray view for one row (1-based origin).
   *
   * The returned view shares the underlying buffer.
   */
  row(table: string | number, origin: number): Float64Array;

  /** Sum of all cells in a table. */
  tableTotal(table: string | number): number;

  /** Sum of diagonal cells in a table. */
  tableDiagonalTotal(table: string | number): number;

  /**
   * Serialise to `.mat` bytes.
   *
   * @throws {import('./errors.js').DespinaError} On encode failure.
   */
  toBytes(): Uint8Array;

  /**
   * Create a deep copy of this matrix.
   *
   * All table data is copied. Mutations to the clone do not affect the
   * original.
   */
  copy(): Matrix;

  /** Check whether a table name exists. */
  has(name: string): boolean;

  /** Iterate table names in header order. */
  keys(): IterableIterator<string>;

  /** Iterate table `Float64Array` values in header order. */
  values(): IterableIterator<Float64Array>;

  /** Iterate `[name, Float64Array]` entries in header order. */
  entries(): IterableIterator<[string, Float64Array]>;

  /** Iterate table names in header order. */
  [Symbol.iterator](): IterableIterator<string>;
}
