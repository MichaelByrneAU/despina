/**
 * Error types for the despina WASM package.
 *
 * `DespinaError` wraps structured errors from the Rust codec, preserving the
 * machine-readable `kind` discriminant and optional byte `offset`.
 */

/**
 * Structured error from the despina codec.
 *
 * Every error carries a `.kind` string suitable for programmatic matching and
 * an optional `.offset` indicating the byte position of the failure.
 */
export class DespinaError extends Error {
  /**
   * @param {string} message  Human-readable description.
   * @param {string} kind     Machine-readable error discriminant.
   * @param {number} [offset] Byte offset where the error occurred.
   */
  constructor(message, kind, offset) {
    super(message);
    this.name = "DespinaError";

    /** @type {string} Machine-readable error kind. */
    this.kind = kind;

    /** @type {number | undefined} Byte offset of the failure, if applicable. */
    this.offset = offset;
  }
}

/**
 * Re-throw a wasm-bindgen JS `Error` as a `DespinaError`.
 *
 * The Rust codec attaches `.kind` and optionally `.offset` to the JS Error
 * object. This helper copies those properties into a proper `DespinaError`.
 *
 * @param {unknown} error  The caught value from a WASM call.
 * @returns {never}
 */
export function rethrow(error) {
  if (error instanceof DespinaError) {
    throw error;
  }

  if (error instanceof Error) {
    const kind = /** @type {string | undefined} */ (
      /** @type {any} */ (error).kind
    );
    const offset = /** @type {number | undefined} */ (
      /** @type {any} */ (error).offset
    );
    throw new DespinaError(error.message, kind ?? "unknown", offset);
  }

  throw new DespinaError(String(error), "unknown", undefined);
}
