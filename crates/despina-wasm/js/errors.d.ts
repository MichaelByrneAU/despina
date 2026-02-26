/**
 * Structured error from the despina codec.
 *
 * Every error carries a `.kind` string suitable for programmatic matching and
 * an optional `.offset` indicating the byte position of the failure.
 */
export declare class DespinaError extends Error {
  /** Machine-readable error kind (e.g. `"unexpected_eof"`, `"invalid_par"`). */
  readonly kind: string;

  /** Byte offset where the error occurred, if applicable. */
  readonly offset: number | undefined;

  constructor(message: string, kind: string, offset?: number);
}

/**
 * Re-throw a wasm-bindgen JS `Error` as a `DespinaError`.
 *
 * Call this in a `catch` block around any raw WASM codec call.
 */
export declare function rethrow(error: unknown): never;
