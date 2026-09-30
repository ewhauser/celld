// A tagged observation tree: user objects can never impersonate a scalar tag.
// This is an observation encoder, not an RPC or persistence implementation.
export type Encoded = { type: string; value?: unknown };
export const encode = (value: unknown, seen = new Set<object>()): Encoded => {
  if (value === undefined) return { type: "undefined" };
  if (value === null) return { type: "null" };
  if (typeof value === "number")
    return {
      type: "number",
      value: Object.is(value, -0)
        ? "-0"
        : Number.isNaN(value)
          ? "NaN"
          : value === Infinity
            ? "+Infinity"
            : value === -Infinity
              ? "-Infinity"
              : value,
    };
  if (typeof value === "bigint")
    return { type: "bigint", value: String(value) };
  if (typeof value === "string" || typeof value === "boolean")
    return { type: typeof value, value };
  if (typeof value !== "object")
    throw new TypeError(`Unsupported observation: ${typeof value}`);
  if (seen.has(value)) throw new TypeError("Cyclic observation");
  seen.add(value);
  try {
    if (value instanceof Date)
      return { type: "Date", value: value.toISOString() };
    if (value instanceof ArrayBuffer)
      return { type: "ArrayBuffer", value: [...new Uint8Array(value)] };
    if (ArrayBuffer.isView(value))
      return {
        type: value.constructor.name,
        value: [
          ...new Uint8Array(value.buffer, value.byteOffset, value.byteLength),
        ],
      };
    if (value instanceof Map)
      return {
        type: "Map",
        value: [...value].map(([k, v]) => [encode(k, seen), encode(v, seen)]),
      };
    if (value instanceof Set)
      return { type: "Set", value: [...value].map((v) => encode(v, seen)) };
    if (Array.isArray(value))
      return {
        type: "Array",
        value: Array.from({ length: value.length }, (_, i) =>
          i in value ? encode(value[i], seen) : { type: "hole" },
        ),
      };
    if (
      Object.getPrototypeOf(value) !== Object.prototype &&
      Object.getPrototypeOf(value) !== null
    )
      throw new TypeError("Unsupported object prototype");
    return {
      type: "Object",
      value: Object.keys(value)
        .sort()
        .map((key) => [
          key,
          encode((value as Record<string, unknown>)[key], seen),
        ]),
    };
  } finally {
    seen.delete(value);
  }
};
export const richValue = () => ({
  missing: undefined,
  nil: null,
  big: 9007199254740993n,
  nan: NaN,
  negativeZero: -0,
  positiveInfinity: Infinity,
  negativeInfinity: -Infinity,
  bytes: new Uint8Array([0, 127, 128, 255]),
  buffer: new Uint8Array([8, 9]).buffer,
  date: new Date("2020-01-02T03:04:05.000Z"),
  map: new Map<unknown, unknown>([
    ["first", 1],
    [2, "second"],
  ]),
  set: new Set(["b", "a"]),
  nested: [false, "", { x: undefined }],
});
