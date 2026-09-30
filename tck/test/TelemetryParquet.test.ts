import { expect, it } from "@effect/vitest";
import { Effect } from "effect";
import { ParquetError, readParquetFooter } from "../src/TelemetryParquet.js";

// Thrift compact protocol, written out so the reader is checked against the
// encoding rules rather than against a file some other reader produced.
const varint = (value: number) => {
  const out: number[] = [];
  let remaining = value;
  do {
    let byte = remaining & 0x7f;
    remaining = Math.floor(remaining / 128);
    if (remaining > 0) byte |= 0x80;
    out.push(byte);
  } while (remaining > 0);
  return out;
};
/** Thrift zigzags every signed integer in the compact protocol. */
const zigzag = (value: number) =>
  varint(value < 0 ? -2 * value - 1 : 2 * value);
const header = (delta: number, type: number) => [(delta << 4) | type];
const binary = (value: string) => {
  const encoded = [...new TextEncoder().encode(value)];
  return [...varint(encoded.length), ...encoded];
};
const I32 = 5;
const I64 = 6;
const BINARY = 8;
const LIST = 9;
const STRUCT = 12;
const STOP = [0];

const schemaElement = (name: string) => [
  // Field 4 (name), reached as a delta of 4 from field 0.
  ...header(4, BINARY),
  ...binary(name),
  ...STOP,
];

const fileMetaData = (options: {
  readonly version: number;
  readonly columns: readonly string[];
  readonly rows: number;
}) => {
  const elements = options.columns.map(schemaElement);
  return [
    ...header(1, I32),
    ...zigzag(options.version),
    // Field 2: list<SchemaElement>. The size is packed into the high nibble
    // while it fits; 15 means a following varint carries it.
    ...header(1, LIST),
    ...(elements.length < 15
      ? [(elements.length << 4) | STRUCT]
      : [(15 << 4) | STRUCT, ...varint(elements.length)]),
    ...elements.flat(),
    ...header(1, I64),
    ...zigzag(options.rows),
    ...STOP,
  ];
};

const parquet = (options: {
  readonly version: number;
  readonly columns: readonly string[];
  readonly rows: number;
  readonly leadingMagic?: string;
  readonly trailingMagic?: string;
  readonly declaredLength?: number;
}) => {
  const footer = fileMetaData(options);
  const length = options.declaredLength ?? footer.length;
  const lengthBytes = [
    length & 0xff,
    (length >> 8) & 0xff,
    (length >> 16) & 0xff,
    (length >> 24) & 0xff,
  ];
  return new Uint8Array([
    ...new TextEncoder().encode(options.leadingMagic ?? "PAR1"),
    // Stand-in for the row-group data a real file carries.
    0x00,
    0x00,
    ...footer,
    ...lengthBytes,
    ...new TextEncoder().encode(options.trailingMagic ?? "PAR1"),
  ]);
};

// The trace schema celld v0.5.0 declared in a file it actually wrote.
const CELLD_TRACE_COLUMNS = [
  "celld_span",
  "node",
  "region",
  "trace_id",
  "span_id",
  "parent_span_id",
  "name",
  "kind",
  "start_unix_us",
  "duration_us",
  "ok",
  "error",
  "request_id",
  "cell",
  "epoch",
  "isolate",
  "queue_wait_us",
  "url",
  "http_status",
  "parent_remote",
] as const;

// The FileMetaData footer of a Parquet file celld v0.5.0 wrote to the bucket
// sink, framed as a minimal file. The row-group pages are omitted; the schema
// and the row count both live in this footer.
const CELLD_FOOTER =
  "UEFSMRUCGfwUSApjZWxsZF9zcGFuFSYAFQwlABgEbm9kZSUATBwAAAAVDCUAGAZyZWdpb24lAEwc" +
  "AAAAFQwlABgIdHJhY2VfaWQlAEwcAAAAFQwlABgHc3Bhbl9pZCUATBwAAAAVDCUCGA5wYXJlbnRf" +
  "c3Bhbl9pZCUATBwAAAAVDCUAGARuYW1lJQBMHAAAABUCJQAYBGtpbmQlFkysEwgSAAAAFQQlABgN" +
  "c3RhcnRfdW5peF91cwAVBCUAGAtkdXJhdGlvbl91cwAVACUAGAJvawAVDCUCGAVlcnJvciUATBwA" +
  "AAAVDCUCGApyZXF1ZXN0X2lkJQBMHAAAABUMJQIYBGNlbGwlAEwcAAAAFQQlAhgFZXBvY2gAFQQl" +
  "AhgHaXNvbGF0ZQAVBCUCGA1xdWV1ZV93YWl0X3VzABUMJQIYA3VybCUATBwAAAAVAiUCGAtodHRw" +
  "X3N0YXR1cyUYTKwTEBIAAAAVACUCGA1wYXJlbnRfcmVtb3RlABYqGRwZ/BMmABwVDBk1AAYQGRgE" +
  "bm9kZRUMFioWlAEWuAEmiAEmCBw2ACglbm9kZV8xMDVjNDJkM2U3ODE4YzM4M2ViMWY3MzY2ZGNk" +
  "MjNiORglbm9kZV8xMDVjNDJkM2U3ODE4YzM4M2ViMWY3MzY2ZGNkMjNiORERABksFQQVABUCABUA" +
  "FRAVAgA8FpIMAAAWwjAVHhayIhWyAQAmABwVDBk1AAYQGRgGcmVnaW9uFQwWKhZcFoABJogCJsAB" +
  "HDYAKAl1cy1lYXN0LTEYCXVzLWVhc3QtMRERABksFQQVABUCABUAFRAVAgA8FvoCAAAW4DAVHhbk" +
  "IxVCACYAHBUMGTUABhAZGAh0cmFjZV9pZBUMFioWiAQWggIm+AMmwAIcNgAoIDExMTEyMjIyMzMz" +
  "MzQ0NDQ1NTU1NjY2Njc3Nzc4ODg1GCAwZmM5NWVmZTgxNTZmM2M4MzUxYjhhOTgwOTI2NjhiMRER" +
  "ABksFQQVABUCABUAFRAVAgAW1CEVXhwWwAoAABb+MBUeFqYkFZ4BACYAHBUMGTUABhAZGAdzcGFu" +
  "X2lkFQwWKhasBxbuBCbaCCbCBBw2ACgQZjM3MGEzYmMwMWJhNzcwNhgQMDgyZDI2OTA2ZTRiZGM1" +
  "OBERABksFQQVABUCABUAFRAVAgA8FqAFAAAWnDEVHhbEJRVeACYAHBUMGTUABhAZGA5wYXJlbnRf" +
  "c3Bhbl9pZBUMFioWlAcWugQmhA0msAkcNgIoEGYzNzBhM2JjMDFiYTc3MDYYEDBiYjYwYjQ4ZTkx" +
  "YjJhZTcREQAZLBUEFQAVAgAVABUQFQIAPBaABSkmAigAABa6MRUeFqImFWYAJgAcFQwZNQAGEBkY" +
  "BG5hbWUVDBYqFtYBFuQBJooPJuoNHDYAKAVmZXRjaBgQY2VsbGQuY2VsbF9mZXRjaBERABksFQQV" +
  "ABUCABUAFRAVAgA8FtIDAAAW2DEVHhaIJxVIACYAHBUCGTUABhAZGARraW5kFQwWKhZmFooBJpQQ" +
  "Js4PHDYAKAQDAAAAGAQBAAAAEREAGSwVBBUAFQIAFQAVEBUCAAAW9jEVFhbQJxUuACYAHBUEGTUA" +
  "BhAZGA1zdGFydF91bml4X3VzFQwWKha0AxbUAibWEibYEBwYCMmUOxrMWwYAGAhsGTgazFsGABYA" +
  "KAjJlDsazFsGABgIbBk4GsxbBgAREQAZLBUEFQAVAgAVABUQFQIAABaMMhUWFv4nFT4AJgAcFQQZ" +
  "NQAGEBkYC2R1cmF0aW9uX3VzFQwWKha0AxbEAiaaFSasExwYCP/zAAAAAAAAGAjWAAAAAAAAABYA" +
  "KAj/8wAAAAAAABgI1gAAAAAAAAAREQAZLBUEFQAVAgAVABUQFQIAABaiMhUWFrwoFT4AJgAcFQAZ" +
  "JQAGGRgCb2sVDBYqFigWOibwFTw2ACgBARgBARERABkcFQAVABUCAAAWuDIVFhb6KBUiACYAHBUM" +
  "GTUABhAZGAVlcnJvchUMFioWTBZwJtgWJqoWHDYqQhIAGSwVBBUAFQIAFQAVEBUCADwWACkmKgAA" +
  "ABbOMhUcFpwpFSYAJgAcFQwZNQAGEBkYCnJlcXVlc3RfaWQVDBYqFrYGFvwBJr4YJpoXHDYWKCBj" +
  "ZjQ3ZmI3NjljYWFlNDg1MDAwMDAwMDAwMDAwMDAwZhggY2Y0N2ZiNzY5Y2FhZTQ4NTAwMDAwMDAw" +
  "MDAwMDAwMDEREQAZLBUEFQAVAgAVABUQFQIAPBaABSkmFhQAABbqMhUeFsIpFaYBACYAHBUMGTUA" +
  "BhAZGARjZWxsFQwWKhbuARaAAibOGiaWGRw2HihAQ291bnRlcjo2MWU2N2M0Zjg0ZDJmMTdlZGZl" +
  "NTRmNDc5YWJmYjk0MzQ0NDdjYmJmMWZiMGM5MjFjNGM1ZTJlNhhAQ291bnRlcjo2MWU2N2M0Zjg0" +
  "ZDJmMTdlZGZlNTRmNDc5YWJmYjk0MzQ0NDdjYmJmMWZiMGM5MjFjNGM1ZTJlNRISABksFQQVABUC" +
  "ABUAFRAVAgA8FuAGKSYeDAAAFogzFR4W6CoVpgIAJgAcFQQZNQAGEBkYBWVwb2NoFQwWKhZiFoYB" +
  "JtQbJpYbHBgIAQAAAAAAAAAYCAEAAAAAAAAAFigoCAEAAAAAAAAAGAgBAAAAAAAAABERABksFQQV" +
  "ABUCABUAFRAVAgA8OSYoAgAAFqYzFRYWji0VRgAmABwVBBk1AAYQGRgHaXNvbGF0ZRUMFioWYhaG" +
  "ASbaHCacHBwYCAAAAAAAAAAAGAgAAAAAAAAAABYWKAgAAAAAAAAAABgIAAAAAAAAAAAREQAZLBUE" +
  "FQAVAgAVABUQFQIAPDkmFhQAABa8MxUWFtQtFUYAJgAcFQQZNQAGEBkYDXF1ZXVlX3dhaXRfdXMV" +
  "DBYqFuABFr4BJoweJqIdHBgIIQAAAAAAAAAYCAQAAAAAAAAAFhYoCCEAAAAAAAAAGAgEAAAAAAAA" +
  "ABERABksFQQVABUCABUAFRAVAgA8OSYWFAAAFtIzFRYWmi4VRgAmABwVDBk1AAYQGRgDdXJsFQwW" +
  "KhaIARasASbEHybgHhw2ICgXaHR0cDovL3Bxbm9kZTo4MDgwL3NpbmsYF2h0dHA6Ly9wcW5vZGU6" +
  "ODA4MC9zaW5rEREAGSwVBBUAFQIAFQAVEBUCADwW5gEpJiAKAAAW6DMVHhbgLhWCAQAmABwVAhk1" +
  "AAYQGRgLaHR0cF9zdGF0dXMVDBYqFloWfibCICaMIBw2ICgEyAAAABgEyAAAABERABksFQQVABUC" +
  "ABUAFRAVAgA8OSYgCgAAFoY0FRYW4i8VNgAmABwVABklAAYZGA1wYXJlbnRfcmVtb3RlFQwWKhY4" +
  "FkomiiE8NgIoAQEYAQAREQAZHBUAFQAVAgA8OSYCKAAAFpw0FRYWmDAVKgAWsiwWKiYIFswhFAAA" +
  "KBlwYXJxdWV0LXJzIHZlcnNpb24gNTkuMi4wGfwTHAAAHAAAHAAAHAAAHAAAHAAAHAAAHAAAHAAA" +
  "HAAAHAAAHAAAHAAAHAAAHAAAHAAAHAAAHAAAHAAAAFkKAABQQVIx";

it.effect("reads the footer celld v0.5.0 actually wrote", () =>
  Effect.sync(() => {
    const footer = readParquetFooter(Buffer.from(CELLD_FOOTER, "base64"));
    expect(footer.rows).toBe(21);
    expect(footer.columns).toEqual([...CELLD_TRACE_COLUMNS].slice(1));
  }),
);

it.effect("reads the declared schema and row count out of the footer", () =>
  Effect.sync(() => {
    const footer = readParquetFooter(
      parquet({ version: 1, columns: [...CELLD_TRACE_COLUMNS], rows: 21 }),
    );
    expect(footer.version).toBe(1);
    expect(footer.rows).toBe(21);
    // The first schema element is the unnamed root of the schema tree.
    expect(footer.columns).toEqual([...CELLD_TRACE_COLUMNS].slice(1));
    expect(footer.columns).toContain("trace_id");
    expect(footer.columns).toContain("duration_us");
  }),
);

it.effect("reads a schema whose element count needs its own varint", () =>
  Effect.sync(() => {
    // 20 elements is above the 15 the compact list header can pack inline, so
    // this is the path celld's real files take.
    const columns = Array.from({ length: 20 }, (_, index) => `column_${index}`);
    const footer = readParquetFooter(parquet({ version: 2, columns, rows: 7 }));
    expect(footer.rows).toBe(7);
    expect(footer.columns).toEqual(columns.slice(1));
  }),
);

it.effect("refuses a file that is not a readable Parquet footer", () =>
  Effect.sync(() => {
    const good = { version: 1, columns: ["root", "trace_id"], rows: 3 };
    // Not Parquet at all.
    expect(() => readParquetFooter(new Uint8Array(4))).toThrow(ParquetError);
    expect(() =>
      readParquetFooter(new TextEncoder().encode("not a parquet file")),
    ).toThrow(ParquetError);
    // Truncated below the minimum framing.
    expect(() => readParquetFooter(new Uint8Array([0x50, 0x41]))).toThrow(
      ParquetError,
    );
    // Magic present at one end only: a half-written upload must not read.
    expect(() =>
      readParquetFooter(parquet({ ...good, leadingMagic: "XXXX" })),
    ).toThrow(ParquetError);
    expect(() =>
      readParquetFooter(parquet({ ...good, trailingMagic: "XXXX" })),
    ).toThrow(ParquetError);
    // A footer length that points outside the file, in both directions.
    expect(() =>
      readParquetFooter(parquet({ ...good, declaredLength: 0 })),
    ).toThrow(ParquetError);
    expect(() =>
      readParquetFooter(parquet({ ...good, declaredLength: 100000 })),
    ).toThrow(ParquetError);
  }),
);

it.effect("refuses a footer that declares no schema", () =>
  Effect.sync(() => {
    expect(() =>
      readParquetFooter(parquet({ version: 1, columns: [], rows: 5 })),
    ).toThrow(ParquetError);
  }),
);

it.effect("refuses a footer with no version or row count", () =>
  Effect.sync(() => {
    // A FileMetaData carrying only the schema: the row count is what makes
    // "records were written" checkable, so its absence must not read as zero.
    const elements = [schemaElement("root"), schemaElement("trace_id")];
    const footer = [
      ...header(2, LIST),
      (elements.length << 4) | STRUCT,
      ...elements.flat(),
      ...STOP,
    ];
    const bytes = new Uint8Array([
      ...new TextEncoder().encode("PAR1"),
      0x00,
      ...footer,
      footer.length & 0xff,
      0,
      0,
      0,
      ...new TextEncoder().encode("PAR1"),
    ]);
    expect(() => readParquetFooter(bytes)).toThrow(ParquetError);
  }),
);
