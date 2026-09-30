// Parquet footer reader for the bucket telemetry sink.
//
// The roadmap asks for the Parquet *schema and records*, so reading the object
// listing is not enough: the file itself has to be opened and its declared
// columns and row count read back. A full Parquet reader is not needed for
// that — the schema and the row count live in the uncompressed Thrift
// FileMetaData footer, which this module decodes directly. Column *values*
// live in compressed pages and are deliberately out of scope; see
// docs/CLI-TELEMETRY.md for what that leaves unasserted.
//
// Layout: "PAR1" <row groups> <FileMetaData> <u32le footer length> "PAR1".
// FileMetaData is Thrift compact protocol:
//   {1: version i32, 2: schema list<SchemaElement>, 3: num_rows i64, ...}
//   SchemaElement{4: name binary}
// The schema list is a flat pre-order tree whose first element is the unnamed
// root, so the leaf/edge columns are every element after it.

export class ParquetError extends Error {}

export interface ParquetFooter {
  readonly version: number;
  readonly rows: number;
  /** Column names in schema order, excluding the synthetic root element. */
  readonly columns: readonly string[];
}

const MAGIC = [0x50, 0x41, 0x52, 0x31]; // "PAR1"

const startsWithMagic = (bytes: Uint8Array, offset: number) =>
  MAGIC.every((byte, index) => bytes[offset + index] === byte);

/** Thrift compact protocol element types, as far as skipping needs them. */
const BOOLEAN_TRUE = 1;
const BOOLEAN_FALSE = 2;
const BYTE = 3;
const I16 = 4;
const I32 = 5;
const I64 = 6;
const DOUBLE = 7;
const BINARY = 8;
const LIST = 9;
const SET = 10;
const MAP = 11;
const STRUCT = 12;

class CompactReader {
  private offset = 0;
  constructor(private readonly bytes: Uint8Array) {}

  private byte() {
    if (this.offset >= this.bytes.length)
      throw new ParquetError("truncated footer");
    return this.bytes[this.offset++]!;
  }

  varint(): bigint {
    let value = 0n;
    let shift = 0n;
    for (;;) {
      if (shift > 63n) throw new ParquetError("oversized varint");
      const byte = this.byte();
      value |= BigInt(byte & 0x7f) << shift;
      if ((byte & 0x80) === 0) return value;
      shift += 7n;
    }
  }

  /** Thrift zigzag-encodes every signed integer in the compact protocol. */
  zigzag(): number {
    const raw = this.varint();
    return Number((raw >> 1n) ^ -(raw & 1n));
  }

  binary(): string {
    const length = Number(this.varint());
    if (length < 0 || this.offset + length > this.bytes.length)
      throw new ParquetError("truncated binary");
    const slice = this.bytes.subarray(this.offset, this.offset + length);
    this.offset += length;
    return new TextDecoder().decode(slice);
  }

  /** Reads a list/set header, returning its element type and size. */
  private listHeader() {
    const header = this.byte();
    const elementType = header & 0x0f;
    const packed = header >> 4;
    return {
      elementType,
      size: packed === 15 ? Number(this.varint()) : packed,
    };
  }

  skip(type: number): void {
    switch (type) {
      case BOOLEAN_TRUE:
      case BOOLEAN_FALSE:
        return;
      case BYTE:
        this.byte();
        return;
      case I16:
      case I32:
      case I64:
        this.zigzag();
        return;
      case DOUBLE:
        for (let index = 0; index < 8; index++) this.byte();
        return;
      case BINARY:
        this.binary();
        return;
      case LIST:
      case SET: {
        const { elementType, size } = this.listHeader();
        for (let index = 0; index < size; index++) this.skip(elementType);
        return;
      }
      case MAP: {
        const size = Number(this.varint());
        if (size === 0) return;
        const types = this.byte();
        for (let index = 0; index < size; index++) {
          this.skip(types >> 4);
          this.skip(types & 0x0f);
        }
        return;
      }
      case STRUCT:
        this.struct(() => false);
        return;
      default:
        throw new ParquetError(`unsupported thrift type ${type}`);
    }
  }

  /**
   * Reads one struct. `onField` returns true when it consumed the value
   * itself; otherwise the value is skipped.
   */
  struct(onField: (id: number, type: number) => boolean): void {
    let previous = 0;
    for (;;) {
      const header = this.byte();
      if (header === 0) return;
      const delta = header >> 4;
      const type = header & 0x0f;
      const id = delta === 0 ? this.zigzag() : previous + delta;
      previous = id;
      if (!onField(id, type)) this.skip(type);
    }
  }

  list(onElement: () => void): void {
    const { size } = this.listHeader();
    for (let index = 0; index < size; index++) onElement();
  }
}

export const readParquetFooter = (bytes: Uint8Array): ParquetFooter => {
  // 4 magic + 4 length + 4 magic is the smallest conceivable file.
  if (bytes.length < 12) throw new ParquetError("file is too short");
  if (!startsWithMagic(bytes, 0))
    throw new ParquetError("missing leading PAR1 magic");
  if (!startsWithMagic(bytes, bytes.length - 4))
    throw new ParquetError("missing trailing PAR1 magic");
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const length = view.getUint32(bytes.length - 8, true);
  const start = bytes.length - 8 - length;
  if (length === 0 || start < 4)
    throw new ParquetError(`implausible footer length ${length}`);
  const reader = new CompactReader(bytes.subarray(start, bytes.length - 8));
  let version: number | undefined;
  let rows: number | undefined;
  const columns: string[] = [];
  reader.struct((id) => {
    if (id === 1) {
      version = reader.zigzag();
      return true;
    }
    if (id === 3) {
      rows = reader.zigzag();
      return true;
    }
    if (id === 2) {
      reader.list(() => {
        reader.struct((fieldId) => {
          if (fieldId !== 4) return false;
          columns.push(reader.binary());
          return true;
        });
      });
      return true;
    }
    return false;
  });
  if (version === undefined || rows === undefined)
    throw new ParquetError("footer is missing version or num_rows");
  if (columns.length === 0) throw new ParquetError("footer declares no schema");
  // The first schema element is the unnamed root of the schema tree.
  return { version, rows, columns: columns.slice(1) };
};
