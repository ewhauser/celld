import { Buffer } from "buffer";
import {
  WorkspaceFS,
  type Limits,
  type OpenFlags,
  type Stat,
} from "./filesystem.ts";
import { check, SandboxError, type Storage } from "./storage.ts";

/** TypeScript and the helper share celld's inode, quota and handle authority. */
export class NativeWorkspaceFS extends WorkspaceFS {
  private readonly invoke: (
    operation: unknown,
    data?: Uint8Array,
  ) => string | Uint8Array;
  private readonly guard: () => void;
  constructor(
    storage: Storage,
    limits: Partial<Limits> | undefined,
    invoke: (operation: unknown, data?: Uint8Array) => string | Uint8Array,
    guard: () => void,
  ) {
    super(storage, limits, guard);
    this.invoke = invoke;
    this.guard = guard;
    this.call({ op: "configure", limits: this.limits });
  }
  private call(operation: Record<string, unknown>, data?: Uint8Array): any {
    const reply = this.invoke(operation, data);
    if (reply instanceof Uint8Array) return Buffer.from(reply);
    const result = JSON.parse(reply);
    if (result.code) throw new SandboxError(result.code);
    return result.value;
  }
  override stat(path: string): Stat {
    return this.call({ op: "stat", path });
  }
  override list(path: string): (Stat & { name: string })[] {
    return this.call({ op: "list", path });
  }
  override mkdir(path: string) {
    this.guard();
    this.call({ op: "mkdir", path });
  }
  override open(path: string, flags: OpenFlags): number {
    check(Object.values(flags).every((v) => typeof v === "boolean"));
    // Native open owns a short transaction, including create/truncate.
    this.guard();
    return this.call({ ...flags, op: "open", path }).handle;
  }
  override close(handle: number) {
    this.call({ op: "close", handle });
  }
  override closeAll() {
    this.call({ op: "closeAll" });
  }
  override fstat(handle: number): Stat {
    return this.call({ op: "fstat", handle });
  }
  override read(handle: number, offset: number, size: number): Buffer {
    return this.call({ op: "read", handle, offset, size });
  }
  override write(
    handle: number,
    offset: number,
    data: Uint8Array,
  ): { written: number; offset: number } {
    this.guard();
    check(data instanceof Uint8Array && data.length <= 65536);
    return this.call({ op: "write", handle, offset }, data);
  }
  override truncate(handle: number, size: number) {
    this.guard();
    this.call({ op: "truncate", handle, size });
  }
  override readFile(path: string): Buffer {
    return this.call({ op: "readFile", path });
  }
  override writeFile(path: string, data: Uint8Array) {
    this.guard();
    check(data instanceof Uint8Array && data.length <= 1024 * 1024, "EFBIG");
    this.call({ op: "writeFile", path }, data);
  }
  override unlink(path: string) {
    this.guard();
    this.call({ op: "unlink", path });
  }
  override rmdir(path: string) {
    this.guard();
    this.call({ op: "rmdir", path });
  }
  override rename(path: string, to: string) {
    this.guard();
    this.call({ op: "rename", path, to });
  }
}
