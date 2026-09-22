import { Buffer } from "buffer";
import { check, integer, SandboxError, type Storage } from "./storage.ts";

export interface Limits {
  maxBytes: number;
  maxFileBytes: number;
  maxInodes: number;
  maxHandles: number;
}
export interface OpenFlags {
  read?: boolean;
  write?: boolean;
  append?: boolean;
  create?: boolean;
  create_new?: boolean;
  truncate?: boolean;
}
interface Handle {
  ino: number;
  read: boolean;
  write: boolean;
  append: boolean;
}
export interface Stat {
  ino: number;
  size: number;
  dir: boolean;
  mode: number;
  atime: number;
  mtime: number;
  ctime: number;
}
const FILE = 0o100644,
  DIR = 0o40755;
const MAX_IO = 65536;

/** AgentFS 0.4 filesystem on the cell's managed SQLite connection.
 * All mutations are synchronous short transactions. Sparse reads synthesize
 * holes, so growth never allocates a buffer proportional to the file size.
 * No symlinks or open-inode deletion: explicitly rejected, never half emulated.
 */
export class WorkspaceFS {
  readonly storage: Storage;
  readonly limits: Limits;
  private chunkSize = 4096;
  private handles = new Map<number, Handle>();
  private nextHandle = 1;
  private beforeMutation: () => void;
  constructor(
    storage: Storage,
    limits: Partial<Limits> = {},
    beforeMutation = () => {},
  ) {
    this.storage = storage;
    this.limits = {
      maxBytes: 64 * 1024 * 1024,
      maxFileBytes: 16 * 1024 * 1024,
      maxInodes: 4096,
      maxHandles: 128,
      ...limits,
    };
    for (const v of Object.values(this.limits)) check(integer(v) > 0);
    this.beforeMutation = beforeMutation;
    storage.transactionSync(() => {
      const exists = this.rows(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='fs_config'",
      ).length;
      if (exists) {
        const version = this.rows(
          "SELECT value FROM fs_config WHERE key='schema_version'",
        )[0]?.value;
        check(
          version === "0.4",
          "EPROTONOSUPPORT",
          "AgentFS schema must be 0.4; import/migrate explicitly",
        );
        this.chunkSize = Number(
          this.rows("SELECT value FROM fs_config WHERE key='chunk_size'")[0]
            ?.value,
        );
        check(
          Number.isSafeInteger(this.chunkSize) &&
            this.chunkSize >= 512 &&
            this.chunkSize <= 65536,
          "EPROTONOSUPPORT",
        );
      }
      for (const sql of [
        "CREATE TABLE IF NOT EXISTS fs_config(key TEXT PRIMARY KEY, value TEXT NOT NULL)",
        `CREATE TABLE IF NOT EXISTS fs_inode(ino INTEGER PRIMARY KEY AUTOINCREMENT, mode INTEGER NOT NULL,
          nlink INTEGER NOT NULL DEFAULT 0, uid INTEGER NOT NULL DEFAULT 0, gid INTEGER NOT NULL DEFAULT 0,
          size INTEGER NOT NULL DEFAULT 0, atime INTEGER NOT NULL, mtime INTEGER NOT NULL, ctime INTEGER NOT NULL,
          rdev INTEGER NOT NULL DEFAULT 0, atime_nsec INTEGER NOT NULL DEFAULT 0,
          mtime_nsec INTEGER NOT NULL DEFAULT 0, ctime_nsec INTEGER NOT NULL DEFAULT 0)`,
        `CREATE TABLE IF NOT EXISTS fs_dentry(id INTEGER PRIMARY KEY AUTOINCREMENT,
          name TEXT NOT NULL, parent_ino INTEGER NOT NULL, ino INTEGER NOT NULL, UNIQUE(parent_ino,name))`,
        "CREATE INDEX IF NOT EXISTS idx_fs_dentry_parent ON fs_dentry(parent_ino,name)",
        "CREATE TABLE IF NOT EXISTS fs_data(ino INTEGER NOT NULL,chunk_index INTEGER NOT NULL,data BLOB NOT NULL,PRIMARY KEY(ino,chunk_index))",
        "CREATE TABLE IF NOT EXISTS fs_symlink(ino INTEGER PRIMARY KEY,target TEXT NOT NULL)",
      ])
        this.rows(sql);
      if (!exists) {
        this.rows(
          "INSERT INTO fs_config VALUES ('schema_version','0.4'),('chunk_size','4096')",
        );
        const now = this.now();
        this.rows(
          "INSERT INTO fs_inode(ino,mode,nlink,atime,mtime,ctime) VALUES(1,?,1,?,?,?)",
          DIR,
          now,
          now,
          now,
        );
      }
      this.directory(this.inode(1));
      try {
        this.directory(this.resolve("/workspace"));
      } catch (e) {
        if ((e as SandboxError).code !== "ENOENT") throw e;
        this.create("/workspace", DIR);
      }
    });
  }
  private rows(sql: string, ...args: any[]): any[] {
    return this.storage.sql.exec(sql, ...args).toArray();
  }
  private now() {
    return Math.floor(Date.now() / 1000);
  }
  private mutate<T>(f: () => T): T {
    this.beforeMutation();
    return this.storage.transactionSync(f);
  }
  path(value: unknown): string {
    check(
      typeof value === "string" &&
        value.length <= 4096 &&
        !value.includes("\0"),
      "EINVAL",
    );
    const parts = value.split("/").filter((p) => p && p !== ".");
    check(
      value.startsWith("/") &&
        parts[0] === "workspace" &&
        !parts.includes(".."),
      "EACCES",
    );
    check(
      parts.length <= 64 && parts.every((p) => Buffer.byteLength(p) <= 255),
      "ENAMETOOLONG",
    );
    return "/" + parts.join("/");
  }
  private inode(ino: number): any {
    const row = this.rows("SELECT * FROM fs_inode WHERE ino=?", ino)[0];
    check(row, "ENOENT");
    check(
      (row.mode & 0o170000) !== 0o120000,
      "ELOOP",
      "symlinks are not supported",
    );
    check([0o100000, 0o40000].includes(row.mode & 0o170000), "ENOTSUP");
    return row;
  }
  private directory(row: any) {
    check((row.mode & 0o170000) === 0o40000, "ENOTDIR");
  }
  private file(row: any) {
    check((row.mode & 0o170000) === 0o100000, "EISDIR");
  }
  private resolve(path: string): any {
    let row = this.inode(1);
    for (const name of this.path(path).slice(1).split("/")) {
      this.directory(row);
      const entry = this.rows(
        "SELECT ino FROM fs_dentry WHERE parent_ino=? AND name=?",
        row.ino,
        name,
      )[0];
      check(entry, "ENOENT");
      row = this.inode(entry.ino);
    }
    return row;
  }
  private parent(path: string): { parent: number; name: string } {
    const p = this.path(path),
      pos = p.lastIndexOf("/");
    const row = pos === 0 ? this.inode(1) : this.resolve(p.slice(0, pos));
    this.directory(row);
    return { parent: row.ino, name: p.slice(pos + 1) };
  }
  private touch(ino: number) {
    const now = this.now();
    this.rows("UPDATE fs_inode SET mtime=?,ctime=? WHERE ino=?", now, now, ino);
  }
  private create(path: string, mode: number): any {
    const { parent, name } = this.parent(path);
    check(
      !this.rows(
        "SELECT ino FROM fs_dentry WHERE parent_ino=? AND name=?",
        parent,
        name,
      ).length,
      "EEXIST",
    );
    check(
      this.rows("SELECT COUNT(*) AS n FROM fs_inode")[0].n <
        this.limits.maxInodes,
      "ENOSPC",
    );
    const now = this.now();
    const row = this.rows(
      "INSERT INTO fs_inode(mode,nlink,atime,mtime,ctime) VALUES(?,1,?,?,?) RETURNING *",
      mode,
      now,
      now,
      now,
    )[0];
    this.rows(
      "INSERT INTO fs_dentry(name,parent_ino,ino) VALUES(?,?,?)",
      name,
      parent,
      row.ino,
    );
    this.touch(parent);
    return row;
  }
  private stats(row: any): Stat {
    return {
      ino: row.ino,
      size: row.size,
      dir: (row.mode & 0o170000) === 0o40000,
      mode: row.mode,
      atime: row.atime,
      mtime: row.mtime,
      ctime: row.ctime,
    };
  }
  stat(path: string): Stat {
    return this.stats(this.resolve(path));
  }
  list(path: string): (Stat & { name: string })[] {
    const dir = this.resolve(path);
    this.directory(dir);
    return this.rows(
      "SELECT name,ino FROM fs_dentry WHERE parent_ino=? ORDER BY name",
      dir.ino,
    ).map((e) => ({ name: e.name, ...this.stats(this.inode(e.ino)) }));
  }
  mkdir(path: string) {
    this.mutate(() => this.create(path, DIR));
  }
  private handle(id: number): Handle {
    const h = this.handles.get(integer(id));
    check(h, "EBADF");
    return h;
  }
  private noHandles(ino: number) {
    check(
      ![...this.handles.values()].some((h) => h.ino === ino),
      "EBUSY",
      "close the inode before removing/replacing it",
    );
  }
  open(path: string, flags: OpenFlags): number {
    check(Object.values(flags).every((v) => typeof v === "boolean"));
    const writable = !!(flags.write || flags.append);
    check(flags.read || writable);
    check(!flags.truncate || writable);
    check(!(flags.create || flags.create_new) || writable);
    check(this.handles.size < this.limits.maxHandles, "EMFILE");
    const op = () => {
      let row;
      try {
        row = this.resolve(path);
      } catch (e) {
        if ((e as SandboxError).code !== "ENOENT") throw e;
      }
      if (row) check(!flags.create_new, "EEXIST");
      else {
        check(flags.create || flags.create_new, "ENOENT");
        row = this.create(path, FILE);
      }
      this.file(row);
      if (flags.truncate) this.truncateInode(row, 0);
      return row.ino;
    };
    const ino =
      writable || flags.create || flags.create_new || flags.truncate
        ? this.mutate(op)
        : op();
    const id = this.nextHandle++;
    this.handles.set(id, {
      ino,
      read: !!flags.read,
      write: writable,
      append: !!flags.append,
    });
    return id;
  }
  close(id: number) {
    this.handle(id);
    this.handles.delete(id);
  }
  closeAll() {
    this.handles.clear();
  }
  fstat(id: number): Stat {
    return this.stats(this.inode(this.handle(id).ino));
  }
  private readInode(row: any, offset: number, size: number): Buffer {
    integer(offset);
    integer(size, MAX_IO);
    const length = Math.max(0, Math.min(size, row.size - offset));
    const result = Buffer.alloc(length);
    if (!length) return result;
    const start = Math.floor(offset / this.chunkSize),
      end = Math.floor((offset + length - 1) / this.chunkSize);
    for (const chunk of this.rows(
      "SELECT chunk_index,data FROM fs_data WHERE ino=? AND chunk_index BETWEEN ? AND ? ORDER BY chunk_index",
      row.ino,
      start,
      end,
    )) {
      const data = Buffer.from(chunk.data),
        chunkStart = chunk.chunk_index * this.chunkSize;
      const from = Math.max(0, offset - chunkStart),
        to = Math.min(data.length, offset + length - chunkStart);
      if (to > from)
        data.copy(result, Math.max(0, chunkStart - offset), from, to);
    }
    return result;
  }
  read(id: number, offset: number, size: number): Buffer {
    const h = this.handle(id);
    check(h.read, "EBADF");
    return this.readInode(this.inode(h.ino), offset, size);
  }
  private quota(row: any, size: number) {
    integer(size, this.limits.maxFileBytes);
    const total = this.rows(
      "SELECT COALESCE(SUM(size),0) AS n FROM fs_inode",
    )[0].n;
    check(total - row.size + size <= this.limits.maxBytes, "ENOSPC");
  }
  private writeInode(row: any, offset: number, data: Uint8Array) {
    integer(offset, this.limits.maxFileBytes);
    integer(data.length, MAX_IO);
    if (!data.length) return;
    const size = Math.max(row.size, offset + data.length);
    this.quota(row, size);
    for (let i = 0; i < data.length; ) {
      const index = Math.floor((offset + i) / this.chunkSize),
        at = (offset + i) % this.chunkSize;
      const n = Math.min(data.length - i, this.chunkSize - at);
      const old = this.rows(
        "SELECT data FROM fs_data WHERE ino=? AND chunk_index=?",
        row.ino,
        index,
      )[0];
      const chunk = Buffer.alloc(Math.max(old?.data.byteLength ?? 0, at + n));
      if (old) Buffer.from(old.data).copy(chunk);
      chunk.set(data.subarray(i, i + n), at);
      this.rows(
        "INSERT INTO fs_data VALUES(?,?,?) ON CONFLICT(ino,chunk_index) DO UPDATE SET data=excluded.data",
        row.ino,
        index,
        chunk,
      );
      i += n;
    }
    this.rows("UPDATE fs_inode SET size=? WHERE ino=?", size, row.ino);
    this.touch(row.ino);
  }
  write(
    id: number,
    offset: number,
    data: Uint8Array,
  ): { written: number; offset: number } {
    const h = this.handle(id);
    check(h.write, "EBADF");
    check(data instanceof Uint8Array);
    return this.mutate(() => {
      const row = this.inode(h.ino);
      const at = h.append ? row.size : offset;
      this.writeInode(row, at, data);
      return { written: data.length, offset: at + data.length };
    });
  }
  private truncateInode(row: any, size: number) {
    this.quota(row, size);
    if (size < row.size) {
      const index = Math.floor(size / this.chunkSize),
        keep = size % this.chunkSize;
      this.rows(
        "DELETE FROM fs_data WHERE ino=? AND chunk_index>=?",
        row.ino,
        index + (keep ? 1 : 0),
      );
      if (keep) {
        const chunk = this.rows(
          "SELECT data FROM fs_data WHERE ino=? AND chunk_index=?",
          row.ino,
          index,
        )[0];
        if (chunk)
          this.rows(
            "UPDATE fs_data SET data=? WHERE ino=? AND chunk_index=?",
            Buffer.from(chunk.data).subarray(0, keep),
            row.ino,
            index,
          );
      }
    }
    this.rows("UPDATE fs_inode SET size=? WHERE ino=?", size, row.ino);
    this.touch(row.ino);
  }
  truncate(id: number, size: number) {
    const h = this.handle(id);
    check(h.write, "EBADF");
    this.mutate(() => this.truncateInode(this.inode(h.ino), size));
  }
  readFile(path: string): Buffer {
    const row = this.resolve(path);
    this.file(row);
    check(
      row.size <= 1024 * 1024,
      "EFBIG",
      "readFile is limited to 1 MiB; use a handle for larger files",
    );
    const chunks = [];
    for (let at = 0; at < row.size; at += MAX_IO)
      chunks.push(this.readInode(row, at, MAX_IO));
    return Buffer.concat(chunks);
  }
  writeFile(path: string, data: Uint8Array) {
    check(data instanceof Uint8Array && data.length <= 1024 * 1024, "EFBIG");
    this.mutate(() => {
      let row;
      try {
        row = this.resolve(path);
      } catch (e) {
        if ((e as SandboxError).code !== "ENOENT") throw e;
        row = this.create(path, FILE);
      }
      this.file(row);
      this.quota(row, data.length);
      this.truncateInode(row, 0);
      for (let at = 0; at < data.length; at += MAX_IO)
        this.writeInode(
          this.inode(row.ino),
          at,
          data.subarray(at, at + MAX_IO),
        );
    });
  }
  private remove(path: string, dir: boolean) {
    check(this.path(path) !== "/workspace", "EBUSY");
    const row = this.resolve(path),
      { parent, name } = this.parent(path);
    if (dir) {
      this.directory(row);
      check(
        !this.rows(
          "SELECT 1 FROM fs_dentry WHERE parent_ino=? LIMIT 1",
          row.ino,
        ).length,
        "ENOTEMPTY",
      );
    } else this.file(row);
    this.noHandles(row.ino);
    this.rows(
      "DELETE FROM fs_dentry WHERE parent_ino=? AND name=?",
      parent,
      name,
    );
    this.rows("UPDATE fs_inode SET nlink=nlink-1 WHERE ino=?", row.ino);
    if (row.nlink <= 1) {
      this.rows("DELETE FROM fs_data WHERE ino=?", row.ino);
      this.rows("DELETE FROM fs_inode WHERE ino=?", row.ino);
    }
    this.touch(parent);
  }
  unlink(path: string) {
    this.mutate(() => this.remove(path, false));
  }
  rmdir(path: string) {
    this.mutate(() => this.remove(path, true));
  }
  rename(from: string, to: string) {
    this.mutate(() => {
      from = this.path(from);
      to = this.path(to);
      check(from !== "/workspace" && to !== "/workspace", "EBUSY");
      const src = this.resolve(from);
      if (from === to) return;
      const dir = this.stats(src).dir;
      check(!dir || !to.startsWith(from + "/"), "EINVAL");
      let dst;
      try {
        dst = this.resolve(to);
      } catch (e) {
        if ((e as SandboxError).code !== "ENOENT") throw e;
      }
      if (dst?.ino === src.ino) return;
      if (dst) {
        check(dir === this.stats(dst).dir, dir ? "ENOTDIR" : "EISDIR");
        this.remove(to, dir);
      }
      const p = this.parent(from),
        q = this.parent(to);
      this.rows(
        "UPDATE fs_dentry SET parent_ino=?,name=? WHERE parent_ino=? AND name=?",
        q.parent,
        q.name,
        p.parent,
        p.name,
      );
      this.touch(src.ino);
      this.touch(p.parent);
      this.touch(q.parent);
    });
  }
}
