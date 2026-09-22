// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//! AgentFS 0.4 operations on the owning cell's managed connection. The caller
//! supplies a serialized cell turn and a transaction for mutations.
use celld_agentfs_ipc::{Error, Reply, Request, Stat};
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::{json, Value};
use std::collections::BTreeMap;
pub(crate) const GUEST_OPERATIONS: u32 = (1 << 14) - 1;
pub(crate) struct Capability {
    pub token: String,
    pub command: String,
    pub epoch: u64,
    pub deadline: u64,
    pub next: u64,
    pub session: Option<u64>,
    pub allowed_operations: u32,
}
impl Capability {
    pub(crate) fn validate(
        &self,
        r: &Request,
        session: u64,
        epoch: u64,
        now: u64,
    ) -> Result<(), Error> {
        if now >= self.deadline
            || r.token != self.token
            || r.command != self.command
            || epoch != self.epoch
            || self.session.is_some_and(|bound| bound != session)
            || r.sequence != self.next
            || self.next > celld_agentfs_ipc::MAX_REQUESTS
            || !self.allows(&r.operation)
        {
            Err(Error::Stale)
        } else {
            Ok(())
        }
    }
    /// The guest grant has a closed operation set. Application-only operations
    /// never become reachable by acquiring a valid IPC capability.
    fn allows(&self, operation: &Value) -> bool {
        let bit = match operation["op"].as_str() {
            Some("stat") => 0,
            Some("list") => 1,
            Some("mkdir") => 2,
            Some("open") => 3,
            Some("close") => 4,
            Some("fstat") => 5,
            Some("read") => 6,
            Some("write") => 7,
            Some("truncate") => 8,
            Some("unlink") => 9,
            Some("rmdir") => 10,
            Some("rename") => 11,
            Some("sync") => 12,
            Some("heartbeat") => 13,
            _ => return false,
        };
        self.allowed_operations & (1 << bit) != 0
    }
    pub(crate) fn bind(
        &mut self,
        r: &Request,
        session: u64,
        epoch: u64,
        now: u64,
    ) -> Result<(), Error> {
        self.validate(r, session, epoch, now)?;
        if self.session.is_none() {
            self.session = Some(session);
        }
        Ok(())
    }
    pub(crate) fn admit(
        &mut self,
        r: &Request,
        session: u64,
        epoch: u64,
        now: u64,
    ) -> Result<(), Error> {
        self.validate(r, session, epoch, now)?;
        self.next += 1;
        Ok(())
    }
}
pub struct Answer {
    pub result: Result<Reply, Error>,
    pub(crate) observed: Option<u64>,
    pub(crate) written: Option<u64>,
}
impl crate::js::GatedAnswer for Answer {
    fn write_position(&self) -> Option<u64> {
        self.written
    }
    fn observed_position(&self) -> Option<u64> {
        self.observed
    }
}
#[derive(Debug)]
pub(crate) enum Failure {
    Fs(Error),
    Sql(rusqlite::Error),
}
impl From<Error> for Failure {
    fn from(e: Error) -> Self {
        Self::Fs(e)
    }
}
impl From<rusqlite::Error> for Failure {
    fn from(e: rusqlite::Error) -> Self {
        Self::Sql(e)
    }
}
impl Failure {
    pub(crate) fn code(&self) -> Error {
        match self {
            Self::Fs(e) => *e,
            Self::Sql(_) => Error::Io,
        }
    }
}
type FsResult<T> = Result<T, Failure>;
fn check(ok: bool, e: Error) -> FsResult<()> {
    if ok {
        Ok(())
    } else {
        Err(e.into())
    }
}
fn number(v: &Value, key: &str) -> FsResult<u64> {
    v[key]
        .as_u64()
        .filter(|n| *n <= 9_007_199_254_740_991)
        .ok_or(Error::Invalid.into())
}
fn string<'a>(v: &'a Value, key: &str) -> FsResult<&'a str> {
    v[key].as_str().ok_or(Error::Invalid.into())
}
fn flag(v: &Value, key: &str) -> FsResult<bool> {
    match v.get(key) {
        None => Ok(false),
        Some(Value::Bool(b)) => Ok(*b),
        _ => Err(Error::Invalid.into()),
    }
}
fn path(p: &str) -> FsResult<String> {
    check(p.len() <= 4096 && !p.contains('\0'), Error::Invalid)?;
    let parts: Vec<_> = p
        .split('/')
        .filter(|s| !s.is_empty() && *s != ".")
        .collect();
    check(
        p.starts_with('/') && parts.first() == Some(&"workspace") && !parts.contains(&".."),
        Error::Access,
    )?;
    check(
        parts.len() <= 64 && parts.iter().all(|s| s.len() <= 255),
        Error::Name,
    )?;
    Ok(format!("/{}", parts.join("/")))
}
fn inode(db: &Connection, ino: u64) -> FsResult<Stat> {
    let row = db
        .prepare_cached("SELECT ino,size,mode,atime,mtime,ctime FROM fs_inode WHERE ino=?")?
        .query_row([ino], |r| {
            Ok(Stat {
                ino: r.get(0)?,
                size: r.get(1)?,
                mode: r.get(2)?,
                atime: r.get(3)?,
                mtime: r.get(4)?,
                ctime: r.get(5)?,
            })
        })
        .optional()?
        .ok_or(Error::Missing)?;
    check(row.mode & 0o170000 != 0o120000, Error::Loop)?;
    check(
        matches!(row.mode & 0o170000, 0o100000 | 0o40000),
        Error::Unsupported,
    )?;
    Ok(row)
}
fn directory(row: &Stat) -> FsResult<()> {
    check(row.mode & 0o170000 == 0o40000, Error::NotDirectory)
}
fn file(row: &Stat) -> FsResult<()> {
    check(row.mode & 0o170000 == 0o100000, Error::IsDirectory)
}
fn resolve(db: &Connection, p: &str) -> FsResult<Stat> {
    let p = path(p)?;
    let mut row = inode(db, 1)?;
    for name in p[1..].split('/') {
        directory(&row)?;
        let ino = db
            .prepare_cached("SELECT ino FROM fs_dentry WHERE parent_ino=? AND name=?")?
            .query_row(params![row.ino, name], |r| r.get(0))
            .optional()?
            .ok_or(Error::Missing)?;
        row = inode(db, ino)?;
    }
    Ok(row)
}
fn parent(db: &Connection, p: &str) -> FsResult<(u64, String)> {
    let p = path(p)?;
    let (prefix, name) = p.rsplit_once('/').unwrap();
    let row = if prefix.is_empty() {
        inode(db, 1)?
    } else {
        resolve(db, prefix)?
    };
    directory(&row)?;
    Ok((row.ino, name.into()))
}
fn touch(db: &Connection, ino: u64) -> FsResult<()> {
    let now = crate::ownership_store::now_ms() / 1000;
    db.execute(
        "UPDATE fs_inode SET mtime=?,ctime=? WHERE ino=?",
        params![now, now, ino],
    )?;
    Ok(())
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Limits {
    bytes: u64,
    file: u64,
    inodes: u64,
    handles: u64,
}
impl Limits {
    fn parse(v: &Value) -> FsResult<Self> {
        let l = Self {
            bytes: number(v, "maxBytes")?,
            file: number(v, "maxFileBytes")?,
            inodes: number(v, "maxInodes")?,
            handles: number(v, "maxHandles")?,
        };
        check(
            l.bytes > 0
                && l.file > 0
                && l.inodes > 0
                && l.inodes <= 4096
                && l.handles > 0
                && l.handles <= 4096,
            Error::Invalid,
        )?;
        Ok(l)
    }
}
#[derive(Clone)]
struct Handle {
    ino: u64,
    read: bool,
    write: bool,
    append: bool,
    guest: bool,
}
#[derive(Clone, Default)]
pub(crate) struct Workspace {
    limits: Option<Limits>,
    chunk: u64,
    handles: BTreeMap<u64, Handle>,
    next: u64,
}
impl Workspace {
    pub(crate) fn revoke(&mut self) {
        self.handles.retain(|_, h| !h.guest);
    }
    fn limits(&self) -> FsResult<&Limits> {
        self.limits.as_ref().ok_or(Error::Unsupported.into())
    }
    fn handle(&self, v: &Value, guest: bool) -> FsResult<&Handle> {
        self.handles
            .get(&number(v, "handle")?)
            .filter(|h| h.guest == guest)
            .ok_or(Error::BadHandle.into())
    }
    fn create(&self, db: &Connection, p: &str, mode: u64) -> FsResult<Stat> {
        let (parent, name) = parent(db, p)?;
        let exists: bool = db.query_row(
            "SELECT EXISTS(SELECT 1 FROM fs_dentry WHERE parent_ino=? AND name=?)",
            params![parent, name],
            |r| r.get(0),
        )?;
        check(!exists, Error::Exists)?;
        let count: u64 = db.query_row("SELECT count(*) FROM fs_inode", [], |r| r.get(0))?;
        check(count < self.limits()?.inodes, Error::Space)?;
        let now = crate::ownership_store::now_ms() / 1000;
        let ino: u64 = db.query_row(
            "INSERT INTO fs_inode(mode,nlink,atime,mtime,ctime) VALUES(?,1,?,?,?) RETURNING ino",
            params![mode, now, now, now],
            |r| r.get(0),
        )?;
        db.execute(
            "INSERT INTO fs_dentry(name,parent_ino,ino) VALUES(?,?,?)",
            params![name, parent, ino],
        )?;
        touch(db, parent)?;
        inode(db, ino)
    }
    fn quota(&self, db: &Connection, row: &Stat, size: u64) -> FsResult<()> {
        let limits = self.limits()?;
        check(size <= limits.file, Error::Invalid)?;
        let total: u64 = db.query_row("SELECT coalesce(sum(size),0) FROM fs_inode", [], |r| {
            r.get(0)
        })?;
        check(
            total
                .checked_sub(row.size)
                .and_then(|n| n.checked_add(size))
                .is_some_and(|n| n <= limits.bytes),
            Error::Space,
        )
    }
    fn read(&self, db: &Connection, row: &Stat, offset: u64, size: u64) -> FsResult<Vec<u8>> {
        check(size <= 65536, Error::Invalid)?;
        let len = size.min(row.size.saturating_sub(offset));
        let mut result = vec![0; len as usize];
        if len == 0 {
            return Ok(result);
        }
        let mut stmt=db.prepare_cached("SELECT chunk_index,data FROM fs_data WHERE ino=? AND chunk_index BETWEEN ? AND ? ORDER BY chunk_index")?;
        let mut rows = stmt.query(params![
            row.ino,
            offset / self.chunk,
            (offset + len - 1) / self.chunk
        ])?;
        while let Some(r) = rows.next()? {
            let index: u64 = r.get(0)?;
            let data: Vec<u8> = r.get(1)?;
            check(data.len() <= self.chunk as usize, Error::Io)?;
            let start = index * self.chunk;
            let from = offset.saturating_sub(start) as usize;
            let to = data.len().min((offset + len - start) as usize);
            if to > from {
                let dest = start.saturating_sub(offset) as usize;
                result[dest..dest + to - from].copy_from_slice(&data[from..to]);
            }
        }
        Ok(result)
    }
    fn write(&self, db: &Connection, row: &Stat, offset: u64, data: &[u8]) -> FsResult<()> {
        check(
            offset <= self.limits()?.file && data.len() <= 65536,
            Error::Invalid,
        )?;
        if data.is_empty() {
            return Ok(());
        }
        let size = row.size.max(
            offset
                .checked_add(data.len() as u64)
                .ok_or(Error::Invalid)?,
        );
        self.quota(db, row, size)?;
        let mut i = 0;
        while i < data.len() {
            let index = (offset + i as u64) / self.chunk;
            let at = ((offset + i as u64) % self.chunk) as usize;
            let n = (data.len() - i).min(self.chunk as usize - at);
            let mut chunk: Vec<u8> = db
                .prepare_cached("SELECT data FROM fs_data WHERE ino=? AND chunk_index=?")?
                .query_row(params![row.ino, index], |r| r.get(0))
                .optional()?
                .unwrap_or_default();
            check(chunk.len() <= self.chunk as usize, Error::Io)?;
            chunk.resize(chunk.len().max(at + n), 0);
            chunk[at..at + n].copy_from_slice(&data[i..i + n]);
            db.execute("INSERT INTO fs_data VALUES(?,?,?) ON CONFLICT(ino,chunk_index) DO UPDATE SET data=excluded.data",params![row.ino,index,chunk])?;
            i += n;
        }
        db.execute(
            "UPDATE fs_inode SET size=? WHERE ino=?",
            params![size, row.ino],
        )?;
        touch(db, row.ino)
    }
    fn truncate(&self, db: &Connection, row: &Stat, size: u64) -> FsResult<()> {
        self.quota(db, row, size)?;
        if size < row.size {
            let index = size / self.chunk;
            let keep = size % self.chunk;
            db.execute(
                "DELETE FROM fs_data WHERE ino=? AND chunk_index>=?",
                params![row.ino, index + u64::from(keep > 0)],
            )?;
            if keep > 0 {
                db.execute(
                    "UPDATE fs_data SET data=substr(data,1,?) WHERE ino=? AND chunk_index=?",
                    params![keep, row.ino, index],
                )?;
            }
        }
        db.execute(
            "UPDATE fs_inode SET size=? WHERE ino=?",
            params![size, row.ino],
        )?;
        touch(db, row.ino)
    }
    fn remove(&self, db: &Connection, p: &str, dir: bool) -> FsResult<()> {
        check(path(p)? != "/workspace", Error::Busy)?;
        let row = resolve(db, p)?;
        let (parent, name) = parent(db, p)?;
        if dir {
            directory(&row)?;
            let exists: bool = db.query_row(
                "SELECT EXISTS(SELECT 1 FROM fs_dentry WHERE parent_ino=?)",
                [row.ino],
                |r| r.get(0),
            )?;
            check(!exists, Error::NotEmpty)?;
        } else {
            file(&row)?;
        }
        check(
            !self.handles.values().any(|h| h.ino == row.ino),
            Error::Busy,
        )?;
        db.execute(
            "DELETE FROM fs_dentry WHERE parent_ino=? AND name=?",
            params![parent, name],
        )?;
        db.execute("UPDATE fs_inode SET nlink=nlink-1 WHERE ino=?", [row.ino])?;
        let links: i64 =
            db.query_row("SELECT nlink FROM fs_inode WHERE ino=?", [row.ino], |r| {
                r.get(0)
            })?;
        if links <= 0 {
            db.execute("DELETE FROM fs_data WHERE ino=?", [row.ino])?;
            db.execute("DELETE FROM fs_inode WHERE ino=?", [row.ino])?;
        }
        touch(db, parent)
    }
    pub(crate) fn execute(
        &mut self,
        db: &Connection,
        v: &Value,
        data: &[u8],
        guest: bool,
    ) -> FsResult<Reply> {
        let op = string(v, "op")?;
        check(
            data.is_empty() || matches!(op, "write" | "writeFile"),
            Error::Invalid,
        )?;
        if op == "configure" {
            check(!guest, Error::Access)?;
            let limits = Limits::parse(&v["limits"])?;
            if let Some(old) = &self.limits {
                check(old == &limits || self.handles.is_empty(), Error::Busy)?;
            }
            let version: String = db.query_row(
                "SELECT value FROM fs_config WHERE key='schema_version'",
                [],
                |r| r.get(0),
            )?;
            check(version == "0.4", Error::Unsupported)?;
            let chunk: String = db.query_row(
                "SELECT value FROM fs_config WHERE key='chunk_size'",
                [],
                |r| r.get(0),
            )?;
            self.chunk = chunk.parse().map_err(|_| Error::Unsupported)?;
            check((512..=65536).contains(&self.chunk), Error::Unsupported)?;
            self.limits = Some(limits);
            self.next = self.next.max(1);
            return Ok(Reply::new(Value::Null));
        }
        self.limits()?;
        let value = match op {
            "stat" => resolve(db, string(v, "path")?)?.value(),
            "list" => {
                let row = resolve(db, string(v, "path")?)?;
                directory(&row)?;
                let mut stmt = db.prepare_cached(
                    "SELECT name,ino FROM fs_dentry WHERE parent_ino=? ORDER BY name LIMIT 4097",
                )?;
                let entries = stmt
                    .query_map([row.ino], |r| {
                        Ok((r.get::<_, String>(0)?, r.get::<_, u64>(1)?))
                    })?
                    .collect::<Result<Vec<_>, _>>()?;
                check(entries.len() <= 4096, Error::Big)?;
                let mut out = vec![];
                for (name, ino) in entries {
                    check(name.len() <= 255, Error::Io)?;
                    let mut s = inode(db, ino)?.value();
                    s["name"] = json!(name);
                    out.push(s);
                }
                json!(out)
            }
            "mkdir" => {
                self.create(db, string(v, "path")?, 0o40755)?;
                Value::Null
            }
            "open" => {
                let read = flag(v, "read")?;
                let write = flag(v, "write")? || flag(v, "append")?;
                let append = flag(v, "append")?;
                let create = flag(v, "create")?;
                let new = flag(v, "create_new")?;
                let truncate = flag(v, "truncate")?;
                check(
                    (read || write) && (!(truncate || create || new) || write),
                    Error::Invalid,
                )?;
                check(
                    (self.handles.len() as u64) < self.limits()?.handles,
                    Error::Handles,
                )?;
                let row = match resolve(db, string(v, "path")?) {
                    Ok(r) => {
                        check(!new, Error::Exists)?;
                        r
                    }
                    Err(Failure::Fs(Error::Missing)) if create || new => {
                        self.create(db, string(v, "path")?, 0o100644)?
                    }
                    Err(e) => return Err(e),
                };
                file(&row)?;
                if truncate {
                    self.truncate(db, &row, 0)?;
                }
                let id = self.next;
                self.next = self
                    .next
                    .checked_add(1)
                    .filter(|n| *n <= 9_007_199_254_740_991)
                    .ok_or(Error::Handles)?;
                self.handles.insert(
                    id,
                    Handle {
                        ino: row.ino,
                        read,
                        write,
                        append,
                        guest,
                    },
                );
                json!({"handle":id})
            }
            "close" => {
                self.handle(v, guest)?;
                self.handles.remove(&number(v, "handle")?);
                Value::Null
            }
            "closeAll" => {
                check(!guest, Error::Access)?;
                self.handles.retain(|_, h| h.guest);
                Value::Null
            }
            "fstat" => inode(db, self.handle(v, guest)?.ino)?.value(),
            "read" => {
                let h = self.handle(v, guest)?;
                check(h.read, Error::BadHandle)?;
                return Ok(Reply {
                    value: Value::Null,
                    data: self.read(
                        db,
                        &inode(db, h.ino)?,
                        number(v, "offset")?,
                        number(v, "size")?,
                    )?,
                });
            }
            "write" => {
                let h = self.handle(v, guest)?;
                check(h.write, Error::BadHandle)?;
                let row = inode(db, h.ino)?;
                let at = if h.append {
                    row.size
                } else {
                    number(v, "offset")?
                };
                self.write(db, &row, at, data)?;
                json!({"written":data.len(),"offset":at+data.len() as u64})
            }
            "truncate" => {
                let h = self.handle(v, guest)?;
                check(h.write, Error::BadHandle)?;
                self.truncate(db, &inode(db, h.ino)?, number(v, "size")?)?;
                Value::Null
            }
            "readFile" => {
                check(!guest, Error::Access)?;
                let row = resolve(db, string(v, "path")?)?;
                file(&row)?;
                check(row.size <= 1024 * 1024, Error::Big)?;
                let mut data = vec![];
                for at in (0..row.size).step_by(65536) {
                    data.extend(self.read(db, &row, at, 65536)?);
                }
                return Ok(Reply {
                    value: Value::Null,
                    data,
                });
            }
            "writeFile" => {
                check(!guest && data.len() <= 1024 * 1024, Error::Big)?;
                let row = match resolve(db, string(v, "path")?) {
                    Ok(r) => r,
                    Err(Failure::Fs(Error::Missing)) => {
                        self.create(db, string(v, "path")?, 0o100644)?
                    }
                    Err(e) => return Err(e),
                };
                file(&row)?;
                self.quota(db, &row, data.len() as u64)?;
                self.truncate(db, &row, 0)?;
                for (i, chunk) in data.chunks(65536).enumerate() {
                    self.write(db, &inode(db, row.ino)?, (i * 65536) as u64, chunk)?;
                }
                Value::Null
            }
            "unlink" | "rmdir" => {
                self.remove(db, string(v, "path")?, op == "rmdir")?;
                Value::Null
            }
            "rename" => {
                let from = path(string(v, "path")?)?;
                let to = path(string(v, "to")?)?;
                check(from != "/workspace" && to != "/workspace", Error::Busy)?;
                let src = resolve(db, &from)?;
                let dir = src.mode & 0o170000 == 0o40000;
                if from == to {
                    return Ok(Reply::new(Value::Null));
                }
                check(!dir || !to.starts_with(&format!("{from}/")), Error::Invalid)?;
                match resolve(db, &to) {
                    Ok(dst) => {
                        if dst.ino == src.ino {
                            return Ok(Reply::new(Value::Null));
                        }
                        check(
                            dir == (dst.mode & 0o170000 == 0o40000),
                            if dir {
                                Error::NotDirectory
                            } else {
                                Error::IsDirectory
                            },
                        )?;
                        self.remove(db, &to, dir)?;
                    }
                    Err(Failure::Fs(Error::Missing)) => {}
                    Err(e) => return Err(e),
                }
                let (p, name) = parent(db, &from)?;
                let (q, new) = parent(db, &to)?;
                db.execute(
                    "UPDATE fs_dentry SET parent_ino=?,name=? WHERE parent_ino=? AND name=?",
                    params![q, new, p, name],
                )?;
                touch(db, src.ino)?;
                touch(db, p)?;
                touch(db, q)?;
                Value::Null
            }
            "sync" | "heartbeat" => Value::Null,
            _ => return Err(Error::Unsupported.into()),
        };
        Ok(Reply::new(value))
    }
}
pub(crate) fn mutates(v: &Value) -> bool {
    matches!(
        v["op"].as_str(),
        Some("mkdir" | "open" | "write" | "truncate" | "writeFile" | "unlink" | "rmdir" | "rename")
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    fn setup() -> (Connection, Workspace) {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch("CREATE TABLE fs_config(key TEXT PRIMARY KEY,value TEXT NOT NULL); INSERT INTO fs_config VALUES('schema_version','0.4'),('chunk_size','512'); CREATE TABLE fs_inode(ino INTEGER PRIMARY KEY AUTOINCREMENT,mode INTEGER,nlink INTEGER DEFAULT 1,size INTEGER DEFAULT 0,atime INTEGER,mtime INTEGER,ctime INTEGER); CREATE TABLE fs_dentry(id INTEGER PRIMARY KEY,name TEXT,parent_ino INTEGER,ino INTEGER,UNIQUE(parent_ino,name)); CREATE TABLE fs_data(ino INTEGER,chunk_index INTEGER,data BLOB,PRIMARY KEY(ino,chunk_index)); INSERT INTO fs_inode VALUES(1,16877,1,0,1,1,1),(2,16877,1,0,1,1,1); INSERT INTO fs_dentry(name,parent_ino,ino) VALUES('workspace',1,2);").unwrap();
        let mut fs = Workspace::default();
        fs.execute(&db,&json!({"op":"configure","limits":{"maxBytes":4096,"maxFileBytes":4096,"maxInodes":16,"maxHandles":4}}),&[],false).unwrap();
        (db, fs)
    }
    fn run(
        db: &Connection,
        fs: &mut Workspace,
        v: Value,
        data: &[u8],
        guest: bool,
    ) -> Result<Reply, Error> {
        let mut next = fs.clone();
        db.execute_batch("BEGIN IMMEDIATE").unwrap();
        let result = next.execute(db, &v, data, guest);
        db.execute_batch(if result.is_ok() { "COMMIT" } else { "ROLLBACK" })
            .unwrap();
        if result.is_ok() {
            *fs = next;
        }
        result.map_err(|e| e.code())
    }
    fn open(db: &Connection, fs: &mut Workspace, p: &str, guest: bool) -> u64 {
        run(
            db,
            fs,
            json!({"op":"open","path":p,"read":true,"write":true,"create":true}),
            &[],
            guest,
        )
        .unwrap()
        .value["handle"]
            .as_u64()
            .unwrap()
    }
    #[test]
    fn sparse_truncate_append_and_raw_bytes() {
        let (db, mut fs) = setup();
        let h = open(&db, &mut fs, "/workspace/file", true);
        run(
            &db,
            &mut fs,
            json!({"op":"write","handle":h,"offset":1023}),
            &[0, 255, 2],
            true,
        )
        .unwrap();
        let data = run(
            &db,
            &mut fs,
            json!({"op":"read","handle":h,"offset":0,"size":2048}),
            &[],
            true,
        )
        .unwrap()
        .data;
        assert_eq!(data.len(), 1026);
        assert_eq!(&data[1023..], &[0, 255, 2]);
        assert!(data[..1023].iter().all(|b| *b == 0));
        run(
            &db,
            &mut fs,
            json!({"op":"truncate","handle":h,"size":1024}),
            &[],
            true,
        )
        .unwrap();
        run(
            &db,
            &mut fs,
            json!({"op":"truncate","handle":h,"size":1030}),
            &[],
            true,
        )
        .unwrap();
        assert_eq!(
            run(
                &db,
                &mut fs,
                json!({"op":"read","handle":h,"offset":1024,"size":10}),
                &[],
                true
            )
            .unwrap()
            .data,
            vec![0; 6]
        );
        let a = run(
            &db,
            &mut fs,
            json!({"op":"open","path":"/workspace/file","append":true}),
            &[],
            true,
        )
        .unwrap()
        .value["handle"]
            .as_u64()
            .unwrap();
        assert_eq!(
            run(
                &db,
                &mut fs,
                json!({"op":"write","handle":a,"offset":0}),
                b"end",
                true
            )
            .unwrap()
            .value["offset"],
            1033
        );
        run(
            &db,
            &mut fs,
            json!({"op":"write","handle":h,"offset":3000}),
            &[],
            true,
        )
        .unwrap();
        assert_eq!(resolve(&db, "/workspace/file").unwrap().size, 1033);
    }
    #[test]
    fn handles_shared_for_busy_but_isolated_by_caller_and_revoked() {
        let (db, mut fs) = setup();
        let app = open(&db, &mut fs, "/workspace/app", false);
        let guest = open(&db, &mut fs, "/workspace/guest", true);
        assert_eq!(
            run(&db, &mut fs, json!({"op":"fstat","handle":app}), &[], true),
            Err(Error::BadHandle)
        );
        assert_eq!(
            run(
                &db,
                &mut fs,
                json!({"op":"unlink","path":"/workspace/app"}),
                &[],
                true
            ),
            Err(Error::Busy)
        );
        run(
            &db,
            &mut fs,
            json!({"op":"rename","path":"/workspace/guest","to":"/workspace/moved"}),
            &[],
            true,
        )
        .unwrap();
        assert_eq!(
            run(
                &db,
                &mut fs,
                json!({"op":"fstat","handle":guest}),
                &[],
                true
            )
            .unwrap()
            .value["ino"],
            resolve(&db, "/workspace/moved").unwrap().ino
        );
        fs.revoke();
        assert_eq!(
            run(
                &db,
                &mut fs,
                json!({"op":"fstat","handle":guest}),
                &[],
                true
            ),
            Err(Error::BadHandle)
        );
        assert!(run(&db, &mut fs, json!({"op":"fstat","handle":app}), &[], false).is_ok());
        run(
            &db,
            &mut fs,
            json!({"op":"unlink","path":"/workspace/moved"}),
            &[],
            true,
        )
        .unwrap();
    }
    #[test]
    fn quotas_and_sql_errors_roll_back_atomic_operations() {
        let (db, mut fs) = setup();
        run(
            &db,
            &mut fs,
            json!({"op":"writeFile","path":"/workspace/a"}),
            &vec![7; 3000],
            false,
        )
        .unwrap();
        assert_eq!(
            run(
                &db,
                &mut fs,
                json!({"op":"writeFile","path":"/workspace/b"}),
                &vec![8; 2000],
                false
            ),
            Err(Error::Space)
        );
        assert_eq!(
            resolve(&db, "/workspace/b").unwrap_err().code(),
            Error::Missing
        );
        db.execute_batch("CREATE TRIGGER fail_second BEFORE INSERT ON fs_data WHEN NEW.chunk_index=1 BEGIN SELECT RAISE(ABORT,'injected'); END").unwrap();
        assert_eq!(
            run(
                &db,
                &mut fs,
                json!({"op":"writeFile","path":"/workspace/a"}),
                &vec![9; 2000],
                false
            ),
            Err(Error::Io)
        );
        assert_eq!(
            run(
                &db,
                &mut fs,
                json!({"op":"readFile","path":"/workspace/a"}),
                &[],
                false
            )
            .unwrap()
            .data,
            vec![7; 3000]
        );
    }
    #[test]
    fn directory_rename_replace_and_confinement() {
        let (db, mut fs) = setup();
        for p in ["/workspace/a", "/workspace/a/child", "/workspace/b"] {
            run(&db, &mut fs, json!({"op":"mkdir","path":p}), &[], true).unwrap();
        }
        assert_eq!(
            run(
                &db,
                &mut fs,
                json!({"op":"rmdir","path":"/workspace/a"}),
                &[],
                true
            ),
            Err(Error::NotEmpty)
        );
        assert_eq!(
            run(
                &db,
                &mut fs,
                json!({"op":"rename","path":"/workspace/a","to":"/workspace/a/child/x"}),
                &[],
                true
            ),
            Err(Error::Invalid)
        );
        run(
            &db,
            &mut fs,
            json!({"op":"rename","path":"/workspace/a","to":"/workspace/b"}),
            &[],
            true,
        )
        .unwrap();
        assert_eq!(
            run(
                &db,
                &mut fs,
                json!({"op":"list","path":"/workspace/b"}),
                &[],
                true
            )
            .unwrap()
            .value[0]["name"],
            "child"
        );
        for p in ["/", "/elsewhere", "/workspace/../escape", "workspace/a"] {
            assert_eq!(resolve(&db, p).unwrap_err().code(), Error::Access);
        }
        assert_eq!(
            run(
                &db,
                &mut fs,
                json!({"op":"unlink","path":"/workspace"}),
                &[],
                true
            ),
            Err(Error::Busy)
        );
    }
    #[test]
    fn random_native_operations_match_independent_dense_model() {
        let (db, mut fs) = setup();
        let h = open(&db, &mut fs, "/workspace/model", true);
        let mut model = Vec::<u8>::new();
        let mut seed = 7_u64;
        for step in 0..400 {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            let offset = (seed >> 32) as usize % 2500;
            if step % 3 == 0 {
                run(
                    &db,
                    &mut fs,
                    json!({"op":"truncate","handle":h,"size":offset}),
                    &[],
                    true,
                )
                .unwrap();
                model.resize(offset, 0);
            } else {
                let data: Vec<u8> = (0..(seed as usize % 133))
                    .map(|i| (i + step) as u8)
                    .collect();
                run(
                    &db,
                    &mut fs,
                    json!({"op":"write","handle":h,"offset":offset}),
                    &data,
                    true,
                )
                .unwrap();
                if !data.is_empty() {
                    model.resize(model.len().max(offset + data.len()), 0);
                    model[offset..offset + data.len()].copy_from_slice(&data);
                }
            }
            assert_eq!(
                run(
                    &db,
                    &mut fs,
                    json!({"op":"readFile","path":"/workspace/model"}),
                    &[],
                    false
                )
                .unwrap()
                .data,
                model,
                "step {step}"
            );
        }
        for name in ["a", "b", "c"] {
            open(&db, &mut fs, &format!("/workspace/{name}"), true);
        }
        assert_eq!(
            run(
                &db,
                &mut fs,
                json!({"op":"open","path":"/workspace/overflow","read":true,"write":true,"create":true}),
                &[],
                true
            ),
            Err(Error::Handles)
        );
        assert_eq!(
            resolve(&db, "/workspace/overflow").unwrap_err().code(),
            Error::Missing
        );
        db.execute(
            "INSERT INTO fs_inode(ino,mode,nlink,atime,mtime,ctime) VALUES(99,41471,1,0,0,0)",
            [],
        )
        .unwrap();
        db.execute(
            "INSERT INTO fs_dentry(name,parent_ino,ino) VALUES('link',2,99)",
            [],
        )
        .unwrap();
        assert_eq!(
            resolve(&db, "/workspace/link").unwrap_err().code(),
            Error::Loop
        );
    }
    #[test]
    fn capability_ordering_and_expiry() {
        let mut c = Capability {
            token: "secret".into(),
            command: "command-1".into(),
            epoch: 7,
            deadline: 10,
            next: 1,
            session: None,
            allowed_operations: GUEST_OPERATIONS,
        };
        let mut r = Request {
            scope: "test".into(),
            token: "wrong".into(),
            command: "command-1".into(),
            sequence: 1,
            operation: json!({"op":"heartbeat"}),
            data: vec![],
        };
        assert_eq!(c.bind(&r, 3, 7, 0), Err(Error::Stale));
        r.token = "secret".into();
        r.command = "other-command".into();
        assert_eq!(c.bind(&r, 3, 7, 0), Err(Error::Stale));
        r.command = "command-1".into();
        assert_eq!(c.bind(&r, 3, 8, 0), Err(Error::Stale));
        assert_eq!(c.bind(&r, 3, 7, 0), Ok(()));
        assert_eq!(c.bind(&r, 4, 7, 0), Err(Error::Stale));
        assert_eq!(c.admit(&r, 3, 7, 0), Ok(()));
        assert_eq!(c.admit(&r, 3, 7, 0), Err(Error::Stale));
        r.sequence = 2;
        assert_eq!(c.admit(&r, 3, 7, 10), Err(Error::Stale));
        r.operation = json!({"op":"configure"});
        assert_eq!(c.admit(&r, 3, 7, 0), Err(Error::Stale));
        r.operation = json!({"op":"write"});
        c.allowed_operations = 1 << 13; // heartbeat only
        assert_eq!(c.admit(&r, 3, 7, 0), Err(Error::Stale));
    }
}
