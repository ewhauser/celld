// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//! Bounded native AgentFS 0.4 stat experiment. Uses only the managed connection.
use celld_agentfs_ipc::{Error, Request, Stat};
use rusqlite::{Connection, OptionalExtension};
pub(crate) struct Capability {
    pub token: String,
    pub deadline: u64,
    pub next: u64,
}
impl Capability {
    pub(crate) fn validate(&self, request: &Request, now: u64) -> Result<(), Error> {
        if now >= self.deadline
            || request.token != self.token
            || request.sequence != self.next
            || self.next > celld_agentfs_ipc::MAX_REQUESTS
        {
            return Err(Error::Stale);
        }
        Ok(())
    }
    pub(crate) fn admit(&mut self, request: &Request, now: u64) -> Result<(), Error> {
        self.validate(request, now)?;
        self.next += 1;
        Ok(())
    }
}
pub struct Answer {
    pub result: Result<Stat, Error>,
    pub(crate) observed: Option<u64>,
}
impl crate::js::GatedAnswer for Answer {
    fn write_position(&self) -> Option<u64> {
        None
    }
    fn observed_position(&self) -> Option<u64> {
        self.observed
    }
}
fn inode(db: &Connection, ino: u64) -> Result<Stat, Error> {
    let s = db
        .prepare_cached("SELECT ino,size,mode,atime,mtime,ctime FROM fs_inode WHERE ino=?")
        .map_err(|_| Error::Io)?
        .query_row([ino], |row| {
            Ok(Stat {
                ino: row.get(0)?,
                size: row.get(1)?,
                mode: row.get(2)?,
                atime: row.get(3)?,
                mtime: row.get(4)?,
                ctime: row.get(5)?,
            })
        })
        .optional()
        .map_err(|_| Error::Io)?
        .ok_or(Error::Missing)?;
    match s.mode & 0o170000 {
        0o100000 | 0o40000 => Ok(s),
        0o120000 => Err(Error::Loop),
        _ => Err(Error::Unsupported),
    }
}
pub(crate) fn stat(db: &Connection, path: &str) -> Result<Stat, Error> {
    if path.len() > 4096 || path.contains('\0') {
        return Err(Error::Invalid);
    }
    let parts: Vec<_> = path
        .split('/')
        .filter(|s| !s.is_empty() && *s != ".")
        .collect();
    if !path.starts_with('/') || parts.first() != Some(&"workspace") || parts.contains(&"..") {
        return Err(Error::Access);
    }
    if parts.len() > 64 || parts.iter().any(|s| s.len() > 255) {
        return Err(Error::Invalid);
    }
    let version: String = db
        .prepare_cached("SELECT value FROM fs_config WHERE key='schema_version'")
        .map_err(|_| Error::Unsupported)?
        .query_row([], |row| row.get(0))
        .map_err(|_| Error::Unsupported)?;
    if version != "0.4" {
        return Err(Error::Unsupported);
    }
    let mut row = inode(db, 1)?;
    for name in parts {
        if row.mode & 0o170000 != 0o40000 {
            return Err(Error::NotDirectory);
        }
        let ino: u64 = db
            .prepare_cached("SELECT ino FROM fs_dentry WHERE parent_ino=? AND name=?")
            .map_err(|_| Error::Io)?
            .query_row(rusqlite::params![row.ino, name], |r| r.get(0))
            .optional()
            .map_err(|_| Error::Io)?
            .ok_or(Error::Missing)?;
        row = inode(db, ino)?;
    }
    Ok(row)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn capability_is_ordered_bounded_and_expires() {
        let mut c = Capability {
            token: "secret".into(),
            deadline: 10,
            next: 1,
        };
        let mut r = Request {
            scope: "test".into(),
            token: "wrong".into(),
            sequence: 1,
            path: "/workspace".into(),
        };
        assert_eq!(c.admit(&r, 0), Err(Error::Stale));
        r.token = "secret".into();
        assert_eq!(c.admit(&r, 0), Ok(()));
        assert_eq!(c.admit(&r, 0), Err(Error::Stale));
        r.sequence = 2;
        assert_eq!(c.admit(&r, 10), Err(Error::Stale));
        c.next = celld_agentfs_ipc::MAX_REQUESTS + 1;
        r.sequence = c.next;
        assert_eq!(c.admit(&r, 0), Err(Error::Stale));
    }
    #[test]
    fn native_stat_confines_paths_and_matches_agentfs() {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch("CREATE TABLE fs_config(key TEXT,value TEXT); INSERT INTO fs_config VALUES('schema_version','0.4'); CREATE TABLE fs_inode(ino INTEGER,size INTEGER,mode INTEGER,atime INTEGER,mtime INTEGER,ctime INTEGER); CREATE TABLE fs_dentry(parent_ino INTEGER,name TEXT,ino INTEGER); INSERT INTO fs_inode VALUES(1,0,16877,1,2,3),(2,0,16877,1,2,3),(3,123,33188,1,2,3),(4,0,41471,1,2,3); INSERT INTO fs_dentry VALUES(1,'workspace',2),(2,'file',3),(2,'link',4);").unwrap();
        assert_eq!(stat(&db, "/workspace/./file").unwrap().size, 123);
        assert_eq!(stat(&db, "/workspace/missing"), Err(Error::Missing));
        assert_eq!(stat(&db, "/workspace/file/x"), Err(Error::NotDirectory));
        assert_eq!(stat(&db, "/workspace/link"), Err(Error::Loop));
        for path in ["/", "/workspace/../outside", "workspace/file", "/elsewhere"] {
            assert_eq!(stat(&db, path), Err(Error::Access));
        }
    }
}
