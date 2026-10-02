//! The full-checkpoint scan reads the WAL header and frame headers through an
//! open file, and finds the same salts the whole-file reader does: across
//! empty, short and torn files, malformed headers, and salts that restart
//! mid-file. `Db::verify` reaches it when the WAL salt changes under it.

use celld_ltx::internal::db as internal;
use celld_ltx::wal::{frame_salts_until_in_file, WalReader};
use celld_ltx::{wal_checksum, Db, LtxHost, WAL_FRAME_HEADER_SIZE, WAL_HEADER_SIZE};
use rusqlite::Connection;
use std::collections::HashSet;
use std::path::Path;

const MAGIC_LE: u32 = 0x377f_0682;
const MAGIC_BE: u32 = 0x377f_0683;
const VERSION: u32 = 3_007_000;

type Salt = (u32, u32);

/// The whole-file scan `Db::detect_full_checkpoint` did before it read
/// through the file handle, kept as the oracle: the salt set, or the
/// header error's message.
fn oracle_salts(bytes: &[u8], until: Salt) -> Result<HashSet<Salt>, String> {
    let rd = WalReader::new(bytes).map_err(|e| e.to_string())?;
    Ok(rd.frame_salts_until(until))
}

fn file_salts(path: &Path, until: Salt) -> Result<HashSet<Salt>, String> {
    let mut file = LtxHost::default().open(path).expect("open wal");
    frame_salts_until_in_file(&mut file, until)
        .expect("read wal")
        .map_err(|e| e.to_string())
}

/// `Db::detect_full_checkpoint`'s answer from a salt scan.
fn detect(
    scan: impl Fn(Salt) -> Result<HashSet<Salt>, String>,
    known: &[Salt],
) -> Result<bool, String> {
    let mut m = scan(known.last().copied().unwrap_or((0, 0)))?;
    for s in known {
        m.remove(s);
    }
    Ok(!m.is_empty())
}

struct Wal {
    big_endian: bool,
    page_size: u32,
    salt: Salt,
    frames: Vec<Salt>,
}

impl Wal {
    fn bytes(&self, rng: &mut Rng) -> Vec<u8> {
        let mut out = Vec::new();
        let magic = if self.big_endian { MAGIC_BE } else { MAGIC_LE };
        for v in [magic, VERSION, self.page_size, 7, self.salt.0, self.salt.1] {
            out.extend_from_slice(&v.to_be_bytes());
        }
        let (c0, c1) = wal_checksum(self.big_endian, 0, 0, &out);
        out.extend_from_slice(&c0.to_be_bytes());
        out.extend_from_slice(&c1.to_be_bytes());
        for (i, salt) in self.frames.iter().enumerate() {
            // Page number, commit, salts, and checksums the scan never checks.
            for v in [i as u32 + 1, 0, salt.0, salt.1, rng.next(), rng.next()] {
                out.extend_from_slice(&v.to_be_bytes());
            }
            out.extend((0..self.page_size).map(|_| rng.next() as u8));
        }
        out
    }

    fn frame_size(&self) -> usize {
        WAL_FRAME_HEADER_SIZE + self.page_size as usize
    }
}

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u32 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        (self.0 >> 32) as u32
    }

    fn below(&mut self, n: usize) -> usize {
        self.next() as usize % n.max(1)
    }
}

/// Compares the two scans on `bytes` for every `until` and `known` the
/// caller could pass: each salt present, the header's, and absent ones.
fn assert_same(dir: &Path, bytes: &[u8], salts: &[Salt], label: &str) {
    let path = dir.join("cell.db-wal");
    LtxHost::default().write(&path, bytes).expect("write wal");

    let mut untils: Vec<Salt> = salts.to_vec();
    untils.extend([(0, 0), (0xdead, 0xbeef)]);
    for &until in &untils {
        assert_eq!(
            file_salts(&path, until),
            oracle_salts(bytes, until),
            "{label}: until {until:?}"
        );
    }

    let mut knowns: Vec<Vec<Salt>> = vec![vec![]];
    for &a in &untils {
        knowns.push(vec![a]);
        for &b in &untils {
            knowns.push(vec![a, b]);
        }
    }
    for known in &knowns {
        assert_eq!(
            detect(|u| file_salts(&path, u), known),
            detect(|u| oracle_salts(bytes, u), known),
            "{label}: known {known:?}"
        );
    }
}

#[test]
fn table() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
    let (a, b, c) = ((1, 2), (3, 4), (5, 6));
    let wal = |frames: Vec<Salt>| Wal {
        big_endian: false,
        page_size: 512,
        salt: a,
        frames,
    };
    let salts = [a, b, c];

    assert_same(dir.path(), &[], &salts, "empty");
    let full = wal(vec![a, a, b]).bytes(&mut rng);
    assert_same(dir.path(), &full[..31], &salts, "short header");
    assert_same(dir.path(), &full[..WAL_HEADER_SIZE], &salts, "header only");
    assert_same(dir.path(), &full, &salts, "two salts");
    assert_same(
        dir.path(),
        &wal(vec![]).bytes(&mut rng),
        &salts,
        "no frames",
    );
    assert_same(
        dir.path(),
        &wal(vec![b, b, a, a, c, a]).bytes(&mut rng),
        &salts,
        "salts restart mid-file",
    );

    let fs = wal(vec![]).frame_size();
    let three = wal(vec![b, c, a]).bytes(&mut rng);
    for cut in [
        WAL_HEADER_SIZE + 1,
        WAL_HEADER_SIZE + WAL_FRAME_HEADER_SIZE - 1,
        WAL_HEADER_SIZE + WAL_FRAME_HEADER_SIZE,
        WAL_HEADER_SIZE + fs + 10,
        WAL_HEADER_SIZE + 2 * fs + WAL_FRAME_HEADER_SIZE - 1,
        WAL_HEADER_SIZE + 2 * fs + WAL_FRAME_HEADER_SIZE,
        three.len() - 1,
    ] {
        assert_same(dir.path(), &three[..cut], &salts, &format!("torn at {cut}"));
    }

    let mut bad_magic = full.clone();
    bad_magic[3] ^= 0xff;
    assert_same(dir.path(), &bad_magic, &salts, "bad magic");
    let mut bad_checksum = full.clone();
    bad_checksum[31] ^= 0xff;
    assert_same(dir.path(), &bad_checksum, &salts, "bad header checksum");
    let mut bad_version = Wal {
        big_endian: true,
        ..wal(vec![a])
    }
    .bytes(&mut rng);
    bad_version[7] ^= 1;
    let (c0, c1) = wal_checksum(true, 0, 0, &bad_version[..24]);
    bad_version[24..28].copy_from_slice(&c0.to_be_bytes());
    bad_version[28..32].copy_from_slice(&c1.to_be_bytes());
    assert_same(dir.path(), &bad_version, &salts, "bad version");
}

#[test]
fn random() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut rng = Rng(0x2545_f491_4f6c_dd1d);
    let pool: Vec<Salt> = (0..4).map(|_| (rng.next(), rng.next())).collect();
    for case in 0..500 {
        let wal = Wal {
            big_endian: rng.below(2) == 0,
            page_size: [0, 8, 64, 512][rng.below(4)],
            salt: pool[rng.below(pool.len())],
            frames: (0..rng.below(12))
                .map(|_| pool[rng.below(pool.len())])
                .collect(),
        };
        let mut bytes = wal.bytes(&mut rng);
        match rng.below(4) {
            // Torn anywhere, from empty to whole.
            0 => bytes.truncate(rng.below(bytes.len() + 1)),
            // A torn trailing frame.
            1 if !wal.frames.is_empty() => {
                let fs = wal.frame_size();
                bytes.truncate(bytes.len() - 1 - rng.below(fs));
            }
            _ => {}
        }
        assert_same(dir.path(), &bytes, &pool, &format!("case {case}"));
    }
}

/// Restarts the WAL `restarts` times behind the `Db`'s back, each restart
/// overwriting a shorter prefix of the frames the last sync read, and
/// returns what `verify` decides.
fn verify_after_restarts(restarts: usize) -> internal::VerifyInfo {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("cell.db");
    let mut db = Db::open(&path).expect("open db");
    let writer = Connection::open(&path).expect("open writer");
    let insert = |n: usize| {
        for _ in 0..n {
            writer
                .execute("INSERT INTO t (v) VALUES (?1)", ["x".repeat(3000)])
                .expect("insert");
        }
    };
    writer
        .execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .expect("create table");
    insert(20);
    db.sync().expect("sync");

    internal::release_read_lock(&mut db).expect("release read lock");
    for n in (1..=restarts).rev() {
        writer
            .query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |_| Ok(()))
            .expect("checkpoint");
        insert(n);
    }
    internal::acquire_read_lock(&mut db).expect("acquire read lock");
    internal::verify(&mut db).expect("verify")
}

#[test]
fn verify_continues_after_one_restart() {
    let info = verify_after_restarts(1);
    assert_eq!(info.offset, WAL_HEADER_SIZE as i64);
    assert!(!info.snapshotting, "{}", info.reason);
}

#[test]
fn verify_snapshots_after_a_missed_restart() {
    let info = verify_after_restarts(2);
    assert!(info.snapshotting);
    assert_eq!(
        info.reason,
        "full or restart checkpoint detected, snapshotting"
    );
}
