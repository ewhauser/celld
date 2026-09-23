// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//! Offline preview bootstrap. The caller must hold an exclusive, operator-issued
//! initialization claim and keep the destination runtime stopped through import.
//! Source reads use persisted LTX checkpoints, not unflushed live/node-log state.
#![allow(clippy::disallowed_methods)] // Offline administrator path, outside Actor execution.

use crate::bucket::Bucket;
use anyhow::{ensure, Context};
use celld_ltx::client::{
    epochs::EpochChain,
    object_store::{ObjectStoreClient, ObjectStoreConfig},
};
use celld_ltx::{ltx, replica, TXID};
use object_store::path::Path as ObjectPath;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;

pub const EXECUTOR: &str = "celld-snapshot-v1";
const MAX_BYTES: u64 = 256 * 1024 * 1024;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Object {
    pub class: String,
    pub id: String,
}
impl Object {
    pub fn scope(&self) -> anyhow::Result<String> {
        let scope = format!("{}:{}", self.class, self.id);
        let component = |s: &str| {
            !s.is_empty()
                && s.bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"_$.-".contains(&b))
        };
        ensure!(
            component(&self.class)
                && component(&self.id)
                && celld_logic::cell::valid_cell_scope(&scope),
            "invalid preview object identity"
        );
        ensure!(
            !crate::deploy::is_reserved_class(&self.class),
            "system classes cannot be preview seeds"
        );
        Ok(scope)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub enum Alarms {
    Clear,
    Preserve,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Entry {
    #[serde(flatten)]
    pub object: Object,
    #[serde(rename = "snapshotID")]
    pub snapshot_id: String,
    pub source_version: String,
    pub digest: String,
}

pub fn validate_selection(objects: &[Object]) -> anyhow::Result<()> {
    ensure!(
        (1..=100).contains(&objects.len()),
        "select between 1 and 100 objects"
    );
    let mut seen = BTreeSet::new();
    for object in objects {
        ensure!(seen.insert(object.scope()?), "duplicate preview object");
    }
    Ok(())
}

/// Reserved epoch zero is a read-only bootstrap image, never a writer epoch.
pub fn bootstrap_key(scope: &str) -> String {
    format!("cells/{scope}/ltx/e0/0000/0000000000000001-0000000000000001.ltx")
}

pub fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn operation_prefix(operation: &str) -> anyhow::Result<String> {
    ensure!(
        !operation.is_empty()
            && operation.len() <= 128
            && operation
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-'),
        "invalid initialization identity"
    );
    Ok(format!("preview-snapshots/{operation}/"))
}

/// Refuse a destination with any runtime/application data. Existing immutable
/// snapshot staging from this operation is permitted; it is never a lease.
pub async fn ensure_unopened(target: &Bucket, operation: &str) -> anyhow::Result<()> {
    let allowed = operation_prefix(operation)?;
    for item in target.list("").await? {
        let key = item.location.as_ref();
        ensure!(
            key.starts_with(&allowed),
            "preview destination is not empty: {key}"
        );
    }
    Ok(())
}

/// Capture and persist one sanitized snapshot with create-only semantics.
/// Never recapture an operation after a lost claim; the Kubernetes manifest pins
/// every returned entry before any bootstrap image is installed.
pub async fn capture(
    source: &Bucket,
    target: &Bucket,
    operation: &str,
    object: &Object,
    alarms: Alarms,
) -> anyhow::Result<Entry> {
    let scope = object.scope()?;
    let snapshot_id = format!("{}{scope}.ltx", operation_prefix(operation)?);
    ensure!(
        target.head(&snapshot_id).await?.is_none(),
        "snapshot already exists; do not recapture an uncertain initialization"
    );
    let base = ObjectPath::from(format!("{}cells/{scope}/ltx", source.prefix));
    let listing = source.store.list_with_delimiter(Some(&base)).await?;
    let mut epochs: Vec<u64> = listing
        .common_prefixes
        .iter()
        .filter_map(|p| p.filename()?.strip_prefix('e')?.parse().ok())
        .collect();
    epochs.sort_unstable();
    ensure!(!epochs.is_empty(), "no persisted checkpoint for {scope}");
    let clients = epochs
        .into_iter()
        .map(|epoch| {
            let config = ObjectStoreConfig {
                path: format!("{}cells/{scope}/ltx/e{epoch}", source.prefix),
                ..Default::default()
            };
            (
                epoch,
                ObjectStoreClient::with_store(config, source.store.clone()),
            )
        })
        .collect();
    let chain = EpochChain::build(clients).await?;
    let source_version = format!(
        "ltx:e{}:txid:{}",
        chain.spans().last().context("empty checkpoint")?.0,
        chain.max_txid().0
    );
    let plan = replica::calc_restore_plan(&chain, TXID(0)).await?;
    let size: u64 = plan.iter().map(|file| file.size.max(0) as u64).sum();
    ensure!(
        size <= MAX_BYTES,
        "checkpoint exceeds preview snapshot limit"
    );
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("snapshot.sqlite");
    replica::restore(&chain, &path, TXID(0)).await?;
    ensure!(
        std::fs::metadata(&path)?.len() <= MAX_BYTES,
        "database exceeds preview snapshot limit"
    );
    sanitize(&path, alarms, 0)?;
    let payload = encode_sqlite(&std::fs::read(path)?)?;
    let entry = Entry {
        object: object.clone(),
        snapshot_id,
        source_version,
        digest: digest(&payload),
    };
    ensure!(
        target
            .put_cas(&entry.snapshot_id, payload, None)
            .await?
            .is_some(),
        "snapshot identity already used"
    );
    Ok(entry)
}

/// Import the complete, already-pinned manifest. The caller must recheck its
/// claim/cancellation before each call and may not overlap an earlier writer.
/// Only byte-identical repeats are accepted, never replacement of a seed.
pub async fn import(target: &Bucket, operation: &str, entry: &Entry) -> anyhow::Result<()> {
    let scope = entry.object.scope()?;
    ensure!(
        entry.snapshot_id == format!("{}{scope}.ltx", operation_prefix(operation)?),
        "snapshot belongs to another initialization"
    );
    let (size, _) = target
        .head(&entry.snapshot_id)
        .await?
        .context("missing pinned snapshot")?;
    ensure!(size <= MAX_BYTES, "snapshot exceeds preview limit");
    let (bytes, _) = target
        .get(&entry.snapshot_id)
        .await?
        .context("missing pinned snapshot")?;
    ensure!(digest(&bytes) == entry.digest, "snapshot digest mismatch");
    // Decode verifies checksums as well as the complete snapshot shape.
    let header = ltx::Header::parse(&bytes)?;
    ensure!(
        u64::from(header.commit) * u64::from(header.page_size) <= MAX_BYTES,
        "expanded snapshot exceeds preview limit"
    );
    ltx::decode_database_image(&bytes)?;
    let key = bootstrap_key(&scope);
    if target.put_cas(&key, bytes.clone(), None).await?.is_none() {
        let (existing, _) = target.get(&key).await?.context("bootstrap disappeared")?;
        ensure!(
            existing == bytes,
            "destination bootstrap conflicts with pinned snapshot"
        );
    }
    Ok(())
}

fn sanitize(path: &std::path::Path, alarms: Alarms, depth: usize) -> anyhow::Result<()> {
    ensure!(depth < 4, "facet nesting exceeds preview limit");
    let db = rusqlite::Connection::open(path)?;
    db.execute_batch("PRAGMA trusted_schema=OFF; DROP TRIGGER IF EXISTS _cf_WAKE_insert; DROP TRIGGER IF EXISTS _cf_WAKE_update; DROP TRIGGER IF EXISTS _cf_WAKE_delete; DROP TABLE IF EXISTS _cf_WAKE; DROP TABLE IF EXISTS _litestream_seq; DROP TABLE IF EXISTS _litestream_lock;")?;
    let has_table = |name: &str| -> rusqlite::Result<bool> {
        db.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name=?1)",
            [name],
            |r| r.get(0),
        )
    };
    // Embedded facets are SQLite images too. Do not silently retain their
    // alarms/control state. All images in the root table are flattened paths.
    if has_table("_cf_FACETS")? {
        let images = db
            .prepare("SELECT scope,path,image FROM _cf_FACETS")?
            .query_map([], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, Vec<u8>>(2)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        for (scope, facet, bytes) in images {
            let file = tempfile::NamedTempFile::new()?;
            std::fs::write(file.path(), bytes)?;
            sanitize(file.path(), alarms, depth + 1)?;
            db.execute(
                "UPDATE _cf_FACETS SET image=?1 WHERE scope=?2 AND path=?3",
                rusqlite::params![std::fs::read(file.path())?, scope, facet],
            )?;
        }
    }
    for table in ["_cf_ALARM", "alarms"] {
        if has_table(table)? && alarms == Alarms::Clear {
            db.execute(&format!("DELETE FROM {table}"), [])?;
        }
    }
    db.execute_batch("PRAGMA wal_checkpoint(TRUNCATE); PRAGMA journal_mode=DELETE; VACUUM;")?;
    Ok(())
}

fn encode_sqlite(bytes: &[u8]) -> anyhow::Result<Vec<u8>> {
    ensure!(
        bytes.len() >= 100 && &bytes[..16] == b"SQLite format 3\0",
        "not a SQLite database"
    );
    let raw = u16::from_be_bytes([bytes[16], bytes[17]]);
    let page_size = if raw == 1 { 65536 } else { u32::from(raw) };
    ensure!(
        page_size.is_power_of_two()
            && (512..=65536).contains(&page_size)
            && bytes.len().is_multiple_of(page_size as usize),
        "invalid SQLite page size"
    );
    let commit = u32::try_from(bytes.len() / page_size as usize)?;
    let pages: Vec<_> = bytes
        .chunks_exact(page_size as usize)
        .enumerate()
        .filter_map(|(i, p)| {
            let n = i as u32 + 1;
            (n != ltx::lock_pgno(page_size)).then(|| (n, p.to_vec()))
        })
        .collect();
    let checksum = pages.iter().fold(celld_ltx::CHECKSUM_FLAG, |sum, (n, p)| {
        sum ^ (ltx::checksum_page(*n, p) & !celld_ltx::CHECKSUM_FLAG)
    });
    let header = ltx::Header {
        version: ltx::VERSION,
        page_size,
        commit,
        min_txid: TXID(1),
        max_txid: TXID(1),
        ..Default::default()
    };
    Ok(ltx::encode_file(&header, &pages, checksum)?)
}

#[cfg(test)]
mod tests;
