// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The persistent object store for `celld dev`.
//!
//! The development supervisor and its node are separate processes, and both
//! write the fleet store during a reload. SQLite supplies the cross-process
//! transaction that a directory of files cannot: a conditional update checks
//! its ETag and installs the new object in one commit. This backend is local to
//! one development machine. It is not a shared-filesystem production mode.

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::future::BoxFuture;
use futures_util::stream::{self, BoxStream};
use futures_util::FutureExt as _;
use futures_util::StreamExt as _;
use object_store::list::{PaginatedListOptions, PaginatedListResult, PaginatedListStore};
use object_store::path::{Path, DELIMITER};
use object_store::{
    Attribute, AttributeValue, Attributes, Error, GetOptions, GetResult, GetResultPayload,
    ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMode, PutMultipartOptions, PutOptions,
    PutPayload, PutResult,
};
use rusqlite::{params, Connection, OptionalExtension as _, TransactionBehavior};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fmt;
use std::ops::ControlFlow;
use std::path::{Path as FsPath, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

const STORE: &str = "celld development store";

#[derive(Clone, Debug)]
pub(crate) struct LocalStore {
    database: PathBuf,
    /// Connections a finished operation gave back. Opening one per operation
    /// cost more than the operation: SQLite holds one process-wide mutex
    /// while it opens and closes a file, and every cell's WAL reads and
    /// commits take that same mutex, so a node activating a few hundred
    /// cells a second queued its cells' SQLite behind this store's opens.
    idle: Arc<Mutex<Vec<Connection>>>,
    writes: Arc<Writes>,
}

/// Group commit for puts. A put queues itself, then waits for the commit
/// lock; whoever holds it commits every queued put in one transaction. The
/// store has one SQLite writer, and a put waiting for it used to sleep in
/// SQLite's busy handler, which polls with backoff up to 100 ms: at a few
/// hundred puts a second, dozens of threads slept there while the writer
/// paid one fsync per put.
#[derive(Debug, Default)]
struct Writes {
    queue: Mutex<Vec<Arc<PendingPut>>>,
    commit: Mutex<()>,
}

#[derive(Debug)]
struct PendingPut {
    key: String,
    body: Bytes,
    mode: PutMode,
    attributes: String,
    modified_ms: i64,
    result: Mutex<Option<object_store::Result<PutResult>>>,
}

/// Idle connections kept for reuse. A burst can hold more at once; those
/// close when they come back to a full pool.
const MAX_IDLE_CONNECTIONS: usize = 32;

/// A connection lent for one operation. It returns to the pool on drop
/// unless a transaction is still open on it.
struct Pooled<'a> {
    store: &'a LocalStore,
    connection: Option<Connection>,
}

impl std::ops::Deref for Pooled<'_> {
    type Target = Connection;

    fn deref(&self) -> &Connection {
        self.connection.as_ref().expect("pooled connection")
    }
}

impl std::ops::DerefMut for Pooled<'_> {
    fn deref_mut(&mut self) -> &mut Connection {
        self.connection.as_mut().expect("pooled connection")
    }
}

impl Drop for Pooled<'_> {
    fn drop(&mut self) {
        let Some(connection) = self.connection.take() else {
            return;
        };
        if !connection.is_autocommit() {
            return;
        }
        let mut idle = self.store.idle.lock().expect("store pool poisoned");
        if idle.len() < MAX_IDLE_CONNECTIONS {
            idle.push(connection);
        }
    }
}

#[derive(Debug)]
struct StoredObject {
    key: String,
    body: Vec<u8>,
    size: u64,
    etag: i64,
    modified_ms: i64,
    attributes: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct StoredAttribute {
    kind: String,
    name: Option<String>,
    value: String,
}

impl LocalStore {
    pub(crate) fn open(database: impl AsRef<FsPath>) -> object_store::Result<Self> {
        let database = database.as_ref().to_path_buf();
        let store = Self {
            database,
            idle: Arc::default(),
            writes: Arc::default(),
        };
        let connection = store.connect()?;
        connection
            .pragma_update(None, "journal_mode", "WAL")
            .map_err(db_error)?;
        connection
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS objects (
                   key TEXT PRIMARY KEY,
                   body BLOB NOT NULL,
                   etag INTEGER NOT NULL,
                   modified_ms INTEGER NOT NULL,
                   attributes TEXT NOT NULL
                 );
                 CREATE TABLE IF NOT EXISTS store_sequence (
                   singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                   next_etag INTEGER NOT NULL
                 );
                 INSERT OR IGNORE INTO store_sequence(singleton, next_etag) VALUES (1, 1);",
            )
            .map_err(db_error)?;
        drop(connection);
        Ok(store)
    }

    fn connect(&self) -> object_store::Result<Pooled<'_>> {
        let idle = self.idle.lock().expect("store pool poisoned").pop();
        let connection = match idle {
            Some(connection) => connection,
            None => {
                let connection = Connection::open(&self.database).map_err(db_error)?;
                configure_connection(&connection)?;
                connection
            }
        };
        Ok(Pooled {
            store: self,
            connection: Some(connection),
        })
    }

    fn read(&self, key: &str) -> object_store::Result<StoredObject> {
        self.connect()?
            .query_row(
                "SELECT key, body, etag, modified_ms, attributes, length(body)
                 FROM objects WHERE key = ?1",
                [key],
                |row| {
                    Ok(StoredObject {
                        key: row.get(0)?,
                        body: row.get(1)?,
                        size: row.get(5)?,
                        etag: row.get(2)?,
                        modified_ms: row.get(3)?,
                        attributes: row.get(4)?,
                    })
                },
            )
            .optional()
            .map_err(db_error)?
            .ok_or_else(|| not_found(key))
    }

    /// Visit, in key order, each object whose key starts with `prefix` and
    /// sorts after `after`, until `visit` breaks. The row holds the columns
    /// [`listed_meta`] reads.
    ///
    /// The bounds are a range on the primary key's index, so a listing reads
    /// the keys it can return and not the bucket. Reading every object and
    /// filtering in Rust cost a quarter of a dev node's CPU and allocations
    /// once the bucket held a few thousand LTX files (2026-10-01).
    fn scan(
        &self,
        prefix: &str,
        after: Option<&str>,
        mut visit: impl FnMut(&str, &rusqlite::Row<'_>) -> object_store::Result<ControlFlow<()>>,
    ) -> object_store::Result<()> {
        let (lower, inclusive) = match after {
            Some(after) if after >= prefix => (after, false),
            _ => (prefix, true),
        };
        let upper = prefix_successor(prefix);
        let connection = self.connect()?;
        let mut statement = connection
            .prepare_cached(&scan_sql(inclusive, upper.is_some()))
            .map_err(db_error)?;
        let mut rows = match &upper {
            Some(upper) => statement.query(rusqlite::named_params! {
                ":lower": lower,
                ":upper": upper,
            }),
            None => statement.query(rusqlite::named_params! { ":lower": lower }),
        }
        .map_err(db_error)?;
        while let Some(row) = rows.next().map_err(db_error)? {
            let key = row.get_ref(0).and_then(|key| Ok(key.as_str()?));
            if visit(key.map_err(db_error)?, row)?.is_break() {
                break;
            }
        }
        Ok(())
    }

    fn put_sync(
        &self,
        key: String,
        body: Bytes,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        let put = Arc::new(PendingPut {
            attributes: encode_attributes(&options.attributes)?,
            modified_ms: crate::asyncrt::wall_ms(),
            key,
            body,
            mode: options.mode,
            result: Mutex::new(None),
        });
        self.writes
            .queue
            .lock()
            .expect("store write queue poisoned")
            .push(put.clone());
        let _commit = self.writes.commit.lock().expect("store commit poisoned");
        // A committer that took this put from the queue answered it before
        // releasing the lock. Unanswered, it is still queued, and this thread
        // commits it with whatever queued behind it.
        if let Some(result) = take_result(&put) {
            return result;
        }
        let batch = std::mem::take(
            &mut *self
                .writes
                .queue
                .lock()
                .expect("store write queue poisoned"),
        );
        match self.commit_puts(&batch) {
            Ok(results) => {
                for (queued, result) in batch.iter().zip(results) {
                    *queued.result.lock().expect("store put poisoned") = Some(result);
                }
            }
            // One put's failure must not fail the others that shared its
            // transaction: commit each alone.
            Err(_) if batch.len() > 1 => {
                for queued in &batch {
                    let result = self
                        .commit_puts(std::slice::from_ref(queued))
                        .and_then(|mut results| results.pop().expect("one result per put"));
                    *queued.result.lock().expect("store put poisoned") = Some(result);
                }
            }
            Err(error) => {
                *put.result.lock().expect("store put poisoned") = Some(Err(error));
            }
        }
        take_result(&put).expect("the committer answers every put it took")
    }

    /// Apply `puts` in order in one transaction. The outer error aborts all
    /// of them; an inner one is that put's own precondition failure, which
    /// writes nothing and leaves the rest to commit.
    fn commit_puts(
        &self,
        puts: &[Arc<PendingPut>],
    ) -> object_store::Result<Vec<object_store::Result<PutResult>>> {
        let mut connection = self.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(db_error)?;
        let mut results = Vec::with_capacity(puts.len());
        for put in puts {
            results.push(apply_put(&transaction, put)?);
        }
        transaction.commit().map_err(db_error)?;
        Ok(results)
    }

    fn copy_sync(&self, from: &str, to: &str, create: bool) -> object_store::Result<()> {
        let source = self.read(from)?;
        let attributes = decode_attributes(&source.attributes)?;
        self.put_sync(
            to.to_string(),
            source.body.into(),
            PutOptions {
                mode: if create {
                    PutMode::Create
                } else {
                    PutMode::Overwrite
                },
                attributes,
                ..PutOptions::default()
            },
        )?;
        Ok(())
    }
}

fn take_result(put: &PendingPut) -> Option<object_store::Result<PutResult>> {
    put.result.lock().expect("store put poisoned").take()
}

fn apply_put(
    transaction: &rusqlite::Transaction<'_>,
    put: &PendingPut,
) -> object_store::Result<object_store::Result<PutResult>> {
    let current = transaction
        .query_row(
            "SELECT etag FROM objects WHERE key = ?1",
            [&put.key],
            |row| row.get::<_, i64>(0),
        )
        .optional()
        .map_err(db_error)?;
    match &put.mode {
        PutMode::Overwrite => {}
        PutMode::Create if current.is_some() => return Ok(Err(already_exists(&put.key))),
        PutMode::Create => {}
        PutMode::Update(version) => {
            let current = current.map(|etag| etag.to_string());
            if current.as_deref().is_none() || version.e_tag.as_deref() != current.as_deref() {
                return Ok(Err(precondition(&put.key)));
            }
        }
    }
    let etag = transaction
        .query_row(
            "UPDATE store_sequence SET next_etag = next_etag + 1
             WHERE singleton = 1 RETURNING next_etag - 1",
            [],
            |row| row.get::<_, i64>(0),
        )
        .map_err(db_error)?;
    transaction
        .execute(
            "INSERT INTO objects(key, body, etag, modified_ms, attributes)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(key) DO UPDATE SET
               body = excluded.body,
               etag = excluded.etag,
               modified_ms = excluded.modified_ms,
               attributes = excluded.attributes",
            params![
                put.key,
                put.body.as_ref(),
                etag,
                put.modified_ms,
                put.attributes
            ],
        )
        .map_err(db_error)?;
    Ok(Ok(PutResult {
        e_tag: Some(etag.to_string()),
        version: None,
    }))
}

fn configure_connection(connection: &Connection) -> object_store::Result<()> {
    // `synchronous` is connection-local. Setting it only while the schema is
    // created leaves later writes at the bundled SQLite default, so a build
    // configuration can silently weaken the development store's durability.
    connection
        .pragma_update(None, "synchronous", "FULL")
        .map_err(db_error)?;
    connection
        .busy_timeout(Duration::from_secs(30))
        .map_err(db_error)
}

impl fmt::Display for LocalStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "LocalStore({})", self.database.display())
    }
}

#[async_trait]
impl ObjectStore for LocalStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        let store = self.clone();
        let key = location.to_string();
        let body: Bytes = payload.into();
        crate::asyncrt::blocking(move || store.put_sync(key, body, options))
            .await
            .map_err(db_error)?
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        Ok(Box::new(LocalUpload {
            store: self.clone(),
            location: location.clone(),
            attributes: options.attributes,
            parts: Arc::new(Mutex::new(Vec::new())),
        }))
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let store = self.clone();
        let key = location.to_string();
        let object = crate::asyncrt::blocking(move || store.read(&key))
            .await
            .map_err(db_error)??;
        let meta = object_meta(&object)?;
        options.check_preconditions(&meta)?;
        let full = 0..object.body.len() as u64;
        let range = match options.range {
            Some(range) => range.as_range(full.end).map_err(db_error)?,
            None => full,
        };
        let body = Bytes::from(object.body).slice(range.start as usize..range.end as usize);
        Ok(GetResult {
            payload: GetResultPayload::Stream(stream::once(async move { Ok(body) }).boxed()),
            meta,
            range,
            attributes: decode_attributes(&object.attributes)?,
        })
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        let store = self.clone();
        let key = location.to_string();
        crate::asyncrt::blocking(move || {
            store
                .connect()?
                .execute("DELETE FROM objects WHERE key = ?1", [key])
                .map_err(db_error)?;
            Ok(())
        })
        .await
        .map_err(db_error)?
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let prefix = prefix.cloned().unwrap_or_default();
        let mut objects = Vec::new();
        let result = self.scan(&key_prefix(&prefix), None, |key, row| {
            // The range already holds only keys below the prefix. This
            // restates object_store's rule on the parsed path: a prefix
            // matches whole segments and never the object it names.
            let location = stored_location(key)?;
            if location
                .prefix_match(&prefix)
                .is_some_and(|mut remainder| remainder.next().is_some())
            {
                objects.push(listed_meta(location, row)?);
            }
            Ok(ControlFlow::Continue(()))
        });
        match result.map(|()| objects) {
            Ok(objects) => stream::iter(objects.into_iter().map(Ok)).boxed(),
            Err(error) => stream::once(async move { Err(error) }).boxed(),
        }
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        let prefix = prefix.cloned().unwrap_or_default();
        let start = key_prefix(&prefix);
        let mut common_prefixes = BTreeSet::new();
        let mut objects = Vec::new();
        // The key prefix of the last common prefix found. Its keys are
        // adjacent in key order, so the rest of them are skipped unparsed.
        let mut within = None::<String>;
        self.scan(&start, None, |key, row| {
            if within
                .as_deref()
                .is_some_and(|within| key.starts_with(within))
            {
                return Ok(ControlFlow::Continue(()));
            }
            let location = stored_location(key)?;
            let Some(mut remainder) = location.prefix_match(&prefix) else {
                return Ok(ControlFlow::Continue(()));
            };
            let Some(child) = remainder.next() else {
                return Ok(ControlFlow::Continue(()));
            };
            if remainder.next().is_some() {
                within = Some(format!("{start}{}{DELIMITER}", child.as_ref()));
                common_prefixes.insert(prefix.child(child));
                return Ok(ControlFlow::Continue(()));
            }
            drop(remainder);
            objects.push(listed_meta(location, row)?);
            Ok(ControlFlow::Continue(()))
        })?;
        Ok(ListResult {
            common_prefixes: common_prefixes.into_iter().collect(),
            objects,
        })
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        let store = self.clone();
        let from = from.to_string();
        let to = to.to_string();
        crate::asyncrt::blocking(move || store.copy_sync(&from, &to, false))
            .await
            .map_err(db_error)?
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        let store = self.clone();
        let from = from.to_string();
        let to = to.to_string();
        crate::asyncrt::blocking(move || store.copy_sync(&from, &to, true))
            .await
            .map_err(db_error)?
    }
}

#[async_trait]
impl PaginatedListStore for LocalStore {
    async fn list_paginated(
        &self,
        prefix: Option<&str>,
        options: PaginatedListOptions,
    ) -> object_store::Result<PaginatedListResult> {
        let prefix = prefix.unwrap_or_default();
        let after = options.page_token.as_deref().or(options.offset.as_deref());
        let delimiter = options.delimiter.as_deref();
        let limit = options.max_keys.unwrap_or(usize::MAX);
        let mut entries: Vec<(String, Option<Path>, Option<ObjectMeta>)> = Vec::new();
        self.scan(prefix, after, |key, row| {
            let Some(remainder) = key.strip_prefix(prefix) else {
                return Ok(ControlFlow::Continue(()));
            };
            let common = delimiter.and_then(|delimiter| {
                remainder
                    .find(delimiter)
                    .map(|index| &key[..prefix.len() + index + delimiter.len()])
            });
            if let Some(common) = common {
                // The previous entry has this common prefix exactly when
                // its key starts with it.
                if let Some((last, Some(_), _)) = entries.last_mut() {
                    if last.starts_with(common) {
                        last.clear();
                        last.push_str(key);
                        return Ok(ControlFlow::Continue(()));
                    }
                }
                entries.push((key.to_string(), Some(stored_location(common)?), None));
            } else {
                let meta = listed_meta(stored_location(key)?, row)?;
                entries.push((key.to_string(), None, Some(meta)));
            }
            // The entry before this one is final, and it is the last one the
            // page returns.
            Ok(if entries.len() > limit {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            })
        })?;
        let truncated = entries.len() > limit;
        entries.truncate(limit);
        let page_token = truncated
            .then(|| entries.last().map(|(last, _, _)| last.clone()))
            .flatten();
        let mut result = ListResult {
            common_prefixes: Vec::new(),
            objects: Vec::new(),
        };
        for (_, common, object) in entries {
            if let Some(common) = common {
                result.common_prefixes.push(common);
            }
            if let Some(object) = object {
                result.objects.push(object);
            }
        }
        Ok(PaginatedListResult { result, page_token })
    }
}

#[derive(Debug)]
struct LocalUpload {
    store: LocalStore,
    location: Path,
    attributes: Attributes,
    parts: Arc<Mutex<Vec<Bytes>>>,
}

#[async_trait]
impl MultipartUpload for LocalUpload {
    fn put_part(&mut self, data: PutPayload) -> BoxFuture<'static, object_store::Result<()>> {
        self.parts.lock().unwrap().push(data.into());
        async { Ok(()) }.boxed()
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        let parts = std::mem::take(&mut *self.parts.lock().unwrap());
        let bytes = parts.iter().map(Bytes::len).sum();
        let mut body = Vec::with_capacity(bytes);
        for part in parts {
            body.extend_from_slice(&part);
        }
        self.store
            .put_opts(
                &self.location,
                body.into(),
                PutOptions {
                    attributes: self.attributes.clone(),
                    ..PutOptions::default()
                },
            )
            .await
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        self.parts.lock().unwrap().clear();
        Ok(())
    }
}

fn object_meta(object: &StoredObject) -> object_store::Result<ObjectMeta> {
    meta(
        stored_location(&object.key)?,
        object.size,
        object.etag,
        object.modified_ms,
    )
}

/// The metadata of a row [`LocalStore::scan`] visited.
fn listed_meta(location: Path, row: &rusqlite::Row<'_>) -> object_store::Result<ObjectMeta> {
    meta(
        location,
        row.get(1).map_err(db_error)?,
        row.get(2).map_err(db_error)?,
        row.get(3).map_err(db_error)?,
    )
}

fn meta(
    location: Path,
    size: u64,
    etag: i64,
    modified_ms: i64,
) -> object_store::Result<ObjectMeta> {
    let modified = SystemTime::UNIX_EPOCH
        .checked_add(Duration::from_millis(modified_ms.max(0) as u64))
        .ok_or_else(|| message_error("the object timestamp is outside the system clock range"))?;
    Ok(ObjectMeta {
        location,
        last_modified: modified.into(),
        size,
        e_tag: Some(etag.to_string()),
        version: None,
    })
}

/// The query behind [`LocalStore::scan`]. Every variant is a range on the
/// key's index, which SQLite seeks only when each bound is a plain
/// comparison, so an absent bound is left out rather than bound to NULL.
fn scan_sql(inclusive: bool, bounded: bool) -> String {
    format!(
        "SELECT key, length(body), etag, modified_ms FROM objects
         WHERE key {} :lower{} ORDER BY key",
        if inclusive { ">=" } else { ">" },
        if bounded { " AND key < :upper" } else { "" },
    )
}

/// The key prefix of the objects below `prefix`. The trailing delimiter
/// keeps `a/bc` out of a listing of `a/b`, and `a/b` itself.
fn key_prefix(prefix: &Path) -> String {
    match prefix.as_ref() {
        "" => String::new(),
        prefix => format!("{prefix}{}", DELIMITER),
    }
}

/// The exclusive upper bound of the keys that start with `prefix`: the
/// prefix with its last character advanced, or `None` when nothing bounds
/// them (an empty prefix, or one made only of `char::MAX`). SQLite compares
/// keys bytewise, and UTF-8 orders bytes as it orders code points.
fn prefix_successor(prefix: &str) -> Option<String> {
    let mut successor = prefix.to_string();
    while let Some(last) = successor.pop() {
        if let Some(next) = (last as u32 + 1..=char::MAX as u32).find_map(char::from_u32) {
            successor.push(next);
            return Some(successor);
        }
    }
    None
}

fn encode_attributes(attributes: &Attributes) -> object_store::Result<String> {
    let mut stored = Vec::with_capacity(attributes.len());
    for (attribute, value) in attributes {
        let (kind, name) = match attribute {
            Attribute::ContentDisposition => ("content-disposition", None),
            Attribute::ContentEncoding => ("content-encoding", None),
            Attribute::ContentLanguage => ("content-language", None),
            Attribute::ContentType => ("content-type", None),
            Attribute::CacheControl => ("cache-control", None),
            Attribute::StorageClass => ("storage-class", None),
            Attribute::Metadata(name) => ("metadata", Some(name.as_ref().to_string())),
            _ => {
                return Err(message_error(
                    "the development store does not support this attribute",
                ))
            }
        };
        stored.push(StoredAttribute {
            kind: kind.to_string(),
            name,
            value: value.as_ref().to_string(),
        });
    }
    serde_json::to_string(&stored).map_err(db_error)
}

fn decode_attributes(encoded: &str) -> object_store::Result<Attributes> {
    let stored: Vec<StoredAttribute> = serde_json::from_str(encoded).map_err(db_error)?;
    let mut attributes = Attributes::with_capacity(stored.len());
    for stored in stored {
        let attribute = match stored.kind.as_str() {
            "content-disposition" => Attribute::ContentDisposition,
            "content-encoding" => Attribute::ContentEncoding,
            "content-language" => Attribute::ContentLanguage,
            "content-type" => Attribute::ContentType,
            "cache-control" => Attribute::CacheControl,
            "storage-class" => Attribute::StorageClass,
            "metadata" => Attribute::Metadata(stored.name.unwrap_or_default().into()),
            kind => return Err(message_error(format!("unknown stored attribute {kind:?}"))),
        };
        attributes.insert(attribute, AttributeValue::from(stored.value));
    }
    Ok(attributes)
}

#[cfg(all(test, celld_internal_tests))]
mod internal_tests {
    include!(env!("CELLD_INTERNAL_LOCAL_STORE_TESTS"));
}

/// The [`Path`] a stored key came from. Every key is stored as
/// `location.to_string()`, which is already percent-encoded, so this parses
/// that form back. `Path::from` would encode it a second time, and a listed
/// `přehled.html` would come back as `p%25C5%2599ehled.html`, a key that
/// names no object (denoland/celld#232).
fn stored_location(key: &str) -> object_store::Result<Path> {
    Ok(Path::parse(key)?)
}

fn db_error(error: impl fmt::Display) -> Error {
    message_error(error.to_string())
}

fn message_error(message: impl Into<String>) -> Error {
    Error::Generic {
        store: STORE,
        source: Box::new(std::io::Error::other(message.into())),
    }
}

fn not_found(path: &str) -> Error {
    Error::NotFound {
        path: path.to_string(),
        source: Box::new(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "the object does not exist",
        )),
    }
}

fn already_exists(path: &str) -> Error {
    Error::AlreadyExists {
        path: path.to_string(),
        source: Box::new(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "the object already exists",
        )),
    }
}

fn precondition(path: &str) -> Error {
    Error::Precondition {
        path: path.to_string(),
        source: Box::new(std::io::Error::other("the ETag does not match")),
    }
}

#[cfg(test)]
mod tests;
