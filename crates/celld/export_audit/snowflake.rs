//! The statements a Snowflake [`super::ConsumerView`] runs, against the
//! tables and views in `crates/export-snowflake/sql` (`EXPORT_TOMBSTONES`,
//! `EXPORT_RECONCILER_FINDINGS`, `CELL_STREAMS`, `CELL_CERTIFIED`,
//! `CELL_SNAPSHOTS`, `CELL_CHANGES_CURRENT`, `CELL_META_CURRENT`).
//!
//! The loader owns the connection; this module owns what the audit asks
//! and writes, so both sides keep one shape. Binds are positional (`?`), in
//! the order the matching `*_binds` function returns them. A root stream's
//! `FACET` is the empty string, as the loader's routing stores it.
//!
//! [`SnowflakeConsumer`] is the [`ConsumerView`] that runs them, so
//! `celld export reconcile | verify | erase --consumer snowflake` audit the
//! tables a blob-stream or Kafka fleet's loader fills.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{anyhow, Context as _};
use async_trait::async_trait;
use celld_export_format::{Consumer, Position, Record, StreamId, StreamState};
use celld_export_snowflake::consume::{Batch, Land, Limits};
use celld_export_snowflake::loader::Erasure;
use celld_export_snowflake::{Loader, Rows, Warehouse};
use serde_json::{json, Map, Value as Json};

use super::{ConsumerView, Finding, RecoveredSession, StreamSummary, Tombstone};

/// Every stream the consumer still holds, with its `deleted` position.
pub const SELECT_STREAMS: &str = "\
SELECT script, class, cell, facet, incarnation, deleted_at
FROM CELL_STREAMS
WHERE NOT removed";

/// Per stream and epoch, the certified position.
pub const SELECT_CERTIFIED: &str = "\
SELECT script, class, cell, facet, incarnation, epoch, txid, commit
FROM CELL_CERTIFIED";

/// Per stream, the winning stream-wide snapshot.
pub const SELECT_STREAM_SNAPSHOTS: &str = "\
SELECT script, class, cell, facet, incarnation, epoch, txid, commit
FROM CELL_SNAPSHOTS
WHERE scope = 'stream'";

/// Per stream and epoch, the nodes that produced live records, and the
/// newest commit time of any record but `recovered` (which recovery sends
/// on a stream of its own).
pub const SELECT_ACTIVITY: &str = "\
SELECT script, class, cell, facet, incarnation, epoch,
       ARRAY_AGG(DISTINCT IFF(origin = 'live', node, NULL)) AS nodes,
       MAX(DATE_PART(epoch_millisecond, committed_at)) AS last_committed_ms
FROM (
    SELECT script, class, cell, facet, incarnation, epoch, origin, node, committed_at
    FROM CELL_META_CURRENT
    WHERE kind <> 'recovered'
    UNION ALL
    SELECT script, class, cell, facet, incarnation, epoch, origin, node, committed_at
    FROM CELL_CHANGES_CURRENT
)
GROUP BY script, class, cell, facet, incarnation, epoch";

/// Per dead session, the `recovered` records held.
pub const SELECT_RECOVERED: &str = "\
SELECT body:session::STRING AS session,
       MAX(body:cells::NUMBER(20, 0)) AS expected,
       COUNT(DISTINCT cell || ':' || body:head:epoch::STRING) AS held,
       BOOLOR_AGG(COALESCE(body:loss::BOOLEAN, FALSE)) AS loss
FROM CELL_META_CURRENT
WHERE kind = 'recovered'
GROUP BY 1";

/// One stream's `rows` and `snapshot` records at or below a position key,
/// for [`super::ConsumerView::state_at`]: the loader feeds them to the
/// reference consumer. Binds: script, class, cell, facet, incarnation,
/// position key.
pub const SELECT_CHANGES_AT: &str = "\
SELECT *
FROM CELL_CHANGES_CURRENT
WHERE script = ? AND class = ? AND cell = ? AND facet = ? AND incarnation = ?
  AND position_key <= ?";

/// Every `rows` and `snapshot` record fragment of one cell at or below a
/// position key, as stored, for [`super::ConsumerView::state_at`]: the
/// reference consumer reassembles, deduplicates and applies them as it does
/// the bucket sink's records. Binds: class, cell, position key.
pub const SELECT_CELL_CHANGES_AT: &str = "\
SELECT kind, script, class, cell, cell_name, facet, incarnation,
       epoch, txid, commit, DATE_PART(epoch_millisecond, committed_at) AS committed_at_ms,
       node, origin, fragment, fragments,
       snapshot_id, table_name, generation, columns, key_columns, row_changes
FROM CELL_CHANGES c
WHERE class = ? AND cell = ? AND position_key <= ?
  AND NOT EXISTS (SELECT 1 FROM EXPORT_TOMBSTONES t
      WHERE t.cleared_at IS NULL AND t.script = c.script AND t.class = c.class
        AND t.cell = c.cell AND t.facet = c.facet
        AND (t.incarnation IS NULL OR t.incarnation = c.incarnation))";

/// The same for every other record of the cell. Binds: class, cell,
/// position key.
pub const SELECT_CELL_META_AT: &str = "\
SELECT kind, script, class, cell, cell_name, facet, incarnation,
       epoch, txid, commit, DATE_PART(epoch_millisecond, committed_at) AS committed_at_ms,
       node, origin, fragment, fragments, body
FROM CELL_META m
WHERE class = ? AND cell = ? AND position_key <= ?
  AND NOT EXISTS (SELECT 1 FROM EXPORT_TOMBSTONES t
      WHERE t.cleared_at IS NULL AND t.script = m.script AND t.class = m.class
        AND t.cell = m.cell AND t.facet = m.facet
        AND (t.incarnation IS NULL OR t.incarnation = m.incarnation))";

/// Close the findings earlier runs recorded and this run did not: each run
/// replaces the open set, so a repaired stream leaves `EXPORT_GAPS`. Binds:
/// the run.
pub const RESOLVE_FINDINGS: &str = "\
UPDATE EXPORT_RECONCILER_FINDINGS
SET resolved_at = CURRENT_TIMESTAMP()
WHERE resolved_at IS NULL
  AND (detail:run::STRING IS NULL OR detail:run::STRING <> ?)";

pub const INSERT_FINDING: &str = "\
INSERT INTO EXPORT_RECONCILER_FINDINGS
    (script, class, cell, facet, incarnation, finding, head_epoch, head_txid, detail, found_at)
SELECT ?, ?, ?, ?, ?, ?, ?, ?, PARSE_JSON(?), CURRENT_TIMESTAMP()";

pub const INSERT_TOMBSTONE: &str = "\
INSERT INTO EXPORT_TOMBSTONES
    (script, class, cell, facet, incarnation, erased_at, reason)
SELECT ?, ?, ?, ?, ?, TO_TIMESTAMP_LTZ(?, 3), ?";

/// Clearing matches the tombstone exactly, a NULL incarnation included.
pub const CLEAR_TOMBSTONE: &str = "\
UPDATE EXPORT_TOMBSTONES
SET cleared_at = TO_TIMESTAMP_LTZ(?, 3)
WHERE script = ? AND class = ? AND cell = ? AND facet = ?
  AND EQUAL_NULL(incarnation, ?)
  AND cleared_at IS NULL";

/// The position key the loader's views compare positions by.
pub fn position_key(epoch: u64, txid: u64, commit: u64) -> String {
    format!("{epoch:020}.{txid:020}.{commit:020}")
}

/// `run` names the reconciler run that found it, for [`RESOLVE_FINDINGS`].
pub fn finding_binds(finding: &Finding, run: &str) -> Vec<Json> {
    let s = &finding.stream;
    vec![
        json!(s.script),
        json!(s.class),
        json!(s.cell),
        json!(s.facet.clone().unwrap_or_default()),
        json!(s.incarnation),
        json!(finding.kind.as_str()),
        json!(finding.head.map(|h| h.epoch)),
        json!(finding.head.map(|h| h.txid)),
        json!(serde_json::to_string(&json!({
            "scope": finding.scope,
            "from": finding.from,
            "certified": finding.certified,
            "epochs": finding.epochs,
            "detail": finding.detail,
            "run": run,
        }))
        .expect("findings encode")),
    ]
}

pub fn tombstone_binds(t: &Tombstone) -> Vec<Json> {
    vec![
        json!(t.script),
        json!(t.class),
        json!(t.cell),
        json!(t.facet.clone().unwrap_or_default()),
        json!(t.incarnation),
        json!(t.erased_at_ms),
        json!(t.reason),
    ]
}

pub fn clear_binds(t: &Tombstone) -> Vec<Json> {
    vec![
        json!(t.cleared_at_ms),
        json!(t.script),
        json!(t.class),
        json!(t.cell),
        json!(t.facet.clone().unwrap_or_default()),
        json!(t.incarnation),
    ]
}

/// The loader's tables as a [`ConsumerView`]: the reference consumer's
/// state as the views derive it, and its writes as the loader makes them.
/// `W` runs statements; `L` lands the reconciler's own records through
/// Snowpipe Streaming, as `celld-export-loader ingest` does.
pub struct SnowflakeConsumer<W, L> {
    inner: Arc<Mutex<Inner<W, L>>>,
    only_cell: Option<String>,
}

struct Inner<W, L> {
    loader: Loader<W>,
    land: L,
    limits: Limits,
    /// How long landed records may take to become queryable.
    visible_timeout: Duration,
}

impl<W, L> SnowflakeConsumer<W, L>
where
    W: Warehouse + Send + 'static,
    L: Land + Send + 'static,
{
    pub fn new(loader: Loader<W>, land: L, limits: Limits, visible_timeout: Duration) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                loader,
                land,
                limits,
                visible_timeout,
            })),
            only_cell: None,
        }
    }

    /// Only this cell's streams, for `verify --cell` and `erase`.
    pub fn only_cell(mut self, cell: Option<String>) -> Self {
        self.only_cell = cell;
        self
    }

    /// Run `f` on the connection off the async threads: the SQL API is
    /// blocking HTTP.
    async fn with<T, F>(&self, f: F) -> anyhow::Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut Inner<W, L>) -> anyhow::Result<T> + Send + 'static,
    {
        let inner = self.inner.clone();
        crate::asyncrt::blocking(move || {
            let mut inner = inner
                .lock()
                .map_err(|_| anyhow!("the Snowflake connection panicked"))?;
            f(&mut inner)
        })
        .await
        .map_err(|e| anyhow!("Snowflake statement: {e}"))?
    }

    /// A statement over every stream, narrowed to [`Self::only_cell`].
    fn scoped(&self, sql: &str) -> (String, Vec<Json>) {
        match &self.only_cell {
            Some(cell) => (
                format!("SELECT * FROM ({sql}) WHERE cell = ?"),
                vec![json!(cell)],
            ),
            None => (sql.to_string(), Vec::new()),
        }
    }
}

fn query<W: Warehouse>(loader: &mut Loader<W>, sql: &str, binds: &[Json]) -> anyhow::Result<Rows> {
    loader
        .query(sql, binds)
        .map_err(|e| anyhow!("{e}"))
        .with_context(|| format!("run {}", sql.lines().next().unwrap_or(sql)))
}

fn text<'a>(rows: &'a Rows, row: usize, column: &str) -> anyhow::Result<&'a str> {
    rows.get(row, column)
        .ok_or_else(|| anyhow!("row {row} has no {column}"))
}

fn number<T: std::str::FromStr>(rows: &Rows, row: usize, column: &str) -> anyhow::Result<T> {
    let value = text(rows, row, column)?;
    value
        .parse()
        .map_err(|_| anyhow!("{column} is {value:?}, not a whole number"))
}

/// Milliseconds as the SQL API renders `DATE_PART(epoch_millisecond, ..)`:
/// a whole number, possibly with a fraction.
fn millis(rows: &Rows, row: usize, column: &str) -> anyhow::Result<i64> {
    let value = text(rows, row, column)?;
    value
        .parse::<i64>()
        .or_else(|_| value.parse::<f64>().map(|f| f as i64))
        .map_err(|_| anyhow!("{column} is {value:?}, not milliseconds"))
}

/// The stream a row names in its five stream columns.
fn stream_of(rows: &Rows, row: usize) -> anyhow::Result<StreamId> {
    Ok(StreamId {
        script: text(rows, row, "script")?.to_string(),
        class: text(rows, row, "class")?.to_string(),
        cell: text(rows, row, "cell")?.to_string(),
        facet: rows
            .get(row, "facet")
            .filter(|f| !f.is_empty())
            .map(str::to_string),
        incarnation: number(rows, row, "incarnation")?,
    })
}

fn position(rows: &Rows, row: usize) -> anyhow::Result<Position> {
    Ok(Position::new(
        number(rows, row, "epoch")?,
        number(rows, row, "txid")?,
        number(rows, row, "commit")?,
    ))
}

/// A `position_key` back as a position.
fn parse_position_key(key: &str) -> anyhow::Result<Position> {
    let parts: Vec<u64> = key
        .split('.')
        .map(str::parse)
        .collect::<Result<_, _>>()
        .map_err(|_| anyhow!("{key:?} is not a position key"))?;
    match parts[..] {
        [epoch, txid, commit] => Ok(Position::new(epoch, txid, commit)),
        _ => Err(anyhow!("{key:?} is not a position key")),
    }
}

/// A column holding JSON, as the SQL API renders an ARRAY or VARIANT.
fn json_column(rows: &Rows, row: usize, column: &str) -> anyhow::Result<Json> {
    serde_json::from_str(text(rows, row, column)?)
        .with_context(|| format!("{column} of row {row} is not JSON"))
}

/// The summaries of [`ConsumerView::streams`] from its four statements.
pub fn summaries(
    streams: &Rows,
    certified: &Rows,
    snapshots: &Rows,
    activity: &Rows,
) -> anyhow::Result<Vec<StreamSummary>> {
    let mut out: BTreeMap<StreamId, StreamSummary> = BTreeMap::new();
    for row in 0..streams.len() {
        let id = stream_of(streams, row)?;
        let mut summary = StreamSummary::new(id.clone());
        summary.deleted_at = streams
            .get(row, "deleted_at")
            .map(parse_position_key)
            .transpose()?;
        out.insert(id, summary);
    }
    for row in 0..certified.len() {
        if let Some(s) = out.get_mut(&stream_of(certified, row)?) {
            let at = position(certified, row)?;
            s.certified.insert(at.epoch, at);
        }
    }
    for row in 0..snapshots.len() {
        if let Some(s) = out.get_mut(&stream_of(snapshots, row)?) {
            let at = position(snapshots, row)?;
            s.snapshot_at = Some(s.snapshot_at.map_or(at, |had| had.max(at)));
        }
    }
    for row in 0..activity.len() {
        if let Some(s) = out.get_mut(&stream_of(activity, row)?) {
            let epoch: u64 = number(activity, row, "epoch")?;
            let nodes: BTreeSet<String> =
                serde_json::from_value(json_column(activity, row, "nodes")?)
                    .context("nodes is not an array of names")?;
            if !nodes.is_empty() {
                s.nodes.entry(epoch).or_default().extend(nodes);
            }
            if activity.get(row, "last_committed_ms").is_some() {
                s.last_committed_ms =
                    s.last_committed_ms
                        .max(millis(activity, row, "last_committed_ms")?);
            }
        }
    }
    Ok(out.into_values().collect())
}

/// One row of [`SELECT_CELL_CHANGES_AT`] or [`SELECT_CELL_META_AT`] back as
/// the record (or fragment) it was routed from.
pub fn record_of(rows: &Rows, row: usize) -> anyhow::Result<Record> {
    let mut fields = Map::new();
    if rows.columns.iter().any(|c| c.eq_ignore_ascii_case("body")) {
        match json_column(rows, row, "body")? {
            Json::Object(body) => fields.extend(body),
            other => return Err(anyhow!("body of row {row} is {other}, not an object")),
        }
    } else {
        if let Some(id) = rows.get(row, "snapshot_id") {
            fields.insert("snapshot_id".into(), json!(id));
        }
        fields.insert("table".into(), json!(text(rows, row, "table_name")?));
        fields.insert(
            "generation".into(),
            json!(number::<u64>(rows, row, "generation")?),
        );
        fields.insert("columns".into(), json_column(rows, row, "columns")?);
        fields.insert("key_columns".into(), json_column(rows, row, "key_columns")?);
        fields.insert("rows".into(), json_column(rows, row, "row_changes")?);
    }
    let stream = stream_of(rows, row)?;
    let at = position(rows, row)?;
    fields.extend([
        ("kind".into(), json!(text(rows, row, "kind")?)),
        ("script".into(), json!(stream.script)),
        ("class".into(), json!(stream.class)),
        ("cell".into(), json!(stream.cell)),
        ("cell_name".into(), json!(rows.get(row, "cell_name"))),
        ("facet".into(), json!(stream.facet)),
        ("incarnation".into(), json!(stream.incarnation)),
        ("epoch".into(), json!(at.epoch)),
        ("txid".into(), json!(at.txid)),
        ("commit".into(), json!(at.commit)),
        (
            "committed_at".into(),
            json!(millis(rows, row, "committed_at_ms")?),
        ),
        ("node".into(), json!(text(rows, row, "node")?)),
        ("origin".into(), json!(text(rows, row, "origin")?)),
        (
            "fragment".into(),
            json!(number::<u32>(rows, row, "fragment")?),
        ),
        (
            "fragments".into(),
            json!(number::<u32>(rows, row, "fragments")?),
        ),
    ]);
    Record::from_json(&serde_json::to_vec(&Json::Object(fields))?)
        .map_err(|e| anyhow!("row {row} is not a record: {e}"))
}

#[async_trait]
impl<W, L> ConsumerView for SnowflakeConsumer<W, L>
where
    W: Warehouse + Send + 'static,
    L: Land + Send + 'static,
{
    async fn streams(&self) -> anyhow::Result<Vec<StreamSummary>> {
        let statements: Vec<(String, Vec<Json>)> = [
            SELECT_STREAMS,
            SELECT_CERTIFIED,
            SELECT_STREAM_SNAPSHOTS,
            SELECT_ACTIVITY,
        ]
        .into_iter()
        .map(|sql| self.scoped(sql))
        .collect();
        let rows = self
            .with(move |inner| {
                statements
                    .iter()
                    .map(|(sql, binds)| query(&mut inner.loader, sql, binds))
                    .collect::<anyhow::Result<Vec<Rows>>>()
            })
            .await?;
        summaries(&rows[0], &rows[1], &rows[2], &rows[3])
    }

    async fn state_at(
        &self,
        stream: &StreamId,
        at: Position,
    ) -> anyhow::Result<Option<StreamState>> {
        let binds = vec![
            json!(stream.class),
            json!(stream.cell),
            json!(position_key(at.epoch, at.txid, at.commit)),
        ];
        let (changes, meta) = self
            .with(move |inner| {
                Ok((
                    query(&mut inner.loader, SELECT_CELL_CHANGES_AT, &binds)?,
                    query(&mut inner.loader, SELECT_CELL_META_AT, &binds)?,
                ))
            })
            .await?;
        let mut consumer = Consumer::new();
        for rows in [&changes, &meta] {
            for row in 0..rows.len() {
                consumer
                    .ingest(record_of(rows, row)?)
                    .map_err(|e| anyhow!("reassemble export records: {e}"))?;
            }
        }
        Ok(consumer.stream(stream))
    }

    async fn recovered(&self) -> anyhow::Result<Vec<RecoveredSession>> {
        let rows = self
            .with(|inner| query(&mut inner.loader, SELECT_RECOVERED, &[]))
            .await?;
        (0..rows.len())
            .map(|row| {
                Ok(RecoveredSession {
                    session: text(&rows, row, "session")?.to_string(),
                    expected: number(&rows, row, "expected")?,
                    held: number(&rows, row, "held")?,
                    loss: text(&rows, row, "loss")?.eq_ignore_ascii_case("true"),
                })
            })
            .collect()
    }

    async fn record_findings(&self, findings: &[Finding]) -> anyhow::Result<()> {
        let run = crate::asyncrt::wall_ms().to_string();
        let inserts: Vec<Vec<Json>> = findings.iter().map(|f| finding_binds(f, &run)).collect();
        self.with(move |inner| {
            for binds in &inserts {
                query(&mut inner.loader, INSERT_FINDING, binds)?;
            }
            // Only once this run's findings are in, so EXPORT_GAPS never
            // loses one that is still open.
            query(&mut inner.loader, RESOLVE_FINDINGS, &[json!(run)])?;
            Ok(())
        })
        .await
    }

    async fn tombstone(&self, tombstone: &Tombstone) -> anyhow::Result<()> {
        let t = tombstone.clone();
        self.with(move |inner| {
            if t.cleared_at_ms.is_some() {
                query(&mut inner.loader, CLEAR_TOMBSTONE, &clear_binds(&t))?;
                return Ok(());
            }
            // As `celld-export-loader erase`: tombstone unless an open one
            // matches, and delete the stream's rows now.
            inner
                .loader
                .erase(&Erasure {
                    script: t.script.clone(),
                    class: t.class.clone(),
                    cell: t.cell.clone(),
                    facet: t.facet.clone(),
                    incarnation: t.incarnation,
                    reason: t.reason.clone(),
                })
                .map_err(|e| anyhow!("erase {} in Snowflake: {e}", t.cell))
        })
        .await
    }

    async fn deliver(&self, records: Vec<Record>) -> anyhow::Result<Option<String>> {
        if records.is_empty() {
            return Ok(None);
        }
        // No `%` or `_`: `visible` matches the tag with LIKE.
        let tag = format!(" (reconciler {})", crate::asyncrt::wall_ms());
        self.with(move |inner| {
            let mut batch = Batch::tagged(&tag);
            let landed = records.len() as u64;
            for record in &records {
                batch.push(record, super::AUDIT_NODE);
                if batch.is_full(&inner.limits) {
                    batch.land(&mut inner.land).map_err(|e| anyhow!("{e}"))?;
                }
            }
            batch.land(&mut inner.land).map_err(|e| anyhow!("{e}"))?;
            let deadline = std::time::Instant::now() + inner.visible_timeout;
            let mut pause = Duration::from_millis(100);
            let visible = inner
                .loader
                .settle(&tag, landed, || {
                    if std::time::Instant::now() >= deadline {
                        return false;
                    }
                    std::thread::sleep(pause);
                    pause = (pause * 2).min(Duration::from_secs(2));
                    true
                })
                .map_err(|e| anyhow!("{e}"))?;
            Ok(Some(if visible < landed {
                format!(
                    "Snowflake: landed {landed} record(s), {visible} routed so far; \
                     the route task routes the rest"
                )
            } else {
                format!("Snowflake: landed and routed {landed} record(s)")
            }))
        })
        .await
    }
}

/// The consumer `--consumer snowflake` audits: the loader's tables, with the
/// loader's settings (`SNOWFLAKE_*`, `EXPORT_BATCH_*`,
/// `EXPORT_VISIBLE_SECONDS`).
#[cfg(feature = "export-snowflake")]
pub fn from_env(
    only_cell: Option<String>,
) -> anyhow::Result<
    SnowflakeConsumer<
        celld_export_snowflake::sql_api::SqlApi,
        celld_export_snowflake::streaming::Streaming,
    >,
> {
    use celld_export_snowflake::settings;
    let e = |e: settings::Error| anyhow!("Snowflake settings: {e}");
    Ok(SnowflakeConsumer::new(
        settings::loader().map_err(e)?,
        settings::streaming().map_err(e)?,
        settings::limits().map_err(e)?,
        settings::visible_timeout().map_err(e)?,
    )
    .only_cell(only_cell))
}
