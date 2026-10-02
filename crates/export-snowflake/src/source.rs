// The loader is a host tool outside celld's execution boundary: its clock,
// timers and tasks are the host's.
#![allow(clippy::disallowed_methods)]

//! Feeding the loader from the export topic, whichever transport carries it
//! (`blob-stream` or `kafka` feature).
//!
//! The loader is a member of a consumer group, `snowflake` by default, over
//! the topic the nodes' sink writes: blob-stream ([`crate::blob_stream`]) or
//! Kafka ([`crate::kafka`]). Each is a [`Source`]; [`run`] is the same loop
//! over either. It reads records into a [`Batch`] and, once the batch is
//! full or has waited `linger`, hands it to a [`Pipeline`] that lands it in
//! `EXPORT_LANDING` (through Snowpipe Streaming, [`crate::streaming`]) while
//! the loop reads on. Up to `concurrency` batches land at once; only once
//! every row of a batch and of every batch before it is acknowledged does
//! the loop store and commit the offsets the batch covered. A crash or a
//! lost lease therefore replays at most the batches that had not landed,
//! and a replayed record is a duplicate every reader drops. The route task
//! moves landed records into the tables on its schedule, and the same loop
//! keeps the Dynamic Tables in step with the schemas it has seen.
//!
//! An append that fails is sent again, with backoff, until it lands; while
//! `concurrency` batches are in the pipeline, nothing more is read. A
//! message that is not a record stops the loop with an error, once what
//! came before it has landed and been committed, so it is read again when
//! the loader restarts: a newer loader may decode it, and an operator can
//! list it in `skip` to drop it. When the group revokes partitions, the
//! loader lands what it holds before letting them go, so the next owner
//! starts where it stopped.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::consume::{message_source, Batch, Land, Limits, Undecodable};
use crate::loader::{LoadError, Loader, SyncReport, Warehouse};
use crate::pipeline::{Pipeline, Progress, DEFAULT_CONCURRENCY};

/// The consumer group the loader joins unless told otherwise.
pub const DEFAULT_GROUP: &str = "snowflake";

/// A consumer-group member reading the export topic. Partitions and
/// offsets are the transport's own; a message's source is
/// `<name>/<partition>/<offset>`.
pub trait Source {
    /// What the partitions were revoked with.
    type Revoked: Revoked;

    /// The transport's name, as sources and `EXPORT_SKIP` spell it:
    /// `blob-stream` or `kafka`.
    fn name(&self) -> &'static str;

    /// Join the group.
    fn start(&mut self) -> anyhow::Result<()>;

    /// The next message, or partitions the group is taking away. An error
    /// stops the loop; [`Next::Failed`] does not.
    fn next(&mut self) -> impl Future<Output = anyhow::Result<Next<Self::Revoked>>>;

    /// Record that every message up to and including `offset` in
    /// `partition` is done with, for the next [`Source::commit`].
    fn store_offset(&mut self, partition: u32, offset: u64) -> anyhow::Result<()>;

    /// Commit what was stored. Returns the partitions the group committed
    /// without this member: another member owns them now.
    fn commit(&mut self) -> impl Future<Output = anyhow::Result<Vec<u32>>>;

    /// Leave the group, committing what was stored and giving up its
    /// partitions.
    fn shutdown(self) -> impl Future<Output = anyhow::Result<()>>;
}

/// Partitions the group is taking away. They are let go once
/// [`Revoked::complete`] returns.
pub trait Revoked {
    fn partitions(&self) -> Vec<u32>;
    fn complete(self) -> impl Future<Output = ()>;
}

/// What [`Source::next`] read.
pub enum Next<R> {
    Record {
        partition: u32,
        offset: u64,
        payload: Vec<u8>,
    },
    Revoked(R),
    /// A read error the source recovers from on its own; the loop reports
    /// it and reads on.
    Failed(anyhow::Error),
}

/// How the loop batches and how often it syncs the Dynamic Tables.
#[derive(Clone, Debug)]
pub struct Settings {
    pub limits: Limits,
    /// The longest a record waits in a batch before the batch lands.
    pub linger: Duration,
    /// Appends in flight at once, and batches landing at once.
    pub concurrency: usize,
    /// How often the Dynamic Tables are synced with the schema union.
    pub sync_every: Duration,
    /// The first wait before sending a failed append again, doubled for
    /// each failure of its batch after, up to `retry_max`.
    pub retry: Duration,
    pub retry_max: Duration,
    /// Messages to drop, by source (`<transport>/<partition>/<offset>`):
    /// ones an operator has looked at and decided are not records.
    pub skip: BTreeSet<String>,
}

impl Default for Settings {
    fn default() -> Self {
        Settings {
            limits: Limits::default(),
            linger: Duration::from_secs(5),
            concurrency: DEFAULT_CONCURRENCY,
            sync_every: Duration::from_secs(60),
            retry: Duration::from_secs(1),
            retry_max: Duration::from_secs(60),
            skip: BTreeSet::new(),
        }
    }
}

/// What the loop did, for the caller to log.
#[derive(Debug)]
pub enum Event<'a> {
    /// Batches landed, every one read before them too, and their offsets
    /// were committed.
    Landed {
        batches: usize,
        records: usize,
        offsets: &'a BTreeMap<u32, u64>,
    },
    /// A message listed in `skip`; its offset is committed with the
    /// batch's.
    Skipped(&'a str),
    /// An append failed; it is sent again after `retry`. When stopping
    /// it is not (`retry` is zero), and its batch is read again.
    LandFailed {
        error: &'a LoadError,
        retry: Duration,
    },
    /// Storing or committing offsets failed. The records have landed, so at
    /// worst another member reads them again.
    CommitFailed(&'a anyhow::Error),
    /// Partitions the group committed without this member: another member
    /// owns them now and may read their last batch again.
    Fenced(&'a [u32]),
    /// The group took partitions away; what the loader read before landed
    /// first.
    Revoked(&'a [u32]),
    /// Reading failed in a way the source recovers from on its own, such as
    /// a broker that is briefly unreachable.
    ReadFailed(&'a anyhow::Error),
    Synced(&'a SyncReport),
    SyncFailed(&'a LoadError),
}

/// Consume until `stop` is cancelled or the consumer fails: land batches
/// through `lander`, commit their offsets, and sync the Dynamic Tables
/// through `loader` every `sync_every`.
/// On stop, what has been read lands (each append tried once more at most),
/// and the source shuts down, committing and giving up its partitions.
///
/// Must run on a multi-threaded Tokio runtime: the Dynamic Table sync
/// blocks, and runs in place on this task's thread.
pub async fn run<S, W, L>(
    mut source: S,
    loader: &mut Loader<W>,
    lander: Arc<L>,
    settings: &Settings,
    stop: CancellationToken,
    mut report: impl FnMut(Event<'_>),
) -> anyhow::Result<()>
where
    S: Source,
    W: Warehouse,
    L: Land + Send + Sync + 'static,
    L::Append: Send + 'static,
{
    source.start()?;
    let name = source.name();
    let mut pipeline = Pipeline::new(
        lander,
        settings.concurrency,
        settings.retry,
        settings.retry_max,
    );
    let mut batch = Batch::default();
    // When the batch's oldest record must land.
    let mut due: Option<Instant> = None;
    let mut next_sync = Instant::now() + settings.sync_every;
    let result = loop {
        // Hand the batch over once it is full or due and there is room. A
        // full batch waiting for room stops the reading.
        let full = batch.is_full(&settings.limits);
        if !batch.is_empty()
            && pipeline.has_room()
            && (full || due.is_some_and(|d| d <= Instant::now()))
        {
            let (rows, offsets) = batch.take();
            pipeline.submit(rows, offsets);
            due = None;
        }
        let reading = !batch.is_full(&settings.limits);
        let wake = match due {
            Some(d) if pipeline.has_room() => d.min(next_sync),
            _ => next_sync,
        };
        tokio::select! {
            biased;
            () = stop.cancelled() => break Ok(()),
            Some(progress) = pipeline.next(), if !pipeline.is_empty() => {
                handle(&mut source, progress, &mut report).await;
            }
            () = tokio::time::sleep_until(wake) => {
                if next_sync <= Instant::now() {
                    match tokio::task::block_in_place(|| loader.sync_dynamic_tables()) {
                        Ok(r) => report(Event::Synced(&r)),
                        Err(e) => report(Event::SyncFailed(&e)),
                    }
                    next_sync = Instant::now() + settings.sync_every;
                }
            }
            next = source.next(), if reading => match next {
                Err(e) => break Err(e.context("read the export topic")),
                Ok(Next::Record { partition, offset, payload }) => {
                    if due.is_none() {
                        due = Some(Instant::now() + settings.linger);
                    }
                    let from = message_source(name, partition, offset);
                    if settings.skip.contains(&from) {
                        batch.skip(partition, offset);
                        report(Event::Skipped(&from));
                    } else if let Err(u) = batch.push_message(name, partition, offset, &payload) {
                        // Commit up to it, never past it.
                        if !land_all(&mut source, &mut pipeline, &mut batch, &stop, &mut report).await {
                            break Ok(());
                        }
                        break Err(not_a_record(&u));
                    }
                }
                Ok(Next::Revoked(revoked)) => {
                    // Land before letting go, or the next owner reads it
                    // again. A stop first lets go without landing; what was
                    // read from them is then read again, which is harmless,
                    // and its offsets are never committed here.
                    let partitions = revoked.partitions();
                    if !land_all(&mut source, &mut pipeline, &mut batch, &stop, &mut report).await {
                        pipeline.forget(&partitions);
                    }
                    due = None;
                    report(Event::Revoked(&partitions));
                    revoked.complete().await;
                }
                Ok(Next::Failed(e)) => report(Event::ReadFailed(&e)),
            },
        }
    };
    // Stopping, or failing: land what has been read, but send nothing
    // twice, so a stop is never held up by a warehouse that is down. What
    // does not land is read again.
    if result.is_ok() && !batch.is_empty() {
        let (rows, offsets) = batch.take();
        pipeline.submit(rows, offsets);
    }
    pipeline.stop_retrying();
    while let Some(progress) = pipeline.next().await {
        handle(&mut source, progress, &mut report).await;
    }
    let shutdown = source.shutdown().await;
    result.and(shutdown.map_err(|e| e.context(format!("shut the {name} consumer down"))))
}

fn not_a_record(u: &Undecodable) -> anyhow::Error {
    anyhow!(
        "{} is not a record ({}). Nothing past it in its partition is committed. \
         If it comes from a newer celld, upgrade the loader; to drop it, add {} to EXPORT_SKIP",
        u.source,
        u.error,
        u.source
    )
}

/// Hand `batch` over and wait until everything in the pipeline has landed
/// and its offsets are committed, retrying until it does. Returns false if
/// `stop` was cancelled first; what has not landed stays in the pipeline.
async fn land_all<S, L>(
    source: &mut S,
    pipeline: &mut Pipeline<L>,
    batch: &mut Batch,
    stop: &CancellationToken,
    report: &mut impl FnMut(Event<'_>),
) -> bool
where
    S: Source,
    L: Land + Send + Sync + 'static,
    L::Append: Send + 'static,
{
    if !batch.is_empty() {
        let (rows, offsets) = batch.take();
        pipeline.submit(rows, offsets);
    }
    loop {
        tokio::select! {
            biased;
            () = stop.cancelled() => return false,
            progress = pipeline.next() => match progress {
                Some(progress) => handle(source, progress, report).await,
                None => return true,
            },
        }
    }
}

/// Commit what landed, or report what failed.
async fn handle<S: Source>(source: &mut S, progress: Progress, report: &mut impl FnMut(Event<'_>)) {
    match progress {
        Progress::Landed {
            batches,
            records,
            offsets,
        } => commit(source, &offsets, batches, records, report).await,
        Progress::Failed { error, retry } => report(Event::LandFailed {
            error: &error,
            retry: retry.unwrap_or_default(),
        }),
    }
}

async fn commit<S: Source>(
    source: &mut S,
    offsets: &BTreeMap<u32, u64>,
    batches: usize,
    records: usize,
    report: &mut impl FnMut(Event<'_>),
) {
    // Batches only of partitions the group took away: nothing to commit.
    if offsets.is_empty() {
        report(Event::Landed {
            batches,
            records,
            offsets,
        });
        return;
    }
    for (&partition, &offset) in offsets {
        if let Err(e) = source.store_offset(partition, offset) {
            report(Event::CommitFailed(&e.context(format!(
                "store offset {offset} of partition {partition}"
            ))));
        }
    }
    match source.commit().await {
        Ok(fenced) => {
            if !fenced.is_empty() {
                report(Event::Fenced(&fenced));
            }
        }
        Err(e) => report(Event::CommitFailed(&e.context("commit offsets"))),
    }
    report(Event::Landed {
        batches,
        records,
        offsets,
    });
}

#[cfg(test)]
mod tests;
