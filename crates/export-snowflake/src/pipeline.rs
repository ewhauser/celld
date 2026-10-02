// The loader is a host tool outside celld's execution boundary: its timers
// and threads are the host's.
#![allow(clippy::disallowed_methods)]

//! Landing several batches at once, a few appends at a time.
//!
//! An append's latency, not the loader's CPU, would bound a loader that sent
//! one append at a time. A [`Pipeline`] takes batches as the loop fills
//! them, encodes each on a blocking thread, and keeps up to `concurrency`
//! appends in flight, oldest batch first, across a batch's appends and
//! across batches, while the loop reads and decodes the next. Snowpipe
//! Streaming's elastic channel takes appends in any order, and rows landing
//! out of order mean nothing to the tables.
//!
//! Offsets are another matter: the pipeline reports a batch as landed only
//! once every append of it and of every batch handed over before it is
//! acknowledged, so the loop commits in order and a restart replays at most
//! what was in flight. A failed append is sent again on its own, after a
//! backoff that doubles with each failure of its batch, until it lands;
//! the batch's other appends and the batches after it carry on meanwhile.
//! At most `concurrency` batches are in the pipeline at once, so a batch
//! that keeps failing stops the loop from reading more.

use std::collections::{BTreeMap, VecDeque};
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::runtime::Handle;
use tokio::sync::mpsc;
use tokio::task::AbortHandle;

use crate::consume::Land;
use crate::loader::{LoadError, WarehouseError};
use crate::LandingRow;

/// Appends in flight, and batches in the pipeline, unless told otherwise.
pub const DEFAULT_CONCURRENCY: usize = 8;

/// What a [`Pipeline`] reports.
#[derive(Debug)]
pub enum Progress {
    /// Batches landed, every one handed over before them too: commit these
    /// offsets, the highest of each partition.
    Landed {
        batches: usize,
        records: usize,
        offsets: BTreeMap<u32, u64>,
    },
    /// An append, or encoding a batch, failed. It is tried again after
    /// `retry`, or, once [`Pipeline::stop_retrying`] was called, never
    /// (`None`), and nothing from its batch on is reported landed.
    Failed {
        error: LoadError,
        retry: Option<Duration>,
    },
}

/// Batches landing, oldest first.
///
/// Work runs on the runtime's blocking threads, and finished work starts
/// what is queued behind it, so appends stay in flight while the caller is
/// busy. The caller learns what finished from [`Pipeline::next`], which
/// reports what landed, in order, and schedules retries.
pub struct Pipeline<L: Land> {
    shared: Arc<Shared<L>>,
    done: mpsc::UnboundedReceiver<Done<L::Append>>,
    concurrency: usize,
    retry: Duration,
    retry_max: Duration,
    retrying: bool,
    flights: VecDeque<Flight>,
    /// The number of the batch at the front of `flights`.
    first: u64,
    /// Failed work waiting out its backoff.
    backoffs: Vec<AbortHandle>,
}

/// What the pipeline and its threads share.
struct Shared<L: Land> {
    lander: Arc<L>,
    runtime: Handle,
    done: mpsc::UnboundedSender<Done<L::Append>>,
    queue: Mutex<Queue<L::Append>>,
}

struct Queue<A> {
    /// Appends ready to send, oldest batch first, and the batch of each.
    ready: VecDeque<(u64, A)>,
    sending: usize,
    /// Appends in flight at most; none once the pipeline is dropped.
    limit: usize,
}

/// A batch in the pipeline.
struct Flight {
    records: usize,
    offsets: BTreeMap<u32, u64>,
    /// How many appends it took, once it is encoded.
    appends: Option<usize>,
    acknowledged: usize,
    /// Work of this batch waiting out a backoff.
    backing_off: usize,
    /// The next backoff.
    wait: Duration,
    /// Failed with retrying stopped: never landed.
    abandoned: bool,
}

impl Flight {
    fn landed(&self) -> bool {
        self.appends == Some(self.acknowledged) && self.backing_off == 0 && !self.abandoned
    }
}

enum Work<A> {
    Encode(Vec<LandingRow>),
    Send(A),
}

/// What happened to a batch's work.
enum Done<A> {
    Encoded(u64, usize),
    Sent(u64),
    Failed(u64, Work<A>, WarehouseError),
    /// Failed work's backoff is over.
    Retry(u64, Work<A>),
    Panicked(Box<dyn std::any::Any + Send>),
}

impl<L> Pipeline<L>
where
    L: Land + Send + Sync + 'static,
    L::Append: Send + 'static,
{
    /// A pipeline landing through `lander` with up to `concurrency` appends
    /// in flight and as many batches in it. A failed append is first sent
    /// again after `retry`, and the wait doubles up to `retry_max`. Must be
    /// made on a Tokio runtime, whose blocking threads it uses.
    pub fn new(lander: Arc<L>, concurrency: usize, retry: Duration, retry_max: Duration) -> Self {
        let concurrency = concurrency.max(1);
        let (done, finished) = mpsc::unbounded_channel();
        Pipeline {
            shared: Arc::new(Shared {
                lander,
                runtime: Handle::current(),
                done,
                queue: Mutex::new(Queue {
                    ready: VecDeque::new(),
                    sending: 0,
                    limit: concurrency,
                }),
            }),
            done: finished,
            concurrency,
            retry,
            retry_max,
            retrying: true,
            flights: VecDeque::new(),
            first: 0,
            backoffs: Vec::new(),
        }
    }

    /// Whether another batch may be handed over.
    pub fn has_room(&self) -> bool {
        self.flights.len() < self.concurrency
    }

    /// Whether no batch is in the pipeline.
    pub fn is_empty(&self) -> bool {
        self.flights.is_empty()
    }

    /// Hand over a batch: its rows and the offsets they cover, as
    /// [`crate::consume::Batch::take`] returns them. A batch with no rows,
    /// only offsets of skipped messages, lands once the batches before it
    /// have. [`Pipeline::has_room`] is advice: the loop may hand over one
    /// more batch to land everything it read.
    pub fn submit(&mut self, rows: Vec<LandingRow>, offsets: BTreeMap<u32, u64>) {
        let batch = self.first + self.flights.len() as u64;
        self.flights.push_back(Flight {
            records: rows.len(),
            offsets,
            appends: rows.is_empty().then_some(0),
            acknowledged: 0,
            backing_off: 0,
            wait: self.retry,
            abandoned: false,
        });
        if !rows.is_empty() {
            // Encoding is not an append: it starts at once.
            self.shared.start(batch, Work::Encode(rows));
        }
    }

    /// Send nothing again from now on: work that fails, and work waiting
    /// out a backoff, is given up, and nothing from its batch on is
    /// reported landed. Work under way or queued is still done, once. For
    /// stopping.
    pub fn stop_retrying(&mut self) {
        self.retrying = false;
        for backoff in self.backoffs.drain(..) {
            backoff.abort();
        }
        for flight in &mut self.flights {
            if flight.backing_off > 0 {
                flight.backing_off = 0;
                flight.abandoned = true;
            }
        }
    }

    /// Never report offsets of `partitions`: the group took them away
    /// before what was read from them landed, and another member reads it
    /// again.
    pub fn forget(&mut self, partitions: &[u32]) {
        for flight in &mut self.flights {
            flight.offsets.retain(|p, _| !partitions.contains(p));
        }
    }

    /// The next thing to report, or `None` once nothing more will be: the
    /// pipeline is empty, or retrying stopped and what is left can never
    /// land. Cancel-safe.
    pub async fn next(&mut self) -> Option<Progress> {
        loop {
            if let Some(landed) = self.take_landed() {
                return Some(landed);
            }
            // Every batch left that has not landed is waiting on work under
            // way, unless it was given up on.
            if self.flights.iter().all(|f| f.abandoned || f.landed()) {
                return None;
            }
            let done = self.done.recv().await.expect("the pipeline holds a sender");
            if let Some(progress) = self.finish(done) {
                return Some(progress);
            }
        }
    }

    fn flight(&mut self, batch: u64) -> &mut Flight {
        let index = usize::try_from(batch - self.first).expect("a batch in the pipeline");
        &mut self.flights[index]
    }

    /// Record what work did; a failure is reported.
    fn finish(&mut self, done: Done<L::Append>) -> Option<Progress> {
        match done {
            Done::Encoded(batch, appends) => self.flight(batch).appends = Some(appends),
            Done::Sent(batch) => self.flight(batch).acknowledged += 1,
            Done::Retry(batch, work) => {
                let flight = self.flight(batch);
                // Aborted too late to stop it sending this: ignore it.
                if !flight.abandoned {
                    flight.backing_off -= 1;
                    self.shared.retry(batch, work);
                }
            }
            Done::Failed(batch, work, source) => {
                let error = LoadError::Warehouse {
                    statement: "land".to_string(),
                    source,
                };
                let (retrying, retry_max) = (self.retrying, self.retry_max);
                let flight = self.flight(batch);
                if !retrying {
                    flight.abandoned = true;
                    return Some(Progress::Failed { error, retry: None });
                }
                let wait = flight.wait;
                flight.wait = (wait * 2).min(retry_max);
                flight.backing_off += 1;
                let done = self.shared.done.clone();
                self.backoffs.retain(|b| !b.is_finished());
                self.backoffs.push(
                    self.shared
                        .runtime
                        .spawn(async move {
                            tokio::time::sleep(wait).await;
                            let _ = done.send(Done::Retry(batch, work));
                        })
                        .abort_handle(),
                );
                return Some(Progress::Failed {
                    error,
                    retry: Some(wait),
                });
            }
            Done::Panicked(panic) => std::panic::resume_unwind(panic),
        }
        None
    }

    /// The batches at the front that have landed, merged, if any.
    fn take_landed(&mut self) -> Option<Progress> {
        let mut landed: Option<(usize, usize, BTreeMap<u32, u64>)> = None;
        while self.flights.front().is_some_and(Flight::landed) {
            let flight = self.flights.pop_front().expect("checked above");
            self.first += 1;
            let (batches, records, offsets) = landed.get_or_insert_with(Default::default);
            *batches += 1;
            *records += flight.records;
            for (partition, offset) in flight.offsets {
                let highest = offsets.entry(partition).or_insert(offset);
                *highest = (*highest).max(offset);
            }
        }
        landed.map(|(batches, records, offsets)| Progress::Landed {
            batches,
            records,
            offsets,
        })
    }
}

impl<L: Land> Drop for Pipeline<L> {
    /// Start nothing more; work under way finishes unheard.
    fn drop(&mut self) {
        let mut queue = self.shared.queue.lock().unwrap_or_else(|e| e.into_inner());
        queue.ready.clear();
        queue.limit = 0;
        for backoff in &self.backoffs {
            backoff.abort();
        }
    }
}

impl<L> Shared<L>
where
    L: Land + Send + Sync + 'static,
    L::Append: Send + 'static,
{
    /// Run `work` on a blocking thread.
    fn start(self: &Arc<Self>, batch: u64, work: Work<L::Append>) {
        let shared = self.clone();
        self.runtime.spawn_blocking(move || {
            let is_send = matches!(work, Work::Send(_));
            let done = std::panic::catch_unwind(AssertUnwindSafe(|| shared.work(batch, work)))
                .unwrap_or_else(Done::Panicked);
            let _ = shared.done.send(done);
            if is_send {
                shared.queue().sending -= 1;
            }
            shared.send_queued();
        });
    }

    fn work(&self, batch: u64, work: Work<L::Append>) -> Done<L::Append> {
        match work {
            Work::Encode(rows) => match self.lander.encode(&rows) {
                Ok(appends) => {
                    let n = appends.len();
                    let mut queue = self.queue();
                    // Behind older batches' appends, ahead of newer ones'.
                    let at = queue.ready.partition_point(|(b, _)| *b <= batch);
                    for (i, append) in appends.into_iter().enumerate() {
                        queue.ready.insert(at + i, (batch, append));
                    }
                    Done::Encoded(batch, n)
                }
                Err(e) => Done::Failed(batch, Work::Encode(rows), e),
            },
            Work::Send(append) => match self.lander.append(&append) {
                Ok(()) => Done::Sent(batch),
                Err(e) => Done::Failed(batch, Work::Send(append), e),
            },
        }
    }

    /// Queue failed work again, first in line.
    fn retry(self: &Arc<Self>, batch: u64, work: Work<L::Append>) {
        match work {
            Work::Encode(rows) => self.start(batch, Work::Encode(rows)),
            Work::Send(append) => {
                self.queue().ready.push_front((batch, append));
                self.send_queued();
            }
        }
    }

    /// Start queued appends while fewer than the limit are in flight.
    fn send_queued(self: &Arc<Self>) {
        let mut queue = self.queue();
        while queue.sending < queue.limit {
            let Some((batch, append)) = queue.ready.pop_front() else {
                return;
            };
            queue.sending += 1;
            self.start(batch, Work::Send(append));
        }
    }

    fn queue(&self) -> std::sync::MutexGuard<'_, Queue<L::Append>> {
        self.queue.lock().unwrap_or_else(|e| e.into_inner())
    }
}

#[cfg(test)]
pub(crate) mod tests;
