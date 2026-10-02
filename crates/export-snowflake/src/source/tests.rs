//! The loop over a fake source, landing through a lander whose appends wait
//! for the test to answer them, so the tests choose the order appends are
//! acknowledged in and which fail.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

use tokio::sync::{mpsc, oneshot};

use super::*;
use crate::consume::Limits;
use crate::loader::{LoaderConfig, Rows, WarehouseError};
use crate::pipeline::tests::{record, scripted, Script, Scripted, Sent};
use crate::Deployment;

/// What the fake source was told, in order.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Call {
    Start,
    Store(u32, u64),
    Commit,
    Shutdown,
}

enum Feed {
    Record(u32, u64, Vec<u8>),
    Revoke(Vec<u32>, oneshot::Sender<()>),
}

struct FakeSource {
    feed: mpsc::UnboundedReceiver<Feed>,
    calls: Arc<Mutex<Vec<Call>>>,
    /// Messages the loop has read.
    read: Arc<AtomicUsize>,
}

struct FakeRevoked(Vec<u32>, oneshot::Sender<()>);

impl Revoked for FakeRevoked {
    fn partitions(&self) -> Vec<u32> {
        self.0.clone()
    }

    async fn complete(self) {
        let _ = self.1.send(());
    }
}

impl Source for FakeSource {
    type Revoked = FakeRevoked;

    fn name(&self) -> &'static str {
        "test"
    }

    fn start(&mut self) -> anyhow::Result<()> {
        self.calls.lock().unwrap().push(Call::Start);
        Ok(())
    }

    async fn next(&mut self) -> anyhow::Result<Next<FakeRevoked>> {
        match self.feed.recv().await {
            Some(Feed::Record(partition, offset, payload)) => {
                self.read.fetch_add(1, Ordering::SeqCst);
                Ok(Next::Record {
                    partition,
                    offset,
                    payload,
                })
            }
            Some(Feed::Revoke(partitions, done)) => {
                Ok(Next::Revoked(FakeRevoked(partitions, done)))
            }
            None => std::future::pending().await,
        }
    }

    fn store_offset(&mut self, partition: u32, offset: u64) -> anyhow::Result<()> {
        self.calls
            .lock()
            .unwrap()
            .push(Call::Store(partition, offset));
        Ok(())
    }

    async fn commit(&mut self) -> anyhow::Result<Vec<u32>> {
        self.calls.lock().unwrap().push(Call::Commit);
        Ok(Vec::new())
    }

    async fn shutdown(self) -> anyhow::Result<()> {
        self.calls.lock().unwrap().push(Call::Shutdown);
        Ok(())
    }
}

/// The Dynamic Table sync's warehouse, which has no schemas.
struct NoSchemas;

impl Warehouse for NoSchemas {
    fn execute_bound(
        &mut self,
        _sql: &str,
        _binds: &[serde_json::Value],
    ) -> Result<Rows, WarehouseError> {
        Ok(Rows::default())
    }
}

struct Harness {
    feed: mpsc::UnboundedSender<Feed>,
    calls: Arc<Mutex<Vec<Call>>>,
    read: Arc<AtomicUsize>,
    lander: Arc<Scripted>,
    events: Arc<Mutex<Vec<String>>>,
    stop: CancellationToken,
    done: tokio::task::JoinHandle<anyhow::Result<()>>,
}

/// A loop that closes a batch at `records`, with `concurrency` appends in
/// flight at most, landing `per_append` rows an append.
fn start(records: usize, concurrency: usize, per_append: usize) -> (Harness, Script) {
    start_with(records, concurrency, per_append, Duration::from_millis(10))
}

/// [`start`], with failed appends first sent again after `retry`.
fn start_with(
    records: usize,
    concurrency: usize,
    per_append: usize,
    retry: Duration,
) -> (Harness, Script) {
    let (feed, next) = mpsc::unbounded_channel();
    let calls = Arc::new(Mutex::new(Vec::new()));
    let read = Arc::new(AtomicUsize::new(0));
    let source = FakeSource {
        feed: next,
        calls: calls.clone(),
        read: read.clone(),
    };
    let (lander, script) = scripted(per_append);
    let settings = Settings {
        limits: Limits {
            records,
            bytes: 1 << 20,
        },
        linger: Duration::from_secs(3600),
        concurrency,
        sync_every: Duration::from_secs(3600),
        retry,
        retry_max: retry * 2,
        skip: Default::default(),
    };
    let events = Arc::new(Mutex::new(Vec::new()));
    let stop = CancellationToken::new();
    let (l, e, s) = (lander.clone(), events.clone(), stop.clone());
    let done = tokio::spawn(async move {
        let mut loader = Loader::new(
            NoSchemas,
            LoaderConfig {
                deployment: Deployment {
                    warehouse: "WH".into(),
                },
                target_lag: "1 minute".into(),
                dynamic_table_prefix: "CF".into(),
            },
        );
        run(source, &mut loader, l, &settings, s, |event| {
            e.lock().unwrap().push(format!("{event:?}"));
        })
        .await
    });
    let harness = Harness {
        feed,
        calls,
        read,
        lander,
        events,
        stop,
        done,
    };
    (harness, script)
}

impl Harness {
    fn record(&self, partition: u32, offset: u64) {
        let _ = self
            .feed
            .send(Feed::Record(partition, offset, record(offset).to_json()));
    }

    fn calls(&self) -> Vec<Call> {
        self.calls.lock().unwrap().clone()
    }

    fn events(&self, kind: &str) -> usize {
        self.events
            .lock()
            .unwrap()
            .iter()
            .filter(|e| e.starts_with(kind))
            .count()
    }

    /// Rows acknowledged, by offset.
    fn landed(&self) -> Vec<u64> {
        let mut offsets: Vec<u64> = self
            .lander
            .landed
            .lock()
            .unwrap()
            .iter()
            .map(|s| s.rsplit('/').next().unwrap().parse().unwrap())
            .collect();
        offsets.sort();
        offsets
    }

    async fn until(&self, what: &str, mut ready: impl FnMut(&Self) -> bool) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while !ready(self) {
            assert!(
                Instant::now() < deadline,
                "timed out waiting for {what}: calls {:?}, events {:?}",
                self.calls(),
                self.events.lock().unwrap()
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// Let the loop run a while, then check it has committed nothing new.
    async fn committed_nothing_past(&self, calls: &[Call]) {
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(self.calls(), calls);
    }

    async fn stop(self) -> Vec<Call> {
        self.stop.cancel();
        self.done.await.unwrap().unwrap();
        let calls = self.calls.lock().unwrap().clone();
        calls
    }
}

/// The first and last offsets an append carries.
fn span(sent: &Sent) -> (u64, u64) {
    let offset = |s: &String| -> u64 { s.rsplit('/').next().unwrap().parse().unwrap() };
    (
        offset(sent.sources.first().unwrap()),
        offset(sent.sources.last().unwrap()),
    )
}

/// The next `n` appends, in the order of the offsets they carry.
async fn appends<const N: usize>(script: &mut Script) -> [Sent; N] {
    let mut sent = Vec::new();
    for _ in 0..N {
        sent.push(script.sent().await);
    }
    sent.sort_by_key(span);
    sent.try_into().ok().unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn acknowledgements_out_of_order_commit_in_order() {
    let (h, mut script) = start(2, 4, 10);
    for offset in 1..=6 {
        h.record(0, offset);
    }
    let [a, b, c] = appends(&mut script).await;
    assert_eq!([span(&a), span(&b), span(&c)], [(1, 2), (3, 4), (5, 6)]);
    // The newest first: its offsets wait for the batches before it.
    c.ack();
    h.committed_nothing_past(&[Call::Start]).await;
    a.ack();
    h.until("the first batch's commit", |h| h.calls().len() == 3)
        .await;
    assert_eq!(h.calls(), [Call::Start, Call::Store(0, 2), Call::Commit]);
    b.ack();
    h.until("the rest's commit", |h| h.calls().len() == 5).await;
    assert_eq!(
        &h.calls()[3..],
        [Call::Store(0, 6), Call::Commit],
        "the second and third batches commit together"
    );
    assert_eq!(h.landed(), [1, 2, 3, 4, 5, 6]);
    let calls = h.stop().await;
    assert_eq!(calls.last(), Some(&Call::Shutdown));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_append_is_retried_while_the_others_land() {
    // Two appends a batch.
    let (h, mut script) = start(2, 4, 1);
    for offset in 1..=4 {
        h.record(0, offset);
    }
    let [a1, a2, b1, b2] = appends(&mut script).await;
    a1.fail();
    a2.ack();
    b1.ack();
    b2.ack();
    // Only the failed append is sent again, and nothing commits meanwhile.
    let again = script.sent().await;
    assert_eq!(span(&again), (1, 1));
    h.committed_nothing_past(&[Call::Start]).await;
    assert_eq!(h.events("LandFailed"), 1);
    again.ack();
    h.until("the commit", |h| h.calls().len() == 3).await;
    assert_eq!(h.calls(), [Call::Start, Call::Store(0, 4), Call::Commit]);
    assert_eq!(h.landed(), [1, 2, 3, 4], "each row acknowledged once");
    h.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nothing_more_is_read_while_the_pipeline_is_full() {
    let (h, mut script) = start(1, 2, 10);
    for offset in 1..=5 {
        h.record(0, offset);
    }
    let [a, b] = appends(&mut script).await;
    script.quiet().await;
    // Two batches landing, and a third full and waiting.
    assert_eq!(h.read.load(Ordering::SeqCst), 3);
    b.fail();
    a.ack();
    h.until("the first commit", |h| h.calls().len() == 3).await;
    assert_eq!(h.calls(), [Call::Start, Call::Store(0, 1), Call::Commit]);
    // The third batch takes the first's place; the second is still failing.
    let [b, c] = appends(&mut script).await;
    assert_eq!([span(&b), span(&c)], [(2, 2), (3, 3)]);
    script.quiet().await;
    assert_eq!(h.read.load(Ordering::SeqCst), 4);
    b.ack();
    c.ack();
    let [d, e] = appends(&mut script).await;
    d.ack();
    e.ack();
    h.until("everything's commit", |h| {
        h.calls().last() == Some(&Call::Commit) && h.calls().contains(&Call::Store(0, 5))
    })
    .await;
    h.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_revocation_waits_for_the_appends_in_flight() {
    let (h, mut script) = start(2, 4, 10);
    h.record(4, 1);
    h.record(4, 2);
    h.record(5, 1);
    let (done, mut completed) = oneshot::channel();
    let _ = h.feed.send(Feed::Revoke(vec![4], done));
    // The full batch, and what was read after it, both in flight.
    let [x, y] = appends(&mut script).await;
    let (a, b) = if x.sources[0] == "test/4/1" {
        (x, y)
    } else {
        (y, x)
    };
    assert_eq!(a.sources, ["test/4/1", "test/4/2"]);
    assert_eq!(b.sources, ["test/5/1"]);
    b.ack();
    h.committed_nothing_past(&[Call::Start]).await;
    assert!(completed.try_recv().is_err(), "not let go yet");
    a.ack();
    completed.await.unwrap();
    // By the time the revocation completes, everything read has landed and
    // its offsets are committed.
    assert_eq!(
        h.calls(),
        [
            Call::Start,
            Call::Store(4, 2),
            Call::Store(5, 1),
            Call::Commit
        ]
    );
    assert_eq!(h.events("Revoked"), 1);
    h.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stop_during_a_revocation_commits_nothing_of_the_revoked_partitions() {
    let (h, mut script) = start(1, 4, 10);
    h.record(4, 1);
    h.record(5, 1);
    let (done, completed) = oneshot::channel();
    let _ = h.feed.send(Feed::Revoke(vec![4], done));
    let [a, b] = appends(&mut script).await;
    h.stop.cancel();
    completed.await.unwrap();
    a.ack();
    b.ack();
    h.until("the shutdown", |h| h.calls().contains(&Call::Shutdown))
        .await;
    // Partition 4 is another member's now; only 5's offset is committed.
    assert_eq!(
        h.calls(),
        [Call::Start, Call::Store(5, 1), Call::Commit, Call::Shutdown]
    );
    h.done.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stopping_lands_what_was_read_and_commits_up_to_the_first_failure() {
    let (h, mut script) = start(2, 4, 10);
    for offset in 1..=7 {
        h.record(0, offset);
    }
    let [a, b, c] = appends(&mut script).await;
    h.until("the last record read", |h| {
        h.read.load(Ordering::SeqCst) == 7
    })
    .await;
    h.stop.cancel();
    // The batch being filled is landed too.
    let d = script.sent().await;
    assert_eq!(span(&d), (7, 7));
    a.ack();
    b.fail();
    c.ack();
    d.ack();
    let calls = h.stop().await;
    // Nothing is sent again, and nothing at or past the failed batch is
    // committed: it and what follows are read again.
    script.quiet().await;
    assert_eq!(
        calls,
        [Call::Start, Call::Store(0, 2), Call::Commit, Call::Shutdown]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stopping_while_appends_wait_to_be_retried_commits_nothing_past_them() {
    // A backoff that cannot run out before the stop: an append sent again
    // would wait for an answer the test never gives.
    let (h, mut script) = start_with(1, 4, 10, Duration::from_secs(3600));
    h.record(0, 1);
    h.record(0, 2);
    let [a, b] = appends(&mut script).await;
    a.fail();
    b.ack();
    h.until("the failure", |h| h.events("LandFailed") == 1)
        .await;
    let calls = h.stop().await;
    // Nothing is sent again, and the second batch, landed behind the
    // first, is not committed.
    script.quiet().await;
    assert_eq!(calls, [Call::Start, Call::Shutdown]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_message_that_is_not_a_record_waits_for_the_appends_in_flight() {
    let (h, mut script) = start(2, 4, 10);
    h.record(0, 1);
    h.record(0, 2);
    h.record(0, 3);
    let _ = h.feed.send(Feed::Record(0, 4, b"{\"kind\":".to_vec()));
    h.record(0, 5);
    let [a, b] = appends(&mut script).await;
    b.ack();
    h.committed_nothing_past(&[Call::Start]).await;
    assert!(!h.done.is_finished(), "the loop waits for the first batch");
    a.ack();
    let calls = h.calls.clone();
    let err = format!("{:#}", h.done.await.unwrap().unwrap_err());
    assert!(err.contains("test/0/4 is not a record"), "{err}");
    assert_eq!(
        *calls.lock().unwrap(),
        [Call::Start, Call::Store(0, 3), Call::Commit, Call::Shutdown]
    );
}
