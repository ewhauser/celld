use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

use celld_export_format::{Body, Envelope, Origin, Position, Record, StreamId, WatermarkBody};
use tokio::sync::mpsc;

use super::*;

// ------------------------------------------------- a lander the test drives

/// An append the [`Scripted`] lander was asked to send, held until the test
/// answers it.
pub(crate) struct Sent {
    /// The sources of its rows.
    pub sources: Vec<String>,
    reply: std::sync::mpsc::Sender<Result<(), WarehouseError>>,
}

impl Sent {
    pub fn ack(self) {
        let _ = self.reply.send(Ok(()));
    }

    pub fn fail(self) {
        let _ = self
            .reply
            .send(Err(WarehouseError::other("Snowpipe Streaming unavailable")));
    }
}

/// Lands `per_append` rows an append; each append waits for the test to
/// answer it through the [`Script`].
pub(crate) struct Scripted {
    per_append: usize,
    sent: mpsc::UnboundedSender<Sent>,
    in_flight: AtomicUsize,
    /// The most appends ever in flight at once.
    pub most: AtomicUsize,
    /// Every row acknowledged, by source.
    pub landed: Mutex<Vec<String>>,
}

/// The test's end of a [`Scripted`] lander.
pub(crate) struct Script(mpsc::UnboundedReceiver<Sent>);

pub(crate) fn scripted(per_append: usize) -> (Arc<Scripted>, Script) {
    let (sent, script) = mpsc::unbounded_channel();
    let lander = Scripted {
        per_append,
        sent,
        in_flight: AtomicUsize::new(0),
        most: AtomicUsize::new(0),
        landed: Mutex::new(Vec::new()),
    };
    (Arc::new(lander), Script(script))
}

impl Land for Scripted {
    type Append = Vec<String>;

    fn encode(&self, rows: &[LandingRow]) -> Result<Vec<Vec<String>>, WarehouseError> {
        Ok(rows
            .chunks(self.per_append)
            .map(|c| c.iter().map(|r| r.source.clone()).collect())
            .collect())
    }

    fn append(&self, sources: &Vec<String>) -> Result<(), WarehouseError> {
        let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.most.fetch_max(now, Ordering::SeqCst);
        let (reply, answer) = std::sync::mpsc::channel();
        let _ = self.sent.send(Sent {
            sources: sources.clone(),
            reply,
        });
        // A test that ends without answering fails the append.
        let result = answer
            .recv()
            .unwrap_or_else(|_| Err(WarehouseError::other("unanswered")));
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
        if result.is_ok() {
            self.landed.lock().unwrap().extend(sources.iter().cloned());
        }
        result
    }
}

impl Script {
    /// The next append sent.
    pub async fn sent(&mut self) -> Sent {
        tokio::time::timeout(Duration::from_secs(10), self.0.recv())
            .await
            .expect("an append is sent")
            .expect("the lander is alive")
    }

    /// The next `n` appends sent, in no particular order, sorted by their
    /// first row's source.
    pub async fn sent_n(&mut self, n: usize) -> Vec<Sent> {
        let mut all = Vec::new();
        for _ in 0..n {
            all.push(self.sent().await);
        }
        all.sort_by(|a, b| a.sources[0].cmp(&b.sources[0]));
        all
    }

    /// Assert that nothing more is sent for a while.
    pub async fn quiet(&mut self) {
        if let Ok(Some(s)) = tokio::time::timeout(Duration::from_millis(100), self.0.recv()).await {
            panic!("unexpected append of {:?}", s.sources);
        }
    }
}

pub(crate) fn record(txid: u64) -> Record {
    Record {
        envelope: Envelope {
            stream: StreamId {
                script: "app".into(),
                class: "Room".into(),
                cell: "r1".into(),
                facet: None,
                incarnation: 1,
            },
            cell_name: None,
            position: Position::new(1, txid, 1),
            committed_at: 1_790_000_000_000,
            node: "node-a".into(),
            origin: Origin::Live,
            fragment: 1,
            fragments: 1,
        },
        body: Body::Watermark(WatermarkBody {
            from: None,
            through: Position::new(1, txid, 1),
            commits: 1,
            records: 1,
        }),
    }
}

// ---------------------------------------------------------------- tests

/// A batch of `offsets` in `partition`, each row's source `p<partition>/<offset>`.
fn batch(partition: u32, offsets: &[u64]) -> (Vec<LandingRow>, BTreeMap<u32, u64>) {
    let rows = offsets
        .iter()
        .map(|&o| LandingRow::from_record(&record(o), format!("p{partition}/{o}")))
        .collect();
    let covered = offsets
        .iter()
        .max()
        .map(|&o| BTreeMap::from([(partition, o)]));
    (rows, covered.unwrap_or_default())
}

fn pipeline(lander: Arc<Scripted>, concurrency: usize) -> Pipeline<Scripted> {
    Pipeline::new(
        lander,
        concurrency,
        Duration::from_millis(5),
        Duration::from_millis(20),
    )
}

/// The next progress, which must come soon.
async fn next(p: &mut Pipeline<Scripted>) -> Option<Progress> {
    tokio::time::timeout(Duration::from_secs(10), p.next())
        .await
        .expect("progress")
}

/// Assert that `p` reports nothing for a while.
async fn nothing(p: &mut Pipeline<Scripted>) {
    if let Ok(progress) = tokio::time::timeout(Duration::from_millis(100), p.next()).await {
        panic!("unexpected {progress:?}");
    }
}

fn landed(progress: Option<Progress>) -> (usize, usize, BTreeMap<u32, u64>) {
    match progress {
        Some(Progress::Landed {
            batches,
            records,
            offsets,
        }) => (batches, records, offsets),
        other => panic!("expected a landing, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn appends_run_concurrently_up_to_the_limit_across_batches() {
    let (lander, mut script) = scripted(2);
    let mut p = pipeline(lander.clone(), 3);
    // Two appends each.
    for b in 0..3 {
        let (rows, offsets) = batch(0, &[b * 10, b * 10 + 1, b * 10 + 2, b * 10 + 3]);
        p.submit(rows, offsets);
    }
    assert!(!p.has_room(), "three batches fill a pipeline of three");
    let progress = tokio::spawn(async move {
        let mut seen = Vec::new();
        while let Some(progress) = p.next().await {
            seen.push(progress);
        }
        seen
    });
    // Three at most at once, whichever batches they are from.
    let first = script.sent_n(3).await;
    script.quiet().await;
    assert_eq!(lander.most.load(Ordering::SeqCst), 3);
    for s in first {
        s.ack();
    }
    for s in script.sent_n(3).await {
        s.ack();
    }
    let seen = progress.await.unwrap();
    let total: usize = seen
        .into_iter()
        .map(|p| landed(Some(p)))
        .map(|(batches, _, _)| batches)
        .sum();
    assert_eq!(total, 3);
    assert_eq!(lander.most.load(Ordering::SeqCst), 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_batch_lands_only_after_every_batch_before_it() {
    let (lander, mut script) = scripted(10);
    let mut p = pipeline(lander, 4);
    for b in 0..3 {
        let (rows, offsets) = batch(b, &[1, 2]);
        p.submit(rows, offsets);
    }
    let [a, b, c]: [Sent; 3] = script.sent_n(3).await.try_into().ok().unwrap();
    // The newest two are acknowledged first: nothing to commit.
    c.ack();
    b.ack();
    nothing(&mut p).await;
    a.ack();
    let (batches, records, offsets) = landed(next(&mut p).await);
    assert_eq!((batches, records), (3, 6), "all three at once, in order");
    assert_eq!(offsets, BTreeMap::from([(0, 2), (1, 2), (2, 2)]));
    assert!(next(&mut p).await.is_none());
    assert!(p.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_append_is_sent_again_on_its_own() {
    let (lander, mut script) = scripted(1);
    let mut p = pipeline(lander.clone(), 4);
    let (rows, offsets) = batch(0, &[1, 2]);
    p.submit(rows, offsets);
    let (rows, offsets) = batch(1, &[5]);
    p.submit(rows, offsets);
    let [a1, a2, b]: [Sent; 3] = script.sent_n(3).await.try_into().ok().unwrap();
    a1.fail();
    match next(&mut p).await {
        Some(Progress::Failed { retry, .. }) => assert_eq!(retry, Some(Duration::from_millis(5))),
        other => panic!("{other:?}"),
    }
    a2.ack();
    b.ack();
    // Batch 1 has landed, but not batch 0. Polling the pipeline lets the
    // backoff run out, and only the failed append is sent again.
    nothing(&mut p).await;
    let again = script.sent().await;
    assert_eq!(again.sources, ["p0/1"]);
    again.fail();
    match next(&mut p).await {
        Some(Progress::Failed { retry, .. }) => {
            assert_eq!(retry, Some(Duration::from_millis(10)), "doubled")
        }
        other => panic!("{other:?}"),
    }
    nothing(&mut p).await;
    script.sent().await.ack();
    let (batches, _, offsets) = landed(next(&mut p).await);
    assert_eq!(batches, 2);
    assert_eq!(offsets, BTreeMap::from([(0, 2), (1, 5)]));
    let mut rows = lander.landed.lock().unwrap().clone();
    rows.sort();
    assert_eq!(rows, ["p0/1", "p0/2", "p1/5"], "each row once");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_batch_of_skipped_messages_lands_in_its_turn() {
    let (lander, mut script) = scripted(10);
    let mut p = pipeline(lander, 4);
    let (rows, offsets) = batch(0, &[1]);
    p.submit(rows, offsets);
    p.submit(Vec::new(), BTreeMap::from([(0, 2)]));
    nothing(&mut p).await;
    script.sent().await.ack();
    let (batches, records, offsets) = landed(next(&mut p).await);
    assert_eq!((batches, records), (2, 1));
    assert_eq!(offsets, BTreeMap::from([(0, 2)]));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn without_retries_a_failure_holds_back_every_later_batch() {
    let (lander, mut script) = scripted(10);
    let mut p = pipeline(lander, 4);
    for b in 0..3 {
        let (rows, offsets) = batch(b, &[1]);
        p.submit(rows, offsets);
    }
    let [a, b, c]: [Sent; 3] = script.sent_n(3).await.try_into().ok().unwrap();
    p.stop_retrying();
    a.ack();
    let (batches, _, offsets) = landed(next(&mut p).await);
    assert_eq!((batches, offsets), (1, BTreeMap::from([(0, 1)])));
    b.fail();
    match next(&mut p).await {
        Some(Progress::Failed { retry: None, .. }) => {}
        other => panic!("{other:?}"),
    }
    c.ack();
    assert!(next(&mut p).await.is_none(), "the third never lands");
    script.quiet().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stopping_gives_up_appends_waiting_to_be_sent_again() {
    let (lander, mut script) = scripted(10);
    // A long backoff, cut short by the stop.
    let mut p = Pipeline::new(
        lander,
        4,
        Duration::from_secs(3600),
        Duration::from_secs(3600),
    );
    let (rows, offsets) = batch(0, &[1]);
    p.submit(rows, offsets);
    script.sent().await.fail();
    assert!(matches!(next(&mut p).await, Some(Progress::Failed { .. })));
    p.stop_retrying();
    assert!(next(&mut p).await.is_none());
    script.quiet().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn forgotten_partitions_are_not_reported() {
    let (lander, mut script) = scripted(10);
    let mut p = pipeline(lander, 4);
    let (mut rows, mut offsets) = batch(0, &[1]);
    let (more, other) = batch(3, &[9]);
    rows.extend(more);
    offsets.extend(other);
    p.submit(rows, offsets);
    p.forget(&[3]);
    script.sent().await.ack();
    let (_, records, offsets) = landed(next(&mut p).await);
    assert_eq!(records, 2);
    assert_eq!(offsets, BTreeMap::from([(0, 1)]));
}
