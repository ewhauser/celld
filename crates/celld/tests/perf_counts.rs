// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// This harness owns real child processes and host deadlines outside celld's
// injected execution boundary.
#![allow(clippy::disallowed_methods)]

//! Count gates on the hot paths (docs/performance-tests.md).
//!
//! Each test starts a `celld dev` node on the celld-perf bench fixture,
//! sends a few requests one at a time, and asserts what the node counted in
//! `/debug/metrics` across them: bucket requests by key class, core
//! messages, output gates, durability proofs, Worker loads. A count does not
//! depend on the machine, so these fail a change that adds a bucket request
//! to a warm read or an extra round trip through the core, however fast the
//! machine that runs them.
//!
//! A node also makes bucket requests on its own timers (its lease, the fleet
//! sample, drain and wake checks). Those touch `nodes/`, `fleet/`, `drain/`
//! and `wake/`, never a cell's keys, so the per-cell assertions below count
//! only `cell_owner` and `cell_data`.

mod support;

use serde_json::Value;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

struct Node {
    child: Child,
    public: String,
    internal: String,
    log: PathBuf,
    _dir: tempfile::TempDir,
    client: reqwest::Client,
}

impl Drop for Node {
    fn drop(&mut self) {
        // The dev store is per test.
        support::stop_dev(&mut self.child);
    }
}

fn fixture() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../perf/fixtures/bench")
}

impl Node {
    async fn start() -> Node {
        Self::start_with(&[]).await
    }

    /// A node with `env` set on top of a clean `CELLD_` environment.
    async fn start_with(env: &[(&str, &str)]) -> Node {
        let dir = tempfile::tempdir().unwrap();
        for file in ["wrangler.json", "index.js"] {
            std::fs::copy(fixture().join(file), dir.path().join(file)).unwrap();
        }
        let port = TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let log = dir.path().join("dev.log");
        let out = std::fs::File::create(&log).unwrap();
        let mut command = Command::new(env!("CARGO_BIN_EXE_celld"));
        for (name, _) in std::env::vars_os() {
            if name.to_string_lossy().starts_with("CELLD_") {
                command.env_remove(name);
            }
        }
        command.envs(env.iter().copied());
        let child = command
            .args(["dev", "--no-watch", "--logs", "--port", &port.to_string()])
            .current_dir(dir.path())
            .env("RUST_LOG", "info")
            .env("CELLD_SHUTDOWN_TOTAL_MS", "1000")
            .stdin(Stdio::null())
            .stdout(Stdio::from(out.try_clone().unwrap()))
            .stderr(Stdio::from(out))
            .spawn()
            .unwrap();
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .unwrap();
        let mut node = Node {
            child,
            public: format!("http://127.0.0.1:{port}"),
            internal: String::new(),
            log,
            _dir: dir,
            client,
        };
        let deadline = Instant::now() + Duration::from_secs(90);
        loop {
            if node.internal.is_empty() {
                if let Some(address) = node.log_text().lines().find_map(|line| {
                    line.split("celld internal listening on ")
                        .nth(1)
                        .and_then(|rest| rest.split_whitespace().next())
                        .map(str::to_string)
                }) {
                    node.internal = format!("http://{address}");
                }
            }
            let ready = node
                .client
                .get(format!("{}/.well-known/celld/health", node.public))
                .send()
                .await
                .is_ok_and(|response| response.status().is_success());
            if ready && !node.internal.is_empty() {
                return node;
            }
            if node.child.try_wait().unwrap().is_some() || Instant::now() >= deadline {
                panic!("celld dev did not start:\n{}", node.log_text());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    fn log_text(&self) -> String {
        std::fs::read_to_string(&self.log).unwrap_or_default()
    }

    async fn get(&self, path: &str) -> Value {
        let response = self
            .client
            .get(format!("{}{path}", self.public))
            .send()
            .await
            .unwrap();
        let status = response.status();
        let body = response.text().await.unwrap();
        assert!(
            status.is_success(),
            "{path}: {status} {body}\n{}",
            self.log_text()
        );
        serde_json::from_str(&body).unwrap_or(Value::Null)
    }

    async fn internal(&self, path: &str) -> Value {
        let body = self
            .client
            .get(format!("{}{path}", self.internal))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        serde_json::from_str(&body).unwrap()
    }

    async fn metrics(&self) -> Metrics {
        Metrics(self.internal("/debug/metrics").await)
    }

    /// The scope of every cell resident on the node.
    async fn residents(&self) -> Vec<String> {
        self.internal("/state").await["residents"]
            .as_array()
            .unwrap()
            .iter()
            .map(|cell| cell.as_str().unwrap().to_string())
            .collect()
    }
}

struct Metrics(Value);

impl Metrics {
    fn counter(&self, label: &str) -> u64 {
        self.0["counters"][label].as_u64().unwrap()
    }

    fn hist_count(&self, label: &str) -> u64 {
        self.0["histograms"][label]["count"].as_u64().unwrap()
    }

    /// Bucket requests whose class is one of `classes` and, with `ops`,
    /// whose op is one of those.
    fn bucket(&self, classes: &[&str], ops: Option<&[&str]>) -> u64 {
        self.0["bucket"]["requests"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|row| classes.contains(&row["class"].as_str().unwrap()))
            .filter(|row| ops.is_none_or(|ops| ops.contains(&row["op"].as_str().unwrap())))
            .map(|row| row["count"].as_u64().unwrap())
            .sum()
    }

    fn bucket_total(&self) -> u64 {
        self.0["bucket"]["requests"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row["count"].as_u64().unwrap())
            .sum()
    }

    /// Per-cell bucket requests, as `op/class/outcome` lines, for messages.
    fn cell_rows(&self) -> String {
        self.0["bucket"]["requests"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|row| row["class"] == "cell_owner" || row["class"] == "cell_data")
            .map(|row| {
                format!(
                    "{}/{}/{}={}",
                    row["op"], row["class"], row["outcome"], row["count"]
                )
            })
            .collect::<Vec<_>>()
            .join(" ")
    }
}

const CELL: &[&str] = &["cell_owner", "cell_data"];

#[tokio::test(flavor = "multi_thread")]
async fn a_warm_read_touches_no_cell_object_and_crosses_the_core_twice() {
    let node = Node::start().await;
    node.get("/do/write?cell=warm&bytes=10").await;
    let before = node.metrics().await;
    for _ in 0..50 {
        node.get("/do/read?cell=warm").await;
    }
    let after = node.metrics().await;
    assert_eq!(
        after.bucket(CELL, None) - before.bucket(CELL, None),
        0,
        "a warm read reached the bucket; before: {} after: {}",
        before.cell_rows(),
        after.cell_rows()
    );
    // One route decision and one output gate per request; nothing else
    // asks the core to decide.
    assert_eq!(
        after.counter("core.requests") - before.counter("core.requests"),
        50
    );
    assert_eq!(
        after.counter("core.outputs") - before.counter("core.outputs"),
        50
    );
    assert_eq!(
        after.hist_count("gate.wait_us") - before.hist_count("gate.wait_us"),
        50
    );
    // No read waits for a durability proof.
    for proof in ["durability.proof_bucket_us", "durability.proof_fleet_us"] {
        assert_eq!(
            after.hist_count(proof) - before.hist_count(proof),
            0,
            "{proof}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_bucket_proof_write_is_one_upload_and_one_ownership_read() {
    let node = Node::start().await;
    node.get("/do/write?cell=writer&bytes=10").await;
    // Let the first write's activation settle.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let before = node.metrics().await;
    for _ in 0..20 {
        node.get("/do/write?cell=writer&bytes=100").await;
    }
    let after = node.metrics().await;
    let proofs = after.hist_count("durability.proof_bucket_us")
        - before.hist_count("durability.proof_bucket_us");
    assert_eq!(
        proofs, 20,
        "a lone node proves each write through the bucket"
    );
    let puts = after.bucket(&["cell_data"], Some(&["put"]))
        - before.bucket(&["cell_data"], Some(&["put"]));
    let owner_reads = after.bucket(&["cell_owner"], Some(&["get"]))
        - before.bucket(&["cell_owner"], Some(&["get"]));
    eprintln!("sequential writes: {puts} uploads, {owner_reads} owner reads");
    assert!(
        (1..=20).contains(&puts),
        "{puts} cell uploads for 20 sequential writes; after: {}",
        after.cell_rows()
    );
    // Each bucket proof confirms ownership with one read of own.json.
    assert_eq!(owner_reads, proofs, "after: {}", after.cell_rows());
    let other = after.bucket(CELL, None) - before.bucket(CELL, None) - puts - owner_reads;
    assert_eq!(
        other,
        0,
        "unexpected cell requests; after: {}",
        after.cell_rows()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_exported_write_shares_its_ownership_read_with_the_export() {
    const WRITES: u64 = 40;
    let node = Node::start_with(&[("CELLD_EXPORT", "1")]).await;
    node.get("/do/write?cell=exported&bytes=10").await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let before = node.metrics().await;
    for _ in 0..WRITES {
        node.get("/do/write?cell=exported&bytes=100").await;
    }
    // An export ticket that proves itself settles after the response; let
    // the last one land so it is counted.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let after = node.metrics().await;
    let owner_reads = after.bucket(&["cell_owner"], Some(&["get"]))
        - before.bucket(&["cell_owner"], Some(&["get"]));
    eprintln!("{WRITES} exported writes: {owner_reads} owner reads");
    // The export of a commit rides the ownership read of the write that made
    // it, so export costs no read of its own. A ticket that arrives after
    // the write's read was asked still reads for itself, hence the margin.
    assert!(
        owner_reads as f64 <= WRITES as f64 * 1.05,
        "{owner_reads} owner reads for {WRITES} exported writes; after: {}",
        after.cell_rows()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_writes_to_one_cell_share_uploads() {
    const WRITES: u64 = 64;
    let node = Arc::new(Node::start().await);
    node.get("/do/write?cell=shared&bytes=10").await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let before = node.metrics().await;
    let writes: Vec<_> = (0..WRITES)
        .map(|_| {
            let node = node.clone();
            tokio::spawn(async move { node.get("/do/write?cell=shared&bytes=100").await })
        })
        .collect();
    for write in writes {
        write.await.unwrap();
    }
    let after = node.metrics().await;
    // Every write waited for an upload, so the count below is not met by
    // writes that skipped the bucket.
    assert_eq!(
        after.hist_count("durability.proof_bucket_us")
            - before.hist_count("durability.proof_bucket_us"),
        WRITES
    );
    let puts = after.bucket(&["cell_data"], Some(&["put"]))
        - before.bucket(&["cell_data"], Some(&["put"]));
    eprintln!("{WRITES} concurrent writes: {puts} uploads");
    // A cell's upload carries every write that committed before it began,
    // so the count is how many uploads fit in the time the writes take to
    // commit, and that depends on the machine: 9 on the machine this was
    // written on, up to 22 on a shared CI runner. Without sharing it is one
    // per write, so the bound is that an upload carries two writes on
    // average, not a count one machine happens to reach.
    assert!(
        puts * 2 <= WRITES,
        "{WRITES} concurrent writes to one cell made {puts} uploads; they should share them"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn activating_a_cell_into_a_warm_isolate_compiles_nothing() {
    let node = Node::start().await;
    node.get("/do/write?cell=first&bytes=10").await;
    let before = node.metrics().await;
    for index in 0..5 {
        node.get(&format!("/do/write?cell=next-{index}&bytes=10"))
            .await;
    }
    let after = node.metrics().await;
    // Up to 32 cells share one isolate, so five more need no new one.
    assert_eq!(
        after.hist_count("isolate.worker_load_us") - before.hist_count("isolate.worker_load_us"),
        0
    );
    assert_eq!(
        after.hist_count("activation.fresh_us") - before.hist_count("activation.fresh_us"),
        5
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn reactivating_an_evicted_cell_reads_only_its_owner_record() {
    let node = Node::start().await;
    node.get("/do/write?cell=cold&bytes=10").await;
    let residents = node.residents().await;
    assert_eq!(residents.len(), 1, "{residents:?}");
    let evicted = node
        .client
        .post(format!("{}/evict/{}", node.internal, residents[0]))
        .send()
        .await
        .unwrap();
    assert!(
        evicted.status().is_success(),
        "{}",
        evicted.text().await.unwrap()
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    while !node.residents().await.is_empty() {
        assert!(Instant::now() < deadline, "the cell never left");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let before = node.metrics().await;
    node.get("/do/read?cell=cold").await;
    let after = node.metrics().await;
    let requests = after.bucket(CELL, None) - before.bucket(CELL, None);
    eprintln!(
        "reactivation: {requests} cell requests: {}",
        after.cell_rows()
    );
    // Eviction left the database on this node, so the cell reopens it
    // without listing or downloading anything: it reads its owner record
    // and claims the next epoch. (A restore from the bucket is S8's.)
    assert!(
        (1..=3).contains(&requests),
        "{requests} cell requests to reactivate an evicted cell; after: {}",
        after.cell_rows()
    );
    let lists = after.bucket(
        &["cell_data"],
        Some(&["list", "list_delimited", "list_paginated"]),
    ) - before.bucket(
        &["cell_data"],
        Some(&["list", "list_delimited", "list_paginated"]),
    );
    assert_eq!(lists, 0, "a local reactivation listed the bucket");
    assert_eq!(
        after.hist_count("activation.local_us") - before.hist_count("activation.local_us"),
        1
    );
    assert_eq!(
        after.counter("core.requests") - before.counter("core.requests"),
        1
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_idle_node_stays_within_its_bucket_budget() {
    let node = Node::start().await;
    node.get("/do/write?cell=idle&bytes=10").await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    let before = node.metrics().await;
    tokio::time::sleep(Duration::from_secs(10)).await;
    let after = node.metrics().await;
    let per_second = (after.bucket_total() - before.bucket_total()) as f64 / 10.0;
    eprintln!("idle: {per_second} bucket requests per second");
    // Lease renewal, the fleet sample, drain and wake checks. A resident
    // cell costs nothing while it waits.
    assert!(
        per_second <= 5.0,
        "{per_second} bucket requests per second while idle"
    );
    assert_eq!(after.bucket(CELL, None) - before.bucket(CELL, None), 0);
}
