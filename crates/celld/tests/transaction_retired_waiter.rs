// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// This harness owns real child processes and host deadlines outside celld's
// injected execution boundary.
#![allow(clippy::disallowed_methods)]

//! A transaction whose block retires before its callback runs, end to end.
//! Event H takes the input gate and, in the same turn, resumes event B, whose
//! `storage.transaction()` queues as a cross-entry waiter on H's ticket. B's
//! handler budget then retires the waiter, and the block resolves without
//! running the callback. The transaction must give its tail slot back:
//! before the fix it kept it, and every later transaction on the object
//! waited on a slot nothing would release, until the block limit reset the
//! object or the handler budget ended the caller.

mod support;

use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const CONFIG: &str = r#"{
  "name": "retired-waiter",
  "main": "index.js",
  "no_bundle": true,
  "compatibility_date": "2026-01-01",
  "durable_objects": { "bindings": [{ "name": "RUN", "class_name": "Run" }] },
  "migrations": [{ "tag": "v1", "new_sqlite_classes": ["Run"] }]
}"#;

const WORKER: &str = r#"
import { DurableObject } from "cloudflare:workers";

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

export class Run extends DurableObject {
  // B parks on a promise only another event resolves.
  async waitThenStatus() {
    await new Promise((resolve) => { this.release = resolve; });
    return await this.status();
  }
  // H takes the gate, then resumes B in the same turn. The grant reaches
  // H's block as a later op, so B's transaction does not ride it as a nested
  // block: it queues as a waiter whose origin is B and whose owner is H.
  async holdAndRelease(ms) {
    this.ctx.blockConcurrencyWhile(() => sleep(ms));
    this.release?.();
    return "holding";
  }
  async plainGet() {
    return (await this.ctx.storage.get("run")) ?? "missing";
  }
  async status() {
    return await this.ctx.storage.transaction(async (txn) => {
      return (await txn.get("run")) ?? "missing";
    });
  }
}

export default {
  async fetch(request, env) {
    const run = env.RUN.getByName("run");
    const url = new URL(request.url);
    try {
      let value;
      switch (url.pathname) {
        case "/health": value = "ok"; break;
        case "/wait-status": value = await run.waitThenStatus(); break;
        case "/hold-release": value = await run.holdAndRelease(Number(url.searchParams.get("ms"))); break;
        case "/plain": value = await run.plainGet(); break;
        case "/status": value = await run.status(); break;
        default: return new Response("not found", { status: 404 });
      }
      return Response.json({ value });
    } catch (error) {
      return new Response(String(error?.stack ?? error), { status: 500 });
    }
  },
};
"#;

/// The handler budget, in seconds. B outlives it while it waits on H's hold.
const BUDGET_S: u64 = 2;
/// H's hold, long enough that B's budget ends while B still queues.
const HOLD_MS: u64 = 3_000;

struct Dev {
    child: Child,
    url: String,
    log: PathBuf,
}

impl Drop for Dev {
    fn drop(&mut self) {
        support::stop_dev(&mut self.child);
    }
}

impl Dev {
    async fn start(client: &reqwest::Client, project: &Path) -> Dev {
        let port = TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let log = project.join("dev.log");
        let out = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log)
            .unwrap();
        let mut command = Command::new(env!("CARGO_BIN_EXE_celld"));
        for (name, _) in std::env::vars_os() {
            if name.to_string_lossy().starts_with("CELLD_") {
                command.env_remove(name);
            }
        }
        let child = command
            .args(["dev", "--no-watch", "--logs", "--port", &port.to_string()])
            .current_dir(project)
            .env("CELLD_SHUTDOWN_TOTAL_MS", "1000")
            .env("CELLD_HANDLER_BUDGET_S", BUDGET_S.to_string())
            .stdin(Stdio::null())
            .stdout(Stdio::from(out.try_clone().unwrap()))
            .stderr(Stdio::from(out))
            .spawn()
            .unwrap();
        let mut dev = Dev {
            child,
            url: format!("http://127.0.0.1:{port}"),
            log,
        };
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            if client
                .get(format!("{}/health", dev.url))
                .send()
                .await
                .is_ok_and(|r| r.status().is_success())
            {
                return dev;
            }
            if dev.child.try_wait().unwrap().is_some() || Instant::now() >= deadline {
                panic!("celld dev did not start:\n{}", dev.log_text());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    fn log_text(&self) -> String {
        std::fs::read_to_string(&self.log).unwrap_or_default()
    }

    async fn call(&self, client: &reqwest::Client, path: &str) -> (reqwest::StatusCode, String) {
        let response = client
            .get(format!("{}{path}", self.url))
            .send()
            .await
            .unwrap_or_else(|error| panic!("{path}: {error}\n{}", self.log_text()));
        let status = response.status();
        (status, response.text().await.unwrap_or_default())
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_transaction_retired_before_its_block_runs_releases_its_slot() {
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(20))
        .build()
        .unwrap();
    let project = tempfile::tempdir().unwrap();
    std::fs::write(project.path().join("wrangler.jsonc"), CONFIG).unwrap();
    std::fs::write(project.path().join("index.js"), WORKER).unwrap();
    let dev = Dev::start(&client, project.path()).await;

    let waiter = {
        let client = client.clone();
        let url = format!("{}/wait-status", dev.url);
        tokio::spawn(async move {
            let response = client.get(url).send().await.unwrap();
            let status = response.status();
            (status, response.text().await.unwrap_or_default())
        })
    };
    // B must be parked on the promise before H resolves it.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let held = Instant::now();
    let (status, body) = dev
        .call(&client, &format!("/hold-release?ms={HOLD_MS}"))
        .await;
    assert!(
        status.is_success(),
        "hold: {status} {body}\n{}",
        dev.log_text()
    );

    // B's budget ends while its transaction still queues behind H's hold.
    let (status, body) = waiter.await.unwrap();
    assert!(
        !status.is_success() && body.contains("budget"),
        "the waiter is retired by its budget: {status} {body}\n{}",
        dev.log_text()
    );

    // Once H's hold has ended the gate is open, and a new transaction must
    // start at once. With the slot still held it waited out the budget.
    let open = held + Duration::from_millis(HOLD_MS + 1_000);
    tokio::time::sleep(open.saturating_duration_since(Instant::now())).await;
    let (status, body) = dev.call(&client, "/plain").await;
    assert!(
        status.is_success(),
        "plain read: {status} {body}\n{}",
        dev.log_text()
    );
    for attempt in 0..2 {
        let started = Instant::now();
        let (status, body) = dev.call(&client, "/status").await;
        assert!(
            status.is_success(),
            "transaction {attempt} after the retired waiter: {status} {body} ({:?})\n{}",
            started.elapsed(),
            dev.log_text()
        );
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "transaction {attempt} waited {:?}",
            started.elapsed()
        );
    }
}
