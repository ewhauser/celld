// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// This harness owns real child processes and host deadlines outside celld's
// injected execution boundary.
#![allow(clippy::disallowed_methods)]

//! A `celld dev` node must not outlive its supervisor. Killing the
//! supervisor runs none of its shutdown code, and only Linux has a
//! parent-death signal, so elsewhere the node has to notice on its own
//! instead of serving under init until someone finds it.

mod support;

use std::net::TcpListener;
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

#[tokio::test]
async fn node_exits_when_its_supervisor_is_killed() {
    let dir = tempfile::tempdir().unwrap();
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("../perf/fixtures/bench");
    for file in ["wrangler.json", "index.js"] {
        std::fs::copy(fixture.join(file), dir.path().join(file)).unwrap();
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
    let mut child = command
        .args(["dev", "--no-watch", "--port", &port.to_string()])
        .current_dir(dir.path())
        .stdin(Stdio::null())
        .stdout(Stdio::from(out.try_clone().unwrap()))
        .stderr(Stdio::from(out))
        .spawn()
        .unwrap();
    let log_text = || std::fs::read_to_string(&log).unwrap_or_default();
    let client = reqwest::Client::new();
    let deadline = Instant::now() + Duration::from_secs(90);
    while !client
        .get(format!("http://127.0.0.1:{port}/.well-known/celld/health"))
        .send()
        .await
        .is_ok_and(|response| response.status().is_success())
    {
        if child.try_wait().unwrap().is_some() || Instant::now() >= deadline {
            support::stop_dev(&mut child);
            panic!("celld dev did not start:\n{}", log_text());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let nodes = support::dev_nodes(&child);
    assert_eq!(nodes.len(), 1, "celld dev runs one node: {nodes:?}");

    child.kill().unwrap();
    child.wait().unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while support::alive(nodes[0]) && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let orphaned = support::alive(nodes[0]);
    if orphaned {
        // SAFETY: sending a signal has no memory-safety preconditions.
        unsafe {
            libc::kill(nodes[0], libc::SIGKILL);
        }
    }
    assert!(
        !orphaned,
        "the node outlived its killed supervisor:\n{}",
        log_text()
    );
}
