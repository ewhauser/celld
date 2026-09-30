// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Process handling shared by the harnesses that start `celld dev`.

use std::process::{Child, Command};
use std::time::{Duration, Instant};

/// The node processes a `celld dev` supervisor runs as its children.
pub fn dev_nodes(supervisor: &Child) -> Vec<libc::pid_t> {
    let output = Command::new("pgrep")
        .args(["-P", &supervisor.id().to_string()])
        .output()
        .expect("run pgrep");
    String::from_utf8_lossy(&output.stdout)
        .split_whitespace()
        .filter_map(|pid| pid.parse().ok())
        .collect()
}

/// Whether `pid` still names a process, zombies included.
pub fn alive(pid: libc::pid_t) -> bool {
    // SAFETY: signal 0 only checks that the process exists.
    unsafe { libc::kill(pid, 0) == 0 }
}

/// Crash a `celld dev` supervisor and wait for its node to die with it.
///
/// The node has no graceful stop, like a crashed or SIGKILLed `celld dev`, so
/// a restart on the same project recovers the way it would after a crash.
/// When this returns the node is gone, so a restart never races it. The node
/// stops itself once its supervisor dies; the SIGKILL after the deadline only
/// keeps a regression from leaking processes past the test run.
pub fn stop_dev(child: &mut Child) {
    let nodes = dev_nodes(child);
    let _ = child.kill();
    let _ = child.wait();
    let deadline = Instant::now() + Duration::from_secs(10);
    while nodes.iter().any(|&pid| alive(pid)) && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
    }
    for &pid in nodes.iter().filter(|&&pid| alive(pid)) {
        // SAFETY: sending a signal has no memory-safety preconditions.
        unsafe {
            libc::kill(pid, libc::SIGKILL);
        }
    }
}
