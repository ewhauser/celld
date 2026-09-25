// Copyright 2026 Deno Land Inc. Apache-2.0 license.

use super::*;

fn run(future: impl std::future::Future<Output = ()>) {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    let runtime = RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap()
    });
    crate::asyncrt::set_host_handle(runtime.handle().clone());
    runtime.block_on(future);
}

/// Every peer is unreachable: the sweep's recovery attempts stay undecided
/// and leave the observed records as the fixture wrote them.
struct Offline;

impl LogTransport for Offline {
    fn post<'a>(
        &'a self,
        _node: &'a str,
        _addr: &'a str,
        _path: &'a str,
        _body: Vec<u8>,
        _deadline: Option<std::time::Duration>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = anyhow::Result<bytes::Bytes>> + Send + 'a>,
    > {
        Box::pin(async { anyhow::bail!("peer is offline") })
    }
}

struct Fixture {
    _dir: tempfile::TempDir,
    bucket: Arc<Bucket>,
    ownership: Arc<crate::ownership_store::BucketOwnership>,
    manager: NodeLogManager,
    tasks: crate::ltx_repl::LtxTaskOwner,
}

impl Fixture {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let database = dir.path().join("bucket.sqlite");
        let bucket = Arc::new(Bucket::open_dev(&database).unwrap());
        let ownership = Arc::new(
            crate::ownership_store::BucketOwnership::new(
                (*bucket).clone(),
                (*bucket).clone(),
                "observer".into(),
                "g".into(),
            )
            .with_lease_ttl_ms(10_000),
        );
        let own_log = Arc::new(OwnLog {
            ownership: ownership.clone(),
            nudge: Box::new(|| {}),
            write_lock: tokio::sync::Mutex::new(()),
        });
        let ltx = Arc::new(
            crate::ltx_repl::LtxRepl::start(
                dir.path(),
                crate::bucket::StorageBackend::Local,
                database.to_str().unwrap().into(),
                String::new(),
                None,
                "auto".into(),
                None,
            )
            .unwrap(),
        );
        let tasks = ltx.take_task_owner();
        let manager = NodeLogManager::new_with_log_transport(
            "observer/g",
            bucket.clone(),
            own_log,
            ltx,
            Arc::new(Offline),
            Default::default(),
        );
        Self {
            _dir: dir,
            bucket,
            ownership,
            manager,
            tasks,
        }
    }

    async fn lease(&self, node: &str, expires_ms: u64, log: serde_json::Value) {
        let mut lease = serde_json::json!({
            "node": node, "expires_ms": expires_ms, "ownership_index_generation": "g1",
        });
        if !log.is_null() {
            lease["log"] = log;
        }
        self.bucket
            .put(
                &format!("nodes/{node}.json"),
                serde_json::to_vec(&lease).unwrap(),
            )
            .await
            .unwrap();
    }

    async fn stop(self) {
        self.tasks.request_stop();
        self.tasks.join().await;
    }
}

fn log(state: &str, ensemble: &[&str], bucket_complete: bool) -> serde_json::Value {
    serde_json::json!({
        "state": state, "epoch": 2, "tiered": 0, "ensemble": ensemble,
        "active": true, "bucket_complete": bucket_complete,
    })
}

#[test]
fn the_sweep_retains_unrecovered_logs_and_member_obligations() {
    run(async {
        let fixture = Fixture::new();
        let now = crate::ownership_store::now_ms();
        let live = now + 600_000;
        // Live leaders: one still needs both followers, one proved its
        // epoch bucket-complete and needs neither.
        fixture
            .lease("live", live, log("open", &["a", "b"], false))
            .await;
        fixture
            .lease("proven", live, log("open", &["a"], true))
            .await;
        // A dead log another node is recovering right now: the sweep leaves
        // it alone and still reports it.
        let mut claimed = log("recovering", &["c"], false);
        claimed["claimant"] = "rescuer".into();
        claimed["claimed_ms"] = now.into();
        fixture.lease("claimed", 1, claimed).await;
        // A dead open log. The sweep tries to recover it, which stays
        // undecided with every peer offline.
        fixture.lease("dead", 1, log("open", &["b"], false)).await;
        // Sealed, and never opened: nothing owed.
        fixture
            .lease("sealed", 1, log("sealed", &["a"], false))
            .await;
        fixture.lease("bare", live, serde_json::Value::Null).await;
        // This process's own live log comes from memory.
        fixture.ownership.set_own_log(Some(log_to_wire(
            &log_tier::LogRecord {
                epoch: 5,
                ensemble: ["d".to_string()].into_iter().collect(),
                tiered: 0,
                bucket_complete: false,
                state: LogState::Open,
                claimant: None,
                claimed_ms: None,
            },
            true,
        )));
        fixture
            .lease("observer", live, serde_json::Value::Null)
            .await;

        fixture.manager.sweep_dead_leaders().await.unwrap();
        fixture.manager.fleet_posture.set(true).unwrap();
        let state = fixture.manager.state_json();
        assert_eq!(state["posture"], "fleet");
        assert_eq!(state["session"], "observer/g");
        assert_eq!(state["shipper_healthy"], false);
        assert_eq!(
            state["own"],
            serde_json::json!({
                "state": "open", "epoch": 5, "ensemble": ["d"],
                "bucket_complete": false, "active": true,
            })
        );
        let fleet = &state["fleet"];
        assert_eq!(fleet["complete"], true);
        assert!(fleet["observed_ms"].as_u64().unwrap() >= now);
        assert_eq!(
            fleet["unrecovered"],
            serde_json::json!([
                {"session": "claimed/g1", "state": "recovering",
                 "lease_expires_ms": 1, "claimant": "rescuer"},
                {"session": "dead/g1", "state": "open",
                 "lease_expires_ms": 1, "claimant": null},
            ])
        );
        assert_eq!(
            fleet["obligations"],
            serde_json::json!({
                "a": ["live/g1"],
                "b": ["dead/g1", "live/g1"],
                "c": ["claimed/g1"],
                "d": ["observer/g"],
            })
        );
        fixture.stop().await;
    });
}

#[test]
fn an_unreadable_record_marks_the_pass_incomplete() {
    run(async {
        let fixture = Fixture::new();
        let live = crate::ownership_store::now_ms() + 600_000;
        fixture
            .lease("live", live, log("open", &["a"], false))
            .await;
        fixture
            .bucket
            .put("nodes/broken.json", b"not a lease".to_vec())
            .await
            .unwrap();
        fixture.manager.sweep_dead_leaders().await.unwrap();
        fixture.manager.fleet_posture.set(true).unwrap();
        let fleet = &fixture.manager.state_json()["fleet"];
        assert_eq!(fleet["complete"], false);
        // What the pass did read is still reported.
        assert_eq!(fleet["obligations"], serde_json::json!({"a": ["live/g1"]}));
        fixture.stop().await;
    });
}

#[test]
fn bucket_posture_and_a_fleet_before_its_first_pass_report_no_fleet_view() {
    run(async {
        let fixture = Fixture::new();
        // Fleet posture, no pass yet.
        fixture.manager.fleet_posture.set(true).unwrap();
        let state = fixture.manager.state_json();
        assert!(state["fleet"].is_null());
        assert!(state["own"].is_null());
        fixture.stop().await;

        // Bucket posture runs no sweep, and reports none even if one ran.
        let fixture = Fixture::new();
        fixture.manager.sweep_dead_leaders().await.unwrap();
        fixture.manager.fleet_posture.set(false).unwrap();
        let state = fixture.manager.state_json();
        assert_eq!(state["posture"], "bucket");
        assert!(state["fleet"].is_null());
        fixture.stop().await;
    });
}

#[test]
fn internal_state_carries_the_node_log_view_in_every_phase() {
    run(async {
        let fixture = Fixture::new();
        fixture.manager.fleet_posture.set(true).unwrap();
        let shutdown = serde_json::json!({"schema_version": 1});
        let state = crate::actor::internal_state_json(
            Some(r#"{"owned_cells": 3}"#),
            shutdown.clone(),
            state_json(Some(&fixture.manager)),
        );
        assert_eq!(state["owned_cells"], 3);
        assert_eq!(state["shutdown"], shutdown);
        assert_eq!(state["node_log"]["posture"], "fleet");
        assert_eq!(state["node_log"]["session"], "observer/g");
        // The terminal control-only phase has no actor snapshot and still
        // answers the node-log view from memory.
        let control_only = crate::actor::internal_state_json(
            None,
            shutdown.clone(),
            state_json(Some(&fixture.manager)),
        );
        assert_eq!(control_only["node_log"]["session"], "observer/g");
        assert!(control_only.get("owned_cells").is_none());
        // No manager: an explicit null, never a missing key.
        let without = crate::actor::internal_state_json(None, shutdown, state_json(None));
        assert!(without["node_log"].is_null());
        assert!(without.as_object().unwrap().contains_key("node_log"));
        fixture.stop().await;
    });
}
