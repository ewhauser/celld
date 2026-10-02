// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The replication loops' dirty sets: a wake drains the cells ticketed since
//! the last one, never the registry, and no ticket is left untracked.

use super::*;
use object_store::memory::InMemory;
use std::time::Instant;

/// A registered cell without a database: the dirty sets read only its
/// watermarks and flags.
fn cell(client: &SharedObjectStoreClient, name: &str, epoch: u64) -> CellHandle {
    Arc::new(Cell {
        snapshot_declined: AtomicBool::new(false),
        paged_vfs: None,
        hydration: None,
        replica: Mutex::new(None),
        client: client.clone(),
        req_seq: AtomicU64::new(0),
        synced_seq: AtomicU64::new(0),
        shipped_seq: AtomicU64::new(0),
        submitted_seq: AtomicU64::new(0),
        shipped_txid: Arc::new(AtomicU64::new(0)),
        submitted_txid: AtomicU64::new(0),
        durable_txid: Arc::new(AtomicU64::new(0)),
        percell_txid: AtomicU64::new(0),
        syncing: AtomicBool::new(false),
        last_sync_ms: AtomicU64::new(0),
        capture_seq: AtomicU64::new(0),
        capture_started_ms: AtomicU64::new(0),
        node_proof_ms: Arc::new(AtomicU64::new(0)),
        ready: Notify::new(),
        compaction: None,
        #[cfg(all(test, celld_internal_tests))]
        sync_credit_pause: Mutex::new(None),
        #[cfg(all(test, celld_internal_tests))]
        observer_cell: name.to_string(),
        #[cfg(all(test, celld_internal_tests))]
        observer_epoch: epoch,
        #[cfg(all(test, celld_internal_tests))]
        durability_ticket_receipts: Mutex::new(Vec::new()),
        #[cfg(all(test, celld_internal_tests))]
        upload_round_receipts: Mutex::new(Vec::new()),
        #[cfg(all(test, celld_internal_tests))]
        fleet_credit_receipts: Mutex::new(Vec::new()),
        #[cfg(all(test, celld_internal_tests))]
        fleet_capture_receipts: Mutex::new(Vec::new()),
        key: (name.to_string(), epoch),
        is_queue: is_queue_cell(name),
        resident: AtomicBool::new(true),
        sync_queued: AtomicBool::new(false),
        ship_queued: AtomicBool::new(false),
        bundle_queued: AtomicBool::new(false),
    })
}

fn client() -> SharedObjectStoreClient {
    SharedObjectStoreClient(Arc::new(ObjectStoreClient::with_store(
        ObjectStoreConfig::default(),
        Arc::new(InMemory::new()),
    )))
}

type Registry = Mutex<BTreeMap<(String, u64), CellHandle>>;

fn registry(client: &SharedObjectStoreClient, residents: usize) -> (Registry, Vec<CellHandle>) {
    let mut map = BTreeMap::new();
    let mut handles = Vec::with_capacity(residents);
    for n in 0..residents {
        let handle = cell(client, &format!("Cart:c{n:06}"), 1);
        map.insert(handle.key.clone(), handle.clone());
        handles.push(handle);
    }
    (Mutex::new(map), handles)
}

/// What `await_durable` does to a cell.
fn ticket(sets: &DirtySets, cell: &CellHandle) {
    cell.req_seq.fetch_add(1, Ordering::SeqCst);
    sets.ticket(cell);
}

/// What a completed upload does to a cell.
fn credit(cell: &Cell) {
    cell.synced_seq
        .fetch_max(cell.req_seq.load(Ordering::SeqCst), Ordering::SeqCst);
}

fn keys(work: &[((String, u64), CellHandle)]) -> Vec<&str> {
    work.iter().map(|((name, _), _)| name.as_str()).collect()
}

#[test]
fn a_cell_is_queued_once_until_drained() {
    let client = client();
    let sets = DirtySets::new(true);
    let a = cell(&client, "Cart:a", 1);
    let mut scratch = Vec::new();
    ticket(&sets, &a);
    ticket(&sets, &a);
    assert_eq!(
        keys(&sets.ship.drain_owed(&mut scratch, ship_owed)),
        ["Cart:a"]
    );
    assert!(sets.ship.drain_owed(&mut scratch, ship_owed).is_empty());
    // Draining released the flag: the next ticket queues the cell again.
    ticket(&sets, &a);
    assert_eq!(
        keys(&sets.ship.drain_owed(&mut scratch, ship_owed)),
        ["Cart:a"]
    );
    // Each loop drains its own set.
    assert_eq!(
        keys(&sets.bundle.drain_owed(&mut scratch, sync_owed)),
        ["Cart:a"]
    );
}

#[test]
fn a_drain_skips_cells_that_left_the_registry_or_are_covered() {
    let client = client();
    let sets = DirtySets::new(true);
    let (gone, covered, live) = (
        cell(&client, "Cart:gone", 1),
        cell(&client, "Cart:covered", 1),
        cell(&client, "Cart:live", 1),
    );
    for handle in [&gone, &covered, &live] {
        ticket(&sets, handle);
    }
    gone.resident.store(false, Ordering::SeqCst);
    credit(&covered);
    let mut scratch = Vec::new();
    assert_eq!(
        keys(&sets.ship.drain_owed(&mut scratch, ship_owed)),
        ["Cart:live"]
    );
}

#[test]
fn an_uncredited_cell_is_requeued_without_a_ticket() {
    let client = client();
    let sets = DirtySets::new(true);
    let (failed, shipped) = (
        cell(&client, "Cart:failed", 1),
        cell(&client, "Cart:shipped", 1),
    );
    ticket(&sets, &failed);
    ticket(&sets, &shipped);
    let mut scratch = Vec::new();
    let work = sets.ship.drain_owed(&mut scratch, ship_owed);
    assert_eq!(work.len(), 2);
    // The round took `shipped` and failed to capture `failed`.
    shipped.submitted_seq.store(1, Ordering::SeqCst);
    sets.ship
        .requeue_owed(work.into_iter().map(|(_, cell)| cell).collect(), ship_owed);
    assert_eq!(
        keys(&sets.ship.drain_owed(&mut scratch, ship_owed)),
        ["Cart:failed"]
    );
}

#[test]
fn a_pipeline_reset_requeues_the_rolled_back_cells() {
    let client = client();
    let sets = DirtySets::new(true);
    let (registry, handles) = registry(&client, 4);
    let mut scratch = Vec::new();
    for handle in &handles {
        ticket(&sets, handle);
    }
    // One round submitted every cell; then the pipeline failed.
    for (_, handle) in sets.ship.drain_owed(&mut scratch, ship_owed) {
        handle.submitted_seq.store(1, Ordering::SeqCst);
    }
    assert!(sets.ship.drain_owed(&mut scratch, ship_owed).is_empty());
    handles[3].shipped_seq.store(1, Ordering::SeqCst);
    reset_submitted(&Arc::new(registry), &sets.ship);
    assert_eq!(
        keys(&sets.ship.drain_owed(&mut scratch, ship_owed)),
        ["Cart:c000000", "Cart:c000001", "Cart:c000002"]
    );
}

#[test]
fn queue_pending_counts_queued_queue_cells() {
    let client = client();
    let sets = DirtySets::new(true);
    let queue = cell(&client, &format!("{}:jobs", crate::deploy::QUEUE_CLASS), 1);
    let cart = cell(&client, "Cart:a", 1);
    assert!(queue.is_queue && !cart.is_queue);
    ticket(&sets, &cart);
    assert!(!sets.ship.queue_pending());
    ticket(&sets, &queue);
    ticket(&sets, &queue);
    assert!(sets.ship.queue_pending());
    let mut scratch = Vec::new();
    assert_eq!(sets.ship.drain_owed(&mut scratch, ship_owed).len(), 2);
    assert!(!sets.ship.queue_pending());
    assert_eq!(sets.ship.queue_cells.load(Ordering::SeqCst), 0);
}

#[test]
fn a_disabled_set_retains_nothing() {
    let client = client();
    let sets = DirtySets::new(false);
    let a = cell(&client, "Cart:a", 1);
    ticket(&sets, &a);
    assert!(sets.bundle.queue.lock().unwrap().is_empty());
    assert!(!a.bundle_queued.load(Ordering::SeqCst));
}

#[test]
fn a_paced_cell_waits_for_its_interval_without_a_scan() {
    let client = client();
    let sets = DirtySets::new(true);
    let (registry, _) = registry(&client, 0);
    let a = cell(&client, "Cart:a", 1);
    a.last_sync_ms.store(1_000, Ordering::SeqCst);
    let mut queue = SyncQueue::default();
    ticket(&sets, &a);
    assert!(queue
        .due(&sets.sync, &registry, Some(100), 1_050)
        .is_empty());
    assert_eq!(queue.deferred.len(), 1);
    // A ticket on a held cell does not queue a second entry.
    ticket(&sets, &a);
    assert!(sets.sync.queue.lock().unwrap().is_empty());
    assert!(queue
        .due(&sets.sync, &registry, Some(100), 1_099)
        .is_empty());
    let due = queue.due(&sets.sync, &registry, Some(100), 1_100);
    assert_eq!(due.len(), 1);
    assert!(queue.deferred.is_empty());
    // Released: the next ticket queues it again.
    ticket(&sets, &a);
    assert_eq!(sets.sync.queue.lock().unwrap().len(), 1);
}

#[test]
fn a_late_sync_redefers_and_unpacing_releases_everything() {
    let client = client();
    let sets = DirtySets::new(true);
    let (registry, _) = registry(&client, 0);
    let (a, b) = (cell(&client, "Cart:a", 1), cell(&client, "Cart:b", 1));
    a.last_sync_ms.store(1_000, Ordering::SeqCst);
    b.last_sync_ms.store(1_000, Ordering::SeqCst);
    let mut queue = SyncQueue::default();
    ticket(&sets, &a);
    ticket(&sets, &b);
    assert!(queue
        .due(&sets.sync, &registry, Some(100), 1_000)
        .is_empty());
    // A direct sync moved `a`'s anchor; its old deadline re-defers it.
    a.last_sync_ms.store(1_080, Ordering::SeqCst);
    let due = queue.due(&sets.sync, &registry, Some(100), 1_100);
    assert!(due.len() == 1 && Arc::ptr_eq(&due[0], &b));
    assert_eq!(queue.deferred.len(), 1);
    // Unpaced (the shipper degraded): every deferred cell is due now.
    let due = queue.due(&sets.sync, &registry, None, 1_101);
    assert!(due.len() == 1 && Arc::ptr_eq(&due[0], &a));
}

#[test]
fn suspending_reseeds_from_the_registry_once() {
    let client = client();
    let sets = DirtySets::new(true);
    let (registry, handles) = registry(&client, 3);
    let mut queue = SyncQueue::default();
    ticket(&sets, &handles[0]);
    handles[1].last_sync_ms.store(1_000, Ordering::SeqCst);
    ticket(&sets, &handles[1]);
    assert_eq!(queue.due(&sets.sync, &registry, Some(100), 1_000).len(), 1);
    // `handles[0]` is claimed, `handles[1]` deferred; then bundling starts.
    ticket(&sets, &handles[2]);
    queue.suspend(&sets.sync);
    assert!(queue.deferred.is_empty() && sets.sync.queue.lock().unwrap().is_empty());
    for handle in &handles {
        assert!(!handle.sync_queued.load(Ordering::SeqCst));
    }
    // The bundle loop credited `handles[2]` meanwhile; the others still owe.
    credit(&handles[2]);
    let due = queue.due(&sets.sync, &registry, None, 2_000);
    assert_eq!(due.len(), 2);
    assert!(!queue.reseed);
}

/// Producers ticket while a consumer drains and credits. A ticket the
/// consumer never saw would leave a cell owed at the end, because nothing
/// here ever scans the registry.
#[test]
fn concurrent_tickets_are_never_lost() {
    let client = client();
    for round in 0..16_u64 {
        let sets = Arc::new(DirtySets::new(true));
        let (_registry, handles) = registry(&client, 16);
        let handles = Arc::new(handles);
        let done = Arc::new(AtomicBool::new(false));
        let consumer = {
            let (sets, done) = (sets.clone(), done.clone());
            std::thread::spawn(move || {
                let mut scratch = Vec::new();
                loop {
                    let finished = done.load(Ordering::SeqCst);
                    for (_, cell) in sets.ship.drain_owed(&mut scratch, sync_owed) {
                        credit(&cell);
                    }
                    if finished {
                        break;
                    }
                }
            })
        };
        let producers: Vec<_> = (0..4_u64)
            .map(|seed| {
                let (sets, handles) = (sets.clone(), handles.clone());
                std::thread::spawn(move || {
                    let mut state = (round * 4 + seed).wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
                    for _ in 0..20_000 {
                        state ^= state << 13;
                        state ^= state >> 7;
                        state ^= state << 17;
                        ticket(&sets, &handles[(state % handles.len() as u64) as usize]);
                    }
                })
            })
            .collect();
        for producer in producers {
            producer.join().unwrap();
        }
        done.store(true, Ordering::SeqCst);
        consumer.join().unwrap();
        for handle in handles.iter() {
            assert!(!sync_owed(handle), "{:?} was left owed", handle.key);
        }
    }
}

/// One steady-state wake of all three loops, as they select work: the sync
/// loop's paced queue, the ship loop's drain and Queue test, the bundle
/// loop's drain. Every hot cell is ticketed and credited per wake.
fn wake(
    sets: &DirtySets,
    registry: &Registry,
    queue: &mut SyncQueue,
    scratch: &mut Vec<CellHandle>,
    hot: &[CellHandle],
) -> usize {
    for cell in hot {
        ticket(sets, cell);
    }
    let synced = queue.due(&sets.sync, registry, None, 0);
    let _ = sets.ship.queue_pending();
    let shipped = sets.ship.drain_owed(scratch, ship_owed);
    let bundled = sets.bundle.drain_owed(scratch, sync_owed);
    for cell in hot {
        credit(cell);
        cell.submitted_seq
            .store(cell.req_seq.load(Ordering::SeqCst), Ordering::SeqCst);
    }
    assert_eq!(synced.len(), hot.len());
    assert_eq!(shipped.len(), hot.len());
    synced.len() + shipped.len() + bundled.len()
}

/// The registry walks this change removed, kept here only as the baseline
/// the scale test compares against.
fn scanning_wake(registry: &Registry, hot: &[CellHandle], sets: &DirtySets) -> usize {
    for cell in hot {
        ticket(sets, cell);
    }
    let synced: Vec<CellHandle> = registry
        .lock()
        .unwrap()
        .values()
        .filter(|cell| sync_owed(cell))
        .cloned()
        .collect();
    let queue_pending = registry
        .lock()
        .unwrap()
        .iter()
        .any(|((name, _), cell)| is_queue_cell(name) && ship_owed(cell));
    let shipped: Vec<((String, u64), CellHandle)> = registry
        .lock()
        .unwrap()
        .iter()
        .filter(|(_, cell)| ship_owed(cell))
        .map(|(key, cell)| (key.clone(), cell.clone()))
        .collect();
    for cell in hot {
        credit(cell);
        cell.submitted_seq
            .store(cell.req_seq.load(Ordering::SeqCst), Ordering::SeqCst);
    }
    synced.len() + shipped.len() + usize::from(queue_pending)
}

/// A wake costs O(dirty cells): with 8 hot cells the work each wake selects
/// and touches is the same at 1,000 and 50,000 residents.
#[test]
fn a_wake_touches_only_the_dirty_cells() {
    let client = client();
    for residents in [1_000, 50_000] {
        let sets = DirtySets::new(true);
        let (registry, handles) = registry(&client, residents);
        let hot: Vec<CellHandle> = handles.iter().step_by(residents / 8).cloned().collect();
        let mut queue = SyncQueue::default();
        let mut scratch = Vec::new();
        for _ in 0..100 {
            assert_eq!(
                wake(&sets, &registry, &mut queue, &mut scratch, &hot),
                3 * hot.len()
            );
        }
    }
}

/// Per-wake selection time at growing residency, against the registry walk
/// it replaced. Timing only; run with
/// `cargo test -p celld --release --lib dirty_tests::wake_cost -- --ignored --nocapture`.
#[test]
#[ignore = "timing benchmark"]
// Offline timing: wall-clock reads and printed results are the point.
#[allow(clippy::disallowed_methods)]
fn wake_cost_is_independent_of_residency() {
    let client = client();
    const WAKES: u32 = 2_000;
    let mut drained_ns = Vec::new();
    for residents in [1_000, 10_000, 50_000] {
        let (registry, handles) = registry(&client, residents);
        let hot: Vec<CellHandle> = handles.iter().step_by(residents / 8).cloned().collect();
        let sets = DirtySets::new(true);
        let mut queue = SyncQueue::default();
        let mut scratch = Vec::new();
        let started = Instant::now();
        for _ in 0..WAKES {
            std::hint::black_box(wake(&sets, &registry, &mut queue, &mut scratch, &hot));
        }
        let drained = started.elapsed() / WAKES;
        let sets = DirtySets::new(true);
        let started = Instant::now();
        for _ in 0..WAKES {
            std::hint::black_box(scanning_wake(&registry, &hot, &sets));
            sets.clear_all_for_test();
        }
        let scanned = started.elapsed() / WAKES;
        eprintln!(
            "residents={residents:>6} hot={} drained={drained:>10.2?}/wake scanned={scanned:>10.2?}/wake",
            hot.len()
        );
        drained_ns.push(drained.as_nanos());
    }
    // 50x the residents may not cost 50x the wake. Generous: noise only.
    assert!(
        drained_ns[2] < drained_ns[0] * 4,
        "per-wake cost grew with residency: {drained_ns:?}"
    );
}

impl DirtySets {
    fn clear_all_for_test(&self) {
        self.sync.clear();
        self.ship.clear();
        self.bundle.clear();
    }
}
