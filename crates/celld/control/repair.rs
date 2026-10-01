// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! `celld control repair-epochs`: raise ownership records that fell behind
//! the data they govern.
//!
//! Every acquire writes the owner record at `epoch + 1` and then writes LTX
//! under `cells/<cell>/ltx/e<epoch+1>/`, and restore refuses to write into
//! an epoch at or below the newest one the bucket already holds. The owner
//! record can only fall behind the bucket when it is rolled back on its own:
//! a control table restored from a point-in-time backup is the case this
//! exists for, because the table then holds epochs older than the LTX the
//! fleet wrote after the backup. Such a cell cannot activate: every claim
//! lands on an epoch the bucket has already used.
//!
//! The repair is the epoch-floor rule of the design: for each cell whose
//! newest non-empty epoch is above its owner record's epoch, write the
//! record unowned at that newest epoch, so the next acquire claims the epoch
//! after it. A record at or above the bucket's newest epoch is consistent
//! and is left alone, which keeps every cell that can activate untouched.
//!
//! The fleet must be stopped. A restore can leave a live node serving a cell
//! at an epoch its rolled-back record no longer shows, and a node can
//! activate a root at an epoch below a dormant facet's data, because facets
//! restore on demand. Clearing either record would let a second node claim
//! the cell while the first still serves it. So the repair refuses while any
//! lease is live, and while any stopped node's log is unrecovered, since a
//! record written unowned lets the next activation skip that recovery.
//!
//! A node can still start during the walk. Before each write the repair
//! reads the lease of the node the record names and leaves the record alone
//! unless that lease has expired with its log sealed: an expired lease never
//! renews, and that node can only take the cell back by changing the record,
//! which fails the conditional write on the token just read. Records left
//! alone are reported, and the command fails, so it can be run again once
//! those nodes have stopped.

use crate::bucket::Bucket;
use crate::ownership_store::{load_node_lease, now_ms};
use anyhow::{bail, ensure, Context};
use futures_util::StreamExt;
use serde_json::{Map, Value};

/// Cells checked at once. Each check is a few listings and one read.
const CONCURRENCY: usize = 16;

/// Cells per page of the `cells/` walk.
const PAGE: usize = 1000;

/// Conditional-write attempts for one cell before it is reported as busy.
const ATTEMPTS: usize = 3;

/// One cell whose ownership record was raised, or would be in a dry run.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Repaired {
    pub cell: String,
    /// The record's epoch before the repair, `None` when it had none.
    pub from: Option<u64>,
    /// The node the record named before the repair, if any.
    pub owner: Option<String>,
    /// The newest epoch the bucket holds, now the record's epoch.
    pub to: u64,
}

/// What a repair pass found.
#[derive(Debug, Default)]
pub struct Report {
    pub scanned: u64,
    pub repaired: u64,
}

/// What one cell's check found.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Outcome {
    /// The record is consistent with the bucket, or the cell has no data.
    Consistent,
    Repaired(Repaired),
    /// The record is behind, but names a node whose lease is live or whose
    /// log is not sealed, so it was left alone.
    Held {
        owner: String,
    },
}

/// Walk every cell in the bucket and raise each ownership record that is
/// behind the cell's newest epoch. `each` sees every repaired cell as it is
/// written (or found, with `dry_run`).
///
/// It refuses while any node lease is live, and while a node that stopped
/// without recovery still holds an open log. Recovering that log writes the
/// cell's acknowledged writes at the dead node's epoch, and a takeover
/// normally waits for it because the owner record names that node. A
/// repaired record names no node, so a cell activated after the repair would
/// not wait, and the recovery would write into an epoch the cell had already
/// left. It fails after the walk when it left any behind record alone.
pub async fn repair_epochs(
    bucket: &Bucket,
    dry_run: bool,
    mut each: impl FnMut(&Repaired) -> anyhow::Result<()>,
) -> anyhow::Result<Report> {
    let leases = lease_states(bucket).await?;
    let live: Vec<&str> = leases
        .iter()
        .filter(|(_, state)| *state == LeaseState::Live)
        .map(|(node, _)| node.as_str())
        .collect();
    if !live.is_empty() {
        bail!(
            "nodes are running: {}. Repairing epochs needs a stopped fleet, because a running \
             node can serve a cell at an epoch its restored record no longer shows. Stop every \
             node, let its lease expire, then run repair-epochs again",
            live.join(", ")
        );
    }
    let unrecovered: Vec<&str> = leases
        .iter()
        .filter(|(_, state)| *state == LeaseState::Unrecovered)
        .map(|(node, _)| node.as_str())
        .collect();
    if !unrecovered.is_empty() {
        bail!(
            "node logs of stopped nodes are not recovered yet: {}. Start one node until those \
             logs are sealed, stop it, then run repair-epochs again",
            unrecovered.join(", ")
        );
    }
    let mut report = Report::default();
    let mut held = Vec::new();
    let mut cursor = None;
    loop {
        let page = bucket
            .common_prefixes_page("cells/", None, cursor, PAGE)
            .await
            .context("enumerate cells")?;
        let mut cells = Vec::with_capacity(page.prefixes.len());
        for prefix in page.prefixes {
            let cell = prefix
                .strip_prefix("cells/")
                .context("a cell listing answered outside cells/")?
                .trim_end_matches('/')
                .to_string();
            ensure!(
                celld_logic::cell::valid_cell_scope(&cell),
                "invalid cell identity under cells/: {cell:?}"
            );
            cells.push(cell);
        }
        let mut checks = futures_util::stream::iter(cells)
            .map(|cell| async move {
                let outcome = repair_cell(bucket, &cell, dry_run)
                    .await
                    .with_context(|| format!("repair the epoch of {cell}"))?;
                anyhow::Ok((cell, outcome))
            })
            .buffer_unordered(CONCURRENCY);
        while let Some(result) = checks.next().await {
            report.scanned += 1;
            match result? {
                (_, Outcome::Consistent) => {}
                (_, Outcome::Repaired(repaired)) => {
                    report.repaired += 1;
                    each(&repaired)?;
                }
                (cell, Outcome::Held { owner }) => held.push(format!("{cell} (owned by {owner})")),
            }
        }
        cursor = page.page_token;
        if cursor.is_none() {
            break;
        }
    }
    if !held.is_empty() {
        bail!(
            "{} cells are behind the bucket but owned by nodes that started during the repair: \
             {}. Stop those nodes, then run repair-epochs again",
            held.len(),
            held.join(", ")
        );
    }
    Ok(report)
}

/// Where a node lease stands for the repair.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LeaseState {
    /// The node may still be running.
    Live,
    /// Expired, with a log that is still open or mid-recovery.
    Unrecovered,
    /// Expired, with a sealed log or none.
    Stopped,
}

/// The state of one node's lease, `None` when it has none.
async fn lease_state(bucket: &Bucket, node: &str) -> anyhow::Result<Option<LeaseState>> {
    use celld_logic::log_tier::LogState;
    let Some(lease) = load_node_lease(bucket, node).await? else {
        return Ok(None);
    };
    Ok(Some(if lease.expires_ms > now_ms() {
        LeaseState::Live
    } else if matches!(
        lease.log_state,
        Some(LogState::Open) | Some(LogState::Recovering)
    ) {
        LeaseState::Unrecovered
    } else {
        LeaseState::Stopped
    }))
}

/// Every node lease and its state.
async fn lease_states(bucket: &Bucket) -> anyhow::Result<Vec<(String, LeaseState)>> {
    let mut states = Vec::new();
    for node in crate::fleet::node_lease_ids(bucket).await? {
        if let Some(state) = lease_state(bucket, &node).await? {
            states.push((node, state));
        }
    }
    Ok(states)
}

/// Raise one cell's ownership record when the bucket is ahead of it.
pub(crate) async fn repair_cell(
    bucket: &Bucket,
    cell: &str,
    dry_run: bool,
) -> anyhow::Result<Outcome> {
    let Some(newest) = newest_epoch(bucket, cell).await? else {
        return Ok(Outcome::Consistent);
    };
    let key = format!("cells/{cell}/own.json");
    for _ in 0..ATTEMPTS {
        let current = bucket.get(&key).await?;
        let (mut record, token) = match &current {
            Some((body, token)) => (
                serde_json::from_slice::<Map<String, Value>>(body)
                    .with_context(|| format!("decode {key}"))?,
                Some(token.as_str()),
            ),
            None => (Map::new(), None),
        };
        let from = match &current {
            Some(_) => Some(
                record
                    .get("epoch")
                    .and_then(Value::as_u64)
                    .with_context(|| format!("{key} has no epoch"))?,
            ),
            None => None,
        };
        // The next acquire claims one past the record, or epoch 1 when there
        // is none. A record at the newest epoch is the one that wrote it, and
        // a cell with no record and only a preview's epoch 0 activates as it
        // always has.
        if newest <= from.unwrap_or(0) {
            return Ok(Outcome::Consistent);
        }
        let owner = record
            .get("node")
            .and_then(Value::as_str)
            .filter(|node| !node.is_empty())
            .map(str::to_string);
        // Read after the record: a node that holds the cell now has a live
        // lease, or an unsealed log, or changes the record before it could
        // hold it again, which fails the write below.
        if let Some(owner) = &owner {
            match lease_state(bucket, owner).await? {
                None | Some(LeaseState::Stopped) => {}
                Some(LeaseState::Live | LeaseState::Unrecovered) => {
                    return Ok(Outcome::Held {
                        owner: owner.clone(),
                    })
                }
            }
        }
        let repaired = Repaired {
            cell: cell.to_string(),
            from,
            owner,
            to: newest,
        };
        if dry_run {
            return Ok(Outcome::Repaired(repaired));
        }
        // Unowned, as a release writes it: the next acquire claims
        // `newest + 1`, which no stream holds yet. Unknown fields stay.
        record.insert("node".into(), Value::String(String::new()));
        record.insert("epoch".into(), Value::from(newest));
        let body = serde_json::to_vec(&record)?;
        if bucket.put_cas(&key, body, token).await?.is_some() {
            tracing::info!(
                event = "control_epoch_repaired",
                cell,
                from = ?repaired.from,
                to = newest,
                "raised an ownership record to the bucket's newest epoch"
            );
            return Ok(Outcome::Repaired(repaired));
        }
        // A node changed the record since it was read; judge it again.
    }
    bail!("{key} kept changing while it was repaired; run repair-epochs again")
}

/// The newest epoch that holds any LTX for `scope` or any facet below it.
/// A facet replicates at its root's epoch, so its streams count against the
/// root's record exactly as the root's own do.
async fn newest_epoch(bucket: &Bucket, scope: &str) -> anyhow::Result<Option<u64>> {
    let mut newest = bucket
        .common_prefixes(&format!("cells/{scope}/ltx/"))
        .await?
        .iter()
        .filter_map(|prefix| {
            prefix
                .trim_end_matches('/')
                .rsplit('/')
                .next()
                .and_then(|name| name.strip_prefix('e'))
                .and_then(|epoch| epoch.parse::<u64>().ok())
        })
        .max();
    for facet in bucket
        .common_prefixes(&format!("cells/{scope}/facets/"))
        .await?
    {
        let Some(facet) = facet
            .trim_end_matches('/')
            .strip_prefix("cells/")
            .map(str::to_string)
        else {
            continue;
        };
        if let Some(epoch) = Box::pin(newest_epoch(bucket, &facet)).await? {
            newest = newest.max(Some(epoch));
        }
    }
    Ok(newest)
}
