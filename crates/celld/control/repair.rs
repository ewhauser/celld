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
//! and is left alone, which keeps every cell that can activate, and every
//! cell a live node owns, untouched. The write is a conditional write on the
//! token just read, so it never overwrites a record a node changed since.

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

/// Walk every cell in the bucket and raise each ownership record that is
/// behind the cell's newest epoch. `each` sees every repaired cell as it is
/// written (or found, with `dry_run`).
///
/// It refuses while a node that stopped without recovery still holds an
/// open log. Recovering that log writes the cell's acknowledged writes at
/// the dead node's epoch, and a takeover normally waits for it because the
/// owner record names that node. A repaired record names no node, so a cell
/// activated after the repair would not wait, and the recovery would write
/// into an epoch the cell had already left.
pub async fn repair_epochs(
    bucket: &Bucket,
    dry_run: bool,
    mut each: impl FnMut(&Repaired) -> anyhow::Result<()>,
) -> anyhow::Result<Report> {
    let unrecovered = unrecovered_logs(bucket).await?;
    if !unrecovered.is_empty() {
        bail!(
            "node logs of stopped nodes are not recovered yet: {}. Start the fleet, or \
             leave it running, until those logs are sealed, then run repair-epochs again",
            unrecovered.join(", ")
        );
    }
    let mut report = Report::default();
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
                let repaired = repair_cell(bucket, &cell, dry_run)
                    .await
                    .with_context(|| format!("repair the epoch of {cell}"))?;
                anyhow::Ok(repaired)
            })
            .buffer_unordered(CONCURRENCY);
        while let Some(result) = checks.next().await {
            report.scanned += 1;
            if let Some(repaired) = result? {
                report.repaired += 1;
                each(&repaired)?;
            }
        }
        cursor = page.page_token;
        if cursor.is_none() {
            return Ok(report);
        }
    }
}

/// Nodes whose lease expired with a log that is still open or mid-recovery.
async fn unrecovered_logs(bucket: &Bucket) -> anyhow::Result<Vec<String>> {
    use celld_logic::log_tier::LogState;
    let now = now_ms();
    let mut unrecovered = Vec::new();
    for node in crate::fleet::node_lease_ids(bucket).await? {
        let Some(lease) = load_node_lease(bucket, &node).await? else {
            continue;
        };
        if lease.expires_ms <= now
            && matches!(
                lease.log_state,
                Some(LogState::Open) | Some(LogState::Recovering)
            )
        {
            unrecovered.push(node);
        }
    }
    Ok(unrecovered)
}

/// Raise one cell's ownership record when the bucket is ahead of it.
async fn repair_cell(
    bucket: &Bucket,
    cell: &str,
    dry_run: bool,
) -> anyhow::Result<Option<Repaired>> {
    let Some(newest) = newest_epoch(bucket, cell).await? else {
        return Ok(None);
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
            return Ok(None);
        }
        let owner = record
            .get("node")
            .and_then(Value::as_str)
            .filter(|node| !node.is_empty())
            .map(str::to_string);
        let repaired = Repaired {
            cell: cell.to_string(),
            from,
            owner,
            to: newest,
        };
        if dry_run {
            return Ok(Some(repaired));
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
            return Ok(Some(repaired));
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
