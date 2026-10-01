// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! `celld control migrate`: move a fleet's coordination records between the
//! bucket and a DynamoDB table, in either direction.
//!
//! A fleet moves in one short stop and a lazy copy of its ownership records
//! (see `docs/design/dynamodb-control-plane.md`, Migration). Copying ten
//! million ownership records up front would take hours of listing, so only
//! the few fleet records move while the fleet is stopped:
//!
//! 1. The command refuses while any node lease is unexpired.
//! 2. It copies every node lease (expired ones and tombstones included,
//!    because recovery reads their folded logs), the drain token, the waker
//!    role, the deploy pointers and the queue attachments, verbatim.
//! 3. It rewrites `fleet/control.json` to name the new store, with
//!    `migrating` naming the old one, and deletes the old copies of the
//!    records it moved.
//! 4. Nodes start on the new store. While the marker says `migrating`, a
//!    read of an ownership record the new store lacks copies it from the
//!    old store with a conditional create ([`super::copy_owner`]). The old
//!    copy is frozen, because no node writes the old store, so every copier
//!    writes the same bytes and one wins.
//! 5. The node that holds the waker role walks the old store's ownership
//!    records, copies each one that is still there and deletes the old copy,
//!    and then clears `migrating`. Owner reads stop consulting the old store.

use super::*;
use crate::ownership_store::now_ms;
use futures_util::StreamExt;

/// Ownership records copied at once by the walk.
const CONCURRENCY: usize = 16;

/// Cells per page of the walk over `cells/`.
const PAGE: usize = 1000;

/// What a migration command did.
#[derive(Clone, Debug)]
pub struct Migrated {
    pub from: Backend,
    pub to: Backend,
    /// Fleet records (leases, singletons, pointers) moved by this run.
    pub moved: usize,
    /// This run finished a migration an earlier run had started.
    pub resumed: bool,
}

/// The store a marker's `migrating` names, opened for this process.
pub(super) async fn source(
    bucket: &Bucket,
    backend: &Backend,
    migrating: &Migrating,
    role: Role,
    settings: &Settings,
    transport: &Option<Arc<dyn Transport>>,
) -> anyhow::Result<Source> {
    match (migrating.from.as_str(), backend) {
        ("bucket", Backend::DynamoDb { .. }) => Ok(Source::Bucket),
        ("dynamodb", Backend::Bucket) => {
            let name = migrating
                .table
                .as_deref()
                .with_context(|| format!("{MARKER_KEY} migrates from a table without naming it"))?;
            let fleet = migrating
                .fleet
                .as_deref()
                .with_context(|| format!("{MARKER_KEY} migrates from a table without a fleet"))?;
            let region = migrating
                .region
                .clone()
                .map_or_else(|| table_region(bucket, None, settings), Ok)?;
            let app = (role == Role::Lease).then_some("celld-lease");
            let table = table_for(bucket, name, region, settings, transport, app)?;
            table.verify_claim(fleet).await?;
            Ok(Source::Table(Arc::new(table)))
        }
        (from, backend) => bail!("{MARKER_KEY} migrates from {from} to {backend}"),
    }
}

/// The marker and its token, read from the bucket itself.
async fn read_marker_with_token(bucket: &Bucket) -> anyhow::Result<Option<(Marker, String)>> {
    let Some((bytes, token)) = bucket.get_bucket_object(MARKER_KEY).await? else {
        return Ok(None);
    };
    let marker = serde_json::from_slice(&bytes).with_context(|| format!("decode {MARKER_KEY}"))?;
    Ok(Some((marker, token)))
}

/// One store's copy of the fleet records: the leases, the singletons, the
/// pointers and the attachments. Ownership records are not in it.
enum Store<'a> {
    Bucket(&'a Bucket),
    Table(&'a Table),
}

impl Store<'_> {
    /// Every fleet record this store holds, as bucket keys and bodies.
    async fn fleet_records(&self) -> anyhow::Result<Vec<(String, Bytes)>> {
        let mut records = Vec::new();
        match self {
            Store::Bucket(bucket) => {
                let mut keys = Vec::new();
                for prefix in ["nodes/", "deploy/"] {
                    keys.extend(
                        bucket
                            .list_bucket_objects(prefix)
                            .await?
                            .into_iter()
                            .map(|object| object.location.to_string()),
                    );
                }
                keys.push("drain/token.json".to_string());
                keys.push("wake/waker.json".to_string());
                for key in keys {
                    if !matches!(ControlKey::parse(&key), Some(record) if !matches!(record, ControlKey::Owner(_)))
                    {
                        continue;
                    }
                    if let Some((body, _)) = bucket.get_bucket_object(&key).await? {
                        records.push((key, body));
                    }
                }
            }
            Store::Table(table) => {
                let mut partitions = table.lease_pks();
                partitions.extend([FLEET_PK.to_string(), DEPLOY_PK.to_string()]);
                for pk in &partitions {
                    for (sk, record) in table.query(pk).await? {
                        if let Some(key) = ControlKey::object_key(pk, &sk) {
                            records.push((key, record.body));
                        }
                    }
                }
            }
        }
        records.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(records)
    }

    async fn put(&self, key: &str, body: Bytes) -> anyhow::Result<()> {
        match self {
            Store::Bucket(bucket) => bucket.put_bucket_object(key, body).await,
            Store::Table(table) => {
                let record = ControlKey::parse(key).context("not a coordination record")?;
                table.put_record(&record, &body).await
            }
        }
    }

    async fn delete(&self, key: &str) -> anyhow::Result<()> {
        match self {
            Store::Bucket(bucket) => bucket.delete_bucket_object(key).await,
            Store::Table(table) => {
                let record = ControlKey::parse(key).context("not a coordination record")?;
                table.delete_record(&record, None).await.map(|_| ())
            }
        }
    }

    /// Delete every fleet record of this store, which a migration left.
    async fn clear_fleet_records(&self) -> anyhow::Result<usize> {
        let records = self.fleet_records().await?;
        for (key, _) in &records {
            self.delete(key).await?;
        }
        Ok(records.len())
    }
}

/// The leases in `records` that have not expired.
fn live_leases(records: &[(String, Bytes)]) -> Vec<String> {
    let now = now_ms();
    records
        .iter()
        .filter_map(|(key, body)| {
            let ControlKey::Lease(node) = ControlKey::parse(key)? else {
                return None;
            };
            let lease: Value = serde_json::from_slice(body).ok()?;
            let expires = lease.get("expires_ms").and_then(Value::as_u64)?;
            (expires > now).then_some(node)
        })
        .collect()
}

/// `celld control migrate --to BACKEND`.
pub async fn migrate(
    bucket: &Bucket,
    settings: &Settings,
    to: Backend,
    create_table: bool,
) -> anyhow::Result<Migrated> {
    migrate_with(bucket, settings, to, create_table, None).await
}

pub(crate) async fn migrate_with(
    bucket: &Bucket,
    settings: &Settings,
    to: Backend,
    create_table: bool,
    transport: Option<Arc<dyn Transport>>,
) -> anyhow::Result<Migrated> {
    let current = read_marker_with_token(bucket).await?;
    let (marker, token) = match &current {
        Some((marker, token)) => (Some(marker), Some(token.as_str())),
        None => (None, None),
    };
    let from = match marker {
        Some(marker) => marker.backend()?,
        // A bucket fleet that never ran a release with the marker.
        None => Backend::Bucket,
    };
    let open =
        |name: &str, region: String| table_for(bucket, name, region, settings, &transport, None);

    if let Some(migrating) = marker.and_then(|marker| marker.migrating.as_ref()) {
        // An earlier run switched the marker. Finish what it left: the old
        // copies of the fleet records, which no node reads any more.
        ensure!(
            from == to,
            "this fleet is still migrating to {from}; the nodes finish copying its ownership \
             records on their own, and `celld control show` reports when they are done"
        );
        let source = source(
            bucket,
            &from,
            migrating,
            Role::Operator,
            settings,
            &transport,
        )
        .await?;
        let previous = match &source {
            Source::Bucket => {
                Store::Bucket(bucket).clear_fleet_records().await?;
                Backend::Bucket
            }
            Source::Table(table) => {
                Store::Table(table).clear_fleet_records().await?;
                Backend::DynamoDb {
                    table: table.name().to_string(),
                }
            }
        };
        return Ok(Migrated {
            from: previous,
            to,
            moved: 0,
            resumed: true,
        });
    }
    ensure!(from != to, "this fleet already keeps its records in {to}");

    // The store the records leave.
    let old_table = match &from {
        Backend::Bucket => None,
        Backend::DynamoDb { table } => {
            let marker = marker.expect("a table fleet has a marker");
            let fleet = marker
                .fleet
                .as_deref()
                .with_context(|| format!("{MARKER_KEY} selects a table without a fleet id"))?;
            let table = open(table, table_region(bucket, Some(marker), settings)?)?;
            table.verify_claim(fleet).await?;
            Some(table)
        }
    };
    let old = match &old_table {
        Some(table) => Store::Table(table),
        None => Store::Bucket(bucket),
    };
    let records = old.fleet_records().await?;
    let moving = |key: &str| records.iter().any(|(moved, _)| moved == key);
    let live = live_leases(&records);
    if !live.is_empty() {
        bail!(
            "stop every node before migrating; these node leases have not expired: {}",
            live.join(", ")
        );
    }

    // The store the records move to, which must hold none yet.
    let (new_table, next) = match &to {
        Backend::Bucket => {
            // A record an interrupted run of this command copied is
            // overwritten again; any other one belongs to someone else.
            if let Some((key, _)) = Store::Bucket(bucket)
                .fleet_records()
                .await?
                .into_iter()
                .find(|(key, _)| !moving(key))
            {
                bail!("the bucket already holds the coordination record {key}");
            }
            let old_table = old_table.as_ref().expect("a table fleet's table");
            let marker = marker.expect("a table fleet has a marker");
            (
                None,
                Marker {
                    format: MARKER_FORMAT,
                    backend: to.name().to_string(),
                    table: None,
                    region: None,
                    fleet: None,
                    migrating: Some(Migrating {
                        from: from.name().to_string(),
                        table: Some(old_table.name().to_string()),
                        region: Some(old_table.region().to_string()),
                        fleet: marker.fleet.clone(),
                    }),
                },
            )
        }
        Backend::DynamoDb { table: name } => {
            let region = table_region(bucket, None, settings)?;
            let table = open(name, region.clone())?;
            if create_table && table.create().await? {
                tracing::info!(table = %name, "created the control table");
            }
            table.check_shape().await?;
            let fleet = table
                .claim(
                    &random_fleet_id(),
                    &bucket_identity(bucket),
                    settings.lease_shards.unwrap_or(1),
                )
                .await?;
            if let Some(found) = table.first_record(&moving).await? {
                bail!("dynamodb://{name} already holds the record {found}; migrate into an empty table");
            }
            table.probe().await?;
            (
                Some(table),
                Marker {
                    format: MARKER_FORMAT,
                    backend: to.name().to_string(),
                    table: Some(name.clone()),
                    region: Some(region),
                    fleet: Some(fleet),
                    migrating: Some(Migrating {
                        from: from.name().to_string(),
                        table: None,
                        region: None,
                        fleet: None,
                    }),
                },
            )
        }
    };
    let new = match &new_table {
        Some(table) => Store::Table(table),
        None => Store::Bucket(bucket),
    };
    for (key, body) in &records {
        new.put(key, body.clone()).await?;
    }
    let body = serde_json::to_vec(&next)?;
    ensure!(
        bucket
            .put_cas_bucket_object(MARKER_KEY, body, token)
            .await?
            .is_some(),
        "{MARKER_KEY} changed during the migration; run it again"
    );
    tracing::info!(
        event = "control_migration_started",
        from = %from,
        to = %to,
        records = records.len(),
        "moved the fleet records; nodes copy ownership records on first touch"
    );
    old.clear_fleet_records().await?;
    Ok(Migrated {
        from,
        to,
        moved: records.len(),
        resumed: false,
    })
}

/// Finish a migration from a serving node: walk the old store's ownership
/// records while this node holds the waker role, then clear `migrating`.
/// Returns once the fleet is no longer migrating.
pub async fn run_migration(bucket: Bucket, node: String, tick_ms: u64) {
    let mut tick = crate::asyncrt::interval(Duration::from_millis(tick_ms.max(1)));
    tick.set_missed_tick_behavior(crate::asyncrt::MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        match migration_pass(&bucket, &node, tick_ms).await {
            Ok(true) => return,
            Ok(false) => {}
            Err(error) => tracing::warn!(%error, "control migration pass failed"),
        }
    }
}

/// One attempt to finish the migration. `Ok(true)` once there is none.
pub(crate) async fn migration_pass(
    bucket: &Bucket,
    node: &str,
    tick_ms: u64,
) -> anyhow::Result<bool> {
    let Some((mut marker, token)) = read_marker_with_token(bucket).await? else {
        bucket.control_route().finish_migration();
        return Ok(true);
    };
    if marker.migrating.is_none() {
        // Another node finished it.
        bucket.control_route().finish_migration();
        return Ok(true);
    }
    let Some(source) = bucket.control_route().migration() else {
        // This client resolved without the migration; leave it to a node
        // that resolved with it.
        return Ok(true);
    };
    let ttl_ms = tick_ms.saturating_mul(3).min(i64::MAX as u64) as i64;
    let hold = || crate::wake::try_hold_waker(bucket, node, now_ms() as i64, ttl_ms);
    if !hold().await {
        return Ok(false);
    }
    let mut renewal = crate::asyncrt::interval(Duration::from_millis((tick_ms).max(1)));
    renewal.set_missed_tick_behavior(crate::asyncrt::MissedTickBehavior::Delay);
    renewal.tick().await;
    let walk = walk(bucket, &source);
    tokio::pin!(walk);
    let copied = loop {
        crate::asyncrt::select_biased! {
            "a due waker-lease renewal wins a tie with the end of the walk";
            _ = renewal.tick() => {
                if !hold().await {
                    tracing::warn!("lost the waker role; another node finishes the migration");
                    return Ok(false);
                }
            },
            copied = &mut walk => break copied?,
        }
    };
    marker.migrating = None;
    let body = serde_json::to_vec(&marker)?;
    if bucket
        .put_cas_bucket_object(MARKER_KEY, body, Some(&token))
        .await?
        .is_none()
    {
        return Ok(false);
    }
    bucket.control_route().finish_migration();
    tracing::info!(
        event = "control_migration_finished",
        owners = copied,
        "every ownership record is in the fleet's store"
    );
    Ok(true)
}

/// Copy every ownership record still in the old store and delete the old
/// copy. Answers how many it found.
async fn walk(bucket: &Bucket, source: &Source) -> anyhow::Result<u64> {
    let mut found = 0;
    match source {
        Source::Bucket => {
            let mut cursor = None;
            loop {
                let page = bucket
                    .common_prefixes_page("cells/", None, cursor, PAGE)
                    .await?;
                let mut results = futures_util::stream::iter(page.prefixes)
                    .map(|prefix| async move {
                        let key = format!("{}/own.json", prefix.trim_end_matches('/'));
                        if bucket.get_bucket_object(&key).await?.is_none() {
                            return anyhow::Ok(false);
                        }
                        // The routed read copies it into the table first.
                        ensure!(
                            bucket.get(&key).await?.is_some(),
                            "{key} did not reach the table"
                        );
                        bucket.delete_bucket_object(&key).await?;
                        Ok(true)
                    })
                    .buffer_unordered(CONCURRENCY);
                while let Some(result) = results.next().await {
                    found += u64::from(result?);
                }
                cursor = page.page_token;
                if cursor.is_none() {
                    return Ok(found);
                }
            }
        }
        Source::Table(table) => {
            let mut after = None;
            loop {
                let (owners, next) = table.owner_page(after).await?;
                let mut results = futures_util::stream::iter(owners)
                    .map(|(cell, record)| async move {
                        let key = format!("cells/{cell}/own.json");
                        // The routed read copies it into the bucket first.
                        ensure!(
                            bucket.get(&key).await?.is_some(),
                            "{key} did not reach the bucket"
                        );
                        table
                            .delete_record(&ControlKey::Owner(cell), Some(&record.token))
                            .await?;
                        anyhow::Ok(())
                    })
                    .buffer_unordered(CONCURRENCY);
                while let Some(result) = results.next().await {
                    result?;
                    found += 1;
                }
                match next {
                    Some(next) => after = Some(next),
                    None => return Ok(found),
                }
            }
        }
    }
}
