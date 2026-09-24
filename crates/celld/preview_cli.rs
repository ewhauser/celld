// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//! Kubernetes-backed previews. Developers write only CelldPreview objects;
//! an explicitly invoked administrator executor owns seed reservation status.
#![allow(clippy::disallowed_methods)]

use crate::{
    bucket::Bucket,
    cli_output::{Format, Output},
    deploy,
    preview_seed::{self, Alarms, Object},
};
use anyhow::{bail, ensure, Context};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};
mod executor;
mod kubernetes;
use kubernetes::{field, live, uid, Kubernetes};

const API: &str = "celld.eric.dev/v1alpha1";
const PREVIEW: &str = "celldpreviews.celld.eric.dev";
const FLEET: &str = "celldfleets.celld.eric.dev";
const RESERVATION: &str = "celldstoragereservations.celld.eric.dev";
const HELP: &str = "celld preview — deploy through the Kubernetes operator

  celld preview NAME --context CONTEXT --namespace NS --fleet FLEET [OPTIONS]
  celld preview status NAME --context CONTEXT --namespace NS [--json]
  celld preview delete NAME --context CONTEXT --namespace NS
  celld preview seed RESERVATION --context CONTEXT
  celld preview seed --watch --context CONTEXT --namespace NS

Deployment options:
  --config PATH         Wrangler project/config (default: current directory)
  --source TEXT         Source label (default: preview name)
  --revision TEXT       Informational source revision
  --ttl-seconds N       Lifetime from creation (default: 86400; immutable)
  --seed-from ALIAS     Administrator-approved source, only at creation
  --object CLASS:ID     Repeat for up to 100 canonical object IDs
  --alarms Clear|Preserve  Seed alarm policy (default: Clear)
  --timeout-seconds N   Wait for infrastructure and application (default: 600)
  --dry-run            Print the CelldPreview JSON without building or writing
  --json               Print machine-readable output

Uses kubectl and its authentication. Bucket credentials come from the standard
AWS chain; storage URLs/endpoints come only from operator-owned resources.
Updates retain the URL and state. TTL and seeding cannot be changed or reset.
The administrator seed command is single-use: it never steals Running claims.
";

#[derive(Debug)]
struct Options {
    command: String,
    name: String,
    context: String,
    namespace: String,
    fleet: Option<String>,
    source: Option<String>,
    revision: Option<String>,
    ttl: Option<u64>,
    seed: Option<String>,
    objects: Vec<Object>,
    alarms: Alarms,
    timeout: u64,
    config: Option<std::path::PathBuf>,
    json: bool,
    dry_run: bool,
}
fn parse(args: Vec<String>) -> anyhow::Result<Options> {
    let mut args = args.into_iter();
    let first = args.next().context("preview name is required")?;
    let (command, name) = if ["deploy", "status", "delete", "seed"].contains(&first.as_str()) {
        (first, args.next().context("name is required")?)
    } else {
        ("deploy".into(), first)
    };
    let mut o = Options {
        command,
        name,
        context: String::new(),
        namespace: "default".into(),
        fleet: None,
        source: None,
        revision: None,
        ttl: None,
        seed: None,
        objects: Vec::new(),
        alarms: Alarms::Clear,
        timeout: 600,
        config: None,
        json: false,
        dry_run: false,
    };
    let mut seen = std::collections::BTreeSet::new();
    while let Some(flag) = args.next() {
        ensure!(
            flag == "--object" || seen.insert(flag.clone()),
            "duplicate option {flag}"
        );
        if flag == "--json" {
            o.json = true;
            continue;
        }
        if flag == "--dry-run" {
            o.dry_run = true;
            continue;
        }
        let value = args
            .next()
            .with_context(|| format!("{flag} requires a value"))?;
        match flag.as_str() {
            "--context" => o.context = value,
            "--namespace" => o.namespace = value,
            "--fleet" => o.fleet = Some(value),
            "--source" => o.source = Some(value),
            "--revision" => o.revision = Some(value),
            "--ttl-seconds" => o.ttl = Some(value.parse()?),
            "--seed-from" => o.seed = Some(value),
            "--object" => {
                let (class, id) = value
                    .split_once(':')
                    .context("--object requires CLASS:ID")?;
                o.objects.push(Object {
                    class: class.into(),
                    id: id.into(),
                });
            }
            "--alarms" => {
                o.alarms = match value.as_str() {
                    "Clear" => Alarms::Clear,
                    "Preserve" => Alarms::Preserve,
                    _ => bail!("--alarms must be Clear or Preserve"),
                }
            }
            "--timeout-seconds" => o.timeout = value.parse()?,
            "--config" => o.config = Some(value.into()),
            _ => bail!("unknown preview option {flag}"),
        }
    }
    ensure!(!o.context.is_empty(), "--context is required");
    if !(o.command == "seed" && o.name == "--watch") {
        kubernetes::name(&o.name)?;
    }
    kubernetes::name(&o.namespace)?;
    ensure!(
        (1..=86400).contains(&o.timeout),
        "timeout must be 1..86400 seconds"
    );
    ensure!(
        o.ttl.is_none_or(|ttl| (60..=604800).contains(&ttl)),
        "TTL must be 60..604800 seconds"
    );
    if o.command == "deploy" {
        kubernetes::name(o.fleet.as_deref().context("--fleet is required")?)?;
        ensure!(
            o.seed.is_some() == !o.objects.is_empty(),
            "--seed-from and --object must be supplied together"
        );
        if o.seed.is_some() {
            preview_seed::validate_selection(&o.objects)?;
        }
        ensure!(
            o.seed.is_some() || !seen.contains("--alarms"),
            "--alarms requires --seed-from"
        );
    } else {
        ensure!(
            o.fleet.is_none()
                && o.source.is_none()
                && o.revision.is_none()
                && o.ttl.is_none()
                && o.seed.is_none()
                && o.objects.is_empty()
                && o.config.is_none()
                && !o.dry_run
                && !seen.contains("--alarms"),
            "deployment options require the deploy command"
        );
    }
    Ok(o)
}

fn manifest(o: &Options) -> Value {
    let mut spec = json!({"fleetRef":{"name":o.fleet},"source":o.source.as_ref().unwrap_or(&o.name),"ttlSeconds":o.ttl.unwrap_or(86400)});
    if let Some(revision) = &o.revision {
        spec["revision"] = json!(revision);
    }
    if let Some(source) = &o.seed {
        spec["seed"] = json!({"source":source,"alarms":o.alarms,"objects":o.objects});
    }
    json!({"apiVersion":API,"kind":"CelldPreview","metadata":{"name":o.name,"namespace":o.namespace},"spec":spec})
}
fn canonical_selection(value: &Value) -> Value {
    let mut value = value.clone();
    if let Some(objects) = value["objects"].as_array_mut() {
        objects.sort_by_key(Value::to_string);
    }
    value
}
fn update_spec(o: &Options, existing: &Value) -> anyhow::Result<Value> {
    live(existing)?;
    ensure!(
        existing["spec"]["fleetRef"]["name"] == json!(o.fleet),
        "preview belongs to a different parent fleet"
    );
    if let Some(ttl) = o.ttl {
        ensure!(
            existing["spec"]["ttlSeconds"] == ttl,
            "TTL is immutable; create another preview"
        );
    }
    if o.seed.is_some() {
        ensure!(
            canonical_selection(&existing["spec"]["seed"])
                == canonical_selection(&manifest(o)["spec"]["seed"]),
            "seed selection is immutable; create another preview"
        );
    }
    let mut spec = existing["spec"].clone();
    if let Some(source) = &o.source {
        spec["source"] = json!(source);
    }
    if let Some(revision) = &o.revision {
        spec["revision"] = json!(revision);
    }
    Ok(spec)
}

pub async fn run(args: Vec<String>) -> anyhow::Result<()> {
    if args.iter().any(|v| v == "--help" || v == "-h") {
        return Output::new(Format::Text).help(HELP);
    }
    let o = parse(args)?;
    let kube = Kubernetes {
        context: o.context.clone(),
        namespace: o.namespace.clone(),
    };
    match o.command.as_str() {
        "seed" if o.name == "--watch" => executor::watch(&kube).await,
        "seed" => executor::run(&kube, &o.name).await,
        "status" => {
            let p = kube.get(PREVIEW, &o.name).await?;
            print_status(&p, o.json)
        }
        "delete" => {
            let p = kube.get(PREVIEW, &o.name).await?;
            kube.delete_preview(&p).await?;
            crate::note!(
                "Preview {} deletion requested; the operator handles shutdown and retention",
                o.name
            );
            Ok(())
        }
        _ => deploy_preview(&kube, &o).await,
    }
}
fn print_status(p: &Value, json_output: bool) -> anyhow::Result<()> {
    if json_output {
        Output::new(Format::Json).bytes(&serde_json::to_vec(p)?)
    } else {
        Output::new(Format::Text).line(format_args!(
            "{}\t{}\t{}",
            field(p, "/metadata/name")?,
            p["status"]["phase"].as_str().unwrap_or("Pending"),
            p["status"]["url"].as_str().unwrap_or("")
        ))
    }
}
fn condition(p: &Value, kind: &str) -> bool {
    p["status"]["conditions"]
        .as_array()
        .is_some_and(|conditions| {
            conditions.iter().any(|c| {
                c["type"] == kind
                    && c["status"] == "True"
                    && c["observedGeneration"] == p["metadata"]["generation"]
            })
        })
}
fn endpoint(storage: &Value) -> Option<&str> {
    storage["endpoint"]["url"].as_str()
}
fn storage_url(storage: &Value) -> anyhow::Result<String> {
    let bucket = field(storage, "/bucket")?;
    ensure!(
        bucket.len() >= 3
            && bucket
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-'),
        "invalid operator bucket"
    );
    let prefix = storage["prefix"].as_str().unwrap_or("");
    if !prefix.is_empty() {
        kubernetes::name(prefix)?;
    }
    Ok(format!(
        "s3://{bucket}{}",
        if prefix.is_empty() {
            String::new()
        } else {
            format!("/{prefix}")
        }
    ))
}
fn open_storage(storage: &Value) -> anyhow::Result<Bucket> {
    // Intentionally do not use FleetFlags or environment bucket/endpoint
    // fallbacks: those may point to a developer's production deployment.
    let region = field(storage, "/region")?;
    ensure!(
        region
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-'),
        "invalid storage region"
    );
    let origin = endpoint(storage)
        .map(str::to_owned)
        .unwrap_or_else(|| format!("https://s3.{region}.amazonaws.com"));
    let parsed = url::Url::parse(&origin)?;
    ensure!(
        ["https", "http"].contains(&parsed.scheme())
            && parsed.username().is_empty()
            && parsed.password().is_none()
            && parsed.query().is_none()
            && parsed.fragment().is_none()
            && parsed.path() == "/",
        "invalid object-store origin"
    );
    Bucket::open(
        &storage_url(storage)?,
        Some(&origin),
        region,
        None,
        Some("celld-preview"),
    )
}
fn preview_storage(p: &Value, child: &Value, parent: &Value) -> anyhow::Result<Value> {
    live(p)?;
    live(child)?;
    live(parent)?;
    let puid = uid(p)?;
    let prefix = format!("p-{}", puid.replace('-', ""));
    let storage = &child["spec"]["storage"];
    ensure!(
        child["metadata"]["namespace"] == p["metadata"]["namespace"]
            && child["metadata"]["name"] == p["status"]["fleetName"],
        "preview child identity changed"
    );
    ensure!(
        child["metadata"]["ownerReferences"]
            .as_array()
            .is_some_and(
                |owners| owners.iter().any(|owner| owner["controller"] == true
                    && owner["apiVersion"] == API
                    && owner["kind"] == "CelldPreview"
                    && owner["uid"] == puid
                    && owner["name"] == p["metadata"]["name"])
            ),
        "child is not owned by this preview"
    );
    ensure!(
        p["spec"]["fleetRef"]["name"] == parent["metadata"]["name"]
            && p["metadata"]["namespace"] == parent["metadata"]["namespace"]
            && p["status"]["parentFleetUID"] == uid(parent)?,
        "preview parent identity changed"
    );
    ensure!(
        storage["prefix"] == prefix
            && storage["bucket"] == parent["spec"]["previews"]["storage"]["bucket"]
            && endpoint(storage) == endpoint(&parent["spec"]["previews"]["storage"])
            && storage["region"] == parent["spec"]["previews"]["storage"]["region"],
        "preview storage differs from parent authorization"
    );
    ensure!(
        storage["previewFleetRef"]["uid"] == uid(parent)?
            && storage["previewFleetRef"]["name"] == parent["metadata"]["name"],
        "child parent reference changed"
    );
    ensure!(
        p["status"]["storageURL"] == storage_url(storage)?,
        "preview storage URL mismatch"
    );
    Ok(storage.clone())
}
async fn deploy_preview(kube: &Kubernetes, o: &Options) -> anyhow::Result<()> {
    if o.dry_run {
        return Output::new(Format::Json).bytes(&serde_json::to_vec_pretty(&manifest(o))?);
    }
    let parent = kube.get(FLEET, o.fleet.as_deref().unwrap()).await?;
    live(&parent)?;
    ensure!(
        parent["spec"]["previews"].is_object(),
        "parent fleet has not enabled previews"
    );
    let existing = kube.find(PREVIEW, &o.name).await?;
    let spec = existing.as_ref().map(|p| update_spec(o, p)).transpose()?;
    let built = deploy::build(&deploy::Options {
        config: o.config.clone(),
        bucket: None,
        endpoint: None,
        region: None,
        dry_run: false,
        json: false,
        vars: BTreeMap::new(),
        local_images: false,
    })?;
    let created = if let Some(mut p) = existing {
        p["spec"] = spec.unwrap();
        kube.replace(&p, false).await?
    } else {
        kube.create(&manifest(o)).await?
    };
    let expected_uid = uid(&created)?.to_owned();
    let deadline = Instant::now() + Duration::from_secs(o.timeout);
    let mut deployed = false;
    loop {
        ensure!(
            Instant::now() < deadline,
            "preview wait timed out; resource retained, inspect `celld preview status {}`",
            o.name
        );
        let p = kube.get(PREVIEW, &o.name).await?;
        ensure!(
            uid(&p)? == expected_uid,
            "preview was replaced while waiting"
        );
        live(&p)?;
        if ["Blocked", "Expired", "Deleting"].contains(&p["status"]["phase"].as_str().unwrap_or(""))
            || ["Failed", "Canceled"].contains(&p["status"]["seedPhase"].as_str().unwrap_or(""))
        {
            bail!("preview cannot proceed: {}", p["status"]);
        }
        let seeded = p["spec"]["seed"].is_null() || p["status"]["seedPhase"] == "Succeeded";
        if seeded && p["status"]["fleetName"].is_string() && p["status"]["storageURL"].is_string() {
            let child = kube.get(FLEET, field(&p, "/status/fleetName")?).await?;
            let current_parent = kube.get(FLEET, o.fleet.as_deref().unwrap()).await?;
            ensure!(
                uid(&current_parent)? == uid(&parent)?,
                "parent fleet was replaced"
            );
            let storage = preview_storage(&p, &child, &current_parent)?;
            if !deployed {
                // An unseeded child may be waiting for its first deployment to
                // become ready. Deploy as soon as its bound storage is known.
                let store = open_storage(&storage)?;
                deploy::write(&store, &built).await?;
                deployed = true;
                crate::note!(
                    "Published {} to preview {}; waiting for the loaded version",
                    built.version,
                    o.name
                );
            }
            if condition(&p, "Ready") && application_ready(&child, &built.version, &built.prefix) {
                let url = field(&p, "/status/url")?;
                let parsed = url::Url::parse(url)?;
                ensure!(
                    ["https", "http"].contains(&parsed.scheme())
                        && parsed.username().is_empty()
                        && parsed.password().is_none(),
                    "invalid preview URL"
                );
                // A version match comes from the operator's private node
                // observations. Public requests can have arbitrary app status.
                if reqwest::Client::builder()
                    .redirect(reqwest::redirect::Policy::none())
                    .timeout(Duration::from_secs(10))
                    .build()?
                    .get(url)
                    .send()
                    .await
                    .is_ok_and(|response| !response.status().is_server_error())
                {
                    return if o.json {
                        Output::new(Format::Json).bytes(&serde_json::to_vec(&json!({"name":o.name,"namespace":o.namespace,"url":url,"version":built.version,"storageURL":storage_url(&storage)?}))?)
                    } else {
                        Output::new(Format::Text).line(format_args!("{url}"))
                    };
                }
            }
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}
fn application_ready(child: &Value, version: &str, prefix: &str) -> bool {
    let a = &child["status"]["application"];
    condition(child, "ApplicationConverged")
        && a["observedVersion"]["version"] == version
        && a["observedVersion"]["prefix"] == prefix
        && a["expectedNodes"].as_u64().is_some_and(|n| n > 0)
        && a["observedNodes"] == a["expectedNodes"]
        && a["unavailableNodes"] == 0
        && a["pendingCells"] == 0
        && a["swappingCells"] == 0
}
#[cfg(test)]
mod tests;
