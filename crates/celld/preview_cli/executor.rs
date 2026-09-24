use super::*;

const GATE: &str = "celld.eric.dev/seed-gate";
const PIN: &str = "celld.eric.dev/seed-reservation-created";
fn annotation<'a>(v: &'a Value, key: &str) -> &'a str {
    v["metadata"]["annotations"][key].as_str().unwrap_or("")
}
fn reservation_name(storage: &Value) -> anyhow::Result<String> {
    let bucket = field(storage, "/bucket")?;
    let prefix = storage["prefix"].as_str().unwrap_or("");
    Ok(if prefix.is_empty() {
        format!("s3-{}", &preview_seed::digest(bucket.as_bytes())[..56])
    } else {
        format!(
            "s3-scope-{}",
            &preview_seed::digest(format!("{bucket}\0{prefix}").as_bytes())[..54]
        )
    })
}
fn deadline_passed(request: &Value) -> anyhow::Result<bool> {
    let deadline =
        chrono::DateTime::parse_from_rfc3339(field(request, "/deadline")?)?.timestamp_millis();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis();
    Ok(deadline <= 0 || now >= deadline as u128)
}
fn reservation_binding(r: &Value, fleet: &Value) -> anyhow::Result<()> {
    live(r)?;
    live(fleet)?;
    let s = &fleet["spec"]["storage"];
    ensure!(
        r["metadata"]["ownerReferences"]
            .as_array()
            .is_none_or(Vec::is_empty),
        "reservation must be permanently retained"
    );
    ensure!(
        r["metadata"]["name"] == reservation_name(s)?
            && r["spec"]["bucket"] == s["bucket"]
            && r["spec"]["prefix"].as_str().unwrap_or("") == s["prefix"].as_str().unwrap_or("")
            && r["spec"]["endpoint"].as_str().unwrap_or("") == endpoint(s).unwrap_or("")
            && r["spec"]["fleetUID"] == uid(fleet)?
            && r["spec"]["fleetNamespace"] == fleet["metadata"]["namespace"]
            && r["spec"]["fleetName"] == fleet["metadata"]["name"]
            && r["spec"]["ownerKind"].is_null(),
        "storage reservation is not bound to the live fleet"
    );
    field(r, "/spec/specHash")?;
    Ok(())
}
fn unopened(r: &Value) -> anyhow::Result<()> {
    for key in [
        GATE,
        "celld.eric.dev/workload-creation-attempted",
        "celld.eric.dev/current-operation",
    ] {
        ensure!(
            annotation(r, key).is_empty(),
            "destination startup/lifecycle gate is already set"
        );
    }
    Ok(())
}
fn pending(r: &Value) -> anyhow::Result<()> {
    live(r)?;
    unopened(r)?;
    ensure!(
        r["status"]["canceled"] != true
            && !deadline_passed(&r["spec"]["initialization"])?
            && ["", "Pending"].contains(&r["status"]["phase"].as_str().unwrap_or("")),
        "reservation is canceled, expired, or already claimed; Running work cannot be stolen"
    );
    ensure!(
        r["status"]["executorID"].is_null() && r["status"]["manifest"].is_null(),
        "reservation has prior execution state"
    );
    Ok(())
}
fn claimed(current: &Value, original: &Value, execution: &str) -> anyhow::Result<()> {
    live(current)?;
    unopened(current)?;
    ensure!(
        uid(current)? == uid(original)? && current["spec"] == original["spec"],
        "reservation identity/request changed"
    );
    ensure!(
        current["status"]["phase"] == "Running"
            && current["status"]["executorID"] == execution
            && current["status"]["targetFleetUID"] == original["spec"]["fleetUID"],
        "initialization claim changed"
    );
    Ok(())
}
async fn shared_authority(kube: &Kubernetes, fleet: &Value) -> anyhow::Result<()> {
    let storage = &fleet["spec"]["storage"];
    let parent = &storage["previewFleetRef"];
    if parent.is_null() {
        return Ok(());
    }
    let root_storage = json!({"bucket":storage["bucket"]});
    let root = kube
        .get(RESERVATION, &reservation_name(&root_storage)?)
        .await?;
    live(&root)?;
    ensure!(
        root["metadata"]["ownerReferences"]
            .as_array()
            .is_none_or(Vec::is_empty)
            && root["spec"]["ownerKind"] == "FleetPreviews"
            && root["spec"]["prefix"].as_str().unwrap_or("").is_empty()
            && root["spec"]["bucket"] == storage["bucket"]
            && root["spec"]["endpoint"].as_str().unwrap_or("") == endpoint(storage).unwrap_or("")
            && root["spec"]["fleetUID"] == parent["uid"]
            && root["spec"]["fleetName"] == parent["name"]
            && root["spec"]["fleetNamespace"] == fleet["metadata"]["namespace"],
        "preview bucket authority changed"
    );
    Ok(())
}
struct Authority {
    source: Value,
    target: Value,
}
async fn authority(kube: &Kubernetes, r: &Value) -> anyhow::Result<Authority> {
    unopened(r)?;
    let request = &r["spec"]["initialization"];
    ensure!(
        request["executor"] == preview_seed::EXECUTOR,
        "unsupported preview executor"
    );
    let target = &request["target"];
    let ns = field(r, "/spec/fleetNamespace")?;
    let target_kube = Kubernetes {
        context: kube.context.clone(),
        namespace: ns.into(),
    };
    let child = target_kube.get(FLEET, field(r, "/spec/fleetName")?).await?;
    reservation_binding(r, &child)?;
    ensure!(
        annotation(&child, PIN) == uid(r)?
            && child["spec"]["storage"]["initialization"] == *request
            && r["spec"]["initialReplicas"] == child["spec"]["replicas"],
        "child does not pin this initialization reservation"
    );
    let preview = target_kube
        .get(PREVIEW, field(target, "/previewName")?)
        .await?;
    let parent = target_kube
        .get(FLEET, field(target, "/previewFleetRef/name")?)
        .await?;
    let storage = preview_storage(&preview, &child, &parent)?;
    ensure!(
        target["previewUID"] == uid(&preview)?
            && target["fleetName"] == child["metadata"]["name"]
            && target["storageURL"] == storage_url(&storage)?
            && target["previewFleetRef"] == storage["previewFleetRef"],
        "initialization target identity mismatch"
    );
    ensure!(
        annotation(&preview, "celld.eric.dev/preview-seed-reservation")
            == field(r, "/metadata/name")?,
        "preview does not pin this reservation"
    );
    ensure!(
        request["deadline"] == preview["status"]["expiresAt"],
        "preview deadline mismatch"
    );
    let mut selected = preview["spec"]["seed"].clone();
    let mut requested = request["selection"].clone();
    for selection in [&mut selected, &mut requested] {
        let objects: Vec<Object> = serde_json::from_value(selection["objects"].clone())?;
        preview_seed::validate_selection(&objects)?;
        selection["objects"]
            .as_array_mut()
            .context("objects missing")?
            .sort_by_key(Value::to_string);
        if selection["alarms"].is_null() {
            selection["alarms"] = json!("Clear");
        }
    }
    ensure!(selected == requested, "preview seed selection changed");
    ensure!(
        parent["spec"]["previews"]["seeding"]["executor"] == preview_seed::EXECUTOR
            && parent["spec"]["previews"]["seeding"]["sources"]
                .as_array()
                .is_some_and(|sources| sources
                    .iter()
                    .any(|s| s["name"] == request["selection"]["source"]
                        && s["fleetRef"] == request["sourceFleet"])),
        "source is not authorized by this parent"
    );
    shared_authority(kube, &child).await?;
    let source_ref = &request["sourceFleet"];
    let source_kube = Kubernetes {
        context: kube.context.clone(),
        namespace: field(source_ref, "/namespace")?.into(),
    };
    let source = source_kube.get(FLEET, field(source_ref, "/name")?).await?;
    live(&source)?;
    ensure!(
        source_ref["uid"] == uid(&source)?,
        "source fleet was replaced"
    );
    let source_res = kube
        .get(RESERVATION, &reservation_name(&source["spec"]["storage"])?)
        .await?;
    reservation_binding(&source_res, &source)?;
    shared_authority(kube, &source).await?;
    ensure!(
        source["spec"]["storage"]["bucket"] != storage["bucket"],
        "source and preview destination must use distinct buckets"
    );
    Ok(Authority {
        source: source["spec"]["storage"].clone(),
        target: storage,
    })
}

/// Read cancellation only between fully awaited transfers. Never abort a PUT
/// future and claim no writer remains. Any uncertain error leaves Running for
/// administrator investigation; overlapping Job retries are refused by pending.
async fn checkpoint(
    kube: &Kubernetes,
    original: &Value,
    execution: &str,
) -> anyhow::Result<Option<Value>> {
    let mut r = kube
        .get(RESERVATION, field(original, "/metadata/name")?)
        .await?;
    claimed(&r, original, execution)?;
    if r["status"]["canceled"] == true || deadline_passed(&r["spec"]["initialization"])? {
        r["status"]["phase"] = json!("Canceled");
        r["status"]["message"] =
            json!("Executor stopped after all outstanding storage calls completed");
        kube.replace(&r, true).await?;
        return Ok(None);
    }
    Ok(Some(r))
}
pub(super) async fn run(kube: &Kubernetes, name: &str) -> anyhow::Result<()> {
    let mut r = kube.get(RESERVATION, name).await?;
    pending(&r)?;
    let auth = authority(kube, &r).await?;
    let mut random = [0_u8; 32];
    getrandom::fill(&mut random).map_err(|e| anyhow::anyhow!("execution identity: {e}"))?;
    let execution = format!("seed-{}", preview_seed::digest(&random));
    r["status"] =
        json!({"phase":"Running","executorID":execution,"targetFleetUID":r["spec"]["fleetUID"]});
    // resourceVersion is the admission CAS. A lost response is not retried.
    let claimed_record = kube.replace(&r, true).await?;
    let result = execute(kube, &claimed_record, &execution, &auth).await;
    if result.is_err() {
        crate::note!("Seed reservation {name} returned an error or uncertain result. Inspect its status and retained snapshots; never reset or steal a Running claim.");
    }
    result
}
async fn execute(
    kube: &Kubernetes,
    original: &Value,
    execution: &str,
    auth: &Authority,
) -> anyhow::Result<()> {
    let Some(_) = checkpoint(kube, original, execution).await? else {
        return Ok(());
    };
    let operation = uid(original)?;
    let source = open_storage(&auth.source)?;
    let target = open_storage(&auth.target)?;
    preview_seed::ensure_unopened(&target, operation).await?;
    let request = &original["spec"]["initialization"];
    let objects: Vec<Object> = serde_json::from_value(request["selection"]["objects"].clone())?;
    let alarms: Alarms = serde_json::from_value(request["selection"]["alarms"].clone())?;
    let mut entries = Vec::new();
    for object in &objects {
        let Some(current) = checkpoint(kube, original, execution).await? else {
            return Ok(());
        };
        let current_auth = authority(kube, &current).await?;
        ensure!(
            current_auth.source == auth.source && current_auth.target == auth.target,
            "storage authority changed"
        );
        entries.push(preview_seed::capture(&source, &target, operation, object, alarms).await?);
    }
    let Some(mut current) = checkpoint(kube, original, execution).await? else {
        return Ok(());
    };
    current["status"]["manifest"] = json!({"objects":entries});
    let pinned = kube.replace(&current, true).await?;
    ensure!(
        pinned["status"]["manifest"] == current["status"]["manifest"],
        "snapshot manifest was not pinned"
    );
    for entry in &entries {
        let Some(current) = checkpoint(kube, original, execution).await? else {
            return Ok(());
        };
        ensure!(
            current["status"]["manifest"] == pinned["status"]["manifest"],
            "pinned manifest changed"
        );
        authority(kube, &current).await?;
        preview_seed::import(&target, operation, entry).await?;
    }
    let Some(mut current) = checkpoint(kube, original, execution).await? else {
        return Ok(());
    };
    authority(kube, &current).await?;
    ensure!(
        !deadline_passed(&current["spec"]["initialization"])?,
        "initialization deadline elapsed before success"
    );
    current["status"]["phase"] = json!("Succeeded");
    current["status"]["message"] =
        json!("All persisted checkpoints imported; no outstanding writes");
    kube.replace(&current, true).await?;
    crate::note!(
        "Seeded {} objects for {}",
        entries.len(),
        field(original, "/metadata/name")?
    );
    Ok(())
}
/// A platform process can poll one authorized namespace. Claims, not this
/// process's memory, exclude competing watchers and overlapping Job retries.
pub(super) async fn watch(kube: &Kubernetes) -> anyhow::Result<()> {
    let mut refused = std::collections::BTreeMap::new();
    loop {
        let list = kube.call(&["get", RESERVATION, "-o", "json"], None).await?;
        let mut present = std::collections::BTreeSet::new();
        for r in list["items"]
            .as_array()
            .context("reservation list has no items")?
        {
            if r["spec"]["fleetNamespace"] != kube.namespace
                || r["spec"]["initialization"]["executor"] != preview_seed::EXECUTOR
                || pending(r).is_err()
            {
                continue;
            }
            let id = uid(r)?.to_owned();
            let version = field(r, "/metadata/resourceVersion")?.to_owned();
            present.insert(id.clone());
            if refused.get(&id) == Some(&version) {
                continue;
            }
            if let Err(error) = run(kube, field(r, "/metadata/name")?).await {
                crate::note!("Preview seed could not complete: {error}");
                refused.insert(id, version);
            }
        }
        refused.retain(|id, _| present.contains(id));
        tokio::time::sleep(Duration::from_secs(10)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn pending_record() -> Value {
        json!({"metadata":{"uid":"r-1"},"spec":{"fleetUID":"f-1","initialization":{"deadline":"2099-01-01T00:00:00Z"}}})
    }
    #[test]
    fn claims_refuse_retries_cancellation_and_startup() {
        let p = pending_record();
        assert!(pending(&p).is_ok());
        for phase in ["Running", "Succeeded", "Failed", "Canceled"] {
            let mut r = p.clone();
            r["status"] = json!({"phase":phase});
            assert!(pending(&r).is_err());
        }
        let mut r = p.clone();
        r["status"] = json!({"canceled":true});
        assert!(pending(&r).is_err());
        let mut r = p.clone();
        r["metadata"]["annotations"] = json!({GATE:"ready:other"});
        assert!(pending(&r).is_err());
        let mut r = p;
        r["spec"]["initialization"]["deadline"] = json!("2000-01-01T00:00:00Z");
        assert!(pending(&r).is_err());
    }
    #[test]
    fn running_claim_pins_uid_request_executor_and_target() {
        let p = pending_record();
        let mut r = p.clone();
        r["status"] = json!({"phase":"Running","executorID":"run-1","targetFleetUID":"f-1"});
        assert!(claimed(&r, &p, "run-1").is_ok());
        for path in [
            "/metadata/uid",
            "/spec/fleetUID",
            "/status/executorID",
            "/status/targetFleetUID",
        ] {
            let mut bad = r.clone();
            *bad.pointer_mut(path).unwrap() = json!("replaced");
            assert!(claimed(&bad, &p, "run-1").is_err());
        }
    }
}
