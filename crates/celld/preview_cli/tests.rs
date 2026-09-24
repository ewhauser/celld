use super::*;
fn args(extra: &[&str]) -> Vec<String> {
    [
        "pr-42",
        "--context",
        "test-cluster",
        "--namespace",
        "previews",
        "--fleet",
        "development",
    ]
    .into_iter()
    .chain(extra.iter().copied())
    .map(str::to_owned)
    .collect()
}
#[test]
fn generates_only_preview_and_multi_object_selection() {
    let o = parse(args(&[
        "--seed-from",
        "production",
        "--object",
        "Cart:one",
        "--object",
        "Customer:two",
        "--revision",
        "abc",
    ]))
    .unwrap();
    let p = manifest(&o);
    assert_eq!(p["kind"], "CelldPreview");
    assert_eq!(p["spec"]["fleetRef"]["name"], "development");
    assert_eq!(p["spec"]["ttlSeconds"], 86400);
    assert_eq!(p["spec"]["seed"]["objects"].as_array().unwrap().len(), 2);
    assert_eq!(p["spec"]["seed"]["alarms"], "Clear");
    assert!(p["spec"]["storage"].is_null());
}
#[test]
fn rejects_ambiguous_or_invalid_options() {
    for extra in [
        vec!["--ttl-seconds", "1"],
        vec!["--bucket", "production"],
        vec!["--seed-from", "production"],
        vec!["--object", "Cart:one"],
        vec!["--alarms", "Preserve"],
        vec![
            "--seed-from",
            "prod",
            "--object",
            "Cart:one",
            "--object",
            "Cart:one",
        ],
    ] {
        assert!(parse(args(&extra)).is_err(), "{extra:?}");
    }
    assert!(parse(vec!["pr-42".into(), "--fleet".into(), "dev".into()]).is_err());
}
#[test]
fn updates_keep_seed_and_ttl_without_reinitializing() {
    let seeded = parse(args(&["--seed-from", "production", "--object", "Cart:one"])).unwrap();
    let mut existing = manifest(&seeded);
    existing["metadata"]["uid"] = json!("p-uid");
    let update = parse(args(&["--revision", "new"])).unwrap();
    let spec = update_spec(&update, &existing).unwrap();
    assert_eq!(spec["seed"], existing["spec"]["seed"]);
    assert_eq!(spec["ttlSeconds"], 86400);
    assert_eq!(spec["revision"], "new");
    assert!(update_spec(&parse(args(&["--ttl-seconds", "3600"])).unwrap(), &existing).is_err());
    assert!(update_spec(
        &parse(args(&["--seed-from", "other", "--object", "Cart:one"])).unwrap(),
        &existing
    )
    .is_err());
}
fn bound_preview() -> (Value, Value, Value) {
    let p = json!({"metadata":{"name":"pr-42","namespace":"previews","uid":"1234-5678"},"spec":{"fleetRef":{"name":"development"}},"status":{"fleetName":"p-12345678","parentFleetUID":"parent-uid","storageURL":"s3://preview-bucket/p-12345678"}});
    let child = json!({"metadata":{"name":"p-12345678","namespace":"previews","uid":"child-uid","ownerReferences":[{"apiVersion":API,"kind":"CelldPreview","name":"pr-42","uid":"1234-5678","controller":true}]},"spec":{"storage":{"bucket":"preview-bucket","prefix":"p-12345678","region":"us-east-1","previewFleetRef":{"name":"development","uid":"parent-uid"}}}});
    let parent = json!({"metadata":{"name":"development","namespace":"previews","uid":"parent-uid"},"spec":{"previews":{"storage":{"bucket":"preview-bucket","region":"us-east-1"}}}});
    (p, child, parent)
}
#[test]
fn deployment_requires_preview_bound_storage_and_ownership() {
    let (p, child, parent) = bound_preview();
    assert!(preview_storage(&p, &child, &parent).is_ok());
    for path in [
        "/spec/storage/bucket",
        "/spec/storage/prefix",
        "/spec/storage/previewFleetRef/uid",
        "/metadata/ownerReferences/0/uid",
    ] {
        let mut bad = child.clone();
        *bad.pointer_mut(path).unwrap() = json!("production");
        assert!(preview_storage(&p, &bad, &parent).is_err());
    }
    let mut bad = p.clone();
    bad["status"]["storageURL"] = json!("s3://production");
    assert!(preview_storage(&bad, &child, &parent).is_err());
}
#[test]
fn ready_is_not_application_deployment_proof() {
    let mut child = json!({"metadata":{"generation":2},"status":{"conditions":[{"type":"ApplicationConverged","status":"True","observedGeneration":2}],"application":{"observedVersion":{"version":"v1","prefix":"deploy/v1"},"expectedNodes":1,"observedNodes":1,"unavailableNodes":0,"pendingCells":0,"swappingCells":0}}});
    assert!(application_ready(&child, "v1", "deploy/v1"));
    assert!(!application_ready(&child, "v2", "deploy/v2"));
    child["status"]["application"]["observedNodes"] = json!(0);
    assert!(!application_ready(&child, "v1", "deploy/v1"));
    child["status"]["application"]["observedNodes"] = json!(1);
    child["metadata"]["generation"] = json!(3);
    assert!(!application_ready(&child, "v1", "deploy/v1"));
}

#[test]
fn platform_watcher_uses_explicit_context_and_namespace() {
    let o = parse(
        [
            "seed",
            "--watch",
            "--context",
            "test-cluster",
            "--namespace",
            "previews",
        ]
        .map(str::to_owned)
        .to_vec(),
    )
    .unwrap();
    assert_eq!(o.command, "seed");
    assert_eq!(o.name, "--watch");
    assert_eq!(o.namespace, "previews");
}
