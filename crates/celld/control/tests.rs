use super::*;
use crate::bucket::StorageBackend;
use object_store::memory::InMemory;
use std::collections::{BTreeMap, VecDeque};
use std::sync::Mutex;

/// What the fake does to the next write.
#[derive(Clone, Copy)]
enum Fault {
    /// Apply the write, then answer as a server error: the ambiguous case.
    AppliedThenServerError,
    /// Refuse the write before applying it, as a throttle does.
    Throttled,
}

/// An in-memory DynamoDB that understands the requests this module sends.
#[derive(Default)]
struct FakeTable {
    items: Mutex<BTreeMap<(String, String), Value>>,
    faults: Mutex<VecDeque<Fault>>,
    /// Extra fields merged into the DescribeTable answer.
    describe_extra: Mutex<Map<String, Value>>,
    ttl_status: Mutex<Option<String>>,
    calls: Mutex<Vec<&'static str>>,
}

fn condition_failed() -> TableError {
    TableError {
        commit: Commit::No,
        code: Some("ConditionalCheckFailedException".into()),
        message: "The conditional request failed".into(),
    }
}

fn key_of(value: &Value) -> (String, String) {
    (
        string_attribute(value, "pk").unwrap(),
        string_attribute(value, "sk").unwrap(),
    )
}

impl FakeTable {
    fn check(&self, request: &Value, current: Option<&Value>) -> Result<(), TableError> {
        match request.get("ConditionExpression").and_then(Value::as_str) {
            None => Ok(()),
            Some("attribute_not_exists(pk)") => match current {
                None => Ok(()),
                Some(_) => Err(condition_failed()),
            },
            Some("v = :v") => {
                let expected = request
                    .pointer("/ExpressionAttributeValues/:v/S")
                    .and_then(Value::as_str);
                let held = current
                    .and_then(|item| item.pointer("/v/S"))
                    .and_then(Value::as_str);
                if expected.is_some() && expected == held {
                    Ok(())
                } else {
                    Err(condition_failed())
                }
            }
            Some(other) => panic!("unexpected condition {other}"),
        }
    }

    fn write_fault(&self) -> Option<Fault> {
        self.faults.lock().unwrap().pop_front()
    }
}

#[async_trait::async_trait]
impl Transport for FakeTable {
    async fn call(&self, op: &'static str, body: Value) -> Result<Value, TableError> {
        self.calls.lock().unwrap().push(op);
        match op {
            "GetItem" => {
                assert_eq!(
                    body["ConsistentRead"],
                    json!(true),
                    "every read is consistent"
                );
                let key = key_of(&body["Key"]);
                let items = self.items.lock().unwrap();
                Ok(match items.get(&key) {
                    Some(item) => json!({ "Item": item }),
                    None => json!({}),
                })
            }
            "PutItem" => {
                let fault = self.write_fault();
                if let Some(Fault::Throttled) = fault {
                    return Err(TableError::from_response(
                        400,
                        br#"{"__type":"com.amazonaws.dynamodb.v20120810#ThrottlingException","message":"slow down"}"#,
                    ));
                }
                let item = body["Item"].clone();
                let key = key_of(&item);
                let mut items = self.items.lock().unwrap();
                self.check(&body, items.get(&key))?;
                items.insert(key, item);
                match fault {
                    Some(Fault::AppliedThenServerError) => Err(TableError::from_response(
                        500,
                        br#"{"__type":"com.amazonaws.dynamodb.v20120810#InternalServerError","message":"oops"}"#,
                    )),
                    _ => Ok(json!({})),
                }
            }
            "DeleteItem" => {
                let key = key_of(&body["Key"]);
                let mut items = self.items.lock().unwrap();
                self.check(&body, items.get(&key))?;
                items.remove(&key);
                Ok(json!({}))
            }
            "Query" => {
                assert_eq!(
                    body["ConsistentRead"],
                    json!(true),
                    "every read is consistent"
                );
                let pk = body
                    .pointer("/ExpressionAttributeValues/:pk/S")
                    .and_then(Value::as_str)
                    .unwrap()
                    .to_string();
                let after = body
                    .get("ExclusiveStartKey")
                    .map(|key| string_attribute(key, "sk").unwrap());
                let limit = body
                    .get("Limit")
                    .and_then(Value::as_u64)
                    .map(|n| n as usize);
                let items = self.items.lock().unwrap();
                let matching: Vec<&Value> = items
                    .iter()
                    .filter(|((item_pk, sk), _)| {
                        *item_pk == pk && after.as_ref().is_none_or(|after| sk > after)
                    })
                    .map(|(_, item)| item)
                    .collect();
                let (page, more) = match limit {
                    Some(limit) if matching.len() > limit => (&matching[..limit], true),
                    _ => (&matching[..], false),
                };
                let mut answer = json!({ "Items": page });
                if more {
                    let last = page.last().unwrap();
                    answer["LastEvaluatedKey"] = json!({ "pk": last["pk"], "sk": last["sk"] });
                }
                Ok(answer)
            }
            "DescribeTable" => {
                let mut table = json!({
                    "TableStatus": "ACTIVE",
                    "KeySchema": [
                        { "AttributeName": "pk", "KeyType": "HASH" },
                        { "AttributeName": "sk", "KeyType": "RANGE" },
                    ],
                    "AttributeDefinitions": [
                        { "AttributeName": "pk", "AttributeType": "S" },
                        { "AttributeName": "sk", "AttributeType": "S" },
                    ],
                });
                for (field, value) in self.describe_extra.lock().unwrap().iter() {
                    table[field] = value.clone();
                }
                Ok(json!({ "Table": table }))
            }
            "DescribeTimeToLive" => Ok(json!({
                "TimeToLiveDescription": {
                    "TimeToLiveStatus": self
                        .ttl_status
                        .lock()
                        .unwrap()
                        .clone()
                        .unwrap_or_else(|| "DISABLED".into()),
                },
            })),
            other => panic!("unexpected operation {other}"),
        }
    }
}

/// A fresh bucket, each with its own name, as separate fleets have.
fn bucket() -> Bucket {
    static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let store = Arc::new(InMemory::new());
    Bucket::with_stores(
        store.clone(),
        store,
        StorageBackend::S3,
        format!("test-{n}"),
        "fleet-a/".into(),
    )
    .with_unresolved_control_for_test()
}

fn table_settings() -> Settings {
    Settings {
        backend: Some(Backend::DynamoDb {
            table: "celld-test".into(),
        }),
        region: Some("us-east-1".into()),
        endpoint: None,
    }
}

/// A bucket resolved against a fresh fake table, the way a node resolves.
async fn table_fleet() -> (Bucket, Arc<FakeTable>) {
    let bucket = bucket();
    let fake = Arc::new(FakeTable::default());
    resolve_with(&bucket, Role::Node, &table_settings(), Some(fake.clone()))
        .await
        .unwrap();
    (bucket, fake)
}

#[test]
fn every_coordination_key_round_trips_through_its_item() {
    let keys = [
        (
            "cells/Room:a/own.json",
            Some(ControlKey::Owner("Room:a".into())),
        ),
        ("nodes/n1.json", Some(ControlKey::Lease("n1".into()))),
        ("drain/token.json", Some(ControlKey::Drain)),
        ("wake/waker.json", Some(ControlKey::Waker)),
        ("deploy/current.json", Some(ControlKey::FleetPointer)),
        (
            "deploy/api/current.json",
            Some(ControlKey::ScriptPointer("api".into())),
        ),
        (
            "deploy/queues/jobs/consumer.json",
            Some(ControlKey::QueueAttachment("jobs".into())),
        ),
    ];
    for (key, expected) in keys {
        let parsed = ControlKey::parse(key);
        assert_eq!(parsed, expected, "{key}");
        let (pk, sk) = parsed.unwrap().item_key();
        if !pk.starts_with("cell#") {
            assert_eq!(ControlKey::object_key(&pk, &sk).as_deref(), Some(key));
        }
    }
    for key in [
        "cells/Room:a/ltx/e1/ltx/0/0000000000000001-0000000000000001.ltx",
        "nodes/a/b.json",
        "nodes/.json",
        "deploy/api/v1/manifest.json",
        "wake/entries/2026-09-29T10:00/Room:a/1.1",
        "wake/retired/Room:a.json",
        "wake/format.json",
        "fleet/peer-auth.json",
        "fleet/capacity-v1.json",
        MARKER_KEY,
    ] {
        assert_eq!(ControlKey::parse(key), None, "{key}");
    }
}

#[test]
fn a_listing_reads_the_partitions_under_its_prefix() {
    let plan = listing_plan("nodes/");
    assert_eq!(plan.partitions, [NODES_PK]);
    assert!(plan.table_only);

    let plan = listing_plan("deploy/");
    assert_eq!(plan.partitions, [DEPLOY_PK]);
    assert!(!plan.table_only, "deploy/ also holds deployments");

    assert!(listing_plan("cells/").partitions.is_empty());
    assert!(listing_plan("wake/entries/").partitions.is_empty());
    assert!(listing_plan("log/").partitions.is_empty());
    assert_eq!(listing_plan("wake/").partitions, [FLEET_PK]);
    assert_eq!(listing_plan("").partitions, [NODES_PK, FLEET_PK, DEPLOY_PK]);
    assert!(!listing_plan("").table_only);
}

#[test]
fn backend_settings_parse_strictly() {
    assert_eq!(Backend::parse("bucket").unwrap(), Backend::Bucket);
    assert_eq!(
        Backend::parse("dynamodb://celld-prod").unwrap(),
        Backend::DynamoDb {
            table: "celld-prod".into()
        }
    );
    assert!(Backend::parse("dynamodb://").is_err());
    assert!(Backend::parse("dynamodb://a").is_err());
    assert!(Backend::parse("dynamodb://bad/name").is_err());
    assert!(Backend::parse("s3://bucket").is_err());
}

#[test]
fn a_node_records_the_table_and_claims_it() {
    crate::asyncrt::test_block_on(async {
        let (bucket, fake) = table_fleet().await;
        let (bytes, _) = bucket.get(MARKER_KEY).await.unwrap().unwrap();
        let marker: Marker = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(marker.backend, "dynamodb");
        assert_eq!(marker.table.as_deref(), Some("celld-test"));
        assert_eq!(marker.region.as_deref(), Some("us-east-1"));
        let fleet = marker.fleet.unwrap();
        let items = fake.items.lock().unwrap();
        let meta = items
            .get(&(META_PK.to_string(), META_SK.to_string()))
            .expect("the node claimed the table");
        assert!(meta["doc"]["S"].as_str().unwrap().contains(&fleet));
        assert!(
            !items.keys().any(|(pk, _)| pk == PROBE_PK),
            "the probe cleans up after itself"
        );
    });
}

#[test]
fn coordination_records_live_in_the_table_and_data_stays_in_the_bucket() {
    crate::asyncrt::test_block_on(async {
        let (bucket, fake) = table_fleet().await;

        let token = bucket
            .put_cas(
                "nodes/n1.json",
                br#"{"node":"n1","expires_ms":1}"#.to_vec(),
                None,
            )
            .await
            .unwrap()
            .expect("create applies");
        assert!(
            bucket
                .put_cas("nodes/n1.json", br#"{"node":"n1"}"#.to_vec(), None)
                .await
                .unwrap()
                .is_none(),
            "a second create is a clean rejection"
        );
        let (body, read_token) = bucket.get("nodes/n1.json").await.unwrap().unwrap();
        assert_eq!(read_token, token);
        assert_eq!(body.as_ref(), br#"{"node":"n1","expires_ms":1}"#);
        let next = bucket
            .put_cas(
                "nodes/n1.json",
                br#"{"node":"n1","expires_ms":2}"#.to_vec(),
                Some(&token),
            )
            .await
            .unwrap()
            .expect("an update with the current token applies");
        assert!(
            bucket
                .put_cas("nodes/n1.json", b"{}".to_vec(), Some(&token))
                .await
                .unwrap()
                .is_none(),
            "a stale token is a clean rejection"
        );
        assert_eq!(bucket.head("nodes/n1.json").await.unwrap().unwrap().1, next);

        // The record is not in the bucket, and an LTX object is.
        bucket
            .put("cells/Room:a/ltx/e1/ltx/0/1-1.ltx", b"ltx".to_vec())
            .await
            .unwrap();
        assert!(bucket
            .get_bucket_object("nodes/n1.json")
            .await
            .unwrap()
            .is_none());
        assert!(bucket
            .get_bucket_object("cells/Room:a/ltx/e1/ltx/0/1-1.ltx")
            .await
            .unwrap()
            .is_some());
        let items = fake.items.lock().unwrap();
        assert!(items.contains_key(&(NODES_PK.to_string(), "n1".to_string())));
        assert!(!items.keys().any(|(pk, _)| pk.starts_with("cell#")));
    });
}

#[test]
fn listings_merge_table_records_with_bucket_objects() {
    crate::asyncrt::test_block_on(async {
        let (bucket, _) = table_fleet().await;
        for node in ["n2", "n1", "n3"] {
            bucket
                .put_cas(&format!("nodes/{node}.json"), b"{}".to_vec(), None)
                .await
                .unwrap()
                .unwrap();
        }
        let nodes: Vec<String> = bucket
            .list("nodes/")
            .await
            .unwrap()
            .into_iter()
            .map(|object| object.location.to_string())
            .collect();
        assert_eq!(nodes, ["nodes/n1.json", "nodes/n2.json", "nodes/n3.json"]);

        // Pages resume exactly.
        let first = bucket.objects_page("nodes/", None, 2).await.unwrap();
        assert_eq!(first.objects.len(), 2);
        let second = bucket
            .objects_page("nodes/", first.page_token.clone(), 2)
            .await
            .unwrap();
        assert_eq!(second.objects.len(), 1);
        assert_eq!(second.objects[0].location.as_ref(), "nodes/n3.json");
        assert!(second.page_token.is_none());

        bucket
            .put("deploy/current.json", br#"{"version":"v1"}"#.to_vec())
            .await
            .unwrap();
        bucket
            .put("deploy/api/current.json", br#"{"version":"v1"}"#.to_vec())
            .await
            .unwrap();
        bucket
            .put("deploy/api/v1/manifest.json", b"{}".to_vec())
            .await
            .unwrap();
        let mut deploy: Vec<String> = bucket
            .list("deploy/")
            .await
            .unwrap()
            .into_iter()
            .map(|object| object.location.to_string())
            .collect();
        deploy.sort();
        assert_eq!(
            deploy,
            [
                "deploy/api/current.json",
                "deploy/api/v1/manifest.json",
                "deploy/current.json",
            ]
        );
        assert!(bucket
            .get_bucket_object("deploy/current.json")
            .await
            .unwrap()
            .is_none());
    });
}

#[test]
fn a_conditional_delete_spares_a_record_that_changed() {
    crate::asyncrt::test_block_on(async {
        let (bucket, _) = table_fleet().await;
        let first = bucket
            .put_cas("nodes/n1.json", b"{\"expires_ms\":0}".to_vec(), None)
            .await
            .unwrap()
            .unwrap();
        let second = bucket
            .put_cas(
                "nodes/n1.json",
                b"{\"expires_ms\":9}".to_vec(),
                Some(&first),
            )
            .await
            .unwrap()
            .unwrap();
        assert!(!bucket
            .delete_if_token("nodes/n1.json", &first)
            .await
            .unwrap());
        assert!(bucket.get("nodes/n1.json").await.unwrap().is_some());
        assert!(bucket
            .delete_if_token("nodes/n1.json", &second)
            .await
            .unwrap());
        assert!(bucket.get("nodes/n1.json").await.unwrap().is_none());
    });
}

#[test]
fn failed_writes_are_classified_for_the_self_fence() {
    crate::asyncrt::test_block_on(async {
        let (bucket, fake) = table_fleet().await;

        fake.faults.lock().unwrap().push_back(Fault::Throttled);
        let error = bucket
            .put_cas("nodes/n1.json", b"{}".to_vec(), None)
            .await
            .unwrap_err();
        assert!(
            crate::bucket::cas_write_did_not_commit(&error),
            "a throttle is refused before it applies: {error:#}"
        );
        assert!(bucket.get("nodes/n1.json").await.unwrap().is_none());

        fake.faults
            .lock()
            .unwrap()
            .push_back(Fault::AppliedThenServerError);
        let error = bucket
            .put_cas("nodes/n1.json", b"{\"node\":\"n1\"}".to_vec(), None)
            .await
            .unwrap_err();
        assert!(
            !crate::bucket::cas_write_did_not_commit(&error),
            "a server error can have applied: {error:#}"
        );
        // It did apply, and the readback shows it.
        assert!(bucket.get("nodes/n1.json").await.unwrap().is_some());
    });
}

#[test]
fn a_write_is_attempted_once() {
    crate::asyncrt::test_block_on(async {
        let (bucket, fake) = table_fleet().await;
        fake.calls.lock().unwrap().clear();
        fake.faults
            .lock()
            .unwrap()
            .push_back(Fault::AppliedThenServerError);
        let _ = bucket.put_cas("nodes/n1.json", b"{}".to_vec(), None).await;
        assert_eq!(*fake.calls.lock().unwrap(), ["PutItem"]);
    });
}

#[test]
fn a_node_refuses_a_store_the_fleet_did_not_choose() {
    crate::asyncrt::test_block_on(async {
        // A bucket fleet: the first node records the bucket.
        let bucket = bucket();
        let fake = Arc::new(FakeTable::default());
        resolve_with(
            &bucket,
            Role::Node,
            &Settings::default(),
            Some(fake.clone()),
        )
        .await
        .unwrap();
        let other = bucket_sharing(&bucket);
        let error = resolve_with(&other, Role::Node, &table_settings(), Some(fake.clone()))
            .await
            .unwrap_err();
        assert!(format!("{error:#}").contains("selects bucket"), "{error:#}");
        assert!(
            fake.items.lock().unwrap().is_empty(),
            "nothing reached the table"
        );
    });
}

#[test]
fn a_table_fleet_cannot_start_on_existing_bucket_state() {
    crate::asyncrt::test_block_on(async {
        // A stopped bucket fleet: its lease expired long ago, but its cell
        // data, and every other sign of a fleet, still refuse the switch.
        for (key, body) in [
            (
                "nodes/old.json",
                br#"{"node":"old","expires_ms":1}"#.to_vec(),
            ),
            ("cells/Room:a/ltx/e7/ltx/0/1-1.ltx", b"ltx".to_vec()),
            ("log/old/g/bundle/e1-00000001.ltxb", b"bundle".to_vec()),
            ("deploy/current.json", b"{}".to_vec()),
            ("deploy/api/current.json", b"{}".to_vec()),
            ("drain/token.json", b"{}".to_vec()),
        ] {
            let bucket = bucket();
            bucket.put(key, body).await.unwrap();
            let fake = Arc::new(FakeTable::default());
            let error = resolve_with(&bucket, Role::Node, &table_settings(), Some(fake.clone()))
                .await
                .unwrap_err();
            assert!(
                format!("{error:#}").contains("already holds fleet state"),
                "{key}: {error:#}"
            );
            assert!(bucket.get(MARKER_KEY).await.unwrap().is_none(), "{key}");
            assert!(
                fake.items.lock().unwrap().is_empty(),
                "{key}: nothing claimed"
            );
        }
        // Deployments without a pointer are not fleet state.
        let bucket = bucket();
        bucket
            .put("deploy/api/v1/manifest.json", b"{}".to_vec())
            .await
            .unwrap();
        resolve_with(
            &bucket,
            Role::Node,
            &table_settings(),
            Some(Arc::new(FakeTable::default())),
        )
        .await
        .unwrap();
    });
}

#[test]
fn an_operator_reaches_a_table_only_through_the_fleet_marker() {
    crate::asyncrt::test_block_on(async {
        let (_, fake) = table_fleet().await;
        let pointer = (DEPLOY_PK.to_string(), "current".to_string());
        // Another, markerless bucket configured for the same table.
        let other = bucket();
        let error = resolve_with(
            &other,
            Role::Operator,
            &table_settings(),
            Some(fake.clone()),
        )
        .await
        .unwrap_err();
        assert!(
            format!("{error:#}").contains("celld control init"),
            "{error:#}"
        );
        assert!(!fake.items.lock().unwrap().contains_key(&pointer));

        // A bucket whose marker names a fleet the table does not serve.
        let other = bucket();
        other
            .put(
                MARKER_KEY,
                serde_json::to_vec(&Marker {
                    format: MARKER_FORMAT,
                    backend: "dynamodb".into(),
                    table: Some("celld-test".into()),
                    region: Some("us-east-1".into()),
                    fleet: Some("someone-else".into()),
                })
                .unwrap(),
            )
            .await
            .unwrap();
        let error = resolve_with(
            &other,
            Role::Operator,
            &Settings::default(),
            Some(fake.clone()),
        )
        .await
        .unwrap_err();
        assert!(format!("{error:#}").contains("serves fleet"), "{error:#}");
    });
}

#[test]
fn an_unresolved_client_resolves_before_its_first_record() {
    crate::asyncrt::test_block_on(async {
        let (bucket, fake) = table_fleet().await;
        // A second client over the same bucket that nothing resolved, as the
        // preview publisher's was.
        let publisher = bucket
            .clone()
            .with_unresolved_control_over_for_test(fake.clone());
        publisher
            .put("deploy/current.json", br#"{"version":"v2"}"#.to_vec())
            .await
            .unwrap();
        assert!(publisher
            .get_bucket_object("deploy/current.json")
            .await
            .unwrap()
            .is_none());
        let (body, _) = bucket.get("deploy/current.json").await.unwrap().unwrap();
        assert_eq!(body.as_ref(), br#"{"version":"v2"}"#);
        // Its resolution was read-only.
        let marker = bucket.get(MARKER_KEY).await.unwrap();
        assert!(marker.is_some());
    });
}

#[test]
fn a_rejected_claim_leaves_no_marker() {
    crate::asyncrt::test_block_on(async {
        let (_, fake) = table_fleet().await;
        let second = bucket();
        let error = resolve_with(&second, Role::Node, &table_settings(), Some(fake.clone()))
            .await
            .unwrap_err();
        assert!(format!("{error:#}").contains("serves fleet"), "{error:#}");
        assert!(second.get(MARKER_KEY).await.unwrap().is_none());
        // Correcting the configuration is enough.
        let resolved = resolve_with(&second, Role::Node, &Settings::default(), Some(fake))
            .await
            .unwrap();
        assert_eq!(resolved.backend, Backend::Bucket);
    });
}

#[test]
fn an_interrupted_setup_adopts_its_own_claim() {
    crate::asyncrt::test_block_on(async {
        let bucket = bucket();
        let fake = Arc::new(FakeTable::default());
        // A setup claimed the table and stopped before it wrote the marker.
        let table = Table::with_transport("celld-test".into(), "us-east-1".into(), fake.clone());
        table
            .claim("first-attempt", &bucket_identity(&bucket))
            .await
            .unwrap();
        let resolved = resolve_with(&bucket, Role::Node, &table_settings(), Some(fake))
            .await
            .unwrap();
        assert_eq!(resolved.fleet.as_deref(), Some("first-attempt"));
    });
}

#[test]
fn a_node_refuses_a_table_that_lost_its_claim() {
    crate::asyncrt::test_block_on(async {
        let (bucket, fake) = table_fleet().await;
        fake.items
            .lock()
            .unwrap()
            .remove(&(META_PK.to_string(), META_SK.to_string()));
        let restarted = bucket_sharing(&bucket);
        let error = resolve_with(
            &restarted,
            Role::Node,
            &table_settings(),
            Some(fake.clone()),
        )
        .await
        .unwrap_err();
        assert!(
            format!("{error:#}").contains("emptied or replaced"),
            "{error:#}"
        );
        assert!(
            !fake
                .items
                .lock()
                .unwrap()
                .contains_key(&(META_PK.to_string(), META_SK.to_string())),
            "the claim is not silently recreated"
        );
    });
}

#[test]
fn init_claims_before_it_records_and_can_run_again() {
    crate::asyncrt::test_block_on(async {
        let bucket = bucket();
        let fake = Arc::new(FakeTable::default());
        let first = init_with(&bucket, &table_settings(), false, Some(fake.clone()))
            .await
            .unwrap();
        let again = init_with(&bucket, &table_settings(), false, Some(fake.clone()))
            .await
            .unwrap();
        assert_eq!(first.fleet, again.fleet);
        let node = bucket_sharing(&bucket);
        resolve_with(&node, Role::Node, &table_settings(), Some(fake))
            .await
            .unwrap();
    });
}

#[test]
fn a_table_that_serves_another_fleet_is_refused() {
    crate::asyncrt::test_block_on(async {
        let fake = Arc::new(FakeTable::default());
        resolve_with(&bucket(), Role::Node, &table_settings(), Some(fake.clone()))
            .await
            .unwrap();
        // A second fleet, in another bucket, pointed at the same table.
        let error = resolve_with(&bucket(), Role::Node, &table_settings(), Some(fake))
            .await
            .unwrap_err();
        assert!(format!("{error:#}").contains("serves fleet"), "{error:#}");
    });
}

#[test]
fn a_table_that_reads_eventually_is_refused() {
    crate::asyncrt::test_block_on(async {
        for (field, value) in [
            (
                "GlobalSecondaryIndexes",
                json!([{ "IndexName": "by-node" }]),
            ),
            ("Replicas", json!([{ "RegionName": "eu-west-1" }])),
        ] {
            let fake = Arc::new(FakeTable::default());
            fake.describe_extra
                .lock()
                .unwrap()
                .insert(field.into(), value);
            let error = resolve_with(&bucket(), Role::Node, &table_settings(), Some(fake))
                .await
                .unwrap_err();
            assert!(
                format!("{error:#}").contains("celld refuses it"),
                "{error:#}"
            );
        }
        let fake = Arc::new(FakeTable::default());
        *fake.ttl_status.lock().unwrap() = Some("ENABLED".into());
        let error = resolve_with(&bucket(), Role::Node, &table_settings(), Some(fake))
            .await
            .unwrap_err();
        assert!(format!("{error:#}").contains("time-to-live"), "{error:#}");
    });
}

#[test]
fn an_operator_command_writes_nothing_while_it_resolves() {
    crate::asyncrt::test_block_on(async {
        let bucket = bucket();
        let resolved = resolve_with(
            &bucket,
            Role::Operator,
            &Settings::default(),
            Some(Arc::new(FakeTable::default())),
        )
        .await
        .unwrap();
        assert_eq!(resolved.backend, Backend::Bucket);
        assert!(bucket.get(MARKER_KEY).await.unwrap().is_none());
    });
}

#[test]
fn the_lease_lane_follows_the_node() {
    crate::asyncrt::test_block_on(async {
        let (bucket, fake) = table_fleet().await;
        let lease = bucket_sharing(&bucket);
        let resolved = resolve_with(&lease, Role::Lease, &Settings::default(), Some(fake))
            .await
            .unwrap();
        assert_eq!(
            resolved.backend,
            Backend::DynamoDb {
                table: "celld-test".into()
            }
        );
        lease
            .put_cas("nodes/n1.json", b"{}".to_vec(), None)
            .await
            .unwrap()
            .unwrap();
        assert!(bucket.get("nodes/n1.json").await.unwrap().is_some());
    });
}

#[test]
fn error_responses_are_classified() {
    let throttled = TableError::from_response(
        400,
        br#"{"__type":"com.amazonaws.dynamodb.v20120810#ProvisionedThroughputExceededException","message":"x"}"#,
    );
    assert_eq!(throttled.commit, Commit::No);
    assert_eq!(
        throttled.code.as_deref(),
        Some("ProvisionedThroughputExceededException")
    );
    let condition = TableError::from_response(
        400,
        br#"{"__type":"com.amazonaws.dynamodb.v20120810#ConditionalCheckFailedException","message":"x"}"#,
    );
    assert!(condition.is_condition_failure());
    assert_eq!(
        TableError::from_response(500, b"not json").commit,
        Commit::Maybe
    );
    assert_eq!(TableError::from_response(503, b"{}").commit, Commit::Maybe);
}

/// A second client over the same objects, with its own route, as the lease
/// lane is a second client over the same bucket.
fn bucket_sharing(bucket: &Bucket) -> Bucket {
    bucket.clone().with_unresolved_control_for_test()
}

/// The table contract against a real endpoint, such as DynamoDB Local:
/// `CELLD_TEST_DYNAMODB_ENDPOINT=http://127.0.0.1:8000`. Skipped otherwise.
#[test]
fn a_live_table_honors_the_contract() {
    let Ok(endpoint) = std::env::var("CELLD_TEST_DYNAMODB_ENDPOINT") else {
        return;
    };
    crate::asyncrt::test_block_on(async {
        let credentials: AwsCredentialProvider = Arc::new(
            object_store::StaticCredentialProvider::new(object_store::aws::AwsCredential {
                key_id: "local".into(),
                secret_key: "local".into(),
                token: None,
            }),
        );
        let transport =
            HttpTransport::new(endpoint, "us-east-1".into(), credentials, None).unwrap();
        let name = format!("celld-test-{}", Table::new_token());
        let table = Table::with_transport(name, "us-east-1".into(), Arc::new(transport));
        assert!(table.create().await.unwrap());
        table.check_shape().await.unwrap();
        table.probe().await.unwrap();
        assert_eq!(table.claim("fleet-1", "s3://a/").await.unwrap(), "fleet-1");
        assert_eq!(table.claim("fleet-2", "s3://a/").await.unwrap(), "fleet-1");
        assert!(table.claim("fleet-3", "s3://b/").await.is_err());
        table.verify_claim("fleet-1").await.unwrap();
        assert!(table.verify_claim("fleet-3").await.is_err());

        let key = ControlKey::Lease("n1".into());
        let token = table.cas_record(&key, b"{}", None).await.unwrap().unwrap();
        assert!(table.cas_record(&key, b"{}", None).await.unwrap().is_none());
        let next = table
            .cas_record(&key, b"{\"a\":1}", Some(&token))
            .await
            .unwrap()
            .unwrap();
        assert!(table
            .cas_record(&key, b"{}", Some(&token))
            .await
            .unwrap()
            .is_none());
        for node in ["n2", "n3"] {
            table
                .cas_record(&ControlKey::Lease(node.into()), b"{}", None)
                .await
                .unwrap()
                .unwrap();
        }
        let (page, cursor) = table.lease_page(None, 2).await.unwrap();
        assert_eq!(page.len(), 2);
        let (rest, _) = table.lease_page(cursor, 2).await.unwrap();
        assert_eq!(rest.len(), 1);
        assert!(!table.delete_record(&key, Some(&token)).await.unwrap());
        assert!(table.delete_record(&key, Some(&next)).await.unwrap());
    });
}

/// Release qualification against real DynamoDB and S3, for what neither the
/// fake nor DynamoDB Local can vouch for: AWS signing with real credentials,
/// the real service's error codes, table creation with point-in-time
/// recovery and deletion protection, and latency. It runs when
/// `CELLD_QUALIFY_DYNAMODB_BUCKET=s3://BUCKET[/PREFIX]` is set, with AWS
/// credentials and `AWS_REGION` in the environment, and is skipped
/// otherwise. Each run creates its own table and bucket prefix and deletes
/// both, whatever the outcome.
#[test]
fn a_real_table_qualifies() {
    let Ok(base) = std::env::var("CELLD_QUALIFY_DYNAMODB_BUCKET") else {
        return;
    };
    let region = std::env::var("AWS_REGION")
        .or_else(|_| std::env::var("AWS_DEFAULT_REGION"))
        .expect("AWS_REGION names the region of the bucket and the table");
    crate::asyncrt::test_block_on(async {
        use futures_util::FutureExt;
        let run = Table::new_token();
        let name = format!("celld-qualify-{run}");
        let open = |fleet: &str| {
            crate::fleet::bucket_client(
                &format!("{}/celld-qualify-{run}/{fleet}", base.trim_end_matches('/')),
                None,
                &region,
            )
            .unwrap()
        };
        let bucket = open("a");
        let other = open("b");
        let settings = Settings {
            backend: Some(Backend::DynamoDb {
                table: name.clone(),
            }),
            region: Some(region.clone()),
            endpoint: None,
        };
        let outcome = std::panic::AssertUnwindSafe(qualify(&bucket, &other, &settings))
            .catch_unwind()
            .await;

        // Clean up whatever the run left, then report the run.
        for client in [&bucket, &other] {
            if let Ok(objects) = client.list("").await {
                for object in objects {
                    let _ = client.delete(object.location.as_ref()).await;
                }
            }
        }
        let table = open_table(&bucket, &name, &region, &settings, None).unwrap();
        let dropped = table.drop_for_test().await;
        if let Err(panic) = outcome {
            std::panic::resume_unwind(panic);
        }
        dropped.unwrap();
    });
}

async fn qualify(bucket: &Bucket, other: &Bucket, settings: &Settings) {
    let Some(Backend::DynamoDb { table: name }) = &settings.backend else {
        unreachable!()
    };
    let region = settings.region.clone().unwrap();

    // Setup creates the table, claims it and records the marker.
    let resolved = init(bucket, settings, true).await.unwrap();
    let fleet = resolved.fleet.clone().unwrap();
    let table = open_table(bucket, name, &region, settings, None).unwrap();
    table.check_shape().await.unwrap();
    let mut recovery = false;
    for _ in 0..60 {
        recovery = table.point_in_time_recovery().await.unwrap();
        if recovery {
            break;
        }
        crate::asyncrt::sleep(Duration::from_secs(1)).await;
    }
    assert!(recovery, "init enables point-in-time recovery");
    let described = table
        .read("DescribeTable", json!({ "TableName": name }))
        .await
        .unwrap();
    assert_eq!(
        described.pointer("/Table/DeletionProtectionEnabled"),
        Some(&json!(true)),
        "init enables deletion protection"
    );
    // A second run adopts the table and the claim.
    assert_eq!(
        init(bucket, settings, true).await.unwrap().fleet.as_deref(),
        Some(fleet.as_str())
    );

    // A node and its lease lane resolve to the table.
    let node = bucket_sharing(bucket);
    resolve_with(&node, Role::Node, settings, None)
        .await
        .unwrap();
    let lease = bucket_sharing(bucket);
    resolve_with(&lease, Role::Lease, settings, None)
        .await
        .unwrap();

    // Conditional writes, the error classes of the real service, and the
    // records' home.
    let key = "nodes/qualify.json";
    let first = node
        .put_cas(key, br#"{"node":"qualify","expires_ms":1}"#.to_vec(), None)
        .await
        .unwrap()
        .expect("a create applies");
    assert!(node
        .put_cas(key, b"{}".to_vec(), None)
        .await
        .unwrap()
        .is_none());
    let mut token = first.clone();
    let mut samples = Vec::new();
    for n in 0..50u64 {
        let started = crate::asyncrt::mono_ms();
        token = lease
            .put_cas(
                key,
                format!(r#"{{"node":"qualify","expires_ms":{n}}}"#).into_bytes(),
                Some(&token),
            )
            .await
            .unwrap()
            .expect("a renewal with the current token applies");
        samples.push(Duration::from_millis(
            crate::asyncrt::mono_ms().saturating_sub(started),
        ));
    }
    assert!(lease
        .put_cas(key, b"{}".to_vec(), Some(&first))
        .await
        .unwrap()
        .is_none());
    assert_eq!(node.head(key).await.unwrap().unwrap().1, token);
    assert!(node.get_bucket_object(key).await.unwrap().is_none());
    assert_eq!(
        node.list("nodes/")
            .await
            .unwrap()
            .into_iter()
            .map(|meta| meta.location.to_string())
            .collect::<Vec<_>>(),
        [key]
    );
    assert!(!node.delete_if_token(key, &first).await.unwrap());
    assert!(node.delete_if_token(key, &token).await.unwrap());
    samples.sort();
    let p50 = samples[samples.len() / 2];
    let p99 = samples[samples.len() * 99 / 100];
    eprintln!("dynamodb://{name} conditional writes: p50 {p50:?}, p99 {p99:?}");
    assert!(
        p99 < Duration::from_secs(1),
        "a lease renewal must land well inside its TTL; p99 was {p99:?}"
    );

    // Another fleet cannot take the table, and leaves no marker.
    let error = resolve_with(other, Role::Node, settings, None)
        .await
        .unwrap_err();
    assert!(format!("{error:#}").contains("serves fleet"), "{error:#}");
    assert!(read_marker(other).await.unwrap().is_none());
}

/// A fleet whose bucket pages its listings, as a cell walk needs, resolved
/// against a fresh fake table.
async fn paged_table_fleet() -> (Bucket, Arc<FakeTable>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let store =
        Arc::new(crate::local_store::LocalStore::open(dir.path().join("store.db")).unwrap());
    let bucket = Bucket::with_stores(
        store.clone(),
        store.clone(),
        StorageBackend::S3,
        "paged".into(),
        "fleet-a/".into(),
    )
    .with_paginated_for_test(store)
    .with_unresolved_control_for_test();
    let fake = Arc::new(FakeTable::default());
    resolve_with(&bucket, Role::Node, &table_settings(), Some(fake.clone()))
        .await
        .unwrap();
    (bucket, fake, dir)
}

async fn put_ltx(bucket: &Bucket, scope: &str, epoch: u64) {
    bucket
        .put(
            &format!("cells/{scope}/ltx/e{epoch}/0000/0000000000000001-0000000000000001.ltx"),
            b"ltx".to_vec(),
        )
        .await
        .unwrap();
}

async fn owner_of(bucket: &Bucket, cell: &str) -> Option<Value> {
    bucket
        .get(&format!("cells/{cell}/own.json"))
        .await
        .unwrap()
        .map(|(body, _)| serde_json::from_slice(&body).unwrap())
}

#[test]
fn repair_raises_only_records_behind_the_bucket() {
    crate::asyncrt::test_block_on(async {
        let (bucket, _, _dir) = paged_table_fleet().await;
        // Rolled back: the record says 2, the root wrote 4 and a facet 5.
        bucket
            .put_cas(
                "cells/Room:behind/own.json",
                br#"{"node":"n1","epoch":2}"#.to_vec(),
                None,
            )
            .await
            .unwrap()
            .unwrap();
        put_ltx(&bucket, "Room:behind", 2).await;
        put_ltx(&bucket, "Room:behind", 4).await;
        put_ltx(&bucket, "Room:behind/facets/aa", 5).await;
        // Consistent: the record names the epoch that wrote.
        bucket
            .put_cas(
                "cells/Room:current/own.json",
                br#"{"node":"n1","epoch":3}"#.to_vec(),
                None,
            )
            .await
            .unwrap()
            .unwrap();
        put_ltx(&bucket, "Room:current", 3).await;
        // No record at all, but data at epoch 2.
        put_ltx(&bucket, "Room:lost", 2).await;
        // No record and only a preview's epoch 0.
        put_ltx(&bucket, "Room:preview", 0).await;

        let mut seen = Vec::new();
        let report = repair_epochs(&bucket, true, |repaired| {
            seen.push(repaired.clone());
            Ok(())
        })
        .await
        .unwrap();
        assert_eq!(report.scanned, 4);
        assert_eq!(report.repaired, 2);
        assert_eq!(
            owner_of(&bucket, "Room:behind").await.unwrap()["epoch"],
            2,
            "a dry run writes nothing"
        );

        seen.clear();
        let report = repair_epochs(&bucket, false, |repaired| {
            seen.push(repaired.clone());
            Ok(())
        })
        .await
        .unwrap();
        assert_eq!(report.repaired, 2);
        seen.sort_by(|a, b| a.cell.cmp(&b.cell));
        assert_eq!(
            seen,
            [
                Repaired {
                    cell: "Room:behind".into(),
                    from: Some(2),
                    owner: Some("n1".into()),
                    to: 5,
                },
                Repaired {
                    cell: "Room:lost".into(),
                    from: None,
                    owner: None,
                    to: 2,
                },
            ]
        );
        assert_eq!(
            owner_of(&bucket, "Room:behind").await.unwrap(),
            json!({"node": "", "epoch": 5})
        );
        assert_eq!(
            owner_of(&bucket, "Room:lost").await.unwrap(),
            json!({"node": "", "epoch": 2})
        );
        assert_eq!(
            owner_of(&bucket, "Room:current").await.unwrap(),
            json!({"node": "n1", "epoch": 3})
        );
        assert_eq!(owner_of(&bucket, "Room:preview").await, None);

        // A second pass finds nothing left to do.
        let report = repair_epochs(&bucket, false, |_| Ok(())).await.unwrap();
        assert_eq!(report.repaired, 0);
    });
}

#[test]
fn repair_waits_for_dead_nodes_logs() {
    crate::asyncrt::test_block_on(async {
        let (bucket, _, _dir) = paged_table_fleet().await;
        bucket
            .put_cas(
                "nodes/dead.json",
                br#"{"node":"dead","expires_ms":1,"log":{"state":"open","epoch":1,"ensemble":[],"tiered":0}}"#
                    .to_vec(),
                None,
            )
            .await
            .unwrap()
            .unwrap();
        put_ltx(&bucket, "Room:lost", 2).await;
        let error = repair_epochs(&bucket, false, |_| Ok(())).await.unwrap_err();
        assert!(format!("{error:#}").contains("dead"), "{error:#}");
        assert_eq!(owner_of(&bucket, "Room:lost").await, None);

        // Once the fleet sealed the log, the repair proceeds.
        let (_, token) = bucket.get("nodes/dead.json").await.unwrap().unwrap();
        bucket
            .put_cas(
                "nodes/dead.json",
                br#"{"node":"dead","expires_ms":1,"log":{"state":"sealed","epoch":1,"ensemble":[],"tiered":0}}"#
                    .to_vec(),
                Some(&token),
            )
            .await
            .unwrap()
            .unwrap();
        let report = repair_epochs(&bucket, false, |_| Ok(())).await.unwrap();
        assert_eq!(report.repaired, 1);
    });
}

async fn put_lease(bucket: &Bucket, node: &str, expires_ms: u64, log: &str) {
    let key = format!("nodes/{node}.json");
    let token = bucket.get(&key).await.unwrap().map(|(_, token)| token);
    let body = format!(
        r#"{{"node":"{node}","expires_ms":{expires_ms},"log":{{"state":"{log}","epoch":1,"ensemble":[],"tiered":0}}}}"#
    );
    bucket
        .put_cas(&key, body.into_bytes(), token.as_deref())
        .await
        .unwrap()
        .unwrap();
}

#[test]
fn repair_refuses_a_running_fleet() {
    crate::asyncrt::test_block_on(async {
        let (bucket, _, _dir) = paged_table_fleet().await;
        put_lease(&bucket, "n1", u64::MAX / 2, "open").await;
        put_ltx(&bucket, "Room:lost", 2).await;
        for dry_run in [true, false] {
            let error = repair_epochs(&bucket, dry_run, |_| Ok(()))
                .await
                .unwrap_err();
            assert!(format!("{error:#}").contains("running: n1"), "{error:#}");
        }
        assert_eq!(owner_of(&bucket, "Room:lost").await, None);

        // Once the node stopped and its log is sealed, the repair proceeds.
        put_lease(&bucket, "n1", 1, "sealed").await;
        let report = repair_epochs(&bucket, false, |_| Ok(())).await.unwrap();
        assert_eq!(report.repaired, 1);
    });
}

/// A node that starts during the walk can activate a root below a dormant
/// facet's epoch, since facets restore on demand. The repair must not clear
/// its record while it may still serve the cell, nor while its log holds
/// writes the bucket does not have yet.
#[test]
fn repair_leaves_a_cell_whose_owner_may_still_serve_it() {
    crate::asyncrt::test_block_on(async {
        let (bucket, _, _dir) = paged_table_fleet().await;
        bucket
            .put_cas(
                "cells/Room:a/own.json",
                br#"{"node":"n2","epoch":3}"#.to_vec(),
                None,
            )
            .await
            .unwrap()
            .unwrap();
        put_ltx(&bucket, "Room:a", 3).await;
        put_ltx(&bucket, "Room:a/facets/aa", 5).await;
        let held = Outcome::Held { owner: "n2".into() };

        put_lease(&bucket, "n2", u64::MAX / 2, "open").await;
        assert_eq!(repair_cell(&bucket, "Room:a", false).await.unwrap(), held);
        // Its lease lapsed with the log still open: recovery comes first.
        put_lease(&bucket, "n2", 1, "open").await;
        assert_eq!(repair_cell(&bucket, "Room:a", false).await.unwrap(), held);
        put_lease(&bucket, "n2", 1, "recovering").await;
        assert_eq!(repair_cell(&bucket, "Room:a", false).await.unwrap(), held);
        assert_eq!(
            owner_of(&bucket, "Room:a").await.unwrap(),
            json!({"node": "n2", "epoch": 3})
        );

        // Stopped and sealed: the record is raised.
        put_lease(&bucket, "n2", 1, "sealed").await;
        assert_eq!(
            repair_cell(&bucket, "Room:a", false).await.unwrap(),
            Outcome::Repaired(Repaired {
                cell: "Room:a".into(),
                from: Some(3),
                owner: Some("n2".into()),
                to: 5,
            })
        );
        assert_eq!(
            owner_of(&bucket, "Room:a").await.unwrap(),
            json!({"node": "", "epoch": 5})
        );
    });
}
