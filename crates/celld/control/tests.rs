use super::*;
use crate::bucket::StorageBackend;
use object_store::memory::InMemory;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
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
    /// Client request tokens of the transactions that applied.
    transactions: Mutex<std::collections::BTreeSet<String>>,
}

fn condition_failed() -> TableError {
    TableError {
        commit: Commit::No,
        code: Some("ConditionalCheckFailedException".into()),
        message: "The conditional request failed".into(),
        reasons: Vec::new(),
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
                let pk = body
                    .pointer("/ExpressionAttributeValues/:pk/S")
                    .and_then(Value::as_str)
                    .unwrap()
                    .to_string();
                assert_eq!(
                    body["ConsistentRead"],
                    json!(pk != LOAD_PK),
                    "every read but the advisory load query is consistent"
                );
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
            "Scan" => {
                assert_eq!(
                    body["ConsistentRead"],
                    json!(true),
                    "every read is consistent"
                );
                let prefix = match body.get("FilterExpression").and_then(Value::as_str) {
                    None => String::new(),
                    Some("begins_with(pk, :cell)") => body
                        .pointer("/ExpressionAttributeValues/:cell/S")
                        .and_then(Value::as_str)
                        .unwrap()
                        .to_string(),
                    Some(other) => panic!("unexpected filter {other}"),
                };
                let after = body.get("ExclusiveStartKey").map(key_of);
                let items = self.items.lock().unwrap();
                // One item per page, so every caller's paging is exercised.
                let mut keys = items
                    .keys()
                    .filter(|key| after.as_ref().is_none_or(|after| *key > after));
                let Some(key) = keys.next() else {
                    return Ok(json!({ "Items": [] }));
                };
                let item = &items[key];
                let mut answer = json!({
                    "Items": if key.0.starts_with(&prefix) { vec![item.clone()] } else { vec![] },
                });
                if keys.next().is_some() {
                    answer["LastEvaluatedKey"] = json!({ "pk": item["pk"], "sk": item["sk"] });
                }
                Ok(answer)
            }
            "TransactWriteItems" => {
                let fault = self.write_fault();
                if let Some(Fault::Throttled) = fault {
                    return Err(TableError::from_response(
                        400,
                        br#"{"__type":"com.amazonaws.dynamodb.v20120810#ThrottlingException","message":"slow down"}"#,
                    ));
                }
                let token = body["ClientRequestToken"].as_str().unwrap().to_string();
                let mut items = self.items.lock().unwrap();
                if !self.transactions.lock().unwrap().contains(&token) {
                    let writes: Vec<&Value> = body["TransactItems"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|item| &item["Put"])
                        .collect();
                    let reasons: Vec<Value> = writes
                        .iter()
                        .map(
                            |put| match self.check(put, items.get(&key_of(&put["Item"]))) {
                                Ok(()) => json!({ "Code": "None" }),
                                Err(_) => json!({ "Code": "ConditionalCheckFailed" }),
                            },
                        )
                        .collect();
                    if reasons.iter().any(|reason| reason["Code"] != "None") {
                        return Err(TableError::from_response(
                            400,
                            json!({
                                "__type": "com.amazonaws.dynamodb.v20120810#TransactionCanceledException",
                                "Message": "Transaction cancelled",
                                "CancellationReasons": reasons,
                            })
                            .to_string()
                            .as_bytes(),
                        ));
                    }
                    for put in writes {
                        items.insert(key_of(&put["Item"]), put["Item"].clone());
                    }
                    self.transactions.lock().unwrap().insert(token);
                }
                match fault {
                    Some(Fault::AppliedThenServerError) => Err(TableError::from_response(
                        500,
                        br#"{"__type":"com.amazonaws.dynamodb.v20120810#InternalServerError","message":"oops"}"#,
                    )),
                    _ => Ok(json!({})),
                }
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
        lease_shards: None,
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
                    migrating: None,
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
            .claim("first-attempt", &bucket_identity(&bucket), 1)
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
        assert_eq!(
            table.claim("fleet-1", "s3://a/", 1).await.unwrap(),
            "fleet-1"
        );
        assert_eq!(
            table.claim("fleet-2", "s3://a/", 1).await.unwrap(),
            "fleet-1"
        );
        assert!(table.claim("fleet-3", "s3://b/", 1).await.is_err());
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

        let named = swap("deploy/api/current.json", r#"{"v":1}"#, Expect::Absent);
        let fleet = swap("deploy/current.json", r#"{"v":1}"#, Expect::Absent);
        let pair = |a: &Swap, b: &Swap| {
            [
                (ControlKey::parse(&a.key).unwrap(), a.clone()),
                (ControlKey::parse(&b.key).unwrap(), b.clone()),
            ]
        };
        let first = pair(&named, &fleet);
        let first: Vec<_> = first
            .iter()
            .map(|(key, swap)| (key.clone(), swap))
            .collect();
        assert_eq!(table.transact(&first).await.unwrap(), None);
        let stale = swap("deploy/api/current.json", r#"{"v":2}"#, Expect::Any);
        let second = pair(&stale, &fleet);
        let second: Vec<_> = second
            .iter()
            .map(|(key, swap)| (key.clone(), swap))
            .collect();
        assert_eq!(
            table.transact(&second).await.unwrap().as_deref(),
            Some("deploy/current.json")
        );
        let (body, _) = table
            .get_record(&ControlKey::parse("deploy/api/current.json").unwrap())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            body.as_ref(),
            br#"{"v":1}"#,
            "a cancelled transaction applies nothing"
        );
    });
}

fn swap(key: &str, body: &str, expect: Expect) -> Swap {
    Swap {
        key: key.into(),
        body: body.as_bytes().to_vec(),
        expect,
    }
}

#[test]
fn deploy_pointers_switch_in_one_transaction_on_a_table() {
    crate::asyncrt::test_block_on(async {
        let (bucket, fake) = table_fleet().await;
        let attachment = bucket
            .put_cas(
                "deploy/queues/jobs/consumer.json",
                br#"{"a":0}"#.to_vec(),
                None,
            )
            .await
            .unwrap()
            .unwrap();
        fake.calls.lock().unwrap().clear();
        let swaps = [
            swap(
                "deploy/queues/jobs/consumer.json",
                r#"{"a":1}"#,
                Expect::Token(attachment.clone()),
            ),
            swap("deploy/api/current.json", r#"{"v":1}"#, Expect::Absent),
            swap("deploy/current.json", r#"{"v":1}"#, Expect::Absent),
        ];
        assert_eq!(bucket.swap_all(&swaps).await.unwrap(), None);
        assert_eq!(*fake.calls.lock().unwrap(), ["TransactWriteItems"]);
        for (key, body) in [
            ("deploy/queues/jobs/consumer.json", r#"{"a":1}"#),
            ("deploy/api/current.json", r#"{"v":1}"#),
            ("deploy/current.json", r#"{"v":1}"#),
        ] {
            assert_eq!(
                bucket.get(key).await.unwrap().unwrap().0.as_ref(),
                body.as_bytes()
            );
        }

        // A stale expectation anywhere leaves every record as it was.
        let (_, fleet_token) = bucket.get("deploy/current.json").await.unwrap().unwrap();
        let lost = bucket
            .swap_all(&[
                swap("deploy/api/current.json", r#"{"v":2}"#, Expect::Any),
                swap(
                    "deploy/queues/jobs/consumer.json",
                    r#"{"a":2}"#,
                    Expect::Token(attachment),
                ),
                swap(
                    "deploy/current.json",
                    r#"{"v":2}"#,
                    Expect::Token(fleet_token),
                ),
            ])
            .await
            .unwrap();
        assert_eq!(lost.as_deref(), Some("deploy/queues/jobs/consumer.json"));
        assert_eq!(
            bucket
                .get("deploy/api/current.json")
                .await
                .unwrap()
                .unwrap()
                .0
                .as_ref(),
            br#"{"v":1}"#
        );
        assert_eq!(
            bucket
                .get("deploy/current.json")
                .await
                .unwrap()
                .unwrap()
                .0
                .as_ref(),
            br#"{"v":1}"#
        );
    });
}

#[test]
fn an_ambiguous_transaction_repeats_with_its_request_token() {
    crate::asyncrt::test_block_on(async {
        let (bucket, fake) = table_fleet().await;
        fake.faults
            .lock()
            .unwrap()
            .push_back(Fault::AppliedThenServerError);
        fake.calls.lock().unwrap().clear();
        let swaps = [
            swap("deploy/api/current.json", r#"{"v":1}"#, Expect::Absent),
            swap("deploy/current.json", r#"{"v":1}"#, Expect::Absent),
        ];
        // The first attempt applied and answered 500; the repeat carries
        // the same token, so it reports the applied outcome instead of the
        // lost race a second create would be.
        assert_eq!(bucket.swap_all(&swaps).await.unwrap(), None);
        assert_eq!(
            *fake.calls.lock().unwrap(),
            ["TransactWriteItems", "TransactWriteItems"]
        );
        assert_eq!(fake.transactions.lock().unwrap().len(), 1);

        // A throttle is not committed and is repeated too.
        fake.faults.lock().unwrap().push_back(Fault::Throttled);
        let (_, token) = bucket.get("deploy/current.json").await.unwrap().unwrap();
        assert_eq!(
            bucket
                .swap_all(&[swap(
                    "deploy/current.json",
                    r#"{"v":2}"#,
                    Expect::Token(token)
                )])
                .await
                .unwrap(),
            None
        );
    });
}

#[test]
fn deploy_pointers_switch_in_order_on_the_bucket() {
    crate::asyncrt::test_block_on(async {
        let bucket = bucket();
        resolve_with(
            &bucket,
            Role::Node,
            &Settings {
                backend: Some(Backend::Bucket),
                region: None,
                endpoint: None,
                lease_shards: None,
            },
            None,
        )
        .await
        .unwrap();
        bucket
            .put_cas("deploy/current.json", br#"{"v":0}"#.to_vec(), None)
            .await
            .unwrap()
            .unwrap();
        let lost = bucket
            .swap_all(&[
                swap("deploy/api/current.json", r#"{"v":1}"#, Expect::Absent),
                swap("deploy/current.json", r#"{"v":1}"#, Expect::Absent),
            ])
            .await
            .unwrap();
        assert_eq!(lost.as_deref(), Some("deploy/current.json"));
        // The bucket has no transaction: the earlier write stays.
        assert_eq!(
            bucket
                .get("deploy/api/current.json")
                .await
                .unwrap()
                .unwrap()
                .0
                .as_ref(),
            br#"{"v":1}"#
        );
    });
}

#[test]
fn lease_shards_are_stable() {
    // A node's shard is part of the table's layout: these must never move.
    assert_eq!(lease_pk(0, 1), "nodes");
    assert_eq!(lease_pk(3, 8), "nodes#3");
    assert_eq!(lease_shard_of_pk("nodes"), Some(0));
    assert_eq!(lease_shard_of_pk("nodes#7"), Some(7));
    assert_eq!(lease_shard_of_pk("nodesx"), None);
    assert_eq!(lease_shard_of_pk("deploy"), None);
    assert_eq!(lease_shard("anything", 1), 0);
    let shards: Vec<u32> = ["n0", "n1", "n2", "n3"]
        .iter()
        .map(|node| lease_shard(node, 8))
        .collect();
    assert_eq!(shards, [3, 0, 1, 6]);
}

/// A table fleet whose leases spread over `shards` partitions.
async fn sharded_fleet(shards: u32) -> (Bucket, Arc<FakeTable>) {
    let bucket = bucket();
    let fake = Arc::new(FakeTable::default());
    let settings = Settings {
        lease_shards: Some(shards),
        ..table_settings()
    };
    resolve_with(&bucket, Role::Node, &settings, Some(fake.clone()))
        .await
        .unwrap();
    (bucket, fake)
}

#[test]
fn leases_spread_over_the_shards_the_claim_fixes() {
    crate::asyncrt::test_block_on(async {
        let (bucket, fake) = sharded_fleet(4).await;
        let nodes: Vec<String> = (0..20).map(|n| format!("n{n:02}")).collect();
        for node in &nodes {
            bucket
                .put_cas(
                    &format!("nodes/{node}.json"),
                    format!(r#"{{"node":"{node}"}}"#).into_bytes(),
                    None,
                )
                .await
                .unwrap()
                .expect("create applies");
        }
        let partitions: BTreeSet<String> = fake
            .items
            .lock()
            .unwrap()
            .keys()
            .filter(|(pk, _)| lease_shard_of_pk(pk).is_some())
            .map(|(pk, _)| pk.clone())
            .collect();
        assert_eq!(
            partitions,
            BTreeSet::from(["nodes#0", "nodes#1", "nodes#2", "nodes#3"].map(String::from))
        );

        // Every way of reading the leases sees every shard.
        let expected: Vec<String> = nodes.iter().map(|n| format!("nodes/{n}.json")).collect();
        let listed: Vec<String> = bucket
            .list("nodes/")
            .await
            .unwrap()
            .into_iter()
            .map(|meta| meta.location.to_string())
            .collect();
        assert_eq!(listed, expected);
        let table = bucket.control_route().resolved().unwrap().unwrap().clone();
        let mut paged = Vec::new();
        let mut cursor = None;
        loop {
            let (page, next) = table.lease_page(cursor, 3).await.unwrap();
            assert!(page.len() <= 3);
            paged.extend(page.into_iter().map(|listed| listed.key));
            match next {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }
        paged.sort();
        assert_eq!(paged, expected);
        let all: Vec<String> = table
            .leases()
            .await
            .unwrap()
            .into_iter()
            .map(|listed| listed.key)
            .collect();
        assert_eq!(all, expected);

        // Another client learns the shards from the claim, not its settings.
        let other = bucket_sharing(&bucket);
        resolve_with(&other, Role::Node, &table_settings(), Some(fake.clone()))
            .await
            .unwrap();
        let (body, _) = other.get("nodes/n07.json").await.unwrap().unwrap();
        assert_eq!(body.as_ref(), br#"{"node":"n07"}"#);
        assert_eq!(other.list("nodes/").await.unwrap().len(), 20);

        // The shards are fixed once claimed.
        let error = init_with(
            &bucket_sharing(&bucket),
            &Settings {
                lease_shards: Some(8),
                ..table_settings()
            },
            false,
            Some(fake.clone()),
        )
        .await
        .unwrap_err();
        assert!(
            format!("{error:#}").contains("claimed with 4 lease shards"),
            "{error:#}"
        );
    });
}

#[test]
fn the_lease_view_is_shared_until_it_ages() {
    crate::asyncrt::test_block_on(async {
        let (bucket, fake) = table_fleet().await;
        let lease = |node: &str| format!(r#"{{"node":"{node}"}}"#).into_bytes();
        bucket
            .put_cas("nodes/a.json", lease("a"), None)
            .await
            .unwrap();
        let queries = || {
            fake.calls
                .lock()
                .unwrap()
                .iter()
                .filter(|op| **op == "Query")
                .count()
        };
        let long = Duration::from_secs(3600);

        let before = queries();
        let first = bucket.table_lease_view(long).await.unwrap().unwrap();
        assert_eq!(queries(), before + 1, "one query per shard");
        assert_eq!(first.nodes().collect::<Vec<_>>(), ["a"]);

        // A clone of the client shares the view.
        bucket
            .put_cas("nodes/b.json", lease("b"), None)
            .await
            .unwrap();
        let shared = bucket
            .clone()
            .table_lease_view(long)
            .await
            .unwrap()
            .unwrap();
        assert!(Arc::ptr_eq(&first, &shared));
        assert_eq!(queries(), before + 1);

        // A caller that needs a fresh answer reads again.
        let fresh = bucket
            .table_lease_view(Duration::ZERO)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(fresh.nodes().collect::<Vec<_>>(), ["a", "b"]);
        assert_eq!(queries(), before + 2);

        // A bucket fleet has no view.
        let plain = bucket_fleet_for_test().await;
        assert!(plain.table_lease_view(long).await.unwrap().is_none());
    });
}

async fn bucket_fleet_for_test() -> Bucket {
    let bucket = bucket();
    resolve_with(
        &bucket,
        Role::Node,
        &Settings {
            backend: Some(Backend::Bucket),
            ..table_settings()
        },
        None,
    )
    .await
    .unwrap();
    bucket
}

/// Two clients of one fleet bucket, as a migration command and a node
/// started after it have, over a store that pages its listings.
struct SharedFleet {
    store: Arc<crate::local_store::LocalStore>,
    _dir: tempfile::TempDir,
}

impl SharedFleet {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let store =
            Arc::new(crate::local_store::LocalStore::open(dir.path().join("store.db")).unwrap());
        Self { store, _dir: dir }
    }

    /// A fresh, unresolved client of the fleet.
    fn client(&self) -> Bucket {
        Bucket::with_stores(
            self.store.clone(),
            self.store.clone(),
            StorageBackend::S3,
            "shared".into(),
            "fleet-a/".into(),
        )
        .with_paginated_for_test(self.store.clone())
        .with_unresolved_control_for_test()
    }
}

fn bucket_settings() -> Settings {
    Settings {
        backend: Some(Backend::Bucket),
        region: Some("us-east-1".into()),
        endpoint: None,
        lease_shards: None,
    }
}

fn follow_marker() -> Settings {
    Settings {
        backend: None,
        region: Some("us-east-1".into()),
        endpoint: None,
        lease_shards: None,
    }
}

const STOPPED_LEASE: &[u8] =
    br#"{"node":"n1","expires_ms":1,"log":{"state":"sealed","epoch":1,"ensemble":[],"tiered":0}}"#;

async fn read_marker_of(bucket: &Bucket) -> Marker {
    let (bytes, _) = bucket.get_bucket_object(MARKER_KEY).await.unwrap().unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

fn owner_item(fake: &FakeTable, cell: &str) -> Option<String> {
    fake.items
        .lock()
        .unwrap()
        .get(&(format!("cell#{cell}"), "own".to_string()))
        .map(|item| item["doc"]["S"].as_str().unwrap().to_string())
}

#[test]
fn a_bucket_fleet_migrates_to_a_table() {
    crate::asyncrt::test_block_on(async {
        let fleet = SharedFleet::new();
        let fake = Arc::new(FakeTable::default());

        // A stopped bucket fleet with two owned cells and its records.
        let old = fleet.client();
        resolve_with(&old, Role::Node, &bucket_settings(), None)
            .await
            .unwrap();
        for (cell, body) in [
            ("Room:a", r#"{"node":"n1","epoch":3}"#),
            ("Room:b", r#"{"node":"","epoch":7}"#),
        ] {
            old.put_cas(
                &format!("cells/{cell}/own.json"),
                body.as_bytes().to_vec(),
                None,
            )
            .await
            .unwrap()
            .unwrap();
            old.put(&format!("cells/{cell}/ltx/e1/0/1-1.ltx"), b"ltx".to_vec())
                .await
                .unwrap();
        }
        old.put_cas("nodes/n1.json", STOPPED_LEASE.to_vec(), None)
            .await
            .unwrap()
            .unwrap();
        old.put("deploy/current.json", br#"{"version":"v1"}"#.to_vec())
            .await
            .unwrap();

        let command = fleet.client();
        let migrated = migrate::migrate_with(
            &command,
            &follow_marker(),
            Backend::DynamoDb {
                table: "celld-test".into(),
            },
            false,
            Some(fake.clone()),
        )
        .await
        .unwrap();
        assert_eq!(migrated.from, Backend::Bucket);
        assert_eq!(migrated.moved, 2);
        let marker = read_marker_of(&command).await;
        assert_eq!(marker.backend, "dynamodb");
        assert_eq!(marker.migrating.as_ref().unwrap().from, "bucket");
        // The fleet records moved; the ownership records wait in the bucket.
        assert!(command
            .get_bucket_object("nodes/n1.json")
            .await
            .unwrap()
            .is_none());
        assert!(command
            .get_bucket_object("deploy/current.json")
            .await
            .unwrap()
            .is_none());
        assert!(command
            .get_bucket_object("cells/Room:a/own.json")
            .await
            .unwrap()
            .is_some());
        assert_eq!(owner_item(&fake, "Room:a"), None);

        // A node on the table reads an ownership record and copies it.
        let node = fleet.client();
        resolve_with(&node, Role::Node, &table_settings(), Some(fake.clone()))
            .await
            .unwrap();
        assert!(node.control_migrating());
        let (body, _) = node.get("cells/Room:a/own.json").await.unwrap().unwrap();
        assert_eq!(body.as_ref(), br#"{"node":"n1","epoch":3}"#);
        assert_eq!(
            owner_item(&fake, "Room:a").as_deref(),
            Some(r#"{"node":"n1","epoch":3}"#)
        );
        let (lease, _) = node.get("nodes/n1.json").await.unwrap().unwrap();
        assert_eq!(lease.as_ref(), STOPPED_LEASE);
        // A create of a record the bucket still holds loses to that record.
        assert!(node
            .put_cas(
                "cells/Room:b/own.json",
                br#"{"node":"n2","epoch":1}"#.to_vec(),
                None
            )
            .await
            .unwrap()
            .is_none());
        assert_eq!(
            owner_item(&fake, "Room:b").as_deref(),
            Some(r#"{"node":"","epoch":7}"#)
        );

        // The walk copies what is left, clears the bucket, and finishes.
        assert!(migrate::migration_pass(&node, "n9", 1000).await.unwrap());
        assert!(!node.control_migrating());
        assert_eq!(read_marker_of(&node).await.migrating, None);
        for cell in ["Room:a", "Room:b"] {
            assert!(node
                .get_bucket_object(&format!("cells/{cell}/own.json"))
                .await
                .unwrap()
                .is_none());
            assert!(owner_item(&fake, cell).is_some());
        }
        assert!(node
            .get_bucket_object("cells/Room:a/ltx/e1/0/1-1.ltx")
            .await
            .unwrap()
            .is_some());
    });
}

#[test]
fn a_table_fleet_migrates_back_to_the_bucket() {
    crate::asyncrt::test_block_on(async {
        let fleet = SharedFleet::new();
        let fake = Arc::new(FakeTable::default());
        let old = fleet.client();
        resolve_with(&old, Role::Node, &table_settings(), Some(fake.clone()))
            .await
            .unwrap();
        old.put_cas(
            "cells/Room:a/own.json",
            br#"{"node":"n1","epoch":4}"#.to_vec(),
            None,
        )
        .await
        .unwrap()
        .unwrap();
        old.put_cas(
            "cells/Room:b/own.json",
            br#"{"node":"","epoch":2}"#.to_vec(),
            None,
        )
        .await
        .unwrap()
        .unwrap();
        old.put_cas("nodes/n1.json", STOPPED_LEASE.to_vec(), None)
            .await
            .unwrap()
            .unwrap();
        old.put("deploy/api/current.json", br#"{"version":"v2"}"#.to_vec())
            .await
            .unwrap();

        let command = fleet.client();
        let migrated = migrate::migrate_with(
            &command,
            &follow_marker(),
            Backend::Bucket,
            false,
            Some(fake.clone()),
        )
        .await
        .unwrap();
        assert_eq!(migrated.moved, 2);
        let marker = read_marker_of(&command).await;
        assert_eq!(marker.backend, "bucket");
        assert_eq!(
            marker.migrating.as_ref().unwrap().table.as_deref(),
            Some("celld-test")
        );
        assert!(!fake
            .items
            .lock()
            .unwrap()
            .keys()
            .any(|(pk, _)| pk == NODES_PK || pk == DEPLOY_PK));

        // Running it again finishes nothing new and moves nothing.
        let again = migrate::migrate_with(
            &command,
            &follow_marker(),
            Backend::Bucket,
            false,
            Some(fake.clone()),
        )
        .await
        .unwrap();
        assert!(again.resumed);
        // A different destination waits for this migration to finish.
        assert!(migrate::migrate_with(
            &command,
            &follow_marker(),
            Backend::DynamoDb {
                table: "celld-test".into()
            },
            false,
            Some(fake.clone()),
        )
        .await
        .is_err());

        let node = fleet.client();
        resolve_with(&node, Role::Node, &follow_marker(), Some(fake.clone()))
            .await
            .unwrap();
        assert_eq!(node.control_scheme(), "s3");
        let (body, _) = node.get("cells/Room:a/own.json").await.unwrap().unwrap();
        assert_eq!(body.as_ref(), br#"{"node":"n1","epoch":4}"#);
        assert!(node
            .get_bucket_object("cells/Room:a/own.json")
            .await
            .unwrap()
            .is_some());
        assert_eq!(
            node.get_bucket_object("deploy/api/current.json")
                .await
                .unwrap()
                .unwrap()
                .0
                .as_ref(),
            br#"{"version":"v2"}"#
        );

        assert!(migrate::migration_pass(&node, "n9", 1000).await.unwrap());
        assert_eq!(read_marker_of(&node).await.migrating, None);
        assert_eq!(owner_item(&fake, "Room:a"), None);
        assert_eq!(owner_item(&fake, "Room:b"), None);
        let (body, _) = node.get("cells/Room:b/own.json").await.unwrap().unwrap();
        assert_eq!(body.as_ref(), br#"{"node":"","epoch":2}"#);
    });
}

#[test]
fn a_migration_waits_for_every_node_to_stop() {
    crate::asyncrt::test_block_on(async {
        let fleet = SharedFleet::new();
        let old = fleet.client();
        resolve_with(&old, Role::Node, &bucket_settings(), None)
            .await
            .unwrap();
        let live = format!(
            r#"{{"node":"n1","expires_ms":{}}}"#,
            crate::ownership_store::now_ms() + 60_000
        );
        old.put_cas("nodes/n1.json", live.into_bytes(), None)
            .await
            .unwrap()
            .unwrap();
        let fake = Arc::new(FakeTable::default());
        let error = migrate::migrate_with(
            &fleet.client(),
            &follow_marker(),
            Backend::DynamoDb {
                table: "celld-test".into(),
            },
            false,
            Some(fake.clone()),
        )
        .await
        .unwrap_err();
        assert!(format!("{error:#}").contains("n1"), "{error:#}");
        assert_eq!(read_marker_of(&old).await.backend, "bucket");
        assert!(fake.items.lock().unwrap().is_empty());
    });
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

#[test]
fn a_migration_into_a_sharded_table_moves_every_lease_and_back() {
    crate::asyncrt::test_block_on(async {
        let fleet = SharedFleet::new();
        let fake = Arc::new(FakeTable::default());
        let old = fleet.client();
        resolve_with(&old, Role::Node, &bucket_settings(), None)
            .await
            .unwrap();
        let nodes = ["n0", "n1", "n2", "n3"];
        for node in nodes {
            let lease = format!(
                r#"{{"node":"{node}","expires_ms":1,"log":{{"state":"sealed","epoch":1,"ensemble":[],"tiered":0}}}}"#
            );
            old.put_cas(&format!("nodes/{node}.json"), lease.into_bytes(), None)
                .await
                .unwrap()
                .unwrap();
        }

        let sharded = Settings {
            lease_shards: Some(8),
            ..follow_marker()
        };
        let migrated = migrate::migrate_with(
            &fleet.client(),
            &sharded,
            Backend::DynamoDb {
                table: "celld-test".into(),
            },
            false,
            Some(fake.clone()),
        )
        .await
        .unwrap();
        assert_eq!(migrated.moved, 4);
        // The leases landed in the shards the claim fixes.
        let partitions: BTreeSet<String> = fake
            .items
            .lock()
            .unwrap()
            .keys()
            .filter(|(pk, _)| lease_shard_of_pk(pk).is_some())
            .map(|(pk, _)| pk.clone())
            .collect();
        assert_eq!(
            partitions,
            BTreeSet::from(["nodes#0", "nodes#1", "nodes#3", "nodes#6"].map(String::from))
        );

        // Moving back finds every lease in every shard.
        let command = fleet.client();
        let node = fleet.client();
        resolve_with(&node, Role::Node, &follow_marker(), Some(fake.clone()))
            .await
            .unwrap();
        assert!(migrate::migration_pass(&node, "n9", 1000).await.unwrap());
        let back = migrate::migrate_with(
            &command,
            &follow_marker(),
            Backend::Bucket,
            false,
            Some(fake.clone()),
        )
        .await
        .unwrap();
        // The four leases, and the waker record the walk was elected by.
        assert_eq!(back.moved, 5);
        assert!(!fake
            .items
            .lock()
            .unwrap()
            .keys()
            .any(|(pk, _)| lease_shard_of_pk(pk).is_some()));
        for node in nodes {
            assert!(command
                .get_bucket_object(&format!("nodes/{node}.json"))
                .await
                .unwrap()
                .is_some());
        }
    });
}

#[test]
fn a_sharded_table_fleet_moves_out_only_when_every_shard_is_stopped() {
    crate::asyncrt::test_block_on(async {
        let fleet = SharedFleet::new();
        let fake = Arc::new(FakeTable::default());
        let old = fleet.client();
        let sharded = Settings {
            lease_shards: Some(4),
            ..table_settings()
        };
        resolve_with(&old, Role::Node, &sharded, Some(fake.clone()))
            .await
            .unwrap();
        // An expired lease whose log a successor must still recover.
        let open_log = br#"{"node":"n2","expires_ms":1,"log":{"state":"open","epoch":3,"ensemble":["b1"],"tiered":0}}"#;
        old.put_cas("nodes/n2.json", open_log.to_vec(), None)
            .await
            .unwrap()
            .unwrap();
        let live = format!(
            r#"{{"node":"n0","expires_ms":{}}}"#,
            crate::ownership_store::now_ms() + 60_000
        );
        let live_token = old
            .put_cas("nodes/n0.json", live.into_bytes(), None)
            .await
            .unwrap()
            .unwrap();
        let shards: BTreeSet<String> = fake
            .items
            .lock()
            .unwrap()
            .keys()
            .filter(|(pk, _)| lease_shard_of_pk(pk).is_some())
            .map(|(pk, _)| pk.clone())
            .collect();
        assert!(!shards.contains("nodes"), "{shards:?}");

        // A live lease in any shard stops the move.
        let error = migrate::migrate_with(
            &fleet.client(),
            &follow_marker(),
            Backend::Bucket,
            false,
            Some(fake.clone()),
        )
        .await
        .unwrap_err();
        assert!(format!("{error:#}").contains("n0"), "{error:#}");
        assert_eq!(read_marker_of(&old).await.backend, "dynamodb");

        // Once it stops, every lease moves, open log and all.
        old.put_cas("nodes/n0.json", STOPPED_LEASE.to_vec(), Some(&live_token))
            .await
            .unwrap()
            .unwrap();
        let command = fleet.client();
        let migrated = migrate::migrate_with(
            &command,
            &follow_marker(),
            Backend::Bucket,
            false,
            Some(fake.clone()),
        )
        .await
        .unwrap();
        assert_eq!(migrated.moved, 2);
        let (moved, _) = command
            .get_bucket_object("nodes/n2.json")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(moved.as_ref(), open_log);
        assert!(command
            .get_bucket_object("nodes/n0.json")
            .await
            .unwrap()
            .is_some());
    });
}

#[test]
fn a_table_fleet_cannot_move_to_another_table() {
    crate::asyncrt::test_block_on(async {
        let fleet = SharedFleet::new();
        let fake = Arc::new(FakeTable::default());
        let old = fleet.client();
        resolve_with(&old, Role::Node, &table_settings(), Some(fake.clone()))
            .await
            .unwrap();
        old.put_cas("nodes/n1.json", STOPPED_LEASE.to_vec(), None)
            .await
            .unwrap()
            .unwrap();
        let before = fake.items.lock().unwrap().len();
        let error = migrate::migrate_with(
            &fleet.client(),
            &follow_marker(),
            Backend::DynamoDb {
                table: "other".into(),
            },
            false,
            Some(fake.clone()),
        )
        .await
        .unwrap_err();
        assert!(format!("{error:#}").contains("bucket first"), "{error:#}");
        let marker = read_marker_of(&old).await;
        assert_eq!(marker.table.as_deref(), Some("celld-test"));
        assert_eq!(marker.migrating, None);
        assert_eq!(fake.items.lock().unwrap().len(), before);
        assert!(old.get("nodes/n1.json").await.unwrap().is_some());
    });
}

#[test]
fn a_deploy_switch_must_fit_one_transaction_on_a_table() {
    crate::asyncrt::test_block_on(async {
        // Two pointers ride beside the attachments in the transaction.
        let (table, _fake) = table_fleet().await;
        crate::deploy::check_switch_fits(&table, MAX_TRANSACT_WRITES - 2)
            .await
            .unwrap();
        // Fifty queues released and fifty claimed: one hundred attachments.
        let error = crate::deploy::check_switch_fits(&table, 100)
            .await
            .unwrap_err();
        let message = format!("{error:#}");
        assert!(message.contains("at most 98"), "{message}");
        assert!(message.contains("Deploy in stages"), "{message}");
        // The bucket writes in order and has no such limit.
        let bucket = bucket_fleet_for_test().await;
        crate::deploy::check_switch_fits(&bucket, 100)
            .await
            .unwrap();
    });
}

fn item_doc(fake: &FakeTable, pk: &str, sk: &str) -> Option<Value> {
    fake.items
        .lock()
        .unwrap()
        .get(&(pk.to_string(), sk.to_string()))
        .map(|item| serde_json::from_str(item["doc"]["S"].as_str().unwrap()).unwrap())
}

#[test]
fn a_table_fleet_keeps_load_out_of_its_leases() {
    crate::asyncrt::test_block_on(async {
        let (bucket, fake) = table_fleet().await;
        let ownership = crate::ownership_store::BucketOwnership::new(
            bucket.clone(),
            bucket.clone(),
            "n1".into(),
            "probe-key".into(),
        )
        .with_lease_ttl_ms(10_000);
        let record = celld_logic::NodeLeaseRecord {
            node: "n1".into(),
            addr: "10.0.0.1:9000".into(),
            expires_ms: crate::ownership_store::now_ms() + 10_000,
            peer_protocol: 1,
            generation: "probe-key".into(),
            log_state: None,
            etag: String::new(),
        };
        let outcome = ownership
            .cas_node_lease(celld_logic::CasGuard::Absent, &record, &mut None)
            .await
            .unwrap();
        assert!(matches!(
            outcome,
            celld_logic::LeaseCasOutcome::Applied { .. }
        ));
        // The load item is published beside the renewal.
        for _ in 0..200 {
            if item_doc(&fake, "load", "n1").is_some() {
                break;
            }
            crate::asyncrt::sleep(Duration::from_millis(5)).await;
        }
        let lease = item_doc(&fake, NODES_PK, "n1").unwrap();
        assert!(lease.get("load").is_none(), "{lease}");
        assert_eq!(lease["addr"], "10.0.0.1:9000");
        let load = item_doc(&fake, "load", "n1").expect("a load item");
        assert!(load.get("log").is_none(), "{load}");
        assert!(load["load"]["sampled_ms"].as_u64().unwrap() > 0);
        assert_eq!(load["expires_ms"], lease["expires_ms"]);

        // Placement reads the load items directly; no shared sample object.
        let (_, peers) = ownership.read_shared_capacity_peers(5_000).await.unwrap();
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].node, "n1");
        assert!(peers[0].sampled_ms > 0);
        assert!(bucket
            .get_bucket_object("fleet/capacity-v1.json")
            .await
            .unwrap()
            .is_none());
        assert!(fake
            .items
            .lock()
            .unwrap()
            .keys()
            .all(|(pk, _)| pk != FLEET_PK));
    });
}
