// Copyright 2026 Deno Land Inc. Apache-2.0 license.

use super::*;
use object_store::UpdateVersion;
use std::sync::Barrier;

const WRITERS: usize = 16;

fn store() -> (tempfile::TempDir, LocalStore) {
    let directory = tempfile::tempdir().unwrap();
    let store = LocalStore::open(directory.path().join("store.sqlite")).unwrap();
    (directory, store)
}

fn put(
    store: &LocalStore,
    key: &str,
    body: &str,
    mode: PutMode,
) -> object_store::Result<PutResult> {
    store.put_sync(
        key.to_string(),
        Bytes::from(body.to_string()),
        PutOptions {
            mode,
            ..PutOptions::default()
        },
    )
}

/// Run `WRITERS` puts at once, so they queue behind one committer and share
/// transactions.
fn concurrently<T: Send>(
    store: &LocalStore,
    each: impl Fn(&LocalStore, usize) -> T + Sync,
) -> Vec<T> {
    let start = Barrier::new(WRITERS);
    std::thread::scope(|scope| {
        let handles = (0..WRITERS)
            .map(|writer| {
                let (start, each) = (&start, &each);
                scope.spawn(move || {
                    start.wait();
                    each(store, writer)
                })
            })
            .collect::<Vec<_>>();
        handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect()
    })
}

#[test]
fn grouped_creates_of_one_key_admit_exactly_one() {
    let (_directory, store) = store();
    let results = concurrently(&store, |store, writer| {
        put(
            store,
            "cells/a/own.json",
            &writer.to_string(),
            PutMode::Create,
        )
    });
    let created = results.iter().filter(|result| result.is_ok()).count();
    assert_eq!(created, 1);
    assert!(results.iter().all(|result| match result {
        Ok(_) => true,
        Err(Error::AlreadyExists { .. }) => true,
        Err(error) => panic!("unexpected error: {error}"),
    }));
    let winner = results
        .iter()
        .position(Result::is_ok)
        .expect("one create succeeded");
    let stored = store.read("cells/a/own.json").unwrap();
    assert_eq!(stored.body, winner.to_string().into_bytes());
}

#[test]
fn grouped_puts_to_distinct_keys_all_commit_with_distinct_etags() {
    let (_directory, store) = store();
    let results = concurrently(&store, |store, writer| {
        put(
            store,
            &format!("cells/{writer}/ltx"),
            &writer.to_string(),
            PutMode::Overwrite,
        )
        .unwrap()
    });
    let etags = results
        .iter()
        .map(|result| result.e_tag.clone().unwrap())
        .collect::<BTreeSet<_>>();
    assert_eq!(etags.len(), WRITERS);
    for writer in 0..WRITERS {
        let stored = store.read(&format!("cells/{writer}/ltx")).unwrap();
        assert_eq!(stored.body, writer.to_string().into_bytes());
    }
}

#[test]
fn grouped_updates_from_one_version_admit_exactly_one() {
    let (_directory, store) = store();
    let first = put(&store, "nodes/n", "first", PutMode::Overwrite).unwrap();
    let version = UpdateVersion {
        e_tag: first.e_tag.clone(),
        version: None,
    };
    let results = concurrently(&store, |store, writer| {
        put(
            store,
            "nodes/n",
            &writer.to_string(),
            PutMode::Update(version.clone()),
        )
    });
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert!(results.iter().all(|result| match result {
        Ok(_) => true,
        Err(Error::Precondition { .. }) => true,
        Err(error) => panic!("unexpected error: {error}"),
    }));
    let current = results
        .iter()
        .find_map(|result| result.as_ref().ok())
        .unwrap();
    let stale = put(&store, "nodes/n", "stale", PutMode::Update(version));
    assert!(matches!(stale, Err(Error::Precondition { .. })));
    let next = put(
        &store,
        "nodes/n",
        "next",
        PutMode::Update(UpdateVersion {
            e_tag: current.e_tag.clone(),
            version: None,
        }),
    );
    assert!(next.is_ok());
}

const LISTED: &[&str] = &[
    "a/b",
    "a/b/c",
    "a/b/d/e",
    "a/b/d/f/g",
    "a/b-c/d",
    "a/b.c",
    "a/bc",
    "a/b0",
    "nodes",
    "nodes-x",
    "nodes.x",
    "nodes0",
    "nodes/n1/0001.ltx",
    "nodes/n1/0002.ltx",
    "nodes/n1-old/0001.ltx",
    "nodes/n2/sub/0001.ltx",
    "nodes/own.json",
    "z",
];

fn listed_store() -> (tempfile::TempDir, LocalStore) {
    let (directory, store) = store();
    for key in LISTED {
        put(&store, key, key, PutMode::Overwrite).unwrap();
    }
    // A key that needs encoding, so a common prefix built from it must
    // come back in its stored form and not be encoded twice.
    let encoded = Path::from_iter(["enc %", "přehled.html"]);
    put(&store, encoded.as_ref(), "encoded", PutMode::Overwrite).unwrap();
    (directory, store)
}

fn keys(objects: &[ObjectMeta]) -> Vec<String> {
    objects
        .iter()
        .map(|object| object.location.to_string())
        .collect()
}

async fn list(store: &LocalStore, prefix: Option<&str>) -> Vec<String> {
    let prefix = prefix.map(Path::from);
    let objects = store
        .list(prefix.as_ref())
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<object_store::Result<Vec<_>>>()
        .unwrap();
    keys(&objects)
}

async fn list_with_delimiter(
    store: &LocalStore,
    prefix: Option<&str>,
) -> (Vec<String>, Vec<String>) {
    let prefix = prefix.map(Path::from);
    let result = store.list_with_delimiter(prefix.as_ref()).await.unwrap();
    let common = result.common_prefixes.iter().map(Path::to_string).collect();
    (common, keys(&result.objects))
}

#[tokio::test]
async fn list_matches_whole_segments() {
    let (_directory, store) = listed_store();
    assert_eq!(
        list(&store, Some("a/b")).await,
        ["a/b/c", "a/b/d/e", "a/b/d/f/g"]
    );
    assert_eq!(
        list(&store, Some("nodes")).await,
        [
            "nodes/n1-old/0001.ltx",
            "nodes/n1/0001.ltx",
            "nodes/n1/0002.ltx",
            "nodes/n2/sub/0001.ltx",
            "nodes/own.json",
        ]
    );
    assert_eq!(
        list(&store, Some("nodes/n1")).await,
        ["nodes/n1/0001.ltx", "nodes/n1/0002.ltx"]
    );
}

#[tokio::test]
async fn list_of_an_object_key_excludes_the_object() {
    let (_directory, store) = listed_store();
    assert!(list(&store, Some("a/b/c")).await.is_empty());
    assert!(list(&store, Some("nodes0")).await.is_empty());
    assert!(list(&store, Some("missing")).await.is_empty());
}

#[tokio::test]
async fn list_without_a_prefix_returns_every_object_in_key_order() {
    let (_directory, store) = listed_store();
    let mut expected = LISTED.iter().map(|key| key.to_string()).collect::<Vec<_>>();
    expected.push(Path::from_iter(["enc %", "přehled.html"]).to_string());
    expected.sort();
    assert_eq!(list(&store, None).await, expected);
    assert_eq!(list(&store, Some("")).await, expected);
}

#[tokio::test]
async fn list_with_delimiter_groups_nested_keys() {
    let (_directory, store) = listed_store();
    assert_eq!(
        list_with_delimiter(&store, Some("nodes")).await,
        (
            vec![
                "nodes/n1".to_string(),
                "nodes/n1-old".to_string(),
                "nodes/n2".to_string(),
            ],
            vec!["nodes/own.json".to_string()],
        )
    );
    assert_eq!(
        list_with_delimiter(&store, Some("a/b")).await,
        (vec!["a/b/d".to_string()], vec!["a/b/c".to_string()])
    );
    assert_eq!(
        list_with_delimiter(&store, Some("a/b/d")).await,
        (vec!["a/b/d/f".to_string()], vec!["a/b/d/e".to_string()])
    );
    assert_eq!(
        list_with_delimiter(&store, Some("a/b/c")).await,
        (vec![], vec![])
    );
    let encoded = Path::from_iter(["enc %"]).to_string();
    assert_eq!(
        list_with_delimiter(&store, None).await,
        (
            vec!["a".to_string(), encoded, "nodes".to_string()],
            vec![
                "nodes".to_string(),
                "nodes-x".to_string(),
                "nodes.x".to_string(),
                "nodes0".to_string(),
                "z".to_string(),
            ],
        )
    );
}

/// Every listing agrees with object_store's own prefix rule applied to the
/// whole bucket, for every prefix the listed keys give rise to.
#[tokio::test]
async fn listings_agree_with_filtering_every_object() {
    let (_directory, store) = listed_store();
    let all = list(&store, None).await;
    let mut prefixes = BTreeSet::from([String::new(), "nod".into(), "a/b/".into()]);
    for key in &all {
        let parts = key.split('/').collect::<Vec<_>>();
        for end in 1..=parts.len() {
            prefixes.insert(parts[..end].join("/"));
        }
    }
    for prefix in &prefixes {
        let prefix_path = Path::parse(prefix).unwrap();
        let below = all
            .iter()
            .filter(|key| {
                Path::parse(key.as_str())
                    .unwrap()
                    .prefix_match(&prefix_path)
                    .is_some_and(|mut remainder| remainder.next().is_some())
            })
            .cloned()
            .collect::<Vec<_>>();
        let listed_prefix = store.list(Some(&prefix_path)).collect::<Vec<_>>().await;
        let listed_prefix = keys(
            &listed_prefix
                .into_iter()
                .collect::<object_store::Result<Vec<_>>>()
                .unwrap(),
        );
        assert_eq!(listed_prefix, below, "list({prefix:?})");

        let mut common = BTreeSet::new();
        let mut objects = Vec::new();
        for key in &below {
            let location = Path::parse(key.as_str()).unwrap();
            let mut remainder = location.prefix_match(&prefix_path).unwrap();
            let child = remainder.next().unwrap();
            if remainder.next().is_some() {
                common.insert(prefix_path.child(child).to_string());
            } else {
                objects.push(key.clone());
            }
        }
        let listed = store.list_with_delimiter(Some(&prefix_path)).await.unwrap();
        let listed_common = listed
            .common_prefixes
            .iter()
            .map(Path::to_string)
            .collect::<Vec<_>>();
        assert_eq!(
            listed_common,
            common.into_iter().collect::<Vec<_>>(),
            "list_with_delimiter({prefix:?})"
        );
        assert_eq!(
            keys(&listed.objects),
            objects,
            "list_with_delimiter({prefix:?})"
        );
    }
}

async fn page(
    store: &LocalStore,
    prefix: &str,
    delimiter: Option<&str>,
    max_keys: usize,
    page_token: Option<String>,
) -> (Vec<String>, Vec<String>, Option<String>) {
    let page = store
        .list_paginated(
            Some(prefix),
            PaginatedListOptions {
                delimiter: delimiter.map(|delimiter| delimiter.to_string().into()),
                max_keys: Some(max_keys),
                page_token,
                ..PaginatedListOptions::default()
            },
        )
        .await
        .unwrap();
    let common = page
        .result
        .common_prefixes
        .iter()
        .map(Path::to_string)
        .collect();
    (common, keys(&page.result.objects), page.page_token)
}

#[tokio::test]
async fn paginated_listing_matches_raw_prefixes_across_pages() {
    let (_directory, store) = listed_store();
    // A paginated prefix is a plain string prefix, not a path.
    let (common, objects, token) = page(&store, "nodes", None, 100, None).await;
    assert!(common.is_empty());
    assert_eq!(
        objects,
        [
            "nodes",
            "nodes-x",
            "nodes.x",
            "nodes/n1-old/0001.ltx",
            "nodes/n1/0001.ltx",
            "nodes/n1/0002.ltx",
            "nodes/n2/sub/0001.ltx",
            "nodes/own.json",
            "nodes0",
        ]
    );
    assert_eq!(token, None);

    let mut pages = Vec::new();
    let mut token = None;
    loop {
        let (common, objects, next) = page(&store, "nodes/", Some("/"), 2, token).await;
        pages.push((common, objects));
        match next {
            Some(next) => token = Some(next),
            None => break,
        }
    }
    let strings = |values: &[&str]| values.iter().map(|value| value.to_string()).collect();
    assert_eq!(
        pages,
        [
            // `nodes/n1-old/` sorts before `nodes/n1/`.
            (strings(&["nodes/n1-old", "nodes/n1"]), vec![]),
            (strings(&["nodes/n2"]), strings(&["nodes/own.json"])),
        ]
    );
}

#[test]
fn prefix_successor_bounds_exactly_the_keys_with_the_prefix() {
    assert_eq!(prefix_successor(""), None);
    assert_eq!(prefix_successor("nodes/").as_deref(), Some("nodes0"));
    assert_eq!(prefix_successor("a\u{d7ff}").as_deref(), Some("a\u{e000}"));
    assert_eq!(prefix_successor("a\u{10ffff}").as_deref(), Some("b"));
    assert_eq!(prefix_successor("\u{10ffff}\u{10ffff}"), None);
}

#[test]
fn scans_seek_the_key_index() {
    let (_directory, store) = store();
    let connection = store.connect().unwrap();
    for inclusive in [false, true] {
        for bounded in [false, true] {
            let sql = scan_sql(inclusive, bounded);
            let plan = connection
                .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))
                .unwrap()
                .query_map(
                    rusqlite::named_params! { ":lower": "a", ":upper": "b" }
                        .iter()
                        .filter(|(name, _)| sql.contains(name))
                        .copied()
                        .collect::<Vec<_>>()
                        .as_slice(),
                    |row| row.get::<_, String>(3),
                )
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap()
                .join("\n");
            assert!(plan.contains("SEARCH objects USING INDEX"), "{sql}\n{plan}");
        }
    }
}
