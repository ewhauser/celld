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
