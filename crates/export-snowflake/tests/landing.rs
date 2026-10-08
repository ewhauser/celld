use celld_export_format::*;
use celld_export_snowflake::{LandingRow, LANDING_COLUMNS};

fn record(facet: Option<&str>, body: Body) -> Record {
    Record {
        envelope: Envelope {
            stream: StreamId {
                script: "app".into(),
                class: "Room".into(),
                cell: "r1".into(),
                facet: facet.map(Into::into),
                incarnation: u64::MAX,
            },
            cell_name: None,
            position: Position::new(u64::MAX, 7, 3),
            committed_at: 1_790_000_000_123,
            node: "node-a".into(),
            origin: Origin::Repair,
            fragment: 2,
            fragments: 3,
        },
        body,
    }
}

#[test]
fn a_landing_row_holds_the_whole_record() {
    let bodies = [
        Body::Rows(RowsBody {
            data: TableRows {
                table: "t".into(),
                generation: 2,
                columns: vec!["k".into(), "v".into()],
                key_columns: vec![ROWID_KEY_COLUMN.into()],
                rows: vec![RowChange(
                    Op::Update,
                    vec![Value::Integer(1)],
                    vec![Value::Real(f64::INFINITY), Value::Blob(vec![1, 2])],
                )],
            },
        }),
        Body::Watermark(WatermarkBody {
            from: None,
            through: Position::new(1, 2, 3),
            commits: 1,
            records: 4,
        }),
        Body::Deleted(DeletedBody {
            facet: Some("f".into()),
            incarnation: Some(9),
            subtree: true,
            through_incarnation: Some(12),
        }),
    ];
    for body in bodies {
        for facet in [None, Some("f/g")] {
            let r = record(facet, body.clone());
            let row = LandingRow::from_record(&r, "blob-stream/3/17");
            assert_eq!(row.to_record().unwrap(), r);
            // The body carries only kind-specific fields.
            let body: serde_json::Map<String, serde_json::Value> =
                serde_json::from_str(&row.body).unwrap();
            for c in LANDING_COLUMNS.iter().filter(|c| **c != "body") {
                assert!(!body.contains_key(*c), "{c} in body");
            }
            // The row's JSON field names are the landing columns.
            let j = serde_json::to_value(&row).unwrap();
            let names: Vec<&str> = j.as_object().unwrap().keys().map(|k| k.as_str()).collect();
            let mut want = LANDING_COLUMNS.to_vec();
            want.sort();
            let mut got = names.clone();
            got.sort();
            assert_eq!(got, want);
        }
    }
}

/// Text that JSON must escape, and text it need not.
const AWKWARD: [&str; 4] = [
    "quote \" backslash \\ slash / newline \n tab \t nul \u{0} unit \u{1f}",
    "héllo wörld, 日本語, emoji 🌍🚀, separators \u{2028}\u{2029}",
    "{\"looks\":\"like json\"}",
    "",
];

fn values() -> Vec<Value> {
    let mut v = vec![
        Value::Null,
        Value::Integer(i64::MIN),
        Value::Integer(i64::MAX),
        Value::Real(-0.0),
        Value::Real(1e300),
        Value::Real(f64::MIN_POSITIVE),
        Value::Real(f64::INFINITY),
        Value::Real(f64::NEG_INFINITY),
        Value::Blob(vec![]),
        Value::Blob((0..=255).collect()),
        Value::Blob(b"{\"$blob\":\"not a tag\"}".to_vec()),
    ];
    v.extend(AWKWARD.map(|t| Value::Text(t.into())));
    v
}

fn table_rows() -> TableRows {
    let values = values();
    TableRows {
        table: AWKWARD[1].into(),
        generation: u64::MAX,
        columns: (0..values.len())
            .map(|i| format!("c{i} {}", AWKWARD[0]))
            .collect(),
        key_columns: vec!["c0".into()],
        rows: vec![
            RowChange(Op::Insert, vec![Value::Integer(1)], values.clone()),
            RowChange(
                Op::Update,
                vec![Value::Text(AWKWARD[0].into())],
                values.clone(),
            ),
            RowChange(Op::Delete, vec![Value::Blob(vec![0, 255])], values),
        ],
    }
}

fn tables() -> Vec<TableGen> {
    vec![
        TableGen {
            table: AWKWARD[0].into(),
            generation: 1,
        },
        TableGen {
            table: "t".into(),
            generation: u64::MAX,
        },
    ]
}

/// A body of every kind, with each optional field both set and not.
fn every_body() -> Vec<Body> {
    let bodies = vec![
        Body::Rows(RowsBody { data: table_rows() }),
        Body::Snapshot(SnapshotBody {
            snapshot_id: AWKWARD[0].into(),
            data: table_rows(),
        }),
        Body::SnapshotEnd(SnapshotEndBody {
            snapshot_id: "s".into(),
            scope: SnapshotScope::Tables,
            tables: tables(),
            records: u64::MAX,
        }),
        Body::Schema(SchemaBody {
            table: AWKWARD[1].into(),
            generation: 3,
            sql: format!("CREATE TABLE \"{}\" (k)", AWKWARD[0]),
            columns: vec![ColumnDef {
                name: AWKWARD[0].into(),
                decl_type: "TEXT".into(),
                pk: 1,
                not_null: true,
                generated: false,
            }],
            dropped: true,
            renamed_from: Some(AWKWARD[1].into()),
            unsupported: false,
        }),
        Body::Schema(SchemaBody {
            table: "t".into(),
            generation: 1,
            sql: String::new(),
            columns: vec![],
            dropped: false,
            renamed_from: None,
            unsupported: true,
        }),
        Body::Link(LinkBody {
            start_txid: 9,
            prev_epoch: Some(u64::MAX),
            prev_txid: Some(8),
            mode: LinkMode::Paged,
        }),
        Body::Link(LinkBody {
            start_txid: 0,
            prev_epoch: None,
            prev_txid: None,
            mode: LinkMode::Fresh,
        }),
        Body::Recovered(RecoveredBody {
            session: "node-a/7".into(),
            head: Position::new(4, 5, u64::MAX),
            loss: true,
            cells: 2,
        }),
        Body::Deleted(DeletedBody {
            facet: None,
            incarnation: None,
            subtree: false,
            through_incarnation: None,
        }),
        Body::Deleted(DeletedBody {
            facet: Some(AWKWARD[1].into()),
            incarnation: Some(9),
            subtree: true,
            through_incarnation: Some(12),
        }),
        Body::Watermark(WatermarkBody {
            from: Some(Position::new(1, 1, 1)),
            through: Position::new(1, 2, 3),
            commits: 1,
            records: 4,
        }),
        Body::Bulk(BulkBody { tables: tables() }),
        Body::Gap(GapBody {
            from: Position::new(1, 1, 1),
            to: Position::new(2, 0, 0),
            reason: AWKWARD[0].into(),
        }),
    ];
    let mut kinds: Vec<Kind> = bodies.iter().map(Body::kind).collect();
    kinds.dedup();
    assert_eq!(kinds, Kind::ALL, "a body of every kind");
    bodies
}

fn every_record() -> Vec<Record> {
    let mut out = Vec::new();
    for body in every_body() {
        for (facet, cell_name) in [(None, None), (Some(AWKWARD[1]), Some(AWKWARD[0]))] {
            let mut r = record(facet, body.clone());
            r.envelope.cell_name = cell_name.map(Into::into);
            r.envelope.stream.cell = AWKWARD[0].into();
            out.push(r);
        }
    }
    out
}

/// The body as the loader built it before it cut bodies from the message:
/// the record encoded as a JSON tree, less the envelope's fields.
fn tree_body(r: &Record) -> serde_json::Value {
    let mut fields = serde_json::to_value(r).unwrap();
    for c in LANDING_COLUMNS {
        fields.as_object_mut().unwrap().remove(c);
    }
    fields
}

#[test]
fn a_row_cut_from_a_message_holds_the_whole_record() {
    for r in every_record() {
        let json = r.to_json();
        let row = LandingRow::from_json(&json, "kafka/0/1").unwrap();
        assert_eq!(row.to_record().unwrap(), r);
        assert_eq!(row, LandingRow::from_record(&r, "kafka/0/1"));
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&row.body).unwrap(),
            tree_body(&r),
            "{}",
            row.body
        );
        // Cut, not re-encoded: every body field's bytes appear in the message.
        let body: std::collections::BTreeMap<String, &serde_json::value::RawValue> =
            serde_json::from_str(&row.body).unwrap();
        let message = String::from_utf8(json.clone()).unwrap();
        for (k, v) in body {
            let field = format!("{}:{}", serde_json::to_string(&k).unwrap(), v.get());
            assert!(message.contains(&field), "{field}");
        }
        // And through the NDJSON line Snowpipe Streaming reads, where the
        // body is the object itself, so it lands as a VARIANT.
        let line = serde_json::to_vec(&row).unwrap();
        let mut written = Vec::new();
        row.write_json(&mut written);
        assert_eq!(written, line);
        let back: LandingRow = serde_json::from_slice(&line).unwrap();
        assert_eq!(back, row);
        assert_eq!(back.to_record().unwrap(), r);
        let line: serde_json::Value = serde_json::from_slice(&line).unwrap();
        assert_eq!(line["body"], tree_body(&r));
    }
}

#[test]
fn a_rows_json_len_bound_holds_and_is_tight() {
    let written = |row: &LandingRow| {
        let mut out = Vec::new();
        row.write_json(&mut out);
        out.len()
    };
    // Every fixed part at its widest: the bound is exact.
    let widest = LandingRow {
        topic: None,
        kind: String::new(),
        script: String::new(),
        class: String::new(),
        cell: String::new(),
        cell_name: None,
        facet: None,
        incarnation: u64::MAX,
        epoch: u64::MAX,
        txid: u64::MAX,
        commit: u64::MAX,
        committed_at: i64::MIN,
        node: String::new(),
        origin: String::new(),
        fragment: u32::MAX,
        fragments: u32::MAX,
        body: "{}".into(),
        source: String::new(),
    };
    assert_eq!(widest.json_len_bound(), written(&widest));
    let topical = LandingRow {
        topic: Some(String::new()),
        ..widest.clone()
    };
    assert_eq!(topical.json_len_bound(), written(&topical));
    // Escapes, control characters, and non-ASCII never pass it.
    let odd = "q\"b\\s\n\u{1}\u{1f}\u{7f}é😀";
    let mut rows: Vec<LandingRow> = every_record()
        .iter()
        .map(|r| LandingRow::from_record(r, "kafka/0/1"))
        .collect();
    for mut row in rows.clone() {
        for s in [
            &mut row.kind,
            &mut row.script,
            &mut row.class,
            &mut row.cell,
            &mut row.node,
            &mut row.origin,
            &mut row.source,
        ] {
            s.push_str(odd);
        }
        row.cell_name = Some(odd.into());
        row.facet = Some(odd.repeat(3));
        row.topic = Some(odd.into());
        rows.push(row);
    }
    for row in &rows {
        let (bound, len) = (row.json_len_bound(), written(row));
        assert!(bound >= len, "{bound} < {len}");
        // Loose only for the envelope's strings, not the body.
        let strings = row.kind.len()
            + row.script.len()
            + row.class.len()
            + row.cell.len()
            + row.cell_name.as_ref().map_or(0, String::len)
            + row.facet.as_ref().map_or(0, String::len)
            + row.node.len()
            + row.origin.len()
            + row.source.len()
            + row.topic.as_ref().map_or(0, String::len);
        assert!(bound - len <= 6 * strings + 400, "{bound} - {len}");
    }
}

#[test]
fn a_row_keeps_the_messages_own_bytes() {
    // Whitespace, field order, and escapes as the message has them, and a
    // field this loader does not know, which a newer celld may add.
    let r = record(None, every_body().remove(0));
    let mut fields: Vec<(String, String)> =
        serde_json::from_slice::<serde_json::Map<String, serde_json::Value>>(&r.to_json())
            .unwrap()
            .into_iter()
            .map(|(k, v)| (k, v.to_string()))
            .collect();
    fields.reverse();
    let table = fields.iter_mut().find(|(k, _)| k == "table").unwrap();
    table.1 = "\"\\u00e9\"".into();
    let mut message = String::from("{ ");
    for (k, v) in &fields {
        message.push_str(&format!("{k:?} :\n {v} ,"));
    }
    // Snowflake refuses an object with a repeated key; the last one wins.
    message.push_str("\"x\": 1, \"x\": 2, \"t\\u0061ble_comment\": [1,  2] }");
    let row = LandingRow::from_json(message.as_bytes(), "kafka/0/1").unwrap();
    let mut want = r.clone();
    let Body::Rows(b) = &mut want.body else {
        unreachable!()
    };
    b.data.table = "é".into();
    assert_eq!(row.to_record().unwrap(), want);
    assert!(
        row.body
            .starts_with("{\"table\":\"\\u00e9\",\"rows\":[[\"I\""),
        "{}",
        row.body
    );
    assert!(
        row.body.ends_with(",\"table_comment\":[1,  2]}"),
        "{}",
        row.body
    );
}

#[test]
fn a_message_that_is_not_a_record_has_no_row() {
    let r = record(None, every_body().remove(0));
    let mut fragment = serde_json::to_value(&r).unwrap();
    fragment["fragment"] = 4.into();
    let mut kind = serde_json::to_value(&r).unwrap();
    kind["kind"] = "nope".into();
    let mut missing = serde_json::to_value(&r).unwrap();
    missing.as_object_mut().unwrap().remove("columns");
    let json = String::from_utf8(r.to_json()).unwrap();
    for bad in [
        "not json".to_string(),
        "[]".into(),
        "{}".into(),
        fragment.to_string(),
        kind.to_string(),
        missing.to_string(),
        json[..json.len() - 1].into(),
        format!("{json} trailing"),
    ] {
        assert!(
            LandingRow::from_json(bad.as_bytes(), "kafka/0/1").is_err(),
            "{bad}"
        );
    }
}

#[test]
fn a_row_names_its_topic_only_when_it_has_one() {
    let r = every_record().remove(0);
    let plain = LandingRow::from_record(&r, "kafka/0/1");
    let line: serde_json::Value = serde_json::to_value(&plain).unwrap();
    assert!(line.get("topic").is_none());

    let row = LandingRow {
        topic: Some(AWKWARD[0].into()),
        ..plain
    };
    let line = serde_json::to_vec(&row).unwrap();
    let mut written = Vec::new();
    row.write_json(&mut written);
    assert_eq!(written, line);
    let back: LandingRow = serde_json::from_slice(&line).unwrap();
    assert_eq!(back, row);
    assert_eq!(back.to_record().unwrap(), r);
    let line: serde_json::Value = serde_json::from_slice(&line).unwrap();
    assert_eq!(line["topic"], AWKWARD[0]);
}
