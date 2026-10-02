//! `Record` decodes in one pass by hand. These tests hold it to the derive
//! it replaced, `Reference` below: on every input both must accept the same
//! record or both refuse.
//!
//! Set `CELLD_RECORDS` to a JSON-lines file of records (such as
//! `cargo run -p celld-export-snowflake --example scenarios`, or
//! `celld export inspect` output) to run the same comparison on it.

mod common;

use std::fmt;

use celld_export_format::*;
use common::*;
use serde::de::{MapAccess, Visitor};
use serde::{Deserialize, Deserializer};
use serde_json::value::RawValue;

/// The derived decoder `Record` had.
#[derive(Deserialize)]
struct Reference {
    #[serde(flatten)]
    envelope: Envelope,
    #[serde(flatten)]
    body: Body,
}

impl Reference {
    fn record(self) -> Record {
        Record {
            envelope: self.envelope,
            body: self.body,
        }
    }
}

/// What `Record::from_json` did with the derive.
fn reference_from_json(bytes: &[u8]) -> Option<Record> {
    let r = serde_json::from_slice::<Reference>(bytes).ok()?.record();
    let (i, n) = (r.envelope.fragment, r.envelope.fragments);
    (n != 0 && i != 0 && i <= n).then_some(r)
}

/// Decodes `bytes` with both, borrowing from the input and reading it as a
/// stream (which hands every string over owned), and returns whether the
/// record was accepted.
fn check(bytes: &[u8]) -> bool {
    let shown = || String::from_utf8_lossy(bytes).into_owned();
    let new = Record::from_json(bytes).ok();
    assert_eq!(new, reference_from_json(bytes), "from_json on {}", shown());
    let new = serde_json::from_reader::<_, Record>(bytes).ok();
    let old = serde_json::from_reader::<_, Reference>(bytes)
        .ok()
        .map(Reference::record);
    assert_eq!(new, old, "from_reader on {}", shown());
    new.is_some()
}

/// A record's top-level entries in order, values as written.
#[derive(Clone)]
struct Object(Vec<(String, String)>);

impl Object {
    fn parse(bytes: &[u8]) -> Object {
        struct Entries(Vec<(String, String)>);
        impl<'de> Deserialize<'de> for Entries {
            fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                struct V;
                impl<'de> Visitor<'de> for V {
                    type Value = Entries;
                    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                        f.write_str("an object")
                    }
                    fn visit_map<A: MapAccess<'de>>(self, mut m: A) -> Result<Entries, A::Error> {
                        let mut out = Vec::new();
                        while let Some((k, v)) = m.next_entry::<String, Box<RawValue>>()? {
                            out.push((k, v.get().to_owned()));
                        }
                        Ok(Entries(out))
                    }
                }
                d.deserialize_map(V)
            }
        }
        Object(serde_json::from_slice::<Entries>(bytes).unwrap().0)
    }

    fn bytes(&self) -> Vec<u8> {
        let fields: Vec<String> = self
            .0
            .iter()
            .map(|(k, v)| format!("{}:{v}", serde_json::to_string(k).unwrap()))
            .collect();
        format!("{{{}}}", fields.join(",")).into_bytes()
    }

    fn kind_last(&self) -> Object {
        let (kind, mut rest): (Vec<_>, Vec<_>) =
            self.0.iter().cloned().partition(|(k, _)| k == "kind");
        rest.extend(kind);
        Object(rest)
    }

    fn reversed(&self) -> Object {
        Object(self.0.iter().rev().cloned().collect())
    }

    fn shuffled(&self, mut seed: u64) -> Object {
        let mut v = self.0.clone();
        for i in (1..v.len()).rev() {
            seed = seed
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            v.swap(i, (seed >> 33) as usize % (i + 1));
        }
        Object(v)
    }

    fn with(&self, i: usize, value: &str) -> Object {
        let mut v = self.0.clone();
        v[i].1 = value.to_owned();
        Object(v)
    }

    fn without(&self, i: usize) -> Object {
        let mut v = self.0.clone();
        v.remove(i);
        Object(v)
    }

    fn inserted(&self, at: usize, key: &str, value: &str) -> Object {
        let mut v = self.0.clone();
        v.insert(at.min(v.len()), (key.to_owned(), value.to_owned()));
        Object(v)
    }
}

fn nested(depth: usize) -> String {
    format!("{}{}", "[".repeat(depth), "]".repeat(depth))
}

/// Values to put where any field's value goes.
fn odd_values() -> Vec<String> {
    let mut v: Vec<String> = [
        "null",
        "true",
        "false",
        "0",
        "1",
        "-1",
        "2",
        "9",
        "10",
        "4294967295",
        "4294967296",
        "9223372036854775807",
        "9223372036854775808",
        "18446744073709551615",
        "18446744073709551616",
        "-9223372036854775808",
        "-9223372036854775809",
        "1.5",
        "1.0",
        "-0",
        "0.0",
        "1e2",
        "1e400",
        "-1e400",
        "1e-400",
        r#""""#,
        r#""x""#,
        r#""rows""#,
        r#""snapshot""#,
        r#""schema""#,
        r#""watermark""#,
        r#""live""#,
        r#""repair""#,
        r#""stream""#,
        r#""clone""#,
        r#""I""#,
        r#""\ud800""#,
        r#""\udc00\ud800""#,
        r#""\u0000""#,
        r#""rows""#,
        "[]",
        "[1]",
        "[1,2,3]",
        "[1,2,3,4]",
        r#"["a","b"]"#,
        r#"[["I",[1],[1]]]"#,
        r#"[["I",[1]]]"#,
        r#"[["I",[1],[1],[]]]"#,
        r#"[[{"I":{}},[1],[1]]]"#,
        r#"[[{"I":null},[1],[1]]]"#,
        r#"[[{"I":{"x":1}},[1],[1]]]"#,
        r#"[["X",[1],[1]]]"#,
        r#"[[0,[1],[1]]]"#,
        r#"[["I",[{"$blob":"AA"}],[{"$real":"inf"}]]]"#,
        r#"[["I",[{"$blob":"AA","$blob":"AA"}],[1]]]"#,
        r#"[["I",[{"$real":"nan"}],[1]]]"#,
        r#"[["I",[1e400],[1]]]"#,
        "{}",
        r#"{"live":null}"#,
        r#"{"live":{}}"#,
        r#"{"live":{"x":1}}"#,
        r#"{"live":[]}"#,
        r#"{"live":null,"repair":null}"#,
        r#"{"stream":{}}"#,
        r#"{"clone":{}}"#,
        r#"{"epoch":1,"txid":2,"commit":3}"#,
        r#"{"epoch":1,"txid":2,"commit":3,"x":1e400}"#,
        r#"{"epoch":1,"txid":2,"commit":3,"x":"\ud800"}"#,
        r#"{"epoch":1,"txid":2,"commit":3,"epoch":1}"#,
        r#"{"epoch":1,"txid":2}"#,
        r#"[{"table":"t","generation":1}]"#,
        r#"[{"table":"t","generation":1,"x":1e400}]"#,
        r#"[{"table":"t","generation":1,"table":"t"}]"#,
        r#"[["t",1]]"#,
        r#"[["t"]]"#,
        r#"[["t",1,2]]"#,
        r#"[{"name":"a","type":"INT"}]"#,
        r#"[{"name":"a","type":"INT","pk":1,"not_null":true,"generated":false}]"#,
        r#"[{"name":"a","type":"INT","x":1e400}]"#,
        r#"[{"name":"a","type":"INT","pk":{}}]"#,
        r#"[["a","INT"]]"#,
        r#"[["a"]]"#,
        r#"[["a","INT",1,true,false,9]]"#,
        r#"{"$blob":"AA"}"#,
        r#"{"rows":null}"#,
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    for depth in [125, 126, 127, 128, 129] {
        v.push(nested(depth));
        v.push(format!(
            r#"{{"epoch":1,"txid":2,"commit":3,"x":{}}}"#,
            nested(depth)
        ));
    }
    v
}

/// Derived variants of one value: its own odd shapes, one level at a time
/// into arrays and objects.
fn variants(raw: &str, depth: usize, out: &mut Vec<String>) {
    let Ok(v) = serde_json::from_str::<serde_json::Value>(raw) else {
        return;
    };
    let text = |v: &serde_json::Value| serde_json::to_string(v).unwrap();
    match &v {
        serde_json::Value::Array(items) => {
            let mut extra = items.iter().map(text).collect::<Vec<_>>();
            extra.push("null".into());
            out.push(format!("[{}]", extra.join(",")));
            if !items.is_empty() {
                let short = items[..items.len() - 1]
                    .iter()
                    .map(text)
                    .collect::<Vec<_>>();
                out.push(format!("[{}]", short.join(",")));
            }
            if depth == 0 {
                return;
            }
            for (i, item) in items.iter().enumerate().take(3) {
                let mut inner = Vec::new();
                variants(&text(item), depth - 1, &mut inner);
                for odd in ["null", "1e400", r#""\ud800""#, "{}", "[]"] {
                    inner.push(odd.into());
                }
                for x in inner {
                    let mut all = items.iter().map(text).collect::<Vec<_>>();
                    all[i] = x;
                    out.push(format!("[{}]", all.join(",")));
                }
            }
        }
        serde_json::Value::Object(map) => {
            let entries: Vec<(String, String)> = map
                .iter()
                .map(|(k, v)| (text(&k.as_str().into()), text(v)))
                .collect();
            let obj = |e: &[(String, String)]| {
                let f: Vec<String> = e.iter().map(|(k, v)| format!("{k}:{v}")).collect();
                format!("{{{}}}", f.join(","))
            };
            for odd in ["1e400", r#""\ud800""#, "null", &nested(126), &nested(127)] {
                let mut e = entries.clone();
                e.push((r#""zz""#.into(), odd.to_string()));
                out.push(obj(&e));
            }
            for i in 0..entries.len() {
                let mut dup = entries.clone();
                dup.push(entries[i].clone());
                out.push(obj(&dup));
                let mut less = entries.clone();
                less.remove(i);
                out.push(obj(&less));
            }
            out.push(format!(
                "[{}]",
                entries
                    .iter()
                    .map(|(_, v)| v.clone())
                    .collect::<Vec<_>>()
                    .join(",")
            ));
            if depth == 0 {
                return;
            }
            for i in 0..entries.len() {
                let mut inner = Vec::new();
                variants(&entries[i].1, depth - 1, &mut inner);
                for odd in ["null", "1e400", "{}", "[]", r#""x""#, "1", "true"] {
                    inner.push(odd.into());
                }
                for x in inner {
                    let mut e = entries.clone();
                    e[i].1 = x;
                    out.push(obj(&e));
                }
            }
        }
        _ => {}
    }
}

/// Compares the two on a record and on what can go wrong with it: key
/// order, `kind` last, unknown, missing, duplicated and escaped keys, and
/// odd values for every field, each with `kind` first and last.
fn check_mutations(bytes: &[u8], deep: bool) -> (usize, usize) {
    let mut seen = (0, 0);
    let mut run = |bytes: &[u8]| {
        seen.0 += 1;
        seen.1 += check(bytes) as usize;
    };
    fn pair(o: Object, run: &mut impl FnMut(&[u8])) {
        run(&o.bytes());
        run(&o.kind_last().bytes());
    }
    macro_rules! both {
        ($o:expr) => {
            pair($o, &mut run)
        };
    }
    let obj = Object::parse(bytes);
    both!(obj.clone());
    both!(obj.reversed());
    for seed in 1..=4 {
        both!(obj.shuffled(seed));
    }
    let n = obj.0.len();
    for (key, value) in [
        ("object", r#""bucket/key""#),
        ("zz", "1e400"),
        ("zz", r#""\ud800""#),
        ("zz", r#"{"a":[1,{"b":null}],"a":2}"#),
        ("zz", &nested(126)),
        ("zz", &nested(127)),
        ("zz", &nested(128)),
        ("Kind", r#""gap""#),
        // Other kinds' fields, which a body of this kind leaves unread.
        ("sql", "1e400"),
        ("sql", r#""\ud800""#),
        ("through", &nested(127)),
        ("tables", r#"[{"table":"t","generation":1,"x":1e400}]"#),
        ("snapshot_id", "1e400"),
        ("rows", "-1e400"),
    ] {
        for at in [0, n / 2, n] {
            both!(obj.inserted(at, key, value));
        }
    }
    let odd = odd_values();
    for i in 0..n {
        both!(obj.without(i));
        let (key, value) = obj.0[i].clone();
        both!(obj.inserted(i + 1, &key, &value));
        both!(obj.inserted(n, &key, &value));
        both!(obj.inserted(n, &key, "null"));
        // The same key with its first letter escaped.
        for o in [obj.clone(), obj.kind_last()] {
            let text = String::from_utf8(o.bytes()).unwrap();
            let escaped = format!("\"\\u{:04x}{}\":", key.as_bytes()[0], &key[1..]);
            let text = text.replacen(&format!("\"{key}\":"), &escaped, 1);
            run(text.as_bytes());
        }
        if !deep {
            continue;
        }
        let mut values = odd.clone();
        variants(&value, 3, &mut values);
        for v in values {
            both!(obj.with(i, &v));
        }
    }
    seen
}

fn corpus() -> Vec<Record> {
    let tg = TableGen {
        table: "t".into(),
        generation: 1,
    };
    let values = TableRows {
        table: "t".into(),
        generation: 3,
        columns: vec!["id".into(), "a".into(), "b".into(), "c".into(), "d".into()],
        key_columns: vec!["id".into()],
        rows: vec![
            RowChange(
                Op::Insert,
                vec![Value::Integer(i64::MIN)],
                vec![
                    Value::Integer(i64::MIN),
                    Value::Real(-0.0),
                    Value::Real(f64::INFINITY),
                    Value::Blob(vec![0, 1, 255]),
                    Value::Null,
                ],
            ),
            RowChange(
                Op::Update,
                vec![Value::Integer(i64::MAX)],
                vec![
                    Value::Integer(i64::MAX),
                    Value::Real(0.1 + 0.2),
                    Value::Real(f64::NEG_INFINITY),
                    Value::Blob(vec![]),
                    Value::Text("é\"\\\n\u{1F600}".into()),
                ],
            ),
            del(7, "gone"),
        ],
    };
    let mut bodies = vec![
        Body::Rows(RowsBody {
            data: table_rows("t", 1, vec![put(1, "a"), upd(1, "b")]),
        }),
        Body::Rows(RowsBody {
            data: values.clone(),
        }),
        Body::Rows(RowsBody {
            data: TableRows {
                key_columns: vec![ROWID_KEY_COLUMN.into()],
                rows: vec![],
                ..values.clone()
            },
        }),
        Body::Snapshot(SnapshotBody {
            snapshot_id: "s1".into(),
            data: table_rows("t", 1, vec![put(1, "a")]),
        }),
        Body::Snapshot(SnapshotBody {
            snapshot_id: "".into(),
            data: values,
        }),
        Body::SnapshotEnd(SnapshotEndBody {
            snapshot_id: "s1".into(),
            scope: SnapshotScope::Stream,
            tables: vec![tg.clone()],
            records: 1,
        }),
        Body::SnapshotEnd(SnapshotEndBody {
            snapshot_id: "s2".into(),
            scope: SnapshotScope::Tables,
            tables: vec![],
            records: 0,
        }),
        Body::Schema(schema("t", 1)),
        Body::Schema(SchemaBody {
            renamed_from: Some("old".into()),
            dropped: true,
            unsupported: true,
            columns: vec![ColumnDef {
                name: "g".into(),
                decl_type: "".into(),
                pk: 2,
                not_null: true,
                generated: true,
            }],
            ..schema("t", 1)
        }),
        Body::Link(LinkBody {
            start_txid: 1,
            prev_epoch: Some(3),
            prev_txid: Some(40),
            mode: LinkMode::Clone,
        }),
        Body::Link(LinkBody {
            start_txid: 1,
            prev_epoch: None,
            prev_txid: None,
            mode: LinkMode::Fresh,
        }),
        Body::Recovered(RecoveredBody {
            session: "node-a/7".into(),
            head: Position::new(12, 4, u64::MAX),
            loss: true,
            cells: 3,
        }),
        Body::Recovered(RecoveredBody {
            session: "node-a/7".into(),
            head: pos(12, 4),
            loss: false,
            cells: 0,
        }),
        Body::Deleted(DeletedBody {
            facet: None,
            incarnation: None,
            subtree: false,
            through_incarnation: None,
        }),
        Body::Deleted(DeletedBody {
            facet: Some("a/b".into()),
            incarnation: Some(99),
            subtree: true,
            through_incarnation: None,
        }),
        Body::Deleted(DeletedBody {
            facet: Some("a/b".into()),
            incarnation: None,
            subtree: false,
            through_incarnation: Some(1 << 40),
        }),
        Body::Watermark(WatermarkBody {
            from: Some(pos(3, 1)),
            through: pos(9, 7),
            commits: 4,
            records: 6,
        }),
        Body::Watermark(WatermarkBody {
            from: None,
            through: pos(9, 7),
            commits: 0,
            records: 0,
        }),
        Body::Bulk(BulkBody { tables: vec![tg] }),
        Body::Bulk(BulkBody { tables: vec![] }),
        Body::Gap(GapBody {
            from: pos(1, 1),
            to: pos(5, 2),
            reason: "attribution".into(),
        }),
    ];
    let mut records: Vec<Record> = bodies.drain(..).map(|b| live(pos(9, 7), b)).collect();
    let mut facet_record = record(
        &facet("rooms/7", u64::MAX),
        Position::new(u64::MAX, 0, 0),
        Origin::Repair,
        Body::Bulk(BulkBody { tables: vec![] }),
    );
    facet_record.envelope.cell_name = None;
    facet_record.envelope.committed_at = i64::MIN;
    facet_record.envelope.fragment = 2;
    facet_record.envelope.fragments = u32::MAX;
    records.push(facet_record);
    let mut snap = records[0].clone();
    snap.envelope.origin = Origin::Snapshot;
    snap.envelope.stream.script = "".into();
    snap.envelope.stream.incarnation = 0;
    records.push(snap);
    records
}

#[test]
fn every_kind_decodes_as_the_derive_did() {
    let mut accepted = 0;
    let mut total = 0;
    for r in corpus() {
        let bytes = r.to_json();
        assert_eq!(Record::from_json(&bytes).unwrap(), r);
        let (n, ok) = check_mutations(&bytes, true);
        total += n;
        accepted += ok;
    }
    // Both outcomes are exercised.
    assert!(
        accepted > 1000 && total - accepted > 1000,
        "{accepted} of {total}"
    );
}

#[test]
fn malformed_input_is_refused_by_both() {
    let good = live(pos(1, 1), Body::Bulk(BulkBody { tables: vec![] })).to_json();
    let text = String::from_utf8(good.clone()).unwrap();
    let mut inputs: Vec<Vec<u8>> = vec![
        b"".to_vec(),
        b"null".to_vec(),
        b"[]".to_vec(),
        b"{}".to_vec(),
        b"1".to_vec(),
        br#""rows""#.to_vec(),
        br#"{"kind":"rows"}"#.to_vec(),
        br#"{"kind":"nope"}"#.to_vec(),
        format!("{text} ").into_bytes(),
        format!(" {text}\n").into_bytes(),
        format!("{text}x").into_bytes(),
        format!("{text}{{}}").into_bytes(),
        format!("[{text}]").into_bytes(),
        format!("\u{feff}{text}").into_bytes(),
        text.replacen('}', ",}", 1).into_bytes(),
        text.replacen(':', " : ", 3).into_bytes(),
    ];
    for cut in [1, 2, 10, good.len() / 2, good.len() - 1] {
        inputs.push(good[..cut].to_vec());
    }
    // Invalid UTF-8 in a string nothing reads and in one something does.
    let mut bad = text.replacen('{', r#"{"zz":"?","#, 1).into_bytes();
    let at = bad.iter().position(|b| *b == b'?').unwrap();
    bad[at] = 0xff;
    inputs.push(bad.clone());
    let kind_last = Object::parse(&good).kind_last().bytes();
    let mut late = String::from_utf8(kind_last)
        .unwrap()
        .replacen('{', r#"{"tables":["?"],"#, 1);
    late = late.replacen(r#""tables":[],"#, "", 1);
    let mut late = late.into_bytes();
    let at = late.iter().position(|b| *b == b'?').unwrap();
    late[at] = 0xff;
    inputs.push(late);
    let mut node = good.clone();
    let at = text.find("node-a").unwrap();
    node[at] = 0xc3;
    inputs.push(node);
    for input in inputs {
        check(&input);
    }
}

#[test]
fn a_kind_index_is_a_kind() {
    // A derived tag also takes the variant's index.
    let r = live(pos(1, 1), Body::Bulk(BulkBody { tables: vec![] }));
    let obj = Object::parse(&r.to_json());
    let i = obj.0.iter().position(|(k, _)| k == "kind").unwrap();
    assert!(check(&obj.with(i, "8").bytes()));
    assert!(check(&obj.with(i, "8").kind_last().bytes()));
    assert!(!check(&obj.with(i, "10").bytes()));
}

#[test]
#[allow(clippy::disallowed_methods)] // A corpus on the host, not node storage.
fn records_from_a_file_decode_as_the_derive_did() {
    let Ok(path) = std::env::var("CELLD_RECORDS") else {
        return;
    };
    let text = std::fs::read_to_string(&path).unwrap();
    let mut lines = 0;
    for line in text.lines().filter(|l| !l.trim().is_empty()) {
        // `celld export inspect` adds the object key; a record has none.
        let mut obj = Object::parse(line.as_bytes());
        obj.0.retain(|(k, _)| k != "object");
        let bytes = obj.bytes();
        assert!(check(&bytes), "a real record must decode: {line}");
        let r = Record::from_json(&bytes).unwrap();
        assert_eq!(Record::from_json(&r.to_json()).unwrap(), r);
        if lines < 300 || lines % 100 == 0 {
            check_mutations(&bytes, lines < 300);
        }
        lines += 1;
    }
    eprintln!("compared {lines} records from {path}");
}
