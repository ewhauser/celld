//! The row layout `insert_landing` reads: one per record the loader lands.
//!
//! One field per envelope field, named as the record's JSON fields are,
//! `body`: the record's other fields as a JSON object string, and `source`:
//! where the loader read the record. `kind` is its own column, so routing
//! never parses the body.

use std::borrow::Cow;
use std::fmt;

use celld_export_format::{DecodeError, Record};
use serde::de::{Deserializer, MapAccess, Visitor};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use serde_json::{Map, Value as Json};

/// `EXPORT_LANDING`'s record columns, in order.
pub const LANDING_COLUMNS: [&str; 17] = [
    "kind",
    "script",
    "class",
    "cell",
    "cell_name",
    "facet",
    "incarnation",
    "epoch",
    "txid",
    "commit",
    "committed_at",
    "node",
    "origin",
    "fragment",
    "fragments",
    "body",
    "source",
];

/// One record as one `EXPORT_LANDING` row.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LandingRow {
    pub kind: String,
    pub script: String,
    pub class: String,
    pub cell: String,
    pub cell_name: Option<String>,
    pub facet: Option<String>,
    pub incarnation: u64,
    pub epoch: u64,
    pub txid: u64,
    pub commit: u64,
    /// Milliseconds since the Unix epoch.
    pub committed_at: i64,
    pub node: String,
    pub origin: String,
    pub fragment: u32,
    pub fragments: u32,
    /// The kind-specific fields as a JSON object.
    pub body: String,
    /// Where the record was read, such as `blob-stream/7/1234` (virtual
    /// partition 7, offset 1234). Only for tracing a row back.
    pub source: String,
}

impl LandingRow {
    /// One record's JSON, as read from the topic, as a row. Fails as
    /// [`Record::from_json`] does when the JSON is not a record. The body
    /// is cut from `json`: each field keeps the bytes it was encoded with,
    /// and nothing is decoded into a tree and encoded again.
    pub fn from_json(json: &[u8], source: impl Into<String>) -> Result<Self, DecodeError> {
        let record = Record::from_json(json)?;
        Ok(Self::split(&record, json, source.into())?)
    }

    pub fn from_record(record: &Record, source: impl Into<String>) -> Self {
        Self::split(record, &record.to_json(), source.into()).expect("an encoded record splits")
    }

    /// The row of `record`, which is `json` decoded.
    pub(crate) fn split(
        record: &Record,
        json: &[u8],
        source: String,
    ) -> Result<Self, serde_json::Error> {
        let Fields(fields) = serde_json::from_slice(json)?;
        let envelope = &LANDING_COLUMNS[..LANDING_COLUMNS.len() - 2];
        let mut body = String::with_capacity(json.len());
        body.push('{');
        for (key, value) in fields {
            if envelope.contains(&&*key) {
                continue;
            }
            if body.len() > 1 {
                body.push(',');
            }
            match key {
                // Borrowed only when the key had no escapes, so it needs none.
                Cow::Borrowed(key) => {
                    body.push('"');
                    body.push_str(key);
                    body.push('"');
                }
                Cow::Owned(key) => body.push_str(&serde_json::to_string(&key)?),
            }
            body.push(':');
            body.push_str(value.get());
        }
        body.push('}');
        let e = &record.envelope;
        Ok(LandingRow {
            kind: name(&record.kind()),
            script: e.stream.script.clone(),
            class: e.stream.class.clone(),
            cell: e.stream.cell.clone(),
            cell_name: e.cell_name.clone(),
            facet: e.stream.facet.clone(),
            incarnation: e.stream.incarnation,
            epoch: e.position.epoch,
            txid: e.position.txid,
            commit: e.position.commit,
            committed_at: e.committed_at,
            node: e.node.clone(),
            origin: name(&e.origin),
            fragment: e.fragment,
            fragments: e.fragments,
            body,
            source,
        })
    }

    /// The record this row holds.
    pub fn to_record(&self) -> Result<Record, DecodeError> {
        let mut fields: Map<String, Json> = serde_json::from_str(&self.body)?;
        let envelope = serde_json::json!({
            "kind": self.kind,
            "script": self.script,
            "class": self.class,
            "cell": self.cell,
            "cell_name": self.cell_name,
            "facet": self.facet,
            "incarnation": self.incarnation,
            "epoch": self.epoch,
            "txid": self.txid,
            "commit": self.commit,
            "committed_at": self.committed_at,
            "node": self.node,
            "origin": self.origin,
            "fragment": self.fragment,
            "fragments": self.fragments,
        });
        let Json::Object(envelope) = envelope else {
            unreachable!()
        };
        fields.extend(envelope);
        Record::from_json(&serde_json::to_vec(&Json::Object(fields))?)
    }
}

/// A `kind` or `origin` as the record's JSON names it.
fn name(v: &impl Serialize) -> String {
    match serde_json::to_value(v) {
        Ok(Json::String(s)) => s,
        other => unreachable!("an envelope name is a string: {other:?}"),
    }
}

/// A JSON object's fields in order, each value as its encoded bytes.
struct Fields<'a>(Vec<(Cow<'a, str>, &'a RawValue)>);

impl<'de> Deserialize<'de> for Fields<'de> {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = Fields<'de>;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a JSON object")
            }
            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                let mut fields = Vec::with_capacity(24);
                while let Some(Key(key)) = map.next_key()? {
                    let value = map.next_value()?;
                    // A record never repeats a field it knows, and decoding
                    // it refuses one that does. One it does not know keeps
                    // its last value, as a JSON tree would, since Snowflake
                    // refuses an object with a repeated key.
                    match fields.iter_mut().find(|(k, _)| *k == key) {
                        Some(field) => field.1 = value,
                        None => fields.push((key, value)),
                    }
                }
                Ok(Fields(fields))
            }
        }
        d.deserialize_map(V)
    }
}

/// A field name, borrowed from the input when it has no escapes.
struct Key<'a>(Cow<'a, str>);

impl<'de> Deserialize<'de> for Key<'de> {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = Key<'de>;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a field name")
            }
            fn visit_borrowed_str<E>(self, s: &'de str) -> Result<Self::Value, E> {
                Ok(Key(Cow::Borrowed(s)))
            }
            fn visit_str<E>(self, s: &str) -> Result<Self::Value, E> {
                Ok(Key(Cow::Owned(s.to_owned())))
            }
        }
        d.deserialize_str(V)
    }
}
