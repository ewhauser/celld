//! The row layout `insert_landing` reads: one per record the loader lands.
//!
//! One field per envelope field, named as the record's JSON fields are,
//! `body`: the record's other fields as a JSON object, and `source`: where
//! the loader read the record. `kind` is its own column, so routing never
//! looks inside the body.

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
    /// The kind-specific fields: a JSON object's text. The row's JSON
    /// carries it as the object itself, which lands as a VARIANT.
    #[serde(with = "json_object")]
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
        let fields: Fields = serde_json::from_slice(json)?;
        let envelope = &LANDING_COLUMNS[..LANDING_COLUMNS.len() - 2];
        let body = fields.object_without(|key| envelope.contains(&key))?;
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

    /// Append the row's JSON, as `serde_json::to_vec` writes it, with the
    /// body copied in rather than checked and copied: `body` must hold one
    /// JSON object, as every row this crate builds does.
    pub fn write_json(&self, out: &mut Vec<u8>) {
        let head = Head {
            kind: &self.kind,
            script: &self.script,
            class: &self.class,
            cell: &self.cell,
            cell_name: &self.cell_name,
            facet: &self.facet,
            incarnation: self.incarnation,
            epoch: self.epoch,
            txid: self.txid,
            commit: self.commit,
            committed_at: self.committed_at,
            node: &self.node,
            origin: &self.origin,
            fragment: self.fragment,
            fragments: self.fragments,
        };
        serde_json::to_writer(&mut *out, &head).expect("a row's envelope encodes");
        out.pop();
        out.extend_from_slice(b",\"body\":");
        out.extend_from_slice(self.body.as_bytes());
        out.extend_from_slice(b",\"source\":");
        serde_json::to_writer(&mut *out, &self.source).expect("a string encodes");
        out.push(b'}');
    }

    /// No less than the length of [`write_json`](Self::write_json)'s
    /// output: room to write the row into that is never outgrown. Counted
    /// from lengths, without reading the strings, so it is cheap, and loose
    /// by at most a few times the strings' lengths, never by the body.
    pub fn json_len_bound(&self) -> usize {
        let option = |o: &Option<String>| o.as_ref().map_or(0, String::len);
        let strings = self.kind.len()
            + self.script.len()
            + self.class.len()
            + self.cell.len()
            + option(&self.cell_name)
            + option(&self.facet)
            + self.node.len()
            + self.origin.len()
            + self.source.len();
        // Each string byte is at most an escape, `\u00XX`.
        JSON_FIXED_BOUND + 6 * strings + self.body.len()
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

/// A row's fields before `body`, in order.
#[derive(Serialize)]
struct Head<'a> {
    kind: &'a str,
    script: &'a str,
    class: &'a str,
    cell: &'a str,
    cell_name: &'a Option<String>,
    facet: &'a Option<String>,
    incarnation: u64,
    epoch: u64,
    txid: u64,
    commit: u64,
    committed_at: i64,
    node: &'a str,
    origin: &'a str,
    fragment: u32,
    fragments: u32,
}

/// A row's JSON less its strings' contents and its body: the keys, the
/// quotes and punctuation, `null` for each option, and each number at its
/// widest.
const JSON_FIXED_BOUND: usize = 319;

/// A `kind` or `origin` as the record's JSON names it.
fn name(v: &impl Serialize) -> String {
    match serde_json::to_value(v) {
        Ok(Json::String(s)) => s,
        other => unreachable!("an envelope name is a string: {other:?}"),
    }
}

/// A JSON object's fields in order, each value as its encoded bytes.
pub(crate) struct Fields<'a>(Vec<(Cow<'a, str>, &'a RawValue)>);

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

impl Fields<'_> {
    /// The value of `key`, as encoded.
    pub(crate) fn get(&self, key: &str) -> Option<&RawValue> {
        self.0
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, value)| *value)
    }

    /// The object without the fields `skip` names, each kept field written
    /// with the bytes it was encoded with, in its original order.
    pub(crate) fn object_without(
        &self,
        skip: impl Fn(&str) -> bool,
    ) -> Result<String, serde_json::Error> {
        let mut out = String::with_capacity(
            self.0
                .iter()
                .map(|(k, v)| k.len() + v.get().len() + 4)
                .sum::<usize>()
                + 2,
        );
        out.push('{');
        for (key, value) in &self.0 {
            if skip(key) {
                continue;
            }
            if out.len() > 1 {
                out.push(',');
            }
            match key {
                // Borrowed only when the key had no escapes, so it needs none.
                Cow::Borrowed(key) => {
                    out.push('"');
                    out.push_str(key);
                    out.push('"');
                }
                Cow::Owned(key) => out.push_str(&serde_json::to_string(key)?),
            }
            out.push(':');
            out.push_str(value.get());
        }
        out.push('}');
        Ok(out)
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

/// `body` in a row's JSON: the object, not a string holding it.
mod json_object {
    use serde::de::Error as _;
    use serde::ser::Error as _;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use serde_json::value::RawValue;

    pub fn serialize<S: Serializer>(body: &str, s: S) -> Result<S::Ok, S::Error> {
        let raw: &RawValue = serde_json::from_str(body).map_err(S::Error::custom)?;
        raw.serialize(s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<String, D::Error> {
        let raw = Box::<RawValue>::deserialize(d)?;
        if !raw.get().starts_with('{') {
            return Err(D::Error::custom("a landing row's body is a JSON object"));
        }
        Ok(Box::<str>::from(raw).into_string())
    }
}
