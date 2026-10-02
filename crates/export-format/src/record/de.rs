//! Decoding a [`Record`] in one pass.
//!
//! The derived decoder of a struct that flattens an internally tagged enum
//! cannot stream: it buffers the whole object in serde's private `Content`
//! tree, decodes the envelope and the body from that, and clones entries
//! along the way. This one reads the object once. Envelope fields go
//! straight to their slots, `kind` picks the body, and a `rows` or
//! `snapshot` body, nearly every record, decodes straight from the input.
//!
//! It accepts and rejects exactly the JSON the derive did; `tests/decode.rs`
//! compares the two. That means reproducing what the derive's buffering
//! does besides decoding the fields:
//!
//! - Every value is parsed in full, even one nothing reads, so a value no
//!   field takes still fails on a number out of range, a lone surrogate, or
//!   nesting too deep ([`Discard`]).
//! - Body fields that come before `kind`, and the fields of the kinds other
//!   than `rows` and `snapshot`, are held in a [`Tree`] that decodes the way
//!   `Content` does, so the derived body types read them as they did. The
//!   node writes `kind` first, so the former is normally empty.
//! - A `Content` unit variant also accepts `{}` as its value, so `op` is
//!   read the same way ([`OpSeed`]).
//!
//! Error messages differ from the derive's; acceptance does not.

use std::borrow::Cow;
use std::fmt;
use std::marker::PhantomData;

use serde::de::{
    self, DeserializeSeed, Deserializer, EnumAccess, IgnoredAny, MapAccess, SeqAccess, Unexpected,
    VariantAccess, Visitor,
};
use serde::Deserialize;

use super::{
    Body, Envelope, Kind, Op, Origin, Position, Record, RowChange, RowsBody, SnapshotBody,
    StreamId, TableRows,
};

impl<'de> Deserialize<'de> for Record {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        d.deserialize_map(RecordVisitor)
    }
}

macro_rules! fields {
    ($($field:ident = $name:literal,)*) => {
        /// A top-level field name: every name the envelope or any kind's
        /// body uses, and `Other` for the rest. A body field missing here
        /// would be dropped; the tests below check none is.
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        enum Field {
            $($field,)*
            Other,
        }

        impl Field {
            fn from_name(name: &str) -> Field {
                match name {
                    $($name => Field::$field,)*
                    _ => Field::Other,
                }
            }

            fn name(self) -> &'static str {
                match self {
                    $(Field::$field => $name,)*
                    Field::Other => "",
                }
            }
        }
    };
}

fields! {
    // The envelope.
    Script = "script",
    Class = "class",
    Cell = "cell",
    Facet = "facet",
    Incarnation = "incarnation",
    CellName = "cell_name",
    Epoch = "epoch",
    Txid = "txid",
    Commit = "commit",
    CommittedAt = "committed_at",
    Node = "node",
    Origin = "origin",
    Fragment = "fragment",
    Fragments = "fragments",
    Kind = "kind",
    // Bodies.
    Table = "table",
    Generation = "generation",
    Columns = "columns",
    KeyColumns = "key_columns",
    Rows = "rows",
    SnapshotId = "snapshot_id",
    Scope = "scope",
    Tables = "tables",
    Records = "records",
    Sql = "sql",
    Dropped = "dropped",
    RenamedFrom = "renamed_from",
    Unsupported = "unsupported",
    StartTxid = "start_txid",
    PrevEpoch = "prev_epoch",
    PrevTxid = "prev_txid",
    Mode = "mode",
    Session = "session",
    Head = "head",
    Loss = "loss",
    Cells = "cells",
    TargetFacet = "target_facet",
    TargetIncarnation = "target_incarnation",
    Subtree = "subtree",
    ThroughIncarnation = "through_incarnation",
    From = "from",
    Through = "through",
    Commits = "commits",
    To = "to",
    Reason = "reason",
}

struct FieldNameSeed;

impl<'de> DeserializeSeed<'de> for FieldNameSeed {
    type Value = Field;

    fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<Field, D::Error> {
        d.deserialize_identifier(self)
    }
}

impl<'de> Visitor<'de> for FieldNameSeed {
    type Value = Field;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a field name")
    }

    fn visit_str<E: de::Error>(self, v: &str) -> Result<Field, E> {
        Ok(Field::from_name(v))
    }

    fn visit_bytes<E: de::Error>(self, v: &[u8]) -> Result<Field, E> {
        Ok(std::str::from_utf8(v).map_or(Field::Other, Field::from_name))
    }
}

/// Fills a slot, refusing a second value for it.
fn fill<T, E: de::Error>(slot: &mut Option<T>, key: Field, value: T) -> Result<(), E> {
    if slot.is_some() {
        return Err(E::duplicate_field(key.name()));
    }
    *slot = Some(value);
    Ok(())
}

fn take<T, E: de::Error>(slot: Option<T>, key: Field) -> Result<T, E> {
    slot.ok_or_else(|| E::missing_field(key.name()))
}

struct RecordVisitor;

impl<'de> Visitor<'de> for RecordVisitor {
    type Value = Record;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("an export record")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Record, A::Error> {
        let mut env = EnvelopeSlots::default();
        let mut body: Option<BodySlots<'de>> = None;
        // Body fields read before `kind`, until it says how to decode them.
        let mut early: Vec<(Field, Tree<'de>)> = Vec::new();
        while let Some(key) = map.next_key_seed(FieldNameSeed)? {
            match key {
                Field::Script => fill(&mut env.script, key, map.next_value()?)?,
                Field::Class => fill(&mut env.class, key, map.next_value()?)?,
                Field::Cell => fill(&mut env.cell, key, map.next_value()?)?,
                Field::Facet => fill(&mut env.facet, key, map.next_value()?)?,
                Field::Incarnation => fill(&mut env.incarnation, key, map.next_value()?)?,
                Field::CellName => fill(&mut env.cell_name, key, map.next_value()?)?,
                Field::Epoch => fill(&mut env.epoch, key, map.next_value()?)?,
                Field::Txid => fill(&mut env.txid, key, map.next_value()?)?,
                Field::Commit => fill(&mut env.commit, key, map.next_value()?)?,
                Field::CommittedAt => fill(&mut env.committed_at, key, map.next_value()?)?,
                Field::Node => fill(&mut env.node, key, map.next_value()?)?,
                Field::Origin => fill(&mut env.origin, key, map.next_value()?)?,
                Field::Fragment => fill(&mut env.fragment, key, map.next_value()?)?,
                Field::Fragments => fill(&mut env.fragments, key, map.next_value()?)?,
                Field::Kind => {
                    if body.is_some() {
                        return Err(de::Error::duplicate_field("kind"));
                    }
                    let mut slots = BodySlots::new(map.next_value_seed(KindSeed)?);
                    for (key, value) in early.drain(..) {
                        slots.put_tree(key, value)?;
                    }
                    body = Some(slots);
                }
                Field::Other => map.next_value_seed(Discard)?,
                _ => match &mut body {
                    Some(slots) => map.next_value_seed(BodyFieldSeed { slots, key })?,
                    None => early.push((key, map.next_value_seed(TreeSeed)?)),
                },
            }
        }
        let body = body.ok_or_else(|| de::Error::missing_field("kind"))?;
        Ok(Record {
            envelope: env.finish()?,
            body: body.finish()?,
        })
    }
}

#[derive(Default)]
struct EnvelopeSlots {
    script: Option<String>,
    class: Option<String>,
    cell: Option<String>,
    facet: Option<Option<String>>,
    incarnation: Option<u64>,
    cell_name: Option<Option<String>>,
    epoch: Option<u64>,
    txid: Option<u64>,
    commit: Option<u64>,
    committed_at: Option<i64>,
    node: Option<String>,
    origin: Option<Origin>,
    fragment: Option<u32>,
    fragments: Option<u32>,
}

impl EnvelopeSlots {
    fn finish<E: de::Error>(self) -> Result<Envelope, E> {
        Ok(Envelope {
            stream: StreamId {
                script: take(self.script, Field::Script)?,
                class: take(self.class, Field::Class)?,
                cell: take(self.cell, Field::Cell)?,
                facet: self.facet.flatten(),
                incarnation: take(self.incarnation, Field::Incarnation)?,
            },
            cell_name: self.cell_name.flatten(),
            position: Position {
                epoch: take(self.epoch, Field::Epoch)?,
                txid: take(self.txid, Field::Txid)?,
                commit: take(self.commit, Field::Commit)?,
            },
            committed_at: take(self.committed_at, Field::CommittedAt)?,
            node: take(self.node, Field::Node)?,
            origin: take(self.origin, Field::Origin)?,
            fragment: take(self.fragment, Field::Fragment)?,
            fragments: take(self.fragments, Field::Fragments)?,
        })
    }
}

/// `kind`, by name or, as a derived tag also accepts, by variant index.
struct KindSeed;

impl<'de> DeserializeSeed<'de> for KindSeed {
    type Value = Kind;

    fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<Kind, D::Error> {
        d.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for KindSeed {
    type Value = Kind;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a record kind")
    }

    fn visit_u64<E: de::Error>(self, v: u64) -> Result<Kind, E> {
        usize::try_from(v)
            .ok()
            .and_then(|i| Kind::ALL.get(i).copied())
            .ok_or_else(|| E::invalid_value(Unexpected::Unsigned(v), &"variant index 0 <= i < 10"))
    }

    fn visit_str<E: de::Error>(self, v: &str) -> Result<Kind, E> {
        const NAMES: &[&str] = &[
            "rows",
            "snapshot",
            "snapshot_end",
            "schema",
            "link",
            "recovered",
            "deleted",
            "watermark",
            "bulk",
            "gap",
        ];
        NAMES
            .iter()
            .position(|n| *n == v)
            .map(|i| Kind::ALL[i])
            .ok_or_else(|| E::unknown_variant(v, NAMES))
    }

    fn visit_bytes<E: de::Error>(self, v: &[u8]) -> Result<Kind, E> {
        match std::str::from_utf8(v) {
            Ok(s) => self.visit_str(s),
            Err(_) => Err(E::invalid_value(Unexpected::Bytes(v), &self)),
        }
    }
}

/// The body being read. `rows` and `snapshot` decode their fields as they
/// come; other kinds collect theirs and decode with the derived body type
/// at the end.
enum BodySlots<'de> {
    Rows(TableSlots),
    Snapshot(Option<String>, TableSlots),
    Other(Kind, Vec<(Tree<'de>, Tree<'de>)>),
}

impl<'de> BodySlots<'de> {
    fn new(kind: Kind) -> Self {
        match kind {
            Kind::Rows => BodySlots::Rows(TableSlots::default()),
            Kind::Snapshot => BodySlots::Snapshot(None, TableSlots::default()),
            _ => BodySlots::Other(kind, Vec::new()),
        }
    }

    fn set<D: Deserializer<'de>>(&mut self, key: Field, d: D) -> Result<(), D::Error> {
        match self {
            BodySlots::Rows(t) => t.set(key, d),
            BodySlots::Snapshot(id, _) if key == Field::SnapshotId => {
                fill(id, key, String::deserialize(d)?)
            }
            BodySlots::Snapshot(_, t) => t.set(key, d),
            BodySlots::Other(_, fields) => {
                fields.push((Tree::name(key), Tree::deserialize(d)?));
                Ok(())
            }
        }
    }

    fn put_tree<E: de::Error>(&mut self, key: Field, value: Tree<'de>) -> Result<(), E> {
        match self {
            BodySlots::Other(_, fields) => {
                fields.push((Tree::name(key), value));
                Ok(())
            }
            _ => self.set(key, TreeDeserializer::new(value)),
        }
    }

    fn finish<E: de::Error>(self) -> Result<Body, E> {
        let (kind, fields) = match self {
            BodySlots::Rows(t) => return Ok(Body::Rows(RowsBody { data: t.finish()? })),
            BodySlots::Snapshot(id, t) => {
                return Ok(Body::Snapshot(SnapshotBody {
                    snapshot_id: take(id, Field::SnapshotId)?,
                    data: t.finish()?,
                }))
            }
            BodySlots::Other(kind, fields) => (kind, fields),
        };
        let d = TreeDeserializer::<E>::new(Tree::Map(fields));
        Ok(match kind {
            Kind::Rows | Kind::Snapshot => unreachable!("decoded as they come"),
            Kind::SnapshotEnd => Body::SnapshotEnd(Deserialize::deserialize(d)?),
            Kind::Schema => Body::Schema(Deserialize::deserialize(d)?),
            Kind::Link => Body::Link(Deserialize::deserialize(d)?),
            Kind::Recovered => Body::Recovered(Deserialize::deserialize(d)?),
            Kind::Deleted => Body::Deleted(Deserialize::deserialize(d)?),
            Kind::Watermark => Body::Watermark(Deserialize::deserialize(d)?),
            Kind::Bulk => Body::Bulk(Deserialize::deserialize(d)?),
            Kind::Gap => Body::Gap(Deserialize::deserialize(d)?),
        })
    }
}

struct BodyFieldSeed<'a, 'de> {
    slots: &'a mut BodySlots<'de>,
    key: Field,
}

impl<'de> DeserializeSeed<'de> for BodyFieldSeed<'_, 'de> {
    type Value = ();

    fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<(), D::Error> {
        self.slots.set(self.key, d)
    }
}

#[derive(Default)]
struct TableSlots {
    table: Option<String>,
    generation: Option<u64>,
    columns: Option<Vec<String>>,
    key_columns: Option<Vec<String>>,
    rows: Option<Vec<RowChange>>,
}

impl TableSlots {
    fn set<'de, D: Deserializer<'de>>(&mut self, key: Field, d: D) -> Result<(), D::Error> {
        match key {
            Field::Table => fill(&mut self.table, key, String::deserialize(d)?),
            Field::Generation => fill(&mut self.generation, key, u64::deserialize(d)?),
            Field::Columns => fill(&mut self.columns, key, Vec::deserialize(d)?),
            Field::KeyColumns => fill(&mut self.key_columns, key, Vec::deserialize(d)?),
            Field::Rows => fill(&mut self.rows, key, RowsSeed.deserialize(d)?),
            // Another kind's field.
            _ => Discard.deserialize(d),
        }
    }

    fn finish<E: de::Error>(self) -> Result<TableRows, E> {
        Ok(TableRows {
            table: take(self.table, Field::Table)?,
            generation: take(self.generation, Field::Generation)?,
            columns: take(self.columns, Field::Columns)?,
            key_columns: take(self.key_columns, Field::KeyColumns)?,
            rows: take(self.rows, Field::Rows)?,
        })
    }
}

/// Never preallocate more than this many elements on a size hint, as serde
/// does.
fn cautious<T>(hint: Option<usize>) -> usize {
    hint.unwrap_or(0)
        .min(1024 * 1024 / std::mem::size_of::<T>().max(1))
}

struct RowsSeed;

impl<'de> DeserializeSeed<'de> for RowsSeed {
    type Value = Vec<RowChange>;

    fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<Self::Value, D::Error> {
        d.deserialize_seq(self)
    }
}

impl<'de> Visitor<'de> for RowsSeed {
    type Value = Vec<RowChange>;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a sequence")
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        let mut rows = Vec::with_capacity(cautious::<RowChange>(seq.size_hint()));
        while let Some(row) = seq.next_element_seed(RowChangeSeed)? {
            rows.push(row);
        }
        Ok(rows)
    }
}

/// [`RowChange`]'s derived decoder, with `op` read by [`OpSeed`].
struct RowChangeSeed;

impl<'de> DeserializeSeed<'de> for RowChangeSeed {
    type Value = RowChange;

    fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<RowChange, D::Error> {
        d.deserialize_tuple_struct("RowChange", 3, self)
    }
}

impl<'de> Visitor<'de> for RowChangeSeed {
    type Value = RowChange;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("tuple struct RowChange")
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<RowChange, A::Error> {
        let op = seq
            .next_element_seed(OpSeed)?
            .ok_or_else(|| de::Error::invalid_length(0, &self))?;
        let key = seq
            .next_element()?
            .ok_or_else(|| de::Error::invalid_length(1, &self))?;
        let row = seq
            .next_element()?
            .ok_or_else(|| de::Error::invalid_length(2, &self))?;
        Ok(RowChange(op, key, row))
    }
}

/// An [`Op`] as a body field decoded from `Content`: a name, or a map of a
/// name to `null` or `{}`.
struct OpSeed;

const OPS: &[&str] = &["I", "U", "D"];

fn op_named<E: de::Error>(name: &str) -> Result<Op, E> {
    match name {
        "I" => Ok(Op::Insert),
        "U" => Ok(Op::Update),
        "D" => Ok(Op::Delete),
        _ => Err(E::unknown_variant(name, OPS)),
    }
}

impl<'de> DeserializeSeed<'de> for OpSeed {
    type Value = Op;

    fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<Op, D::Error> {
        d.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for OpSeed {
    type Value = Op;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("enum Op")
    }

    fn visit_str<E: de::Error>(self, v: &str) -> Result<Op, E> {
        op_named(v)
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Op, A::Error> {
        let single = || de::Error::invalid_value(Unexpected::Map, &"map with a single key");
        let op = map.next_key_seed(OpNameSeed)?.ok_or_else(single)?;
        map.next_value_seed(UnitSeed)?;
        if map.next_key::<IgnoredAny>()?.is_some() {
            return Err(single());
        }
        Ok(op)
    }
}

struct OpNameSeed;

impl<'de> DeserializeSeed<'de> for OpNameSeed {
    type Value = Op;

    fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<Op, D::Error> {
        d.deserialize_identifier(self)
    }
}

impl<'de> Visitor<'de> for OpNameSeed {
    type Value = Op;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("variant identifier")
    }

    fn visit_str<E: de::Error>(self, v: &str) -> Result<Op, E> {
        op_named(v)
    }
}

/// A unit variant's value in `Content`: `null` or an empty map.
struct UnitSeed;

impl<'de> DeserializeSeed<'de> for UnitSeed {
    type Value = ();

    fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<(), D::Error> {
        d.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for UnitSeed {
    type Value = ();

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("unit")
    }

    fn visit_unit<E: de::Error>(self) -> Result<(), E> {
        Ok(())
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<(), A::Error> {
        match map.next_key::<IgnoredAny>()? {
            None => Ok(()),
            Some(_) => Err(de::Error::invalid_type(Unexpected::Map, &self)),
        }
    }
}

/// Parses a value in full, as buffering it would, and drops it.
struct Discard;

impl<'de> DeserializeSeed<'de> for Discard {
    type Value = ();

    fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<(), D::Error> {
        d.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for Discard {
    type Value = ();

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("any value")
    }

    fn visit_bool<E: de::Error>(self, _: bool) -> Result<(), E> {
        Ok(())
    }

    fn visit_i64<E: de::Error>(self, _: i64) -> Result<(), E> {
        Ok(())
    }

    fn visit_u64<E: de::Error>(self, _: u64) -> Result<(), E> {
        Ok(())
    }

    fn visit_f64<E: de::Error>(self, _: f64) -> Result<(), E> {
        Ok(())
    }

    fn visit_str<E: de::Error>(self, _: &str) -> Result<(), E> {
        Ok(())
    }

    fn visit_bytes<E: de::Error>(self, _: &[u8]) -> Result<(), E> {
        Ok(())
    }

    fn visit_unit<E: de::Error>(self) -> Result<(), E> {
        Ok(())
    }

    fn visit_none<E: de::Error>(self) -> Result<(), E> {
        Ok(())
    }

    fn visit_some<D: Deserializer<'de>>(self, d: D) -> Result<(), D::Error> {
        d.deserialize_any(self)
    }

    fn visit_newtype_struct<D: Deserializer<'de>>(self, d: D) -> Result<(), D::Error> {
        d.deserialize_any(self)
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<(), A::Error> {
        while seq.next_element_seed(Discard)?.is_some() {}
        Ok(())
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<(), A::Error> {
        while map.next_key_seed(Discard)?.is_some() {
            map.next_value_seed(Discard)?;
        }
        Ok(())
    }
}

/// A buffered JSON value that decodes the way serde's `Content` does, for
/// the values JSON produces.
#[derive(Debug)]
enum Tree<'de> {
    Unit,
    Bool(bool),
    U64(u64),
    I64(i64),
    F64(f64),
    Str(Cow<'de, str>),
    Seq(Vec<Tree<'de>>),
    Map(Vec<(Tree<'de>, Tree<'de>)>),
}

impl<'de> Tree<'de> {
    fn name(key: Field) -> Self {
        Tree::Str(Cow::Borrowed(key.name()))
    }

    fn unexpected(&self) -> Unexpected<'_> {
        match self {
            Tree::Unit => Unexpected::Unit,
            Tree::Bool(b) => Unexpected::Bool(*b),
            Tree::U64(n) => Unexpected::Unsigned(*n),
            Tree::I64(n) => Unexpected::Signed(*n),
            Tree::F64(n) => Unexpected::Float(*n),
            Tree::Str(s) => Unexpected::Str(s),
            Tree::Seq(_) => Unexpected::Seq,
            Tree::Map(_) => Unexpected::Map,
        }
    }
}

impl<'de> Deserialize<'de> for Tree<'de> {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        d.deserialize_any(TreeSeed)
    }
}

struct TreeSeed;

impl<'de> DeserializeSeed<'de> for TreeSeed {
    type Value = Tree<'de>;

    fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<Tree<'de>, D::Error> {
        d.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for TreeSeed {
    type Value = Tree<'de>;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("any value")
    }

    fn visit_bool<E: de::Error>(self, v: bool) -> Result<Tree<'de>, E> {
        Ok(Tree::Bool(v))
    }

    fn visit_i64<E: de::Error>(self, v: i64) -> Result<Tree<'de>, E> {
        Ok(Tree::I64(v))
    }

    fn visit_u64<E: de::Error>(self, v: u64) -> Result<Tree<'de>, E> {
        Ok(Tree::U64(v))
    }

    fn visit_f64<E: de::Error>(self, v: f64) -> Result<Tree<'de>, E> {
        Ok(Tree::F64(v))
    }

    fn visit_borrowed_str<E: de::Error>(self, v: &'de str) -> Result<Tree<'de>, E> {
        Ok(Tree::Str(Cow::Borrowed(v)))
    }

    fn visit_str<E: de::Error>(self, v: &str) -> Result<Tree<'de>, E> {
        Ok(Tree::Str(Cow::Owned(v.to_owned())))
    }

    fn visit_string<E: de::Error>(self, v: String) -> Result<Tree<'de>, E> {
        Ok(Tree::Str(Cow::Owned(v)))
    }

    fn visit_unit<E: de::Error>(self) -> Result<Tree<'de>, E> {
        Ok(Tree::Unit)
    }

    fn visit_none<E: de::Error>(self) -> Result<Tree<'de>, E> {
        Ok(Tree::Unit)
    }

    fn visit_some<D: Deserializer<'de>>(self, d: D) -> Result<Tree<'de>, D::Error> {
        d.deserialize_any(self)
    }

    fn visit_newtype_struct<D: Deserializer<'de>>(self, d: D) -> Result<Tree<'de>, D::Error> {
        d.deserialize_any(self)
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Tree<'de>, A::Error> {
        let mut items = Vec::with_capacity(cautious::<Tree>(seq.size_hint()));
        while let Some(item) = seq.next_element_seed(TreeSeed)? {
            items.push(item);
        }
        Ok(Tree::Seq(items))
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Tree<'de>, A::Error> {
        let mut entries = Vec::with_capacity(cautious::<(Tree, Tree)>(map.size_hint()));
        while let Some(key) = map.next_key_seed(TreeSeed)? {
            entries.push((key, map.next_value_seed(TreeSeed)?));
        }
        Ok(Tree::Map(entries))
    }
}

/// Decodes a [`Tree`] as serde's `ContentDeserializer` decodes `Content`.
struct TreeDeserializer<'de, E> {
    tree: Tree<'de>,
    error: PhantomData<E>,
}

impl<'de, E> TreeDeserializer<'de, E> {
    fn new(tree: Tree<'de>) -> Self {
        TreeDeserializer {
            tree,
            error: PhantomData,
        }
    }
}

impl<'de, E: de::Error> TreeDeserializer<'de, E> {
    fn invalid_type(&self, expected: &dyn de::Expected) -> E {
        E::invalid_type(self.tree.unexpected(), expected)
    }
}

fn visit_seq<'de, V: Visitor<'de>, E: de::Error>(
    items: Vec<Tree<'de>>,
    visitor: V,
) -> Result<V::Value, E> {
    let mut seq = TreeSeq {
        iter: items.into_iter(),
        count: 0,
        error: PhantomData,
    };
    let value = visitor.visit_seq(&mut seq)?;
    let remaining = seq.iter.len();
    if remaining > 0 {
        return Err(E::invalid_length(
            seq.count + remaining,
            &"fewer elements in sequence",
        ));
    }
    Ok(value)
}

fn visit_map<'de, V: Visitor<'de>, E: de::Error>(
    entries: Vec<(Tree<'de>, Tree<'de>)>,
    visitor: V,
) -> Result<V::Value, E> {
    let mut map = TreeMap {
        iter: entries.into_iter(),
        value: None,
        count: 0,
        error: PhantomData,
    };
    let value = visitor.visit_map(&mut map)?;
    let remaining = map.iter.len();
    if remaining > 0 {
        return Err(E::invalid_length(
            map.count + remaining,
            &"fewer elements in map",
        ));
    }
    Ok(value)
}

impl<'de, E: de::Error> Deserializer<'de> for TreeDeserializer<'de, E> {
    type Error = E;

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, E> {
        match self.tree {
            Tree::Unit => visitor.visit_unit(),
            Tree::Bool(v) => visitor.visit_bool(v),
            Tree::U64(v) => visitor.visit_u64(v),
            Tree::I64(v) => visitor.visit_i64(v),
            Tree::F64(v) => visitor.visit_f64(v),
            Tree::Str(Cow::Borrowed(v)) => visitor.visit_borrowed_str(v),
            Tree::Str(Cow::Owned(v)) => visitor.visit_string(v),
            Tree::Seq(v) => visit_seq(v, visitor),
            Tree::Map(v) => visit_map(v, visitor),
        }
    }

    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, E> {
        match self.tree {
            Tree::Unit => visitor.visit_unit(),
            _ => visitor.visit_some(self),
        }
    }

    fn deserialize_unit<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, E> {
        match &self.tree {
            Tree::Unit => visitor.visit_unit(),
            Tree::Map(v) if v.is_empty() => visitor.visit_unit(),
            _ => Err(self.invalid_type(&visitor)),
        }
    }

    fn deserialize_unit_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, E> {
        match &self.tree {
            Tree::Map(v) if v.is_empty() => visitor.visit_unit(),
            Tree::Seq(v) if v.is_empty() => visitor.visit_unit(),
            _ => self.deserialize_any(visitor),
        }
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, E> {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_seq<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, E> {
        match self.tree {
            Tree::Seq(v) => visit_seq(v, visitor),
            _ => Err(self.invalid_type(&visitor)),
        }
    }

    fn deserialize_tuple<V: Visitor<'de>>(self, _len: usize, visitor: V) -> Result<V::Value, E> {
        self.deserialize_seq(visitor)
    }

    fn deserialize_tuple_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _len: usize,
        visitor: V,
    ) -> Result<V::Value, E> {
        self.deserialize_seq(visitor)
    }

    fn deserialize_map<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, E> {
        match self.tree {
            Tree::Map(v) => visit_map(v, visitor),
            _ => Err(self.invalid_type(&visitor)),
        }
    }

    fn deserialize_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, E> {
        match self.tree {
            Tree::Seq(v) => visit_seq(v, visitor),
            Tree::Map(v) => visit_map(v, visitor),
            _ => Err(self.invalid_type(&visitor)),
        }
    }

    fn deserialize_enum<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, E> {
        let (variant, value) = match self.tree {
            Tree::Map(entries) => {
                let mut iter = entries.into_iter();
                match (iter.next(), iter.next()) {
                    (Some((variant, value)), None) => (variant, Some(value)),
                    _ => return Err(E::invalid_value(Unexpected::Map, &"map with a single key")),
                }
            }
            s @ Tree::Str(_) => (s, None),
            other => return Err(E::invalid_type(other.unexpected(), &"string or map")),
        };
        visitor.visit_enum(TreeEnum {
            variant,
            value,
            error: PhantomData,
        })
    }

    fn deserialize_ignored_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, E> {
        visitor.visit_unit()
    }

    // `Content` decodes the rest from the same variants `deserialize_any`
    // visits, for the values JSON produces.
    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf identifier
    }
}

struct TreeSeq<'de, E> {
    iter: std::vec::IntoIter<Tree<'de>>,
    count: usize,
    error: PhantomData<E>,
}

impl<'de, E: de::Error> SeqAccess<'de> for TreeSeq<'de, E> {
    type Error = E;

    fn next_element_seed<T: DeserializeSeed<'de>>(
        &mut self,
        seed: T,
    ) -> Result<Option<T::Value>, E> {
        match self.iter.next() {
            Some(item) => {
                self.count += 1;
                seed.deserialize(TreeDeserializer::new(item)).map(Some)
            }
            None => Ok(None),
        }
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.iter.len())
    }
}

struct TreeMap<'de, E> {
    iter: std::vec::IntoIter<(Tree<'de>, Tree<'de>)>,
    value: Option<Tree<'de>>,
    count: usize,
    error: PhantomData<E>,
}

impl<'de, E: de::Error> MapAccess<'de> for TreeMap<'de, E> {
    type Error = E;

    fn next_key_seed<K: DeserializeSeed<'de>>(&mut self, seed: K) -> Result<Option<K::Value>, E> {
        match self.iter.next() {
            Some((key, value)) => {
                self.count += 1;
                self.value = Some(value);
                seed.deserialize(TreeDeserializer::new(key)).map(Some)
            }
            None => Ok(None),
        }
    }

    fn next_value_seed<V: DeserializeSeed<'de>>(&mut self, seed: V) -> Result<V::Value, E> {
        let value = self
            .value
            .take()
            .expect("next_value_seed after next_key_seed");
        seed.deserialize(TreeDeserializer::new(value))
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.iter.len())
    }
}

struct TreeEnum<'de, E> {
    variant: Tree<'de>,
    value: Option<Tree<'de>>,
    error: PhantomData<E>,
}

impl<'de, E: de::Error> EnumAccess<'de> for TreeEnum<'de, E> {
    type Error = E;
    type Variant = TreeVariant<'de, E>;

    fn variant_seed<V: DeserializeSeed<'de>>(
        self,
        seed: V,
    ) -> Result<(V::Value, Self::Variant), E> {
        let variant = seed.deserialize(TreeDeserializer::new(self.variant))?;
        Ok((
            variant,
            TreeVariant {
                value: self.value,
                error: PhantomData,
            },
        ))
    }
}

struct TreeVariant<'de, E> {
    value: Option<Tree<'de>>,
    error: PhantomData<E>,
}

impl<'de, E: de::Error> VariantAccess<'de> for TreeVariant<'de, E> {
    type Error = E;

    fn unit_variant(self) -> Result<(), E> {
        match self.value {
            Some(value) => Deserialize::deserialize(TreeDeserializer::<E>::new(value)),
            None => Ok(()),
        }
    }

    fn newtype_variant_seed<T: DeserializeSeed<'de>>(self, seed: T) -> Result<T::Value, E> {
        match self.value {
            Some(value) => seed.deserialize(TreeDeserializer::new(value)),
            None => Err(E::invalid_type(Unexpected::UnitVariant, &"newtype variant")),
        }
    }

    fn tuple_variant<V: Visitor<'de>>(self, _len: usize, visitor: V) -> Result<V::Value, E> {
        match self.value {
            Some(Tree::Seq(v)) => visit_seq(v, visitor),
            Some(other) => Err(E::invalid_type(other.unexpected(), &"tuple variant")),
            None => Err(E::invalid_type(Unexpected::UnitVariant, &"tuple variant")),
        }
    }

    fn struct_variant<V: Visitor<'de>>(
        self,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, E> {
        match self.value {
            Some(Tree::Map(v)) => visit_map(v, visitor),
            Some(Tree::Seq(v)) => visit_seq(v, visitor),
            Some(other) => Err(E::invalid_type(other.unexpected(), &"struct variant")),
            None => Err(E::invalid_type(Unexpected::UnitVariant, &"struct variant")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::{
        BulkBody, DeletedBody, GapBody, LinkBody, RecoveredBody, SchemaBody, SnapshotEndBody,
        WatermarkBody,
    };

    /// The field names a derived struct decodes, as it hands them to
    /// `deserialize_struct`.
    fn names<'de, T: Deserialize<'de>>() -> Vec<&'static str> {
        struct Probe<'a>(&'a mut Vec<&'static str>);
        impl<'de> Deserializer<'de> for Probe<'_> {
            type Error = de::value::Error;
            fn deserialize_any<V: Visitor<'de>>(self, _: V) -> Result<V::Value, Self::Error> {
                Err(de::Error::custom("not a struct"))
            }
            fn deserialize_struct<V: Visitor<'de>>(
                self,
                _: &'static str,
                fields: &'static [&'static str],
                _: V,
            ) -> Result<V::Value, Self::Error> {
                self.0.extend(fields);
                Err(de::Error::custom("probed"))
            }
            serde::forward_to_deserialize_any! {
                bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
                bytes byte_buf option unit unit_struct newtype_struct seq tuple
                tuple_struct map enum identifier ignored_any
            }
        }
        let mut names = Vec::new();
        let _ = T::deserialize(Probe(&mut names));
        assert!(
            !names.is_empty(),
            "{} is a struct",
            std::any::type_name::<T>()
        );
        names
    }

    #[test]
    fn every_field_has_a_name() {
        let all = [
            names::<StreamId>(),
            names::<Position>(),
            names::<TableRows>(),
            names::<SnapshotEndBody>(),
            names::<SchemaBody>(),
            names::<LinkBody>(),
            names::<RecoveredBody>(),
            names::<DeletedBody>(),
            names::<WatermarkBody>(),
            names::<BulkBody>(),
            names::<GapBody>(),
        ]
        .concat();
        for name in all {
            assert_ne!(Field::from_name(name), Field::Other, "{name}");
            assert_eq!(Field::from_name(name).name(), name);
        }
    }
}
