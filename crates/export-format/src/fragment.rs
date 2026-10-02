//! Splitting a record under `CELLD_EXPORT_MAX_RECORD_BYTES` and putting the
//! fragments back together.
//!
//! A `rows` or `snapshot` record whose encoding is too large is split by rows
//! into fragments of the same table and position, `i` of `k`. A single row
//! that does not fit on its own turns the whole record into a `bulk` record
//! for its table: the consumer's copy of that table is then unknown until a
//! snapshot covers it, which is what `bulk` says. Every other kind is small
//! and is never split.

use std::collections::BTreeMap;

use crate::dedup::RecordKey;
use crate::record::{Body, BulkBody, Record, RowChange};

/// The result of [`split`] and [`split_encoded`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Split<T = Record> {
    /// The record as `1 of 1`, or its fragments `1..=k of k`, each within the
    /// limit.
    Fragments(Vec<T>),
    /// A row did not fit alone; this `bulk` record replaces the input.
    Bulk(Box<Record>),
}

/// A record with its JSON, exactly as [`Record::to_json`] encodes it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Encoded {
    pub record: Record,
    pub json: Vec<u8>,
}

/// Split `record` so that every piece encodes to at most `max_bytes`.
///
/// The input's `fragment` and `fragments` are overwritten. Kinds other than
/// `rows` and `snapshot` come back whole, whatever their size.
pub fn split(record: Record, max_bytes: usize) -> Split {
    match pieces(record, max_bytes) {
        Pieces::Whole(record, _) => Split::Fragments(vec![record]),
        Pieces::Fragments(fragments) => Split::Fragments(fragments),
        Pieces::Bulk(bulk) => Split::Bulk(bulk),
    }
}

/// [`split`], with each piece's JSON. A record is encoded once: the
/// encoding that measured a record that fits is the one returned, and each
/// fragment is encoded once it is final.
pub fn split_encoded(record: Record, max_bytes: usize) -> Split<Encoded> {
    let encode = |record: Record| Encoded {
        json: record.to_json(),
        record,
    };
    match pieces(record, max_bytes) {
        Pieces::Whole(record, Some(json)) => Split::Fragments(vec![Encoded { record, json }]),
        Pieces::Whole(record, None) => Split::Fragments(vec![encode(record)]),
        Pieces::Fragments(fragments) => {
            Split::Fragments(fragments.into_iter().map(encode).collect())
        }
        Pieces::Bulk(bulk) => Split::Bulk(bulk),
    }
}

// Returned and matched at once, never stored; boxing the common case would
// cost an allocation per record.
#[allow(clippy::large_enum_variant)]
enum Pieces {
    /// `1 of 1`, with its JSON when measuring it took one.
    Whole(Record, Option<Vec<u8>>),
    Fragments(Vec<Record>),
    Bulk(Box<Record>),
}

fn pieces(mut record: Record, max_bytes: usize) -> Pieces {
    record.envelope.fragment = 1;
    record.envelope.fragments = 1;
    let Some(data) = record.body.table_rows() else {
        return Pieces::Whole(record, None);
    };
    if data.rows.is_empty() {
        return Pieces::Whole(record, None);
    }
    let json = record.to_json();
    if json.len() <= max_bytes {
        return Pieces::Whole(record, Some(json));
    }
    drop(json);

    // The size of a fragment with no rows, with the fragment counters at
    // their widest so the estimate never undercounts.
    let mut empty = record.clone();
    empty.envelope.fragment = u32::MAX;
    empty.envelope.fragments = u32::MAX;
    empty
        .body
        .table_rows_mut()
        .expect("checked above")
        .rows
        .clear();
    let base = empty.to_json().len();

    let mut chunks: Vec<Vec<RowChange>> = Vec::new();
    let mut current: Vec<RowChange> = Vec::new();
    let mut size = base;
    for row in &data.rows {
        let row_len = serde_json::to_vec(row).expect("rows always encode").len();
        // One comma between rows.
        let add = row_len + usize::from(!current.is_empty());
        if base + row_len > max_bytes {
            let table = data.table_gen();
            let mut bulk = empty;
            bulk.envelope.fragment = 1;
            bulk.envelope.fragments = 1;
            bulk.body = Body::Bulk(BulkBody {
                tables: vec![table],
            });
            return Pieces::Bulk(Box::new(bulk));
        }
        if size + add > max_bytes {
            chunks.push(std::mem::take(&mut current));
            size = base;
        }
        size += row_len + usize::from(!current.is_empty());
        current.push(row.clone());
    }
    if !current.is_empty() {
        chunks.push(current);
    }

    let k = u32::try_from(chunks.len()).expect("fragment count fits u32");
    let fragments = chunks
        .into_iter()
        .enumerate()
        .map(|(i, rows)| {
            let mut r = empty.clone();
            r.envelope.fragment = i as u32 + 1;
            r.envelope.fragments = k;
            r.body.table_rows_mut().expect("checked above").rows = rows;
            r
        })
        .collect();
    Pieces::Fragments(fragments)
}

/// Why a fragment was refused.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ReassembleError {
    #[error("fragment {fragment} of {fragments} is out of range")]
    OutOfRange { fragment: u32, fragments: u32 },
    #[error("fragment disagrees with its siblings on the fragment count or header")]
    Inconsistent,
    #[error("fragmented record of a kind that is never split")]
    NotSplittable,
}

/// Collects fragments and yields each record once all of its fragments are
/// present. Duplicate fragments are ignored, so it tolerates at-least-once
/// delivery.
#[derive(Default, Debug)]
pub struct Reassembler {
    partial: BTreeMap<RecordKey, BTreeMap<u32, Record>>,
}

impl Reassembler {
    pub fn new() -> Self {
        Self::default()
    }

    /// Offer one fragment. Returns the whole record when this completes it.
    pub fn push(&mut self, fragment: Record) -> Result<Option<Record>, ReassembleError> {
        let (i, k) = (fragment.envelope.fragment, fragment.envelope.fragments);
        if k == 0 || i == 0 || i > k {
            return Err(ReassembleError::OutOfRange {
                fragment: i,
                fragments: k,
            });
        }
        if k == 1 {
            return Ok(Some(fragment));
        }
        if fragment.body.table_rows().is_none() {
            return Err(ReassembleError::NotSplittable);
        }
        let key = RecordKey::of(&fragment);
        let parts = self.partial.entry(key.clone()).or_default();
        if let Some(sibling) = parts.values().next() {
            if !same_header(sibling, &fragment) {
                return Err(ReassembleError::Inconsistent);
            }
        }
        parts.entry(i).or_insert(fragment);
        if parts.len() as u32 != k {
            return Ok(None);
        }
        let parts = self.partial.remove(&key).expect("present");
        let mut iter = parts.into_values();
        let mut whole = iter.next().expect("k > 1");
        let rows = &mut whole.body.table_rows_mut().expect("checked").rows;
        for part in iter {
            rows.extend(into_rows(part.body));
        }
        whole.envelope.fragment = 1;
        whole.envelope.fragments = 1;
        Ok(Some(whole))
    }

    /// Records with some but not all fragments.
    pub fn incomplete(&self) -> usize {
        self.partial.len()
    }
}

fn into_rows(body: Body) -> Vec<RowChange> {
    match body {
        Body::Rows(b) => b.data.rows,
        Body::Snapshot(b) => b.data.rows,
        _ => unreachable!("only rows and snapshot records are fragmented"),
    }
}

/// Everything but the fragment index and the rows must match.
fn same_header(a: &Record, b: &Record) -> bool {
    let mut a = a.clone();
    let mut b = b.clone();
    a.envelope.fragment = 0;
    b.envelope.fragment = 0;
    a.body.table_rows_mut().expect("checked").rows.clear();
    b.body.table_rows_mut().expect("checked").rows.clear();
    a == b
}
