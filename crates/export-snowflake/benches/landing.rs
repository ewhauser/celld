//! A message off the export topic to the NDJSON Snowpipe Streaming appends:
//! decoding it into a landing row, then encoding the row.
use celld_export_format::{Body, Value};
use celld_export_snowflake::consume::{Batch, Land};
use celld_export_snowflake::streaming::{payloads, payloads_from, Buffers, MAX_REQUEST_BYTES};
use celld_export_snowflake::{LandingRow, WarehouseError};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::hint::black_box;
#[path = "../../export-format/benches/support/mod.rs"]
mod support;

/// Keeps what lands.
#[derive(Default)]
struct Keep(Vec<LandingRow>);

impl Land for Keep {
    fn land(&mut self, rows: &[LandingRow]) -> Result<(), WarehouseError> {
        self.0.extend_from_slice(rows);
        Ok(())
    }
}

/// Encodes what lands as the appends would carry it.
struct Encode;

impl Land for Encode {
    fn land(&mut self, rows: &[LandingRow]) -> Result<(), WarehouseError> {
        black_box(payloads(rows, MAX_REQUEST_BYTES)?);
        Ok(())
    }
}

/// `rows` rows of the shared fixture, each blob `blob` bytes long.
fn message(rows: usize, blob: usize) -> Vec<u8> {
    let mut record = support::rows_record(rows, 128);
    let Body::Rows(body) = &mut record.body else {
        unreachable!()
    };
    for (i, change) in body.data.rows.iter_mut().enumerate() {
        change.2[2] = Value::Blob((0..blob).map(|n| (n + i) as u8).collect());
    }
    record.to_json()
}

fn land(batch: &mut Batch, payload: &[u8], to: &mut impl Land) {
    batch.push_message("kafka", 0, 1, payload).unwrap();
    batch.land(to).unwrap();
}

fn landing(c: &mut Criterion) {
    let mut g = c.benchmark_group("export_landing");
    for (name, rows, blob) in [("1", 1, 32), ("128", 128, 32), ("128_blob_4k", 128, 4096)] {
        let payload = message(rows, blob);
        let mut batch = Batch::default();
        let mut kept = Keep::default();
        land(&mut batch, &payload, &mut kept);
        assert_eq!(kept.0.len(), 1);
        g.throughput(Throughput::Bytes(payload.len() as u64));
        g.bench_with_input(BenchmarkId::new("to_row", name), &payload, |b, p| {
            let mut kept = Keep::default();
            b.iter(|| {
                land(&mut batch, black_box(p), &mut kept);
                kept.0.clear();
            })
        });
        g.bench_with_input(BenchmarkId::new("to_ndjson", name), &kept.0, |b, rows| {
            b.iter(|| payloads(black_box(rows), MAX_REQUEST_BYTES).unwrap())
        });
        g.bench_with_input(
            BenchmarkId::new("message_to_ndjson", name),
            &payload,
            |b, p| b.iter(|| land(&mut batch, black_box(p), &mut Encode)),
        );
    }
    g.finish();
}

/// `n` rows of about 1.3 KB of JSON each, as real records run: a few
/// changes to a cell, each with a different cell and offset.
fn realistic_rows(n: usize) -> Vec<LandingRow> {
    (0..n)
        .map(|i| {
            let mut record = support::rows_record(1 + i % 3, [800, 350, 200][i % 3]);
            record.envelope = support::envelope(i, i as u64);
            LandingRow::from_record(&record, format!("kafka/{}/{i}", i % 16))
        })
        .collect()
}

fn appends(c: &mut Criterion) {
    let mut g = c.benchmark_group("export_appends");
    let rows = realistic_rows(10_000);
    let bytes: usize = payloads(&rows, MAX_REQUEST_BYTES)
        .unwrap()
        .iter()
        .map(Vec::len)
        .sum();
    g.throughput(Throughput::Bytes(bytes as u64));
    g.bench_function("payloads/10k", |b| {
        b.iter(|| payloads(black_box(&rows), MAX_REQUEST_BYTES).unwrap())
    });
    // As `Streaming` lands: buffers given back once their appends are done.
    let mut buffers = Buffers::default();
    g.bench_function("payloads_reused/10k", |b| {
        b.iter(|| {
            for p in payloads_from(black_box(&rows), MAX_REQUEST_BYTES, &mut buffers).unwrap() {
                buffers.give(black_box(p));
            }
        })
    });
    g.finish();
}

criterion_group!(benches, landing, appends);
criterion_main!(benches);
