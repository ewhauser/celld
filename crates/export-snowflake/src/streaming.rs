//! Landing rows through Snowpipe Streaming's REST API (high-performance
//! architecture), on the elastic channel of [`LANDING_PIPE`].
//!
//! The elastic channel suits the export: an append answered 200 is durably
//! buffered by Snowflake, and appends are not ordered, which the tables never
//! needed, since completeness comes from positions and watermarks. There is
//! no channel to open and no offset token to track: the loader commits its
//! topic offsets once every append of a batch is acknowledged. Snowpipe
//! Streaming bills per GB and runs on no warehouse of ours.
//!
//! Connecting: the key-pair JWT the SQL API uses asks the account's host for
//! the ingest host (`GET /v2/streaming/hostname`), then trades itself for a
//! token scoped to that host (`POST /oauth/token`). Appends go to the ingest
//! host with the scoped token. A 401 connects again. Appends run on
//! several threads at once and share one token: whichever finds it missing
//! or stale fetches the next while the others wait, and a 401 drops only
//! the token it was answered for, so one expiry fetches one token.
//!
//! An append carries at most [`MAX_REQUEST_BYTES`] of NDJSON, so a batch
//! may take several. A failed append is sent again with the same request id
//! and the next `retryCount`, which Snowflake may still apply twice; a row
//! landed twice is a duplicate every reader drops.

use std::io::Read as _;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::Deserialize;

use crate::consume::Land;
use crate::loader::WarehouseError;
use crate::sql_api::{
    request_id, Clock, Connection, KeyPair, ATTEMPTS, MAX_BODY, TOKEN_LIFETIME, TOKEN_MARGIN,
};
use crate::{LandingRow, LANDING_PIPE};

/// Snowpipe Streaming's limit on one append's payload, less room to spare.
pub const MAX_REQUEST_BYTES: usize = 4_000_000;

/// How long a scoped token is used before a new one is fetched. Snowflake
/// does not document its lifetime; a 401 also fetches one.
const SCOPED_TOKEN_REFRESH: u64 = 30 * 60;

pub struct Streaming {
    control: String,
    account: String,
    user: String,
    database: String,
    schema: String,
    pipe: String,
    key: KeyPair,
    agent: ureq::Agent,
    /// Shared by every append in flight.
    auth: Mutex<Auth>,
    /// Waits between retries; a test makes it a no-op.
    pub pause: fn(Duration),
    /// The largest append payload; tests make it small.
    pub max_request_bytes: usize,
    /// Payload buffers, back from appends that are done with them.
    buffers: Arc<Mutex<Buffers>>,
}

struct Auth {
    clock: Clock,
    jwt: Option<(String, u64)>,
    ingest: Option<Arc<Ingest>>,
}

struct Ingest {
    /// Scheme and host, such as `https://xy12345.snowflakecomputing.com`.
    base: String,
    token: String,
    fetched: u64,
}

#[derive(Deserialize)]
struct ErrorBody {
    code: Option<String>,
    message: Option<String>,
}

impl Streaming {
    /// Land through [`LANDING_PIPE`] in `connection`'s database and schema,
    /// as `connection`'s user with `key`.
    pub fn new(connection: &Connection, key: KeyPair, clock: Clock) -> Self {
        let agent = ureq::Agent::config_builder()
            .timeout_global(Some(Duration::from_secs(120)))
            .http_status_as_error(false)
            .build()
            .new_agent();
        Streaming {
            control: connection.base_url(),
            account: connection.account.clone(),
            user: connection.user.clone(),
            database: connection.database.clone(),
            schema: connection.schema.clone(),
            pipe: LANDING_PIPE.to_string(),
            key,
            agent,
            auth: Mutex::new(Auth {
                clock,
                jwt: None,
                ingest: None,
            }),
            pause: std::thread::sleep,
            max_request_bytes: MAX_REQUEST_BYTES,
            buffers: Arc::default(),
        }
    }

    fn jwt(&self, auth: &mut Auth) -> String {
        let now = (auth.clock)();
        match &auth.jwt {
            Some((t, expires)) if now + TOKEN_MARGIN < *expires => t.clone(),
            _ => {
                let t = self.key.jwt(&self.account, &self.user, now);
                auth.jwt = Some((t.clone(), now + TOKEN_LIFETIME));
                t
            }
        }
    }

    /// The scoped token, fetching one first if there is none or it is
    /// stale. Appends that ask while one is being fetched wait for it.
    fn ingest(&self) -> Result<Arc<Ingest>, WarehouseError> {
        let mut auth = self.auth.lock().unwrap_or_else(|e| e.into_inner());
        let now = (auth.clock)();
        match &auth.ingest {
            Some(i) if now < i.fetched + SCOPED_TOKEN_REFRESH => Ok(i.clone()),
            _ => {
                let ingest = Arc::new(self.connect(&mut auth)?);
                auth.ingest = Some(ingest.clone());
                Ok(ingest)
            }
        }
    }

    /// Drop the scoped token Snowflake refused, unless another append has
    /// already replaced it.
    fn refused(&self, ingest: &Arc<Ingest>) {
        let mut auth = self.auth.lock().unwrap_or_else(|e| e.into_inner());
        if auth.ingest.as_ref().is_some_and(|i| Arc::ptr_eq(i, ingest)) {
            auth.ingest = None;
        }
    }

    /// Find the ingest host and fetch a token scoped to it.
    fn connect(&self, state: &mut Auth) -> Result<Ingest, WarehouseError> {
        let auth = format!("Bearer {}", self.jwt(state));
        let url = format!("{}/v2/streaming/hostname", self.control);
        let sent = self
            .agent
            .get(&url)
            .header("Authorization", &auth)
            .header("X-Snowflake-Authorization-Token-Type", "KEYPAIR_JWT")
            .header("User-Agent", "celld-export-loader")
            .call();
        let host = answer(sent, "the ingest host")?;
        // Documented as a JSON object and shown as bare text; take either.
        let host = match serde_json::from_str::<serde_json::Value>(&host) {
            Ok(serde_json::Value::Object(o)) => o
                .get("hostname")
                .and_then(|h| h.as_str())
                .unwrap_or_default()
                .to_string(),
            Ok(serde_json::Value::String(s)) => s,
            _ => host.trim().to_string(),
        };
        // An account name's underscores are dashes in a host name.
        let host = host.trim().replace('_', "-");
        if host.is_empty() {
            return Err(WarehouseError::other(
                "Snowpipe Streaming returned no ingest host",
            ));
        }
        let form = format!(
            "grant_type={}&scope={}",
            form_encode("urn:ietf:params:oauth:grant-type:jwt-bearer"),
            form_encode(&host)
        );
        let sent = self
            .agent
            .post(&format!("{}/oauth/token", self.control))
            .header("Authorization", &auth)
            .header("User-Agent", "celld-export-loader")
            .content_type("application/x-www-form-urlencoded")
            .send(form.as_bytes());
        let token = answer(sent, "a scoped token")?;
        let token = match serde_json::from_str::<serde_json::Value>(&token) {
            Ok(serde_json::Value::Object(o)) => o
                .get("token")
                .and_then(|t| t.as_str())
                .unwrap_or_default()
                .to_string(),
            _ => token.trim().to_string(),
        };
        if token.is_empty() {
            return Err(WarehouseError::other(
                "Snowpipe Streaming returned no scoped token",
            ));
        }
        let scheme = self.control.split("://").next().unwrap_or("https");
        Ok(Ingest {
            base: format!("{scheme}://{host}"),
            token,
            fetched: (state.clock)(),
        })
    }

    /// Append one NDJSON payload, retrying what may be retried.
    fn send(&self, payload: &[u8]) -> Result<(), WarehouseError> {
        let request = request_id()?;
        let mut last = String::new();
        let mut delay = Duration::from_secs(1);
        // Requests sent under this id so far: `retryCount` counts those,
        // not attempts that failed before sending.
        let mut sent = 0;
        for attempt in 0..ATTEMPTS {
            if attempt > 0 {
                (self.pause)(delay);
                delay *= 2;
            }
            let ingest = match self.ingest() {
                Ok(i) => i,
                Err(e) => {
                    last = e.message;
                    continue;
                }
            };
            let url = format!(
                "{}/v2/streaming/data/databases/{}/schemas/{}/pipes/{}/channels/ELASTIC/rows?requestId={request}&retryCount={sent}",
                ingest.base, self.database, self.schema, self.pipe
            );
            let response = self
                .agent
                .post(&url)
                .header("Authorization", &format!("Bearer {}", ingest.token))
                .header("User-Agent", "celld-export-loader")
                .content_type("application/x-ndjson")
                .send(payload);
            sent += 1;
            let mut response = match response {
                Ok(r) => r,
                Err(e) => {
                    last = e.to_string();
                    continue;
                }
            };
            let status = response.status().as_u16();
            let mut text = String::new();
            let _ = response
                .body_mut()
                .with_config()
                .limit(MAX_BODY)
                .reader()
                .read_to_string(&mut text);
            match status {
                200 => return Ok(()),
                401 => {
                    // The scoped token expired; fetch another.
                    self.refused(&ingest);
                    last = format!("401: {text}");
                }
                408 | 429 | 500 | 502 | 503 | 504 => last = format!("{status}: {text}"),
                _ => {
                    return Err(match serde_json::from_str::<ErrorBody>(&text) {
                        Ok(b) => WarehouseError {
                            code: b.code,
                            sql_state: None,
                            message: format!(
                                "Snowpipe Streaming answered {status}: {}",
                                b.message.unwrap_or(text)
                            ),
                        },
                        Err(_) => WarehouseError::other(format!(
                            "Snowpipe Streaming answered {status}: {text}"
                        )),
                    })
                }
            }
        }
        Err(WarehouseError::other(format!(
            "Snowpipe Streaming gave up after {ATTEMPTS} attempts: {last}"
        )))
    }
}

impl Streaming {
    fn spares(&self) -> std::sync::MutexGuard<'_, Buffers> {
        self.buffers.lock().unwrap_or_else(|e| e.into_inner())
    }
}

/// One append's NDJSON. Its buffer goes back to the [`Streaming`] it came
/// from once it is dropped: when it has landed, or is given up.
pub struct Payload {
    bytes: Vec<u8>,
    spares: Arc<Mutex<Buffers>>,
}

impl Drop for Payload {
    fn drop(&mut self) {
        let bytes = std::mem::take(&mut self.bytes);
        self.spares
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .give(bytes);
    }
}

impl Land for Streaming {
    type Append = Payload;

    fn encode(&self, rows: &[LandingRow]) -> Result<Vec<Payload>, WarehouseError> {
        // Take the spares rather than hold the lock: batches encode at once.
        let mut spares = std::mem::take(&mut *self.spares());
        let encoded = payloads_from(rows, self.max_request_bytes, &mut spares);
        self.spares().keep(spares);
        Ok(encoded?
            .into_iter()
            .map(|bytes| Payload {
                bytes,
                spares: self.buffers.clone(),
            })
            .collect())
    }

    fn append(&self, payload: &Payload) -> Result<(), WarehouseError> {
        self.send(&payload.bytes)
    }
}

/// `rows` as NDJSON payloads of at most `limit` bytes each, in order.
pub fn payloads(rows: &[LandingRow], limit: usize) -> Result<Vec<Vec<u8>>, WarehouseError> {
    payloads_from(rows, limit, &mut Buffers::default())
}

/// [`payloads`], written into `buffers`' spares where it has them. A
/// payload closes when the next row would take it past `limit`.
///
/// Each row is written straight into its payload, which is allocated once,
/// for what is left of the rows up to `limit`, and never grows. Only a row
/// that might not fit in what is left of its payload is written apart
/// first, to see which payload it goes in.
pub fn payloads_from(
    rows: &[LandingRow],
    limit: usize,
    buffers: &mut Buffers,
) -> Result<Vec<Vec<u8>>, WarehouseError> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let line_bound = |row: &LandingRow| row.json_len_bound() + 1;
    // What the rows not yet written may take.
    let mut left: usize = rows.iter().map(line_bound).sum();
    let mut out = Vec::new();
    let mut current = buffers.take(left.min(limit));
    let mut line = Vec::new();
    for row in rows {
        let bound = line_bound(row);
        if current.len() + bound <= current.capacity().min(limit) {
            row.write_json(&mut current);
            current.push(b'\n');
        } else {
            line.clear();
            line.reserve(bound);
            row.write_json(&mut line);
            line.push(b'\n');
            if line.len() > limit {
                return Err(WarehouseError::other(format!(
                    "the record from {} is {} bytes, more than one append may carry ({limit})",
                    row.source,
                    line.len()
                )));
            }
            if current.len() + line.len() > limit {
                let next = buffers.take(left.min(limit));
                out.push(std::mem::replace(&mut current, next));
            }
            current.extend_from_slice(&line);
        }
        left -= bound;
    }
    out.push(current);
    Ok(out)
}

/// The most payload buffers [`Buffers`] keeps between batches: enough for
/// a batch of the default `EXPORT_BATCH_BYTES`, 8 MiB, in appends of
/// [`MAX_REQUEST_BYTES`].
const SPARE_BUFFERS: usize = 3;

/// Payload buffers kept from one batch for the next, so a payload is
/// written into memory already allocated and touched. A buffer is the
/// pool's again only once its append is done with it: [`Buffers::give`].
#[derive(Default)]
pub struct Buffers(Vec<Vec<u8>>);

impl Buffers {
    /// An empty buffer of at least `capacity` bytes: the smallest spare
    /// that holds that many, or a new one.
    fn take(&mut self, capacity: usize) -> Vec<u8> {
        let spare = (0..self.0.len())
            .filter(|&i| self.0[i].capacity() >= capacity)
            .min_by_key(|&i| self.0[i].capacity());
        match spare {
            Some(i) => self.0.swap_remove(i),
            None => Vec::with_capacity(capacity),
        }
    }

    /// Keep what `other` kept too.
    fn keep(&mut self, other: Buffers) {
        for buffer in other.0 {
            self.give(buffer);
        }
    }

    /// Keep `buffer` for a later payload, in place of a smaller one once
    /// [`SPARE_BUFFERS`] are kept.
    pub fn give(&mut self, mut buffer: Vec<u8>) {
        buffer.clear();
        if self.0.len() < SPARE_BUFFERS {
            self.0.push(buffer);
        } else if let Some(smallest) = self.0.iter_mut().min_by_key(|b| b.capacity()) {
            if smallest.capacity() < buffer.capacity() {
                *smallest = buffer;
            }
        }
    }
}

fn answer(
    sent: Result<ureq::http::Response<ureq::Body>, ureq::Error>,
    what: &str,
) -> Result<String, WarehouseError> {
    let mut response =
        sent.map_err(|e| WarehouseError::other(format!("asking for {what}: {e}")))?;
    let status = response.status().as_u16();
    let mut text = String::new();
    response
        .body_mut()
        .with_config()
        .limit(MAX_BODY)
        .reader()
        .read_to_string(&mut text)
        .map_err(|e| WarehouseError::other(format!("asking for {what}: {e}")))?;
    if status != 200 {
        return Err(WarehouseError::other(format!(
            "asking for {what}: Snowflake answered {status}: {text}"
        )));
    }
    Ok(text)
}

/// `application/x-www-form-urlencoded` for a value.
fn form_encode(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(source: &str, body_bytes: usize) -> LandingRow {
        LandingRow {
            kind: "watermark".into(),
            script: "app".into(),
            class: "Room".into(),
            cell: "r1".into(),
            cell_name: None,
            facet: None,
            incarnation: u64::MAX,
            epoch: 1,
            txid: 2,
            commit: 3,
            committed_at: 1_790_000_000_000,
            node: "n".into(),
            origin: "live".into(),
            fragment: 1,
            fragments: 1,
            body: format!("{{\"x\":\"{}\"}}", "x".repeat(body_bytes)),
            source: source.into(),
        }
    }

    #[test]
    fn payloads_split_at_the_limit_and_keep_every_row_whole() {
        let rows: Vec<LandingRow> = (0..10).map(|i| row(&format!("s{i}"), 100)).collect();
        let line = serde_json::to_vec(&rows[0]).unwrap().len() + 1;
        let out = payloads(&rows, line * 3 + 1).unwrap();
        assert_eq!(out.len(), 4);
        let mut back = Vec::new();
        for p in &out {
            assert!(p.len() <= line * 3 + 1);
            for l in std::str::from_utf8(p).unwrap().lines() {
                back.push(serde_json::from_str::<LandingRow>(l).unwrap());
            }
        }
        assert_eq!(back, rows);
        // u64::MAX is a JSON number the pipe casts to NUMBER(20, 0).
        assert!(std::str::from_utf8(&out[0])
            .unwrap()
            .contains("\"incarnation\":18446744073709551615"));
        assert!(payloads(&[], 10).unwrap().is_empty());
        assert!(payloads(&[row("big", 100)], 50).is_err());
        // The body lands as a VARIANT, so it must be JSON, and is nested.
        assert!(std::str::from_utf8(&out[0])
            .unwrap()
            .contains(&format!("\"body\":{{\"x\":\"{}\"}}", "x".repeat(100))));
    }

    /// `payloads` as it was: each row encoded on its own, then copied in.
    fn payloads_as_they_were(
        rows: &[LandingRow],
        limit: usize,
    ) -> Result<Vec<Vec<u8>>, WarehouseError> {
        let mut out: Vec<Vec<u8>> = Vec::new();
        let mut current: Vec<u8> = Vec::new();
        for row in rows {
            let mut line = serde_json::to_vec(row).unwrap();
            line.push(b'\n');
            if line.len() > limit {
                return Err(WarehouseError::other(format!(
                    "the record from {} is {} bytes, more than one append may carry ({limit})",
                    row.source,
                    line.len()
                )));
            }
            if current.len() + line.len() > limit {
                out.push(std::mem::take(&mut current));
            }
            current.extend_from_slice(&line);
        }
        if !current.is_empty() {
            out.push(current);
        }
        Ok(out)
    }

    #[test]
    fn payloads_are_the_bytes_they_were() {
        // Mixed sizes, escapes, and options, from a fixed seed.
        let mut seed = 0x9e37_79b9_7f4a_7c15_u64;
        let mut next = move |n: u64| {
            seed = seed
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            (seed >> 33) % n
        };
        let rows: Vec<LandingRow> = (0..2_000)
            .map(|i| {
                let size = match next(10) {
                    0 => next(8_000),
                    1 => 0,
                    _ => next(1_500),
                } as usize;
                let mut r = row(&format!("kafka/{}/{i}", next(16)), size);
                if next(4) == 0 {
                    r.cell = format!("c\"\\\n\u{1}é{i}");
                    r.cell_name = Some("名前".repeat(next(5) as usize));
                    r.facet = Some(String::new());
                }
                r.incarnation = next(u64::MAX);
                r
            })
            .collect();
        let longest = rows
            .iter()
            .map(|r| serde_json::to_vec(r).unwrap().len() + 1);
        let longest = longest.max().unwrap();
        let mut buffers = Buffers::default();
        for limit in [
            longest,
            longest + 1,
            9_000,
            20_011,
            100_000,
            MAX_REQUEST_BYTES,
        ] {
            let old = payloads_as_they_were(&rows, limit).unwrap();
            let new = payloads(&rows, limit).unwrap();
            assert_eq!(new, old, "limit {limit}");
            // Never grown past what was allocated for them.
            for p in &new {
                assert!(p.capacity() <= limit, "limit {limit}");
            }
            // Again into buffers kept from earlier batches.
            for _ in 0..2 {
                let again = payloads_from(&rows, limit, &mut buffers).unwrap();
                assert_eq!(again, old, "limit {limit}");
                again.into_iter().for_each(|p| buffers.give(p));
            }
        }
        // One row exactly at the limit is a payload of its own.
        let at = longest;
        let big = rows
            .iter()
            .position(|r| serde_json::to_vec(r).unwrap().len() + 1 == at)
            .unwrap();
        let out = payloads(&rows[big - 1..=big + 1], at).unwrap();
        assert_eq!(
            out,
            payloads_as_they_were(&rows[big - 1..=big + 1], at).unwrap()
        );
        assert_eq!(out.len(), 3);
        assert_eq!(out[1].len(), at);
        // One byte over is the same error, even after payloads that fit.
        let new = payloads_from(&rows, at - 1, &mut buffers).unwrap_err();
        let old = payloads_as_they_were(&rows, at - 1).unwrap_err();
        assert_eq!(new.message, old.message);
        assert!(
            new.message.contains(&format!("is {at} bytes")),
            "{}",
            new.message
        );
        assert!(payloads(&[], 10).unwrap().is_empty());
    }

    #[test]
    fn spare_buffers_are_bounded_and_keep_the_largest() {
        let mut buffers = Buffers::default();
        for capacity in [10, 1_000, 100, 5_000, 1] {
            buffers.give(Vec::with_capacity(capacity));
        }
        let mut kept: Vec<usize> = buffers.0.iter().map(Vec::capacity).collect();
        kept.sort();
        assert_eq!(kept, [100, 1_000, 5_000]);
        let mut taken = buffers.take(2_000);
        assert!(taken.capacity() >= 5_000 && taken.is_empty());
        taken.extend_from_slice(b"x");
        buffers.give(taken);
        assert_eq!(buffers.take(1).capacity(), 100);
        assert!(buffers.take(2_000).capacity() >= 5_000);
        assert_eq!(buffers.take(9_000).capacity(), 9_000);
    }

    #[test]
    fn form_values_are_encoded() {
        assert_eq!(
            form_encode("urn:ietf:params:oauth:grant-type:jwt-bearer"),
            "urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer"
        );
        assert_eq!(form_encode("a-b.c_d:80"), "a-b.c_d%3A80");
    }
}
